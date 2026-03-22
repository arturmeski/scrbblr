//! Watcher module — tracks playback state and decides when to scrobble.
//!
//! This module implements the core scrobbling logic. It processes two streams
//! of events coming from two separate `playerctl --follow` processes:
//!
//! 1. **Metadata events** — emitted when the currently playing track changes.
//!    Each event contains the artist, album, title, and duration (in microseconds).
//!
//! 2. **Status events** — emitted when the player's state changes between
//!    Playing, Paused, and Stopped.
//!
//! The `ScrobbleTracker` state machine combines these events to accurately
//! measure how long the user actually listened to each track (excluding paused
//! time), and decides whether to scrobble based on the Last.fm threshold:
//! **50% of the track's duration or 4 minutes, whichever is shorter**.
//!
//! ## Architecture
//!
//! The tracker is generic over a callback function (`scrobble_fn`), which is
//! called whenever a track qualifies for scrobbling. In production, this
//! callback inserts into SQLite. In tests, it pushes to a `Vec` for assertion.
//!
//! For testing, a separate `TestableTracker` struct exists that replaces
//! `Instant::now()` with a manually-advanced clock, so we can simulate
//! time passing without actual delays.

use std::time::Instant;

use crate::db::{self, NewScrobble};
use rusqlite::Connection;

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

/// Events processed by the watcher's main event loop.
///
/// These are sent over an `mpsc::channel` from the reader threads (one per
/// playerctl process) to the main thread which owns the `ScrobbleTracker`.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A new track started playing. Emitted when playerctl's metadata output
    /// produces a new line (i.e., the track changed).
    Metadata {
        artist: String,
        album: String,
        title: String,
        /// Track duration in microseconds, as reported by MPRIS (`mpris:length`).
        /// For example, 186_000_000 = 186 seconds = 3m06s.
        /// `None` if the player didn't provide duration info.
        duration_us: Option<u64>,
    },
    /// The player's playback state changed (Playing, Paused, or Stopped).
    Status(PlayerStatus),
    /// One of the playerctl child processes ended (stdout closed).
    /// When both metadata and status processes have sent Eof, the watcher
    /// evaluates the last track and shuts down.
    Eof,
}

/// Possible playback states reported by `playerctl --follow status`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PlayerStatus {
    Playing,
    Paused,
    Stopped,
}

// ---------------------------------------------------------------------------
// Internal track representation
// ---------------------------------------------------------------------------

/// Holds metadata about the track currently being monitored.
/// This is internal to the tracker — not exposed outside the module.
#[derive(Debug, Clone)]
struct CurrentTrack {
    artist: String,
    album: String,
    title: String,
    /// Duration in microseconds as reported by MPRIS, or None if unavailable.
    duration_us: Option<u64>,
}

impl CurrentTrack {
    /// Calculate the scrobble threshold for this track.
    ///
    /// Follows the Last.fm convention:
    /// - If duration is known: min(50% of duration, 240 seconds)
    /// - If duration is unknown: 240 seconds (4 minutes)
    ///
    /// This means short tracks (< 8 min) need 50% play time, while long
    /// tracks cap out at 4 minutes of required listening.
    fn threshold_secs(&self) -> f64 {
        match self.duration_us {
            Some(us) => {
                let half = (us as f64 / 1_000_000.0) * 0.5;
                half.min(240.0)
            }
            None => 240.0,
        }
    }

    /// Convert the MPRIS duration (microseconds) to whole seconds for storage.
    /// Returns None if the duration was not provided by the player.
    fn duration_secs(&self) -> Option<i64> {
        self.duration_us.map(|us| (us / 1_000_000) as i64)
    }
}

// ---------------------------------------------------------------------------
// ScrobbleTracker — production version using real wall-clock time
// ---------------------------------------------------------------------------

/// The core state machine that tracks playback and decides when to scrobble.
///
/// Generic over `F` — the callback invoked when a track qualifies for
/// scrobbling. This allows the same logic to be used with a real DB callback
/// in production and a test collector in unit tests.
///
/// ## State
///
/// - `current_track` — the track currently being monitored (None if idle)
/// - `is_playing` — whether the player is currently in Playing state
/// - `playing_since` — the `Instant` when the current Playing stretch began
///   (None if paused or no track)
/// - `accumulated_secs` — total seconds of actual play time for the current
///   track, accumulated across multiple play/pause cycles
pub struct ScrobbleTracker<F: FnMut(NewScrobble)> {
    current_track: Option<CurrentTrack>,
    is_playing: bool,
    playing_since: Option<Instant>,
    accumulated_secs: f64,
    scrobble_fn: F,
    /// The source label recorded alongside each scrobble (e.g. `"MPD"`).
    source: String,
}

impl<F: FnMut(NewScrobble)> ScrobbleTracker<F> {
    pub fn new(scrobble_fn: F, source: String) -> Self {
        Self {
            current_track: None,
            is_playing: false,
            playing_since: None,
            accumulated_secs: 0.0,
            scrobble_fn,
            source,
        }
    }

    /// Process an incoming event and update internal state.
    ///
    /// ## Event handling:
    ///
    /// - **Metadata**: A new track started. Evaluate the previous track
    ///   (scrobble if threshold met), then begin tracking the new one.
    ///   We assume the new track starts in Playing state.
    ///
    /// - **Status(Playing)**: Resume accumulating play time. If already
    ///   playing, this is a no-op (avoids double-counting).
    ///
    /// - **Status(Paused)**: Flush the current playing stretch into
    ///   `accumulated_secs` and stop the clock. The track remains active
    ///   so play time continues to accumulate when resumed.
    ///
    /// - **Status(Stopped)**: Evaluate the current track immediately (the
    ///   same logic as a new Metadata event or Eof). If the threshold is
    ///   met, scrobble; if not, discard. Either way, reset tracking state
    ///   so that a spurious subsequent Status(Playing) cannot accumulate
    ///   phantom time against the same track.
    ///
    /// - **Eof**: A playerctl process ended. Evaluate the current track
    ///   one last time (so the final track can be scrobbled on shutdown).
    pub fn handle_event(&mut self, event: Event) {
        match event {
            Event::Metadata {
                artist,
                album,
                title,
                duration_us,
            } => {
                // Evaluate the previous track before switching to the new one.
                self.evaluate_previous_track();

                // Start tracking the new track.
                self.current_track = Some(CurrentTrack {
                    artist,
                    album,
                    title,
                    duration_us,
                });
                self.accumulated_secs = 0.0;

                // A metadata event means the player is actively playing the new track.
                self.is_playing = true;
                self.playing_since = Some(Instant::now());
            }
            Event::Status(status) => match status {
                PlayerStatus::Playing => {
                    // Only start the clock if we weren't already playing.
                    // This prevents double-counting if we receive redundant Playing events.
                    if !self.is_playing {
                        self.is_playing = true;
                        self.playing_since = Some(Instant::now());
                    }
                }
                PlayerStatus::Paused => {
                    // Flush the elapsed time from the current playing stretch
                    // into the accumulator, then stop the clock.
                    self.flush_playing_time();
                    self.is_playing = false;
                    self.playing_since = None;
                }
                PlayerStatus::Stopped => {
                    // Treat Stop as a final decision: evaluate the current
                    // track now (scrobble if threshold met, discard if not)
                    // and clear all tracking state. This prevents a spurious
                    // Status(Playing) that some players emit after Stopped
                    // from accumulating phantom time against the same track.
                    self.evaluate_previous_track();
                    self.accumulated_secs = 0.0;
                    self.is_playing = false;
                }
            },
            Event::Eof => {
                // Evaluate the last track when the player process ends.
                self.evaluate_previous_track();
                self.current_track = None;
            }
        }
    }

    /// If the player is currently in Playing state, calculate how much time
    /// has elapsed since `playing_since` and add it to `accumulated_secs`.
    /// Resets `playing_since` to None so the time isn't counted twice.
    fn flush_playing_time(&mut self) {
        if let Some(since) = self.playing_since.take() {
            self.accumulated_secs += since.elapsed().as_secs_f64();
        }
    }

    /// Check whether the current track has been played long enough to qualify
    /// for scrobbling. If so, invoke `scrobble_fn` with the track data.
    ///
    /// This is called:
    /// - When a new track starts (to evaluate the outgoing track)
    /// - On Eof (to evaluate the last track before shutdown)
    fn evaluate_previous_track(&mut self) {
        // Flush any in-progress playing time first.
        self.flush_playing_time();

        if let Some(track) = self.current_track.take() {
            let threshold = track.threshold_secs();

            // Only scrobble if the user listened for at least the threshold duration.
            if self.accumulated_secs >= threshold {
                let now = chrono::Local::now()
                    .naive_local()
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string();

                // Extract duration before moving fields out of `track`.
                let track_dur = track.duration_secs();

                let scrobble = NewScrobble {
                    artist: track.artist,
                    album: track.album,
                    title: track.title,
                    track_duration_secs: track_dur,
                    played_duration_secs: self.accumulated_secs.round() as i64,
                    scrobbled_at: now,
                    source: self.source.clone(),
                };
                (self.scrobble_fn)(scrobble);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Parsing functions
// ---------------------------------------------------------------------------

/// Parse a metadata line from playerctl's `--follow metadata --format` output.
///
/// Expected format (tab-separated):
///   `{artist}\t{album}\t{title}\t{mpris:length}`
///
/// The duration field (mpris:length) is optional — if missing or unparseable,
/// it will be `None`. At minimum, we need 3 tab-separated fields (artist,
/// album, title). Lines with both artist and title empty are rejected.
///
/// Example input:
///   `"††† (Crosses)\t††† (Crosses)\tThis Is a Trick\t186000000"`
pub fn parse_metadata_line(line: &str) -> Option<Event> {
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.len() < 3 {
        return None;
    }

    let artist = parts[0].trim().to_string();
    let album = parts[1].trim().to_string();
    let title = parts[2].trim().to_string();

    // The 4th field is mpris:length in microseconds. It may be missing entirely,
    // empty, or contain a non-numeric value — all of which result in None.
    let duration_us = parts.get(3).and_then(|s| s.trim().parse::<u64>().ok());

    // Reject lines where both artist and title are empty (no useful metadata).
    if artist.is_empty() && title.is_empty() {
        return None;
    }

    Some(Event::Metadata {
        artist,
        album,
        title,
        duration_us,
    })
}

/// Parse a status line from playerctl's `--follow status` output.
///
/// Expected values: "Playing", "Paused", or "Stopped" (with optional
/// trailing whitespace/newlines).
///
/// Returns `None` for unrecognized status strings.
pub fn parse_status_line(line: &str) -> Option<Event> {
    match line.trim() {
        "Playing" => Some(Event::Status(PlayerStatus::Playing)),
        "Paused" => Some(Event::Status(PlayerStatus::Paused)),
        "Stopped" => Some(Event::Status(PlayerStatus::Stopped)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Player name normalisation
// ---------------------------------------------------------------------------

/// Normalise a D-Bus player name (e.g. `"com.blitzfc.qbz"`) into a
/// human-readable source label (e.g. `"Qobuz"`).
///
/// Logic:
/// 1. Take the last `.`-separated segment of the D-Bus name.
/// 2. Apply known mappings (matched case-insensitively):
///    - `"qbz"` or `"qbz2"` → `"Qobuz"`
/// 3. Otherwise: capitalise the first character of the segment.
pub fn normalise_player_name(player: &str) -> String {
    let segment = player.rsplit('.').next().unwrap_or(player);
    match segment.to_lowercase().as_str() {
        "qbz" | "qbz2" => "Qobuz".to_string(),
        _ => {
            let mut chars = segment.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => {
                    let upper: String = first.to_uppercase().collect();
                    upper + chars.as_str()
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Factory for production use
// ---------------------------------------------------------------------------

/// Create a `ScrobbleTracker` wired up to insert scrobbles into the database.
///
/// The callback acquires the mutex, inserts the scrobble, and logs the result
/// to stderr. The `Arc<Mutex<Connection>>` is shared with the main thread
/// but only accessed from the main event loop (single-threaded), so contention
/// is minimal.
///
/// The `source` parameter names the player or service that produced these
/// scrobbles (e.g. `"MPD"`, `"Qobuz"`). It is stored alongside each scrobble
/// and shown in source-breakdown reports.
pub fn create_db_tracker(
    conn: std::sync::Arc<std::sync::Mutex<Connection>>,
    source: String,
) -> ScrobbleTracker<impl FnMut(NewScrobble)> {
    ScrobbleTracker::new(move |scrobble: NewScrobble| {
        let conn = conn.lock().unwrap();
        match db::insert_scrobble(&conn, &scrobble) {
            Ok(_) => {
                eprintln!(
                    "[scrobbled] {}: {} - {} ({}s)",
                    scrobble.source, scrobble.artist, scrobble.title, scrobble.played_duration_secs
                );
            }
            Err(e) => {
                eprintln!("[error] Failed to insert scrobble: {}", e);
            }
        }
    }, source)
}

// ===========================================================================
// Test-only code
// ===========================================================================

/// A version of `ScrobbleTracker` that replaces `Instant::now()` with a
/// manually-controlled clock. This lets tests simulate time passing
/// (e.g., "advance 100 seconds") without real delays.
///
/// Instead of calling a callback, scrobbled tracks are collected into the
/// `scrobbled` Vec for inspection in assertions.
#[cfg(test)]
pub struct TestableTracker {
    current_track: Option<CurrentTrack>,
    is_playing: bool,
    /// Simulated timestamp (in seconds) when the current Playing stretch began.
    playing_since_secs: Option<f64>,
    /// Accumulated play time for the current track (in seconds).
    accumulated_secs: f64,
    /// All tracks that were scrobbled during the test.
    pub scrobbled: Vec<NewScrobble>,
    /// The current simulated time, in seconds since the start of the test.
    clock_secs: f64,
    /// Source label attached to every scrobble produced during the test.
    source: String,
}

#[cfg(test)]
impl TestableTracker {
    pub fn new() -> Self {
        Self {
            current_track: None,
            is_playing: false,
            playing_since_secs: None,
            accumulated_secs: 0.0,
            scrobbled: Vec::new(),
            clock_secs: 0.0,
            source: "test".to_string(),
        }
    }

    /// Advance the simulated clock by the given number of seconds.
    /// Call this between events to simulate time passing.
    pub fn advance_time(&mut self, secs: f64) {
        self.clock_secs += secs;
    }

    /// Flush elapsed playing time from the simulated clock into the accumulator.
    /// Mirrors `ScrobbleTracker::flush_playing_time()` but uses `clock_secs`
    /// instead of `Instant::elapsed()`.
    fn flush_playing_time(&mut self) {
        if let Some(since) = self.playing_since_secs.take() {
            self.accumulated_secs += self.clock_secs - since;
        }
    }

    /// Evaluate the current track against the scrobble threshold.
    /// If it qualifies, push it onto the `scrobbled` Vec.
    fn evaluate_previous_track(&mut self) {
        self.flush_playing_time();

        if let Some(track) = self.current_track.take() {
            let threshold = track.threshold_secs();
            if self.accumulated_secs >= threshold {
                let track_dur = track.duration_secs();
                let scrobble = NewScrobble {
                    artist: track.artist,
                    album: track.album,
                    title: track.title,
                    track_duration_secs: track_dur,
                    played_duration_secs: self.accumulated_secs.round() as i64,
                    scrobbled_at: format!("test-time-{}", self.clock_secs),
                    source: self.source.clone(),
                };
                self.scrobbled.push(scrobble);
            }
        }
    }

    /// Process an event — mirrors `ScrobbleTracker::handle_event()` exactly,
    /// but uses simulated time instead of real wall-clock time.
    pub fn handle_event(&mut self, event: Event) {
        match event {
            Event::Metadata {
                artist,
                album,
                title,
                duration_us,
            } => {
                self.evaluate_previous_track();
                self.current_track = Some(CurrentTrack {
                    artist,
                    album,
                    title,
                    duration_us,
                });
                self.accumulated_secs = 0.0;
                self.is_playing = true;
                self.playing_since_secs = Some(self.clock_secs);
            }
            Event::Status(status) => match status {
                PlayerStatus::Playing => {
                    if !self.is_playing {
                        self.is_playing = true;
                        self.playing_since_secs = Some(self.clock_secs);
                    }
                }
                PlayerStatus::Paused => {
                    self.flush_playing_time();
                    self.is_playing = false;
                    self.playing_since_secs = None;
                }
                PlayerStatus::Stopped => {
                    self.evaluate_previous_track();
                    self.accumulated_secs = 0.0;
                    self.is_playing = false;
                }
            },
            Event::Eof => {
                self.evaluate_previous_track();
                self.current_track = None;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // =======================================================================
    // Parsing tests
    // =======================================================================

    #[test]
    fn test_parse_metadata_line_normal() {
        // Standard line with all four fields present.
        let line = "††† (Crosses)\t††† (Crosses)\tThis Is a Trick\t186000000";
        let event = parse_metadata_line(line).unwrap();
        assert_eq!(
            event,
            Event::Metadata {
                artist: "††† (Crosses)".to_string(),
                album: "††† (Crosses)".to_string(),
                title: "This Is a Trick".to_string(),
                duration_us: Some(186_000_000),
            }
        );
    }

    #[test]
    fn test_parse_metadata_line_missing_duration() {
        // Duration field is present but empty — should parse as None.
        let line = "Artist\tAlbum\tTitle\t";
        let event = parse_metadata_line(line).unwrap();
        assert_eq!(
            event,
            Event::Metadata {
                artist: "Artist".to_string(),
                album: "Album".to_string(),
                title: "Title".to_string(),
                duration_us: None,
            }
        );
    }

    #[test]
    fn test_parse_metadata_line_no_duration_field() {
        // Only three fields (no duration column at all).
        let line = "Artist\tAlbum\tTitle";
        let event = parse_metadata_line(line).unwrap();
        assert_eq!(
            event,
            Event::Metadata {
                artist: "Artist".to_string(),
                album: "Album".to_string(),
                title: "Title".to_string(),
                duration_us: None,
            }
        );
    }

    #[test]
    fn test_parse_metadata_line_empty_artist_and_title() {
        // Both artist and title are empty — should be rejected.
        let line = "\tAlbum\t\t100";
        assert!(parse_metadata_line(line).is_none());
    }

    #[test]
    fn test_parse_metadata_line_too_few_fields() {
        // Only two fields — not enough to form a valid metadata event.
        let line = "Artist\tAlbum";
        assert!(parse_metadata_line(line).is_none());
    }

    #[test]
    fn test_parse_status_line() {
        assert_eq!(
            parse_status_line("Playing"),
            Some(Event::Status(PlayerStatus::Playing))
        );
        assert_eq!(
            parse_status_line("Paused"),
            Some(Event::Status(PlayerStatus::Paused))
        );
        assert_eq!(
            parse_status_line("Stopped"),
            Some(Event::Status(PlayerStatus::Stopped))
        );
        // Unrecognized status should return None.
        assert_eq!(parse_status_line("Unknown"), None);
        // Trailing newline should be handled gracefully.
        assert_eq!(
            parse_status_line("Playing\n"),
            Some(Event::Status(PlayerStatus::Playing))
        );
    }

    // =======================================================================
    // Threshold calculation tests
    // =======================================================================

    #[test]
    fn test_threshold_known_duration() {
        let track = CurrentTrack {
            artist: "A".into(),
            album: "B".into(),
            title: "C".into(),
            duration_us: Some(186_000_000), // 186 seconds
        };
        // 50% of 186s = 93s. min(93, 240) = 93s.
        assert!((track.threshold_secs() - 93.0).abs() < 0.01);
    }

    #[test]
    fn test_threshold_long_track() {
        let track = CurrentTrack {
            artist: "A".into(),
            album: "B".into(),
            title: "C".into(),
            duration_us: Some(600_000_000), // 600 seconds = 10 minutes
        };
        // 50% of 600s = 300s. min(300, 240) = 240s (capped at 4 minutes).
        assert!((track.threshold_secs() - 240.0).abs() < 0.01);
    }

    #[test]
    fn test_threshold_unknown_duration() {
        let track = CurrentTrack {
            artist: "A".into(),
            album: "B".into(),
            title: "C".into(),
            duration_us: None,
        };
        // Unknown duration defaults to 240s (4 minutes).
        assert!((track.threshold_secs() - 240.0).abs() < 0.01);
    }

    // =======================================================================
    // Scrobble decision tests (using TestableTracker with simulated time)
    // =======================================================================

    #[test]
    fn test_scrobble_after_threshold() {
        // Play a track for longer than its threshold — it should be scrobbled
        // when the next track starts.
        let mut tracker = TestableTracker::new();

        tracker.handle_event(Event::Metadata {
            artist: "††† (Crosses)".into(),
            album: "††† (Crosses)".into(),
            title: "This Is a Trick".into(),
            duration_us: Some(186_000_000), // threshold = 93s
        });

        // Simulate playing for 100 seconds (above the 93s threshold).
        tracker.advance_time(100.0);

        // When the next track arrives, the previous one gets evaluated.
        tracker.handle_event(Event::Metadata {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            title: "Digital Bath".into(),
            duration_us: Some(291_000_000),
        });

        assert_eq!(tracker.scrobbled.len(), 1);
        assert_eq!(tracker.scrobbled[0].title, "This Is a Trick");
        assert_eq!(tracker.scrobbled[0].played_duration_secs, 100);
    }

    #[test]
    fn test_no_scrobble_below_threshold() {
        // Skip a track after only 10 seconds — should NOT be scrobbled.
        let mut tracker = TestableTracker::new();

        tracker.handle_event(Event::Metadata {
            artist: "††† (Crosses)".into(),
            album: "††† (Crosses)".into(),
            title: "This Is a Trick".into(),
            duration_us: Some(186_000_000), // threshold = 93s
        });

        tracker.advance_time(10.0); // Only 10s — well below 93s threshold.

        tracker.handle_event(Event::Metadata {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            title: "Digital Bath".into(),
            duration_us: Some(291_000_000),
        });

        assert_eq!(tracker.scrobbled.len(), 0);
    }

    #[test]
    fn test_pause_resume_accumulates_correctly() {
        // Play 50s → pause for 1 hour → resume → play 60s more.
        // Total actual play time = 110s, which exceeds the 107.5s threshold.
        // The 1-hour pause should NOT count.
        let mut tracker = TestableTracker::new();

        tracker.handle_event(Event::Metadata {
            artist: "††† (Crosses)".into(),
            album: "††† (Crosses)".into(),
            title: "Telepathy".into(),
            duration_us: Some(215_000_000), // threshold = 107.5s
        });

        // Play for 50 seconds, then pause.
        tracker.advance_time(50.0);
        tracker.handle_event(Event::Status(PlayerStatus::Paused));

        // Paused for 1 hour — this time should NOT be counted.
        tracker.advance_time(3600.0);
        tracker.handle_event(Event::Status(PlayerStatus::Playing));

        // Play for 60 more seconds. Total play time: 50 + 60 = 110s.
        tracker.advance_time(60.0);

        // Next track triggers evaluation of "Telepathy".
        tracker.handle_event(Event::Metadata {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            title: "Digital Bath".into(),
            duration_us: Some(291_000_000),
        });

        assert_eq!(tracker.scrobbled.len(), 1);
        assert_eq!(tracker.scrobbled[0].title, "Telepathy");
        // 50 + 60 = 110 seconds of actual play time.
        assert_eq!(tracker.scrobbled[0].played_duration_secs, 110);
    }

    #[test]
    fn test_pause_resume_below_threshold() {
        // Play 30s → pause → play 30s = 60s total, below the 107.5s threshold.
        let mut tracker = TestableTracker::new();

        tracker.handle_event(Event::Metadata {
            artist: "††† (Crosses)".into(),
            album: "††† (Crosses)".into(),
            title: "Telepathy".into(),
            duration_us: Some(215_000_000), // threshold = 107.5s
        });

        tracker.advance_time(30.0);
        tracker.handle_event(Event::Status(PlayerStatus::Paused));
        tracker.advance_time(500.0); // Long pause — doesn't count.
        tracker.handle_event(Event::Status(PlayerStatus::Playing));
        tracker.advance_time(30.0);

        // Total play time = 60s, below 107.5s threshold.
        tracker.handle_event(Event::Metadata {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            title: "Digital Bath".into(),
            duration_us: Some(291_000_000),
        });

        assert_eq!(tracker.scrobbled.len(), 0);
    }

    #[test]
    fn test_eof_evaluates_last_track() {
        // The last track should be scrobbled when the player process ends (Eof),
        // not just when a new track starts.
        let mut tracker = TestableTracker::new();

        tracker.handle_event(Event::Metadata {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            title: "Digital Bath".into(),
            duration_us: Some(291_000_000), // threshold = 145.5s
        });

        tracker.advance_time(200.0);
        tracker.handle_event(Event::Eof);

        assert_eq!(tracker.scrobbled.len(), 1);
        assert_eq!(tracker.scrobbled[0].title, "Digital Bath");
    }

    #[test]
    fn test_unknown_duration_uses_4min_threshold() {
        // When duration is unknown, the threshold falls back to 240s (4 minutes).
        let mut tracker = TestableTracker::new();

        // First attempt: play for 200s (below 240s) — should NOT scrobble.
        tracker.handle_event(Event::Metadata {
            artist: "Unknown".into(),
            album: "".into(),
            title: "Mystery".into(),
            duration_us: None,
        });
        tracker.advance_time(200.0);
        tracker.handle_event(Event::Eof);
        assert_eq!(tracker.scrobbled.len(), 0);

        // Second attempt: play for 250s (above 240s) — should scrobble.
        let mut tracker2 = TestableTracker::new();
        tracker2.handle_event(Event::Metadata {
            artist: "Unknown".into(),
            album: "".into(),
            title: "Mystery".into(),
            duration_us: None,
        });
        tracker2.advance_time(250.0);
        tracker2.handle_event(Event::Eof);
        assert_eq!(tracker2.scrobbled.len(), 1);
    }

    #[test]
    fn test_multiple_tracks_sequence() {
        // Simulate a listening session with 3 tracks:
        //   Track 1: played fully (186s > 93s threshold) → scrobbled
        //   Track 2: skipped quickly (5s < 107.5s threshold) → NOT scrobbled
        //   Track 3: played fully (291s > 145.5s threshold) → scrobbled
        let mut tracker = TestableTracker::new();

        // Track 1: ††† (Crosses) - This Is a Trick
        tracker.handle_event(Event::Metadata {
            artist: "††† (Crosses)".into(),
            album: "††† (Crosses)".into(),
            title: "This Is a Trick".into(),
            duration_us: Some(186_000_000), // threshold = 93s
        });
        tracker.advance_time(186.0);

        // Track 2: ††† (Crosses) - Telepathy (skipped after 5 seconds)
        tracker.handle_event(Event::Metadata {
            artist: "††† (Crosses)".into(),
            album: "††† (Crosses)".into(),
            title: "Telepathy".into(),
            duration_us: Some(215_000_000), // threshold = 107.5s
        });
        tracker.advance_time(5.0);

        // Track 3: Deftones - Digital Bath
        tracker.handle_event(Event::Metadata {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            title: "Digital Bath".into(),
            duration_us: Some(291_000_000), // threshold = 145.5s
        });
        tracker.advance_time(291.0);

        // End of session.
        tracker.handle_event(Event::Eof);

        // Only tracks 1 and 3 should be scrobbled.
        assert_eq!(tracker.scrobbled.len(), 2);
        assert_eq!(tracker.scrobbled[0].title, "This Is a Trick");
        assert_eq!(tracker.scrobbled[1].title, "Digital Bath");
    }

    #[test]
    fn test_stop_below_threshold_no_scrobble() {
        // Play a track for 3 seconds then stop — should NOT be scrobbled,
        // even if a spurious Status(Playing) follows (some players emit this)
        // and a large amount of phantom time elapses before the next event.
        let mut tracker = TestableTracker::new();

        tracker.handle_event(Event::Metadata {
            artist: "††† (Crosses)".into(),
            album: "††† (Crosses)".into(),
            title: "This Is a Trick".into(),
            duration_us: Some(186_000_000), // threshold = 93s
        });

        tracker.advance_time(3.0);
        tracker.handle_event(Event::Status(PlayerStatus::Stopped));

        // Spurious Playing emitted by the player after stopping — this used
        // to start the clock again, letting phantom time push accumulated_secs
        // past the threshold.
        tracker.handle_event(Event::Status(PlayerStatus::Playing));
        tracker.advance_time(200.0); // phantom time — player isn't actually playing

        // Session ends (or a new track arrives).
        tracker.handle_event(Event::Eof);

        assert_eq!(
            tracker.scrobbled.len(),
            0,
            "3-second play must not be scrobbled"
        );
    }

    #[test]
    fn test_stop_above_threshold_scrobbles_immediately() {
        // Play a track past its threshold, then stop — should be scrobbled
        // at the moment Stop arrives, not waiting for the next track or Eof.
        let mut tracker = TestableTracker::new();

        tracker.handle_event(Event::Metadata {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            title: "Digital Bath".into(),
            duration_us: Some(291_000_000), // threshold = 145.5s
        });

        tracker.advance_time(200.0); // above threshold
        tracker.handle_event(Event::Status(PlayerStatus::Stopped));

        assert_eq!(tracker.scrobbled.len(), 1);
        assert_eq!(tracker.scrobbled[0].title, "Digital Bath");
        assert_eq!(tracker.scrobbled[0].played_duration_secs, 200);
    }
}
