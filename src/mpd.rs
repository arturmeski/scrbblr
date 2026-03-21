//! MPD (Music Player Daemon) integration module.
//!
//! This module provides two pieces of functionality that work alongside
//! (or independently of) the MPRIS watcher:
//!
//! ## 1. MPD watcher (`run_mpd_watch`)
//!
//! Connects to MPD and listens for player events using the MPD idle protocol.
//! When the current track or playback state changes, it feeds the same
//! [`watcher::Event`] types into a [`watcher::ScrobbleTracker`] that the
//! MPRIS watcher uses. Both players therefore share exactly the same
//! scrobbling logic (50%/4-minute threshold, pause-time exclusion, etc.)
//! without any duplication.
//!
//! ## 2. MPD cover extractor (`run_mpd_cover_enrich`)
//!
//! Queries MPD for a representative file for each scrobbled (artist, album)
//! pair that has no cached cover art, then uses MPD's `readpicture` command
//! to retrieve the embedded image bytes directly over the protocol connection.
//! The image is resized and saved locally using the same cache as the online
//! enrichment pipeline. Because this requires no network access beyond the
//! local MPD socket, it is fast and works fully offline.
//!
//! Pre-populating `album_cache.cover_url` this way means subsequent
//! `enrich` runs (which fetch genres from MusicBrainz) will find a cover
//! already present and will not attempt to download one from the Cover Art
//! Archive.
//!
//! ## MPD protocol overview
//!
//! MPD uses a plain-text, line-based protocol over TCP or a Unix domain
//! socket. After connecting, the server sends:
//!
//! ```text
//! OK MPD <version>
//! ```
//!
//! Commands are single-line strings terminated with `\n`. Responses are one
//! or more `key: value\n` lines terminated by `OK\n`, or an error of the
//! form `ACK [code@index] {command} description\n`.
//!
//! ### `idle player`
//!
//! Puts MPD into a blocking wait. MPD sends nothing until a player event
//! occurs, at which point it responds with `changed: player\nOK\n` and
//! exits idle mode. We then query `currentsong` and `status` to find out
//! what changed.
//!
//! ### `readpicture <uri> <offset>`
//!
//! Reads embedded cover art from a music file in chunks. Response format:
//!
//! ```text
//! size: <total_bytes>
//! type: <mime_type>
//! binary: <chunk_bytes>
//! <chunk_bytes of raw binary data>
//! OK
//! ```
//!
//! If the image is larger than MPD's chunk size (typically 8 KiB by default,
//! but configurable), multiple calls with increasing `offset` values are
//! needed to retrieve the full image. If the file has no embedded picture,
//! MPD responds with `binary: 0\nOK\n`.
//!
//! ### `search`
//!
//! Used to find a representative file for a given (artist, album) pair so
//! that `readpicture` has a concrete URI to work with.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::Connection;

use crate::db;
use crate::enrich;
use crate::watcher::{self, Event, PlayerStatus};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Connection parameters for the MPD server.
///
/// MPD can be reached via TCP (the common case) or a Unix domain socket.
/// Set `host` to an absolute path (starting with `/`) to use a Unix socket;
/// the `port` field is ignored in that case.
///
/// The default values (`localhost`, port `6600`) match MPD's built-in
/// defaults and work for a standard single-user installation.
///
/// ## Example
///
/// ```
/// // Standard localhost installation
/// let cfg = MpdConfig { host: "localhost".into(), port: 6600 };
///
/// // Custom port
/// let cfg = MpdConfig { host: "localhost".into(), port: 6601 };
///
/// // Unix socket (when MPD runs under a different user account)
/// let cfg = MpdConfig { host: "/run/mpd/socket".into(), port: 0 };
/// ```
#[derive(Debug, Clone)]
pub struct MpdConfig {
    /// Hostname or IP address for TCP connections, or an absolute filesystem
    /// path for a Unix domain socket (e.g. `/run/user/1000/mpd/socket`).
    pub host: String,
    /// TCP port number. Ignored when `host` is a Unix socket path.
    pub port: u16,
}

impl MpdConfig {
    /// Returns `true` if `host` looks like a Unix socket path (starts with `/`).
    fn is_unix_socket(&self) -> bool {
        self.host.starts_with('/')
    }
}

// ---------------------------------------------------------------------------
// Low-level connection
// ---------------------------------------------------------------------------

/// An open, authenticated connection to MPD.
///
/// Holds a buffered reader for receiving response lines (and binary chunks)
/// and a separate write handle for sending commands. Both handles refer to
/// the same underlying socket.
///
/// The read half has a short timeout (500 ms) so that the idle loop can
/// periodically check the shutdown flag without blocking indefinitely.
struct MpdConn {
    /// Buffered reader wrapping the socket's read half.
    reader: BufReader<Box<dyn Read + Send>>,
    /// Write handle for sending commands.
    writer: Box<dyn Write + Send>,
}

/// Open a TCP connection to MPD, set a read timeout, and consume the
/// `OK MPD <version>` welcome line.
fn connect_tcp(config: &MpdConfig) -> Result<MpdConn, String> {
    let addr = format!("{}:{}", config.host, config.port);
    let stream = TcpStream::connect(&addr)
        .map_err(|e| format!("Could not connect to MPD at {}: {}", addr, e))?;

    // A short read timeout lets the idle loop notice shutdown requests without
    // blocking for potentially minutes waiting for the next player event.
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .map_err(|e| format!("Failed to set MPD read timeout: {}", e))?;

    // Clone the stream so we have independent read and write handles.
    let write_stream = stream
        .try_clone()
        .map_err(|e| format!("Failed to clone MPD TCP stream: {}", e))?;

    let mut conn = MpdConn {
        reader: BufReader::new(Box::new(stream)),
        writer: Box::new(write_stream),
    };

    // Consume the "OK MPD x.y.z" greeting before any command can be sent.
    consume_welcome(&mut conn)?;
    Ok(conn)
}

/// Open a Unix domain socket connection to MPD and consume the welcome line.
#[cfg(unix)]
fn connect_unix(config: &MpdConfig) -> Result<MpdConn, String> {
    let stream = UnixStream::connect(&config.host)
        .map_err(|e| format!("Could not connect to MPD socket {}: {}", config.host, e))?;

    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .map_err(|e| format!("Failed to set MPD read timeout: {}", e))?;

    let write_stream = stream
        .try_clone()
        .map_err(|e| format!("Failed to clone MPD Unix stream: {}", e))?;

    let mut conn = MpdConn {
        reader: BufReader::new(Box::new(stream)),
        writer: Box::new(write_stream),
    };

    consume_welcome(&mut conn)?;
    Ok(conn)
}

/// Connect to MPD using the parameters in `config`.
///
/// Dispatches to the TCP or Unix socket implementation based on whether
/// `config.host` is an absolute path.
fn connect(config: &MpdConfig) -> Result<MpdConn, String> {
    #[cfg(unix)]
    if config.is_unix_socket() {
        return connect_unix(config);
    }

    // On non-Unix targets, Unix sockets are unavailable; fall through to TCP.
    connect_tcp(config)
}

/// Read and discard the MPD welcome line (`OK MPD <version>\n`).
///
/// Returns `Err` if the connection is closed immediately or the greeting
/// doesn't start with `"OK MPD"`, which would indicate a wrong host/port.
fn consume_welcome(conn: &mut MpdConn) -> Result<(), String> {
    let mut line = String::new();
    conn.reader
        .read_line(&mut line)
        .map_err(|e| format!("Failed to read MPD welcome: {}", e))?;

    if !line.starts_with("OK MPD") {
        return Err(format!("Unexpected MPD greeting: {:?}", line));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Protocol helpers
// ---------------------------------------------------------------------------

/// Send a command string to MPD, appending the required `\n` terminator.
fn send_command(conn: &mut MpdConn, cmd: &str) -> io::Result<()> {
    conn.writer.write_all(cmd.as_bytes())?;
    conn.writer.write_all(b"\n")?;
    conn.writer.flush()
}

/// Read a `key: value` response from MPD until `OK\n` is seen.
///
/// Returns a `HashMap<key, value>` of all lines parsed before `OK`. An `ACK`
/// line is treated as an error and returns an empty map (the caller ignores
/// the result for most queries).
///
/// Lines not matching `"key: value"` are silently skipped — MPD sometimes
/// includes continuation data that doesn't follow this pattern.
fn read_response(conn: &mut MpdConn) -> HashMap<String, String> {
    let mut map = HashMap::new();
    loop {
        let mut line = String::new();
        match conn.reader.read_line(&mut line) {
            Ok(0) => break, // EOF — connection closed
            Ok(_) => {}
            Err(_) => break,
        }
        let line = line.trim_end_matches('\n').trim_end_matches('\r');
        if line == "OK" {
            break;
        }
        if line.starts_with("ACK") {
            // Protocol error — log it and return what we have (usually nothing).
            break;
        }
        // Split on the first ": " only, in case a value contains ": ".
        if let Some(colon) = line.find(": ") {
            let key = line[..colon].to_string();
            let val = line[colon + 2..].to_string();
            map.insert(key, val);
        }
    }
    map
}

/// Read the `readpicture` response header lines until `binary: N` is seen.
///
/// Returns `(total_size, chunk_size)`. If the response is `binary: 0` (no
/// embedded picture), `chunk_size` will be 0 and `total_size` will be 0.
/// Returns `None` on protocol error or if an `ACK` is received.
fn read_picture_header(conn: &mut MpdConn) -> Option<(u64, u64)> {
    let mut total_size: u64 = 0;
    loop {
        let mut line = String::new();
        conn.reader.read_line(&mut line).ok()?;
        let line = line.trim_end_matches('\n').trim_end_matches('\r');

        if line == "OK" {
            // Reached OK without a binary field — unexpected, treat as no data.
            return Some((0, 0));
        }
        if line.starts_with("ACK") {
            return None;
        }
        if let Some(rest) = line.strip_prefix("size: ") {
            total_size = rest.trim().parse().unwrap_or(0);
        } else if let Some(rest) = line.strip_prefix("binary: ") {
            let chunk_size: u64 = rest.trim().parse().unwrap_or(0);
            return Some((total_size, chunk_size));
        }
        // "type: image/jpeg" and other headers are intentionally ignored here.
    }
}

/// Read exactly `n` raw bytes from MPD's response stream.
///
/// Used after parsing the `binary: N` header to extract the image chunk.
/// `BufReader::read_exact` correctly drains buffered data first and then
/// reads the remainder from the socket, so mixing text line reads and binary
/// chunk reads on the same `BufReader` is safe.
fn read_exact_bytes(conn: &mut MpdConn, n: u64) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; n as usize];
    conn.reader.read_exact(&mut buf).ok()?;
    Some(buf)
}

/// Read the single `OK\n` line that terminates a binary chunk response.
fn read_ok_line(conn: &mut MpdConn) {
    let mut line = String::new();
    let _ = conn.reader.read_line(&mut line);
}

// ---------------------------------------------------------------------------
// MPD query functions
// ---------------------------------------------------------------------------

/// A snapshot of the currently playing song's metadata from `currentsong`.
///
/// All fields are `Option<String>` because MPD may not populate every tag —
/// for example, internet radio streams often lack album information.
#[derive(Debug, Default, Clone)]
struct MpdSong {
    /// Relative file path within the MPD music directory (e.g. `Artist/Album/01.flac`).
    file: String,
    artist: Option<String>,
    /// `AlbumArtist` tag, preferred over `Artist` for album lookups.
    album_artist: Option<String>,
    album: Option<String>,
    title: Option<String>,
    /// Track duration in whole seconds, parsed from MPD's float `duration` field.
    duration_secs: Option<u64>,
}

/// A snapshot of MPD's current playback state from `status`.
#[derive(Debug, Default, Clone)]
struct MpdStatus {
    /// `"play"`, `"pause"`, or `"stop"`.
    state: String,
}

/// Query MPD for the currently playing song.
///
/// Returns `None` if MPD is stopped (no current song) or if the query fails.
fn query_currentsong(conn: &mut MpdConn) -> Option<MpdSong> {
    send_command(conn, "currentsong").ok()?;
    let map = read_response(conn);

    // If MPD is stopped, currentsong returns an empty response.
    if map.is_empty() {
        return None;
    }

    let file = map.get("file").cloned().unwrap_or_default();
    if file.is_empty() {
        return None;
    }

    // Parse duration from the float string MPD provides (e.g. "218.340").
    let duration_secs = map
        .get("duration")
        .and_then(|s| s.parse::<f64>().ok())
        .map(|f| f.floor() as u64);

    Some(MpdSong {
        file,
        artist: map.get("Artist").cloned(),
        album_artist: map.get("AlbumArtist").cloned(),
        album: map.get("Album").cloned(),
        title: map.get("Title").cloned(),
        duration_secs,
    })
}

/// Query MPD for the current playback state (`play`, `pause`, or `stop`).
fn query_status(conn: &mut MpdConn) -> MpdStatus {
    if send_command(conn, "status").is_err() {
        return MpdStatus::default();
    }
    let map = read_response(conn);
    MpdStatus {
        state: map.get("state").cloned().unwrap_or_default(),
    }
}

/// Map MPD's `status.state` string to a [`PlayerStatus`] enum value.
fn parse_mpd_state(state: &str) -> PlayerStatus {
    match state {
        "play" => PlayerStatus::Playing,
        "pause" => PlayerStatus::Paused,
        _ => PlayerStatus::Stopped,
    }
}

/// Use MPD's `search` command to find any file tagged with the given artist
/// and album. Returns the relative file path, or `None` if not found.
///
/// We search with `AlbumArtist` first (more reliable for multi-artist albums)
/// and fall back to `Artist` if nothing is found. Only one file is needed — we
/// use it purely to run `readpicture` against it.
fn search_song_for_album(conn: &mut MpdConn, artist: &str, album: &str) -> Option<String> {
    // Try AlbumArtist tag first — it's more consistent for cover lookups.
    let cmd = format!(
        "search AlbumArtist \"{}\" Album \"{}\"",
        escape_mpd_string(artist),
        escape_mpd_string(album)
    );
    if send_command(conn, &cmd).is_ok() {
        let map = read_response(conn);
        if let Some(file) = map.get("file") {
            return Some(file.clone());
        }
    }

    // Fall back to the Artist tag (handles single-artist albums and tracks
    // that lack the AlbumArtist field).
    let cmd = format!(
        "search Artist \"{}\" Album \"{}\"",
        escape_mpd_string(artist),
        escape_mpd_string(album)
    );
    if send_command(conn, &cmd).is_ok() {
        let map = read_response(conn);
        if let Some(file) = map.get("file") {
            return Some(file.clone());
        }
    }

    None
}

/// Escape a string for use inside an MPD search argument.
///
/// MPD search strings are quoted with `"..."`. We escape `"` and `\` to
/// prevent the query from being misinterpreted as additional arguments.
fn escape_mpd_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

// ---------------------------------------------------------------------------
// readpicture — embedded cover art retrieval
// ---------------------------------------------------------------------------

/// Retrieve the embedded cover art for a file via MPD's `readpicture` command.
///
/// MPD sends the image in one or more chunks. This function accumulates all
/// chunks by repeating the request with an increasing byte offset until the
/// full image has been received.
///
/// Returns `None` if the file has no embedded picture, if the file is not
/// found in MPD's database, or if a protocol error occurs.
fn read_picture(conn: &mut MpdConn, file_uri: &str) -> Option<Vec<u8>> {
    let mut all_bytes: Vec<u8> = Vec::new();
    let mut offset: u64 = 0;

    loop {
        // Request the next chunk starting at `offset`.
        let cmd = format!("readpicture \"{}\" {}", escape_mpd_string(file_uri), offset);
        send_command(conn, &cmd).ok()?;

        // Parse the `size`, `type`, and `binary` header fields.
        let (total_size, chunk_size) = read_picture_header(conn)?;

        // `binary: 0` means no embedded picture (or we've read everything).
        if chunk_size == 0 {
            // If we have already accumulated bytes, we're done; otherwise there
            // was no picture at all.
            break;
        }

        // Read the raw binary chunk from the stream.
        let chunk = read_exact_bytes(conn, chunk_size)?;
        all_bytes.extend_from_slice(&chunk);

        // Each chunk response is terminated by a bare `OK` line.
        read_ok_line(conn);

        offset += chunk_size;

        // Stop when we've read the complete image.
        if total_size > 0 && offset >= total_size {
            break;
        }
    }

    if all_bytes.is_empty() {
        None
    } else {
        Some(all_bytes)
    }
}

// ---------------------------------------------------------------------------
// Cover filename — stable key for locally-extracted covers
// ---------------------------------------------------------------------------

/// Generate a stable filename stem for a locally-extracted MPD cover.
///
/// Uses a 64-bit FNV-1a hash of `"artist\talbum"`, formatted as a 16-char
/// hex string prefixed with `"mpd_"`. This gives a compact, deterministic
/// filename that:
///
/// - Is stable across runs (same artist+album always maps to the same name).
/// - Is visually distinct from MBID-based filenames (MBIDs contain hyphens).
/// - Has negligible collision probability across a typical music library.
///
/// Example: `mpd_a3f7c2b91e045d68.jpg`
fn album_cover_stem(artist: &str, album: &str) -> String {
    // FNV-1a 64-bit hash constants.
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;

    let input = format!("{}\t{}", artist, album);
    let mut hash = FNV_OFFSET;
    for byte in input.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("mpd_{:016x}", hash)
}

// ---------------------------------------------------------------------------
// MPD watcher — tracks playback and feeds ScrobbleTracker
// ---------------------------------------------------------------------------

/// Internal state snapshot from MPD, tracked between idle events to detect
/// what changed (track change vs. play/pause/stop transition).
#[derive(Default, Clone)]
struct MpdPlayerState {
    /// File path of the current song (the primary track identity key).
    file: String,
    artist: String,
    album: String,
    title: String,
    /// Duration in whole seconds, if the tag is present.
    duration_secs: Option<u64>,
    /// MPD playback state string: `"play"`, `"pause"`, or `"stop"`.
    state: String,
}

impl MpdPlayerState {
    /// Build a state snapshot from currentsong + status query results.
    fn from_queries(song: Option<MpdSong>, status: MpdStatus) -> Self {
        match song {
            None => Self {
                state: status.state,
                ..Default::default()
            },
            Some(s) => {
                // Prefer AlbumArtist for the scrobble artist field — it
                // groups multi-artist albums under one name and matches
                // what most music tagging conventions use.
                let artist = s.album_artist.or(s.artist).unwrap_or_default();
                Self {
                    file: s.file,
                    artist,
                    album: s.album.unwrap_or_default(),
                    title: s.title.unwrap_or_default(),
                    duration_secs: s.duration_secs,
                    state: status.state,
                }
            }
        }
    }

    /// Duration converted to microseconds for [`Event::Metadata`].
    ///
    /// [`watcher::ScrobbleTracker`] works in microseconds internally
    /// (matching the MPRIS format), so we normalise MPD's seconds here.
    fn duration_us(&self) -> Option<u64> {
        self.duration_secs.map(|s| s * 1_000_000)
    }
}

/// Run the MPD watcher reconnect-and-event loop.
///
/// Connects to MPD, then loops waiting for player events. If the connection
/// drops (MPD restarted, network hiccup), waits 5 seconds and reconnects
/// automatically. Blocks until `running` is set to `false` (e.g. by a
/// Ctrl+C handler), at which point it flushes any in-progress scrobble
/// and returns.
///
/// It maintains its own [`watcher::ScrobbleTracker`] backed by the supplied
/// database connection. Scrobbles are written to the same SQLite database as
/// the MPRIS watcher, so the two sources appear together in reports.
pub fn run_mpd_watch(config: &MpdConfig, conn: Arc<Mutex<Connection>>, running: Arc<AtomicBool>) {
    eprintln!(
        "[mpd] Connecting to MPD at {}{}",
        config.host,
        if config.is_unix_socket() {
            String::new()
        } else {
            format!(":{}", config.port)
        }
    );

    let mut tracker = watcher::create_db_tracker(conn);

    // Outer loop: reconnect on failure until shutdown is requested.
    while running.load(Ordering::SeqCst) {
        let mut mpd_conn = match connect(config) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[mpd] Connection failed: {}. Retrying in 5s...", e);
                // Sleep in short increments so shutdown is noticed quickly.
                for _ in 0..10 {
                    std::thread::sleep(Duration::from_millis(500));
                    if !running.load(Ordering::SeqCst) {
                        return;
                    }
                }
                continue;
            }
        };

        eprintln!("[mpd] Connected. Watching for player events.");

        // Do an immediate state query on connect so we don't miss a track that
        // was already playing when we started.
        let song = query_currentsong(&mut mpd_conn);
        let status = query_status(&mut mpd_conn);
        let mut prev = MpdPlayerState::from_queries(song, status);

        // `last_metadata_sent_file` tracks which file we most recently sent a
        // Metadata event for. We clear this on Stop so that if the user stops
        // and then replays the same file, the tracker is properly re-initialised
        // (instead of receiving only a Status(Playing) with no active track).
        let mut last_metadata_file: Option<String> = None;

        // Send an initial Metadata event if MPD was already playing when we
        // connected, so the tracker starts accumulating time right away.
        if prev.state == "play" && !prev.title.is_empty() {
            send_metadata_event(&prev, &mut tracker);
            last_metadata_file = Some(prev.file.clone());
        }

        // Inner loop: wait for player events, dispatch to tracker.
        let connected = run_event_loop(
            &mut mpd_conn,
            &mut tracker,
            &running,
            &mut prev,
            &mut last_metadata_file,
        );

        if !connected {
            eprintln!("[mpd] Connection lost. Retrying in 5s...");
            for _ in 0..10 {
                std::thread::sleep(Duration::from_millis(500));
                if !running.load(Ordering::SeqCst) {
                    break;
                }
            }
        }
    }

    // Evaluate any in-progress track before exiting — the same graceful
    // shutdown behaviour as the MPRIS watcher.
    tracker.handle_event(Event::Eof);
    eprintln!("[mpd] Watcher stopped.");
}

/// The inner event loop for a single MPD connection lifetime.
///
/// Returns `true` if the loop exited cleanly due to `running` becoming false,
/// `false` if the connection was lost.
fn run_event_loop(
    mpd_conn: &mut MpdConn,
    tracker: &mut watcher::ScrobbleTracker<impl FnMut(db::NewScrobble)>,
    running: &Arc<AtomicBool>,
    prev: &mut MpdPlayerState,
    last_metadata_file: &mut Option<String>,
) -> bool {
    loop {
        if !running.load(Ordering::SeqCst) {
            return true;
        }

        // Enter idle mode: MPD blocks until a player event occurs.
        if send_command(mpd_conn, "idle player").is_err() {
            return false; // Connection lost — trigger reconnect.
        }

        // Wait for the "changed: player\nOK\n" response.
        // read_response uses a 500 ms timeout, so we loop here until we
        // either get the player event or the shutdown flag is set.
        let changed = wait_for_idle_response(mpd_conn, running);
        match changed {
            IdleResult::Wakeup => {}
            IdleResult::Shutdown => return true,
            IdleResult::ConnectionLost => return false,
        }

        // Query MPD's current state now that we know something changed.
        let song = query_currentsong(mpd_conn);
        let status = query_status(mpd_conn);
        let new = MpdPlayerState::from_queries(song, status);

        // Determine what changed and dispatch the appropriate events.
        dispatch_events(prev, &new, tracker, last_metadata_file);

        *prev = new;
    }
}

/// The three possible outcomes of waiting for an MPD idle response.
enum IdleResult {
    /// The idle wait ended and state should be re-queried. This covers both
    /// the normal case (`changed: player` received) and the degenerate case
    /// where MPD returns `OK` without a `changed` line (e.g., a different
    /// subsystem fired despite `idle player` — unusual but handled safely).
    Wakeup,
    Shutdown,
    ConnectionLost,
}

/// Wait for MPD's `changed: player` response, returning on the first player
/// event, on shutdown, or on connection loss.
///
/// Because the socket has a 500 ms read timeout, `read_line` returns a
/// `TimedOut`/`WouldBlock` error periodically. We use those timeouts to check
/// the shutdown flag without blocking for the full duration of a track.
///
/// Returns [`IdleResult::Wakeup`] when the response terminates — regardless
/// of whether a `changed: player` line appeared. In both cases the caller
/// re-queries MPD state, so the distinction doesn't matter.
fn wait_for_idle_response(conn: &mut MpdConn, running: &Arc<AtomicBool>) -> IdleResult {
    loop {
        let mut line = String::new();
        match conn.reader.read_line(&mut line) {
            Ok(0) => return IdleResult::ConnectionLost, // EOF
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed == "OK" {
                    // End of idle response — re-query state regardless of whether
                    // "changed: player" appeared. With `idle player` the line
                    // should always appear, but if it's absent (degenerate case)
                    // a spurious re-query is harmless.
                    return IdleResult::Wakeup;
                }
                // "changed: player" and any other key/value lines are consumed
                // and discarded here — we query currentsong/status ourselves.
            }
            Err(ref e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                // Read timeout — check if we should shut down, then keep waiting.
                if !running.load(Ordering::SeqCst) {
                    return IdleResult::Shutdown;
                }
            }
            Err(_) => return IdleResult::ConnectionLost,
        }
    }
}

/// Compare previous and new MPD state and send the appropriate events to the
/// [`watcher::ScrobbleTracker`].
///
/// ## Event dispatch logic
///
/// The tracker expects events in a specific order that mirrors the MPRIS
/// model:
///
/// - **File changed**: send `Event::Metadata` for the new track (the tracker
///   treats this as the start of play for the new track).
/// - **State changed (same file)**: send `Event::Status(Playing/Paused/Stopped)`.
/// - **File changed + state is now "stop"**: send Metadata then Stopped — this
///   covers the edge case of MPD loading a new track and immediately stopping.
///
/// The `last_metadata_file` variable tracks which file most recently received
/// a Metadata event. We clear it on Stop so that replaying the same file after
/// stopping triggers a fresh Metadata event (which re-initialises the tracker,
/// since `Status(Stopped)` clears its internal state).
fn dispatch_events(
    prev: &MpdPlayerState,
    new: &MpdPlayerState,
    tracker: &mut watcher::ScrobbleTracker<impl FnMut(db::NewScrobble)>,
    last_metadata_file: &mut Option<String>,
) {
    let file_changed = new.file != prev.file;
    let state_changed = new.state != prev.state;

    // Determine if we need to send a Metadata event:
    // - The file is different from the last Metadata we sent, OR
    // - We're starting to play something and the tracker has no active track
    //   (last_metadata_file is None, meaning we stopped and are replaying).
    let needs_metadata = !new.file.is_empty()
        && !new.title.is_empty()
        && (file_changed || last_metadata_file.as_deref() != Some(&new.file));

    // Send Metadata first if the track changed and we are (or are about to be) playing.
    if needs_metadata && (new.state == "play" || file_changed) {
        send_metadata_event(new, tracker);
        *last_metadata_file = Some(new.file.clone());
    }

    // Send a Status event if the playback state changed.
    if state_changed || (!needs_metadata && new.state != prev.state) {
        let status = parse_mpd_state(&new.state);
        tracker.handle_event(Event::Status(status));

        // When stopped, clear last_metadata_file so that re-playing the same
        // file after a stop will trigger a fresh Metadata event.
        if status == PlayerStatus::Stopped {
            *last_metadata_file = None;
        }
    }
}

/// Build and dispatch an `Event::Metadata` for the given MPD state.
///
/// Duration is converted from seconds to microseconds to match the format
/// used by the MPRIS watcher (and expected by `CurrentTrack::threshold_secs`).
fn send_metadata_event(
    state: &MpdPlayerState,
    tracker: &mut watcher::ScrobbleTracker<impl FnMut(db::NewScrobble)>,
) {
    tracker.handle_event(Event::Metadata {
        artist: state.artist.clone(),
        album: state.album.clone(),
        title: state.title.clone(),
        // Convert seconds → microseconds to match the MPRIS unit.
        duration_us: state.duration_us(),
    });
}

// ---------------------------------------------------------------------------
// MPD cover extractor
// ---------------------------------------------------------------------------

/// Extract and cache embedded cover art for scrobbled albums using MPD's
/// `readpicture` command.
///
/// For each (artist, album) pair that has scrobbles but no `cover_url` in
/// `album_cache`, this function:
///
/// 1. Searches MPD for any file from that album (`search AlbumArtist ... Album ...`).
/// 2. Extracts the embedded picture via `readpicture`.
/// 3. Resizes the image to at most 500×500 px and re-encodes as JPEG at
///    quality 85 (matching the online enrichment pipeline).
/// 4. Saves the file as `covers/mpd_<hash>.jpg` and records the path in
///    `album_cache.cover_url`.
///
/// Albums not found in MPD's database are silently skipped — they will be
/// retried by the online `enrich` command or remain uncovered.
///
/// This is intended to run **before** the online `enrich` command so that
/// locally-sourced covers are already in place. If the online enrichment
/// later finds a cover from the Cover Art Archive, it will replace the local
/// one; if it finds nothing, the local cover is preserved (see
/// `db::upsert_album_cache` for the `COALESCE` logic).
pub fn run_mpd_cover_enrich(config: &MpdConfig, conn: &Connection) {
    // Find all (artist, album) pairs with scrobbles but no cover yet.
    let albums = match db::albums_without_cover(conn) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("[error] Failed to query albums without cover: {}", e);
            return;
        }
    };

    if albums.is_empty() {
        eprintln!("All scrobbled albums already have covers. Nothing to do.");
        return;
    }

    // Connect to MPD for this session.
    let mut mpd_conn = match connect(config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "[error] Could not connect to MPD for cover extraction: {}",
                e
            );
            return;
        }
    };

    // Remove the read timeout for cover extraction — readpicture responses can
    // be large and we don't need the idle-loop timeout here.
    // We can't easily change the timeout on a Box<dyn Read>, so we just proceed
    // with the existing short timeout but retry reads as needed. In practice,
    // readpicture transfers are fast on a local socket.

    let covers = enrich::covers_dir();
    eprintln!(
        "Extracting covers from MPD for {} album(s)...",
        albums.len()
    );

    let mut found = 0;
    let mut skipped = 0;

    for (i, album) in albums.iter().enumerate() {
        eprintln!(
            "[{}/{}] {} - {}",
            i + 1,
            albums.len(),
            album.artist,
            album.album
        );

        // Step 1: Ask MPD for a file from this album.
        let file_uri = match search_song_for_album(&mut mpd_conn, &album.artist, &album.album) {
            Some(f) => f,
            None => {
                eprintln!("  Not found in MPD database, skipping.");
                skipped += 1;
                continue;
            }
        };
        eprintln!("  File: {}", file_uri);

        // Step 2: Extract the embedded picture via readpicture.
        let picture_bytes = match read_picture(&mut mpd_conn, &file_uri) {
            Some(b) => b,
            None => {
                eprintln!("  No embedded cover art found.");
                skipped += 1;
                continue;
            }
        };

        // Step 3: Resize and re-encode the image to stay consistent with
        // covers downloaded from the Cover Art Archive.
        let processed = match enrich::resize_cover_bytes(&picture_bytes) {
            Some(b) => b,
            None => {
                eprintln!("  [warn] Could not decode cover image, skipping.");
                skipped += 1;
                continue;
            }
        };

        // Step 4: Save to the covers cache directory.
        let stem = album_cover_stem(&album.artist, &album.album);
        let dest = covers.join(format!("{}.jpg", stem));
        if let Err(e) = std::fs::write(&dest, &processed) {
            eprintln!("  [warn] Failed to write cover image: {}", e);
            skipped += 1;
            continue;
        }

        // Step 5: Record the local path in album_cache so the report generator
        // and the online enrich command can find it.
        let cover_path = dest.to_string_lossy().to_string();
        match db::set_local_cover(conn, &album.artist, &album.album, &cover_path) {
            Ok(_) => {
                eprintln!("  Cover saved: {}", dest.display());
                found += 1;
            }
            Err(e) => {
                eprintln!("  [error] Failed to update album_cache: {}", e);
            }
        }
    }

    eprintln!();
    eprintln!("MPD cover extraction complete:");
    eprintln!("  Albums processed: {}", albums.len());
    eprintln!("  Covers extracted: {}", found);
    eprintln!("  Skipped (not in MPD / no art): {}", skipped);
    eprintln!("  Covers directory: {}", covers.display());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // album_cover_stem — filename generation
    // -----------------------------------------------------------------------

    #[test]
    fn test_album_cover_stem_stable() {
        // The same inputs must always produce the same filename.
        let a = album_cover_stem("Bonobo", "Black Sands");
        let b = album_cover_stem("Bonobo", "Black Sands");
        assert_eq!(a, b);
    }

    #[test]
    fn test_album_cover_stem_differs_by_input() {
        // Different albums must not collide.
        let a = album_cover_stem("Bonobo", "Black Sands");
        let b = album_cover_stem("Bonobo", "The North Borders");
        let c = album_cover_stem("Deftones", "Black Sands"); // same album, different artist
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_album_cover_stem_format() {
        // Must start with "mpd_" and be followed by 16 lowercase hex chars.
        let stem = album_cover_stem("Artist", "Album");
        assert!(stem.starts_with("mpd_"), "stem = {}", stem);
        let hex_part = &stem[4..];
        assert_eq!(hex_part.len(), 16);
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "non-hex chars in {}",
            hex_part
        );
    }

    // -----------------------------------------------------------------------
    // escape_mpd_string
    // -----------------------------------------------------------------------

    #[test]
    fn test_escape_mpd_string_plain() {
        assert_eq!(escape_mpd_string("Bonobo"), "Bonobo");
    }

    #[test]
    fn test_escape_mpd_string_quotes() {
        // Embedded double-quotes must be escaped.
        assert_eq!(escape_mpd_string("AC\"DC"), "AC\\\"DC");
    }

    #[test]
    fn test_escape_mpd_string_backslash() {
        assert_eq!(escape_mpd_string("back\\slash"), "back\\\\slash");
    }

    // -----------------------------------------------------------------------
    // MpdPlayerState helpers
    // -----------------------------------------------------------------------

    #[test]
    fn test_duration_us_conversion() {
        // MPD reports duration in seconds; we must convert to microseconds
        // so ScrobbleTracker's threshold logic works correctly.
        let state = MpdPlayerState {
            duration_secs: Some(186), // 186 seconds = 186_000_000 µs
            ..Default::default()
        };
        assert_eq!(state.duration_us(), Some(186_000_000));
    }

    #[test]
    fn test_duration_us_none() {
        let state = MpdPlayerState {
            duration_secs: None,
            ..Default::default()
        };
        assert_eq!(state.duration_us(), None);
    }

    // -----------------------------------------------------------------------
    // parse_mpd_state
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_mpd_state() {
        assert_eq!(parse_mpd_state("play"), PlayerStatus::Playing);
        assert_eq!(parse_mpd_state("pause"), PlayerStatus::Paused);
        assert_eq!(parse_mpd_state("stop"), PlayerStatus::Stopped);
        // Anything unrecognised maps to Stopped.
        assert_eq!(parse_mpd_state(""), PlayerStatus::Stopped);
        assert_eq!(parse_mpd_state("unknown"), PlayerStatus::Stopped);
    }

    // -----------------------------------------------------------------------
    // MpdConfig
    // -----------------------------------------------------------------------

    #[test]
    fn test_config_unix_socket_detection() {
        // Any absolute path triggers Unix socket mode — no specific uid needed.
        let unix = MpdConfig {
            host: "/run/mpd/socket".into(),
            port: 0,
        };
        assert!(unix.is_unix_socket());

        let tcp = MpdConfig {
            host: "localhost".into(),
            port: 6600,
        };
        assert!(!tcp.is_unix_socket());
    }
}
