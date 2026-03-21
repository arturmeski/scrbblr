//! MPRIS + MPD Scrobbler — a local music scrobbler for Linux.
//!
//! This is the CLI entry point. It provides five subcommands:
//!
//! - `watch` — monitors a player via `playerctl` and/or MPD, recording
//!   scrobbles to SQLite. Both sources can run simultaneously.
//! - `report` — generates listening statistics from the stored data
//! - `enrich` — fetches album art and genre info from MusicBrainz,
//!   and/or extracts embedded covers from MPD
//! - `last-scrobble`— prints the newest scrobble timestamp
//! - `pin-album`    — manually assign a MusicBrainz ID to an album
//!
//! ## Architecture overview
//!
//! ### MPRIS watcher
//!
//! The `watch` command spawns two `playerctl --follow` child processes:
//!
//! 1. **Metadata follower** — emits a line each time the track changes,
//!    providing artist, album, title, and duration.
//! 2. **Status follower** — emits "Playing", "Paused", or "Stopped" on
//!    playback state changes.
//!
//! Each child process gets its own reader thread that parses lines and sends
//! typed `Event` values over an `mpsc::channel` to the main thread. The main
//! thread owns the `ScrobbleTracker` state machine, which processes events
//! sequentially and decides when to write scrobbles to the database.
//!
//! ```text
//!   [playerctl metadata] ──reader thread──→ ┐
//!                                            ├─ mpsc::channel ─→ [main: ScrobbleTracker → SQLite]
//!   [playerctl status]   ──reader thread──→ ┘
//! ```
//!
//! ### MPD watcher (on by default, disabled with `--no-mpd`)
//!
//! Unless `--no-mpd` is passed, a separate thread is spawned that connects to MPD
//! using the idle protocol. It maintains its own `ScrobbleTracker` and writes
//! to the same database. The two watchers are fully independent — neither
//! knows about the other, and no synchronisation is needed between them.
//!
//! ```text
//!   [MPD idle player] ──mpd thread──→ ScrobbleTracker → SQLite (same DB)
//! ```
//!
//! Ctrl+C triggers graceful shutdown of both watchers: the MPRIS watcher
//! receives an `Eof` event through its channel, and the MPD watcher notices
//! the shared `running` flag becoming false on its next idle timeout.

mod db;
mod enrich;
mod mpd;
mod report;
mod watcher;

use clap::{Parser, Subcommand};
use std::io::BufRead;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

/// Default playerctl player name. This matches the MPRIS bus name for the
/// user's primary player (qbz). Can be overridden with `--player`.
const DEFAULT_PLAYER: &str = "com.blitzfc.qbz";

// ---------------------------------------------------------------------------
// CLI definition (using clap derive)
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "mpris-scrobbler", about = "MPRIS scrobbler using playerctl")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Watch playerctl metadata and MPD, scrobbling tracks to the local database.
    ///
    /// Both watchers are active by default. Use `--no-mpris` or `--no-mpd` to
    /// disable one of them. Each maintains its own ScrobbleTracker and writes
    /// directly to the same SQLite database.
    Watch {
        /// MPRIS player name for playerctl (the D-Bus service name).
        /// Run `playerctl -l` to see available players.
        /// Omit entirely with `--no-mpris` if you only want to watch MPD.
        #[arg(long, default_value = DEFAULT_PLAYER)]
        player: String,

        /// Disable the MPRIS/playerctl watcher. Use this when you only want
        /// to scrobble from MPD and do not have playerctl set up.
        #[arg(long)]
        no_mpris: bool,

        /// Disable the MPD watcher. MPD scrobbling is on by default;
        /// pass this flag to run MPRIS-only.
        #[arg(long)]
        no_mpd: bool,

        /// MPD server hostname or IP address for TCP connections, or an
        /// absolute path to a Unix domain socket
        /// (e.g. `/run/mpd/socket`).
        #[arg(long, default_value = "localhost")]
        mpd_host: String,

        /// MPD server TCP port. Ignored when `--mpd-host` is a socket path.
        #[arg(long, default_value = "6600")]
        mpd_port: u16,

        /// Path to the SQLite database file. If not specified, defaults to
        /// ~/.local/share/mpris-scrobbler/scrobbles.db (respects $XDG_DATA_HOME).
        #[arg(long)]
        db_path: Option<String>,
    },
    /// Generate scrobble reports from the local database.
    Report {
        /// Time period to report on: today, week, month, year, or all.
        #[arg(long, default_value = "all")]
        period: String,

        /// Output the report as JSON instead of terminal tables.
        #[arg(long)]
        json: bool,

        /// Output the report as standalone HTML.
        #[arg(long)]
        html: bool,

        /// Output directory path (used with --html). Creates a directory with
        /// index.html and a covers/ subdirectory. If omitted, prints HTML to stdout.
        #[arg(long)]
        output: Option<String>,

        /// Maximum number of entries in top-N lists (top artists, albums, tracks).
        /// All-time sections default to 2.5x this value (rounded to nearest 5).
        #[arg(long, default_value = "10")]
        limit: i64,

        /// Override the all-time top-N limit. If not set, defaults to 2.5x
        /// --limit rounded to the nearest multiple of 5.
        #[arg(long)]
        all_time_limit: Option<i64>,

        /// Path to the SQLite database file. Same default as `watch`.
        #[arg(long)]
        db_path: Option<String>,
    },
    /// Fetch album art and genre info for all scrobbled albums.
    ///
    /// By default, connects to MPD and extracts embedded cover art from music
    /// files via `readpicture` — fully offline, no network access required.
    /// Pass `--no-mpd-covers` to skip this step.
    ///
    /// Pass `--online` to also query MusicBrainz for album metadata (MBID,
    /// genre) and download cover art from the Cover Art Archive for albums
    /// that still have no cover after the local extraction step.
    Enrich {
        /// Query MusicBrainz for album metadata (MBID, genre) and download
        /// cover art from the Cover Art Archive for albums that still have
        /// no cover after local extraction.
        #[arg(long)]
        online: bool,

        /// Re-fetch metadata for all albums from MusicBrainz, even those
        /// already cached. Only meaningful together with `--online`.
        #[arg(long)]
        force: bool,

        /// Disable the MPD embedded cover extraction step. By default,
        /// `enrich` connects to MPD and extracts embedded cover art from
        /// music files via `readpicture` before doing anything else.
        #[arg(long)]
        no_mpd_covers: bool,

        /// MPD server hostname, IP address, or Unix socket path.
        /// Used for embedded cover extraction (unless `--no-mpd-covers`).
        #[arg(long, default_value = "localhost")]
        mpd_host: String,

        /// MPD server TCP port. Used for embedded cover extraction when
        /// connecting via TCP (ignored for Unix socket paths).
        #[arg(long, default_value = "6600")]
        mpd_port: u16,

        /// Path to the SQLite database file. Same default as `watch`.
        #[arg(long)]
        db_path: Option<String>,
    },
    /// Print the newest scrobble timestamp and exit.
    LastScrobble {
        /// Path to the SQLite database file. Same default as `watch`.
        #[arg(long)]
        db_path: Option<String>,
    },
    /// Manually pin a MusicBrainz release ID to an album that automatic search
    /// could not find. Fetches genres and cover art for the given MBID and
    /// stores them in the album cache, overwriting any previous entry.
    PinAlbum {
        /// Artist name as it appears in the scrobble database.
        #[arg(long)]
        artist: String,

        /// Album name as it appears in the scrobble database.
        #[arg(long)]
        album: String,

        /// MusicBrainz release UUID to pin to this album.
        #[arg(long)]
        mbid: String,

        /// Optional direct URL to a cover image (JPEG or PNG). Use this when
        /// the Cover Art Archive has no image for the given MBID. The image is
        /// downloaded, resized, and stored locally just like a CAA cover.
        #[arg(long)]
        cover_url: Option<String>,

        /// Path to the SQLite database file. Same default as `watch`.
        #[arg(long)]
        db_path: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Database path resolution
// ---------------------------------------------------------------------------

/// Determine the default database path, following the XDG Base Directory spec.
///
/// Path: $XDG_DATA_HOME/mpris-scrobbler/scrobbles.db
/// Falls back to: ~/.local/share/mpris-scrobbler/scrobbles.db
///
/// Creates the parent directory if it doesn't exist.
fn default_db_path() -> String {
    let data_dir = std::env::var("XDG_DATA_HOME").unwrap_or_else(|_| {
        let home = std::env::var("HOME").expect("HOME not set");
        format!("{}/.local/share", home)
    });
    let dir = format!("{}/mpris-scrobbler", data_dir);
    std::fs::create_dir_all(&dir).expect("Failed to create data directory");
    format!("{}/scrobbles.db", dir)
}

// ---------------------------------------------------------------------------
// Watch command implementation
// ---------------------------------------------------------------------------

/// Run the `watch` subcommand: spawn playerctl processes and/or an MPD watcher,
/// read events, and scrobble tracks to the database.
///
/// # Arguments
/// - `player` — MPRIS player name passed to playerctl. Ignored when `use_mpris` is `false`.
/// - `use_mpris` — whether to start the playerctl-based MPRIS watcher.
/// - `mpd_config` — if `Some`, start an MPD watcher thread in parallel.
/// - `db_path` — path to the SQLite database file.
///
/// Both watchers, when active, share the same `Arc<Mutex<Connection>>`
/// (scrobble writes are infrequent, so mutex contention is negligible) and
/// the same `running` flag (both stop on Ctrl+C).
fn run_watch(player: &str, use_mpris: bool, mpd_config: Option<mpd::MpdConfig>, db_path: &str) {
    // Open (or create) the database and wrap in Arc<Mutex<>> for sharing
    // between the MPRIS callback and the MPD watcher thread.
    let conn = db::open_db(db_path).expect("Failed to open database");
    let conn = Arc::new(Mutex::new(conn));

    eprintln!("Database: {}", db_path);
    if use_mpris {
        eprintln!("Watching MPRIS player: {}", player);
    }
    if mpd_config.is_some() {
        eprintln!("Watching MPD");
    }

    // Shared shutdown flag. Both the Ctrl+C handler and the MPD watcher thread
    // observe this flag; when it goes false, each cleans up and returns.
    let running = Arc::new(AtomicBool::new(true));

    // --- Spawn MPD watcher thread (optional) ---
    // The MPD thread owns its own ScrobbleTracker and runs its own idle loop.
    // It writes scrobbles directly to the database via a clone of `conn`.
    // No shared channel with the MPRIS loop — the two watchers are independent.
    let mpd_handle = if let Some(mpd_cfg) = mpd_config {
        let conn_clone = conn.clone();
        let running_clone = running.clone();
        Some(thread::spawn(move || {
            mpd::run_mpd_watch(&mpd_cfg, conn_clone, running_clone);
        }))
    } else {
        None
    };

    // --- MPRIS watcher (optional) ---
    // If MPRIS is disabled (--no-mpris), we skip spawning playerctl entirely.
    // In that case the channel is still created (so the Ctrl+C handler has
    // a sender), and the main loop exits as soon as the shutdown flag is set.
    let (tx, rx) = mpsc::channel::<watcher::Event>();

    // Track how many reader threads have sent Eof so we know when both
    // playerctl processes have ended. Set to 2 when MPRIS is disabled so the
    // main loop exits immediately on Ctrl+C (no processes to wait for).
    let expected_eofs: usize = if use_mpris { 2 } else { 0 };

    let mut meta_handle = None;
    let mut status_handle = None;

    if use_mpris {
        // --- Spawn playerctl metadata follower ---
        // Outputs one tab-separated line per track change:
        //   artist\talbum\ttitle\tmpris:length
        let metadata_proc = Command::new("playerctl")
            .args([
                "-p",
                player,
                "--follow",
                "metadata",
                "--format",
                "{{artist}}\t{{album}}\t{{title}}\t{{mpris:length}}",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn();

        // --- Spawn playerctl status follower ---
        // Outputs one line per state change: "Playing", "Paused", or "Stopped".
        let status_proc = Command::new("playerctl")
            .args(["-p", player, "--follow", "status"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn();

        match (metadata_proc, status_proc) {
            (Ok(meta_proc), Ok(stat_proc)) => {
                // --- Metadata reader thread ---
                // Reads lines from the metadata process stdout, parses them into
                // `Event::Metadata` values, and sends them through the channel.
                // Sends `Event::Eof` when the process exits (stdout closes).
                let tx_meta = tx.clone();
                meta_handle = Some(thread::spawn(move || {
                    let stdout = meta_proc.stdout.expect("No stdout for metadata process");
                    let reader = std::io::BufReader::new(stdout);
                    for line in reader.lines() {
                        match line {
                            Ok(l) => {
                                if let Some(event) = watcher::parse_metadata_line(&l)
                                    && tx_meta.send(event).is_err()
                                {
                                    break; // Receiver dropped — shutting down.
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    let _ = tx_meta.send(watcher::Event::Eof);
                }));

                // --- Status reader thread ---
                // Same pattern as the metadata reader, but parses status lines.
                let tx_status = tx.clone();
                status_handle = Some(thread::spawn(move || {
                    let stdout = stat_proc.stdout.expect("No stdout for status process");
                    let reader = std::io::BufReader::new(stdout);
                    for line in reader.lines() {
                        match line {
                            Ok(l) => {
                                if let Some(event) = watcher::parse_status_line(&l)
                                    && tx_status.send(event).is_err()
                                {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    let _ = tx_status.send(watcher::Event::Eof);
                }));
            }
            (meta_result, _) => {
                // At least one spawn failed — MPRIS watcher cannot run.
                // Print the error and send synthetic Eofs so the main loop
                // below exits cleanly; the MPD watcher (if any) continues.
                if let Err(e) = meta_result {
                    eprintln!("[warn] Failed to spawn playerctl: {}", e);
                }
                eprintln!("[warn] MPRIS watcher will be inactive.");
                let _ = tx.send(watcher::Event::Eof);
                let _ = tx.send(watcher::Event::Eof);
            }
        }
    }

    // --- Ctrl+C handler ---
    // Sets the `running` flag to false (which the MPD thread observes) and
    // sends an Eof event to unblock the MPRIS main loop below.
    let running_clone = running.clone();
    let tx_ctrlc = tx.clone();
    ctrlc::set_handler(move || {
        eprintln!("\nShutting down...");
        running_clone.store(false, Ordering::SeqCst);
        let _ = tx_ctrlc.send(watcher::Event::Eof);
    })
    .expect("Failed to set Ctrl+C handler");

    // --- MPRIS main event loop ---
    // Receives events from both reader threads and the Ctrl+C handler,
    // and feeds them into the MPRIS ScrobbleTracker state machine.
    // When MPRIS is disabled, this loop exits immediately on shutdown.
    let mut tracker = watcher::create_db_tracker(conn);
    let mut eof_count = 0;
    let mut flushed = false;

    while running.load(Ordering::SeqCst) {
        match rx.recv() {
            Ok(event) => {
                if event == watcher::Event::Eof {
                    eof_count += 1;
                    // Wait for both playerctl processes to signal completion
                    // before flushing the last track. The Ctrl+C handler also
                    // sends one Eof, so we may receive more than `expected_eofs`.
                    if eof_count >= expected_eofs {
                        tracker.handle_event(watcher::Event::Eof);
                        flushed = true;
                        break;
                    }
                    continue;
                }
                tracker.handle_event(event);
            }
            // Channel disconnected — all senders dropped.
            Err(_) => break,
        }
    }

    // Final flush in case the loop exited via channel disconnect rather than
    // the normal Eof path above (e.g., all senders dropped simultaneously).
    if !flushed {
        tracker.handle_event(watcher::Event::Eof);
    }

    // Wait for the MPD watcher thread to finish its graceful shutdown.
    // It will notice `running == false` on the next idle timeout (≤ 500 ms).
    if let Some(handle) = mpd_handle {
        let _ = handle.join();
    }

    eprintln!("Goodbye.");

    // Wait for MPRIS reader threads to finish.
    if let Some(h) = meta_handle {
        let _ = h.join();
    }
    if let Some(h) = status_handle {
        let _ = h.join();
    }
}

/// Round a value to the nearest multiple of 5.
fn round_to_5(n: i64) -> i64 {
    ((n + 2) / 5) * 5
}

// ---------------------------------------------------------------------------
// Report command implementation
// ---------------------------------------------------------------------------

/// Run the `report` subcommand.
///
/// Three output modes:
/// - **Terminal** (default): queries a single `--period` and prints ASCII tables.
/// - **JSON** (`--json`): same single-period data as pretty-printed JSON.
/// - **HTML** (`--html`): generates a multi-period report (Today / Week / Month /
///   All Time) with bar charts and album cover art. Auto-runs enrichment first.
///   With `--output <dir>`, writes `index.html` + `covers/` to a directory.
fn run_report(
    period: &str,
    json: bool,
    html: bool,
    output: Option<&str>,
    limit: i64,
    all_time_limit: i64,
    db_path: &str,
) {
    let conn = match db::open_db(db_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to open database at {}: {}", db_path, e);
            std::process::exit(1);
        }
    };

    // Validate the period argument before querying.
    let valid_periods = ["today", "week", "month", "year", "all"];
    if !valid_periods.contains(&period) {
        eprintln!(
            "Invalid period '{}'. Valid options: {}",
            period,
            valid_periods.join(", ")
        );
        std::process::exit(1);
    }

    if json && html {
        eprintln!("Please choose one output format: either --json or --html.");
        std::process::exit(1);
    }

    // For HTML reports, enrich only the albums that will actually appear in
    // the report (across all periods). Uses quiet mode so "nothing to do"
    // isn't printed when everything is already cached.
    if html {
        let needed = report::albums_needed_for_report(&conn, limit, all_time_limit);
        enrich::run_enrich_targeted(&conn, &needed, true);
    }

    if html {
        // HTML report gathers all periods (today, week, month, all) internally.
        let html_report = report::render_html_report(&conn, limit, all_time_limit);
        if let Some(dir) = output {
            // Create the output directory structure:
            //   <dir>/index.html
            //   <dir>/covers/<filename>.jpg
            let dir_path = std::path::Path::new(dir);
            let covers_dir = dir_path.join("covers");
            if let Err(e) = std::fs::create_dir_all(&covers_dir) {
                eprintln!("Failed to create directory {}: {}", covers_dir.display(), e);
                std::process::exit(1);
            }

            // Copy cover image files into the output covers/ subdirectory.
            // Skip files where the destination is already up-to-date (same
            // size AND dest is not older than src), so repeated report runs
            // are fast but a re-pinned cover is always picked up.
            let mut copied = 0;
            let mut skipped = 0;
            for src in &html_report.cover_files {
                if let Some(filename) = src.file_name() {
                    let dest = covers_dir.join(filename);
                    let src_meta = std::fs::metadata(src);
                    let dest_meta = std::fs::metadata(&dest);
                    let up_to_date = match (src_meta, dest_meta) {
                        (Ok(s), Ok(d)) => {
                            let same_size = s.len() == d.len() && s.len() > 0;
                            let dest_fresh = d
                                .modified()
                                .ok()
                                .zip(s.modified().ok())
                                .map(|(dt, st)| dt >= st)
                                .unwrap_or(false);
                            same_size && dest_fresh
                        }
                        _ => false,
                    };
                    if up_to_date {
                        skipped += 1;
                        continue;
                    }
                    if let Err(e) = std::fs::copy(src, &dest) {
                        eprintln!("  [warn] Failed to copy {}: {}", src.display(), e);
                    } else {
                        copied += 1;
                    }
                }
            }

            // Write the HTML file.
            let index_path = dir_path.join("index.html");
            if let Err(e) = std::fs::write(&index_path, &html_report.html) {
                eprintln!("Failed to write {}: {}", index_path.display(), e);
                std::process::exit(1);
            }

            eprintln!("Wrote HTML report: {}", index_path.display());
            if copied > 0 || skipped > 0 {
                eprintln!(
                    "Covers: {} copied, {} unchanged in {}",
                    copied,
                    skipped,
                    covers_dir.display()
                );
            }
        } else {
            // No --output: print HTML to stdout (covers won't load,
            // but useful for piping).
            print!("{}", html_report.html);
        }
        return;
    }

    // Terminal or JSON report: single period.
    let effective_limit = if period == "all" {
        all_time_limit
    } else {
        limit
    };
    match report::gather_report(&conn, period, effective_limit) {
        Ok(data) => {
            if json {
                report::print_json_report(&data);
            } else {
                report::print_terminal_report(&data);
            }
        }
        Err(e) => {
            eprintln!("Failed to generate report: {}", e);
            std::process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Watch {
            player,
            no_mpris,
            no_mpd,
            mpd_host,
            mpd_port,
            db_path,
        } => {
            let path = db_path.unwrap_or_else(default_db_path);

            // MPD is on by default; --no-mpd disables it.
            let mpd_config = if no_mpd {
                None
            } else {
                Some(mpd::MpdConfig {
                    host: mpd_host,
                    port: mpd_port,
                })
            };

            run_watch(&player, !no_mpris, mpd_config, &path);
        }
        Commands::Report {
            period,
            json,
            html,
            output,
            limit,
            all_time_limit,
            db_path,
        } => {
            let path = db_path.unwrap_or_else(default_db_path);
            let atl =
                all_time_limit.unwrap_or_else(|| round_to_5((limit as f64 * 2.5).round() as i64));
            run_report(&period, json, html, output.as_deref(), limit, atl, &path);
        }
        Commands::Enrich {
            online,
            force,
            no_mpd_covers,
            mpd_host,
            mpd_port,
            db_path,
        } => {
            if no_mpd_covers && !online {
                eprintln!("Nothing to do. Pass --online and/or omit --no-mpd-covers.");
                eprintln!("  (default)      Extract embedded covers from music files via MPD (offline)");
                eprintln!("  --online       Fetch metadata and covers from MusicBrainz / Cover Art Archive");
                std::process::exit(0);
            }

            let path = db_path.unwrap_or_else(default_db_path);
            let conn = match db::open_db(&path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to open database at {}: {}", path, e);
                    std::process::exit(1);
                }
            };

            // Run MPD cover extraction first (fast, local, no rate limits).
            // Pre-populating cover_url means the online enrichment that follows
            // will skip fetching covers for albums that already have one.
            if !no_mpd_covers {
                let mpd_cfg = mpd::MpdConfig {
                    host: mpd_host,
                    port: mpd_port,
                };
                mpd::run_mpd_cover_enrich(&mpd_cfg, &conn);
            }

            // Online enrichment: MusicBrainz lookup for MBID + genre, Cover Art
            // Archive for any albums still missing a cover after the MPD pass.
            if online {
                enrich::run_enrich(&conn, force, false);
            }
        }
        Commands::LastScrobble { db_path } => {
            let path = db_path.unwrap_or_else(default_db_path);
            let conn = match db::open_db(&path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to open database at {}: {}", path, e);
                    std::process::exit(1);
                }
            };
            match db::latest_scrobble_at(&conn) {
                Ok(Some(ts)) => println!("{}", ts),
                Ok(None) => {}
                Err(e) => {
                    eprintln!("Failed to query latest scrobble: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Commands::PinAlbum {
            artist,
            album,
            mbid,
            cover_url,
            db_path,
        } => {
            let path = db_path.unwrap_or_else(default_db_path);
            let conn = match db::open_db(&path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to open database at {}: {}", path, e);
                    std::process::exit(1);
                }
            };
            enrich::enrich_by_mbid(&conn, &artist, &album, &mbid, cover_url.as_deref());
        }
    }
}
