//! MPRIS Scrobbler — a local music scrobbler for MPRIS-compatible players.
//!
//! This is the CLI entry point. It provides four subcommands:
//!
//! - `watch` — monitors a player via `playerctl` and records scrobbles to SQLite
//! - `report` — generates listening statistics from the stored scrobble data
//! - `enrich` — fetches album art and genre info from MusicBrainz
//! - `last-scrobble` — prints the newest scrobble timestamp
//! - `pin-album` — manually assign a MusicBrainz ID to an album the automatic search missed
//!
//! ## Architecture overview
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
//! Ctrl+C triggers a graceful shutdown: the handler sends an `Eof` event
//! through the channel, causing the tracker to evaluate the last track
//! before exiting.

mod db;
mod enrich;
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
    /// Watch playerctl metadata and scrobble tracks to the local database.
    Watch {
        /// Player name for playerctl (the MPRIS bus name).
        /// Run `playerctl -l` to see available players.
        #[arg(long, default_value = DEFAULT_PLAYER)]
        player: String,

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
    /// Fetch album art and genre info from MusicBrainz for all scrobbled albums.
    ///
    /// This command looks up each unique (artist, album) pair that doesn't yet
    /// have cached metadata, queries MusicBrainz for the release, downloads
    /// cover art from the Cover Art Archive, and stores everything locally.
    Enrich {
        /// Re-fetch metadata for all albums, even those already cached.
        #[arg(long)]
        force: bool,

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

/// Run the `watch` subcommand: spawn playerctl processes, read events, and
/// scrobble tracks to the database.
fn run_watch(player: &str, db_path: &str) {
    // Open (or create) the database and wrap it in Arc<Mutex<>> for sharing
    // with the scrobble callback. In practice, only the main thread accesses
    // it, but the Mutex is needed because the callback closure is FnMut and
    // could theoretically be called from different contexts.
    let conn = db::open_db(db_path).expect("Failed to open database");
    let conn = Arc::new(Mutex::new(conn));

    eprintln!("Database: {}", db_path);
    eprintln!("Watching player: {}", player);

    // Channel for sending events from reader threads to the main event loop.
    let (tx, rx) = mpsc::channel::<watcher::Event>();

    // --- Spawn playerctl metadata follower ---
    // This process outputs one tab-separated line per track change:
    //   artist\talbum\ttitle\tmpris:length
    let metadata_cmd = Command::new("playerctl")
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

    let metadata_proc = match metadata_cmd {
        Ok(proc) => proc,
        Err(e) => {
            eprintln!("Failed to spawn playerctl metadata: {}", e);
            std::process::exit(1);
        }
    };

    // --- Spawn playerctl status follower ---
    // This process outputs one line per state change: "Playing", "Paused", or "Stopped".
    let status_cmd = Command::new("playerctl")
        .args(["-p", player, "--follow", "status"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn();

    let status_proc = match status_cmd {
        Ok(proc) => proc,
        Err(e) => {
            eprintln!("Failed to spawn playerctl status: {}", e);
            std::process::exit(1);
        }
    };

    // --- Metadata reader thread ---
    // Reads lines from the metadata process's stdout, parses them into
    // `Event::Metadata` values, and sends them through the channel.
    // Sends `Event::Eof` when the process ends (stdout closes).
    let tx_meta = tx.clone();
    let meta_handle = thread::spawn(move || {
        let stdout = metadata_proc
            .stdout
            .expect("No stdout for metadata process");
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
                Err(_) => break, // Read error — process likely ended.
            }
        }
        // Signal that this process has ended.
        let _ = tx_meta.send(watcher::Event::Eof);
    });

    // --- Status reader thread ---
    // Same pattern as the metadata reader, but parses status lines instead.
    let tx_status = tx.clone();
    let status_handle = thread::spawn(move || {
        let stdout = status_proc.stdout.expect("No stdout for status process");
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
    });

    // --- Ctrl+C handler ---
    // Sets the `running` flag to false and sends an Eof event to unblock
    // the main loop, allowing a graceful shutdown that evaluates the last track.
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let tx_ctrlc = tx.clone();
    ctrlc::set_handler(move || {
        eprintln!("\nShutting down...");
        running_clone.store(false, Ordering::SeqCst);
        let _ = tx_ctrlc.send(watcher::Event::Eof);
    })
    .expect("Failed to set Ctrl+C handler");

    // --- Main event loop ---
    // Receives events from both reader threads and the Ctrl+C handler,
    // and feeds them into the ScrobbleTracker state machine.
    let mut tracker = watcher::create_db_tracker(conn);
    let mut eof_count = 0;

    while running.load(Ordering::SeqCst) {
        match rx.recv() {
            Ok(event) => {
                if event == watcher::Event::Eof {
                    eof_count += 1;
                    // Wait for both child processes to end before shutting down.
                    // (The Ctrl+C handler also sends Eof, so we may get up to 3.)
                    if eof_count >= 2 {
                        tracker.handle_event(watcher::Event::Eof);
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

    // Final evaluation: ensure the last track is scrobbled if it qualifies.
    // This is safe to call even if Eof was already handled above — the tracker
    // handles the "no current track" case gracefully.
    tracker.handle_event(watcher::Event::Eof);

    eprintln!("Goodbye.");

    // Wait for reader threads to finish (they should already be done since
    // the child processes have ended or been killed).
    let _ = meta_handle.join();
    let _ = status_handle.join();
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
        Commands::Watch { player, db_path } => {
            let path = db_path.unwrap_or_else(default_db_path);
            run_watch(&player, &path);
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
        Commands::Enrich { force, db_path } => {
            let path = db_path.unwrap_or_else(default_db_path);
            let conn = match db::open_db(&path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to open database at {}: {}", path, e);
                    std::process::exit(1);
                }
            };
            enrich::run_enrich(&conn, force, false);
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
