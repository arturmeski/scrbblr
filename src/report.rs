//! Report module — generates listening statistics in three output formats.
//!
//! This module reads scrobble data from the database (via `db` module queries)
//! and presents it in one of three formats:
//!
//! 1. **Terminal tables** — box-drawing character tables with bar charts,
//!    showing overview stats, top artists/albums/tracks, and recent scrobbles
//!    for a single period.
//!
//! 2. **JSON** — the same single-period data serialized as pretty-printed JSON,
//!    suitable for piping into other tools (e.g., `jq`).
//!
//! 3. **HTML** — a standalone, multi-period report with a dark theme. The HTML
//!    output includes all periods (Today, This Week, This Month, All Time) in
//!    one page, with:
//!    - KPI cards for each period (scrobbles, listen time, unique counts)
//!    - Horizontal bar charts for top artists, top albums, and top tracks
//!    - Album cover art grids (fetched via the `enrich` module)
//!    - Genre badges from MusicBrainz
//!    - A recent scrobbles table (All Time section only)
//!
//! Terminal and JSON reports operate on a single `--period` flag.
//! HTML reports ignore `--period` and always render all periods together.
//!
//! ## Output structure for `--html --output <dir>`
//!
//! ```text
//! <dir>/
//! ├── index.html       ← properly indented, human-readable HTML
//! └── covers/
//!     ├── <mbid1>.jpg   ← cover images copied from the enrichment cache
//!     └── <mbid2>.jpg
//! ```
//!
//! Cover images are referenced via relative `covers/<filename>` paths so the
//! directory is self-contained and can be moved or shared as-is.

use crate::db;
use rusqlite::Connection;
use serde::Serialize;
use std::fmt::Write as _;

/// Maximum bar width used in terminal tables.
const MAX_BAR_WIDTH: usize = 20;
/// Minimum bar width used in terminal tables when terminal is narrow.
const MIN_BAR_WIDTH: usize = 8;

// ---------------------------------------------------------------------------
// Duration formatting
// ---------------------------------------------------------------------------

/// Format a duration in seconds into a human-readable string.
///
/// Examples:
///   - 45 seconds  → "45s"
///   - 120 seconds → "2m"
///   - 3660 seconds → "1h 01m"
///   - 0 seconds   → "0s"
pub fn format_duration(secs: i64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        if mins == 0 {
            format!("{}h", hours)
        } else {
            format!("{}h {:02}m", hours, mins)
        }
    }
}

/// Format a play count with correct singular/plural grammar.
fn format_play_count(plays: i64) -> String {
    if plays == 1 {
        "1 play".to_string()
    } else {
        format!("{} plays", plays)
    }
}

/// Build a short "mood" label from top genres.
///
/// Selection rule:
/// - take the top genre first
/// - then keep scanning in rank order and only take the next genre if its
///   listen time differs from the previously selected genre
///
/// This avoids showing multiple mood tags with identical play-time weight.
fn mood_labels(top_genres: &[db::TopGenre], max_labels: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut last_selected_time: Option<i64> = None;

    for g in top_genres {
        if out.len() >= max_labels {
            break;
        }
        if let Some(prev_time) = last_selected_time
            && g.listen_time_secs == prev_time
        {
            continue;
        }
        out.push(g.genre.clone());
        last_selected_time = Some(g.listen_time_secs);
    }

    out
}

/// Convert an ISO 8601 timestamp into a short, privacy-friendly relative
/// time label. Avoids exposing exact listening times. Labels are kept compact
/// so they fit well in both terminal tables and HTML columns.
///
/// Examples:
///   - 10 minutes ago → "Moments ago"
///   - 50 minutes ago → "Within hour"
///   - 2 hours ago    → "A while ago"
///   - 8 hours ago    → "Today"
///   - yesterday      → "Yesterday"
///   - 3 days ago     → "Few days"
///   - 2 weeks ago    → "About week"
///   - 5 weeks ago    → "About month"
///   - 3 months ago   → "Few months"
///   - 14 months ago  → "Over year"
fn fuzzy_time(iso_timestamp: &str) -> String {
    let Ok(then) = chrono::NaiveDateTime::parse_from_str(iso_timestamp, "%Y-%m-%dT%H:%M:%S") else {
        return "Some time".to_string();
    };
    let now = chrono::Local::now().naive_local();
    let delta = now.signed_duration_since(then);
    let hours = delta.num_hours();
    let days = delta.num_days();

    // Use calendar-date comparison for "Today" and "Yesterday" so that a track
    // scrobbled before midnight is never labelled "Today" after the date rolls over.
    let today = now.date();
    let then_date = then.date();

    match () {
        _ if delta.num_minutes() < 20 => "Moments ago".to_string(),
        _ if delta.num_minutes() < 60 => "Within hour".to_string(),
        _ if hours < 3 => "A while ago".to_string(),
        _ if then_date == today => "Today".to_string(),
        _ if then_date == today - chrono::Duration::days(1) => "Yesterday".to_string(),
        _ if days < 5 => "Few days".to_string(),
        _ if days < 14 => "About week".to_string(),
        _ if days < 21 => "2 weeks".to_string(),
        _ if days < 45 => "About month".to_string(),
        _ if days < 90 => "Few months".to_string(),
        _ if days < 365 => "Months ago".to_string(),
        _ => "Over year".to_string(),
    }
}

/// Terse relative time label for terminal tables.
///
/// Uses minimal-width labels like "2m", "1h", "3d" to avoid column bloat.
/// Privacy is still maintained — no exact timestamps are shown.
///
/// Examples:
///   - 2 minutes ago  → "2m"
///   - 45 minutes ago → "45m"
///   - 2 hours ago    → "2h"
///   - yesterday      → "1d"
///   - 10 days ago    → "10d"
///   - 5 weeks ago    → "5w"
///   - 3 months ago   → "3mo"
///   - 2 years ago    → "2y"
fn terse_time(iso_timestamp: &str) -> String {
    let Ok(then) = chrono::NaiveDateTime::parse_from_str(iso_timestamp, "%Y-%m-%dT%H:%M:%S") else {
        return "?".to_string();
    };
    let now = chrono::Local::now().naive_local();
    let delta = now.signed_duration_since(then);
    let mins = delta.num_minutes();
    let hours = delta.num_hours();
    let days = delta.num_days();

    match () {
        _ if mins < 1 => "<1m".to_string(),
        _ if mins < 60 => format!("{}m", mins),
        _ if hours < 24 => format!("{}h", hours),
        _ if days < 14 => format!("{}d", days),
        _ if days < 90 => format!("{}w", days / 7),
        _ if days < 730 => format!("{}mo", days / 30),
        _ => format!("{}y", days / 365),
    }
}

// ---------------------------------------------------------------------------
// Report data structures
// ---------------------------------------------------------------------------

/// The complete report data, containing all sections. This struct is used
/// both for terminal rendering and JSON serialization, so all fields
/// implement `Serialize`.
#[derive(Debug, Serialize)]
pub struct ReportData {
    /// Which time period this report covers.
    pub period: PeriodInfo,
    /// Aggregate statistics (total scrobbles, listen time, unique counts).
    pub overview: db::Overview,
    /// Top artists ranked by play count.
    pub top_artists: Vec<db::TopArtist>,
    /// Top albums ranked by play count.
    pub top_albums: Vec<db::TopAlbum>,
    /// Top tracks ranked by play count.
    pub top_tracks: Vec<db::TopTrack>,
    /// Top genres derived from cached album metadata.
    pub top_genres: Vec<db::TopGenre>,
    /// Breakdown of scrobbles by source (player/service).
    pub top_sources: Vec<db::TopSource>,
    /// Most recent scrobbles (newest first), limited to 20.
    pub recent_scrobbles: Vec<db::Scrobble>,
}

/// Metadata about the time period the report covers.
/// For "all time" reports, `from` and `to` will be `None`.
#[derive(Debug, Serialize)]
pub struct PeriodInfo {
    /// The period name as provided by the user (e.g., "week", "month", "all").
    pub name: String,
    /// ISO 8601 start of the period, or None for "all".
    pub from: Option<String>,
    /// ISO 8601 end of the period (current time), or None for "all".
    pub to: Option<String>,
}

// ---------------------------------------------------------------------------
// Terminal box-drawing table renderer
//
// Uses Unicode box-drawing characters (─ │ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼) and block
// elements (█ ░) to produce tables with horizontal bar charts directly in
// the terminal.  No external crate needed.
// ---------------------------------------------------------------------------

/// Render a table with box-drawing characters.
/// `headers` are the column names, `rows` are the cell values.
/// The last column in each row is treated as a numeric value for the bar chart
/// if `bar_max` is Some (the maximum value for scaling bars).
fn print_box_table(
    headers: &[&str],
    rows: &[Vec<String>],
    bar_max: Option<i64>,
    shrinkable_cols: &[usize],
) {
    if rows.is_empty() {
        return;
    }

    // Calculate column widths (max of header and all cell widths).
    let ncols = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| visible_width(h)).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < ncols {
                widths[i] = widths[i].max(visible_width(cell));
            }
        }
    }

    // Fit table to terminal width (best-effort): reduce bar width first, then
    // shrink only selected text columns.
    let terminal_width = terminal_columns();
    let mut bar_width = if bar_max.is_some() { MAX_BAR_WIDTH } else { 0 };

    while table_width(&widths, bar_width) > terminal_width && bar_width > MIN_BAR_WIDTH {
        bar_width -= 1;
    }

    while table_width(&widths, bar_width) > terminal_width {
        let mut changed = false;
        let mut widest: Option<usize> = None;
        for &idx in shrinkable_cols {
            if idx >= widths.len() {
                continue;
            }
            let min_w = 6usize;
            if widths[idx] <= min_w {
                continue;
            }
            if widest.is_none_or(|w| widths[idx] > widths[w]) {
                widest = Some(idx);
            }
        }
        if let Some(i) = widest {
            widths[i] -= 1;
            changed = true;
        }
        if !changed {
            break;
        }
    }

    // If we have a bar chart, add space for it after the last column.
    let bar_col = if bar_max.is_some() { bar_width + 2 } else { 0 };

    let header_cells: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| truncate_cell(h, widths[i]))
        .collect();

    // Top border: ┌──────┬──────┐
    print!("  ┌");
    for (i, w) in widths.iter().enumerate() {
        print!("{}", "─".repeat(w + 2));
        if bar_max.is_some() && i == ncols - 1 {
            print!("┬{}", "─".repeat(bar_col));
        }
        if i < ncols - 1 {
            print!("┬");
        }
    }
    println!("┐");

    // Header row: │ Header │ Header │
    print!("  │");
    for (i, h) in header_cells.iter().enumerate() {
        print!(" {:<width$} ", h, width = widths[i]);
        if bar_max.is_some() && i == ncols - 1 {
            print!("│ {:<width$}", "", width = bar_col - 1);
        }
        if i < ncols - 1 {
            print!("│");
        }
    }
    println!("│");

    // Header separator: ├──────┼──────┤
    print!("  ├");
    for (i, w) in widths.iter().enumerate() {
        print!("{}", "─".repeat(w + 2));
        if bar_max.is_some() && i == ncols - 1 {
            print!("┼{}", "─".repeat(bar_col));
        }
        if i < ncols - 1 {
            print!("┼");
        }
    }
    println!("┤");

    // Data rows: │ data │ data │ ████░░░░ │
    for row in rows {
        print!("  │");
        for (i, cell) in row.iter().enumerate() {
            if i < ncols {
                let shown = truncate_cell(cell, widths[i]);
                print!(" {:<width$} ", shown, width = widths[i]);
                if bar_max.is_some() && i == ncols - 1 {
                    // Render bar chart in the extra column.
                    let val: i64 = cell.parse().unwrap_or(0);
                    let max = bar_max.unwrap_or(1).max(1);
                    let filled = ((val as f64 / max as f64) * bar_width as f64).round() as usize;
                    let empty = bar_width - filled;
                    print!("│ {}{} ", "█".repeat(filled), "░".repeat(empty));
                }
                if i < ncols - 1 {
                    print!("│");
                }
            }
        }
        println!("│");
    }

    // Bottom border: └──────┴──────┘
    print!("  └");
    for (i, w) in widths.iter().enumerate() {
        print!("{}", "─".repeat(w + 2));
        if bar_max.is_some() && i == ncols - 1 {
            print!("┴{}", "─".repeat(bar_col));
        }
        if i < ncols - 1 {
            print!("┴");
        }
    }
    println!("┘");
}

/// Best-effort terminal width in character columns.
///
/// Tries (in order):
/// 1. `ioctl` TIOCGWINSZ on stdout
/// 2. `COLUMNS` environment variable
/// 3. Fallback to 80
fn terminal_columns() -> usize {
    // Try ioctl first — this is the only reliable method.
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        #[repr(C)]
        struct Winsize {
            ws_row: libc::c_ushort,
            ws_col: libc::c_ushort,
            ws_xpixel: libc::c_ushort,
            ws_ypixel: libc::c_ushort,
        }
        let mut ws = MaybeUninit::<Winsize>::uninit();
        let ret = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) };
        if ret == 0 {
            let ws = unsafe { ws.assume_init() };
            if ws.ws_col >= 40 {
                return ws.ws_col as usize;
            }
        }
    }

    if let Ok(cols) = std::env::var("COLUMNS")
        && let Ok(parsed) = cols.parse::<usize>()
        && parsed >= 40
    {
        return parsed;
    }
    80
}

/// Compute rendered table width for current settings.
fn table_width(widths: &[usize], bar_width: usize) -> usize {
    let ncols = widths.len();
    let mut total = 2 + 1 + 1; // indent + left border + right border
    for (i, w) in widths.iter().enumerate() {
        total += w + 2;
        if i < ncols - 1 {
            total += 1;
        }
    }
    if bar_width > 0 {
        total += 1 + (bar_width + 2);
    }
    total
}

/// Visible character width (simple char-count approximation).
fn visible_width(s: &str) -> usize {
    s.chars().count()
}

/// Truncate a cell to a target width using ASCII ellipsis.
fn truncate_cell(s: &str, width: usize) -> String {
    let len = visible_width(s);
    if len <= width {
        return s.to_string();
    }
    if width <= 3 {
        return s.chars().take(width).collect();
    }
    let mut out: String = s.chars().take(width - 3).collect();
    out.push_str("...");
    out
}

// ---------------------------------------------------------------------------
// Report generation
// ---------------------------------------------------------------------------

/// Return the set of `(artist, album)` pairs that will appear anywhere in the
/// HTML report at the given limits. Used to focus automatic enrichment on
/// exactly what the report needs rather than the entire scrobble library.
pub fn albums_needed_for_report(
    conn: &Connection,
    limit: i64,
    all_time_limit: i64,
) -> std::collections::HashSet<(String, String)> {
    let mut pairs = std::collections::HashSet::new();

    let periods: &[(&str, i64)] = &[
        ("today", limit),
        ("week", limit),
        ("month", limit),
        ("all", all_time_limit),
    ];

    for (period, lim) in periods {
        let cover_lim = album_cover_grid_limit(*lim);

        if let Ok(albums) = db::top_albums(conn, period, cover_lim) {
            for a in albums {
                pairs.insert((a.artist, a.album));
            }
        }
        if let Ok(tracks) = db::top_tracks(conn, period, *lim) {
            for t in tracks {
                pairs.insert((t.artist, t.album));
            }
        }
        // Include the most-played album per top artist so artist_cover()
        // has something to work with even when that album is outside top_albums.
        if let Ok(artists) = db::top_artists(conn, period, *lim) {
            for a in artists {
                if let Some(album) = db::artist_top_album(conn, &a.artist) {
                    pairs.insert((a.artist, album));
                }
            }
        }
    }

    pairs
}

/// Query the database and assemble all sections of the report.
///
/// This is the main entry point for report generation. It runs all the
/// necessary SQL queries for the given period and limit, and returns
/// a `ReportData` struct ready for rendering.
///
/// # Arguments
///
/// - `conn`   — SQLite database connection
/// - `period` — time period filter ("today", "week", "month", "year", "all")
/// - `limit`  — maximum number of entries in each top-N list
pub fn gather_report(
    conn: &Connection,
    period: &str,
    limit: i64,
) -> Result<ReportData, rusqlite::Error> {
    // Compute the date range for the header/JSON metadata.
    let range = db::period_range(period);
    let period_info = PeriodInfo {
        name: period.to_string(),
        from: range.as_ref().map(|(f, _)| f.clone()),
        to: range.as_ref().map(|(_, t)| t.clone()),
    };

    // Run all queries. Each query internally applies the same period filter.
    Ok(ReportData {
        period: period_info,
        overview: db::overview(conn, period)?,
        top_artists: db::top_artists(conn, period, limit)?,
        top_albums: db::top_albums(conn, period, limit)?,
        top_tracks: db::top_tracks(conn, period, limit)?,
        top_genres: db::top_genres(conn, period, limit)?,
        top_sources: db::top_sources(conn, period)?,
        // Recent scrobbles are always limited to 20, regardless of the --limit flag.
        recent_scrobbles: db::recent_scrobbles(conn, period, 20)?,
    })
}

// ---------------------------------------------------------------------------
// Terminal output
// ---------------------------------------------------------------------------

/// Render the report as box-drawing tables with bar charts to stdout.
///
/// Output sections (each printed only if non-empty):
///   1. Header with period name and date range
///   2. Overview KPIs
///   3. Top Artists with bar chart
///   4. Top Albums with bar chart
///   5. Top Genres with bar chart
///   6. Top Tracks with bar chart
///   7. Recent Scrobbles
pub fn print_terminal_report(data: &ReportData) {
    // --- Header ---
    let period_label = match data.period.name.as_str() {
        "today" => "Today",
        "week" => "This Week",
        "month" => "This Month",
        "year" => "This Year",
        _ => "All Time",
    };
    let date_range = match (&data.period.from, &data.period.to) {
        (Some(from), Some(_)) if data.period.name == "today" => format!("  {}", &from[..10]),
        (Some(from), Some(to)) => format!("  {} → {}", &from[..10], &to[..10]),
        _ => String::new(),
    };
    println!();
    println!(
        "  ┌─ Listening Report ─── {} ──{}─┐",
        period_label, date_range
    );
    println!();

    // --- Overview KPIs ---
    println!(
        "  Scrobbles: {}   Listen time: {}   Artists: {}   Albums: {}   Tracks: {}",
        data.overview.total_scrobbles,
        format_duration(data.overview.total_listen_time_secs),
        data.overview.unique_artists,
        data.overview.unique_albums,
        data.overview.unique_tracks,
    );

    let mood = mood_labels(&data.top_genres, 6);
    if !mood.is_empty() && data.period.name != "all" {
        println!("  Mood: {}", mood.join(" · "));
    }

    // --- Source Breakdown ---
    if !data.top_sources.is_empty() {
        println!("\n  Sources");
        let rows: Vec<Vec<String>> = data
            .top_sources
            .iter()
            .map(|s| {
                vec![
                    s.source.clone(),
                    s.scrobbles.to_string(),
                    format_duration(s.listen_time_secs),
                ]
            })
            .collect();
        print_box_table(&["Source", "Scrobbles", "Time"], &rows, None, &[0]);
    }

    // --- Top Artists ---
    if !data.top_artists.is_empty() {
        println!("\n  Top Artists");
        let max_plays = data.top_artists.first().map(|a| a.plays).unwrap_or(1);
        let rows: Vec<Vec<String>> = data
            .top_artists
            .iter()
            .enumerate()
            .map(|(i, a)| {
                vec![
                    format!("{:>2}", i + 1),
                    a.artist.clone(),
                    format_duration(a.listen_time_secs),
                    a.plays.to_string(),
                ]
            })
            .collect();
        print_box_table(
            &["#", "Artist", "Time", "Plays"],
            &rows,
            Some(max_plays),
            &[1],
        );
    }

    // --- Top Albums ---
    if !data.top_albums.is_empty() {
        println!("\n  Top Albums");
        let max_plays = data.top_albums.first().map(|a| a.plays).unwrap_or(1);
        let rows: Vec<Vec<String>> = data
            .top_albums
            .iter()
            .enumerate()
            .map(|(i, a)| {
                vec![
                    format!("{:>2}", i + 1),
                    a.artist.clone(),
                    a.album.clone(),
                    format_duration(a.listen_time_secs),
                    a.plays.to_string(),
                ]
            })
            .collect();
        print_box_table(
            &["#", "Artist", "Album", "Time", "Plays"],
            &rows,
            Some(max_plays),
            &[1, 2],
        );
    }

    // --- Top Genres ---
    if !data.top_genres.is_empty() {
        println!("\n  Top Genres");
        let max_plays = data.top_genres.first().map(|g| g.plays).unwrap_or(1);
        let rows: Vec<Vec<String>> = data
            .top_genres
            .iter()
            .enumerate()
            .map(|(i, g)| {
                vec![
                    format!("{:>2}", i + 1),
                    g.genre.clone(),
                    format_duration(g.listen_time_secs),
                    g.plays.to_string(),
                ]
            })
            .collect();
        print_box_table(
            &["#", "Genre", "Time", "Plays"],
            &rows,
            Some(max_plays),
            &[1],
        );
    }

    // --- Top Tracks ---
    if !data.top_tracks.is_empty() {
        println!("\n  Top Tracks");
        let max_plays = data.top_tracks.first().map(|t| t.plays).unwrap_or(1);
        let rows: Vec<Vec<String>> = data
            .top_tracks
            .iter()
            .enumerate()
            .map(|(i, t)| {
                vec![
                    format!("{:>2}", i + 1),
                    t.artist.clone(),
                    t.title.clone(),
                    format_duration(t.listen_time_secs),
                    t.plays.to_string(),
                ]
            })
            .collect();
        print_box_table(
            &["#", "Artist", "Title", "Time", "Plays"],
            &rows,
            Some(max_plays),
            &[1, 2],
        );
    }

    // --- Recent Scrobbles ---
    if !data.recent_scrobbles.is_empty() {
        println!("\n  Recent Scrobbles");
        let rows: Vec<Vec<String>> = data
            .recent_scrobbles
            .iter()
            .map(|s| {
                vec![
                    terse_time(&s.scrobbled_at),
                    s.artist.clone(),
                    s.title.clone(),
                    s.album.clone(),
                ]
            })
            .collect();
        print_box_table(
            &["Ago", "Artist", "Title", "Album"],
            &rows,
            None,
            &[1, 2, 3],
        );
    }

    println!();
}

// ---------------------------------------------------------------------------
// JSON output
// ---------------------------------------------------------------------------

/// Serialize the report as pretty-printed JSON and write it to stdout.
/// This is the output format used with `--json`.
pub fn print_json_report(data: &ReportData) {
    println!(
        "{}",
        serde_json::to_string_pretty(data).expect("Failed to serialize report")
    );
}

/// Result of rendering an HTML report. Contains the HTML string and a list
/// of cover image files that should be copied into a `covers/` subdirectory
/// next to the HTML file.
pub struct HtmlReport {
    /// The generated HTML document.
    pub html: String,
    /// Absolute paths to cover image files that the HTML references.
    /// Each should be copied to `<output_dir>/covers/<filename>`.
    pub cover_files: Vec<std::path::PathBuf>,
}

/// Render a multi-period HTML report.
///
/// The report contains sections for Today, This Week, This Month, and All Time,
/// each with their own KPIs, top artists/albums/tracks with bar charts, and
/// album cover art cards.
///
/// Cover images are referenced via relative `covers/<filename>` paths.
pub fn render_html_report(conn: &Connection, limit: i64, all_time_limit: i64) -> HtmlReport {
    // Gather data for each period. All-time uses its own (larger) limit;
    // shorter periods use the base limit as-is.
    let periods: &[(&str, i64)] = &[
        ("today", limit),
        ("week", limit),
        ("month", limit),
        ("all", all_time_limit),
    ];
    let reports: Vec<ReportData> = periods
        .iter()
        .filter_map(|(p, l)| gather_report(conn, p, *l).ok())
        .collect();

    // Build a stable source → colour index from all-time play counts so that
    // the same source always gets the same colour regardless of which period
    // is being rendered. Most-played source gets palette slot 0, and so on.
    let ordered_sources: Vec<String> = db::top_sources(conn, "all")
        .unwrap_or_default()
        .into_iter()
        .map(|s| s.source)
        .collect();

    let mut cover_files: Vec<std::path::PathBuf> = Vec::new();
    let mut h = HtmlWriter::new();

    // --- HTML head + CSS ---
    h.line("<!doctype html>");
    h.line("<html lang=\"en\">");
    h.line("<head>");
    h.indent();
    h.line("<meta charset=\"utf-8\">");
    h.line("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    h.line("<title>Listening Report</title>");
    h.line("<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">");
    h.line("<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>");
    h.line("<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap\" rel=\"stylesheet\">");
    h.line("<style>");
    h.raw(CSS);
    h.line("</style>");
    h.dedent();
    h.line("</head>");
    h.line("<body>");
    h.open("<div class=\"wrap\">");

    // --- Hero header ---
    let generated_at = chrono::Local::now().format("%Y-%m-%d %H:%M").to_string();
    h.open("<div class=\"hero\">");
    h.line("<h1>Listening Report</h1>");
    h.linef(format_args!(
        "<div class=\"sub\">Generated by <a href=\"https://github.com/arturmeski/scrbblr\">scrbblr</a> on {}</div>",
        html_escape(&generated_at)
    ));
    h.close("</div>");

    // --- Mobile-friendly jump menu ---
    // Show links only for periods that will actually render.
    let nav_periods: Vec<(&str, &str)> = reports
        .iter()
        .filter(|d| d.overview.total_scrobbles > 0)
        .map(|d| match d.period.name.as_str() {
            "today" => ("Today", "period-today"),
            "week" => ("This Week", "period-week"),
            "month" => ("This Month", "period-month"),
            _ => ("All Time", "period-all"),
        })
        .collect();
    if !nav_periods.is_empty() {
        h.open("<nav class=\"jump-nav\" aria-label=\"Report sections\">");
        for (label, id) in &nav_periods {
            h.linef(format_args!(
                "<a href=\"#{}\">{}</a>",
                html_attr_escape(id),
                html_escape(label)
            ));
        }
        h.close("</nav>");
    }

    // --- Period sections (Today → Week → Month → All Time) ---
    for data in &reports {
        let (label, is_all_time, section_id) = match data.period.name.as_str() {
            "today" => ("Today", false, "period-today"),
            "week" => ("This Week", false, "period-week"),
            "month" => ("This Month", false, "period-month"),
            _ => ("All Time", true, "period-all"),
        };
        let range_str = match (&data.period.from, &data.period.to) {
            (Some(from), Some(_)) if data.period.name == "today" => from[..10].to_string(),
            (Some(from), Some(to)) => format!("{} → {}", &from[..10], &to[..10]),
            _ => String::new(),
        };

        // Skip empty periods.
        if data.overview.total_scrobbles == 0 {
            continue;
        }

        h.blank();
        h.open(&format!(
            "<div class=\"period-block\" id=\"{}\">",
            section_id
        ));
        h.linef(format_args!(
            "<div class=\"period-title\">{}</div>",
            html_escape(label)
        ));
        if !range_str.is_empty() {
            h.linef(format_args!(
                "<div class=\"period-range\">{}</div>",
                html_escape(&range_str)
            ));
        }

        let mood = mood_labels(&data.top_genres, 6);
        if !mood.is_empty() && !is_all_time {
            h.open("<div class=\"mood\">");
            h.linef(format_args!(
                "<span class=\"mood-title\">Mood of {}:</span>",
                html_escape(label)
            ));
            for genre in mood {
                h.linef(format_args!(
                    "<span class=\"mood-pill\">{}</span>",
                    html_escape(&genre)
                ));
            }
            h.close("</div>");
        }

        // KPIs
        h.open("<div class=\"kpis\">");
        write_kpi(
            &mut h,
            "Scrobbles",
            &data.overview.total_scrobbles.to_string(),
        );
        write_kpi(
            &mut h,
            "Listen time",
            &format_duration(data.overview.total_listen_time_secs),
        );
        write_kpi(&mut h, "Artists", &data.overview.unique_artists.to_string());
        write_kpi(&mut h, "Albums", &data.overview.unique_albums.to_string());
        write_kpi(&mut h, "Tracks", &data.overview.unique_tracks.to_string());
        h.close("</div>");

        // Source breakdown — simple bar table showing scrobbles per player/service.
        // Only rendered when there is more than one source, or when the single
        // source differs from the default expectation, so it doesn't clutter
        // reports for users with a single player.
        if !data.top_sources.is_empty() {
            let source_rows: Vec<BarRow> = data
                .top_sources
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    // Prefix the source name with a colour dot matching the
                    // palette slot assigned to it in the all-time ranking.
                    let label = if let Some((bg, _)) = source_colours(&s.source, &ordered_sources) {
                        format!(
                            "<span style=\"display:inline-block;width:9px;height:9px;\
                             border-radius:50%;background:{};margin-right:5px;\
                             vertical-align:middle\"></span>{}",
                            bg,
                            html_escape(&s.source)
                        )
                    } else {
                        html_escape(&s.source)
                    };
                    BarRow {
                        cells: vec![(i + 1).to_string(), label],
                        value: s.scrobbles,
                        suffix: format_duration(s.listen_time_secs),
                        cover: None,
                        raw_cells: true,
                    }
                })
                .collect();
            write_bar_table(
                &mut h,
                "Sources",
                &["#", "Source", "Scrobbles", ""],
                &source_rows,
            );
        }

        // Album cover grid — shown first after KPIs to visually illustrate
        // the period's listening at a glance.
        //
        // We intentionally round the cover count up to a full desktop row so
        // the grid doesn't end with a partially filled row when the user picks
        // a limit like 20 and desktop layout has 6 columns.
        let cover_limit = album_cover_grid_limit(limit);
        let cover_albums = db::top_albums(conn, &data.period.name, cover_limit)
            .unwrap_or_else(|_| data.top_albums.clone());
        if !cover_albums.is_empty() {
            h.blank();
            h.open("<section>");
            h.line("<h3 class=\"section-title\">Top Album Covers</h3>");
            h.open("<div class=\"grid\">");
            for a in &cover_albums {
                let meta = db::album_cache_meta(conn, &a.artist, &a.album)
                    .ok()
                    .flatten();
                let genre = meta.as_ref().and_then(|m| m.genre.as_ref());
                let cmd = pin_album_cmd(
                    &a.artist,
                    &a.album,
                    meta.as_ref().and_then(|m| m.mbid.as_deref()),
                );
                // Tint the card background with the dominant source's colour.
                let card_style = a
                    .dominant_source
                    .as_deref()
                    .and_then(|src| source_colours(src, &ordered_sources))
                    .map(|(bg, border)| {
                        format!(" style=\"background:{};border-color:{}\"", bg, border)
                    })
                    .unwrap_or_default();
                h.linef(format_args!("<article class=\"album\"{}>", card_style));

                let cover_rel = resolve_cover(
                    meta.as_ref().and_then(|m| m.cover_url.clone()),
                    &mut cover_files,
                );
                if let Some(ref rel) = cover_rel {
                    h.linef(format_args!(
                        "<img class=\"cover\" src=\"{}\" alt=\"{}\">",
                        html_attr_escape(rel),
                        html_attr_escape(&format!("{} - {}", a.artist, a.album))
                    ));
                } else {
                    h.line("<div class=\"ph\">No cover</div>");
                }
                h.linef(format_args!(
                    "<button class=\"pin-dot\" data-cmd=\"{}\" title=\"Copy pin-album command\" \
                     onclick=\"var b=this;navigator.clipboard.writeText(b.dataset.cmd)\
                     .then(function(){{b.textContent='✓';setTimeout(function(){{b.textContent='📌'}},1500)}})\
                     .catch(function(){{b.textContent='!'}})\" \
                     >📌</button>",
                    html_attr_escape(&cmd)
                ));

                h.open("<div class=\"meta\">");
                h.linef(format_args!(
                    "<div class=\"t\">{}</div>",
                    html_escape(&a.album)
                ));
                h.linef(format_args!(
                    "<div class=\"a\">{}</div>",
                    html_escape(&a.artist)
                ));
                h.linef(format_args!(
                    "<div class=\"a\">{} · {}</div>",
                    html_escape(&format_play_count(a.plays)),
                    html_escape(&format_duration(a.listen_time_secs))
                ));
                if let Some(g) = genre {
                    let labels = top_genre_labels(g, 3);
                    if !labels.is_empty() {
                        h.open("<div class=\"genre\">");
                        for label in labels {
                            h.linef(format_args!(
                                "<span class=\"pill\">{}</span>",
                                html_escape(&label)
                            ));
                        }
                        h.close("</div>");
                    }
                }
                h.close("</div>"); // .meta
                h.close("</article>");
            }
            h.close("</div>"); // .grid
            h.close("</section>");
        }

        // Top Albums as a bar table, matching the visual language used by
        // Top Artists and Top Tracks.
        let album_rows: Vec<BarRow> = data
            .top_albums
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let cover_url = db::album_cache_meta(conn, &a.artist, &a.album)
                    .ok()
                    .flatten()
                    .and_then(|m| m.cover_url);
                let cover = resolve_cover(cover_url, &mut cover_files);
                let source_cell = a
                    .dominant_source
                    .as_deref()
                    .map(html_escape)
                    .unwrap_or_else(|| "-".to_string());
                BarRow {
                    cells: vec![
                        (i + 1).to_string(),
                        html_escape(&a.artist),
                        html_escape(&a.album),
                        source_cell,
                    ],
                    value: a.plays,
                    suffix: format_duration(a.listen_time_secs),
                    cover,
                    raw_cells: true,
                }
            })
            .collect();
        write_bar_table(
            &mut h,
            "Top Albums",
            &["#", "Artist", "Album", "Source", "Plays", ""],
            &album_rows,
        );

        // Top Artists with bar chart and mini cover.
        // Each artist's cover is taken from their most-played album in this
        // specific period.
        let artist_rows: Vec<BarRow> = data
            .top_artists
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let cover = resolve_cover(
                    db::artist_cover(conn, &a.artist, &data.period.name),
                    &mut cover_files,
                );
                BarRow {
                    cells: vec![(i + 1).to_string(), a.artist.clone()],
                    value: a.plays,
                    suffix: format_duration(a.listen_time_secs),
                    cover,
                    raw_cells: false,
                }
            })
            .collect();
        write_bar_table(
            &mut h,
            "Top Artists",
            &["#", "Artist", "Plays", ""],
            &artist_rows,
        );

        // Top Genres with bar chart.
        let genre_rows: Vec<BarRow> = data
            .top_genres
            .iter()
            .enumerate()
            .map(|(i, g)| BarRow {
                cells: vec![(i + 1).to_string(), g.genre.clone()],
                value: g.plays,
                suffix: format_duration(g.listen_time_secs),
                cover: None,
                raw_cells: false,
            })
            .collect();
        write_bar_table(
            &mut h,
            "Top Genres",
            &["#", "Genre", "Plays", ""],
            &genre_rows,
        );

        // Top Tracks with bar chart and mini cover.
        // Each track's cover comes from its album.
        let track_rows: Vec<BarRow> = data
            .top_tracks
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let cover_url = db::album_cache_meta(conn, &t.artist, &t.album)
                    .ok()
                    .flatten()
                    .and_then(|m| m.cover_url);
                let cover = resolve_cover(cover_url, &mut cover_files);
                let source_cell = t
                    .dominant_source
                    .as_deref()
                    .map(html_escape)
                    .unwrap_or_else(|| "-".to_string());
                BarRow {
                    cells: vec![
                        (i + 1).to_string(),
                        html_escape(&t.artist),
                        html_escape(&t.title),
                        source_cell,
                    ],
                    value: t.plays,
                    suffix: format_duration(t.listen_time_secs),
                    cover,
                    raw_cells: true,
                }
            })
            .collect();
        write_bar_table(
            &mut h,
            "Top Tracks",
            &["#", "Artist", "Title", "Source", "Plays", ""],
            &track_rows,
        );

        // Recent scrobbles (only for All Time section to avoid repetition)
        if is_all_time && !data.recent_scrobbles.is_empty() {
            write_plain_table(
                &mut h,
                "Recent Scrobbles",
                &["When", "Artist", "Title", "Album"],
                &data
                    .recent_scrobbles
                    .iter()
                    .map(|s| {
                        vec![
                            fuzzy_time(&s.scrobbled_at),
                            s.artist.clone(),
                            s.title.clone(),
                            s.album.clone(),
                        ]
                    })
                    .collect::<Vec<_>>(),
            );
        }

        h.close("</div>"); // .period-block
    }

    h.close("</div>"); // .wrap
    h.line("</body>");
    h.line("</html>");

    // Deduplicate cover files — the same album may appear in multiple
    // period sections, but we only need to copy each image once.
    cover_files.sort();
    cover_files.dedup();

    HtmlReport {
        html: h.finish(),
        cover_files,
    }
}

// ---------------------------------------------------------------------------
// CSS (kept minified since it's a style block, not meant to be read in HTML)
// ---------------------------------------------------------------------------

const CSS: &str = "\
:root {
  --bg: #11100d; --panel: #1b1813; --panel2: #241f18; --text: #f1eadf;
  --muted: #b8a892; --line: #3b3228; --accent: #f0c06a; --accent2: #d89a3d;
  --bar: #3a2a16; --bar-fill: #d89a3d;
}
* { box-sizing: border-box; }
body {
  margin: 0; color: var(--text);
  font-family: \"JetBrains Mono\", \"Fira Code\", \"IBM Plex Mono\", \"Cascadia Code\", \"SFMono-Regular\", Menlo, Consolas, monospace;
  background:
    radial-gradient(1200px 420px at 10% -10%, #2a231a, transparent),
    radial-gradient(1000px 380px at 100% 0%, #31230f, transparent),
    var(--bg);
}
.wrap { max-width: 1100px; margin: 0 auto; padding: 32px 18px 56px; }
h1, h2, h3 { margin: 0; }
.hero {
  padding: 22px 24px; border: 1px solid var(--line);
  background: linear-gradient(145deg, var(--panel), #15120d);
  border-radius: 14px; box-shadow: 0 14px 40px rgba(0,0,0,.25);
}
.sub { margin-top: 6px; color: var(--muted); font-size: 14px; }
.sub a {
  color: var(--bar-fill);
  text-decoration-color: #a87937;
  text-underline-offset: 2px;
}
.sub a:hover { color: #ffd38e; }
.jump-nav {
  position: sticky; top: 0; z-index: 20;
  margin-top: 12px; padding: 8px;
  display: flex; gap: 8px; overflow-x: auto;
  background: rgba(17, 16, 13, 0.84);
  border: 1px solid var(--line); border-radius: 10px;
  backdrop-filter: blur(8px);
}
.jump-nav a {
  white-space: nowrap; text-decoration: none; color: var(--text);
  font-size: 12px; font-weight: 600;
  padding: 7px 10px; border-radius: 8px;
  border: 1px solid #5b4631;
  background: linear-gradient(140deg, #2b2218, #34281c);
}
.jump-nav a:hover { border-color: #a87937; }
.period-block {
  margin-top: 32px; padding: 20px 24px; border: 1px solid var(--line);
  background: var(--panel); border-radius: 14px;
}
.period-title { font-size: 22px; font-weight: 700; margin-bottom: 4px; }
.period-range { font-size: 13px; color: var(--muted); margin-bottom: 14px; }
.mood {
  margin-bottom: 12px;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
}
.mood-title { font-size: 11px; color: var(--muted); }
.mood-pill {
  font-size: 9px;
  line-height: 1;
  letter-spacing: 0.01em;
  color: #e9c98f;
  padding: 2px 5px;
  border: 1px solid #7e5e2f;
  border-radius: 999px;
}
.kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; }
.kpi { background: var(--panel2); border: 1px solid var(--line); border-radius: 10px; padding: 10px; }
.kpi .k { font-size: 11px; color: var(--muted); }
.kpi .v { margin-top: 4px; font-size: 22px; font-weight: 700; }
section { margin-top: 18px; }
.section-title { font-size: 16px; margin-bottom: 8px; }
.card { background: var(--panel2); border: 1px solid var(--line); border-radius: 10px; overflow-x: auto; overflow-y: hidden; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid var(--line); }
th { font-size: 11px; color: var(--muted); letter-spacing: .02em; text-transform: uppercase; }
tr:last-child td { border-bottom: none; }
.mono { font-family: \"JetBrains Mono\", \"Fira Code\", \"IBM Plex Mono\", \"Cascadia Code\", \"SFMono-Regular\", Menlo, Consolas, monospace; }
.bar-cell { width: 40%; }
.bar-wrap { display: flex; align-items: center; gap: 8px; }
/* bar-track is flex:1 so the bar width% is relative to the available track space,
   not the whole bar-wrap container (which includes the label). Fixes proportionality. */
.bar-track { flex: 1; }
.bar { height: 18px; border-radius: 4px; background: var(--bar-fill); min-width: 2px; }
.bar-label { font-size: 12px; color: var(--muted); white-space: nowrap; }
.grid { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px; }
.album { position: relative; background: var(--panel2); border: 1px solid var(--line); border-radius: 10px; overflow: hidden; }
.cover { width: 100%; aspect-ratio: 1/1; object-fit: cover; background: #0d1116; }
.ph { width: 100%; aspect-ratio: 1/1; display: grid; place-items: center; place-content: center;
       color: #a99984;
       background: linear-gradient(135deg, #17130d, #2c2116); font-size: 12px; }
.pin-dot { position: absolute; top: 5px; right: 5px; width: 22px; height: 22px;
           border-radius: 50%; background: rgba(0,0,0,0.45); border: none;
           cursor: pointer; font-size: 12px; line-height: 22px; text-align: center;
           opacity: 0; transition: opacity 0.15s; z-index: 1; }
.album:hover .pin-dot { opacity: 1; }
.pin-dot:hover { background: rgba(0,0,0,0.75); }
.meta { padding: 8px; }
.t { font-size: 13px; font-weight: 700; line-height: 1.2; }
.a { margin-top: 3px; font-size: 11px; color: var(--muted); }
.genre { margin-top: 5px; display: flex; gap: 5px; flex-wrap: wrap; }
.pill {
  display: inline-block;
  font-size: 9px;
  line-height: 1;
  letter-spacing: 0.01em;
  color: #e9c98f;
  padding: 2px 5px;
  border: 1px solid #7e5e2f;
  border-radius: 999px;
}
.mini-cover { width: 36px; height: 36px; border-radius: 4px; object-fit: cover; vertical-align: middle; }
.mini-ph { display: inline-block; width: 36px; height: 36px; border-radius: 4px;
           background: linear-gradient(135deg, #17130d, #2c2116); vertical-align: middle; }
.nowrap { white-space: nowrap; }
.note { margin-top: 18px; color: var(--muted); font-size: 12px; }
@media (max-width: 980px) { .grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
@media (max-width: 700px) {
  body { font-size: 13px; }
  .wrap { padding: 16px 12px 36px; }
  .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .jump-nav { margin-top: 10px; padding: 7px; }
  th, td { padding: 7px 8px; }
  .source-col { display: none; }
  .bar-cell { width: 32%; }
  .bar-wrap { gap: 4px; }
  .bar-label { font-size: 11px; }
  /* On mobile show rank + duration only; drop the raw play-count and the bar graphic.
     The .bar-label (duration text) remains visible inside .bar-cell. */
  .play-count { display: none; }
  .bar { display: none; }
}
";

/// Source colour palette — muted tints that work as card backgrounds on the
/// dark theme. Assigned in order to sources ranked by all-time scrobble count,
/// so the most-listened source always gets the first colour.
///
/// Format: (background tint, border accent)
const SOURCE_PALETTE: &[(&str, &str)] = &[
    ("rgba(240,192,106,0.22)", "rgba(240,192,106,0.60)"), // amber  (warm)
    ("rgba(100,160,240,0.22)", "rgba(100,160,240,0.60)"), // blue   (cool)
    ("rgba(140,210,130,0.22)", "rgba(140,210,130,0.60)"), // green
    ("rgba(200,130,220,0.22)", "rgba(200,130,220,0.60)"), // purple
    ("rgba(220,130,130,0.22)", "rgba(220,130,130,0.60)"), // red
];

/// Look up the card background and border colours for a source name, given
/// the ordered list of all-time sources (most played first).
fn source_colours<'a>(source: &str, ordered_sources: &[String]) -> Option<(&'a str, &'a str)> {
    ordered_sources
        .iter()
        .position(|s| s == source)
        .and_then(|i| SOURCE_PALETTE.get(i % SOURCE_PALETTE.len()))
        .copied()
}

/// Desktop column count for the album cover grid.
const ALBUM_GRID_COLUMNS: i64 = 6;

/// Round a user-provided limit up to the nearest full grid row.
///
/// Examples with 6-column rows:
/// - limit 20 -> 24
/// - limit 18 -> 18
/// - limit 1  -> 6
fn album_cover_grid_limit(limit: i64) -> i64 {
    let columns = ALBUM_GRID_COLUMNS;
    let safe = limit.max(1);
    ((safe + columns - 1) / columns) * columns
}

// ---------------------------------------------------------------------------
// HtmlWriter — helper for producing indented HTML output
// ---------------------------------------------------------------------------

/// A simple helper that tracks indentation depth so the generated HTML is
/// human-readable.  Each `open()` increases indent, each `close()` decreases.
struct HtmlWriter {
    buf: String,
    depth: usize,
}

impl HtmlWriter {
    fn new() -> Self {
        Self {
            buf: String::with_capacity(128 * 1024),
            depth: 0,
        }
    }

    /// Write a self-contained line at the current indent level.
    fn line(&mut self, s: &str) {
        self.write_indent();
        self.buf.push_str(s);
        self.buf.push('\n');
    }

    /// Write a formatted line at the current indent level.
    fn linef(&mut self, args: std::fmt::Arguments<'_>) {
        self.write_indent();
        let _ = write!(self.buf, "{}", args);
        self.buf.push('\n');
    }

    /// Write raw text (no indent, no newline added — the text provides its own).
    fn raw(&mut self, s: &str) {
        self.buf.push_str(s);
    }

    /// Write an opening tag and increase indent for children.
    fn open(&mut self, tag: &str) {
        self.line(tag);
        self.indent();
    }

    /// Decrease indent and write a closing tag.
    fn close(&mut self, tag: &str) {
        self.dedent();
        self.line(tag);
    }

    /// Write a blank line for visual separation.
    fn blank(&mut self) {
        self.buf.push('\n');
    }

    fn indent(&mut self) {
        self.depth += 1;
    }

    fn dedent(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    fn write_indent(&mut self) {
        for _ in 0..self.depth {
            self.buf.push_str("  ");
        }
    }

    /// Consume the writer and return the finished HTML string.
    fn finish(self) -> String {
        self.buf
    }
}

// ---------------------------------------------------------------------------
// HTML content helpers
// ---------------------------------------------------------------------------

/// Convert an absolute cover file path (from the DB) into a relative path
/// for use in HTML (`covers/<filename>`), and track the source file for
/// copying into the output directory. Returns `None` if the path is missing
/// or the file doesn't exist on disk.
fn resolve_cover(
    abs_path: Option<String>,
    cover_files: &mut Vec<std::path::PathBuf>,
) -> Option<String> {
    let src = abs_path?;
    let src_path = std::path::Path::new(&src);
    let filename = src_path.file_name()?;
    if src_path.exists() {
        cover_files.push(src_path.to_path_buf());
    }
    Some(format!("covers/{}", filename.to_string_lossy()))
}

fn write_kpi(h: &mut HtmlWriter, key: &str, val: &str) {
    h.open("<div class=\"kpi\">");
    h.linef(format_args!("<div class=\"k\">{}</div>", html_escape(key)));
    h.linef(format_args!(
        "<div class=\"v mono\">{}</div>",
        html_escape(val)
    ));
    h.close("</div>");
}

/// A row in a bar-chart table. `cells` are the text columns, `value` is
/// the numeric value used for the bar width, `suffix` is shown after the
/// bar (e.g., listen time), and `cover` is an optional relative path to
/// a mini cover image shown at the start of the row.
struct BarRow {
    cells: Vec<String>,
    value: i64,
    suffix: String,
    /// Relative path to a cover image (e.g., "covers/UUID.jpg"), or None.
    cover: Option<String>,
    /// When true, `cells` contain pre-escaped HTML and must not be escaped again.
    raw_cells: bool,
}

/// Render a table where the last column is a horizontal bar chart.
fn write_bar_table(h: &mut HtmlWriter, title: &str, headers: &[&str], rows: &[BarRow]) {
    if rows.is_empty() {
        return;
    }
    let max_val = rows.iter().map(|r| r.value).max().unwrap_or(1).max(1);

    h.blank();
    h.open("<section>");
    h.linef(format_args!(
        "<h3 class=\"section-title\">{}</h3>",
        html_escape(title)
    ));
    h.open("<div class=\"card\">");
    h.open("<table>");

    // Check if any row has a cover image — if so, add a cover column.
    let has_covers = rows.iter().any(|r| r.cover.is_some());

    // Header
    h.open("<thead>");
    h.open("<tr>");
    if has_covers {
        h.line("<th></th>"); // empty header for the cover column
    }
    // The second-to-last header is always the play-count column; mark it so
    // it can be hidden together with the td.play-count cells on mobile.
    let play_count_idx = headers.len().saturating_sub(2);
    for (i, hdr) in headers.iter().enumerate() {
        if *hdr == "Source" {
            h.linef(format_args!(
                "<th class=\"source-col\">{}</th>",
                html_escape(hdr)
            ));
        } else if i == play_count_idx {
            h.linef(format_args!(
                "<th class=\"play-count\">{}</th>",
                html_escape(hdr)
            ));
        } else {
            h.linef(format_args!("<th>{}</th>", html_escape(hdr)));
        }
    }
    h.close("</tr>");
    h.close("</thead>");

    // Body
    h.open("<tbody>");
    for row in rows {
        h.open("<tr>");
        if has_covers {
            if let Some(ref src) = row.cover {
                h.linef(format_args!(
                    "<td><img class=\"mini-cover\" src=\"{}\" alt=\"\"></td>",
                    html_attr_escape(src)
                ));
            } else {
                h.line("<td><span class=\"mini-ph\"></span></td>");
            }
        }
        for (i, cell) in row.cells.iter().enumerate() {
            if headers.get(i).is_some_and(|h| *h == "Source") {
                if row.raw_cells {
                    h.linef(format_args!("<td class=\"source-col\">{}</td>", cell));
                } else {
                    h.linef(format_args!(
                        "<td class=\"source-col\">{}</td>",
                        html_escape(cell)
                    ));
                }
            } else if row.raw_cells {
                h.linef(format_args!("<td>{}</td>", cell));
            } else {
                h.linef(format_args!("<td>{}</td>", html_escape(cell)));
            }
        }
        let pct = (row.value as f64 / max_val as f64 * 100.0).round() as u32;
        // play-count is hidden on mobile via CSS; bar-label (duration) stays visible
        h.linef(format_args!("<td class=\"play-count\">{}</td>", row.value));
        h.linef(format_args!(
            "<td class=\"bar-cell\">\n\
             {s}  <div class=\"bar-wrap\">\n\
             {s}    <div class=\"bar-track\">\n\
             {s}      <div class=\"bar\" style=\"width:{pct}%\"></div>\n\
             {s}    </div>\n\
             {s}    <span class=\"bar-label\">{suffix}</span>\n\
             {s}  </div>\n\
             {s}</td>",
            s = "  ".repeat(h.depth),
            pct = pct,
            suffix = html_escape(&row.suffix)
        ));
        h.close("</tr>");
    }
    h.close("</tbody>");

    h.close("</table>");
    h.close("</div>"); // .card
    h.close("</section>");
}

/// Render a plain table (no bar chart).
fn write_plain_table(h: &mut HtmlWriter, title: &str, headers: &[&str], rows: &[Vec<String>]) {
    if rows.is_empty() {
        return;
    }
    h.blank();
    h.open("<section>");
    h.linef(format_args!(
        "<h3 class=\"section-title\">{}</h3>",
        html_escape(title)
    ));
    h.open("<div class=\"card\">");
    h.open("<table>");

    h.open("<thead>");
    h.open("<tr>");
    for hdr in headers {
        h.linef(format_args!("<th>{}</th>", html_escape(hdr)));
    }
    h.close("</tr>");
    h.close("</thead>");

    h.open("<tbody>");
    for row in rows {
        h.open("<tr>");
        for (i, col) in row.iter().enumerate() {
            if i == 0 {
                h.linef(format_args!(
                    "<td class=\"nowrap\">{}</td>",
                    html_escape(col)
                ));
            } else {
                h.linef(format_args!("<td>{}</td>", html_escape(col)));
            }
        }
        h.close("</tr>");
    }
    h.close("</tbody>");

    h.close("</table>");
    h.close("</div>");
    h.close("</section>");
}

/// Build the `pin-album` shell command for the given artist and album.
/// Uses the known MBID when available, otherwise substitutes `MBID_HERE`.
/// Double-quotes in string values are escaped so the command is safe to paste.
fn pin_album_cmd(artist: &str, album: &str, mbid: Option<&str>) -> String {
    let a = artist.replace('"', "\\\"");
    let b = album.replace('"', "\\\"");
    let m = mbid.unwrap_or("MBID_HERE");
    format!("scrbblr pin-album --artist \"{a}\" --album \"{b}\" --mbid \"{m}\"")
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn html_attr_escape(s: &str) -> String {
    html_escape(s).replace('"', "&quot;")
}

/// Split a comma-separated genre list and return the top `max_labels` labels,
/// applying the same deprioritisation rule as `top_genres`: multi-word genres
/// rank before single-word broad descriptors ("rock", "electronic", etc.).
fn top_genre_labels(genre_csv: &str, max_labels: usize) -> Vec<String> {
    let mut labels: Vec<String> = genre_csv
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // Stable sort so that multi-word genres always precede single-word ones,
    // preserving the original MusicBrainz order within each group.
    labels.sort_by_key(|g| db::is_deprioritised_genre(g));

    labels.truncate(max_labels);
    labels
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // =======================================================================
    // Duration formatting
    // =======================================================================

    #[test]
    fn test_format_duration_seconds() {
        assert_eq!(format_duration(45), "45s");
    }

    #[test]
    fn test_format_duration_minutes() {
        assert_eq!(format_duration(120), "2m");
        assert_eq!(format_duration(300), "5m");
    }

    #[test]
    fn test_format_duration_hours() {
        assert_eq!(format_duration(3600), "1h");
        assert_eq!(format_duration(3660), "1h 01m");
        assert_eq!(format_duration(7380), "2h 03m");
    }

    #[test]
    fn test_format_duration_zero() {
        assert_eq!(format_duration(0), "0s");
    }

    #[test]
    fn test_format_play_count() {
        assert_eq!(format_play_count(0), "0 plays");
        assert_eq!(format_play_count(1), "1 play");
        assert_eq!(format_play_count(2), "2 plays");
    }

    #[test]
    fn test_truncate_cell() {
        assert_eq!(truncate_cell("abcdef", 6), "abcdef");
        assert_eq!(truncate_cell("abcdef", 5), "ab...");
        assert_eq!(truncate_cell("abcdef", 3), "abc");
    }

    #[test]
    fn test_album_cover_grid_limit_rounds_to_full_rows() {
        assert_eq!(album_cover_grid_limit(20), 24);
        assert_eq!(album_cover_grid_limit(18), 18);
        assert_eq!(album_cover_grid_limit(1), 6);
        assert_eq!(album_cover_grid_limit(0), 6);
    }

    #[test]
    fn test_top_genre_labels_caps_to_three() {
        // "alternative rock" and "industrial" are multi-word → rank first.
        // "electronic" and "darkwave" are single-word → deprioritised.
        // With limit 3 we get both multi-word genres + one single-word.
        let labels = top_genre_labels("alternative rock, electronic, industrial, darkwave", 3);
        assert_eq!(
            labels,
            vec![
                "alternative rock".to_string(),
                "electronic".to_string(),
                "industrial".to_string(),
            ]
        );
    }

    #[test]
    fn test_mood_labels_caps_to_three() {
        let genres = vec![
            db::TopGenre {
                genre: "trip hop".to_string(),
                plays: 5,
                listen_time_secs: 1000,
            },
            db::TopGenre {
                genre: "electronic".to_string(),
                plays: 4,
                listen_time_secs: 900,
            },
            db::TopGenre {
                genre: "ambient".to_string(),
                plays: 3,
                listen_time_secs: 800,
            },
            db::TopGenre {
                genre: "downtempo".to_string(),
                plays: 2,
                listen_time_secs: 700,
            },
        ];
        assert_eq!(
            mood_labels(&genres, 3),
            vec![
                "trip hop".to_string(),
                "electronic".to_string(),
                "ambient".to_string(),
            ]
        );
    }

    // =======================================================================
    // Report gathering
    // =======================================================================

    #[test]
    fn test_gather_report_empty_db() {
        // An empty database should produce a valid report with all zeros.
        let conn = db::open_memory_db().unwrap();
        let report = gather_report(&conn, "all", 10).unwrap();
        assert_eq!(report.overview.total_scrobbles, 0);
        assert!(report.top_artists.is_empty());
        assert!(report.top_genres.is_empty());
        assert!(report.recent_scrobbles.is_empty());
    }

    #[test]
    fn test_gather_report_with_data() {
        // Populate a test database and verify the report contains expected data.
        let conn = db::open_memory_db().unwrap();
        let scrobbles = vec![
            db::NewScrobble {
                artist: "††† (Crosses)".into(),
                album: "††† (Crosses)".into(),
                title: "This Is a Trick".into(),
                track_duration_secs: Some(186),
                played_duration_secs: 186,
                scrobbled_at: chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
                source: "test".into(),
            },
            db::NewScrobble {
                artist: "††† (Crosses)".into(),
                album: "††† (Crosses)".into(),
                title: "Telepathy".into(),
                track_duration_secs: Some(215),
                played_duration_secs: 200,
                scrobbled_at: chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
                source: "test".into(),
            },
            db::NewScrobble {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                title: "Digital Bath".into(),
                track_duration_secs: Some(291),
                played_duration_secs: 291,
                scrobbled_at: chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
                source: "test".into(),
            },
        ];
        for s in &scrobbles {
            db::insert_scrobble(&conn, s).unwrap();
        }

        let report = gather_report(&conn, "all", 10).unwrap();

        // Overview should reflect all 3 scrobbles.
        assert_eq!(report.overview.total_scrobbles, 3);
        // Two distinct artists: ††† (Crosses) and Deftones.
        assert_eq!(report.top_artists.len(), 2);
        // ††† (Crosses) has 2 plays, so it should be ranked first.
        assert_eq!(report.top_artists[0].artist, "††† (Crosses)");
        // All 3 tracks are unique.
        assert_eq!(report.top_tracks.len(), 3);
        // No cached genres yet in this test fixture.
        assert!(report.top_genres.is_empty());
        // Recent scrobbles should contain all 3.
        assert_eq!(report.recent_scrobbles.len(), 3);
        // Period should be "all" with no date range.
        assert_eq!(report.period.name, "all");
    }

    #[test]
    fn test_json_serialization() {
        // Verify that the report can be serialized to JSON and contains
        // expected fields.
        let conn = db::open_memory_db().unwrap();
        db::insert_scrobble(
            &conn,
            &db::NewScrobble {
                artist: "Test".into(),
                album: "Album".into(),
                title: "Song".into(),
                track_duration_secs: Some(180),
                played_duration_secs: 180,
                scrobbled_at: chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
                source: "test".into(),
            },
        )
        .unwrap();

        let report = gather_report(&conn, "all", 10).unwrap();
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"total_scrobbles\":1"));
        assert!(json.contains("\"artist\":\"Test\""));
    }

    #[test]
    fn test_html_render_contains_sections() {
        let conn = db::open_memory_db().unwrap();
        db::insert_scrobble(
            &conn,
            &db::NewScrobble {
                artist: "Test Artist".into(),
                album: "Test Album".into(),
                title: "Test Song".into(),
                track_duration_secs: Some(180),
                played_duration_secs: 170,
                scrobbled_at: chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
                source: "test".into(),
            },
        )
        .unwrap();
        db::upsert_album_cache(
            &conn,
            &db::AlbumCacheEntry {
                artist: "Test Artist".into(),
                album: "Test Album".into(),
                musicbrainz_id: None,
                cover_url: None,
                genre: Some("trip hop".into()),
                fetched_at: chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
            },
        )
        .unwrap();

        let report = render_html_report(&conn, 10, 25);
        assert!(report.html.contains("<html"));
        // Multi-period report should contain All Time section.
        assert!(report.html.contains("All Time"));
        assert!(report.html.contains("jump-nav"));
        assert!(report.html.contains("#period-all"));
        assert!(report.html.contains("Top Albums"));
        assert!(report.html.contains("Top Artists"));
        assert!(report.html.contains("Top Genres"));
        assert!(report.html.contains("Mood of"));
        assert!(!report.html.contains("Mood of All Time"));
        assert!(report.html.contains("Recent Scrobbles"));
        assert!(report.html.contains("Test Artist"));
        // Bar chart elements should be present.
        assert!(report.html.contains("bar-wrap"));
        // No covers cached in test DB, so no cover files to copy.
        assert!(report.cover_files.is_empty());
    }

    // =======================================================================
    // Fuzzy time labels
    // =======================================================================

    #[test]
    fn test_fuzzy_time_recent() {
        let ten_min_ago = (chrono::Local::now().naive_local() - chrono::Duration::minutes(10))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(fuzzy_time(&ten_min_ago), "Moments ago");
    }

    #[test]
    fn test_fuzzy_time_within_hour() {
        let fifty_min_ago = (chrono::Local::now().naive_local() - chrono::Duration::minutes(50))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(fuzzy_time(&fifty_min_ago), "Within hour");
    }

    #[test]
    fn test_fuzzy_time_hours() {
        let two_hours_ago = (chrono::Local::now().naive_local() - chrono::Duration::hours(2))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(fuzzy_time(&two_hours_ago), "A while ago");
    }

    #[test]
    fn test_fuzzy_time_yesterday() {
        // Use calendar noon of yesterday so the result is unambiguously
        // "Yesterday" regardless of the time of day the test runs.
        let yesterday_noon = (chrono::Local::now().date_naive() - chrono::Duration::days(1))
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(fuzzy_time(&yesterday_noon), "Yesterday");
    }

    #[test]
    fn test_fuzzy_time_calendar_boundary() {
        // A track scrobbled at 20:00 yesterday must say "Yesterday", not "Today",
        // even when fewer than 12 hours have elapsed (e.g. run just after midnight).
        // This is the calendar-date boundary bug the function was written to fix.
        let yesterday_evening = (chrono::Local::now().date_naive() - chrono::Duration::days(1))
            .and_hms_opt(20, 0, 0)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(fuzzy_time(&yesterday_evening), "Yesterday");
    }

    #[test]
    fn test_fuzzy_time_old() {
        let long_ago = (chrono::Local::now().naive_local() - chrono::Duration::days(400))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(fuzzy_time(&long_ago), "Over year");
    }

    #[test]
    fn test_fuzzy_time_invalid() {
        assert_eq!(fuzzy_time("not-a-date"), "Some time");
    }

    #[test]
    fn test_terse_time_labels() {
        let mins_ago = |m: i64| {
            (chrono::Local::now().naive_local() - chrono::Duration::minutes(m))
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string()
        };
        let hours_ago = |h: i64| {
            (chrono::Local::now().naive_local() - chrono::Duration::hours(h))
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string()
        };
        let days_ago = |d: i64| {
            (chrono::Local::now().naive_local() - chrono::Duration::days(d))
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string()
        };

        assert_eq!(terse_time(&mins_ago(5)), "5m");
        assert_eq!(terse_time(&mins_ago(45)), "45m");
        assert_eq!(terse_time(&hours_ago(2)), "2h");
        assert_eq!(terse_time(&days_ago(3)), "3d");
        assert_eq!(terse_time(&days_ago(20)), "2w");
        assert_eq!(terse_time(&days_ago(400)), "13mo");
        assert_eq!(terse_time("not-a-date"), "?");
    }
}
