//! Database module for scrbblr.
//!
//! This module handles all SQLite interactions: schema creation, inserting
//! scrobble records, and querying data for reports. The database is stored
//! as a single SQLite file (by default at `~/.local/share/scrbblr/scrobbles.db`).
//!
//! ## Tables
//!
//! - **`scrobbles`** — one row per scrobbled track. Stores artist, album, title,
//!   the track's full duration, the actual time spent listening (paused time
//!   excluded), and an ISO 8601 timestamp. This is the core data that the
//!   `watch` command writes and the `report` command reads.
//!
//! - **`album_cache`** — one row per unique (artist, album) pair. Populated by
//!   the `enrich` command (or automatically when generating an HTML report).
//!   Stores the MusicBrainz release ID, a local file path to the downloaded
//!   cover art image, and genre/tag strings from MusicBrainz. Albums with
//!   `cover_url = NULL` or `genre = NULL` are considered incomplete and can be
//!   re-tried on enrichment runs after a cooldown period.
//!
//! ## Query patterns
//!
//! All report queries accept a period string (`"today"`, `"week"`, `"month"`,
//! `"year"`, `"all"`). The `period_range()` function converts this to an
//! ISO 8601 date range, and `where_clause()` generates the corresponding SQL
//! `WHERE` fragment. This keeps period logic in one place rather than
//! duplicating it across every query function.

use chrono::{Datelike, NaiveDateTime};
use rusqlite::{Connection, Result, params};
use serde::Serialize;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// A scrobble record as stored in the database.
/// This is the "full" version with the auto-generated `id`, used when reading
/// data back from the DB (e.g., for reports or JSON export).
#[derive(Debug, Clone, Serialize)]
pub struct Scrobble {
    pub id: i64,
    pub artist: String,
    pub album: String,
    pub title: String,
    /// The track's full duration in seconds, as reported by MPRIS.
    /// May be `None` if the player didn't provide duration info.
    pub track_duration_secs: Option<i64>,
    /// How long the user actually listened (in seconds). Only time spent
    /// in the "Playing" state counts — paused time is excluded.
    pub played_duration_secs: i64,
    /// ISO 8601 timestamp of when the scrobble was recorded.
    pub scrobbled_at: String,
}

/// Data required to insert a new scrobble. Same fields as `Scrobble` but
/// without the `id`, which is auto-assigned by SQLite.
#[derive(Debug, Clone)]
pub struct NewScrobble {
    pub artist: String,
    pub album: String,
    pub title: String,
    pub track_duration_secs: Option<i64>,
    pub played_duration_secs: i64,
    pub scrobbled_at: String,
    /// The source that produced this scrobble (e.g. "MPD", "Qobuz").
    pub source: String,
}

/// Aggregate overview statistics for a given time period.
#[derive(Debug, Clone, Serialize)]
pub struct Overview {
    pub total_scrobbles: i64,
    /// Sum of `played_duration_secs` across all scrobbles in the period.
    pub total_listen_time_secs: i64,
    pub unique_artists: i64,
    pub unique_albums: i64,
    /// Unique tracks, identified by the (artist, title) pair.
    pub unique_tracks: i64,
}

/// A row in the "top artists" ranking.
#[derive(Debug, Clone, Serialize)]
pub struct TopArtist {
    pub artist: String,
    /// Number of scrobbles for this artist.
    pub plays: i64,
    /// Total seconds spent listening to this artist.
    pub listen_time_secs: i64,
}

/// A row in the "top albums" ranking, grouped by (artist, album).
#[derive(Debug, Clone, Serialize)]
pub struct TopAlbum {
    pub artist: String,
    pub album: String,
    pub plays: i64,
    pub listen_time_secs: i64,
    /// The source (player) responsible for the most scrobbles of this album
    /// in the queried period. `None` for historical rows without a source.
    pub dominant_source: Option<String>,
}

/// A row in the "top tracks" ranking, grouped by (artist, title).
/// Includes the album name so we can look up the cover image.
#[derive(Debug, Clone, Serialize)]
pub struct TopTrack {
    pub artist: String,
    pub album: String,
    pub title: String,
    pub plays: i64,
    pub listen_time_secs: i64,
    pub dominant_source: Option<String>,
}

/// A row in the "top genres" ranking.
///
/// Note: a single scrobble may contribute to multiple genres if its album has
/// multiple comma-separated genres in `album_cache.genre`.
#[derive(Debug, Clone, Serialize)]
pub struct TopGenre {
    pub genre: String,
    pub plays: i64,
    pub listen_time_secs: i64,
}

/// A row in the "top sources" breakdown, showing which player or source
/// contributed most scrobbles (e.g. "MPD", "Qobuz").
#[derive(Debug, Clone, Serialize)]
pub struct TopSource {
    pub source: String,
    pub scrobbles: i64,
    pub listen_time_secs: i64,
}

// ---------------------------------------------------------------------------
// Database initialization
// ---------------------------------------------------------------------------

/// Open (or create) the database at the given file path and ensure the
/// schema exists. This is safe to call on an already-initialized DB because
/// all CREATE statements use `IF NOT EXISTS`.
pub fn open_db(path: &str) -> Result<Connection> {
    let conn = Connection::open(path)?;
    init_schema(&conn)?;
    Ok(conn)
}

/// Open an in-memory SQLite database with the schema applied.
/// Used exclusively by unit tests to avoid touching the filesystem.
#[cfg(test)]
pub fn open_memory_db() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    init_schema(&conn)?;
    Ok(conn)
}

/// Create all tables and indexes if they don't already exist.
///
/// ## Tables
///
/// ### `scrobbles` — one row per scrobbled track
///   - `id`                   — auto-incrementing primary key
///   - `artist`               — artist name (required)
///   - `album`                — album name (defaults to empty string)
///   - `title`                — track title (required)
///   - `track_duration_secs`  — full track length in seconds (nullable)
///   - `played_duration_secs` — actual listening time in seconds (required)
///   - `scrobbled_at`         — ISO 8601 timestamp string (required)
///
/// ### `album_cache` — cached metadata for album art and genres
///
/// Populated by the `enrich` command (or automatically when `report --html`
/// is used). Stores MusicBrainz IDs, local cover art file paths, and genre
/// info to avoid repeated API lookups. Keyed by the (artist, album) pair.
/// Albums with `cover_url = NULL` are considered incomplete and will be
/// re-tried on the next enrichment run.
///
///   - `id`             — auto-incrementing primary key
///   - `artist`         — artist name (matches scrobbles.artist)
///   - `album`          — album name (matches scrobbles.album)
///   - `musicbrainz_id` — MusicBrainz release MBID (UUID string, nullable)
///   - `cover_url`      — Cover Art Archive URL or local file path (nullable)
///   - `genre`          — comma-separated genre tags from MusicBrainz (nullable)
///   - `fetched_at`     — ISO 8601 timestamp of when the lookup was performed
///   - UNIQUE(artist, album) — prevents duplicate cache entries
///
/// ## Indexes
///   - `idx_scrobbled_at` — for efficient time-range queries in reports
///   - `idx_artist`       — for efficient GROUP BY artist queries
fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS scrobbles (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            artist               TEXT NOT NULL,
            album                TEXT NOT NULL DEFAULT '',
            title                TEXT NOT NULL,
            track_duration_secs  INTEGER,
            played_duration_secs INTEGER NOT NULL,
            scrobbled_at         TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_scrobbled_at ON scrobbles(scrobbled_at);
        CREATE INDEX IF NOT EXISTS idx_artist ON scrobbles(artist);",
    )?;
    // Add the source column to existing databases. SQLite returns an error if
    // the column already exists, which we silently ignore for idempotency.
    conn.execute("ALTER TABLE scrobbles ADD COLUMN source TEXT", [])
        .ok();
    conn.execute_batch(
        "
        -- Cache table for album metadata (cover art, genres) from MusicBrainz.
        -- Populated by the 'enrich' command or automatically when generating
        -- HTML reports. Rows with cover_url = NULL are re-tried on next run.
        CREATE TABLE IF NOT EXISTS album_cache (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            artist          TEXT NOT NULL,
            album           TEXT NOT NULL,
            musicbrainz_id  TEXT,
            cover_url       TEXT,
            genre           TEXT,
            fetched_at      TEXT,
            UNIQUE(artist, album)
        );",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

/// Insert a new scrobble record and return its auto-generated row ID.
pub fn insert_scrobble(conn: &Connection, s: &NewScrobble) -> Result<i64> {
    conn.execute(
        "INSERT INTO scrobbles (artist, album, title, track_duration_secs, played_duration_secs, scrobbled_at, source)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            s.artist,
            s.album,
            s.title,
            s.track_duration_secs,
            s.played_duration_secs,
            s.scrobbled_at,
            s.source,
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Return the newest scrobble timestamp (`scrobbled_at`) in the database.
///
/// Returns `Ok(None)` when there are no scrobbles yet.
pub fn latest_scrobble_at(conn: &Connection) -> Result<Option<String>> {
    let mut stmt =
        conn.prepare("SELECT scrobbled_at FROM scrobbles ORDER BY scrobbled_at DESC LIMIT 1")?;
    let mut rows = stmt.query([])?;
    if let Some(row) = rows.next()? {
        let ts: String = row.get(0)?;
        Ok(Some(ts))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Period helpers
// ---------------------------------------------------------------------------

/// Compute the ISO 8601 date range `(from, to)` for a named period.
///
/// Supported periods:
///   - `"today"` — from midnight today to now
///   - `"week"`  — from Monday 00:00 of the current week to now
///   - `"month"` — from the 1st of the current month to now
///   - `"year"`  — from January 1st of the current year to now
///   - `"all"`   — returns `None` (no filtering)
///
/// The `from` and `to` strings are formatted as `YYYY-MM-DDTHH:MM:SS` so
/// they can be compared directly against the `scrobbled_at` column using
/// SQLite's string comparison (ISO 8601 sorts lexicographically).
pub fn period_range(period: &str) -> Option<(String, String)> {
    let now = chrono::Local::now().naive_local();
    let today_start = now.date().and_hms_opt(0, 0, 0).unwrap();

    let from = match period {
        "today" => today_start,
        "week" => {
            // Calculate how many days back from today to reach Monday (0 = Mon, 6 = Sun).
            let weekday = now.date().weekday().num_days_from_monday();
            today_start - chrono::Duration::days(weekday as i64)
        }
        "month" => {
            // First day of the current month at midnight.
            let d = now.date();
            NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(d.year(), d.month(), 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            )
        }
        "year" => {
            // January 1st of the current year at midnight.
            let d = now.date();
            NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(d.year(), 1, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            )
        }
        // "all": no date filtering.
        "all" => return None,
        // Any unrecognized period: no date filtering (validated earlier in CLI).
        _ => return None,
    };

    let from_str = from.format("%Y-%m-%dT%H:%M:%S").to_string();
    let to_str = now.format("%Y-%m-%dT%H:%M:%S").to_string();
    Some((from_str, to_str))
}

/// Build a SQL WHERE clause fragment for time-period filtering.
///
/// Returns a tuple of:
///   - The clause string (empty if no filtering, or " WHERE scrobbled_at BETWEEN ?1 AND ?2")
///   - A vector of bind parameter values (empty if no filtering, or [from, to])
///
/// This is used by all query functions below to optionally restrict results
/// to a specific time period.
fn where_clause(period: &str) -> (String, Vec<String>) {
    match period_range(period) {
        Some((from, to)) => (
            " WHERE scrobbled_at BETWEEN ?1 AND ?2".to_string(),
            vec![from, to],
        ),
        None => (String::new(), vec![]),
    }
}

// ---------------------------------------------------------------------------
// Query functions
//
// Each function takes a period string and constructs a SQL query with an
// optional WHERE clause for time filtering. The `mapper` closure is defined
// once and passed to both branches of the if/else (with/without params) to
// avoid Rust's "each closure has a unique type" issue.
// ---------------------------------------------------------------------------

/// Get aggregate overview statistics for the given period.
///
/// Unique tracks are counted by concatenating artist + null byte + title,
/// so that "Artist A - Song X" and "Artist B - Song X" are counted separately.
pub fn overview(conn: &Connection, period: &str) -> Result<Overview> {
    let (whr, p) = where_clause(period);
    let sql = format!(
        "SELECT
            COUNT(*),
            COALESCE(SUM(played_duration_secs), 0),
            COUNT(DISTINCT artist),
            COUNT(DISTINCT album),
            COUNT(DISTINCT artist || '\\0' || title)
         FROM scrobbles{}",
        whr
    );

    let mut stmt = conn.prepare(&sql)?;
    let row = if p.is_empty() {
        stmt.query_row([], |row| {
            Ok(Overview {
                total_scrobbles: row.get(0)?,
                total_listen_time_secs: row.get(1)?,
                unique_artists: row.get(2)?,
                unique_albums: row.get(3)?,
                unique_tracks: row.get(4)?,
            })
        })
    } else {
        stmt.query_row(params![p[0], p[1]], |row| {
            Ok(Overview {
                total_scrobbles: row.get(0)?,
                total_listen_time_secs: row.get(1)?,
                unique_artists: row.get(2)?,
                unique_albums: row.get(3)?,
                unique_tracks: row.get(4)?,
            })
        })
    }?;
    Ok(row)
}

/// Get the top N artists by play count for the given period,
/// ordered by number of scrobbles descending.
///
/// Ties are broken by total listened time (descending), then artist name for
/// stable output ordering.
pub fn top_artists(conn: &Connection, period: &str, limit: i64) -> Result<Vec<TopArtist>> {
    let (whr, p) = where_clause(period);
    let sql = format!(
        "SELECT artist, COUNT(*) as plays, COALESCE(SUM(played_duration_secs), 0) as listen_time
         FROM scrobbles{}
         GROUP BY artist
         ORDER BY plays DESC, listen_time DESC, artist ASC
         LIMIT {}",
        whr, limit
    );

    let mut stmt = conn.prepare(&sql)?;
    let mapper = |row: &rusqlite::Row| {
        Ok(TopArtist {
            artist: row.get(0)?,
            plays: row.get(1)?,
            listen_time_secs: row.get(2)?,
        })
    };
    if p.is_empty() {
        stmt.query_map([], mapper)?.collect()
    } else {
        stmt.query_map(params![p[0], p[1]], mapper)?.collect()
    }
}

/// Get the top N albums by play count for the given period,
/// grouped by MusicBrainz release ID when known (falling back to the
/// (artist, album) pair) and ordered by scrobble count descending.
///
/// Using the MBID as the group key means tracks from the same physical
/// album that were scrobbled under different artist tags (common in
/// classical recordings where individual tracks credit a soloist or
/// ensemble while others credit the full orchestra or choir) are
/// correctly counted as a single album. The artist returned is the one
/// that has an album_cache entry (i.e. the one that was enriched), so
/// cover art lookups in the report continue to work.
///
/// Ties are broken by total listened time (descending), then artist + album
/// names for stable output ordering.
pub fn top_albums(conn: &Connection, period: &str, limit: i64) -> Result<Vec<TopAlbum>> {
    let (whr, p) = where_clause(period);
    // Group by (album, MBID) rather than (artist, album) so that scrobbles
    // tagged with different artist names for the same physical release are
    // counted together.
    //
    // For each row the group key resolves as:
    //   1. The MBID from the direct (artist, album) album_cache entry, or
    //   2. The MBID from any album_cache entry sharing the same album name
    //      (catches variants tagged with a performer vs. composer), or
    //   3. The composite "artist::album" string (no MBID found — treated as
    //      a unique album, same as before).
    //
    // The displayed artist is similarly resolved: the enriched (cached)
    // artist takes priority so that album_cache_meta() can find the cover.
    // The outer query uses `whr` which starts with " WHERE ...".
    // The dominant_source subquery already has its own WHERE, so we need an
    // AND-form of the same filter to avoid double WHERE.
    let subquery_period_filter = if whr.is_empty() {
        String::new()
    } else {
        " AND s2.scrobbled_at BETWEEN ?1 AND ?2".to_string()
    };

    let sql = format!(
        "SELECT
             CASE WHEN c.artist IS NOT NULL THEN s.artist
                  ELSE COALESCE(
                      (SELECT ac.artist FROM album_cache ac
                       WHERE ac.album = s.album AND ac.musicbrainz_id IS NOT NULL
                       ORDER BY ac.artist LIMIT 1),
                      s.artist
                  )
             END AS artist,
             s.album AS album,
             COUNT(*) AS plays,
             COALESCE(SUM(s.played_duration_secs), 0) AS listen_time,
             -- Dominant source: the player responsible for the most scrobbles
             -- of this album in the queried period. Used to colour-code cards.
             (SELECT s2.source
              FROM scrobbles s2
              WHERE s2.album = s.album
                AND s2.source IS NOT NULL{}
              GROUP BY s2.source
              ORDER BY COUNT(*) DESC
              LIMIT 1) AS dominant_source
         FROM scrobbles s
         LEFT JOIN album_cache c ON c.artist = s.artist AND c.album = s.album{}
         GROUP BY s.album, COALESCE(
             c.musicbrainz_id,
             (SELECT ac.musicbrainz_id FROM album_cache ac
              WHERE ac.album = s.album AND ac.musicbrainz_id IS NOT NULL
              ORDER BY ac.musicbrainz_id LIMIT 1),
             s.artist || '::' || s.album
         )
         ORDER BY plays DESC, listen_time DESC, artist ASC, album ASC
         LIMIT {}",
        subquery_period_filter, whr, limit
    );

    let mut stmt = conn.prepare(&sql)?;
    let mapper = |row: &rusqlite::Row| {
        Ok(TopAlbum {
            artist: row.get(0)?,
            album: row.get(1)?,
            plays: row.get(2)?,
            listen_time_secs: row.get(3)?,
            dominant_source: row.get(4)?,
        })
    };
    if p.is_empty() {
        stmt.query_map([], mapper)?.collect()
    } else {
        stmt.query_map(params![p[0], p[1]], mapper)?.collect()
    }
}

/// Get the top N tracks by play count for the given period,
/// grouped by (artist, title) and ordered by scrobble count descending.
/// The album field is the most frequently scrobbled album for that track
/// (used for cover art lookup).
///
/// Ties are broken by total listened time (descending), then artist + title
/// names for stable output ordering.
pub fn top_tracks(conn: &Connection, period: &str, limit: i64) -> Result<Vec<TopTrack>> {
    let (whr, p) = where_clause(period);
    // Use a subquery to pick the most common album for each (artist, title).
    // In SQLite, the non-aggregated `album` column in a GROUP BY returns an
    // arbitrary row's value, but wrapping it in a subquery with its own
    // GROUP BY and ORDER BY ensures we get the most frequent album.
    let subquery_period_filter = if whr.is_empty() {
        String::new()
    } else {
        " AND s2.scrobbled_at BETWEEN ?1 AND ?2".to_string()
    };

    let sql = format!(
        "SELECT artist, album, title, COUNT(*) as plays,
                COALESCE(SUM(played_duration_secs), 0) as listen_time,
                (SELECT s2.source
                 FROM scrobbles s2
                 WHERE s2.artist = scrobbles.artist
                   AND s2.title = scrobbles.title
                   AND s2.source IS NOT NULL{}
                 GROUP BY s2.source
                 ORDER BY COUNT(*) DESC
                 LIMIT 1) AS dominant_source
         FROM scrobbles{}
         GROUP BY artist, title
         ORDER BY plays DESC, listen_time DESC, artist ASC, title ASC
         LIMIT {}",
        subquery_period_filter, whr, limit
    );

    let mut stmt = conn.prepare(&sql)?;
    let mapper = |row: &rusqlite::Row| {
        Ok(TopTrack {
            artist: row.get(0)?,
            album: row.get(1)?,
            title: row.get(2)?,
            plays: row.get(3)?,
            listen_time_secs: row.get(4)?,
            dominant_source: row.get(5)?,
        })
    };
    if p.is_empty() {
        stmt.query_map([], mapper)?.collect()
    } else {
        stmt.query_map(params![p[0], p[1]], mapper)?.collect()
    }
}

/// Get the top N genres for the given period.
///
/// Genre values come from `album_cache.genre` (comma-separated) and are
/// associated with scrobbles via `(artist, album)` joins.
///
/// Ties are broken by total listened time (descending), then genre name.
pub fn top_genres(conn: &Connection, period: &str, limit: i64) -> Result<Vec<TopGenre>> {
    let rows: Vec<(String, i64)> = if let Some((from, to)) = period_range(period) {
        let mut stmt = conn.prepare(
            "SELECT c.genre, s.played_duration_secs
             FROM scrobbles s
             JOIN album_cache c ON s.artist = c.artist AND s.album = c.album
             WHERE c.genre IS NOT NULL
               AND c.genre != ''
               AND s.scrobbled_at BETWEEN ?1 AND ?2",
        )?;
        let mapped = stmt.query_map(params![from, to], |row| {
            let genre: String = row.get(0)?;
            let played_duration_secs: i64 = row.get(1)?;
            Ok((genre, played_duration_secs))
        })?;
        mapped.collect::<Result<Vec<_>>>()?
    } else {
        let mut stmt = conn.prepare(
            "SELECT c.genre, s.played_duration_secs
             FROM scrobbles s
             JOIN album_cache c ON s.artist = c.artist AND s.album = c.album
             WHERE c.genre IS NOT NULL
               AND c.genre != ''",
        )?;
        let mapped = stmt.query_map([], |row| {
            let genre: String = row.get(0)?;
            let played_duration_secs: i64 = row.get(1)?;
            Ok((genre, played_duration_secs))
        })?;
        mapped.collect::<Result<Vec<_>>>()?
    };

    // key -> (display_label, plays, listen_secs)
    // The key normalises hyphen/space variants so labels like "post-rock" and
    // "post rock" are grouped together.
    let mut agg: HashMap<String, (String, i64, i64)> = HashMap::new();
    for (genre_csv, played_secs) in rows {
        for genre in split_genre_list(&genre_csv) {
            let key = canonical_genre_key(&genre);
            if key.is_empty() {
                continue;
            }
            let entry = agg.entry(key).or_insert_with(|| (genre.clone(), 0, 0));
            if prefer_display_genre(&entry.0, &genre) {
                entry.0 = genre;
            }
            entry.1 += 1;
            entry.2 += played_secs;
        }
    }

    let mut out: Vec<TopGenre> = agg
        .into_iter()
        .map(|(_, (genre, plays, listen_time_secs))| TopGenre {
            genre: genre.trim().to_string(),
            plays,
            listen_time_secs,
        })
        .collect();
    out.sort_by(|a, b| {
        // Deprioritised genres (e.g. "ambient") always rank below specific
        // genres regardless of play count. They still appear in the list if
        // they are the only genres present.
        let a_dep = is_deprioritised_genre(&a.genre);
        let b_dep = is_deprioritised_genre(&b.genre);
        match (a_dep, b_dep) {
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            _ => b
                .plays
                .cmp(&a.plays)
                .then_with(|| b.listen_time_secs.cmp(&a.listen_time_secs))
                .then_with(|| a.genre.cmp(&b.genre)),
        }
    });
    out.truncate(limit.max(0) as usize);
    Ok(out)
}

/// Get the top sources (players) by scrobble count for the given period,
/// ordered by scrobble count descending. Rows with a NULL source are excluded
/// — these are historical records written before the `source` column existed.
pub fn top_sources(conn: &Connection, period: &str) -> Result<Vec<TopSource>> {
    let (whr, p) = where_clause(period);
    // Extend the WHERE clause to also filter out NULL sources.
    let source_filter = if whr.is_empty() {
        " WHERE source IS NOT NULL".to_string()
    } else {
        format!("{} AND source IS NOT NULL", whr)
    };
    let sql = format!(
        "SELECT source, COUNT(*) as scrobbles, COALESCE(SUM(played_duration_secs), 0) as listen_time
         FROM scrobbles{}
         GROUP BY source
         ORDER BY scrobbles DESC, listen_time DESC, source ASC",
        source_filter
    );

    let mut stmt = conn.prepare(&sql)?;
    let mapper = |row: &rusqlite::Row| {
        Ok(TopSource {
            source: row.get(0)?,
            scrobbles: row.get(1)?,
            listen_time_secs: row.get(2)?,
        })
    };
    if p.is_empty() {
        stmt.query_map([], mapper)?.collect()
    } else {
        stmt.query_map(params![p[0], p[1]], mapper)?.collect()
    }
}

/// Get the most recent scrobbles for the given period, ordered by
/// timestamp descending (newest first), limited to `limit` entries.
pub fn recent_scrobbles(conn: &Connection, period: &str, limit: i64) -> Result<Vec<Scrobble>> {
    let (whr, p) = where_clause(period);
    let sql = format!(
        "SELECT id, artist, album, title, track_duration_secs, played_duration_secs, scrobbled_at
         FROM scrobbles{}
         ORDER BY scrobbled_at DESC
         LIMIT {}",
        whr, limit
    );

    let mut stmt = conn.prepare(&sql)?;
    let mapper = |row: &rusqlite::Row| {
        Ok(Scrobble {
            id: row.get(0)?,
            artist: row.get(1)?,
            album: row.get(2)?,
            title: row.get(3)?,
            track_duration_secs: row.get(4)?,
            played_duration_secs: row.get(5)?,
            scrobbled_at: row.get(6)?,
        })
    };
    if p.is_empty() {
        stmt.query_map([], mapper)?.collect()
    } else {
        stmt.query_map(params![p[0], p[1]], mapper)?.collect()
    }
}

// ---------------------------------------------------------------------------
// Album cache queries (for the `enrich` command)
// ---------------------------------------------------------------------------

/// An (artist, album) pair that has scrobbles but no entry in `album_cache`.
/// These are the albums that need enrichment (MusicBrainz lookup + cover fetch).
#[derive(Debug, Clone)]
pub struct UncachedAlbum {
    pub artist: String,
    pub album: String,
}

/// Find all unique (artist, album) pairs in `scrobbles` that either:
/// - don't have a corresponding row in `album_cache` at all, or
/// - have a cached row but with `cover_url` still NULL, or
/// - have a cached row but with `genre` still NULL.
///
/// This ensures albums are re-tried if a previous run failed to find cover art
/// or genre metadata, but only after a cooldown to avoid hitting MusicBrainz
/// on every single report generation.
/// Reset the `fetched_at` timestamp to NULL for all `album_cache` rows that
/// have no `cover_url`. This makes [`uncached_albums`] pick them up again on
/// the next enrichment run, bypassing the 7-day cooldown.
///
/// Used by `enrich --online --retry-covers` to re-attempt cover downloads
/// without forcing a full re-fetch of all albums.
pub fn reset_missing_cover_timestamps(conn: &Connection) -> Result<usize> {
    let count = conn.execute(
        "UPDATE album_cache SET fetched_at = NULL WHERE cover_url IS NULL",
        [],
    )?;
    Ok(count)
}

/// Reset `fetched_at` to NULL for cover-missing albums for one artist only.
///
/// Matching is case-insensitive (`LOWER(artist) = LOWER(?1)`), so callers can
/// pass user input without worrying about exact casing.
pub fn reset_missing_cover_timestamps_for_artist(conn: &Connection, artist: &str) -> Result<usize> {
    let count = conn.execute(
        "UPDATE album_cache
         SET fetched_at = NULL
         WHERE cover_url IS NULL
           AND LOWER(artist) = LOWER(?1)",
        params![artist],
    )?;
    Ok(count)
}

/// Reset `fetched_at` to NULL for MPD-sourced albums that have no genre yet.
///
/// This allows online enrichment to re-try genre lookups immediately for
/// MPD albums, bypassing the 7-day cooldown used by [`uncached_albums`].
pub fn reset_missing_genre_timestamps_for_mpd(conn: &Connection) -> Result<usize> {
    let count = conn.execute(
        "UPDATE album_cache
         SET fetched_at = NULL
         WHERE genre IS NULL
           AND EXISTS (
             SELECT 1 FROM scrobbles s
             WHERE s.artist = album_cache.artist
               AND s.album = album_cache.album
               AND s.source = 'MPD'
           )",
        [],
    )?;
    Ok(count)
}

/// Reset `fetched_at` to NULL for one artist's MPD-sourced genre-missing albums.
///
/// Matching is case-insensitive (`LOWER(artist) = LOWER(?1)`).
pub fn reset_missing_genre_timestamps_for_mpd_artist(
    conn: &Connection,
    artist: &str,
) -> Result<usize> {
    let count = conn.execute(
        "UPDATE album_cache
         SET fetched_at = NULL
         WHERE genre IS NULL
           AND LOWER(artist) = LOWER(?1)
           AND EXISTS (
             SELECT 1 FROM scrobbles s
             WHERE s.artist = album_cache.artist
               AND s.album = album_cache.album
               AND s.source = 'MPD'
           )",
        params![artist],
    )?;
    Ok(count)
}

pub fn uncached_albums(conn: &Connection) -> Result<Vec<UncachedAlbum>> {
    // Retry incomplete cache entries at most once per 7 days.
    let retry_before = (chrono::Local::now().naive_local() - chrono::Duration::days(7))
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();

    let mut stmt = conn.prepare(
        "SELECT DISTINCT s.artist, s.album
         FROM scrobbles s
         LEFT JOIN album_cache c ON s.artist = c.artist AND s.album = c.album
         WHERE s.album != ''
           AND (
             c.id IS NULL
             OR (
               (c.cover_url IS NULL OR c.genre IS NULL)
               AND (c.fetched_at IS NULL OR c.fetched_at < ?1)
             )
           )
         ORDER BY s.artist, s.album",
    )?;
    let rows = stmt.query_map(params![retry_before], |row| {
        Ok(UncachedAlbum {
            artist: row.get(0)?,
            album: row.get(1)?,
        })
    })?;
    rows.collect()
}

/// Return all distinct albums with scrobbles for a specific artist.
///
/// Matching is case-insensitive. Empty album names are excluded.
pub fn albums_for_artist(conn: &Connection, artist: &str) -> Result<Vec<UncachedAlbum>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT s.artist, s.album
         FROM scrobbles s
         WHERE s.album != ''
           AND LOWER(s.artist) = LOWER(?1)
         ORDER BY s.artist, s.album",
    )?;

    let rows = stmt.query_map(params![artist], |row| {
        Ok(UncachedAlbum {
            artist: row.get(0)?,
            album: row.get(1)?,
        })
    })?;
    rows.collect()
}

/// Returns true if `genre` should rank below more specific genres.
///
/// Single-word genres ("rock", "electronic", "ambient") are broad descriptors
/// that appear as secondary tags on many artists. Multi-word genres ("prog
/// rock", "alternative metal") are more specific and should rank first.
pub fn is_deprioritised_genre(genre: &str) -> bool {
    canonical_genre_key(genre).split_whitespace().count() < 2
}

/// Split a comma-separated genre string into cleaned labels.
fn split_genre_list(csv: &str) -> Vec<String> {
    csv.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// Canonical key for grouping genre labels.
///
/// Rules:
/// - case-insensitive
/// - `-` and `_` are treated as spaces
/// - repeated spaces are collapsed
fn canonical_genre_key(genre: &str) -> String {
    let lowered = genre
        .chars()
        .map(|c| match c {
            '-' | '_' => ' ',
            _ => c.to_ascii_lowercase(),
        })
        .collect::<String>();
    lowered.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Decide whether `candidate` should replace `existing` as the display label.
///
/// Preference: if we already have a hyphenated label and we later see a spaced
/// variant for the same canonical key, prefer the spaced variant.
fn prefer_display_genre(existing: &str, candidate: &str) -> bool {
    existing.contains('-') && candidate.contains(' ') && !candidate.contains('-')
}

/// Data for inserting a new album_cache entry.
#[derive(Debug, Clone)]
pub struct AlbumCacheEntry {
    pub artist: String,
    pub album: String,
    pub musicbrainz_id: Option<String>,
    pub cover_url: Option<String>,
    pub genre: Option<String>,
    pub fetched_at: String,
}

/// Insert or update an album_cache entry.
///
/// Uses `ON CONFLICT DO UPDATE` (SQLite upsert syntax) rather than
/// `INSERT OR REPLACE` so that a locally-extracted MPD cover is not
/// silently discarded when the online enrichment runs later.
///
/// Specifically, `cover_url` is updated with `COALESCE(new, existing)`:
/// if the incoming entry has no cover URL (the online enrichment found
/// nothing on the Cover Art Archive), the previously stored local cover
/// (e.g. one extracted from MPD via `readpicture`) is preserved. If the
/// incoming entry has a cover URL, it takes precedence.
///
/// All other fields (`musicbrainz_id`, `genre`, `fetched_at`) are always
/// overwritten with the new values.
pub fn upsert_album_cache(conn: &Connection, entry: &AlbumCacheEntry) -> Result<()> {
    conn.execute(
        "INSERT INTO album_cache (artist, album, musicbrainz_id, cover_url, genre, fetched_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(artist, album) DO UPDATE SET
             musicbrainz_id = excluded.musicbrainz_id,
             cover_url      = COALESCE(excluded.cover_url, cover_url),
             genre          = excluded.genre,
             fetched_at     = excluded.fetched_at",
        params![
            entry.artist,
            entry.album,
            entry.musicbrainz_id,
            entry.cover_url,
            entry.genre,
            entry.fetched_at,
        ],
    )?;
    Ok(())
}

/// Record a locally-extracted cover art path for an (artist, album) pair.
///
/// Unlike [`upsert_album_cache`], this function only writes `cover_url`
/// (and `fetched_at`), leaving `musicbrainz_id` and `genre` unchanged if a
/// cache row already exists, or NULL if this is the first time we're seeing
/// this album. It is used by the MPD cover extractor to cache cover art from
/// embedded file tags before a MusicBrainz lookup has been performed.
///
/// A subsequent call to `upsert_album_cache` (from the online enrichment)
/// will fill in `musicbrainz_id` and `genre` while preserving this cover URL
/// if no better one is found online.
pub fn set_local_cover(
    conn: &Connection,
    artist: &str,
    album: &str,
    cover_path: &str,
) -> Result<()> {
    let fetched_at = chrono::Local::now()
        .naive_local()
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();

    conn.execute(
        "INSERT INTO album_cache (artist, album, cover_url, fetched_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(artist, album) DO UPDATE SET
             cover_url  = ?3,
             fetched_at = ?4",
        params![artist, album, cover_path, fetched_at],
    )?;
    Ok(())
}

/// Find all (artist, album) pairs that have scrobbles but no `cover_url`
/// in `album_cache` yet.
///
/// Used by the MPD cover extractor to determine which albums need a cover.
/// Albums whose `album_cache` row already has a non-NULL `cover_url` are
/// excluded even if `musicbrainz_id` or `genre` are missing — the cover
/// is the only thing the extractor cares about.
///
/// Albums with an empty `album` field are excluded (these are typically
/// singles or radio streams without proper album tags).
/// Returns albums where at least one scrobble came from MPD (`source = 'MPD'`)
/// and that still have no `cover_url` in `album_cache`.
///
/// Scoping to MPD-sourced albums avoids pointless `readpicture` requests for
/// albums scrobbled exclusively via MPRIS players (e.g. Qobuz, Spotify) that
/// MPD almost certainly does not have on disk.
pub fn albums_without_cover_from_mpd(conn: &Connection) -> Result<Vec<UncachedAlbum>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT s.artist, s.album
         FROM scrobbles s
         LEFT JOIN album_cache c ON s.artist = c.artist AND s.album = c.album
         WHERE s.album != ''
           AND s.source = 'MPD'
           AND (c.id IS NULL OR c.cover_url IS NULL)
         ORDER BY s.artist, s.album",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(UncachedAlbum {
            artist: row.get(0)?,
            album: row.get(1)?,
        })
    })?;
    rows.collect()
}

/// A cached MPD-local cover entry keyed by `(artist, album)`.
///
/// `cover_url` is expected to point to a local file generated by MPD cover
/// extraction (`covers/mpd_<hash>.jpg`).
#[derive(Debug, Clone)]
pub struct MpdLocalCoverAlbum {
    pub artist: String,
    pub album: String,
    pub cover_url: String,
}

/// Find MPD-sourced albums that currently point to a local MPD cover file.
///
/// Used by the `repair-mpd-covers` command to revalidate and, if needed,
/// re-fetch previously cached covers.
///
/// We only include rows where:
///
/// - at least one scrobble came from MPD (`source = 'MPD'`), and
/// - `album_cache.cover_url` is a non-NULL path containing `mpd_`.
pub fn albums_with_local_mpd_cover(conn: &Connection) -> Result<Vec<MpdLocalCoverAlbum>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT s.artist, s.album, c.cover_url
         FROM scrobbles s
         JOIN album_cache c ON s.artist = c.artist AND s.album = c.album
         WHERE s.source = 'MPD'
           AND c.cover_url IS NOT NULL
           AND c.cover_url LIKE '%mpd_%'
         ORDER BY s.artist, s.album",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(MpdLocalCoverAlbum {
            artist: row.get(0)?,
            album: row.get(1)?,
            cover_url: row.get(2)?,
        })
    })?;
    rows.collect()
}

/// Cached album metadata returned by [`album_cache_meta`].
#[derive(Debug, Clone)]
pub struct AlbumCacheMeta {
    pub cover_url: Option<String>,
    pub genre: Option<String>,
    pub mbid: Option<String>,
}

/// Fetch cached metadata (cover URL/path and genre string) for a specific
/// `(artist, album)` pair.
pub fn album_cache_meta(
    conn: &Connection,
    artist: &str,
    album: &str,
) -> Result<Option<AlbumCacheMeta>> {
    let mut stmt = conn.prepare(
        "SELECT cover_url, genre, musicbrainz_id
         FROM album_cache
         WHERE artist = ?1 AND album = ?2
         LIMIT 1",
    )?;

    let mut rows = stmt.query(params![artist, album])?;
    if let Some(row) = rows.next()? {
        Ok(Some(AlbumCacheMeta {
            cover_url: row.get(0)?,
            genre: row.get(1)?,
            mbid: row.get(2)?,
        }))
    } else {
        Ok(None)
    }
}

/// Find a cover image for a given artist for the requested period.
///
/// The selected image is the cover of the artist's most-played album in that
/// period (ties break by listen time, then album name). This keeps mini covers
/// in top-artist rows aligned with what the listener actually played most.
///
/// Falls back to an album-name-only cache lookup when the direct
/// (artist, album) join finds no cover, so artists whose scrobbles carry
/// a performer tag (e.g. a soloist on a classical recording) still get
/// the cover that was pinned under a different artist tag for the same album.
pub fn artist_cover(conn: &Connection, artist: &str, period: &str) -> Option<String> {
    // The cover subquery first tries the direct (artist, album) cache entry;
    // if that has no cover it falls back to any cache entry for the same
    // album name that does have one (same MBID-merging logic as top_albums).
    let cover_expr = "COALESCE(
                       c.cover_url,
                       (SELECT ac.cover_url FROM album_cache ac
                        WHERE ac.album = s.album AND ac.cover_url IS NOT NULL
                        ORDER BY ac.artist LIMIT 1)
                     )";

    let time_filter = match period_range(period) {
        Some((from, to)) => format!("AND s.scrobbled_at BETWEEN '{}' AND '{}'", from, to),
        None => String::new(),
    };

    let sql = format!(
        "SELECT {cover_expr} AS cover_url
         FROM scrobbles s
         LEFT JOIN album_cache c ON c.artist = s.artist AND c.album = s.album
         WHERE s.artist = ?1
           {time_filter}
           AND {cover_expr} IS NOT NULL
         GROUP BY s.album
         ORDER BY COUNT(*) DESC,
                  COALESCE(SUM(s.played_duration_secs), 0) DESC,
                  s.album ASC
         LIMIT 1"
    );

    let mut stmt = conn.prepare(&sql).ok()?;
    let mut rows = stmt.query(params![artist]).ok()?;
    rows.next().ok()?.and_then(|row| row.get(0).ok())
}

/// Return the name of the most-played album for `artist` (any period).
/// Used to ensure the artist's cover album is included in targeted enrichment.
pub fn artist_top_album(conn: &Connection, artist: &str) -> Option<String> {
    let mut stmt = conn
        .prepare(
            "SELECT album FROM scrobbles
             WHERE artist = ?1 AND album != ''
             GROUP BY album
             ORDER BY COUNT(*) DESC
             LIMIT 1",
        )
        .ok()?;
    let mut rows = stmt.query(params![artist]).ok()?;
    rows.next().ok()?.and_then(|row| row.get(0).ok())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Format today's date with the given HH:MM:SS component.
    fn today_at(hms: &str) -> String {
        format!("{}T{}", chrono::Local::now().format("%Y-%m-%d"), hms)
    }

    /// Format a date N days in the past with the given HH:MM:SS component.
    fn days_ago_at(days: i64, hms: &str) -> String {
        let dt = chrono::Local::now() - chrono::Duration::days(days);
        format!("{}T{}", dt.format("%Y-%m-%d"), hms)
    }

    /// Populate an in-memory database with a mix of test data:
    /// - 3 scrobbles for ††† (Crosses) across two days
    /// - 2 scrobbles for Deftones
    ///
    /// This gives us enough data to test rankings, listen time sums, etc.
    fn seed_db(conn: &Connection) {
        let scrobbles = vec![
            NewScrobble {
                artist: "††† (Crosses)".to_string(),
                album: "††† (Crosses)".to_string(),
                title: "This Is a Trick".to_string(),
                track_duration_secs: Some(186),
                played_duration_secs: 186,
                scrobbled_at: today_at("10:00:00"),
                source: "test".into(),
            },
            NewScrobble {
                artist: "††† (Crosses)".to_string(),
                album: "††† (Crosses)".to_string(),
                title: "Telepathy".to_string(),
                track_duration_secs: Some(215),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:05:00"),
                source: "test".into(),
            },
            NewScrobble {
                artist: "Deftones".to_string(),
                album: "White Pony".to_string(),
                title: "Digital Bath".to_string(),
                track_duration_secs: Some(291),
                played_duration_secs: 291,
                scrobbled_at: today_at("10:10:00"),
                source: "test".into(),
            },
            NewScrobble {
                artist: "Deftones".to_string(),
                album: "White Pony".to_string(),
                title: "Knife Prty".to_string(),
                track_duration_secs: Some(290),
                played_duration_secs: 250,
                scrobbled_at: days_ago_at(1, "14:00:00"),
                source: "test".into(),
            },
            NewScrobble {
                artist: "††† (Crosses)".to_string(),
                album: "††† (Crosses)".to_string(),
                title: "This Is a Trick".to_string(),
                track_duration_secs: Some(186),
                played_duration_secs: 180,
                scrobbled_at: days_ago_at(7, "09:00:00"),
                source: "test".into(),
            },
        ];
        for s in &scrobbles {
            insert_scrobble(conn, s).unwrap();
        }
    }

    #[test]
    fn test_schema_creation() {
        let conn = open_memory_db().unwrap();
        // A freshly created DB should have zero scrobbles.
        let ov = overview(&conn, "all").unwrap();
        assert_eq!(ov.total_scrobbles, 0);
    }

    #[test]
    fn test_insert_and_query() {
        let conn = open_memory_db().unwrap();
        let s = NewScrobble {
            artist: "††† (Crosses)".to_string(),
            album: "††† (Crosses)".to_string(),
            title: "This Is a Trick".to_string(),
            track_duration_secs: Some(186),
            played_duration_secs: 186,
            scrobbled_at: today_at("10:00:00"),
            source: "test".into(),
        };
        // First insert should get row ID 1.
        let id = insert_scrobble(&conn, &s).unwrap();
        assert_eq!(id, 1);

        // Verify the overview reflects the single scrobble.
        let ov = overview(&conn, "all").unwrap();
        assert_eq!(ov.total_scrobbles, 1);
        assert_eq!(ov.total_listen_time_secs, 186);
        assert_eq!(ov.unique_artists, 1);
    }

    #[test]
    fn test_top_artists() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        let artists = top_artists(&conn, "all", 10).unwrap();
        assert_eq!(artists.len(), 2);
        // ††† (Crosses) has 3 plays, Deftones has 2 — sorted by play count.
        assert_eq!(artists[0].artist, "††† (Crosses)");
        assert_eq!(artists[0].plays, 3);
        assert_eq!(artists[1].artist, "Deftones");
        assert_eq!(artists[1].plays, 2);
    }

    #[test]
    fn test_top_albums() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        let albums = top_albums(&conn, "all", 10).unwrap();
        assert_eq!(albums.len(), 2);
        // ††† (Crosses) album has 3 plays, White Pony has 2.
        assert_eq!(albums[0].album, "††† (Crosses)");
        assert_eq!(albums[0].plays, 3);
    }

    #[test]
    fn test_top_tracks() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        let tracks = top_tracks(&conn, "all", 10).unwrap();
        // "This Is a Trick" appears twice (two scrobbles), others appear once each.
        assert_eq!(tracks[0].title, "This Is a Trick");
        assert_eq!(tracks[0].plays, 2);
    }

    #[test]
    fn test_top_genres() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        // Map seeded albums to genres.
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "††† (Crosses)".to_string(),
                album: "††† (Crosses)".to_string(),
                musicbrainz_id: None,
                cover_url: None,
                genre: Some("darkwave, electronic".to_string()),
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Deftones".to_string(),
                album: "White Pony".to_string(),
                musicbrainz_id: None,
                cover_url: None,
                genre: Some("alternative metal".to_string()),
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();

        let genres = top_genres(&conn, "all", 10).unwrap();
        // Multi-word genres rank first regardless of play count.
        // Single-word genres ("darkwave", "electronic") are deprioritised.
        assert_eq!(genres[0].genre, "alternative metal");
        assert_eq!(genres[0].plays, 2);
        // Single-word genres follow, ordered by plays then listen time.
        assert_eq!(genres[1].genre, "darkwave");
        assert_eq!(genres[1].plays, 3);
        assert_eq!(genres[2].genre, "electronic");
        assert_eq!(genres[2].plays, 3);
    }

    #[test]
    fn test_top_genres_merges_hyphen_and_space_variants() {
        let conn = open_memory_db().unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "A".to_string(),
                album: "H".to_string(),
                title: "T1".to_string(),
                track_duration_secs: Some(200),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:00:00"),
                source: "test".into(),
            },
        )
        .unwrap();
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "A".to_string(),
                album: "S".to_string(),
                title: "T2".to_string(),
                track_duration_secs: Some(180),
                played_duration_secs: 180,
                scrobbled_at: today_at("10:01:00"),
                source: "test".into(),
            },
        )
        .unwrap();

        // Insert hyphenated first, spaced second. We should group both and
        // prefer the spaced display label.
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "A".to_string(),
                album: "H".to_string(),
                musicbrainz_id: None,
                cover_url: None,
                genre: Some("post-rock".to_string()),
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "A".to_string(),
                album: "S".to_string(),
                musicbrainz_id: None,
                cover_url: None,
                genre: Some("post rock".to_string()),
                fetched_at: today_at("12:00:01"),
            },
        )
        .unwrap();

        let genres = top_genres(&conn, "all", 10).unwrap();
        assert_eq!(genres.len(), 1);
        assert_eq!(genres[0].genre, "post rock");
        assert_eq!(genres[0].plays, 2);
        assert_eq!(genres[0].listen_time_secs, 380);
    }

    #[test]
    fn test_top_artists_tie_breaks_by_listen_time() {
        let conn = open_memory_db().unwrap();

        // Both artists have 1 play; SlowCore should rank above FastPop because
        // it has higher total listened time.
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "FastPop".to_string(),
                album: "A".to_string(),
                title: "Short".to_string(),
                track_duration_secs: Some(120),
                played_duration_secs: 120,
                scrobbled_at: today_at("11:00:00"),
                source: "test".into(),
            },
        )
        .unwrap();
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "SlowCore".to_string(),
                album: "B".to_string(),
                title: "Long".to_string(),
                track_duration_secs: Some(320),
                played_duration_secs: 320,
                scrobbled_at: today_at("11:05:00"),
                source: "test".into(),
            },
        )
        .unwrap();

        let artists = top_artists(&conn, "all", 10).unwrap();
        assert_eq!(artists.len(), 2);
        assert_eq!(artists[0].artist, "SlowCore");
        assert_eq!(artists[0].plays, 1);
        assert_eq!(artists[1].artist, "FastPop");
    }

    #[test]
    fn test_top_albums_tie_breaks_by_listen_time() {
        let conn = open_memory_db().unwrap();

        // Both albums have 1 play; Album B should rank above Album A because
        // listened time is greater.
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Artist".to_string(),
                album: "Album A".to_string(),
                title: "One".to_string(),
                track_duration_secs: Some(150),
                played_duration_secs: 150,
                scrobbled_at: today_at("12:00:00"),
                source: "test".into(),
            },
        )
        .unwrap();
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Artist".to_string(),
                album: "Album B".to_string(),
                title: "Two".to_string(),
                track_duration_secs: Some(240),
                played_duration_secs: 240,
                scrobbled_at: today_at("12:05:00"),
                source: "test".into(),
            },
        )
        .unwrap();

        let albums = top_albums(&conn, "all", 10).unwrap();
        assert_eq!(albums.len(), 2);
        assert_eq!(albums[0].album, "Album B");
        assert_eq!(albums[0].plays, 1);
        assert_eq!(albums[1].album, "Album A");
    }

    #[test]
    fn test_top_albums_merges_multi_artist_same_mbid() {
        // Classical albums often have different MPRIS artist tags per track
        // (soloist on one, ensemble on another). Verify that top_albums groups
        // them under one entry when album_cache maps one of the artist variants
        // to a MBID.
        let conn = open_memory_db().unwrap();

        let album = "Vivaldi: Gloria; Nisi Dominus";

        // Three tracks: two with artist A, one with artist B. Same album name.
        let ts1 = today_at("10:00:00");
        let ts2 = today_at("10:05:00");
        let ts3 = today_at("10:10:00");
        for (artist, ts) in &[
            ("Choir", ts1.as_str()),
            ("Choir", ts2.as_str()),
            ("Soloist", ts3.as_str()),
        ] {
            insert_scrobble(
                &conn,
                &NewScrobble {
                    artist: (*artist).to_string(),
                    album: album.to_string(),
                    title: "Track".to_string(),
                    track_duration_secs: Some(300),
                    played_duration_secs: 300,
                    scrobbled_at: (*ts).to_string(),
                    source: "test".into(),
                },
            )
            .unwrap();
        }

        // Without any album_cache entry, both (artist, album) pairs are
        // separate groups — pre-fix behaviour reproduced here.
        let albums_no_cache = top_albums(&conn, "all", 10).unwrap();
        assert_eq!(
            albums_no_cache.len(),
            2,
            "without cache: two separate groups"
        );

        // Pin the album via album_cache for "Choir". The MBID lookup in
        // top_albums will now resolve "Soloist"'s rows to the same MBID.
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Choir".to_string(),
                album: album.to_string(),
                musicbrainz_id: Some("test-mbid-vivaldi".to_string()),
                cover_url: Some("covers/test.jpg".to_string()),
                genre: Some("classical".to_string()),
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();

        let albums = top_albums(&conn, "all", 10).unwrap();
        assert_eq!(albums.len(), 1, "with cache: merged into one group");
        assert_eq!(albums[0].plays, 3);
        assert_eq!(albums[0].album, album);
        // The returned artist should be the cached one so cover lookup works.
        assert_eq!(albums[0].artist, "Choir");
    }

    #[test]
    fn test_top_tracks_tie_breaks_by_listen_time() {
        let conn = open_memory_db().unwrap();

        // Both tracks have 1 play; "Epic" should rank above "Brief" because
        // listened time is greater.
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Band".to_string(),
                album: "LP".to_string(),
                title: "Brief".to_string(),
                track_duration_secs: Some(100),
                played_duration_secs: 100,
                scrobbled_at: today_at("13:00:00"),
                source: "test".into(),
            },
        )
        .unwrap();
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Band".to_string(),
                album: "LP".to_string(),
                title: "Epic".to_string(),
                track_duration_secs: Some(360),
                played_duration_secs: 360,
                scrobbled_at: today_at("13:05:00"),
                source: "test".into(),
            },
        )
        .unwrap();

        let tracks = top_tracks(&conn, "all", 10).unwrap();
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].title, "Epic");
        assert_eq!(tracks[0].plays, 1);
        assert_eq!(tracks[1].title, "Brief");
    }

    #[test]
    fn test_recent_scrobbles() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        let recent = recent_scrobbles(&conn, "all", 3).unwrap();
        assert_eq!(recent.len(), 3);
        // Results are ordered by scrobbled_at DESC — most recent first.
        assert_eq!(recent[0].scrobbled_at, today_at("10:10:00"));
    }

    #[test]
    fn test_overview_listen_time() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        let ov = overview(&conn, "all").unwrap();
        assert_eq!(ov.total_scrobbles, 5);
        // Total listen time: 186 + 200 + 291 + 250 + 180 = 1107 seconds.
        assert_eq!(ov.total_listen_time_secs, 1107);
        assert_eq!(ov.unique_artists, 2);
    }

    #[test]
    fn test_insert_missing_duration() {
        // Verify that tracks with unknown duration can be stored and retrieved.
        let conn = open_memory_db().unwrap();
        let s = NewScrobble {
            artist: "Unknown".to_string(),
            album: "".to_string(),
            title: "Mystery".to_string(),
            track_duration_secs: None,
            played_duration_secs: 240,
            scrobbled_at: today_at("12:00:00"),
            source: "test".into(),
        };
        insert_scrobble(&conn, &s).unwrap();
        let recent = recent_scrobbles(&conn, "all", 1).unwrap();
        assert!(recent[0].track_duration_secs.is_none());
        assert_eq!(recent[0].played_duration_secs, 240);
    }

    #[test]
    fn test_latest_scrobble_at() {
        let conn = open_memory_db().unwrap();
        assert!(latest_scrobble_at(&conn).unwrap().is_none());

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "A".to_string(),
                album: "X".to_string(),
                title: "T1".to_string(),
                track_duration_secs: Some(100),
                played_duration_secs: 100,
                scrobbled_at: today_at("10:00:00"),
                source: "test".into(),
            },
        )
        .unwrap();
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "A".to_string(),
                album: "Y".to_string(),
                title: "T2".to_string(),
                track_duration_secs: Some(120),
                played_duration_secs: 120,
                scrobbled_at: today_at("12:34:56"),
                source: "test".into(),
            },
        )
        .unwrap();

        let expected = today_at("12:34:56");
        assert_eq!(
            latest_scrobble_at(&conn).unwrap().as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn test_artist_cover_uses_most_played_album_and_period() {
        let conn = open_memory_db().unwrap();

        let now = chrono::Local::now().naive_local();
        let old_day = (now - chrono::Duration::days(10))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        let recent = (now - chrono::Duration::minutes(10))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();

        // Older period: two plays on "Old Album".
        for i in 0..2 {
            insert_scrobble(
                &conn,
                &NewScrobble {
                    artist: "Artist X".to_string(),
                    album: "Old Album".to_string(),
                    title: format!("Old Song {}", i),
                    track_duration_secs: Some(180),
                    played_duration_secs: 180,
                    scrobbled_at: old_day.clone(),
                    source: "test".into(),
                },
            )
            .unwrap();
        }

        // Today: one play on "New Album".
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Artist X".to_string(),
                album: "New Album".to_string(),
                title: "New Song".to_string(),
                track_duration_secs: Some(200),
                played_duration_secs: 200,
                scrobbled_at: recent,
                source: "test".into(),
            },
        )
        .unwrap();

        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Artist X".to_string(),
                album: "Old Album".to_string(),
                musicbrainz_id: None,
                cover_url: Some("covers/old.jpg".to_string()),
                genre: None,
                fetched_at: now.format("%Y-%m-%dT%H:%M:%S").to_string(),
            },
        )
        .unwrap();
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Artist X".to_string(),
                album: "New Album".to_string(),
                musicbrainz_id: None,
                cover_url: Some("covers/new.jpg".to_string()),
                genre: None,
                fetched_at: now.format("%Y-%m-%dT%H:%M:%S").to_string(),
            },
        )
        .unwrap();

        // Across all time, Old Album wins with more plays.
        assert_eq!(
            artist_cover(&conn, "Artist X", "all").as_deref(),
            Some("covers/old.jpg")
        );
        // For today, only New Album is present.
        assert_eq!(
            artist_cover(&conn, "Artist X", "today").as_deref(),
            Some("covers/new.jpg")
        );
    }

    // -----------------------------------------------------------------------
    // albums_without_cover_from_mpd
    // -----------------------------------------------------------------------

    #[test]
    fn test_albums_without_cover_from_mpd_returns_uncovered() {
        let conn = open_memory_db().unwrap();
        // Seed with MPD-sourced scrobbles so the query can find them.
        for (artist, album, title) in &[
            ("††† (Crosses)", "††† (Crosses)", "This Is a Trick"),
            ("Deftones", "White Pony", "Digital Bath"),
        ] {
            insert_scrobble(
                &conn,
                &NewScrobble {
                    artist: artist.to_string(),
                    album: album.to_string(),
                    title: title.to_string(),
                    track_duration_secs: Some(200),
                    played_duration_secs: 200,
                    scrobbled_at: today_at("10:00:00"),
                    source: "MPD".to_string(),
                },
            )
            .unwrap();
        }

        // No cache entries yet — both MPD-sourced albums should appear.
        let albums = albums_without_cover_from_mpd(&conn).unwrap();
        let names: Vec<&str> = albums.iter().map(|a| a.album.as_str()).collect();
        assert!(names.contains(&"††† (Crosses)"), "got: {:?}", names);
        assert!(names.contains(&"White Pony"), "got: {:?}", names);
    }

    #[test]
    fn test_albums_without_cover_from_mpd_excludes_covered() {
        let conn = open_memory_db().unwrap();
        for (artist, album, title) in &[
            ("††† (Crosses)", "††† (Crosses)", "This Is a Trick"),
            ("Deftones", "White Pony", "Digital Bath"),
        ] {
            insert_scrobble(
                &conn,
                &NewScrobble {
                    artist: artist.to_string(),
                    album: album.to_string(),
                    title: title.to_string(),
                    track_duration_secs: Some(200),
                    played_duration_secs: 200,
                    scrobbled_at: today_at("10:00:00"),
                    source: "MPD".to_string(),
                },
            )
            .unwrap();
        }

        // Give "White Pony" a cover — it should drop out of the results.
        set_local_cover(&conn, "Deftones", "White Pony", "covers/wp.jpg").unwrap();

        let albums = albums_without_cover_from_mpd(&conn).unwrap();
        let names: Vec<&str> = albums.iter().map(|a| a.album.as_str()).collect();
        assert!(!names.contains(&"White Pony"), "got: {:?}", names);
        assert!(names.contains(&"††† (Crosses)"), "got: {:?}", names);
    }

    #[test]
    fn test_albums_without_cover_from_mpd_excludes_non_mpd() {
        let conn = open_memory_db().unwrap();
        // Scrobble an album via a non-MPD source — should not appear.
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Tourist".to_string(),
                album: "Inside Out".to_string(),
                title: "Inside Out".to_string(),
                track_duration_secs: Some(300),
                played_duration_secs: 300,
                scrobbled_at: today_at("10:00:00"),
                source: "Qobuz".to_string(),
            },
        )
        .unwrap();

        let albums = albums_without_cover_from_mpd(&conn).unwrap();
        let names: Vec<&str> = albums.iter().map(|a| a.album.as_str()).collect();
        assert!(
            !names.contains(&"Inside Out"),
            "Qobuz album should not appear: {:?}",
            names
        );
    }

    // -----------------------------------------------------------------------
    // set_local_cover
    // -----------------------------------------------------------------------

    #[test]
    fn test_albums_with_local_mpd_cover_returns_only_mpd_local_rows() {
        let conn = open_memory_db().unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                title: "Digital Bath".into(),
                track_duration_secs: Some(291),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:00:00"),
                source: "MPD".into(),
            },
        )
        .unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Tourist".into(),
                album: "Inside Out".into(),
                title: "Inside Out".into(),
                track_duration_secs: Some(240),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:05:00"),
                source: "MPD".into(),
            },
        )
        .unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Bonobo".into(),
                album: "Black Sands".into(),
                title: "Kiara".into(),
                track_duration_secs: Some(220),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:10:00"),
                source: "Qobuz".into(),
            },
        )
        .unwrap();

        set_local_cover(
            &conn,
            "Deftones",
            "White Pony",
            "/tmp/scrbblr/covers/mpd_deadbeef.jpg",
        )
        .unwrap();
        set_local_cover(
            &conn,
            "Tourist",
            "Inside Out",
            "/tmp/scrbblr/covers/itunes_123.jpg",
        )
        .unwrap();
        set_local_cover(
            &conn,
            "Bonobo",
            "Black Sands",
            "/tmp/scrbblr/covers/mpd_abcdef01.jpg",
        )
        .unwrap();

        let rows = albums_with_local_mpd_cover(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].artist, "Deftones");
        assert_eq!(rows[0].album, "White Pony");
        assert!(rows[0].cover_url.contains("mpd_"));
    }

    #[test]
    fn test_set_local_cover_inserts_new_row() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        // Album has no cache row yet.
        set_local_cover(&conn, "Deftones", "White Pony", "covers/wp.jpg").unwrap();

        let meta = album_cache_meta(&conn, "Deftones", "White Pony")
            .unwrap()
            .expect("cache row should exist");
        assert_eq!(meta.cover_url.as_deref(), Some("covers/wp.jpg"));
        // musicbrainz_id and genre should be NULL (not set by set_local_cover).
        assert!(meta.mbid.is_none());
        assert!(meta.genre.is_none());
    }

    #[test]
    fn test_set_local_cover_updates_existing_row() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        // Seed an existing cache entry with a genre but no cover.
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                musicbrainz_id: Some("some-mbid".into()),
                cover_url: None,
                genre: Some("nu-metal".into()),
                fetched_at: days_ago_at(90, "00:00:00"),
            },
        )
        .unwrap();

        // Now set the cover via MPD extraction.
        set_local_cover(&conn, "Deftones", "White Pony", "covers/wp_local.jpg").unwrap();

        let meta = album_cache_meta(&conn, "Deftones", "White Pony")
            .unwrap()
            .expect("cache row should exist");
        // Cover is updated.
        assert_eq!(meta.cover_url.as_deref(), Some("covers/wp_local.jpg"));
        // The existing mbid and genre must be preserved — set_local_cover
        // must not overwrite them.
        assert_eq!(meta.mbid.as_deref(), Some("some-mbid"));
        assert_eq!(meta.genre.as_deref(), Some("nu-metal"));
    }

    // -----------------------------------------------------------------------
    // upsert_album_cache — COALESCE cover_url preservation
    // -----------------------------------------------------------------------

    #[test]
    fn test_upsert_preserves_local_cover_when_online_finds_none() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        // Step 1: MPD cover extractor sets a local cover.
        set_local_cover(&conn, "Deftones", "White Pony", "covers/wp_local.jpg").unwrap();

        // Step 2: Online enrichment finds the MBID and genre, but no CAA cover.
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                musicbrainz_id: Some("abc-123".into()),
                cover_url: None, // no cover from CAA
                genre: Some("nu-metal, alternative metal".into()),
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();

        // The local cover must be preserved because COALESCE(NULL, existing) = existing.
        let meta = album_cache_meta(&conn, "Deftones", "White Pony")
            .unwrap()
            .expect("cache row should exist");
        assert_eq!(
            meta.cover_url.as_deref(),
            Some("covers/wp_local.jpg"),
            "local cover should be preserved when online enrichment finds nothing"
        );
        // Genre and MBID should be updated.
        assert_eq!(meta.mbid.as_deref(), Some("abc-123"));
        assert!(meta.genre.is_some());
    }

    #[test]
    fn test_upsert_replaces_local_cover_when_online_finds_one() {
        let conn = open_memory_db().unwrap();
        seed_db(&conn);

        // Step 1: MPD cover extractor sets a local cover.
        set_local_cover(&conn, "Deftones", "White Pony", "covers/wp_local.jpg").unwrap();

        // Step 2: Online enrichment finds a cover from the Cover Art Archive.
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                musicbrainz_id: Some("abc-123".into()),
                cover_url: Some("covers/abc-123.jpg".into()), // has CAA cover
                genre: Some("nu-metal".into()),
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();

        // The CAA cover takes priority over the local one.
        let meta = album_cache_meta(&conn, "Deftones", "White Pony")
            .unwrap()
            .expect("cache row should exist");
        assert_eq!(
            meta.cover_url.as_deref(),
            Some("covers/abc-123.jpg"),
            "CAA cover should replace the local cover"
        );
    }

    #[test]
    fn test_albums_for_artist_case_insensitive() {
        let conn = open_memory_db().unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Pink Floyd".into(),
                album: "The Division Bell".into(),
                title: "Marooned".into(),
                track_duration_secs: Some(329),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:00:00"),
                source: "MPRIS".into(),
            },
        )
        .unwrap();
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Pink Floyd".into(),
                album: "The Wall".into(),
                title: "Mother".into(),
                track_duration_secs: Some(345),
                played_duration_secs: 210,
                scrobbled_at: today_at("10:05:00"),
                source: "MPRIS".into(),
            },
        )
        .unwrap();
        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                title: "Digital Bath".into(),
                track_duration_secs: Some(291),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:10:00"),
                source: "MPRIS".into(),
            },
        )
        .unwrap();

        let albums = albums_for_artist(&conn, "pInK fLoYd").unwrap();
        assert_eq!(albums.len(), 2);
        assert!(
            albums
                .iter()
                .any(|a| a.artist == "Pink Floyd" && a.album == "The Division Bell")
        );
        assert!(
            albums
                .iter()
                .any(|a| a.artist == "Pink Floyd" && a.album == "The Wall")
        );
    }

    #[test]
    fn test_reset_missing_cover_timestamps_for_artist_only() {
        let conn = open_memory_db().unwrap();

        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Pink Floyd".into(),
                album: "The Division Bell".into(),
                musicbrainz_id: Some("mbid-1".into()),
                cover_url: None,
                genre: None,
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                musicbrainz_id: Some("mbid-2".into()),
                cover_url: None,
                genre: None,
                fetched_at: today_at("12:01:00"),
            },
        )
        .unwrap();

        let updated = reset_missing_cover_timestamps_for_artist(&conn, "pink floyd").unwrap();
        assert_eq!(updated, 1);

        let pink_fetched: Option<String> = conn
            .query_row(
                "SELECT fetched_at FROM album_cache WHERE artist = 'Pink Floyd' AND album = 'The Division Bell'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(pink_fetched.is_none());

        let deftones_fetched: Option<String> = conn
            .query_row(
                "SELECT fetched_at FROM album_cache WHERE artist = 'Deftones' AND album = 'White Pony'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let expected = today_at("12:01:00");
        assert_eq!(deftones_fetched.as_deref(), Some(expected.as_str()));
    }

    #[test]
    fn test_reset_missing_genre_timestamps_for_mpd_only() {
        let conn = open_memory_db().unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                title: "Digital Bath".into(),
                track_duration_secs: Some(291),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:00:00"),
                source: "MPD".into(),
            },
        )
        .unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Bonobo".into(),
                album: "Black Sands".into(),
                title: "Kiara".into(),
                track_duration_secs: Some(220),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:01:00"),
                source: "Qobuz".into(),
            },
        )
        .unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Tourist".into(),
                album: "Inside Out".into(),
                title: "Inside Out".into(),
                track_duration_secs: Some(240),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:02:00"),
                source: "MPD".into(),
            },
        )
        .unwrap();

        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                musicbrainz_id: Some("mbid-1".into()),
                cover_url: Some("covers/mpd_x.jpg".into()),
                genre: None,
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Bonobo".into(),
                album: "Black Sands".into(),
                musicbrainz_id: Some("mbid-2".into()),
                cover_url: Some("covers/itunes_x.jpg".into()),
                genre: None,
                fetched_at: today_at("12:01:00"),
            },
        )
        .unwrap();
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Tourist".into(),
                album: "Inside Out".into(),
                musicbrainz_id: Some("mbid-3".into()),
                cover_url: Some("covers/mpd_y.jpg".into()),
                genre: Some("downtempo".into()),
                fetched_at: today_at("12:02:00"),
            },
        )
        .unwrap();

        let updated = reset_missing_genre_timestamps_for_mpd(&conn).unwrap();
        assert_eq!(updated, 1);

        let deftones_fetched: Option<String> = conn
            .query_row(
                "SELECT fetched_at FROM album_cache WHERE artist='Deftones' AND album='White Pony'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(deftones_fetched.is_none());

        let bonobo_fetched: Option<String> = conn
            .query_row(
                "SELECT fetched_at FROM album_cache WHERE artist='Bonobo' AND album='Black Sands'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let bonobo_expected = today_at("12:01:00");
        assert_eq!(bonobo_fetched.as_deref(), Some(bonobo_expected.as_str()));

        let tourist_fetched: Option<String> = conn
            .query_row(
                "SELECT fetched_at FROM album_cache WHERE artist='Tourist' AND album='Inside Out'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let tourist_expected = today_at("12:02:00");
        assert_eq!(tourist_fetched.as_deref(), Some(tourist_expected.as_str()));
    }

    #[test]
    fn test_reset_missing_genre_timestamps_for_mpd_artist_only() {
        let conn = open_memory_db().unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Pink Floyd".into(),
                album: "The Wall".into(),
                title: "Mother".into(),
                track_duration_secs: Some(345),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:00:00"),
                source: "MPD".into(),
            },
        )
        .unwrap();

        insert_scrobble(
            &conn,
            &NewScrobble {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                title: "Digital Bath".into(),
                track_duration_secs: Some(291),
                played_duration_secs: 200,
                scrobbled_at: today_at("10:01:00"),
                source: "MPD".into(),
            },
        )
        .unwrap();

        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Pink Floyd".into(),
                album: "The Wall".into(),
                musicbrainz_id: Some("mbid-1".into()),
                cover_url: Some("covers/mpd_1.jpg".into()),
                genre: None,
                fetched_at: today_at("12:00:00"),
            },
        )
        .unwrap();
        upsert_album_cache(
            &conn,
            &AlbumCacheEntry {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                musicbrainz_id: Some("mbid-2".into()),
                cover_url: Some("covers/mpd_2.jpg".into()),
                genre: None,
                fetched_at: today_at("12:01:00"),
            },
        )
        .unwrap();

        let updated = reset_missing_genre_timestamps_for_mpd_artist(&conn, "pink floyd").unwrap();
        assert_eq!(updated, 1);

        let pink_fetched: Option<String> = conn
            .query_row(
                "SELECT fetched_at FROM album_cache WHERE artist='Pink Floyd' AND album='The Wall'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(pink_fetched.is_none());

        let deftones_fetched: Option<String> = conn
            .query_row(
                "SELECT fetched_at FROM album_cache WHERE artist='Deftones' AND album='White Pony'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let expected = today_at("12:01:00");
        assert_eq!(deftones_fetched.as_deref(), Some(expected.as_str()));
    }
}
