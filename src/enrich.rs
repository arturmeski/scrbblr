//! Enrich module — fetches album metadata from MusicBrainz and cover art
//! from the Cover Art Archive.
//!
//! This module is used in two ways:
//! - Explicitly via the `enrich` CLI subcommand.
//! - Automatically when `report --html` is used (to ensure covers are
//!   available before generating the HTML).
//!
//! The enrichment process:
//!
//! 1. Queries the database for all unique (artist, album) pairs that don't
//!    yet have an entry in `album_cache`.
//! 2. For each, searches the MusicBrainz API for matching releases.
//! 3. If found, fetches genre/tag information from MusicBrainz.
//! 4. Downloads the front cover image from the Cover Art Archive, trying:
//!    a. The specific release endpoint
//!    b. The release-group endpoint (covers any edition of the album)
//!    c. Other candidate releases from the search results
//! 5. Stores the metadata in `album_cache` and the cover image on disk.
//!
//! ## Rate limiting
//!
//! MusicBrainz requires a maximum of 1 request per second. We enforce this
//! by sleeping between API calls. The Cover Art Archive has no documented
//! rate limit, but we apply the same 1 req/sec policy to be a good citizen.
//!
//! ## API endpoints used
//!
//! - MusicBrainz release search:
//!   `https://musicbrainz.org/ws/2/release/?query=artist:{}&release:{}&fmt=json`
//!
//! - MusicBrainz release details (for tags/genres and release-group ID):
//!   `https://musicbrainz.org/ws/2/release/{mbid}?inc=genres+tags+release-groups&fmt=json`
//!
//! - Cover Art Archive (release-specific):
//!   `https://coverartarchive.org/release/{mbid}/front`
//!
//! - Cover Art Archive (release-group, any edition):
//!   `https://coverartarchive.org/release-group/{rgid}/front`
//!
//! ## User-Agent
//!
//! MusicBrainz requires a descriptive User-Agent header. We use:
//!   `mpris-scrobbler/0.1.0 (https://github.com/arturmeski/mpris_scrobbler)`

use crate::db::{self, AlbumCacheEntry};
use image::codecs::jpeg::JpegEncoder;
use reqwest::blocking::Client;
use rusqlite::Connection;
use serde::Deserialize;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

/// User-Agent string sent with all HTTP requests to MusicBrainz.
/// Required by MusicBrainz API policy:
/// https://musicbrainz.org/doc/MusicBrainz_API/Rate_Limiting
const USER_AGENT: &str = "mpris-scrobbler/0.1.0 (https://github.com/arturmeski/mpris_scrobbler)";

/// Build the shared HTTP client used for all MusicBrainz and Cover Art Archive requests.
fn build_client() -> Client {
    Client::builder()
        .user_agent(USER_AGENT)
        // Follow redirects (Cover Art Archive uses them).
        .redirect(reqwest::redirect::Policy::limited(5))
        .timeout(Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client")
}

/// Return the current local time formatted as an ISO 8601 datetime string.
fn now_str() -> String {
    chrono::Local::now()
        .naive_local()
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string()
}

/// Minimum delay between API requests to respect MusicBrainz rate limits.
const RATE_LIMIT_DELAY: Duration = Duration::from_millis(1100);

/// Maximum dimension (width or height) for cached cover art in pixels.
/// Images larger than this are downscaled before saving.
const MAX_COVER_DIM: u32 = 500;

/// JPEG quality used when re-encoding resized cover art (0–100).
const COVER_JPEG_QUALITY: u8 = 85;

// ---------------------------------------------------------------------------
// MusicBrainz API response types (only the fields we need)
// ---------------------------------------------------------------------------

/// Response from the MusicBrainz release search endpoint.
#[derive(Debug, Deserialize)]
struct MbReleaseSearchResponse {
    releases: Vec<MbRelease>,
}

/// A single release from MusicBrainz search results.
#[derive(Debug, Deserialize)]
struct MbRelease {
    /// The MusicBrainz release ID (UUID).
    id: String,
    /// Release title.
    #[allow(dead_code)]
    title: String,
    /// Match score (0-100) indicating how well this result matches the query.
    score: u32,
}

/// Response from the MusicBrainz recording search endpoint.
#[derive(Debug, Deserialize)]
struct MbRecordingSearchResponse {
    #[serde(default)]
    recordings: Vec<MbRecording>,
}

/// A single recording result from MusicBrainz.
#[derive(Debug, Deserialize)]
struct MbRecording {
    /// Match score (0-100) indicating how well this result matches the query.
    score: u32,
    /// Releases containing this recording.
    #[serde(default)]
    releases: Vec<MbRecordingRelease>,
}

/// A lightweight release reference from recording results.
#[derive(Debug, Deserialize)]
struct MbRecordingRelease {
    id: String,
}

/// Response from the MusicBrainz release details endpoint (with genres/tags
/// and release-group info).
#[derive(Debug, Deserialize)]
struct MbReleaseDetails {
    /// Genre tags associated with the release.
    #[serde(default)]
    genres: Vec<MbGenre>,
    /// User-submitted tags (broader than genres, includes styles, moods, etc.).
    #[serde(default)]
    tags: Vec<MbTag>,
    /// The release-group this release belongs to. All editions of the same
    /// album share one release-group.
    #[serde(rename = "release-group")]
    release_group: Option<MbReleaseGroup>,
}

/// Response from the MusicBrainz release-group details endpoint.
#[derive(Debug, Deserialize)]
struct MbReleaseGroupDetails {
    /// Genre tags associated with the release-group.
    #[serde(default)]
    genres: Vec<MbGenre>,
    /// User-submitted tags for the release-group.
    #[serde(default)]
    tags: Vec<MbTag>,
}

/// A release-group from MusicBrainz. Groups all editions/pressings of
/// the same album under a single ID.
#[derive(Debug, Deserialize)]
struct MbReleaseGroup {
    id: String,
}

/// A genre entry from MusicBrainz.
#[derive(Debug, Deserialize)]
struct MbGenre {
    name: String,
}

/// A tag entry from MusicBrainz (user-submitted, with vote count).
#[derive(Debug, Deserialize)]
struct MbTag {
    name: String,
    /// Number of votes for this tag. Higher = more reliable.
    #[serde(default)]
    count: i32,
}

// ---------------------------------------------------------------------------
// Cover art directory
// ---------------------------------------------------------------------------

/// Get the directory where cover art images are stored, creating it if needed.
///
/// Default: `~/.local/share/mpris-scrobbler/covers/`
/// (respects `$XDG_DATA_HOME`)
pub fn covers_dir() -> PathBuf {
    let data_dir = std::env::var("XDG_DATA_HOME").unwrap_or_else(|_| {
        let home = std::env::var("HOME").expect("HOME not set");
        format!("{}/.local/share", home)
    });
    let dir = PathBuf::from(format!("{}/mpris-scrobbler/covers", data_dir));
    fs::create_dir_all(&dir).expect("Failed to create covers directory");
    dir
}

// ---------------------------------------------------------------------------
// MusicBrainz API functions
// ---------------------------------------------------------------------------

/// Search MusicBrainz for releases matching the given artist and album name.
///
/// Returns a list of MusicBrainz release IDs (MBIDs) with score >= 80,
/// ordered by score descending. Returns up to 5 candidates so we can try
/// multiple releases for cover art.
///
/// Uses progressively looser search strategies while keeping the artist field
/// exact to avoid false matches:
/// 1. Exact Lucene field search: `artist:"X" AND release:"Y"`
/// 2. Retry with parenthetical/bracketed suffixes removed
///    (e.g., "If (Killing Eve)" -> "If")
/// 3. Progressively shorten cleaned titles by dropping trailing words
/// 4. Retry with artist aliases (e.g. "††† (Crosses)" -> "Crosses")
/// 5. Fallback via recording search when release search yields nothing
fn search_releases(client: &Client, artist: &str, album: &str) -> Vec<String> {
    let artist_variants = artist_search_variants(artist);
    let album_variants = album_search_variants(album);
    if artist_variants.is_empty() || album_variants.is_empty() {
        return vec![];
    }

    // Strategy 1/2/3: exact artist, then album variants.
    for (idx, variant) in album_variants.iter().enumerate() {
        if idx > 0 {
            eprintln!("  Retrying with alternate album name: \"{}\"...", variant);
            thread::sleep(RATE_LIMIT_DELAY);
        }
        let query = format!(
            "artist:\"{}\" AND release:\"{}\"",
            artist_variants[0], variant
        );
        let results = run_mb_search(client, &query);
        if !results.is_empty() {
            return results;
        }
    }

    // Strategy 4: retry with alternate artist aliases and relaxed score.
    for alt_artist in artist_variants.iter().skip(1) {
        eprintln!(
            "  Retrying with alternate artist name: \"{}\"...",
            alt_artist
        );
        for (idx, variant) in album_variants.iter().enumerate() {
            if idx > 0 {
                eprintln!("  Retrying with alternate album name: \"{}\"...", variant);
            }
            thread::sleep(RATE_LIMIT_DELAY);
            let query = format!("artist:\"{}\" AND release:\"{}\"", alt_artist, variant);
            let results = run_mb_search_with_min_score(client, &query, 60);
            if !results.is_empty() {
                return results;
            }
        }
    }

    // Strategy 5: fallback via recording search. This helps when metadata
    // contains track-ish album names like "If (Killing Eve)".
    let recording_query = album_variants
        .first()
        .cloned()
        .unwrap_or_else(|| album.to_string());
    eprintln!(
        "  Retrying via recording search for \"{}\"...",
        recording_query
    );
    thread::sleep(RATE_LIMIT_DELAY);
    let mut recording_results = run_mb_recording_search(
        client,
        &format!(
            "artist:\"{}\" AND recording:\"{}\"",
            artist_variants[0], recording_query
        ),
        60,
    );
    if !recording_results.is_empty() {
        return recording_results;
    }
    if let Some(shortest) = album_variants.last() {
        eprintln!(
            "  Retrying via recording search for shorter name \"{}\"...",
            shortest
        );
        thread::sleep(RATE_LIMIT_DELAY);
        recording_results = run_mb_recording_search(
            client,
            &format!(
                "artist:\"{}\" AND recording:\"{}\"",
                artist_variants[0], shortest
            ),
            50,
        );
        if !recording_results.is_empty() {
            return recording_results;
        }
    }

    // Strategy 6: title-only release search — no artist constraint.
    // Helps when the scrobbled artist doesn't match MusicBrainz release credits
    // (common for classical recordings credited to performers rather than composer).
    // Only try the first two variants (original + bracket-stripped): single-word
    // or two-word truncations are too broad without an artist anchor and risk
    // returning wrong results at score 85.
    for (idx, variant) in album_variants.iter().take(2).enumerate() {
        if idx > 0 {
            eprintln!(
                "  Retrying title-only with stripped name: \"{}\"...",
                variant
            );
        } else {
            eprintln!("  Retrying with title-only search (no artist constraint)...");
        }
        thread::sleep(RATE_LIMIT_DELAY);
        let query = format!("release:\"{}\"", variant);
        let results = run_mb_search_with_min_score(client, &query, 85);
        if !results.is_empty() {
            return results;
        }
    }

    vec![]
}

/// Generate search variants for an album title, ordered from strict to loose.
///
/// Examples:
/// - "If (Killing Eve)" -> ["If (Killing Eve)", "If"]
/// - "Purple Rain Deluxe Edition" ->
///   ["Purple Rain Deluxe Edition", "Purple Rain Deluxe", "Purple Rain", "Purple"]
fn album_search_variants(album: &str) -> Vec<String> {
    let original = trim_title_edges(&normalise_spaces(album));
    if original.is_empty() {
        return vec![];
    }

    let mut variants: Vec<String> = vec![original.clone()];

    // Remove parenthetical/bracketed suffixes like "(Deluxe)" or "[Remastered]".
    let stripped = trim_title_edges(&strip_bracketed_segments(&original));
    if !stripped.is_empty() && stripped != original {
        variants.push(stripped.clone());
    }

    // Progressively shorten non-empty cleaned candidates down to a single word.
    for base in [original, stripped] {
        if base.is_empty() {
            continue;
        }
        // Avoid generating malformed cut-offs like "If (Killing" by only
        // shortening titles that don't contain bracketed segments.
        if base.contains('(') || base.contains(')') || base.contains('[') || base.contains(']') {
            continue;
        }
        let words: Vec<&str> = base.split_whitespace().collect();
        if words.len() > 1 {
            for end in (1..words.len()).rev() {
                variants.push(words[..end].join(" "));
            }
        }
    }

    // Deduplicate while keeping insertion order.
    let mut unique: Vec<String> = Vec::new();
    for v in variants {
        let cleaned = trim_title_edges(&v);
        if !cleaned.is_empty() && !unique.iter().any(|u| u == &cleaned) {
            unique.push(cleaned);
        }
    }
    unique
}

/// Generate artist-name variants for better matching.
///
/// Example: "††† (Crosses)" -> ["††† (Crosses)", "†††", "Crosses"]
fn artist_search_variants(artist: &str) -> Vec<String> {
    let original = normalise_spaces(artist);
    if original.is_empty() {
        return vec![];
    }

    let mut variants = vec![original.clone()];

    // Prefer parenthetical alias if present.
    if let (Some(start), Some(end)) = (original.find('('), original.rfind(')'))
        && start < end
    {
        let before = trim_title_edges(&normalise_spaces(&original[..start]));
        if !before.is_empty() {
            variants.push(before);
        }
        let inside = normalise_spaces(&original[start + 1..end]);
        if !inside.is_empty() {
            variants.push(inside);
        }
    }

    // Keep alnum/space only as a final alias.
    let cleaned: String = original
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect();
    let cleaned = normalise_spaces(&cleaned);
    if !cleaned.is_empty() {
        variants.push(cleaned);
    }

    let mut unique = Vec::new();
    for v in variants {
        if !unique.iter().any(|u| u == &v) {
            unique.push(v);
        }
    }
    unique
}

/// Collapse all whitespace runs to single spaces and trim.
fn normalise_spaces(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Trim common punctuation/noise from both title ends.
fn trim_title_edges(s: &str) -> String {
    s.trim_matches(|c: char| {
        c.is_whitespace()
            || matches!(
                c,
                ',' | '.' | ';' | ':' | '!' | '?' | '-' | '_' | '"' | '\''
            )
    })
    .to_string()
}

/// Remove content in balanced `(...)` and `[...]` segments.
///
/// Unbalanced brackets are preserved as-is.
fn strip_bracketed_segments(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut paren_depth = 0i32;
    let mut square_depth = 0i32;

    for ch in s.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
            }
            ')' => {
                if paren_depth > 0 {
                    paren_depth -= 1;
                } else {
                    out.push(ch);
                }
            }
            '[' => {
                square_depth += 1;
            }
            ']' => {
                if square_depth > 0 {
                    square_depth -= 1;
                } else {
                    out.push(ch);
                }
            }
            _ => {
                if paren_depth == 0 && square_depth == 0 {
                    out.push(ch);
                }
            }
        }
    }

    normalise_spaces(&out)
}

/// Execute a MusicBrainz release search with the given query string.
/// Returns release IDs with score >= 80.
fn run_mb_search(client: &Client, query: &str) -> Vec<String> {
    run_mb_search_with_min_score(client, query, 80)
}

/// Execute a MusicBrainz release search with a configurable score floor.
fn run_mb_search_with_min_score(client: &Client, query: &str, min_score: u32) -> Vec<String> {
    let url = format!(
        "https://musicbrainz.org/ws/2/release/?query={}&fmt=json&limit=5",
        urlencoded(query)
    );

    let response = match client.get(&url).send() {
        Ok(r) => r,
        Err(_) => return vec![],
    };

    if !response.status().is_success() {
        eprintln!(
            "  [warn] MusicBrainz search returned HTTP {}",
            response.status()
        );
        return vec![];
    }

    let body: MbReleaseSearchResponse = match response.json() {
        Ok(b) => b,
        Err(_) => return vec![],
    };

    body.releases
        .into_iter()
        .filter(|r| r.score >= min_score)
        .map(|r| r.id)
        .collect()
}

/// Execute a MusicBrainz recording search and return release IDs from matches.
fn run_mb_recording_search(client: &Client, query: &str, min_score: u32) -> Vec<String> {
    let url = format!(
        "https://musicbrainz.org/ws/2/recording/?query={}&fmt=json&limit=10",
        urlencoded(query)
    );

    let response = match client.get(&url).send() {
        Ok(r) => r,
        Err(_) => return vec![],
    };

    if !response.status().is_success() {
        eprintln!(
            "  [warn] MusicBrainz recording search returned HTTP {}",
            response.status()
        );
        return vec![];
    }

    let body: MbRecordingSearchResponse = match response.json() {
        Ok(b) => b,
        Err(_) => return vec![],
    };

    let mut ids: Vec<String> = Vec::new();
    for rec in body.recordings.into_iter().filter(|r| r.score >= min_score) {
        for rel in rec.releases {
            if !ids.iter().any(|id| id == &rel.id) {
                ids.push(rel.id);
            }
        }
    }
    ids
}

/// Fetch release details from MusicBrainz, including genres/tags and the
/// release-group ID.
///
/// Returns `(genres_string, release_group_id)`.
fn fetch_release_details(client: &Client, mbid: &str) -> (Option<String>, Option<String>) {
    let url = format!(
        "https://musicbrainz.org/ws/2/release/{}?inc=genres+tags+release-groups&fmt=json",
        mbid
    );

    let response = match client.get(&url).send() {
        Ok(r) => r,
        Err(_) => return (None, None),
    };

    if !response.status().is_success() {
        return (None, None);
    }

    let details: MbReleaseDetails = match response.json() {
        Ok(d) => d,
        Err(_) => return (None, None),
    };

    // Extract release-group ID now so we can use it as a genre fallback.
    let rgid = details.release_group.map(|rg| rg.id);

    // Extract genres: prefer curated release genres, then release tags,
    // then release-group genres/tags.
    let mut genre = pick_genre_string(details.genres, details.tags);
    if genre.is_none()
        && let Some(ref release_group_id) = rgid
    {
        thread::sleep(RATE_LIMIT_DELAY);
        genre = fetch_release_group_genre(client, release_group_id);
    }

    (genre, rgid)
}

/// Fetch release-group genres/tags and return a genre string if available.
fn fetch_release_group_genre(client: &Client, release_group_id: &str) -> Option<String> {
    let url = format!(
        "https://musicbrainz.org/ws/2/release-group/{}?inc=genres+tags&fmt=json",
        release_group_id
    );

    let response = client.get(&url).send().ok()?;
    if !response.status().is_success() {
        return None;
    }

    let details: MbReleaseGroupDetails = response.json().ok()?;
    pick_genre_string(details.genres, details.tags)
}

/// Non-genre MusicBrainz tags to discard.
///
/// MusicBrainz community tags include technical metadata (encodings, years,
/// language names, format codes) that are meaningless as genre labels.
const TAG_BLOCKLIST: &[&str] = &[
    // Languages
    "english",
    "french",
    "german",
    "spanish",
    "italian",
    "portuguese",
    "japanese",
    "korean",
    "chinese",
    "russian",
    "swedish",
    "norwegian",
    "danish",
    "dutch",
    "polish",
    "finnish",
    "czech",
    "hungarian",
    "turkish",
    "arabic",
    "hebrew",
    "greek",
    "romanian",
    "ukrainian",
    "catalan",
    // Technical / format descriptors
    "isrc",
    "cd-text",
    "asin",
    "barcode",
    "album",
    "single",
    "ep",
    "compilation",
    "soundtrack",
    "live",
    "instrumental",
    "digital",
    "remaster",
    "remastered",
    "deluxe",
    "bonus",
    "stereo",
    "mono",
];

/// Return true if a MusicBrainz tag looks like a real genre label.
///
/// Rejects:
/// - Tags containing digits (years like "2022", encodings like "iso-8859-1")
/// - Known non-genre technical / language tags
fn is_genre_tag(tag: &str) -> bool {
    if tag.chars().any(|c| c.is_ascii_digit()) {
        return false;
    }
    let lower = tag.to_ascii_lowercase();
    !TAG_BLOCKLIST.contains(&lower.as_str())
}

/// Build a comma-separated genre string from MusicBrainz genre/tag lists.
///
/// Preference order:
/// 1. Curated `genres`
/// 2. Community `tags` (filtered to entries with at least one vote and
///    passing the genre-tag heuristic to strip technical metadata)
fn pick_genre_string(genres: Vec<MbGenre>, tags: Vec<MbTag>) -> Option<String> {
    let mut names: Vec<String> = genres.into_iter().map(|g| g.name).collect();
    if names.is_empty() {
        names = tags
            .into_iter()
            .filter(|t| t.count >= 1 && is_genre_tag(&t.name))
            .map(|t| t.name)
            .collect();
    }
    if names.is_empty() {
        None
    } else {
        Some(names.join(", "))
    }
}

/// Try to download cover art, using multiple fallback strategies:
///
/// 1. Try the specific release endpoint: `/release/{mbid}/front`
/// 2. If that fails and we have a release-group ID, try: `/release-group/{rgid}/front`
///    (this returns cover art from any edition of the album)
///
/// Returns the local file path on success, or None if no art is found anywhere.
fn download_cover_with_fallback(
    client: &Client,
    mbid: &str,
    release_group_id: Option<&str>,
    covers_dir: &Path,
) -> Option<String> {
    // Use the MBID as the filename regardless of which endpoint succeeds,
    // so we have a consistent cache key.
    let dest = covers_dir.join(format!("{}.jpg", mbid));

    // Skip download if we already have the file from a previous run.
    if dest.exists() {
        return Some(dest.to_string_lossy().to_string());
    }

    // Strategy 1: Try the specific release.
    let release_url = format!("https://coverartarchive.org/release/{}/front", mbid);
    if let Some(path) = try_download(client, &release_url, &dest) {
        eprintln!("  Cover downloaded (from release).");
        return Some(path);
    }

    // Strategy 2: Try the release-group (any edition of the album).
    if let Some(rgid) = release_group_id {
        let rg_url = format!("https://coverartarchive.org/release-group/{}/front", rgid);
        thread::sleep(RATE_LIMIT_DELAY);
        if let Some(path) = try_download(client, &rg_url, &dest) {
            eprintln!("  Cover downloaded (from release-group).");
            return Some(path);
        }
    }

    None
}

/// Decode image bytes, downscale if either dimension exceeds `MAX_COVER_DIM`,
/// and re-encode as JPEG at `COVER_JPEG_QUALITY`.
///
/// Returns the processed JPEG bytes, or `None` if decoding or encoding fails.
/// Images already within the size limit are still re-encoded to JPEG so that
/// format is consistent regardless of what the Cover Art Archive sent.
pub fn resize_cover_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let img = image::load_from_memory(bytes).ok()?;

    // Downscale only if the image exceeds the target dimension on either axis.
    let img = if img.width() > MAX_COVER_DIM || img.height() > MAX_COVER_DIM {
        img.resize(
            MAX_COVER_DIM,
            MAX_COVER_DIM,
            image::imageops::FilterType::Lanczos3,
        )
    } else {
        img
    };

    let mut out: Vec<u8> = Vec::new();
    img.write_with_encoder(JpegEncoder::new_with_quality(&mut out, COVER_JPEG_QUALITY))
        .ok()?;
    Some(out)
}

/// Attempt to download an image from the given URL and save it to `dest`.
/// The image is resized to at most `MAX_COVER_DIM` pixels per side before
/// being written, to keep cached covers from consuming excessive disk space.
/// Returns the local file path on success, or None on failure.
fn try_download(client: &Client, url: &str, dest: &Path) -> Option<String> {
    let response = client.get(url).send().ok()?;

    if !response.status().is_success() {
        return None;
    }

    let bytes = response.bytes().ok()?;

    let processed = match resize_cover_bytes(&bytes) {
        Some(b) => b,
        None => {
            eprintln!("  [warn] Could not decode cover image; skipping.");
            return None;
        }
    };

    let mut file = fs::File::create(dest).ok()?;
    file.write_all(&processed).ok()?;

    Some(dest.to_string_lossy().to_string())
}

// ---------------------------------------------------------------------------
// URL encoding helper
// ---------------------------------------------------------------------------

/// Simple URL encoding for query parameters.
/// Encodes characters that are not unreserved per RFC 3986.
fn urlencoded(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Main enrichment function
// ---------------------------------------------------------------------------

/// Enrich only the albums in `needed` that aren't already cached.
///
/// Used by `report --html` so we fetch covers only for what the report
/// will actually display, rather than the entire scrobble library.
pub fn run_enrich_targeted(
    conn: &Connection,
    needed: &std::collections::HashSet<(String, String)>,
    quiet: bool,
) {
    let all_uncached = match db::uncached_albums(conn) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[error] Failed to query uncached albums: {}", e);
            return;
        }
    };
    // Filter uncached list to only those the report needs.
    let albums: Vec<db::UncachedAlbum> = all_uncached
        .into_iter()
        .filter(|a| needed.contains(&(a.artist.clone(), a.album.clone())))
        .collect();
    run_enrich_albums(conn, albums, quiet);
}

/// Run the enrichment process: find all uncached albums and fetch their
/// metadata and cover art from MusicBrainz / Cover Art Archive.
///
/// Progress is printed to stderr so the user can see what's happening.
///
/// # Arguments
///
/// - `conn` — SQLite database connection
/// - `force` — if true, re-fetch all albums even if already cached
/// - `quiet` — if true, suppress the "nothing to do" hint (used when called
///   from `report --html` where the user didn't explicitly ask for enrichment)
pub fn run_enrich(conn: &Connection, force: bool, quiet: bool) {
    if force {
        conn.execute("DELETE FROM album_cache", [])
            .expect("Failed to clear album_cache");
        eprintln!("Cleared album cache (force mode).");
    }
    let albums = db::uncached_albums(conn).expect("Failed to query uncached albums");
    run_enrich_albums(conn, albums, quiet);
}

/// Inner enrichment loop shared by `run_enrich` and `run_enrich_targeted`.
fn run_enrich_albums(conn: &Connection, albums: Vec<db::UncachedAlbum>, quiet: bool) {
    if albums.is_empty() {
        if !quiet {
            eprintln!("All albums are already cached. Nothing to do.");
            eprintln!("Use --force to re-fetch everything.");
        }
        return;
    }

    let client = build_client();
    let covers = covers_dir();

    eprintln!("Found {} album(s) to enrich.", albums.len());

    let mut success_count = 0;
    let mut cover_count = 0;

    for (i, album) in albums.iter().enumerate() {
        eprintln!(
            "[{}/{}] {} - {}",
            i + 1,
            albums.len(),
            album.artist,
            album.album
        );

        // Step 1: Search MusicBrainz for matching releases.
        thread::sleep(RATE_LIMIT_DELAY);
        let candidates = search_releases(&client, &album.artist, &album.album);

        if candidates.is_empty() {
            eprintln!("  No match found on MusicBrainz.");

            // Cache a "no match" entry so we don't re-query next time.
            let entry = AlbumCacheEntry {
                artist: album.artist.clone(),
                album: album.album.clone(),
                musicbrainz_id: None,
                cover_url: None,
                genre: None,
                fetched_at: now_str(),
            };
            match db::upsert_album_cache(conn, &entry) {
                Ok(_) => success_count += 1,
                Err(e) => eprintln!("  [error] Failed to cache: {}", e),
            }
            continue;
        }

        // Use the best candidate (first in the list) for metadata.
        let primary_mbid = &candidates[0];
        eprintln!("  Found MBID: {}", primary_mbid);

        // Step 2: Fetch genres/tags and release-group ID from the primary release.
        thread::sleep(RATE_LIMIT_DELAY);
        let (genre, release_group_id) = fetch_release_details(&client, primary_mbid);
        if let Some(ref g) = genre {
            eprintln!("  Genres: {}", g);
        }

        // Step 3: Try to download cover art with fallback strategies.
        thread::sleep(RATE_LIMIT_DELAY);
        let mut cover = download_cover_with_fallback(
            &client,
            primary_mbid,
            release_group_id.as_deref(),
            &covers,
        );

        // Step 4: If still no cover, try other candidate releases.
        if cover.is_none() && candidates.len() > 1 {
            eprintln!(
                "  No cover from primary release, trying {} other candidate(s)...",
                candidates.len() - 1
            );
            for alt_mbid in &candidates[1..] {
                thread::sleep(RATE_LIMIT_DELAY);
                let alt_url = format!("https://coverartarchive.org/release/{}/front", alt_mbid);
                let dest = covers.join(format!("{}.jpg", primary_mbid));
                if let Some(path) = try_download(&client, &alt_url, &dest) {
                    eprintln!("  Cover downloaded (from alternate release {}).", alt_mbid);
                    cover = Some(path);
                    break;
                }
            }
        }

        if cover.is_some() {
            cover_count += 1;
        } else {
            eprintln!("  No cover art available from any source.");
        }

        // Step 5: Store the result in album_cache.
        let entry = AlbumCacheEntry {
            artist: album.artist.clone(),
            album: album.album.clone(),
            musicbrainz_id: Some(primary_mbid.clone()),
            cover_url: cover,
            genre,
            fetched_at: now_str(),
        };

        match db::upsert_album_cache(conn, &entry) {
            Ok(_) => success_count += 1,
            Err(e) => eprintln!("  [error] Failed to cache: {}", e),
        }
    }

    eprintln!();
    eprintln!("Enrichment complete:");
    eprintln!("  Albums processed: {}", albums.len());
    eprintln!("  Successfully cached: {}", success_count);
    eprintln!("  Covers downloaded: {}", cover_count);
    eprintln!("  Covers directory: {}", covers.display());
}

/// Manually pin a MusicBrainz release to an (artist, album) pair.
///
/// Fetches genres and cover art for `mbid`, then upserts the result into
/// `album_cache`, overwriting any existing entry. Called by `pin-album`.
pub fn enrich_by_mbid(
    conn: &Connection,
    artist: &str,
    album: &str,
    mbid: &str,
    cover_url_override: Option<&str>,
) {
    let client = build_client();
    let covers = covers_dir();

    eprintln!("Fetching release details for MBID {}...", mbid);
    let (genre, release_group_id) = fetch_release_details(&client, mbid);

    if let Some(ref g) = genre {
        eprintln!("  Genres: {}", g);
    }

    let cover = if let Some(url) = cover_url_override {
        // User supplied a direct image URL — download it using the MBID as the
        // filename so it fits into the same local cache as CAA covers.
        // We proceed even if MusicBrainz returned no metadata (genre/rgid):
        // the user explicitly asked for this cover, so MusicBrainz being
        // unreachable or the release having no genres is not a reason to bail.
        if genre.is_none() && release_group_id.is_none() {
            eprintln!(
                "  [warn] MusicBrainz returned no data for MBID {}. \
                 Proceeding with cover download only.",
                mbid
            );
        }
        eprintln!("  Downloading cover from provided URL...");
        let dest = covers.join(format!("{}.jpg", mbid));
        let result = try_download(&client, url, &dest);
        if result.is_some() {
            eprintln!("  Cover downloaded.");
        } else {
            eprintln!("  [warn] Failed to download cover from provided URL.");
        }
        result
    } else {
        // Without a user-supplied URL we depend entirely on MusicBrainz /
        // Cover Art Archive. If MB returned nothing the MBID is likely wrong.
        if genre.is_none() && release_group_id.is_none() {
            eprintln!(
                "  [error] No data returned for MBID {}. It may be invalid or not found.",
                mbid
            );
            return;
        }
        let result =
            download_cover_with_fallback(&client, mbid, release_group_id.as_deref(), &covers);
        if result.is_some() {
            eprintln!("  Cover downloaded.");
        } else {
            eprintln!("  No cover art available on Cover Art Archive.");
        }
        result
    };

    let entry = AlbumCacheEntry {
        artist: artist.to_string(),
        album: album.to_string(),
        musicbrainz_id: Some(mbid.to_string()),
        cover_url: cover,
        genre,
        fetched_at: now_str(),
    };

    match db::upsert_album_cache(conn, &entry) {
        Ok(_) => eprintln!("Pinned \"{}\" by \"{}\" to MBID {}.", album, artist, mbid),
        Err(e) => eprintln!("[error] Failed to update cache: {}", e),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_urlencoded_simple() {
        assert_eq!(urlencoded("hello"), "hello");
    }

    #[test]
    fn test_urlencoded_spaces_and_special() {
        // Spaces should be encoded as %20.
        assert_eq!(urlencoded("hello world"), "hello%20world");
        // Quotes should be encoded.
        assert_eq!(urlencoded("\"test\""), "%22test%22");
    }

    #[test]
    fn test_urlencoded_unicode() {
        // Unicode characters like † should be percent-encoded byte by byte.
        let encoded = urlencoded("†");
        assert!(encoded.starts_with('%'));
        // † is U+2020, encoded in UTF-8 as E2 80 A0.
        assert_eq!(encoded, "%E2%80%A0");
    }

    #[test]
    fn test_urlencoded_preserves_unreserved() {
        // RFC 3986 unreserved characters should not be encoded.
        assert_eq!(urlencoded("A-Z_a-z.0~9"), "A-Z_a-z.0~9");
    }

    #[test]
    fn test_strip_bracketed_segments() {
        assert_eq!(strip_bracketed_segments("If (Killing Eve)"), "If");
        assert_eq!(strip_bracketed_segments("Album [Deluxe Edition]"), "Album");
        assert_eq!(strip_bracketed_segments("Name (Live) [Remaster]"), "Name");
    }

    #[test]
    fn test_album_search_variants_parenthetical() {
        let variants = album_search_variants("If (Killing Eve)");
        assert_eq!(
            variants,
            vec!["If (Killing Eve)".to_string(), "If".to_string()]
        );
    }

    #[test]
    fn test_album_search_variants_progressive_shortening() {
        let variants = album_search_variants("Purple Rain Deluxe");
        assert_eq!(
            variants,
            vec![
                "Purple Rain Deluxe".to_string(),
                "Purple Rain".to_string(),
                "Purple".to_string(),
            ]
        );
    }

    #[test]
    fn test_artist_search_variants_parenthetical_alias() {
        let variants = artist_search_variants("††† (Crosses)");
        assert_eq!(
            variants,
            vec![
                "††† (Crosses)".to_string(),
                "†††".to_string(),
                "Crosses".to_string(),
            ]
        );
    }

    #[test]
    fn test_pick_genre_string_prefers_genres_then_tags() {
        let genre = pick_genre_string(
            vec![MbGenre {
                name: "trip hop".to_string(),
            }],
            vec![MbTag {
                name: "electronic".to_string(),
                count: 5,
            }],
        );
        assert_eq!(genre.as_deref(), Some("trip hop"));

        let from_tags = pick_genre_string(
            vec![],
            vec![
                MbTag {
                    name: "ambient".to_string(),
                    count: 0,
                },
                MbTag {
                    name: "electronic".to_string(),
                    count: 2,
                },
            ],
        );
        assert_eq!(from_tags.as_deref(), Some("electronic"));
    }

    #[test]
    fn test_uncached_albums_query() {
        // Verify the uncached_albums query works correctly.
        let conn = db::open_memory_db().unwrap();

        // Insert some scrobbles.
        db::insert_scrobble(
            &conn,
            &db::NewScrobble {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                title: "Digital Bath".into(),
                track_duration_secs: Some(291),
                played_duration_secs: 291,
                scrobbled_at: "2026-03-19T10:00:00".into(),
            },
        )
        .unwrap();
        db::insert_scrobble(
            &conn,
            &db::NewScrobble {
                artist: "††† (Crosses)".into(),
                album: "††† (Crosses)".into(),
                title: "Telepathy".into(),
                track_duration_secs: Some(215),
                played_duration_secs: 200,
                scrobbled_at: "2026-03-19T10:05:00".into(),
            },
        )
        .unwrap();

        // Both albums should be uncached initially.
        let uncached = db::uncached_albums(&conn).unwrap();
        assert_eq!(uncached.len(), 2);

        // Cache one album WITH a cover_url.
        db::upsert_album_cache(
            &conn,
            &db::AlbumCacheEntry {
                artist: "Deftones".into(),
                album: "White Pony".into(),
                musicbrainz_id: Some("test-id".into()),
                cover_url: Some("/path/to/cover.jpg".into()),
                genre: Some("alternative metal".into()),
                fetched_at: "2026-03-19T12:00:00".into(),
            },
        )
        .unwrap();

        // Now only one album should be uncached.
        let uncached = db::uncached_albums(&conn).unwrap();
        assert_eq!(uncached.len(), 1);
        assert_eq!(uncached[0].artist, "††† (Crosses)");

        // Cache the other album but WITHOUT a cover_url — it should still
        // appear as "uncached" so enrichment retries the cover download.
        db::upsert_album_cache(
            &conn,
            &db::AlbumCacheEntry {
                artist: "††† (Crosses)".into(),
                album: "††† (Crosses)".into(),
                musicbrainz_id: Some("test-id-2".into()),
                cover_url: None,
                genre: None,
                fetched_at: "2000-01-01T00:00:00".into(),
            },
        )
        .unwrap();

        // Should still show as uncached because cover_url is NULL.
        let uncached = db::uncached_albums(&conn).unwrap();
        assert_eq!(uncached.len(), 1);
        assert_eq!(uncached[0].artist, "††† (Crosses)");
    }

    #[test]
    fn test_upsert_album_cache_replaces() {
        // Verify that upserting the same (artist, album) replaces the old entry.
        let conn = db::open_memory_db().unwrap();

        let entry1 = AlbumCacheEntry {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            musicbrainz_id: Some("id-1".into()),
            cover_url: None,
            genre: Some("rock".into()),
            fetched_at: "2026-03-19T10:00:00".into(),
        };
        db::upsert_album_cache(&conn, &entry1).unwrap();

        // Upsert with different data for the same album.
        let entry2 = AlbumCacheEntry {
            artist: "Deftones".into(),
            album: "White Pony".into(),
            musicbrainz_id: Some("id-2".into()),
            cover_url: Some("/path/to/cover.jpg".into()),
            genre: Some("alternative metal, nu metal".into()),
            fetched_at: "2026-03-19T12:00:00".into(),
        };
        db::upsert_album_cache(&conn, &entry2).unwrap();

        // Should still be only one entry, with the updated data.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM album_cache", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);

        // Verify the MBID was updated.
        let mbid: String = conn
            .query_row(
                "SELECT musicbrainz_id FROM album_cache WHERE artist = 'Deftones'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(mbid, "id-2");
    }

    #[test]
    fn test_uncached_albums_respects_retry_cooldown() {
        let conn = db::open_memory_db().unwrap();

        db::insert_scrobble(
            &conn,
            &db::NewScrobble {
                artist: "Unloved".into(),
                album: "If (Killing Eve)".into(),
                title: "If".into(),
                track_duration_secs: Some(180),
                played_duration_secs: 160,
                scrobbled_at: "2026-03-19T10:00:00".into(),
            },
        )
        .unwrap();

        // Recent failed lookup should NOT be retried immediately.
        db::upsert_album_cache(
            &conn,
            &db::AlbumCacheEntry {
                artist: "Unloved".into(),
                album: "If (Killing Eve)".into(),
                musicbrainz_id: None,
                cover_url: None,
                genre: None,
                fetched_at: chrono::Local::now()
                    .naive_local()
                    .format("%Y-%m-%dT%H:%M:%S")
                    .to_string(),
            },
        )
        .unwrap();

        let uncached = db::uncached_albums(&conn).unwrap();
        assert!(uncached.is_empty());
    }
}
