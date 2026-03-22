# scrbblr

A local music scrobbler for MPRIS-compatible players, built in Rust. It tracks what you listen to via `playerctl`, stores scrobble data in a local SQLite database, and generates listening reports.

## Features

- **Accurate play time tracking** — monitors both metadata changes and play/pause status, so paused time doesn't count toward scrobble thresholds
- **Last.fm-style scrobble rules** — a track is scrobbled after 50% of its duration or 4 minutes of play, whichever is shorter
- **Local storage** — all data stays on your machine in a single SQLite file
- **Reports** — terminal tables or JSON output, filterable by time period (today, week, month, year, all time)
- **Adaptive terminal tables** — report columns shrink to fit narrower terminal widths
- **HTML reports** — generate a standalone dark-themed HTML file with album art cards
- **Terminal-style typography** — HTML uses JetBrains Mono with monospace fallbacks
- **Fair ranking tie-breaks** — top artists/albums/tracks are ranked by plays first, then total listen time
- **Album visuals + bars** — HTML shows both large album cover grids and top-album bar tables
- **Genre stats** — reports include top genres (plays + listen time) when metadata is available
- **Mood labels by period** — each report section highlights up to 6 dominant genres
- **Mobile jump menu** — sticky section links (Today/Week/Month/All Time) reduce scrolling on phones
- **Enrichment** — fetch album covers + genres from MusicBrainz/Cover Art Archive
- **Incremental publish helper** — query latest scrobble and publish only when new data exists
- **Configurable player** — defaults to `com.blitzfc.qbz`, configurable via `--player`

## Requirements

- [playerctl](https://github.com/altdesktop/playerctl)
- Rust toolchain (for building)

## Installation

### Quick install (interactive)

The repo includes an interactive install script that builds the binary, installs it,
sets up the systemd service, and starts it — asking before each step:

```bash
./install.sh
```

### Manual install

Build and install the binary into `~/.local/bin`:

```bash
cargo build --release
install -Dm755 target/release/scrbblr ~/.local/bin/scrbblr
```

Make sure `~/.local/bin` is in your `PATH`:

```bash
command -v scrbblr
```

If that prints nothing, add this to your shell profile (`~/.bashrc`, `~/.zshrc`, etc.):

```bash
export PATH="$HOME/.local/bin:$PATH"
```

Then open a new shell and verify:

```bash
scrbblr --help
```

## Autostart (recommended)

Use a **systemd user service** so scrobbling starts automatically when you log in.

### 1) Install the service unit

Copy the provided unit file from the repo:

```bash
mkdir -p ~/.config/systemd/user
cp contrib/systemd/user/scrbblr.service ~/.config/systemd/user/
```

If needed, edit the player in the service (`--player com.blitzfc.qbz`).

### 2) Enable and start

```bash
systemctl --user daemon-reload
systemctl --user enable --now scrbblr.service
```

### 3) Verify

```bash
systemctl --user status scrbblr.service
journalctl --user -u scrbblr.service -f
```

### Optional: keep running without active login

If you want user services to keep running after logout/reboot (without an active shell login), enable lingering:

```bash
loginctl enable-linger "$USER"
```

### Important session note

`playerctl` and MPRIS are tied to the session D-Bus. The scrobbler must run in the **same user/session** as your player. If the player runs under another account/session, this service will not see it.

## Report workflow

Recommended workflow:

- Keep only `watch` running as a service.
- Generate HTML reports manually whenever you want:

```bash
scrbblr report --html --output ~/music-report
```

This creates a self-contained directory:

```
~/music-report/
├── index.html       # Open this in a browser
└── covers/
    ├── <mbid1>.jpg
    ├── <mbid2>.jpg
    └── ...
```

When `--html` is used, the tool automatically runs enrichment first (for missing albums) so covers/genres are available in the generated report.

## Building

```bash
cargo build --release
```

The binary will be at `target/release/scrbblr`.

## Usage

### Scrobbling

Start the watcher to begin recording what you listen to:

```bash
# Default player (com.blitzfc.qbz)
scrbblr watch

# Specify a different player
scrbblr watch --player spotify
```

The watcher runs in the foreground and logs scrobbles to stderr:

```
Database: /home/user/.local/share/scrbblr/scrobbles.db
Watching player: com.blitzfc.qbz
[scrobbled] ††† (Crosses) - This Is a Trick (186s)
[scrobbled] ††† (Crosses) - Telepathy (200s)
```

Press `Ctrl+C` to stop. The last track will be evaluated before shutdown.

### Reports

All-time sections default to 2.5x the `--limit` value (rounded to nearest 5)
for a broader view; shorter periods use the limit as-is. Default limit is 10,
giving 25 for all-time. Use `--all-time-limit` to override explicitly.

Top lists use this ordering logic:

- Primary: number of plays (descending)
- Secondary: total listen time (descending)
- Final stable tie-break: name fields (artist/album/title)

In HTML reports, album covers are shown in fixed full rows (6 columns on desktop, 3 on tablet, 2 on mobile). For cleaner layout, the cover grid rounds the visual cover count up to the next full desktop row when enough albums exist.

The HTML head loads JetBrains Mono from Google Fonts and falls back to local monospace fonts when offline.

```bash
# All-time summary with top artists, albums, genres, tracks
scrbblr report

# Filter by period
scrbblr report --period today
scrbblr report --period week
scrbblr report --period month
scrbblr report --period year

# JSON output
scrbblr report --json

# HTML output to stdout (no covers)
scrbblr report --html

# HTML output to directory (with covers)
scrbblr report --html --output ~/music-report

# Change the number of entries in top-N lists (default: 20)
scrbblr report --limit 20
```

Example terminal output:

```
=== Scrobble Report: This Week (2026-03-13 → 2026-03-19) ===

+-------------------+----------+
| Metric            | Value    |
+-------------------+----------+
| Total scrobbles   | 142      |
| Total listen time | 8h 23m   |
| Unique artists    | 31       |
| Unique albums     | 47       |
| Unique tracks     | 98       |
+-------------------+----------+

Top Artists
+---+----------------+-------+-------------+
| # | Artist         | Plays | Listen Time |
+---+----------------+-------+-------------+
| 1 | ††† (Crosses)  | 23    | 1h 12m      |
| 2 | Deftones       | 18    | 1h 05m      |
+---+----------------+-------+-------------+
```

### Options

```
scrbblr watch [OPTIONS]
    --player <NAME>      Player name for playerctl [default: com.blitzfc.qbz]
    --db-path <PATH>     Path to the SQLite database

scrbblr report [OPTIONS]
    --period <PERIOD>    today, week, month, year, all [default: all]
    --json               Output as JSON
    --html               Output as standalone HTML
    --output <PATH>      Write HTML report to this directory (index.html + covers/)
    --limit <LIMIT>      Number of entries in top-N lists [default: 10]
    --all-time-limit <N> Override all-time top-N limit [default: 2.5x --limit]
    --db-path <PATH>     Path to the SQLite database

scrbblr enrich [OPTIONS]
    --force              Re-fetch metadata for all albums
    --db-path <PATH>     Path to the SQLite database

scrbblr last-scrobble [OPTIONS]
    --db-path <PATH>     Path to the SQLite database
```

### Enrichment (covers + genres)

Enrichment is automatic for `report --html`, but you can still run it manually if you want to prefetch metadata:

```bash
scrbblr enrich
```

When MusicBrainz matching is tricky, enrichment now retries with normalised
album variants (e.g. strips parenthetical suffixes like `(Killing Eve)`, then
progressively shortens trailing words), tries artist aliases for symbol-heavy
names, and falls back to recording search before giving up.

Genre extraction order:

1. Release `genres`
2. Release `tags`
3. Release-group `genres`
4. Release-group `tags`

Automatic enrichment (triggered by `report --html`) uses a retry cooldown for
incomplete cache entries (missing cover or missing genre): those entries are
re-tried after 7 days, not on every report run.

Use force mode when you want immediate backfill/refresh for everything:

```bash
scrbblr enrich --force
```

Genre normalisation notes:

- Genre labels are currently passed through from MusicBrainz with light cleanup only.
- We split comma-separated values and trim spaces.
- For aggregation, hyphen/space variants are grouped (e.g. `post-rock` + `post rock`).
- When both forms exist, the spaced form is preferred for display.
- Album cards display at most 3 genre pills for readability.
- Top Genre and Mood sections aggregate using that normalised grouping.

Downloaded covers are stored in:

`~/.local/share/scrbblr/covers/`

#### Manually pinning an album (`pin-album`)

Sometimes automatic search fails — most commonly for classical recordings where
the scrobbled artist tag (e.g. a choir or soloist) doesn't match the release
credits on MusicBrainz, or where the album title wording differs significantly.
Enrich will print:

```
No match found on MusicBrainz.
```

When that happens you can pin the correct MusicBrainz release manually:

1. Search for the release on [musicbrainz.org](https://musicbrainz.org).
2. Open the release page. The MBID is the UUID in the URL:
   `https://musicbrainz.org/release/`**`f2ff907a-0355-451b-9c68-f0b7c09bb145`**
3. Copy the exact artist and album strings from the `enrich` output (the
   `[N/M] Artist - Album` line) and run:

```bash
scrbblr pin-album \
  --artist "Coro della Radiotelevisione Svizzera" \
  --album  "Vivaldi: Gloria; Nisi Dominus; Nulla in mundo pax" \
  --mbid   "f2ff907a-0355-451b-9c68-f0b7c09bb145"
```

This fetches genres and cover art for that specific release and stores them in
the local cache, overwriting any previous (failed) entry. Re-run `enrich` or
`report --html` afterwards to pick up the result.

If the Cover Art Archive has no image for the release (the command prints
"No cover art available"), supply one with `--cover-url`:

```bash
scrbblr pin-album \
  --artist    "Coro della Radiotelevisione Svizzera" \
  --album     "Vivaldi: Gloria; Nisi Dominus; Nulla in mundo pax" \
  --mbid      "f2ff907a-0355-451b-9c68-f0b7c09bb145" \
  --cover-url "https://example.com/cover.jpg"
```

The image is downloaded, resized to 500 px, and stored locally just like a
Cover Art Archive image. Any HTTPS image URL works — Discogs, Wikipedia,
Bandcamp, etc.

### Incremental publish script

If you publish the report to a remote host, use the included helper script:

```bash
./scrbblr-publish.sh
```

It runs `report --html` and `rsync` only when a newer scrobble exists.
The script tracks the last published scrobble timestamp in:

`$XDG_STATE_HOME/scrbblr/last-published-scrobble.txt`

Set defaults in a config file so you can run the script without passing flags:

`~/.config/scrbblr/publish.conf`

If you use `./install.sh`, an example config is installed there automatically
when the file does not already exist.

Example:

```bash
OUTPUT_DIR="$HOME/music-report"
REMOTE_TARGET="user@host:/var/www/music-report"
DB_PATH=""
```

Legacy fallback is also supported:

`~/.scrbblr-publish.conf`

Flags:

```bash
./scrbblr-publish.sh --output ~/music-report --remote user@host:/var/www/music-report
./scrbblr-publish.sh --db-path /custom/path/scrobbles.db

# Keep running and check every 5 minutes (default interval):
./scrbblr-publish.sh --watch

# Custom interval (seconds):
./scrbblr-publish.sh --watch --interval 600

# Force regeneration even when no new scrobbles exist:
./scrbblr-publish.sh --force
```

The installer also places this helper in `~/.local/bin` as:

`scrbblr-publish`

## Data storage

Scrobbles are stored in SQLite at `~/.local/share/scrbblr/scrobbles.db` (respects `$XDG_DATA_HOME`).

Each scrobble records:

| Field | Description |
|-------|-------------|
| artist | Artist name |
| album | Album name |
| title | Track title |
| track_duration_secs | Full track duration in seconds |
| played_duration_secs | Actual time spent listening |
| scrobbled_at | ISO 8601 timestamp |

## How it works

The watcher spawns two `playerctl --follow` processes:

1. **Metadata follower** — emits a line each time the track changes
2. **Status follower** — emits `Playing`, `Paused`, or `Stopped` on state changes

A state machine accumulates play time only while the player is in `Playing` state. When a new track starts (or the player stops), the previous track is evaluated against the scrobble threshold and recorded if it qualifies.

## Troubleshooting

### Service is running but nothing is scrobbled

Check the player name:

```bash
playerctl -l
```

If needed, edit `~/.config/systemd/user/scrbblr.service` and change `--player ...`, then reload/restart:

```bash
systemctl --user daemon-reload
systemctl --user restart scrbblr.service
```

### Player is in another account/session

MPRIS is session-scoped. The service must run in the same account/session as the player process.

### Check logs

```bash
journalctl --user -u scrbblr.service -n 200
journalctl --user -u scrbblr.service -f
```

### Verify database is being written

```bash
scrbblr report --period today
```

## Licence

MIT
