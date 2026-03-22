# AGENTS.md — Coding Agent Guidelines

## Project Overview

**scrbblr** is a Rust CLI tool that scrobbles music from MPRIS-compatible
players and MPD on Linux. It stores data in SQLite, generates terminal/JSON/HTML
reports, and enriches album metadata from MusicBrainz/Cover Art Archive.

Single binary, no async, flat module structure. Rust 2024 edition.

## Build / Test / Lint

```bash
cargo build                          # debug build
cargo build --release                # release build
cargo test                           # run all tests (currently 63)
cargo test <name>                    # single test, e.g. cargo test test_top_artists
cargo test --lib db::tests           # all tests in one module
cargo test -- --nocapture            # show println/eprintln output
cargo fmt                            # format (default rustfmt, no config file)
cargo clippy                         # lint (no clippy.toml)
```

Always run `cargo fmt`, `cargo test`, and `cargo build` before committing.
There is no CI pipeline — local verification is the only gate.

## Module Structure

All source lives in `src/`. Flat layout, no nested modules.

| File         | Role                                                        |
|--------------|-------------------------------------------------------------|
| `main.rs`    | CLI (clap derive), subcommand dispatch, process management  |
| `db.rs`      | SQLite schema, all queries, data structs                    |
| `watcher.rs` | playerctl event parsing, ScrobbleTracker state machine      |
| `report.rs`  | Terminal tables, JSON output, HTML report + CSS generation  |
| `enrich.rs`  | MusicBrainz/CAA API, cover downloads, genre extraction      |

Database access goes through `db.rs` public functions only — no raw SQL elsewhere.

## Language & Spelling

Use **British English** everywhere: comments, docs, README, variable names.

- `normalise` not `normalize`
- `behaviour` not `behavior`
- `licence` not `license`
- `colour` not `color` (in prose; CSS property names stay American per spec)

## Code Style

### Formatting

Default `rustfmt` (no `rustfmt.toml`). 4-space indent, trailing commas in
multi-line expressions. Run `cargo fmt` before every commit.

### Naming

- Functions: `snake_case` — `open_memory_db`, `parse_metadata_line`
- Types/Structs: `PascalCase` — `ScrobbleTracker`, `TopArtist`
- Enums: `PascalCase` type + variants — `Event::Metadata`, `PlayerStatus::Playing`
- Constants: `SCREAMING_SNAKE_CASE` — `DEFAULT_PLAYER`, `RATE_LIMIT_DELAY`
- Tests: `test_` prefix — `test_top_artists`, `test_schema_creation`

### Imports

No strict grouping enforced. General pattern: `crate::` imports, external crates,
then `std`. Let `cargo fmt` handle ordering within groups.

```rust
use crate::db;
use rusqlite::Connection;
use serde::Serialize;
use std::fmt::Write as _;
```

### Section Separators

Use box-drawing comment blocks between major sections:

```rust
// ---------------------------------------------------------------------------
// Section Name
// ---------------------------------------------------------------------------
```

### Documentation

- Module-level `//!` doc comments at top of every `.rs` file.
- `///` doc comments on all public functions and important private functions.
- Inline `//` comments explaining "why" not just "what".
- Comments should be thorough — this codebase favours extensive documentation.

## Error Handling

**Layered approach — no panics in production code paths:**

1. **`Result<T>` + `?`** in library functions (all `db.rs` queries, `gather_report`, etc.)
2. **`eprintln!` + `std::process::exit(1)`** for fatal CLI errors in `main.rs`
3. **`.expect("descriptive message")`** only for truly unrecoverable cases
   (missing `HOME`, failed dir creation)
4. **`.unwrap()` only in test code** — never in production paths
5. **`match` on `Result`** with `eprintln!` for non-fatal errors (failed cache writes, etc.)

All user-facing progress/error output goes to **stderr** via `eprintln!`.
Only report content (tables, JSON, HTML) goes to stdout.

Logging prefixes: `[scrobbled]`, `[error]`, `[warn]`, or unprefixed for progress.

## Rust 2024 Edition Features

The codebase uses edition 2024 features freely:

- Let chains: `if let Some(x) = foo && condition { ... }`
- `is_none_or()` method
- Other recently stabilised APIs

## Test Patterns

Every source file has `#[cfg(test)] mod tests` at the bottom.

### Key test helpers

- `db::open_memory_db()` — in-memory SQLite with schema. Used by all modules.
- `db::tests::seed_db(conn)` — populates 5 test scrobbles across two artists/days.
- `watcher::TestableTracker` — simulated clock (`advance_time(secs)`) + scrobble
  collection for unit-testing the state machine without real time.

### Test conventions

- Each test creates its own `open_memory_db()` — no shared state.
- `.unwrap()` is fine in tests (panic = test failure).
- Use `.into()` for string literals in test data: `artist: "Deftones".into()`.
- Descriptive names: `test_top_genres_merges_hyphen_and_space_variants`.

## Key Architectural Notes

- **No async** — uses `reqwest::blocking` and `std::thread`.
- **No logging crate** — direct `eprintln!` calls throughout.
- **No template engine** — HTML built via custom `HtmlWriter` helper in `report.rs`.
- **No unsafe code** except one `libc::ioctl` for terminal width (`#[cfg(unix)]`).
- **XDG paths** — DB at `$XDG_DATA_HOME/scrbblr/scrobbles.db`, covers in
  `covers/` subdirectory. State marker for publish in `$XDG_STATE_HOME`.
- MusicBrainz rate limiting: 1-second delay between API calls (`RATE_LIMIT_DELAY`).
- HTML report is fully self-contained (relative `covers/` paths, no external JS).
- Font loaded from Google Fonts CDN with monospace fallback chain for offline use.

## Files Outside src/

| File                          | Purpose                                    |
|-------------------------------|--------------------------------------------|
| `install.sh`                  | Interactive installer (binary + service)   |
| `uninstall.sh`                | Interactive uninstaller                    |
| `scrbblr-publish.sh`  | Incremental report publish (rsync)         |
| `contrib/examples/publish.conf.example` | Config template for publish      |
| `contrib/systemd/user/`       | systemd user service unit                  |
| `FUTURE.md`                   | Parking lot for future feature ideas       |
