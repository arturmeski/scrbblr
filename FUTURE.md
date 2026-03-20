# Future Ideas

## Genre Enrichment Fallback

- Add a Last.fm genre fallback when MusicBrainz returns no usable genre data.
- Keep MusicBrainz as primary source; use Last.fm only as a secondary source.
- Add API key configuration (environment variable + CLI/docs guidance).
- Cache Last.fm-derived genres in `album_cache` and mark source for transparency.
- Keep existing cooldown behaviour to avoid repeated API calls.
