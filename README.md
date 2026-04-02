# Dauphong Hydra Source Crawler

Crawler that generates and maintains `sources/dauphong.json` from torrents published by user **dauphong** on The Pirate Bay, in the format expected by [Hydra Launcher](https://github.com/hydralauncher/hydra).

## Features (V2)

- **3-tier HTTP backend** for Cloudflare bypass:
  1. [`curl_cffi`](https://pypi.org/project/curl_cffi/) — browser-grade TLS fingerprint impersonation (recommended)
  2. `curl.exe` via subprocess — WSL2 Windows networking stack fallback
  3. Python `requests` with rotating User-Agent and realistic headers
- **Automatic backend escalation**: if Cloudflare blocks one backend (403/503), the next tier is tried automatically. Once promoted, the script **stays on the working backend** for all subsequent pages — it does not reset to the primary backend on each new request.
- **Smart 429 handling**: rate-limit responses (429) retry the *same* backend up to `--max-retries` times (honoring `Retry-After`), then escalate to the next backend.
- **Exponential backoff & retry**: configurable retries per page with backoff on network/server errors.
- **Title sanitization**: iterative HTML entity decoding, control character removal, NFC unicode normalization, whitespace cleanup.
- **Seed-based pruning**: on subsequent runs, entries explicitly seen with 0 seeds are removed from the output.
- **Version dedup**: keeps only the N most recent uploads per game (groups by stripping version numbers; output titles are preserved as-is).
- **Auto-resume**: if a previous run was in-progress or interrupted (Ctrl+C), the next run automatically picks up from the last completed page. Use `--resume` to force resume regardless of recorded status.
- **Incremental save**: writes output atomically (via `.tmp` rename) after every page — never loses progress.
- **Merge by infohash**: safe to re-run periodically; updates existing entries, adds new ones.

## Output format

Compatible with the Hydra Launcher source standard:

```json
{
  "name": "Dauphong",
  "downloads": [
    {
      "title": "Game Title v1.2.3",
      "uris": ["magnet:?xt=urn:btih:...&dn=...&tr=..."],
      "uploadDate": "2024-11-05T00:00:00.000Z",
      "fileSize": "16.9 GB"
    }
  ]
}
```

## Meta file

`sources/dauphong_meta.json` tracks crawl state for resumption and observability:

```json
{
  "last_run": "2026-04-02T12:00:00+00:00",
  "last_completed_page": 230,
  "total_pages_known": 231,
  "total_entries": 49000,
  "pruned_zero_seed": 150,
  "deduped_entries": 14668,
  "http_backend": "curl_cffi",
  "run_status": "completed"
}
```

## Requirements

- Python 3.8+
- `requests` (required)
- `curl_cffi` (recommended — best Cloudflare bypass)

## Installation

```bash
python3 -m pip install -r requirements.txt

# Recommended: install curl_cffi for browser-grade TLS fingerprint
python3 -m pip install curl_cffi
```

## Usage

```bash
# Full crawl with auto-detection of best HTTP backend
python3 -u scripts/dauphong_crawler_v2.py --sleep 0.5

# Limit to first 5 pages (for testing)
python3 -u scripts/dauphong_crawler_v2.py --max-pages 5 --sleep 0.5

# Force a specific HTTP backend
python3 -u scripts/dauphong_crawler_v2.py --http-backend curl_cffi --sleep 0.5

# Resume interrupted crawl from a specific page
python3 -u scripts/dauphong_crawler_v2.py --start-page 106 --sleep 0.5

# Force-resume from last saved page (even if status is not 'interrupted')
python3 -u scripts/dauphong_crawler_v2.py --resume --sleep 0.5

# Keep all versions (disable dedup)
python3 -u scripts/dauphong_crawler_v2.py --max-versions 0 --sleep 0.5

# Keep entries even if they have 0 seeds
python3 -u scripts/dauphong_crawler_v2.py --no-prune --sleep 0.5
```

> **Auto-resume:** If the meta file shows `run_status: "in_progress"` (crash) or `"interrupted"` (Ctrl+C), the script automatically resumes from the last completed page. No need to pass `--start-page` manually. Use `--resume` to force resume regardless of status.

## Options

| Option | Default | Description |
|---|---|---|
| `--output, -o` | `sources/dauphong.json` | Output JSON file path |
| `--meta` | auto from `--output` | Meta JSON file path |
| `--start-page N` | `0` (auto-resume) | 0-indexed starting page |
| `--max-pages N` | unlimited | Max pages to fetch |
| `--sleep SECONDS` | `1.0` | Delay between page requests |
| `--max-versions N` | `3` | Keep only N most recent uploads per game (0 = disable) |
| `--max-retries N` | `3` | Max retries per page/request |
| `--max-consecutive-errors N` | `10` | Abort after N consecutive page failures |
| `--http-backend` | `auto` | Force backend: `auto`, `curl_cffi`, `curl_exe`, `requests` |
| `--no-prune` | off | Skip removal of entries seen with 0 seeds |
| `--resume` | off | Force resume from last saved page regardless of `run_status` |

## How version dedup works

The `--max-versions` feature groups entries by a normalized game name (stripping version numbers like `v1.2.3`, `Build 12345`, parenthetical tags, etc.) and keeps only the N most recent by `uploadDate`. **The original title is never modified** — dedup only affects which entries survive in the output.

Example: if there are 10 entries for "Factorio v1.0", "Factorio v1.1", ..., "Factorio v1.1.107", only the 3 most recently uploaded are kept.

## Notes

- Adjust `--sleep` to reduce server load and avoid rate-limiting (recommended: 0.5–1.0).
- The dauphong user has ~2000+ pages (~60k+ torrents). A full crawl may take a while.
- The script handles Ctrl+C gracefully: saves state, writes meta as "interrupted", and can auto-resume.
- On WSL2, `curl.exe` is detected automatically as a fallback when `curl_cffi` is not installed.

## V1 (legacy)

The original V1 crawler is preserved at `scripts/dauphong_crawler.py` for reference. V2 (`scripts/dauphong_crawler_v2.py`) is the recommended version.

## License

Use the crawler in accordance with local laws and the terms of service of the sites queried.
