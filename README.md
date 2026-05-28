# Dauphong Hydra Source Crawler

Crawler that generates and maintains `sources/dauphong.json` from torrents published by user **dauphong** on The Pirate Bay, in the format expected by [Hydra Launcher](https://github.com/hydralauncher/hydra).

## Versions

| Script | Approach | Speed |
|---|---|---|
| `dauphong_crawler_v3.py` | **Parallel** — one worker per proxy fetching different pages simultaneously | Fastest (~10x) |
| `dauphong_crawler_v2.py` | **Sequential** — single proxy with rotation on error | Normal |
| `dauphong_crawler.py` | V1 legacy (kept for reference) | Slow |

## Features (V2 & V3)

- **Proxy rotation**: reads a `proxy_list` file with HTTP proxies. V2 rotates on error; V3 distributes pages across proxies in parallel.
- **Automatic failover**: if a proxy fails (timeout, connection error, non-200), switches to the next one. V3 re-queues failed pages for another proxy to pick up.
- **Title sanitization**: iterative HTML entity decoding, control character removal, NFC unicode normalization, whitespace cleanup.
- **Seed-based pruning**: on subsequent runs, entries explicitly seen with 0 seeds are removed from the output.
- **Version dedup**: keeps only the N most recent uploads per game (groups by stripping version numbers; output titles are preserved as-is).
- **Incremental save**: writes output atomically (via `.tmp` rename) — never loses progress. V2 saves every page; V3 saves every `--save-interval` pages (default 10).
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

## Proxy list

Create a `scripts/proxy_list` file with one HTTP proxy per line:

```
http://38.154.203.95:5863
http://198.105.121.200:6462
http://64.137.96.74:6641
```

Lines starting with `#` are ignored, blank lines are skipped.

## Requirements

- Python 3.8+
- `requests`

## Installation

```bash
python3 -m pip install -r requirements.txt
```

## Usage

### V3 — Parallel (recommended)

```bash
# Full crawl with all proxies working in parallel
python3 -u scripts/dauphong_crawler_v3.py

# Limit to first 50 pages (for testing)
python3 -u scripts/dauphong_crawler_v3.py --max-pages 50

# Keep all versions (disable dedup)
python3 -u scripts/dauphong_crawler_v3.py --max-versions 0

# Keep entries even if they have 0 seeds
python3 -u scripts/dauphong_crawler_v3.py --no-prune

# Custom proxy file and output
python3 -u scripts/dauphong_crawler_v3.py --proxy-file /path/to/proxies.txt --output custom.json
```

### V2 — Sequential

```bash
# Full crawl with proxy rotation on error
python3 -u scripts/dauphong_crawler_v2.py --sleep 0.5

# Limit to first 5 pages (for testing)
python3 -u scripts/dauphong_crawler_v2.py --max-pages 5

# Custom proxy file
python3 -u scripts/dauphong_crawler_v2.py --proxy-file /path/to/proxies.txt
```

## Options

### V3

| Option | Default | Description |
|---|---|---|
| `--output, -o` | `sources/dauphong.json` | Output JSON file path |
| `--proxy-file` | `scripts/proxy_list` | Path to proxy list file |
| `--max-pages N` | unlimited | Max pages to fetch |
| `--max-versions N` | `3` | Keep only N most recent uploads per game (0 = disable) |
| `--timeout SECONDS` | `30` | HTTP request timeout per page |
| `--no-prune` | off | Skip removal of entries seen with 0 seeds |
| `--save-interval N` | `10` | Save results every N completed pages |

### V2

| Option | Default | Description |
|---|---|---|
| `--output, -o` | `sources/dauphong.json` | Output JSON file path |
| `--proxy-file` | `scripts/proxy_list` | Path to proxy list file |
| `--max-pages N` | unlimited | Max pages to fetch |
| `--sleep SECONDS` | `1.0` | Delay between page requests |
| `--max-versions N` | `3` | Keep only N most recent uploads per game (0 = disable) |
| `--max-consecutive-errors N` | `10` | Abort after N consecutive page failures |
| `--timeout SECONDS` | `30` | HTTP request timeout per page |
| `--no-prune` | off | Skip removal of entries seen with 0 seeds |

## How version dedup works

The `--max-versions` feature groups entries by a normalized game name (stripping version numbers like `v1.2.3`, `Build 12345`, parenthetical tags, etc.) and keeps only the N most recent by `uploadDate`. **The original title is never modified** — dedup only affects which entries survive in the output.

Example: if there are 10 entries for "Factorio v1.0", "Factorio v1.1", ..., "Factorio v1.1.107", only the 3 most recently uploaded are kept.

## Notes

- V3 is the recommended version — it uses all proxies in parallel for maximum speed.
- V2 is simpler but sequential; useful when you have few proxies or want less parallelism.
- The dauphong user has ~2000+ pages (~60k+ torrents). V3 with 10 proxies can do a full crawl in a few minutes.
- The more proxies you have, the faster V3 runs. Minimum recommended: 5 proxies.

## V1 (legacy)

The original V1 crawler is preserved at `scripts/dauphong_crawler.py` for reference.

## License

Use the crawler in accordance with local laws and the terms of service of the sites queried.
