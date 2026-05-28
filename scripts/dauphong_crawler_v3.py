#!/usr/bin/env python3
"""Dauphong Hydra Source Crawler — V3

Crawls torrents from user ``dauphong`` on The Pirate Bay (via apibay.org)
and produces a Hydra-Launcher-compatible JSON source file.

Fetches multiple pages in parallel — one per proxy — for maximum throughput.
Failed pages are retried on other proxies automatically.
"""

import re
import os
import sys
import json
import time
import queue
import base64
import html
import argparse
import threading
import unicodedata
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote_plus

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APIBAY_USER = "dauphong"

TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.demonii.com:1337",
    "udp://9.rarbg.com:2710/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "udp://opentracker.i2p.rocks:6969/announce",
    "udp://tracker.internetwarriors.net:1337/announce",
    "udp://tracker.leechers-paradise.org:6969/announce",
    "udp://coppersurfer.tk:6969/announce",
    "udp://tracker.zer0day.to:1337/announce",
]


# ---------------------------------------------------------------------------
# Utilities (same as V2)
# ---------------------------------------------------------------------------

def bytes_to_human(size_bytes):
    try:
        n = float(size_bytes)
    except (TypeError, ValueError):
        return str(size_bytes) if size_bytes else None
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024.0:
            s = f"{n:.1f}"
            if s.endswith(".0"):
                s = s[:-2]
            return f"{s} {unit}"
        n /= 1024.0
    s = f"{n:.1f}"
    if s.endswith(".0"):
        s = s[:-2]
    return f"{s} PB"


def build_magnet(info_hash, name):
    uri = f"magnet:?xt=urn:btih:{info_hash.upper()}&dn={quote_plus(name)}"
    for tr in TRACKERS:
        uri += f"&tr={quote_plus(tr)}"
    return uri


def normalize_upload_date(uploaded):
    if not uploaded:
        return None
    if uploaded.endswith("+00:00"):
        uploaded = uploaded[:-6] + ".000Z"
    elif uploaded.endswith("Z") and "." not in uploaded:
        uploaded = uploaded[:-1] + ".000Z"
    return uploaded


def normalize_infohash(h):
    if not h:
        return None
    s = h.strip()
    if re.fullmatch(r"[A-Fa-f0-9]{40}", s):
        return s.lower()
    s2 = s.upper()
    padding = "=" * ((8 - len(s2) % 8) % 8)
    try:
        b = base64.b32decode(s2 + padding)
        return b.hex()
    except Exception:
        return s.lower()


def sanitize_title(raw):
    if not raw:
        return raw
    text = raw
    while True:
        decoded = html.unescape(text)
        if decoded == text:
            break
        text = decoded
    text = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", text)
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r" {2,}", " ", text)
    text = text.strip()
    return text


def normalize_game_name(title):
    t = title
    t = re.sub(r"\s+v\d[\d.]*[a-z0-9._-]*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s+Build\s+\d+", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*[\(\[].*?[\)\]]", "", t)
    t = re.sub(r"\s+\d+\.\d[\d.]*$", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t.casefold()


# ---------------------------------------------------------------------------
# Proxy loading
# ---------------------------------------------------------------------------

def load_proxies(proxy_file):
    if not os.path.exists(proxy_file):
        sys.exit(f"[error] Proxy list not found: {proxy_file}")
    proxies = []
    with open(proxy_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                proxies.append(line)
    if not proxies:
        sys.exit(f"[error] No proxies found in {proxy_file}")
    return proxies


# ---------------------------------------------------------------------------
# Synchronous fetch (used by workers)
# ---------------------------------------------------------------------------

def fetch_page(url, proxy_url, timeout):
    """Fetch a single page through a proxy. Returns parsed JSON or None."""
    try:
        r = requests.get(
            url,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=timeout,
        )
        if r.status_code == 200:
            return r.json()
    except requests.RequestException:
        pass
    return None


# ---------------------------------------------------------------------------
# Apibay parser (same as V2)
# ---------------------------------------------------------------------------

def parse_apibay_page(data):
    entries = []
    for item in data:
        try:
            info_hash = item.get("info_hash") or item.get("infohash") or ""
            raw_name = item.get("name") or ""
            if not raw_name or info_hash == "0000000000000000000000000000000000000000":
                continue
            seeds = int(item.get("seeders") or item.get("seed") or 0)
            leechers = int(item.get("leechers") or item.get("leech") or 0)
            size = item.get("size")
            added = item.get("added")
            uploaded = None
            if added:
                try:
                    uploaded = datetime.fromtimestamp(int(added), tz=timezone.utc).isoformat().replace("+00:00", "Z")
                except Exception:
                    pass
            entries.append({
                "infohash_raw": info_hash,
                "name": raw_name,
                "seeds": seeds,
                "leechers": leechers,
                "size": size,
                "uploaded_raw": uploaded,
            })
        except Exception:
            continue
    return entries


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def load_existing(output_path):
    if not os.path.exists(output_path):
        return {}
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        result = {}
        for d in data.get("downloads", []):
            uris = d.get("uris", [])
            if uris:
                m = re.search(r"urn:btih:([A-Fa-f0-9]+)", uris[0], re.I)
                if m:
                    ih = m.group(1).lower()
                    if "title" in d:
                        d["title"] = sanitize_title(d["title"])
                    result[ih] = d
        return result
    except Exception:
        return {}


def sort_downloads(downloads):
    groups = defaultdict(list)
    for d in downloads:
        groups[d["title"]].append(d)
    result = []
    for title in sorted(groups.keys(), key=str.casefold):
        group = groups[title]
        group.sort(key=lambda x: x.get("uploadDate") or "", reverse=True)
        result.extend(group)
    return result


def write_json(path, downloads):
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"name": "Dauphong", "downloads": downloads}, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Dedup & pruning
# ---------------------------------------------------------------------------

def dedup_downloads(downloads, max_versions):
    if max_versions <= 0:
        return downloads, 0
    groups = defaultdict(list)
    for d in downloads:
        key = normalize_game_name(d["title"])
        groups[key].append(d)
    kept = []
    removed = 0
    for key in groups:
        entries = groups[key]
        entries.sort(key=lambda x: x.get("uploadDate") or "", reverse=True)
        kept.extend(entries[:max_versions])
        removed += max(0, len(entries) - max_versions)
    kept = sort_downloads(kept)
    return kept, removed


def prune_zero_seed(accumulated, seen_this_run):
    to_remove = [ih for ih, seeds in seen_this_run.items() if seeds <= 0 and ih in accumulated]
    for ih in to_remove:
        del accumulated[ih]
    return len(to_remove)


# ---------------------------------------------------------------------------
# Parallel crawl
# ---------------------------------------------------------------------------

def crawl(output_path, proxy_file, *, max_pages=None, max_versions=3,
          timeout=30, no_prune=False, save_interval=10):
    # ── proxies ─────────────────────────────────────────────────────────
    proxies = load_proxies(proxy_file)
    n_proxies = len(proxies)
    print(f"[crawl] Loaded {n_proxies} proxies from {proxy_file!r}")

    # ── load existing data ──────────────────────────────────────────────
    accumulated = load_existing(output_path)
    print(f"[crawl] {len(accumulated)} existing entries loaded from {output_path!r}")

    # ── get total page count (single request, first proxy) ──────────────
    data = fetch_page(
        f"https://apibay.org/q.php?q=pcnt:{APIBAY_USER}",
        proxies[0], timeout,
    )
    total_pages = None
    if data is not None:
        if isinstance(data, str):
            total_pages = int(data)
        elif isinstance(data, list) and data:
            total_pages = int(data[0].get("pages") or data[0].get("page_count") or 0)
        elif isinstance(data, (int, float)):
            total_pages = int(data)
    if total_pages:
        print(f"[apibay] Total pages: {total_pages}")
    else:
        sys.exit("[apibay] Could not determine total pages — aborting.")

    # Apply max_pages limit
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)
        print(f"[crawl] Limited to {total_pages} pages by --max-pages.")

    if total_pages <= 0:
        print("[crawl] No pages to fetch.")
        return

    # ── page queue ──────────────────────────────────────────────────────
    page_queue = queue.Queue()
    for page_idx in range(total_pages):
        page_queue.put(page_idx)

    # ── shared state ────────────────────────────────────────────────────
    lock = threading.Lock()
    seen_this_run = {}
    pages_done = 0
    pages_failed = 0
    new_total = 0
    upd_total = 0
    should_stop = threading.Event()

    # Track which proxy is assigned to which thread (for logging)
    thread_proxy_map = {}

    def worker(proxy_url):
        """Fetch pages from queue until empty."""
        nonlocal pages_done, pages_failed, new_total, upd_total

        proxy_dict = {"http": proxy_url, "https": proxy_url}
        worker_pages_done = 0
        worker_pages_failed = 0

        while not should_stop.is_set():
            try:
                page_idx = page_queue.get(timeout=1)
            except queue.Empty:
                break

            if page_idx == 0:
                url = f"https://apibay.org/q.php?q=user:{APIBAY_USER}"
            else:
                url = f"https://apibay.org/q.php?q=user:{APIBAY_USER}:{page_idx}"

            try:
                r = requests.get(url, proxies=proxy_dict, timeout=timeout)
            except requests.RequestException as exc:
                worker_pages_failed += 1
                # Re-queue for another proxy to try (infinite retry across all proxies)
                page_queue.put(page_idx)
                page_queue.task_done()
                continue

            if r.status_code != 200:
                worker_pages_failed += 1
                page_queue.put(page_idx)
                page_queue.task_done()
                continue

            try:
                page_data = r.json()
            except Exception:
                worker_pages_failed += 1
                page_queue.put(page_idx)
                page_queue.task_done()
                continue

            page_entries = parse_apibay_page(page_data)

            if page_entries:
                w_new = 0
                w_upd = 0
                w_skip = 0

                for e in page_entries:
                    ih = normalize_infohash(e.get("infohash_raw") or "")
                    if not ih:
                        continue
                    seeds = e.get("seeds", 0)

                    title = sanitize_title(e.get("name") or "")
                    if not title:
                        continue

                    uri = build_magnet(ih, title)
                    uploaded = normalize_upload_date(e.get("uploaded_raw"))
                    file_size = bytes_to_human(e.get("size"))

                    entry = {"title": title, "uris": [uri]}
                    if uploaded:
                        entry["uploadDate"] = uploaded
                    if file_size:
                        entry["fileSize"] = file_size

                    with lock:
                        seen_this_run[ih] = seeds
                        if ih in accumulated:
                            w_upd += 1
                        else:
                            w_new += 1
                        accumulated[ih] = entry

                with lock:
                    new_total += w_new
                    upd_total += w_upd
                    pages_done += 1

            worker_pages_done += 1
            page_queue.task_done()

        # Worker finished — report
        with lock:
            pages_failed += worker_pages_failed

    # ── launch workers ──────────────────────────────────────────────────
    n_workers = min(n_proxies, total_pages)
    print(f"[crawl] Launching {n_workers} workers for {total_pages} pages...")

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = []
        for i in range(n_workers):
            proxy = proxies[i % n_proxies]
            f = executor.submit(worker, proxy)
            futures.append(f)

        # Monitor loop — periodic saves and progress reporting
        start_time = time.monotonic()
        last_save = 0
        while any(not f.done() for f in futures):
            time.sleep(2)
            elapsed = time.monotonic() - start_time
            with lock:
                d = pages_done
                remaining = page_queue.qsize()
            pct = (d / total_pages * 100) if total_pages else 0
            rate = (d / elapsed) if elapsed > 0 else 0
            eta = ((total_pages - d) / rate) if rate > 0 else 0
            print(f"  [{d}/{total_pages}] {pct:.0f}% | {rate:.1f} pg/s | ETA {eta:.0f}s | queue: {remaining}")

            # Periodic save
            if d > 0 and d - last_save >= save_interval:
                with lock:
                    downloads = sort_downloads(list(accumulated.values()))
                write_json(output_path, downloads)
                last_save = d
                print(f"  [save] {len(downloads)} entries written.")

        # Drain remaining futures
        for f in futures:
            f.result()

    # ── final stats ─────────────────────────────────────────────────────
    elapsed = time.monotonic() - start_time
    print(f"\n[crawl] Finished in {elapsed:.1f}s — {pages_done} pages, {pages_failed} failed retries")
    print(f"  +{new_total} new, ~{upd_total} updated, total: {len(accumulated)}")

    # ── post-crawl: pruning ─────────────────────────────────────────────
    pruned_count = 0
    if not no_prune and seen_this_run:
        pruned_count = prune_zero_seed(accumulated, seen_this_run)
        if pruned_count:
            print(f"[prune] Removed {pruned_count} entries with 0 seeds.")

    # ── post-crawl: dedup ───────────────────────────────────────────────
    downloads = sort_downloads(list(accumulated.values()))
    deduped_count = 0
    if max_versions > 0:
        downloads, deduped_count = dedup_downloads(downloads, max_versions)
        if deduped_count:
            print(f"[dedup] Removed {deduped_count} older versions (keeping max {max_versions} per game).")

    # ── final write ─────────────────────────────────────────────────────
    write_json(output_path, downloads)
    print(f"[crawl] Done. Final entries: {len(downloads)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_proxy = os.path.join(script_dir, "proxy_list")

    parser = argparse.ArgumentParser(
        description="Crawl dauphong torrents in parallel and generate a Hydra-Launcher-compatible JSON source.",
    )
    parser.add_argument("--output", "-o", default="sources/dauphong.json",
                        help="Output JSON file path (default: sources/dauphong.json)")
    parser.add_argument("--proxy-file", default=default_proxy,
                        help=f"Path to proxy list file (default: {default_proxy})")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Max pages to fetch (default: unlimited)")
    parser.add_argument("--max-versions", type=int, default=3,
                        help="Keep only N most recent uploads per game. 0 = disable (default: 3)")
    parser.add_argument("--timeout", type=int, default=30,
                        help="HTTP request timeout in seconds (default: 30)")
    parser.add_argument("--no-prune", action="store_true",
                        help="Skip removal of entries seen with 0 seeds this run")
    parser.add_argument("--save-interval", type=int, default=10,
                        help="Save results every N completed pages (default: 10)")

    args = parser.parse_args()

    crawl(
        output_path=args.output,
        proxy_file=args.proxy_file,
        max_pages=args.max_pages,
        max_versions=args.max_versions,
        timeout=args.timeout,
        no_prune=args.no_prune,
        save_interval=args.save_interval,
    )


if __name__ == "__main__":
    main()
