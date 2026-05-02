#!/usr/bin/env python3
"""Dauphong Hydra Source Crawler — V2

Crawls torrents from user ``dauphong`` on The Pirate Bay (via apibay.org)
and produces a Hydra-Launcher-compatible JSON source file.

Improvements over V1
---------------------
* 3-tier HTTP backend: curl_cffi → curl.exe (WSL2) → requests
* Exponential backoff + backend escalation on Cloudflare blocks
* Title sanitization (HTML entities, control chars, trailing spaces)
* Seed-based pruning: removes entries explicitly seen with 0 seeds
* Keep only the N most recent uploads per game (version-aware grouping)
* Enhanced meta with auto-resume on interrupted runs
"""

import re
import os
import sys
import json
import time
import shutil
import signal
import argparse
import base64
import html
import random
import subprocess
import unicodedata
from datetime import datetime, timezone
from collections import defaultdict
from urllib.parse import quote_plus

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

# Realistic User-Agent pool for the requests fallback tier.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# Utility helpers (proven from V1)
# ---------------------------------------------------------------------------

def bytes_to_human(size_bytes):
    """Convert bytes (int or str) to a human-readable string like '16.9 GB'."""
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
    """Build a full magnet URI with standard trackers."""
    uri = f"magnet:?xt=urn:btih:{info_hash.upper()}&dn={quote_plus(name)}"
    for tr in TRACKERS:
        uri += f"&tr={quote_plus(tr)}"
    return uri


def normalize_upload_date(uploaded):
    """Ensure upload date is ISO 8601 with milliseconds: ``2023-06-30T23:00:00.000Z``."""
    if not uploaded:
        return None
    if uploaded.endswith("+00:00"):
        uploaded = uploaded[:-6] + ".000Z"
    elif uploaded.endswith("Z") and "." not in uploaded:
        uploaded = uploaded[:-1] + ".000Z"
    return uploaded


def normalize_infohash(h):
    """Normalize an info-hash to lowercase 40-char hex."""
    if not h:
        return None
    s = h.strip()
    if re.fullmatch(r"[A-Fa-f0-9]{40}", s):
        return s.lower()
    # Try base32 decode (some APIs return base32-encoded hashes)
    s2 = s.upper()
    padding = "=" * ((8 - len(s2) % 8) % 8)
    try:
        b = base64.b32decode(s2 + padding)
        return b.hex()
    except Exception:
        return s.lower()


# ---------------------------------------------------------------------------
# Phase 3 — Title sanitization
# ---------------------------------------------------------------------------

def sanitize_title(raw):
    """Clean a torrent title of encoding artefacts.

    Pipeline:
    1. Iterative HTML-entity decoding (handles double-encoding)
    2. Remove control characters (< 0x20, 0x7F–0x9F)
    3. NFC unicode normalization
    4. Collapse multiple spaces → single space
    5. Strip leading/trailing whitespace
    """
    if not raw:
        return raw
    # 1 — iterative HTML unescape
    text = raw
    while True:
        decoded = html.unescape(text)
        if decoded == text:
            break
        text = decoded
    # 2 — remove control characters
    text = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", text)
    # 3 — NFC normalization
    text = unicodedata.normalize("NFC", text)
    # 4 — collapse whitespace
    text = re.sub(r" {2,}", " ", text)
    # 5 — strip
    text = text.strip()
    return text


# ---------------------------------------------------------------------------
# Phase 5 — Game-name normalization (for dedup grouping ONLY)
# ---------------------------------------------------------------------------

def normalize_game_name(title):
    """Derive a grouping key from a title by stripping versions/builds/tags.

    This is used **only** for deciding which entries belong to the same game
    when applying the ``--max-versions`` limit.  The output title is NEVER
    modified by this function.
    """
    t = title
    # Strip version-like patterns:  v1.2.3, v1.2.3a, v1.2.3-hotfix, v1.2
    t = re.sub(r"\s+v\d[\d.]*[a-z0-9._-]*", "", t, flags=re.IGNORECASE)
    # Strip "Build 12345" / "b12345"
    t = re.sub(r"\s+Build\s+\d+", "", t, flags=re.IGNORECASE)
    # Strip parenthetical/bracket suffixes: (Early Access), [GOG], (x64)
    t = re.sub(r"\s*[\(\[].*?[\)\]]", "", t)
    # Strip trailing bare version numbers:  "Game 1.2.3" → "Game"
    t = re.sub(r"\s+\d+\.\d[\d.]*$", "", t)
    # Collapse whitespace, strip, casefold
    t = re.sub(r"\s+", " ", t).strip()
    return t.casefold()


# ---------------------------------------------------------------------------
# Phase 1 — HTTP backends
# ---------------------------------------------------------------------------

class FetchResult:
    """Minimal response wrapper returned by all backends."""
    __slots__ = ("status_code", "data", "headers")

    def __init__(self, status_code, data=None, headers=None):
        self.status_code = status_code
        self.data = data
        self.headers = headers or {}

    def json(self):
        return self.data


class _BackendCurlCffi:
    """Tier 1: curl_cffi — browser-grade TLS fingerprint."""
    name = "curl_cffi"

    def __init__(self):
        from curl_cffi.requests import Session  # noqa: F811
        self._session = Session(impersonate="chrome")

    def fetch_json(self, url, timeout=15):
        r = self._session.get(url, timeout=timeout)
        data = None
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                pass
        return FetchResult(r.status_code, data, dict(r.headers))


class _BackendCurlExe:
    """Tier 2: curl.exe via subprocess — WSL2 Windows networking stack."""
    name = "curl_exe"

    def __init__(self):
        self._exe = shutil.which("curl.exe")
        if not self._exe:
            raise RuntimeError("curl.exe not found in PATH")

    def fetch_json(self, url, timeout=15):
        cmd = [self._exe, "-s", "-w", "\n%{http_code}", "--max-time", str(timeout), url]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        output = proc.stdout.rstrip("\r\n")
        last_nl = output.rfind("\n")
        if last_nl == -1:
            code = int(output) if output.isdigit() else 0
            return FetchResult(code)
        body = output[:last_nl]
        code_str = output[last_nl + 1 :]
        code = int(code_str) if code_str.isdigit() else 0
        data = None
        if code == 200 and body:
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                pass
        return FetchResult(code, data)


class _BackendRequests:
    """Tier 3: Python requests with rotating UA and realistic headers."""
    name = "requests"

    def __init__(self):
        import requests as _req  # noqa: F811
        self._session = _req.Session()

    def _headers(self):
        ua = random.choice(USER_AGENTS)
        return {
            "User-Agent": ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://thepiratebay.org/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
        }

    def fetch_json(self, url, timeout=15):
        import requests as _req  # noqa: F811
        try:
            r = self._session.get(url, headers=self._headers(), timeout=timeout)
        except _req.RequestException:
            raise
        data = None
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                pass
        return FetchResult(r.status_code, data, dict(r.headers))


# Ordered list of backend classes to try (best → worst).
_BACKEND_CLASSES = [_BackendCurlCffi, _BackendCurlExe, _BackendRequests]
_BACKEND_MAP = {cls.name: cls for cls in _BACKEND_CLASSES}


def _create_backend(preference="auto"):
    """Instantiate the best available HTTP backend.

    Returns ``(primary_backend, fallback_list)`` where *fallback_list*
    contains backends that can be tried on Cloudflare escalation.
    """
    if preference != "auto":
        cls = _BACKEND_MAP.get(preference)
        if cls is None:
            sys.exit(f"[error] Unknown backend: {preference!r}")
        try:
            backend = cls()
        except Exception as exc:
            sys.exit(f"[error] Cannot initialise backend {preference!r}: {exc}")
        # Build fallback list from everything after the chosen one
        idx = _BACKEND_CLASSES.index(cls)
        fallbacks = []
        for fb_cls in _BACKEND_CLASSES[idx + 1 :]:
            try:
                fallbacks.append(fb_cls())
            except Exception:
                pass
        return backend, fallbacks

    # Auto-detect: try each in order
    primary = None
    fallbacks = []
    for cls in _BACKEND_CLASSES:
        try:
            inst = cls()
            if primary is None:
                primary = inst
            else:
                fallbacks.append(inst)
        except Exception:
            pass

    if primary is None:
        sys.exit("[error] No HTTP backend available. Install 'requests' at minimum.")
    return primary, fallbacks


# ---------------------------------------------------------------------------
# Phase 2 — Fetch with retry + backend escalation
# ---------------------------------------------------------------------------

def fetch_json_robust(url, backend_pool, start_idx=0, *, timeout=15, max_retries=5):
    """Fetch JSON with retries, exponential backoff, and **persistent** backend escalation.

    Parameters
    ----------
    url         : URL to fetch.
    backend_pool: Ordered list of all backends (best → worst).
    start_idx   : Index into *backend_pool* to start from.  The caller should
                  save the returned index and pass it on the next call so the
                  same backend is reused until it fails hard.

    Returns
    -------
    ``(FetchResult, backend_idx)`` where *backend_idx* is the index of the
    backend that produced the result.  On success it is the backend that
    returned HTTP 200; on complete failure it is the index of the last backend
    tried.  Always pass this value as *start_idx* on the next call.

    Escalation rules
    ----------------
    * **429**: wait ``Retry-After`` seconds, retry the **same** backend up to
      *max_retries* times, then escalate to the next backend.
    * **403 / 503**: escalate to the next backend immediately (Cloudflare block).
    * **Network error / other HTTP**: retry up to *max_retries* times with
      exponential backoff, then escalate to next backend.
    """
    last_result = FetchResult(0)

    for i in range(start_idx, len(backend_pool)):
        backend = backend_pool[i]
        base_delay = 2.0
        hard_attempts = 0  # counts non-429 failures for this backend

        while True:  # retry loop for this backend
            try:
                result = backend.fetch_json(url, timeout=timeout)
            except Exception as exc:
                hard_attempts += 1
                print(f"  [{backend.name}] network error ({hard_attempts}/{max_retries}): {exc}")
                if hard_attempts >= max_retries:
                    print(f"  [{backend.name}] max retries reached — escalating to next backend")
                    break  # escalate
                time.sleep(min(base_delay * (2 ** (hard_attempts - 1)), 120))
                continue

            last_result = result

            if result.status_code == 200:
                return result, i  # ← caller saves this index

            if result.status_code == 429:
                hard_attempts += 1
                retry_after = int(result.headers.get("Retry-After", 60))
                retry_after = max(30, min(retry_after, 300))
                if hard_attempts >= max_retries:
                    print(f"  [{backend.name}] 429 rate-limited {hard_attempts}x — escalating to next backend")
                    break
                print(f"  [{backend.name}] 429 rate-limited — waiting {retry_after}s ({hard_attempts}/{max_retries})")
                time.sleep(retry_after)
                continue

            if result.status_code in (403, 503):
                print(f"  [{backend.name}] HTTP {result.status_code} (Cloudflare block) — escalating to next backend")
                break  # escalate immediately

            # Other HTTP errors — retry with backoff, then escalate
            hard_attempts += 1
            if hard_attempts >= max_retries:
                print(f"  [{backend.name}] HTTP {result.status_code}, max retries reached — escalating to next backend")
                break
            delay = min(base_delay * (2 ** (hard_attempts - 1)), 120)
            print(f"  [{backend.name}] HTTP {result.status_code} — retrying in {delay:.0f}s ({hard_attempts}/{max_retries})")
            time.sleep(delay)

    # All backends exhausted
    return last_result, len(backend_pool) - 1


# ---------------------------------------------------------------------------
# Apibay page parser
# ---------------------------------------------------------------------------

def parse_apibay_page(data):
    """Parse a list of items from apibay JSON into raw entry dicts.

    Returns ALL entries (including seeds==0) so the caller can decide
    what to keep and what to mark for pruning.
    """
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
# I/O helpers
# ---------------------------------------------------------------------------

def load_existing(output_path):
    """Load existing downloads as ``{lowercase_infohash: entry_dict}``."""
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
                    # Sanitize titles from previous crawls (migration)
                    if "title" in d:
                        d["title"] = sanitize_title(d["title"])
                    result[ih] = d
        return result
    except Exception:
        return {}


def load_meta(meta_path):
    """Load meta file, returning empty dict if missing/corrupt."""
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def sort_downloads(downloads):
    """Group by title, sort each group by uploadDate desc, flatten alphabetically."""
    groups = defaultdict(list)
    for d in downloads:
        groups[d["title"]].append(d)
    result = []
    for title in sorted(groups.keys(), key=str.casefold):
        group = groups[title]
        group.sort(key=lambda x: x.get("uploadDate") or "", reverse=True)
        result.extend(group)
    return result


def _atomic_write(path, data_dict):
    """Write *data_dict* as JSON atomically (via .tmp rename)."""
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data_dict, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def write_json(output_path, downloads):
    """Write the Hydra-Launcher-compatible JSON output."""
    _atomic_write(output_path, {"name": "Dauphong", "downloads": downloads})


def write_meta(meta_path, meta):
    """Write the meta file atomically."""
    _atomic_write(meta_path, meta)


def write_history(history_path, entry):
    """Append *entry* to the history log (list-of-objects JSON)."""
    if os.path.exists(history_path):
        try:
            with open(history_path, "r", encoding="utf-8") as f:
                history = json.load(f)
            if not isinstance(history, list):
                history = []
        except Exception:
            history = []
    else:
        history = []
    history.append(entry)
    _atomic_write(history_path, history)


def load_titles_snapshot(snapshot_path):
    """Load the title set saved at the end of the last completed run.

    Returns a ``set`` of title strings, or ``None`` if no snapshot exists.
    """
    if not os.path.exists(snapshot_path):
        return None
    try:
        with open(snapshot_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(data)
    except Exception:
        pass
    return None


def write_titles_snapshot(snapshot_path, titles):
    """Persist the published title set for diffing in the next completed run."""
    _atomic_write(snapshot_path, sorted(titles))


# ---------------------------------------------------------------------------
# Phase 5 — Dedup (keep top N per game)
# ---------------------------------------------------------------------------

def dedup_downloads(downloads, max_versions):
    """Keep only the *max_versions* most recent entries per game.

    Grouping uses ``normalize_game_name()`` on the title — the title in the
    output is **never** modified.  Returns ``(filtered_list, removed_count)``.
    """
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

    # Re-sort for consistent output
    kept = sort_downloads(kept)
    return kept, removed


# ---------------------------------------------------------------------------
# Phase 4 — Seed-based pruning
# ---------------------------------------------------------------------------

def prune_zero_seed(accumulated, seen_this_run):
    """Remove entries that were seen in the current run with 0 seeds.

    *accumulated*: ``{infohash: entry_dict}`` — modified in-place.
    *seen_this_run*: ``{infohash: seed_count}``

    Entries NOT seen this run are kept (partial-crawl safety).
    Returns the number of entries removed.
    """
    to_remove = [ih for ih, seeds in seen_this_run.items() if seeds <= 0 and ih in accumulated]
    for ih in to_remove:
        del accumulated[ih]
    return len(to_remove)


# ---------------------------------------------------------------------------
# Main crawl loop
# ---------------------------------------------------------------------------

def crawl(
    output_path,
    meta_path,
    *,
    max_pages=None,
    sleep_between=1.0,
    start_page=0,
    resume=False,
    max_versions=3,
    max_retries=5,
    max_consecutive_errors=10,
    http_backend="auto",
    no_prune=False,
):
    # ── backends ──────────────────────────────────────────────────────────
    primary, fallbacks = _create_backend(http_backend)
    backend_pool = [primary] + list(fallbacks)
    current_backend_idx = 0
    _backend_names = ", ".join(b.name for b in backend_pool)
    print(f"[crawl] HTTP backend pool: {_backend_names}")

    # ── load existing data ────────────────────────────────────────────────
    accumulated = load_existing(output_path)
    print(f"[crawl] {len(accumulated)} existing entries loaded from {output_path!r}.")

    # Derive companion file paths
    _base = os.path.splitext(output_path)[0]
    history_path = _base + "_history.json"
    snapshot_path = _base + "_titles_snapshot.json"

    # initial_titles comes from the last *completed* run's snapshot so that
    # interrupted runs (which write non-deduped incremental data to the JSON)
    # don't pollute the diff.  Fallback to accumulated on the very first run.
    initial_titles = load_titles_snapshot(snapshot_path)
    if initial_titles is None:
        initial_titles = {e["title"] for e in accumulated.values()}
        print(f"[history] No snapshot found — using accumulated titles as baseline ({len(initial_titles)}).")
    else:
        print(f"[history] Snapshot loaded: {len(initial_titles)} titles as baseline for this run.")

    # ── meta / auto-resume ────────────────────────────────────────────────
    meta = load_meta(meta_path)
    # Auto-resume: trigger when --resume flag is set, OR run was left in a
    # non-completed state (in_progress = crash, interrupted = Ctrl+C).
    resumable_statuses = {"in_progress", "interrupted"}
    should_resume = resume or (start_page == 0 and meta.get("run_status") in resumable_statuses)
    if should_resume and start_page == 0:
        resume_page = meta.get("last_completed_page", -1) + 1
        if resume_page > 0:
            prev_status = meta.get("run_status", "unknown")
            print(f"[crawl] Resuming from page {resume_page + 1} (previous status: {prev_status!r}).")
            start_page = resume_page

    # Mark run as in-progress
    meta.update({
        "run_status": "in_progress",
        "last_run": datetime.now(timezone.utc).isoformat(),
        "http_backend": primary.name,
    })
    write_meta(meta_path, meta)

    # ── get total page count ──────────────────────────────────────────────
    total_pages = meta.get("total_pages_known")
    try:
        pcnt_result, current_backend_idx = fetch_json_robust(
            f"https://apibay.org/q.php?q=pcnt:{APIBAY_USER}",
            backend_pool, current_backend_idx, timeout=15, max_retries=max_retries,
        )
        if pcnt_result.status_code == 200 and pcnt_result.data is not None:
            pcnt_data = pcnt_result.data
            if isinstance(pcnt_data, str):
                total_pages = int(pcnt_data) or total_pages
            elif isinstance(pcnt_data, list) and pcnt_data:
                total_pages = int(pcnt_data[0].get("pages") or pcnt_data[0].get("page_count") or 0) or total_pages
            elif isinstance(pcnt_data, (int, float)):
                total_pages = int(pcnt_data) or total_pages
        if total_pages:
            print(f"[apibay] Total pages: {total_pages}")
            meta["total_pages_known"] = total_pages
            write_meta(meta_path, meta)
        else:
            print("[apibay] Could not determine total pages — will crawl until empty page.")
    except Exception as exc:
        print(f"[apibay] pcnt fetch error: {exc}")

    # ── pagination loop ───────────────────────────────────────────────────
    page_index = start_page
    pages_fetched = 0
    consecutive_errors = 0
    aborted_on_error = False
    seen_this_run = {}  # {infohash: seed_count} — for pruning

    # Graceful Ctrl+C: save state before exit
    interrupted = False

    def _on_sigint(sig, frame):
        nonlocal interrupted
        interrupted = True
        print("\n[crawl] Interrupt received — finishing current page then saving state...")

    prev_handler = signal.signal(signal.SIGINT, _on_sigint)

    try:
        while not interrupted:
            if max_pages is not None and pages_fetched >= max_pages:
                print(f"[apibay] --max-pages={max_pages} limit reached.")
                break
            if total_pages is not None and page_index >= total_pages:
                print(f"[apibay] All {total_pages} pages processed.")
                break

            if page_index == 0:
                url = f"https://apibay.org/q.php?q=user:{APIBAY_USER}"
            else:
                url = f"https://apibay.org/q.php?q=user:{APIBAY_USER}:{page_index}"

            print(f"[apibay] Page {page_index + 1}: {url}")

            prev_backend_idx = current_backend_idx
            result, current_backend_idx = fetch_json_robust(
                url, backend_pool, current_backend_idx, timeout=15, max_retries=max_retries
            )
            if current_backend_idx != prev_backend_idx:
                print(f"  [crawl] Backend switched: {backend_pool[prev_backend_idx].name} → {backend_pool[current_backend_idx].name} (will stay on new backend)")
                meta["http_backend"] = backend_pool[current_backend_idx].name

            if result.status_code != 200 or result.data is None:
                consecutive_errors += 1
                print(f"[apibay] Page {page_index + 1} failed (HTTP {result.status_code}). Consecutive errors: {consecutive_errors}/{max_consecutive_errors}")
                if consecutive_errors >= max_consecutive_errors:
                    print(f"[apibay] Max consecutive errors reached — aborting crawl.")
                    aborted_on_error = True
                    break
                if consecutive_errors % 3 == 0:
                    print(f"[apibay] Pausing 5 minutes after {consecutive_errors} consecutive errors...")
                    time.sleep(300)
                else:
                    time.sleep(sleep_between * 3)
                page_index += 1
                pages_fetched += 1
                continue

            consecutive_errors = 0

            page_entries = parse_apibay_page(result.data)
            if not page_entries:
                print("[apibay] Empty page — stopping pagination.")
                break

            # ── merge ─────────────────────────────────────────────────────
            new_count = 0
            upd_count = 0
            skipped_no_seed = 0

            for e in page_entries:
                ih = normalize_infohash(e.get("infohash_raw") or "")
                if not ih:
                    continue

                seeds = e.get("seeds", 0)
                # Track every infohash seen for pruning decisions later
                seen_this_run[ih] = seeds

                if seeds <= 0:
                    skipped_no_seed += 1
                    continue

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

                if ih in accumulated:
                    upd_count += 1
                else:
                    new_count += 1
                accumulated[ih] = entry

            print(f"  +{new_count} new, ~{upd_count} updated, -{skipped_no_seed} no-seed, total: {len(accumulated)}")

            # ── incremental save after each page ──────────────────────────
            downloads = sort_downloads(list(accumulated.values()))
            write_json(output_path, downloads)
            meta.update({
                "last_completed_page": page_index,
                "total_entries": len(downloads),
                "last_run": datetime.now(timezone.utc).isoformat(),
            })
            write_meta(meta_path, meta)

            page_index += 1
            pages_fetched += 1

            if not interrupted:
                time.sleep(sleep_between)

    finally:
        signal.signal(signal.SIGINT, prev_handler)

    # ── post-crawl: pruning ───────────────────────────────────────────────
    pruned_count = 0
    if not no_prune and seen_this_run:
        pruned_count = prune_zero_seed(accumulated, seen_this_run)
        if pruned_count:
            print(f"[prune] Removed {pruned_count} entries with 0 seeds.")

    # ── post-crawl: dedup ─────────────────────────────────────────────────
    downloads = sort_downloads(list(accumulated.values()))
    deduped_count = 0
    if max_versions > 0:
        downloads, deduped_count = dedup_downloads(downloads, max_versions)
        if deduped_count:
            print(f"[dedup] Removed {deduped_count} older versions (keeping max {max_versions} per game).")

    # ── final write ───────────────────────────────────────────────────────
    write_json(output_path, downloads)
    status = "interrupted" if interrupted else "error" if aborted_on_error else "completed"
    meta.update({
        "run_status": status,
        "last_run": datetime.now(timezone.utc).isoformat(),
        "total_entries": len(downloads),
        "pruned_zero_seed": pruned_count,
        "deduped_entries": deduped_count,
        "last_completed_page": page_index - 1 if pages_fetched > 0 else meta.get("last_completed_page", -1),
    })
    write_meta(meta_path, meta)

    # ── history log (only on full successful run) ─────────────────────────
    if status == "completed":
        final_titles = {d["title"] for d in downloads}
        titles_added = sorted(final_titles - initial_titles)
        titles_removed = sorted(initial_titles - final_titles)
        history_entry = {
            "timestamp": meta["last_run"],
            "run_status": status,
            "total_entries": len(downloads),
            "total_pages_crawled": pages_fetched,
            "total_pages_known": total_pages,
            "http_backend": backend_pool[current_backend_idx].name,
            "pruned_zero_seed": pruned_count,
            "deduped_entries": deduped_count,
            "entries_added": len(titles_added),
            "entries_removed": len(titles_removed),
            "titles_added": titles_added,
            "titles_removed": titles_removed,
        }
        # Persist snapshot BEFORE writing history so next run has accurate baseline
        write_titles_snapshot(snapshot_path, final_titles)
        write_history(history_path, history_entry)
        print(f"[history] Entry recorded in {history_path!r}: +{len(titles_added)} added, -{len(titles_removed)} removed.")

    print(f"[crawl] Done ({status}). Final entries: {len(downloads)}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Crawl dauphong torrents from The Pirate Bay and generate a Hydra-Launcher-compatible JSON source.",
    )
    parser.add_argument(
        "--output", "-o", default="sources/dauphong.json",
        help="Output JSON file path (default: sources/dauphong.json)",
    )
    parser.add_argument(
        "--meta", default=None,
        help="Meta JSON file path (default: derived from --output)",
    )
    parser.add_argument(
        "--start-page", type=int, default=0,
        help="0-indexed starting page. 0 = auto-resume from meta if interrupted (default: 0)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=None,
        help="Max pages to fetch (default: unlimited)",
    )
    parser.add_argument(
        "--sleep", type=float, default=1.0,
        help="Delay in seconds between page requests (default: 1.0)",
    )
    parser.add_argument(
        "--max-versions", type=int, default=3,
        help="Keep only the N most recent uploads per game. 0 = disable dedup (default: 3)",
    )
    parser.add_argument(
        "--max-retries", type=int, default=3,
        help="Max retries per page/request (default: 3)",
    )
    parser.add_argument(
        "--max-consecutive-errors", type=int, default=10,
        help="Abort crawl after N consecutive page failures (default: 10)",
    )
    parser.add_argument(
        "--http-backend", default="auto",
        choices=["auto", "curl_cffi", "curl_exe", "requests"],
        help="Force a specific HTTP backend (default: auto-detect best available)",
    )
    parser.add_argument(
        "--no-prune", action="store_true",
        help="Skip removal of entries seen with 0 seeds this run",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Force resume from last saved page even if run_status is not 'in_progress' or 'interrupted'",
    )

    args = parser.parse_args()

    # Derive meta path
    meta_path = args.meta
    if meta_path is None:
        base, _ = os.path.splitext(args.output)
        meta_path = base + "_meta.json"

    crawl(
        output_path=args.output,
        meta_path=meta_path,
        max_pages=args.max_pages,
        sleep_between=args.sleep,
        start_page=args.start_page,
        resume=args.resume,
        max_versions=args.max_versions,
        max_retries=args.max_retries,
        max_consecutive_errors=args.max_consecutive_errors,
        http_backend=args.http_backend,
        no_prune=args.no_prune,
    )


if __name__ == "__main__":
    main()
