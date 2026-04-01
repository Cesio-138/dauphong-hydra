#!/usr/bin/env python3
import re
import os
import json
import time
import shutil
import argparse
import base64
import html
import subprocess
from datetime import datetime
from collections import defaultdict
from urllib.parse import quote_plus

import requests

APIBAY_USER = "dauphong"

# Detect curl.exe (Windows) for WSL2 environments where the Linux TCP/IP
# fingerprint gets rate-limited by Cloudflare but Windows does not.
CURL_EXE = shutil.which("curl.exe")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

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


def bytes_to_human(size_bytes):
    """Convert bytes (int or str) to a human-readable string like '16.9 GB'."""
    try:
        n = float(size_bytes)
    except (TypeError, ValueError):
        return str(size_bytes) if size_bytes else None
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if n < 1024.0:
            s = f"{n:.1f}"
            if s.endswith('.0'):
                s = s[:-2]
            return f"{s} {unit}"
        n /= 1024.0
    s = f"{n:.1f}"
    if s.endswith('.0'):
        s = s[:-2]
    return f"{s} PB"


def decode_html(text):
    """Decode HTML entities repeatedly until stable (handles double-encoding)."""
    while True:
        decoded = html.unescape(text)
        if decoded == text:
            return text
        text = decoded


class FetchResult:
    """Minimal response object returned by _fetch_json."""
    __slots__ = ('status_code', 'data', 'headers')
    def __init__(self, status_code, data=None, headers=None):
        self.status_code = status_code
        self.data = data
        self.headers = headers or {}

    def json(self):
        return self.data


def _fetch_json(url, session=None, timeout=15):
    """Fetch JSON from *url*.

    On WSL2, routes through curl.exe (Windows networking stack) to avoid
    Cloudflare rate-limiting the Linux TCP/IP fingerprint.  Falls back to
    Python ``requests`` when curl.exe is not available.
    """
    if CURL_EXE:
        cmd = [CURL_EXE, '-s', '-w', '\n%{http_code}', '--max-time', str(timeout), url]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        output = proc.stdout.rstrip('\r\n')
        # Last line is the HTTP status code
        last_nl = output.rfind('\n')
        if last_nl == -1:
            # Only status code, empty body
            code = int(output) if output.isdigit() else 0
            return FetchResult(code)
        body = output[:last_nl]
        code_str = output[last_nl + 1:]
        code = int(code_str) if code_str.isdigit() else 0
        data = None
        if code == 200 and body:
            data = json.loads(body)
        return FetchResult(code, data)
    else:
        r = (session or requests).get(url, headers=HEADERS, timeout=timeout)
        return FetchResult(r.status_code, r.json() if r.status_code == 200 else None, dict(r.headers))


def build_magnet(info_hash, name):
    """Build a full magnet URI with standard trackers."""
    uri = f"magnet:?xt=urn:btih:{info_hash.upper()}&dn={quote_plus(name)}"
    for tr in TRACKERS:
        uri += f"&tr={quote_plus(tr)}"
    return uri


def normalize_upload_date(uploaded):
    """Ensure upload date is in ISO 8601 format with milliseconds, e.g. 2023-06-30T23:00:00.000Z."""
    if not uploaded:
        return None
    if uploaded.endswith('+00:00'):
        uploaded = uploaded[:-6] + '.000Z'
    elif re.search(r'Z$', uploaded) and '.' not in uploaded:
        uploaded = uploaded[:-1] + '.000Z'
    return uploaded





def normalize_infohash(h):
    if not h:
        return None
    s = h.strip()
    if re.fullmatch(r'[A-Fa-f0-9]{40}', s):
        return s.lower()
    s2 = s.upper()
    padding = '=' * ((8 - len(s2) % 8) % 8)
    try:
        b = base64.b32decode(s2 + padding)
        return b.hex()
    except Exception:
        return s.lower()


def _parse_apibay_page(data):
    """Parse a list of items from apibay JSON response into raw entry dicts."""
    entries = []
    for item in data:
        try:
            info_hash = item.get('info_hash') or item.get('infohash') or ''
            name = decode_html(item.get('name') or '').strip()
            if not name or info_hash == '0000000000000000000000000000000000000000':
                continue
            seeds = int(item.get('seeders') or item.get('seed') or 0)
            size = item.get('size')
            added = item.get('added')
            uploaded = None
            if added:
                try:
                    uploaded = datetime.utcfromtimestamp(int(added)).isoformat() + 'Z'
                except Exception:
                    uploaded = None
            entries.append({
                'infohash_raw': info_hash,
                'name': name,
                'seeds': seeds,
                'size': size,
                'uploaded_raw': uploaded,
            })
        except Exception:
            continue
    return entries


# ---------------------------------------------------------------------------
# Incremental I/O helpers
# ---------------------------------------------------------------------------

def load_existing(output_path):
    """Load existing downloads from output JSON as a dict keyed by lowercase infohash."""
    if not os.path.exists(output_path):
        return {}
    try:
        with open(output_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        result = {}
        for d in data.get('downloads', []):
            uris = d.get('uris', [])
            if uris:
                m = re.search(r'urn:btih:([A-Fa-f0-9]+)', uris[0], re.I)
                if m:
                    ih = m.group(1).lower()
                    # Decode any HTML entities left over from previous crawls
                    if 'title' in d:
                        d['title'] = decode_html(d['title']).strip()
                    result[ih] = d
        return result
    except Exception:
        return {}


def load_meta(meta_path):
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _sort_downloads(downloads):
    """Group by title, sort each group by uploadDate desc, then flatten alphabetically."""
    groups = defaultdict(list)
    for d in downloads:
        groups[d['title']].append(d)
    result = []
    for title in sorted(groups.keys(), key=str.casefold):
        group = groups[title]
        group.sort(key=lambda x: x.get('uploadDate') or '', reverse=True)
        result.extend(group)
    return result


def _write_json(output_path, downloads):
    """Atomically write the JSON output file."""
    dirpath = os.path.dirname(output_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    tmp = output_path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump({"name": "Dauphong", "downloads": downloads}, f, indent=2, ensure_ascii=False)
    os.replace(tmp, output_path)


def _write_meta(meta_path, last_page, total_entries):
    """Write crawl progress metadata."""
    meta = {
        'last_run': datetime.utcnow().isoformat() + 'Z',
        'last_page': last_page,
        'total_entries': total_entries,
    }
    dirpath = os.path.dirname(meta_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2)


# ---------------------------------------------------------------------------
# Main crawl loop
# ---------------------------------------------------------------------------

def crawl(output_path, meta_path, max_pages=None, sleep_between=1.0, start_page=0):
    session = requests.Session()

    if CURL_EXE:
        print(f'[crawl] Using curl.exe ({CURL_EXE}) for Windows networking stack.')
    else:
        print('[crawl] curl.exe not found — using Python requests (Linux stack).')

    # Load previously crawled entries so we don't lose them when resuming
    accumulated = load_existing(output_path)
    print(f'[crawl] {len(accumulated)} existing entries loaded from "{output_path}".')

    # Get total page count from apibay
    total_pages = None
    try:
        result = _fetch_json(
            f'https://apibay.org/q.php?q=pcnt:{APIBAY_USER}',
            session=session,
            timeout=15,
        )
        if result.status_code != 200:
            print(f'[apibay] pcnt request returned HTTP {result.status_code}')
        else:
            pcnt_data = result.json()
            if isinstance(pcnt_data, str):
                total_pages = int(pcnt_data) or None
            elif isinstance(pcnt_data, list) and pcnt_data:
                total_pages = int(pcnt_data[0].get('pages') or pcnt_data[0].get('page_count') or 0) or None
            elif isinstance(pcnt_data, (int, float)):
                total_pages = int(pcnt_data) or None
            print(f'[apibay] Total pages reported: {total_pages}')
    except Exception as e:
        print(f'[apibay] Could not fetch page count: {e}')

    # Paginate and write incrementally after every page.
    # page_index 0 -> q=user:dauphong
    # page_index N -> q=user:dauphong:N  (for N >= 1)
    page_index = start_page
    pages_fetched = 0

    while True:
        if max_pages is not None and pages_fetched >= max_pages:
            print(f'[apibay] --max-pages={max_pages} limit reached.')
            break
        if total_pages is not None and page_index >= total_pages:
            print(f'[apibay] All {total_pages} pages processed.')
            break

        if page_index == 0:
            url = f'https://apibay.org/q.php?q=user:{APIBAY_USER}'
        else:
            url = f'https://apibay.org/q.php?q=user:{APIBAY_USER}:{page_index}'

        print(f'[apibay] Page {page_index + 1}: {url}')

        try:
            result = _fetch_json(url, session=session, timeout=15)
            if result.status_code == 429:
                retry_after = int(result.headers.get('Retry-After', 60))
                print(f'[apibay] 429 on page {page_index + 1}. Waiting {retry_after}s...')
                time.sleep(retry_after)
                continue
            if result.status_code != 200:
                print(f'[apibay] HTTP {result.status_code} on page {page_index + 1}.')
                break
            data = result.json()
        except Exception as e:
            print(f'[apibay] Error on page {page_index + 1}: {e}')
            break

        page_entries = _parse_apibay_page(data)
        print(f'[apibay] Page {page_index + 1}: {len(page_entries)} raw entries.')

        if not page_entries:
            print('[apibay] Empty page — stopping.')
            break

        # Merge into accumulated dict (infohash -> output entry), seeds > 0 only
        new_count = 0
        upd_count = 0
        for e in page_entries:
            if (e.get('seeds') or 0) <= 0:
                continue
            ih = normalize_infohash(e.get('infohash_raw') or '')
            if not ih:
                continue

            title = e.get('name') or ''
            uri = build_magnet(ih, title)
            uploaded = normalize_upload_date(e.get('uploaded_raw'))
            file_size = bytes_to_human(e.get('size'))

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

        print(f'[apibay] Page {page_index + 1}: {new_count} new, {upd_count} updated, accumulated total: {len(accumulated)}.')

        # Write sorted output after every page (atomic via .tmp)
        downloads = _sort_downloads(list(accumulated.values()))
        _write_json(output_path, downloads)
        _write_meta(meta_path, page_index, len(downloads))
        print(f'[apibay] File updated: {len(downloads)} seeded entries.')

        page_index += 1
        pages_fetched += 1

        if pages_fetched > 0:
            time.sleep(sleep_between)

    print(f'[crawl] Done. Total entries: {len(accumulated)}.')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Crawl dauphong torrents and generate sources/dauphong.json')
    parser.add_argument('--output', '-o', default='sources/dauphong.json',
                        help='Output file path (default: sources/dauphong.json)')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='Maximum number of pages to fetch (default: unlimited)')
    parser.add_argument('--sleep', type=float, default=1.0,
                        help='Delay in seconds between requests (default: 1.0)')
    parser.add_argument('--start-page', type=int, default=0,
                        help='0-indexed starting page for resuming an interrupted crawl (default: 0)')
    args = parser.parse_args()

    base, _ext = os.path.splitext(args.output)
    meta_path = base + '_meta.json'

    if args.start_page > 0:
        meta = load_meta(meta_path)
        print(f'[crawl] Resuming from page {args.start_page + 1}. Previous meta: {meta}')

    crawl(
        output_path=args.output,
        meta_path=meta_path,
        max_pages=args.max_pages,
        sleep_between=args.sleep,
        start_page=args.start_page,
    )


if __name__ == '__main__':
    main()
