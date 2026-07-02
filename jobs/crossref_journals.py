"""Stage 1 of the Crossref journals job: fetch api.crossref.org/journals into the
`crossref_journal` staging table (full-snapshot TRUNCATE + reload). Stage 2 is
jobs/sync_crossref_journals.py.

  python -m jobs.crossref_journals [--max-pages N]
"""
import argparse
import json
import os
import sys
import time
import urllib.parse

STATE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".crossref_fetch_state.json"
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from sqlalchemy import text
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from db import engine

BASE_URL = "https://api.crossref.org/journals"
HEADERS = {
    "Accept": "application/json",
    "User-Agent": f"mailto:{os.getenv('CROSSREF_MAILTO', 'dev@ourresearch.org')}",
}
if os.getenv("CROSSREF_API_KEY"):
    HEADERS["crossref-api-key"] = os.getenv("CROSSREF_API_KEY")


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def _get(url):
    r = requests.get(url, headers=HEADERS, timeout=60)
    if r.status_code == 429:
        time.sleep(int(r.headers.get("Retry-After", 60)))
    r.raise_for_status()
    return r


def _row(item):
    issns = [i for i in (item.get("ISSN") or []) if i]
    return {
        "issns": issns,
        "title": item.get("title"),
        "publisher": item.get("publisher"),
        "raw": json.dumps(item),
    }


def _save_state(cursor, page, rows):
    with open(STATE_FILE, "w") as f:
        json.dump({"cursor": cursor, "page": page, "rows": rows}, f)


def fetch(max_pages=None, resume=False):
    insert = text(
        "INSERT INTO crossref_journal (issns, title, publisher, raw) "
        "VALUES (:issns, :title, :publisher, CAST(:raw AS JSONB))"
    )
    # Resume from a saved cursor (the harness caps background jobs, so a full
    # ~130-page fetch may need several passes). A fresh run truncates and starts over.
    if resume and os.path.exists(STATE_FILE):
        st = json.load(open(STATE_FILE))
        cursor, page, rows_loaded = st["cursor"], st["page"], st["rows"]
        print(f"resuming from page {page} ({rows_loaded} already staged)", flush=True)
    else:
        cursor, page, rows_loaded = "*", 1, 0
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE crossref_journal RESTART IDENTITY"))

    while True:
        url = f"{BASE_URL}?rows=1000&cursor={urllib.parse.quote(cursor)}"
        msg = _get(url).json()["message"]
        items = msg.get("items") or []
        if not items:
            break
        batch = [_row(it) for it in items if it.get("ISSN")]
        if batch:
            with engine.begin() as conn:
                conn.execute(insert, batch)
            rows_loaded += len(batch)
        print(f"page {page}: {len(items)} items ({rows_loaded} staged so far)", flush=True)
        if "next-cursor" not in msg or (max_pages and page >= max_pages):
            break
        cursor = msg["next-cursor"]
        page += 1
        _save_state(cursor, page, rows_loaded)  # persist so we can resume
        time.sleep(0.1)

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
    print(f"staged {rows_loaded} crossref journals (DONE)", flush=True)
    return rows_loaded


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages (sampling)")
    ap.add_argument("--resume", action="store_true", help="continue from saved cursor")
    args = ap.parse_args()
    fetch(max_pages=args.max_pages, resume=args.resume)


if __name__ == "__main__":
    main()
