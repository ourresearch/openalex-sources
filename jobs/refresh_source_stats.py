"""Refresh source_works_count + source_publication_years from the OpenAlex API.

Cursor-pages api.openalex.org/sources (select= keeps responses tiny) and
TRUNCATE+COPY-reloads both stats tables in one transaction, stamped with
today's as_of. Replaces the one-time Databricks snapshots from the #548
migration. Consumers: resolve_conflicts (winner selection) and apply_oa_flags
(is_fully_open_in_jstage).

  python -m jobs.refresh_source_stats [--dry-run] [--max-pages N]

OPENALEX_UI_ADMIN_API_KEY (config var / .env) lifts the API rate limit so the
full ~1,400-page sweep runs unthrottled; without it the job still works.
"""
import argparse
import io
import os
from datetime import date

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from db import engine

API = "https://api.openalex.org/sources"
SELECT = "id,works_count,first_publication_year,last_publication_year"
PER_PAGE = 200


@retry(wait=wait_exponential(multiplier=2, max=60), stop=stop_after_attempt(5))
def _get(params):
    r = requests.get(API, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch(max_pages=None):
    """id -> (works_count, first_publication_year, last_publication_year)."""
    params = {"select": SELECT, "per-page": PER_PAGE, "cursor": "*"}
    api_key = os.environ.get("OPENALEX_UI_ADMIN_API_KEY")
    if api_key:
        params["api_key"] = api_key
    stats, page = {}, 0
    while params["cursor"]:
        d = _get(params)
        for r in d["results"]:
            source_id = int(r["id"].rsplit("/S", 1)[1])
            stats[source_id] = (
                r.get("works_count"),
                r.get("first_publication_year"),
                r.get("last_publication_year"),
            )
        params["cursor"] = d["meta"].get("next_cursor")
        page += 1
        if page % 100 == 0:
            print(f"page {page}: {len(stats)} sources", flush=True)
        if max_pages and page >= max_pages:
            break
    print(f"fetched {len(stats)} sources in {page} pages", flush=True)
    return stats


def load(stats):
    today = date.today().isoformat()
    null = "\\N"
    works_buf, years_buf = io.StringIO(), io.StringIO()
    for source_id, (works, first_year, last_year) in stats.items():
        works_buf.write(f"{source_id}\t{works if works is not None else 0}\t{today}\n")
        if first_year is not None or last_year is not None:
            first = first_year if first_year is not None else null
            last = last_year if last_year is not None else null
            years_buf.write(f"{source_id}\t{first}\t{last}\t{today}\n")
    works_buf.seek(0)
    years_buf.seek(0)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute("TRUNCATE source_works_count")
            cur.copy_expert(
                "COPY source_works_count (source_id, works_count, as_of) FROM STDIN",
                works_buf,
            )
            cur.execute("TRUNCATE source_publication_years")
            cur.copy_expert(
                "COPY source_publication_years (source_id, first_publication_year, "
                "last_publication_year, as_of) FROM STDIN",
                years_buf,
            )
        raw.commit()
    finally:
        raw.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="fetch only, no DB write")
    ap.add_argument("--max-pages", type=int, help="stop after N pages (testing)")
    args = ap.parse_args()
    stats = fetch(max_pages=args.max_pages)
    if args.dry_run:
        sample = list(stats.items())[:3]
        print(f"dry run — would load {len(stats)} rows; sample: {sample}", flush=True)
        return
    if args.max_pages:
        raise SystemExit("refusing to TRUNCATE+reload from a partial --max-pages fetch")
    load(stats)
    print(f"loaded {len(stats)} works counts + publication years (DONE)", flush=True)


if __name__ == "__main__":
    main()
