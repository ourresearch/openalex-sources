"""Stage 1 of the DataCite job: fetch api.datacite.org/clients (every DataCite
repository/periodical) into the `datacite_client` staging table (full-snapshot
TRUNCATE + reload). Stage 2 is jobs/sync_datacite_clients.py.

  python -m jobs.datacite_clients
"""
import argparse
import json
import time

import requests
from sqlalchemy import text
from tenacity import retry, stop_after_attempt, wait_exponential

from db import engine

API = "https://api.datacite.org"


@retry(wait=wait_exponential(multiplier=2, max=60), stop=stop_after_attempt(5))
def _get(url):
    r = requests.get(url, timeout=60, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()


def fetch_providers():
    """provider_id -> provider name, for publisher-ish enrichment on mints."""
    names, page = {}, 1
    while True:
        d = _get(f"{API}/providers?page[size]=1000&page[number]={page}")
        for p in d["data"]:
            names[p["id"]] = (p["attributes"].get("name") or "").strip() or None
        if page >= d["meta"]["totalPages"]:
            return names
        page += 1


def _row(item, provider_names):
    a = item["attributes"]
    issn_struct = a.get("issn") or {}
    issns = [v.strip().upper() for v in issn_struct.values() if v and v.strip()]
    provider = ((item.get("relationships") or {}).get("provider") or {}).get("data") or {}
    return {
        "id": item["id"],
        "display_name": (a.get("name") or "").strip() or None,
        "issns": sorted(set(issns)),
        "url": (a.get("url") or "").strip() or None,
        "client_type": a.get("clientType"),
        "provider_id": provider.get("id"),
        "provider_name": provider_names.get(provider.get("id")),
        "raw": json.dumps(a),
    }


def fetch():
    provider_names = fetch_providers()
    print(f"{len(provider_names)} providers", flush=True)
    insert = text(
        "INSERT INTO datacite_client (id, display_name, issns, url, client_type, "
        "provider_id, provider_name, raw) VALUES (:id, :display_name, :issns, :url, "
        ":client_type, :provider_id, :provider_name, CAST(:raw AS JSONB)) "
        "ON CONFLICT (id) DO NOTHING"
    )
    total, page = 0, 1
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE datacite_client"))
        while True:
            d = _get(f"{API}/clients?page[size]=1000&page[number]={page}")
            rows = [_row(it, provider_names) for it in d["data"]]
            if rows:
                conn.execute(insert, rows)
                total += len(rows)
            print(f"page {page}/{d['meta']['totalPages']}: {total} staged", flush=True)
            if page >= d["meta"]["totalPages"]:
                break
            page += 1
            time.sleep(0.1)
    print(f"staged {total} datacite clients (DONE)", flush=True)


def main():
    argparse.ArgumentParser().parse_args()
    fetch()


if __name__ == "__main__":
    main()
