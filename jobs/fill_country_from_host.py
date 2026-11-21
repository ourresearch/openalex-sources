"""Fill a NULL sources.country_code from the source's host when the host's other titles agree
(oxjobs #2208 and #2513), so a journal that gets a host also gets its country.

OpenAlex defines a source's country as its home country, typically its publisher's. When a journal has no country
but its host (the publisher, else the institution: the order CreateSourcesApi uses for host_organization) has >= 3
other titles with a country and all of them carry the same one, that country is the journal's. Two one-off fills
under this rule passed blind bars against the journals' own sites (#2208, 13 Nov 2026; #2513, 21 Nov 2026) and
filled about 5,000 sources. Hosts whose titles are only 95-99% one country passed once and failed once (#2513: a
publisher with a US registered address and a Kenyan office), so they are left to a checked one-off fill. Between them, host waves (#2385, #1582, #2408) bound hosts to ~3,000 journals that then sat
without a country, so the gap regrows whenever hosts are bound. This job closes it as they are.

Held (left NULL), the same holds the two oxjobs applied:
  - DOAJ journals (jobs.doaj fills those from DOAJ's own country of publisher);
  - sources listed in data/host_country_holds.csv (held by those oxjobs after checking);
  - the publisher's own OpenAlex record lists countries and not this one (Bentham, Inderscience);
  - the host institution's own country differs;
  - the country would be CN and the host is a publisher: #2513's bar found such publishers' journals naming a Hong
    Kong or US company on their own sites (4 of 4 wrong), so they are left to a checked one-off fill;
  - >= 80% of the source's works that have an author country (>= 20 of them) come from one other country, the sign
    of a wrong host.
Only NULLs are written, and `country` (the name) only where NULL. Guard inside the UPDATE: still NULL, same host.
Works don't embed a source's country (the API joins it at query time), so nothing is re-stamped.

    python -m jobs.fill_country_from_host --dry-run     # report what would be filled, write nothing
    python -m jobs.fill_country_from_host               # fill; prints a RECEIPT json line (keep it to revert)
    python -m jobs.fill_country_from_host --revert <receipt.json>

OPENALEX_UI_ADMIN_API_KEY (as refresh_source_stats) lifts the API rate limit for the hold checks.
"""
import argparse
import csv
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parents[1]))
from db import engine  # noqa: E402
from sources_lib import ISO_CODE_TO_NAME  # noqa: E402

API = "https://api.openalex.org"
HOLDS = Path(__file__).parents[1] / "data" / "host_country_holds.csv"
MIN_TITLES, MIN_SHARE = 3, 1.0           # host's other titles with a country; all in one country
AUTHOR_MIN, AUTHOR_SHARE = 20, 0.8       # works with an author country; share from one other country
EQUIV = {"DD": "DE"}                     # East Germany's titles count as Germany

CANDIDATES = """
WITH s AS (
    SELECT id, country_code, COALESCE(is_in_doaj, false) AS doaj,
           CASE WHEN publisher_id IS NOT NULL THEN 'P' || publisher_id
                WHEN institution_id IS NOT NULL THEN 'I' || institution_id END AS host
    FROM sources
    WHERE type IN ('journal', 'book series', 'ebook platform')  -- merged sources are deleted (D1)
),
cc AS (SELECT host, country_code, count(*) AS n FROM s
       WHERE host IS NOT NULL AND country_code IS NOT NULL GROUP BY 1, 2),
hc AS (SELECT host, (array_agg(country_code ORDER BY n DESC, country_code))[1] AS top_cc,
              max(n) AS top_n, sum(n) AS n_with
       FROM cc GROUP BY host)
SELECT s.id, s.host, hc.top_cc, hc.top_n, hc.n_with, s.doaj
FROM s JOIN hc ON hc.host = s.host
WHERE s.country_code IS NULL AND hc.n_with >= :min_titles AND hc.top_n >= :min_share * hc.n_with
ORDER BY s.id
"""


def api(path, **params):
    key = os.environ.get("OPENALEX_UI_ADMIN_API_KEY")
    if key:
        params["api_key"] = key
    for attempt in range(5):
        r = requests.get(f"{API}{path}", params=params, timeout=60)
        if r.status_code == 404:
            return None
        if r.ok:
            return r.json()
        time.sleep(3 * (attempt + 1))
    r.raise_for_status()


def norm(code):
    return EQUIV.get(code, code) if code else code


def hold_reason(cand, held_ids, host_ccs, authors):
    """Why this candidate is left NULL, or None to fill it.
    host_ccs: the host record's own country codes (publisher country_codes, or [institution country_code]).
    authors: (works with an author country, {country: works}) for the source."""
    if cand["id"] in held_ids:
        return "held by an oxjob (data/host_country_holds.csv)"
    if cand["doaj"]:
        return "DOAJ journal (jobs.doaj fills it)"
    if cand["top_cc"] == "CN" and cand["host"].startswith("P"):
        return "CN via a publisher host (registered offshore, #2513 bar)"
    if host_ccs and cand["top_cc"] not in {norm(c) for c in host_ccs}:
        return "host record names another country"
    n_with, by_cc = authors
    if n_with >= AUTHOR_MIN and by_cc:
        top, n = max(by_cc.items(), key=lambda kv: kv[1])
        if norm(top) != cand["top_cc"] and n >= AUTHOR_SHARE * n_with:
            return "authors >= 80% in another country (host likely wrong)"
    return None


def host_countries(host):
    if host.startswith("P"):
        rec = api(f"/publishers/{host}", select="id,country_codes") or {}
        return rec.get("country_codes") or []
    rec = api(f"/institutions/{host}", select="id,country_code") or {}
    return [rec["country_code"]] if rec.get("country_code") else []


def author_countries(source_id):
    """(works with any author country, {country: works}); a work with authors in two countries is in both groups."""
    src = f"primary_location.source.id:S{source_id}"
    d = api("/works", filter=src, group_by="authorships.countries") or {}
    by_cc = {}
    for g in d.get("group_by", []):
        code = str(g["key"]).rsplit("/", 1)[-1].upper()
        if len(code) == 2:
            by_cc[code] = g["count"]
    if not by_cc:
        return 0, {}
    n_with = (api("/works", filter=f"{src},authorships.countries:!null", per_page=1, select="id") or {})
    return n_with.get("meta", {}).get("count", 0), by_cc


def load_holds():
    if not HOLDS.exists():
        return set()
    with open(HOLDS, encoding="utf-8") as fh:
        return {int(r["source_id"]) for r in csv.DictReader(fh)}


def fill(conn, receipt, dry_run):
    held_ids = load_holds()
    cands = [dict(r._mapping) for r in conn.execute(text(CANDIDATES), {"min_titles": MIN_TITLES, "min_share": MIN_SHARE})]
    host_cache, to_fill, holds = {}, [], Counter()
    for c in cands:
        c["top_cc"] = norm(c["top_cc"])
        if c["top_cc"] not in ISO_CODE_TO_NAME:
            holds["not an ISO 3166-1 country"] += 1
            continue
        cheap = hold_reason(c, held_ids, [], (0, {}))
        if cheap:
            holds[cheap] += 1
            continue
        if c["host"] not in host_cache:
            host_cache[c["host"]] = host_countries(c["host"])
        why = hold_reason(c, held_ids, host_cache[c["host"]], author_countries(c["id"]))
        if why:
            holds[why] += 1
        else:
            to_fill.append(c)
    receipt["candidates"], receipt["held"] = len(cands), dict(holds)
    print(f"candidates {len(cands)}; to fill {len(to_fill)}; held {dict(holds)}", flush=True)
    if dry_run or not to_fill:
        receipt["changed"], receipt["country_name_filled"] = {}, []
        receipt["would_fill"] = {str(c["id"]): c["top_cc"] for c in to_fill}
        return
    done = {r.id: r.name_filled for r in conn.execute(text("""
        WITH v AS (SELECT * FROM unnest(CAST(:ids AS bigint[]), CAST(:codes AS text[]), CAST(:names AS text[]),
                                        CAST(:kinds AS text[]), CAST(:hids AS bigint[])) AS v(id, code, name, kind, hid)),
        t AS (SELECT s.id, v.code, v.name, s.country IS NULL AS name_filled
              FROM sources s JOIN v ON v.id = s.id
              WHERE s.country_code IS NULL
                AND ((v.kind = 'P' AND s.publisher_id = v.hid)
                     OR (v.kind = 'I' AND s.publisher_id IS NULL AND s.institution_id = v.hid))
              FOR UPDATE OF s)
        UPDATE sources s SET country_code = t.code, country = COALESCE(s.country, t.name), updated_date = now()
        FROM t WHERE s.id = t.id
        RETURNING s.id, t.name_filled
    """), {"ids": [c["id"] for c in to_fill], "codes": [c["top_cc"] for c in to_fill],
           "names": [ISO_CODE_TO_NAME[c["top_cc"]] for c in to_fill],
           "kinds": [c["host"][0] for c in to_fill], "hids": [int(c["host"][1:]) for c in to_fill]})}
    receipt["changed"] = {str(c["id"]): [None, c["top_cc"]] for c in to_fill if c["id"] in done}
    receipt["country_name_filled"] = sorted(str(i) for i, named in done.items() if named)
    print(f"filled {len(done)} of {len(to_fill)}; top codes "
          f"{Counter(c['top_cc'] for c in to_fill if c['id'] in done).most_common(10)}", flush=True)


def revert(conn, receipt, path):
    prior = json.load(open(path, encoding="utf-8"))
    named = set(prior.get("country_name_filled", []))
    wrote = {int(sid): code for sid, (_, code) in prior["changed"].items()}
    done = {r.id for r in conn.execute(text("""
        UPDATE sources s SET country_code = NULL, country = CASE WHEN v.named THEN NULL ELSE s.country END,
                             updated_date = now()
        FROM unnest(CAST(:ids AS bigint[]), CAST(:codes AS text[]), CAST(:named AS boolean[])) AS v(id, code, named)
        WHERE s.id = v.id AND s.country_code = v.code
        RETURNING s.id
    """), {"ids": list(wrote), "codes": list(wrote.values()), "named": [str(s) in named for s in wrote]})}
    receipt["reverted"] = len(done)
    receipt["skipped"] = {str(s): f"no longer {c}" for s, c in wrote.items() if s not in done}
    print(f"reverted {len(done)} of {len(wrote)}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--revert", metavar="RECEIPT")
    args = ap.parse_args()
    receipt = {"job": "fill_country_from_host", "run_at": datetime.now(timezone.utc).isoformat(),
               "dry_run": args.dry_run, "revert_of": args.revert}
    with engine.begin() as conn:
        if args.revert:
            revert(conn, receipt, args.revert)
        else:
            fill(conn, receipt, args.dry_run)
    print("RECEIPT " + json.dumps(receipt, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
