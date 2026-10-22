"""Mint sources for the Diamond Discovery Hub journals OpenAlex has no source for,
and register each journal's own OAI-PMH feed where it has one (Kyle, 2026-10-22:
"add these journals manually using that same process"). Same steps as the
mints + register_endpoint() in handoff/zendesk-ojs-tickets-2026-10-05.py and
the #1417 RUNBOOK (steps 3-5); the manifest is ddh-mint-2026-10-22.csv next to
this file (one row per journal, built from DDH's OAI record + a live probe of
the journal's feed).

Per journal, in ONE transaction:
  1. refuse if any of its ISSNs is already on a source (that's a merge, not a mint)
     or an ISSN-less source has the same normalized name (a legacy twin: add the
     ISSNs there instead); refused rows are listed, nothing is written for them
  2. mint_source(type journal, DDH title/ISSNs/publisher/homepage/country)
  3. source_oa_override curated_is_oa=true: DDH vets every journal as open access
     under open licences with no fees (the Monday apply_oa_flags turns it into is_oa)
  4. if the manifest gives a feed: RUNBOOK step 4 registry check (any row on the
     same host, or on the host in the feed's record ids, that harvests the whole
     install or the same journal blocks: that is a #1418 split, so the source is
     minted but no row is written); otherwise one oai_pmh_endpoint row,
     is_journal_host=true, ready for the next daily harvest
then recompute_listed_in puts the new sources on ddh (their ISSNs are already in
source_list_member).

Run on the dyno (dry run by default):
    heroku run -a openalex-sources --no-tty "python handoff/ddh-mint-2026-10-22.py"
    heroku run -a openalex-sources --no-tty "python handoff/ddh-mint-2026-10-22.py --execute"
The receipt is printed as JSON after the RECEIPT marker (the dyno's disk is thrown away).
"""
import csv
import json
import os
import re
import secrets
import string
import sys
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from sources_lib import MatchContext, mint_source, normalize_name, recompute_listed_in

EXECUTE = "--execute" in sys.argv
MANIFEST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ddh-mint-2026-10-22.csv")
OA_NOTE = "DDH 2026-10-21: Diamond OA (all five DDH criteria true, incl. open licences and no fees)"
MAX_NEW_ROWS_PER_HOST = 20  # RUNBOOK step 4 (2026-10-01)
ALNUM = string.ascii_lowercase + string.digits

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "minted": [], "endpoints": [], "refused": [], "split_held": [], "actions": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def host(url):
    return (urlparse(url).hostname or "").lower().removeprefix("www.")


def journal_key(url, pmh_set):
    """What one row harvests: (host, journal) or (host, None) for a whole install."""
    if pmh_set:
        return pmh_set
    path = urlparse(url).path
    m = re.search(r"/index\.php/([^/]+)/oai", path) or re.search(r"^/([^/]+)/oai", path)
    j = m.group(1) if m else None
    return None if j in (None, "index", "oai") else j


def family(key):
    """Kind of per-journal key: 'journal:479' (Hrcak), 'journals:asinag' (OpenEdition),
    '495' (bibliotekanauki), 'xphi' (an OJS journal path)."""
    return key.split(":", 1)[0] if ":" in key else ("num" if key.isdigit() else "path")


def blocking_rows(conn, pmh_url, pmh_set, id_host):
    """RUNBOOK step 4, conservative: a row on the same host (or on the host in the
    feed's record ids) is harmless only if it is clearly ANOTHER journal's row of
    the same kind. A whole-install row, the same journal, or anything else (e.g. an
    'openaire' set spanning the platform) blocks."""
    hosts = {host(pmh_url)} | ({id_host} if id_host else set())
    rows = {}
    for h in hosts:
        for r in conn.execute(text(
                "SELECT id, pmh_url, pmh_set, status, source_id FROM oai_pmh_endpoint "
                "WHERE lower(pmh_url) LIKE :a OR lower(pmh_url) LIKE :b"),
                {"a": f"%://{h}%", "b": f"%://www.{h}%"}):
            rows[r.id] = r
    ours = journal_key(pmh_url, pmh_set).lower()
    block = []
    for r in rows.values():
        theirs = journal_key(r.pmh_url, r.pmh_set)
        if theirs is None or theirs.lower() == ours or family(theirs) != family(ours):
            block.append([r.id, r.pmh_url, r.pmh_set, r.status, r.source_id])
    return block, len(rows)


def register_endpoint(conn, pmh_url, pmh_set, source_id):
    eid = "".join(secrets.choice(ALNUM) for _ in range(20))
    conn.execute(text(
        "INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
        "source_id, is_journal_host, green_scrape, rand) "
        "VALUES (:id, :u, :s, 'oai_dc', true, true, 'active', :sid, true, true, random())"),
        {"id": eid, "u": pmh_url, "s": pmh_set, "sid": source_id})
    return eid


class DryRun(Exception):
    pass


def main():
    rows = list(csv.DictReader(open(MANIFEST, newline="", encoding="utf-8")))
    log(f"manifest: {len(rows)} journals, {sum(1 for r in rows if r['pmh_url'])} with a feed")
    new_rows_per_host = Counter()
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn)
            before_ddh = conn.execute(text("SELECT COUNT(*) FROM sources WHERE 'ddh' = ANY(listed_in)")).scalar()
            names = {}
            for sid, name, n_issn in conn.execute(text(
                    "SELECT s.id, s.display_name, (SELECT COUNT(*) FROM source_issn si WHERE si.source_id = s.id) "
                    "FROM sources s")):
                names.setdefault(normalize_name(name or ""), []).append((sid, n_issn))
            for r in rows:
                name, issns = r["display_name"], [i for i in r["issns"].split(";") if i]
                owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                     {"i": issns}).fetchall()
                if owned:
                    receipt["refused"].append({"ddh_id": r["ddh_id"], "name": name, "why": "issn_on_source",
                                               "detail": [list(map(str, o)) for o in owned]})
                    log(f"REFUSED {name!r}: ISSN already on a source {[tuple(o) for o in owned]}")
                    continue
                twins = [sid for sid, n in names.get(normalize_name(name), []) if n == 0]
                if twins:
                    receipt["refused"].append({"ddh_id": r["ddh_id"], "name": name, "why": "issnless_namesake", "detail": twins})
                    log(f"REFUSED {name!r}: ISSN-less source(s) with the same name {twins}")
                    continue
                sid = mint_source(conn, ctx, name, source_type="journal", issns=issns,
                                  publisher=r["publisher"] or None, homepage_url=r["homepage_url"] or None,
                                  country_code=r["country_code"] or None, country=r["country"] or None)
                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, true, :n)"),
                             {"s": sid, "n": OA_NOTE})
                receipt["minted"].append({"source_id": sid, "ddh_id": r["ddh_id"], "name": name, "issns": issns})
                log(f"MINTED {sid} {name!r} {issns}")
                if not r["pmh_url"]:
                    continue
                # pmh_set may list several sets of the same journal ("bjas|JAS": current + archived run)
                for pmh_set in (r["pmh_set"].split("|") if r["pmh_set"] else [None]):
                    block, n_host_rows = blocking_rows(conn, r["pmh_url"], pmh_set, r["id_host"] or None)
                    if block:
                        receipt["split_held"].append({"source_id": sid, "name": name, "pmh_url": r["pmh_url"],
                                                      "pmh_set": pmh_set, "blocking_rows": block})
                        log(f"  NO ENDPOINT for {sid} [{pmh_set or '-'}]: {len(block)} row(s) on this install could "
                            f"harvest the same records -> #1418 split: {block[:3]}")
                        continue
                    h = host(r["pmh_url"])
                    if new_rows_per_host[h] >= MAX_NEW_ROWS_PER_HOST:
                        receipt["split_held"].append({"source_id": sid, "name": name, "pmh_url": r["pmh_url"],
                                                      "pmh_set": pmh_set, "why": "host_cap"})
                        log(f"  NO ENDPOINT for {sid} [{pmh_set or '-'}]: {h} already got {MAX_NEW_ROWS_PER_HOST} new rows")
                        continue
                    eid = register_endpoint(conn, r["pmh_url"], pmh_set, sid)
                    new_rows_per_host[h] += 1
                    receipt["endpoints"].append({"endpoint_id": eid, "source_id": sid, "pmh_url": r["pmh_url"],
                                                 "pmh_set": pmh_set, "other_rows_on_host": n_host_rows})
                    log(f"  REGISTERED endpoint {eid} {r['pmh_url']} [{pmh_set or '-'}] -> {sid} journal_host "
                        f"({n_host_rows} other rows on the host, none overlapping)")
            changed = recompute_listed_in(conn)
            after_ddh = conn.execute(text("SELECT COUNT(*) FROM sources WHERE 'ddh' = ANY(listed_in)")).scalar()
            log(f"minted {len(receipt['minted'])}, endpoints {len(receipt['endpoints'])}, held for #1418 "
                f"{len(receipt['split_held'])}, refused {len(receipt['refused'])}")
            log(f"sources.listed_in rows changed (all lists): {changed}; listed in ddh: {before_ddh} -> {after_ddh}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back (source ids above are not reserved)")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
