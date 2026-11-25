"""Former title + ISSNs onto the journal's one record (oxjob #2389, ZD 24215; support factory #2254, written off desk
2026-11-25 by a #2263 lane; desk runs it).

Policy choice: follows the sources charter decision of 2026-07-17 (Jason + Casey) D5 "current name wins" =
source-duplicate-merges RULES-DRAFT R3 + R10: a renamed journal is ONE record under its current title; the old titles
ride along as alternate titles and the old ISSNs in its ISSN list. No predecessor/successor field and no second record.
The same rule ran as #1979 (16 Nov, openalex-sources 285db99: Poultry Studies, BSLI) and #1225 (21 Nov).

  S5407043372 'Hong Kong Review of Belt and Road Studies' (ISSNs 3007-5173 print, 3007-732X online; issn_l 3007-5173;
  64 works 2023-2026) + ISSNs 2959-8397 (print) and 2959-8400 (online) + alternate title 'OBOR Working Papers'.

Evidence (receipts in #2389 work/, all 2026-11-25):
  - The journal's own site (D5 witness; Wayback captures, since oborjournal.org shows a Cloudflare challenge off desk):
    the issue archive lists 'OBOR Working Papers No. 1 (2023)' and 'No. 2 (2023)' with ISSN (Print) 2959-8397, ISSN
    (Online) 2959-8400, then 'Hong Kong Review of Belt and Road Studies No. 1(3) (2024)' onward with 3007-5173 /
    3007-732X: one numbering run, one OJS site, one publisher (Hong Kong Chu Hai College). About page: the journal
    "was founded in 2023". (work/journal_site_wayback_2026-11-25.md)
  - Crossref: all 74 DOIs under prefix 10.63596 that belong to the journal, the 2023 OBOR Working Papers issues
    included, are deposited under the current title with ISSNs 3007-5173 / 3007-732X; Crossref has no journal for
    2959-8397 or 2959-8400 (work/crossref_prefix_10.63596_2026-11-25.txt).
  - Live API: the 12 records of 2023 (the 10 OBOR Working Papers articles + 2 issue records) already sit on
    S5407043372 (works_count 64, first_publication_year 2023); /sources/issn:2959-8397 and issn:2959-8400 are 404.
  - Registry mirror openalex.sources.sources: no source holds 2959-8397 or 2959-8400, or the title 'OBOR Working
    Papers'; S5407043372 has no alternate titles and is not curator-overridden.

Effect: /sources/issn:2959-8397 and issn:2959-8400 resolve to S5407043372; the former title is searchable on it.
No work moves (no record carries the old ISSNs); the journal's 64 works get the longer ISSN list in their source
object at the next End 2 End: no pre-clear needed. issn_l stays 3007-5173 (walden matches works by the whole ISSN
list, never by issn_l; jobs/reconcile_issn_l leaves two-family sources alone, as on Poultry Studies since 16 Nov).
No OA flag, name, host, publisher or homepage is touched.

Run on desk from the openalex-sources checkout, OUTSIDE the End 2 End window (03:00 UTC until the night publishes; ideally
before 19:30 CT so it rides that night); dry run by default, everything in ONE transaction:
    cp <this file> ~/ox/openalex-sources/handoff/oxjob2389-obor-former-title-2026-11-25.py && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2389-obor-former-title-2026-11-25.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2389-obor-former-title-2026-11-25.py --execute
The receipt (before-state + actions + after) is printed as JSON after the RECEIPT marker: save it as
handoff/oxjob2389-obor-former-title-2026-11-25-receipt.json and in #2389 work/.
Revert: DELETE FROM source_issn WHERE source_id = 5407043372 AND issn IN ('2959-8397', '2959-8400'); then
UPDATE sources SET alternate_titles = <receipt before.alternate_titles> WHERE id = 5407043372; then, in Python,
sources_lib.sync_issn_l_flags(conn, 5407043372) (re-derives sources.issns).
Check after the night: https://api.openalex.org/sources/issn:2959-8397 -> S5407043372, issn lists all four,
alternate_titles has 'OBOR Working Papers'.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import insert_issns, normalize_name, sync_issn_l_flags

EXECUTE = "--execute" in sys.argv
JOB = 2389
APPROVED = ("support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit); sources charter "
            "D5 2026-07-17 current name wins = RULES-DRAFT R3 + R10, as #1979 and #1225")

SID = 5407043372
EXPECT_NAME = "Hong Kong Review of Belt and Road Studies"
EXPECT_ISSNS = {"3007-5173", "3007-732X"}
EXPECT_ISSN_L = "3007-5173"
NEW_ISSNS = ["2959-8397", "2959-8400"]
ALT_TITLES = ["OBOR Working Papers"]

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved_by": APPROVED,
           "before": {}, "actions": [], "after": {}}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row_dict(r):
    return {k: (None if v is None else v if isinstance(v, (list, dict)) else str(v)) for k, v in r.items()}


def snapshot(conn, key):
    r = conn.execute(text("SELECT * FROM sources WHERE id = :i"), {"i": SID}).mappings().fetchone()
    assert r is not None, f"S{SID} missing (merged? see source_merge)"
    row = row_dict(r)
    row["_issns"] = sorted(x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"),
                                                      {"i": SID}))
    receipt[key][str(SID)] = row
    return row


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {normalize_name(t) for t in alts + [row.display_name or ""]}
    gained = []
    for t in titles:
        if t and t.strip() and normalize_name(t) not in have:
            gained.append(t.strip())
            have.add(normalize_name(t))
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), updated_date = now() WHERE id = :id"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "id": sid})
        log(f"alternate_titles {sid} += {gained}")
    else:
        log(f"alternate_titles {sid}: nothing to add")


def add_issns(conn, sid, issns):
    for issn in issns:
        taken = conn.execute(text("SELECT source_id FROM source_issn WHERE issn = :i"), {"i": issn}).scalar()
        assert taken is None, f"ISSN {issn} already on source {taken}: a merge question, stop and look"
        m = conn.execute(text("SELECT issn_l FROM issn_to_issnl WHERE issn = :i"), {"i": issn}).fetchone()
        log(f"ISSN {issn}: ISSN IC map -> {m[0] if m else 'not in the map (the journal site is the evidence)'}")
    insert_issns(conn, sid, issns, None)  # never touches a stored issn_l; re-derives sources.issns
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": sid})
    log(f"ISSNs {sid} += {issns}")


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            b = snapshot(conn, "before")
            # guards: still the journal we checked on 2026-11-25, with exactly the ISSNs we saw
            assert normalize_name(b["display_name"]) == normalize_name(EXPECT_NAME), b["display_name"]
            assert b["type"] == "journal", b["type"]
            assert set(b["_issns"]) == EXPECT_ISSNS, b["_issns"]
            assert b["issn_l"] == EXPECT_ISSN_L, b["issn_l"]

            add_issns(conn, SID, NEW_ISSNS)
            add_alt_titles(conn, SID, ALT_TITLES)
            sync_issn_l_flags(conn, SID)

            a = snapshot(conn, "after")
            assert set(a["_issns"]) == EXPECT_ISSNS | set(NEW_ISSNS), a["_issns"]
            assert set(a["issns"] or []) == EXPECT_ISSNS | set(NEW_ISSNS), a["issns"]
            assert a["issn_l"] == EXPECT_ISSN_L, a["issn_l"]
            assert "OBOR Working Papers" in (a["alternate_titles"] or []), a["alternate_titles"]
            flags = dict(conn.execute(text("SELECT issn, is_issn_l FROM source_issn WHERE source_id = :i"), {"i": SID}).fetchall())
            assert [k for k, v in flags.items() if v] == [EXPECT_ISSN_L], flags
            print(f"  {SID}: {a['display_name']!r} issn_l={a['issn_l']} issns={a['issns']} alt={a['alternate_titles']}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
