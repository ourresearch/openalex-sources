"""Register and load the Canadian scholarly journals list (Coalition Publica) as a public source list (oxjob #1205, ZD 27313).
Support factory #2254, written and run on desk 2026-11-24 from the openalex-sources checkout.

Why: the list's maintainer asked for it to be carried as a journal list (ZD 27313). It meets the published bar for adding
a list (help center data/source-lists, "an open, ISSN-keyed list"; the 2026-10-15 launch post: a list a country or
consortium decides on and commits to maintaining gets integrated): CC BY 4.0, every journal has an ISSN, maintained
outside OpenAlex by Coalition Publica (Erudit and PKP), updated twice a year. Source: Borealis doi:10.5683/SP4/YBWVYC,
version 1.0 released 2026-10-16 (file revues_can_2026-10-snapshot.tab, 1,327 journals, active and ceased), flattened to
data/source_lists/canadian-journals-2026-10-16.csv (name, ISSN-L + alternative ISSNs). One flat list, no levels.
Live API ISSN match on 2026-11-24: 1,098 of 1,327 journals -> 1,092 sources.

What it does, in ONE transaction (dry run by default; rolls back unless --execute). Rows only, no DDL, so it runs on the
lanes_registry login. It plays the role a migration (038-048) played for earlier lists:
  1. INSERT the source_list row (ON CONFLICT DO NOTHING).
  2. The same full-replace load as jobs.load_source_list.run(): DELETE + INSERT source_list_member from the CSV,
     list_version = 2026-10-16, loaded_at = now().
  3. recompute_listed_in() once (the single writer of sources.listed_in; it recomputes every list).
The guard refuses to commit if any other list's count moves.

Run on desk, NOT while the End 2 End is running (03:00 UTC until it publishes; the registry is read live by federation):
    cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1205-canadian-journals-list-2026-11-24.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1205-canadian-journals-list-2026-11-24.py --execute
Save the JSON after RECEIPT as handoff/oxjob1205-canadian-journals-list-2026-11-24-receipt.json and commit both files.
Revert (one transaction): the same command with --revert --execute (deletes the source_list row; members go by
ON DELETE CASCADE; then recompute_listed_in()). The id then leaves downstream data the same way it arrived: the
Databricks Sources job + Sync Sources to Elasticsearch (sources), the next End 2 End (works Delta), then a works ES
re-send of the works that carried it (listed_in is outside the works content hash). Save those work ids before the
revert. Then remove the id from elastic-api config/source-lists.yaml and openalex-gui src/listedIn.js.
"""
import csv
import json
import os
import re
import sys
from datetime import date, datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_issns, recompute_listed_in

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
JOB, TICKETS = 1205, [27313]
VERSION = date(2026, 10, 16)
CSV_DIR = os.path.join(os.path.dirname(HERE), "data", "source_lists")

LISTS = [
    ("canadian-journals", "Canadian scholarly journals (Coalition Publica)",
     "Coalition Publica (Érudit and the Public Knowledge Project)", "https://doi.org/10.5683/SP4/YBWVYC",
     "Scholarly journals (peer or editorial review, published periodically) managed mainly from a Canada-based "
     "institution, association or society, with an ISSN, and not associated with predatory publishers. All fields and "
     "languages; active and ceased journals."),
]
IDS = [lid for lid, *_ in LISTS]
EXPECT = {"canadian-journals": {"csv_rows": 1327, "member_rows": 1797}}  # the reviewed file; anything else stops the run
for lid, name, maint, url, scope in LISTS:
    assert "%" not in name + maint + url + scope  # a literal % broke a release once (045 gotcha); keep it out

ISSN_RE = re.compile(r"^\d{4}-\d{3}[\dX]$")


def read_rows(path):
    """jobs.load_source_list.read_rows, inlined: importing the jobs package loads sentry_sdk, which run_registry's uv
    environment does not install."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for i, r in enumerate(csv.DictReader(f), start=2):
            issns = [x for x in normalize_issns(re.split(r"[;,\s]+", r.get("issns") or "")) if x]
            bad = [x for x in issns if not ISSN_RE.match(x)]
            if bad:
                print(f"  {os.path.basename(path)} line {i}: skipping malformed ISSN(s) {bad} for {r.get('name')!r}")
                issns = [x for x in issns if ISSN_RE.match(x)]
            if not issns:
                continue
            active = (r.get("active") or "true").strip().lower() in ("true", "1", "yes", "y")
            wd = (r.get("withdrawn_date") or "").strip() or None
            rows.append({"name": (r.get("name") or "").strip() or None, "issns": issns, "active": active,
                         "withdrawn_date": date.fromisoformat(wd) if wd else None,
                         "withdrawal_reason": (r.get("withdrawal_reason") or "").strip() or None})
    return rows


receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert": REVERT, "job": JOB,
           "tickets": TICKETS, "before": {}, "lists": {}, "after": {}}


def pairs(conn):
    """Every (source id, list id) pair outside the lists this script loads: compared before and after, so a swap
    between two sources that leaves a count unchanged still stops the run."""
    return set(conn.execute(text("SELECT id, x FROM sources, UNNEST(listed_in) x WHERE NOT (x = ANY(:ids))"),
                            {"ids": IDS}).fetchall())


def counts(conn):
    rows = conn.execute(text("SELECT x, COUNT(*) FROM sources, UNNEST(listed_in) x GROUP BY x")).fetchall()
    return {k: v for k, v in rows}


conn = engine.connect()
trans = conn.begin()
try:
    # Fail fast instead of queueing behind another registry writer's locks (sources.listed_in is rewritten for all lists).
    conn.execute(text("SET LOCAL lock_timeout = '30s'"))
    existing = [r[0] for r in conn.execute(text("SELECT id FROM source_list WHERE id = ANY(:ids)"), {"ids": IDS})]
    before = counts(conn)
    before_pairs = pairs(conn)
    if not REVERT:
        assert not existing, f"{existing} already registered: this script adds a new list; reload with a new handoff"
    receipt["before"] = {"registered": existing, "listed_in_counts": before,
                         "sources_with_any_list": conn.execute(text(
                             "SELECT COUNT(*) FROM sources WHERE listed_in IS NOT NULL")).scalar()}
    print("BEFORE", json.dumps(receipt["before"]), flush=True)

    if REVERT:
        n = conn.execute(text("DELETE FROM source_list WHERE id = ANY(:ids)"), {"ids": IDS}).rowcount
        receipt["lists"]["deleted_source_list_rows"] = n
    else:
        for lid, name, maint, url, scope in LISTS:
            conn.execute(text("""INSERT INTO source_list (id, display_name, maintainer, url, scope)
                                 VALUES (:id, :dn, :m, :u, :s) ON CONFLICT (id) DO NOTHING"""),
                         {"id": lid, "dn": name, "m": maint, "u": url, "s": scope})
            path = os.path.join(CSV_DIR, f"{lid}-{VERSION.isoformat()}.csv")
            rows = read_rows(path)
            assert len(rows) == EXPECT[lid]["csv_rows"], (lid, len(rows))
            prior = conn.execute(text("SELECT COUNT(*) FROM source_list_member WHERE list_id = :id"),
                                 {"id": lid}).scalar()
            conn.execute(text("DELETE FROM source_list_member WHERE list_id = :id"), {"id": lid})
            members, seen = [], {}
            for r in rows:  # same dedupe as jobs.load_source_list.run(): an ISSN on two rows keeps the active one
                for issn in r["issns"]:
                    prev = seen.get(issn)
                    if prev is not None and (members[prev]["active"] or not r["active"]):
                        continue
                    rec = {"list_id": lid, "issn": issn, "name": r["name"], "active": r["active"],
                           "withdrawn_date": r["withdrawn_date"], "withdrawal_reason": r["withdrawal_reason"]}
                    if prev is not None:
                        members[prev] = rec
                    else:
                        seen[issn] = len(members)
                        members.append(rec)
            assert len(members) == EXPECT[lid]["member_rows"], (lid, len(members))
            # One statement per list (desk apply lane, 16 Nov): executemany sent one round trip per row, ~15 rows/s
            # to Heroku, so the 8 lists held the transaction open for ~35 min.
            conn.execute(text("""INSERT INTO source_list_member
                                   (list_id, issn, name, active, withdrawn_date, withdrawal_reason)
                                 SELECT list_id, issn, name, active, withdrawn_date, withdrawal_reason
                                 FROM json_to_recordset(CAST(:j AS json)) AS x(list_id text, issn text, name text,
                                      active boolean, withdrawn_date date, withdrawal_reason text)"""),
                         {"j": json.dumps(members, default=str)})
            conn.execute(text("UPDATE source_list SET list_version = :v, loaded_at = now() WHERE id = :id"),
                         {"v": VERSION, "id": lid})
            matched = conn.execute(text("""SELECT COUNT(DISTINCT si.source_id) FROM source_list_member m
                                           JOIN source_issn si ON si.issn = m.issn
                                           WHERE m.list_id = :id AND m.active"""), {"id": lid}).scalar()
            receipt["lists"][lid] = {"csv": os.path.basename(path), "csv_rows": len(rows),
                                     "csv_active": sum(r["active"] for r in rows), "members_before": prior,
                                     "member_rows": len(members), "active_issns": sum(m["active"] for m in members),
                                     "registry_sources_matched": matched}
            print(lid, json.dumps(receipt["lists"][lid]), flush=True)

    changed = recompute_listed_in(conn)
    after = counts(conn)
    receipt["after"] = {"listed_in_rows_changed_all_lists": changed,
                        "new_list_counts": {k: after.get(k, 0) for k in IDS},
                        "other_lists_moved": {k: [before.get(k, 0), after.get(k, 0)]
                                              for k in sorted(set(before) | set(after))
                                              if k not in IDS and before.get(k, 0) != after.get(k, 0)}}
    moved_pairs = before_pairs ^ pairs(conn)
    receipt["after"]["other_list_pairs_moved"] = len(moved_pairs)
    print("AFTER", json.dumps(receipt["after"]), flush=True)
    assert not moved_pairs, f"{len(moved_pairs)} (source, list) pairs of other lists moved, e.g. {sorted(moved_pairs)[:5]}"
    # Guard: loading this list must not move any other list. If it does, the registry drifted since its last
    # recompute; stop and look before committing.
    assert not receipt["after"]["other_lists_moved"], "other lists moved; investigate before --execute"
    if EXECUTE:
        trans.commit()
        print("committed")
    else:
        trans.rollback()
        print("DRY RUN: rolled back")
    print("RECEIPT", json.dumps(receipt, default=str))
except Exception:
    trans.rollback()
    raise
finally:
    conn.close()
