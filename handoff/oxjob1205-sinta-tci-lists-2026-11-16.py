"""Register and load SINTA (Indonesia, S1-S6) and TCI (Thailand, Tier 1-2) as public source lists (oxjob #1205, ZD 26568).
Support factory #2254, written off desk 2026-11-16 (close-out lane for #1205); run on desk from the openalex-sources checkout.

Why: Kyle's yes (2026-10-26, ZD 26568 + #1205 TICKETS.md): make SINTA and TCI public `listed_in` lists like the other
national lists. The CSVs and adapters exist since #1426 (data/source_lists/sinta-s1..s6-2026-09-29.csv,
tci-1/2-2026-09-29.csv; jobs/fetch_source_list.py); #1426 kept them unregistered (Casey 2026-09-30) because walden's
sync_source_lists publishes every registered list. Live 2026-11-16: no sinta-*/tci-* in /sources?group_by=listed_in,
/source-lists/sinta-s1 -> 404.

What it does, in ONE transaction (dry run by default; rolls back unless --execute). Rows only, no DDL, so it runs on the
lanes_registry login. It plays the role a migration (038-048) played for earlier lists:
  1. INSERT the 8 source_list rows (ON CONFLICT DO NOTHING; same conventions as 040/045: one list per level, ids and
     display names keep the maintainer's own labels, the scope says which level is the top one).
  2. For each list, the same full-replace load as jobs.load_source_list.run(): DELETE + INSERT source_list_member from
     the CSV, list_version = 2026-09-29, loaded_at = now().
  3. recompute_listed_in() once (the single writer of sources.listed_in; it recomputes every list).
Expected (live API ISSN match, 2026-11-16): sinta-s1 146, s2 1,426, s3 2,980, s4 5,104, s5 4,142, s6 131 sources;
tci-1 372, tci-2 573. The registry's own match is printed per list; within a few % of these is right.

Run on desk, NOT while the End 2 End is running (05:00 UTC until it publishes; the registry is read live by federation):
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1205-sinta-tci-lists-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1205-sinta-tci-lists-2026-11-16.py --execute
Save the JSON after RECEIPT as handoff/oxjob1205-sinta-tci-lists-2026-11-16-receipt.json and commit both files.
Revert (one transaction): DELETE FROM source_list WHERE id IN ('sinta-s1','sinta-s2','sinta-s3','sinta-s4','sinta-s5',
'sinta-s6','tci-1','tci-2');  (members go by ON DELETE CASCADE)  then  python -c "from db import engine;
from sources_lib import recompute_listed_in; c=engine.begin().__enter__(); print(recompute_listed_in(c))"  -- or
re-run this script's --revert path below.
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
JOB, TICKETS = 1205, [26568]
VERSION = date(2026, 9, 29)
CSV_DIR = os.path.join(os.path.dirname(HERE), "data", "source_lists")

SINTA_MAINTAINER = "Ministry of Higher Education, Science and Technology of Indonesia (Kemdiktisaintek)"
SINTA_URL = "https://sinta.kemdiktisaintek.go.id/journals"
TCI_MAINTAINER = "Thai-Journal Citation Index Centre (TCI)"
TCI_URL = "https://tci-thailand.org/"
ORD = {1: "the top rank of six", 2: "second of six ranks", 3: "third of six ranks", 4: "fourth of six ranks",
       5: "fifth of six ranks", 6: "sixth of six ranks"}

LISTS = [(f"sinta-s{k}", f"SINTA, S{k}", SINTA_MAINTAINER, SINTA_URL,
          f"Journals accredited at rank S{k} ({ORD[k]}) in SINTA (Science and Technology Index), Indonesia's national "
          "journal accreditation, all fields. Only journals whose accreditation covers the current year.")
         for k in range(1, 7)] + [
    ("tci-1", "TCI, Tier 1", TCI_MAINTAINER, TCI_URL,
     "Journals certified in Tier 1 (the top tier of two) by the Thai-Journal Citation Index Centre, Thailand's national "
     "journal certification, all fields."),
    ("tci-2", "TCI, Tier 2", TCI_MAINTAINER, TCI_URL,
     "Journals certified in Tier 2 (second of two tiers) by the Thai-Journal Citation Index Centre, Thailand's national "
     "journal certification, all fields."),
]
IDS = [lid for lid, *_ in LISTS]
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


def counts(conn):
    rows = conn.execute(text("SELECT x, COUNT(*) FROM sources, UNNEST(listed_in) x GROUP BY x")).fetchall()
    return {k: v for k, v in rows}


conn = engine.connect()
trans = conn.begin()
try:
    existing = [r[0] for r in conn.execute(text("SELECT id FROM source_list WHERE id = ANY(:ids)"), {"ids": IDS})]
    before = counts(conn)
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
    print("AFTER", json.dumps(receipt["after"]), flush=True)
    # Guard: loading these lists must not move any other list. If it does, the registry drifted since its last
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
