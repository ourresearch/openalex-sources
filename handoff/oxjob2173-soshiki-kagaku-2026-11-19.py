"""Registry fix for ZD 3444 / 3396, oxjob #2173 (support factory #2254), 2026-11-19: 組織科学 (Organizational Science, the
Academic Association for Organizational Science's journal) gets its ISSNs, homepage and English title.
Witness (RULINGS D5: a third party): J-STAGE's journal page https://www.jstage.jst.go.jp/browse/soshikikagaku/-char/en
(fetched 19 Nov 2026) names 'Organizational Science', Online ISSN 2187-932X, Print ISSN 0286-9713, ISSN-L 0286-9713;
CiNii Research names the same journal and society for its articles. No live source holds either ISSN (API 19 Nov: 0).
Its MAG copies, which MAG filed under INFORMS's Organization Science (S206124708), move to it through location source_id
curations (oxjob #2173); with the ISSNs, an ISSN lookup finds the journal too.

Run on desk (dry run by default; ONE transaction):
    cd ~/ox/openalex-sources && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2173-soshiki-kagaku-2026-11-19.py [--execute]
Revert: DELETE FROM source_issn WHERE source_id = 4306556653 AND issn IN ('0286-9713', '2187-932X'); then restore the
receipt's `before` issn_l / issns / homepage_url / alternate_titles of 4306556653.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name, refresh_issns_column

EXECUTE = "--execute" in sys.argv
JOB = 2173
SID = 4306556653
ISSNS = [("0286-9713", True), ("2187-932X", False)]
HOMEPAGE = "https://www.jstage.jst.go.jp/browse/soshikikagaku/-char/en"
ALTS = ["Organizational Science", "Soshiki kagaku"]
EXPECT = {SID: "組織科学"}
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB,
           "before": {}, "actions": [], "after": {}}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        row = {k: (None if v is None else str(v)) for k, v in r.items()}
        row["_issns"] = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": r["id"]})]
        row["_endpoints"] = [x[0] for x in conn.execute(text("SELECT id FROM oai_pmh_endpoint WHERE source_id = :i"), {"i": r["id"]})]
        receipt["before"][str(r["id"])] = row


def expect_name(conn, sid, name):
    row = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    assert row is not None, f"source {sid} missing"
    assert normalize_name(row.display_name) == normalize_name(name), (sid, row.display_name, name)


def upd(conn, sid, **fields):
    sets = ", ".join(f"{k} = :{k}" for k in fields)
    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"),
                     {**fields, "id": sid}).rowcount
    assert n == 1, (sid, fields)
    log(f"UPDATE sources {sid}: {fields}")


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [row.display_name or ""]}
    gained = []
    for t in titles:
        if t and t.strip() and t.strip().lower() not in have:
            gained.append(t.strip())
            have.add(t.strip().lower())
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "id": sid})
        log(f"alternate_titles {sid} += {gained}")


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            snapshot(conn, {SID})
            expect_name(conn, SID, EXPECT[SID])
            taken = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                 {"i": [i for i, _ in ISSNS]}).fetchall()
            assert not taken, f"ISSN already held: {taken}"
            for issn, is_l in ISSNS:
                conn.execute(text("INSERT INTO source_issn (source_id, issn, is_issn_l) VALUES (:s, :i, :l)"),
                             {"s": SID, "i": issn, "l": is_l})
                log(f"source_issn += ({SID}, {issn}, is_issn_l={is_l})")
            upd(conn, SID, issn_l="0286-9713", homepage_url=HOMEPAGE)
            refresh_issns_column(conn, SID)
            add_alt_titles(conn, SID, ALTS)
            r = conn.execute(text("SELECT id, display_name, issn_l, issns, homepage_url, alternate_titles FROM sources "
                                  "WHERE id = :i"), {"i": SID}).mappings().one()
            receipt["after"][str(SID)] = {k: (None if v is None else str(v)) for k, v in r.items()}
            print(f"  {dict(r)}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
