"""Registry fix for ZD 27365 item 4, oxjob #2173 (support factory #2254), 2026-11-19: ADS's observing-proposal collections
and VizieR are not journals.
Checked 19 Nov 2026 (#2173 work/, locations by ADS bibstem): each record holds MAG copies of ADS records of one kind:
  S4306505931 'Chandra Proposal'     2,627 (cxo..prop)   S4306507475 'cxo' 1,734 (cxo..prop) -> merged into it
  S4306512385 'HST Proposal'         5,798 (hst..prop)   S4306512384 'hst' 3,600 (hst..prop) -> merged into it
  S4306535189 'XMM-Newton Proposal'  2,101 (xmm..prop)
  S4306528930 'RXTE Proposal'        1,245 (rxte.prop)
  S4306514652 'IUE Proposal'         2,815 (iue..prop)
  S4306534428 'VizieR Online Data Catalog' 15,922 (yCat: CDS's catalogue service, which hosts the datasets)
The proposal records are the observatories' archives of accepted observing proposals, and VizieR hosts catalogues: a
work's source is whoever hosts it (data-bugs 'What right means', 2026-10-27), so each is retyped 'repository'
(journal before). The two abbreviated twins merge into the full-title records (post-D1 merge, D5 current name wins);
their MAG copies, named 'cxo' and 'hst', find the winners through openalex.sources.merged_source_names (#2173 wake-up).

Run on desk (dry run by default; ONE transaction):
    cd ~/ox/openalex-sources && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2173-proposal-sources-2026-11-19.py [--execute]
Revert: UPDATE sources SET type = 'journal' WHERE id IN (the six retyped ids); a merged loser is restored by
re-inserting its receipt `before` row and deleting its source_merge row (rule zendesk_2173_abbreviated_duplicate).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name, refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
JOB = 2173
APPROVED = "support factory #2254 (charter rules 2-3; D5 2026-07-17 current name wins)"
RULE = "zendesk_2173_abbreviated_duplicate"
MERGES = [
    (4306507475, 4306505931, "cxo is ADS's Chandra proposal collection (same bibstem cxo..prop)"),
    (4306512384, 4306512385, "hst is ADS's HST proposal collection (same bibstem hst..prop)"),
]
RETYPE = [4306505931, 4306512385, 4306535189, 4306528930, 4306514652, 4306534428]
EXPECT = {4306505931: "Chandra Proposal", 4306507475: "cxo", 4306512385: "HST Proposal", 4306512384: "hst",
          4306535189: "XMM-Newton Proposal", 4306528930: "RXTE Proposal", 4306514652: "IUE Proposal",
          4306534428: "VizieR Online Data Catalog"}
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


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in handoff/oxjob629-knob-2026-11-18.py and oxjob629-ticket-merges-2026-11-15.py."""
    rows = {r.id: r for r in conn.execute(
        text("SELECT id, display_name, alternate_titles, override_timestamp, issn_l FROM sources WHERE id IN (:l, :w)"),
        {"l": loser, "w": winner})}
    assert loser in rows and winner in rows, (loser, winner)
    assert rows[loser].override_timestamp is None, f"loser {loser} is curator-overridden"
    conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :l"), {"l": loser})
    moved = conn.execute(text("UPDATE source_issn SET source_id = :w, is_issn_l = FALSE WHERE source_id = :l"),
                         {"l": loser, "w": winner}).rowcount
    ep = conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
    dc = conn.execute(text("UPDATE source_datacite_id SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
    bm = conn.execute(text("INSERT INTO ojs_beacon_mint (source_id, edition, receipt, minted_at) "
                           "SELECT :w, edition, receipt, minted_at FROM ojs_beacon_mint WHERE source_id = :l "
                           "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner}).rowcount
    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                      "SELECT :w, curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :l "
                      "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner})
    conn.execute(text("DELETE FROM source_oa_override WHERE source_id = :l"), {"l": loser})
    add_alt_titles(conn, winner, [rows[loser].display_name] + list(rows[loser].alternate_titles or []))
    issns = [r[0] for r in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": winner})]
    if issns and rows[winner].issn_l is None:
        issn_l = resolve_issn_l(conn, issns, member_only=True)
        conn.execute(text("UPDATE sources SET issn_l = :l WHERE id = :id"), {"l": issn_l, "id": winner})
    conn.execute(text("UPDATE source_issn si SET is_issn_l = (si.issn IS NOT DISTINCT FROM s.issn_l) "
                      "FROM sources s WHERE s.id = :id AND si.source_id = :id"), {"id": winner})
    refresh_issns_column(conn, winner)
    conn.execute(text("INSERT INTO source_merge (loser_id, winner_id, rule, source_feed, detail) "
                      "VALUES (:l, :w, :r, 'zendesk', CAST(:d AS JSONB))"),
                 {"l": loser, "w": winner, "r": rule, "d": json.dumps({**detail, "loser_name": rows[loser].display_name},
                                                                        ensure_ascii=False)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    log(f"MERGED {loser} -> {winner} ({rule}): issns moved {moved}, endpoints {ep}, datacite {dc}, beacon {bm}")


class DryRun(Exception):
    pass


def main():
    ids = set(EXPECT)
    try:
        with engine.begin() as conn:
            snapshot(conn, ids)
            for sid, name in EXPECT.items():
                expect_name(conn, sid, name)
            assert conn.execute(text("SELECT count(*) FROM sources WHERE id = ANY(:i) AND type = 'journal'"),
                                {"i": list(ids)}).scalar() == len(ids)
            for loser, winner, note in MERGES:
                merge(conn, loser, winner, RULE, {"tickets": [27365], "oxjob": JOB, "approved_by": APPROVED, "note": note})
            for sid in RETYPE:
                upd(conn, sid, type="repository")
            for r in conn.execute(text("SELECT id, display_name, type, alternate_titles FROM sources WHERE id = ANY(:i) "
                                       "ORDER BY id"), {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} type={r['type']} alts={r['alternate_titles']}")
            gone = ids - {int(k) for k in receipt["after"]}
            log(f"merged-away sources gone: {sorted(gone)}")
            assert gone == {m[0] for m in MERGES}, gone
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
