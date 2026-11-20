"""Registry fix for ZD 22564 (oxjob #1580), support factory #2254, written off desk 2026-11-16 (#2263 lane).

Merge (loser -> winner; same steps as merge() in handoff/oxjob629-ticket-merges-2026-11-15.py: loser deleted, ledger
row in source_merge, which also serves the merged-ID redirect):
  S7407089440 'Journal of 21 September university for medical and applied sciences' (ISSN 5937-2958, 6 works, is_oa
  false) -> S7407058046 (the same journal, MASJ: 2958-5929 / 2958-5937, is_oa true).
Why: Crossref's deposits for MASJ vol 1 issue 1 (10.65693/masj.2022.v1i1 and .11/.14/.15/.17/.18) carry the e-ISSN
with its halves swapped, 5937-2958 (container-title = MASJ's). handoff/zendesk-ojs-tickets-2026-10-05.py removed that
"reversed junk ISSN" from S7407058046; the Crossref feed then minted S7407089440 for it, so those 6 MASJ articles read
hybrid/closed on a non-OA twin. The ISSN rides along to the winner so the feed can't re-mint (as #629 did for
InfoScience Trends' 1010-5026 / 2091-1114). Live receipts: #1580 work/recheck_2026-11-16.json.

Run on desk from the openalex-sources checkout (dry run by default; ONE transaction):
    cp <this file> handoff/ && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1580-masj-merge-2026-11-16.py [--execute]
Receipt JSON prints after the RECEIPT marker: save it as handoff/oxjob1580-masj-merge-2026-11-16-receipt.json and in
#1580 work/.
Revert: re-insert the loser's `before` row, move ISSN 5937-2958 back (UPDATE source_issn SET source_id=7407089440
WHERE issn='5937-2958'), re-run refresh_issns_column on both, DELETE FROM source_merge WHERE loser_id=7407089440.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import refresh_datacite_column, refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
JOB = 1580
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); objective duplicate: same title, publisher, DOI prefix"
LOSER, WINNER = 7407089440, 7407058046
EXPECT = {LOSER: "Journal of 21 September university for medical and applied sciences",
          WINNER: "Journal of 21 September university for medical and applied sciences"}

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
        row["_oa_override"] = [dict(x) for x in conn.execute(text("SELECT * FROM source_oa_override WHERE source_id = :i"), {"i": r["id"]}).mappings()]
        receipt["before"][str(r["id"])] = row


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [row.display_name or ""]}
    gained = [t.strip() for t in titles if t and t.strip() and t.strip().lower() not in have]
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained), "id": sid})
        log(f"alternate_titles {sid} += {gained}")


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in handoff/oxjob629-ticket-merges-2026-11-15.py."""
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
    if dc:  # added 2026-11-20 (#1496), after this script ran: keep the derived column in step
        refresh_datacite_column(conn, winner)
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
    try:
        with engine.begin() as conn:
            ids = {LOSER, WINNER}
            snapshot(conn, ids)
            for sid, name in EXPECT.items():
                got = conn.execute(text("SELECT display_name FROM sources WHERE id = :i"), {"i": sid}).scalar()
                assert got == name, (sid, got)
            # guards: the loser holds only the swapped ISSN and no feed; the winner is the OA MASJ record
            assert receipt["before"][str(LOSER)]["_issns"] == ["5937-2958"], receipt["before"][str(LOSER)]["_issns"]
            assert receipt["before"][str(LOSER)]["_endpoints"] == [], receipt["before"][str(LOSER)]["_endpoints"]
            assert "5937-2958" not in receipt["before"][str(WINNER)]["_issns"]
            assert conn.execute(text("SELECT is_oa FROM sources WHERE id = :i"), {"i": WINNER}).scalar() is True
            merge(conn, LOSER, WINNER, "zendesk_1580_swapped_issn_twin",
                  {"tickets": [22564], "oxjob": JOB, "approved_by": APPROVED,
                   "note": "MASJ vol 1 issue 1 DOIs (10.65693/masj.2022.v1i1.*) deposited at Crossref under 5937-2958 "
                           "(2958-5937 with its halves swapped); the ISSN rides along so the Crossref feed can't re-mint"})
            for r in conn.execute(text("SELECT id, display_name, type, is_oa, issn_l, issns, publisher_id, institution_id, "
                                       "homepage_url, alternate_titles FROM sources WHERE id = ANY(:i) ORDER BY id"),
                                  {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} is_oa={r['is_oa']} issn_l={r['issn_l']} issns={r['issns']}")
            assert set(receipt["after"]) == {str(WINNER)}, receipt["after"].keys()
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
