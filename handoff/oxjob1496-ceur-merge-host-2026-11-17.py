"""Registry fix for CEUR Workshop Proceedings (oxjob #1496, ZD 24867 TIB), support factory #2254, written off desk
2026-11-17 (#2263 lane). Same merge() steps as handoff/oxjob1580-masj-merge-2026-11-16.py (itself oxjob629's).

1. Merge (loser -> winner): S7407060682 'ceur-ws.org' (type repository, no ISSN, is_oa false, minted 2026-10-16 from
   CEUR-WS.org's DataCite client; holds 414 works with CEUR's new DOIs 10.83095/ceurws_*) -> S4210204615 'CEUR Workshop
   Proceedings' (ISSN 1613-0073, is_oa true). Why: since Oct 2026 CEUR-WS.org registers DataCite DOIs for some volumes
   (prefix 10.83095, 448 DOIs on 17 Nov; publisher 'CEUR-WS.org', no container, so sync_datacite_clients minted a
   repository source). They are papers of CEUR Workshop Proceedings (DOI URLs resolve to ceur-ws.org/Vol-NNNN/...),
   so they belong on the ISSN record; today all 414 read green, as repository copies on a non-OA twin, not as the
   venue's own open copies. The DataCite link moves to the winner
   (merge() re-points source_datacite_id), so the feed can't re-mint.
2. Winner host: publisher_id 4310312761 (Samara National Research University, wrong) -> NULL, institution_id ->
   4210134436 (German Informatics Society / Gesellschaft fuer Informatik, ROR 03220m208). ceur-ws.org itself: "a free,
   diamond open-access publication service of GI eV ... hosted on a server of Gesellschaft fuer Informatik"
   (#1496 work/ceur_homepage_2026-11-17.txt). Registry rule: host = publisher_id if set, else institution_id. The
   loser already has GI as its host. If `publisher` text names Samara, it is set to 'Gesellschaft für Informatik e.V.'.

Run on desk from the openalex-sources checkout (dry run by default; ONE transaction):
    cp <this file> handoff/ && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1496-ceur-merge-host-2026-11-17.py [--execute]
Receipt JSON prints after the RECEIPT marker: save it as handoff/oxjob1496-ceur-merge-host-2026-11-17-receipt.json and
in #1496 work/. Re-stamps ~414 works (source change) plus the host on ~524: rides the End 2 End; register it in
change-throughput-coordinator/changes.yaml first.
Revert: re-insert the loser's `before` row (receipt), UPDATE source_datacite_id SET source_id=7407060682 WHERE
source_id=4210204615 AND datacite_id IN (<the loser's before links>), DELETE FROM source_merge WHERE
loser_id=7407060682, remove 'ceur-ws.org' from the winner's alternate_titles, and set the winner's publisher_id,
institution_id and publisher back to their `before` values.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
JOB = 1496
APPROVED = ("support factory #2254 (Jason 2026-11-15 yes); objective duplicate: same publisher CEUR-WS.org, same "
            "site, DOIs resolve to CEUR volumes; host from the venue's own site (D5 rule)")
LOSER, WINNER = 7407060682, 4210204615
EXPECT = {LOSER: "ceur-ws.org", WINNER: "CEUR Workshop Proceedings"}
OLD_PUBLISHER_ID, GI_INSTITUTION = 4310312761, 4210134436
GI_NAME = "Gesellschaft für Informatik e.V."

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
        row["_datacite"] = [x[0] for x in conn.execute(text("SELECT datacite_id FROM source_datacite_id WHERE source_id = :i"), {"i": r["id"]})]
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
    """Same steps as merge() in handoff/oxjob1580-masj-merge-2026-11-16.py."""
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
    try:
        with engine.begin() as conn:
            ids = {LOSER, WINNER}
            snapshot(conn, ids)
            for sid, name in EXPECT.items():
                got = conn.execute(text("SELECT display_name FROM sources WHERE id = :i"), {"i": sid}).scalar()
                assert got == name, (sid, got)
            lb, wb = receipt["before"][str(LOSER)], receipt["before"][str(WINNER)]
            # guards: the loser is the DataCite-minted twin (no ISSN, no OAI feed, >=1 DataCite client link);
            # the winner is the ISSN record, OA, still hosted by the wrong publisher id
            assert lb["_issns"] == [], lb["_issns"]
            assert lb["_endpoints"] == [], lb["_endpoints"]
            assert len(lb["_datacite"]) >= 1, lb["_datacite"]
            assert "1613-0073" in wb["_issns"], wb["_issns"]
            assert conn.execute(text("SELECT is_oa FROM sources WHERE id = :i"), {"i": WINNER}).scalar() is True
            pub_id, inst_id, pub = conn.execute(text("SELECT publisher_id, institution_id, publisher FROM sources "
                                                     "WHERE id = :i"), {"i": WINNER}).fetchone()
            assert pub_id == OLD_PUBLISHER_ID, pub_id
            assert inst_id in (None, GI_INSTITUTION), inst_id
            merge(conn, LOSER, WINNER, "zendesk_1496_datacite_client_twin",
                  {"tickets": [24867], "oxjob": JOB, "approved_by": APPROVED,
                   "note": "CEUR-WS.org DataCite client (DOIs 10.83095/ceurws_*, from Oct 2026) minted a repository "
                           "twin of the ISSN record; the client link rides along so the DataCite feed can't re-mint"})
            new_pub = GI_NAME if (pub is None or "samara" in pub.lower()) else pub
            conn.execute(text("UPDATE sources SET publisher_id = NULL, institution_id = :g, publisher = :p, "
                              "updated_date = now() WHERE id = :w"), {"g": GI_INSTITUTION, "p": new_pub, "w": WINNER})
            log(f"HOST {WINNER}: publisher_id {pub_id} -> NULL, institution_id {inst_id} -> {GI_INSTITUTION}, "
                f"publisher {pub!r} -> {new_pub!r}")
            for r in conn.execute(text("SELECT id, display_name, type, is_oa, issn_l, issns, publisher, publisher_id, "
                                       "institution_id, homepage_url, alternate_titles FROM sources WHERE id = ANY(:i) "
                                       "ORDER BY id"), {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} is_oa={r['is_oa']} issn_l={r['issn_l']} "
                      f"publisher_id={r['publisher_id']} institution_id={r['institution_id']}")
            receipt["after"]["_datacite_winner"] = [x[0] for x in conn.execute(
                text("SELECT datacite_id FROM source_datacite_id WHERE source_id = :i"), {"i": WINNER})]
            assert set(receipt["after"]) == {str(WINNER), "_datacite_winner"}, receipt["after"].keys()
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
