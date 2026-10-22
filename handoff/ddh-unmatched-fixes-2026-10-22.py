"""Registry fixes for Diamond Discovery Hub journals that the 2026-10-21 ddh load
could not match by ISSN, although OpenAlex has the journal (Kyle: "yes to the 10",
2026-10-22). Each source was checked by hand against DDH's homepage/publisher and
the source's own works (landing domains, years); see the commit message.

  2 merges   Annals de Medicina (two ISSN-less runs of one journal) and SORT
             (S973185361 'Sort' holds the print ISSN, 2 works; ISSN-less
             S99957990 has 25 works).
  10 adds    DDH's ISSNs onto the 10 surviving sources; then recompute_listed_in
             puts them on ddh (their ISSNs are already in source_list_member).

Run on the dyno (dry run by default; everything in ONE transaction):
    heroku run -a openalex-sources --no-tty "python handoff/ddh-unmatched-fixes-2026-10-22.py"
    heroku run -a openalex-sources --no-tty "python handoff/ddh-unmatched-fixes-2026-10-22.py --execute"
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker,
because the dyno's disk is thrown away; save it from the terminal.

Merges follow the post-D1 pattern of handoff/zendesk-ojs-tickets-2026-10-05.py
(loser deleted, ledger row in source_merge); jobs.merge_duplicates is disabled.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from sources_lib import insert_issns, normalize_name, recompute_listed_in, refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv

# (loser, winner, expected loser name, expected winner name, why)
MERGES = [
    (4306502279, 4306502280, "Annals de medicina", "Annals de Medicina",
     "ddh: same journal (Academia de Ciencies Mediques de Catalunya i de Balears), 2010-14 and 2019-21 runs, both ISSN-less"),
    (973185361, 99957990, "Sort", "Sort-statistics and Operations Research Transactions",
     "ddh: SORT (Idescat); loser holds print ISSN 1696-2281 with 2 works, winner is the ISSN-less 25-work record"),
]

# source id -> (expected display_name, ISSNs from DDH)
ADDS = {
    4306502280: ("Annals de Medicina", ["0210-7465", "2013-7109"]),
    4306535468: ("Zagadnienia Doradztwa Rolniczego", ["1232-3578", "2719-8901"]),
    4306511644: ("Gozdarski vestnik", ["0017-2723", "1581-1638"]),
    4306506690: ("Comprendre: Revista catalana de filosofia", ["1139-9759", "2385-5002"]),
    4306522897: ("Povijest u nastavi", ["1334-1375", "1849-1480"]),
    99957990: ("Sort-statistics and Operations Research Transactions", ["2013-8830"]),
    4306528804: ("Romanische Studien", ["2511-7882", "2364-4753"]),
    4306535834: ("Zeszyty Naukowe Warszawskiej Wyższej Szkoły Informatyki", ["1896-396X", "2082-8349"]),
    101353132: ("Acta Zoologica Cracoviensia", ["2299-6060", "2300-0163"]),  # bibliotekanauki.pl: ISSN/EISSN + DOI prefix 10.3409/azc
    7407065778: ("Suomen Sukututkimusseuran Vuosikirja", ["0355-3183"]),       # JUFO pairs 0355-3183 with 2343-2845
}

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "before": {}, "before_issns": {}, "actions": [], "after": {}}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        receipt["before"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
    for sid, issn in conn.execute(text("SELECT source_id, issn FROM source_issn WHERE source_id = ANY(:i)"),
                                  {"i": list(ids)}):
        receipt["before_issns"].setdefault(str(sid), []).append(issn)


def expect_name(conn, sid, name):
    row = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    assert row is not None, f"source {sid} missing"
    assert normalize_name(row.display_name) == normalize_name(name), (sid, row.display_name, name)


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [row.display_name or ""]}
    gained = [t for t in titles if t and t.strip().lower() not in have]
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained), "id": sid})
        log(f"alternate_titles {sid} += {gained}")


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in handoff/zendesk-ojs-tickets-2026-10-05.py."""
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
                      "VALUES (:l, :w, :r, 'ddh', CAST(:d AS JSONB))"),
                 {"l": loser, "w": winner, "r": rule, "d": json.dumps(detail)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    log(f"MERGED {loser} -> {winner} ({rule}): issns moved {moved}, endpoints {ep}, datacite {dc}, beacon {bm}")


def add_issns(conn, sid, issns):
    owners = dict(conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"), {"i": issns}).fetchall())
    clash = {i: s for i, s in owners.items() if s != sid}
    assert not clash, f"source {sid}: ISSNs already owned elsewhere {clash}"
    existing = {r[0] for r in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": sid})}
    missing = [i for i in issns if i not in existing]
    if not missing:
        log(f"source {sid}: already has {issns}")
        return
    issn_l = resolve_issn_l(conn, sorted(existing | set(issns)), member_only=True)
    insert_issns(conn, sid, missing, issn_l)
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": sid})
    log(f"ADDED ISSNs {missing} to {sid} (issn_l candidate {issn_l})")


class DryRun(Exception):
    pass


def main():
    ids = {x for m in MERGES for x in m[:2]} | set(ADDS)
    try:
        with engine.begin() as conn:
            snapshot(conn, ids)
            before_ddh = conn.execute(text("SELECT COUNT(*) FROM sources WHERE 'ddh' = ANY(listed_in)")).scalar()
            for loser, winner, lname, wname, why in MERGES:
                expect_name(conn, loser, lname)
                expect_name(conn, winner, wname)
                merge(conn, loser, winner, "ddh_unmatched_duplicate",
                      {"why": why, "list": "ddh", "approved_by": "kyle 2026-10-22"})
            for sid, (name, issns) in ADDS.items():
                expect_name(conn, sid, name)
                add_issns(conn, sid, issns)
            changed = recompute_listed_in(conn)
            after_ddh = conn.execute(text("SELECT COUNT(*) FROM sources WHERE 'ddh' = ANY(listed_in)")).scalar()
            for r in conn.execute(text("SELECT id, display_name, issn_l, issns, listed_in FROM sources "
                                       "WHERE id = ANY(:i) ORDER BY id"), {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} issn_l={r['issn_l']} issns={r['issns']} listed_in={r['listed_in']}")
            gone = ids - {int(k) for k in receipt["after"]}
            log(f"merged-away sources gone: {sorted(gone)}")
            log(f"sources.listed_in rows changed (all lists): {changed}; listed in ddh: {before_ddh} -> {after_ddh}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
