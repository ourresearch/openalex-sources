"""Registry merges for ZD 27365, oxjob #2173 (support factory #2254), 2026-11-19: abbreviated duplicates of five venues.
Each pair was checked live on 2026-11-19 (receipts in the #2173 job, work/duplicates_live_2026-11-19.txt): both records
hold MAG copies of the same venue's ADS records, under the same ADS bibstem.

  S4306503678 'ATel'  (4,790 works, ADS 'ATel')  -> S2764461182 'The astronomer's telegram' (6,753, ADS 'ATel')
  S4306520295 'MPEC'  (394, ADS 'MPEC')          -> S4306519990 'Minor Planet Electronic Circulars' (872, ADS 'MPEC')
  S4306520294 'MPBu'  (820, ADS 'MPBu')          -> S154778200 'The Minor planet bulletin' (1,146, ISSN 1052-8091, ADS 'MPBu')
  S4306514763 'Izvestiya Ordena Trudovogo Krasnogo Znameni Krymskoj Astrofizicheskoj Observatorii' (623, ADS 'IzKry')
                                                 -> S186307442 'Bulletin of the Crimean Astrophysical Observatory'
                                                    (1,883; already carries the Izvestiya's ISSN 0367-8466 and its own
                                                    site's records, jncrao.ru/izvcrao)
  S4306509628 'EPSC'  (2,391, ADS 'EPSC')        -> S4306509627 'epsc' (4,480, ADS 'EPSC' / 'epsc')

Merges use the post-D1 pattern of handoff/oxjob629-knob-2026-11-18.py (loser deleted, ledger row in source_merge, which
also serves the merged-ID redirect); D5 2026-07-17 (current name wins: the winner keeps its name, the loser's name
becomes an alternate title). MAG copies named 'ATel', 'MPEC', 'MPBu' and the Izvestiya name find their winner through
openalex.sources.merged_source_names rows (a Databricks write, the #2173 wake-up's step); 'EPSC' finds 'epsc' through
walden's normalized-name tier.

Run on desk (dry run by default; everything in ONE transaction):
    cd ~/ox/openalex-sources && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2173-abbreviated-duplicates-2026-11-19.py [--execute]
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker.
Revert: re-insert each loser's `before` row (with its source_issn rows), move back what the MERGED line says moved, drop
the added alternate titles from the winner, and delete its source_merge row (rule zendesk_2173_abbreviated_duplicate).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import refresh_datacite_column, normalize_name, refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
JOB = 2173
APPROVED = "support factory #2254 (charter rules 2-3; D5 2026-07-17 current name wins)"
RULE = "zendesk_2173_abbreviated_duplicate"
MERGES = [
    (4306503678, 2764461182, "ATel is The Astronomer's Telegram (same ADS bibstem ATel)"),
    (4306520295, 4306519990, "MPEC is the Minor Planet Electronic Circulars (same ADS bibstem MPEC)"),
    (4306520294, 154778200, "MPBu is The Minor Planet Bulletin (same ADS bibstem MPBu)"),
    (4306514763, 186307442, "the Izvestiya (ISSN 0367-8466) is already on the Bulletin's record with its own site's records"),
    (4306509628, 4306509627, "EPSC and epsc are the same congress (same ADS bibstem EPSC)"),
]
EXPECT = {4306503678: "ATel", 2764461182: "The astronomer's telegram", 4306520295: "MPEC",
          4306519990: "Minor Planet Electronic Circulars", 4306520294: "MPBu", 154778200: "The Minor planet bulletin",
          4306514763: "Izvestiya Ordena Trudovogo Krasnogo Znameni Krymskoj Astrofizicheskoj Observatorii",
          186307442: "Bulletin of the Crimean Astrophysical Observatory", 4306509628: "EPSC", 4306509627: "epsc"}

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
    ids = set(EXPECT)
    try:
        with engine.begin() as conn:
            snapshot(conn, ids)
            for sid, name in EXPECT.items():
                expect_name(conn, sid, name)
            for loser, winner, note in MERGES:
                merge(conn, loser, winner, RULE, {"tickets": [27365], "oxjob": JOB, "approved_by": APPROVED, "note": note})
            for r in conn.execute(text("SELECT id, display_name, type, issn_l, issns, publisher_id, institution_id, "
                                       "homepage_url, alternate_titles FROM sources WHERE id = ANY(:i) ORDER BY id"),
                                  {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} type={r['type']} issns={r['issns']} alts={r['alternate_titles']}")
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
