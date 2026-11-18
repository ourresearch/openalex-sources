"""Registry fix for ZD 27833 (Bulletin KNOB: four sources share the name), oxjob #629, support factory #2254, 2026-11-18.
Checked live and in this registry on 2026-11-18 (receipts in the #629 job, work/registry_knob_2026-11-18.json):

  S4210207069 'Bulletin KNOB'  journal, ISSN 0166-0470 / 2589-3343, DOAJ, host KNOB, homepage bulletin.knob.nl  = WINNER
  S4377196171 'Bulletin KNOB'  journal, no ISSN, 97 works, bound to the journal's own OJS feed
              http://bulletin.knob.nl/index.php/knob/oai (endpoint 841df62c5bc3ec9a5f6)     -> merged into the winner
  S4306505203 'Bulletin KNOB'  journal, no ISSN, 0 works, no endpoint                           -> merged into the winner
  S4377196173 'Bulletin KNOB'  repository, 273 works: it is NOT KNOB. Its 14 endpoints are all
              http://journal.lppmunindra.ac.id/index.php/index/oai (Universitas Indraprasta PGRI's OJS portal, whose
              OAI Identify says repositoryName 'Official Journal of Universitas Indraprasta PGRI', repositoryIdentifier
              'ojs.localhost'; every sampled work is an Unindra article, DOI prefix 10.30998). Renamed to the portal's own
              name, host institution I3132561136 Universitas Indraprasta PGRI, homepage the portal. Its endpoints stay
              where they are: re-binding the per-journal sets is #1418's (ojs-install-splits) program.

Merges use the post-D1 pattern of handoff/oxjob629-ticket-merges-2026-11-15.py (loser deleted, ledger row in
source_merge, which also serves the merged-ID redirect, #1124); D5 2026-07-17 (current name wins). The journal's own OJS
record merged into the journal follows the 15 Nov precedents (ZD 8250 Scientia et technica, ZD 4886 CR Math).

Run on desk (dry run by default; everything in ONE transaction):
    cd ~/ox/openalex-sources && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob629-knob-2026-11-18.py [--execute]
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker.
Revert: the receipt's `before` holds every touched row. A merged loser is restored by re-inserting its `before` row,
moving endpoint 841df62c5bc3ec9a5f6 back to 4377196171 (the MERGED line names the counts), and deleting its
source_merge row (rule zendesk_629_ticket_merge, detail.tickets [27833]); the rename is reverted by restoring
display_name / institution_id / homepage_url / alternate_titles / override fields of 4377196173 from `before`.
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
JOB = 629
APPROVED = "support factory #2254 (charter rules 2-3; D5 2026-07-17 current name wins)"

WINNER = 4210207069
UNINDRA = 4377196173
MERGES = [
    (4377196171, WINNER, [27833], "the journal's own OJS feed (bulletin.knob.nl/index.php/knob/oai) minted as a second record"),
    (4306505203, WINNER, [27833], "no-ISSN record, 0 works, no endpoint"),
]
EXPECT = {WINNER: "Bulletin KNOB", 4377196171: "Bulletin KNOB", 4306505203: "Bulletin KNOB", UNINDRA: "Bulletin KNOB"}
UNINDRA_NAME = "Official Journal of Universitas Indraprasta PGRI"
UNINDRA_HOMEPAGE = "https://journal.lppmunindra.ac.id/"
I_UNINDRA = 3132561136
KNOB_ENDPOINT = "841df62c5bc3ec9a5f6"
UNINDRA_PMH = "http://journal.lppmunindra.ac.id/index.php/index/oai"

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
            # guards: the KNOB feed is on the record we merge, every endpoint of the misnamed record is Unindra's
            assert conn.execute(text("SELECT source_id FROM oai_pmh_endpoint WHERE id = :e"),
                                {"e": KNOB_ENDPOINT}).scalar() == 4377196171
            urls = {r[0] for r in conn.execute(text("SELECT DISTINCT pmh_url FROM oai_pmh_endpoint WHERE source_id = :s"),
                                               {"s": UNINDRA})}
            assert urls == {UNINDRA_PMH}, urls
            assert conn.execute(text("SELECT count(*) FROM sources WHERE id = :s AND type = 'repository'"),
                                {"s": UNINDRA}).scalar() == 1
            assert conn.execute(text("SELECT count(*) FROM sources WHERE id = :s AND override_timestamp IS NULL"),
                                {"s": UNINDRA}).scalar() == 1

            for loser, winner, tickets, note in MERGES:
                merge(conn, loser, winner, "zendesk_629_ticket_merge",
                      {"tickets": tickets, "oxjob": JOB, "approved_by": APPROVED, "note": note})

            # The Unindra portal record: its own name, host and homepage. No alternate title: 'Bulletin KNOB' was never
            # a name of this portal, so keeping it would keep the wrong name searchable.
            old = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": UNINDRA}).scalar()
            upd(conn, UNINDRA, display_name=UNINDRA_NAME, display_name_before_override=old,
                override_timestamp=datetime.now(timezone.utc), institution_id=I_UNINDRA, homepage_url=UNINDRA_HOMEPAGE)

            for r in conn.execute(text("SELECT id, display_name, type, issn_l, issns, publisher_id, institution_id, "
                                       "homepage_url, alternate_titles FROM sources WHERE id = ANY(:i) ORDER BY id"),
                                  {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} type={r['type']} issns={r['issns']} "
                      f"inst={r['institution_id']} hp={r['homepage_url']} alts={r['alternate_titles']}")
            receipt["after"]["_endpoint_" + KNOB_ENDPOINT] = conn.execute(
                text("SELECT source_id FROM oai_pmh_endpoint WHERE id = :e"), {"e": KNOB_ENDPOINT}).scalar()
            gone = ids - {int(k) for k in receipt["after"] if not k.startswith("_")}
            log(f"merged-away sources gone: {sorted(gone)}")
            assert gone == {m[0] for m in MERGES}, gone
            assert receipt["after"]["_endpoint_" + KNOB_ENDPOINT] == WINNER
            left = conn.execute(text("SELECT count(*) FROM sources WHERE lower(display_name) = 'bulletin knob'")).scalar()
            log(f"sources named 'Bulletin KNOB' after: {left}")
            assert left == 1, left
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
