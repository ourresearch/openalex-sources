"""Registry fix for ZD 7176 (Respublica Literaria: one journal, two source records), oxjob #629, support factory #2254,
2026-11-23. Checked live and in the registry's warehouse mirror on 2026-11-23 (receipts in the #629 job:
work/zd7176_live_before_2026-11-23.jsonl, work/zd7176_registry_mirror_2026-11-23.jsonl):

  S4387284834 'Respublica Literaria'  journal, ISSN 2713-3125 (issn_l), 492 primary works 2020-2026, Crossref DOIs
              10.47850/rl... deposited WITH the ISSN, publisher 'Institute of Philosophy and Law of the Siberian Branch
              of the Russian Academy of Sciences', no endpoint, not overridden                         = WINNER
  S4306525920 'Respublica literaria'  journal, no ISSN, 167 primary works 2020-2022, all Crossref DOIs 10.47850/rl.
              2020-2022... deposited WITHOUT an ISSN (container-title 'Respublica literaria', same Crossref member
              27387 and publisher), no publisher, no endpoint, not overridden                         -> merged into the winner

The editorial office (ZD 7176) and the journal's own site (https://respublicaliteraria.ru/index.php/rl/about: 'Respublica
Literaria', founded 2020 by the Institute of Philosophy and Law SB RAS, e-ISSN 2713-3125) say it is one journal. The
two names differ only in the case of one letter. Rule: D5 2026-07-17 / #1979 R3 (one journal = one record; the
record holding the journal's ISSN wins). The winner also gets the journal's homepage (empty today; the site root
redirects there). No host institution: OpenAlex has no institution record for the publisher.

Works side (dry run on openalex.works.locations_w_sources, 23 Nov): the loser's 169 Crossref locations (raw name
'Respublica literaria', journal-article/issue/volume) bind today by walden's exact display-name tier
(CreateLocationsWithSources cell 1). With the loser gone that exact name matches nothing, and the normalized-name
fallback (cell 15) binds them to the winner, the only live source whose normalized name is 'respublicaliteraria'.
So no merged_source_names row is needed (PLAN.md gotcha: a loser name that normalizes to the winner's own name needs
none).

Merge uses the post-D1 pattern of handoff/oxjob629-gaurj-2026-11-20.py (loser deleted, ledger row in source_merge,
which also serves the merged-ID redirect, #1124; refresh_datacite_column when DataCite rows move, #1496).

Run on desk (dry run by default; everything in ONE transaction), with this file at
handoff/oxjob629-respublica-2026-11-23.py:
    cd ~/ox/openalex-sources && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob629-respublica-2026-11-23.py [--execute]
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker.
Revert: the receipt's `before` holds both rows. Restore the loser by re-inserting its `before` row and deleting its
source_merge row (rule zendesk_629_ticket_merge, detail.tickets [7176]); on the winner restore homepage_url from `before`
(empty).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name, refresh_datacite_column, refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
JOB = 629
APPROVED = "support factory #2254 (charter rules 2-3; D5 2026-07-17; #1979 R3: one journal, the ISSN record wins)"

WINNER = 4387284834
LOSER = 4306525920
MERGES = [
    (LOSER, WINNER, [7176], "same journal: Crossref deposits of 2020-2022 (10.47850/rl.2020-2022...) carry no ISSN, so "
                            "they minted a second record; the journal's site and the editors (ZD 7176) name one journal, "
                            "e-ISSN 2713-3125, founded 2020"),
]
EXPECT = {WINNER: "Respublica Literaria",
          LOSER: "Respublica literaria"}
WINNER_ISSN = "2713-3125"
HOMEPAGE = "https://respublicaliteraria.ru/index.php/rl"

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
        row["_datacite"] = [x[0] for x in conn.execute(text("SELECT datacite_id FROM source_datacite_id WHERE source_id = :i"),
                                                       {"i": r["id"]})]
        receipt["before"][str(r["id"])] = row


def expect_name(conn, sid, name):
    row = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    assert row is not None, f"source {sid} missing"
    assert row.display_name == name, (sid, row.display_name, name)


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
    """Same steps as merge() in handoff/oxjob629-gaurj-2026-11-20.py (and oxjob629-ticket-merges-2026-11-15.py)."""
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
    if dc:  # #1496 template fix (2026-11-20): keep the derived sources.datacite_ids column in step
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
            # guards: the winner holds the journal's ISSN, the loser holds none; both are journals; the two names are
            # the same name; the winner's homepage is still empty (so we only fill, never overwrite)
            assert normalize_name(EXPECT[WINNER]) == normalize_name(EXPECT[LOSER])
            win_issns = {r[0] for r in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :s"), {"s": WINNER})}
            assert WINNER_ISSN in win_issns, win_issns
            assert conn.execute(text("SELECT count(*) FROM source_issn WHERE source_id = :s"), {"s": LOSER}).scalar() == 0
            assert conn.execute(text("SELECT count(*) FROM sources WHERE id = ANY(:i) AND type = 'journal'"),
                                {"i": list(ids)}).scalar() == 2
            w = conn.execute(text("SELECT homepage_url, override_timestamp FROM sources WHERE id = :s"),
                             {"s": WINNER}).fetchone()
            assert w.homepage_url is None, w

            for loser, winner, tickets, note in MERGES:
                merge(conn, loser, winner, "zendesk_629_ticket_merge",
                      {"tickets": tickets, "oxjob": JOB, "approved_by": APPROVED, "note": note})

            upd(conn, WINNER, homepage_url=HOMEPAGE)

            for r in conn.execute(text("SELECT id, display_name, type, issn_l, issns, publisher, publisher_id, "
                                       "institution_id, homepage_url, alternate_titles FROM sources "
                                       "WHERE id = ANY(:i) ORDER BY id"), {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} type={r['type']} issns={r['issns']} "
                      f"inst={r['institution_id']} hp={r['homepage_url']} alts={r['alternate_titles']}")
            gone = ids - {int(k) for k in receipt["after"]}
            log(f"merged-away sources gone: {sorted(gone)}")
            assert gone == {LOSER}, gone
            after = receipt["after"][str(WINNER)]
            assert after["issn_l"] == WINNER_ISSN, after
            assert after["display_name"] == EXPECT[WINNER], after
            left = conn.execute(text("SELECT count(*) FROM sources WHERE lower(display_name) = 'respublica literaria'")).scalar()
            log(f"sources named 'Respublica Literaria' (any case) after: {left}")
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
