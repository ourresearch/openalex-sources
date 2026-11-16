"""Renamed journals: one record per journal, current title wins (oxjob #1979, support factory #2254, 2026-11-15).
Policy choice: follows #629 D5 / RULES-DRAFT R3 + R10 (Jason + Casey, 2026-07-17): "Current name wins ... OpenAlex is a
graph of the scholarly literature, not the history of it. Whatever the venue is called today is the name; old ISSNs
ride along in the winner's array"; merging across ISSNs is allowed when the world treats them as one journal.
Every record below was re-checked on the live API on 2026-11-15 23:30-23:50 CT (receipts: #1979 work/live_2026-11-15_*.txt).

  ZD 26815  S4210226532 'Tavukçuluk Araştırma Dergisi' (1302-3209, 2147-9003; 201 works, all issues to 2026-09):
            renamed in place 'Poultry Studies'; + ISSNs 2791-7258 (print), 2822-4043 (online), on no source today
            (both 404); homepage -> https://www.poultrystudies.org/ ; old titles kept in alternate_titles.
            Evidence: poultrystudies.org prints both new ISSNs and '"Poultry Studies" is the successor to the "Journal of
            Poultry Research"'; DergiPark's jpr journal is now titled Poultry Studies (/en/pub/jpr -> /en/pub/post) and
            its archive is the same volume run (vol 16-23). Crossref still deposits under 1302-3209, so no work moves.
  ZD 4050   merge S2764786680 'Papeles del CEIC' (1695-6494; works 2017 -> 2024-03) into S4404676734 'Papeles de
            identidad' (3045-5650; works 2024-10 -> 2026). Crossref lists both ISSNs under one title 'Papeles de
            Identidad Contar la investigación de frontera' (EHU Press); same DOI prefix 10.1387/pceic.
  ZD 7509   merge S4387289554 'Revista da Faculdade Paulo Picanço' (2764-6289; vol 1 -> vol 6 n1 2026) into
            S7407059090 'Journal of Health Education and Practice (JHEP)' (3086-4585; vol 6 n2 2026 ->). The journal's
            own page (journalhep.com.br/index.php/rfpp) says 'Título anterior: Revista da Faculdade Paulo Picanço
            (ISSN 2764-6289)'; same OJS path, same DOI prefix 10.59483, same publisher.
  ZD 23883  S7407061505 'The Bulletin of Linguistic and Intercultural Studies' (2821-496X, 2971-9615): + ISSNs
            3153-2578 (print), 3100-8291 (online), on no source today (both 404); + alternate title
            'Buletinul de studii lingvistice şi interculturale'. Evidence: DOAJ record e0ed6669b26c49e6bb2e97a52b76b6be
            (pissn 3153-2578, eissn 3100-8291, replaces 2971-9615); animv.ro/en/en_bsli/ prints all four ISSNs.
            Desk dry run 2026-11-16 10:25 CT: source_issn ALREADY holds 3153-2578 and 3100-8291 on 7407061505 (an
            earlier write that skipped refresh_issns_column; the only source in the registry whose sources.issns
            array differs from source_issn), but sources.issns (what walden and the API read) still lists only the
            old two. So for BSLI this re-derives the array from source_issn (sync_issn_l_flags) instead of inserting.

issn_l is left as stored everywhere (walden matches works by the whole ISSN list, never by issn_l; jobs/reconcile_issn_l
leaves two-family sources alone). No OA flag, host or publisher is touched.

Run on desk from the openalex-sources checkout, OUTSIDE the End 2 End window (05:00 UTC until the night publishes);
dry run by default, everything in ONE transaction:
    cp <this file> handoff/oxjob1979-renamed-journals-2026-11-15.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1979-renamed-journals-2026-11-15.py            # dry run
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1979-renamed-journals-2026-11-15.py --execute
The receipt (before-state + actions + after) is printed as JSON after the RECEIPT marker: save it as
handoff/oxjob1979-renamed-journals-2026-11-15-receipt.json and in #1979 work/.
Revert: the receipt's `before` holds every touched row. Rename: display_name + alternate_titles + homepage_url back,
override_timestamp NULL, DELETE FROM source_issn WHERE issn IN the added ISSNs (Poultry Studies only; BSLI's two
source_issn rows predate this run: revert only its sources.issns array to the `before` value). Merge: re-insert the loser's `before` row,
move back its ISSNs / endpoints / DataCite ids / OA override / beacon rows (all in `before`), delete its source_merge row (rule zendesk_1979_rename_merge).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import insert_issns, normalize_name, refresh_issns_column, resolve_issn_l, sync_issn_l_flags

EXECUTE = "--execute" in sys.argv
JOB = 1979
APPROVED = "support factory #2254 (Jason 2026-11-15 yes; #629 D5 2026-07-17 current name wins, R3/R10)"
RULE = "zendesk_1979_rename_merge"

# (loser = former title, winner = current title, tickets, note)
MERGES = [
    (2764786680, 4404676734, [4050], "renamed Sept 2024: Papeles del CEIC 1695-6494 -> Papeles de identidad 3045-5650 (EHU Press); D5/R3"),
    (4387289554, 7407059090, [7509], "renamed 2026: Revista da Faculdade Paulo Picanço 2764-6289 -> Journal of Health, Education and Practice 3086-4585; D5/R3"),
]

EXPECT = {
    4210226532: "Tavukçuluk Araştırma Dergisi",
    2764786680: "Papeles del CEIC",
    4404676734: "Papeles de identidad",
    4387289554: "Revista da Faculdade Paulo Picanço",
    7407059090: "Journal of Health Education and Practice (JHEP)",
    7407061505: "The Bulletin of Linguistic and Intercultural Studies",
}
EXPECT_ISSNS = {  # every ISSN each record holds today (live API 2026-11-15)
    4210226532: {"1302-3209", "2147-9003"},
    2764786680: {"1695-6494"},
    4404676734: {"3045-5650"},
    4387289554: {"2764-6289"},
    7407059090: {"3086-4585"},
    7407061505: {"2821-496X", "2971-9615", "3100-8291", "3153-2578"},  # desk 2026-11-16: rows already there
}
# sources.issns arrays that lag source_issn today; re-derived from source_issn (asserted first)
STALE_ISSNS_ARRAY = {7407061505: ["2821-496X", "2971-9615"]}

RENAMES = {4210226532: "Poultry Studies"}
HOMEPAGES = {4210226532: "https://www.poultrystudies.org/"}
NEW_ISSNS = {
    4210226532: ["2791-7258", "2822-4043"],
}
ALT_TITLES = {
    4210226532: ["Tavukçuluk Araştırma Dergisi", "Journal of Poultry Research"],
    4404676734: ["Papeles de identidad. Contar la investigación de frontera",
                 "Papeles del CEIC. International Journal on Collective Identity Research"],
    7407059090: ["Journal of Health, Education and Practice"],
    7407061505: ["Buletinul de studii lingvistice şi interculturale"],
}

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
        # desk review 2026-11-16 (#1979): merge() also moves these three; record them so a revert can move them back
        for t in ("source_datacite_id", "source_oa_override", "ojs_beacon_mint"):
            row["_" + t] = [{k: (None if v is None else str(v)) for k, v in x.items()} for x in
                            conn.execute(text(f"SELECT * FROM {t} WHERE source_id = :i"), {"i": r["id"]}).mappings()]
        receipt["before"][str(r["id"])] = row


def expect_name(conn, sid, name):
    row = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    assert row is not None, f"source {sid} missing"
    assert normalize_name(row.display_name) == normalize_name(name), (sid, row.display_name, name)


def expect_issns(conn, sid, issns):
    have = {x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": sid})}
    assert have == issns, (sid, sorted(have), sorted(issns))


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


def rename(conn, sid, new_name):
    old = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).scalar()
    if old == new_name:
        return
    # override_timestamp: feed refreshes (enrich_journal, clean_source_names) never overwrite a curated name, so
    # Crossref's stale title for 1302-3209 can't put the old name back
    upd(conn, sid, display_name=new_name, display_name_before_override=old,
        override_timestamp=datetime.now(timezone.utc))
    add_alt_titles(conn, sid, [old])


def add_issns(conn, sid, issns):
    for issn in issns:
        taken = conn.execute(text("SELECT source_id FROM source_issn WHERE issn = :i"), {"i": issn}).scalar()
        assert taken is None, f"ISSN {issn} already on source {taken}: a merge question, stop and look"
        m = conn.execute(text("SELECT issn_l FROM issn_to_issnl WHERE issn = :i"), {"i": issn}).fetchone()
        log(f"ISSN {issn}: ISSN IC map -> {m[0] if m else 'not in the map yet (DOAJ / journal page are the evidence)'}")
    insert_issns(conn, sid, issns, None)  # never touches a stored issn_l
    sync_issn_l_flags(conn, sid)
    log(f"ISSNs {sid} += {issns}")


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in handoff/oxjob629-ticket-merges-2026-11-15.py (post-D1: loser deleted, ledger row in
    source_merge, which also serves the merged-ID redirect, #1124)."""
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
            ids = set(EXPECT)
            snapshot(conn, ids)
            for sid, name in EXPECT.items():
                expect_name(conn, sid, name)
            for sid, issns in EXPECT_ISSNS.items():
                expect_issns(conn, sid, issns)

            for loser, winner, tickets, note in MERGES:
                merge(conn, loser, winner, RULE,
                      {"tickets": tickets, "oxjob": JOB, "approved_by": APPROVED, "note": note})
            for sid, name in RENAMES.items():
                rename(conn, sid, name)
            for sid, url in HOMEPAGES.items():
                upd(conn, sid, homepage_url=url)
            for sid, issns in NEW_ISSNS.items():
                add_issns(conn, sid, issns)
            for sid, stale in STALE_ISSNS_ARRAY.items():
                have = conn.execute(text("SELECT issns FROM sources WHERE id = :id"), {"id": sid}).scalar()
                assert list(have or []) == stale, (sid, have, stale)
                sync_issn_l_flags(conn, sid)  # flags + re-derives sources.issns from source_issn
                new = conn.execute(text("SELECT issns FROM sources WHERE id = :id"), {"id": sid}).scalar()
                log(f"issns array {sid}: {stale} -> {list(new)} (re-derived from source_issn)")
            for sid, titles in ALT_TITLES.items():
                add_alt_titles(conn, sid, titles)

            for r in conn.execute(text("SELECT id, display_name, type, issn_l, issns, publisher_id, institution_id, "
                                       "homepage_url, alternate_titles, override_timestamp FROM sources "
                                       "WHERE id = ANY(:i) ORDER BY id"),
                                  {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} issn_l={r['issn_l']} issns={r['issns']} "
                      f"pub={r['publisher_id']} hp={r['homepage_url']} alt={r['alternate_titles']}")
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
