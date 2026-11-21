"""Renamed journals become one record under their current title (oxjob #1225, ZD 7482 and ZD 22707 category 2;
support factory #2254, written off desk 2026-11-21 by lane #2263; desk runs it). Every value below was re-checked live
on 2026-11-21 14:30-14:50 CT (API + the registry's warehouse mirror openalex.sources.sources; receipts in the #1225 job:
work/before_night_2026-11-22.json, work/renamed_merges_check_2026-11-21.json).

Rule: sources charter decision 2026-07-17 (Jason/Casey) D5 "current name wins" = source-duplicate-merges RULES-DRAFT R3
(+ R10): a renamed journal is ONE record, carrying its current title; the old titles ride along as alternate titles
and the old ISSNs in its ISSN list. Applied by #1979 (17 Nov) and #629 GAURJ (20 Nov). Same post-D1 merge() as
handoff/oxjob629-gaurj-2026-11-20.py (executed 20 Nov): loser deleted, ledger row in source_merge (which serves the
merged-ID redirect, #1124).

  1. S4210181446 'International Journal of Geographical Information Systems' (T&F, 1987-1996; ISSNs 0269-3798 +
     1362-3087)  ->  S111872859 'International Journal of Geographical Information Science' (its title from 1997;
     ISSNs 1365-8816 + 1365-8824).  ZD 7482.
     Why it matters: 1362-3087 is IJGIS's electronic ISSN but sits on the old-title record, and walden's ISSN tier
     tries the electronic ISSN first, so 1,951 of IJGIS's 1,953 Taylor & Francis DOIs (10.1080/13658816.*, 1997-2026)
     are filed under the 1987-1996 title (live 21 Nov: doi_starts_with:10.1080/13658816 -> S4210181446 1,951,
     S111872859 2). S111872859 itself holds 522 works, mostly the Persian GIS journal records 7482 reported (moved off by
     this job's curation batches 3-4, night 11-22). The loser has more works than the winner: R3 still names the current
     title the winner (merge_duplicates' winner_stale guard is for exact-name duplicates, not renames).
  2. S4210203371 'Klinische Wochenschrift' (Springer, 1922-1991; ISSN 0023-2173; 8 works)  ->  S114689480 'Journal of
     Molecular Medicine' (0946-2716 + 1432-1440).  ZD 22707 category 2.
  3. S4210201922 'The Clinical Investigator' (Springer, 1992-1994, the title between the two; ISSN 0941-0198; 0 works)
     ->  S114689480.  Same lineage (Springer deposits all three titles' back issues with J Mol Med's e-ISSN 1432-1440).
  4. S4210190852 'The New-England Medical Review and Journal' (1827; ISSNs 2163-2952 + 2163-2928; 2 works)  ->
     S62468778 'New England Journal of Medicine'. NEJM already carries its other former title, the Boston Medical and
     Surgical Journal (ISSN 0096-6762, alternate title), as R3 wants: nothing else to do there.
  then recompute_listed_in() (lists follow the ISSNs).

Effect after the next sources build + End 2 End: ~3,230 works move from S4210181446 to S111872859 (its 3,225 primary
works + copies), so IJGIS's own articles are on IJGIS; 11 works from S4210203371 and 2 from S4210190852 move to their
journal's current record. ~3,250 works re-stamped: no pre-clear needed. No OA change: none of the six records has an
OA flag (is_oa, DOAJ, high-OA-rate, J-STAGE all false in the mirror, 21 Nov).

Run on desk (dry run by default; ONE transaction), with this file at handoff/oxjob1225-renamed-journal-merges-2026-11-21.py:
    cp <this file> ~/ox/openalex-sources/handoff/oxjob1225-renamed-journal-merges-2026-11-21.py && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-renamed-journal-merges-2026-11-21.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-renamed-journal-merges-2026-11-21.py --execute
Do not run while an End 2 End is between its 05:00 UTC start and publish. The receipt (before-state + actions) is
printed as JSON after the RECEIPT marker: save it in the #1225 job's work/.
Revert (from the receipt's `before`): per loser, re-insert its `before` row into sources (INSERT ... OVERRIDING SYSTEM
VALUE; sources.id is an identity column), move its `_issns` back (UPDATE source_issn SET source_id = <loser> WHERE issn
IN (...) AND source_id = <winner>), set its issn_l back, DELETE FROM source_merge WHERE loser_id = <loser> AND rule =
'oxjob1225_renamed_journal'; on each winner restore alternate_titles from `before` and re-sync its is_issn_l flags;
recompute_listed_in.
Checks after the night: https://api.openalex.org/works?filter=doi_starts_with:10.1080/13658816,primary_location.source.id:S111872859
(expect ~1,950, was 2); https://api.openalex.org/sources/S4210181446 (redirects to S111872859);
https://api.openalex.org/works?filter=primary_location.source.id:S111872859,publication_year:2016-2021 (expect ~700 T&F
articles and 0 Persian, was 133 with 118 Persian).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name, recompute_listed_in, refresh_datacite_column, refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
JOB = 1225
RULE = "oxjob1225_renamed_journal"
APPROVED = ("support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit); sources charter "
            "D5 2026-07-17 current name wins = RULES-DRAFT R3 + R10, as #1979 and #629 GAURJ")

IJGIS, IJGSYS = 111872859, 4210181446
JMM, KW, CLIN_INV = 114689480, 4210203371, 4210201922
NEJM, NEMRJ = 62468778, 4210190852

# (loser, winner, tickets, note)
MERGES = [
    (IJGSYS, IJGIS, [7482], "former title 1987-1996 (renamed 1997); its record holds IJGIS's e-ISSN 1362-3087, so 1,951 of "
                            "IJGIS's 1,953 T&F DOIs sat on the old title"),
    (KW, JMM, [22707], "former title 1922-1991 (then The Clinical Investigator, then J Mol Med); Springer deposits its back "
                       "issues with J Mol Med's e-ISSN 1432-1440"),
    (CLIN_INV, JMM, [22707], "former title 1992-1994, between Klinische Wochenschrift and J Mol Med"),
    (NEMRJ, NEJM, [22707], "former title 1827 (then Boston Medical and Surgical Journal, then NEJM)"),
]
EXPECT_NAME = {
    IJGIS: "International Journal of Geographical Information Science",
    IJGSYS: "International Journal of Geographical Information Systems",
    JMM: "Journal of Molecular Medicine",
    KW: "Klinische Wochenschrift",
    CLIN_INV: "The Clinical Investigator",
    NEJM: "New England Journal of Medicine",
    NEMRJ: "The New-England Medical Review and Journal",
}
EXPECT_ISSNS = {
    IJGIS: {"1365-8816", "1365-8824"},
    IJGSYS: {"0269-3798", "1362-3087"},
    JMM: {"0946-2716", "1432-1440"},
    KW: {"0023-2173"},
    CLIN_INV: {"0941-0198"},
    NEJM: {"0028-4793", "0096-6762", "1533-4406", "2331-4710"},
    NEMRJ: {"2163-2952", "2163-2928"},
}
WINNER_ISSN_L = {IJGIS: "1365-8816", JMM: "0946-2716", NEJM: "0028-4793"}

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved_by": APPROVED,
           "before": {}, "actions": [], "after": {}}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        row = {k: (None if v is None else v if isinstance(v, (list, dict)) else str(v)) for k, v in r.items()}
        row["_issns"] = sorted(x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"),
                                                          {"i": r["id"]}))
        row["_endpoints"] = [x[0] for x in conn.execute(text("SELECT id FROM oai_pmh_endpoint WHERE source_id = :i"),
                                                        {"i": r["id"]})]
        receipt["before"][str(r["id"])] = row


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
    """Same steps as merge() in handoff/oxjob629-gaurj-2026-11-20.py (executed 20 Nov)."""
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
    ids = set(EXPECT_NAME)
    try:
        with engine.begin() as conn:
            snapshot(conn, ids)
            # guards: every record is still the journal we checked, with exactly the ISSNs we saw, not overridden (losers)
            for sid in ids:
                b = receipt["before"].get(str(sid))
                assert b is not None, f"S{sid} missing (merged earlier? see source_merge)"
                assert normalize_name(b["display_name"]) == normalize_name(EXPECT_NAME[sid]), (sid, b["display_name"])
                assert b["type"] == "journal", (sid, b["type"])
                assert set(b["_issns"]) == EXPECT_ISSNS[sid], (sid, b["_issns"])
            for w, l_ in WINNER_ISSN_L.items():
                assert receipt["before"][str(w)]["issn_l"] == l_, (w, receipt["before"][str(w)]["issn_l"])
            for loser, winner, tickets, note in MERGES:
                assert receipt["before"][str(loser)]["override_timestamp"] is None, f"S{loser} is curator-overridden: look"
                eps = receipt["before"][str(loser)]["_endpoints"]
                if eps:
                    log(f"note: S{loser} carries endpoint(s) {eps}; they move to S{winner}")

            for loser, winner, tickets, note in MERGES:
                merge(conn, loser, winner, RULE, {"tickets": tickets, "oxjob": JOB, "approved_by": APPROVED, "note": note})

            log(f"listed_in rows changed {recompute_listed_in(conn)}")

            for r in conn.execute(text("SELECT id, display_name, type, issn_l, issns, alternate_titles, listed_in "
                                       "FROM sources WHERE id = ANY(:i) ORDER BY id"), {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else v if isinstance(v, (list, dict)) else str(v))
                                                  for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} issn_l={r['issn_l']} issns={r['issns']} "
                      f"alts={r['alternate_titles']} listed_in={r['listed_in']}", flush=True)
            gone = ids - {int(k) for k in receipt["after"]}
            log(f"merged-away sources gone: {sorted(gone)}")
            assert gone == {IJGSYS, KW, CLIN_INV, NEMRJ}, gone
            want = {IJGIS: EXPECT_ISSNS[IJGIS] | EXPECT_ISSNS[IJGSYS],
                    JMM: EXPECT_ISSNS[JMM] | EXPECT_ISSNS[KW] | EXPECT_ISSNS[CLIN_INV],
                    NEJM: EXPECT_ISSNS[NEJM] | EXPECT_ISSNS[NEMRJ]}
            for w, issns in want.items():
                got = {x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :s"), {"s": w})}
                assert got == issns, (w, got, issns)
                assert receipt["after"][str(w)]["issn_l"] == WINNER_ISSN_L[w], (w, receipt["after"][str(w)]["issn_l"])
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
