"""Two publisher / organization names typed as journal sources: oxjob #654 (support factory #2254), 2026-11-16.
Written by an off-desk lane; every value re-checked on the live API on 2026-11-16 16:20 CT (#654 work/
tickets_recheck_2026-11-16.md).

  ZD 2952  S2764603233 'American Institutes for Research' (type journal, no ISSN, no host, 140 works: ERIC report
           records, 126 typed report). AIR is a research nonprofit (ROR 00490n048 = I1293631320), not a journal.
           -> type 'other', host = institution I1293631320 (publisher_id stays NULL; there is no AIR publisher entity).
           Same recipe as Cairn.info / OpenEdition in handoff/26471-sources-2026-10-24.py (Kyle, 2026-10-24).
  ZD 5765  S4306528834 'Routledge, Taylor & Francis Group' (type journal, no ISSN, no host, 176 works: 166 books,
           ERIC landing pages of Routledge books; 46 of them already also sit on Routledge eBooks)
           -> merged into S4306463855 'Routledge eBooks' (ebook platform, host P4310319847 Routledge, 1.9M works).
           Same steps as merge() in handoff/oxjob629-ticket-merges-2026-11-15.py (loser deleted, source_merge row,
           which serves the 301; the loser's name goes into the winner's alternate_titles so the name-matcher keeps
           sending those ERIC records to Routledge eBooks). #1899's triage proposed exactly this merge (review_edits.csv).

Guards (any failure rolls the whole run back; nothing is half-applied):
  - identity: registry names must match the API names above under sources_lib.normalize_name;
  - AIR: type is still 'journal', publisher_id and institution_id still NULL, issn_l NULL;
  - Routledge loser: type 'journal', issn_l NULL, not curator-overridden; winner: type 'ebook platform',
    publisher_id 4310319847;
  - every registry table with a source_id column (information_schema scan) must hold no row for the loser, except
    the two stats tables, which are deleted with the record (refresh_source_stats rebuilds them). source_merge rows
    whose winner is the loser are re-pointed to the new winner (chain collapse) and listed in the receipt.

WORKS: ~316 works re-stamp on the next End 2 End (140 AIR: source type + host; 176 Routledge: source id).
Run after night N publishes and before 22:30 CT, so it rides night N+1; never between 05:00 UTC and the publish.

Run from the openalex-sources checkout (copy this file into handoff/ first); dry run by default, ONE transaction:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob654-publisher-pseudo-journals-2026-11-16.py [--execute]
    python3 <same runner> handoff/oxjob654-publisher-pseudo-journals-2026-11-16.py --revert <receipt.json> [--execute]
The receipt (full before rows as JSONB, actions, after rows) is printed as JSON after the RECEIPT marker: save it as
handoff/oxjob654-publisher-pseudo-journals-2026-11-16-receipt.json and commit it with this script.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name

ARGS = sys.argv[1:]
EXECUTE = "--execute" in ARGS
REVERT = ARGS[ARGS.index("--revert") + 1] if "--revert" in ARGS else None
JOB = 654
APPROVED = ("support factory #2254 (Jason 2026-11-15 yes; standing authority #1778: reversible from the receipt); "
            "recipes: 26471 retype (Kyle 2026-10-24), #629 merge")
RULE = "zendesk_654_publisher_pseudo_journal"

AIR, I_AIR = 2764603233, 1293631320
RT_LOSER, RT_WINNER, P_ROUTLEDGE = 4306528834, 4306463855, 4310319847
EXPECT = {
    AIR: "American Institutes for Research",
    RT_LOSER: "Routledge, Taylor & Francis Group",
    RT_WINNER: "Routledge eBooks",
}
STATS = ("source_works_count", "source_publication_years")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "tickets": [2952, 5765],
           "approved": APPROVED, "mode": "revert" if REVERT else "apply", "before": {}, "stats_before": {},
           "merge_chain_repointed": [], "actions": [], "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row_json(conn, sid):
    return conn.execute(text("SELECT to_jsonb(s) FROM sources s WHERE id = :id"), {"id": sid}).scalar()


def source_id_tables(conn):
    return [r[0] for r in conn.execute(text(
        "SELECT DISTINCT c.table_name FROM information_schema.columns c "
        "JOIN information_schema.tables t ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
        "WHERE c.table_schema = 'public' AND c.column_name = 'source_id' AND t.table_type = 'BASE TABLE' "
        "AND c.table_name <> 'sources' ORDER BY 1"))]


def apply(conn):
    for sid in EXPECT:
        receipt["before"][str(sid)] = row_json(conn, sid)
    for sid, name in EXPECT.items():
        b = receipt["before"][str(sid)]
        assert b is not None, f"source {sid} missing"
        assert normalize_name(b["display_name"]) == normalize_name(name), (sid, b["display_name"], name)

    # ZD 2952: AIR is an organization, not a journal
    a = receipt["before"][str(AIR)]
    assert a["type"] == "journal", ("AIR type drifted", a["type"])
    assert a["publisher_id"] is None and a["institution_id"] is None, ("AIR host drifted", a["publisher_id"], a["institution_id"])
    assert a["issn_l"] is None, ("AIR has an ISSN-L now", a["issn_l"])
    n = conn.execute(text("UPDATE sources SET type = 'other', institution_id = :i, publisher_id = NULL, "
                          "updated_date = now() WHERE id = :id AND type = 'journal'"), {"i": I_AIR, "id": AIR}).rowcount
    assert n == 1
    log(f"UPDATE sources {AIR}: type journal -> other; institution_id NULL -> {I_AIR} (host I{I_AIR}, ROR 00490n048)")

    # ZD 5765: Routledge publisher name as a journal -> merged into Routledge eBooks
    lo, wi = receipt["before"][str(RT_LOSER)], receipt["before"][str(RT_WINNER)]
    assert lo["type"] == "journal" and lo["issn_l"] is None, ("loser drifted", lo["type"], lo["issn_l"])
    assert lo.get("override_timestamp") is None, "loser is curator-overridden"
    assert wi["type"] == "ebook platform" and wi["publisher_id"] == P_ROUTLEDGE, ("winner drifted", wi["type"], wi["publisher_id"])
    for t in source_id_tables(conn):
        rows = conn.execute(text(f"SELECT to_jsonb(x) FROM {t} x WHERE source_id = :l"), {"l": RT_LOSER}).scalars().all()
        if t in STATS:
            receipt["stats_before"][t] = rows
            continue
        assert not rows, f"loser {RT_LOSER} is bound in {t} ({len(rows)} rows): leave it out and say so in #654's Log"
    lm = conn.execute(text("SELECT count(*) FROM source_merge WHERE loser_id = :l"), {"l": RT_LOSER}).scalar()
    assert lm == 0, "loser already has a source_merge row"
    chain = conn.execute(text("SELECT id, loser_id FROM source_merge WHERE winner_id = :l"), {"l": RT_LOSER}).fetchall()
    for mid, old_loser in chain:
        conn.execute(text("UPDATE source_merge SET winner_id = :w WHERE id = :m"), {"w": RT_WINNER, "m": mid})
        receipt["merge_chain_repointed"].append({"source_merge_id": mid, "loser_id": old_loser})
        log(f"source_merge {mid}: winner {RT_LOSER} -> {RT_WINNER} (earlier merge of {old_loser})")
    alts = list(wi.get("alternate_titles") or [])
    have = {t.strip().lower() for t in alts + [wi["display_name"] or ""]}
    gained = [t for t in [lo["display_name"]] + list(lo.get("alternate_titles") or []) if t and t.strip().lower() not in have]
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), updated_date = now() WHERE id = :w"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "w": RT_WINNER})
        log(f"alternate_titles {RT_WINNER} += {gained}")
    conn.execute(text("INSERT INTO source_merge (loser_id, winner_id, rule, source_feed, detail) "
                      "VALUES (:l, :w, :r, 'zendesk', CAST(:d AS JSONB))"),
                 {"l": RT_LOSER, "w": RT_WINNER, "r": RULE,
                  "d": json.dumps({"tickets": [5765], "oxjob": JOB, "approved_by": APPROVED, "loser_name": lo["display_name"],
                                   "note": "publisher name typed as a journal; ERIC records of Routledge books"},
                                  ensure_ascii=False)})
    for t in STATS:
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": RT_LOSER})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": RT_LOSER}).rowcount
    assert n == 1
    log(f"MERGED {RT_LOSER} -> {RT_WINNER} ({RULE})")

    # after-state
    for sid in EXPECT:
        receipt["after"][str(sid)] = row_json(conn, sid)
    aa = receipt["after"][str(AIR)]
    assert aa["type"] == "other" and aa["institution_id"] == I_AIR and aa["publisher_id"] is None
    assert receipt["after"][str(RT_LOSER)] is None
    print(f"  AIR {AIR}: type={aa['type']} institution_id={aa['institution_id']} publisher_id={aa['publisher_id']}")
    print(f"  Routledge eBooks {RT_WINNER}: alternate_titles={receipt['after'][str(RT_WINNER)].get('alternate_titles')}")
    log(f"done: 1 retyped, 1 merged; merge chain re-pointed {len(receipt['merge_chain_repointed'])}")


def revert(conn, path):
    rc = json.load(open(path))
    assert rc.get("job") == JOB and rc.get("mode") == "apply" and rc.get("execute"), "not an executed #654 apply receipt"
    b = rc["before"]
    for sid in EXPECT:
        receipt["before"][str(sid)] = row_json(conn, sid)
    # AIR back to its before type and host
    a = b[str(AIR)]
    n = conn.execute(text("UPDATE sources SET type = :t, institution_id = :i, publisher_id = :p, updated_date = now() "
                          "WHERE id = :id"), {"t": a["type"], "i": a["institution_id"], "p": a["publisher_id"], "id": AIR}).rowcount
    assert n == 1
    log(f"REVERT sources {AIR}: type -> {a['type']}, institution_id -> {a['institution_id']}, publisher_id -> {a['publisher_id']}")
    # Routledge loser back
    assert row_json(conn, RT_LOSER) is None, "loser exists already"
    conn.execute(text("INSERT INTO sources OVERRIDING SYSTEM VALUE "
                      "SELECT * FROM jsonb_populate_record(NULL::sources, CAST(:r AS JSONB))"),
                 {"r": json.dumps(b[str(RT_LOSER)], ensure_ascii=False)})
    for t, rows in rc.get("stats_before", {}).items():
        assert t in STATS
        for r in rows:
            conn.execute(text(f"INSERT INTO {t} SELECT * FROM jsonb_populate_record(NULL::{t}, CAST(:r AS JSONB)) "
                              "ON CONFLICT DO NOTHING"), {"r": json.dumps(r)})
    conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), updated_date = now() WHERE id = :w"),
                 {"a": json.dumps(b[str(RT_WINNER)].get("alternate_titles"), ensure_ascii=False), "w": RT_WINNER})
    for m in rc.get("merge_chain_repointed", []):
        conn.execute(text("UPDATE source_merge SET winner_id = :l WHERE id = :m"), {"l": RT_LOSER, "m": m["source_merge_id"]})
    n = conn.execute(text("DELETE FROM source_merge WHERE loser_id = :l AND winner_id = :w AND rule = :r"),
                     {"l": RT_LOSER, "w": RT_WINNER, "r": RULE}).rowcount
    log(f"REVERT merge {RT_LOSER} -> {RT_WINNER}: loser re-inserted, winner alternate_titles restored, "
        f"source_merge rows deleted {n}, chain rows restored {len(rc.get('merge_chain_repointed', []))}")
    for sid in EXPECT:
        receipt["after"][str(sid)] = row_json(conn, sid)


def main():
    try:
        with engine.begin() as conn:
            if REVERT:
                revert(conn, REVERT)
            else:
                apply(conn)
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
