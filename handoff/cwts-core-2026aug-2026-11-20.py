"""Load the 2026 edition of CWTS core sources into sources.is_core (the `cwts-core` list).

Why: Nees Jan van Eck (CWTS, Leiden) emailed Kyle on 20 Nov 2026 ("Update core sources and core publications
dataset"): CWTS has a new identification of core sources and core publications in OpenAlex, based on the OpenAlex
snapshot of 31 Aug 2026, https://doi.org/10.5281/zenodo.23890190 (CC0). The registry still carries the 2024 edition
(Zenodo 13879982, Aug 2024 snapshot): live API 20 Nov 2026, sources?filter=is_core:true = 35,422, of which 35,421 are
core in the 2024 file. The 2025 edition (Zenodo 17200868) was never loaded.

Input: data/source_lists/cwts-core-2026-08-31.tsv = the rows of the record's source.tsv with is_cwts_core_source = 1
(40,477 sources, keyed by OpenAlex source id, so no ISSN matching). source.tsv md5 d4fc67fa986158680a70c04cbaea8015.

What it does, in ONE transaction (dry run by default; rolls back unless --execute). Rows only, no DDL:
  1. Target = edition ids that are active registry sources (present, not a source_merge loser). Edition ids that are gone (merged or
     deleted since 31 Aug) are SKIPPED, not carried to their winner: 31 of the 32 are book titles and proceedings
     volumes since merged into ebook platforms (Elsevier eBooks, Springer eBooks...) or conference series; carrying
     them over would flag whole publisher book platforms as core. Listed in the receipt.
  2. is_core = TRUE on target rows not already core; is_core = FALSE on core rows not in the target. Sources minted
     after the snapshot (id > the edition's highest id) keep their value: CWTS could not assess them (today only
     S7407122749 Chemical Engineering Research and Design, split out of core S166351637 on 17 Nov, #1726).
     updated_date = now() on changed rows, as every registry handoff does.
  3. source_list 'cwts-core': url, list_version 2026-08-31, loaded_at, scope text name the 2026 edition.
  4. recompute_listed_in() once (single writer of sources.listed_in; cwts-core in listed_in follows is_core).
Expected (from the live API, 20 Nov): +7,269 core, -2,245 not core (2,086 that CWTS now lists as not core, 159 absent
from the file: 104 have no works), 32 skipped, after = 40,446 active core sources. Works re-stamp: up to ~8.6M
(5.5M in gaining sources, 3.1M in losing ones) because is_core is in the dehydrated source on every location.

Run from the openalex-sources checkout, NOT while the End 2 End is running (05:00 UTC until it publishes; the registry
is read live by federation). Register it as a rider with #1642 first (kind: registry).
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/cwts-core-2026aug-2026-11-20.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/cwts-core-2026aug-2026-11-20.py --execute
Save the JSON after RECEIPT as handoff/cwts-core-2026aug-2026-11-20-receipt.json and commit both files.
Revert: run_registry.py handoff/cwts-core-2026aug-2026-11-20.py --revert handoff/cwts-core-2026aug-2026-11-20-receipt.json [--execute]
(puts every flipped row back, NULLs included, restores the source_list row, recomputes listed_in).
Next edition: copy this file, point EDITION_FILE / VERSION / RECORD_URL at the new record, reset the EXPECT_* numbers.
"""
import csv
import json
import os
import sys
from datetime import date, datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import recompute_listed_in

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
LIST_ID = "cwts-core"
VERSION = date(2026, 8, 31)
RECORD_URL = "https://doi.org/10.5281/zenodo.23890190"
EDITION_FILE = os.path.join(os.path.dirname(HERE), "data", "source_lists", "cwts-core-2026-08-31.tsv")
SCOPE = ("Sources CWTS (Leiden University) identifies as core sources for the Leiden Ranking Open Edition. 2026 "
         "edition, from the OpenAlex snapshot of 31 Aug 2026 (Zenodo 23890190), loaded 20 Nov 2026. Derived from "
         "sources.is_core.")
EXPECT_BEFORE, EXPECT_AFTER, TOL = 35422, 40446, 300
# Active = not a merge loser. Since D1 (migration 021) merged losers are deleted and source_merge is the sole merge
# record (walden's CreateSources anti-joins it), so a loser row that still exists is not served either.
NOT_LOSER = "NOT EXISTS (SELECT 1 FROM source_merge m WHERE m.loser_id = s.id)"

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert": REVERT,
           "list": LIST_ID, "record": RECORD_URL, "list_version": VERSION.isoformat()}


def listed_in_counts(conn):
    rows = conn.execute(text("SELECT x, COUNT(*) FROM sources, UNNEST(listed_in) x GROUP BY x")).fetchall()
    return {k: v for k, v in rows}


def listed_in_map(conn):
    return {r[0]: r[1] for r in conn.execute(text("SELECT id, listed_in FROM sources WHERE listed_in IS NOT NULL"))}


def core_ids(conn):
    return {r[0] for r in conn.execute(text("SELECT id FROM sources WHERE is_core"))}


def active_core_count(conn):
    return conn.execute(text(f"SELECT COUNT(*) FROM sources s WHERE s.is_core AND {NOT_LOSER}")).scalar()


def set_core(conn, ids, value):
    if not ids:
        return 0
    return conn.execute(text("UPDATE sources SET is_core = :v, updated_date = now() WHERE id = ANY(:ids)"),
                        {"v": value, "ids": sorted(ids)}).rowcount


conn = engine.connect()
trans = conn.begin()
try:
    before_lists = listed_in_counts(conn)
    before_map = listed_in_map(conn)
    max_edition_id = None
    before_core = core_ids(conn)
    before_row = conn.execute(text("SELECT id, display_name, maintainer, url, scope, list_version, loaded_at "
                                   "FROM source_list WHERE id = :id"), {"id": LIST_ID}).mappings().first()
    assert before_row, "no source_list row 'cwts-core'"
    receipt["before"] = {"is_core_all_rows": len(before_core), "is_core_active": active_core_count(conn),
                         "listed_in_counts": before_lists, "source_list_row": dict(before_row)}
    print("BEFORE", json.dumps({k: v for k, v in receipt["before"].items() if k != "listed_in_counts"}, default=str),
          flush=True)

    if REVERT:
        path = next(a for a in sys.argv[1:] if a.endswith(".json"))
        old = json.load(open(path))
        to_false, to_true = set(old["set_true"]), set(old["set_false"])
        to_null = set(old.get("set_true_was_null", [])) & to_false
        receipt["reverted_receipt"] = os.path.basename(path)
        n_true, n_false = set_core(conn, to_true, True), set_core(conn, to_false - to_null, False)
        n_false += set_core(conn, to_null, None)
        r = old["before"]["source_list_row"]
        conn.execute(text("UPDATE source_list SET url = :u, scope = :s, list_version = :v, loaded_at = :l "
                          "WHERE id = :id"),
                     {"u": r["url"], "s": r["scope"], "v": r["list_version"], "l": r["loaded_at"], "id": LIST_ID})
        receipt["set_true"], receipt["set_false"] = sorted(to_true), sorted(to_false)
        receipt["rows_changed"] = {"to_true": n_true, "to_false": n_false}
    else:
        with open(EDITION_FILE, newline="", encoding="utf-8") as f:
            edition = {int(r["source_id"]) for r in csv.DictReader(f, delimiter="\t")}
        max_edition_id = max(edition)
        assert abs(receipt["before"]["is_core_active"] - EXPECT_BEFORE) <= TOL, \
            f"is_core active = {receipt['before']['is_core_active']}, expected ~{EXPECT_BEFORE}; registry drifted, look first"
        active = {r[0] for r in conn.execute(text(
            f"SELECT s.id FROM sources s WHERE s.id = ANY(:ids) AND {NOT_LOSER}"), {"ids": sorted(edition)})}
        skipped = edition - active
        winners = {r[0]: r[1] for r in conn.execute(text(
            "SELECT loser_id, winner_id FROM source_merge WHERE loser_id = ANY(:ids)"), {"ids": sorted(skipped)})}
        to_true = active - before_core
        to_false = {i for i in before_core if i not in active and i <= max_edition_id}
        kept_new = sorted(i for i in before_core if i > max_edition_id)
        was_null = sorted(r[0] for r in conn.execute(text(
            "SELECT id FROM sources WHERE id = ANY(:ids) AND is_core IS NULL"), {"ids": sorted(to_true)}))
        n_true, n_false = set_core(conn, to_true, True), set_core(conn, to_false, False)
        conn.execute(text("UPDATE source_list SET url = :u, scope = :s, list_version = :v, loaded_at = now() "
                          "WHERE id = :id"), {"u": RECORD_URL, "s": SCOPE, "v": VERSION, "id": LIST_ID})
        receipt.update({
            "edition_file": os.path.basename(EDITION_FILE), "edition_core": len(edition),
            "max_edition_id": max_edition_id, "active_targets": len(active),
            "skipped_not_active": [{"id": i, "merged_into": winners.get(i)} for i in sorted(skipped)],
            "kept_minted_after_snapshot": kept_new,
            "set_true": sorted(to_true), "set_true_was_null": was_null, "set_false": sorted(to_false),
            "rows_changed": {"to_true": n_true, "to_false": n_false}})
        print("EDITION", json.dumps({"edition_core": len(edition), "active_targets": len(active),
                                     "skipped": len(skipped), "kept_minted_after_snapshot": kept_new,
                                     "to_true": n_true, "to_false": n_false}), flush=True)

    changed = recompute_listed_in(conn)
    after_lists = listed_in_counts(conn)
    after_map = listed_in_map(conn)
    flipped = set(receipt["set_true"]) | set(receipt["set_false"])
    drift = sorted(i for i in set(before_map) | set(after_map)
                   if before_map.get(i) != after_map.get(i) and i not in flipped)
    receipt["listed_in_drift"] = [{"id": i, "before": before_map.get(i), "after": after_map.get(i)} for i in drift]
    print("DRIFT", json.dumps(receipt["listed_in_drift"]), flush=True)
    receipt["after"] = {"is_core_all_rows": len(core_ids(conn)), "is_core_active": active_core_count(conn),
                        "listed_in_rows_changed": changed,
                        "cwts_core_listed": [before_lists.get(LIST_ID, 0), after_lists.get(LIST_ID, 0)],
                        "other_lists_moved": {k: [before_lists.get(k, 0), after_lists.get(k, 0)]
                                              for k in sorted(set(before_lists) | set(after_lists))
                                              if k != LIST_ID and before_lists.get(k, 0) != after_lists.get(k, 0)}}
    print("AFTER", json.dumps(receipt["after"]), flush=True)
    # Guards: besides the is_core flips, recompute may only fill listed_in on sources minted after the edition's
    # snapshot that have none yet (the daily mint does not call recompute_listed_in; dry run 20 Nov: 5 such journals
    # minted 19-20 Nov, +9 memberships on 6 file-loaded lists). Any other drift: stop and look. And the core count
    # must land where the live-API dry run said.
    if not REVERT:
        bad = [d for d in receipt["listed_in_drift"] if not (d["id"] > max_edition_id and d["before"] is None)]
        assert not bad, f"listed_in drift beyond fresh mints: {bad[:10]}; investigate before --execute"
        assert abs(receipt["after"]["is_core_active"] - EXPECT_AFTER) <= TOL, "after count off; investigate"
    if EXECUTE:
        trans.commit()
        print("committed")
    else:
        trans.rollback()
        print("DRY RUN: rolled back")
    print("RECEIPT", json.dumps(receipt, default=str))
except Exception:
    trans.rollback()
    raise
finally:
    conn.close()
