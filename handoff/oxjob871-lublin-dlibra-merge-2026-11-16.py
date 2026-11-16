"""Retire Lublin University of Technology's old dLibra repository source into ROCK (oxjob #871, ZD 23521).
Support factory #2254, written off desk 2026-11-16 (lane for #871); run on desk from the openalex-sources checkout.

Why: the university replaced its dLibra Digital Library (bc.pollub.pl, S4306400364) with DSpace 7 ROCK
(rock.pollub.pl, S7407055439). Live 2026-11-16: S4306400364 holds 0 works (works?filter=locations.source.id:S4306400364
-> 0) and bc.pollub.pl does not answer, yet the institution I8264552 still lists it in repositories[] beside ROCK.
The requester (the repository manager) asked for it to be marked superseded and taken off the institution's list.
Same shape as #629's ZD 8179 (ELAR UrFU on its old domain merged into the new one, 2026-11-15).

What it does, in ONE transaction (dry run by default):
  1. Snapshot both sources and every oai_pmh_endpoint row on either (printed: the ROCK row's pmh_url is the receipt
     for #871 acceptance item 3, expected https://rock.pollub.pl/server/oai/request).
  2. Guards: loser is the Lublin Digital Library repository hosted by I8264552, not curator-overridden; winner is ROCK
     hosted by I8264552; every endpoint on the loser points at bc.pollub.pl.
  3. Any loser endpoint still able to harvest gets ready_to_run = false, in_walden = false (status untouched, so no
     retirement receipt is needed; the host is dead and nothing in works comes from it).
  4. merge(): same steps as merge() in handoff/oxjob629-ticket-merges-2026-11-15.py (loser deleted, its endpoints /
     ISSNs / DataCite ids move to the winner, ledger row in source_merge, which also serves the merged-ID redirect).

Run on desk:
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob871-lublin-dlibra-merge-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob871-lublin-dlibra-merge-2026-11-16.py --execute
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker; save it as
handoff/oxjob871-lublin-dlibra-merge-2026-11-16-receipt.json and commit both files.
Revert: re-insert the loser's `before` row into sources (INSERT ... OVERRIDING SYSTEM VALUE: sources.id is an
identity column, migration 012), move its endpoints back (UPDATE oai_pmh_endpoint SET
source_id = 4306400364, ready_to_run/in_walden back to their `before` values WHERE id IN <receipt endpoint ids>),
remove the gained alternate title from 7407055439, and DELETE FROM source_merge WHERE loser_id = 4306400364.
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
JOB = 871
TICKETS = [23521]
APPROVED = "support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit)"
LOSER, WINNER, INSTITUTION = 4306400364, 7407055439, 8264552
LOSER_NAME_PREFIX = "lublin university of technology digital library"
WINNER_NAME = "repository of open collections and knowledge (rock)"
RULE = "zendesk_871_superseded_repository"

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB,
           "before": {}, "endpoints_before": [], "actions": [], "after": {}, "endpoints_after": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def jsonable(row):
    # lists/dicts (JSONB columns such as alternate_titles) stay JSON so the receipt row re-inserts as-is (desk review)
    return {k: (None if v is None else v if isinstance(v, (list, dict)) else str(v)) for k, v in dict(row).items()}


def endpoints(conn, ids):
    return [jsonable(r) for r in conn.execute(
        text("SELECT * FROM oai_pmh_endpoint WHERE source_id = ANY(:i) ORDER BY source_id, id"), {"i": list(ids)}).mappings()]


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
    try:
        with engine.begin() as conn:
            for r in conn.execute(text("SELECT * FROM sources WHERE id IN (:l, :w)"), {"l": LOSER, "w": WINNER}).mappings():
                receipt["before"][str(r["id"])] = jsonable(r)
            receipt["endpoints_before"] = endpoints(conn, [LOSER, WINNER])
            for e in receipt["endpoints_before"]:
                print(f"  endpoint {e['id']} source={e['source_id']} pmh_url={e.get('pmh_url')} set={e.get('pmh_set')} "
                      f"status={e.get('status')} ready_to_run={e.get('ready_to_run')} in_walden={e.get('in_walden')}")

            lo, wi = receipt["before"].get(str(LOSER)), receipt["before"].get(str(WINNER))
            assert lo is not None, "loser S4306400364 already gone (merged earlier?) - check source_merge"
            assert wi is not None, "winner S7407055439 missing"
            assert (lo["display_name"] or "").strip().lower().startswith(LOSER_NAME_PREFIX), lo["display_name"]
            assert (wi["display_name"] or "").strip().lower() == WINNER_NAME, wi["display_name"]
            assert lo["type"] == "repository" and wi["type"] == "repository", (lo["type"], wi["type"])
            assert lo["institution_id"] == str(INSTITUTION), lo["institution_id"]
            assert wi["institution_id"] == str(INSTITUTION), wi["institution_id"]
            loser_eps = [e for e in receipt["endpoints_before"] if e["source_id"] == str(LOSER)]
            for e in loser_eps:
                assert "bc.pollub.pl" in (e.get("pmh_url") or ""), e
            rock_eps = [e for e in receipt["endpoints_before"] if e["source_id"] == str(WINNER)]
            log(f"ROCK endpoints on file: {[e.get('pmh_url') for e in rock_eps]}")

            for e in loser_eps:
                if e.get("ready_to_run") in ("True", None) or e.get("in_walden") in ("True", None):
                    n = conn.execute(text("UPDATE oai_pmh_endpoint SET ready_to_run = FALSE, in_walden = FALSE "
                                          "WHERE id = :id AND source_id = :l"), {"id": e["id"], "l": LOSER}).rowcount
                    assert n == 1, e
                    log(f"endpoint {e['id']} ({e.get('pmh_url')}): ready_to_run, in_walden -> false (host dead)")

            merge(conn, LOSER, WINNER, RULE, {"tickets": TICKETS, "oxjob": JOB, "approved_by": APPROVED,
                                              "note": "dLibra bc.pollub.pl replaced by DSpace ROCK rock.pollub.pl; "
                                                      "loser holds 0 works (live API 2026-11-16), host dead"})

            for r in conn.execute(text("SELECT id, display_name, type, institution_id, publisher, country_code, "
                                       "homepage_url, alternate_titles FROM sources WHERE id IN (:l, :w)"),
                                  {"l": LOSER, "w": WINNER}).mappings():
                receipt["after"][str(r["id"])] = jsonable(r)
            receipt["endpoints_after"] = endpoints(conn, [LOSER, WINNER])
            assert str(LOSER) not in receipt["after"], "loser still present"
            assert receipt["after"][str(WINNER)]["institution_id"] == str(INSTITUTION)
            print(json.dumps(receipt["after"], ensure_ascii=False, indent=1))
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
