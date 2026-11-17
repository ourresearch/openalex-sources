"""Retire the ANET OAI endpoint for the University of Antwerp and merge its source into IRUA (oxjob #227, ZD 27248).
Support factory #2254, desk apply lane, 2026-11-16. Run on desk from the openalex-sources checkout.

Why: endpoint 5f258b6453bc4859c54 (http://anet.be/oai/abua/server.phtml, source S4377196495 "Anet (University of
Antwerp)") harvests the same c:irua:N records as IRUA's own endpoint mmv3envg3kaaztya9tmo
(https://repository.uantwerpen.be/oai/abua/, S4306401849). anet.be is being retired, and the IRUA repository manager
(ZD 27248, 2026-11-09) asked us to drop the duplicate source. It must happen before #227's parser fix
ships: once the parser reads IRUA's bare <dc> wrapper, both endpoints deliver the same native_ids and repo_parsed keeps
whichever copy it reads first. S4377196495 holds 0 works (live API 2026-11-16: works?filter=locations.source.id ->
0); S4306401849 holds 5.

What it does, in ONE transaction (dry run by default):
  1. Snapshot both sources and every oai_pmh_endpoint row on either.
  2. Guards: loser is "Anet (University of Antwerp)", winner is IRUA, both repositories hosted by I149213910; the ANET
     endpoint is active, on the loser, at anet.be; every loser endpoint is at anet.be; IRUA's endpoint is on the winner.
  3. Retire the ANET endpoint: endpoint_retirement_audit receipt row, then status 'retired' + retirement_reason +
     retired_at (same now()) + ready_to_run/in_walden false (migration 028/034 trigger checks the same-transaction
     receipt at commit).
  4. merge(): same steps as merge() in handoff/oxjob871-lublin-dlibra-merge-2026-11-16.py (loser deleted, its
     endpoints / ISSNs / DataCite ids move to the winner, ledger row in source_merge, which serves the redirect).

Run:
    cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob227-anet-retire-merge-2026-11-16.py
    ... same + --execute
Receipt: printed as JSON after the RECEIPT marker; saved as handoff/oxjob227-anet-retire-merge-2026-11-16-receipt.json.
Revert: re-insert the loser's `before` row into sources (INSERT ... OVERRIDING SYSTEM VALUE), DELETE FROM source_merge
WHERE loser_id = 4377196495, remove the gained alternate title from 4306401849, then UPDATE oai_pmh_endpoint SET
source_id = 4377196495, status = 'active', retirement_reason = NULL, retired_at = NULL, ready_to_run/in_walden back to
their `before` values WHERE id = '5f258b6453bc4859c54' (leaving 'retired' is allowed since migration 034; the audit
row stays as history).
"""
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
JOB = 227
TICKETS = [27248]
APPROVED = ("support factory #2254 (Jason 2026-11-15 yes; standing authority, reversible registry edit); "
            "registry route approved by #1831 2026-11-16 09:45 CT")
LOSER, WINNER, INSTITUTION = 4377196495, 4306401849, 149213910
LOSER_NAME = "anet (university of antwerp)"
WINNER_NAME = "institutional repository university of antwerp (university of antwerp)"
ANET_EP, IRUA_EP = "5f258b6453bc4859c54", "mmv3envg3kaaztya9tmo"
RULE = "zendesk_27248_duplicate_repository"
REASON = ("oxjob #227 / ZD 27248: anet.be OAI endpoint duplicates IRUA's own endpoint mmv3envg3kaaztya9tmo "
          "(same c:irua records); anet.be is being retired and the IRUA repository manager asked to drop it")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB,
           "before": {}, "endpoints_before": [], "actions": [], "after": {}, "endpoints_after": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def jsonable(row):
    return {k: (None if v is None else v if isinstance(v, (list, dict, bool)) else str(v)) for k, v in dict(row).items()}


def endpoints(conn, ids):
    return [jsonable(r) for r in conn.execute(
        text("SELECT * FROM oai_pmh_endpoint WHERE source_id = ANY(:i) ORDER BY source_id, id"),
        {"i": list(ids)}).mappings()]


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
    """Same steps as merge() in handoff/oxjob871-lublin-dlibra-merge-2026-11-16.py."""
    rows = {r.id: r for r in conn.execute(
        text("SELECT id, display_name, alternate_titles, override_timestamp, issn_l FROM sources WHERE id IN (:l, :w)"),
        {"l": loser, "w": winner})}
    assert loser in rows and winner in rows, (loser, winner)
    assert rows[loser].override_timestamp is None, f"loser {loser} is curator-overridden"
    conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :l"), {"l": loser})
    moved = conn.execute(text("UPDATE source_issn SET source_id = :w, is_issn_l = FALSE WHERE source_id = :l"),
                         {"l": loser, "w": winner}).rowcount
    ep = conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :w WHERE source_id = :l"),
                      {"l": loser, "w": winner}).rowcount
    dc = conn.execute(text("UPDATE source_datacite_id SET source_id = :w WHERE source_id = :l"),
                      {"l": loser, "w": winner}).rowcount
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
                 {"l": loser, "w": winner, "r": rule,
                  "d": json.dumps({**detail, "loser_name": rows[loser].display_name}, ensure_ascii=False)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    log(f"MERGED {loser} -> {winner} ({rule}): issns moved {moved}, endpoints {ep}, datacite {dc}, beacon {bm}")


def retire(conn, ep_row):
    run_id = str(uuid.uuid4())
    manifest_hash = hashlib.sha256(json.dumps(ep_row, sort_keys=True, default=str).encode()).hexdigest()
    plan_hash = hashlib.sha256(open(os.path.abspath(__file__), "rb").read()).hexdigest()
    conn.execute(text("""
        INSERT INTO endpoint_retirement_audit (
            endpoint_id, run_id, pmh_url, pmh_set, source_id_before, legacy_source_endpoint_source_id,
            legacy_sources_endpoint_source_ids_before, status_before, retirement_reason_before, retired_at_before,
            ready_to_run_before, in_walden_before, status_after, retirement_reason, retired_at, reason,
            database_name, schema_name, manifest_hash, plan_hash, review_ref, executed_by, executed_at)
        SELECT e.id, CAST(:run AS UUID), e.pmh_url, e.pmh_set, e.source_id, NULL,
               CAST(ARRAY[] AS BIGINT[]), e.status, e.retirement_reason, e.retired_at,
               e.ready_to_run, e.in_walden, 'retired', :reason, now(), :reason,
               current_database(), 'public', :mh, :ph, :ref, :by, now()
        FROM oai_pmh_endpoint e WHERE e.id = :id AND e.status = 'active'"""),
                 {"run": run_id, "reason": REASON, "mh": manifest_hash, "ph": plan_hash, "id": ep_row["id"],
                  "ref": f"oxjob #{JOB}; ZD {TICKETS[0]}; {APPROVED}",
                  "by": "support factory desk lane #227 (claude) via run_registry.py"})
    n = conn.execute(text("UPDATE oai_pmh_endpoint SET status = 'retired', retirement_reason = :reason, "
                          "retired_at = now(), ready_to_run = FALSE, in_walden = FALSE "
                          "WHERE id = :id AND status = 'active' AND source_id = :l"),
                     {"reason": REASON, "id": ep_row["id"], "l": LOSER}).rowcount
    assert n == 1, n
    log(f"RETIRED endpoint {ep_row['id']} ({ep_row.get('pmh_url')}): status retired, ready_to_run/in_walden false; "
        f"audit run_id {run_id}")


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            for r in conn.execute(text("SELECT * FROM sources WHERE id IN (:l, :w)"), {"l": LOSER, "w": WINNER}).mappings():
                receipt["before"][str(r["id"])] = jsonable(r)
            receipt["before"]["source_works_count"] = [jsonable(r) for r in conn.execute(
                text("SELECT * FROM source_works_count WHERE source_id IN (:l, :w)"), {"l": LOSER, "w": WINNER}).mappings()]
            receipt["endpoints_before"] = endpoints(conn, [LOSER, WINNER])
            for e in receipt["endpoints_before"]:
                print(f"  endpoint {e['id']} source={e['source_id']} pmh_url={e.get('pmh_url')} set={e.get('pmh_set')} "
                      f"status={e.get('status')} ready_to_run={e.get('ready_to_run')} in_walden={e.get('in_walden')}")

            lo, wi = receipt["before"].get(str(LOSER)), receipt["before"].get(str(WINNER))
            assert lo is not None, "loser S4377196495 already gone (merged earlier?) - check source_merge"
            assert wi is not None, "winner S4306401849 missing"
            assert (lo["display_name"] or "").strip().lower() == LOSER_NAME, lo["display_name"]
            assert (wi["display_name"] or "").strip().lower() == WINNER_NAME, wi["display_name"]
            assert lo["type"] == "repository" and wi["type"] == "repository", (lo["type"], wi["type"])
            assert lo["institution_id"] == str(INSTITUTION), lo["institution_id"]
            assert wi["institution_id"] == str(INSTITUTION), wi["institution_id"]
            eps = {e["id"]: e for e in receipt["endpoints_before"]}
            assert ANET_EP in eps and eps[ANET_EP]["source_id"] == str(LOSER), "ANET endpoint not on the loser"
            assert eps[ANET_EP]["status"] == "active", eps[ANET_EP]["status"]
            assert "anet.be" in (eps[ANET_EP].get("pmh_url") or ""), eps[ANET_EP]
            assert IRUA_EP in eps and eps[IRUA_EP]["source_id"] == str(WINNER), "IRUA endpoint not on the winner"
            for e in receipt["endpoints_before"]:
                if e["source_id"] == str(LOSER):
                    assert "anet.be" in (e.get("pmh_url") or ""), e

            retire(conn, eps[ANET_EP])
            merge(conn, LOSER, WINNER, RULE, {"tickets": TICKETS, "oxjob": JOB, "approved_by": APPROVED,
                                              "note": "anet.be OAI endpoint duplicated IRUA's own feed; loser held 0 "
                                                      "works (live API 2026-11-16); repository manager asked"})

            for r in conn.execute(text("SELECT id, display_name, type, institution_id, homepage_url, alternate_titles "
                                       "FROM sources WHERE id IN (:l, :w)"), {"l": LOSER, "w": WINNER}).mappings():
                receipt["after"][str(r["id"])] = jsonable(r)
            receipt["endpoints_after"] = endpoints(conn, [LOSER, WINNER])
            receipt["after"]["retirement_audit"] = [jsonable(r) for r in conn.execute(
                text("SELECT * FROM endpoint_retirement_audit WHERE endpoint_id = :id"), {"id": ANET_EP}).mappings()]
            assert str(LOSER) not in receipt["after"], "loser still present"
            a = {e["id"]: e for e in receipt["endpoints_after"]}
            assert a[ANET_EP]["status"] == "retired" and a[ANET_EP]["source_id"] == str(WINNER), a[ANET_EP]
            assert a[ANET_EP]["ready_to_run"] is False and a[ANET_EP]["in_walden"] is False, a[ANET_EP]
            assert a[IRUA_EP]["status"] == "active", a[IRUA_EP]
            for e in receipt["endpoints_after"]:
                print(f"  after: endpoint {e['id']} source={e['source_id']} status={e.get('status')} "
                      f"ready_to_run={e.get('ready_to_run')} in_walden={e.get('in_walden')}")
            print(json.dumps({k: v for k, v in receipt["after"].items() if k != "retirement_audit"},
                             ensure_ascii=False, indent=1))
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
