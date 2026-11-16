"""Registry fix for oxjob #919 / ZD 23637 (support factory #2254), written off desk 2026-11-16, run on desk.

The OAI endpoint db5833065afc2a58f4e harvested SOAS's old EPrints (eprints.soas.ac.uk/perl/oai2) but is bound to
source 4306400728 'Center for International and Regional Studies (Georgetown University)'. walden's
CreateLocationsWithSources takes a repository location's source from openalex.sources.endpoint_to_source
(= oai_pmh_endpoint.source_id, rebuilt by CreateSources), so every record that endpoint last harvested is served
under Georgetown CIRS: 7,105 locations on 7,028 works on 2026-11-16, every one id'd pmh:oai:eprints.soas.ac.uk:N
(receipt: #919 work/gt_locations_2026-11-16.tsv). repo_works is keyed on native_id alone, so re-binding the endpoint
moves those records to SOAS Research Online (4306402168) without creating duplicates.

This script: ONE UPDATE, oai_pmh_endpoint db5833065afc2a58f4e source_id 4306400728 -> 4306402168.
The endpoint stays as it is (ready_to_run false, not retired): eprints.soas.ac.uk has no DNS.

Run on desk (dry run by default; one transaction):
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py \
        handoff/oxjob919-soas-endpoint-rebind-2026-11-16.py [--execute]
Revert: UPDATE oai_pmh_endpoint SET source_id = 4306400728 WHERE id = 'db5833065afc2a58f4e';
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
ENDPOINT = "db5833065afc2a58f4e"
GEORGETOWN_CIRS = 4306400728
SOAS = 4306402168
SOAS_GOOD_ENDPOINT = "2fbe1b6f3980c7339c9"  # SOAS's Worktribe feed, already bound to SOAS (#916, 2026-08-28)

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 919,
           "tickets": [23637], "before": {}, "actions": [], "after": {}}


class DryRun(Exception):
    pass


def endpoint_row(conn, eid):
    r = conn.execute(text("SELECT * FROM oai_pmh_endpoint WHERE id = :i"), {"i": eid}).mappings().fetchone()
    return None if r is None else {k: (None if v is None else str(v)) for k, v in r.items()}


def main():
    try:
        with engine.begin() as conn:
            names = dict(conn.execute(text("SELECT id, display_name FROM sources WHERE id = ANY(:i)"),
                                      {"i": [GEORGETOWN_CIRS, SOAS]}).fetchall())
            assert "Georgetown" in (names.get(GEORGETOWN_CIRS) or ""), names
            assert "SOAS" in (names.get(SOAS) or ""), names

            before = endpoint_row(conn, ENDPOINT)
            assert before is not None, f"endpoint {ENDPOINT} missing"
            receipt["before"][ENDPOINT] = before
            receipt["before"][SOAS_GOOD_ENDPOINT] = endpoint_row(conn, SOAS_GOOD_ENDPOINT)
            assert "eprints.soas.ac.uk" in (before.get("pmh_url") or ""), before.get("pmh_url")
            assert str(before["source_id"]) == str(GEORGETOWN_CIRS), before["source_id"]
            receipt["before"]["endpoints_on_georgetown_cirs"] = [r[0] for r in conn.execute(
                text("SELECT id FROM oai_pmh_endpoint WHERE source_id = :s"), {"s": GEORGETOWN_CIRS})]

            n = conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :soas WHERE id = :e AND source_id = :gt"),
                             {"soas": SOAS, "e": ENDPOINT, "gt": GEORGETOWN_CIRS}).rowcount
            assert n == 1, n
            receipt["actions"].append(f"UPDATE oai_pmh_endpoint {ENDPOINT}: source_id {GEORGETOWN_CIRS} -> {SOAS}")
            receipt["after"][ENDPOINT] = endpoint_row(conn, ENDPOINT)
            receipt["after"]["endpoints_on_georgetown_cirs"] = [r[0] for r in conn.execute(
                text("SELECT id FROM oai_pmh_endpoint WHERE source_id = :s"), {"s": GEORGETOWN_CIRS})]

            if not EXECUTE:
                raise DryRun()
        receipt["actions"].append("COMMITTED")
    except DryRun:
        receipt["actions"].append("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=2, default=str))


if __name__ == "__main__":
    main()
