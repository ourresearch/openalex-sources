"""Registry edit for oxjob #613 / Zendesk ticket 4784 (Utrecht University Library, 2025-06-10):
bioRxiv and medRxiv get the source-level "whole repository is open" override arXiv, PubMed Central and Zenodo
already carry (openalex-sources migration 013: source_oa_override curated_is_oa TRUE).

  bioRxiv  S4306402567  "bioRxiv (Cold Spring Harbor Laboratory)"  repository, is_oa false
  medRxiv  S3005729997  "medRxiv"                                   repository, is_oa false

Why (support factory lane, off desk, 15 Nov 2026; receipts in oxjob #613 work/):
  - Policy choice: follows RULINGS.md section 3, OA evidence, "Keep a source-level 'this whole repository is open'
    status, and grant it sparingly" (#1580).
  - 4,362 of 359,799 works with a bioRxiv location are served closed (3,702 bioRxiv-only preprints, mostly 2025-2026);
    medRxiv 608 of 91,522. Cause: bioRxiv's 'cc_no' licence (free to read, all rights reserved) is not a CC licence,
    so the Crossref location's is_oa stays false unless a crawl found the PDF.
  - 4/4 closed examples checked: api.biorxiv.org licence cc_no, full PDF downloads without a login
    (work/biorxiv_check_2026-11-15.tsv).

HOW TO RUN (desk): copy this file to openalex-sources/handoff/613-biorxiv-medrxiv-oa-2026-11-15.py, then from the
openalex-sources checkout (dry run by default; ONE transaction):
    python3 <runner> handoff/613-biorxiv-medrxiv-oa-2026-11-15.py [--with-medrxiv] [--execute]
where the runner fetches DATABASE_URL from Heroku into the child env only (oxjob #1880 work/run_registry.py), as for
handoff/9061-cjpe-2026-10-28.py. The receipt (before-state + actions) is printed as JSON after the RECEIPT marker;
save it to #613 work/biorxiv_medrxiv_oa_override_receipt.json.

REVERT: DELETE FROM source_oa_override WHERE source_id IN (4306402567, 3005729997); then
UPDATE sources SET is_oa_high_oa_rate = <before>, high_oa_rate_start_year = <before> WHERE id = ...; and recompute
is_oa (sources_lib.recompute_is_oa). The weekly apply_oa_flags (Mon 05:55 UTC) computes the same values either way.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name

EXECUTE = "--execute" in sys.argv
SOURCES = {
    4306402567: "bioRxiv (Cold Spring Harbor Laboratory)",
}
# medRxiv only with --with-medrxiv: from the container 1 of 3 closed medRxiv PDFs opened (W7160759861, cc_no); the other
# two (W4400742452 10.1101/2024.07.15.24310431, W3032961231) hit medRxiv's 403 bot check. Open two from desk first.
if "--with-medrxiv" in sys.argv:
    SOURCES[3005729997] = "medRxiv"
NOTE = ("oxjob #613 / ZD 4784: whole preprint server free to read (cc_no = free to read, all rights reserved); "
        "4/4 closed bioRxiv PDFs fetched without login 2026-11-15; follows RULINGS #1580 (source-level open status)")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 613, "ticket": 4784,
           "before": {}, "actions": [], "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn, sid):
    r = conn.execute(text("SELECT * FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
    return {k: (None if v is None else str(v)) for k, v in r.items()} if r else None


def main():
    try:
        with engine.begin() as conn:
            for sid, expect in SOURCES.items():
                before = row(conn, sid)
                assert before is not None, f"source {sid} missing"
                receipt["before"][str(sid)] = before
                receipt["before"][f"source_oa_override {sid}"] = [dict(r._mapping) for r in conn.execute(
                    text("SELECT * FROM source_oa_override WHERE source_id = :id"), {"id": sid})]
                assert normalize_name(before["display_name"]) == normalize_name(expect), before["display_name"]
                assert before["type"] == "repository", f"{sid} type drifted: {before['type']}"

                existing = conn.execute(text("SELECT curated_is_oa, curated_flip_year FROM source_oa_override "
                                             "WHERE source_id = :id"), {"id": sid}).fetchone()
                if existing is None:
                    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) "
                                      "VALUES (:s, TRUE, :n)"), {"s": sid, "n": NOTE})
                    log(f"INSERT source_oa_override {sid}: curated_is_oa TRUE, no flip year")
                else:
                    log(f"source_oa_override {sid} already exists {tuple(existing)}: left as is; stop and look")
                    continue

                # apply_oa_flags for this one source (its own precedence: curated_is_oa wins). A TRUE override with
                # no flip year takes the HOAR list start year if listed (repositories have no ISSN: NULL = all years).
                hoar_start = conn.execute(text("SELECT MIN(h.start_year) FROM source_issn si "
                                               "JOIN high_oa_rate_issn h ON h.issn_l = si.issn "
                                               "WHERE si.source_id = :id"), {"id": sid}).scalar()
                conn.execute(text("UPDATE sources SET is_oa_high_oa_rate = TRUE, high_oa_rate_start_year = :y, "
                                  "updated_date = now() WHERE id = :id"), {"y": hoar_start, "id": sid})
                conn.execute(text("""
                    UPDATE sources SET is_oa = (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
                                                OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE)),
                                       updated_date = now()
                    WHERE id = :id"""), {"id": sid})
                log(f"OA flags {sid}: is_oa_high_oa_rate TRUE, high_oa_rate_start_year {hoar_start}, is_oa recomputed")

                after = row(conn, sid)
                receipt["after"][str(sid)] = {k: after[k] for k in (
                    "display_name", "type", "is_oa", "is_oa_high_oa_rate", "high_oa_rate_start_year", "is_in_doaj")}
            print(json.dumps(receipt["after"], indent=1), flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
