"""TMLR gets a journal-host feed endpoint (oxjob #2097).

Why: Transactions on Machine Learning Research (S4393919742, ISSN 2835-8856, DOAJ, CC BY) mints no DOIs and publishes
on OpenReview, so no ingest lane sees its papers: the source holds 8 works while the journal has published 5,095, and
each paper's work stays an arXiv preprint. TMLR's own index, https://jmlr.org/tmlr/papers/ , lists every paper. The
oxjob's build_tmlr_feed.py turns that index into oai_dc records in the harvester's own file format and they are
written to s3://openalex-ingest/repositories/<this endpoint>/, where walden's Repo ingest reads every endpoint's files.
This row binds those records to S4393919742 and flags it is_journal_host (#1409), so CreateWorksBase makes each a
publisher, publishedVersion location and the location matcher can fold it into the paper's arXiv work.

What it does, in ONE transaction (dry run by default, rolled back):
  1. Guards: S4393919742 is the journal 'Transactions on machine learning research' with ISSN 2835-8856; no endpoint
     is bound to it yet; no endpoint mentions jmlr.org or openreview.net; the endpoint id is unused.
  2. INSERT oai_pmh_endpoint 2n937yccr59mdw6yixfy: pmh_url https://jmlr.org/tmlr/papers/ (the page the records come
     from; it is NOT an OAI-PMH server), ready_to_run false (the harvester never calls it: the files are written by the
     builder), in_walden true, status active, is_journal_host true, green_scrape false (OpenReview answers scripts with
     a bot challenge; the records carry their PDF link already).
  3. homepage_url https://jmlr.org/tmlr/ on S4393919742 when it is NULL (DOAJ's journal URL for this ISSN).

Run on desk from the openalex-sources checkout:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2097-tmlr-feed-endpoint-2026-11-17.py
    ... the same with --execute
The receipt prints after RECEIPT and, with --execute, is written next to this file.

Revert: ... run_registry.py handoff/oxjob2097-tmlr-feed-endpoint-2026-11-17.py --revert [--execute]
  = DELETE FROM oai_pmh_endpoint WHERE id = '2n937yccr59mdw6yixfy' AND source_id = 4393919742;
    UPDATE sources SET homepage_url = NULL WHERE id = 4393919742 AND homepage_url = 'https://jmlr.org/tmlr/' (only if
    this script set it). Records already ingested stay until their native ids (oai:jmlr.org:tmlr/<id>) are added to
    openalex.works.location_denylist; the oxjob's PLAN.md carries that step.
Live after the next sources snapshot + End 2 End (the night after the write).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
JOB = 2097
SOURCE = 4393919742
SOURCE_NAME = "transactions on machine learning research"
ISSN = "2835-8856"
ENDPOINT = "2n937yccr59mdw6yixfy"
PMH_URL = "https://jmlr.org/tmlr/papers/"
HOMEPAGE = "https://jmlr.org/tmlr/"
# The table has no free-text note column; harvest_identify_response says what an Identify call would: there is none.
NOTE = ("No OAI-PMH server: this is TMLR's paper index page. Records are written to S3 by oxjob #2097's "
        "build_tmlr_feed.py; ready_to_run stays false so the harvester never calls it.")
RECEIPT_PATH = os.path.join(HERE, "oxjob2097-tmlr-feed-endpoint-2026-11-17-receipt.json")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "mode": "revert" if REVERT else "apply", "job": JOB, "before": {}, "actions": [], "after": {}, "revert": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def jsonable(row):
    return {k: (None if v is None else v if isinstance(v, (list, dict, bool, int)) else str(v))
            for k, v in dict(row).items()}


def rows(conn, sql, **kw):
    return [jsonable(r) for r in conn.execute(text(sql), kw).mappings()]


def apply(conn):
    src = rows(conn, "SELECT id, display_name, type, issn_l, issns, homepage_url, is_oa FROM sources WHERE id = :id",
               id=SOURCE)
    assert len(src) == 1, f"S{SOURCE} not found (merged?)"
    s = src[0]
    receipt["before"]["source"] = s
    assert (s["display_name"] or "").strip().lower() == SOURCE_NAME, s["display_name"]
    assert s["type"] == "journal", s["type"]
    assert s["issn_l"] == ISSN or ISSN in json.dumps(s["issns"]), (s["issn_l"], s["issns"])

    on_source = rows(conn, "SELECT id, pmh_url, pmh_set, status FROM oai_pmh_endpoint WHERE source_id = :id", id=SOURCE)
    receipt["before"]["endpoints_on_source"] = on_source
    assert not on_source, f"S{SOURCE} already has endpoint(s): {on_source}; look first"
    on_host = rows(conn, "SELECT id, pmh_url, pmh_set, source_id, status FROM oai_pmh_endpoint "
                         "WHERE pmh_url ILIKE '%jmlr.org%' OR pmh_url ILIKE '%openreview.net%'")
    receipt["before"]["endpoints_on_hosts"] = on_host
    assert not on_host, f"an endpoint already names jmlr.org/openreview.net: {on_host}; look first"
    assert not rows(conn, "SELECT id FROM oai_pmh_endpoint WHERE id = :e", e=ENDPOINT), "endpoint id taken"
    assert not rows(conn, "SELECT endpoint_id FROM endpoint_deletion_audit WHERE endpoint_id = :e", e=ENDPOINT)

    conn.execute(text(
        "INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
        "source_id, is_journal_host, green_scrape, harvest_identify_response, rand) "
        "VALUES (:id, :u, NULL, 'oai_dc', false, true, 'active', :sid, true, false, :note, random())"),
        {"id": ENDPOINT, "u": PMH_URL, "sid": SOURCE, "note": NOTE})
    log(f"REGISTERED endpoint {ENDPOINT} {PMH_URL} -> S{SOURCE} is_journal_host, ready_to_run false (files, not harvest)")
    receipt["revert"].append(f"DELETE FROM oai_pmh_endpoint WHERE id = '{ENDPOINT}' AND source_id = {SOURCE};")

    receipt["homepage_set"] = False
    if s["homepage_url"] is None:
        n = conn.execute(text("UPDATE sources SET homepage_url = :h, updated_date = now() "
                              "WHERE id = :id AND homepage_url IS NULL"), {"h": HOMEPAGE, "id": SOURCE}).rowcount
        assert n == 1, n
        receipt["homepage_set"] = True
        log(f"S{SOURCE} homepage_url NULL -> {HOMEPAGE}")
        receipt["revert"].append(f"UPDATE sources SET homepage_url = NULL WHERE id = {SOURCE} AND homepage_url = '{HOMEPAGE}';")

    receipt["after"]["endpoint"] = rows(conn, "SELECT id, pmh_url, pmh_set, source_id, ready_to_run, in_walden, status, "
                                              "is_journal_host, green_scrape, harvest_identify_response FROM oai_pmh_endpoint WHERE id = :e",
                                        e=ENDPOINT)
    receipt["after"]["source"] = rows(conn, "SELECT id, display_name, homepage_url FROM sources WHERE id = :id", id=SOURCE)
    assert len(receipt["after"]["endpoint"]) == 1


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    n = conn.execute(text("DELETE FROM oai_pmh_endpoint WHERE id = :e AND source_id = :s"),
                     {"e": ENDPOINT, "s": SOURCE}).rowcount
    assert n == 1, f"guard: expected 1 endpoint row deleted, got {n}"
    log(f"deleted endpoint {ENDPOINT}")
    if r.get("homepage_set"):
        n = conn.execute(text("UPDATE sources SET homepage_url = NULL, updated_date = now() "
                              "WHERE id = :id AND homepage_url = :h"), {"id": SOURCE, "h": HOMEPAGE}).rowcount
        log(f"S{SOURCE} homepage_url -> NULL ({n} row)")


def main():
    try:
        with engine.begin() as conn:
            if REVERT:
                revert(conn)
            else:
                apply(conn)
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
        path = RECEIPT_PATH.replace("-receipt.json", "-revert-receipt.json") if REVERT else RECEIPT_PATH
        json.dump(receipt, open(path, "w"), ensure_ascii=False, indent=1, default=str)
        print("receipt:", path)
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
