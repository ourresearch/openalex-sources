"""ICLR gets a feed endpoint for its own proceedings (oxjob #2100).

Why: the International Conference on Learning Representations (S4306419637, a conference source) mints no DOIs and
publishes on OpenReview, so no ingest lane sees its papers: each one's work stays an arXiv preprint (a random 180 of
the 2024-2026 papers: 0 carried an ICLR location). ICLR's own site, https://proceedings.iclr.cc/ , lists every 2024-2026
paper (11,314) with an abstract page and a free PDF. The oxjob's build_iclr_feed.py turns those pages into oai_dc
records in the harvester's own file format, written to s3://openalex-ingest/repositories/<this endpoint>/, where walden's
Repo ingest reads every endpoint's files (the route #2097 built for TMLR). This row binds those records to S4306419637.
The records state conferenceObject + publishedVersion themselves, so each lands as a published-version location with a
PDF and leads its work (one work, published copy primary: #1099). is_journal_host is set because the endpoint is the
venue's own host (#1409); CreateWorksBase reads that flag for journal sources only today, so for this conference
source it changes nothing until that gate covers conferences (the oxjob's step list).

What it does, in ONE transaction (dry run by default, rolled back):
  1. Guards: S4306419637 is the conference 'International Conference on Learning Representations'; no endpoint is
     bound to it yet; no endpoint mentions iclr.cc or openreview.net; the endpoint id is unused.
  2. INSERT oai_pmh_endpoint j804cxp8hy6htjgkk8hy: pmh_url https://proceedings.iclr.cc/ (the site the records come
     from; NOT an OAI-PMH server), ready_to_run false (the harvester never calls it: the files are written by the
     builder), in_walden true, status active, is_journal_host true, green_scrape false (the records carry their PDF).
  3. homepage_url https://iclr.cc/ on S4306419637 when it is NULL.

Run on desk from the openalex-sources checkout:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2100-iclr-feed-endpoint-2026-11-17.py
    ... the same with --execute
The receipt prints after RECEIPT and, with --execute, is written next to this file.

Revert: ... run_registry.py handoff/oxjob2100-iclr-feed-endpoint-2026-11-17.py --revert [--execute]
  = DELETE FROM oai_pmh_endpoint WHERE id = 'j804cxp8hy6htjgkk8hy' AND source_id = 4306419637;
    UPDATE sources SET homepage_url = NULL WHERE id = 4306419637 AND homepage_url = 'https://iclr.cc/' (only if this
    script set it). Records already ingested stay until their native ids (oai:proceedings.iclr.cc:<year>/<hash>) are
    added to openalex.works.location_denylist; the oxjob's PLAN.md carries that step.
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
JOB = 2100
SOURCE = 4306419637
SOURCE_NAME = "international conference on learning representations"
ENDPOINT = "j804cxp8hy6htjgkk8hy"
PMH_URL = "https://proceedings.iclr.cc/"
HOMEPAGE = "https://iclr.cc/"
# The table has no free-text note column; harvest_identify_response says what an Identify call would: there is none.
NOTE = ("No OAI-PMH server: this is ICLR's proceedings site. Records are written to S3 by oxjob #2100's "
        "build_iclr_feed.py; ready_to_run stays false so the harvester never calls it.")
RECEIPT_PATH = os.path.join(HERE, "oxjob2100-iclr-feed-endpoint-2026-11-17-receipt.json")

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
    assert s["type"] == "conference", s["type"]

    on_source = rows(conn, "SELECT id, pmh_url, pmh_set, status FROM oai_pmh_endpoint WHERE source_id = :id", id=SOURCE)
    receipt["before"]["endpoints_on_source"] = on_source
    assert not on_source, f"S{SOURCE} already has endpoint(s): {on_source}; look first"
    on_host = rows(conn, "SELECT id, pmh_url, pmh_set, source_id, status FROM oai_pmh_endpoint "
                         "WHERE pmh_url ILIKE '%iclr.cc%' OR pmh_url ILIKE '%openreview.net%'")
    receipt["before"]["endpoints_on_hosts"] = on_host
    assert not on_host, f"an endpoint already names iclr.cc/openreview.net: {on_host}; look first"
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
