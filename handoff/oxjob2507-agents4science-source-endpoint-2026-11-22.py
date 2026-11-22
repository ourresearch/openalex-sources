"""Agents4Science gets a source record and a feed endpoint (oxjob #2507).

Why: the Open Conference of AI Agents for Science (Agents4Science; first edition 22 Oct 2025, Stanford) ran its whole
review on OpenReview: three AI reviewers on every paper plus human reviewers, every note CC BY 4.0. OpenAlex holds
none of it and has no source for the conference (no registry source names it or OpenReview). Witnesses that the venue
exists: its own site, https://agents4science.stanford.edu/ , its OpenReview group
(https://openreview.net/group?id=Agents4Science/2025/Conference), and the organizers' report on arXiv (2511.15534).
oxjob #2507's build_a4s_feed.py writes its 244 reviewed papers and their 810 reviews as oai_dc records in the
harvester's file format to s3://openalex-ingest/repositories/<this endpoint>/, where walden's Repo ingest reads them;
this row binds those records to the new source. No OAI-PMH server is involved: ready_to_run false, so the harvester
never calls the endpoint.

What it does, in ONE transaction (dry run by default, rolled back):
  1. Guards: no source already names the conference (display name or an alternate title holding 'agents4science' or
     'ai agents for science'); no endpoint names openreview.net or agents4science; the endpoint id is unused.
  2. Mint source 'Open Conference of AI Agents for Science' (type conference, the series, as OpenAlex names
     conferences without the year; alternate title 'Agents4Science'; homepage https://agents4science.stanford.edu/).
  3. INSERT oai_pmh_endpoint ni7plyt3kadohy25hzva: pmh_url the venue's OpenReview group (where the records come from;
     NOT an OAI-PMH server), ready_to_run false, in_walden true, status active, is_journal_host true (the venue's own
     host, as #2100's ICLR endpoint), green_scrape false (the records carry their PDF links).

Run on desk from this checkout:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2507-agents4science-source-endpoint-2026-11-22.py
    ... the same with --execute
The receipt prints after RECEIPT and, with --execute, is written next to this file.

Revert: ... run_registry.py handoff/oxjob2507-agents4science-source-endpoint-2026-11-22.py --revert [--execute]
  = DELETE the endpoint row, then the source row (refused if anything else now points at the source: another endpoint,
    a merge). Records already ingested leave with the feed's own revert (oxjob #2507 PLAN.md): S3 files removed and the
    native ids (oai:openreview.net:agents4science2025/<id>) deleted through openalex.repo.repo_works_backfill.
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
from sources_lib import MatchContext, mint_source  # noqa: E402

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
JOB = 2507
NAME = "Open Conference of AI Agents for Science"
ALT_TITLES = ["Agents4Science"]
HOMEPAGE = "https://agents4science.stanford.edu/"
ENDPOINT = "ni7plyt3kadohy25hzva"
PMH_URL = "https://openreview.net/group?id=Agents4Science/2025/Conference"
# The table has no free-text note column; harvest_identify_response says what an Identify call would: there is none.
NOTE = ("No OAI-PMH server: this is the venue's OpenReview group. Records are written to S3 by oxjob #2507's "
        "build_a4s_feed.py; ready_to_run stays false so the harvester never calls it.")
RECEIPT_PATH = os.path.join(HERE, "oxjob2507-agents4science-source-endpoint-2026-11-22-receipt.json")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "mode": "revert" if REVERT else "apply", "job": JOB, "before": {}, "actions": [], "after": {}}


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
    named = rows(conn, "SELECT id, display_name, type FROM sources "
                       "WHERE display_name ILIKE '%agents4science%' OR display_name ILIKE '%ai agents for science%' "
                       "OR alternate_titles::text ILIKE '%agents4science%' "
                       "OR alternate_titles::text ILIKE '%ai agents for science%'")
    receipt["before"]["sources_naming_it"] = named
    assert not named, f"a source already names the conference: {named}; look first"
    on_host = rows(conn, "SELECT id, pmh_url, source_id, status FROM oai_pmh_endpoint "
                         "WHERE pmh_url ILIKE '%openreview.net%' OR pmh_url ILIKE '%agents4science%'")
    receipt["before"]["endpoints_on_hosts"] = on_host
    assert not on_host, f"an endpoint already names openreview.net/agents4science: {on_host}; look first"
    assert not rows(conn, "SELECT id FROM oai_pmh_endpoint WHERE id = :e", e=ENDPOINT), "endpoint id taken"
    assert not rows(conn, "SELECT endpoint_id FROM endpoint_deletion_audit WHERE endpoint_id = :e", e=ENDPOINT)

    ctx = MatchContext(conn)
    sid = mint_source(conn, ctx, NAME, source_type="conference", homepage_url=HOMEPAGE)
    conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                 {"a": json.dumps(ALT_TITLES), "id": sid})
    receipt["source_id"] = sid
    log(f"MINTED S{sid} '{NAME}' (conference, alternate title {ALT_TITLES}, homepage {HOMEPAGE})")

    conn.execute(text(
        "INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
        "source_id, is_journal_host, green_scrape, harvest_identify_response, rand) "
        "VALUES (:id, :u, NULL, 'oai_dc', false, true, 'active', :sid, true, false, :note, random())"),
        {"id": ENDPOINT, "u": PMH_URL, "sid": sid, "note": NOTE})
    log(f"REGISTERED endpoint {ENDPOINT} {PMH_URL} -> S{sid} is_journal_host, ready_to_run false (files, not harvest)")

    receipt["after"]["source"] = rows(conn, "SELECT id, display_name, type, homepage_url, alternate_titles, is_oa "
                                            "FROM sources WHERE id = :id", id=sid)
    receipt["after"]["endpoint"] = rows(conn, "SELECT id, pmh_url, source_id, ready_to_run, in_walden, status, "
                                              "is_journal_host, green_scrape FROM oai_pmh_endpoint WHERE id = :e",
                                        e=ENDPOINT)
    assert len(receipt["after"]["source"]) == 1 and len(receipt["after"]["endpoint"]) == 1


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    sid = r["source_id"]
    n = conn.execute(text("DELETE FROM oai_pmh_endpoint WHERE id = :e AND source_id = :s"),
                     {"e": ENDPOINT, "s": sid}).rowcount
    assert n == 1, f"guard: expected 1 endpoint row deleted, got {n}"
    log(f"deleted endpoint {ENDPOINT}")
    others = rows(conn, "SELECT id FROM oai_pmh_endpoint WHERE source_id = :s", s=sid)
    merged = rows(conn, "SELECT loser_id FROM source_merge WHERE winner_id = :s OR loser_id = :s", s=sid)
    assert not others and not merged, f"S{sid} gained links since the mint ({others}, {merged}); look first"
    n = conn.execute(text("DELETE FROM sources WHERE id = :s AND display_name = :n"), {"s": sid, "n": NAME}).rowcount
    assert n == 1, f"guard: expected 1 source row deleted, got {n}"
    log(f"deleted source S{sid}")


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
        log("DRY RUN - rolled back (the source id above is not reserved)")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
