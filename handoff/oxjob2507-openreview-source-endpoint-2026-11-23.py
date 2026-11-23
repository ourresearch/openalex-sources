"""OpenReview gets a repository source and a plain feed endpoint for Agents4Science's rejected submissions (oxjob #2507).

Why: the Agents4Science 2025 feed (handoff oxjob2507-agents4science-source-endpoint-2026-11-22.py) put all its records
on the conference's own endpoint, flagged is_journal_host. Since oxjob #2494 (21 Nov) CreateWorksBase reads every record
on a conference's own endpoint as the published version from the publisher's host, so the 196 rejected submissions
went live on night 2026-11-23 as published at the conference (and typed article, from their submittedVersion tag).
They were never published there: OpenReview, the platform, hosts them as submitted manuscripts. This row gives
OpenReview a repository source (witness: its own site, https://openreview.net/ ; no registry source names it) and an
endpoint that is NOT a journal host. The feed re-sends the 196 records under it with the same OAI ids and a newer
datestamp, so repo_works updates them in place (keyed by native id): same work ids, now a submitted version on
OpenReview, typed preprint.

What it does, in ONE transaction (dry run by default, rolled back):
  1. Guards: no source names OpenReview; the only endpoint naming openreview.net is the conference's
     (ni7plyt3kadohy25hzva); the new endpoint id is unused.
  2. Mint source 'OpenReview' (type repository, homepage https://openreview.net/).
  3. INSERT oai_pmh_endpoint olz3gr672weg55vcdvj9: pmh_url https://openreview.net/ (NOT an OAI-PMH server),
     ready_to_run false, in_walden true, status active, is_journal_host FALSE, green_scrape false.

Run on desk from the openalex-sources checkout:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2507-openreview-source-endpoint-2026-11-23.py
    ... the same with --execute
Revert: ... --revert [--execute] = DELETE the endpoint row, then the source row (refused if anything else now points at
the source). Re-sending the 196 records under the conference endpoint puts them back (they'd be published-version again).
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
NAME = "OpenReview"
HOMEPAGE = "https://openreview.net/"
ENDPOINT = "olz3gr672weg55vcdvj9"
VENUE_ENDPOINT = "ni7plyt3kadohy25hzva"  # the conference's own endpoint (2026-11-22 handoff)
PMH_URL = "https://openreview.net/"
# The table has no free-text note column; harvest_identify_response says what an Identify call would: there is none.
NOTE = ("No OAI-PMH server: OpenReview's site. Submitted manuscripts a venue did not publish (Agents4Science 2025's "
        "rejected papers) are written to S3 by oxjob #2507's build_a4s_feed.py; ready_to_run stays false.")
RECEIPT_PATH = os.path.join(HERE, "oxjob2507-openreview-source-endpoint-2026-11-23-receipt.json")

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
    named = rows(conn, "SELECT id, display_name, type FROM sources WHERE display_name ILIKE '%openreview%' "
                       "OR alternate_titles::text ILIKE '%openreview%' OR homepage_url ILIKE '%openreview.net%'")
    receipt["before"]["sources_naming_it"] = named
    assert not named, f"a source already names OpenReview: {named}; look first"
    on_host = rows(conn, "SELECT id, pmh_url, source_id, status FROM oai_pmh_endpoint WHERE pmh_url ILIKE '%openreview.net%'")
    receipt["before"]["endpoints_on_host"] = on_host
    assert [e["id"] for e in on_host] == [VENUE_ENDPOINT], f"unexpected endpoints on openreview.net: {on_host}; look first"
    assert not rows(conn, "SELECT id FROM oai_pmh_endpoint WHERE id = :e", e=ENDPOINT), "endpoint id taken"
    assert not rows(conn, "SELECT endpoint_id FROM endpoint_deletion_audit WHERE endpoint_id = :e", e=ENDPOINT)

    ctx = MatchContext(conn)
    sid = mint_source(conn, ctx, NAME, source_type="repository", homepage_url=HOMEPAGE)
    receipt["source_id"] = sid
    log(f"MINTED S{sid} '{NAME}' (repository, homepage {HOMEPAGE})")

    conn.execute(text(
        "INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
        "source_id, is_journal_host, green_scrape, harvest_identify_response, rand) "
        "VALUES (:id, :u, NULL, 'oai_dc', false, true, 'active', :sid, false, false, :note, random())"),
        {"id": ENDPOINT, "u": PMH_URL, "sid": sid, "note": NOTE})
    log(f"REGISTERED endpoint {ENDPOINT} {PMH_URL} -> S{sid} not a journal host, ready_to_run false (files, not harvest)")

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
