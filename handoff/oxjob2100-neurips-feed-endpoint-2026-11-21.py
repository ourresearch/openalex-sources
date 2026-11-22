"""NeurIPS gets a feed endpoint for its own proceedings (oxjob #2100, step 7; ZD 7541).

Why: NeurIPS (S4363606243, the conference source 'neural information processing systems', ISSN 1049-5258) minted no
DOIs before NeurIPS 2022, so for 1987-2021 OpenAlex holds only what MAG had (papers.nips.cc PDF links, no version) and
the papers' arXiv preprints: W2626778328 ('Attention Is All You Need', NeurIPS 2017) is a 2017 arXiv preprint with no
NeurIPS copy (ZD 7541). NeurIPS's own site, https://proceedings.neurips.cc/ , lists every paper with an abstract page
and a free PDF (13,912 for 1987-2021). The oxjob's build_neurips_feed.py turns those pages into oai_dc records in the
harvester's own file format, written to s3://openalex-ingest/repositories/<this endpoint>/, where walden's Repo ingest
reads every endpoint's files (the route #2097 built for TMLR and step 2 used for ICLR, endpoint j804cxp8hy6htjgkk8hy).
This row binds those records to S4363606243. The records state conferenceObject + publishedVersion, so each lands as a
published-version location with a PDF and leads its work (one work, published copy primary: #1099). is_journal_host is
set because the endpoint is the venue's own host (#1409); since #2494 (walden 26c57f6c) CreateWorksBase reads that flag
for conference sources too, so these copies read host_type publisher and their date outranks an arXiv DataCite date
(#2097's preprint_outranked_publication_dates).

What it does, in ONE transaction (dry run by default, rolled back):
  1. Guards: S4363606243 is the conference 'neural information processing systems'; no endpoint is bound to it yet;
     no endpoint mentions neurips.cc or nips.cc; the endpoint id is unused.
  2. INSERT oai_pmh_endpoint eijnazg474c9pb6u2fac: pmh_url https://proceedings.neurips.cc/ (the site the records come
     from; NOT an OAI-PMH server), ready_to_run false (the harvester never calls it: the files are written by the
     builder), in_walden true, status active, is_journal_host true, green_scrape false (the records carry their PDF).
  3. homepage_url https://neurips.cc/ on S4363606243 when it is NULL (it is NULL on the API, 21 Nov 2026).

Run on desk from the openalex-sources checkout (copy this file to handoff/ first):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2100-neurips-feed-endpoint-2026-11-21.py
    ... the same with --execute
The receipt prints after RECEIPT and, with --execute, is written next to this file.

Revert: ... run_registry.py handoff/oxjob2100-neurips-feed-endpoint-2026-11-21.py --revert [--execute]
  = DELETE FROM oai_pmh_endpoint WHERE id = 'eijnazg474c9pb6u2fac' AND source_id = 4363606243;
    UPDATE sources SET homepage_url = NULL WHERE id = 4363606243 AND homepage_url = 'https://neurips.cc/' (only if this
    script set it). Records already ingested stay until their native ids (oai:proceedings.neurips.cc:<year>/<hash>) are
    stub-deleted through openalex.repo.repo_works_backfill (#881 delete path), as PLAN.md step 2's revert says for ICLR.
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
SOURCE = 4363606243
SOURCE_NAME = "neural information processing systems"
ENDPOINT = "eijnazg474c9pb6u2fac"
PMH_URL = "https://proceedings.neurips.cc/"
HOMEPAGE = "https://neurips.cc/"
# The table has no free-text note column; harvest_identify_response says what an Identify call would: there is none.
NOTE = ("No OAI-PMH server: this is NeurIPS's proceedings site. Records are written to S3 by oxjob #2100's "
        "build_neurips_feed.py; ready_to_run stays false so the harvester never calls it.")
RECEIPT_PATH = os.path.join(HERE, "oxjob2100-neurips-feed-endpoint-2026-11-21-receipt.json")

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
                         "WHERE pmh_url ILIKE '%neurips.cc%' OR pmh_url ILIKE '%nips.cc%'")
    receipt["before"]["endpoints_on_hosts"] = on_host
    assert not on_host, f"an endpoint already names neurips.cc/nips.cc: {on_host}; look first"
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
