"""CEUR-WS.org proceedings get a feed endpoint on CEUR Workshop Proceedings S4210204615 (oxjob #1496; ZD 24867).

Why: CEUR Workshop Proceedings (ceur-ws.org, ISSN 1613-0073) has published ~4,300 workshop and conference volumes
since 1995 with no DOIs (DataCite DOIs, 10.83095, only since Oct 2026), so OpenAlex holds about 1,300 of its ~70,000
papers. CEUR has no OAI-PMH server; each volume's index page marks every paper up (ceur-make's CEURTITLE / CEURAUTHOR
classes) with a free PDF. The oxjob's work/ceur/build_ceur_feed.py turns those into oai_dc records in the harvester's
own file format, written to s3://openalex-ingest/repositories/<endpoint>/, where walden's Repo ingest reads every
endpoint's files (the route #2097 built for TMLR, #2100 used for ICLR and NeurIPS and #1496 used for PMLR). This row
binds the records to the CEUR source. The records state conferenceObject + publishedVersion, so each lands as a
published-version location and leads its work (one work, published copy primary: #1099). is_journal_host is set
because the endpoint is the venue's own host (#1409).

What it does, in ONE transaction (dry run by default, rolled back):
  1. Guards: the source exists unmerged with the expected name and type; no endpoint mentions ceur-ws.org; the endpoint
     id is unused (and not in endpoint_deletion_audit). Endpoints already on the source are recorded, not changed.
  2. INSERT one oai_pmh_endpoint: pmh_url https://ceur-ws.org/ (the site the records come from; NOT an OAI-PMH server;
     pmh_set ceur-ws only labels the row), ready_to_run false (the harvester never calls it: the builder writes the
     files), in_walden true, status active, is_journal_host true, green_scrape false (the records carry their PDF).

Run on desk from the openalex-sources checkout:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1496-ceur-feed-endpoint-2026-11-24.py
    ... the same with --execute
Revert: ... --revert [--execute] = DELETE FROM oai_pmh_endpoint WHERE id = '<the id>' AND pmh_url =
'https://ceur-ws.org/'. Records already ingested stay until their native ids (oai:ceur-ws.org:Vol-<n>/<paper>) are
stub-deleted through openalex.repo.repo_works_backfill (#881 delete path).
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
JOB = 1496
PMH_URL = "https://ceur-ws.org/"
# series: (endpoint id, source id, expected display_name (lowercased), expected type)
ENDPOINTS = {
    "CEUR-WS": ("klmqstttv7fooh1stjig", 4210204615, "ceur workshop proceedings", "journal"),
}
NOTE = ("No OAI-PMH server: this is CEUR-WS.org's site (series {series}). Records are written to S3 by oxjob "
        "#1496's work/ceur/build_ceur_feed.py from each volume's index page; ready_to_run stays false so the harvester "
        "never calls it.")
RECEIPT_PATH = os.path.join(HERE, "oxjob1496-ceur-feed-endpoint-2026-11-24-receipt.json")

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
    on_host = rows(conn, "SELECT id, pmh_url, pmh_set, source_id, status FROM oai_pmh_endpoint "
                         "WHERE pmh_url ILIKE '%ceur-ws%'")
    receipt["before"]["endpoints_on_host"] = on_host
    assert not on_host, f"an endpoint already names ceur-ws: {on_host}; look first"
    ids = [e[0] for e in ENDPOINTS.values()]
    assert len(set(ids)) == len(ids)
    taken = rows(conn, "SELECT id FROM oai_pmh_endpoint WHERE id = ANY(:ids)", ids=ids)
    assert not taken, f"endpoint id(s) taken: {taken}"
    assert not rows(conn, "SELECT endpoint_id FROM endpoint_deletion_audit WHERE endpoint_id = ANY(:ids)", ids=ids)
    for series, (ep, sid, name, typ) in ENDPOINTS.items():
        src = rows(conn, "SELECT id, display_name, type, homepage_url, is_oa FROM sources WHERE id = :id",
                   id=sid)
        assert len(src) == 1, f"S{sid} not found"
        s = src[0]
        receipt["before"][series] = {"source": s, "endpoints_on_source": rows(
            conn, "SELECT id, pmh_url, pmh_set, status, is_journal_host FROM oai_pmh_endpoint WHERE source_id = :id",
            id=sid)}
        assert (s["display_name"] or "").strip().lower() == name, (series, s["display_name"])
        assert s["type"] == typ, (series, s["type"])
        assert not rows(conn, "SELECT winner_id FROM source_merge WHERE loser_id = :id", id=sid), (series, "merged")
        conn.execute(text(
            "INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
            "source_id, is_journal_host, green_scrape, harvest_identify_response, rand) "
            "VALUES (:id, :u, :set, 'oai_dc', false, true, 'active', :sid, true, false, :note, random())"),
            {"id": ep, "u": PMH_URL, "set": series.lower(), "sid": sid, "note": NOTE.format(series=series)})
        log(f"REGISTERED endpoint {ep} {PMH_URL} set {series.lower()} -> S{sid} ({series}) is_journal_host, "
            f"ready_to_run false (files, not harvest)")
    receipt["revert"].append("DELETE FROM oai_pmh_endpoint WHERE id IN (%s) AND pmh_url = '%s';"
                             % (", ".join(f"'{i}'" for i in ids), PMH_URL))
    receipt["after"]["endpoints"] = rows(
        conn, "SELECT id, pmh_url, pmh_set, source_id, ready_to_run, in_walden, status, is_journal_host, green_scrape "
              "FROM oai_pmh_endpoint WHERE id = ANY(:ids) ORDER BY source_id", ids=ids)
    assert len(receipt["after"]["endpoints"]) == len(ids)


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    ids = [e[0] for e in ENDPOINTS.values()]
    n = conn.execute(text("DELETE FROM oai_pmh_endpoint WHERE id = ANY(:ids) AND pmh_url = :u"),
                     {"ids": ids, "u": PMH_URL}).rowcount
    assert n == len(ids), f"guard: expected {len(ids)} endpoint rows deleted, got {n}"
    log(f"deleted {n} endpoints")


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
