"""PMLR proceedings get feed endpoints, one per target source (oxjob #1496; ZD 27223, 23504, 5719, 8619, 4335).

Why: Proceedings of Machine Learning Research (proceedings.mlr.press) publishes ICML, AISTATS, UAI, COLT, CoRL, ACML,
MIDL, L4DC, ALT and ~225 workshop and smaller-conference volumes (40,596 papers in 349 volumes, 22 Nov 2026) with no
DOIs, so OpenAlex holds most of these papers only as arXiv preprints (ICML 2022-2025: 15 works carry the ICML source).
PMLR publishes one citeproc.yaml per volume with every paper's metadata, abstract, landing page and free PDF. The
oxjob's build_pmlr_feed.py turns those into oai_dc records in the harvester's own file format, written to
s3://openalex-ingest/repositories/<endpoint>/, where walden's Repo ingest reads every endpoint's files (the route
#2097 built for TMLR and #2100 used for ICLR and NeurIPS). Each row below binds one endpoint's records to its source.
The records state conferenceObject + publishedVersion, so each lands as a published-version location with a PDF and
leads its work (one work, published copy primary: #1099). is_journal_host is set because the endpoint is the venue's own
host (#1409); since #2494 CreateWorksBase reads that flag for conference sources too (host_type publisher; the venue
copy's date outranks an arXiv DataCite date, #2097).

What it does, in ONE transaction (dry run by default, rolled back):
  1. Guards: each source exists unmerged with the expected name and type; no endpoint mentions mlr.press; the endpoint
     ids are unused (and not in endpoint_deletion_audit). Endpoints already on a source are recorded, not changed.
  2. INSERT one oai_pmh_endpoint per source: pmh_url https://proceedings.mlr.press/ (the site the records come from; NOT
     an OAI-PMH server; pmh_set pmlr-<series> only labels the row: the active (pmh_url, pmh_set) pair must be unique (migration
     032), the harvester never calls a ready_to_run false endpoint and walden does not read pmh_set), ready_to_run false (the harvester never calls it: the builder writes
     the files), in_walden true, status active, is_journal_host true, green_scrape false (the records carry their PDF).

Run on desk from the openalex-sources checkout:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1496-pmlr-feed-endpoints-2026-11-22.py
    ... the same with --execute
Revert: ... --revert [--execute] = DELETE FROM oai_pmh_endpoint WHERE id IN (<the ten ids>) AND pmh_url =
'https://proceedings.mlr.press/'. Records already ingested stay until their native ids (oai:proceedings.mlr.press:<vol>/<id>)
are stub-deleted through openalex.repo.repo_works_backfill (#881 delete path), as #2100's ICLR revert says.
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
PMH_URL = "https://proceedings.mlr.press/"
# series: (endpoint id, source id, expected display_name (lowercased), expected type)
ENDPOINTS = {
    "ICML": ("r1uwta3ljkswpwsrku7f", 4306419644, "international conference on machine learning", "conference"),
    "AISTATS": ("b6nv0li5q6holc0atjzf", 4306419146, "international conference on artificial intelligence and statistics", "conference"),
    "UAI": ("wq7uk9d11rt32ctzf69x", 4306421103, "uncertainty in artificial intelligence", "conference"),
    "COLT": ("pqqhhbydryq4jb76jhea", 4306418075, "conference on learning theory", "conference"),
    "CoRL": ("cuynjesiz6105yjkpevg", 4306506823, "conference on robot learning", "conference"),
    "ACML": ("v5j4ph7fyjprwszjw6eu", 4306417739, "asian conference on machine learning", "conference"),
    "MIDL": ("x61bqwp99zm9wn1yq564", 4306519517, "medical imaging with deep learning", "journal"),
    "L4DC": ("qkzec0risd8ba9ly7i3h", 4306518315, "learning for dynamics and control", "journal"),
    "ALT": ("bfpxjz97zd07bhffwj4n", 4306417654, "algorithmic learning theory", "conference"),
    "PMLR": ("yl687x2elee958uxdqp2", 4363608721, "proceedings of machine learning research", "conference"),
}
NOTE = ("No OAI-PMH server: this is PMLR's proceedings site (series {series}). Records are written to S3 by oxjob "
        "#1496's build_pmlr_feed.py from each volume's citeproc.yaml; ready_to_run stays false so the harvester never "
        "calls it.")
RECEIPT_PATH = os.path.join(HERE, "oxjob1496-pmlr-feed-endpoints-2026-11-22-receipt.json")

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
                         "WHERE pmh_url ILIKE '%mlr.press%'")
    receipt["before"]["endpoints_on_host"] = on_host
    assert not on_host, f"an endpoint already names mlr.press: {on_host}; look first"
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
            {"id": ep, "u": PMH_URL, "set": f"pmlr-{series.lower()}", "sid": sid, "note": NOTE.format(series=series)})
        log(f"REGISTERED endpoint {ep} {PMH_URL} set pmlr-{series.lower()} -> S{sid} ({series}) is_journal_host, "
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
