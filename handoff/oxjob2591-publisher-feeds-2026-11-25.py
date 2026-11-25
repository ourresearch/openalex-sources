"""Dove Medical Press' and IntechOpen's own publisher feeds stop posing as repositories (oxjob #2591 phase 2, data-bugs).
Written by the #2591 session on desk 2026-11-25 from #2572's EDP script (oxjobs working/edp-sciences-feed-on-chiba-repository/
work/oxjob2572_edp_feed_registry.py, route verified live after night 2026-11-25) and #2591's phase 1 rename guards.

Why: two publishers' own OAI feeds are registered as repositories, so every article or chapter they publish carries a
second, fake 'repository copy' labelled submittedVersion beside the publisher's Crossref record of the same item:
  Dove Medical Press   endpoint 6721e13cc293496fb3c (dovepress.com/oai.php, Identify 'Dove Press OAI Repository'), bound to
                       S4306401012 'Dove Medical Press (Taylor and Francis Group)', typed repository: 117,395 visible works.
  IntechOpen           endpoint a0895c2844d0f4e6910 (intechopen.com/oai/index.php, Identify 'IntechOpen'), bound to
                       S4306401291, named after ONE chapter ('Contact-less Assessment of In-vivo Body Signals Using
                       Microwave Doppler Radar (InTech)') and hosted by I4210154139 'Intech (Slovakia)', a Slovak company
                       with 2 works: 54,619 visible works credit their copy to that chapter title and that company.
Same class as #2572 (EDP Sciences' feed on Chiba Institute of Technology), whose route this follows.

The walden side (same night, see the oxjob): feed records that duplicate their publisher's Crossref record and add
nothing are dropped (openalex.works.location_denylist, endpoint tag 'oxjob2591_publisher_feeds_p2'); records carrying
something their work has nowhere else (PDF link, harvested fulltext, citation edges, abstract) whose Crossref copy sits on
a journal move to that journal (openalex.sources.article_page_journal_sources, platforms 'dove' / 'intechopen'); the
rest stay on the feed's source.

What this script does, in ONE transaction (dry run by default, rolled back):
  1. Guards: each endpoint still has its URL, is bound to its source, active, is_journal_host false, and is the only
     endpoint on that source; the IntechOpen source still carries the chapter name, institution 4210154139, no ISSN, no
     DataCite client, no publisher record.
  2. Endpoints: is_journal_host true, so the records walden moves to their journal read as the publisher's own copy
     (CreateWorksBase gives host_type 'publisher' only to a journal-host endpoint's record on a journal or conference
     source, so the records left on the feed's repository source are unchanged). The endpoints stay active: retiring
     them would stop what only the feeds carry for new items (IntechOpen: unique citation edges on ~25% of its own
     records), so that is a separate decision after measuring recent items; new items keep arriving as today.
  3. Source 4306401291: named 'IntechOpen OAI-PMH repository', publisher IntechOpen (P4310322558), country GB, homepage
     https://www.intechopen.com, institution NULL (Intech (Slovakia) hosts nothing here). It keeps the ~22K records no drop
     or move takes (chapters whose Crossref copy sits on an ebook-platform source and that carry something only they have,
     and feed-minted twins of chapter works, a merge lead).
  Source 4306401012 (Dove) is left as it is: it names the right publisher and hosts no institution.

Run on desk from the openalex-sources checkout:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2591-publisher-feeds-2026-11-25.py [--execute]
Receipt: with --execute written next to this file as oxjob2591-publisher-feeds-2026-11-25-receipt.json (a pending copy is
written before the commit).

Revert:
    ... run_registry.py handoff/oxjob2591-publisher-feeds-2026-11-25.py --revert [--execute]
  = each endpoint's is_journal_host back to its before-value, and the source's name / publisher / publisher_id /
    country / homepage / institution back to the before-row, each only where the row still holds exactly what this
    script wrote (every written field compared); otherwise it stops and writes nothing. If the apply died after its
    commit and before writing the final receipt, revert reads the pending receipt (written, with the values to be
    written, before the commit), so 'did it commit?' is answered by the database, never by the exit code.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.expanduser("~/ox/openalex-sources"))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402

ARGS = set(sys.argv[1:])
assert ARGS <= {"--execute", "--revert"}, f"unknown arguments: {ARGS - {'--execute', '--revert'}}"
EXECUTE = "--execute" in ARGS
REVERT = "--revert" in ARGS
JOB = 2591
APPROVED = ("oxjob #2591 phase 2 under standing authority (data-bugs charter rule 3: reversible registry edit; route of "
            "#2572 verified live after night 2026-11-25; selection v4 after outside review; bar on fresh random draws; "
            "guards 0 contributor dropped, 0 stranded works)")
ENDPOINTS = {
    "6721e13cc293496fb3c": {"url": "http://dovepress.com/oai.php", "source": 4306401012, "label": "Dove Medical Press"},
    "a0895c2844d0f4e6910": {"url": "http://intechopen.com/oai/index.php", "source": 4306401291, "label": "IntechOpen"},
}
SOURCE = 4306401291
OLD = {"display_name": "Contact-less Assessment of In-vivo Body Signals Using Microwave Doppler Radar (InTech)",
       "publisher": "InTech", "publisher_id": None, "institution_id": 4210154139, "homepage_url": None, "country_code": None}
NEW = {"display_name": "IntechOpen OAI-PMH repository", "publisher": "IntechOpen", "publisher_id": 4310322558,
       "institution_id": None, "homepage_url": "https://www.intechopen.com", "country_code": "GB"}
RECEIPT_PATH = os.path.join(HERE, "oxjob2591-publisher-feeds-2026-11-25-receipt.json")
PENDING_PATH = RECEIPT_PATH.replace("-receipt.json", "-pending.json")
EP_COLS = "id, pmh_url, pmh_set, source_id, status, retirement_reason, retired_at, ready_to_run, in_walden, is_journal_host"
SRC_COLS = ("id, display_name, type, issn_l, issns, datacite_ids, publisher, publisher_id, institution_id, homepage_url, "
            "country_code, is_oa, alternate_titles")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "mode": "revert" if REVERT else "apply", "job": JOB, "approved_by": APPROVED,
           "before": {}, "actions": [], "after": {}, "revert": []}


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


def is_true(v):
    return v in (True, "True", "true")


def apply(conn):
    for ep, spec in ENDPOINTS.items():
        e = rows(conn, f"SELECT {EP_COLS} FROM oai_pmh_endpoint WHERE id = :id FOR UPDATE", id=ep)
        assert len(e) == 1, f"endpoint {ep} not found"
        e = e[0]
        assert (e["pmh_url"] or "").rstrip("/") == spec["url"], (ep, e["pmh_url"])
        assert int(e["source_id"]) == spec["source"], (ep, e["source_id"])
        assert not e["pmh_set"], (ep, "has a set: stop, look")
        assert e["status"] == "active", f"{ep} status {e['status']}: already retired? nothing written"
        assert not is_true(e["is_journal_host"]), f"{ep} already is_journal_host: stop, look"
        bound = [r["id"] for r in rows(conn, "SELECT id FROM oai_pmh_endpoint WHERE source_id = :s ORDER BY id", s=spec["source"])]
        assert bound == [ep], f"other endpoints on S{spec['source']}: {bound}"
        receipt["before"][ep] = e

    s = rows(conn, f"SELECT {SRC_COLS} FROM sources WHERE id = :id FOR UPDATE", id=SOURCE)
    assert len(s) == 1, "IntechOpen source not found (merged?)"
    s = s[0]
    receipt["before"]["source"] = s
    for col, v in OLD.items():
        got = s[col] if s[col] is None else (int(s[col]) if col.endswith("_id") else s[col])
        assert got == v, (col, s[col])
    assert s["type"] == "repository", s["type"]
    assert not s["issn_l"] and not s["issns"], "has an ISSN: stop, look"
    assert not s["datacite_ids"], "has a DataCite client: stop, look"
    assert not (s["alternate_titles"] or []), ("alternate titles of another entity", s["alternate_titles"])
    clash = rows(conn, "SELECT id, display_name FROM sources WHERE lower(display_name) = lower(:n) AND id <> :id",
                 n=NEW["display_name"], id=SOURCE)
    assert not clash, f"a source already carries the new name: {clash}"
    # publisher_id 4310322558: the registry holds no publishers table; checked live 25 Nov (api.openalex.org/publishers/P4310322558 = IntechOpen)
    # pending receipt on disk before anything is written: before-rows AND the exact values this run will write, so a
    # revert works even if the process dies after the commit but before the final receipt (revert() falls back to it)
    receipt["expected_after"] = {"endpoints": {ep: {"source_id": spec["source"], "pmh_url": receipt["before"][ep]["pmh_url"],
                                                    "is_journal_host": True} for ep, spec in ENDPOINTS.items()},
                                 "source": {c: v for c, v in NEW.items()}}
    if EXECUTE:
        with open(PENDING_PATH, "w") as fh:
            json.dump(receipt, fh, indent=1, default=str)
            fh.flush()
            os.fsync(fh.fileno())

    for ep, spec in ENDPOINTS.items():
        n = conn.execute(text(
            "UPDATE oai_pmh_endpoint SET is_journal_host = true "
            "WHERE id = :id AND source_id = :src AND status = 'active' AND coalesce(is_journal_host, false) = false"),
            {"id": ep, "src": spec["source"]}).rowcount
        assert n == 1, f"guard: expected 1 endpoint row for {ep}, got {n}"
        log(f"endpoint {ep} ({spec['url']}, {spec['label']}): is_journal_host true (was false); stays active")

    sets = ", ".join(f"{c} = :{c}" for c in NEW)
    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id AND display_name = :old "
                          "AND institution_id = :inst AND publisher_id IS NULL AND type = 'repository'"),
                     dict(NEW, id=SOURCE, old=OLD["display_name"], inst=OLD["institution_id"])).rowcount
    assert n == 1, f"guard: expected 1 source row, got {n}"
    log(f"source S{SOURCE}: '{OLD['display_name']}' (I{OLD['institution_id']}) -> '{NEW['display_name']}' "
        f"(publisher P{NEW['publisher_id']} IntechOpen, GB, no institution)")

    receipt["after"]["endpoints"] = rows(conn, f"SELECT {EP_COLS} FROM oai_pmh_endpoint WHERE id = ANY(:ids) ORDER BY id",
                                         ids=list(ENDPOINTS))
    receipt["after"]["source"] = rows(conn, f"SELECT {SRC_COLS} FROM sources WHERE id = :id", id=SOURCE)[0]
    receipt["revert"].append("run this script with --revert (restores each before-row where the row still holds what was written)")


def revert(conn):
    # the final receipt if the apply finished writing it, else the pending one (written before the apply's commit);
    # either way the database must hold exactly the values the apply wrote, field by field, or nothing is changed
    path = RECEIPT_PATH if os.path.exists(RECEIPT_PATH) else PENDING_PATH
    with open(path) as fh:
        r = json.load(fh)
    assert r.get("mode") == "apply" and r.get("execute"), f"{path} is not an executed apply: nothing to revert"
    exp = r["expected_after"]
    receipt["before"]["applied_receipt"] = {"path": path, "run_at": r["run_at"]}
    for ep in ENDPOINTS:
        now = rows(conn, f"SELECT {EP_COLS} FROM oai_pmh_endpoint WHERE id = :id FOR UPDATE", id=ep)[0]
        for col, v in exp["endpoints"][ep].items():
            got = is_true(now[col]) if col == "is_journal_host" else now[col]
            assert got == v, (ep, col, "not what the apply wrote (never committed, or changed since): stop, look", now[col])
        b = r["before"][ep]["is_journal_host"]
        before = None if b is None else is_true(b)
        n = conn.execute(text("UPDATE oai_pmh_endpoint SET is_journal_host = :v WHERE id = :id AND is_journal_host = true"),
                         {"v": before, "id": ep}).rowcount
        assert n == 1, (ep, n)
        log(f"endpoint {ep}: is_journal_host back to {before}")
    now = rows(conn, f"SELECT {SRC_COLS} FROM sources WHERE id = :id FOR UPDATE", id=SOURCE)[0]
    for col, v in exp["source"].items():
        got = now[col] if now[col] is None else (int(now[col]) if col.endswith("_id") else now[col])
        assert got == v, (col, "not what the apply wrote (never committed, or changed since): stop, look", now[col])
    b = r["before"]["source"]
    vals = {c: (int(b[c]) if c.endswith("_id") and b[c] is not None else b[c]) for c in NEW}
    sets = ", ".join(f"{c} = :{c}" for c in NEW)
    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id AND display_name = :cur"),
                     dict(vals, id=SOURCE, cur=NEW["display_name"])).rowcount
    assert n == 1, n
    log(f"source S{SOURCE}: back to '{b['display_name']}' (I{b['institution_id']})")


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
        with open(path + ".tmp", "w") as fh:
            json.dump(receipt, fh, ensure_ascii=False, indent=1, default=str)
        os.replace(path + ".tmp", path)
        if not REVERT and os.path.exists(PENDING_PATH):
            os.remove(PENDING_PATH)
        print("receipt:", path)
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
