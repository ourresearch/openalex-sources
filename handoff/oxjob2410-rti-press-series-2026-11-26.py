"""RTI Press series sources for oxjob #2410 (ZD 5353; support factory #2254), written off desk 2026-11-26 (lane #2263).
Run on desk from the openalex-sources checkout, like #1225's oxjob1225-followup-2026-11-17.py (same helpers, same
receipt form).

WHY: RTI Press (RTI International's diamond open-access press; Crossref member 2090 'RTI Press', prefix 10.3768; ROR
02qfscv73 = institution I4396570550) publishes numbered series, each with its own ISSN. Its About page
(https://www.rti.org/rti-press/about, read 26 Nov 2026): "RTI Press publishes Research Briefs (ISSN 2378-7929), Policy
Briefs (ISSN 2378-7937), Research Reports (ISSN 2378-7902), Methods Reports (ISSN 2378-7813), Occasional Papers (ISSN
2378-7996), Conference Proceedings (ISSN 2690-0343), and books"; each series' own page repeats its ISSN. Only
Occasional Papers has a source (S7407060977, DOAJ). The other five have none, so ~214 RTI reports have no source; the
61 newer Crossref deposits that carry an ISSN carry the one their DOI's series code names (61/61), and the names below
are the container titles those deposits use. Witness rule: RULINGS § 3 D5 (the venue's own site and its deposits).

MINTS (all: publisher text 'RTI Press', country US, host institution I4396570550 'RTI Press' until the publisher record
exists; see --publisher-id):
  RTI Press Research Reports       journal     2378-7902  https://www.rti.org/rti-press-collection/research-reports
  RTI Press Methods Reports        journal     2378-7813  https://www.rti.org/rti-press-collection/methods-reports
  RTI Press Research Briefs        journal     2378-7929  https://www.rti.org/rti-press-collection/research-briefs
  RTI Press Policy Briefs          journal     2378-7937  https://www.rti.org/rti-press-collection/policy-briefs
  RTI Press Conference Proceedings conference  2690-0343  https://www.rti.org/rti-press-collection/conference-proceedings
  Type: ISSN-bearing report and brief series are typed journal across OpenAlex (RTI Press Occasional Papers itself, ETS
  Research Report Series, UN-DESA policy brief); a proceedings series is a conference venue (#2494, AIP Conf. Proc.).
EDIT: S7407060977 RTI Press Occasional Papers homepage_url NULL -> https://www.rti.org/rti-press-collection/occasional-papers
  (only while NULL).
PUBLISHER (optional, --publisher-id=<P id without the P>): after desk mints the 'RTI Press' publisher record in
  openalex.publishers.publishers (#2410 PLAN.md NEEDS DESK step 1; ROR 02qfscv73; ruling D12 + Casey's 6 Oct host rule
  #1563: a publisher twin by ROR -> publisher_id), set publisher_id on the five mints, S7407060977 and S7407145182 RTI
  Press eBooks, each only while its publisher_id is NULL. Sources' host_organization then reads the publisher (walden
  CreateSourcesApi: publisher_id wins over institution_id for non-repositories), and I4396570550 gains the publisher
  role through the shared ROR id. institution_id stays as it is.
OA: not set here (never source_oa_override by hand). After --execute, post one users-api curation per mint (entity
  sources, property is_oa, value 'true'; RTI's About page: "RTI Press is a Diamond Open Access publisher; neither
  authors nor readers pay"), before 20:00 CT so apply_source_curations (daily 02:00 UTC) carries it the same night:
  #2410 work/handoff/post_oa_curations.py.txt does it from this script's receipt.

Run on desk, outside the End 2 End window; dry run by default, ONE transaction:
    cp <this file> ~/ox/openalex-sources/handoff/oxjob2410-rti-press-series-2026-11-26.py && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2410-rti-press-series-2026-11-26.py [--publisher-id=N]
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2410-rti-press-series-2026-11-26.py [--publisher-id=N] --execute
Revert: delete the five minted sources (their source_issn rows first); homepage_url and publisher_id back to the
receipt's `before` values.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import MatchContext, mint_source, recompute_is_oa, recompute_listed_in  # noqa: E402

EXECUTE = "--execute" in sys.argv
PUBLISHER_ID = next((int(a.split("=", 1)[1]) for a in sys.argv[1:] if a.startswith("--publisher-id=")), None)
JOB = 2410
TICKET = 5353
INSTITUTION = 4396570550  # I4396570550 RTI Press (ROR 02qfscv73)
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); RULINGS § 3 D5 and D12; standing authority"
RECEIPT_PATH = os.path.join(HERE, "oxjob2410-rti-press-series-2026-11-26-receipt.json")
COLL = "https://www.rti.org/rti-press-collection/"

MINTS = [
    dict(type="journal", name="RTI Press Research Reports", issns=["2378-7902"], alt=["RTI Press Research Report Series"],
         homepage=COLL + "research-reports"),
    dict(type="journal", name="RTI Press Methods Reports", issns=["2378-7813"], alt=["RTI Press Methods Report Series"],
         homepage=COLL + "methods-reports"),
    dict(type="journal", name="RTI Press Research Briefs", issns=["2378-7929"], alt=["RTI Press Research Brief Series"],
         homepage=COLL + "research-briefs"),
    dict(type="journal", name="RTI Press Policy Briefs", issns=["2378-7937"], alt=[], homepage=COLL + "policy-briefs"),
    dict(type="conference", name="RTI Press Conference Proceedings", issns=["2690-0343"], alt=[],
         homepage=COLL + "conference-proceedings"),
]
OCCASIONAL = 7407060977  # S7407060977 RTI Press Occasional Papers (2378-7996)
EBOOKS = 7407145182  # S7407145182 RTI Press eBooks (#1225)
OCCASIONAL_HOME = COLL + "occasional-papers"

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "ticket": TICKET,
           "approved": APPROVED, "publisher_id": PUBLISHER_ID, "before": {}, "actions": [], "minted": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, sid):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = :i"), {"i": sid}).mappings():
        receipt["before"][str(sid)] = {k: (v if isinstance(v, (list, dict, bool, int, type(None))) else str(v))
                                       for k, v in r.items()}
        return r
    return None


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [row.display_name or ""]}
    gained = [t.strip() for t in titles if t and t.strip() and t.strip().lower() not in have]
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "id": sid})
        log(f"alternate_titles S{sid} += {gained}")


def main():
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            for sid in (OCCASIONAL, EBOOKS):
                r = snapshot(conn, sid)
                # not merged away: the live API resolves both ids to themselves (checked 26 Nov 2026 09:12 CT)
                assert r is not None, f"S{sid} missing"
                assert r["institution_id"] == INSTITUTION, f"S{sid} host is {r['institution_id']}, expected {INSTITUTION}"
            minted = []
            for m in MINTS:
                owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                     {"i": m["issns"]}).fetchall()
                assert not owned, f"{m['name']}: ISSN already on a source {owned}: edit that source instead"
                same = conn.execute(text("SELECT id, display_name FROM sources WHERE "
                                         "lower(regexp_replace(display_name, '[^[:alnum:]]', '', 'g')) = "
                                         "lower(regexp_replace(:n, '[^[:alnum:]]', '', 'g'))"), {"n": m["name"]}).fetchall()
                assert not same, f"{m['name']}: a source with the same normalized name exists {same}: edit it instead"
                sid = mint_source(conn, ctx, m["name"], source_type=m["type"], issns=m["issns"], publisher="RTI Press",
                                  homepage_url=m["homepage"], country_code="US", country="United States")
                add_alt_titles(conn, sid, m["alt"])
                conn.execute(text("UPDATE sources SET institution_id = :i, updated_date = now() "
                                  "WHERE id = :id AND institution_id IS NULL"), {"i": INSTITUTION, "id": sid})
                got = conn.execute(text("SELECT type, issn_l, institution_id FROM sources WHERE id = :id"),
                                   {"id": sid}).fetchone()
                assert got.type == m["type"] and got.issn_l == m["issns"][0] and got.institution_id == INSTITUTION, \
                    f"S{sid} came out {tuple(got)}"
                minted.append(sid)
                receipt["minted"].append({"source_id": sid, "name": m["name"], "type": m["type"], "issns": m["issns"],
                                          "homepage_url": m["homepage"]})
                log(f"MINTED S{sid} {m['type']} {m['name']!r} {m['issns']} host I{INSTITUTION} (ZD {TICKET})")
            n = conn.execute(text("UPDATE sources SET homepage_url = :u, updated_date = now() "
                                  "WHERE id = :id AND homepage_url IS NULL"), {"u": OCCASIONAL_HOME, "id": OCCASIONAL}).rowcount
            log(f"S{OCCASIONAL} homepage_url NULL -> {OCCASIONAL_HOME}: {n} row(s)")
            if PUBLISHER_ID:
                for sid in minted + [OCCASIONAL, EBOOKS]:
                    n = conn.execute(text("UPDATE sources SET publisher_id = :p, updated_date = now() "
                                          "WHERE id = :id AND publisher_id IS NULL"), {"p": PUBLISHER_ID, "id": sid}).rowcount
                    log(f"S{sid} publisher_id NULL -> {PUBLISHER_ID}: {n} row(s)")
            else:
                log("no --publisher-id: publisher_id left NULL (host stays institution I4396570550)")
            log(f"recompute_is_oa: {recompute_is_oa(conn)} source(s) changed")
            log(f"recompute_listed_in: {recompute_listed_in(conn)}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
        json.dump(receipt, open(RECEIPT_PATH, "w"), ensure_ascii=False, indent=1, default=str)
        print("receipt:", RECEIPT_PATH)
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
