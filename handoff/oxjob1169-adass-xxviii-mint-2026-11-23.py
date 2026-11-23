"""Mint 'Astronomical Data Analysis Software and Systems XXVIII' (ASP Conference Series vol. 523, 2019) for
Zendesk 27219 (oxjob #1169, support factory #2254), under card D5's ruling (RULINGS.md § 3 Coverage, 16 Nov 2026):
add a venue once a party outside OpenAlex confirms it exists (its own publisher's site is ideal).

Why: MAG filed ASP vols 522 (ADASS XXVII, 2020) and 523 (ADASS XXVIII, 2019) under one source, S4306503611
'Astronomical Data Analysis Software and Systems XXVII'. No XXVIII source exists (API name search, 23 Nov). The ticket's
work W3170673267 (ASP 523, p. 59) and W3024383460 (the XXVIII volume itself) sit on the XXVII source. After this mint,
oxjob #1169 moves the 100 vol-523 locations with `locations` `source_id` curations (#2173), built from the live API:
ADS bibcode 2019ASPC..523 on the location, or (no ASPC bibcode) work biblio.volume 523
(#1169 work/adass_xxviii_curations_planned_2026-11-23.jsonl).

Witness (read from desk 23 Nov 2026 10:45 CT):
  - The publisher's own site, aspbooks.org (Astronomical Society of the Pacific): volume listing names vol. 523
    'Astronomical Data Analysis Software and Systems XXVIII' (table of contents book_id=594), next to vol. 522 'XXVII'
    (book_id=585); paper page http://aspbooks.org/custom/publications/paper/523-0059.html: 'Volume: 523, Astronomical
    Data Analysis Software and Systems XXVIII'.
  - NASA ADS bibcodes 2019ASPC..523.* on 93 of the locations.
Host: P4322696608 'Astronomical Society of the Pacific' (live publisher). Type conference, as S4306503611. No ISSN
(the series ISSN belongs to the series, not the volume). is_oa stays derived.

Run on desk from the openalex-sources checkout (rehearsal by default, ONE transaction, rolled back):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1169-adass-xxviii-mint-2026-11-23.py
    ... the same with --execute
Revert: ... --revert [--execute] = DELETE the minted source row (refused if anything else now points at it). Before
reverting, post `source_id` curations moving the 100 locations back to S4306503611 (each row's `before`).
Live after the next sources snapshot + Sync Sources (the source is 404 in the API until then).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import MatchContext, mint_source, normalize_name  # noqa: E402

EXECUTE = "--execute" in sys.argv
REVERT = "--revert" in sys.argv
JOB = 1169
NAME = "Astronomical Data Analysis Software and Systems XXVIII"
ALT = ["ADASS XXVIII", "ASP Conference Series vol. 523"]
HOMEPAGE = "http://aspbooks.org/a/volumes/table_of_contents/?book_id=594"
PUBLISHER = "Astronomical Society of the Pacific"
PUBLISHER_ID = 4322696608
SIBLING = 4306503611  # 'Astronomical Data Analysis Software and Systems XXVII' (MAG-era, mixes vols 522 and 523)
RECEIPT_PATH = os.path.join(HERE, "oxjob1169-adass-xxviii-mint-2026-11-23-receipt.json")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "mode": "revert" if REVERT else "apply", "job": JOB, "ticket": 27219,
           "approved": "card D5 ruling (oxjob #2254 RULINGS, 16 Nov 2026); standing authority (reversible)",
           "before": {}, "actions": [], "after": {}}


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
    named = rows(conn, "SELECT id, display_name, type FROM sources WHERE display_name ILIKE '%Analysis Software and Systems XXVIII%' "
                       "OR alternate_titles::text ILIKE '%Analysis Software and Systems XXVIII%' "
                       "OR alternate_titles::text ILIKE '%ADASS XXVIII%' OR homepage_url ILIKE '%book_id=594%'")
    receipt["before"]["sources_naming_it"] = named
    assert not named, f"a source already names ADASS XXVIII: {named}; bind there"
    ctx = MatchContext(conn, name_link=True)
    twins = sorted({t for n in [NAME] + ALT for t, _ in ctx.name_index.get(normalize_name(n), [])})
    assert not twins, f"namesake(s) {twins}: look first"
    sib = rows(conn, "SELECT id, display_name, type, publisher_id FROM sources WHERE id = :i", i=SIBLING)
    receipt["before"]["sibling"] = sib
    assert sib and sib[0]["display_name"] == "Astronomical Data Analysis Software and Systems XXVII", sib

    sid = mint_source(conn, ctx, NAME, source_type="conference", publisher=PUBLISHER, homepage_url=HOMEPAGE,
                      country_code="US", country="United States")
    conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), publisher_id = :p WHERE id = :id"),
                 {"a": json.dumps(ALT), "p": PUBLISHER_ID, "id": sid})
    receipt["source_id"] = sid
    log(f"MINTED S{sid} conference {NAME!r} alt {ALT} host P{PUBLISHER_ID} homepage {HOMEPAGE} (ZD 27219)")
    receipt["after"]["source"] = rows(conn, "SELECT id, display_name, type, publisher, publisher_id, homepage_url, "
                                            "alternate_titles, country_code, is_oa FROM sources WHERE id = :id", id=sid)
    assert len(receipt["after"]["source"]) == 1


def revert(conn):
    r = json.load(open(RECEIPT_PATH))
    assert r.get("mode") == "apply" and r.get("execute"), "receipt is not an executed apply"
    sid = r["source_id"]
    eps = rows(conn, "SELECT id FROM oai_pmh_endpoint WHERE source_id = :s", s=sid)
    merged = rows(conn, "SELECT loser_id FROM source_merge WHERE winner_id = :s OR loser_id = :s", s=sid)
    issns = rows(conn, "SELECT issn FROM source_issn WHERE source_id = :s", s=sid)
    assert not eps and not merged and not issns, f"S{sid} gained links since the mint ({eps}, {merged}, {issns}); look first"
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
