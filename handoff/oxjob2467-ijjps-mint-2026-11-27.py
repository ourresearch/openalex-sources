"""Mint International Journal of Justice and Police Sciences (ISSN 3108-0804, online) for Zendesk 28024 (oxjob #2467,
support factory #2254), under card D5's ruling (RULINGS.md § 3 Coverage, 16 Nov 2026): add a journal on no list we load
once a third party outside OpenAlex and the work confirms it exists; build its details from the venue's own site.

Witness (receipts: #2467 work/witness_ijjps_2026-11-27.md): the journal's own site https://www.justicepolicejournal.com/
(read 27 Nov 2026 06:20 CT): 'ISSN 3108-0804 (Online)' on every page; editorial board with names and affiliations
(EIC K. Jaishankar, IIJPS; editors at Ajman University, Griffith University; 35-member advisory board); 2 issues
(Vol. 1 No. 1 Jan-Jun 2025: 9 items; Vol. 1 No. 2 Jul-Dec 2025: 6 items), each article with its Zenodo DOI and PDF;
Highwire tags citation_journal_title / citation_issn 3108-0804 / citation_publisher 'International Institute of Justice
and Police Sciences'; footer 'Published by the International Institute of Justice and Police Sciences and Appa
Publications, Bengaluru, India'. Host: OpenAlex institution I4405253450 'International Institute of Justice and Police
Sciences' (ROR https://ror.org/00p56p671, IN), so institution_id is set (the 24 Nov Replication Research mint left the
host empty, #693). No OpenAlex source holds 3108-0804 (sources/issn:3108-0804 -> 404; name search 0, 27 Nov).
5 of the 15 works' DataCite records name ISSN 3108-0804 as IsPartOf (10 DOI locations), so walden's DataCite
container-ISSN tier moves them off Zenodo S4306400562 onto this source on the first night after the mint (as for
#2074's Cuadernos del MPD S7407122756, 17 Nov). The other 10 works get location source_id curations once it is live.
No OA override here: the is_oa curation follows once the source answers on the API (LANE.md: a journal's OA flag is a
curation).

Run on desk (rehearsal by default, one transaction):
    cd ~/ox/openalex-sources && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file> [--execute]
Revert: DELETE FROM source_issn WHERE source_id = <minted id>; DELETE FROM sources WHERE id = <minted id>;
(no endpoint, no merge, no override; the DataCite tier then leaves the 10 DOI locations on Zenodo again the next night).
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.getcwd())
from sqlalchemy import text

from db import engine
from sources_lib import MatchContext, mint_source, normalize_name, recompute_listed_in

EXECUTE = "--execute" in sys.argv
MINT = dict(ticket=28024, name="International Journal of Justice and Police Sciences", issns=["3108-0804"],
            type="journal", publisher="International Institute of Justice and Police Sciences",
            institution_id=4405253450, homepage="https://www.justicepolicejournal.com/", cc="IN", country="India",
            alt=["IJJPS", "International Journal of Justice & Police Sciences"])
receipt = {"written": "off-desk lane 27 Nov 2026 (oxjob #2467)", "run_at": datetime.now(timezone.utc).isoformat(),
           "execute": EXECUTE, "job": 2467, "approved": "card D5 ruling (oxjob #2254 RULINGS, 16 Nov 2026)",
           "actions": [], "minted": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def main():
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            m = MINT
            owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                 {"i": m["issns"]}).fetchall()
            assert not owned, f"ISSN already on a source {owned}: add nothing, bind there"
            namesakes = sorted({t for n in [m["name"]] + m["alt"] for t, _ in ctx.name_index.get(normalize_name(n), [])})
            assert not namesakes, f"same-named source(s) {namesakes}: check them first, add the ISSN there if it is this journal"
            sid = mint_source(conn, ctx, m["name"], source_type=m["type"], issns=m["issns"], publisher=m["publisher"],
                              homepage_url=m["homepage"], country_code=m["cc"], country=m["country"])
            conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), institution_id = :inst "
                              "WHERE id = :id AND institution_id IS NULL"),
                         {"a": json.dumps(m["alt"], ensure_ascii=False), "inst": m["institution_id"], "id": sid})
            row = conn.execute(text("SELECT id, display_name, type, issn_l, publisher, publisher_id, institution_id, "
                                    "homepage_url, country_code, is_oa, alternate_titles FROM sources WHERE id = :id"),
                               {"id": sid}).mappings().one()
            issns = conn.execute(text("SELECT issn, is_issn_l FROM source_issn WHERE source_id = :id"), {"id": sid}).fetchall()
            assert row["institution_id"] == m["institution_id"] and "3108-0804" in [x[0] for x in issns], dict(row)
            receipt["minted"].append({"source_id": sid, "row": dict(row), "source_issn": [list(x) for x in issns],
                                      "ticket": m["ticket"]})
            log(f"MINTED S{sid} journal {m['name']!r} {m['issns']} host institution I{m['institution_id']} alt {m['alt']} "
                f"(ZD {m['ticket']})")
            log(f"listed_in rows changed {recompute_listed_in(conn)}")
            receipt["revert"] = [f"DELETE FROM source_issn WHERE source_id = {sid};", f"DELETE FROM sources WHERE id = {sid};"]
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
