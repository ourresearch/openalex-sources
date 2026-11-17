"""Mint Cuadernos del MPD (ISSN 2524-972X print, 2524-9738 online) for Zendesk 26922 item 2 (oxjob #2074, support
factory #2254), under card D5's ruling (RULINGS.md § 3 Coverage, 16 Nov 2026): add a journal on no list we load once a
third party outside OpenAlex and the work confirms it exists.

Witnesses (receipts: #2074 work/recheck_2026-11-16.json):
  - ISSN Argentina (CAICYT, the national ISSN centre): biblionumber 120526 'Cuadernos del MPD', ISSN 2524-972X (print);
    biblionumber 120527, ISSN 2524-9738 (electronic, available online). Publisher: Dirección de Jurisprudencia,
    Ministerio Público de la Defensa de la Ciudad Autónoma de Buenos Aires. Other title: Cuadernos del Ministerio
    Público de la Defensa.
  - Latindex directory ficha 23046: same title, ISSN-L 2524-972X, Argentina, since 2014, semestral, vigente.
  - The publisher's own page for issue 10: https://www.mpdefensa.gob.ar/publicaciones/cuadernos-del-mpd-10. The site
    blocks desk's IP too (WAF page 'Su acceso ha sido bloqueado', curl and a signed-out Chrome, 17 Nov), so desk read the
    Wayback copy (web.archive.org/web/20230326033308/...): Ministerio Público de la Defensa CABA, issue 10 (May 2020)
    'es parte de la colección Cuadernos de Jurisprudencia del Ministerio Público de la Defensa'. Added as a 2nd alt title.
No OpenAlex source holds either ISSN (sources/issn:2524-972X and issn:2524-9738 -> 404; name search 0, 16 Nov).
4 DataCite records name ISSN 2524-972X as IsPartOf (zenodo.23671508/9 -> W7223681567; zenodo.23993670/1 ->
W7225394146), so the DataCite tier binds both works to the new source after one nightly.
No OA override: the journal's OA status wasn't verified from the container (is_oa stays derived).

Run on desk (rehearsal by default, one transaction):
    cd ~/ox/openalex-sources && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file> [--execute]
Revert: delete the minted source's source_issn and sources rows (no endpoint, no merge, no override).
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
MINT = dict(ticket=26922, name="Cuadernos del MPD", issns=["2524-972X", "2524-9738"], type="journal",
            publisher="Ministerio Público de la Defensa de la Ciudad Autónoma de Buenos Aires",
            homepage="https://www.mpdefensa.gob.ar/publicaciones", cc="AR", country="Argentina",
            alt=["Cuadernos del Ministerio Público de la Defensa",
                 "Cuadernos de Jurisprudencia del Ministerio Público de la Defensa"])
receipt = {"reviewed": "desk apply lane 17 Nov 2026 (copied from off-desk run 2074-1116-1555-130f, 2 edits)",
           "run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": 2074,
           "approved": "card D5 ruling (oxjob #2254 RULINGS, 16 Nov 2026)", "actions": [], "minted": []}


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
            twins = sorted({t for n in [m["name"]] + m["alt"] for t, _ in ctx.name_index.get(normalize_name(n), [])
                            if not ctx.sid_to_issns.get(t)})
            assert not twins, f"ISSN-less namesake(s) {twins}: add the ISSNs there instead"
            sid = mint_source(conn, ctx, m["name"], source_type=m["type"], issns=m["issns"], publisher=m["publisher"],
                              homepage_url=m["homepage"], country_code=m["cc"], country=m["country"])
            conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                         {"a": json.dumps(m["alt"], ensure_ascii=False), "id": sid})
            receipt["minted"].append({"source_id": sid, "name": m["name"], "issns": m["issns"], "ticket": m["ticket"]})
            log(f"MINTED S{sid} journal {m['name']!r} {m['issns']} alt {m['alt']} (ZD {m['ticket']})")
            log(f"listed_in rows changed {recompute_listed_in(conn)}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
