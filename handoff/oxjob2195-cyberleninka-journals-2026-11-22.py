"""oxjob #2195 (data-bugs): give a source record to the journals whose articles OpenAlex holds as CyberLeninka (cyberleninka.ru)
article records but that have no OpenAlex source, each confirmed by the ISSN International Centre's record (ruling D5).

CyberLeninka publishes each article on behalf of its journal and its article page names the journal and its ISSN (citation_*
meta tags), as does CyberLeninka's journal page (/journal/n/<slug>). The witness is outside CyberLeninka: the ISSN register's
record (portal.issn.org) of that ISSN, whose title (original-script title proper, key title or a title variant) must equal the
journal's CyberLeninka title. The manifest is built and bar-checked in oxjob #2195 (work/decide_w10.py; PLAN.md § Bar, 'Mint tier').

Two actions, one transaction for the whole manifest:
  - mint: a journal OpenAlex lacks. Same refusals as jobs.add_journal.apply:
      an ISSN already on a registry source -> refused (OpenAlex holds the journal; a bind, not a mint)
      an ISSN-less source with the same normalised name -> refused (a legacy twin)
    else mint_source(type journal), then alternate_titles = the ISSN record's romanized and original titles.
  - fill_issn: the journal already has ONE ISSN-less journal source carrying its exact title (a MAG-era record): attach the
    ISSNs to that source instead of minting a twin. Refused if the source has gained an ISSN since, or an ISSN is now owned.
recompute_listed_in at the end (a new ISSN may already be on a loaded list).

  python3 run_registry.py handoff/oxjob2195-cyberleninka-journals-2026-11-22.py <manifest.ndjson> [--execute]
  python3 run_registry.py handoff/oxjob2195-cyberleninka-journals-2026-11-22.py --revert <receipt.json> [--execute]
Dry run by default (rolls back). Receipt: written next to the manifest as <manifest>.receipt.json and printed.
Revert deletes exactly the minted ids in the receipt (their source_issn rows, then the sources rows), refusing if any of them
gained rows elsewhere since (oai_pmh_endpoint, source_merge), and removes exactly the ISSN rows fill_issn attached (resetting
issn_l where the fill set it).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import (MatchContext, insert_issns, mint_source, normalize_name, recompute_listed_in,  # noqa: E402
                         resolve_issn_l)


class DryRun(Exception):
    pass


def run(manifest, execute):
    rows = [json.loads(l) for l in open(manifest, encoding="utf-8") if l.strip()]
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": execute, "manifest": os.path.basename(manifest),
               "oxjob": 2195, "minted": [], "filled": [], "refused": []}
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            for i, r in enumerate(rows, 1):
                issns = [x.upper() for x in r.get("issns") or []]
                owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                     {"i": issns}).fetchall() if issns else []
                if owned:
                    receipt["refused"].append({"code": r["code"], "why": "issn_on_source", "detail": [list(map(str, o)) for o in owned]})
                    continue
                if r.get("action") == "fill_issn":
                    sid = r["source_id"]
                    s = conn.execute(text("SELECT type, issn_l FROM sources WHERE id = :id"), {"id": sid}).fetchone()
                    has = conn.execute(text("SELECT count(*) FROM source_issn WHERE source_id = :id"), {"id": sid}).scalar()
                    if s is None or s.type != "journal" or s.issn_l or has:
                        receipt["refused"].append({"code": r["code"], "why": "fill_target_changed", "detail": [sid]})
                        continue
                    insert_issns(conn, sid, issns, resolve_issn_l(conn, issns, member_only=True))
                    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": sid})
                    ctx.register(sid, issns)
                    receipt["filled"].append({"code": r["code"], "source_id": sid, "issns": issns})
                    continue
                name = r["display_name"]
                twins = [t for t, _ in ctx.name_index.get(normalize_name(name), []) if not ctx.sid_to_issns.get(t)]
                if twins:
                    receipt["refused"].append({"code": r["code"], "why": "issnless_namesake", "detail": twins[:5]})
                    continue
                sid = mint_source(conn, ctx, name, source_type="journal", issns=issns, publisher=r.get("publisher"),
                                  country_code=r.get("country_code"), country=r.get("country"))
                if r.get("alternate_titles"):
                    conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                                 {"a": json.dumps(r["alternate_titles"], ensure_ascii=False), "id": sid})
                receipt["minted"].append({"code": r["code"], "source_id": sid})
                if i % 100 == 0:
                    print(f"{i}/{len(rows)} rows, minted {len(receipt['minted'])}, filled {len(receipt['filled'])}, "
                          f"refused {len(receipt['refused'])}", flush=True)
            changed = recompute_listed_in(conn)
            print(f"minted {len(receipt['minted'])}, filled {len(receipt['filled'])}, refused {len(receipt['refused'])}; "
                  f"listed_in rows changed {changed}")
            if not execute:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back (ids above are not reserved)")
    out = manifest + (".receipt.json" if execute else ".dryrun.json")
    json.dump(receipt, open(out, "w"), ensure_ascii=False)
    print("receipt:", out)


def revert(receipt_path, execute):
    rec = json.load(open(receipt_path))
    ids = [m["source_id"] for m in rec["minted"]]
    try:
        with engine.begin() as conn:
            used = conn.execute(text("SELECT count(*) FROM oai_pmh_endpoint WHERE source_id = ANY(:i)"), {"i": ids}).scalar()
            merged = conn.execute(text("SELECT count(*) FROM source_merge WHERE winner_id = ANY(:i) OR loser_id = ANY(:i)"),
                                  {"i": ids}).scalar()
            if used or merged:
                raise SystemExit(f"refusing: {used} endpoint rows and {merged} merge rows now use these sources")
            conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = ANY(:i)"), {"i": ids})  # issn_l -> source_issn FK
            n_issn = conn.execute(text("DELETE FROM source_issn WHERE source_id = ANY(:i)"), {"i": ids}).rowcount
            n_src = conn.execute(text("DELETE FROM sources WHERE id = ANY(:i)"), {"i": ids}).rowcount
            n_fill = 0
            for f in rec.get("filled", []):
                conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :id AND issn_l = ANY(:i)"),
                             {"id": f["source_id"], "i": f["issns"]})
                n_fill += conn.execute(text("DELETE FROM source_issn WHERE source_id = :id AND issn = ANY(:i)"),
                                       {"id": f["source_id"], "i": f["issns"]}).rowcount
            changed = recompute_listed_in(conn)
            print(f"deleted {n_src} sources, {n_issn} source_issn rows; removed {n_fill} filled ISSN rows; "
                  f"listed_in rows changed {changed}")
            if not execute:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--execute"]
    if args[0] == "--revert":
        revert(args[1], "--execute" in sys.argv)
    else:
        run(args[0], "--execute" in sys.argv)
