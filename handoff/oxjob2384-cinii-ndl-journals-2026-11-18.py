"""oxjob #2384 (data-bugs): mint the Japanese journals that CiNii's article records name but OpenAlex has no source for,
each built from its National Diet Library catalogue record (ruling D5: a third party confirms the journal exists; the
source's name, ISSNs, publisher and country come from that witness, never from a work's venue string).

Witness: NDL's list of the journals its periodical-article index covers (雑誌記事索引 採録誌一覧, sairokushi_202610.tsv,
https://www.ndl.go.jp/jp/data/sakuin/sakuin_index.html). The manifest (one JSON object per line: bibid, display_name,
alternate_titles, issns, publisher, country_code, country, type, witness) is built and bar-checked in oxjob #2384
(work/build_manifest.py, BAR.md). Wave 1 binds the journals' CiNii article records through
openalex.sources.article_page_journal_sources (platform 'cinii', oxjob 2384) after this mint.

Per row, ONE transaction for the whole manifest, same refusals as jobs.add_journal.apply:
  - an ISSN already on a registry source -> refused (OpenAlex holds the journal; a bind, not a mint)
  - an ISSN-less source with the same normalised name -> refused (a legacy twin, e.g. the MAG cohort)
  - else mint_source(type journal), then alternate_titles = NDL's parallel titles
recompute_listed_in at the end (a new source's ISSN may already be on a loaded list).

  python3 run_registry.py handoff/oxjob2384-cinii-ndl-journals-2026-11-18.py <manifest.ndjson> [--execute]
  python3 run_registry.py handoff/oxjob2384-cinii-ndl-journals-2026-11-18.py --revert <receipt.json> [--execute]
Dry run by default (rolls back). Receipt: written next to the manifest as <manifest>.receipt.json and printed.
Revert deletes exactly the minted ids in the receipt (their source_issn rows, then the sources rows), refusing if any of
them gained rows elsewhere since (oai_pmh_endpoint, source_merge).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import MatchContext, mint_source, normalize_name, recompute_listed_in  # noqa: E402


class DryRun(Exception):
    pass


def mint(manifest, execute):
    rows = [json.loads(l) for l in open(manifest, encoding="utf-8") if l.strip()]
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": execute, "manifest": os.path.basename(manifest),
               "oxjob": 2384, "minted": [], "refused": []}
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            for i, r in enumerate(rows, 1):
                name, issns = r["display_name"], [x.upper() for x in r.get("issns") or []]
                owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                     {"i": issns}).fetchall() if issns else []
                if owned:
                    receipt["refused"].append({"bibid": r["bibid"], "why": "issn_on_source", "detail": [list(map(str, o)) for o in owned]})
                    continue
                twins = [t for t, _ in ctx.name_index.get(normalize_name(name), []) if not ctx.sid_to_issns.get(t)]
                if twins:
                    receipt["refused"].append({"bibid": r["bibid"], "why": "issnless_namesake", "detail": twins[:5]})
                    continue
                sid = mint_source(conn, ctx, name, source_type="journal", issns=issns, publisher=r.get("publisher"),
                                  country_code=r.get("country_code"), country=r.get("country"))
                if r.get("alternate_titles"):
                    conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                                 {"a": json.dumps(r["alternate_titles"], ensure_ascii=False), "id": sid})
                receipt["minted"].append({"bibid": r["bibid"], "source_id": sid})
                if i % 500 == 0:
                    print(f"{i}/{len(rows)} rows, minted {len(receipt['minted'])}, refused {len(receipt['refused'])}", flush=True)
            changed = recompute_listed_in(conn)
            print(f"minted {len(receipt['minted'])}, refused {len(receipt['refused'])}; listed_in rows changed {changed}")
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
            changed = recompute_listed_in(conn)
            print(f"deleted {n_src} sources, {n_issn} source_issn rows; listed_in rows changed {changed}")
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
        mint(args[0], "--execute" in sys.argv)
