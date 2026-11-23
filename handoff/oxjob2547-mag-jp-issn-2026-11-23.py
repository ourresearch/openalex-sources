"""oxjob #2547 (sources charter): merge ISSN-less MAG-era Japanese journal records (S4306..., mostly CiNii works) into
the record of the same journal, identified by its National Diet Library record (ruling D5; charter D5 2026-07-17
honor-the-ISSN). Their ISSN, ISSN-L, publisher text and country come from jobs.ndl_journals (NDL's indexed-journal list
as a registry feed) run with --bind-csv data/ndl/oxjob2547-binds-2026-11-23.csv; this handoff does only the merges.

Manifest (one JSON object per line, ONE transaction): {"action": "merge", "loser", "winner", "kind", "issns", "bibids"}
  issn_holder    282: the NDL record's ISSN already sits on the winner (麻酔 -> Masui).
  issnless_twin  256: two ISSN-less records match one NDL record; the bigger wins and jobs.ndl_journals gives it the ISSN.
Post-D1 pattern of handoff/oxjob629-respublica-2026-11-23.py: loser deleted, its names kept as the winner's alternate
titles, a source_merge row (rule oxjob2547_ndl_issn, detail.loser_name) for the 301 redirect and walden's merged-name
tier. Guards: the loser holds no ISSN and is not curator-overridden; the winner holds the ISSN, or (twin) holds none and
no other source holds any of them. 8 merges whose loser name another live source carries are left out of the manifest.
Then oxjobs working/japanese-mag-journals-no-issn/work/sidetables.py (merged_source_names, merged_source_ids,
article_page_journal_sources) so the losers' name-matched works follow them.

  python3 <oxjobs>/working/source-record-corrections-from-tickets/work/run_registry.py \\
      handoff/oxjob2547-mag-jp-issn-2026-11-23.py handoff/oxjob2547-mag-jp-issn-2026-11-23.ndjson [--execute]
  ... --revert <receipt.json> [--execute]
Receipt: <manifest>.receipt.json (execute) or .dryrun.json. Revert: re-insert each loser's `before` row, delete its
source_merge row, move back any endpoints, restore the winner's alternate_titles from `before`.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import clean_control_chars, recompute_listed_in, refresh_datacite_column, refresh_issns_column  # noqa: E402

JOB, RULE = 2547, "oxjob2547_ndl_issn"
APPROVED = "Casey (oxjob #2547, 23 Nov 2026); ruling D5; charter D5 2026-07-17 honor-the-ISSN"


class DryRun(Exception):
    pass


def snap(conn, sid, receipt):
    if str(sid) in receipt["before"]:
        return
    r = conn.execute(text("SELECT * FROM sources WHERE id = :i"), {"i": sid}).mappings().fetchone()
    if r is not None:
        receipt["before"][str(sid)] = {k: (v if isinstance(v, (list, dict)) or v is None else str(v)) for k, v in r.items()}


def merge(conn, m, receipt):
    loser, winner = m["loser"], m["winner"]
    rows = {r.id: r for r in conn.execute(
        text("SELECT id, display_name, alternate_titles, override_timestamp FROM sources WHERE id IN (:l, :w)"),
        {"l": loser, "w": winner})}
    if loser not in rows or winner not in rows:
        return "missing_source"
    if rows[loser].override_timestamp is not None:
        return "loser_overridden"
    if conn.execute(text("SELECT count(*) FROM source_issn WHERE source_id = :i"), {"i": loser}).scalar():
        return "loser_has_issn"
    w_issns = {x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": winner})}
    elsewhere = conn.execute(text("SELECT count(*) FROM source_issn WHERE issn = ANY(:i) AND source_id <> :w"),
                             {"i": m["issns"], "w": winner}).scalar()
    if not w_issns & set(m["issns"]) and (m["kind"] != "issnless_twin" or w_issns or elsewhere):
        return "winner_lacks_issn"
    snap(conn, loser, receipt)
    snap(conn, winner, receipt)
    ep = [x[0] for x in conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :w WHERE source_id = :l RETURNING id"),
                                     {"l": loser, "w": winner})]
    dc = conn.execute(text("UPDATE source_datacite_id SET source_id = :w WHERE source_id = :l"),
                      {"l": loser, "w": winner}).rowcount
    if dc:
        refresh_datacite_column(conn, winner)
    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                      "SELECT :w, curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :l "
                      "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner})
    conn.execute(text("DELETE FROM source_oa_override WHERE source_id = :l"), {"l": loser})
    w = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :i"), {"i": winner}).fetchone()
    alts = list(w.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [w.display_name or ""]}
    gained = []
    for t in [rows[loser].display_name] + list(rows[loser].alternate_titles or []):
        t = clean_control_chars((t or "").strip())
        if t and t.lower() not in have:
            gained.append(t)
            have.add(t.lower())
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "id": winner})
    conn.execute(text("INSERT INTO source_merge (loser_id, winner_id, rule, source_feed, detail) "
                      "VALUES (:l, :w, :r, 'ndl', CAST(:d AS JSONB))"),
                 {"l": loser, "w": winner, "r": RULE,
                  "d": json.dumps({"oxjob": JOB, "kind": m["kind"], "issns": m["issns"], "ndl_bibids": m["bibids"],
                                   "approved_by": APPROVED, "loser_name": rows[loser].display_name}, ensure_ascii=False)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    assert conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount == 1, loser
    refresh_issns_column(conn, winner)
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    receipt["merged"].append({"loser": loser, "winner": winner, "kind": m["kind"], "alts_gained": gained,
                              "endpoints": ep, "datacite": dc})
    return "merged"


def apply(manifest, execute):
    rows = [json.loads(l) for l in open(manifest, encoding="utf-8") if l.strip()]
    rows = [r for r in rows if r["action"] == "merge"]
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": execute, "job": JOB,
               "manifest": os.path.basename(manifest), "before": {}, "merged": [], "refused": []}
    try:
        with engine.begin() as conn:
            for i, m in enumerate(rows, 1):
                out = merge(conn, m, receipt)
                if out != "merged":
                    receipt["refused"].append({"loser": m["loser"], "winner": m["winner"], "why": out})
                if i % 100 == 0:
                    print(f"{i}/{len(rows)}: merged {len(receipt['merged'])}, refused {len(receipt['refused'])}", flush=True)
            changed = recompute_listed_in(conn)
            why = {}
            for r in receipt["refused"]:
                why[r["why"]] = why.get(r["why"], 0) + 1
            print(f"merged {len(receipt['merged'])}, refused {len(receipt['refused'])} {why}; listed_in changed {changed}")
            if not execute:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    out = manifest + (".receipt.json" if execute else ".dryrun.json")
    json.dump(receipt, open(out, "w"), ensure_ascii=False, default=str)
    print("receipt:", out)


def revert(path, execute):
    rec = json.load(open(path))
    try:
        with engine.begin() as conn:
            jsonb = {r[0] for r in conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'sources' AND data_type = 'jsonb'"))}
            for m in reversed(rec["merged"]):
                assert not m["datacite"], f"loser {m['loser']} had DataCite clients: move them back by hand"
                b = rec["before"][str(m["loser"])]
                cols = list(b)
                conn.execute(text(f"INSERT INTO sources ({', '.join(cols)}) OVERRIDING SYSTEM VALUE "
                                  f"VALUES ({', '.join(('CAST(:' + c + ' AS JSONB)') if c in jsonb else ':' + c for c in cols)})"),
                             {c: (json.dumps(b[c], ensure_ascii=False) if c in jsonb and b[c] is not None else b[c])
                              for c in cols})
                conn.execute(text("DELETE FROM source_merge WHERE loser_id = :l AND winner_id = :w AND rule = :r"),
                             {"l": m["loser"], "w": m["winner"], "r": RULE})
                if m["endpoints"]:
                    conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :l WHERE id = ANY(:e)"),
                                 {"l": m["loser"], "e": m["endpoints"]})
                wb = rec["before"][str(m["winner"])]
                conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :w"),
                             {"a": json.dumps(wb.get("alternate_titles") or [], ensure_ascii=False), "w": m["winner"]})
            changed = recompute_listed_in(conn)
            print(f"reverted {len(rec['merged'])} merges; listed_in changed {changed}")
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
        apply(args[0], "--execute" in sys.argv)
