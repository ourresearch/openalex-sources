"""oxjob #2547 (sources charter): ISSN, ISSN-L, publisher text and country JP for the ISSN-less MAG-era Japanese journals
(S4306..., 100+ works each, mostly CiNii records), from their National Diet Library record; and merges where the
journal already has a record (the ISSN holder) or two ISSN-less records of one journal exist (twins).

Witness (ruling D5): NDL's list of the journals its periodical-article index covers (雑誌記事索引 採録誌一覧,
sairokushi_202610.tsv, https://www.ndl.go.jp/data/sakuin/sakuin_index). A source takes a record only when its title
matches the record's, its works' publisher (MAG's CiNii string) agrees with NDL's publisher for most works (and under
10% name another same-titled NDL journal's publisher), and 90% of its works fall inside NDL's years. Never from a work's
venue string, never from CiNii or JaLC. Plan and bar: oxjobs working/japanese-mag-journals-no-issn (plan.py, BAR in
README: CiNii prints the same ISSN on the journal's own articles for 196 of 197 sampled writes; the one other is the
same magazine's print ISSN).

Manifest (one JSON object per line, applied in order, ONE transaction):
  fill   source_id, issns, issn_l, publisher, country_code
         guards: the source is a journal and holds no ISSN; no ISSN is on any source yet (else refused, 'issn_on_source').
         ISSNs via insert_issns (issn_l = the issn_to_issnl answer among them, else the first); publisher text only when
         the source has no publisher text and no publisher_id / institution_id; country_code/country only when NULL.
  merge  loser, winner, kind (issn_holder | issnless_twin)
         post-D1 pattern of handoff/oxjob629-respublica-2026-11-23.py: loser deleted, its names kept as the winner's
         alternate titles, a source_merge row (rule oxjob2547_ndl_issn, detail.loser_name) for the 301 redirect and
         walden's merged-name tier. guards: loser holds no ISSN and is not curator-overridden; winner holds the ISSN
         (a twin winner gains it from its fill row above).
recompute_listed_in at the end (a new ISSN may already be on a loaded list).

Run on desk or the mini, dry run by default (rolls back):
  python3 <oxjobs>/working/source-record-corrections-from-tickets/work/run_registry.py \
      handoff/oxjob2547-mag-jp-issn-2026-11-23.py <manifest.ndjson> [--execute]
  ... --revert <receipt.json> [--execute]
Receipt: <manifest>.receipt.json (execute) or .dryrun.json: every touched row's before-state and what was done.
Revert: fills -> delete the added source_issn rows and restore issn_l / issns / publisher / country_code / country from
`before`; merges -> re-insert the loser's `before` row, delete its source_merge row, restore the winner's
alternate_titles from `before`. Walden side tables (merged_source_names / merged_source_ids / article_page_journal_sources)
are reverted by the oxjob's own undo (work/sidetables.py --revert).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import (clean_control_chars, insert_issns, recompute_listed_in, refresh_datacite_column,  # noqa: E402
                         refresh_issns_column, resolve_issn_l)

JOB, RULE = 2547, "oxjob2547_ndl_issn"
APPROVED = "Casey (go, #2527 recommendations session, 23 Nov 2026); ruling D5; charter D5 2026-07-17 honor-the-ISSN"
COUNTRY = ("JP", "Japan")


class DryRun(Exception):
    pass


def snap(conn, sid, receipt):
    if str(sid) in receipt["before"]:
        return
    r = conn.execute(text("SELECT * FROM sources WHERE id = :i"), {"i": sid}).mappings().fetchone()
    if r is None:
        return
    row = {k: (v if isinstance(v, (list, dict)) or v is None else str(v)) for k, v in r.items()}
    row["_issns"] = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": sid})]
    receipt["before"][str(sid)] = row


def fill(conn, m, receipt):
    sid = m["source_id"]
    s = conn.execute(text("SELECT id, type, publisher, publisher_id, institution_id, country_code FROM sources "
                          "WHERE id = :i"), {"i": sid}).fetchone()
    if s is None or s.type != "journal":
        return "not_a_live_journal"
    if conn.execute(text("SELECT count(*) FROM source_issn WHERE source_id = :i"), {"i": sid}).scalar():
        return "has_issn_now"
    issns = [x.upper() for x in m["issns"]]
    if issns:
        owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"), {"i": issns}).fetchall()
        if owned:
            return "issn_on_source " + ",".join(f"{a}@{b}" for a, b in owned)
    snap(conn, sid, receipt)
    did = {}
    if issns:
        issn_l = m.get("issn_l") if m.get("issn_l") in issns else resolve_issn_l(conn, issns, member_only=True)
        insert_issns(conn, sid, issns, issn_l)
        did["issns"], did["issn_l"] = issns, issn_l
    sets = {}
    pub = clean_control_chars(m.get("publisher"))
    if pub and not s.publisher and s.publisher_id is None and s.institution_id is None:
        sets["publisher"] = pub
    if not s.country_code:
        sets["country_code"], sets["country"] = COUNTRY
    if sets:
        conn.execute(text(f"UPDATE sources SET {', '.join(f'{k} = :{k}' for k in sets)} WHERE id = :id"),
                     {**sets, "id": sid})
        did.update(sets)
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :i"), {"i": sid})
    receipt["filled"].append({"source_id": sid, **did})
    return "filled"


def merge(conn, m, receipt):
    loser, winner = m["loser"], m["winner"]
    rows = {r.id: r for r in conn.execute(
        text("SELECT id, display_name, alternate_titles, override_timestamp, issn_l, type FROM sources WHERE id IN (:l, :w)"),
        {"l": loser, "w": winner})}
    if loser not in rows or winner not in rows:
        return "missing_source"
    if rows[loser].override_timestamp is not None:
        return "loser_overridden"
    if conn.execute(text("SELECT count(*) FROM source_issn WHERE source_id = :i"), {"i": loser}).scalar():
        return "loser_has_issn"
    w_issns = {x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": winner})}
    if not w_issns & set(m["issns"]):
        return "winner_lacks_issn"
    snap(conn, loser, receipt)
    snap(conn, winner, receipt)
    ep = [x[0] for x in conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :w WHERE source_id = :l RETURNING id"),
                                     {"l": loser, "w": winner})]
    dc = conn.execute(text("UPDATE source_datacite_id SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
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
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1, loser
    refresh_issns_column(conn, winner)
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    receipt["merged"].append({"loser": loser, "winner": winner, "kind": m["kind"], "alts_gained": gained,
                              "endpoints": ep, "datacite": dc})
    return "merged"


def apply(manifest, execute):
    rows = [json.loads(l) for l in open(manifest, encoding="utf-8") if l.strip()]
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": execute, "job": JOB,
               "manifest": os.path.basename(manifest), "before": {}, "filled": [], "merged": [], "refused": []}
    try:
        with engine.begin() as conn:
            for i, m in enumerate(rows, 1):
                out = fill(conn, m, receipt) if m["action"] == "fill" else merge(conn, m, receipt)
                if out not in ("filled", "merged"):
                    receipt["refused"].append({**{k: m.get(k) for k in ("action", "source_id", "loser", "winner")},
                                               "why": out})
                if i % 500 == 0:
                    print(f"{i}/{len(rows)}: filled {len(receipt['filled'])}, merged {len(receipt['merged'])}, "
                          f"refused {len(receipt['refused'])}", flush=True)
            changed = recompute_listed_in(conn)
            n_issn = sum(1 for f in receipt["filled"] if f.get("issns"))
            print(f"filled {len(receipt['filled'])} ({n_issn} with ISSNs), merged {len(receipt['merged'])}, "
                  f"refused {len(receipt['refused'])}; listed_in rows changed {changed}")
            why = {}
            for r in receipt["refused"]:
                k = r["why"].split(" ")[0]
                why[k] = why.get(k, 0) + 1
            print("refused by reason:", why)
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
            for m in reversed(rec["merged"]):
                b = rec["before"][str(m["loser"])]
                cols = [k for k in b if not k.startswith("_")]
                conn.execute(text(f"INSERT INTO sources ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})"),
                             {c: (json.dumps(b[c], ensure_ascii=False) if isinstance(b[c], (list, dict)) else b[c])
                              for c in cols})
                conn.execute(text("DELETE FROM source_merge WHERE loser_id = :l AND winner_id = :w AND rule = :r"),
                             {"l": m["loser"], "w": m["winner"], "r": RULE})
                if m["endpoints"]:
                    conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :l WHERE id = ANY(:e)"),
                                 {"l": m["loser"], "e": m["endpoints"]})
                assert not m["datacite"], f"loser {m['loser']} had DataCite clients: move them back by hand"
                wb = rec["before"][str(m["winner"])]
                conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :w"),
                             {"a": json.dumps(wb.get("alternate_titles") or [], ensure_ascii=False), "w": m["winner"]})
            for f in rec["filled"]:
                b = rec["before"][str(f["source_id"])]
                conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :i"), {"i": f["source_id"]})
                if f.get("issns"):
                    conn.execute(text("DELETE FROM source_issn WHERE source_id = :i AND issn = ANY(:x)"),
                                 {"i": f["source_id"], "x": f["issns"]})
                conn.execute(text("UPDATE sources SET publisher = :p, country_code = :c, country = :n, issn_l = :l, "
                                  "updated_date = now() WHERE id = :i"),
                             {"p": b.get("publisher"), "c": b.get("country_code"), "n": b.get("country"),
                              "l": b.get("issn_l"), "i": f["source_id"]})
                refresh_issns_column(conn, f["source_id"])
            changed = recompute_listed_in(conn)
            print(f"reverted {len(rec['merged'])} merges, {len(rec['filled'])} fills; listed_in changed {changed}")
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
