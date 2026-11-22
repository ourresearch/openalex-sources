"""Registry fixes for oxjob #653 (onboard missing preprint servers), support factory #2254. v3, written 2026-11-19 12:50 CT
by the off-desk lane (project step 2 prep). Run on desk (lanes have no registry key). ONE transaction; dry run by default.
Helpers (log, snapshot, add_alt_titles, merge) are v2's verbatim, as desk fixed them 17 Nov (post-D1 merge, oai_pmh_endpoint).

1. MINT TechRxiv. IEEE's preprint server (techrxiv.org, since 2019). No source by name or homepage (walden sources 19 Nov
   13:00 CT: none matching 'techrxiv'; API source search 0). Crossref: 30,954 deposits on 10.36227 registered on
   techrxiv.org / techrxiv.figshare.com, institution 'TechRxiv', no container/ISSN; 19,686 works with no source (live API
   19 Nov). repository, publisher Institute of Electrical and Electronics Engineers, homepage https://www.techrxiv.org.
2. MINT ESS Open Archive (Earth and Space Science Open Archive, ESSOAr; Wikidata Q53731276, aliases 'ESS Open Archive',
   'EESOAr'; AGU's preprint server since Feb 2018, on Wiley's Authorea platform since 2023). No source by name or homepage.
   Crossref: 26,368 deposits registered on essopenarchive.org / essoar.org (10.1002/essoar.N 13,286 and
   10.22541/essoar.N 13,082), institution 'Earth and Space Science Open Archive'; ~11.9K works with no source.
   repository, publisher American Geophysical Union, homepage https://essopenarchive.org, alternate titles
   'Earth and Space Science Open Archive', 'ESSOAr'.
   (Live techrxiv.org / essopenarchive.org pages answer Cloudflare 'Just a moment...' off desk: a desk lane may confirm
   name and publisher on each About page first; change MINTS below if they differ.)
3. AUTHOREA S4363604296 ('Authorea', repository, 103 works; walden now routes 10.22541 DOIs registered on authorea.com to
   it, walden commit 'Source Authorea preprints ...', night 11-20): fill homepage_url https://www.authorea.com and
   publisher Wiley, only where NULL.
4. MERGE Authorea's empty duplicates into S4363604296: S4306402105 'Authorea (Authorea)' (repository, 0 works, homepage
   authorea.com) and S4306503842 'Authorea Preprints' (journal, 0 works, no ISSN). Live API 19 Nov: works_count 0 each.

    cd ~/ox/openalex-sources && cp <this file> handoff/oxjob653-registry-v3-2026-11-19.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob653-registry-v3-2026-11-19.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob653-registry-v3-2026-11-19.py --execute
Expect: MINTED S<a> 'TechRxiv'; MINTED S<b> 'ESS Open Archive'; alternate_titles <b> += [...]; FILL S4363604296 ...;
MERGED 4306402105 -> 4363604296; MERGED 4306503842 -> 4363604296. Receipt (before-state, actions, minted ids) after RECEIPT.
Then: walden rows for S<a>/S<b> in the #2092 tier (#653 PLAN.md Handoff, project step 2).
Revert: 1-2. DELETE FROM sources WHERE id IN (<a>, <b>) (no ISSN, endpoint or merge rows were written for them);
3. UPDATE sources SET homepage_url = NULL, publisher = NULL WHERE id = 4363604296 (receipt `before` has the old values);
4. re-insert sources rows 4306402105 and 4306503842 from the receipt's `before` snapshot (OVERRIDING SYSTEM VALUE), move
back any ISSN / oai_pmh_endpoint / source_datacite_id rows the receipt lists under them, DELETE FROM source_merge WHERE
loser_id IN (4306402105, 4306503842), and restore S4363604296's alternate_titles from its `before` row.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.expanduser("~/ox/openalex-sources")
sys.path.insert(0, REPO if os.path.isdir(REPO) else os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import MatchContext, mint_source, normalize_name, refresh_issns_column, resolve_issn_l
# NB: not sources_lib.merge_source: it still writes sources.merge_into_id (dropped by D1, migration 021) and
# source_endpoint (dropped by 037). merge() below is the post-D1 helper of handoff/oxjob875-zenodo-journals-2026-11-16.py.

EXECUTE = "--execute" in sys.argv
JOB = 653
APPROVED = "support factory #2254 (standing authority); preprint servers are sources (#653)"

MINTS = [
    dict(name="TechRxiv", publisher="Institute of Electrical and Electronics Engineers",
         homepage="https://www.techrxiv.org", alts=[],
         dup_sql="display_name ILIKE '%techrxiv%' OR homepage_url ILIKE '%techrxiv%'"),
    dict(name="ESS Open Archive", publisher="American Geophysical Union",
         homepage="https://essopenarchive.org", alts=["Earth and Space Science Open Archive", "ESSOAr"],
         dup_sql="display_name ~* '(^|[^a-z])ess open archive' OR display_name ILIKE '%earth and space science open archive%' "
                 "OR display_name ILIKE '%essoar%' OR homepage_url ILIKE '%essopenarchive%' OR homepage_url ILIKE '%essoar%'"),
]
AUTHOREA, AUTHOREA_HOME, AUTHOREA_PUB = 4363604296, "https://www.authorea.com", "Wiley"
MERGES = [(4306402105, "Authorea (Authorea)"), (4306503842, "Authorea Preprints")]
MERGE_RULE = "oxjob653_duplicate_authorea"

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
           "before": {}, "actions": [], "minted": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        row = {k: (None if v is None else str(v)) for k, v in r.items()}
        row["_issns"] = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"),
                                                     {"i": r["id"]})]
        row["_endpoints"] = [str(x[0]) for x in conn.execute(
            text("SELECT id FROM oai_pmh_endpoint WHERE source_id = :i"), {"i": r["id"]})]
        row["_datacite_ids"] = [str(x[0]) for x in conn.execute(
            text("SELECT * FROM source_datacite_id WHERE source_id = :i"), {"i": r["id"]})]
        receipt["before"][str(r["id"])] = row


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [row.display_name or ""]}
    gained = [t.strip() for t in titles if t and t.strip() and t.strip().lower() not in have]
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "id": sid})
        log(f"alternate_titles {sid} += {gained}")


def merge(conn, loser, winner, rule, detail):
    """Post-D1 merge, same steps as merge() in handoff/oxjob875-zenodo-journals-2026-11-16.py (and #1979's, #629's):
    loser deleted, ledger row in source_merge (merged-ID redirect, #1124). Returns 'merged'."""
    rows = {r.id: r for r in conn.execute(
        text("SELECT id, display_name, alternate_titles, override_timestamp, issn_l FROM sources WHERE id IN (:l, :w)"),
        {"l": loser, "w": winner})}
    assert loser in rows and winner in rows, (loser, winner)
    assert rows[loser].override_timestamp is None, f"loser {loser} is curator-overridden"
    # chain collapse (desk apply lane 21 Nov, as #654's / #740's scripts): earlier merges into the loser now point at
    # the winner, so their redirects don't dangle on a deleted id. Revert: receipt merge_chain_repointed.
    for mid, old_loser in conn.execute(text("SELECT id, loser_id FROM source_merge WHERE winner_id = :l"),
                                       {"l": loser}).fetchall():
        conn.execute(text("UPDATE source_merge SET winner_id = :w WHERE id = :m"), {"w": winner, "m": mid})
        receipt.setdefault("merge_chain_repointed", []).append({"source_merge_id": mid, "loser_id": old_loser,
                                                                "was_winner": loser})
        log(f"source_merge {mid}: {old_loser} -> {loser} re-pointed to {winner}")
    conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :l"), {"l": loser})
    moved = conn.execute(text("UPDATE source_issn SET source_id = :w, is_issn_l = FALSE WHERE source_id = :l"),
                         {"l": loser, "w": winner}).rowcount
    ep = conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
    dc = conn.execute(text("UPDATE source_datacite_id SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
    bm = conn.execute(text("INSERT INTO ojs_beacon_mint (source_id, edition, receipt, minted_at) "
                           "SELECT :w, edition, receipt, minted_at FROM ojs_beacon_mint WHERE source_id = :l "
                           "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner}).rowcount
    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                      "SELECT :w, curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :l "
                      "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner})
    conn.execute(text("DELETE FROM source_oa_override WHERE source_id = :l"), {"l": loser})
    add_alt_titles(conn, winner, [rows[loser].display_name] + list(rows[loser].alternate_titles or []))
    issns = [r[0] for r in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": winner})]
    if issns and rows[winner].issn_l is None:
        issn_l = resolve_issn_l(conn, issns, member_only=True)
        conn.execute(text("UPDATE sources SET issn_l = :l WHERE id = :id"), {"l": issn_l, "id": winner})
    conn.execute(text("UPDATE source_issn si SET is_issn_l = (si.issn IS NOT DISTINCT FROM s.issn_l) "
                      "FROM sources s WHERE s.id = :id AND si.source_id = :id"), {"id": winner})
    refresh_issns_column(conn, winner)
    conn.execute(text("INSERT INTO source_merge (loser_id, winner_id, rule, source_feed, detail) "
                      "VALUES (:l, :w, :r, 'zendesk', CAST(:d AS JSONB))"),
                 {"l": loser, "w": winner, "r": rule, "d": json.dumps({**detail, "loser_name": rows[loser].display_name},
                                                                        ensure_ascii=False)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    log(f"MERGED {loser} -> {winner} ({rule}): issns moved {moved}, endpoints {ep}, datacite {dc}, beacon {bm}")
    return "merged"


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            snapshot(conn, [AUTHOREA] + [m[0] for m in MERGES])
            ctx = MatchContext(conn, name_link=True)
            # 1-2. mints
            for m in MINTS:
                dup = conn.execute(text("SELECT id, display_name, homepage_url FROM sources WHERE "
                                        + m["dup_sql"])).fetchall()
                assert not dup, f"{m['name']} already in the registry: {dup} (use that id in the walden edit; mint nothing)"
                twins = ctx.name_index.get(normalize_name(m["name"]), [])
                assert not twins, f"namesake(s) of {m['name']}: {twins}"
                sid = mint_source(conn, ctx, m["name"], source_type="repository",
                                  publisher=m["publisher"], homepage_url=m["homepage"])
                got = conn.execute(text("SELECT display_name, type FROM sources WHERE id = :i"), {"i": sid}).fetchone()
                assert got.type == "repository" and got.display_name == m["name"], got
                if m["alts"]:
                    add_alt_titles(conn, sid, m["alts"])
                receipt["minted"].append({"source_id": sid, "name": m["name"], "type": got.type})
                log(f"MINTED S{sid} repository {m['name']!r} publisher {m['publisher']!r} homepage {m['homepage']}")
            # 3. Authorea homepage + publisher, fill-only
            cur = conn.execute(text("SELECT display_name, type, homepage_url, publisher, publisher_id FROM sources "
                                    "WHERE id = :i"), {"i": AUTHOREA}).fetchone()
            assert cur is not None and cur.display_name == "Authorea" and cur.type == "repository", cur
            sets = {}
            if not cur.homepage_url:
                sets["homepage_url"] = AUTHOREA_HOME
            if not cur.publisher and cur.publisher_id is None:
                sets["publisher"] = AUTHOREA_PUB
            if sets:
                conn.execute(text("UPDATE sources SET " + ", ".join(f"{k} = :{k}" for k in sets)
                                  + ", updated_date = now() WHERE id = :id"), {**sets, "id": AUTHOREA})
                log(f"FILL S{AUTHOREA} {sets}")
            else:
                log(f"FILL S{AUTHOREA} skipped: homepage {cur.homepage_url!r}, publisher {cur.publisher!r}")
            # 4. empty duplicates of Authorea
            for loser, name in MERGES:
                row = conn.execute(text("SELECT display_name FROM sources WHERE id = :i"), {"i": loser}).fetchone()
                if row is None:
                    log(f"MERGE S{loser} skipped: not in the registry (already merged?)")
                    continue
                assert row.display_name == name, (loser, row.display_name)
                outcome = merge(conn, loser, AUTHOREA, rule=MERGE_RULE,
                                detail={"job": 653, "why": "empty duplicate of Authorea (0 works, live API 19 Nov 2026)"})
                log(f"MERGE S{loser} -> S{AUTHOREA}: {outcome}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
