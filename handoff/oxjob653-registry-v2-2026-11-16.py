"""Registry fixes for oxjob #653 (onboard missing preprint servers), support factory #2254. v2, written 2026-11-16 16:50 CT
by the off-desk lane; SUPERSEDES oxjob653-latarxiv-2026-11-16.py (same steps 1 and 4, plus steps 2 and 3). Run on desk
(lanes have no registry key). Everything in ONE transaction; dry run by default.

1. MINT LatArXiv (ZD 4800). No LatArXiv source (API source search 'latarxiv': 0 hits, 16 Nov 16:15 CT). Values read on the
   site 15 Nov (#653 work/bar_2026-11-15_latarxiv_pages.json): repository, publisher Paideia Publishing Services
   ("servicios preprints a cargo de Paideia Publishing Services"), homepage https://preprints.latarxiv.org/index.php/latarxiv,
   no ISSN, country NULL. 139 works on 10.62059/latarxiv.preprints.N have no source.
2. MINT Open Science Brazil (ZD 5785). An Open Preprint Systems 3.3.0.24 server ("Sobre o Servidor ... Open Science Brazil
   e um repositorio de dados abertos gerenciado pela Editora ALUMNI IN"; footer "Open Science Brazil. ISSN: 2764-9822.
   Editora ALUMNI IN, Sao Paulo-SP, Brazil"; read 16 Nov 16:40 CT at https://cienciaabertabrasil.org/revista/about).
   API: sources/issn:2764-9822 404, source search 0 hits; 41 works on 10.37497/opsbrazil.N, all with no source.
   repository, publisher Editora ALUMNI IN, homepage https://cienciaabertabrasil.org/revista, ISSN 2764-9822, BR.
   (#1225's lane found the same on 16 Nov: "Not a journal: OPS preprint server; mint as repository", mints/summary.json.)
3. HOST of EcoEvoRxiv S4306402488 (ZD 1335): Center for Open Science I2799848540 -> California Digital Library
   I2801248553. CDL has hosted it since Oct 2022 (requester is CDL's publishing programme; its own sibling EarthArXiv
   S4306402614 already has host I2801248553; 988 of 1,000 sampled Crossref deposits on 10.32942 are registered on
   ecoevorxiv.org). Guarded: only if institution_id is still 2799848540. Re-stamps the ~4K works with an EcoEvoRxiv
   location (host embedded) on the next night: inside rider 653-preprint-server-tiers-20261117.
4. MERGE the empty duplicate medRxiv S4306400573 (0 works, same Wikidata Q58465838) into medRxiv S3005729997 (ZD 8055).

    cd ~/ox/openalex-sources && cp <this file> handoff/oxjob653-registry-v2-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob653-registry-v2-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob653-registry-v2-2026-11-16.py --execute
Expect: MINTED S<a> 'LatArXiv'; MINTED S<b> 'Open Science Brazil'; HOST S4306402488 2799848540 -> 2801248553;
MERGE S4306400573 -> S3005729997: merged. The receipt (before-state, actions, minted ids) prints after RECEIPT.
Revert: 1-2. DELETE FROM source_issn WHERE source_id = <b>; DELETE FROM sources WHERE id IN (<a>, <b>) (no endpoint or
merge rows were written for them); 3. UPDATE sources SET institution_id = 2799848540, updated_date = now()
WHERE id = 4306402488 AND institution_id = 2801248553; 4. (post-D1 merge: the loser row is deleted) re-insert sources row
4306400573 from the receipt's `before` snapshot (OVERRIDING SYSTEM VALUE), move back any ISSN / oai_pmh_endpoint /
source_datacite_id rows the receipt lists under the loser, DELETE FROM source_merge WHERE loser_id = 4306400573, and drop
the alternate title the merge added to S3005729997 (its receipt `before` row has the old alternate_titles).

Desk fix 2026-11-17 09:20 CT (#653 apply lane): the off-desk draft called sources_lib.merge_source and read
source_endpoint; both are gone from the live registry (merge_into_id dropped by D1, migration 021; source_endpoint by
migration 037), so the rehearsal failed. Step 4 now uses the post-D1 merge() of handoff/oxjob875-zenodo-journals-2026-11-16.py
(executed 17 Nov), and the snapshot reads oai_pmh_endpoint.
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
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); Jason 2026-07-20 (#653: preprint servers are sources)"

MINTS = [
    dict(ticket=4800, name="LatArXiv", publisher="Paideia Publishing Services",
         homepage="https://preprints.latarxiv.org/index.php/latarxiv", issns=[], cc=None, country=None,
         dup_sql="display_name ILIKE '%latarxiv%' OR homepage_url ILIKE '%latarxiv%'"),
    dict(ticket=5785, name="Open Science Brazil", publisher="Editora ALUMNI IN",
         homepage="https://cienciaabertabrasil.org/revista", issns=["2764-9822"], cc="BR", country="Brazil",
         dup_sql="display_name ILIKE 'open science brazil%' OR homepage_url ILIKE '%cienciaabertabrasil%'"),
]
HOST_SRC, HOST_OLD, HOST_NEW = 4306402488, 2799848540, 2801248553   # EcoEvoRxiv: COS -> California Digital Library
MERGE_LOSER, MERGE_WINNER, MERGE_RULE = 4306400573, 3005729997, "oxjob653_duplicate_medrxiv"

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
            snapshot(conn, [MERGE_LOSER, MERGE_WINNER, HOST_SRC])
            ctx = MatchContext(conn, name_link=True)
            # 1-2. mints
            for m in MINTS:
                dup = conn.execute(text("SELECT id, display_name, homepage_url FROM sources WHERE "
                                        + m["dup_sql"])).fetchall()
                assert not dup, f"{m['name']} already in the registry: {dup} (use that id in the walden edit; mint nothing)"
                for issn in m["issns"]:
                    own = conn.execute(text("SELECT source_id FROM source_issn WHERE issn = :i"), {"i": issn}).fetchall()
                    assert not own, f"ISSN {issn} already on {own}"
                twins = ctx.name_index.get(normalize_name(m["name"]), [])
                assert not twins, f"namesake(s) of {m['name']}: {twins}"
                sid = mint_source(conn, ctx, m["name"], source_type="repository", issns=m["issns"],
                                  publisher=m["publisher"], homepage_url=m["homepage"],
                                  country_code=m["cc"], country=m["country"])
                got = conn.execute(text("SELECT display_name, type, issn_l FROM sources WHERE id = :i"), {"i": sid}).fetchone()
                assert got.type == "repository" and got.display_name == m["name"], got
                receipt["minted"].append({"source_id": sid, "name": m["name"], "type": got.type, "issn_l": got.issn_l,
                                          "ticket": m["ticket"]})
                log(f"MINTED S{sid} repository {m['name']!r} publisher {m['publisher']!r} homepage {m['homepage']} "
                    f"issns {m['issns']} (ZD {m['ticket']})")
            # 3. EcoEvoRxiv host
            cur = conn.execute(text("SELECT display_name, institution_id FROM sources WHERE id = :i"),
                               {"i": HOST_SRC}).fetchone()
            assert cur is not None and cur.display_name == "EcoEvoRxiv", cur
            if cur.institution_id == HOST_OLD:
                n = conn.execute(text("UPDATE sources SET institution_id = :new, updated_date = now() "
                                      "WHERE id = :id AND institution_id = :old"),
                                 {"new": HOST_NEW, "id": HOST_SRC, "old": HOST_OLD}).rowcount
                assert n == 1
                log(f"HOST S{HOST_SRC} {HOST_OLD} -> {HOST_NEW} (EcoEvoRxiv: COS -> California Digital Library, ZD 1335)")
            else:
                log(f"HOST S{HOST_SRC} skipped: institution_id is {cur.institution_id}, not {HOST_OLD}")
            # 4. empty duplicate medRxiv
            names = dict(conn.execute(text("SELECT id, display_name FROM sources WHERE id IN (:l, :w)"),
                                      {"l": MERGE_LOSER, "w": MERGE_WINNER}).fetchall())
            assert names.get(MERGE_LOSER) == "medRxiv (Cold Spring Harbor Laboratory)", names
            assert names.get(MERGE_WINNER) == "medRxiv", names
            outcome = merge(conn, MERGE_LOSER, MERGE_WINNER, rule=MERGE_RULE,
                            detail={"job": 653, "ticket": 8055,
                                    "why": "empty duplicate (0 works, same Wikidata Q58465838)"})
            log(f"MERGE S{MERGE_LOSER} -> S{MERGE_WINNER}: {outcome}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
