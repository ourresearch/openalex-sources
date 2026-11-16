"""Registry fixes for the support tickets on oxjob #875 (Zenodo-deposited journal articles under their journal
source), support factory #2254, 2026-11-15. Every journal was re-checked live on 2026-11-15 (receipts in the #875
job, work/probe_2026-11-15.json).

Why minting is enough: since #766/#693 the DataCite tier puts a DataCite location on the journal whose ISSN the
record names as IsPartOf. Zenodo writes that relation from the ISSN box of a deposit's Journal section. RDIPP
(ZD 27442, S7407122481, minted 13 Nov) proves it: its Zenodo deposits sat on the journal after one nightly.

Mints (Kyle's add bar, SOP 4b, 22 Oct: a source, a list we load, or 3+ years of records; no OAI feed, as for RDIPP,
since the DataCite records carry everything):
  ZD 5994   SANUM: Revista Cientifico-Sanitaria  2530-5468  erih-plus, latindex; 416 deposits name the ISSN
  ZD 24213  Legal Bulletin                       2658-5448  pkp-beacon, russia-white-list; DOAJ; 566 deposits
  ZD 23934  Dan-ag: Journal of Educational Studies 2244-0518  deposits since 2015; 39 deposits
            (e-ISSN 3155-6833 left out: provisional, international registration pending, per the editor)
  ZD 4880   Journal on Tourism & Sustainability  2732-7566  Zenodo community since Dec 2022 (70 records); its
            deposits leave the ISSN box empty, so they group once the journal fills it
  ZD 26794  Abstracts & Proceedings (International Conference on Political Economy) 3049-4834, type conference;
            papers 2017-2024; deposits leave the ISSN box empty (same as above)
Other edits:
  ZD 27127  S2737730586 Neutrosophic Sets and Systems + e-ISSN 2331-608X (DOAJ: eissn 2331-608X, pissn 2331-6055;
            no source owns it)
  ZD 25908  S7407062341 display name drops '(HEC Recognised)': Crossref's title for 3007-0910 is 'International
            Journal of Agriculture Innovations and Cutting-Edge Research'
  ZD 25909  S7407059680 'International "Journal of Academic Research for Humanities"' -> Crossref's title for
            2663-192X, 'Journal of Academic Research for Humanities' (also the ISSN key title the editor names)

Run on desk (dry run by default; everything in ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob875-zenodo-journals-2026-11-15.py [--execute]
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker.
Revert: minted sources: delete their source_oa_override, source_issn and sources rows (no endpoint, no merge);
ISSN add: DELETE FROM source_issn WHERE issn = '2331-608X' (then sync_issn_l_flags); renames: set display_name back
to the receipt's `before` value and display_name_before_override / override_timestamp to NULL.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import (MatchContext, insert_issns, mint_source, normalize_name, recompute_listed_in,
                         resolve_issn_l)

EXECUTE = "--execute" in sys.argv
JOB = 875
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); Kyle's add bar (SOP 4b, 2026-10-22)"

MINTS = [
    dict(ticket=5994, name="SANUM: Revista Científico-Sanitaria", issns=["2530-5468"], type="journal",
         publisher="Servicios Públicos de UGT-Andalucía", homepage="https://revistacientificasanum.com",
         cc="ES", country="Spain",
         oa="ZD 5994 (#875): Diamond OA per the journal homepage ('Acceso Abierto Diamante'); 198/200 Zenodo DataCite "
            "deposits naming ISSN 2530-5468 are CC BY-NC-ND 4.0, 2 CC BY 4.0"),
    dict(ticket=24213, name="Legal Bulletin", issns=["2658-5448"], type="journal",
         publisher="LLC «MIAS Expert»", homepage="https://en.legalbulletin.ru", cc="RU", country="Russia",
         oa="ZD 24213 (#875): in DOAJ (CC BY); 200/200 sampled Zenodo DataCite deposits naming ISSN 2658-5448 are CC BY 4.0"),
    dict(ticket=23934, name="Dan-ag: Journal of Educational Studies", issns=["2244-0518"], type="journal",
         publisher="Xavier University - Ateneo de Cagayan, School of Education", homepage="https://dan-ag.xu.edu.ph",
         cc="PH", country="Philippines",
         oa="ZD 23934 (#875): open access per the journal; 23/23 Zenodo DataCite deposits naming ISSN 2244-0518 are CC BY 4.0"),
    dict(ticket=4880, name="Journal on Tourism & Sustainability", issns=["2732-7566"], type="journal",
         publisher=None, homepage="https://journal.ontourism.academy", cc=None, country=None,
         oa="ZD 4880 (#875): every article published on Zenodo; 25/25 sampled records of Zenodo community 'jots' are CC BY 4.0"),
    dict(ticket=26794, name="Abstracts & Proceedings (International Conference on Political Economy)",
         issns=["3049-4834"], type="conference", publisher="IJOPEC Publication", homepage="https://icopec.org",
         cc="GB", country="United Kingdom", alt=["ICOPEC Abstracts & Proceedings"],
         oa="ZD 26794 (#875): papers deposited on Zenodo under CC BY 4.0 (2/2 DataCite records checked; requester states all 170)"),
]
ADD_ISSNS = [(2737730586, "Neutrosophic sets and systems", ["2331-608X"], 27127)]
RENAMES = [
    (7407062341, "International Journal of Agriculture Innovations and Cutting-Edge Research (HEC Recognised)",
     "International Journal of Agriculture Innovations and Cutting-Edge Research", 25908),
    (7407059680, 'International "Journal of Academic Research for Humanities"',
     "Journal of Academic Research for Humanities", 25909),
]

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
           "before": {}, "actions": [], "minted": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        row = {k: (None if v is None else str(v)) for k, v in r.items()}
        row["_issns"] = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": r["id"]})]
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


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            snapshot(conn, [s for s, *_ in ADD_ISSNS] + [s for s, *_ in RENAMES])
            ctx = MatchContext(conn, name_link=True)
            for m in MINTS:
                owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                     {"i": m["issns"]}).fetchall()
                assert not owned, f"{m['name']}: ISSN already on a source {owned}"
                twins = [t for t, _ in ctx.name_index.get(normalize_name(m["name"]), []) if not ctx.sid_to_issns.get(t)]
                assert not twins, f"{m['name']}: ISSN-less namesake(s) {twins}: add the ISSN there instead"
                sid = mint_source(conn, ctx, m["name"], source_type=m["type"], issns=m["issns"],
                                  publisher=m["publisher"], homepage_url=m["homepage"],
                                  country_code=m["cc"], country=m["country"])
                if m.get("alt"):
                    add_alt_titles(conn, sid, m["alt"])
                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, true, :n) "
                                  "ON CONFLICT (source_id) DO NOTHING"), {"s": sid, "n": m["oa"]})
                receipt["minted"].append({"source_id": sid, "name": m["name"], "issns": m["issns"], "type": m["type"],
                                          "ticket": m["ticket"]})
                log(f"MINTED S{sid} {m['type']} {m['name']!r} {m['issns']} (ZD {m['ticket']}); OA override: {m['oa']}")
            for sid, expect, issns, ticket in ADD_ISSNS:
                row = conn.execute(text("SELECT display_name, type FROM sources WHERE id = :id"), {"id": sid}).fetchone()
                assert row and normalize_name(row.display_name) == normalize_name(expect) and row.type == "journal", (sid, row)
                owned = dict(conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"), {"i": issns}).fetchall())
                assert all(v == sid for v in owned.values()), f"S{sid}: ISSN on another source {owned}"
                missing = [i for i in issns if i not in owned]
                have = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": sid})]
                insert_issns(conn, sid, missing, resolve_issn_l(conn, sorted(set(have) | set(issns)), member_only=True))
                conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": sid})
                log(f"ISSNS S{sid} {expect!r} += {missing} (ZD {ticket})")
            for sid, old, new, ticket in RENAMES:
                cur = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).scalar()
                assert cur == old, (sid, cur, old)
                n = conn.execute(text("UPDATE sources SET display_name = :n, display_name_before_override = :o, "
                                      "override_timestamp = now(), updated_date = now() WHERE id = :id"),
                                 {"n": new, "o": old, "id": sid}).rowcount
                assert n == 1
                add_alt_titles(conn, sid, [old.replace('"', "")])
                log(f"RENAME S{sid} {old!r} -> {new!r} (ZD {ticket})")
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
