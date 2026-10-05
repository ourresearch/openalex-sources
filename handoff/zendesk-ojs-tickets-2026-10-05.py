"""Registry edits for the outstanding OJS support tickets (triage 2026-10-05).

Run from ~/PycharmProjects/openalex-sources with its venv:
    .venv/bin/python <this file> [--dry-run] [--with-new-publishers]

--with-new-publishers also applies the edits that point at the six publisher
entities minted by mint_publishers.py (ids 4404678057..4404678062). Leave it off
until that Delta insert has succeeded.

Everything runs in ONE transaction; a before-state receipt is written next to
this file. Nothing is posted to Zendesk here.
"""
import json
import secrets
import string
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.expanduser("~/PycharmProjects/openalex-sources"))
from dotenv import load_dotenv
load_dotenv(os.path.expanduser("~/PycharmProjects/openalex-sources/.env"))
from sqlalchemy import text

from db import engine
from sources_lib import MatchContext, mint_source, resolve_issn_l, refresh_issns_column

DRY = "--dry-run" in sys.argv
WITH_PUBS = "--with-new-publishers" in sys.argv

P_TIB, P_21, P_AUSTRAL, P_SCCOT, P_AUT, P_RAHARJA = (
    4404678057, 4404678058, 4404678059, 4404678060, 4404678061, 4404678062)
P_DELFT, P_ROYAL_DANISH, P_UGR, P_EHU, P_ABMS, P_UDEM = (
    4310318546, 4310317204, 4404571743, 4310321029, 4310322325, 4310313007)

TUD = [158780706, 2738990503, 4389157789, 4387997389, 4387997476, 5407047506, 4404674245,
       4387291382, 4404670296, 4210229803, 4387997629, 4389158184, 4387289482, 4389157787,
       4387292231, 4389157594, 4390297236, 4389157788, 4389157785, 5407047363]
TIB = [4210204555, 4390296887, 4220650788, 4387292475, 4394708522, 5407027134, 5407036457,
       4387281391, 4210189716, 4387290664, 4210233641, 4404675833, 4404674246, 5407050839,
       5407036535, 4387290800, 7407058799]
RAHARJA = [4210170622, 4210221692, 4210174159, 4210208522, 4210218183, 4210237027,
           4210179464, 4387292415]

OA_OVERRIDES = {
    7407056188: "ZD 22017 Harvest (AUT, diamond, CC BY, not a Beacon mint)",
    7407058223: "ZD 22009 Punto y Coma (Universidad del Sagrado Corazon, diamond)",
    7407058046: "ZD 22564 Journal of 21 September University (diamond, 30/35 cc-by)",
    4387292231: "ZD 22642 TU Delft OPEN diamond platform", 4387997629: "ZD 22642 TU Delft OPEN diamond platform",
    4389157785: "ZD 22642 TU Delft OPEN diamond platform", 4389157788: "ZD 22642 TU Delft OPEN diamond platform",
    4404670296: "ZD 22642 TU Delft OPEN diamond platform", 4404674245: "ZD 22642 TU Delft OPEN diamond platform",
    5407047363: "ZD 22642 TU Delft OPEN diamond platform", 5407047506: "ZD 22642 TU Delft OPEN diamond platform",
    4387290800: "ZD 3292/22076/23348 TIB Open Publishing diamond platform", 4394708522: "ZD 3292/22076/23348 TIB Open Publishing diamond platform",
    4404675833: "ZD 3292/22076/23348 TIB Open Publishing diamond platform", 5407036535: "ZD 3292/22076/23348 TIB Open Publishing diamond platform",
    5407050839: "ZD 3292/22076/23348 TIB Open Publishing diamond platform", 7407058799: "ZD 3292/22076/23348 TIB Open Publishing diamond platform",
}

ALNUM = string.ascii_lowercase + string.digits
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "dry_run": DRY,
           "with_new_publishers": WITH_PUBS, "before": {}, "actions": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        receipt["before"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}


def upd(conn, sid, **fields):
    sets = ", ".join(f"{k} = :{k}" for k in fields)
    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"),
                     {**fields, "id": sid}).rowcount
    assert n == 1, (sid, fields)
    log(f"UPDATE sources {sid}: {fields}")


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [row.display_name or ""]}
    gained = [t for t in titles if t and t.strip().lower() not in have]
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained), "id": sid})
        log(f"alternate_titles {sid} += {gained}")


def merge(conn, loser, winner, rule, detail, drop_issns=()):
    """Post-D1 merge: ISSNs/endpoints/datacite/beacon/override links move to the
    winner, loser names become winner alternate titles, ledger row written, loser
    DELETED (walden CreateSources anti-joins source_merge losers)."""
    rows = {r.id: r for r in conn.execute(
        text("SELECT id, display_name, alternate_titles, override_timestamp, issn_l FROM sources WHERE id IN (:l, :w)"),
        {"l": loser, "w": winner})}
    assert loser in rows and winner in rows, (loser, winner)
    assert rows[loser].override_timestamp is None, f"loser {loser} is curator-overridden"
    conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :l"), {"l": loser})
    if drop_issns:
        n = conn.execute(text("DELETE FROM source_issn WHERE source_id = :l AND issn = ANY(:d)"),
                         {"l": loser, "d": list(drop_issns)}).rowcount
        log(f"merge {loser}->{winner}: dropped ISSNs {list(drop_issns)} ({n} rows)")
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
                 {"l": loser, "w": winner, "r": rule, "d": json.dumps(detail)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    log(f"MERGED {loser} -> {winner} ({rule}): issns moved {moved}, endpoints {ep}, datacite {dc}, beacon {bm}")


def register_endpoint(conn, pmh_url, pmh_set, source_id, journal_host=True):
    existing = conn.execute(text("SELECT id FROM oai_pmh_endpoint WHERE pmh_url = :u AND pmh_set IS NOT DISTINCT FROM :s"),
                            {"u": pmh_url, "s": pmh_set}).fetchone()
    if existing:
        log(f"endpoint exists {existing.id} for {pmh_url} [{pmh_set}] - not re-registered")
        return existing.id
    eid = "".join(secrets.choice(ALNUM) for _ in range(20))
    conn.execute(text(
        "INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
        "source_id, is_journal_host, green_scrape, rand) "
        "VALUES (:id, :u, :s, 'oai_dc', true, true, 'active', :sid, :jh, true, random())"),
        {"id": eid, "u": pmh_url, "s": pmh_set, "sid": source_id, "jh": journal_host})
    log(f"REGISTERED endpoint {eid} {pmh_url} [{pmh_set}] -> source {source_id} journal_host={journal_host}")
    return eid


def mint(conn, ctx, name, stype, **kw):
    sid = mint_source(conn, ctx, name, source_type=stype, **kw)
    log(f"MINTED source {sid} '{name}' ({stype}) {kw}")
    return sid


class DryRun(Exception):
    pass


def main():
    try:
        _run()
    except DryRun:
        print("dry run rolled back; the actions listed above were NOT applied")


def _run():
    with engine.begin() as conn:
        snapshot(conn, [4210237689, 4387286140, 4404663812, 4306505365, 154365192, 4306531837, 4306526737,
                        7407065488, 4210206229, 4210223969, 4210188977, 4210173069, 4210178450, 2898597390,
                        7407058223, 7407056188, 7407057786, 5407054642, 7407058046, 5407036096, 4306401213,
                        130340731] + TUD + TIB + RAHARJA)
        ctx = MatchContext(conn)

        # --- host organisation / homepage edits with existing publisher entities
        upd(conn, 4210237689, publisher_id=P_ROYAL_DANISH, publisher="Royal Danish Library")              # ZD 2595/3286
        upd(conn, 4387286140, publisher_id=P_ABMS, homepage_url="https://www.biomolbiomed.com")             # ZD 4518
        upd(conn, 4404663812, publisher_id=P_EHU, publisher="Universidad del País Vasco / Euskal Herriko Unibertsitatea",
            homepage_url="https://ojs.ehu.eus/index.php/cabas/index")                                       # ZD 4833
        upd(conn, 4210173069, publisher_id=P_UDEM, publisher="Université de Montréal")                       # ZD 7092/7358/8121
        for sid in TUD:                                                                                      # ZD 22642
            conn.execute(text("UPDATE sources SET publisher_id = :p, publisher = COALESCE(publisher, 'TU Delft OPEN Publishing'), "
                              "updated_date = now() WHERE id = :id AND publisher_id IS NULL"), {"p": P_DELFT, "id": sid})
        log(f"TU Delft: publisher_id={P_DELFT} on null-host sources among {len(TUD)}")

        # --- title / APC / junk-ISSN fixes
        upd(conn, 4210188977, display_name="Revista de Arquitectura (Bogotá)",                               # ZD 6999
            display_name_before_override="Revista de Arquitectura", override_timestamp=datetime.now(timezone.utc))
        add_alt_titles(conn, 4210188977, ["Revista de Arquitectura"])
        upd(conn, 4210206229, apc_usd=None, apc_prices=None)                                                 # ZD 6565 (DOAJ: has_apc false)
        n = conn.execute(text("DELETE FROM source_issn WHERE source_id = 7407058046 AND issn = '5937-2958'")).rowcount
        refresh_issns_column(conn, 7407058046)
        log(f"MASJ 7407058046: removed reversed junk ISSN 5937-2958 ({n})")
        upd(conn, 7407058046, homepage_url="https://ojs.21umas.edu.ye/masj/en")

        # --- merges (Teorema, RELat, Cabas twin, RJCCN twin, FQS repo-typed twin)
        merge(conn, 154365192, 4306531837, "zendesk_6231_teorema_issn_twin",
              {"ticket": 6231, "note": "ISSN source (71 works) folded into the MAG-named source that holds 589 works via name fallback"})
        upd(conn, 4306531837, publisher_id=P_UGR, publisher="Editorial Universidad de Granada",
            homepage_url="https://revistaseug.ugr.es/index.php/teorema/index", country_code="ES", country="Spain")
        merge(conn, 7407065488, 4306526737, "zendesk_6318_relat_beacon_twin",
              {"ticket": [6318, 7443, 7444, 8976], "note": "Beacon mint (0 works) folded into the MAG-named source holding 282 works via name fallback; beacon flag moves"})
        upd(conn, 4306526737, homepage_url="https://recyt.fecyt.es/index.php/rel", publisher="Sociedad de Estudios Latinos",
            country_code="ES", country="Spain")
        merge(conn, 4306505365, 4404663812, "zendesk_4833_cabas_empty_twin", {"ticket": 4833, "note": "0 works, no ISSN"})
        merge(conn, 7407057786, 5407054642, "zendesk_22095_rjccn_bogus_issn_twin",
              {"ticket": 22095, "note": "ISSN 1687-1073 belongs to another journal (ISSN portal); dropped, not moved"},
              drop_issns=["1687-1073"])
        upd(conn, 5407054642, homepage_url="https://rjccn.org/index.php/rjccn")
        n = conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = 130340731, is_journal_host = true "
                              "WHERE id = 'd4c23c3df74ad9f390a'")).rowcount
        log(f"FQS endpoint d4c23c3df74ad9f390a -> journal 130340731, journal_host ({n})")
        merge(conn, 4306401213, 130340731, "zendesk_5534_fqs_repo_typed_twin",
              {"ticket": 5534, "note": "repository-typed FQS source; its endpoint now binds to the journal"})

        # --- OA curation overrides (final word for is_oa_high_oa_rate; apply_oa_flags applies them)
        for sid, note in OA_OVERRIDES.items():
            conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, true, :n) "
                              "ON CONFLICT (source_id) DO UPDATE SET curated_is_oa = true, note = EXCLUDED.note"),
                         {"s": sid, "n": note})
        log(f"source_oa_override: {len(OA_OVERRIDES)} rows curated_is_oa=true")

        # --- endpoints
        n = conn.execute(text("UPDATE oai_pmh_endpoint SET ready_to_run = true, is_journal_host = true "
                              "WHERE id = '9v23g3kaxikjhbzamwxc'")).rowcount
        log(f"BRJSSH endpoint 9v23g3kaxikjhbzamwxc enabled ({n})")                                           # ZD 24513
        register_endpoint(conn, "https://recyt.fecyt.es/index.php/rel/oai", None, 4306526737)                # ZD 6318
        register_endpoint(conn, "https://ojs.21umas.edu.ye/masj/en/oai", None, 7407058046)                   # ZD 22552/22564
        jas = mint(conn, ctx, "Journal of Administrative Sciences", "journal",
                   publisher="21 September University for Medical and Applied Sciences",
                   homepage_url="https://ojs.21umas.edu.ye/jas/en", country_code="YE", country="Yemen")
        register_endpoint(conn, "https://ojs.21umas.edu.ye/jas/en/oai", None, jas)
        irss = mint(conn, ctx, "Institutional Repository for Scientific Scholarship at 21 September University for Medical and Applied Sciences",
                    "repository", homepage_url="https://ojs.21umas.edu.ye/irss/en", country_code="YE", country="Yemen")
        upd(conn, irss, institution_id=4387156351)
        register_endpoint(conn, "https://ojs.21umas.edu.ye/irss/en/oai", None, irss, journal_host=False)
        jedl = mint(conn, ctx, "Journal of Education and Development Lab", "journal",
                    publisher="Education and Development Lab", homepage_url="https://jedl.us/index.php/jedl",
                    country_code="BD", country="Bangladesh")
        register_endpoint(conn, "https://jedl.us/index.php/jedl/oai", None, jedl)                            # ZD 24928
        conv = mint(conn, ctx, "Convergence Africana: Journal of Integrated Studies", "journal",
                    homepage_url="https://convergenceafricana.com/index.php/convergenceafricana",
                    country_code="NG", country="Nigeria")
        register_endpoint(conn, "https://convergenceafricana.com/index.php/convergenceafricana/oai", None, conv)  # ZD 24936
        receipt["minted"] = {"jas": jas, "irss": irss, "jedl": jedl, "convergence_africana": conv}

        # --- edits that need the six NEW publisher entities
        if WITH_PUBS:
            for sid in TIB:                                                                                  # ZD 3292/22076/23348
                upd(conn, sid, publisher_id=P_TIB, publisher="TIB Open Publishing")
            upd(conn, 7407058046, publisher_id=P_21)                                                         # ZD 22564
            upd(conn, jas, publisher_id=P_21)
            upd(conn, 4210178450, publisher_id=P_AUSTRAL, homepage_url="https://ojs.austral.edu.ar/australcomunicacion/")  # ZD 7796
            upd(conn, 2898597390, publisher_id=P_SCCOT, publisher="Sociedad Colombiana de Cirugía Ortopédica y Traumatología",
                homepage_url="https://revistasccot.org/index.php/rccot/")                                   # ZD 8610
            upd(conn, 4210223969, publisher_id=P_AUT, publisher="Auckland University of Technology",
                homepage_url="https://ojs.aut.ac.nz/ethnographic-edge/")                                    # ZD 6691
            upd(conn, 7407056188, publisher_id=P_AUT)                                                        # ZD 22017
            for sid in RAHARJA:                                                                              # ZD 23469
                upd(conn, sid, publisher_id=P_RAHARJA)
        else:
            log("SKIPPED publisher-dependent edits (TIB x17, 21 Sept, Austral, SCCOT, AUT x2, Raharja x8): run again with --with-new-publishers")

        if DRY:
            log("DRY RUN: rolling back")
            raise DryRun()
    out = __file__.replace(".py", f"_receipt_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json")
    json.dump(receipt, open(out, "w"), indent=1, ensure_ascii=False)
    print("receipt", out)


if __name__ == "__main__":
    main()
