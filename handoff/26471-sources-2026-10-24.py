"""Registry edits for Zendesk ticket 26471 (Eric Jeangirard, MESR / scanR, 2026-10-23):
French platform sources plus country codes on "<Publisher> eBooks" sources (Kyle: "yes", 2026-10-24).
Every value was checked against the live API and ROR on 2026-10-23.

  Cairn.info    S4406923042  type repository -> other; host -> I4405258322 Cairn.info (ROR 00faswz36)
  OpenEdition   S4306402281  type repository -> other; host -> I4387156132 OpenEdition Center (ROR 02aja8v82); country FR
  OE eBooks     S4306477199  merged into S4306402281 (3 primary works, 11 locations)
  Hypotheses    S4306401026  type repository -> other (academic blog platform; host already OpenEdition Center)
  Persee        S4406923040  name -> "Persée"; host -> I4405254588 Persée (ROR 0200cc312); type stays repository
  HAL           S4306402512  name -> "HAL"; host CNRS -> I4405252976 CCSD (ROR 00680hx61);
                             merged in: S4406922276 (harvests hal.science at large, 211K works), S4406922470 HAL - CNAM,
                             S4306400468 HAL (Universite Cote d'Azur)
  Numdam        S4306402264  host Universite Savoie Mont Blanc -> I4405278365 MathDoc (ROR 05vcv0p21); country FR
  eBooks        country_code = the host publisher's single country, on the 1,183 null-country "<X> eBooks"
                sources in handoff/26471-ebooks-country.csv (90 FR). Works don't embed a source's country.

Run on the dyno (dry run by default; everything in ONE transaction):
    heroku run -a openalex-sources --no-tty "python handoff/26471-sources-2026-10-24.py"
    heroku run -a openalex-sources --no-tty "python handoff/26471-sources-2026-10-24.py --execute"
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker,
because the dyno's disk is thrown away; save it from the terminal.

Merges follow the post-D1 pattern of handoff/zendesk-ojs-tickets-2026-10-05.py
(loser deleted, ledger row in source_merge); jobs.merge_duplicates is disabled.
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name, refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
TICKET = 26471
APPROVED = "kyle 2026-10-24"

CAIRN, OPENEDITION, OE_EBOOKS, HYPOTHESES, PERSEE, HAL, NUMDAM = (
    4406923042, 4306402281, 4306477199, 4306401026, 4406923040, 4306402512, 4306402264)
HAL_LOSERS = {4406922276: "INRIA a CCSD electronic archive server",
              4406922470: "HAL - CNAM",
              4306400468: "HAL (Université Côte d'Azur)"}
I_CAIRN, I_OPENEDITION, I_PERSEE, I_CCSD, I_MATHDOC = 4405258322, 4387156132, 4405254588, 4405252976, 4405278365

EXPECT = {
    CAIRN: "Cairn.info",
    OPENEDITION: "OpenEdition (OpenEdition)",
    OE_EBOOKS: "OpenEdition eBooks",
    HYPOTHESES: "Hypotheses (OpenEdition)",
    PERSEE: "Persée (Ministère de lEnseignement supérieur et de la Recherche)",
    HAL: "HAL (Le Centre pour la Communication Scientifique Directe)",
    NUMDAM: "French digital mathematics library (Numdam)",
    **HAL_LOSERS,
}

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "ticket": TICKET,
           "before": {}, "ebooks_skipped": {}, "actions": [], "after": {}}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        receipt["before"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}


def expect_name(conn, sid, name):
    row = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    assert row is not None, f"source {sid} missing"
    assert normalize_name(row.display_name) == normalize_name(name), (sid, row.display_name, name)


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


def rename(conn, sid, new_name):
    old = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).scalar()
    upd(conn, sid, display_name=new_name, display_name_before_override=old,
        override_timestamp=datetime.now(timezone.utc))
    add_alt_titles(conn, sid, [old])


def set_host_institution(conn, sid, inst_id):
    """walden shows institution_id as the host when the source is a repository or publisher_id is NULL."""
    upd(conn, sid, institution_id=inst_id, publisher_id=None)


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in handoff/zendesk-ojs-tickets-2026-10-05.py."""
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
                 {"l": loser, "w": winner, "r": rule, "d": json.dumps(detail)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    log(f"MERGED {loser} -> {winner} ({rule}): issns moved {moved}, endpoints {ep}, datacite {dc}, beacon {bm}")


def country_name(conn, code):
    """The registry's own most common spelling for this code (no country library on the dyno)."""
    return conn.execute(text("SELECT country FROM sources WHERE country_code = :c AND country IS NOT NULL "
                             "GROUP BY country ORDER BY COUNT(*) DESC LIMIT 1"), {"c": code}).scalar()


def ebooks(conn):
    rows = list(csv.DictReader(open(os.path.join(HERE, "26471-ebooks-country.csv"), encoding="utf-8")))
    names, done, by_code = {}, 0, {}
    for r in rows:
        sid, code = int(r["source_id"]), r["country_code"]
        cur = conn.execute(text("SELECT display_name, country_code FROM sources WHERE id = :id"), {"id": sid}).fetchone()
        if cur is None or cur.country_code is not None or normalize_name(cur.display_name) != normalize_name(r["display_name"]):
            receipt["ebooks_skipped"][str(sid)] = None if cur is None else [cur.display_name, cur.country_code]
            continue
        if code not in names:
            names[code] = country_name(conn, code)
        conn.execute(text("UPDATE sources SET country_code = :c, country = COALESCE(country, :n), updated_date = now() "
                          "WHERE id = :id"), {"c": code, "n": names[code], "id": sid})
        done += 1
        by_code[code] = by_code.get(code, 0) + 1
    log(f"eBooks country_code: set on {done} of {len(rows)}; skipped {len(receipt['ebooks_skipped'])} "
        f"(gone, renamed or already set); FR {by_code.get('FR', 0)}; codes with no registry spelling: "
        f"{sorted(c for c, n in names.items() if n is None)}")


class DryRun(Exception):
    pass


def main():
    ids = set(EXPECT)
    try:
        with engine.begin() as conn:
            snapshot(conn, ids)
            for sid, name in EXPECT.items():
                expect_name(conn, sid, name)
            france = country_name(conn, "FR")
            assert france, "no registry spelling for FR"

            # 1 Cairn.info, 2 OpenEdition, 5 Hypotheses: publisher-side platforms, not repositories
            set_host_institution(conn, CAIRN, I_CAIRN)
            set_host_institution(conn, OPENEDITION, I_OPENEDITION)
            upd(conn, OPENEDITION, country_code="FR", country=france)
            for sid in (CAIRN, OPENEDITION, HYPOTHESES):
                upd(conn, sid, type="other")
            # 4 OpenEdition eBooks folded into OpenEdition
            merge(conn, OE_EBOOKS, OPENEDITION, "zendesk_26471_openedition_ebooks",
                  {"ticket": TICKET, "approved_by": APPROVED,
                   "note": "3-work ebook-platform stub; Eric Jeangirard asked to merge it into OpenEdition"})
            # 6 Persee
            rename(conn, PERSEE, "Persée")
            set_host_institution(conn, PERSEE, I_PERSEE)
            # 7 HAL
            rename(conn, HAL, "HAL")
            set_host_institution(conn, HAL, I_CCSD)
            for loser in HAL_LOSERS:
                merge(conn, loser, HAL, "zendesk_26471_hal_portal",
                      {"ticket": TICKET, "approved_by": APPROVED,
                       "note": "HAL portal / harvest of hal.science under its own source; Eric Jeangirard asked to fold it into HAL"})
            # 9 Numdam
            set_host_institution(conn, NUMDAM, I_MATHDOC)
            upd(conn, NUMDAM, country_code="FR", country=france)
            # 10 "<Publisher> eBooks" country codes
            ebooks(conn)

            for r in conn.execute(text("SELECT id, display_name, type, publisher_id, institution_id, country_code, "
                                       "alternate_titles FROM sources WHERE id = ANY(:i) ORDER BY id"),
                                  {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} type={r['type']} pub={r['publisher_id']} "
                      f"inst={r['institution_id']} cc={r['country_code']}")
            gone = ids - {int(k) for k in receipt["after"]}
            log(f"merged-away sources gone: {sorted(gone)}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
