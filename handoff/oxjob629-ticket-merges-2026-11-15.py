"""Registry fixes for the support tickets on oxjob #629 (duplicate source profiles), support factory #2254, 2026-11-15.
Every pair was re-checked on the live API and in this registry on 2026-11-15 (receipts in the #629 job, work/).

Merges (loser -> winner; post-D1 pattern of handoff/26471-sources-2026-10-24.py: loser deleted, ledger row in
source_merge, which also serves the merged-ID redirect, #1124):
  ZD 1539   S4306518830 'Loading...' (1 work, no ISSN)            -> S4210241036 'Loading' (ISSN 1923-2691)
  ZD 1128   S2764676769 'Journal of Interactive Systems' 2236-3297 -> S4210193165 'Journal on Interactive Systems' 2763-7719
            (renamed 2021, same SBC journal; D5 2026-07-17: current name wins, merging across ISSNs allowed)
  ZD 4179/4246  S4306519048, S4306519049 (Magister, no ISSN)      -> S2898136286 Magister (0212-6796, 2340-4728);
            winner renamed to the current title, homepage -> the Oviedo OJS, host -> P4310321244 University of Oviedo
  ZD 4185   S4306525453 Recerques (no ISSN)                       -> S4387285788 (0210-380X); winner name gets its
            punctuation back, homepage -> RACO
  ZD 4886   S4377196128 'Comptes Rendus Mathematique' (repository, 0 works) -> S128122872 (the journal)
  ZD 6367/22931  S4210206586 (0012-320X, 1965-1997 title)          -> S4210186007 (1160-7742); winner renamed to the
            current title 'La Revue des Sciences de Gestion', homepage larsg.fr, + ISSNs 1291-2905 and 3040-1593
            (both in the ISSN IC map, neither on any source)
  ZD 4250/4691  S4306463825 'Quae eBooks', S4306469880 'Ed. Quae eBooks' -> S4306463182 'Editions Quae eBooks'
  ZD 4024   S4306466917 'bu,press eBooks'                          -> S4306466692 'Bozen-Bolzano University Press eBooks'
  ZD 8250   S4306401714 Scientia et technica (repository record of the journal's own OJS) -> S4306529292 (the journal)
  ZD 7952   S4377196330 'mediaTUM (Technical University of Munich)' -> S4306400453 mediaTUM; winner name shortened
  ZD 8179   S4406922546 'ELAR UrFU' (elar.usu.ru, the archive's old domain) -> S4306401523 (elar.urfu.ru)
  ZD 7377   S7407055458 'TSpace' (no endpoint)                     -> S4306400261 'TSpace (University of Toronto)'
  ZD 23287  S4306527878 'Revista Processando o Saber' (0 works)    -> S7407061304 'Processando o Saber'
  ZD 27304  S7407063507 (1010-5026), S7407063508 (2091-1114)       -> S4404669622 InfoScience Trends (3041-9875):
            Crossref lists InfoScience Trends under all three ISSNs, and every DOI on them is an InfoScience Trends
            article (prefixes 10.61882 / 10.66224, suffix ist.*); the ISSNs ride along so the Crossref feed can't re-mint
Other edits:
  ZD 6259   S4306532545 'Thelème/Thelème/Revista de filología francesa' -> 'Thélème: Revista Complutense de Estudios Franceses'
  ZD 1426   S4306401150 CORA host APC Microbiome Institute (I4210141138) -> University College Cork (I27577105)
  ZD 2161   SAE journals with no publisher -> P4322614468 SAE International (the entity the other SAE journals use)

Run on desk (dry run by default; everything in ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob629-ticket-merges-2026-11-15.py [--execute]
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker.
Revert: the receipt's `before` holds every touched row; a merged loser is restored by re-inserting its `before` row
and moving back the ISSNs / endpoints the log line names, then deleting its source_merge row.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import refresh_datacite_column, insert_issns, normalize_name, refresh_issns_column, resolve_issn_l, sync_issn_l_flags

EXECUTE = "--execute" in sys.argv
JOB = 629
APPROVED = "support factory #2254 (Jason 2026-11-15 yes; D5 2026-07-17 current name wins)"

# (loser, winner, tickets, note)
MERGES = [
    (4306518830, 4210241036, [1539], "'Loading...' no-ISSN record of Loading (1923-2691)"),
    (2764676769, 4210193165, [1128], "renamed 2021: Journal of Interactive Systems 2236-3297 -> Journal on Interactive Systems 2763-7719 (SBC); D5"),
    (4306519048, 2898136286, [4179, 4246], "Magister, University of Oviedo; no-ISSN record"),
    (4306519049, 2898136286, [4179, 4246], "Magister, University of Oviedo; no-ISSN record"),
    (4306525453, 4387285788, [4185], "Recerques, no-ISSN record"),
    (4377196128, 128122872, [4886], "repository-typed record of the journal's own OJS, 0 works"),
    (4210206586, 4210186007, [6367, 22931], "title history 0012-320X (1965-97) -> 1160-7742; publisher's request; D5"),
    (4306463825, 4306463182, [4250, 4691], "Editions Quae ebook platform name variant"),
    (4306469880, 4306463182, [4250, 4691], "Editions Quae ebook platform name variant (Agritrop)"),
    (4306466917, 4306466692, [4024], "bu,press = Bozen-Bolzano University Press"),
    (4306401714, 4306529292, [8250], "repository record of the journal's own OJS"),
    (4377196330, 4306400453, [7952], "mediaTUM twin (mediatum.ub.tum.de vs mediatum2.ub.tum.de)"),
    (4406922546, 4306401523, [8179], "ELAR UrFU on its old domain elar.usu.ru"),
    (7407055458, 4306400261, [7377], "TSpace twin, same homepage, no endpoint"),
    (4306527878, 7407061304, [23287], "no-ISSN record, 0 works"),
    (7407063507, 4404669622, [27304, 22429, 22777, 22947], "InfoScience Trends DOIs (10.61882/ist.*) deposited under 1010-5026"),
    (7407063508, 4404669622, [27304, 22429, 22777, 22947], "InfoScience Trends DOIs (10.66224/ist.*) deposited under 2091-1114"),
]

EXPECT = {
    4306518830: "Loading...", 4210241036: "Loading",
    2764676769: "Journal of Interactive Systems", 4210193165: "Journal on Interactive Systems",
    4306519048: "Magister : revista de formación del profesorado e investigación educativa",
    4306519049: "Magister: Revista miscelánea de investigación", 2898136286: "Magister",
    4306525453: "Recerques: Història, economia i cultura", 4387285788: "Recerques història economia cultura",
    4377196128: "Comptes Rendus Mathématique", 128122872: "Comptes Rendus Mathématique",
    4210206586: "La Revue des Sciences de Gestion Direction et Gestion",
    4210186007: "La Revue des sciences de gestion/La Revue des sciences de gestion, Direction et gestion",
    4306463825: "Quae eBooks", 4306469880: "Ed. Quae eBooks", 4306463182: "Éditions Quæ eBooks",
    4306466917: "bu,press eBooks", 4306466692: "Bozen-Bolzano University Press eBooks",
    4306401714: "Scientia et technica (Universidad Tecnológica de Pereira)", 4306529292: "Scientia et technica",
    4377196330: "mediaTUM  (Technical University of Munich)",
    4306400453: "mediaTUM – the media and publications repository of the Technical University Munich (Technical University Munich)",
    4406922546: "Electronic Archive of Ural Federal University (ELAR UrFU)",
    4306401523: "Electronic scientific archive of UrFU (Ural Federal University)",
    7407055458: "TSpace", 4306400261: "TSpace (University of Toronto)",
    4306527878: "Revista Processando o Saber", 7407061304: "Processando o Saber",
    7407063507: "InfoScience Trends", 7407063508: "InfoScience Trends", 4404669622: "InfoScience Trends",
    4306532545: "Thélème/Thélème/Revista de filología francesa",
    4306401150: "Cork Open Research Archive (University College Cork, Ireland)",
}

RENAMES = {
    2898136286: "Magister: Revista de Formación del Profesorado e Investigación Educativa",
    4387285788: "Recerques: Història, Economia i Cultura",
    4210186007: "La Revue des Sciences de Gestion",
    4306400453: "mediaTUM (Technical University of Munich)",
    4306532545: "Thélème: Revista Complutense de Estudios Franceses",
}
HOMEPAGES = {
    2898136286: "https://reunido.uniovi.es/index.php/MSG",
    4387285788: "https://raco.cat/index.php/Recerques",
    4210186007: "https://larsg.fr/",
}
P_OVIEDO, P_SAE, I_UCC, I_APC_MICROBIOME = 4310321244, 4322614468, 27577105, 4210141138
REVUE_NEW_ISSNS = ["1291-2905", "3040-1593"]
SAE_JOURNAL_FILTER = ("type = 'journal' AND publisher_id IS NULL AND (id IN (4210202285, 2764470252) "
                      "OR display_name ILIKE 'SAE international journal%%')")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB,
           "before": {}, "actions": [], "after": {}}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        row = {k: (None if v is None else str(v)) for k, v in r.items()}
        row["_issns"] = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": r["id"]})]
        row["_endpoints"] = [x[0] for x in conn.execute(text("SELECT id FROM oai_pmh_endpoint WHERE source_id = :i"), {"i": r["id"]})]
        receipt["before"][str(r["id"])] = row


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
    gained = []
    for t in titles:
        if t and t.strip() and t.strip().lower() not in have:
            gained.append(t.strip())
            have.add(t.strip().lower())
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "id": sid})
        log(f"alternate_titles {sid} += {gained}")


def rename(conn, sid, new_name):
    old = conn.execute(text("SELECT display_name FROM sources WHERE id = :id"), {"id": sid}).scalar()
    if old == new_name:
        return
    upd(conn, sid, display_name=new_name, display_name_before_override=old,
        override_timestamp=datetime.now(timezone.utc))
    add_alt_titles(conn, sid, [old])


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in handoff/26471-sources-2026-10-24.py."""
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
    if dc:  # added 2026-11-20 (#1496), after this script ran: keep the derived column in step
        refresh_datacite_column(conn, winner)
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


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            sae = [r[0] for r in conn.execute(text(f"SELECT id FROM sources WHERE {SAE_JOURNAL_FILTER} ORDER BY id"))]
            ids = set(EXPECT) | set(sae)
            snapshot(conn, ids)
            for sid, name in EXPECT.items():
                expect_name(conn, sid, name)
            # guards on the non-merge edits
            cora = conn.execute(text("SELECT institution_id FROM sources WHERE id = 4306401150")).scalar()
            assert cora == I_APC_MICROBIOME, cora
            for issn in REVUE_NEW_ISSNS:
                assert conn.execute(text("SELECT 1 FROM issn_to_issnl WHERE issn = :i"), {"i": issn}).fetchone(), issn
                taken = conn.execute(text("SELECT source_id FROM source_issn WHERE issn = :i"), {"i": issn}).scalar()
                assert taken is None, (issn, taken)

            for loser, winner, tickets, note in MERGES:
                merge(conn, loser, winner, "zendesk_629_ticket_merge",
                      {"tickets": tickets, "oxjob": JOB, "approved_by": APPROVED, "note": note})
            for sid, name in RENAMES.items():
                rename(conn, sid, name)
            for sid, url in HOMEPAGES.items():
                upd(conn, sid, homepage_url=url)
            # Magister: host University of Oviedo (registry text already says so; the id pointed at CIG Media Group)
            upd(conn, 2898136286, publisher_id=P_OVIEDO, publisher="Universidad de Oviedo")
            # Revue des Sciences de Gestion: the two missing title-history ISSNs
            insert_issns(conn, 4210186007, REVUE_NEW_ISSNS, None)
            sync_issn_l_flags(conn, 4210186007)
            log(f"ISSNs 4210186007 += {REVUE_NEW_ISSNS}")
            # CORA: repositories show their institution as host
            upd(conn, 4306401150, institution_id=I_UCC)
            # SAE journals
            log(f"SAE journals with no publisher: {sae}")
            for sid in sae:
                upd(conn, sid, publisher_id=P_SAE, publisher="SAE International")

            for r in conn.execute(text("SELECT id, display_name, type, issn_l, issns, publisher_id, institution_id, "
                                       "homepage_url, alternate_titles FROM sources WHERE id = ANY(:i) ORDER BY id"),
                                  {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} issns={r['issns']} pub={r['publisher_id']} "
                      f"inst={r['institution_id']} hp={r['homepage_url']}")
            gone = ids - {int(k) for k in receipt["after"]}
            log(f"merged-away sources gone: {sorted(gone)}")
            assert gone == {m[0] for m in MERGES}, gone
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
