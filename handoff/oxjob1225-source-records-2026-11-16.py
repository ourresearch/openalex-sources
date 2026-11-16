"""Source records for nine support tickets on oxjob #1225 (support factory #2254), written off desk 2026-11-16
(lane #2263); run on desk from the openalex-sources checkout. Every journal was checked live and on its own site on
2026-11-16 (#1225 work/mints/summary.json + journal-add-<ticket>.csv; work/recheck_2026-11-16.json).

MINTS (RULINGS § 3 D5, Jason 16 Nov: a third party outside OpenAlex and the work confirms the venue exists; built from
the venue's own site). They bind on the next nightly: Crossref works by the container-title name fallback
(CreateLocationsWithSources final_display_name_fallback, unique normalized name; AMU and The Unjournal deposits carry
no ISSN) and DataCite (Zenodo) works by IsPartOf ISSN (Journal on Labour 23 works, IJPADE 7 of 17).
  ZD 22392  Annales Medicinae Urgentis   3044-4489 (online), 3044-3105 (print)  amu.hr editorial board, Hrcak, ISSN
            portal; CC BY 4.0 per amu.hr/copyright-and-licensing -> OA override. 47 Crossref DOIs 10.64266, no source now.
  ZD 22394  International Journal of Progression and Development in Education  2980-0986  ERIH PLUS + PKP beacon;
            CC BY 4.0, no APC per ijpade.com/index.php/pub/openaccesspolicy -> OA override. Its OJS feed is NOT
            registered here (records carry no DOI; harvesting would duplicate the Zenodo copies).
  ZD 24195  Journal on Labour   3107-572X (online), 3108-1029 (print)  cllra.com editorial board, NLU Delhi journals
            page, ISSN portal; host institution I866667741 (National Law University, Delhi); no OA claim on its site.
  ZD 7360   The Unjournal   (no ISSN)  unjournal.org, ROR 01yppp702 (I4405259233, host institution); Crossref (PubPub)
            registers its journal-article DOIs (10.21428/d28e8e57) with container-title 'The Unjournal' (13 of 55; the
            42 peer-review records have no container and stay unbound).
EDITS
  ZD 3860   S4363607345 'Specialpaedagogik' -> 'Specialpædagogik'; + ISSN 0107-0649 (its works' repository pages state
            it); homepage tidsskriftetspecialpaedagogik.dk, publisher text, DK. Subscription journal: no OA change.
  ZD 8199   S5407056695 'Research Announcements "Heritage BG"': homepage periodicals.uni-sofia.bg/index.php/AR-NBg;
            alternate titles (ISSN portal / Bulgarian); OA override (CC BY-NC-ND 4.0, site OpenAccess page).
  ZD 4144   S7407062200 'Aequitas Magazine': homepage aequitasmagazine.it; host institution I4405255386 (ROR 02ys9r941).
  ZD 6757   S7407062266 IMJPL: + former ISSN 2676-7619 ('Pure life', same ISSN-H); homepage; host institution
            I2800359231 (Al-Mustafa International University); MERGE S4306516001 'Journal of Pure Life' into it.
  ZD 9099   S4210226519 'Drassana': host institution I4405267464 (Museu Marítim de Barcelona); MERGE S4306508396
            'Drassana: revista del Museu Marítim' into it.
  ZD 6596   S2764580224 Chinese Journal of Ship Research: homepage english.ship-research.com; MERGE S4306542228
            '中国舰船研究' (its locations are www.ship-research.com/cn/article/doi/10.19693/j.issn.1673-3185.*) into it.
  ZD 2243   University of Minnesota publisher fixes (Kate Sheridan, UMN Libraries): Constitutional Commentary
            S2764695005 publisher_id 4310316492 (Princeton University Press) -> 4310315924 (University of Minnesota Law
            School); Minnesota Journal of Law, Science & Technology S4393917561 NULL -> 4310315924; four UMN Libraries
            titles (S4210169552 J. College Orientation, S4210198397 Panorama, S4210206938 J. Regional Medical Campuses,
            S4210209514 Minnesota J. Law & Inequality) 4310316571 (University of Minnesota) -> 4310316065 (University
            of Minnesota Libraries Publishing), as their sibling titles already read. Each only if the current value
            is the expected one.
  ZD 289    (premium) four arXiv-named venue ids retired in the 2023-03-28 consolidation (S2596994788, S2594936402,
            S2595115542, S4306503241) 404 today. They have no sources row (post-D1 a merge deletes the loser), so the
            fix is the ledger alone: one source_merge row each -> 4306400194 arXiv (Cornell University), rule
            zendesk_289_retired_arxiv_2023. notebooks/elastic/sync_merged_ids reads source_merge daily and the API
            then answers each id with a 301 to S4306400194. Guard: no sources row and no source_merge row for the id.
Host institution: CreateSourcesApi uses institution_id as a journal's host when publisher_id is NULL (Casey/Rohan
2026-08-14); this script sets it only where publisher_id IS NULL and institution_id IS NULL.

Run on desk, outside the End 2 End window (05:00 UTC until the night publishes); dry run by default, ONE transaction:
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-source-records-2026-11-16.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-source-records-2026-11-16.py --execute
Receipt: JSON after RECEIPT (and handoff/oxjob1225-source-records-2026-11-16-receipt.json with --execute); `before` holds
every touched row. Revert: delete minted sources (their source_issn + source_oa_override rows first); for edits put the
`before` values back and DELETE the added source_issn rows / override rows; un-merge per the #871 receipt pattern
(re-insert the loser's `before` row with OVERRIDING SYSTEM VALUE, move its ISSNs/endpoints back, DELETE FROM source_merge
WHERE rule = 'zendesk_1225'); the arXiv ledger rows: DELETE FROM source_merge WHERE rule = 'zendesk_289_retired_arxiv_2023'.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import (MatchContext, insert_issns, mint_source, normalize_name,  # noqa: E402
                         recompute_is_oa, recompute_listed_in, refresh_issns_column, resolve_issn_l)

EXECUTE = "--execute" in sys.argv
JOB = 1225
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); RULINGS § 3 D5 (Jason 2026-11-16); standing authority"
RECEIPT_PATH = os.path.join(HERE, "oxjob1225-source-records-2026-11-16-receipt.json")

MINTS = [
    dict(ticket=22392, name="Annales Medicinae Urgentis", issns=["3044-4489", "3044-3105"],
         publisher="Croatian Society of Emergency Medicine", homepage="https://amu.hr/", cc="HR", country="Croatia",
         institution=None,
         oa="ZD 22392 (#1225): all articles open access under CC BY 4.0 per https://amu.hr/copyright-and-licensing/"),
    dict(ticket=22394, name="International Journal of Progression and Development in Education", issns=["2980-0986"],
         publisher=None, homepage="https://ijpade.com/index.php/pub", cc="TR", country="Turkey", institution=None,
         alt=["IJPADE"],
         oa="ZD 22394 (#1225): free to read, no APC, CC BY 4.0 per https://ijpade.com/index.php/pub/openaccesspolicy"),
    dict(ticket=24195, name="Journal on Labour", issns=["3107-572X", "3108-1029"],
         publisher="Centre for Labour Law Research and Advocacy, National Law University Delhi",
         homepage="https://www.cllra.com/about-jol", cc="IN", country="India", institution=866667741, oa=None),
    dict(ticket=7360, name="The Unjournal", issns=[], publisher="The Unjournal", homepage="https://unjournal.org",
         cc=None, country=None, institution=4405259233, oa=None),
]
EDITS = [
    dict(ticket=3860, sid=4363607345, expect="specialpaedagogik", rename="Specialpædagogik", add_issns=["0107-0649"],
         homepage="https://tidsskriftetspecialpaedagogik.dk/",
         publisher="Interessentselskabet Specialpædagogik", cc="DK", country="Denmark"),
    dict(ticket=8199, sid=5407056695, expect='research announcements "heritage bg"',
         homepage="https://periodicals.uni-sofia.bg/index.php/AR-NBg",
         alt=['Bjuletin "Nasledstvo BG" - Naučni izvestija', 'Бюлетин "Наследство БГ" - Научни известия', "Nasledstvo BG"],
         oa="ZD 8199 (#1225): articles open access under CC BY-NC-ND 4.0 per "
            "https://periodicals.uni-sofia.bg/index.php/AR-NBg/en/OpenAccess"),
    dict(ticket=4144, sid=7407062200, expect="aequitas magazine", homepage="https://www.aequitasmagazine.it/",
         institution=4405255386),
    dict(ticket=6757, sid=7407062266, expect="international multidisciplinary journal of pure life",
         add_issns=["2676-7619"], homepage="https://p-l.journals.miu.ac.ir/", institution=2800359231,
         merge=[(4306516001, "journal of pure life")]),
    dict(ticket=9099, sid=4210226519, expect="drassana", institution=4405267464,
         merge=[(4306508396, "drassana: revista del museu marítim")]),
    dict(ticket=6596, sid=2764580224, expect="chinese journal of ship research",
         homepage="https://english.ship-research.com/", merge=[(4306542228, "中国舰船研究")]),
]
ARXIV = 4306400194
RETIRED_ARXIV = [2596994788, 2594936402, 2595115542, 4306503241]  # ZD 289, ids quoted by the requester (2023)
PUBLISHER_SETS = [  # (source_id, expected current publisher_id or None, new publisher_id, ticket)
    (2764695005, 4310316492, 4310315924, 2243),
    (4393917561, None, 4310315924, 2243),
    (4210169552, 4310316571, 4310316065, 2243),
    (4210198397, 4310316571, 4310316065, 2243),
    (4210206938, 4310316571, 4310316065, 2243),
    (4210209514, 4310316571, 4310316065, 2243),
]
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
           "before": {}, "actions": [], "minted": [], "merged": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, sid):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = :i"), {"i": sid}).mappings():
        row = {k: (v if isinstance(v, (list, dict, bool, int, type(None))) else str(v)) for k, v in r.items()}
        row["_issns"] = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": sid})]
        row["_endpoints"] = [x[0] for x in conn.execute(text("SELECT id FROM oai_pmh_endpoint WHERE source_id = :i"), {"i": sid})]
        row["_datacite_ids"] = [x[0] for x in conn.execute(
            text("SELECT datacite_id FROM source_datacite_id WHERE source_id = :i"), {"i": sid})]
        row["_oa_override"] = [dict(x) for x in conn.execute(
            text("SELECT * FROM source_oa_override WHERE source_id = :i"), {"i": sid}).mappings()]
        receipt["before"][str(sid)] = row
        return r
    return None


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [row.display_name or ""]}
    gained = [t.strip() for t in titles if t and t.strip() and t.strip().lower() not in have]
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "id": sid})
        log(f"alternate_titles S{sid} += {gained}")


def fill(conn, sid, col, val, ticket):
    cur = conn.execute(text(f"SELECT {col} FROM sources WHERE id = :id"), {"id": sid}).scalar()
    if cur in (None, ""):
        conn.execute(text(f"UPDATE sources SET {col} = :v, updated_date = now() WHERE id = :id"), {"v": val, "id": sid})
        log(f"S{sid} {col}: NULL -> {val!r} (ZD {ticket})")
    else:
        log(f"S{sid} {col} already {cur!r}; left (ZD {ticket})")


def set_host_institution(conn, sid, inst, ticket):
    r = conn.execute(text("SELECT publisher_id, institution_id FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    # The registry has no institutions table today, so this check is a no-op; the five ids were checked live 16 Nov.
    ok = conn.execute(text("SELECT 1 FROM institutions WHERE id = :i"), {"i": inst}).fetchone() \
        if conn.execute(text("SELECT to_regclass('public.institutions')")).scalar() else True
    if r.publisher_id is None and r.institution_id is None and ok:
        conn.execute(text("UPDATE sources SET institution_id = :i, updated_date = now() WHERE id = :id"), {"i": inst, "id": sid})
        log(f"S{sid} institution_id: NULL -> {inst} (host; ZD {ticket})")
    else:
        log(f"S{sid} host left: publisher_id={r.publisher_id} institution_id={r.institution_id} (ZD {ticket})")


def oa_override(conn, sid, note, ticket):
    n = conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, true, :n) "
                          "ON CONFLICT (source_id) DO NOTHING"), {"s": sid, "n": note}).rowcount
    log(f"S{sid} source_oa_override curated_is_oa true: {'added' if n else 'row exists, left'} (ZD {ticket})")


def add_issns(conn, sid, issns, ticket):
    owned = dict(conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"), {"i": issns}).fetchall())
    assert all(v == sid for v in owned.values()), f"S{sid}: ISSN on another source {owned} (ZD {ticket})"
    missing = [i for i in issns if i not in owned]
    if missing:
        have = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": sid})]
        insert_issns(conn, sid, missing, resolve_issn_l(conn, sorted(set(have) | set(issns)), member_only=True))
        conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": sid})
    log(f"ISSNS S{sid} += {missing} (ZD {ticket})")


def merge(conn, loser, winner, rule, detail):
    """Post-D1 merge, same steps as merge() in handoff/oxjob629-ticket-merges-2026-11-15.py: the loser's ISSNs,
    endpoints, DataCite ids, beacon rows and OA override move to the winner, its names become alternate titles, a
    ledger row goes into source_merge (which also serves the merged-id redirect), and the loser row is deleted."""
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
        conn.execute(text("UPDATE sources SET issn_l = :l WHERE id = :id"),
                     {"l": resolve_issn_l(conn, issns, member_only=True), "id": winner})
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


def main():
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            for m in MINTS:
                if m["issns"]:
                    owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                         {"i": m["issns"]}).fetchall()
                    assert not owned, f"{m['name']}: ISSN already on a source {owned}"
                same = conn.execute(text("SELECT id, display_name FROM sources WHERE "
                                         "lower(regexp_replace(display_name, '[^[:alnum:]]', '', 'g')) = "
                                         "lower(regexp_replace(:n, '[^[:alnum:]]', '', 'g'))"), {"n": m["name"]}).fetchall()
                assert not same, f"{m['name']}: a source with the same normalized name exists {same}: edit it instead"
                sid = mint_source(conn, ctx, m["name"], source_type="journal", issns=m["issns"],
                                  publisher=m["publisher"], homepage_url=m["homepage"],
                                  country_code=m["cc"], country=m["country"])
                if m.get("alt"):
                    add_alt_titles(conn, sid, m["alt"])
                if m.get("institution"):
                    set_host_institution(conn, sid, m["institution"], m["ticket"])
                if m.get("oa"):
                    oa_override(conn, sid, m["oa"], m["ticket"])
                receipt["minted"].append({"source_id": sid, "name": m["name"], "issns": m["issns"], "ticket": m["ticket"]})
                log(f"MINTED S{sid} journal {m['name']!r} {m['issns']} (ZD {m['ticket']})")
            for e in EDITS:
                sid, t = e["sid"], e["ticket"]
                r = snapshot(conn, sid)
                assert r is not None, f"S{sid} missing (merged away? post-D1 losers have no row) (ZD {t})"
                assert (r["display_name"] or "").strip().lower() == e["expect"], (sid, r["display_name"], e["expect"])
                if e.get("add_issns"):
                    add_issns(conn, sid, e["add_issns"], t)
                for col, key in (("homepage_url", "homepage"), ("publisher", "publisher"), ("country_code", "cc"),
                                 ("country", "country")):
                    if e.get(key):
                        fill(conn, sid, col, e[key], t)
                if e.get("institution"):
                    set_host_institution(conn, sid, e["institution"], t)
                if e.get("alt"):
                    add_alt_titles(conn, sid, e["alt"])
                if e.get("oa"):
                    oa_override(conn, sid, e["oa"], t)
                if e.get("rename"):
                    n = conn.execute(text("UPDATE sources SET display_name = :n, display_name_before_override = :o, "
                                          "override_timestamp = now(), updated_date = now() WHERE id = :id"),
                                     {"n": e["rename"], "o": r["display_name"], "id": sid}).rowcount
                    assert n == 1
                    add_alt_titles(conn, sid, [r["display_name"]])
                    log(f"RENAME S{sid} {r['display_name']!r} -> {e['rename']!r} (ZD {t})")
                for loser, lname in e.get("merge", []):
                    lr = snapshot(conn, loser)
                    assert lr is not None and (lr["display_name"] or "").strip().lower() == lname, (loser, lr and lr["display_name"])
                    merge(conn, loser, sid, "zendesk_1225", {"ticket": t, "oxjob": JOB, "approved_by": APPROVED})
                    receipt["merged"].append({"loser": loser, "winner": sid, "ticket": t})
                    log(f"MERGED S{loser} {lname!r} -> S{sid} (ZD {t})")
            for sid, expect, new, t in PUBLISHER_SETS:
                r = snapshot(conn, sid)
                assert r is not None, f"S{sid} missing (merged away?) (ZD {t})"
                cur = r["publisher_id"]
                if cur == expect:
                    conn.execute(text("UPDATE sources SET publisher_id = :p, updated_date = now() WHERE id = :id"),
                                 {"p": new, "id": sid})
                    log(f"S{sid} publisher_id {cur} -> {new} (ZD {t})")
                else:
                    log(f"S{sid} publisher_id is {cur}, expected {expect}: left (ZD {t})")
            w = conn.execute(text("SELECT display_name, type FROM sources WHERE id = :id"), {"id": ARXIV}).fetchone()
            assert w and w.type == "repository" and "arxiv" in w.display_name.lower(), w
            for loser in RETIRED_ARXIV:
                live = conn.execute(text("SELECT 1 FROM sources WHERE id = :id"), {"id": loser}).fetchone()
                led = conn.execute(text("SELECT winner_id FROM source_merge WHERE loser_id = :id"), {"id": loser}).fetchone()
                if live or led:
                    log(f"S{loser}: left (live row {bool(live)}, ledger {led and led[0]}) (ZD 289)")
                    continue
                conn.execute(text("INSERT INTO source_merge (loser_id, winner_id, rule, source_feed, detail) "
                                  "VALUES (:l, :w, 'zendesk_289_retired_arxiv_2023', 'zendesk', CAST(:d AS JSONB))"),
                             {"l": loser, "w": ARXIV, "d": json.dumps({"ticket": 289, "oxjob": JOB, "approved_by": APPROVED,
                                                                        "note": "arXiv-named venue retired 2023-03-28; 404 since"})})
                receipt["merged"].append({"loser": loser, "winner": ARXIV, "ticket": 289, "ledger_only": True})
                log(f"LEDGER S{loser} -> S{ARXIV} (ZD 289; redirect via sync_merged_ids)")
            log(f"recompute_is_oa: {recompute_is_oa(conn)} source(s) changed")
            log(f"recompute_listed_in: {recompute_listed_in(conn)}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
        json.dump(receipt, open(RECEIPT_PATH, "w"), ensure_ascii=False, indent=1, default=str)
        print("receipt:", RECEIPT_PATH)
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
