"""Registry fixes, round 2, for the support tickets on oxjob #875 (Zenodo-deposited journal articles under their journal
source), support factory #2254, written off-desk 2026-11-16 (#2263); every journal re-checked live that day (receipts in
the #875 job: work/probe_2026-11-16*.json, work/registry_round2_2026-11-16.json, EXPLORE.md).

Why minting is enough (proven by round 1, handoff/oxjob875-zenodo-journals-2026-11-15.py): the DataCite tier puts a
Zenodo deposit on the journal-type source whose ISSN the deposit names as IsPartOf. Night 11-16 put 282/282 Legal
Bulletin, 190/198 SANUM and 12/12 Dan-ag ISSN-naming works on their new sources (count them with include_xpac=true:
most are xpac works).

Mints, under Jason's D5 ruling (16 Nov, RULINGS.md § 3: add a journal on no list once its own website confirms it
exists; name and details from that site; ISSN optional). Witness = the journal's own site, read 16 Nov:
  ZD 24106  Muththarasi Journal - Art, Literary & Culture Tamil Journal  2584-1238  muththarasi.org (schema.org
            Periodical, Editorial Board, OJS portal); 339 Zenodo deposits name the ISSN (2024-2026)
  ZD 26662  Journal of Smart Computing  2979-8752  ciotr.com/jsc (OJS, Editorial Team, EiC Gerhard-Wilhelm Weber); 2 deposits
  ZD 26975  Afaq Research Horizons  3156-9617  afaqresearch.com (OJS, editorial board, e-ISSN, Diamond OA); 11 deposits
  ZD 27366  MP Journal of Medical Research  3144-8402  journal.meetingspremium.com (Editorial Board, ISSN, publisher);
            0 deposits name the ISSN (assigned 9 Nov 2026), so its own OAI feed is registered too: 61 records, each
            carrying its Zenodo DOI as dc:identifier (jobs.add_journal probe 16 Nov: READY_NEW_SOURCE, no sets)
  ZD 27614  SWARNRUDRA: An International Multidisciplinary Journal of Academic Research  3140-0108
            swarnrudrajournal.com (e-ISSN, Editorial + Advisory Board, publisher address); 19 deposits
  ZD 24283  Acta & Verba  3087-3150  Universidad San Sebastian's page uss.cl/formacion-integral/revista-acta-verba/
            ('Revista Acta&Verba', issues since Jan 2025); the page answered 403 to the cloud lane, so DESK: open it
            first; if it doesn't show the journal, set SKIP_ACTA = True. 15 deposits
Not minted: ZD 6389 The Epochal Transition (its four Zenodo records were deleted by Zenodo on 10 Aug 2026, 'User was
blocked'; one author is also editor and publisher, no editorial board: fails D5's 'signs of a real venue').
Other edits:
  ZD 23934  S7407122593 Dan-ag + e-ISSN 3155-6833: the 8 deposits of 2026 name only it (the editor's e-ISSN,
            provisionally assigned by the National Library of the Philippines; valid check digit; no source owns it)
  ZD 4880   S7407122594 (minted 15 Nov from the ticket's ISSN 2732-7566, 0 works) is a duplicate of S7407062212
            'Journal on Tourism & Sustainability' (eISSN 2515-6780, DOAJ, OTS GB, 10 works): merge it in (merge_source:
            ISSN 2732-7566 moves to the winner); fill the winner's homepage if empty; register the journal's own OJS
            feed (otsjournal.org, 93 records, Zenodo DOIs in dc:identifier; add_journal probe picked set 'driver')
  ZD 26794  S7407122595 ICOPEC 'Abstracts & Proceedings' type conference -> journal: the DataCite tier maps only
            journal-type sources (CreateLocationsWithSources datacite_issn_journal_map: s.type = 'journal'), so as a
            'conference' its deposits could never group even once they name ISSN 3049-4834. An ISSN-registered serial,
            like S4306524103 (IAC proceedings, journal).

Run on desk (dry run by default; ONE transaction):
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources && \
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob875-zenodo-journals-2026-11-16.py [--execute]
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker; save it as
handoff/oxjob875-zenodo-journals-2026-11-16-receipt.txt and in the #875 job's work/.
Revert: minted sources: delete their oai_pmh_endpoint, source_oa_override, source_issn and sources rows; Dan-ag:
DELETE FROM source_issn WHERE issn = '3155-6833' (then sync_issn_l_flags(7407122593)); JOTS merge (post-D1, the loser
row is deleted): re-insert sources row 7407122594 from the receipt's `before` snapshot (OVERRIDING SYSTEM VALUE), UPDATE
source_issn SET source_id = 7407122594, is_issn_l = TRUE WHERE issn = '2732-7566', move its source_oa_override back,
DELETE FROM source_merge WHERE loser_id = 7407122594, refresh both issns columns and drop the alternate title the merge
added; delete the JOTS endpoint row; homepage back to the receipt's value; ICOPEC: type back to 'conference'.
"""
import importlib.util
import json
import os
import secrets
import string
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import (MatchContext, insert_issns, mint_source, normalize_name, recompute_listed_in,
                         refresh_issns_column, resolve_issn_l)
# NB: not sources_lib.merge_source: it still writes sources.merge_into_id, dropped by D1 (migration 021, 13 Aug).
# merge() below is the post-D1 helper of handoff/oxjob1979-renamed-journals-2026-11-15.py (loser deleted, ledger row
# in source_merge, which also serves the merged-ID redirect, #1124).

# jobs/__init__ pulls sentry_sdk; load add_journal (stdlib only) by file path for its feed-overlap guard
_spec = importlib.util.spec_from_file_location("add_journal", os.path.join(os.path.dirname(HERE), "jobs", "add_journal.py"))
_aj = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(_aj)
feed_host, feed_overlaps = _aj.host, _aj.overlaps

EXECUTE = "--execute" in sys.argv
SKIP_ACTA = False  # desk: True if the uss.cl page does not show the journal
JOB = 875
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); D5 ruling 2026-11-16 (RULINGS.md § 3)"

MINTS = [
    dict(ticket=24106, name="Muththarasi Journal – Art, Literary & Culture Tamil Journal", issns=["2584-1238"],
         alt=["முத்தரசி ஆய்விதழ்", "MUTHTHARASI – Art Literary Culture Tamil Journal"],
         publisher="Muththarasi Publications & Journals", homepage="https://muththarasi.org/publications-journals/",
         cc="IN", country="India",
         witness="muththarasi.org: schema.org Periodical 'முத்தரசி ஆய்விதழ்' / 'Muththarasi Journal – Art, Literary & "
                 "Culture Tamil Journal', ISSN 2584-1238, Editorial Board page, OJS portal tamiljournal.muththarasi.org",
         oa="ZD 24106 (#875): open access per the journal site; 340/340 Zenodo DataCite deposits naming ISSN 2584-1238 are CC BY 4.0"),
    dict(ticket=26662, name="Journal of Smart Computing", issns=["2979-8752"], alt=["JSC"],
         publisher="Center of Intelligent Operations and Technology Research", homepage="https://ciotr.com/jsc/",
         cc="GB", country="United Kingdom",
         witness="ciotr.com/jsc: OJS journal site, ISSN 2979-8752, Editorial Team, Editor-in-Chief Prof. Gerhard-Wilhelm "
                 "Weber, publisher CIOTR (United Kingdom), biannual, founded 2025",
         oa="ZD 26662 (#875): 'peer-reviewed, open-access journal' per its site; 2/2 Zenodo DataCite deposits naming ISSN 2979-8752 are CC BY 4.0"),
    dict(ticket=26975, name="Afaq Research Horizons", issns=["3156-9617"],
         alt=["مجلة آفاق بحثية متعددة التخصصات في العلوم الاجتماعية والإنسانية", "آفاق بحثية متعددة التخصصات"],
         publisher="جمعية النهضة لدعم التنمية المحلية", homepage="https://afaqresearch.com", cc="MA", country="Morocco",
         witness="afaqresearch.com: OJS journal site, e-ISSN 3156-9617, editorial board and scientific committee pages, "
                 "publisher جمعية النهضة لدعم التنمية المحلية (Morocco), quarterly",
         oa="ZD 26975 (#875): Diamond Open Access, no APCs, per its site; 11/11 Zenodo DataCite deposits naming ISSN 3156-9617 are CC BY 4.0"),
    dict(ticket=27366, name="MP Journal of Medical Research", issns=["3144-8402"], alt=["MP J Med Res"],
         publisher="MeetingsPremium LLC", homepage="https://journal.meetingspremium.com", cc="US", country="United States",
         witness="journal.meetingspremium.com/a-propos: ISSN (online) 3144-8402 (U.S. ISSN Center), publisher MeetingsPremium "
                 "LLC, Editor-in-Chief, Editorial Board, peer-review process",
         oa="ZD 27366 (#875): open access per its site; its OAI records carry CC BY 4.0 and info:eu-repo/semantics/openAccess",
         feed=dict(pmh_url="https://journal.meetingspremium.com/oai", pmh_set=None, id_host="journal.meetingspremium.com")),
    dict(ticket=27614, name="SWARNRUDRA: An International Multidisciplinary Journal of Academic Research", issns=["3140-0108"],
         alt=["SWARNRUDRA"], publisher="Swarnrudra Publication", homepage="https://swarnrudrajournal.com", cc="IN", country="India",
         witness="swarnrudrajournal.com: OJS journal site, e-ISSN 3140-0108, Editor Abhishek Dixit, Editorial Board and "
                 "Advisory Board pages, publisher Swarnrudra Publication (Shahjahanpur, India)",
         oa="ZD 27614 (#875): 'Open Access, CC BY 4.0' per its site; 22/22 Zenodo DataCite deposits naming ISSN 3140-0108 are CC BY 4.0"),
    dict(ticket=24283, name="Acta & Verba", issns=["3087-3150"], alt=["Revista Acta & Verba"],
         publisher="Universidad San Sebastián", homepage="https://www.uss.cl/formacion-integral/revista-acta-verba/",
         cc="CL", country="Chile",
         witness="uss.cl/formacion-integral/revista-acta-verba/ ('Revista Acta&Verba', the journal of USS's Dirección de "
                 "Formación Integral, issues since Jan 2025); USS researcher portal lists its articles",
         oa="ZD 24283 (#875): 15/15 Zenodo DataCite deposits naming ISSN 3087-3150 are CC BY 4.0, open on Zenodo and the USS site"),
]
ADD_ISSNS = [(7407122593, "Dan-ag: Journal of Educational Studies", ["3155-6833"], 23934)]
MERGES = [(7407122594, 7407062212, "Journal on Tourism & Sustainability", 4880)]  # loser, winner
HOMEPAGES = [(7407062212, "https://otsjournal.org/", 4880)]  # only if empty
RETYPES = [(7407122595, "Abstracts & Proceedings (International Conference on Political Economy)", "conference", "journal", 26794)]
FEEDS_EXISTING = [(7407062212, dict(pmh_url="https://otsjournal.org/index.php/jots/oai", pmh_set="driver",
                                    id_host="ojs2.199.250.217.133"), 4880)]

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
           "before": {}, "actions": [], "minted": [], "endpoints": [], "held": []}
ALNUM = string.ascii_lowercase + string.digits


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


def register_feed(conn, sid, feed, ticket):
    """jobs.add_journal's RUNBOOK step 4: no endpoint when a row on the same host could harvest the same records."""
    hosts = {feed_host(feed["pmh_url"])} | ({feed["id_host"]} if feed.get("id_host") else set())
    rows_on_host = {}
    for h in hosts:
        for e in conn.execute(text("SELECT id, pmh_url, pmh_set, status, source_id FROM oai_pmh_endpoint "
                                   "WHERE lower(pmh_url) LIKE :a OR lower(pmh_url) LIKE :b"),
                              {"a": f"%://{h}%", "b": f"%://www.{h}%"}):
            rows_on_host[e.id] = e
    block = [[e.id, e.pmh_url, e.pmh_set, e.status, e.source_id] for e in rows_on_host.values()
             if feed_overlaps(feed["pmh_url"], feed["pmh_set"], e.pmh_url, e.pmh_set)]
    if block:
        receipt["held"].append({"source_id": sid, "pmh_url": feed["pmh_url"], "pmh_set": feed["pmh_set"], "blocking_rows": block})
        log(f"  NO ENDPOINT for S{sid} (ZD {ticket}): {len(block)} row(s) could harvest the same records: {block[:3]}")
        return
    eid = "".join(secrets.choice(ALNUM) for _ in range(20))
    conn.execute(text(
        "INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
        "source_id, is_journal_host, green_scrape, rand) "
        "VALUES (:id, :u, :s, 'oai_dc', true, true, 'active', :sid, true, true, random())"),
        {"id": eid, "u": feed["pmh_url"], "s": feed["pmh_set"], "sid": sid})
    receipt["endpoints"].append({"endpoint_id": eid, "source_id": sid, "pmh_url": feed["pmh_url"], "pmh_set": feed["pmh_set"],
                                 "ticket": ticket})
    log(f"  REGISTERED endpoint {eid} {feed['pmh_url']} [{feed['pmh_set'] or '-'}] -> S{sid} journal_host (ZD {ticket}); "
        f"{len(rows_on_host)} other rows on the host, none overlapping; the next daily openalex-ingest harvest picks it up")


def merge(conn, loser, winner, rule, detail):
    """Post-D1 merge, same steps as merge() in handoff/oxjob1979-renamed-journals-2026-11-15.py and
    handoff/oxjob629-ticket-merges-2026-11-15.py: loser deleted, ledger row in source_merge (merged-ID redirect, #1124)."""
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


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            snapshot(conn, [s for s, *_ in ADD_ISSNS] + [x for m in MERGES for x in m[:2]] + [s for s, *_ in RETYPES])
            ctx = MatchContext(conn, name_link=True)
            for m in MINTS:
                if m["ticket"] == 24283 and SKIP_ACTA:
                    log("SKIP Acta & Verba (desk: uss.cl page did not confirm it)")
                    continue
                owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                     {"i": m["issns"]}).fetchall()
                assert not owned, f"{m['name']}: ISSN already on a source {owned}"
                twins = [t for t, _ in ctx.name_index.get(normalize_name(m["name"]), []) if not ctx.sid_to_issns.get(t)]
                assert not twins, f"{m['name']}: ISSN-less namesake(s) {twins}: add the ISSN there instead"
                sid = mint_source(conn, ctx, m["name"], source_type="journal", issns=m["issns"],
                                  publisher=m["publisher"], homepage_url=m["homepage"],
                                  country_code=m["cc"], country=m["country"])
                add_alt_titles(conn, sid, m.get("alt") or [])
                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, true, :n) "
                                  "ON CONFLICT (source_id) DO NOTHING"), {"s": sid, "n": m["oa"]})
                receipt["minted"].append({"source_id": sid, "name": m["name"], "issns": m["issns"], "ticket": m["ticket"],
                                          "witness": m["witness"]})
                log(f"MINTED S{sid} journal {m['name']!r} {m['issns']} (ZD {m['ticket']}); witness: {m['witness']}; OA override: {m['oa']}")
                if m.get("feed"):
                    register_feed(conn, sid, m["feed"], m["ticket"])
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
            for loser, winner, expect, ticket in MERGES:
                rows = {r.id: r for r in conn.execute(text("SELECT id, display_name, type FROM sources WHERE id IN (:l, :w)"),
                                                      {"l": loser, "w": winner})}
                assert all(normalize_name(rows[i].display_name) == normalize_name(expect) and rows[i].type == "journal"
                           for i in (loser, winner)), rows
                merge(conn, loser, winner, rule="oxjob875_same_journal_two_issns",
                      detail={"ticket": ticket, "job": JOB, "why": "minted 15 Nov from the ticket's ISSN 2732-7566; the "
                              "journal's site and DOAJ name 2515-6780, already on the winner"})
            for sid, url, ticket in HOMEPAGES:
                n = conn.execute(text("UPDATE sources SET homepage_url = :u, updated_date = now() "
                                      "WHERE id = :id AND (homepage_url IS NULL OR homepage_url = '')"), {"u": url, "id": sid}).rowcount
                log(f"HOMEPAGE S{sid} {'set to ' + url if n else 'already set, left'} (ZD {ticket})")
            for sid, expect, old, new, ticket in RETYPES:
                n = conn.execute(text("UPDATE sources SET type = :new, updated_date = now() WHERE id = :id AND type = :old"),
                                 {"new": new, "old": old, "id": sid}).rowcount
                assert n == 1, (sid, "type was not", old)
                log(f"RETYPE S{sid} {expect!r} {old} -> {new} (ZD {ticket})")
            for sid, feed, ticket in FEEDS_EXISTING:
                register_feed(conn, sid, feed, ticket)
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
