"""Registry fixes for ZD 1986 on oxjob #1899 (support factory #2254), written off-desk 2026-11-24 21:15 CT (#2263).
Every source re-checked on the live API that evening (receipts: #1899 work/recheck_1986_2026-11-24.json).

1. BMJ split (merge, loser -> winner; post-D1 pattern of handoff/oxjob629-ticket-merges-2026-11-15.py: loser deleted,
   ledger row in source_merge, which also serves the merged-ID redirect, #1124):
     S192814187 'BMJ' (print ISSN 0959-8138; 2,179 works, 2000-2001)  ->  S4393917726 'BMJ' (online ISSN 1756-1833;
     64,802 works). ISSN Portal: 0959-8138 'BMJ. British medical journal' (print) and 1756-1833 'BMJ British medical
     journal' (online, title variants BMJ.com, thebmj.com): one journal. The winner keeps 1756-1833 as ISSN-L and gains
     0959-8138; its empty homepage gets the loser's (https://www.bmj.com/thebmj). Both already host on BMJ.
2. Society named instead of the publisher (Policy choice: follows RULINGS § 3 Publishers, D12: "a publisher record says
   who publishes a journal", witnessed by its Crossref membership plus the journal's own site):
     S38008053  The Journal of Immunology       P4320800377 American Association of Immunologists -> P4310311648 OUP
                Crossref: latest DOIs 10.1093/jimmun/* member 286 Oxford University Press; ISSN Portal 0022-1767:
                publisher transfer AAI -> Oxford University Press effective 2025-01-01, receiving URL
                https://academic.oup.com/jimmunol/ (homepage set to it).
     S139212996 Clinical Chemistry               P4310320272 American Association for Clinical Chemistry -> P4310311648 OUP
                Crossref: latest DOIs 10.1093/clinchem/* member 286 Oxford University Press.
     S102125482 J Pharmacol Exp Ther (JPET)      P4310316283 ASPET -> P4310320990 Elsevier BV
                Crossref: latest DOIs 10.1016/j.jpet.* member 78 Elsevier BV.
   DESK, before --execute: the journal sites answered Cloudflare 403 to the cloud lane. Open
     https://academic.oup.com/jimmunol , https://academic.oup.com/clinchem and
     https://www.sciencedirect.com/journal/journal-of-pharmacology-and-experimental-therapeutics
   in a signed-out browser. If a site does not name that publisher (e.g. "published by Oxford University Press on behalf
   of ..."), set its SKIP flag below to True.
   DESK CHECK 24 Nov 21:40 CT: the three pages (and ScienceDirect's article pages) stay on Cloudflare's "Just a moment"
   in a signed-out desk Chrome too, so the sites were read by where they live instead: http://www.jimmunol.org/ 301s to
   https://academic.oup.com/jimmunol, http://intl.clinchem.org/ 301s to https://academic.oup.com/clinchem/, and
   jpet.aspetjournals.org is a CNAME of secure.jbs.elsevierhealth.com (Elsevier's journal platform); ASPET's own
   journals page (aspet.org/journals) sends subscription inquiries to Elsevier / ScienceDirect. Crossref's journal
   records name the same publishers (api.crossref.org/journals/0022-1767, 0009-9147: Oxford University Press;
   0022-3565: Elsevier). No SKIP set. Clin Chem's homepage moves to its redirect target (the old one 301s there).
   Not changed: The Journal of the Acoustical Society of America S11296630 stays on P4310320226 ASA: ASA (member 231)
   registers its DOIs; whether a distributor named on the site may win is card D43's question.

Run on desk (dry run by default; everything in ONE transaction):
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources && \
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1899-bmj-society-hosts-2026-11-24.py [--execute]
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker; save it as
handoff/oxjob1899-bmj-society-hosts-2026-11-24-receipt.json and in #1899's work/.
Works re-stamped: ~180.8K (J Immunol 107,398 + JPET 40,032 + Clin Chem 31,149 + BMJ 2,179); pre-clear --add 217,000.
Revert: the receipt's `before` holds every touched row. Hosts: UPDATE sources SET publisher_id = <before>, publisher =
<before>, homepage_url = <before> WHERE id = <id>. BMJ merge: re-insert the loser's `before` row (OVERRIDING SYSTEM
VALUE), UPDATE source_issn SET source_id = 192814187, is_issn_l = TRUE WHERE issn = '0959-8138', DELETE FROM
source_merge WHERE loser_id = 192814187, refresh_issns_column for both, drop the alternate title the merge added, and set
the winner's homepage back to NULL.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import refresh_datacite_column, normalize_name, refresh_issns_column, resolve_issn_l

EXECUTE = "--execute" in sys.argv
JOB = 1899
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); D12 2026-11-17 (RULINGS.md § 3 Publishers)"

SKIP_JIMMUNOL = False  # desk: True if academic.oup.com/jimmunol does not name Oxford University Press as publisher
SKIP_CLINCHEM = False  # desk: True if academic.oup.com/clinchem does not name Oxford University Press as publisher
SKIP_JPET = False      # desk: True if the JPET ScienceDirect page does not name Elsevier as publisher

# (loser, winner, tickets, note)
MERGES = [
    (192814187, 4393917726, [1986], "BMJ print ISSN 0959-8138 (2000-2001 works) into the BMJ online-ISSN 1756-1833 record; "
                                    "one journal per the ISSN Portal"),
]
EXPECT = {192814187: "BMJ", 4393917726: "BMJ",
          38008053: "The Journal of Immunology", 139212996: "Clinical Chemistry",
          102125482: "Journal of Pharmacology and Experimental Therapeutics"}
HOMEPAGE_IF_EMPTY = {4393917726: "https://www.bmj.com/thebmj"}
P_OUP, P_ELSEVIER = 4310311648, 4310320990
# (source, expected current publisher_id, new publisher_id, publisher text, new homepage or None, skip flag, witness)
HOSTS = [
    (38008053, 4320800377, P_OUP, "Oxford University Press", "https://academic.oup.com/jimmunol", SKIP_JIMMUNOL,
     "Crossref member 286 OUP registers 10.1093/jimmun/*; ISSN Portal 0022-1767 transfer AAI -> OUP effective 2025-01-01"),
    (139212996, 4310320272, P_OUP, "Oxford University Press", "https://academic.oup.com/clinchem", SKIP_CLINCHEM,
     "Crossref member 286 OUP registers 10.1093/clinchem/*; intl.clinchem.org 301s to academic.oup.com/clinchem"),
    (102125482, 4310316283, P_ELSEVIER, "Elsevier BV", None, SKIP_JPET,
     "Crossref member 78 Elsevier BV registers 10.1016/j.jpet.*; jpet.aspetjournals.org is served by Elsevier (CNAME secure.jbs.elsevierhealth.com)"),
]

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
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


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in handoff/oxjob629-ticket-merges-2026-11-15.py (with its 2026-11-20 datacite refresh)."""
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
    if dc:
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
            ids = set(EXPECT)
            snapshot(conn, ids)
            for sid, name in EXPECT.items():
                expect_name(conn, sid, name)
            # P_OUP / P_ELSEVIER: publisher ids already used as publisher_id by other registry rows (guard)
            for p in (P_OUP, P_ELSEVIER):
                used = conn.execute(text("SELECT COUNT(*) FROM sources WHERE publisher_id = :p"), {"p": p}).scalar()
                assert used > 0, f"P{p} hosts no source in the registry: wrong id?"
                log(f"P{p} already hosts {used} registry sources")
            for loser, winner, tickets, note in MERGES:
                merge(conn, loser, winner, "zendesk_1899_same_journal_two_issns",
                      {"tickets": tickets, "oxjob": JOB, "approved_by": APPROVED, "note": note})
            for sid, url in HOMEPAGE_IF_EMPTY.items():
                n = conn.execute(text("UPDATE sources SET homepage_url = :u, updated_date = now() "
                                      "WHERE id = :id AND (homepage_url IS NULL OR homepage_url = '')"),
                                 {"u": url, "id": sid}).rowcount
                log(f"HOMEPAGE {sid} {'set to ' + url if n else 'already set, left'}")
            for sid, was, new, pub_text, homepage, skip, witness in HOSTS:
                if skip:
                    log(f"SKIP host {sid} (desk: site did not confirm)")
                    continue
                cur = conn.execute(text("SELECT publisher_id FROM sources WHERE id = :id"), {"id": sid}).scalar()
                if cur == new:
                    log(f"HOST {sid} already P{new}, left")
                    continue
                assert cur == was, f"{sid}: publisher_id drifted: registry {cur}, expected {was}"
                fields = dict(publisher_id=new, publisher=pub_text)
                if homepage:
                    fields["homepage_url"] = homepage
                upd(conn, sid, **fields)
                log(f"HOST {sid} P{was} -> P{new} ({pub_text}); witness: {witness}")

            for r in conn.execute(text("SELECT id, display_name, type, issn_l, issns, publisher_id, institution_id, "
                                       "homepage_url, alternate_titles FROM sources WHERE id = ANY(:i) ORDER BY id"),
                                  {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
                print(f"  {r['id']}: {r['display_name']!r} issns={r['issns']} pub={r['publisher_id']} "
                      f"hp={r['homepage_url']}")
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
