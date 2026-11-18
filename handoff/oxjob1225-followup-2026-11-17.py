"""Follow-up source records for oxjob #1225 (support factory #2254), written off desk 2026-11-17 (lane #2263); run on
desk from the openalex-sources checkout, like oxjob1225-source-records-2026-11-16.py (same helpers, same receipt form).

PUBLISHER FIXES (ZD 2243, a June 2024 list of UMN Libraries titles; checked on each journal's own site
17 Nov 2026):
  S4387291174 Climate Literacy in Education (2836-4546): pubs.lib.umn.edu/index.php/cle/about says the works are
              "published by the University of Minnesota Libraries Publishing"; now P4310320514 (University of
              Minnesota Press) -> P4310316065 (University of Minnesota Libraries Publishing).
  S4387286171 Journal of Social Equity and Public Administration (2832-9287): jsepajournal.org/index.php/jsepa/about
              "The journal is published by the University of Minnesota Libraries Publishing"; same change.
  Left on purpose: S112590812 JTLU (its site: "published and sponsored by the University of Minnesota Center for
  Transportation Studies"; University of Minnesota is right). S56840690 'Law & inequality' (ISSNs 0737-089X,
  2573-0037) vs S4210209514 'Minnesota journal of law & inequality' (2767-1992): a title change with new ISSNs, not
  merged here.

MINT (ZD 8948, RULINGS § 3 D5, Jason 16 Nov: the venue's own site is the witness):
  Modern Electrodynamics / Современная электродинамика, ISSN 2949-0553, https://modern-electrodynamics.ru (editorial
  board and chief-editor sections; contact: Institute for Theoretical and Applied Electromagnetics of the Russian
  Academy of Sciences (ИТПЭ РАН), Moscow; "Все права защищены", so no OA claim). Its DataCite DOIs (10.24412/2949-0553-*,
  19 in OpenAlex) are registered by CyberLeninka's DataCite client with no container and no ISSN, so the mint alone
  binds nothing: the CyberLeninka records bind through #2195's rows in openalex.sources.article_page_journal_sources
  (platform cyberleninka) once rows naming the new source exist (desk step after this script; see #1225 PLAN.md).

MINTS (ZD 1278, 5353; D5 book presses; #1528 POLICY 2026-10-03: a book's or chapter's source is its series, else
the publisher's ebook platform). Walden's ebook-platform tier (CreateLocationsWithSources cell 11) binds a NULL-source
book/chapter location whose Crossref publisher, normalized, equals a platform's name minus 'eBooks', normalized.
  Teseo eBooks (type ebook platform; Editorial Teseo, Buenos Aires; editorialteseo.com and teseopress.com; ROR 02bf8e216
              = I4392738277 'Teseo (Argentina)', set as host). Crossref publisher string 'Teseo' (member 34661, prefix
              10.55778): 1,054 NULL-source locations on 17 Nov (969 edited-book, 80 book-chapter, all 10.55778).
  RTI Press eBooks (type ebook platform; RTI International's open-access, peer-reviewed imprint, rti.org/rti-press; ROR
              02qfscv73 = I4396570550 'RTI Press', set as host). Crossref publisher 'RTI Press' (member 2090, prefix
              10.3768): 174 NULL-source book/chapter locations, all 10.3768. RTI's 320 'report' records carry no
              container or ISSN and are not book types: they stay without a source (next step in #1225 PLAN.md).
  No existing platform holds the keys 'teseo', 'editorialteseo' or 'rtipress' (checked 17 Nov; the co-publisher
  platforms 'Universidad Nacional de La Plata, Teseo y CLACSO eBooks' and 'Johns Hopkins University Press and RTI Press
  eBooks' have other keys); the script asserts it again.

Run on desk, outside the End 2 End window; dry run by default, ONE transaction:
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-followup-2026-11-17.py
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1225-followup-2026-11-17.py --execute
Revert: publisher_id back to 4310320514 on the two sources (receipt `before`); delete the three minted sources (the
journal's source_issn row first).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import MatchContext, mint_source, recompute_is_oa, recompute_listed_in  # noqa: E402

EXECUTE = "--execute" in sys.argv
JOB = 1225
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); RULINGS § 3 D5 (Jason 2026-11-16); standing authority"
RECEIPT_PATH = os.path.join(HERE, "oxjob1225-followup-2026-11-17-receipt.json")

MINTS = [
    dict(ticket=8948, type="journal", name="Modern Electrodynamics", issns=["2949-0553"],
         alt=["Современная электродинамика"],
         publisher="Institute for Theoretical and Applied Electromagnetics of the Russian Academy of Sciences",
         homepage="https://modern-electrodynamics.ru/", cc="RU", country="Russia", institution=None),
    dict(ticket=1278, type="ebook platform", name="Teseo eBooks", issns=[], alt=["Editorial Teseo eBooks"],
         publisher="Editorial Teseo", homepage="https://www.editorialteseo.com/", cc="AR", country="Argentina",
         institution=4392738277),
    dict(ticket=5353, type="ebook platform", name="RTI Press eBooks", issns=[], alt=[],
         publisher="RTI Press", homepage="https://www.rti.org/rti-press", cc="US", country="United States",
         institution=4396570550),
]
PUBLISHER_SETS = [  # (source_id, expected current publisher_id, new publisher_id, ticket)
    (4387291174, 4310320514, 4310316065, 2243),
    (4387286171, 4310320514, 4310316065, 2243),
]

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
           "before": {}, "actions": [], "minted": []}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, sid):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = :i"), {"i": sid}).mappings():
        receipt["before"][str(sid)] = {k: (v if isinstance(v, (list, dict, bool, int, type(None))) else str(v))
                                       for k, v in r.items()}
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


def main():
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            for m in MINTS:
                if m["issns"]:
                    owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                         {"i": m["issns"]}).fetchall()
                    assert not owned, f"{m['name']}: ISSN already on a source {owned}"
                if m["type"] == "ebook platform":
                    # the platform tier keys on the name minus 'eBooks', normalized; a key two platforms share binds nothing
                    for t in [m["name"]] + m["alt"]:
                        k = conn.execute(text(
                            "SELECT id, display_name FROM sources WHERE type = 'ebook platform' AND "
                            "lower(regexp_replace(regexp_replace(display_name, '\\s*[eE][bB]ooks$', ''), '[^[:alnum:]]', '', 'g')) = "
                            "lower(regexp_replace(regexp_replace(:n, '\\s*[eE][bB]ooks$', ''), '[^[:alnum:]]', '', 'g'))"),
                            {"n": t}).fetchall()
                        assert not k, f"{t}: platform key already taken by {k}"
                same = conn.execute(text("SELECT id, display_name FROM sources WHERE "
                                         "lower(regexp_replace(display_name, '[^[:alnum:]]', '', 'g')) = "
                                         "lower(regexp_replace(:n, '[^[:alnum:]]', '', 'g'))"), {"n": m["name"]}).fetchall()
                assert not same, f"{m['name']}: a source with the same normalized name exists {same}: edit it instead"
                sid = mint_source(conn, ctx, m["name"], source_type=m["type"], issns=m["issns"],
                                  publisher=m["publisher"], homepage_url=m["homepage"],
                                  country_code=m["cc"], country=m["country"])
                add_alt_titles(conn, sid, m["alt"])
                if m["institution"]:
                    r = conn.execute(text("SELECT publisher_id, institution_id FROM sources WHERE id = :id"),
                                     {"id": sid}).fetchone()
                    if r.publisher_id is None and r.institution_id is None:
                        conn.execute(text("UPDATE sources SET institution_id = :i, updated_date = now() WHERE id = :id"),
                                     {"i": m["institution"], "id": sid})
                        log(f"S{sid} institution_id: NULL -> {m['institution']} (host; ZD {m['ticket']})")
                receipt["minted"].append({"source_id": sid, "name": m["name"], "issns": m["issns"], "ticket": m["ticket"]})
                log(f"MINTED S{sid} {m['type']} {m['name']!r} {m['issns']} (ZD {m['ticket']})")
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
