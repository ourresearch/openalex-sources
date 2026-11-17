"""Zendesk 5993 (oxjob #1176, support factory #2254): Environmental Epigenetics (Oxford University Press, eISSN 2058-5888)
has no OpenAlex source. Its ISSN sits on Current Zoology S58136343, so 289 of the journal's 292 works (DOIs 10.1093/eep/...)
are filed under Current Zoology (live API, 17 Nov 2026 14:00 CT; receipts in oxjobs #1176 work/live_2026-11-17.json).

Evidence:
  - Crossref journals/2058-5888: 'Environmental Epigenetics', Oxford University Press, electronic ISSN 2058-5888 only, 284 DOIs.
  - Crossref journals/2396-9814: 'Current Zoology', print 1674-5507 + electronic 2396-9814. So 2396-9814 STAYS on Current
    Zoology (the requester and the September recipe named it too, wrongly).
  - DOAJ: Environmental Epigenetics, eissn 2058-5888, Oxford University Press, GB, OA since 2015, https://academic.oup.com/eep.
    Current Zoology is in DOAJ on its own ISSNs (since 2009), so its DOAJ flag and OA don't change.
  - The ISSN register gives 2058-5888 the ISSN-L 1674-5507 (Current Zoology's; #1899 triage, 17 Nov). That is how
    sources_lib.match_source's ISSN-L expansion pulls it onto Current Zoology. Once the new source owns 2058-5888, the
    expansion never runs for it (it runs only when no source owns an incoming ISSN), so the fix holds with no lock.
Policy: one journal, one source; the host is the publisher Crossref names (RULINGS #1164), an existing entity:
P4310311648 Oxford University Press (ROR 0336mm561). DOAJ flags come from the doaj_journal staging table, with the same SQL
jobs.doaj.apply uses, for these two sources only.

One transaction (rehearsal by default; --execute commits):
  1. guards: S58136343 is a journal, issn_l 1674-5507, ISSNs exactly {1674-5507, 2058-5888, 2396-9814}; no other source owns
     2058-5888; no source's normalized name or alternate title is 'environmental epigenetics'; recompute_is_oa changes 0 rows
     before the edit (else stop: someone else's drift).
  2. DELETE source_issn (58136343, '2058-5888'); re-derive Current Zoology's issns column.
  3. mint_source 'Environmental Epigenetics' (journal, issns ['2058-5888'], publisher 'Oxford University Press', homepage
     https://academic.oup.com/eep, GB / United Kingdom), then publisher_id = 4310311648.
  4. DOAJ flags for the new source and Current Zoology from doaj_journal.
  5. recompute_is_oa: the new source flips to true iff doaj_journal lists 2058-5888 (asserted); recompute_listed_in.
  6. asserts after: 2058-5888 owned by the new source, issn_l 2058-5888; Current Zoology issns [1674-5507, 2396-9814].
Works: walden matches works to sources by ISSN (Crossref tier), so the next End 2 End moves the 289 EEP works to the new
source (~289 works re-stamp, plus Current Zoology's counts). Check after the night:
  https://api.openalex.org/works?filter=doi_starts_with:10.1093/eep&group_by=primary_location.source.id

Run on desk:
    cd ~/ox/openalex-sources && python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/<this file> [--execute]
Revert:
    ... run_registry.py handoff/<this file> --revert handoff/<receipt.json> [--execute]
  deletes the minted source's source_issn and sources rows, puts 2058-5888 back on S58136343, re-derives issns,
  recompute_is_oa / recompute_listed_in.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.getcwd())
from sqlalchemy import text

from db import engine
from sources_lib import (MatchContext, mint_source, normalize_name, recompute_is_oa, recompute_listed_in,
                         refresh_issns_column, sync_issn_l_flags)

EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
CZ = 58136343
CZ_ISSN_L = "1674-5507"
CZ_BEFORE = {"1674-5507", "2058-5888", "2396-9814"}
CZ_AFTER = ["1674-5507", "2396-9814"]
MOVE = "2058-5888"
OUP = 4310311648
NEW = dict(name="Environmental Epigenetics", type="journal", issns=[MOVE], publisher="Oxford University Press",
           homepage="https://academic.oup.com/eep", cc="GB", country="United Kingdom")
receipt = {"job": 1176, "ticket": 5993, "run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE,
           "revert_of": REVERT, "reviewed": "off-desk support-factory lane 1176 (17 Nov 2026), tested on a throwaway "
           "Postgres built from migrations/", "before": {}, "after": {}, "minted": None, "actions": []}

DOAJ_FLAGS = """
    SELECT si.source_id, MIN(d.oa_start_year) AS start_year, MIN(d.license) AS license
    FROM doaj_journal d CROSS JOIN LATERAL unnest(d.issns) AS di(issn)
    JOIN source_issn si ON si.issn = di.issn
    WHERE si.source_id = ANY(:ids) GROUP BY si.source_id"""
SNAP_COLS = ("id", "display_name", "type", "issn_l", "issns", "publisher", "publisher_id", "homepage_url", "country_code",
             "is_oa", "is_in_doaj", "is_in_doaj_start_year", "doaj_license", "listed_in")


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snap(conn, sid):
    r = conn.execute(text(f"SELECT {', '.join(SNAP_COLS)} FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
    return {k: (list(v) if isinstance(v, list) else v) for k, v in r.items()} if r else None


def owned(conn, sid):
    return {r[0] for r in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :s"), {"s": sid})}


def apply_doaj(conn, ids):
    """jobs.doaj.apply's set/unset, for these sources only."""
    flags = {r.source_id: r for r in conn.execute(text(DOAJ_FLAGS), {"ids": ids})}
    for sid in ids:
        f = flags.get(sid)
        if f:
            n = conn.execute(text(
                "UPDATE sources SET is_in_doaj = TRUE, is_in_doaj_start_year = :y, doaj_license = :l, updated_date = now() "
                "WHERE id = :id AND (is_in_doaj IS DISTINCT FROM TRUE OR is_in_doaj_start_year IS DISTINCT FROM :y "
                "OR doaj_license IS DISTINCT FROM :l)"), {"y": f.start_year, "l": f.license, "id": sid}).rowcount
        else:
            n = conn.execute(text(
                "UPDATE sources SET is_in_doaj = FALSE, is_in_doaj_start_year = NULL, doaj_license = NULL, "
                "updated_date = now() WHERE id = :id AND is_in_doaj = TRUE"), {"id": sid}).rowcount
        log(f"DOAJ S{sid}: {'listed' if f else 'not listed'} (start {f.start_year if f else None}), rows changed {n}")
    return {sid: sid in flags for sid in ids}


def forward(conn):
    stray = recompute_is_oa(conn)
    assert stray == 0, f"recompute_is_oa changed {stray} rows BEFORE the edit (someone else's drift): stop and look"
    cz = snap(conn, CZ)
    assert cz and cz["type"] == "journal" and cz["issn_l"] == CZ_ISSN_L, f"S{CZ} not as reviewed: {cz}"
    assert owned(conn, CZ) == CZ_BEFORE, f"S{CZ} ISSNs drifted: {sorted(owned(conn, CZ))}"
    other = conn.execute(text("SELECT source_id FROM source_issn WHERE issn = :i AND source_id <> :s"),
                         {"i": MOVE, "s": CZ}).fetchall()
    assert not other, f"{MOVE} also on {other}"
    ctx = MatchContext(conn, name_link=True)
    twins = [t for t, _ in ctx.name_index.get(normalize_name(NEW["name"]), [])]
    alt = conn.execute(text("SELECT id FROM sources WHERE alternate_titles IS NOT NULL AND EXISTS ("
                            "SELECT 1 FROM jsonb_array_elements_text(alternate_titles) a WHERE lower(a) = :n)"),
                       {"n": NEW["name"].lower()}).fetchall()
    assert not twins and not alt, f"a source already carries the name {NEW['name']!r}: {twins} {alt}: bind there instead"
    receipt["before"] = {f"S{CZ}": cz}

    conn.execute(text("DELETE FROM source_issn WHERE source_id = :s AND issn = :i"), {"s": CZ, "i": MOVE})
    sync_issn_l_flags(conn, CZ)
    ctx.issn_to_sid.pop(MOVE, None)
    ctx.sid_to_issns[CZ].discard(MOVE)
    log(f"S{CZ}: removed {MOVE}")

    sid = mint_source(conn, ctx, NEW["name"], source_type=NEW["type"], issns=NEW["issns"], publisher=NEW["publisher"],
                      homepage_url=NEW["homepage"], country_code=NEW["cc"], country=NEW["country"])
    conn.execute(text("UPDATE sources SET publisher_id = :p WHERE id = :id"), {"p": OUP, "id": sid})
    receipt["minted"] = sid
    log(f"MINTED S{sid} journal {NEW['name']!r} {NEW['issns']} host P{OUP} homepage {NEW['homepage']} (ZD 5993)")

    listed = apply_doaj(conn, [sid, CZ])
    flips = recompute_is_oa(conn)
    assert flips == (1 if listed[sid] else 0), f"recompute_is_oa changed {flips} rows, expected {int(listed[sid])}"
    log(f"recompute_is_oa changed {flips}; listed_in rows changed {recompute_listed_in(conn)}")

    new, cz2 = snap(conn, sid), snap(conn, CZ)
    assert new["issn_l"] == MOVE and new["issns"] == [MOVE] and owned(conn, sid) == {MOVE}, f"new source wrong: {new}"
    assert cz2["issns"] == CZ_AFTER and cz2["issn_l"] == CZ_ISSN_L, f"S{CZ} after: {cz2}"
    assert cz2["is_oa"] == cz["is_oa"] and cz2["is_in_doaj"] == cz["is_in_doaj"], f"S{CZ} OA moved: {cz} -> {cz2}"
    receipt["after"] = {f"S{CZ}": cz2, f"S{sid}": new}
    log(f"after: S{sid} {new}; S{CZ} issns {cz2['issns']}")


def revert(conn, path):
    rec = json.load(open(path))
    sid = rec["minted"]
    assert rec.get("execute") and sid, "receipt is not from an executed forward run"
    assert owned(conn, sid) == {MOVE}, f"S{sid} ISSNs changed since the run: {sorted(owned(conn, sid))}"
    receipt["before"] = {f"S{sid}": snap(conn, sid), f"S{CZ}": snap(conn, CZ)}
    conn.execute(text("DELETE FROM source_issn WHERE source_id = :s"), {"s": sid})
    conn.execute(text("DELETE FROM sources WHERE id = :s"), {"s": sid})
    conn.execute(text("INSERT INTO source_issn (source_id, issn, is_issn_l) VALUES (:s, :i, FALSE)"), {"s": CZ, "i": MOVE})
    refresh_issns_column(conn, CZ)
    apply_doaj(conn, [CZ])
    log(f"REVERTED: deleted S{sid}; {MOVE} back on S{CZ}; recompute_is_oa {recompute_is_oa(conn)}; "
        f"listed_in {recompute_listed_in(conn)}")
    receipt["after"] = {f"S{CZ}": snap(conn, CZ)}


def main():
    try:
        with engine.begin() as conn:
            revert(conn, REVERT) if REVERT else forward(conn)
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
