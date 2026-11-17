"""Split two source records that each hold two journals (oxjob #1726, support factory #2254), written off desk
2026-11-15 by an off-desk lane (#2263); desk runs it. Every value below was re-checked live on 2026-11-15 (receipts in
the #1726 job, work/live_2026-11-15.json and work/live_26811_2026-11-15.json).

Why the records are wrong. The ISSN International Centre's ISSN-L table gives two different journals one ISSN-L
when they share an old "history" family (ISSN-H), and the registry treats one ISSN-L as one journal:
  cherd  S166351637 'Process Safety and Environmental Protection' holds Chemical Engineering Research and Design's
         ISSNs 0263-8762 / 1744-3563 next to PSEP's 0957-5820 / 1744-3598 / 1876-4800 (the IC's ISSN-L for all five:
         0957-5820; ISSN-H 9028-4658, the old Trans IChemE Parts A and B). Crossref: two journals, CHERD 9,335 DOIs on
         0263-8762, PSEP 9,045 DOIs on 0957-5820; both publish ~500-1,500 papers a year, every year since 2019.
         ZD 26196, 26978, 6388; ZD 5645 on another job.
  iic    S1002663397 'GRURRR. Gewerblicher Rechtsschutz ... /IIC/ ...' holds IIC (Springer, 0018-9855 / 2195-0237,
         Crossref 1,722 DOIs) with GRUR (C.H. Beck, 0016-9420) and GRUR Int. (0435-8600); the IC's ISSN-L for all four:
         0016-9420 (ISSN-H 9115-2933). 1,846 of its 1,851 primary works are IIC's. ZD 26811.

Policy choice: follows the sources rules (working/source-duplicate-merges/RULES-DRAFT.md, confirmed 2026-07-17):
R1 (a journal is one title's print + electronic ISSNs; here each ISSN record carries its own title), R3's guard (two
streams publishing in the same years are two journals, not a rename) and R3 itself for GRUR Int. -> GRUR
International (the ISSN record of 0435-8600 lists 'GRUR International' as a title variant; OUP's GRUR International
S4210174795 continues it), R6 (the side with more works keeps the id). Kyle approved the split reply on ZD 26978
(desk 2026-11-07/09).

What it writes (ONE transaction; dry run unless --execute; --only cherd|iic runs one entry):
  cherd  1. remove 0263-8762 and 1744-3563 from S166351637, and its 'Part A, Chemical engineering research and design'
            alternate title; override_timestamp set (curator-touched: no automated merge may take it, R7).
         2. mint 'Chemical Engineering Research and Design' (journal) with 0263-8762 + 1744-3563, issn_l 0263-8762,
            copying from S166351637: publisher, publisher_id (Elsevier BV 4310320990), country, country_code, societies
            (IChemE) and is_core (the 2024 CWTS core flag was given to the pooled record); homepage
            https://www.sciencedirect.com/journal/chemical-engineering-research-and-design; alternate title 'Transactions
            of the Institution of Chemical Engineers. Part A, Chemical engineering research and design';
            override_timestamp set. APC left empty (PSEP's price is not CHERD's).
  iic    1. S1002663397: issn_l 0016-9420 -> 0018-9855; remove 0016-9420 and 0435-8600; rename to Crossref's title
            'IIC - International Review of Intellectual Property and Competition Law' (old name kept in
            display_name_before_override, override_timestamp); publisher Springer Science+Business Media (4310319900);
            homepage https://link.springer.com/journal/40319; alternate titles drop 'GRUR', keep IIC's. APC fields are left
            untouched.
         2. mint 'Gewerblicher Rechtsschutz und Urheberrecht' (journal, C.H. Beck 4310320552, DE) with 0016-9420,
            alternate title 'GRUR', override_timestamp.
         3. add 0435-8600 to S4210174795 GRUR International (+ alternate title 'Gewerblicher Rechtsschutz und
            Urheberrecht. Internationaler Teil'); its issn_l stays 2632-8550.
  then   recompute_listed_in (lists follow the ISSNs: CHERD takes dongbi-c, poland-140, norway-1, jufo-1, vabb-shw,
         russia-white-list-1; IIC keeps jufo-3 / norway-2; GRUR takes jufo-1 / norway-1).

Effect after the next End 2 End (walden CreateLocationsWithSources matches Crossref locations by their ISSN list):
~10,121 CHERD works move off S166351637 onto the new source; ~25-30 GRUR-ISSN works leave IIC. No works are deleted;
far under every guardrail line (no pre-clear needed). jobs.reconcile_issn_l will leave the new rows alone (their
IC ISSN-L is owned by another source: class owned_elsewhere).

Run from the openalex-sources checkout (copy this file to handoff/ first; dry run by default):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1726-source-splits-2026-11-15.py [--execute] [--only cherd|iic]
Do not run while an End 2 End is between its 05:00 UTC start and publish. The receipt (before-state, actions, minted
ids, after-state) is printed as JSON after the RECEIPT marker: save it in the #1726 job's work/.

Revert (from the receipt): cherd: DELETE the minted source's source_issn rows, INSERT 0263-8762 and 1744-3563 back on
166351637, DELETE the minted sources row, put alternate_titles / override_timestamp of 166351637 back to `before`,
sync_issn_l_flags(166351637), recompute_listed_in. iic: same for the GRUR mint; DELETE 0435-8600 from 4210174795 and
its added alternate title; INSERT 0016-9420 and 0435-8600 back on 1002663397, then display_name / issn_l / publisher /
publisher_id / homepage_url / alternate_titles / display_name_before_override / override_timestamp back to
`before`; sync_issn_l_flags on each; recompute_listed_in.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import MatchContext, insert_issns, mint_source, normalize_name, recompute_listed_in, sync_issn_l_flags

JOB = 1726
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); RULES-DRAFT R1/R3/R6 (2026-07-17); Kyle ZD 26978 desk 2026-11-07"

PSEP = 166351637
CHERD_ISSNS = ["0263-8762", "1744-3563"]
PSEP_ISSNS = ["0957-5820", "1744-3598", "1876-4800"]
CHERD_NAME = "Chemical Engineering Research and Design"
CHERD_ALT = "Transactions of the Institution of Chemical Engineers. Part A, Chemical engineering research and design"
CHERD_HOME = "https://www.sciencedirect.com/journal/chemical-engineering-research-and-design"
ELSEVIER = 4310320990

IIC = 1002663397
IIC_ISSNS = ["0018-9855", "2195-0237"]
GRUR_ISSN = "0016-9420"
GRUR_INT_ISSN = "0435-8600"
IIC_NAME = "IIC - International Review of Intellectual Property and Competition Law"
IIC_HOME = "https://link.springer.com/journal/40319"
SPRINGER = 4310319900
GRUR_NAME = "Gewerblicher Rechtsschutz und Urheberrecht"
BECK = 4310320552
GRUR_INTL = 4210174795  # GRUR International (OUP), 2632-8550 / 2632-8623
GRUR_INT_ALT = "Gewerblicher Rechtsschutz und Urheberrecht. Internationaler Teil"

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "job": JOB, "approved": APPROVED,
           "before": {}, "checks": [], "actions": [], "minted": [], "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn, sid):
    r = conn.execute(text("SELECT * FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
    if r is None:
        return None
    out = {k: (v if isinstance(v, (list, dict, bool, int)) or v is None else str(v)) for k, v in r.items()}
    out["_source_issn"] = [dict(x._mapping) for x in conn.execute(
        text("SELECT issn, is_issn_l FROM source_issn WHERE source_id = :id ORDER BY issn"), {"id": sid})]
    return out


def own_issns(conn, sid):
    return {x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": sid})}


def active_namesakes(conn, name, exclude=()):
    key = normalize_name(name)
    # post-D1 (migration 021) a merged loser's row is deleted, so every row is a live source
    return [r.id for r in conn.execute(text("SELECT id, display_name FROM sources"))
            if normalize_name(r.display_name) == key and r.id not in exclude]


def merges_into(conn, sid):
    return [{k: (str(v) if v is not None else None) for k, v in r._mapping.items()}
            for r in conn.execute(text("SELECT * FROM source_merge WHERE winner_id = :id ORDER BY merged_at"), {"id": sid})]


def set_alts(conn, sid, alts):
    conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), updated_date = now() WHERE id = :id"),
                 {"a": json.dumps(alts, ensure_ascii=False), "id": sid})


def remove_issns(conn, sid, issns):
    n = conn.execute(text("DELETE FROM source_issn WHERE source_id = :id AND issn = ANY(:i)"),
                     {"id": sid, "i": list(issns)}).rowcount
    assert n == len(issns), (sid, issns, n)
    sync_issn_l_flags(conn, sid)


def mint(conn, ctx, name, issns, publisher, publisher_id, cc, country, homepage, alts, ticket, copy=None):
    owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"), {"i": issns}).fetchall()
    assert not owned, f"{name}: ISSN still owned {owned}"
    twins = active_namesakes(conn, name)
    assert not twins, f"{name}: an active source already has this name {twins}: stop and look"
    sid = mint_source(conn, ctx, name, source_type="journal", issns=issns, publisher=publisher,
                      homepage_url=homepage, country_code=cc, country=country)
    sets = {"publisher_id": publisher_id, "alternate_titles": json.dumps(alts, ensure_ascii=False)}
    if copy:
        sets.update(copy)
    cols = ", ".join(f"{k} = CAST(:{k} AS JSONB)" if k in ("alternate_titles", "societies") else f"{k} = :{k}" for k in sets)
    conn.execute(text(f"UPDATE sources SET {cols}, override_timestamp = now(), updated_date = now() WHERE id = :id"),
                 {**sets, "id": sid})
    new = row(conn, sid)
    assert sorted(i["issn"] for i in new["_source_issn"]) == sorted(issns), new["_source_issn"]
    assert new["issn_l"] == issns[0], f"S{sid} issn_l {new['issn_l']} (expected {issns[0]})"
    receipt["minted"].append({"source_id": sid, "name": name, "issns": issns, "ticket": ticket})
    log(f"MINTED S{sid} journal {name!r} {issns} issn_l {new['issn_l']} publisher_id {publisher_id} ({ticket})")
    return sid


def entry_cherd(conn, ctx):
    before = row(conn, PSEP)
    receipt["before"][str(PSEP)] = before
    receipt["before"][f"merges_into_{PSEP}"] = merges_into(conn, PSEP)
    assert before, "S166351637 missing (merged?)"
    assert normalize_name(before["display_name"]) == normalize_name("Process Safety and Environmental Protection"), before["display_name"]
    assert before["issn_l"] == "0957-5820", f"issn_l is {before['issn_l']}, expected 0957-5820: stop and look"
    assert own_issns(conn, PSEP) == set(CHERD_ISSNS + PSEP_ISSNS), own_issns(conn, PSEP)
    assert int(before["publisher_id"] or 0) == ELSEVIER, before["publisher_id"]
    for m in receipt["before"][f"merges_into_{PSEP}"]:
        if "chemical engineering research" in json.dumps(m).lower():
            receipt["checks"].append(f"CHERD-named merge loser into S{PSEP}: {m} -> desk: point its redirect at the new CHERD id")
            print("CHECK:", receipt["checks"][-1], flush=True)

    remove_issns(conn, PSEP, CHERD_ISSNS)
    alts = [a for a in (before["alternate_titles"] or []) if "chemical engineering research" not in normalize_name(a)]
    set_alts(conn, PSEP, alts)
    conn.execute(text("UPDATE sources SET override_timestamp = now() WHERE id = :id"), {"id": PSEP})
    log(f"S{PSEP}: removed {CHERD_ISSNS} and the Part A alternate title; override_timestamp set")

    copy = {"country": before["country"], "country_code": before["country_code"],
            "societies": json.dumps(before["societies"]) if before["societies"] is not None else None,
            "is_core": before["is_core"]}
    if copy["societies"] is None:
        copy.pop("societies")
    sid = mint(conn, ctx, CHERD_NAME, CHERD_ISSNS, before["publisher"], ELSEVIER, before["country_code"],
               before["country"], CHERD_HOME, [CHERD_ALT], "ZD 26196, 26978, 6388", copy=copy)
    return [PSEP, sid]


def entry_iic(conn, ctx):
    before = row(conn, IIC)
    receipt["before"][str(IIC)] = before
    receipt["before"][f"merges_into_{IIC}"] = merges_into(conn, IIC)
    intl = row(conn, GRUR_INTL)
    receipt["before"][str(GRUR_INTL)] = intl
    assert before, "S1002663397 missing (merged?)"
    assert before["display_name"].startswith("GRURRR. Gewerblicher Rechtsschutz"), before["display_name"]
    assert before["issn_l"] == GRUR_ISSN, before["issn_l"]
    assert own_issns(conn, IIC) == set(IIC_ISSNS + [GRUR_ISSN, GRUR_INT_ISSN]), own_issns(conn, IIC)
    assert intl and normalize_name(intl["display_name"]) == "grur international", intl
    assert own_issns(conn, GRUR_INTL) == {"2632-8550", "2632-8623"}, own_issns(conn, GRUR_INTL)
    assert not active_namesakes(conn, IIC_NAME, exclude=(IIC,)), "an active source already has IIC's name"

    conn.execute(text("UPDATE sources SET issn_l = :l WHERE id = :id"), {"l": IIC_ISSNS[0], "id": IIC})
    remove_issns(conn, IIC, [GRUR_ISSN, GRUR_INT_ISSN])
    alts = [a for a in (before["alternate_titles"] or []) if normalize_name(a) not in ("grur", normalize_name(IIC_NAME))]
    alts = alts + [t for t in ("IIC", "International Review of Intellectual Property and Competition Law")
                   if t.lower() not in {x.lower() for x in alts}]
    set_alts(conn, IIC, alts)
    conn.execute(text(
        "UPDATE sources SET display_name = :n, display_name_before_override = :o, override_timestamp = now(), "
        "publisher = 'Springer Science+Business Media', publisher_id = :p, homepage_url = :h, "
        "updated_date = now() "
        "WHERE id = :id"), {"n": IIC_NAME, "o": before["display_name"], "p": SPRINGER, "h": IIC_HOME, "id": IIC})
    log(f"S{IIC}: issn_l -> {IIC_ISSNS[0]}; removed {GRUR_ISSN}, {GRUR_INT_ISSN}; renamed {IIC_NAME!r}; "
        f"publisher Springer {SPRINGER}; homepage; alternate titles {alts}")

    sid = mint(conn, ctx, GRUR_NAME, [GRUR_ISSN], "C.H. Beck", BECK, "DE", "Germany", None, ["GRUR"], "ZD 26811")

    insert_issns(conn, GRUR_INTL, [GRUR_INT_ISSN], None)
    intl_alts = list(intl["alternate_titles"] or [])
    if GRUR_INT_ALT.lower() not in {a.lower() for a in intl_alts}:
        set_alts(conn, GRUR_INTL, intl_alts + [GRUR_INT_ALT])
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": GRUR_INTL})
    after_intl = row(conn, GRUR_INTL)
    assert after_intl["issn_l"] == intl["issn_l"], after_intl["issn_l"]
    log(f"S{GRUR_INTL} GRUR International += {GRUR_INT_ISSN} (R3: predecessor GRUR Int.) and alternate title {GRUR_INT_ALT!r}")
    return [IIC, sid, GRUR_INTL]


ENTRIES = {"cherd": entry_cherd, "iic": entry_iic}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--only", choices=sorted(ENTRIES))
    args = ap.parse_args()
    receipt["execute"] = args.execute
    receipt["only"] = args.only
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            touched = []
            for name, fn in ENTRIES.items():
                if args.only and name != args.only:
                    continue
                touched += fn(conn, ctx)
            log(f"listed_in rows changed {recompute_listed_in(conn)}")
            for sid in touched:
                r = row(conn, sid)
                receipt["after"][str(sid)] = {k: r[k] for k in ("display_name", "issn_l", "issns", "publisher", "publisher_id",
                                                                  "alternate_titles", "listed_in", "is_core", "country_code",
                                                                  "override_timestamp", "_source_issn")}
            print(json.dumps(receipt["after"], indent=1, ensure_ascii=False, default=str), flush=True)
            if not args.execute:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
