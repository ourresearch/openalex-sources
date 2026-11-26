"""Stale journal homepages from support tickets, batch 3 (oxjob #1177, support factory, 2026-11-26).

10 sources, homepage_url only (nothing else changes; homepages re-stamp no works). Written off desk; each new homepage
was checked on 26 Nov 2026 (receipts: ~/ox/oxjobs/working/source-homepage-url-stale/work/receipts_2026-11-26.md;
expected values read from the registry mirror openalex.sources.sources, column webpage, at 06:35 CT, NULL checked as
NULL, not ''):
  - 28374 South African Computer Journal S8301326: the editor-in-chief asks for the journal's own site since 2023,
    https://www.sacj.org.za/ (200, ISSNs 1015-7999 / 2313-7835 on the page; its notice "Fraudulent cloning of SACJ"
    says "The only site for SACJ is https://sacj.org.za/"). Was http://sacj.cs.uct.ac.za/.
  - 27630 (Pluto Journals' 18 Nov follow-up; the ticket's Oxjob is now #1899 for its 25 Nov publisher asks) nine Pluto
    Journals records with no homepage -> Pluto's own page for each journal (each page read 26 Nov; print and online
    ISSNs on the page equal the record's two ISSNs).

Guard per field, re-checked inside each UPDATE: the registry still holds the value read on 26 Nov ("expect").
A source whose fields drifted is skipped and reported (a SKIP showing '' where None was expected means the registry
stores an empty string: change that row's expected value to "" and rerun; nothing else).

    python handoff/oxjob1177-homepages-2026-11-26.py                         # dry run, rolled back
    python handoff/oxjob1177-homepages-2026-11-26.py --execute               # write + print the receipt
    python handoff/oxjob1177-homepages-2026-11-26.py --revert <receipt.json> [--execute]   # put the old values back

On desk run it through run_registry.py (lanes_registry login, #1831).
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
FIELDS = {"homepage_url", "institution_id", "publisher_id", "display_name"}

EDITS = [
    # (source id, tickets, {field: (expected now, new)})
    (8301326, [28374], {"homepage_url": ("http://sacj.cs.uct.ac.za/", "https://www.sacj.org.za/")}),
    (164212335, [27630], {"homepage_url": (None, "https://www.plutojournals.com/asq/")}),
    (4210178821, [27630], {"homepage_url": (None, "https://www.plutojournals.com/bethlehem-university-journal/")}),
    (4220651541, [27630], {"homepage_url": (None, "https://www.plutojournals.com/ijcds/")}),
    (4210236504, [27630], {"homepage_url": (
        None, "https://www.plutojournals.com/international-journal-of-disability-and-social-justice/")}),
    (4210192752, [27630], {"homepage_url": (None, "https://www.plutojournals.com/islamophobia/")}),
    (4210179858, [27630], {"homepage_url": (
        None, "https://www.plutojournals.com/journal-for-the-study-of-indentureship-and-its-legacies/")}),
    (4210220040, [27630], {"homepage_url": (None, "https://www.plutojournals.com/the-journal-of-fair-trade/")}),
    (4210205121, [27630], {"homepage_url": (None, "https://www.plutojournals.com/journal-of-global-faultlines/")}),
    (4210197680, [27630], {"homepage_url": (None, "https://www.plutojournals.com/scj/")}),
]


class DryRun(Exception):
    pass


def apply(conn, receipt):
    assert len({e[0] for e in EDITS}) == len(EDITS), "duplicate source ids"
    receipt["changed"], receipt["skipped"] = {}, {}
    for sid, tickets, sets in EDITS:
        assert set(sets) <= FIELDS
        cur = conn.execute(text("SELECT display_name, homepage_url, institution_id, publisher_id, "
                                "display_name_before_override, CAST(override_timestamp AS text) AS override_ts "
                                "FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
        drift = {f: cur[f] for f, (old, _) in sets.items() if cur is None or cur[f] != old} if cur else {"row": None}
        if drift:
            receipt["skipped"][f"S{sid}"] = {"tickets": tickets, "registry_now": drift}
            continue
        entry = {"tickets": tickets}
        for f, (old, new) in sets.items():
            n = conn.execute(text(f"UPDATE sources SET {f} = :new, updated_date = now() "
                                  f"WHERE id = :id AND {f} IS NOT DISTINCT FROM :old"),
                             {"new": new, "old": old, "id": sid}).rowcount
            assert n == 1, (sid, f)
            entry[f] = [old, new]
        if "display_name" in sets:
            conn.execute(text("UPDATE sources SET display_name_before_override = :o, override_timestamp = now() "
                              "WHERE id = :id"), {"o": cur["display_name"], "id": sid})
            entry["override_before"] = [cur["display_name_before_override"], cur["override_ts"]]
        receipt["changed"][f"S{sid}"] = entry
    print(f"changed {len(receipt['changed'])} of {len(EDITS)}; skipped {len(receipt['skipped'])}", flush=True)
    for s, why in receipt["skipped"].items():
        print("SKIP", s, why, flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    receipt["reverted"], receipt["skipped"] = 0, {}
    for s, e in prior["changed"].items():
        sid, done = int(s.lstrip("S")), []
        for f in FIELDS & set(e):
            old, new = e[f]
            n = conn.execute(text(f"UPDATE sources SET {f} = :old, updated_date = now() "
                                  f"WHERE id = :id AND {f} IS NOT DISTINCT FROM :new"),
                             {"old": old, "new": new, "id": sid}).rowcount
            (done if n else receipt["skipped"].setdefault(s, [])).append(f)
        if "display_name" in done:
            dbo, ots = e.get("override_before", [None, None])
            conn.execute(text("UPDATE sources SET display_name_before_override = :dbo, override_timestamp = "
                              "CAST(:ots AS timestamptz) WHERE id = :id"), {"dbo": dbo, "ots": ots, "id": sid})
        receipt["reverted"] += bool(done)
    print(f"reverted {receipt['reverted']} of {len(prior['changed'])}; skipped {receipt['skipped']}", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT,
               "job": "oxjob #1177 (support factory, batch 3)"}
    try:
        with engine.begin() as conn:
            if REVERT:
                revert(conn, receipt)
            else:
                apply(conn, receipt)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
