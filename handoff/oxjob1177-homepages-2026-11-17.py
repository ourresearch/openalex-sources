"""Stale journal homepages from support tickets, batch 2 (oxjob #1177, support factory, 2026-11-17).

17 sources named by the four tickets the support pipeline attached to #1177 on 16 Nov (27487, 27629, 27630, 27631).
Written off desk; each new homepage was checked on 17 Nov 2026 (receipts: ~/ox/oxjobs/working/source-homepage-url-stale/
work/receipts_2026-11-17.md and new_tickets_registry_2026-11-17.json; expected values read from the registry mirror
openalex.sources.sources the same morning):
  - 27631 Przeglad Geograficzny S4210197784: the journal's new site (the old IGiPZ page now redirects there).
  - 27629 UJoSSH S4210235718: moved from ojs.unud.ac.id (whole host "Database connection failed!") to ejournal3.unud.ac.id,
    where its Sept 2026 Crossref DOIs land; the page shows both ISSNs.
  - 27630 four Pluto Journals records whose homepages are a for-sale domain (IJCS, ReOrient), an expired domain (WRPE)
    or a JSTOR listing (Socialist Lawyer) -> Pluto's own page for each journal (ISSNs on the page match). Policy
    Perspectives S4210188301 (null) gets its Pluto page too. The publisher asked to blank them; 'What right means'
    2026-10-26 (what the record-owning source serves now) gives the live Pluto page instead.
  - 27487 ten UTK V.O.L. Journals sources -> https://voljournals.utk.edu/<code>/ (the pages bot-check a cloud fetch;
    each journal's newest Crossref DOIs land under that path). JOHS S7407122353 already has it.
    TSC S4210205028 also moves host P4310316066 (Institute for Nuclear Security, a nuclear-security institute that
    hosts only IJNS and TSC) -> P4310316583 (University of Tennessee, Knoxville, its V.O.L. siblings' publisher;
    Crossref: "University of Tennessee"). WORKS RE-STAMP: TSC's ~144 works get the new host. Homepages alone
    re-stamp nothing.

Guard per field, re-checked inside each UPDATE: the registry still holds the value read on 17 Nov ("expect").
A source whose fields drifted is skipped and reported (a SKIP showing '' where None was expected means the registry
stores an empty string: change that row's expected value to "" and rerun; nothing else).

    python handoff/oxjob1177-homepages-2026-11-17.py                         # dry run, rolled back
    python handoff/oxjob1177-homepages-2026-11-17.py --execute               # write + print the receipt
    python handoff/oxjob1177-homepages-2026-11-17.py --revert <receipt.json> [--execute]   # put the old values back

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
    (4210197784, [27631], {"homepage_url": ("https://www.igipz.pan.pl/przeglad-geograficzny.html",
                                            "https://przegladgeograficzny.igipz.pan.pl/homepg_pl.html")}),
    (4210235718, [27629], {"homepage_url": ("http://ojs.unud.ac.id/index.php/UJoSSH",
                                            "https://ejournal3.unud.ac.id/index.php/UJoSSH")}),
    (2764545783, [27630], {"homepage_url": ("http://www.cubastudiesjournal.org/ijcs_home.cfm",
                                            "https://www.plutojournals.com/ijcs/")}),
    (4210213848, [27630], {"homepage_url": ("http://www.reorientmag.com", "https://www.plutojournals.com/reorient/")}),
    (2764927909, [27630], {"homepage_url": ("http://www.jstor.org/action/showPublication?journalCode=socialistlawyer",
                                            "https://www.plutojournals.com/socialist-lawyer/")}),
    (2764431486, [27630], {"homepage_url": ("http://wrpe.plutojournals.org", "https://www.plutojournals.com/wrpe/")}),
    (4210188301, [27630], {"homepage_url": (None, "https://www.plutojournals.com/policy-perspectives/")}),
    (5407050864, [27487], {"homepage_url": (None, "https://voljournals.utk.edu/gamut/")}),
    (4210190182, [27487], {"homepage_url": ("http://trace.tennessee.edu/ijns", "https://voljournals.utk.edu/ijns/")}),
    (4210215001, [27487], {"homepage_url": (None, "https://voljournals.utk.edu/jasm/")}),
    (4387287603, [27487], {"homepage_url": (None, "https://voljournals.utk.edu/jaepl/")}),
    (7407089516, [27487], {"homepage_url": (None, "https://voljournals.utk.edu/jatmi/")}),
    (4387288902, [27487], {"homepage_url": (None, "https://voljournals.utk.edu/jcskls/")}),
    (4387279907, [27487], {"homepage_url": (None, "https://voljournals.utk.edu/nqsp/")}),
    (5406991733, [27487], {"homepage_url": (None, "https://voljournals.utk.edu/pursuit/")}),
    (4387286300, [27487], {"homepage_url": (None, "https://voljournals.utk.edu/sfcproceedings/")}),
    (4210205028, [27487], {"homepage_url": ("https://trace.tennessee.edu/tsc/", "https://voljournals.utk.edu/tsc/"),
                           "publisher_id": (4310316066, 4310316583)}),
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
               "job": "oxjob #1177 (support factory)"}
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
