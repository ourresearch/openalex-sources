"""oxjob #2170: SAE International source records (support ticket 2161).

1. Homepages: the 17 SAE journal / technical-paper sources get their current SAE Mobilus page. 13 held a dead
   saejournals.org host, a JSTOR page, an old Mobilus /content/ path or nothing. Each URL was fetched on
   2026-11-18 and returned 200 with the journal's own title (oxjobs #2170 work/receipts/homepage_checks.json).
2. Publisher: SAE Standards (S7407122200) and SAE International eBooks (S4306463866) have no publisher_id, so their
   ~48K works don't roll up to SAE International (P4322614468), the publisher the 17 journals already use (#629,
   15 Nov). Both are SAE's own: every work carries an SAE DOI (10.4271) and lives on saemobilus.sae.org.

Run on desk (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2170-sae-sources-2026-11-18.py [--execute]
Revert: --revert <receipt.json> [--execute] writes each touched row's before values back
(homepage_url, publisher_id, publisher).
"""
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import engine  # noqa: E402
from sources_lib import normalize_name  # noqa: E402

EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
JOB = 2170
P_SAE = 4322614468
MOBILUS = "https://saemobilus.sae.org/periodicals/"

# source id -> (expected display name, new homepage)
HOMEPAGES = {
    4306529006: ("SAE International Journal of Advances and Current Practices in Mobility",
                 MOBILUS + "sae-international-journal-advances-current-practices-mobility-sae-jrnl-00016"),
    2764416828: ("SAE International Journal of Aerospace", MOBILUS + "sae-international-journal-aerospace-sae-jrnl-00001"),
    2764694759: ("SAE International journal of alternative powertrains",
                 MOBILUS + "sae-international-journal-alternative-powertrains-sae-jrnl-00008"),
    2765010150: ("SAE International journal of commercial vehicles",
                 MOBILUS + "sae-international-journal-commercial-vehicles-sae-jrnl-00002"),
    4210193755: ("SAE International Journal of Connected and Automated Vehicles",
                 MOBILUS + "sae-international-journal-connected-automated-vehicles-sae-jrnl-00012"),
    4210208336: ("SAE International Journal of Electrified Vehicles",
                 MOBILUS + "sae-international-journal-electrified-vehicles-sae-jrnl-00014"),
    2764835558: ("SAE International Journal of Engines", MOBILUS + "sae-international-journal-engines-sae-jrnl-00003"),
    2764470252: ("SAE international journal of fuels and lubricants",
                 MOBILUS + "sae-international-journal-fuels-lubricants-sae-jrnl-00004"),
    2764978717: ("SAE International Journal of Materials and Manufacturing",
                 MOBILUS + "sae-international-journal-materials-manufacturing-sae-jrnl-00005"),
    2764653268: ("SAE International journal of passenger cars. Electronic and electrical systems",
                 MOBILUS + "sae-international-journal-passenger-cars-electronic-electrical-systems-sae-jrnl-00007"),
    4210171723: ("SAE International Journal of Passenger Cars - Mechanical Systems",
                 MOBILUS + "sae-international-journal-passenger-cars-mechanical-systems-sae-jrnl-00006"),
    4210180931: ("SAE International Journal of Passenger Vehicle Systems",
                 MOBILUS + "sae-international-journal-passenger-vehicle-systems-sae-jrnl-00015"),
    4210193364: ("SAE International journal of sustainable transportation, energy, environment & policy",
                 MOBILUS + "sae-international-journal-sustainable-transportation-energy-environment-policy-sae-jrnl-00013"),
    4210171679: ("SAE International journal of transportation cybersecurity and privacy",
                 MOBILUS + "sae-international-journal-transportation-cybersecurity-privacy-sae-jrnl-00011"),
    2764622389: ("SAE International Journal of Transportation Safety",
                 MOBILUS + "sae-international-journal-transportation-safety-sae-jrnl-00009"),
    4210178771: ("SAE International journal of vehicle dynamics, stability, and NVH",
                 MOBILUS + "sae-international-journal-vehicle-dynamics-stability-nvh-sae-jrnl-00010"),
    4210202285: ("SAE technical papers on CD-ROM/SAE technical paper series", "https://saemobilus.sae.org/technical-papers"),
}
# source id -> expected display name; these get publisher_id = SAE International
PUBLISHER = {7407122200: "SAE Standards", 4306463866: "SAE International eBooks"}
FIELDS = ("homepage_url", "publisher_id", "publisher")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB,
           "before": {}, "actions": [], "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def rows(conn, ids):
    return {r["id"]: dict(r) for r in conn.execute(
        text("SELECT id, display_name, homepage_url, publisher_id, publisher FROM sources WHERE id = ANY(:i)"),
        {"i": list(ids)}).mappings()}


def upd(conn, sid, **fields):
    sets = ", ".join(f"{k} = :{k}" for k in fields)
    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"),
                     {**fields, "id": sid}).rowcount
    assert n == 1, (sid, fields)
    log(f"UPDATE sources {sid}: {fields}")


def apply(conn):
    ids = set(HOMEPAGES) | set(PUBLISHER)
    before = rows(conn, ids)
    assert set(before) == ids, f"missing sources: {ids - set(before)}"
    receipt["before"] = {str(k): {f: v for f, v in r.items()} for k, r in before.items()}
    for sid, (name, _) in HOMEPAGES.items():
        assert normalize_name(before[sid]["display_name"]) == normalize_name(name), (sid, before[sid]["display_name"])
    for sid, name in PUBLISHER.items():
        assert normalize_name(before[sid]["display_name"]) == normalize_name(name), (sid, before[sid]["display_name"])
        assert before[sid]["publisher_id"] in (None, P_SAE), (sid, before[sid]["publisher_id"])
    for sid, (_, url) in HOMEPAGES.items():
        if before[sid]["homepage_url"] != url:
            upd(conn, sid, homepage_url=url)
    for sid in PUBLISHER:
        if before[sid]["publisher_id"] != P_SAE:
            upd(conn, sid, publisher_id=P_SAE, publisher="SAE International")
    return ids


def revert(conn, path):
    old = json.load(open(path))["before"]
    for sid, r in old.items():
        upd(conn, int(sid), **{f: r[f] for f in FIELDS})
    return {int(s) for s in old}


def main():
    try:
        with engine.begin() as conn:
            ids = revert(conn, REVERT) if REVERT else apply(conn)
            receipt["after"] = {str(k): r for k, r in rows(conn, ids).items()}
            for k, r in sorted(receipt["after"].items()):
                print(f"  {k}: {r['display_name']!r} pub={r['publisher_id']} hp={r['homepage_url']}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
