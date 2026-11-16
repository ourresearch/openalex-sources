"""Wrong host institutions on 37 repository sources (oxjob #668, 2026-11-16; off-desk lane, run on desk).

Copy into openalex-sources/handoff/ and run from there (it imports db.engine like every handoff script).
Manifest: ~/ox/oxjobs/working/dissertation-repo-affiliations/work/source_host_fixes_2026-11-16.csv (37 rows,
built live 2026-11-16 by build_manifest.py: each source's sampled location URLs sit on the new host's domain or its
resolver prefix, never on the old host's). Groups:
  - top-dissertation-sources (12, ~994K works): e.g. NUSL S4306402266 hosted by Univ. of Arkansas for Medical
    Sciences -> National Library of Technology; Lume S4306401468 -> UFRGS (was its hospital); UMM -> Univ.
    Muhammadiyah Malang (was Univ. of Maine at Machias).
  - eprints-southampton (10, ~47K): EPrints-software repositories of other organizations hosted by Southampton;
    6 get their real institution, 4 (company/project archives) get no host.
  - belgrade-hospital (15, ~29K): University of Belgrade faculty/institute repositories hosted by a hospital
    (KBC Dr Dragisa Misovic) -> their own faculty/institute.

WORKS RE-STAMP: works with a location in these sources get the new host embedded (~1.07M works). Register it as a rider
in change-throughput-coordinator/changes.yaml first and run --execute only on the evening of that night, before 23:00 CT.

Guard per row, re-checked inside the UPDATE: institution_id still equals the old (wrong) host. A rename is applied only
when the registry display_name still equals the manifest's display_name_now (else it is skipped and reported).

    python handoff/oxjob668-source-hosts-2026-11-16.py <manifest.csv>                      # dry run, rolled back
    python handoff/oxjob668-source-hosts-2026-11-16.py <manifest.csv> --execute            # write + print the receipt
    python handoff/oxjob668-source-hosts-2026-11-16.py --revert <receipt.json> [--execute]  # put the old values back

Desk apply lane 16 Nov: the receipt now keeps each renamed row's display_name_before_override and override_timestamp,
and --revert restores them too. On desk run it through run_registry.py (lanes_registry login, #1831).
"""
import csv
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
num = lambda oid: int(oid.lstrip("SI")) if oid else None


class DryRun(Exception):
    pass


def apply(conn, receipt, manifest):
    rows = list(csv.DictReader(open(manifest, encoding="utf-8")))
    assert len({r["source_id"] for r in rows}) == len(rows), "duplicate source ids"
    receipt["changed"], receipt["skipped"] = {}, {}
    for r in rows:
        sid, old, new = num(r["source_id"]), num(r["host_expected_now"]), num(r["host_new_id"])
        cur = conn.execute(text("SELECT display_name, institution_id, display_name_before_override, "
                                 "CAST(override_timestamp AS text) AS override_ts FROM sources WHERE id = :id"),
                            {"id": sid}).fetchone()
        if cur is None or cur.institution_id != old:
            receipt["skipped"][r["source_id"]] = f"institution_id is {None if cur is None else cur.institution_id}, not {old}"
            continue
        n = conn.execute(text("UPDATE sources SET institution_id = :new, updated_date = now() "
                              "WHERE id = :id AND institution_id = :old"), {"new": new, "id": sid, "old": old}).rowcount
        assert n == 1, r
        entry = {"institution_id": [old, new]}
        if r["rename_to"]:
            if (cur.display_name or "").strip() == r["display_name_now"].strip():
                conn.execute(text("UPDATE sources SET display_name = :n, display_name_before_override = :o, "
                                  "override_timestamp = now() WHERE id = :id"),
                             {"n": r["rename_to"], "o": cur.display_name, "id": sid})
                entry["display_name"] = [cur.display_name, r["rename_to"]]
                entry["override_before"] = [cur.display_name_before_override, cur.override_ts]
            else:
                entry["rename_skipped"] = f"registry name {cur.display_name!r} differs from the manifest's"
        receipt["changed"][r["source_id"]] = entry
    print(f"changed {len(receipt['changed'])} of {len(rows)}; skipped {len(receipt['skipped'])}", flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    receipt["reverted"], receipt["skipped"] = 0, {}
    for s, e in prior["changed"].items():
        old, new = e["institution_id"]
        n = conn.execute(text("UPDATE sources SET institution_id = :old, updated_date = now() "
                              "WHERE id = :id AND institution_id IS NOT DISTINCT FROM CAST(:new AS bigint)"),
                         {"old": old, "new": new, "id": num(s)}).rowcount
        if n and "display_name" in e:
            dbo, ots = e.get("override_before", [None, None])
            conn.execute(text("UPDATE sources SET display_name = :o, display_name_before_override = :dbo, "
                              "override_timestamp = :ots WHERE id = :id AND display_name = :n"),
                         {"o": e["display_name"][0], "n": e["display_name"][1], "dbo": dbo, "ots": ots, "id": num(s)})
        receipt["reverted"] += n
        if not n:
            receipt["skipped"][s] = "edited since"
    print(f"reverted {receipt['reverted']} of {len(prior['changed'])}", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT}
    try:
        with engine.begin() as conn:
            if REVERT:
                revert(conn, receipt)
            else:
                apply(conn, receipt, sys.argv[1])
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
