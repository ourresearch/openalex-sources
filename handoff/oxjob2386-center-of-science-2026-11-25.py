"""Center of Science journals: last host + three homepages (oxjob #2386, support factory, ZD 6878, 2026-11-25).

The publisher (centerofsciencepress.com, Iraq) asked on ZD 6878 for its four journals to show it as publisher and to
carry their homepages. #1582 already bound three of them to P4404684918 'Center of Science' (its own D12 mint) on
19 Nov; the fourth, Center of Engineering Sciences, was minted 12 Nov and missed that wave. Written off desk on
25 Nov 2026; every value checked that morning on the publisher's own site (each journal page shows its ISSN; receipts:
oxjobs #2386 work/publisher_site_receipts_2026-11-25.json; expected values read from the registry mirror
openalex.sources.sources at its 03:00 UTC 25 Nov snapshot):

  S7407122438 Center of Engineering Sciences (eISSN 3137-3151): publisher_id NULL -> 4404684918 (Center of Science;
      the registry's publisher text already says 'Center of Science'), homepage NULL -> https://centerofsciencepress.com/CES
  S7407056831 Center of Computer Science (eISSN 3135-2308): homepage NULL -> https://centerofsciencepress.com/CoCS
  S5407056242 Center of Artificial Intelligence (ISSN 3107-264X): homepage NULL -> https://centerofsciencepress.com/ai
      (its OJS path is /index.php/ai; /CAI answers 404)
  S5407054464 Center of Medicine (3106-5228): already right (host P4404684918, homepage .../COM); checked, not written.

WORKS RE-STAMP: only S7407122438's works (4 on the live API, 25 Nov) get the host embedded; homepages re-stamp nothing.
Run --execute outside an End 2 End run (03:00 UTC until it publishes): after the night publishes and before 19:30 CT
(the input cut-off since 24 Nov, #2473) it rides that evening's run (rider 2386-center-of-science-<night>, no pre-clear).

Guard per row, re-checked inside each UPDATE: the row still has the expected display_name and issn_l (so no value lands
on the wrong journal), and each field still holds the value read on 25 Nov (NULL or '' for "empty"). The host is
written only while publisher_id AND institution_id are both empty. A row that drifted is skipped and reported.

    python handoff/oxjob2386-center-of-science-2026-11-25.py                         # dry run, rolled back
    python handoff/oxjob2386-center-of-science-2026-11-25.py --execute               # write + print the receipt
    python handoff/oxjob2386-center-of-science-2026-11-25.py --revert <receipt.json> [--execute]

On desk run it through run_registry.py (lanes_registry login, #1831). Revert puts each field back to its old value only
if it still carries the value this run wrote.
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
FIELDS = {"homepage_url", "publisher_id"}
EMPTY = None  # expected "empty": NULL, or '' for text columns
P_CENTER_OF_SCIENCE = 4404684918

# (source id, (display_name, issn_l) it must still have, {field: (expected now, new)})
EDITS = [
    (7407122438, ("Center of Engineering Sciences", "3137-3151"),
     {"publisher_id": (EMPTY, P_CENTER_OF_SCIENCE), "homepage_url": (EMPTY, "https://centerofsciencepress.com/CES")}),
    (7407056831, ("Center of Computer Science", "3135-2308"),
     {"homepage_url": (EMPTY, "https://centerofsciencepress.com/CoCS")}),
    (5407056242, ("Center of Artificial Intelligence", "3107-264X"),
     {"homepage_url": (EMPTY, "https://centerofsciencepress.com/ai")}),
]
# Checked, never written: the journal that is already right.
CHECK_ONLY = [(5407054464, ("Center of Medicine", "3106-5228"),
               {"publisher_id": P_CENTER_OF_SCIENCE, "homepage_url": "https://centerofsciencepress.com/COM"})]


class DryRun(Exception):
    pass


def _empty(v):
    return v is None or v == ""


def _matches(cur, expected):
    return _empty(cur) if expected is EMPTY else cur == expected


def _row(conn, sid):
    return conn.execute(text("SELECT display_name, issn_l, homepage_url, publisher_id, institution_id "
                             "FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()


def apply(conn, receipt):
    assert len({e[0] for e in EDITS}) == len(EDITS), "duplicate source ids"
    receipt["changed"], receipt["skipped"], receipt["checked"] = {}, {}, {}
    for sid, (name, issn_l), want in CHECK_ONLY:
        cur = _row(conn, sid)
        receipt["checked"][f"S{sid}"] = dict(cur) if cur else None
        print(f"CHECK S{sid}", "ok" if cur and cur["display_name"] == name and cur["issn_l"] == issn_l
              and all(cur[f] == v for f, v in want.items()) else f"DIFFERS {dict(cur) if cur else None}", flush=True)
    for sid, (name, issn_l), sets in EDITS:
        assert set(sets) <= FIELDS
        cur = _row(conn, sid)
        if cur is None:
            receipt["skipped"][f"S{sid}"] = {"row": None}
            continue
        drift = {f: cur[f] for f, (old, _) in sets.items() if not _matches(cur[f], old)}
        if cur["display_name"] != name or cur["issn_l"] != issn_l:
            drift["identity"] = [cur["display_name"], cur["issn_l"]]
        if "publisher_id" in sets and not _empty(cur["institution_id"]):
            drift["institution_id"] = cur["institution_id"]
        if drift:
            receipt["skipped"][f"S{sid}"] = {"registry_now": drift}
            continue
        entry = {}
        for f, (_, new) in sets.items():
            old = cur[f]
            n = conn.execute(text(f"UPDATE sources SET {f} = :new, updated_date = now() "
                                  f"WHERE id = :id AND display_name = :name AND issn_l = :issn_l "
                                  f"AND {f} IS NOT DISTINCT FROM :old"
                                  + (" AND institution_id IS NULL" if f == "publisher_id" else "")),
                             {"new": new, "old": old, "id": sid, "name": name, "issn_l": issn_l}).rowcount
            assert n == 1, (sid, f, n)
            entry[f] = [old, new]
        receipt["changed"][f"S{sid}"] = entry
    print(f"changed {len(receipt['changed'])} of {len(EDITS)}; skipped {len(receipt['skipped'])}", flush=True)
    for s, why in receipt["skipped"].items():
        print("SKIP", s, why, flush=True)
    for s, e in receipt["changed"].items():
        print("CHANGE", s, e, flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    receipt["reverted"], receipt["skipped"] = 0, {}
    for s, e in prior["changed"].items():
        sid, done = int(s.lstrip("S")), []
        for f in sorted(FIELDS & set(e)):
            old, new = e[f]
            n = conn.execute(text(f"UPDATE sources SET {f} = :old, updated_date = now() "
                                  f"WHERE id = :id AND {f} IS NOT DISTINCT FROM :new"),
                             {"old": old, "new": new, "id": sid}).rowcount
            (done if n else receipt["skipped"].setdefault(s, [])).append(f)
        receipt["reverted"] += bool(done)
    print(f"reverted {receipt['reverted']} of {len(prior['changed'])}; skipped {receipt['skipped']}", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT,
               "job": "oxjob #2386 (support factory, ZD 6878)"}
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
