"""Stale journal homepages from support tickets, batch 4 (oxjob #1177, support factory, 2026-11-27).

2 sources, homepage_url only (nothing else changes; homepages re-stamp no works). Written off desk; each new homepage
was checked on 27 Nov 2026 (receipts: ~/ox/oxjobs/working/source-homepage-url-stale/work/receipts_2026-11-27.md;
expected values read from the registry mirror openalex.sources.sources, column webpage, at 09:45 CT, both non-NULL):
  - 28599 Botanikai Kozlemenyek S4210219211: the editor asks for the journal's OJS site, in use since 2021,
    https://ojs.mtak.hu/index.php/BotKozlem/index (200, title "Botanikai Kozlemenyek", ISSNs 0006-8144 / 2415-9662
    on the page = the record's). Was http://www.botkozlem.elte.hu, whose page now says (in Hungarian) "this site is
    not updated; for current content visit the NEW site" and links to ojs3.mtak.hu/index.php/BotKozlem.
  - 28547 Kritika Kultura S35038778: the journal moved to Ateneo's Archium, https://archium.ateneo.edu/kk/ (Ateneo de
    Manila's own Kritika Kultura page, www.ateneo.edu/soh/literary-cultural-studies/kk, links there; Cloudflare
    bot-checks cloud fetches of archium itself). Was http://kritikakultura.ateneo.net/ (502); the journal's last
    Crossref landing host, ajol.ateneo.edu, no longer resolves.

Guard per field, re-checked inside each UPDATE: the registry still holds the value read on 27 Nov ("expect").
A source whose fields drifted is skipped and reported (a SKIP showing '' where None was expected means the registry
stores an empty string: change that row's expected value to "" and rerun; nothing else).

    python handoff/oxjob1177-homepages-2026-11-27.py                         # dry run, rolled back
    python handoff/oxjob1177-homepages-2026-11-27.py --execute               # write + print the receipt
    python handoff/oxjob1177-homepages-2026-11-27.py --revert <receipt.json> [--execute]   # put the old values back

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
    (4210219211, [28599], {"homepage_url": ("http://www.botkozlem.elte.hu",
                                            "https://ojs.mtak.hu/index.php/BotKozlem/index")}),
    (35038778, [28547], {"homepage_url": ("http://kritikakultura.ateneo.net/", "https://archium.ateneo.edu/kk/")}),
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
               "job": "oxjob #1177 (support factory, batch 4)"}
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
