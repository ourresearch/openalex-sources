"""Journals credited to the wrong publisher (oxjob #2693, written 2026-11-27, run after night 2026-11-28 publishes):
re-host 277 journals that moved to another publisher, and 45 ASME / ASTM sources that sit on ASM International (a
different society, from a name match).

Phase 1 (277 journals, ~906K works): a journal's publisher_id is written once and never revisited, so a journal that
moved (Annals of the Rheumatic Diseases BMJ -> Elsevier, Knee Surgery Sports Traumatology Arthroscopy Springer ->
Wiley, Child Development Wiley -> OUP) still credits the old company. Each row moves only when ALL hold (Policy choice:
ruling D12, RULINGS.md § 3 Publishers: a publisher record says who publishes a journal, witnessed by its Crossref
membership plus the journal's own site; #1563's 6 Oct host rule; the pattern #1899 used for J Immunol AAI -> OUP):
  - Crossref's journal record (/journals/{issn}, current) and the registrant of >= 80% of the journal's 2024-26 DOIs
    name the same new publisher, and the journal record's title matches ours;
  - the new publisher registers the majority of ALL the journal's works (the backfile's DOIs moved with the title;
    891,098 of the 906,808 works carry the new publisher's registration);
  - the journal's newest DOI lands on the new publisher's own platform;
  - the row is not curated (override_timestamp) and the current host is a publisher company, not a society;
  - the new publisher is one existing OpenAlex record (hand-reviewed map in oxjob #2693 work/06_build.py; all 23
    targets checked live on api.openalex.org 27 Nov: live, unmerged, lineage as expected).
Bar: fresh random 40 of the changes (seed 26931128): the journal's own site names the new host 39/40; the miss (IEEE
Xplore names the journal's own press PSPC) is held. 33 more journals held (backfile stays with the old publisher, site
still on the old platform, signals disagree): oxjob #2693 held.json.

Phase 2 (45 sources, ~150K works): P4310316053 'ASM International' (ROR 0244fmj21, the materials society) hosts 37 ASME
journals and 8 ASTM sources whose own registry text says 'ASME International' / 'ASTM International' (DOI prefixes
10.1115 / 10.1520): an identity repair to P4310321320 American Society of Mechanical Engineers / P4320800609 ASTM
International. ASME is moving its journals to Wiley in 2026; whether a society's journals credit the society or its
publishing partner is a separate open question, and may move ASME's rows again. ASM International is wrong either way.

handoff/oxjob2693-moved-journal-hosts-2026-11-28.csv (oxjob #2693 work/06_build.py + 08_platform_check.py) lists per
source: expected (current) publisher_id and text, the new publisher_id and name, works, witness. Its sha256 is pinned
below: a changed manifest refuses to run.

WORKS RE-STAMP: every work with a location in these sources gets the new host (~1,063,000 works incl. 2,011 xpac). A
rider on the End 2 End that follows the write (changes.yaml id 2693-moved-journal-hosts): run --execute only after the
night before has published, never while a run is going.

Write: one transaction. The rows are locked (FOR UPDATE) before their before-state is read; the UPDATE re-checks per row
that publisher_id still equals the expected old id, institution_id IS NULL and override_timestamp IS NULL. The receipt
is written to disk BEFORE the commit (status 'pending'), then marked 'committed'.

    python handoff/oxjob2693-moved-journal-hosts-2026-11-28.py              # dry run, rolled back
    python handoff/oxjob2693-moved-journal-hosts-2026-11-28.py --execute    # write; receipt -> ...-receipt.json
    python handoff/oxjob2693-moved-journal-hosts-2026-11-28.py --revert handoff/oxjob2693-moved-journal-hosts-2026-11-28-receipt.json [--execute]

Revert takes only a committed --execute receipt. Per row it puts back publisher_id only if the row still carries the id
this run wrote and is still un-curated and not institution-hosted, and the publisher text only if it is still the text
this run wrote. The works follow on the next End 2 End.
"""
import argparse
import csv
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "oxjob2693-moved-journal-hosts-2026-11-28.csv")
RECEIPT = os.path.join(HERE, "oxjob2693-moved-journal-hosts-2026-11-28-receipt.json")
MANIFEST_SHA256 = "1647b4d20f0f1b35059c24d674fe1c79d1afcb93f38097dcaf39d9c94a4c05d3"
EXPECTED_ROWS = 322


class DryRun(Exception):
    pass


def save(path, receipt):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(receipt, f, ensure_ascii=False, indent=1, default=str)
    os.replace(tmp, path)


def rehost(conn, receipt):
    raw = open(CSV, "rb").read()
    digest = hashlib.sha256(raw).hexdigest()
    assert digest == MANIFEST_SHA256, f"manifest changed since review: sha256 {digest}"
    rows = list(csv.DictReader(open(CSV, encoding="utf-8")))
    ids = [int(r["source_id"]) for r in rows]
    assert len(rows) == EXPECTED_ROWS, f"manifest has {len(rows)} rows, expected {EXPECTED_ROWS}"
    assert len(set(ids)) == len(ids), "duplicate source ids in the manifest"
    assert all(r["expected_publisher_id"] != r["new_publisher_id"] for r in rows), "a row moves to its own host"
    for p in sorted({int(r["new_publisher_id"]) for r in rows}):
        used = conn.execute(text("SELECT COUNT(*) FROM sources WHERE publisher_id = :p"), {"p": p}).scalar()
        assert used > 0, f"P{p} hosts no registry source: wrong id?"
    receipt["manifest_sha256"] = digest
    before = {r.id: r for r in conn.execute(text(
        "SELECT id, publisher_id, publisher FROM sources WHERE id = ANY(:i) ORDER BY id FOR UPDATE"), {"i": ids})}
    done = {r.id for r in conn.execute(text("""
        UPDATE sources s SET publisher_id = v.new_id, publisher = v.new_text, updated_date = now()
        FROM unnest(CAST(:ids AS bigint[]), CAST(:old AS bigint[]), CAST(:new AS bigint[]), CAST(:texts AS text[]))
             AS v(id, old_id, new_id, new_text)
        WHERE s.id = v.id AND s.publisher_id = v.old_id AND s.institution_id IS NULL AND s.override_timestamp IS NULL
        RETURNING s.id
    """), {"ids": ids, "old": [int(r["expected_publisher_id"]) for r in rows],
           "new": [int(r["new_publisher_id"]) for r in rows], "texts": [r["new_publisher_text"] for r in rows]})}
    receipt["changed"], receipt["skipped"] = {}, {}
    for r in rows:
        sid = int(r["source_id"])
        b = before.get(sid)
        if sid in done:
            receipt["changed"][str(sid)] = {"publisher_id": [b.publisher_id, int(r["new_publisher_id"])],
                                            "publisher": [b.publisher, r["new_publisher_text"]], "phase": int(r["phase"])}
        else:
            receipt["skipped"][str(sid)] = ("missing" if b is None else
                                            f"publisher_id now {b.publisher_id}, expected {r['expected_publisher_id']}, "
                                            "or curated / institution-hosted")
    by_phase = Counter(v["phase"] for v in receipt["changed"].values())
    print(f"re-hosted {len(done)} of {len(rows)} (phase 1: {by_phase[1]}, phase 2: {by_phase[2]}); "
          f"skipped {len(receipt['skipped'])}", flush=True)
    print("targets:", Counter(r["new_publisher_text"] for r in rows if int(r["source_id"]) in done).most_common(10), flush=True)


def revert(conn, receipt, path):
    prior = json.load(open(path, encoding="utf-8"))
    assert prior.get("job") == 2693 and prior.get("execute") is True and prior.get("status") == "committed", \
        "not a committed --execute receipt of this script"
    receipt["skipped"], receipt["reverted"] = {}, 0
    ids = [int(s) for s in prior["changed"]]
    conn.execute(text("SELECT id FROM sources WHERE id = ANY(:i) ORDER BY id FOR UPDATE"), {"i": ids})
    for sid, ch in prior["changed"].items():
        old_id, new_id = ch["publisher_id"]
        old_text, new_text = ch["publisher"]
        n = conn.execute(text("""
            UPDATE sources SET publisher_id = :old,
                   publisher = CASE WHEN publisher IS NOT DISTINCT FROM :new_text THEN :old_text ELSE publisher END,
                   updated_date = now()
            WHERE id = :id AND publisher_id = :new AND institution_id IS NULL AND override_timestamp IS NULL
        """), {"old": old_id, "old_text": old_text, "new_text": new_text, "id": int(sid), "new": new_id}).rowcount
        if n:
            receipt["reverted"] += 1
        else:
            receipt["skipped"][sid] = f"publisher_id no longer {new_id}, or curated / institution-hosted since"
    print(f"reverted {receipt['reverted']}; skipped {len(receipt['skipped'])} (edited since)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--revert", metavar="RECEIPT")
    args = ap.parse_args()
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": args.execute, "revert_of": args.revert,
               "job": 2693, "status": "dry-run"}
    out = None
    if args.execute:
        out = RECEIPT if not args.revert else RECEIPT.replace("-receipt.json", "-revert-receipt.json")
        assert not os.path.exists(out), f"{out} exists: one receipt per write"
    try:
        with engine.begin() as conn:
            if args.revert:
                revert(conn, receipt, args.revert)
            else:
                rehost(conn, receipt)
            if not args.execute:
                raise DryRun()
            receipt["status"] = "pending"
            save(out, receipt)  # on disk before the commit
        receipt["status"] = "committed"
        save(out, receipt)
        print(f"COMMITTED; receipt {out}")
    except DryRun:
        print("DRY RUN - rolled back")
    print(json.dumps({k: v for k, v in receipt.items() if k not in ("changed", "skipped")} |
                     {"changed": len(receipt.get("changed", {})), "skipped": receipt.get("skipped")}, default=str))


if __name__ == "__main__":
    main()
