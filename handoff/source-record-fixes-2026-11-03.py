"""Source-record fixes for 8 oxjobs (2026-11-03): 17 entries, one change each, from
handoff/source-record-fixes-2026-11-03.json. Rohan runs --execute himself.

  #1590 Rheumatology S183989336 renamed from a person's name        #1747 Medical Research Archives homepage
  #1440 Molecular Life, JOMEINO: names that were article titles       #1895 Luleå's repository renamed + hosted
  #1797 Kyiv repositories eKMAIR / eKNUTSHIR on their own hosts       #871  ROCK hosted by Lublin University of Technology
  #1723 Forman Journal of Economic Studies: OA from 2022, host         #1580 21 September University: JAS and IRSS open

Each entry sets some fields of one source. `expect` holds the live value of every one of those fields (2026-11-03) plus
identity guards. Per entry, against the row as it is now:
  - every field at its expected value       -> apply;
  - every set field already at its new value -> already done (a re-run changes nothing);
  - anything else                            -> stop: the whole run fails and nothing is written (no partial batch).
Renames work like rename() in handoff/26471-sources-2026-10-24.py (display_name_before_override, override_timestamp,
so clean_source_names and merges leave them alone), but they copy the old name into alternate_titles only where the
entry says so.

OA entries add a source_oa_override row and set the two flags apply_oa_flags derives from it (curated_is_oa wins;
start year = flip year + 1, or NULL with no flip year and no high_oa_rate listing). is_oa itself is re-derived by
sources_lib.recompute_is_oa, its single writer. Tripwires: recompute_is_oa changes 0 rows before this batch and
exactly one row per applied OA entry after it, so the weekly apply_oa_flags run (Mon 05:55 UTC) finds nothing to change.

Undo: --undo ENTRY[,ENTRY] or --undo all runs the inverse of each entry (set and expect swapped; the override row
deleted), with the same guards. Dry run by default here too.

Run from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 <runner> handoff/source-record-fixes-2026-11-03.py [--execute] [--only ENTRY[,ENTRY]] [--undo ENTRY[,ENTRY]|all]
where <runner> is oxjob #1899 work/run_registry.py (DATABASE_URL from Heroku into the child env only).
The receipt (before-state, decisions, actions, after-state) is printed as JSON after the RECEIPT marker.
"""
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
MANIFEST = os.path.join(HERE, "source-record-fixes-2026-11-03.json")

# Guarded before a change only: applying an entry changes them as a side effect (is_oa via recompute_is_oa,
# override_timestamp on a rename).
DERIVED = {"is_oa", "override_timestamp"}
JSONB = {"alternate_titles"}


class Drift(Exception):
    pass


class DryRun(Exception):
    pass


def load(path=MANIFEST):
    raw = open(path, "rb").read()
    entries = json.loads(raw)["entries"]
    names = [e["entry"] for e in entries]
    assert len(names) == len(set(names)), "entry names must be unique"
    for e in entries:
        assert e["oxjob"] and e["tickets"], f"{e['entry']}: oxjob and ticket required"
        assert set(e["set"]) <= set(e["expect"]), f"{e['entry']}: every set field needs its before-value in expect"
        assert e["set"] or e["oa"], f"{e['entry']}: changes nothing"
        if "display_name" in e["set"]:
            assert e["expect"]["override_timestamp"] is None, f"{e['entry']}: renames a curator-set name"
        oa = e["oa"]
        if oa is not None:
            # The flags apply_oa_flags derives from this override (its CASE on source_oa_override).
            flip = oa["curated_flip_year"]
            assert e["set"].get("is_oa_high_oa_rate", e["expect"]["is_oa_high_oa_rate"]) is oa["curated_is_oa"], e["entry"]
            want_start = flip + 1 if flip is not None else None
            assert e["set"].get("high_oa_rate_start_year", e["expect"]["high_oa_rate_start_year"]) == want_start, e["entry"]
            assert e["expect"]["is_oa"] is False and oa["curated_is_oa"] is True, f"{e['entry']}: tripwire count assumes false -> true"
    return entries, hashlib.sha256(raw).hexdigest()


def inverse(e):
    """The undo of an entry: set and expect swapped for the changed fields, identity guards kept, override deleted."""
    s, x = e["set"], e["expect"]
    guards = {k: v for k, v in x.items() if k not in s and k not in DERIVED}
    inv = dict(e, set={k: x[k] for k in s}, expect={**guards, **s}, undo=True)
    if e["oa"] is not None:
        inv["oa"] = dict(e["oa"], delete=True)
    return inv


def _norm(k, v):
    if k in JSONB and isinstance(v, str):
        return json.loads(v)
    return v


def decide(e, row, override):
    """'apply' or 'done' for one entry against the source's current row and its source_oa_override row (or None)."""
    assert row is not None, f"{e['entry']}: source {e['source_id']} missing (merged or deleted)"
    s, x = e["set"], e["expect"]
    cur = {k: _norm(k, row[k]) for k in x}
    off_before = {k: (cur[k], v) for k, v in x.items() if cur[k] != v}
    off_after = {k: (cur[k], v) for k, v in s.items() if cur[k] != v}
    off_after.update({k: (cur[k], v) for k, v in x.items() if k not in s and k not in DERIVED and cur[k] != v})
    oa = e["oa"]
    if oa is not None:
        have = None if override is None else (override["curated_is_oa"], override["curated_flip_year"])
        want = (oa["curated_is_oa"], oa["curated_flip_year"])
        start, end = (want, None) if oa.get("delete") else (None, want)  # the override row before / after this entry
        if have != start:
            off_before["source_oa_override"] = (have, start)
        if have != end:
            off_after["source_oa_override"] = (have, end)
    if not off_before:
        return "apply"
    if not off_after:
        return "done"
    raise Drift(f"{e['entry']} (S{e['source_id']}): field: (now, expected) {off_before}")


def main():
    from sqlalchemy import text

    from db import engine
    from sources_lib import recompute_is_oa

    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    pick_ap = ap.add_mutually_exclusive_group()
    pick_ap.add_argument("--only", help="ENTRY[,ENTRY]: apply only these entries")
    pick_ap.add_argument("--undo", help="ENTRY[,ENTRY] or all: run the inverse of these entries")
    args = ap.parse_args()
    execute = args.execute
    entries, sha = load()
    names = [e["entry"] for e in entries]
    if args.undo:
        pick = names if args.undo == "all" else args.undo.split(",")
        mode = "undo"
    else:
        pick = args.only.split(",") if args.only else names
        mode = "apply"
    unknown = set(pick) - set(names)
    assert not unknown, f"unknown entries: {sorted(unknown)}"
    run = [e for e in entries if e["entry"] in pick]
    if mode == "undo":
        run = [inverse(e) for e in reversed(run)]

    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": execute, "mode": mode,
               "manifest_sha256": sha, "entries": [e["entry"] for e in run], "before": {}, "overrides_before": {},
               "decisions": {}, "actions": [], "is_oa_recomputed": {}, "after": {}}

    def log(msg):
        print(msg, flush=True)
        receipt["actions"].append(msg)

    def row(conn, sid):
        r = conn.execute(text("SELECT * FROM sources WHERE id = :id FOR UPDATE"), {"id": sid}).mappings().fetchone()
        return dict(r) if r else None

    def override(conn, sid):
        r = conn.execute(text("SELECT curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :id"),
                         {"id": sid}).mappings().fetchone()
        return dict(r) if r else None

    try:
        with engine.begin() as conn:
            stray = recompute_is_oa(conn)
            receipt["is_oa_recomputed"]["before_batch"] = stray
            assert stray == 0, f"is_oa was out of step on {stray} sources before this batch: stop and look"
            for sid in dict.fromkeys(e["source_id"] for e in run):
                receipt["before"][str(sid)] = row(conn, sid)
                receipt["overrides_before"][str(sid)] = override(conn, sid)
            oa_applied = 0
            for e in run:
                sid, name = e["source_id"], e["entry"]
                verdict = decide(e, row(conn, sid), override(conn, sid))
                receipt["decisions"][name] = verdict
                if verdict == "done":
                    log(f"{name} (oxjob #{e['oxjob']}, S{sid}): already done")
                    continue
                cols = dict(e["set"])
                if "display_name" in cols:
                    # As rename(): mark the name curated. The undo puts back the before-value, NULL (load() checks).
                    cols["override_timestamp"] = None if e.get("undo") else datetime.now(timezone.utc)
                if cols:
                    sets = ", ".join(f"{k} = CAST(:{k} AS JSONB)" if k in JSONB else f"{k} = :{k}" for k in cols)
                    params = {k: (json.dumps(v, ensure_ascii=False) if k in JSONB and v is not None else v)
                              for k, v in cols.items()}
                    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"),
                                     {**params, "id": sid}).rowcount
                    assert n == 1, (name, n)
                    shown = {k: (str(v) if k == "override_timestamp" else v) for k, v in cols.items()}
                    log(f"{name} (oxjob #{e['oxjob']}, S{sid}): UPDATE sources {shown}")
                oa = e["oa"]
                if oa is not None and oa.get("delete"):
                    n = conn.execute(text("DELETE FROM source_oa_override WHERE source_id = :s AND curated_is_oa = :a "
                                          "AND curated_flip_year IS NOT DISTINCT FROM :y"),
                                     {"s": sid, "a": oa["curated_is_oa"], "y": oa["curated_flip_year"]}).rowcount
                    assert n == 1, (name, n)
                    log(f"{name} (oxjob #{e['oxjob']}, S{sid}): DELETE source_oa_override")
                    oa_applied += 1
                elif oa is not None:
                    if oa["curated_flip_year"] is None:
                        # apply_oa_flags uses the high_oa_rate list's start year when there is no flip year.
                        listed = conn.execute(text("SELECT COUNT(*) FROM source_issn si JOIN high_oa_rate_issn h "
                                                   "ON h.issn_l = si.issn WHERE si.source_id = :id"), {"id": sid}).scalar()
                        assert listed == 0, f"{name}: on the high_oa_rate list, so its start year is not NULL: stop and look"
                    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                                      "VALUES (:s, :a, :y, :n)"),
                                 {"s": sid, "a": oa["curated_is_oa"], "y": oa["curated_flip_year"], "n": oa["note"]})
                    log(f"{name} (oxjob #{e['oxjob']}, S{sid}): INSERT source_oa_override "
                        f"curated_is_oa {oa['curated_is_oa']}, curated_flip_year {oa['curated_flip_year']}")
                    oa_applied += 1
            flipped = recompute_is_oa(conn)
            receipt["is_oa_recomputed"]["after_batch"] = flipped
            assert flipped == oa_applied, f"recompute_is_oa changed {flipped} rows, expected {oa_applied}: stop and look"
            log(f"is_oa re-derived by recompute_is_oa on {flipped} sources")
            for sid in receipt["before"]:
                r = row(conn, int(sid))
                receipt["after"][sid] = {k: r[k] for k in (
                    "display_name", "display_name_before_override", "alternate_titles", "publisher", "publisher_id",
                    "institution_id", "homepage_url", "country_code", "type", "is_oa", "is_oa_high_oa_rate",
                    "high_oa_rate_start_year", "override_timestamp")}
                receipt["after"][sid]["source_oa_override"] = override(conn, int(sid))
            counts = {v: list(receipt["decisions"].values()).count(v) for v in ("apply", "done")}
            print(f"{mode}: {counts['apply']} entries applied, {counts['done']} already done, "
                  f"{len(receipt['before'])} sources", flush=True)
            if not execute:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
