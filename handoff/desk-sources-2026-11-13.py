"""Support-desk source fixes Kyle decided 2026-11-07..13 (oxjob #1899 pattern): entries in handoff/desk-sources-2026-11-13.json.

  27271 AICRJ host -> eScholarship            27077 Geodynamica OA (no flip) + host eScholarship
  27066 Ethnologia Europaea flip year 2018      27072 IJOPCD OA, flip year 2022 (no APC: unconfirmed)
  27302 SIAM J Math Data Sci curated NOT OA      27434 Revista PET/Conexoes de Saberes host UFPA + homepage
  27295 Kingston repository host + homepage (the OAI endpoint repoint is separate: precedent repo_repoint_desk_not_casey)

Same guards as handoff/source-record-fixes-2026-11-03.py (per entry: every field at its expected value -> apply; every set
field already at its new value -> done; anything else -> the whole run stops, nothing written), generalised for OA:
  - an OA entry may turn a source OA (true) or not OA (false), or move the flip year of an already-OA source;
  - `oa.before` is the source_oa_override row expected now ([curated_is_oa, curated_flip_year] or null): null -> INSERT,
    else a guarded UPDATE; the undo puts `before` back (DELETE when null);
  - load() checks each OA entry's `set` against what jobs/apply_oa_flags.py derives from the override (is_oa beats flip
    year beats computed; start year = flip + 1, NULL when curated false, NULL with no flip and no high_oa_rate listing),
    so the weekly apply_oa_flags run finds nothing to change;
  - tripwire: recompute_is_oa changes 0 rows before the batch and, after it, exactly the entries whose `is_oa_after`
    differs from their expected is_oa (an already-OA DOAJ source moving its flip year changes no is_oa).

--probe prints, read-only, the current value of every column the manifest names plus the override row and the
high_oa_rate listing for each source (used to fill `expect`). Dry run by default; ONE transaction:
    python3 <runner> handoff/desk-sources-2026-11-13.py [--probe] [--execute] [--only E[,E]] [--undo E[,E]|all]
<runner> = oxjob #1899 work/run_registry.py. The receipt is printed as JSON after the RECEIPT marker.
"""
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
MANIFEST = os.path.join(HERE, "desk-sources-2026-11-13.json")

DERIVED = {"is_oa", "override_timestamp"}
JSONB = {"alternate_titles"}
OA_FIELDS = ("is_oa_high_oa_rate", "high_oa_rate_start_year")


class Drift(Exception):
    pass


class DryRun(Exception):
    pass


def oa_flags(oa):
    """(is_oa_high_oa_rate, high_oa_rate_start_year) that apply_oa_flags derives from this override (no listing)."""
    a, y = oa["curated_is_oa"], oa["curated_flip_year"]
    hoar = a if a is not None else (True if y is not None else None)
    assert hoar is not None, "override sets neither is_oa nor a flip year"
    start = None if (a is False or not hoar) else (y + 1 if y is not None else None)
    return hoar, start


def load(path=MANIFEST):
    raw = open(path, "rb").read()
    entries = json.loads(raw)["entries"]
    names = [e["entry"] for e in entries]
    assert len(names) == len(set(names)), "entry names must be unique"
    for e in entries:
        assert e["oxjob"] and e["tickets"], f"{e['entry']}: oxjob and ticket required"
        assert set(e["set"]) <= set(e["expect"]), f"{e['entry']}: every set field needs its before-value in expect"
        assert e["set"] or e["oa"], f"{e['entry']}: changes nothing"
        assert "display_name" not in e["set"], f"{e['entry']}: no renames in this batch"
        oa = e["oa"]
        if oa is not None:
            hoar, start = oa_flags(oa)
            got = tuple(e["set"].get(k, e["expect"][k]) for k in OA_FIELDS)
            assert got == (hoar, start), f"{e['entry']}: set {got} but apply_oa_flags would derive {(hoar, start)}"
            assert "is_oa_after" in e and "is_oa" in e["expect"], f"{e['entry']}: OA entries state is_oa before and after"
            b = oa.get("before")
            assert b is None or (len(b) == 2), e["entry"]
    return entries, hashlib.sha256(raw).hexdigest()


def inverse(e):
    s, x = e["set"], e["expect"]
    guards = {k: v for k, v in x.items() if k not in s and k not in DERIVED}
    inv = dict(e, set={k: x[k] for k in s}, expect={**guards, **s}, undo=True)
    if e["oa"] is not None:
        b = e["oa"].get("before")
        inv["oa"] = dict(e["oa"], before=[e["oa"]["curated_is_oa"], e["oa"]["curated_flip_year"]],
                         restore=b)   # restore None -> DELETE
        inv["expect"] = dict(inv["expect"], is_oa=e["is_oa_after"])
        inv["is_oa_after"] = x["is_oa"]
    return inv


def _norm(k, v):
    if k in JSONB and isinstance(v, str):
        return json.loads(v)
    return v


def _want_override(oa):
    """(before, after) override rows as (curated_is_oa, curated_flip_year) tuples or None."""
    b = oa.get("before")
    before = tuple(b) if b is not None else None
    if "restore" in oa:   # undo
        r = oa["restore"]
        return before, (tuple(r) if r is not None else None)
    return before, (oa["curated_is_oa"], oa["curated_flip_year"])


def decide(e, row, override):
    assert row is not None, f"{e['entry']}: source {e['source_id']} missing (merged or deleted)"
    s, x = e["set"], e["expect"]
    cur = {k: _norm(k, row[k]) for k in x}
    off_before = {k: (cur[k], v) for k, v in x.items() if cur[k] != v}
    off_after = {k: (cur[k], v) for k, v in s.items() if cur[k] != v}
    off_after.update({k: (cur[k], v) for k, v in x.items() if k not in s and k not in DERIVED and cur[k] != v})
    if e["oa"] is not None:
        have = None if override is None else (override["curated_is_oa"], override["curated_flip_year"])
        start, end = _want_override(e["oa"])
        if have != start:
            off_before["source_oa_override"] = (have, start)
        if have != end:
            off_after["source_oa_override"] = (have, end)
        if cur.get("is_oa") != e["is_oa_after"]:
            off_after["is_oa"] = (cur.get("is_oa"), e["is_oa_after"])
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
    ap.add_argument("--probe", action="store_true")
    pick_ap = ap.add_mutually_exclusive_group()
    pick_ap.add_argument("--only")
    pick_ap.add_argument("--undo")
    args = ap.parse_args()

    if args.probe:
        man = json.load(open(MANIFEST))["entries"]
        cols = sorted({k for e in man for k in list(e["expect"]) + list(e["set"])} |
                      {"display_name", "type", "is_oa", "is_in_doaj", "is_in_scielo", "is_fully_open_in_jstage",
                       "is_oa_high_oa_rate", "high_oa_rate_start_year", "publisher", "publisher_id", "institution_id",
                       "homepage_url", "apc_usd", "override_timestamp"})
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            for sid in dict.fromkeys(e["source_id"] for e in man):
                r = conn.execute(text("SELECT * FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
                r = {k: r[k] for k in cols if k in r} if r else None
                o = conn.execute(text("SELECT curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :id"),
                                 {"id": sid}).mappings().fetchone()
                lst = conn.execute(text("SELECT h.issn_l, h.start_year FROM source_issn si JOIN high_oa_rate_issn h "
                                        "ON h.issn_l = si.issn WHERE si.source_id = :id"), {"id": sid}).fetchall()
                print("PROBE", json.dumps({"source_id": sid, "row": dict(r) if r else None,
                                           "override": dict(o) if o else None,
                                           "high_oa_rate_listing": [list(x) for x in lst]}, default=str, ensure_ascii=False))
        return

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
            want_flips = 0
            for e in run:
                sid, name = e["source_id"], e["entry"]
                verdict = decide(e, row(conn, sid), override(conn, sid))
                receipt["decisions"][name] = verdict
                if verdict == "done":
                    log(f"{name} (oxjob #{e['oxjob']}, S{sid}): already done")
                    continue
                cols = dict(e["set"])
                if cols:
                    sets = ", ".join(f"{k} = CAST(:{k} AS JSONB)" if k in JSONB else f"{k} = :{k}" for k in cols)
                    params = {k: (json.dumps(v, ensure_ascii=False) if k in JSONB and v is not None else v)
                              for k, v in cols.items()}
                    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"),
                                     {**params, "id": sid}).rowcount
                    assert n == 1, (name, n)
                    log(f"{name} (oxjob #{e['oxjob']}, S{sid}): UPDATE sources {cols}")
                oa = e["oa"]
                if oa is not None:
                    before, after = _want_override(oa)
                    if after is None:
                        n = conn.execute(text("DELETE FROM source_oa_override WHERE source_id = :s AND curated_is_oa "
                                              "IS NOT DISTINCT FROM :a AND curated_flip_year IS NOT DISTINCT FROM :y"),
                                         {"s": sid, "a": before[0], "y": before[1]}).rowcount
                        assert n == 1, (name, n)
                        log(f"{name} (oxjob #{e['oxjob']}, S{sid}): DELETE source_oa_override {before}")
                    else:
                        if after[0] is True and after[1] is None:
                            listed = conn.execute(text("SELECT COUNT(*) FROM source_issn si JOIN high_oa_rate_issn h "
                                                       "ON h.issn_l = si.issn WHERE si.source_id = :id"), {"id": sid}).scalar()
                            assert listed == 0, f"{name}: on the high_oa_rate list, so its start year is not NULL: stop and look"
                        if before is None:
                            conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                                              "VALUES (:s, :a, :y, :n)"),
                                         {"s": sid, "a": after[0], "y": after[1], "n": oa["note"]})
                            log(f"{name} (oxjob #{e['oxjob']}, S{sid}): INSERT source_oa_override {after}")
                        else:
                            n = conn.execute(text("UPDATE source_oa_override SET curated_is_oa = :a, curated_flip_year = :y, "
                                                  "note = COALESCE(note, '') || ' | ' || :n WHERE source_id = :s AND curated_is_oa "
                                                  "IS NOT DISTINCT FROM :ba AND curated_flip_year IS NOT DISTINCT FROM :by"),
                                             {"s": sid, "a": after[0], "y": after[1], "n": oa["note"], "ba": before[0], "by": before[1]}).rowcount
                            assert n == 1, (name, n)
                            log(f"{name} (oxjob #{e['oxjob']}, S{sid}): UPDATE source_oa_override {before} -> {after}")
                    if e["expect"]["is_oa"] != e["is_oa_after"]:
                        want_flips += 1
            flipped = recompute_is_oa(conn)
            receipt["is_oa_recomputed"]["after_batch"] = flipped
            assert flipped == want_flips, f"recompute_is_oa changed {flipped} rows, expected {want_flips}: stop and look"
            log(f"is_oa re-derived by recompute_is_oa on {flipped} sources (expected {want_flips})")
            for e in run:
                if e["oa"] is not None and receipt["decisions"][e["entry"]] == "apply":
                    got = row(conn, e["source_id"])["is_oa"]
                    assert got == e["is_oa_after"], f"{e['entry']}: is_oa {got} after, expected {e['is_oa_after']}"
            for sid in receipt["before"]:
                r = row(conn, int(sid))
                receipt["after"][sid] = {k: r[k] for k in (
                    "display_name", "publisher", "publisher_id", "institution_id", "homepage_url", "type", "is_oa",
                    "is_in_doaj", "is_oa_high_oa_rate", "high_oa_rate_start_year", "apc_usd")}
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
