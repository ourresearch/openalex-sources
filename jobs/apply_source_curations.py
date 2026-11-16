"""Copy journals' open-access corrections from the central curations table into
source_oa_override (oxjob #1224).

Jason, 2026-11-14: every correction goes through users-api's one curations table,
never a side table. A source's OA flag is curated there (entity 'sources', property
'is_oa' with value 'true'/'false', or 'oa_flip_year' with the last closed year), and
this job makes source_oa_override a derived copy of it:

- for each source with a curation, the latest curation per field wins (by created,
  then id; users-api never deletes source curations) and is written to its column
  (is_oa -> curated_is_oa, oa_flip_year -> curated_flip_year);
- a field nobody curated keeps the row's value, so the overrides written by hand
  before this job existed stay until a curation replaces them;
- the note names the curations (and keeps the hand-written note it replaced).

Then the OA flags of the changed sources are re-derived with apply_oa_flags' own
precedence (only their rows) and is_oa recomputed by its single writer. The works
re-stamp in the next End 2End.

Schedule it daily before the End 2 End (05:00 UTC). USERS_API_KEY must belong to a
site curator or admin: GET /curations lists everyone's rows only for them.

  python -m jobs.apply_source_curations [--dry-run] [--receipt PATH]
"""
import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

from sqlalchemy import text

from db import engine
from jobs.apply_oa_flags import apply as apply_oa_flags
from sources_lib import recompute_is_oa

USERS_API_URL = os.environ.get("USERS_API_URL", "https://user.openalex.org")
PER_PAGE = 100
COLUMN = {"is_oa": "curated_is_oa", "oa_flip_year": "curated_flip_year"}
NOTE_PREFIX = "users-api curations: "


def fetch_curations(api_key, base=USERS_API_URL, opener=urllib.request.urlopen):
    """Every 'sources' curation, all pages."""
    rows, page = [], 1
    while True:
        req = urllib.request.Request(
            f"{base}/curations?entity=sources&per_page={PER_PAGE}&page={page}",
            headers={"Authorization": f"Bearer {api_key}", "User-Agent": "openalex-sources"})
        with opener(req, timeout=60) as resp:
            results = json.load(resp)["results"]
        rows += results
        if len(results) < PER_PAGE:
            return rows
        page += 1


def _value(prop, raw):
    if prop == "is_oa":
        return {"true": True, "false": False}[raw]
    return int(raw)


def latest_by_source(curations):
    """{source_id: {prop: (value, curation_id)}}: the latest 'replace' per (source,
    field). Rows of another shape are left out (users-api refuses them anyway)."""
    latest = {}
    for c in curations:
        prop, eid = c.get("property"), c.get("entity_id") or ""
        if c.get("entity") != "sources" or prop not in COLUMN or c.get("action") != "replace" \
                or not eid.startswith("https://openalex.org/S"):
            continue
        key = (int(eid.rsplit("/S", 1)[1]), prop)
        rank = (c.get("created") or "", c.get("id") or "")
        if key not in latest or rank > latest[key][0]:
            latest[key] = (rank, _value(prop, c["value"]), c["id"])
    out = {}
    for (sid, prop), (_rank, value, cid) in latest.items():
        out.setdefault(sid, {})[prop] = (value, cid)
    return out


def plan(desired, existing):
    """The override rows to write: ``[(source_id, before, after)]``, ``before`` the
    current row (or None), ``after`` the row this job wants. ``existing``:
    {source_id: {'curated_is_oa', 'curated_flip_year', 'note'}}."""
    changes = []
    for sid in sorted(desired):
        fields = desired[sid]
        before = existing.get(sid)
        after = dict(before or {"curated_is_oa": None, "curated_flip_year": None, "note": None})
        for prop, (value, _cid) in fields.items():
            after[COLUMN[prop]] = value
        note = NOTE_PREFIX + ", ".join(cid for _prop, (_v, cid) in sorted(fields.items()))
        old = (before or {}).get("note")
        if old and not old.startswith(NOTE_PREFIX):
            note += f" | was: {old}"
        elif old and " | was: " in old:
            note += old[old.index(" | was: "):]
        after["note"] = note
        if after != before:
            changes.append((sid, before, after))
    return changes


class _DryRun(Exception):
    pass


def run(dry_run=False, curations=None, receipt_path=None):
    if curations is None:
        key = os.environ.get("USERS_API_KEY") or os.environ["OPENALEX_API_KEY"]
        curations = fetch_curations(key)
    desired = latest_by_source(curations)
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "dry_run": dry_run,
               "curations": len(curations), "sources": len(desired), "changes": [], "skipped": []}
    try:
        with engine.begin() as conn:
            _write(conn, desired, curations, receipt, receipt_path, dry_run)
    except _DryRun:
        print("DRY RUN - rolled back", flush=True)
    return receipt


def _write(conn, desired, curations, receipt, receipt_path, dry_run):
    # A merged source's row is deleted (source_merge is the ledger, migration 021).
    live = {r.id: r for r in conn.execute(text(
        "SELECT id, is_oa, is_oa_high_oa_rate, high_oa_rate_start_year "
        "FROM sources WHERE id = ANY(:ids)"), {"ids": list(desired)})}
    for sid in [s for s in desired if s not in live]:
        receipt["skipped"].append({"source_id": sid, "why": "not in sources (merged or deleted)"})
        del desired[sid]
    existing = {r.source_id: {"curated_is_oa": r.curated_is_oa, "curated_flip_year": r.curated_flip_year,
                              "note": r.note}
                for r in conn.execute(text(
                    "SELECT * FROM source_oa_override WHERE source_id = ANY(:ids)"), {"ids": list(desired)})}
    changes = plan(desired, existing)
    for sid, before, after in changes:
        conn.execute(text("""
            INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note)
            VALUES (:s, :o, :f, :n)
            ON CONFLICT (source_id) DO UPDATE SET curated_is_oa = EXCLUDED.curated_is_oa,
                curated_flip_year = EXCLUDED.curated_flip_year, note = EXCLUDED.note"""),
            {"s": sid, "o": after["curated_is_oa"], "f": after["curated_flip_year"], "n": after["note"]})
        src = live[sid]
        receipt["changes"].append({"source_id": sid, "override_before": before, "override_after": after,
                                   "flags_before": {"is_oa": src.is_oa, "is_oa_high_oa_rate": src.is_oa_high_oa_rate,
                                                    "high_oa_rate_start_year": src.high_oa_rate_start_year}})
    ids = [sid for sid, _b, _a in changes]
    flagged = apply_oa_flags(conn, source_ids=ids) if ids else 0
    oa = recompute_is_oa(conn) if ids else 0
    after_flags = {r.id: r for r in conn.execute(text(
        "SELECT id, is_oa, is_oa_high_oa_rate, high_oa_rate_start_year FROM sources WHERE id = ANY(:ids)"),
        {"ids": ids})}
    for ch in receipt["changes"]:
        r = after_flags[ch["source_id"]]
        ch["flags_after"] = {"is_oa": r.is_oa, "is_oa_high_oa_rate": r.is_oa_high_oa_rate,
                             "high_oa_rate_start_year": r.high_oa_rate_start_year}
    receipt.update(flag_rows=flagged, is_oa_recomputed=oa)
    print(f"{len(curations)} curations on {len(desired)} sources; {len(changes)} overrides written; "
          f"flags re-derived on {flagged}; is_oa recomputed on {oa}; dry_run={dry_run}", flush=True)
    if receipt_path:
        with open(receipt_path, "w") as f:
            json.dump(receipt, f, indent=1, default=str)
    if dry_run:
        raise _DryRun()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--receipt")
    args = ap.parse_args()
    run(dry_run=args.dry_run, receipt_path=args.receipt)


if __name__ == "__main__":
    main()
