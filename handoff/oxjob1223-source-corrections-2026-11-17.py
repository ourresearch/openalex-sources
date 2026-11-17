"""Source-record corrections, follow-up: oxjob #1223 (support factory), 13 sources, 12 tickets, 17 Nov 2026 CT.

What the 16 Nov write (handoff/oxjob1223-source-corrections-2026-11-16.py, 210/210 live) left open, from #1223
work/followup_desk_2026-11-17.json: 3 TU Delft OPEN proceedings re-hosted to I4387153198 (premium ZD 26881), 2 renames
(SUNU XALAAT 4968, MACAU 24910), 5 homepages, 1 publisher (9070), 1 country (23856, per the ISSN Portal), 3 alternate
titles added, 3 wrong alternate titles removed. Each value checked on the journal's site, DOAJ or the ISSN Portal on
17 Nov (manifest `why`).

Manifest: ~/ox/oxjobs/working/source-metadata-corrections-path/work/registry_manifest_2026-11-17.json. Same entry shape
and guards as the 16 Nov script (identity, drift, host prediction, per-entry SAVEPOINT, is_oa tripwires), plus
`alt_remove`: titles dropped from alternate_titles, matched case-insensitively ("done" when none is there).

WORKS RE-STAMP: ~255 works (3 TU Delft proceedings 193, S7407065712 62: host); renames re-stamp source names on 186 + 3,369.
Rider in change-throughput-coordinator/changes.yaml (#1642), night 2026-11-18.

    python3 <runner> handoff/oxjob1223-source-corrections-2026-11-17.py <manifest.json> [--only S1,S2] [--execute]
    python3 <runner> handoff/oxjob1223-source-corrections-2026-11-17.py --revert <receipt.json> [--execute]
<runner> = ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py (lanes_registry login).
Receipt: handoff/oxjob1223-source-corrections-2026-11-17-receipt.json (the JSON after RECEIPT).
"""
import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from sources_lib import insert_issns, normalize_name, recompute_is_oa, refresh_issns_column

ARGS = sys.argv[1:]
EXECUTE = "--execute" in ARGS
REVERT = ARGS[ARGS.index("--revert") + 1] if "--revert" in ARGS else None
ONLY = set(ARGS[ARGS.index("--only") + 1].split(",")) if "--only" in ARGS else None
POSITIONAL = [a for i, a in enumerate(ARGS) if not a.startswith("--") and (i == 0 or ARGS[i - 1] not in ("--revert", "--only"))]
COLS = ("display_name", "display_name_before_override", "override_timestamp", "alternate_titles", "publisher_id",
        "institution_id", "homepage_url", "country_code", "type", "issn_l", "issns", "is_oa", "is_in_doaj",
        "is_oa_high_oa_rate", "high_oa_rate_start_year")
ISSN = re.compile(r"^\d{4}-\d{3}[\dX]$")


class DryRun(Exception):
    pass


class Skip(Exception):
    pass


def nurl(u):
    if not u:
        return None
    u = u.strip().lower()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return u.rstrip("/")


def host_of(r):
    """The host walden shows: a repository always uses the institution; otherwise the publisher if set."""
    if r["type"] == "repository":
        return f"I{r['institution_id']}" if r["institution_id"] else None
    if r["publisher_id"]:
        return f"P{r['publisher_id']}"
    if r["institution_id"]:
        return f"I{r['institution_id']}"
    return None


def jsonable(r):
    return None if r is None else {k: (v if isinstance(v, (int, float, bool, list, dict)) or v is None else str(v))
                                   for k, v in r.items()}


def get_row(conn, sid, lock=True):
    q = "SELECT " + ", ".join(COLS) + " FROM sources WHERE id = :id" + (" FOR UPDATE" if lock else "")
    r = conn.execute(text(q), {"id": sid}).mappings().fetchone()
    return dict(r) if r else None


def get_override(conn, sid):
    r = conn.execute(text("SELECT curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :id"),
                     {"id": sid}).mappings().fetchone()
    return dict(r) if r else None


def apply_entry(conn, e):
    """Apply one entry. Returns (decision dict, oa_flip_expected 0/1). Raises Skip to roll back this entry only."""
    sid, now, st = e["source_id"], e["now"], e["set"]
    row = get_row(conn, sid)
    if row is None:
        raise Skip("source missing (merged or deleted)")
    target_name = st.get("display_name")
    names = {normalize_name(now["display_name"])} | ({normalize_name(target_name)} if target_name else set())
    if normalize_name(row["display_name"]) not in names:
        raise Skip(f"identity: registry name {row['display_name']!r} is not the API's {now['display_name']!r}")
    dec, cols = {}, {}
    # host
    if "publisher_id" in st or "institution_id" in st:
        cur = host_of(row)
        if cur == e["host_after"]:
            dec["host"] = "done"
        elif cur == now["host"]:
            for k in ("publisher_id", "institution_id"):
                if k in st:
                    cols[k] = st[k]
            pred = host_of({**row, **cols})
            if pred != e["host_after"]:
                raise Skip(f"host: write would give {pred}, not {e['host_after']}")
            dec["host"] = [cur, e["host_after"]]
        else:
            raise Skip(f"host drift: registry host {cur}, API showed {now['host']}, target {e['host_after']}")
    # simple fields
    for k, same in (("homepage_url", lambda a, b: nurl(a) == nurl(b)), ("country_code", lambda a, b: (a or None) == (b or None))):
        if k not in st:
            continue
        if same(row[k], st[k]):
            dec[k] = "done"
        elif same(row[k], now[k]):
            cols[k] = st[k]
            dec[k] = [row[k], st[k]]
        else:
            raise Skip(f"{k} drift: registry {row[k]!r}, API showed {now[k]!r}, target {st[k]!r}")
    # rename
    if target_name:
        if (row["display_name"] or "").strip() == target_name.strip():
            dec["display_name"] = "done"
        elif normalize_name(row["display_name"]) == normalize_name(now["display_name"]) or \
                (row["display_name"] or "").strip() == (now["display_name"] or "").strip():
            cols["display_name"] = target_name
            cols["display_name_before_override"] = row["display_name_before_override"] or row["display_name"]
            cols["override_timestamp"] = datetime.now(timezone.utc)
            dec["display_name"] = [row["display_name"], target_name]
        else:
            raise Skip(f"display_name drift: registry {row['display_name']!r}, API showed {now['display_name']!r}")
    if cols:
        sets = ", ".join(f"{k} = :{k}" for k in cols)
        n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"), {**cols, "id": sid}).rowcount
        assert n == 1, (sid, n)
    # alternate titles
    if e.get("alt_add"):
        cur_row = get_row(conn, sid, lock=False)
        alts = list(cur_row["alternate_titles"] or [])
        have = {t.strip().lower() for t in alts + [cur_row["display_name"] or ""]}
        gained = []
        for t in e["alt_add"]:
            if t and t.strip().lower() not in have:
                gained.append(t)
                have.add(t.strip().lower())
        if gained:
            conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), updated_date = now() WHERE id = :id"),
                         {"a": json.dumps(alts + gained, ensure_ascii=False), "id": sid})
        dec["alternate_titles_added"] = gained
    if e.get("alt_remove"):
        cur_row = get_row(conn, sid, lock=False)
        alts = list(cur_row["alternate_titles"] or [])
        drop = {t.strip().lower() for t in e["alt_remove"]}
        keep = [t for t in alts if (t or "").strip().lower() not in drop]
        removed = [t for t in alts if (t or "").strip().lower() in drop]
        if removed:
            conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), updated_date = now() WHERE id = :id"),
                         {"a": json.dumps(keep, ensure_ascii=False), "id": sid})
            dec["alternate_titles_removed"] = removed
        else:
            dec["alternate_titles_removed"] = "done"
    # ISSNs
    if e.get("issn_add"):
        added, notes = [], {}
        for i in e["issn_add"]:
            i = i.strip().upper()
            if not ISSN.match(i):
                notes[i] = "not an ISSN"
                continue
            owner = conn.execute(text("SELECT source_id FROM source_issn WHERE issn = :i"), {"i": i}).scalar()
            if owner == sid:
                notes[i] = "done"
            elif owner is not None:
                notes[i] = f"owned by S{owner}: not moved"
            else:
                insert_issns(conn, sid, [i], None)
                added.append(i)
        if added:
            refresh_issns_column(conn, sid)
        dec["issn_added"], dec["issn_notes"] = added, notes
    # OA override
    flip = 0
    oa = e.get("oa")
    if oa:
        want = (oa["curated_is_oa"], oa["curated_flip_year"])
        ov = get_override(conn, sid)
        if ov is not None:
            dec["oa"] = "done" if (ov["curated_is_oa"], ov["curated_flip_year"]) == want else \
                f"not applied: override already {ov['curated_is_oa']}/{ov['curated_flip_year']}"
        else:
            listed = conn.execute(text("SELECT COUNT(*) FROM source_issn si JOIN high_oa_rate_issn h ON h.issn_l = si.issn "
                                       "WHERE si.source_id = :id"), {"id": sid}).scalar()
            if oa["curated_flip_year"] is None and listed:
                dec["oa"] = "not applied: on the high_oa_rate list (start year would not be NULL)"
            else:
                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                                  "VALUES (:s, :a, :y, :n)"),
                             {"s": sid, "a": oa["curated_is_oa"], "y": oa["curated_flip_year"],
                              "n": (oa.get("note") or "")[:900] + " (#1223/#1899)"})
                start = oa["curated_flip_year"] + 1 if oa["curated_flip_year"] is not None else None
                conn.execute(text("UPDATE sources SET is_oa_high_oa_rate = :a, high_oa_rate_start_year = :y, "
                                  "updated_date = now() WHERE id = :id"), {"a": oa["curated_is_oa"], "y": start, "id": sid})
                dec["oa"] = [row["is_oa_high_oa_rate"], oa["curated_is_oa"], row["high_oa_rate_start_year"], start]
                flip = 1 if (oa["curated_is_oa"] and row["is_oa"] is not True) else 0
    return dec, flip


def run_apply(conn, receipt):
    man = json.load(open(POSITIONAL[0], encoding="utf-8"))
    entries = [e for e in man["entries"] if ONLY is None or e["entry"] in ONLY]
    assert len({e["entry"] for e in entries}) == len(entries), "duplicate entries"
    if ONLY:
        assert {e["entry"] for e in entries} == ONLY, f"unknown entries: {ONLY - {e['entry'] for e in entries}}"
    stray = recompute_is_oa(conn)
    receipt["is_oa_recomputed_before"] = stray
    assert stray == 0, f"is_oa was out of step on {stray} sources before this batch: stop and look"
    flips = 0
    for e in entries:
        sid = e["source_id"]
        receipt["before"][e["entry"]] = jsonable(get_row(conn, sid, lock=False))
        receipt["overrides_before"][e["entry"]] = get_override(conn, sid)
        try:
            with conn.begin_nested():
                dec, flip = apply_entry(conn, e)
            receipt["decisions"][e["entry"]] = dec
            flips += flip
        except Skip as s:
            receipt["skipped"][e["entry"]] = str(s)
            print(f"SKIP {e['entry']} (tickets {e['tickets']}): {s}", flush=True)
    flipped = recompute_is_oa(conn)
    receipt["is_oa_recomputed_after"] = {"changed": flipped, "expected": flips}
    assert flipped == flips, f"recompute_is_oa changed {flipped} rows, expected {flips}: stop and look"
    for name in receipt["decisions"]:
        sid = int(name[1:])
        receipt["after"][name] = jsonable(get_row(conn, sid, lock=False))
        receipt["after"][name]["source_oa_override"] = get_override(conn, sid)
    changed = [n for n, d in receipt["decisions"].items() if any(isinstance(v, list) and v for v in d.values())]
    receipt["changed"] = changed
    print(f"entries {len(entries)}: changed {len(changed)}, all done already {len(receipt['decisions']) - len(changed)}, "
          f"skipped {len(receipt['skipped'])}; is_oa flipped {flipped}", flush=True)


def run_revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    receipt["reverted"], receipt["revert_skipped"] = [], {}
    for name in prior["changed"]:
        sid, b, a, d = int(name[1:]), prior["before"][name], prior["after"][name], prior["decisions"][name]
        cur = jsonable(get_row(conn, sid))
        cols = {}
        for k in ("publisher_id", "institution_id", "homepage_url", "country_code", "display_name",
                  "display_name_before_override", "override_timestamp", "alternate_titles",
                  "is_oa_high_oa_rate", "high_oa_rate_start_year"):
            if b.get(k) != a.get(k):
                if cur.get(k) != a.get(k):
                    receipt["revert_skipped"][name] = f"{k} edited since: {cur.get(k)!r}"
                    break
                cols[k] = b.get(k)
        else:
            if cols:
                sets = ", ".join(f"{k} = CAST(:{k} AS JSONB)" if k == "alternate_titles" else f"{k} = :{k}" for k in cols)
                params = {k: (json.dumps(v, ensure_ascii=False) if k == "alternate_titles" and v is not None else v)
                          for k, v in cols.items()}
                conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"), {**params, "id": sid})
            for i in d.get("issn_added") or []:
                conn.execute(text("DELETE FROM source_issn WHERE source_id = :s AND issn = :i"), {"s": sid, "i": i})
            if d.get("issn_added"):
                refresh_issns_column(conn, sid)
            if isinstance(d.get("oa"), list) and prior["overrides_before"][name] is None:
                conn.execute(text("DELETE FROM source_oa_override WHERE source_id = :s"), {"s": sid})
            receipt["reverted"].append(name)
    receipt["is_oa_recomputed_after"] = recompute_is_oa(conn)
    print(f"reverted {len(receipt['reverted'])} of {len(prior['changed'])}; skipped {len(receipt['revert_skipped'])}; "
          f"is_oa re-derived on {receipt['is_oa_recomputed_after']}", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT,
               "manifest": POSITIONAL[0] if POSITIONAL else None, "before": {}, "overrides_before": {},
               "decisions": {}, "skipped": {}, "after": {}}
    try:
        with engine.begin() as conn:
            if REVERT:
                run_revert(conn, receipt)
            else:
                run_apply(conn, receipt)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
