"""Fill host organizations on OJS journal sources from the install-domain manifest (oxjob #1563).

Consumes the judged manifest (Databricks openalex_dev.sources.oxjob1563_write_manifest, exported to
CSV): one row per source with the host organization the install domain proposes and an LLM-judge YES
(confidence >= 0.8, domain not a platform). Casey, 2026-10-06: target is the university's publisher
entity when a ROR-matched one exists, else the institution entity.

Manifest columns used: source_id, target_kind ('publisher'|'institution'), target_id, dom.

Per-row guards (any failure -> skip + counted, never a write):
  - source exists (merged sources are deleted outright, so missing covers merged)
  - BOTH publisher_id and institution_id are currently NULL (never overwrites; re-runs are idempotent)
  - the registry's current homepage_url host still equals, or ends with, the manifest domain
    (drift guard: the manifest was judged on a snapshot; minted sources carry no publisher
    string, so the domain is the only stable thing to re-check)

Default DRY RUN; --execute to write. --limit N for the canary rung. Writes a receipt CSV.

  python -m jobs.fill_host_org_from_domain --manifest m.csv [--limit 10] [--execute] [--receipt r.csv]
"""
import argparse
import csv
import re
from datetime import datetime, timezone

from sqlalchemy import text

from db import engine

VALID_KINDS = {"publisher", "institution"}
HOST_RE = re.compile(r"^(?:https?://)?(?:www\.)?([^/:?#]+)", re.I)


def host_of(url):
    m = HOST_RE.match((url or "").strip().lower())
    return m.group(1) if m else ""


def on_domain(host, dom):
    return bool(host) and (host == dom or host.endswith("." + dom))


def load_manifest(path, limit=None):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            kind = row["target_kind"].strip()
            if kind not in VALID_KINDS:
                raise ValueError(f"bad target_kind {kind!r} on source {row['source_id']}")
            rows.append({
                "source_id": int(row["source_id"]),
                "kind": kind,
                "target_id": int(float(row["target_id"])),
                "dom": row["dom"].strip().lower(),
            })
            if limit and len(rows) >= limit:
                break
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--execute", action="store_true", help="write (default: dry run)")
    ap.add_argument("--limit", type=int, default=None, help="only the first N manifest rows (canary)")
    ap.add_argument("--receipt", default=None, help="receipt CSV path (default: handoff/oxjob1563-host-org-<ts>.csv)")
    args = ap.parse_args()

    manifest = load_manifest(args.manifest, args.limit)
    counts = {"linked": 0, "already_linked": 0, "missing_source": 0, "domain_drift": 0}
    drift_examples = []

    current = {}
    ids = [m["source_id"] for m in manifest]
    with engine.connect() as conn:
        for i in range(0, len(ids), 5000):
            for r in conn.execute(text(
                "SELECT id, publisher_id, institution_id, homepage_url FROM sources WHERE id = ANY(:ids)"
            ), {"ids": ids[i:i + 5000]}).fetchall():
                current[r.id] = r

    to_write = []
    for m in manifest:
        row = current.get(m["source_id"])
        if row is None:
            counts["missing_source"] += 1
            continue
        if row.publisher_id is not None or row.institution_id is not None:
            counts["already_linked"] += 1
            continue
        if not on_domain(host_of(row.homepage_url), m["dom"]):
            counts["domain_drift"] += 1
            if len(drift_examples) < 10:
                drift_examples.append((m["source_id"], row.homepage_url, m["dom"]))
            continue
        counts["linked"] += 1
        to_write.append((m["kind"], m["target_id"], m["source_id"], m["dom"]))

    written = []
    if args.execute:
        # One VALUES-join UPDATE per 500 rows (Heroku round-trips); every guard re-checked server-side.
        CHUNK = 500
        by_kind = {}
        for kind, tid, sid, dom in to_write:
            by_kind.setdefault(kind, []).append((sid, tid, dom))
        for kind, rows in by_kind.items():
            column = "publisher_id" if kind == "publisher" else "institution_id"
            for i in range(0, len(rows), CHUNK):
                chunk = rows[i:i + CHUNK]
                values_sql = ", ".join(f"({sid}, {tid}, :d{j})" for j, (sid, tid, _) in enumerate(chunk))
                params = {f"d{j}": dom for j, (_, _, dom) in enumerate(chunk)}
                with engine.begin() as conn:
                    res = conn.execute(text(
                        f"UPDATE sources s SET {column} = v.tid, updated_date = now() "
                        f"FROM (VALUES {values_sql}) AS v(sid, tid, dom) "
                        "WHERE s.id = v.sid "
                        "  AND s.publisher_id IS NULL AND s.institution_id IS NULL "
                        "  AND (lower(regexp_replace(coalesce(s.homepage_url, ''), '^(https?://)?(www\\.)?([^/:?#]+).*$', '\\3')) = v.dom "
                        "       OR lower(regexp_replace(coalesce(s.homepage_url, ''), '^(https?://)?(www\\.)?([^/:?#]+).*$', '\\3')) LIKE '%.' || v.dom) "
                        "RETURNING s.id"
                    ), params)
                    done_ids = {r[0] for r in res.fetchall()}
                for sid, tid, dom in chunk:
                    written.append((sid, kind, tid, dom, sid in done_ids))
                print(f"{column}: committed {len(done_ids)}/{len(chunk)}", flush=True)
    else:
        for kind, tid, sid, dom in to_write[:20]:
            print(f"WOULD SET {kind}_id={tid} on source {sid} ({dom})")
        if len(to_write) > 20:
            print(f"... and {len(to_write) - 20} more")

    mode = "EXECUTE" if args.execute else "DRY RUN"
    print(f"\n{mode} — {len(manifest)} manifest rows: {counts}")
    for sid, got, dom in drift_examples:
        print(f"  drift: source {sid} homepage {got!r} not on {dom}")

    if args.execute and written:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
        path = args.receipt or f"handoff/oxjob1563-host-org-{ts}.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            wr = csv.writer(f)
            wr.writerow(["source_id", "target_kind", "target_id", "dom", "written"])
            wr.writerows(written)
        print(f"receipt: {path} ({sum(1 for w in written if w[4])} written)")


if __name__ == "__main__":
    main()
