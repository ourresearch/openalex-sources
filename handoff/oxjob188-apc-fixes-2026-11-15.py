"""Registry fixes for the six open support tickets on oxjob #188 (diamond vs gold, APC), support factory #2254,
2026-11-15. Every value was checked on the journal's own page or its DOAJ record; receipts in the #188 job,
work/receipts/. Re-checked live 2026-11-15 23:30 CT (work/receipts/live_*_2026-11-15T2330.json).

  ZD 8512   S2764708357 Theoretical Economics Letters (SCIRP): APC $599 (scirp.org/journal/apc?journalid=666)
  ZD 6025   S2739093061 Mycosphere: EUR 2500 (mycosphere.org/submissions.php + DOAJ); apc_usd 2763 at ECB 2026-11-13 1.1052
  ZD 6027   S4210203566 Studies in Fungi: USD 1500 (DOAJ has_apc; maxapress.com/sif/article_processing_charges)
  ZD 6028   S4210238882 Forestry Research: USD 1950 (DOAJ has_apc; maxapress.com/forres/article_processing_charges)
  ZD 23139  S4210194694 DOCA: OA override (curated_is_oa true, no flip: OA since its first issue 2021), APC 0, homepage
            (hope.uzh.ch/doca/aboutthedatabase: CC BY-NC-ND 4.0, "Doca does not charge any publishing fees")
  ZD 1537   S4210236331 Annales Henri Lebesgue: host Centre Mersenne -> ENS Rennes (I4210146044), APC 0
            (annales.lebesgue.fr/index.php/AHL/about: "fully open access and free of author charges ... owned by the
            Ecole Normale Superieure de Rennes"; Centre Mersenne hosts the platform)

Part C (--with-scirp-fill): the same fix for every Scientific Research Publishing (SCIRP, P4310320480) journal: each
journal's 'Regular APC' from its own fee page (scirp.org/journal/apc?journalid=N, read on desk 2026-11-16, matched by
ISSN; oxjob #188 work/scirp_apc_pages_2026-11-16.jsonl). SCIRP has charged authors per article since it began in 2007,
so one current fee gives the right colour for every year its works appear. List: handoff/oxjob188-scirp-apc-2026-11-16.csv.
Written only where the source still has no fee (apc_usd NULL, apc_prices NULL or []) and its row still exists (since
D1, 13 Aug 2026, a merge deletes the loser's row; there is no merge_into_id column).
DOAJ's fee column (3,324 journals) is NOT here: one current fee for every year would turn old free articles gold
(21% of those works predate 2015); it waits on #1855's per-year rule.

Why the APC fixes the colour: walden CreateWorksBase calls an OA source's work diamond when ZEROIFNULL(apc_usd) = 0,
else gold (oxjob #188). A known fee makes the journal's works gold on the next End 2 End.

Guards: every column is checked against its expected before-value (FOR UPDATE); any drift stops the whole run and
nothing is written. DOCA's OA override is applied the way jobs/apply_oa_flags.py derives it (curated true, no flip ->
is_oa_high_oa_rate true, high_oa_rate_start_year NULL), then recompute_is_oa must change exactly DOCA.

Run on desk (dry run by default; ONE transaction). Safe to run twice: the ticket fixes first, then --with-scirp-fill
once handoff/oxjob188-scirp-apc-2026-11-16.csv is built (finished fixes report "already done").
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob188-apc-fixes-2026-11-15.py [--execute]
The receipt is printed as JSON after the RECEIPT marker.
Revert: --undo --execute puts every column back to its receipt `before` value and deletes DOCA's override row.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import recompute_is_oa  # noqa: E402

EXECUTE = "--execute" in sys.argv
SCIRP_FILL = "--with-scirp-fill" in sys.argv
SCIRP_CSV = os.path.join(HERE, "oxjob188-scirp-apc-2026-11-16.csv")
UNDO = "--undo" in sys.argv
JSONB = {"apc_prices"}


def usd(p):
    return [{"price": p, "currency": "USD"}]


# id: (expected before, new values, ticket note). apc_prices before may be NULL or [] (both mean none).
FIXES = {
    2764708357: (dict(apc_usd=None, apc_prices="none"),
                 dict(apc_usd=599, apc_prices=usd(599)),
                 "ZD 8512 Theoretical Economics Letters: SCIRP fee page lists APC $599"),
    2739093061: (dict(apc_usd=None, apc_prices="none"),
                 dict(apc_usd=2763, apc_prices=[{"price": 2500, "currency": "EUR"}]),
                 "ZD 6025 Mycosphere: EUR 2500 (journal page + DOAJ)"),
    4210203566: (dict(apc_usd=None, apc_prices="none"),
                 dict(apc_usd=1500, apc_prices=usd(1500)),
                 "ZD 6027 Studies in Fungi: USD 1500 (DOAJ)"),
    4210238882: (dict(apc_usd=None, apc_prices="none"),
                 dict(apc_usd=1950, apc_prices=usd(1950)),
                 "ZD 6028 Forestry Research: USD 1950 (DOAJ)"),
    4210194694: (dict(apc_usd=None, apc_prices="none", homepage_url=None, is_oa=False,
                      is_oa_high_oa_rate=False, high_oa_rate_start_year=None),
                 dict(apc_usd=0, apc_prices=usd(0), homepage_url="https://www.hope.uzh.ch/doca",
                      is_oa_high_oa_rate=True, high_oa_rate_start_year=None),
                 "ZD 23139 DOCA: fully OA since 2021, CC BY-NC-ND, no publishing fees"),
    4210236331: (dict(apc_usd=None, apc_prices="none", institution_id=None),
                 dict(apc_usd=0, apc_prices=usd(0), publisher_id=None, institution_id=4210146044,
                      publisher="École Normale Supérieure de Rennes"),
                 "ZD 1537 Annales Henri Lebesgue: owned by ENS Rennes, free of author charges"),
}
OA_OVERRIDE = {4210194694: "ZD 23139 DOCA (Univ. Zurich HOPE): fully OA since its first issue (2021), CC BY-NC-ND, "
                           "no fees; verified 2026-11-15 (oxjob #188)"}
COLS = ["id", "display_name", "apc_usd", "apc_prices", "publisher_id", "institution_id", "publisher", "homepage_url",
        "is_oa", "is_in_doaj", "is_oa_high_oa_rate", "high_oa_rate_start_year"]

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "mode": "undo" if UNDO else "apply",
           "scirp_fill": SCIRP_FILL, "before": {}, "override_before": {}, "actions": [], "after": {},
           "scirp_filled": [], "scirp_skipped": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn, sid):
    r = conn.execute(text(f"SELECT {', '.join(COLS)} FROM sources WHERE id = :id FOR UPDATE"), {"id": sid}).mappings().one()
    return dict(r)


def norm(k, v):
    if k == "apc_prices":
        return "none" if v in (None, []) else v
    return v


def write(conn, sid, cols):
    sets = ", ".join(f"{k} = CAST(:{k} AS JSONB)" if k in JSONB else f"{k} = :{k}" for k in cols)
    params = {k: (json.dumps(v) if k in JSONB and v is not None else v) for k, v in cols.items()}
    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"), {**params, "id": sid}).rowcount
    assert n == 1, (sid, cols)
    log(f"UPDATE sources {sid}: {cols}")


with engine.begin() as conn:
    stray = recompute_is_oa(conn)
    assert stray == 0, f"is_oa out of step on {stray} sources before this batch: stop and look"
    for sid, (expect, new, note) in FIXES.items():
        r = row(conn, sid)
        receipt["before"][str(sid)] = {k: (v if not hasattr(v, "isoformat") else v.isoformat()) for k, v in r.items()}
        o = conn.execute(text("SELECT curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :s"),
                         {"s": sid}).mappings().fetchone()
        receipt["override_before"][str(sid)] = dict(o) if o else None
        if UNDO:
            continue
        drift = {k: (r[k], v) for k, v in expect.items() if norm(k, r[k]) != v}
        if all(norm(k, r[k]) == norm(k, v) for k, v in new.items()):
            log(f"S{sid}: already done ({note})")
            continue
        assert not drift, f"S{sid} drifted from what this script expects: {drift}; nothing written"
        if sid in OA_OVERRIDE:
            assert o is None, (sid, "override row exists", dict(o))
        write(conn, sid, new)
        log(f"  why: {note}")
    if UNDO:
        sys.exit("undo: use the receipts from the --execute runs (handoff/oxjob188-apc-fixes-receipt-*.json); "
                 "set each column back to `before` and DELETE FROM source_oa_override WHERE source_id = 4210194694")
    inserted = 0
    for sid, note in OA_OVERRIDE.items():
        if receipt["override_before"][str(sid)] is not None:   # a second run (e.g. --with-scirp-fill alone)
            assert receipt["override_before"][str(sid)]["curated_is_oa"] is True, (sid, receipt["override_before"][str(sid)])
            log(f"source_oa_override {sid}: already there")
            continue
        conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, true, :n)"),
                     {"s": sid, "n": note})
        inserted += 1
        log(f"INSERT source_oa_override {sid} curated_is_oa=true")
    if SCIRP_FILL:
        import csv
        rows = [r for r in csv.DictReader(open(SCIRP_CSV)) if r["new_apc_usd"]]
        for r in rows:
            sid, fee = int(r["source"].lstrip("S")), int(r["new_apc_usd"])
            assert 0 < fee < 10000, (sid, fee)
            old = conn.execute(text("SELECT apc_prices, publisher_id FROM sources WHERE id = :id FOR UPDATE"),
                               {"id": sid}).fetchone()
            # the list came from the live API's host_organization:P4310320480; the registry may hold a pre-merge
            # publisher id, so the host is recorded, not guarded
            n = conn.execute(text(
                "UPDATE sources SET apc_usd = :u, apc_prices = CAST(:p AS JSONB), updated_date = now() "
                "WHERE id = :id AND apc_usd IS NULL "
                "AND (apc_prices IS NULL OR apc_prices = '[]'::jsonb)"),
                {"u": fee, "p": json.dumps(usd(fee)), "id": sid}).rowcount
            (receipt["scirp_filled"] if n == 1 else receipt["scirp_skipped"]).append(
                {"id": sid, "fee": fee, "page": r["apc_page"], "apc_prices_before": old[0] if old else "missing",
                 "publisher_id": old[1] if old else None})
        log(f"SCIRP fill: {len(receipt['scirp_filled'])} of {len(rows)} written "
            f"(skipped, no longer blank or not SCIRP: {len(receipt['scirp_skipped'])}); "
            f"revert: per scirp_filled row, apc_usd = NULL and apc_prices = its apc_prices_before")
    changed = recompute_is_oa(conn)
    log(f"recompute_is_oa changed {changed} source(s)")
    assert changed == inserted, f"expected exactly DOCA's is_oa to change ({inserted}), got {changed}"
    for sid in FIXES:
        receipt["after"][str(sid)] = {k: (v if not hasattr(v, "isoformat") else v.isoformat())
                                      for k, v in row(conn, sid).items()}
    assert receipt["after"]["4210194694"]["is_oa"] is True
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))
    if not EXECUTE:
        raise RuntimeError("dry run: rolled back (pass --execute to write)")

out = os.path.join(HERE, f"oxjob188-apc-fixes-receipt-{datetime.now(timezone.utc):%Y%m%dT%H%MZ}.json")  # one per run
json.dump(receipt, open(out, "w"), indent=1, ensure_ascii=False, default=str)
print("receipt written:", out)
