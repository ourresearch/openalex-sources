"""Apply NeurIPS's OA curation (oxjob #2100 step 7, 2026-11-21 CT) with jobs.apply_source_curations, from desk
through the lanes_registry route (#1831).

users-api curation cur-daL8HMPXcCC7 (oxjob #2100 work/curations_neurips_is_oa_2026-11-21.jsonl): S4363606243, the
conference source 'neural information processing systems', is_oa 'true'. Every paper on proceedings.neurips.cc
(1987-2024) is free to read with no APC; the flag read false, so NeurIPS-led works before 2022 read closed or green.
No flip year: the proceedings were free from the first volume. Its works re-stamp in the next End 2 End (night
2026-11-22), with the NeurIPS proceedings feed (endpoint eijnazg474c9pb6u2fac, registered the same evening).
The daily 02:00 UTC run would copy it too; this run does it now, scoped, with a receipt.

Guards: only S4363606243 is touched; it ends is_oa TRUE.

    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file> [--execute]
Receipt: handoff/oxjob2100-neurips-oa-2026-11-21-receipt.json (override and flags before/after).
Revert: post a newer is_oa 'false' curation for S4363606243, then run apply_source_curations for that id; registry
only: restore the receipt row's override_before (DELETE FROM source_oa_override WHERE source_id = 4363606243 if it
was null), then apply_oa_flags + recompute_is_oa.
"""
import os
import sys
import types

sys.path.insert(0, os.getcwd())  # the openalex-sources checkout
os.environ.pop("SENTRY_DSN", None)
sys.modules.setdefault("sentry_sdk", types.ModuleType("sentry_sdk"))  # jobs/__init__; no DSN = no-op

from jobs import apply_source_curations as asc  # noqa: E402

EXECUTE = "--execute" in sys.argv
SOURCE = 4363606243
RECEIPT = "handoff/oxjob2100-neurips-oa-2026-11-21-receipt.json"

curations = asc.fetch_curations(os.environ["OPENALEX_API_KEY"])
desired = {s: f for s, f in asc.latest_by_source(curations).items() if s == SOURCE}
assert set(desired) == {SOURCE}, "no curation for S4363606243"
assert desired[SOURCE]["is_oa"][0] is True, desired

receipt = asc.run(dry_run=True, curations=curations, receipt_path=RECEIPT, source_ids=[SOURCE])
assert {c["source_id"] for c in receipt["changes"]} <= {SOURCE}
assert not receipt["skipped"], receipt["skipped"]
for c in receipt["changes"]:
    assert c["flags_after"]["is_oa"] is True, c
print(f"rehearsal ok: {len(receipt['changes'])} override(s), is_oa TRUE", flush=True)
if EXECUTE:
    receipt = asc.run(dry_run=False, curations=curations, receipt_path=RECEIPT, source_ids=[SOURCE])
    print("COMMITTED", flush=True)
