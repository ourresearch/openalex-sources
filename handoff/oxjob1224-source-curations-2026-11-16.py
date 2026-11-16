"""First run of jobs.apply_source_curations (oxjob #1224, 2026-11-16 CT), from desk through the
lanes_registry route (#1831), before the job is deployed and scheduled.

users-api now takes source curations (entity 'sources', is_oa / oa_flip_year; users-api b012277).
74 were posted 16 Nov for 37 journals verified fully open access on their own sites (oxjob #1224
work/verified_ready_2026-11-15.json, receipts work/verify/; ruling #1239: accept a verified OA claim,
keep the OA-from year). This run copies them into source_oa_override and re-derives those sources'
OA flags; their works re-stamp in night 2026-11-17's End 2 End.

Guards: every override written is one of the 37; every one ends is_oa TRUE.

Run from the openalex-sources checkout or worktree (rehearsal by default, ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file> [--execute]
Receipt: handoff/oxjob1224-source-curations-2026-11-16-receipt.json (override and flag before/after per source).
Revert: POST is_oa 'false' curations for the 37 (oxjob #1224 work/post_source_curations.py docstring), then
run jobs.apply_source_curations; or, registry only, restore each receipt row's override_before (DELETE
where it was null) and flags_before.
"""
import json
import os
import sys
import types

sys.path.insert(0, os.getcwd())  # the openalex-sources checkout
os.environ.pop("SENTRY_DSN", None)
sys.modules.setdefault("sentry_sdk", types.ModuleType("sentry_sdk"))  # jobs/__init__; no DSN = no-op

from jobs import apply_source_curations as asc  # noqa: E402

EXECUTE = "--execute" in sys.argv
READY = os.path.expanduser(
    "~/ox/oxjobs/backlog/source-oa-flags-wrong-and-propagating/work/verified_ready_2026-11-15.json")
EXPECTED = {int(r["source"][1:]) for r in json.load(open(READY)) if r["verdict"] == "fully_oa"}
RECEIPT = "handoff/oxjob1224-source-curations-2026-11-16-receipt.json"
assert len(EXPECTED) == 37, len(EXPECTED)

curations = asc.fetch_curations(os.environ["OPENALEX_API_KEY"])
desired = asc.latest_by_source(curations)
assert set(desired) == EXPECTED, sorted(set(desired) ^ EXPECTED)

receipt = asc.run(dry_run=True, curations=curations, receipt_path=RECEIPT)
assert {c["source_id"] for c in receipt["changes"]} <= EXPECTED
assert not receipt["skipped"], receipt["skipped"]
bad = [c for c in receipt["changes"] if c["flags_after"]["is_oa"] is not True]
assert not bad, bad
print(f"rehearsal ok: {len(receipt['changes'])} overrides, all is_oa TRUE after", flush=True)
if EXECUTE:
    receipt = asc.run(dry_run=False, curations=curations, receipt_path=RECEIPT)
    print("COMMITTED", flush=True)
