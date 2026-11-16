"""Second run of jobs.apply_source_curations (oxjob #1224, 2026-11-16 ~16:00 CT), lanes_registry route.

Two ATRI Press journals re-verified 16 Nov when their site came back (oxjob #1224 work/verify/S5407049177.txt,
S5407049196.txt; ZD 6874, 6875): Journal of Modern Social Sciences and Journal of Current Social Issues Studies,
immediate OA from Vol. 1 (2024), CC BY 4.0. Their 4 curations (is_oa true, oa_flip_year 2023) were posted to users-api
16 Nov; this run copies them into source_oa_override. Guard: only these two change; both end is_oa TRUE. Scoped to them (source_ids): other sessions were
backfilling hand-written overrides as curations at the same time; their own apply copies those.

    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file> [--execute]
Receipt: handoff/oxjob1224-atri-oa-2026-11-16-receipt.json. Revert: as the 37-journal run
(handoff/oxjob1224-source-curations-2026-11-16.py): is_oa 'false' curations + apply_source_curations, or the
receipt's override_before (null: DELETE) and flags_before.
"""
import os
import sys
import types

sys.path.insert(0, os.getcwd())  # the openalex-sources checkout
os.environ.pop("SENTRY_DSN", None)
sys.modules.setdefault("sentry_sdk", types.ModuleType("sentry_sdk"))  # jobs/__init__; no DSN = no-op

from jobs import apply_source_curations as asc  # noqa: E402

EXECUTE = "--execute" in sys.argv
EXPECTED = {5407049177, 5407049196}
RECEIPT = "handoff/oxjob1224-atri-oa-2026-11-16-receipt.json"

curations = asc.fetch_curations(os.environ["OPENALEX_API_KEY"])
receipt = asc.run(dry_run=True, curations=curations, receipt_path=RECEIPT, source_ids=EXPECTED)
changed = {c["source_id"] for c in receipt["changes"]}
assert changed == EXPECTED, sorted(changed ^ EXPECTED)
assert all(c["flags_after"]["is_oa"] is True for c in receipt["changes"]), receipt["changes"]
print(f"rehearsal ok: {len(changed)} overrides, both is_oa TRUE after", flush=True)
if EXECUTE:
    asc.run(dry_run=False, curations=curations, receipt_path=RECEIPT, source_ids=EXPECTED)
    print("COMMITTED", flush=True)
