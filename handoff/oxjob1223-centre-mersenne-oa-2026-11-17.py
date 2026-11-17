"""Apply the Centre Mersenne journals' OA curations (oxjob #1223, 2026-11-17 CT) with
jobs.apply_source_curations, from desk through the lanes_registry route (#1831).

users-api curations (oxjob #1223 work/source_curations_2026-11-17.jsonl): is_oa 'true' plus the
OA-from flip year (ruling #1239: keep the OA-from date), each checked on the journal's own site:
  S140186129  AFST                     flip 2007 (DOAJ oa_start 2008; ZD 1536)
  S4210194595 Publ. math. Besancon     flip 2016 (status quo; the site gives no OA-from year)
  S4210224905 SMAI-JCM                 flip 2014 (free to read since volume 1, 2015)
  S4210171431 Confluentes Mathematici  flip 2012 (World Scientific until 31 Dec 2012; OA since 1 Jan 2013)
Their works re-stamp in the next End 2 End (night 2026-11-18).

Guards: only these 4 sources are touched; each ends is_oa TRUE with the flip year above.

    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py <this file> [--execute]
Receipt: handoff/oxjob1223-centre-mersenne-oa-2026-11-17-receipt.json (override and flags before/after).
Revert: post newer curations (oa_flip_year back to each receipt row's override_before, or is_oa as before), then
run apply_source_curations for these ids; registry only: restore each receipt row's override_before (DELETE
FROM source_oa_override WHERE source_id = <id> where it was null), then apply_oa_flags + recompute_is_oa.
"""
import os
import sys
import types

sys.path.insert(0, os.getcwd())  # the openalex-sources checkout
os.environ.pop("SENTRY_DSN", None)
sys.modules.setdefault("sentry_sdk", types.ModuleType("sentry_sdk"))  # jobs/__init__; no DSN = no-op

from jobs import apply_source_curations as asc  # noqa: E402

EXECUTE = "--execute" in sys.argv
FLIP = {140186129: 2007, 4210194595: 2016, 4210224905: 2014, 4210171431: 2012}
RECEIPT = "handoff/oxjob1223-centre-mersenne-oa-2026-11-17-receipt.json"

curations = asc.fetch_curations(os.environ["OPENALEX_API_KEY"])
desired = {s: f for s, f in asc.latest_by_source(curations).items() if s in FLIP}
assert set(desired) == set(FLIP), sorted(set(FLIP) - set(desired))
for sid, fields in desired.items():
    assert fields["is_oa"][0] is True and fields["oa_flip_year"][0] == FLIP[sid], (sid, fields)

receipt = asc.run(dry_run=True, curations=curations, receipt_path=RECEIPT, source_ids=list(FLIP))
assert {c["source_id"] for c in receipt["changes"]} <= set(FLIP)
assert not receipt["skipped"], receipt["skipped"]
for c in receipt["changes"]:
    assert c["flags_after"]["is_oa"] is True, c
    assert c["override_after"]["curated_flip_year"] == FLIP[c["source_id"]], c
print(f"rehearsal ok: {len(receipt['changes'])} overrides, all is_oa TRUE, flip years as curated", flush=True)
if EXECUTE:
    receipt = asc.run(dry_run=False, curations=curations, receipt_path=RECEIPT, source_ids=list(FLIP))
    print("COMMITTED", flush=True)
