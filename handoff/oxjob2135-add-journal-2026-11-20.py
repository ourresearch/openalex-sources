"""oxjob #2135 (support factory #2254), desk apply lane 21 Nov 2026: mint IJSCEES (ZD 8536) + its OJS feed, from the
manifest the off-desk lane (#2263) wrote 20 Nov 2026 in ~/ox/oxjobs/working/doi-less-ojs-journal-no-ingest-lane/work/
desk_add_journal_rows_2026-11-20.json. Policy choice: follows D5 (RULINGS § 3, 16 Nov).
  8536  mint International Journal of Smart Control and Electrical Engineering Systems (ISSN 3137-168X, Iraqi Trainers
        Union, IQ) + https://ijscees.org/index.php/index/oai set pub
Same code path as `python -m jobs.add_journal apply --json ...` (ONE transaction, dry run by default), run through
run_registry.py (lanes_registry login: rows only). on_lists left empty: no source_oa_override row (the OA flag is a
curation, #1224 / 16 Nov rule).
Before: no OpenAlex source by ISSN 3137-168X or name (API 21 Nov 17:00 CT; registry 20 Nov); 0 repo_works oai:oai.ijscees.org:%.
Revert (receipt lists the ids): DELETE the new oai_pmh_endpoint, source_issn and sources rows.
"""
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
# load jobs/add_journal.py by path: jobs/__init__.py imports sentry_sdk, which run_registry's env doesn't carry
spec = importlib.util.spec_from_file_location("add_journal", os.path.join(os.path.dirname(HERE), "jobs", "add_journal.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

ROWS = os.path.expanduser("~/ox/oxjobs/working/doi-less-ojs-journal-no-ingest-lane/work/desk_add_journal_rows_2026-11-20.json")
mod.apply("--json " + ROWS, execute="--execute" in sys.argv, rows=json.load(open(ROWS)))
