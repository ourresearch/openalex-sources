"""oxjob #693 (support factory #2254), desk lane 24 Nov 2026: mint the journal 'Replication Research' (ISSN 3052-5977;
ZD 24980) from the manifest in ~/ox/oxjobs/working/datacite-issn-source-matching/work/
add_journal_replication_research_2026-11-24.json. Policy choice: follows D5 (RULINGS § 3, 16 Nov): the journal's own
site and the ISSN register confirm it (receipts in that work/ dir: witness_replication_research_2026-11-24.md).
No OAI feed: its 4 articles are DataCite DOIs (client tib.ulbms) whose container EISSN 3052-5977 the End 2 End's
DataCite container-ISSN tier reads once this source exists.
Same code path as `python -m jobs.add_journal apply --json ...` (ONE transaction, dry run by default), run through
run_registry.py (lanes_registry login: rows only). No source_oa_override row: the OA flag is a curation (#1224 rule).
Before: no OpenAlex source by ISSN 3052-5977 (API /sources?filter=issn:3052-5977 count 0, 24 Nov 09:10 CT).
Revert (receipt lists the id): DELETE the new source_issn and sources rows.
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

ROWS = os.path.expanduser("~/ox/oxjobs/working/datacite-issn-source-matching/work/add_journal_replication_research_2026-11-24.json")
mod.apply("--json " + ROWS, execute="--execute" in sys.argv, rows=json.load(open(ROWS)))
