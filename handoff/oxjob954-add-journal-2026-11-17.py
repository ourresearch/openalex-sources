"""oxjob #954 (support factory #2254), desk apply lane 17 Nov 2026: the add_journal manifest the off-desk lane (#2263)
prepared in the #954 job, work/desk_add_journal_rows_2026-11-16.json (feeds re-probed 17 Nov; desk_steps_2026-11-17.md § 1):
  25638 RAM Publisher: mint JULIAN, ECONBUS, JIHS (+ their OJS sets)       Policy choice: follows D5 (RULINGS § 3, 16 Nov)
  7210  mint SOLAV Journal (+ https://solav.me/oai, whole feed)
  8394  mint Evolutio Journal (+ https://www.evolutio.ar/oai-pmh)
  25663 mint RIIAE (+ riidici.com set revista-riiae)
  7233  set S:A on existing S2738442270 Archeologia e Calcolatori
Same code path as `heroku run -a openalex-sources "python -m jobs.add_journal apply --json ..."` (jobs.add_journal.apply,
ONE transaction, dry run by default), run from desk through run_registry.py (lanes_registry login: rows only) because
lanes hold no Heroku key. Before: none of the five new names has a source (API + registry, 17 Nov 10:30 CT); S2738442270
has no S:A endpoint.
Revert (receipt lists every id): for each minted source DELETE its oai_pmh_endpoint, source_oa_override, source_issn and
sources rows; for 7233 DELETE the S:A oai_pmh_endpoint row.
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
apply = mod.apply

ROWS = os.path.expanduser("~/ox/oxjobs/working/resolve-repo-add-request-backlog/work/desk_add_journal_rows_2026-11-16.json")
apply("--json " + ROWS, execute="--execute" in sys.argv, rows=json.load(open(ROWS)))
