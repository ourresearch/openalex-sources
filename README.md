# openalex-sources

The authoritative store for the OpenAlex **sources** entity (journals, repositories,
conference series, ebook platforms — ~281K rows). A Heroku app with a Postgres database
and scheduled background jobs; **no web server**. Databricks reads it through a federated
Unity Catalog connection and continues to build the API/Elasticsearch representations.

This app replaces the Databricks `CreateSources` DLT pipeline as the place where sources
are **created, deduplicated, enriched, merged, and curated** (oxjob #548).

## Design in one paragraph

Every feed (Crossref, DataCite, DOAJ, ISSN portal, ...) is a two-step job: **fetch** a
full snapshot into a staging table, then **sync** it against the registry through a match
cascade — ISSN first, then feed-native id, then mint. Dedup is enforced by write-time
invariants (`UNIQUE(issn)`, one-DataCite-client-one-source, a single id sequence) instead
of per-run cleanup. When a feed's identifiers match more than one source, nothing is
guessed: a **conflict row** is queued, and a resolver job auto-merges only exact-name
duplicates, parking everything else for a human.

## Tables

| table | what it is |
|---|---|
| `sources` | The registry AND the Databricks read contract (read directly via federation). PK = OpenAlex S-id (BIGINT). New ids auto-mint from `source_id_seq`. `issns` is a derived column refreshed from `source_issn` on every write (like `datacite_ids`). Merged sources stay as redirect rows (`merge_into_id`, `merge_into_date`); consumers filter `merge_into_id IS NULL`. |
| `source_issn` | Normalized ISSN membership. **UNIQUE(issn)** is the one-ISSN-one-source invariant. `is_issn_l` marks the linking ISSN. |
| `source_datacite_id` | DataCite client → source link. PK on the client id = one-client-one-source. `sources.datacite_ids` (JSONB) is derived from this table. |
| `issn_to_issnl` | ISSN → ISSN-L map, reloaded weekly from the ISSN International Centre's daily file (~2.6M rows). |
| `source_type` | Controlled vocabulary for `sources.type`. |
| `source_merge` | Audit log of every merge (loser, winner, rule, detail JSONB). |
| `source_ingest_issue` | Conflict queue. One row per (feed, issue type, matched id-set) — ever; resolved rows keep their `resolution`. |
| `source_works_count` | Operational snapshot of per-source works counts from Databricks (winner-selection signal for merges). Check `as_of` before trusting. |
| `crossref_journal`, `datacite_client`, `doaj_journal` | Full-snapshot staging tables (TRUNCATE + reload on each fetch). |
| `jstage_journal`, `ojs_journal`, `high_oa_rate_issn`, `source_publication_years` | OA-flag mapping tables imported from Databricks (`scripts/import_oa_flag_tables.py`); drive `jobs/apply_oa_flags`. |

## Jobs

All run as `python -m jobs.<name>` on one-off dynos. Sync jobs accept `--dry-run`
(classify and report, write nothing) and `--limit N`.

| job | what it does |
|---|---|
| `crossref_journals` | Fetch api.crossref.org/journals → `crossref_journal` (~137K). |
| `sync_crossref_journals` | Upsert staged journals via `sources_lib.upsert_journal_by_issn` (mint / enrich / conflict). Also derives the SciELO flag from the Crossref publisher prefix. |
| `datacite_clients` | Fetch api.datacite.org/clients + providers → `datacite_client` (~4.4K). |
| `sync_datacite_clients` | ISSN-first cascade: already linked → fill-NULLs; ISSN match → link; no ISSN → unique-exact-name link; else mint (`periodical`→journal, else repository). |
| `doaj` | Fetch the public DOAJ CSV (~23K) and apply `is_in_doaj` / `doaj_license` / `is_in_doaj_start_year`, including delistings with a guarded `is_oa` recompute. `--mint` also adds journals the registry lacks (ISSN → unique-name link → mint cascade). |
| `issn_to_issnl` | Reload the ISSN→ISSN-L table from issn.org (atomic TRUNCATE + COPY). |
| `resolve_conflicts` | Drain the conflict queue: auto-merge 2-way, exact-normalized-name, type-compatible, un-curated pairs (winner = more works, then lower id); mark the rest `needs_review`. |
| `apply_oa_flags` | Recompute `is_ojs`, `is_oa_high_oa_rate`, `is_fully_open_in_jstage`, and the derived `is_oa` from the mapping tables. Feeds never assert `is_oa`; this job derives it. |

## Scheduling (Advanced Scheduler)

Triggers are managed via the Service API (`https://api.advancedscheduler.io/triggers`,
Bearer `ADVANCED_SCHEDULER_API_TOKEN`) — no dashboard clicking. Current schedule (UTC):

| when | job |
|---|---|
| Mon 05:00 | `issn_to_issnl` |
| Mon 05:30 | `datacite_clients && sync_datacite_clients` |
| Mon 05:45 | `doaj --mint` |
| Mon 05:55 | `apply_oa_flags` |
| daily 06:00 | `crossref_journals && sync_crossref_journals` |
| daily 06:30 | `resolve_conflicts` |

Failure emails go to all app collaborators on the first failed execution per trigger per
day (exit-code based).

## Core library

`sources_lib.py` holds the primitives every feed shares:

- `upsert_journal_by_issn(conn, issns, ...)` — the match/mint/enrich/conflict cascade.
  Enrichment is override-guarded: a source touched by a curator (`override_timestamp`)
  never has its `display_name` overwritten by a feed.
- `merge_source(conn, loser_id, winner_id, rule, ...)` — first-class merge: ISSNs move to
  the winner, the loser becomes a redirect, the winner's ISSN-L is re-resolved, and the
  merge is audited.
- `normalize_issns`, `normalize_name`, `resolve_issn_l`, `insert_issns` — shared helpers.

## Migrations

Raw SQL in `migrations/NNN_*.sql`, applied in order by `migrate.py` (tracked in
`schema_migrations`, idempotent). Heroku runs `python migrate.py` automatically on every
deploy (release phase). `DATABASE_URL` points at the live Heroku Postgres, so running
`migrate.py` locally also migrates production — that is the intended workflow.

## Local development

```bash
source .venv/bin/activate           # Python 3.13; pip install -r requirements.txt
python -m jobs.sync_crossref_journals --dry-run --limit 2000
```

`.env` (gitignored) needs `DATABASE_URL`; `CROSSREF_API_KEY` and
`ADVANCED_SCHEDULER_API_TOKEN` are optional locally (both are set as Heroku config vars).

Prefer running fetch/sync jobs **on Heroku** (`heroku run:detached -a openalex-sources
"python -m jobs.X"`) — the dyno sits next to the database, so bulk writes are ~30× faster
than over the WAN, and long fetches aren't at the mercy of your laptop.

## Deploying

```bash
git push origin main          # code review / backup
git push heroku main          # deploy; release phase runs migrations
```

## Databricks side

- Federated catalog: `openalex_sources` (UC connection `postgres-sources`); read
  `openalex_sources.public.sources` directly (`issns` federates as a proper array;
  JSONB columns federate as strings — parse with `from_json`). Legacy column names are
  the consumer's job: `issn_l AS issn`, `homepage_url AS webpage`.
- Until the Phase-5 cutover, the walden `CreateSources` DLT still builds the production
  sources table from a frozen 2026-06-30 snapshot; changes made here become
  production-visible at cutover. See the parity audit + cutover checklist in
  oxjobs `working/sources-table-to-postgres/`.

## License

[MIT](LICENSE) © OurResearch
