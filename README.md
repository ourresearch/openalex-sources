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
| `sources` | The registry AND the Databricks read contract (read directly via federation). PK = OpenAlex S-id (BIGINT). `id` is a `GENERATED ALWAYS AS IDENTITY` column — new ids auto-mint; explicit ids (backfill / walden-mint import only) require `OVERRIDING SYSTEM VALUE` + a `setval` resync to MAX(id). `issns` is a derived column refreshed from `source_issn` on every write (like `datacite_ids`). Merged sources stay as redirect rows (`merge_into_id`, `merge_into_date`); consumers filter `merge_into_id IS NULL`. |
| `source_issn` | Normalized ISSN membership. **UNIQUE(issn)** is the one-ISSN-one-source invariant. `is_issn_l` marks the linking ISSN. A composite FK (mig. 014) forces `sources.issn_l` to be one of the source's own ISSNs, so `issn_l` can never point at another source's ISSN. |
| `source_datacite_id` | DataCite client → source link. PK on the client id = one-client-one-source. `sources.datacite_ids` (JSONB) is derived from this table. |
| `source_endpoint` | OAI endpoint → source link (PK endpoint_id, FKs to `endpoint` + `sources`). Read daily by walden's CreateSources snapshot into `openalex.sources.endpoint_to_source` for CreateLocationsWithSources' repo-matching tier. `merge_source` re-points a loser's links to the winner. |
| `issn_to_issnl` | ISSN → ISSN-L map, reloaded weekly from the ISSN International Centre's daily file (~2.6M rows). |
| `source_type` | Controlled vocabulary for `sources.type`. |
| `source_merge` | Audit log of every merge (loser, winner, rule, detail JSONB). |
| `source_ingest_issue` | Conflict queue. One row per (feed, issue type, matched id-set) — ever; resolved rows keep their `resolution`. |
| `source_works_count`, `source_publication_years` | Per-source works counts + publication spans, refreshed weekly from the OpenAlex API (`jobs/refresh_source_stats`; `as_of`-stamped). Works counts drive merge winner selection; publication spans drive `is_fully_open_in_jstage`. |
| `crossref_journal`, `datacite_client`, `doaj_journal` | Full-snapshot staging tables (TRUNCATE + reload on each fetch). |
| `jstage_journal`, `ojs_journal`, `high_oa_rate_issn` | OA-flag mapping tables, one-time imports from Databricks (2026-07-02); drive `jobs/apply_oa_flags`. Membership in `high_oa_rate_issn` IS the flag (mig. 019 — curator force-excludes were deleted rather than kept as false rows). Not refreshed — slated to be dropped once the registry's own flags fully supersede them. |
| `source_list`, `source_list_member` | External source lists exposed as `sources.listed_in` (oxjob #1205): list registry (id, maintainer, URL, loaded edition) + ISSN-keyed membership for file-loaded lists. `sources.listed_in` is DERIVED by `sources_lib.recompute_listed_in` = `is_core`→`cwts-core`, `is_in_doaj`→`doaj`, plus active members via `source_issn`. Non-normative: membership only. |

## Jobs

All run as `python -m jobs.<name>` on one-off dynos. Sync jobs accept `--dry-run`
(classify and report, write nothing) and `--limit N`.

| job | what it does |
|---|---|
| `crossref_journals` | Fetch api.crossref.org/journals → `crossref_journal` (~137K). |
| `sync_crossref_journals` | Reconcile staged journals via the shared match cascade (mint / enrich / conflict; no name fallback). Also derives the SciELO flag from the Crossref publisher prefix. |
| `datacite_clients` | Fetch api.datacite.org/clients + providers → `datacite_client` (~4.4K). |
| `sync_datacite_clients` | Shared cascade, ISSN-first: already linked → fill-NULLs; ISSN match → link; no ISSN → guarded name link; else mint (`periodical`→journal, else repository). |
| `doaj` | Fetch the public DOAJ CSV (~23K) and apply `is_in_doaj` / `doaj_license` / `is_in_doaj_start_year`, including delistings. `--mint` also adds journals the registry lacks (shared cascade: ISSN → guarded name link → mint). |
| `issn_to_issnl` | Reload the ISSN→ISSN-L table from issn.org (atomic TRUNCATE + COPY). |
| `resolve_conflicts` | Drain the conflict queue: auto-merge 2-way, exact-normalized-name, type-compatible, un-curated pairs (winner = more works, then lower id); mark the rest `needs_review`. |
| `apply_oa_flags` | Recompute `is_ojs`, `is_oa_high_oa_rate`, `is_fully_open_in_jstage` from the mapping tables. |
| `refresh_source_stats` | Reload `source_works_count` + `source_publication_years` from api.openalex.org/sources (~282K sources, ~1,400 cursor pages, one-transaction TRUNCATE+COPY). Uses `OPENALEX_UI_ADMIN_API_KEY` to run unthrottled. |
| `load_source_list` | **By hand, on request** (no schedule, by decision — oxjob #1205). Full-replace one external source list from a CSV in `data/source_lists/` into `source_list_member`, then `recompute_listed_in`. Run when a maintainer sends a new edition (email / support ticket). |

## Source lists (`sources.listed_in`)

"Which external lists is this source on?" — a multivalued, non-normative column
(`listed_in text[]`, e.g. `{doyens,cwts-core}`) that replaces adding one
boolean per list. Lists live in `source_list`; ISSN-keyed membership for lists we
load from a file lives in `source_list_member`. `recompute_listed_in` is the single
writer of the column and is called by `jobs/doaj` and `jobs/load_source_list`.

**Updating a list is a manual step, on purpose.** There is no fetch job: the
maintainers email a new edition (or it arrives in a support ticket), someone
converts it to the CSV shape documented in `jobs/load_source_list.py`
(`name,issns,active,withdrawn_date,withdrawal_reason`), drops it in
`data/source_lists/<list>-<YYYY-MM-DD>.csv`, and runs:

```bash
python -m jobs.load_source_list --list doyens \
    --csv data/source_lists/doyens-2026-07-01.csv --version 2026-07-01 --dry-run
python -m jobs.load_source_list --list doyens \
    --csv data/source_lists/doyens-2026-07-01.csv --version 2026-07-01
```

Idempotent (re-running the same file changes nothing). Adding a brand-new list =
one `INSERT INTO source_list` (id is the public value users filter on; kebab-case)
plus a load. Walden mirrors the column verbatim (`CreateSources`) into the sources
API and the dehydrated source on work locations, and builds the `source-lists`
entity (`api.openalex.org/source-lists/<id>`) from `source_list` itself.

**A list is not live until four things outside this repo are done** (oxjob #1205):

1. **Works backfill in Elasticsearch.** `listed_in` is excluded from the works
   content hash (so a list load does not re-stamp ~100M works' `updated_date`),
   which also means the nightly ES sync never picks the affected works up. Run the
   Databricks job *Sync All Works to Elasticsearch* with `is_full_sync=false`,
   `listed_in=<id>` (or `*` for every list) after the next nightly end2end has
   rebuilt `openalex_works`. Until then `works?filter=primary_location.source.listed_in:<id>`
   is nearly empty while `sources?filter=listed_in:<id>` is complete.
2. **elastic-api `config/source-lists.yaml`**: add the id under `values:`. It is a
   closed vocabulary — OQL rejects `listed in is <id>` as `invalid_value` until it
   is there. (`PROPERTIES_VERSION` does not change for a new value.)
3. **openalex-gui `src/listedIn.js`**: a short label, or the site shows the bare id.
4. **Help center** `content/data/source-lists.md` Values table (+ `updated:`).

## Scheduling (Advanced Scheduler)

Triggers are managed via the Service API (`https://api.advancedscheduler.io/triggers`,
Bearer `ADVANCED_SCHEDULER_API_TOKEN`) — no dashboard clicking. Current schedule (UTC):

| when | job |
|---|---|
| Mon 05:00 | `issn_to_issnl` |
| Mon 05:15 | `refresh_source_stats` |
| Mon 05:30 | `datacite_clients && sync_datacite_clients` |
| Mon 05:45 | `doaj --mint` |
| Mon 05:55 | `apply_oa_flags` |
| daily 06:00 | `crossref_journals && sync_crossref_journals` |
| daily 06:30 | `resolve_conflicts` |

Failure emails go to all app collaborators on the first failed execution per trigger per
day (exit-code based).

## Core library

`sources_lib.py` holds the primitives every feed shares:

- `MatchContext(conn, name_link=..., exclude_from_names=...)` + `match_source(...)` —
  THE match cascade, one implementation for every feed: direct ISSN match → ISSN-L
  expansion (incoming ISSNs are resolved through the `issn_to_issnl` map, catching
  print/online twins whose ISSN sets don't overlap) → guarded unique-name match
  (`name_link_guard`: ≥3 name tokens, no publisher contradiction, previously-parked
  sources stay parked) → no match. Ambiguous/refused outcomes park in the conflict queue.
- `mint_source(...)` / `enrich_journal(...)` — mint with an auto-minted S-id; feed-refresh
  a matched journal. Enrichment is override-guarded: a source touched by a curator
  (`override_timestamp`) never has its `display_name` overwritten by a feed.
- `recompute_is_oa(conn)` — the SINGLE writer of `is_oa` (any of the four OA signals);
  every feed job calls it at the end of its run instead of asserting `is_oa` itself.
- `merge_source(conn, loser_id, winner_id, rule, ...)` — first-class merge: ISSNs move to
  the winner, the loser becomes a redirect (`issn_l` cleared), the winner's ISSN-L is
  re-resolved over its own enlarged set, and the merge is audited.
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
