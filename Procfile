# openalex-sources is a database + background-jobs app (no web dyno).
# Heroku runs migrations automatically on each deploy via the release phase.
release: alembic upgrade head

# Worker / scheduled job processes are added in Phase 2+ (ingest, mint, enrich).
