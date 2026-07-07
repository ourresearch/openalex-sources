"""Stage 2 of the DataCite job: reconcile `datacite_client` staging against the
sources registry via the shared match cascade (sources_lib) — ISSN-FIRST (the
Databricks pipeline never checked ISSN overlap for DataCite, a duplicate-source
vector this fixes):

  1. client already linked (source_datacite_id)     -> enrich NULL fields only
  2. unlinked, has ISSNs -> match_source (no name fallback):
       1 source  -> LINK it (+ enrich)
       >1        -> conflict row (merge candidate, feed='datacite')
       0         -> MINT (periodical->journal, else repository)
  3. unlinked, no ISSNs -> match_source name fallback among unlinked sources:
     unique guarded match -> LINK; refused/ambiguous -> conflict row;
     none -> MINT repository

  python -m jobs.sync_datacite_clients [--dry-run] [--limit N]
"""
import argparse
from collections import Counter

from sqlalchemy import text

from db import engine
from sources_lib import MatchContext, match_source, mint_source, normalize_issns, park_multi_match

CLIENT_TYPE_TO_SOURCE_TYPE = {"periodical": "journal", "repository": "repository"}

# DataCite parks dead/transferred clients under administrative pseudo-providers;
# they are not live venues and must never mint or link (the 2026-07-07 post-cutover
# LLM audit found 234 sources minted from these buckets — parked as
# 'invalid_source' in the queue).
ADMIN_PROVIDER_PREFIXES = ("deactivated repositories", "repositories moved")


def _refresh_datacite_jsonb(conn, source_id):
    conn.execute(text(
        "UPDATE sources SET datacite_ids = COALESCE((SELECT jsonb_agg(datacite_id ORDER BY datacite_id) "
        "FROM source_datacite_id WHERE source_id = :id), '[]'::jsonb), "
        "datacite_id = COALESCE(datacite_id, (SELECT MIN(datacite_id) FROM source_datacite_id "
        "WHERE source_id = :id)), updated_date = now() WHERE id = :id"
    ), {"id": source_id})


def _link(conn, client_id, source_id):
    conn.execute(text(
        "INSERT INTO source_datacite_id (datacite_id, source_id) VALUES (:d, :s) "
        "ON CONFLICT (datacite_id) DO NOTHING"), {"d": client_id, "s": source_id})
    _refresh_datacite_jsonb(conn, source_id)


def _enrich(conn, source_id, client):
    """Fill missing homepage_url / publisher from the client; never overwrite
    existing values. The WHERE keeps updated_date untouched when nothing fills."""
    conn.execute(text(
        "UPDATE sources SET "
        "  homepage_url = COALESCE(homepage_url, :url), "
        "  publisher = COALESCE(publisher, CASE WHEN publisher_id IS NULL THEN :prov END), "
        "  updated_date = now() "
        "WHERE id = :id "
        "  AND ((homepage_url IS NULL AND :url IS NOT NULL) "
        "       OR (publisher IS NULL AND publisher_id IS NULL AND :prov IS NOT NULL))"
    ), {"id": source_id, "url": client.url, "prov": client.provider_name})


def _mint(conn, ctx, client, issns):
    sid = mint_source(
        conn, ctx, client.display_name,
        source_type=CLIENT_TYPE_TO_SOURCE_TYPE.get(client.client_type, "repository"),
        issns=issns, publisher=client.provider_name, homepage_url=client.url,
        register_name=False,  # minted client is linked; keep out of the name index
    )
    _link(conn, client.id, sid)
    return sid


def run(dry_run=False, limit=None, batch=200):
    with engine.connect() as conn:
        sql = "SELECT id, display_name, issns, url, client_type, provider_name FROM datacite_client ORDER BY id"
        if limit:
            sql += f" LIMIT {int(limit)}"
        clients = conn.execute(text(sql)).fetchall()
        linked = dict(conn.execute(text("SELECT datacite_id, source_id FROM source_datacite_id")).fetchall())
        # name index excludes sources already carrying a datacite link (rule 3)
        ctx = MatchContext(conn, name_link=True, exclude_from_names=set(linked.values()))
    print(f"{len(clients)} staged clients; {len(linked)} already linked; dry_run={dry_run}", flush=True)

    counts = Counter()
    conn = engine.connect()
    trans = conn.begin()
    done = 0
    try:
        for c in clients:
            if (c.provider_name or "").strip().lower().startswith(ADMIN_PROVIDER_PREFIXES):
                counts["skipped_admin_provider"] += 1
                continue
            issns = normalize_issns(list(c.issns or []))
            if c.id in linked:
                counts["already_linked"] += 1
                if not dry_run:
                    _enrich(conn, linked[c.id], c)
                kind = None
            else:
                # name fallback only for ISSN-less clients: a client WITH ISSNs
                # that matches nothing is a new source, not a name-link candidate
                kind, val = match_source(conn, ctx, issns, c.display_name, "datacite",
                                         publisher=c.provider_name, use_name=not issns,
                                         dry_run=dry_run)
            if kind is None:
                pass
            elif kind == "issn":
                counts["linked_by_issn"] += 1
                if not dry_run:
                    _link(conn, c.id, val)
                    _enrich(conn, val, c)
                linked[c.id] = val
            elif kind == "issn_multi":
                counts["conflict"] += 1
                if not dry_run:
                    park_multi_match(conn, "datacite", issns, val,
                                     f"{c.id}: {c.display_name}")
            elif kind == "name":
                counts["linked_by_name"] += 1
                if not dry_run:
                    _link(conn, c.id, val)
                    _enrich(conn, val, c)
                linked[c.id] = val
            elif kind == "name_parked":
                counts[f"name_link_parked_{val}"] += 1
            elif kind == "name_multi":
                counts["conflict"] += 1
                if not dry_run:
                    park_multi_match(conn, "datacite", issns, val,
                                     f"{c.id}: {c.display_name}")
            else:  # 'none'
                counts["added"] += 1
                if not dry_run:
                    linked[c.id] = _mint(conn, ctx, c, issns)
            done += 1
            if done % batch == 0:
                if not dry_run:
                    trans.commit()
                    trans = conn.begin()
                print(f"  {done}/{len(clients)} clients", flush=True)
        if dry_run:
            trans.rollback()
        else:
            trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()
    print("outcome summary:", dict(counts), flush=True)
    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
