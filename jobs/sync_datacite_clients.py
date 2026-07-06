"""Stage 2 of the DataCite job: reconcile `datacite_client` staging against the
sources registry with an ISSN-FIRST match cascade (the Databricks pipeline never
checked ISSN overlap for DataCite -- a duplicate-source vector this fixes):

  1. client already linked (source_datacite_id)     -> enrich NULL fields only
  2. unlinked, has ISSNs -> match source_issn:
       1 source  -> LINK it (+ enrich)
       >1        -> conflict row (merge candidate, feed='datacite')
       0         -> MINT (periodical->journal, else repository)
  3. unlinked, no ISSNs -> unique exact-normalized-name match among unlinked
     sources -> LINK; none -> MINT repository; ambiguous -> conflict row

  python -m jobs.sync_datacite_clients [--dry-run] [--limit N]
"""
import argparse
from collections import Counter, defaultdict

from sqlalchemy import text

from db import engine
from sources_lib import insert_issns, name_link_guard, normalize_issns, normalize_name, resolve_issn_l

CLIENT_TYPE_TO_SOURCE_TYPE = {"periodical": "journal", "repository": "repository"}


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


def _mint(conn, client, issns):
    stype = CLIENT_TYPE_TO_SOURCE_TYPE.get(client.client_type, "repository")
    issn_l = resolve_issn_l(conn, issns) if issns else None
    # is_oa=FALSE at mint (CreateSources parity: is_oa is derived from the DOAJ/
    # SciELO/J-STAGE/high-OA-rate flags by jobs/apply_oa_flags, not asserted by feeds)
    sid = conn.execute(text(
        "INSERT INTO sources (display_name, type, issn_l, publisher, homepage_url, is_oa) "
        "VALUES (:dn, :t, :l, :pub, :url, FALSE) RETURNING id"
    ), {"dn": client.display_name, "t": stype, "l": issn_l,
        "pub": client.provider_name, "url": client.url}).scalar()
    insert_issns(conn, sid, issns, issn_l)
    _link(conn, client.id, sid)
    return sid


def _conflict(conn, client, matched_ids, issue_type="multi_match", extra=""):
    conn.execute(text(
        "INSERT INTO source_ingest_issue (source_feed, issue_type, issns, matched_source_ids, detail) "
        "VALUES ('datacite', :t, :i, :m, :d) "
        "ON CONFLICT (source_feed, issue_type, matched_source_ids) DO NOTHING"
    ), {"t": issue_type, "i": normalize_issns(list(client.issns or [])), "m": sorted(matched_ids),
        "d": f"{client.id}: {client.display_name}{extra}"})


def run(dry_run=False, limit=None, batch=200):
    with engine.connect() as conn:
        sql = "SELECT id, display_name, issns, url, client_type, provider_name FROM datacite_client ORDER BY id"
        if limit:
            sql += f" LIMIT {int(limit)}"
        clients = conn.execute(text(sql)).fetchall()
        linked = dict(conn.execute(text("SELECT datacite_id, source_id FROM source_datacite_id")).fetchall())
        issn_to_sid = dict(conn.execute(text("SELECT issn, source_id FROM source_issn")).fetchall())
        # name index over active sources without a datacite link (rule 3)
        name_index = defaultdict(list)
        linked_sids = set(linked.values())
        for sid, name, publisher in conn.execute(text(
                "SELECT id, display_name, publisher FROM sources WHERE merge_into_id IS NULL")).fetchall():
            if sid not in linked_sids:
                name_index[normalize_name(name)].append((sid, publisher))
    print(f"{len(clients)} staged clients; {len(linked)} already linked; dry_run={dry_run}", flush=True)

    counts = Counter()
    conn = engine.connect()
    trans = conn.begin()
    done = 0
    try:
        for c in clients:
            issns = normalize_issns(list(c.issns or []))
            if c.id in linked:
                counts["already_linked"] += 1
                if not dry_run:
                    _enrich(conn, linked[c.id], c)
            elif issns:
                matched = {issn_to_sid[i] for i in issns if i in issn_to_sid}
                if len(matched) == 1:
                    sid = next(iter(matched))
                    counts["linked_by_issn"] += 1
                    if not dry_run:
                        _link(conn, c.id, sid)
                        _enrich(conn, sid, c)
                    linked[c.id] = sid
                elif len(matched) > 1:
                    counts["conflict"] += 1
                    if not dry_run:
                        _conflict(conn, c, matched)
                else:
                    counts["added"] += 1
                    if not dry_run:
                        sid = _mint(conn, c, issns)
                        for i in issns:
                            issn_to_sid[i] = sid
                        linked[c.id] = sid
            else:
                candidates = name_index.get(normalize_name(c.display_name), [])
                if len(candidates) == 1:
                    sid, src_publisher = candidates[0]
                    refused = name_link_guard(c.display_name, src_publisher, c.provider_name)
                    if refused:
                        counts[f"name_link_parked_{refused}"] += 1
                        if not dry_run:
                            _conflict(conn, c, [sid], issue_type="name_link_conflict",
                                      extra=f" | {refused} | provider={c.provider_name} | src_pub={src_publisher}")
                    else:
                        counts["linked_by_name"] += 1
                        if not dry_run:
                            _link(conn, c.id, sid)
                            _enrich(conn, sid, c)
                        linked[c.id] = sid
                elif len(candidates) > 1:
                    counts["conflict"] += 1
                    if not dry_run:
                        _conflict(conn, c, [s for s, _ in candidates])
                else:
                    counts["added"] += 1
                    if not dry_run:
                        sid = _mint(conn, c, [])
                        linked[c.id] = sid
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
