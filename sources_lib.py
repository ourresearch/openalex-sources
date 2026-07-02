"""Core source-registry primitives, generic over the feed (Crossref, ISSN
portal, DOAJ, ...). Ports the guts add_missing_journals match/mint/update
logic onto the normalized sources + source_issn schema.

Match is on ISSN (source_issn.UNIQUE(issn) is the authoritative index):
  - 0 incoming ISSNs match an existing source -> MINT a new journal source
  - exactly 1 source matches            -> ENRICH it (add missing ISSNs; fill
                                           display_name/publisher, override-guarded)
  - >1 source matches                   -> CONFLICT: log a merge candidate, skip

Merges (merge_source) are first-class: loser's ISSNs move to the winner, the
loser keeps a merge_into_id redirect, and every merge is audited in source_merge.
"""
import json

from sqlalchemy import text


def normalize_issns(issns):
    """Uppercase, strip, drop blanks, dedupe preserving order."""
    seen, out = set(), []
    for issn in issns or []:
        if not issn:
            continue
        issn = issn.strip().upper()
        if issn and issn not in seen:
            seen.add(issn)
            out.append(issn)
    return out


def resolve_issn_l(conn, issns):
    """Best-effort ISSN-L: use the issn_to_issnl map if it knows any of these
    ISSNs, else fall back to the first ISSN."""
    if not issns:
        return None
    row = conn.execute(
        text("SELECT issn_l FROM issn_to_issnl WHERE issn = ANY(:issns) AND issn_l IS NOT NULL LIMIT 1"),
        {"issns": issns},
    ).fetchone()
    return row[0] if row else issns[0]


def _insert_issns(conn, source_id, issns, issn_l):
    for issn in issns:
        conn.execute(
            text(
                "INSERT INTO source_issn (source_id, issn, is_issn_l) "
                "VALUES (:sid, :issn, :is_l) ON CONFLICT (issn) DO NOTHING"
            ),
            {"sid": source_id, "issn": issn, "is_l": issn == issn_l},
        )


def upsert_journal_by_issn(
    conn,
    issns,
    display_name=None,
    publisher=None,
    crossref_id=None,
    source_feed="crossref",
    dry_run=False,
):
    """Returns (outcome, source_id). outcome in
    {added, updated, unchanged, conflict, skipped_no_issn}."""
    issns = normalize_issns(issns)
    if not issns:
        return "skipped_no_issn", None

    matched = sorted(
        r[0]
        for r in conn.execute(
            text("SELECT DISTINCT source_id FROM source_issn WHERE issn = ANY(:issns)"),
            {"issns": issns},
        )
    )

    if len(matched) > 1:
        if not dry_run:
            conn.execute(
                text(
                    "INSERT INTO source_ingest_issue "
                    "(source_feed, issue_type, issns, matched_source_ids, detail) "
                    "VALUES (:f, 'multi_match', :i, :m, :d)"
                ),
                {"f": source_feed, "i": issns, "m": matched, "d": display_name},
            )
        return "conflict", None

    # ---- mint a new journal source -------------------------------------
    if not matched:
        if dry_run:
            return "added", None
        issn_l = resolve_issn_l(conn, issns)
        sid = conn.execute(
            text(
                "INSERT INTO sources (display_name, type, issn_l, publisher, crossref_id) "
                "VALUES (:dn, 'journal', :l, :pub, :cid) RETURNING id"
            ),
            {"dn": display_name, "l": issn_l, "pub": publisher, "cid": crossref_id},
        ).scalar()
        _insert_issns(conn, sid, issns, issn_l)
        return "added", sid

    # ---- enrich the single matching source -----------------------------
    sid = matched[0]
    existing = {
        r[0]
        for r in conn.execute(
            text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": sid}
        )
    }
    missing = [i for i in issns if i not in existing]

    row = conn.execute(
        text(
            "SELECT display_name, publisher, publisher_id, crossref_id, override_timestamp "
            "FROM sources WHERE id = :id"
        ),
        {"id": sid},
    ).fetchone()

    updates = {}
    # display_name: refresh from feed only when not human-overridden (guts parity)
    if display_name and row.override_timestamp is None and display_name != row.display_name:
        updates["display_name"] = display_name
    # publisher: only fill when we don't already have a resolved publisher
    if publisher and row.publisher_id is None and not row.publisher:
        updates["publisher"] = publisher
    if crossref_id and not row.crossref_id:
        updates["crossref_id"] = crossref_id

    if not missing and not updates:
        return "unchanged", sid

    if dry_run:
        return "updated", sid

    if missing:
        issn_l = resolve_issn_l(conn, list(existing | set(issns)))
        _insert_issns(conn, sid, missing, issn_l)
    if updates:
        set_clause = ", ".join(f"{k} = :{k}" for k in updates)
        conn.execute(
            text(f"UPDATE sources SET {set_clause}, updated_date = now() WHERE id = :id"),
            {**updates, "id": sid},
        )
    else:
        conn.execute(
            text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": sid}
        )
    return "updated", sid


def merge_source(conn, loser_id, winner_id, rule, source_feed=None, detail=None):
    """Merge loser into winner. Returns an outcome string; 'merged' on success,
    otherwise the guard that refused ('already_merged', 'loser_overridden', ...).

    Effects: loser's ISSNs re-point to the winner, loser gets merge_into_id +
    merge_into_date, winner's issn_l is re-resolved over its enlarged ISSN set,
    and the merge is recorded in source_merge.
    """
    if loser_id == winner_id:
        return "same_source"
    rows = {
        r.id: r
        for r in conn.execute(
            text(
                "SELECT id, merge_into_id, override_timestamp "
                "FROM sources WHERE id IN (:l, :w)"
            ),
            {"l": loser_id, "w": winner_id},
        )
    }
    if loser_id not in rows or winner_id not in rows:
        return "missing_source"
    if rows[loser_id].merge_into_id is not None or rows[winner_id].merge_into_id is not None:
        return "already_merged"
    if rows[loser_id].override_timestamp is not None:
        return "loser_overridden"  # a curator touched the loser; needs a human

    conn.execute(
        text("UPDATE source_issn SET source_id = :w, is_issn_l = FALSE WHERE source_id = :l"),
        {"l": loser_id, "w": winner_id},
    )
    conn.execute(
        text(
            "UPDATE sources SET merge_into_id = :w, merge_into_date = now(), "
            "updated_date = now() WHERE id = :l"
        ),
        {"l": loser_id, "w": winner_id},
    )

    # re-resolve the winner's ISSN-L over its enlarged ISSN set
    issns = [
        r[0]
        for r in conn.execute(
            text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": winner_id}
        )
    ]
    issn_l = resolve_issn_l(conn, issns)
    conn.execute(
        text("UPDATE sources SET issn_l = :l, updated_date = now() WHERE id = :id"),
        {"l": issn_l, "id": winner_id},
    )
    conn.execute(
        text("UPDATE source_issn SET is_issn_l = (issn = :l) WHERE source_id = :id"),
        {"l": issn_l, "id": winner_id},
    )

    conn.execute(
        text(
            "INSERT INTO source_merge (loser_id, winner_id, rule, source_feed, detail) "
            "VALUES (:l, :w, :r, :f, CAST(:d AS JSONB))"
        ),
        {
            "l": loser_id,
            "w": winner_id,
            "r": rule,
            "f": source_feed,
            "d": json.dumps(detail) if detail is not None else None,
        },
    )
    return "merged"
