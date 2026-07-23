"""Core source-registry primitives, generic over the feed (Crossref, ISSN
portal, DOAJ, DataCite, ...).

Every feed sync is the same cascade, implemented ONCE here:

  1. build a MatchContext (in-memory ISSN / name / parked indexes, one scan each)
  2. per staged row, match_source() finds the existing source:
       ISSN matches 1 source   -> ('issn', sid)
       ISSN matches >1         -> ('issn_multi', ids)   caller parks a merge candidate
       unique name match       -> ('name', sid)          guarded: name_link_guard +
                                                          previously-parked stay parked
       ambiguous / refused     -> ('name_multi', ids) / ('name_parked', reason)
       nothing                 -> ('none', None)
  3. the feed job acts on the outcome: enrich_journal() / mint_source() /
     park_multi_match() / its own feed-native link step (e.g. source_datacite_id)

is_oa is DERIVED, with a single writer: feeds set only their own signal column
(is_in_doaj, is_in_scielo, ...) and call recompute_is_oa() at the end of the run.

Merges (merge_source) are first-class: loser's ISSNs move to the winner, the
loser keeps a merge_into_id redirect, and every merge is audited in source_merge.
"""
import json
import unicodedata
from collections import defaultdict

from sqlalchemy import text


def normalize_name(name):
    """Diacritic/case/punctuation-insensitive form for exact-name comparison."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.casefold().replace("&", " and ")
    s = "".join(c if c.isalnum() else " " for c in s)
    s = " ".join(s.split())
    if s.startswith("the "):
        s = s[4:]
    return s


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


def name_link_guard(name, source_publisher, feed_publisher):
    """Guard for name-only auto-links (no shared ISSN / feed-native id evidence).
    Returns None when the link may proceed, else the refusal reason.

    An exact-normalized-name match is weak evidence on its own: generic titles
    ("Currents", "Kritika", "Matrix") exist independently at multiple publishers,
    and the 2026-07-06 pre-cutover audit found real cross-publisher mis-links from
    this path. A name-only link must clear both checks:
      - the name has >=3 tokens (generic short titles are never auto-linked), and
      - the two publishers, when both known, don't contradict (substring-tolerant).
    Refused links belong in source_ingest_issue as 'name_link_conflict'.
    """
    if len(normalize_name(name).split()) < 3:
        return "short_name"
    a, b = normalize_name(source_publisher), normalize_name(feed_publisher)
    if a and b and a != b and a not in b and b not in a:
        return "publisher_conflict"
    return None


def resolve_issn_l(conn, issns, member_only=False):
    """Best-effort ISSN-L: use the issn_to_issnl map if it knows any of these
    ISSNs, else fall back to the first ISSN. member_only restricts the answer
    to the given set -- required wherever the result becomes sources.issn_l of
    a source that owns exactly these ISSNs (issn_l membership is FK-enforced,
    mig. 014)."""
    if not issns:
        return None
    sql = "SELECT issn_l FROM issn_to_issnl WHERE issn = ANY(:issns) AND issn_l IS NOT NULL"
    if member_only:
        sql += " AND issn_l = ANY(:issns)"
    sql += " ORDER BY issn LIMIT 1"
    row = conn.execute(text(sql), {"issns": list(issns)}).fetchone()
    return row[0] if row else issns[0]


def refresh_issns_column(conn, source_id):
    """Re-derive sources.issns (ISSN-L first, then alphabetical) from source_issn.
    source_issn is the authority; the array column exists for readers (Databricks
    federates it as a proper array). Called by every path that writes source_issn."""
    conn.execute(
        text(
            "UPDATE sources SET issns = (SELECT array_agg(issn ORDER BY is_issn_l DESC, issn) "
            "FROM source_issn WHERE source_id = :id) WHERE id = :id"
        ),
        {"id": source_id},
    )


def insert_issns(conn, source_id, issns, issn_l):
    """Attach ISSNs and maintain the ISSN-L invariant: sources.issn_l is one of
    the source's OWN ISSNs (FK-enforced, mig. 014) and is_issn_l flags exactly
    the matching row. Never overwrites an existing issn_l (API-visible, may be
    curated) -- flags are re-synced to whatever issn_l ends up being, so a
    freshly resolved candidate that loses to the stored value can't leave a
    second is_issn_l row behind."""
    for issn in issns:
        conn.execute(
            text(
                "INSERT INTO source_issn (source_id, issn, is_issn_l) "
                "VALUES (:sid, :issn, FALSE) ON CONFLICT (issn) DO NOTHING"
            ),
            {"sid": source_id, "issn": issn},
        )
    if issn_l:
        # fill only when unset, and only with an ISSN the source actually owns
        conn.execute(
            text(
                "UPDATE sources SET issn_l = :l WHERE id = :id AND issn_l IS NULL "
                "AND EXISTS (SELECT 1 FROM source_issn WHERE source_id = :id AND issn = :l)"
            ),
            {"l": issn_l, "id": source_id},
        )
    conn.execute(
        text(
            "UPDATE source_issn si SET is_issn_l = (si.issn IS NOT DISTINCT FROM s.issn_l) "
            "FROM sources s WHERE s.id = :id AND si.source_id = :id "
            "AND si.is_issn_l IS DISTINCT FROM (si.issn IS NOT DISTINCT FROM s.issn_l)"
        ),
        {"id": source_id},
    )
    refresh_issns_column(conn, source_id)


class MatchContext:
    """In-memory match indexes for one reconcile run (one scan per table).

    name_link=True additionally builds the name index over active sources and
    the parked set for the guarded name-link fallback; exclude_from_names drops
    sources that must not be name-link candidates (e.g. the DataCite sync
    excludes sources that already carry a datacite link).
    """

    def __init__(self, conn, name_link=False, exclude_from_names=()):
        self.issn_to_sid = {}
        self.sid_to_issns = defaultdict(set)
        for sid, issn in conn.execute(text("SELECT source_id, issn FROM source_issn")):
            self.issn_to_sid[issn] = sid
            self.sid_to_issns[sid].add(issn)
        # issn_l is FK-guaranteed to be an owned ISSN (mig. 014), so this index
        # is normally a subset of issn_to_sid; it still catches any source whose
        # issn_l membership is pending repair (e.g. a parked collision pair)
        self.issnl_to_sid = {
            issn_l: sid
            for sid, issn_l in conn.execute(text(
                "SELECT id, issn_l FROM sources WHERE merge_into_id IS NULL AND issn_l IS NOT NULL"))
        }
        self.name_link = name_link
        self.name_index = defaultdict(list)  # normalized name -> [(sid, publisher)]
        self.parked = set()
        if name_link:
            skip = set(exclude_from_names)
            for sid, name, publisher in conn.execute(text(
                    "SELECT id, display_name, publisher FROM sources WHERE merge_into_id IS NULL")):
                if sid not in skip:
                    self.name_index[normalize_name(name)].append((sid, publisher))
            # sources with an unresolved name-link refusal stay parked even if
            # the guard would now pass (covers audited unlinks awaiting review)
            self.parked = {r[0] for r in conn.execute(text(
                "SELECT DISTINCT unnest(matched_source_ids) FROM source_ingest_issue "
                "WHERE issue_type = 'name_link_conflict' AND resolved_at IS NULL"))}

    def classify(self, issns):
        """No-write fast path: added / updated / unchanged / conflict."""
        matched = {self.issn_to_sid[i] for i in issns if i in self.issn_to_sid}
        if not matched:
            return "added"
        if len(matched) > 1:
            return "conflict"
        sid = next(iter(matched))
        return "updated" if set(issns) - self.sid_to_issns[sid] else "unchanged"

    def register(self, sid, issns=(), name=None, publisher=None):
        """Keep the indexes current after an in-run mint / link / enrich."""
        for i in issns:
            self.issn_to_sid[i] = sid
            self.sid_to_issns[sid].add(i)
        if self.name_link and name:
            self.name_index[normalize_name(name)].append((sid, publisher))


def match_source(conn, ctx, issns, display_name, source_feed,
                 publisher=None, use_name=None, dry_run=False):
    """The shared match step. Returns (kind, value):

      ('issn', sid)            exactly one source owns an incoming ISSN
      ('issn_multi', ids)      merge candidate -- caller decides (park_multi_match)
      ('name', sid)            unique guarded name match (only when use_name)
      ('name_parked', reason)  guard refused; queue row written here
      ('name_multi', ids)      ambiguous name -- caller decides
      ('none', None)           no match; caller mints

    use_name defaults to ctx.name_link; pass False to skip the name fallback for
    a specific call (e.g. DataCite clients WITH ISSNs mint on no-match instead).
    """
    matched = {ctx.issn_to_sid[i] for i in issns if i in ctx.issn_to_sid}
    if not matched and issns:
        # ISSN-L expansion: no incoming ISSN is owned directly, but the registry
        # map may link one to a source we already have (print vs online ISSN
        # split). Without this, such rows minted duplicates whose issn_l
        # collided with the existing source (2026-07-07 audit: 8 pairs).
        mapped = {r[0] for r in conn.execute(text(
            "SELECT DISTINCT issn_l FROM issn_to_issnl "
            "WHERE issn = ANY(:i) AND issn_l IS NOT NULL"), {"i": list(issns)})}
        for i in mapped | set(issns):
            if i in ctx.issn_to_sid:
                matched.add(ctx.issn_to_sid[i])
            if i in ctx.issnl_to_sid:
                matched.add(ctx.issnl_to_sid[i])
    matched = sorted(matched)
    if len(matched) == 1:
        return "issn", matched[0]
    if matched:
        return "issn_multi", matched

    use_name = ctx.name_link if use_name is None else (use_name and ctx.name_link)
    if not use_name or not display_name:
        return "none", None
    candidates = ctx.name_index.get(normalize_name(display_name), [])
    if len(candidates) == 1:
        sid, src_publisher = candidates[0]
        refused = name_link_guard(display_name, src_publisher, publisher)
        if not refused and sid in ctx.parked:
            refused = "previously_parked"
        if refused:
            if not dry_run:
                conn.execute(text(
                    "INSERT INTO source_ingest_issue "
                    "(source_feed, issue_type, issns, matched_source_ids, detail) "
                    "VALUES (:f, 'name_link_conflict', :i, :m, :d) "
                    "ON CONFLICT (source_feed, issue_type, matched_source_ids) DO NOTHING"
                ), {"f": source_feed, "i": list(issns), "m": [sid],
                    "d": f"{refused}: {display_name} | feed_pub={publisher} | src_pub={src_publisher}"})
            return "name_parked", refused
        return "name", sid
    if len(candidates) > 1:
        return "name_multi", [s for s, _ in candidates]
    return "none", None


def park_multi_match(conn, source_feed, issns, matched_ids, detail):
    """Queue a merge candidate. One row per (feed, id-set) ever -- persisting
    conflicts re-surface every sync run and must not re-accumulate (mig. 005)."""
    conn.execute(text(
        "INSERT INTO source_ingest_issue "
        "(source_feed, issue_type, issns, matched_source_ids, detail) "
        "VALUES (:f, 'multi_match', :i, :m, :d) "
        "ON CONFLICT (source_feed, issue_type, matched_source_ids) DO NOTHING"
    ), {"f": source_feed, "i": list(issns), "m": sorted(matched_ids), "d": detail})


def mint_source(conn, ctx, display_name, source_type="journal", issns=(),
                publisher=None, crossref_id=None, homepage_url=None,
                register_name=True):
    """Mint a new source (id assigned by the identity column) and register it in
    the context. is_oa starts FALSE: it is derived, see recompute_is_oa.

    register_name=False keeps the mint out of the context's name index — the
    DataCite sync needs this because its index excludes already-linked sources,
    and a just-minted client IS linked (two distinct same-named clients must not
    collapse onto one source).
    """
    # preprint servers are typed repository regardless of feed (parity with the
    # retired CreateSources DLT rule)
    lowered = (display_name or "").lower()
    if source_type == "journal" and ("rxiv" in lowered or "research square" in lowered):
        source_type = "repository"
    issns = list(issns)
    issn_l = resolve_issn_l(conn, issns) if issns else None
    if issn_l and issn_l not in issns:
        # the registry map's linking ISSN isn't in the feed's set: adopt it as
        # owned (issn_l membership is FK-enforced) -- unless another source owns
        # it, which match_source's ISSN-L expansion should have caught; fall
        # back to our own set rather than point at someone else's ISSN
        owned = conn.execute(
            text("SELECT 1 FROM source_issn WHERE issn = :l"), {"l": issn_l}
        ).fetchone()
        if owned:
            issn_l = resolve_issn_l(conn, issns, member_only=True)
        else:
            issns.append(issn_l)
    sid = conn.execute(text(
        "INSERT INTO sources (display_name, type, issn_l, publisher, crossref_id, "
        "homepage_url, is_oa) VALUES (:dn, :t, :l, :pub, :cid, :url, FALSE) RETURNING id"
    ), {"dn": display_name, "t": source_type, "l": issn_l, "pub": publisher,
        "cid": crossref_id, "url": homepage_url}).scalar()
    if issns:
        insert_issns(conn, sid, issns, issn_l)
    ctx.register(sid, issns, name=display_name if register_name else None,
                 publisher=publisher)
    return sid


def enrich_journal(conn, ctx, sid, issns=(), display_name=None, publisher=None,
                   crossref_id=None):
    """Feed-refresh of an ISSN-matched journal: attach missing ISSNs, refresh
    display_name unless curator-overridden (guts parity), fill publisher when we
    have no resolved publisher, fill crossref_id. Returns 'updated'/'unchanged'."""
    existing = {r[0] for r in conn.execute(
        text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": sid})}
    missing = [i for i in issns if i not in existing]

    row = conn.execute(text(
        "SELECT display_name, publisher, publisher_id, crossref_id, override_timestamp "
        "FROM sources WHERE id = :id"), {"id": sid}).fetchone()

    updates = {}
    if display_name and row.override_timestamp is None and display_name != row.display_name:
        updates["display_name"] = display_name
    if publisher and row.publisher_id is None and not row.publisher:
        updates["publisher"] = publisher
    if crossref_id and not row.crossref_id:
        updates["crossref_id"] = crossref_id

    if not missing and not updates:
        return "unchanged"

    if missing:
        # member_only: the candidate must come from the source's own (enlarged)
        # set -- a map-preferred outsider ISSN must not become issn_l here (it
        # used to leave a second is_issn_l flag behind: 591 sources, mig. 014)
        issn_l = resolve_issn_l(conn, list(existing | set(issns)), member_only=True)
        insert_issns(conn, sid, missing, issn_l)
        ctx.register(sid, missing)
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
    return "updated"


def recompute_is_oa(conn):
    """THE single writer of sources.is_oa (= any of the four OA signals).
    Feeds set only their own signal column and call this at the end of the run.
    Returns the number of rows whose is_oa changed."""
    return conn.execute(text("""
        UPDATE sources SET
            is_oa = (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
                     OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE)),
            updated_date = now()
        WHERE merge_into_id IS NULL
          AND is_oa IS DISTINCT FROM
              (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
               OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE))
    """)).rowcount


def merge_source(conn, loser_id, winner_id, rule, source_feed=None, detail=None):
    """Merge loser into winner. Returns an outcome string; 'merged' on success,
    otherwise the guard that refused ('already_merged', 'loser_overridden', ...).

    Effects: loser's ISSNs and endpoint links re-point to the winner, loser gets
    merge_into_id + merge_into_date, winner's issn_l is re-resolved over its
    enlarged ISSN set, and the merge is recorded in source_merge.
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
        text("UPDATE source_endpoint SET source_id = :w WHERE source_id = :l"),
        {"l": loser_id, "w": winner_id},
    )

    # preserve the loser's identity strings on the winner (deduped
    # case-insensitively; skip anything that normalizes to the winner's own
    # display_name). Cross-language merges otherwise shed the venue's
    # other-language titles from active search.
    names = {
        r.id: (r.display_name, list(r.alternate_titles or []))
        for r in conn.execute(
            text("SELECT id, display_name, alternate_titles FROM sources "
                 "WHERE id IN (:l, :w)"),
            {"l": loser_id, "w": winner_id},
        )
    }
    l_name, l_alt = names[loser_id]
    w_name, w_alt = names[winner_id]
    have = {t.strip().lower() for t in w_alt + [w_name or ""]}
    w_norm = normalize_name(w_name or "")
    gained = []
    for t in [l_name or ""] + l_alt:
        t = (t or "").strip()
        if t and t.lower() not in have and normalize_name(t) != w_norm:
            gained.append(t)
            have.add(t.lower())
    if gained:
        conn.execute(
            text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) "
                 "WHERE id = :id"),
            {"a": json.dumps(w_alt + gained), "id": winner_id},
        )

    conn.execute(
        text(
            # issn_l = NULL: the loser's ISSNs just moved to the winner, and a
            # redirect row must not keep referencing an ISSN it no longer owns
            "UPDATE sources SET merge_into_id = :w, merge_into_date = now(), "
            "issn_l = NULL, updated_date = now() WHERE id = :l"
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
    issn_l = resolve_issn_l(conn, issns, member_only=True) if issns else None
    conn.execute(
        text("UPDATE sources SET issn_l = :l, updated_date = now() WHERE id = :id"),
        {"l": issn_l, "id": winner_id},
    )
    conn.execute(
        text("UPDATE source_issn SET is_issn_l = (issn = :l) WHERE source_id = :id"),
        {"l": issn_l, "id": winner_id},
    )
    refresh_issns_column(conn, loser_id)   # -> NULL (all ISSNs moved away)
    refresh_issns_column(conn, winner_id)

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
