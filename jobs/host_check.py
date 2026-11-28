"""Host check for journals whose Crossref journal-level publisher changed (oxjob #2693).

A source's publisher_id is written once and the feeds only fill nulls, so a journal that moves to another publisher
keeps crediting the old one (Annals of the Rheumatic Diseases BMJ -> Elsevier, Knee Surgery Sports Traumatology
Arthroscopy Springer -> Wiley; 277 journals / ~906K works on 28 Nov 2026). Crossref's journal record names the new
publisher once the title transfers, so a change there is the signal:

  watch()  after each Crossref journals sync: store the journal-record publisher string(s) per source
           (source_crossref_publisher) and queue every source whose string changed since the last run
           (host_check_queue). The first run only seeds.
  run()    work the queue. A journal moves only when ALL hold (the rule oxjob #2693 used and checked by hand, 39/40):
           its current host and the new publisher are both publisher companies in the reviewed map below, of
           different companies; Crossref's journal record names the new one and its title matches ours; the newest
           journal article's DOI and the majority of ALL the journal's DOIs are registered by the new company (the
           backfile moved with the title); and the newest DOI lands on that company's own platform. Anything else is
           held with its reason. A society or university host is never moved here (whether a society journal
           credits the society or its publishing partner is a separate question). At most MAX_MOVES per run: a run
           that would move more holds them all and reports it, since a mass change means a feed problem, not
           transfers.

    python -m jobs.host_check [--dry-run] [--limit N]

Revert one move: UPDATE sources SET publisher_id = q.publisher_id_then ... from its host_check_queue row
(publisher_id_after = what was written; note holds the old publisher text).
"""
import argparse
import json
import os
import re
import time
from difflib import SequenceMatcher
from urllib.parse import urlparse

import requests
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name

MAX_MOVES = 25
UA = {"User-Agent": f"openalex-sources/host_check (mailto:{os.getenv('CROSSREF_MAILTO', 'dev@ourresearch.org')})"}
if os.getenv("CROSSREF_API_KEY"):
    UA["crossref-api-key"] = os.getenv("CROSSREF_API_KEY")

# Company families: a registrant string (Crossref's publisher on each DOI) belongs to a family by these patterns.
FAMILY_PATTERNS = [
    ("elsevier", r"elsevier|cell press|academic press|pergamon"),
    ("springer", r"springer|palgrave|nature portfolio|nature publishing|biomed central|\bbmc\b|\badis\b"),
    ("wiley", r"wiley|blackwell|hindawi"),
    ("tf", r"taylor|informa|routledge|crc press"),
    ("sage", r"\bsage\b"),
    ("oup", r"oxford university press|\boup\b"),
    ("cup", r"cambridge university press"),
    ("bmj", r"\bbmj\b"),
    ("wk", r"wolters|lippincott|ovid technologies|medknow"),
    ("degruyter", r"de gruyter|\bbrill\b"),
    ("mdpi", r"\bmdpi\b|multidisciplinary digital"),
    ("frontiers", r"frontiers media"),
    ("emerald", r"emerald"),
    ("tup", r"tsinghua university press"),
    ("map", r"maximum academic press"),
    ("ieee", r"\bieee\b|institute of electrical and electronics"),
]
# Each family's own platforms (where its journals' DOIs land), incl. sites it runs under another name.
PLATFORM = {
    "elsevier": r"(^|\.)(sciencedirect\.com|elsevier\.com|elsevierhealth\.com|cell\.com|thelancet\.com)$",
    "springer": r"(^|\.)(springer\.com|springeropen\.com|nature\.com|biomedcentral\.com)$",
    "wiley": r"(^|\.)(wiley\.com|hindawi\.com)$",
    "tf": r"(^|\.)(tandfonline\.com|taylorfrancis\.com)$",
    "sage": r"(^|\.)sagepub\.com$",
    "oup": r"(^|\.)oup\.com$",
    "cup": r"(^|\.)cambridge\.org$",
    "bmj": r"(^|\.)bmj\.com$",
    "wk": r"(^|\.)(lww\.com|ovid\.com|ahajournals\.org|medknow\.com)$",
    "degruyter": r"(^|\.)(degruyter\.com|degruyterbrill\.com|brill\.com|sciendo\.com)$",
    "mdpi": r"(^|\.)mdpi\.com$",
    "frontiers": r"(^|\.)(frontiersin\.org|frontierspartnerships\.org|ebm-journal\.org|ssph-journal\.org)$",
    "emerald": r"(^|\.)emerald\.com$",
    "tup": r"(^|\.)sciopen\.com$|^ieeexplore\.ieee\.org$",  # TUP's journals are also all on IEEE Xplore
    "map": r"(^|\.)maxapress\.com$",
    "ieee": r"(^|\.)ieee\.org$",
}
# Crossref journal-record publisher string -> (publisher_id, family, name). Hand-reviewed for oxjob #2693; a string not
# listed here is never moved automatically.
JOURNAL_PUBLISHER = {
    "Elsevier": (4310320990, "elsevier", "Elsevier BV"),
    "Wiley (John Wiley & Sons)": (4310320595, "wiley", "Wiley"),
    "Wiley (Blackwell Publishing)": (4310320595, "wiley", "Wiley"),
    "Hindawi Limited": (4310319869, "wiley", "Hindawi Publishing Corporation"),
    "Oxford University Press": (4310311648, "oup", "Oxford University Press"),
    "SAGE Publications": (4310320017, "sage", "SAGE Publishing"),
    "Springer-Verlag": (4310319900, "springer", "Springer Science+Business Media"),
    "Springer (Biomed Central Ltd.)": (4310320256, "springer", "BioMed Central"),
    "Informa UK (Taylor & Francis)": (4310320547, "tf", "Taylor & Francis"),
    "Walter de Gruyter GmbH (Brill)": (4310320561, "degruyter", "Brill"),
    "Walter de Gruyter GmbH (Brill Deutschland GmbH)": (4310320561, "degruyter", "Brill"),
    "Walter de Gruyter GmbH": (4310313990, "degruyter", "De Gruyter"),
    "Ovid Technologies (Wolters Kluwer) - Medknow Publications": (4310320448, "wk", "Medknow"),
    "Medknow Publications": (4310320448, "wk", "Medknow"),
    "Ovid Technologies (Wolters Kluwer) - Lippincott Williams & Wilkins": (4310315671, "wk", "Lippincott Williams & Wilkins"),
    "Ovid Technologies (Wolters Kluwer) - American Heart Association": (4310315671, "wk", "Lippincott Williams & Wilkins"),
    "Tsinghua University Press": (4310311901, "tup", "Tsinghua University Press"),
    "Frontiers Media SA": (4310320527, "frontiers", "Frontiers Media"),
    "Cambridge University Press": (4310311721, "cup", "Cambridge University Press"),
    "Maximum Academic Press": (4404685419, "map", "Maximum Academic Press"),
    "Emerald (MCB UP)": (4310319811, "emerald", "Emerald Publishing Limited"),
    "MDPI AG": (4310310987, "mdpi", "Multidisciplinary Digital Publishing Institute"),
    "BMJ": (4310319945, "bmj", "BMJ"),
}
# Publisher records that are a company of one of the families (current hosts a moved journal can have). Societies,
# universities and platforms are absent on purpose.
COMPANY_FAMILY = {pid: fam for pid, fam, _ in JOURNAL_PUBLISHER.values()}
COMPANY_FAMILY.update({
    4310319965: "springer", 4310320108: "springer", 4310319908: "springer", 4310319703: "springer",
    4310319702: "springer", 4310320449: "tf", 4310320584: "tf", 4310318547: "wk", 4310315860: "wk",
    4310320322: "degruyter", 4310319808: "ieee",
})


def family(s):
    s = (s or "").lower()
    for fam, rx in FAMILY_PATTERNS:
        if re.search(rx, s):
            return fam
    return None


def titles_match(a, b):
    a, b = normalize_name(a), normalize_name(b)
    return bool(a and b) and (a == b or a in b or b in a or SequenceMatcher(None, a, b).ratio() >= 0.85)


def decide(src, ev):
    """src: the source row (publisher_id, institution_id, override_timestamp, display_name). ev: Crossref and DOI
    evidence (journal_publisher, journal_title, newest_doi, newest_publisher, facet {registrant: n}, landing_host).
    Returns (status, target or None, reason)."""
    if src.get("override_timestamp"):
        return "held", None, "curated row"
    if src.get("institution_id"):
        return "held", None, "hosted by an institution"
    cur = src.get("publisher_id")
    if not cur:
        return "held", None, "no host (filled by other jobs)"
    target = JOURNAL_PUBLISHER.get(ev.get("journal_publisher") or "")
    if not target:
        return "held", None, f"Crossref journal publisher not in the reviewed map: {ev.get('journal_publisher')!r}"
    tid, tfam, _ = target
    if cur == tid:
        return "same_host", None, "already on the publisher Crossref names"
    cfam = COMPANY_FAMILY.get(cur)
    if cfam is None:
        return "held", None, f"current host P{cur} is not a mapped publisher company (society, university, platform?)"
    if cfam == tfam:
        return "held", None, f"same company ({tfam}): an imprint change, not a transfer"
    if not titles_match(ev.get("journal_title"), src.get("display_name")):
        return "held", None, f"Crossref journal title differs: {ev.get('journal_title')!r}"
    if family(ev.get("newest_publisher")) != tfam:
        return "held", None, f"newest article registered by {ev.get('newest_publisher')!r}"
    facet = ev.get("facet") or {}
    total = sum(facet.values())
    share = sum(n for k, n in facet.items() if family(k) == tfam) / total if total else 0
    if share < 0.5:
        return "held", None, f"new publisher registers {share:.0%} of the journal's DOIs: the backfile stayed"
    host = (ev.get("landing_host") or "").lower()
    if not re.search(PLATFORM[tfam], host):
        return "held", None, f"newest DOI lands on {host or 'nothing'}, not the new publisher's platform"
    return "move", target, f"registrant share {share:.0%}; newest DOI {ev.get('newest_doi')} lands on {host}"


def watch(conn):
    """Record Crossref's journal-record publisher per source; queue sources whose string changed. Returns n queued."""
    conn.execute(text("""
        CREATE TEMP TABLE cj_pub ON COMMIT DROP AS
        SELECT si.source_id, string_agg(DISTINCT btrim(d.publisher), ' | ' ORDER BY btrim(d.publisher)) AS publisher
        FROM crossref_journal d CROSS JOIN LATERAL unnest(d.issns) AS di(issn)
        JOIN source_issn si ON si.issn = upper(btrim(di.issn))
        WHERE coalesce(btrim(d.publisher), '') <> ''
        GROUP BY si.source_id
    """))
    norm = "lower(regexp_replace(btrim({}), '\\s+', ' ', 'g'))"
    queued = conn.execute(text(f"""
        INSERT INTO host_check_queue (source_id, crossref_before, crossref_after, publisher_id_then)
        SELECT c.source_id, p.publisher, c.publisher, s.publisher_id
        FROM cj_pub c JOIN source_crossref_publisher p USING (source_id) JOIN sources s ON s.id = c.source_id
        WHERE {norm.format('p.publisher')} <> {norm.format('c.publisher')}
        ON CONFLICT (source_id) WHERE status = 'queued'
        DO UPDATE SET crossref_after = EXCLUDED.crossref_after
    """)).rowcount
    conn.execute(text(f"""
        INSERT INTO source_crossref_publisher (source_id, publisher, seen_at)
        SELECT source_id, publisher, now() FROM cj_pub
        ON CONFLICT (source_id) DO UPDATE SET publisher = EXCLUDED.publisher, seen_at = now(),
          changed_at = CASE WHEN {norm.format('source_crossref_publisher.publisher')} <> {norm.format('EXCLUDED.publisher')}
                            THEN now() ELSE source_crossref_publisher.changed_at END
    """))
    print(f"host_check.watch: {queued} sources queued (Crossref journal publisher changed)", flush=True)
    return queued


def _get(url, **kw):
    for k in range(4):
        try:
            r = requests.get(url, headers=UA, timeout=60, **kw)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(5 * (k + 1))
                continue
            return r
        except requests.RequestException:
            time.sleep(5 * (k + 1))
    return None


def evidence(issns):
    """Crossref's journal record, the newest journal article and the registrants of all the journal's DOIs, plus where
    the newest DOI lands. Tries each ISSN until Crossref knows one."""
    for issn in issns:
        r = _get(f"https://api.crossref.org/journals/{issn}")
        if r is None or r.status_code != 200:
            continue
        m = r.json()["message"]
        ev = {"issn": issn, "journal_publisher": m.get("publisher"), "journal_title": m.get("title")}
        w = _get(f"https://api.crossref.org/journals/{issn}/works",
                 params={"rows": 1, "filter": "type:journal-article", "sort": "published", "order": "desc",
                         "facet": "publisher-name:20", "select": "DOI,publisher"})
        if w is not None and w.status_code == 200:
            wm = w.json()["message"]
            items = wm.get("items") or []
            ev["facet"] = (wm.get("facets") or {}).get("publisher-name", {}).get("values") or {}
            if items:
                ev["newest_doi"], ev["newest_publisher"] = items[0].get("DOI"), items[0].get("publisher")
        if ev.get("newest_doi"):
            d = _get("https://doi.org/" + ev["newest_doi"], allow_redirects=False)
            if d is not None and d.status_code in (301, 302, 303, 307, 308):
                ev["landing_host"] = urlparse(d.headers.get("Location", "")).netloc.lower()
        return ev
    return {}


def run(dry_run=False, limit=300, max_moves=MAX_MOVES):
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT q.id AS qid, s.id, s.display_name, s.publisher_id, s.publisher, s.institution_id, s.override_timestamp,
                   coalesce(array_remove(array_prepend(s.issn_l, array_agg(si.issn ORDER BY si.issn)), NULL), '{}') AS issns
            FROM host_check_queue q JOIN sources s ON s.id = q.source_id
            LEFT JOIN source_issn si ON si.source_id = s.id
            WHERE q.status = 'queued'
            GROUP BY q.id, s.id ORDER BY q.queued_at, q.id LIMIT :n
        """), {"n": limit}).mappings().all()
    print(f"host_check.run: {len(rows)} queued journals to check; dry_run={dry_run}", flush=True)
    decided = []
    for r in rows:
        ev = evidence(list(dict.fromkeys(r["issns"])))
        status, target, why = decide(dict(r), ev) if ev else ("held", None, "Crossref knows none of its ISSNs")
        decided.append((r, status, target, why, ev))
        time.sleep(0.2)
    moves = [d for d in decided if d[1] == "move"]
    if len(moves) > max_moves:
        print(f"host_check.run: {len(moves)} moves exceed the {max_moves} cap; holding them all", flush=True)
        try:
            import sentry_sdk
            sentry_sdk.capture_message(f"host_check: {len(moves)} host moves in one run (cap {max_moves}); none written")
        except Exception:
            pass
        decided = [(r, "held" if s == "move" else s, None, f"run over the move cap: {why}" if s == "move" else why, ev)
                   for r, s, t, why, ev in decided]
    counts = {}
    try:
        with engine.begin() as conn:
            _write(conn, decided, counts)
            if dry_run:
                raise _DryRun()
    except _DryRun:
        pass
    print(f"host_check.run: {counts}{' (dry run, rolled back)' if dry_run else ''}", flush=True)
    return counts


class _DryRun(Exception):
    pass


def _write(conn, decided, counts):
    for r, status, target, why, ev in decided:
        after = None
        if status == "move":
            tid, _, name = target
            n = conn.execute(text("""
                UPDATE sources SET publisher_id = :new, publisher = :name, updated_date = now()
                WHERE id = :id AND publisher_id = :old AND institution_id IS NULL AND override_timestamp IS NULL
            """), {"new": tid, "name": name, "id": r["id"], "old": r["publisher_id"]}).rowcount
            if n:
                status, after = "moved", tid
                why = f"P{r['publisher_id']} ({r['publisher']}) -> P{tid} ({name}); {why}"
            else:
                status, why = "held", "row changed since queued"
        counts[status] = counts.get(status, 0) + 1
        print(f"  S{r['id']} {r['display_name']!r}: {status}: {why}", flush=True)
        conn.execute(text("""
            UPDATE host_check_queue SET status = :st, checked_at = now(), publisher_id_after = :after,
                   note = :note WHERE id = :qid
        """), {"st": status, "after": after, "qid": r["qid"],
               "note": json.dumps({"why": why, "evidence": ev}, ensure_ascii=False)[:4000]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=300)
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
