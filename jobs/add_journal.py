"""Add one journal (or a few) to OpenAlex by harvesting its own OAI-PMH feed: the
support-ticket version of the #1417 RUNBOOK, generalised from the 2026-10-05
Zendesk OJS fixes and the 2026-10-22 DDH mints (handoff/ddh-mint-2026-10-22.py).

Two steps, so a human (or the support desk) reviews what will be written:

  python -m jobs.add_journal probe <journal homepage or OAI URL> [--issn 1234-5678 ...] [--ticket 25012] \
      [--out handoff/journal-add-<ticket>.csv]
      Read-only, runs anywhere (no DATABASE_URL): finds the OAI feed (Identify), the journal's
      set (ListSets), checks a set-scoped ListRecords (record count, tombstones, ISSNs and title
      in dc:source), looks the ISSNs up in the OpenAlex API and in every list we load
      (data/source_lists). Writes a one-row manifest and prints a verdict: READY_NEW_SOURCE,
      READY_EXISTING_SOURCE or HOLD (with the reason), and the quality bar: pass if the journal
      has a source, is on any of our lists, or has 3+ years of records (Kyle 2026-10-22).

  python -m jobs.add_journal apply <manifest.csv | --json '[{row}, ...]'> [--execute]
      (--json lets the support desk pass a probed row without deploying a file.) On the dyno, dry run by default, ONE transaction, receipt printed after RECEIPT. Per row:
        - source_id given: the source must exist and be a journal; ISSNs from the feed that no
          other source owns are added (RUNBOOK step 3)
        - no source_id: refuse if any ISSN is on a source or an ISSN-less source has the same
          normalized name; else mint_source(type journal)
        - curated_is_oa=true -> source_oa_override (only with evidence in oa_note)
        - feed: RUNBOOK step 4 registry check (a row on the same host, or on the host in the
          record ids, that harvests the whole install or the same journal blocks: that is a #1418
          split; the source is still minted/updated, the feed is held); else one oai_pmh_endpoint
          row, is_journal_host=true, picked up by the next daily openalex-ingest harvest
        - recompute_listed_in (a new source's ISSNs may already be on a loaded list)

Manifest columns: ticket, display_name, issns (;-separated), publisher, homepage_url,
country_code, country, source_id, pmh_url, pmh_set ('|' for several sets of one journal),
id_host, feed_records, curated_is_oa, oa_note, verdict, note.
"""
import argparse
import csv
import json
import os
import re
import secrets
import string
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timezone

UA = "OpenAlex-journal-add/1.0 (support@openalex.org; +https://openalex.org)"
O = "{http://www.openarchives.org/OAI/2.0/}"
DC = "{http://purl.org/dc/elements/1.1/}"
ISSN_RE = re.compile(r"\b\d{4}-\d{3}[\dxX]\b")
BAD_CTRL = re.compile(rb"[\x00-\x08\x0B\x0C\x0E-\x1F]")  # same repair as the harvester
PLACEHOLDER_IDS = {"ojs.pkp.sfu.ca", "localhost", "ojs.localhost", "generic.eprints.org"}
MAX_NEW_ROWS_PER_HOST = 20  # RUNBOOK step 4 (2026-10-01)
FIELDS = ["ticket", "display_name", "issns", "publisher", "homepage_url", "country_code", "country",
          "source_id", "pmh_url", "pmh_set", "id_host", "feed_records", "curated_is_oa", "oa_note",
          "on_lists", "bar", "verdict", "note"]
LISTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "source_lists")
MIN_YEARS = 3


# --- pure helpers (unit-tested) ------------------------------------------------

def valid_issn(i):
    d = i.replace("-", "").upper()
    if len(d) != 8 or not d[:7].isdigit():
        return False
    chk = (11 - sum(int(c) * w for c, w in zip(d[:7], range(8, 1, -1))) % 11) % 11
    return d[7] == ("X" if chk == 10 else str(chk))


def list_memberships(issns, lists_dir=LISTS_DIR):
    """Every list we load (data/source_lists/<list>-<date>.csv, newest edition per list) on which
    one of these ISSNs is an ACTIVE member: the 60-odd public source lists (DDH, Latindex, ERIH PLUS,
    national lists...), the internal SINTA / TCI tiering lists, and the PKP Beacon of OJS journals
    (data/ojs_beacon). Works for journals with no source yet, which the API's listed_in can't see."""
    want = {i.upper() for i in issns}
    newest = {}
    for f in sorted(os.listdir(lists_dir)) if os.path.isdir(lists_dir) else []:
        m = re.match(r"(.+)-(\d{4}-\d{2}-\d{2})\.csv$", f)
        if m and m.group(2) >= newest.get(m.group(1), ("", ""))[0]:
            newest[m.group(1)] = (m.group(2), f)
    beacon_dir = os.path.join(os.path.dirname(lists_dir), "ojs_beacon")
    if os.path.isdir(beacon_dir):  # the PKP Beacon (OJS journals), loaded as ojs_beacon_issn (#1424)
        eds = sorted(f for f in os.listdir(beacon_dir) if re.match(r"ojs-beacon-\d{4}-\d{2}-\d{2}\.csv$", f))
        if eds:
            newest["pkp-beacon"] = ("", os.path.join("..", "ojs_beacon", eds[-1]))
    hits = []
    for list_id, (_, f) in sorted(newest.items()):
        with open(os.path.join(lists_dir, f), newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if (r.get("active") or "true").strip().lower() in ("true", "1", "yes", "y") and \
                        want & {x.upper() for x in re.split(r"[;,\s]+", r.get("issns") or "") if x}:
                    hits.append(list_id)
                    break
    return hits


def quality_bar(source_id, on_lists, years, this_year=None):
    """Kyle 2026-10-22: add automatically when the journal already has a source, is on any of our
    lists, or has MIN_YEARS+ years of records; anything newer goes to a human."""
    this_year = this_year or datetime.now(timezone.utc).year
    if source_id:
        return "pass: existing source"
    if on_lists:
        return "pass: on " + ", ".join(on_lists[:6]) + (f" +{len(on_lists) - 6}" if len(on_lists) > 6 else "")
    if years and this_year - years[0] + 1 >= MIN_YEARS:
        return f"pass: records since {years[0]}"
    return "below: new journal, on none of our lists" + (f", records since {years[0]}" if years else "")


def host(url):
    return (urllib.parse.urlparse(url).hostname or "").lower().removeprefix("www.")


def journal_key(url, pmh_set):
    """What one endpoint row harvests: a journal key, or None for a whole install."""
    if pmh_set:
        return pmh_set
    path = urllib.parse.urlparse(url).path
    m = re.search(r"/index\.php/([^/]+)/oai", path) or re.search(r"^/([^/]+)/oai", path)
    j = m.group(1) if m else None
    return None if j in (None, "index", "oai") else j


def family(key):
    """Kind of per-journal key: 'journal:479' (Hrcak), 'journals:x' (OpenEdition),
    '495' (bibliotekanauki), 'xphi' (an OJS journal path)."""
    return key.split(":", 1)[0] if ":" in key else ("num" if key.isdigit() else "path")


def overlaps(ours_url, ours_set, their_url, their_set):
    """RUNBOOK step 4, conservative: an existing row is harmless only if it is clearly
    ANOTHER journal's row of the same kind."""
    ours = (journal_key(ours_url, ours_set) or "").lower()
    theirs = journal_key(their_url, their_set)
    return theirs is None or not ours or theirs.lower() == ours or family(theirs) != family(ours)


def oai_candidates(url, html=b""):
    """OAI base URLs to try for a journal page, best first: OJS install URL (+ set), the
    journal-level URL, links on the page, then common paths."""
    u = urllib.parse.urlparse(url)
    base = f"{u.scheme}://{u.netloc}"
    out = []
    if url.rstrip("/").endswith("/oai") or "verb=" in url:
        out.append(url.split("?")[0])
    m = re.match(r"(.*?)/index\.php/([^/?#]+)", url)
    if m:
        out += [m.group(1) + "/index.php/index/oai", m.group(1) + "/index.php/" + m.group(2) + "/oai"]
    else:  # OJS with restful URLs: host[/prefix]/<journal>/...
        parts = [p for p in u.path.split("/") if p]
        if parts:
            out += [base + "/index/oai", base + "/" + parts[0] + "/oai"]
    for link in re.findall(rb'href="([^"]*?/oai)(?:\?[^"]*)?"', html or b""):
        out.append(urllib.parse.urljoin(url, link.decode(errors="ignore")))
    out += [base + "/oai", base + "/index.php/index/oai", base + "/oai/request"]
    seen = []
    for x in out:
        if x not in seen:
            seen.append(x)
    return seen


def journal_path(url):
    m = re.search(r"/index\.php/([^/?#]+)", url)
    if m and m.group(1) != "index":
        return m.group(1)
    parts = [p for p in urllib.parse.urlparse(url).path.split("/") if p]
    return parts[0] if parts and parts[0] not in ("index", "oai") else None


def pick_set(sets, url, name_hint=""):
    """The journal's top-level set: its OJS path, else the only top-level set, else the
    best set-name match. Returns (setSpec, setName, how) or (None, None, why)."""
    top = [(s, n) for s, n in sets if ":" not in s]
    path = (journal_path(url) or "").lower()
    for s, n in top:
        if path and s.lower() == path:
            return s, n, "set = journal path"
    if len(top) == 1:
        return top[0][0], top[0][1], "single-journal install"
    if name_hint:
        norm = lambda x: re.sub(r"[^a-z0-9]+", " ", x.lower()).strip()
        best = [(s, n) for s, n in sets if norm(n) == norm(name_hint)]
        if len(best) == 1:
            return best[0][0], best[0][1], "set name = journal title"
    return None, None, f"{len(top)} top-level sets, none matches '{path or name_hint}'"


def parse_records(xml_bytes):
    root = ET.fromstring(BAD_CTRL.sub(b"", xml_bytes))
    err = root.find(f".//{O}error")
    if err is not None:
        return {"error": err.get("code") or "error"}
    recs = root.findall(f".//{O}record")
    tok = root.find(f".//{O}resumptionToken")
    more = tok is not None and (tok.text or "").strip()
    size = int(tok.get("completeListSize")) if tok is not None and tok.get("completeListSize") else None
    deleted = sum(1 for r in recs if r.find(O + "header").get("status") == "deleted")
    sources = [e.text or "" for r in recs for e in r.iter(DC + "source")]
    issns = Counter(i.upper() for s in sources for i in ISSN_RE.findall(s) if valid_issn(i))
    titles = Counter(re.sub(r"\s*[;,]?\s*(Vol|Bd|No|Nr|Núm|Num|Issue|Iss)\b.*$", "", s.split(";")[0]).strip()
                     for s in sources if s.strip())
    pubs = Counter((e.text or "").strip() for r in recs for e in r.iter(DC + "publisher") if (e.text or "").strip())
    years = sorted(int(y) for r in recs for e in r.iter(DC + "date") for y in re.findall(r"\b(19\d\d|20\d\d)\b", e.text or "")[:1])
    ids = [r.findtext(f"{O}header/{O}identifier") or "" for r in recs]
    m = re.match(r"oai:([^:]+):", ids[0]) if ids else None
    return {"records_on_page": len(recs), "deleted_on_page": deleted, "more": bool(more),
            "size": size if size is not None else (None if more else len(recs)),
            "issns": [i for i, _ in issns.most_common()], "title": titles.most_common(1)[0][0] if titles else "",
            "publisher": pubs.most_common(1)[0][0] if pubs else "", "id_host": m.group(1).lower() if m else "",
            "years": (years[0], years[-1]) if years else None}


# --- probe (read-only) ---------------------------------------------------------

def _get(url, timeout=60):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=timeout)
        return r.status, r.geturl(), r.read()
    except Exception as e:  # noqa: BLE001
        return getattr(e, "code", None), url, str(e).encode()


def _list_sets(base):
    out, url = [], base + "?verb=ListSets"
    while url and len(out) < 20000:
        st, _, b = _get(url, timeout=120)
        if st != 200:
            return out
        try:
            root = ET.fromstring(BAD_CTRL.sub(b"", b))
        except ET.ParseError:
            return out
        out += [((s.findtext(O + "setSpec") or "").strip(), (s.findtext(O + "setName") or "").strip())
                for s in root.iter(O + "set")]
        tok = root.find(f".//{O}resumptionToken")
        t = (tok.text or "").strip() if tok is not None else ""
        url = f"{base}?verb=ListSets&resumptionToken={urllib.parse.quote(t)}" if t else None
    return out


def _openalex_sources(issns, name):
    hits = {}
    q = "|".join(issns)
    if q:
        params = {"filter": "issn:" + q, "select": "id,display_name,issn,type,works_count"}
        if os.environ.get("OPENALEX_API_KEY"):  # keyless calls share a small daily budget per IP
            params["api_key"] = os.environ["OPENALEX_API_KEY"]
        st, _, b = _get("https://api.openalex.org/sources?" + urllib.parse.urlencode(params))
        if st != 200:
            return None  # lookup failed (e.g. 429 without a key): say so rather than report "no source"
        for s in json.loads(b)["results"]:
            hits[s["id"]] = s
    return list(hits.values())


def probe(url, issns=(), ticket="", name_hint=""):
    row = dict.fromkeys(FIELDS, "")
    row.update(ticket=ticket, homepage_url=url, curated_is_oa="", note="")
    notes, verdict = [], "READY_NEW_SOURCE"
    st, final, html = _get(url)
    gen = re.search(rb'<meta name="generator" content="([^"]+)"', html or b"")
    if gen:
        notes.append(gen.group(1).decode())
    base = repo_id = None
    is_ojs_feed = False
    for cand in oai_candidates(final or url, html if st == 200 else b""):
        s, _, b = _get(cand + "?verb=Identify")
        if s == 200 and b"<Identify" in b:
            base = cand
            is_ojs_feed = b"Open Journal Systems" in b or b"pkp.sfu.ca/ojs" in b
            m = re.search(rb"<repositoryIdentifier>([^<]+)", b)
            repo_id = m.group(1).decode().strip() if m else ""
            break
    if not base:
        row.update(verdict="HOLD", note=f"no OAI-PMH feed found from {url} (homepage HTTP {st})")
        return row
    sets = _list_sets(base)
    pmh_set, set_name, how = pick_set(sets, final or url, name_hint) if sets else (None, None, "no sets")
    if sets and pmh_set is None and journal_key(base, None) is None:
        row.update(pmh_url=base, verdict="HOLD", note=f"install feed {base}: {how}; pick the set by hand")
        return row
    q = {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
    if pmh_set:
        q["set"] = pmh_set
    s, _, b = _get(base + "?" + urllib.parse.urlencode(q), timeout=120)
    try:
        rec = parse_records(b) if s == 200 else {"error": f"HTTP {s}"}
    except ET.ParseError:
        rec = {"error": "malformed XML"}
    if rec.get("error"):
        row.update(pmh_url=base, pmh_set=pmh_set or "", verdict="HOLD", note=f"ListRecords: {rec['error']}")
        return row
    feed_issns = [i for i in rec["issns"] if valid_issn(i)]
    given = [re.sub(r"[\u2010-\u2014 ]", "-", i.strip()).upper() for i in issns]  # tickets carry non-breaking hyphens
    given = [i for i in given if valid_issn(i)]
    all_issns = list(dict.fromkeys(given + feed_issns))
    if given and feed_issns and not set(given) & set(feed_issns):
        notes.append(f"feed ISSNs {feed_issns} differ from the ticket's {given}")
        verdict = "HOLD"
    if rec["deleted_on_page"] == rec["records_on_page"]:
        notes.append("first page is all tombstones")
        verdict = "HOLD"
    if rec.get("years"):
        notes.append(f"first page dated {rec['years'][0]}-{rec['years'][1]}")
    if repo_id in PLACEHOLDER_IDS:
        notes.append(f"placeholder repository id {repo_id} (fine since #1407 Phase A unless walden excludes it)")
    existing = _openalex_sources(all_issns, set_name or rec["title"])
    if existing is None:
        notes.append("OpenAlex API lookup failed (set OPENALEX_API_KEY); apply still matches by registry ISSN")
        existing = []
    if len(existing) > 1:
        verdict = "HOLD"
        notes.append("ISSNs on several OpenAlex sources: " + ", ".join(f"{s['id'][-11:]} {s['display_name']!r}" for s in existing))
    elif existing:
        s0 = existing[0]
        row["source_id"] = s0["id"].rsplit("/S", 1)[1]
        notes.append(f"existing source {s0['id'][-11:]} {s0['display_name']!r} ({s0['works_count']} works, {s0['type']})")
        if verdict != "HOLD":
            verdict = "READY_EXISTING_SOURCE"
    if not all_issns:
        notes.append("no ISSN in the ticket or the feed: a mint needs a name check by hand")
        verdict = "HOLD"
    on_lists = list_memberships(all_issns)
    # Kyle 2026-10-22: an OJS journal is on the OJS list; our Beacon snapshot lags new journals, so a
    # live OJS install (page generator or OAI Identify) counts as pkp-beacon membership too
    if "pkp-beacon" not in on_lists and (any("Open Journal Systems" in n for n in notes) or is_ojs_feed):
        on_lists.append("pkp-beacon (live OJS)")
    row.update(on_lists=";".join(on_lists),
               bar=quality_bar(row["source_id"], on_lists, rec.get("years")))
    row.update(display_name=" ".join((set_name or rec["title"] or name_hint).split()), issns=";".join(all_issns),
               publisher=" ".join(rec["publisher"].split()), pmh_url=base, pmh_set=pmh_set or "",
               id_host=(repo_id or rec["id_host"] or "").lower() if (repo_id or "") not in PLACEHOLDER_IDS else rec["id_host"],
               feed_records=rec["size"] if rec["size"] is not None else f">{rec['records_on_page']}",
               verdict=verdict, note=f"{how}; " + "; ".join(notes))
    return row


# --- apply (dyno) ----------------------------------------------------------------

def apply(manifest, execute=False, rows=None):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from sqlalchemy import text

    from db import engine
    from sources_lib import (MatchContext, insert_issns, mint_source, normalize_name,
                             recompute_listed_in, resolve_issn_l)

    alnum = string.ascii_lowercase + string.digits
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": execute, "manifest": manifest,
               "minted": [], "updated": [], "endpoints": [], "held": [], "refused": [], "actions": []}

    def log(msg):
        print(msg, flush=True)
        receipt["actions"].append(msg)

    if rows is None:
        rows = list(csv.DictReader(open(manifest, newline="", encoding="utf-8")))
    rows = [{k: ("" if v is None else str(v)) for k, v in r.items()} for r in rows]
    per_host = Counter()

    class DryRun(Exception):
        pass

    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            for r in rows:
                name = r["display_name"]
                issns = [i.strip().upper() for i in r["issns"].split(";") if i.strip()]
                if r.get("verdict") == "HOLD":
                    receipt["refused"].append({"name": name, "why": "manifest verdict HOLD: " + r.get("note", "")})
                    log(f"SKIP {name!r}: manifest says HOLD")
                    continue
                source_id = r.get("source_id")
                if not source_id:
                    # the API lags the registry by up to a day: match on ISSN here (the shared match cascade)
                    owners = {x[0] for x in conn.execute(text("SELECT source_id FROM source_issn WHERE issn = ANY(:i)"), {"i": issns})}
                    if len(owners) == 1:
                        source_id = str(owners.pop())
                        log(f"MATCHED {name!r} by ISSN to registry source {source_id}")
                if source_id:
                    sid = int(source_id)
                    src = conn.execute(text("SELECT id, type FROM sources WHERE id = :id"), {"id": sid}).fetchone()
                    if src is None or src.type != "journal":
                        receipt["refused"].append({"name": name, "why": f"source {sid} missing or not a journal"})
                        log(f"REFUSED {name!r}: source {sid} missing or not a journal")
                        continue
                    owned = dict(conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"),
                                              {"i": issns}).fetchall())
                    if any(s != sid for s in owned.values()):
                        receipt["refused"].append({"name": name, "why": "issn_on_other_source", "detail": {k: v for k, v in owned.items() if v != sid}})
                        log(f"REFUSED {name!r}: ISSN on another source {owned} (a merge, not an add)")
                        continue
                    missing = [i for i in issns if i not in owned]
                    if missing:
                        have = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": sid})]
                        insert_issns(conn, sid, missing, resolve_issn_l(conn, sorted(set(have) | set(issns)), member_only=True))
                        conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :id"), {"id": sid})
                    receipt["updated"].append({"source_id": sid, "name": name, "issns_added": missing})
                    log(f"EXISTING {sid} {name!r}: ISSNs added {missing}")
                else:
                    owned = conn.execute(text("SELECT issn, source_id FROM source_issn WHERE issn = ANY(:i)"), {"i": issns}).fetchall()
                    if owned:
                        receipt["refused"].append({"name": name, "why": "issn_on_source", "detail": [list(map(str, o)) for o in owned]})
                        log(f"REFUSED {name!r}: ISSN already on a source {[tuple(o) for o in owned]}")
                        continue
                    # ISSN-less legacy twin (e.g. the 2022-10-17 MAG cohort): add the ISSNs there instead
                    twins = [t for t, _ in ctx.name_index.get(normalize_name(name), []) if not ctx.sid_to_issns.get(t)]
                    if twins:
                        receipt["refused"].append({"name": name, "why": "issnless_namesake", "detail": twins})
                        log(f"REFUSED {name!r}: ISSN-less source(s) with the same name {twins} (add the ISSNs there instead)")
                        continue
                    sid = mint_source(conn, ctx, name, source_type="journal", issns=issns,
                                      publisher=r.get("publisher") or None, homepage_url=r.get("homepage_url") or None,
                                      country_code=r.get("country_code") or None, country=r.get("country") or None)
                    receipt["minted"].append({"source_id": sid, "name": name, "issns": issns, "ticket": r.get("ticket")})
                    log(f"MINTED {sid} {name!r} {issns}")
                if (r.get("curated_is_oa") or "").lower() == "true":
                    assert r.get("oa_note"), f"{name}: curated_is_oa needs an oa_note with the evidence"
                    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, true, :n) "
                                      "ON CONFLICT (source_id) DO NOTHING"), {"s": sid, "n": r["oa_note"]})
                    log(f"  OA override on {sid}: {r['oa_note']}")
                if not r.get("pmh_url"):
                    continue
                for pmh_set in (r["pmh_set"].split("|") if r.get("pmh_set") else [None]):
                    hosts = {host(r["pmh_url"])} | ({r["id_host"]} if r.get("id_host") else set())
                    rows_on_host = {}
                    for h in hosts:
                        for e in conn.execute(text(
                                "SELECT id, pmh_url, pmh_set, status, source_id FROM oai_pmh_endpoint "
                                "WHERE lower(pmh_url) LIKE :a OR lower(pmh_url) LIKE :b"), {"a": f"%://{h}%", "b": f"%://www.{h}%"}):
                            rows_on_host[e.id] = e
                    block = [[e.id, e.pmh_url, e.pmh_set, e.status, e.source_id] for e in rows_on_host.values()
                             if overlaps(r["pmh_url"], pmh_set, e.pmh_url, e.pmh_set)]
                    if block:
                        receipt["held"].append({"source_id": sid, "name": name, "pmh_url": r["pmh_url"], "pmh_set": pmh_set, "blocking_rows": block})
                        log(f"  NO ENDPOINT for {sid} [{pmh_set or '-'}]: {len(block)} row(s) could harvest the same records -> #1418 split: {block[:3]}")
                        continue
                    h = host(r["pmh_url"])
                    if per_host[h] >= MAX_NEW_ROWS_PER_HOST:
                        receipt["held"].append({"source_id": sid, "name": name, "pmh_url": r["pmh_url"], "pmh_set": pmh_set, "why": "host_cap"})
                        log(f"  NO ENDPOINT for {sid}: host cap ({MAX_NEW_ROWS_PER_HOST}) reached for {h}")
                        continue
                    eid = "".join(secrets.choice(alnum) for _ in range(20))
                    conn.execute(text(
                        "INSERT INTO oai_pmh_endpoint (id, pmh_url, pmh_set, metadata_prefix, ready_to_run, in_walden, status, "
                        "source_id, is_journal_host, green_scrape, rand) "
                        "VALUES (:id, :u, :s, 'oai_dc', true, true, 'active', :sid, true, true, random())"),
                        {"id": eid, "u": r["pmh_url"], "s": pmh_set, "sid": sid})
                    per_host[h] += 1
                    receipt["endpoints"].append({"endpoint_id": eid, "source_id": sid, "pmh_url": r["pmh_url"], "pmh_set": pmh_set})
                    log(f"  REGISTERED endpoint {eid} {r['pmh_url']} [{pmh_set or '-'}] -> {sid} journal_host "
                        f"({len(rows_on_host)} other rows on the host, none overlapping)")
            changed = recompute_listed_in(conn)
            log(f"minted {len(receipt['minted'])}, existing {len(receipt['updated'])}, endpoints {len(receipt['endpoints'])}, "
                f"held {len(receipt['held'])}, refused {len(receipt['refused'])}; listed_in rows changed {changed}")
            if not execute:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back (ids above are not reserved)")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))
    return receipt


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("probe")
    p.add_argument("url")
    p.add_argument("--issn", action="append", default=[])
    p.add_argument("--ticket", default="")
    p.add_argument("--name", default="", help="journal title hint, for multi-journal installs")
    p.add_argument("--out", help="write/append the manifest row here")
    a = sub.add_parser("apply")
    a.add_argument("manifest", nargs="?")
    a.add_argument("--json", help="manifest rows as a JSON list (instead of a CSV path)")
    a.add_argument("--execute", action="store_true")
    args = ap.parse_args()
    if args.cmd == "probe":
        row = probe(args.url, args.issn, args.ticket, args.name)
        print(json.dumps(row, ensure_ascii=False, indent=1))
        print(f"VERDICT {row['verdict']}  BAR {row['bar']}")
        if args.out:
            new = not os.path.exists(args.out)
            with open(args.out, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=FIELDS)
                if new:
                    w.writeheader()
                w.writerow(row)
            print(f"manifest: {args.out}")
    else:
        if bool(args.manifest) == bool(args.json):
            ap.error("apply needs a manifest path or --json, not both")
        rows = json.loads(args.json) if args.json else None
        apply(args.manifest or "--json", execute=args.execute, rows=rows)


if __name__ == "__main__":
    main()
