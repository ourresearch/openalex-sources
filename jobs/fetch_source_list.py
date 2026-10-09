"""Fetch an external source list from its maintainer and write the CSV that
jobs.load_source_list expects (oxjob #1205, phase 4).

One adapter per list. Each downloads the maintainer's current machine-readable
file, keeps journals/series with an ISSN, decides `active`, and writes
data/source_lists/<list>-<YYYY-MM-DD>.csv with the loader's columns
(name,issns,active,withdrawn_date,withdrawal_reason). Nothing here touches the
database: the load itself stays a separate, by-hand step (see README).

  python -m jobs.fetch_source_list medline            # writes the CSV, prints stats
  python -m jobs.fetch_source_list norway --date 2026-09-18

Allow lists only (never deny lists). Level/tier registers (norway, jufo, jpps)
become ONE LIST PER LEVEL (`norway-1`, `norway-2`, `jufo-1`..`jufo-3`,
`jpps-1`..`jpps-3`): the maintainers built those levels precisely to get away
from the in-or-out binary, so collapsing them would lose the point (Jason,
2026-09-18). Non-level states (pending, new title, no stars, level 0) are not
lists. A grouped adapter returns {list_id: rows} and writes one CSV per level:

  python -m jobs.fetch_source_list jufo     # writes jufo-1, jufo-2, jufo-3

A few adapters read PDFs or spreadsheets and need extra tools on the machine
that runs the fetch (never on the Heroku dyno, which only loads the CSVs):
openpyxl (in requirements), poppler's `pdftotext` (anvur) and pdfplumber
(ccf, publindex): `uv run --with openpyxl --with pdfplumber python -m jobs.fetch_source_list ccf`.
"""
import argparse
import calendar
import csv
import gzip
import html
import io
import json
import re
import sys
import threading
import time
import zipfile
from collections import Counter
from datetime import date
from pathlib import Path
from urllib.request import Request, urlopen

ISSN_RE = re.compile(r"^\d{4}-\d{3}[\dXx]$")
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "source_lists"
UA = "openalex-sources/fetch_source_list (support@openalex.org)"


def _get(url, retries=3, ua=UA, timeout=300):
    for attempt in range(retries):
        try:
            with urlopen(Request(url, headers={"User-Agent": ua}), timeout=timeout) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            if attempt == retries - 1:
                raise
            print(f"  retry {attempt + 1}: {url} ({e})", file=sys.stderr)
            time.sleep(2 * (attempt + 1))


def _issns(*vals):
    out = []
    for v in vals:
        for x in re.split(r"[;,\s]+", (v or "").strip()):
            x = x.upper()
            if x and ISSN_RE.match(x) and x not in out:
                out.append(x)
    return out


def _row(name, issns, active=True, withdrawn_date="", reason=""):
    return {
        "name": (name or "").strip(),
        "issns": ";".join(issns),
        "active": "true" if active else "false",
        "withdrawn_date": withdrawn_date or "",
        "withdrawal_reason": reason or "",
    }


# --- medline ----------------------------------------------------------------
# Journals currently indexed for MEDLINE, from the NLM Catalog (E-utilities).
# J_Medline.txt is NOT this: it spans all of PubMed history. US public domain;
# NLM asks for "Courtesy of the U.S. National Library of Medicine".
def fetch_medline():
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    q = _get(base + "esearch.fcgi?db=nlmcatalog&term=currentlyindexed%5BAll%5D"
             "&usehistory=y&retmax=0&retmode=json")
    es = json.loads(q)["esearchresult"]
    total, webenv, qk = int(es["count"]), es["webenv"], es["querykey"]
    print(f"  NLM Catalog currentlyindexed: {total:,}")
    rows = []
    for start in range(0, total, 500):
        raw = _get(base + f"esummary.fcgi?db=nlmcatalog&query_key={qk}&WebEnv={webenv}"
                   f"&retstart={start}&retmax=500&retmode=json")
        res = json.loads(raw)["result"]
        for uid in res["uids"]:
            d = res[uid]
            issns = _issns(*[i.get("issn") for i in d.get("issnlist", [])])
            titles = d.get("titlemainlist") or []
            title = (titles[0].get("title") if titles else "") or d.get("medlineta") or ""
            if issns:
                rows.append(_row(title.rstrip("."), issns))
        time.sleep(0.4)  # 3 req/s without an API key
    return rows


# --- norway -----------------------------------------------------------------
# Norwegian Register for Scientific Journals, Series and Publishers (HK-dir),
# table 851. NLOD 2.0 + CC BY 4.0. Approved = level 1 or 2 in the current year;
# "X" = under discussion, the previous year's level stands; 0 = not approved.
def fetch_norway(today):
    raw = _get("https://kanalregister.hkdir.no/api/krtabeller/bulk-csv?rptNr=851")
    rd = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    year = today.year
    rows, skipped = {"norway-1": [], "norway-2": []}, 0
    for r in rd:
        level = ""
        for y in (year, year - 1, year - 2):
            v = (r.get(f"Nivå {y}") or "").strip()
            if v and v != "X":
                level = v
                break
        if level not in ("1", "2"):
            skipped += 1
            continue
        issns = _issns(r.get("Print ISSN"), r.get("Online ISSN"))
        if not issns:
            continue
        active = (r.get("Aktiv") or "").strip() == "1"
        ended = (r.get("Nedlagt år") or "").strip()
        rows[f"norway-{level}"].append(_row(r.get("Internasjonal tittel") or r.get("Original tittel"), issns,
                         active, f"{ended}-01-01" if (not active and ended.isdigit()) else "",
                         "" if active else f"inactive in the register (Nedlagt år {ended or '?'}; year only)"))
    print(f"  norway: level 1 {len(rows['norway-1']):,}, level 2 {len(rows['norway-2']):,}, skipped {skipped:,} rows with level 0 / blank")
    return rows


# --- jufo -------------------------------------------------------------------
# Finnish Publication Forum (JUFO), bulk JSON. Journals/series only (Type
# 'Lehti/sarja'), level 1-3. Licence for the data not stated by the maintainer
# (site material is CC BY 4.0) — confirm before publishing.
def fetch_jufo():
    raw = _get("https://jufo-rest.csc.fi/v1.1/massa.json.zip")
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        name = [n for n in z.namelist() if n.endswith("massa.json")][0]
        data = json.loads(z.read(name))
    rows, skipped = {"jufo-1": [], "jufo-2": [], "jufo-3": []}, 0
    for d in data:
        if not (d.get("Type") or "").lower().startswith("lehti/"):
            continue
        level = (d.get("Level") or "").strip()
        if level not in ("1", "2", "3"):
            skipped += 1
            continue
        issns = _issns(d.get("ISSN1"), d.get("ISSN2"), d.get("ISSNL"))
        if not issns:
            continue
        active = (d.get("Active") or "").strip().lower() == "active"
        end = (d.get("Year_End") or "").strip()
        rows[f"jufo-{level}"].append(_row(d.get("Name"), issns, active,
                         f"{end}-01-01" if (not active and end.isdigit()) else "",
                         "" if active else f"inactive in JUFO (Year_End {end or '?'}; year only)"))
    print(f"  jufo: " + ", ".join(f"level {k[-1]} {len(v):,}" for k, v in rows.items())
          + f", skipped {skipped:,} journal rows with level 0 / not evaluated")
    return rows


# --- erih-plus --------------------------------------------------------------
# ERIH PLUS approved list (HK-dir), kanalregister table 855. Flat list.
# Licence: NLOD via the API terms, but erihplus.hkdir.no says CC BY-NC 4.0 —
# confirm with HK-dir before publishing.
def fetch_erih_plus():
    raw = _get("https://kanalregister.hkdir.no/api/krtabeller/bulk-csv?rptNr=855")
    rd = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    rows = []
    for r in rd:
        issns = _issns(r.get("Print ISSN"), r.get("Online ISSN"))
        if not issns:
            continue
        status = (r.get("Active") or "").strip()
        active = status.lower() == "active"
        m = re.search(r"(\d{4})", status)
        rows.append(_row(r.get("International Title") or r.get("Original Title"), issns, active,
                         f"{m.group(1)}-01-01" if (not active and m) else "",
                         "" if active else f"{status} (year only)"))
    return rows


# --- scielo -----------------------------------------------------------------
# SciELO network journals via ArticleMeta. Current status is the LAST entry of
# the v51 history (v50 is 'C' on every row); D = deceased, S = suspended.
# Certified network collections only (not the thematic/independent ones).
def _partial_date(yyyymmdd):
    """ArticleMeta history dates are YYYYMMDD with '00' (or nothing) for unknown
    month/day, e.g. '20060000' / '201505'. Coerce unknown parts to 01 so the
    loader's date.fromisoformat accepts them; '' if the year is unknown."""
    y, m, d = yyyymmdd[:4], yyyymmdd[4:6], yyyymmdd[6:8]
    if not y.isdigit():
        return ""
    m = int(m) if m.isdigit() and 1 <= int(m) <= 12 else 1
    d = int(d) if d.isdigit() and int(d) >= 1 else 1
    d = min(d, calendar.monthrange(int(y), m)[1])  # ArticleMeta has e.g. 20230931
    return f"{y}-{m:02d}-{d:02d}"


def fetch_scielo():
    base = "https://articlemeta.scielo.org/api/v1/"
    colls = json.loads(_get(base + "collection/identifiers/"))
    by_issn = {}
    for c in colls:
        code = c.get("code") or c.get("acron")
        if not code:
            continue
        if (c.get("status") or "") != "certified" or "scielonetwork" not in str(c.get("network_classification") or ""):
            continue
        js = json.loads(_get(base + f"journal/?collection={code}"))
        for j in js:
            issns = _issns(*(j.get("issns") or []), j.get("code"))
            if not issns:
                continue
            hist = j.get("v51") or []
            cur = "C"
            if hist:
                last = max(hist, key=lambda h: (h.get("c") or h.get("a") or ""))
                cur = (last.get("d") or last.get("b") or "C").upper()
                cur_date = last.get("c") or ""
            else:
                cur_date = ""
            title = (j.get("v100") or [{}])[0].get("_") or ""
            key = issns[0]
            prev = by_issn.get(key)
            row = _row(title, issns, cur == "C",
                       _partial_date(cur_date) if cur != "C" else "",
                       "" if cur == "C" else {"D": "deceased", "S": "suspended"}.get(cur, cur) + f" in SciELO collection {code}")
            # a journal in several collections: active if active anywhere
            if prev is None or (row["active"] == "true" and prev["active"] == "false"):
                by_issn[key] = row
        time.sleep(0.2)
    return list(by_issn.values())


# --- latindex ---------------------------------------------------------------
# Latindex Catálogo 2.0 (UNAM + 23 partner institutions; Ibero-America).
# The site has a per-letter browse page (idMod=1 = Catálogo 2.0, current
# journals) with a signed CSV export link; the link is only on the page, so it
# is two requests per letter. 403s non-browser user agents; slow and flaky, so
# generous timeouts + retries. CC BY-NC-SA with a cite-the-source clause.
BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"


def _get_browser(url, retries=4, timeout=90):
    """Browser-UA GET with backoff. An EMPTY 200 body counts as a failure: AJOL
    answers a burst of requests with 0-byte 200s for a while (2026-09-18: 583 of
    600 pages), and the block lifts after a pause, so back off hard."""
    for attempt in range(retries):
        try:
            with urlopen(Request(url, headers={"User-Agent": BROWSER_UA}), timeout=timeout) as r:
                body = r.read()
            if body:
                return body
            raise RuntimeError("empty body (soft block?)")
        except Exception as e:  # noqa: BLE001
            if attempt == retries - 1:
                raise
            wait = 30 * (attempt + 1) if "empty body" in str(e) else 5 * (attempt + 1)
            print(f"  retry {attempt + 1} in {wait}s: {url} ({e})", file=sys.stderr)
            time.sleep(wait)


def fetch_latindex():
    rows, seen = [], set()
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        page = _get_browser(f"https://latindex.org/latindex/revistasTitulo?idLtr={letter}&idMod=1").decode("utf-8", "replace")
        m = re.search(r'href="(https://latindex\.org/latindex/exportar/indiceTitulo/csv/' + letter + r'/1[^"]*)"', page)
        if not m:
            raise RuntimeError(f"latindex: no CSV export link on letter page {letter}")
        raw = _get_browser(m.group(1).replace("&amp;", "&")).decode("utf-8-sig", "replace")
        n = 0
        for r in csv.DictReader(io.StringIO(raw), delimiter=";"):
            issns = _issns(r.get("issn_l"), r.get("issn_e"), r.get("issn_imp"))
            if not issns or issns[0] in seen:
                continue
            seen.add(issns[0])
            # the idMod=1 browse is Catálogo 2.0 current journals only; catalogada is a belt-and-braces check
            if (r.get("catalogada") or "1").strip() != "1":
                continue
            rows.append(_row(r.get("tit_propio"), issns))
            n += 1
        print(f"  latindex: {letter} {n:,}")
        time.sleep(2)
    return rows


# --- jpps -------------------------------------------------------------------
# Journal Publishing Practices and Standards (AJOL + INASP): every journal on
# the AJOL / NepJOL / BanglaJOL / CamJOL / MongoliaJOL / SLJOL platforms is
# assessed and given a level. One list per star level (jpps-1, jpps-2, jpps-3);
# "no stars", "new title", "pending" and "inactive title" are not lists. The
# directory has no ISSNs, so each starred journal's platform page is fetched
# (OJS or Ubiquity; both print "ISSN: NNNN-NNNX"). No licence stated
# (© INASP and AJOL); credited on the entity page like every other list.
JPPS_LEVELS = {"1 star": "jpps-1", "2 stars": "jpps-2", "3 stars": "jpps-3"}


def _norm_title(t):
    return re.sub(r"[^a-z0-9]+", " ", (t or "").lower()).strip()


def _openalex_issns(title, platform_url, iso2):
    """Fallback when a platform page can't be read (AJOL soft-blocks bursts):
    look the journal up in OpenAlex itself and accept a candidate only on a
    strong signal: its homepage is the same platform path, or its title matches
    exactly AND its country matches the JPPS flag. Generic titles ("Journal of
    Management") therefore never match a big-publisher namesake."""
    import os
    from urllib.parse import quote, urlparse
    hdrs = {"User-Agent": UA}
    key = os.environ.get("OPENALEX_ORG_API_KEY") or os.environ.get("OPENALEX_API_KEY")  # org key has the higher daily budget
    if key:
        hdrs["Authorization"] = f"Bearer {key}"
    url = ("https://api.openalex.org/sources?search=" + quote(title)
           + "&per_page=5&select=display_name,issn,homepage_url,country_code")
    try:
        with urlopen(Request(url, headers=hdrs), timeout=60) as r:
            results = json.loads(r.read()).get("results") or []
    except Exception as e:  # noqa: BLE001
        print(f"  jpps: openalex lookup failed for {title!r} ({e})", file=sys.stderr)
        return []
    want_path = urlparse(platform_url).path.rstrip("/").lower()
    want_host = urlparse(platform_url).netloc.lower().removeprefix("www.")
    for c in results:
        hp = urlparse(c.get("homepage_url") or "")
        same_platform = (hp.netloc.lower().removeprefix("www.") == want_host
                         and (hp.path.rstrip("/").lower() == want_path or want_path == ""))
        # exact title, or the candidate title is the JPPS title plus an acronym suffix
        # ("... Development (AJERD)"); country must match unless OpenAlex has none
        cand, want = _norm_title(c.get("display_name")), _norm_title(title)
        title_ok = cand == want or (want and cand.startswith(want + " ") and len(cand) - len(want) <= 12)
        cc = (c.get("country_code") or "").upper()
        country_ok = cc == (iso2 or "").upper() or not cc
        if same_platform or (title_ok and country_ok):
            return _issns(*(c.get("issn") or []))
    return []


def _get_via_zyte(url):
    """Fetch a page through Zyte (different egress IP) when the site soft-blocks
    desk. Needs ZYTE_API_KEY; returns "" if unset or on failure."""
    import base64
    import os
    key = os.environ.get("ZYTE_API_KEY")
    if not key:
        return ""
    body = json.dumps({"url": url, "httpResponseBody": True}).encode()
    req = Request("https://api.zyte.com/v1/extract", data=body, headers={
        "Authorization": "Basic " + base64.b64encode(f"{key}:".encode()).decode(),
        "Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=90) as r:
            data = json.loads(r.read())
        return base64.b64decode(data.get("httpResponseBody") or b"").decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        print(f"  zyte failed for {url}: {e}", file=sys.stderr)
        return ""


def fetch_jpps():
    html = _get_browser("https://www.journalquality.info/en/journals-all/").decode("utf-8", "replace")
    rows = {v: [] for v in JPPS_LEVELS.values()}
    entries = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        cells = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).replace("&nbsp;", " ").strip()
                 for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        href = re.search(r'href="(https?://(?!www\.journalquality)[^"]+)"', tr)
        iso2 = re.search(r"/img/iso2/([a-z]{2})\.png", tr)
        if len(cells) >= 3 and href and cells[2].lower() in JPPS_LEVELS:
            entries.append((cells[1], JPPS_LEVELS[cells[2].lower()], href.group(1), iso2.group(1) if iso2 else ""))
    print(f"  jpps: {len(entries):,} starred journals in the directory; fetching platform pages for ISSNs", flush=True)
    # page-level cache across runs: AJOL's soft block comes and goes mid-run, so a
    # re-run only needs to fetch the pages that failed last time
    cache_path = Path("/tmp/sl/jpps-issn-cache.json")
    try:
        cache = json.loads(cache_path.read_text())
    except Exception:  # noqa: BLE001
        cache = {}
    via_page = via_api = missing = 0
    for i, (title, list_id, url, iso2) in enumerate(entries, 1):
        issns = cache.get(url) or []
        if not issns:
            page = ""
            try:
                page = _get_browser(url, retries=1, timeout=45).decode("utf-8", "replace")
            except Exception:  # noqa: BLE001
                page = _get_via_zyte(url)  # AJOL soft-block: fetch from another IP
            text = re.sub(r"<[^>]+>", " ", page)
            issns = _issns(*re.findall(r"ISSN[^0-9]{0,12}(\d{4}-\d{3}[\dXx])", text))
            if issns:
                cache[url] = issns
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(json.dumps(cache))
            time.sleep(3)  # AJOL soft-blocks bursts with empty 200s (see _get_browser); 2 s tripped it on 2026-09-18
        if issns:
            via_page += 1
        else:
            issns = _openalex_issns(title, url, iso2)
            if issns:
                via_api += 1
            else:
                missing += 1
                print(f"  jpps: no ISSN for {title!r} ({url})", file=sys.stderr)
                continue
        rows[list_id].append(_row(title, issns))
        if i % 50 == 0:
            print(f"  jpps: {i:,}/{len(entries):,} (page {via_page}, openalex {via_api}, missing {missing})", flush=True)
    print("  jpps: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items())
          + f"; ISSNs via platform page {via_page:,}, via OpenAlex {via_api:,}, missing {missing:,}")
    return rows


# --- ki-jl ------------------------------------------------------------------
# Karolinska Institutet Journal List (KI-JL), one xlsx on the KI staff portal
# (staff.ki.se/research-support/karolinska-institutet-journal-list-kijl).
# Level 1-3 (meets criteria / high standard / highest); level 0 = "not
# recommended" is a non-list state and is NOT loaded (oxjob #1288). First
# edition decided by the Faculty Board 2026-05-05; annual after that. No data
# licence stated. The file id in the URL may change with the next edition.
KI_JL_URL = "https://staff.ki.se/media/173283/download"


def _xlsx_rows(raw, sheet=None):
    import openpyxl  # not needed by the scheduled jobs; keep the import local
    wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    ws = wb[sheet] if sheet else wb.worksheets[0]
    return [tuple(c for c in r) for r in ws.iter_rows(values_only=True)]


def _cell(v):
    return str(v).strip() if v is not None else ""


def fetch_ki_jl():
    rows_in = _xlsx_rows(_get(KI_JL_URL), "journals")
    hdr = next(i for i, r in enumerate(rows_in) if _cell(r[0]) == "Journal title")
    rows, skipped = {"ki-jl-1": [], "ki-jl-2": [], "ki-jl-3": []}, 0
    for r in rows_in[hdr + 1:]:
        title, p_issn, e_issn, level = (_cell(x) for x in r[:4])
        if not title:
            continue
        if level not in ("1", "2", "3"):
            skipped += 1
            continue
        issns = _issns(p_issn, e_issn)
        if not issns:
            continue
        rows[f"ki-jl-{level}"].append(_row(title, issns))
    print("  ki-jl: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items())
          + f"; skipped {skipped:,} rows at level 0 / blank")
    return rows


# --- abdc -------------------------------------------------------------------
# Australian Business Deans Council Journal Quality List. The xlsx link on
# abdc.edu.au/abdc-journal-quality-list/ carries a version stamp and moves with
# each revision, so it is resolved from the landing page. Sheet "<year> JQL";
# ratings A* (top), A, B, C. First list whose top tier is not the highest
# number; ids keep ABDC's own labels (oxjob #1288). No data licence stated.
ABDC_PAGE = "https://abdc.edu.au/abdc-journal-quality-list/"
ABDC_RATINGS = {"A*": "abdc-a-star", "A": "abdc-a", "B": "abdc-b", "C": "abdc-c"}


def fetch_abdc():
    page = _get(ABDC_PAGE)
    if page[:2] == b"\x1f\x8b":  # abdc.edu.au gzips the HTML whatever we accept
        page = gzip.decompress(page)
    page = page.decode("utf-8", "replace")
    m = re.search(r'https://abdc\.edu\.au/wp-content/uploads/[^"\']+?\.xlsx?', page)
    if not m:
        raise SystemExit("abdc: no xlsx link on the landing page")
    url = m.group(0)
    print(f"  abdc: {url}")
    rows_in = _xlsx_rows(_get(url))
    hdr = next(i for i, r in enumerate(rows_in) if "Journal Title" in [_cell(x) for x in r])
    cols = [_cell(x) for x in rows_in[hdr]]
    i_title, i_p, i_e = cols.index("Journal Title"), cols.index("ISSN"), cols.index("ISSNOnline")
    i_rating = next(i for i, c in enumerate(cols) if c.endswith("rating"))
    rows, skipped = {v: [] for v in ABDC_RATINGS.values()}, 0
    for r in rows_in[hdr + 1:]:
        title = _cell(r[i_title])
        if not title:
            continue
        list_id = ABDC_RATINGS.get(_cell(r[i_rating]).upper())
        if not list_id:
            skipped += 1
            continue
        issns = _issns(_cell(r[i_p]), _cell(r[i_e]))
        if not issns:
            continue
        rows[list_id].append(_row(title, issns))
    print("  abdc: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items())
          + f"; skipped {skipped:,} rows with no A*/A/B/C rating")
    return rows


# --- tci --------------------------------------------------------------------
# Thai-Journal Citation Index Centre: journals certified in Tier 1 (top) or
# Tier 2. Tier 3 means "not certified", so it is not a list. The site's own
# backend returns every record in one call (undocumented endpoint behind the
# React app). A renamed journal's current record is status 'name_changed' (the
# old-name record goes 'inactive'), so both 'active' and 'name_changed' count.
# No licence stated. NOT registered in source_list (Casey, 2026-09-30: internal
# for now, used only to tier OJS journals in jobs/ojs_beacon --lists; walden's
# sync_source_lists publishes every registered list). oxjob #1426.
TCI_URL = "https://tci-thailand.org/backend/journal/list_all_journal"
TCI_TIERS = {"1": "tci-1", "2": "tci-2"}


def fetch_tci():
    body = json.dumps({"start_item": 0, "offset": 5000, "tiers": [], "status": [], "area": [],
                       "main_area": [], "option": "", "search": ""}).encode()
    req = Request(TCI_URL, data=body, headers={"User-Agent": UA, "Content-Type": "application/json"})
    with urlopen(req, timeout=300) as r:
        data = json.loads(r.read())
    journals = data.get("journals") or []
    if len(journals) < int(data.get("journal_num") or 0) or len(journals) < 1000:
        raise SystemExit(f"tci: got {len(journals)} of {data.get('journal_num')} records; refusing a partial list")
    rows, skipped = {v: [] for v in TCI_TIERS.values()}, Counter()
    for j in journals:
        list_id = TCI_TIERS.get(str(j.get("tci_tier")))
        if not list_id or j.get("status") not in ("active", "name_changed"):
            skipped[(j.get("status"), j.get("tci_tier"))] += 1
            continue
        issns = _issns(j.get("issn"), j.get("eissn"))
        if issns:
            rows[list_id].append(_row(j.get("name_eng") or j.get("name_local"), issns))
    print("  tci: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items())
          + f"; skipped {sum(skipped.values()):,} (status, tier): {dict(skipped)}")
    return rows


# --- sinta ------------------------------------------------------------------
# SINTA (Science and Technology Index), Indonesia's national journal
# accreditation, ranks S1 (top) to S6. One list per rank, ids keep SINTA's own
# labels (sinta-s1 .. sinta-s6; the 041 note on direction). No bulk file or API:
# the public listing (10 journals a page) carries id, title, ISSNs and the LAST
# rank, but it also lists journals whose accreditation has expired. Validity is
# only on each profile's "History Accreditation" table (one rank per year), so
# every profile is fetched: a journal is active when that table reaches the
# current year, and listed under its current rank; an expired one is kept
# inactive under its last rank (withdrawn_date = 31 Dec of its last year).
# ~16.8K profiles: hours (the server drops connection attempts under load, hence
# the short timeout and retries); progress is cached in the temp dir, so a rerun
# resumes. No licence stated. NOT registered in
# source_list, like tci (internal, oxjob #1426).
SINTA = "https://sinta.kemdiktisaintek.go.id"
SINTA_WORKERS = 3
# SINTA answers 403 to any User-Agent not starting "Mozilla/5.0"; the identified
# crawler form (as Googlebot's) passes and still says who we are.
SINTA_UA = "Mozilla/5.0 (compatible; openalex-sources/fetch_source_list; +mailto:support@openalex.org)"


def _sinta_page(n):
    h = _get(f"{SINTA}/journals/index/?page={n}", retries=4, ua=SINTA_UA, timeout=30).decode("utf-8", "replace")
    out = []
    for card in h.split('<div class="list-item row')[1:]:
        m = re.search(r'/journals/profile/(\d+)">\s*(.*?)\s*<i ', card, re.S)
        if not m:
            continue
        p = re.search(r"P-ISSN\s*:\s*([\dXx]{8})?", card)
        e = re.search(r"E-ISSN\s*:\s*([\dXx]{8})?", card)
        rank = re.search(r'num-stat accredited">.*?</i>\s*(S[1-6])\s', card, re.S)
        web = re.search(r'href="([^"]+)"><i class="el el-globe mr-1', card)
        fmt = lambda v: f"{v[:4]}-{v[4:]}".upper() if v else ""  # noqa: E731
        out.append({"id": int(m.group(1)), "name": html.unescape(re.sub(r"\s+", " ", m.group(2))),
                    "issns": _issns(fmt(p and p.group(1)), fmt(e and e.group(1))),
                    "last_rank": rank.group(1) if rank else "", "website": web.group(1) if web else ""})
    total = re.search(r"Total Records ([\d.]+)", h)
    return out, int(total.group(1).replace(".", "")) if total else None


def _sinta_history(sinta_id):
    """{year: rank} from the profile's History Accreditation table."""
    h = _get(f"{SINTA}/journals/profile/{sinta_id}", retries=4, ua=SINTA_UA, timeout=30).decode("utf-8", "replace")
    i = h.find("History Accreditation")
    if i < 0:
        return {}
    rows = h[i:h.find("</table>", i)].split("</tr>")
    years = [int(y) for y in re.findall(r"<small>\s*(\d{4})\s*</small>", rows[0])]
    ranks = re.findall(r'title="Sinta (\d)"', rows[1]) if len(rows) > 1 else []
    return {y: f"S{r}" for y, r in zip(years, ranks)}


def fetch_sinta(today):
    import tempfile
    from concurrent.futures import ThreadPoolExecutor
    cache = Path(tempfile.gettempdir()) / "sinta-cache.jsonl"
    done = {}
    if cache.exists():
        for line in cache.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            done[(rec["kind"], rec["key"])] = rec["value"]
    lock = threading.Lock()

    def remember(kind, key, value):
        with lock:
            done[(kind, key)] = value
            with open(cache, "a", encoding="utf-8") as f:
                f.write(json.dumps({"kind": kind, "key": key, "value": value}, ensure_ascii=False) + "\n")

    first, total = _sinta_page(1)
    pages = -(-total // 10)
    remember("page", 1, first)
    print(f"  sinta: {total:,} journals on {pages:,} pages; cache {cache}", flush=True)

    def page(n):
        if ("page", n) not in done:
            try:
                remember("page", n, _sinta_page(n)[0])
            except Exception as e:  # noqa: BLE001 -- a rerun resumes from the cache
                print(f"  sinta: page {n} failed ({e})", file=sys.stderr)

    def profile(sid):
        if ("history", sid) not in done:
            try:
                remember("history", sid, {str(y): r for y, r in _sinta_history(sid).items()})
            except Exception as e:  # noqa: BLE001
                print(f"  sinta: profile {sid} failed ({e})", file=sys.stderr)

    with ThreadPoolExecutor(SINTA_WORKERS) as pool:
        list(pool.map(page, range(1, pages + 1)))
        missing = [n for n in range(1, pages + 1) if ("page", n) not in done]
        if missing:
            raise SystemExit(f"sinta: {len(missing)} listing pages failed; rerun to resume")
        cards = {c["id"]: c for n in range(1, pages + 1) for c in done[("page", n)]}
        print(f"  sinta: {len(cards):,} journals listed; fetching profiles", flush=True)
        for i, _ in enumerate(pool.map(profile, sorted(cards)), 1):
            if i % 500 == 0:
                print(f"  sinta: {i:,}/{len(cards):,} profiles", flush=True)

    missing = [sid for sid in cards if ("history", sid) not in done]
    if missing:
        raise SystemExit(f"sinta: {len(missing)} profiles failed; rerun to resume")
    if sum(1 for sid in cards if not done[("history", sid)]) > len(cards) / 2:
        raise SystemExit("sinta: most profiles have no History Accreditation table; layout changed?")
    rows = {f"sinta-s{k}": [] for k in range(1, 7)}
    counts = Counter()
    for sid, c in sorted(cards.items()):
        hist = {int(y): r for y, r in done[("history", sid)].items()}
        if not c["issns"]:
            counts["no_issn"] += 1
            continue
        if today.year in hist:
            rows[f"sinta-{hist[today.year].lower()}"].append(_row(c["name"], c["issns"]))
            counts["active"] += 1
        elif hist or c["last_rank"]:
            last = max(hist) if hist else None
            rank = hist[last] if hist else c["last_rank"]
            rows[f"sinta-{rank.lower()}"].append(_row(
                c["name"], c["issns"], active=False,
                withdrawn_date=f"{last}-12-31" if last else "", reason="accreditation expired"))
            counts["expired"] += 1
        else:
            counts["no_rank"] += 1
    print("  sinta: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items()) + f"; {dict(counts)}")
    return rows


# --- batch 5 (oxjob #1615) ----------------------------------------------------
# National research-evaluation registers, business-school lists and CS / HSS
# lists suggested after Jason's coffee with Ross Mounce (2026-10-09). Same rules
# as #1205: allow lists only, one list per level, ids and display names keep the
# maintainer's own labels, and the scope text says which level is the top one.


def _ddmmyyyy(v):
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", (v or "").strip())
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else ""


def _pdftotext(raw):
    """Text of a PDF with its column layout kept (poppler's pdftotext; desk has it
    via Homebrew). Only the by-hand fetches below need it."""
    import subprocess
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
        f.write(raw)
        f.flush()
        return subprocess.run(["pdftotext", "-layout", f.name, "-"], check=True,
                              capture_output=True).stdout.decode("utf-8", "replace")


def _issn8(v):
    """'01245996' -> '0124-5996' (lists that print ISSNs without the hyphen)."""
    v = (v or "").strip().upper()
    return f"{v[:4]}-{v[4:]}" if re.match(r"^\d{7}[\dX]$", v) else v


# --- russia-white-list --------------------------------------------------------
# Russia's "White List" (Белый список) of journals, kept by the Russian Center
# for Scientific Information (RCSI) for the Ministry of Science's interagency
# working group. Levels 1 (top) to 4. A journal's level is the most recent of
# its level_2026 / level_2025 / level_2023 columns. state 'discontinued' =
# removed from the list (kept inactive); state 'warning' is a notice about
# paying Elsevier APCs, not a removal, so those journals stay members. The file
# carries an `oax_id` column, but we join on ISSN like every other list.
RUSSIA_CSV = "https://journalrank.rcsi.science/ru/record-sources/download/?dataType=Csv"


def fetch_russia():
    csv.field_size_limit(10 ** 8)
    raw = _get(RUSSIA_CSV, ua=BROWSER_UA).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(raw), delimiter="\t")
    need = {"title", "issns", "level_2023", "level_2025", "state", "date_discontinued"}
    if not need <= set(reader.fieldnames or []):
        raise SystemExit(f"russia: columns changed; missing {need - set(reader.fieldnames or [])}")
    rows, counts = {f"russia-white-list-{n}": [] for n in range(1, 5)}, Counter()
    for r in reader:
        level = (r.get("level_2026") or r.get("level_2025") or r.get("level_2023") or "").strip()
        issns = _issns((r.get("issns") or "").replace("|", ";"))
        if level not in ("1", "2", "3", "4") or not issns:
            counts["no_level_or_issn"] += 1
            continue
        active = (r.get("state") or "").strip() != "discontinued"
        counts["active" if active else "discontinued"] += 1
        rows[f"russia-white-list-{level}"].append(_row(
            r.get("title"), issns, active=active,
            withdrawn_date="" if active else _ddmmyyyy(r.get("date_discontinued")),
            reason="" if active else "removed from the White List"))
    if sum(len(v) for v in rows.values()) < 25000:
        raise SystemExit(f"russia: only {sum(len(v) for v in rows.values())} rows; refusing a partial list")
    print("  russia: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items()) + f"; {dict(counts)}")
    return rows


# --- poland -------------------------------------------------------------------
# Polish Ministry of Science list of scientific journals (wykaz czasopism
# naukowych), announced 5 Jan 2024; still current (the next list is due at the
# end of 2026, under rules signed 30 Apr 2026). Each journal carries points:
# 20, 40, 70, 100, 140, 200 (top). One list per points level (poland-200 ..
# poland-20). The xlsx is one of the announcement's gov.pl attachments; when a
# new list is announced, point POLAND_PAGE at the new announcement. The
# conference-proceedings sheet is not loaded (no ISSNs).
POLAND_PAGE = ("https://www.gov.pl/web/nauka/komunikat-ministra-nauki-z-dnia-05-stycznia-2024-r-w-sprawie-"
               "wykazu-czasopism-naukowych-i-recenzowanych-materialow-z-konferencji-miedzynarodowych")
POLAND_POINTS = (20, 40, 70, 100, 140, 200)


def fetch_poland():
    page = _get(POLAND_PAGE, ua=BROWSER_UA).decode("utf-8", "replace")
    raw = None
    for att in dict.fromkeys(re.findall(r"/attachment/[0-9a-f-]{36}", page)):
        body = _get("https://www.gov.pl" + att, ua=BROWSER_UA)
        if body[:2] == b"PK":  # the xlsx (the other attachments are PDFs)
            raw = body
            break
    if raw is None:
        raise SystemExit("poland: no xlsx attachment on the announcement page")
    rows_in = _xlsx_rows(raw)
    hdr = next(i for i, r in enumerate(rows_in) if _cell(r[0]).rstrip(".").lower() == "lp")
    cols = [_cell(x).lower() for x in rows_in[hdr]]
    i_title, i_pts = cols.index("tytuł 1"), cols.index("punktacja")
    issn_cols = [i for i, c in enumerate(cols[:i_pts]) if c in ("issn", "e-issn")]
    rows, skipped = {f"poland-{p}": [] for p in POLAND_POINTS}, Counter()
    for r in rows_in[hdr + 1:]:
        title = _cell(r[i_title])
        try:
            pts = int(float(_cell(r[i_pts])))
        except ValueError:
            skipped["no_points"] += 1
            continue
        issns = _issns(*(_cell(r[i]) for i in issn_cols))
        if f"poland-{pts}" not in rows or not issns:
            skipped[f"points={pts}" if issns else "no_issn"] += 1
            continue
        rows[f"poland-{pts}"].append(_row(title, issns))
    print("  poland: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items()) + f"; skipped {dict(skipped)}")
    return rows


# --- vabb-shw -----------------------------------------------------------------
# Flemish Academic Bibliography for the Social Sciences and Humanities
# (VABB-SHW), ECOOM (University of Antwerp) for the Flemish government: the
# journals whose publications count as peer-reviewed in Flanders' university
# funding model. One list. "Status peer review" is "1", "1, vanaf <year>" or
# "1, tot en met <y1> (0, vanaf <y2>)"; the last kind is no longer peer-reviewed
# from y2 on, so it is kept inactive once y2 has come. The VABB-categorie codes
# (Issn, Isia..Isie) are not documented on the page; they are not used.
VABB_CSV = "https://www.ecoom.be/nodes/tijdschrifteninvabbshwversie1520142023/en/download"


def fetch_vabb(today):
    raw = _get(VABB_CSV, ua=BROWSER_UA).decode("utf-8-sig", "replace")
    rows, counts = [], Counter()
    for r in csv.reader(io.StringIO(raw), delimiter=";"):
        if not r or r[0] == "VABB-categorie" or len(r) < 4:
            continue
        issns = _issns(r[1])
        if not issns:
            counts["no_issn"] += 1
            continue
        ended = re.search(r"\(0, vanaf (\d{4})\)", r[3])
        active = not (ended and int(ended.group(1)) <= today.year)
        counts["active" if active else "not_peer_reviewed_now"] += 1
        rows.append(_row(r[2], issns, active=active,
                         withdrawn_date="" if active else f"{ended.group(1)}-01-01",
                         reason="" if active else "no longer counted as peer-reviewed"))
    if len(rows) < 10000:
        raise SystemExit(f"vabb: only {len(rows)} rows; refusing a partial list")
    print(f"  vabb-shw: {len(rows):,} rows; {dict(counts)}")
    return rows


# --- fnege --------------------------------------------------------------------
# FNEGE (Fondation Nationale pour l'Enseignement de la Gestion des Entreprises)
# ranking of management journals, 2025 edition (next due 2028). Ranks 1* (top),
# 1, 2, 3, 4. Rows ranked "EM" (4 journals; the page does not define it) are not
# loaded. The page is one public HTML table with pISSN and eISSN.
FNEGE_PAGE = "https://fnege.org/classement-des-revues-scientifiques-en-sciences-de-gestion/"
FNEGE_RANKS = {"1*": "fnege-1-star", "1": "fnege-1", "2": "fnege-2", "3": "fnege-3", "4": "fnege-4"}


def fetch_fnege():
    page = _get(FNEGE_PAGE, ua=BROWSER_UA).decode("utf-8", "replace")
    table = max(re.findall(r"<table.*?</table>", page, re.S), key=len)
    cell = lambda c: html.unescape(re.sub(r"<[^>]+>", "", c)).strip()  # noqa: E731
    trs = [[cell(c) for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.S)]
           for tr in re.findall(r"<tr.*?</tr>", table, re.S)]
    hdr = trs[0]
    rank_col = max(i for i, c in enumerate(hdr) if re.match(r"FNEGE_\d{4}$", c))
    print(f"  fnege: ranking column {hdr[rank_col]}")
    i_t, i_p, i_e = hdr.index("TITLE"), hdr.index("pISSN"), hdr.index("eISSN")
    rows, skipped = {v: [] for v in FNEGE_RANKS.values()}, Counter()
    for r in trs[1:]:
        list_id = FNEGE_RANKS.get(r[rank_col].replace(" ", ""))
        issns = _issns(r[i_p], r[i_e])
        if not list_id or not issns:
            skipped[r[rank_col] if not list_id else "no_issn"] += 1
            continue
        rows[list_id].append(_row(r[i_t], issns))
    print("  fnege: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items()) + f"; skipped {dict(skipped)}")
    return rows


# --- tr-dizin -----------------------------------------------------------------
# TR Dizin, TÜBİTAK ULAKBİM's national index of Turkish journals (used in
# Turkey's associate-professor criteria). One list: journals on the current
# year's list (journalYear contains today's year). Journals indexed in earlier
# years only are kept inactive. Open JSON search API, 100 journals a page.
TRDIZIN_API = "https://search.trdizin.gov.tr/api/defaultSearch/journal/?q=&order=title-asc&page={}&limit=100"


def fetch_trdizin(today):
    hits, n = [], 1
    while True:
        d = json.loads(_get(TRDIZIN_API.format(n), ua=BROWSER_UA))
        page = [h["_source"] for h in d["hits"]["hits"]]
        if not page:
            break
        hits += page
        n += 1
        time.sleep(0.3)
    total = d["hits"]["total"]["value"]
    if len(hits) < total:
        raise SystemExit(f"tr-dizin: got {len(hits)} of {total}; refusing a partial list")
    rows, counts = [], Counter()
    for h in hits:
        years = sorted(y["year"] for y in h.get("journalYear") or [])
        issns = _issns(h.get("issn"), h.get("eissn"))
        if not years or not issns:
            counts["no_year_or_issn"] += 1
            continue
        active = today.year in years
        counts["active" if active else "earlier_years_only"] += 1
        rows.append(_row(h.get("title"), issns, active=active,
                         withdrawn_date="" if active else f"{years[-1]}-12-31",
                         reason="" if active else "not on the current TR Dizin list"))
    if counts["active"] < 1000:
        raise SystemExit(f"tr-dizin: only {counts['active']} journals on the {today.year} list; too early in the year?")
    print(f"  tr-dizin: {len(rows):,} rows; {dict(counts)}")
    return rows


# --- dhet ---------------------------------------------------------------------
# South Africa's Department of Higher Education and Training (DHET) list of
# approved South African journals. Published inside the "ZA Publications
# Monitor" master list (CREST, Stellenbosch University), which unions every
# list DHET accepts for subsidy (WoS, Scopus, DOAJ, IBSS, the Norwegian
# register, SciELO SA, DHET). Only the DHET column is loaded; the others are
# vendor indexes or lists we already carry. status "Eligible for subsidy" =
# active; Delisted / Discontinued / Inactive etc. are kept inactive.
DHET_PAGE = "https://db.crest.sun.ac.za/zapublications/"


def fetch_dhet():
    page = _get(DHET_PAGE, ua=BROWSER_UA).decode("utf-8", "replace")
    files = sorted(set(re.findall(r"downloadable_journal_master_list_(\d{8})\.xlsx", page)))
    if not files:
        raise SystemExit("dhet: no master-list xlsx on the page")
    url = f"{DHET_PAGE}downloadfile/downloadable_journal_master_list_{files[-1]}.xlsx"
    print(f"  dhet: {url}")
    rows_in = _xlsx_rows(_get(url, ua=BROWSER_UA))
    cols = [_cell(x) for x in rows_in[0]]
    i_t, i_p, i_e, i_s, i_d = (cols.index(c) for c in ("standard journal title", "issn", "eissn", "status", "DHET"))
    rows, counts = [], Counter()
    for r in rows_in[1:]:
        if _cell(r[i_d]).upper() != "X":
            continue
        issns = _issns(_cell(r[i_p]), _cell(r[i_e]))
        if not issns:
            counts["no_issn"] += 1
            continue
        status = _cell(r[i_s])
        active = status.lower().startswith("eligible for subsidy")
        counts[status or "blank"] += 1
        rows.append(_row(_cell(r[i_t]), issns, active=active, reason="" if active else status))
    print(f"  dhet: {len(rows):,} rows; {dict(counts)}")
    return rows


# --- ft50, utd24 --------------------------------------------------------------
# Two short business-school research lists, typed in by hand (titles only on
# the maintainers' pages, so the ISSNs were mapped to OpenAlex sources by hand
# on 2026-10-09, every title an exact match). FT50: the 50 journals behind the
# Financial Times Research Rank, as revised in April 2026 (Human Relations,
# Journal of Business Ethics and Organization Studies out; Academy of Management
# Annals, American Sociological Review and Psychological Science in). UTD24: the
# 24 journals behind UT Dallas's Top 100 Business School Research Rankings.
# Re-check both pages when the maintainers announce a change.
FT50 = [
    ("Academy of Management Journal", "0001-4273;1948-0989"),
    ("Academy of Management Review", "0363-7425;1930-3807"),
    ("Academy of Management Annals", "1941-6067;1941-6520"),
    ("Accounting, Organizations and Society", "0361-3682;1873-6289"),
    ("Administrative Science Quarterly", "0001-8392;1930-3815"),
    ("American Economic Review", "0002-8282;1944-7981"),
    ("American Sociological Review", "0003-1224;1939-8271"),
    ("Contemporary Accounting Research", "0823-9150;1911-3846"),
    ("Econometrica", "0012-9682;1468-0262"),
    ("Entrepreneurship Theory and Practice", "1042-2587;1540-6520"),
    ("Harvard Business Review", "0017-8012"),
    ("Human Resource Management", "0090-4848;1099-050X"),
    ("Information Systems Research", "1047-7047;1526-5536"),
    ("Journal of Accounting and Economics", "0165-4101;1879-1980"),
    ("Journal of Accounting Research", "0021-8456;1475-679X"),
    ("Journal of Applied Psychology", "0021-9010;1939-1854"),
    ("Journal of Business Venturing", "0883-9026;1873-2003"),
    ("Journal of Consumer Psychology", "1057-7408;1532-7663"),
    ("Journal of Consumer Research", "0093-5301;1537-5277"),
    ("Journal of Finance", "0022-1082;1540-6261"),
    ("Journal of Financial and Quantitative Analysis", "0022-1090;1756-6916"),
    ("Journal of Financial Economics", "0304-405X;1879-2774"),
    ("Journal of International Business Studies", "0047-2506;1478-6990"),
    ("Journal of Management", "0149-2063;1557-1211"),
    ("Journal of Management Information Systems", "0742-1222;1557-928X"),
    ("Journal of Management Studies", "0022-2380;1467-6486"),
    ("Journal of Marketing", "0022-2429;1547-7185"),
    ("Journal of Marketing Research", "0022-2437;1547-7193"),
    ("Journal of Operations Management", "0272-6963;1873-1317"),
    ("Journal of Political Economy", "0022-3808;1537-534X"),
    ("Journal of the Academy of Marketing Science", "0092-0703;1552-7824"),
    ("Management Science", "0025-1909;1526-5501"),
    ("Manufacturing & Service Operations Management", "1523-4614;1526-5498"),
    ("Marketing Science", "0732-2399;1526-548X"),
    ("MIS Quarterly", "0276-7783;2162-9730"),
    ("Operations Research", "0030-364X;1526-5463"),
    ("Organization Science", "1047-7039;1526-5455"),
    ("Organizational Behavior and Human Decision Processes", "0749-5978;1095-9920"),
    ("Production and Operations Management", "1059-1478;1937-5956"),
    ("Psychological Science", "0956-7976;1467-9280"),
    ("Quarterly Journal of Economics", "0033-5533;1531-4650"),
    ("Research Policy", "0048-7333;1873-7625"),
    ("Review of Accounting Studies", "1380-6653;1573-7136"),
    ("Review of Economic Studies", "0034-6527;1467-937X"),
    ("Review of Finance", "1572-3097;1875-824X"),
    ("Review of Financial Studies", "0893-9454;1465-7368"),
    ("MIT Sloan Management Review", "1532-8937;1532-9194;0019-848X"),
    ("Strategic Entrepreneurship Journal", "1932-4391;1932-443X"),
    ("Strategic Management Journal", "0143-2095;1097-0266"),
    ("The Accounting Review", "0001-4826;1558-7967"),
]
UTD24 = [
    "The Accounting Review", "Journal of Accounting and Economics", "Journal of Accounting Research",
    "Journal of Finance", "Journal of Financial Economics", "Review of Financial Studies",
    "Information Systems Research", "INFORMS Journal on Computing", "MIS Quarterly",
    "Journal of Consumer Research", "Journal of Marketing", "Journal of Marketing Research", "Marketing Science",
    "Management Science", "Operations Research", "Journal of Operations Management",
    "Manufacturing & Service Operations Management", "Production and Operations Management",
    "Academy of Management Journal", "Academy of Management Review", "Administrative Science Quarterly",
    "Organization Science", "Journal of International Business Studies", "Strategic Management Journal",
]
UTD24_EXTRA = {"INFORMS Journal on Computing": "0899-1499;1091-9856;1526-5528"}


def fetch_ft50():
    return [_row(t, _issns(i)) for t, i in FT50]


def fetch_utd24():
    ft = dict(FT50)
    return [_row(t, _issns(ft.get(t) or UTD24_EXTRA[t])) for t in UTD24]


# --- anvur --------------------------------------------------------------------
# ANVUR (Italy's national agency for the evaluation of universities and
# research) journal lists for the non-bibliometric areas (08 architecture,
# 10-14 humanities, history/philosophy/education/psychology, law, economics and
# statistics, political and social sciences), used in the national academic
# qualification (ASN). Two lists: `anvur-scientific` (journals rated scientific
# in any area) and `anvur-class-a` (Class A in at least one hiring sector; the
# top tier, and a subset of the scientific journals in practice). One PDF per
# list and area, one row per ISSN. Legend: A / S = in the list; A(year) /
# S(year) = from 1 Jan of that year; a(year) / s(year) = only until 31 Dec of
# that year (kept inactive).
ANVUR_PAGE = "https://www.anvur.it/it/ricerca/riviste/elenchi-di-riviste-classificate"


def fetch_anvur():
    page = _get(ANVUR_PAGE, ua=BROWSER_UA).decode("utf-8", "replace")
    pdfs = re.findall(r'href="(/sites/default/files/[^"]*Riviste%20(SCI|CLA)_Area(\d+)_(\d{8})\.pdf)"', page)
    if not pdfs:
        raise SystemExit("anvur: no per-area PDFs on the page")
    # newest edition only (dates are DDMMYYYY)
    stamp = max({d for *_, d in pdfs}, key=lambda d: d[4:] + d[2:4] + d[:2])
    pdfs = [(u, kind, area) for u, kind, area, d in pdfs if d == stamp]
    print(f"  anvur: edition {stamp}, {len(pdfs)} PDFs")
    issn_re = re.compile(r"\b(\d{4}-\d{3}[\dXx])\b")
    found = {"anvur-class-a": {}, "anvur-scientific": {}}  # list -> {issn: [title, active]}
    for url, kind, area in sorted(pdfs):
        list_id, mark = ("anvur-class-a", "A") if kind == "CLA" else ("anvur-scientific", "S")
        text = _pdftotext(_get("https://www.anvur.it" + url, ua=BROWSER_UA))
        n = 0
        for line in text.splitlines():
            m = issn_re.search(line)
            if not m:
                continue
            title, rest = line[:m.start()].strip(), line[m.end():]
            now = re.search(rf"(?<![\w(]){mark}(\(\d{{4}}\))?(?!\w)", rest)
            past = re.search(rf"(?<![\w(]){mark.lower()}\(\d{{4}}\)", rest)
            if not (now or past):
                continue
            issn = m.group(1).upper()
            prev = found[list_id].get(issn)
            found[list_id][issn] = [title or (prev and prev[0]) or "", bool(now) or bool(prev and prev[1])]
            n += 1
        print(f"  anvur: {kind} area {area}: {n:,} rows")
    rows = {}
    for list_id, by_issn in found.items():
        by_title = {}
        for issn, (title, active) in by_issn.items():  # the PDFs give print and online ISSNs on separate rows
            by_title.setdefault((title, active), []).append(issn)
        rows[list_id] = [_row(t, sorted(i), active=a, reason="" if a else "no longer on the list")
                         for (t, a), i in sorted(by_title.items())]
    print("  anvur: " + ", ".join(f"{k} {len(v):,} rows ({sum(r['active'] == 'true' for r in v):,} active)"
                                   for k, v in rows.items()))
    return rows


# --- kci ----------------------------------------------------------------------
# Korea Citation Index (KCI), National Research Foundation of Korea: journals
# with KCI accreditation. Three lists: kci-excellent (우수등재, the top tier),
# kci-registered (등재) and kci-candidate (등재후보). The NRF's open API needs a
# data.go.kr service key and the site's Excel export needs a login, so the
# public journal search is read instead: filter2 = the accreditation code
# (09 / 02 / 03), 100 journals a page, then each journal's page for its ISSN
# and eISSN. ~4.3K pages; cached in the temp dir so a rerun resumes.
KCI = "https://www.kci.go.kr/kciportal"
KCI_STATUS = {"09": "kci-excellent", "02": "kci-registered", "03": "kci-candidate"}
KCI_WORKERS = 6  # each journal page redirects to a citation page the server takes ~10 s to build


def _kci_post(url, data):
    from urllib.parse import urlencode
    for attempt in range(4):
        try:
            req = Request(url, data=urlencode(data).encode(), headers={"User-Agent": BROWSER_UA})
            with urlopen(req, timeout=90) as r:
                return r.read().decode("utf-8", "replace")
        except Exception as e:  # noqa: BLE001
            if attempt == 3:
                raise
            print(f"  kci: retry {attempt + 1} ({e})", file=sys.stderr)
            time.sleep(5 * (attempt + 1))


def fetch_kci():
    import tempfile
    from concurrent.futures import ThreadPoolExecutor
    cache = Path(tempfile.gettempdir()) / "kci-cache.jsonl"
    done = {}
    if cache.exists():
        for line in cache.read_text(encoding="utf-8").splitlines():
            rec = json.loads(line)
            done[rec["key"]] = rec["value"]
    lock = threading.Lock()

    def remember(key, value):
        with lock:
            done[key] = value
            with open(cache, "a", encoding="utf-8") as f:
                f.write(json.dumps({"key": key, "value": value}, ensure_ascii=False) + "\n")

    journals = {}  # sereId -> (insiId, list_id); a journal under two codes keeps the higher one (09 first)
    for code, list_id in KCI_STATUS.items():
        # One sorted page per code: paging the default (relevance) order returns a different mix on
        # each request, and the result rows repeat journals (one row per co-publisher), e.g. 4,035
        # rows = 2,691 journals for code 02 on 2026-10-09.
        h = _kci_post(f"{KCI}/po/search/poSereSearList.kci", {
            "poSearchBean.searType": "journal", "poSearchBean.filter2": code, "poSearchBean.docsCount": "10000",
            "poSearchBean.startPg": "1", "poSearchBean.resultForm": "Y",
            "poSearchBean.sortName": "INDE_TITL", "poSearchBean.sortDir": "asc"})
        stated = re.search(rf'id="resiDivCd{code}".*?\(<span>([\d,]+)</span>\)', h, re.S)
        found = re.findall(r"sereSearBean\.insiId=(\w+)&(?:amp;)?sereSearBean\.sereId=(\w+)", h)
        n_rows = len(re.findall(r'name="SERE_ID"', h))
        if stated and n_rows < int(stated.group(1).replace(",", "")):
            raise SystemExit(f"kci: {list_id}: {n_rows} rows of {stated.group(1)}; refusing a partial list")
        for insi, sere in dict.fromkeys(found):
            journals.setdefault(sere, (insi, list_id))
        print(f"  kci: {list_id}: {n_rows:,} rows, {len({s for _, s in found}):,} journals", flush=True)
        time.sleep(2)

    def detail(sere):
        if sere in done:
            return
        insi = journals[sere][0]
        try:
            h = _get(f"{KCI}/ci/seriesSearch/ciSereInfoView.kci?sereSearBean.insiId={insi}"
                     f"&sereSearBean.sereId={sere}", retries=4, ua=BROWSER_UA, timeout=60).decode("utf-8", "replace")
        except Exception as e:  # noqa: BLE001 -- a rerun resumes from the cache
            print(f"  kci: {sere} failed ({e})", file=sys.stderr)
            return
        text = re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"<script.*?</script>", "", h, flags=re.S))))
        p = re.search(r"\bISSN\s+(\d{4}-\d{3}[\dXx])", text)
        e = re.search(r"\beISSN\s+(\d{4}-\d{3}[\dXx])", text)
        t = re.search(r"<title>\s*([^<|]+)", h)
        latest = re.search(r"최근 발행정보\s+(\d{4})년", text)
        remember(sere, {"issns": _issns(p and p.group(1), e and e.group(1)),
                        "title": html.unescape(t.group(1)).strip() if t else "",
                        "latest_year": int(latest.group(1)) if latest else None})
        time.sleep(0.3)

    with ThreadPoolExecutor(KCI_WORKERS) as pool:
        for i, _ in enumerate(pool.map(detail, sorted(journals)), 1):
            if i % 500 == 0:
                print(f"  kci: {i:,}/{len(journals):,} journal pages", flush=True)
    missing = [s for s in journals if s not in done]
    if missing:
        raise SystemExit(f"kci: {len(missing)} journal pages failed; rerun to resume")
    rows, counts = {v: [] for v in KCI_STATUS.values()}, Counter()
    for sere, (insi, list_id) in sorted(journals.items()):
        d = done[sere]
        if not d["issns"]:
            counts["no_issn"] += 1
            continue
        rows[list_id].append(_row(d["title"], d["issns"]))
        counts[f"latest_issue_{'none' if not d['latest_year'] else ('<2024' if d['latest_year'] < 2024 else '2024+')}"] += 1
    print("  kci: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items()) + f"; {dict(counts)}")
    return rows


# --- ccf ----------------------------------------------------------------------
# China Computer Federation (CCF) list of recommended international journals,
# 7th edition (2026). Classes A (top), B, C. Journals only: the conference half
# has no ISSNs. The PDF names each journal with its DBLP page; DBLP ids are
# mapped to ISSNs with the dblp_id column of the RCSI White List file (a free
# DBLP-ISSN crosswalk), else by an exact title match against OpenAlex sources,
# else CCF_ISSNS below (checked by hand 2026-10-09). Needs pdfplumber
# (`uv run --with pdfplumber --with openpyxl python -m jobs.fetch_source_list ccf`).
CCF_PAGE = "https://www.ccf.org.cn/Academic_Evaluation/By_category/"
CCF_PDF = ("https://www.ccf.org.cn/ccf/contentcore/resource/download"
           "?ID=112CF3BF7E1140ACEB271ADAED12A67ADFABB8FF099E40C2759502A85C8A281F")
CCF_ISSNS = {  # title -> ISSNs, for journals neither crosswalk resolves (checked by hand 2026-10-09)
    # renamed Journal of Computer Languages in 2019; DBLP's `cl` stream covers both
    "Computer Languages, Systems and Structures": "1477-8424;1873-6866;2590-1184;2665-9182",
    "ACM Transactions in Quantum Computing": "2643-6809;2643-6817",
    "ACM Distributed Ledger Technologies: Research and Practice": "2769-6472;2769-6480",
    # renamed from Frontiers of Information Technology & Electronic Engineering (FITEE); CCF notes "原 FITEE"
    "ENGINEERING Information Technology & Electronic Engineering": "3069-8928;3069-8936;2095-9184;2095-9230",
}


def _openalex_title_issns(title):
    from urllib.parse import quote
    import os
    norm = lambda t: re.sub(r"[^a-z0-9]+", " ", t.lower().replace("&", "and")).strip().removeprefix("the ")  # noqa: E731
    key = os.environ.get("OPENALEX_ORG_API_KEY") or os.environ.get("OPENALEX_API_KEY") or ""
    url = (f"https://api.openalex.org/sources?search={quote(title)}&per_page=10"
           f"&select=display_name,issn,works_count&api_key={key}")
    res = json.loads(_get(url)).get("results") or []
    exact = [r for r in res if norm(r["display_name"]) == norm(title) and r.get("issn")]
    return max(exact, key=lambda r: r["works_count"])["issn"] if exact else []


def fetch_ccf():
    import pdfplumber
    csv.field_size_limit(10 ** 8)
    crosswalk = {}
    for r in csv.DictReader(io.StringIO(_get(RUSSIA_CSV, ua=BROWSER_UA).decode("utf-8-sig")), delimiter="\t"):
        if (r.get("dblp_id") or "").strip():
            crosswalk[r["dblp_id"].strip()] = _issns(r["issns"].replace("|", ";"))
    found = []
    with pdfplumber.open(io.BytesIO(_get(CCF_PDF, ua=BROWSER_UA))) as pdf:
        kind = cls = None
        for page in pdf.pages:
            text = page.extract_text() or ""
            marks = [("kind", "journal" if m.group(1) == "期刊" else "conference") if m.group(1) else ("class", m.group(3))
                     for m in re.finditer(r"推荐国际学术(期刊|会议)|([一二三])、\s*([ABC])\s*类", text)]
            tables, ti, seq = page.extract_tables(table_settings={"text_x_tolerance": 1.5}), 0, []
            for what, value in marks:  # a class heading starts the next table on the page
                if what == "kind":
                    kind = value
                else:
                    cls = value
                    if ti < len(tables):
                        seq.append((kind, cls, tables[ti]))
                        ti += 1
            seq += [(kind, cls, t) for t in tables[ti:]]
            for k, c, table in seq:
                for r in table:
                    if k != "journal" or not r or not re.match(r"^\s*\d+\s*$", str(r[0] or "")):
                        continue
                    cells = [re.sub(r"\s+", " ", (x or "").replace("\n", " ")).strip() for x in r]
                    m = re.search(r"/db/journals/([A-Za-z0-9_-]+)", " ".join(cells).replace(" ", ""))
                    title = re.sub(r"\s*（原.*?）\s*", " ", cells[2] if len(cells) > 2 else "").strip()
                    found.append((c, title, m.group(1) if m else None))
    rows, how = {f"ccf-{c.lower()}": [] for c in "ABC"}, Counter()
    for c, title, dblp in found:
        issns = crosswalk.get(dblp) if dblp else None
        if issns:
            how["dblp_crosswalk"] += 1
        elif title in CCF_ISSNS:
            issns = _issns(CCF_ISSNS[title])
            how["by_hand"] += 1
        else:
            issns = _openalex_title_issns(title)
            how["openalex_title" if issns else "unresolved"] += 1
            if issns:
                print(f"  ccf: by OpenAlex title: {c} {title!r} -> {';'.join(issns)}")
            if not issns:
                print(f"  ccf: unresolved {c} {title!r} (dblp {dblp})")
            time.sleep(0.2)
        if issns:
            rows[f"ccf-{c.lower()}"].append(_row(title, issns))
    if len(found) < 250:
        raise SystemExit(f"ccf: only {len(found)} journal rows parsed; layout changed?")
    print("  ccf: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items()) + f"; {dict(how)}")
    return rows


# --- fecyt-seal ---------------------------------------------------------------
# Spanish journals holding FECYT's quality seal (Sello de Calidad FECYT), the
# Spanish Foundation for Science and Technology's journal evaluation; a seal
# lasts about two years, and the listing shows current holders only. One list.
FECYT_PAGE = "https://calidadrevistas.fecyt.es/revistas-sello-fecyt?page={}"


def fetch_fecyt():
    rows, seen, n = [], set(), 0
    cell = lambda c: html.unescape(re.sub(r"<[^>]+>", " ", c)).strip()  # noqa: E731
    while True:
        h = _get(FECYT_PAGE.format(n), ua=BROWSER_UA).decode("utf-8", "replace")
        trs = re.findall(r"<tr[^>]*>(.*?)</tr>", h.split("<tbody>", 1)[-1], re.S) if "<tbody>" in h else []
        new = 0
        for tr in trs:
            tds = [cell(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
            if len(tds) < 3:
                continue
            issns = _issns(tds[1], tds[2])
            if not issns or issns[0] in seen:
                continue
            seen.add(issns[0])
            rows.append(_row(re.sub(r"\s+", " ", tds[0]), issns))
            new += 1
        if not new:
            break
        n += 1
        time.sleep(1)
    if len(rows) < 400:
        raise SystemExit(f"fecyt: only {len(rows)} journals; refusing a partial list")
    print(f"  fecyt-seal: {len(rows):,} journals from {n} pages")
    return rows


# --- nbra ---------------------------------------------------------------------
# Núcleo Básico de Revistas Científicas Argentinas (NBRA), CAICYT-CONICET: the
# core set of Argentine scientific journals (rolling three-year membership).
# One list. The members page lists every journal; each journal's page gives
# "ISSN NNNN-NNNX (Impresa / En línea)".
NBRA_PAGE = "http://www.caicyt-conicet.gov.ar/sitio/comunicacion-cientifica/nucleo-basico/revistas-integrantes/"


def fetch_nbra():
    page = _get_browser(NBRA_PAGE).decode("utf-8", "replace")
    stated = re.search(r"El NBRA contiene (\d+) revistas", page)
    seg = page[page.find("Por orden alfab"):]
    links = list(dict.fromkeys(re.findall(r'href="(https?://www\.caicyt-conicet\.gov\.ar/sitio/[a-z0-9-]+/)"', seg)))
    rows, skipped = [], []
    for i, url in enumerate(links, 1):
        h = _get_browser(url).decode("utf-8", "replace")
        issns = _issns(*re.findall(r"ISSN\s*(\d{4}-\d{3}[\dXx])", h))
        title = re.search(r"<h1[^>]*>(.*?)</h1>", h, re.S)
        title = html.unescape(re.sub(r"<[^>]+>", "", title.group(1))).strip() if title else url.rstrip("/").rsplit("/", 1)[-1]
        if issns:
            rows.append(_row(title, issns))
        else:
            skipped.append(url)
        if i % 100 == 0:
            print(f"  nbra: {i}/{len(links)} journal pages", flush=True)
        time.sleep(0.5)
    print(f"  nbra: {len(rows):,} journals (page says {stated and stated.group(1)}); no ISSN on {len(skipped)}: {skipped[:5]}")
    return rows


# --- publindex ----------------------------------------------------------------
# Publindex, Colombia's national journal index (Minciencias), call 977 of 2026,
# final results 28 Jul 2026. Categories A1 (top), A2, B, C, plus the new
# "Reconocida" (recognised: meets the editorial-quality bar and enters the
# national indexing system without a category). One list per category. The
# results are a PDF table (read with pdfplumber); ISSNs are printed without the
# hyphen. (The open dataset on datos.gov.co stops at the 2022 call.)
PUBLINDEX_PDF = "https://minciencias.gov.co/sites/default/files/listado_revistas_indexadas_y_reconocidas.pdf"
PUBLINDEX_CATS = {"A1": "publindex-a1", "A2": "publindex-a2", "B": "publindex-b", "C": "publindex-c",
                  "RECONOCIDA": "publindex-recognized"}


def fetch_publindex():
    import pdfplumber
    rows, seen = {v: [] for v in PUBLINDEX_CATS.values()}, set()
    with pdfplumber.open(io.BytesIO(_get(PUBLINDEX_PDF, ua=BROWSER_UA))) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                for r in table:
                    # No. | TÍTULO | ISSN impreso | ISSN electrónico | ISSN L | instituciones | CATEGORÍA | clasificado por
                    c = [re.sub(r"\s+", " ", (x or "").replace("\n", " ")).strip() for x in r]
                    if len(c) < 7 or not re.match(r"^\d+$", c[0]) or c[0] in seen:
                        continue
                    list_id = PUBLINDEX_CATS.get(c[6].upper())
                    issns = _issns(*(_issn8(x) for x in c[2:5]))
                    if list_id and issns:
                        seen.add(c[0])
                        rows[list_id].append(_row(c[1], issns))
    if len(seen) < 400:
        raise SystemExit(f"publindex: only {len(seen)} journals parsed; layout changed?")
    print("  publindex: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items()) + f"; {len(seen)} numbered rows")
    return rows


ADAPTERS = {
    "sinta": fetch_sinta,                      # grouped: sinta-s1 .. sinta-s6
    "tci": lambda today: fetch_tci(),          # grouped: tci-1, tci-2
    "ki-jl": lambda today: fetch_ki_jl(),      # grouped: ki-jl-1, ki-jl-2, ki-jl-3
    "abdc": lambda today: fetch_abdc(),        # grouped: abdc-a-star, abdc-a, abdc-b, abdc-c
    "medline": lambda today: fetch_medline(),
    "norway": fetch_norway,
    "jufo": lambda today: fetch_jufo(),
    "erih-plus": lambda today: fetch_erih_plus(),
    "scielo": lambda today: fetch_scielo(),
    "latindex": lambda today: fetch_latindex(),
    "jpps": lambda today: fetch_jpps(),        # grouped: jpps-1, jpps-2, jpps-3
    # batch 5 (oxjob #1615)
    "russia": lambda today: fetch_russia(),    # grouped: russia-white-list-1 .. -4
    "poland": lambda today: fetch_poland(),    # grouped: poland-20 .. poland-200
    "vabb-shw": fetch_vabb,
    "fnege": lambda today: fetch_fnege(),      # grouped: fnege-1-star, fnege-1 .. fnege-4
    "tr-dizin": fetch_trdizin,
    "dhet": lambda today: fetch_dhet(),
    "ft50": lambda today: fetch_ft50(),
    "utd24": lambda today: fetch_utd24(),
    "anvur": lambda today: fetch_anvur(),      # grouped: anvur-class-a, anvur-scientific
    "kci": lambda today: fetch_kci(),          # grouped: kci-excellent, kci-registered, kci-candidate
    "ccf": lambda today: fetch_ccf(),          # grouped: ccf-a, ccf-b, ccf-c
    "fecyt-seal": lambda today: fetch_fecyt(),
    "nbra": lambda today: fetch_nbra(),
    "publindex": lambda today: fetch_publindex(),  # grouped: publindex-a1 .. -c, publindex-recognized
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("list", choices=sorted(ADAPTERS))
    ap.add_argument("--date", help="edition date to stamp on the file name (default: today)")
    args = ap.parse_args()
    today = date.fromisoformat(args.date) if args.date else date.today()

    print(f"fetching {args.list} ...")
    result = ADAPTERS[args.list](today)
    grouped = result if isinstance(result, dict) else {args.list: result}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for list_id, rows in grouped.items():
        out = OUT_DIR / f"{list_id}-{today.isoformat()}.csv"
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["name", "issns", "active", "withdrawn_date", "withdrawal_reason"])
            w.writeheader()
            w.writerows(rows)
        n_active = sum(r["active"] == "true" for r in rows)
        n_issn = sum(len(r["issns"].split(";")) for r in rows)
        print(f"wrote {out} — {len(rows):,} rows ({n_active:,} active), {n_issn:,} ISSNs")
        print(f"next: python -m jobs.load_source_list --list {list_id} --csv {out.relative_to(OUT_DIR.parent.parent)} "
              f"--version {today.isoformat()} --dry-run")


if __name__ == "__main__":
    main()
