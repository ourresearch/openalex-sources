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
"""
import argparse
import calendar
import csv
import io
import json
import re
import sys
import time
import zipfile
from datetime import date
from pathlib import Path
from urllib.request import Request, urlopen

ISSN_RE = re.compile(r"^\d{4}-\d{3}[\dXx]$")
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "source_lists"
UA = "openalex-sources/fetch_source_list (support@openalex.org)"


def _get(url, retries=3):
    for attempt in range(retries):
        try:
            with urlopen(Request(url, headers={"User-Agent": UA}), timeout=300) as r:
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
    key = os.environ.get("OPENALEX_API_KEY")
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
    via_page = via_api = missing = 0
    for i, (title, list_id, url, iso2) in enumerate(entries, 1):
        issns = []
        try:
            page = _get_browser(url, retries=1, timeout=45).decode("utf-8", "replace")
            text = re.sub(r"<[^>]+>", " ", page)
            issns = _issns(*re.findall(r"ISSN[^0-9]{0,12}(\d{4}-\d{3}[\dXx])", text))
        except Exception:  # noqa: BLE001
            pass
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
        time.sleep(2)  # AJOL soft-blocks bursts with empty 200s (see _get_browser)
    print("  jpps: " + ", ".join(f"{k} {len(v):,}" for k, v in rows.items())
          + f"; ISSNs via platform page {via_page:,}, via OpenAlex {via_api:,}, missing {missing:,}")
    return rows


ADAPTERS = {
    "medline": lambda today: fetch_medline(),
    "norway": fetch_norway,
    "jufo": lambda today: fetch_jufo(),
    "erih-plus": lambda today: fetch_erih_plus(),
    "scielo": lambda today: fetch_scielo(),
    "latindex": lambda today: fetch_latindex(),
    "jpps": lambda today: fetch_jpps(),        # grouped: jpps-1, jpps-2, jpps-3
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
