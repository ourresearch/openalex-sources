"""Fetch an external source list from its maintainer and write the CSV that
jobs.load_source_list expects (oxjob #1205, phase 4).

One adapter per list. Each downloads the maintainer's current machine-readable
file, keeps journals/series with an ISSN, decides `active`, and writes
data/source_lists/<list>-<YYYY-MM-DD>.csv with the loader's columns
(name,issns,active,withdrawn_date,withdrawal_reason). Nothing here touches the
database: the load itself stays a separate, by-hand step (see README).

  python -m jobs.fetch_source_list medline            # writes the CSV, prints stats
  python -m jobs.fetch_source_list norway --date 2026-09-18

Allow lists only (never deny lists). Level/tier registers (norway, jufo) are
loaded as ONE list of every approved channel (level >= 1); the level itself is
not part of the API value because levels are re-set every year.
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
    rows, skipped = [], 0
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
        rows.append(_row(r.get("Internasjonal tittel") or r.get("Original tittel"), issns,
                         active, f"{ended}-01-01" if (not active and ended.isdigit()) else "",
                         "" if active else f"inactive in the register (Nedlagt år {ended or '?'}; year only)"))
    print(f"  norway: kept {len(rows):,}, skipped {skipped:,} rows with level 0 / blank")
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
    rows, skipped = [], 0
    for d in data:
        if not (d.get("Type") or "").lower().startswith("lehti/"):
            continue
        if (d.get("Level") or "").strip() not in ("1", "2", "3"):
            skipped += 1
            continue
        issns = _issns(d.get("ISSN1"), d.get("ISSN2"), d.get("ISSNL"))
        if not issns:
            continue
        active = (d.get("Active") or "").strip().lower() == "active"
        end = (d.get("Year_End") or "").strip()
        rows.append(_row(d.get("Name"), issns, active,
                         f"{end}-01-01" if (not active and end.isdigit()) else "",
                         "" if active else f"inactive in JUFO (Year_End {end or '?'}; year only)"))
    print(f"  jufo: kept {len(rows):,}, skipped {skipped:,} journal rows with level 0 / not evaluated")
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


ADAPTERS = {
    "medline": lambda today: fetch_medline(),
    "norway": fetch_norway,
    "jufo": lambda today: fetch_jufo(),
    "erih-plus": lambda today: fetch_erih_plus(),
    "scielo": lambda today: fetch_scielo(),
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("list", choices=sorted(ADAPTERS))
    ap.add_argument("--date", help="edition date to stamp on the file name (default: today)")
    args = ap.parse_args()
    today = date.fromisoformat(args.date) if args.date else date.today()

    print(f"fetching {args.list} ...")
    rows = ADAPTERS[args.list](today)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"{args.list}-{today.isoformat()}.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["name", "issns", "active", "withdrawn_date", "withdrawal_reason"])
        w.writeheader()
        w.writerows(rows)
    n_active = sum(r["active"] == "true" for r in rows)
    n_issn = sum(len(r["issns"].split(";")) for r in rows)
    print(f"wrote {out} — {len(rows):,} rows ({n_active:,} active), {n_issn:,} ISSNs")
    print(f"next: python -m jobs.load_source_list --list {args.list} --csv {out.relative_to(OUT_DIR.parent.parent)} "
          f"--version {today.isoformat()} --dry-run")


if __name__ == "__main__":
    main()
