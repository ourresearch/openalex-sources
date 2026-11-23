"""NDL's indexed-journal list -> ndl_journal, and the fields it fills on the journals tied to it (oxjob #2547).

The National Diet Library publishes the list of journals its periodical-article index covers (雑誌記事索引 採録誌一覧,
https://www.ndl.go.jp/data/sakuin/sakuin_index, sairokushi_YYYYMM.tsv, ~27K Japanese journals). Each run reloads it
whole, ties every journal that owns one of a record's ISSNs to that record (source_ndl_journal, matched_by 'issn'), and,
for every tied journal, fills only what is empty:
  - ISSNs and ISSN-L, when the journal has no ISSN, its tied records share an ISSN, and no other source holds any of them;
  - publisher text (NDL's current publisher), when it has no publisher text, publisher_id or institution_id;
  - country_code JP, when NULL and NDL's place of publication is in Japan.
Ties without an ISSN come from checked one-off binds (--bind-csv, e.g. data/ndl/oxjob2547-binds-2026-11-23.csv: 4,571
ISSN-less MAG-era journals matched on title, publisher and years, bar 196/197 against CiNii). Works embed a source's
ISSN and ISSN-L (an ISSN fill re-stamps its works) but not its publisher text or country.

    python -m jobs.ndl_journals --dry-run [--bind-csv <csv>]     # everything in one transaction, rolled back
    python -m jobs.ndl_journals [--bind-csv <csv>]               # prints a RECEIPT json line (keep it to revert)
    python -m jobs.ndl_journals --revert <receipt.json> [--dry-run]
"""
import argparse
import csv
import json
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parents[1]))
from db import engine  # noqa: E402

INDEX = "https://www.ndl.go.jp/data/sakuin/sakuin_index"
BASE = "https://www.ndl.go.jp"
UA = {"User-Agent": "OpenAlex-sources/1.0 (+https://openalex.org; support@openalex.org)"}
ISSN = re.compile(r"\d{4}-\d{3}[\dX]")
JA = re.compile(r"[\u3040-\u30ff\u4e00-\u9fff]")
FOREIGN_JA = {"北京", "上海", "台北", "香港", "ソウル", "京城", "大連", "新京", "奉天", "天津", "南京", "ニューヨーク",
              "ロンドン", "パリ", "ワシントン", "バンコク", "シンガポール", "ホノルル", "ロサンゼルス", "サンパウロ"}
JP_LATIN = {"tokyo", "osaka", "kyoto", "sendai", "sapporo", "wako", "toyokawa", "tsukuba", "yokohama", "nagoya", "kobe",
            "fukuoka", "hiroshima", "japan"}


class DryRun(Exception):
    pass


def clean_publisher(seg):
    """'東京 : ぎょうせい, [1958]-' segment -> 'ぎょうせい': no place, parallel name, years, cataloguer's brackets."""
    seg = re.split(r"\s:\s", seg, maxsplit=1)[-1]
    seg = re.split(r"\s*=\s*", seg)[0]
    seg = re.sub(r",?\s*\[?c?[\d?]{2}[\d?-]{2}.*$", "", seg)
    seg = re.sub(r"\s*\[?(ほか|他)\]?$", "", seg).strip(" []")
    return "" if not seg or re.match(r"^[\d?]", seg) else seg


def place_in_japan(publisher_raw):
    m = re.match(r"^\s*\[?([^:\]\[]+?)\]?\s:\s", publisher_raw or "")
    if not m:
        return False
    p = re.sub(r"\s*\(.*\)$", "", m.group(1)).strip()
    return (bool(JA.search(p)) and p not in FOREIGN_JA) or p.lower() in JP_LATIN


def parse(tsv, edition):
    out = []
    for line in tsv.splitlines():
        f = line.split("\t")
        if len(f) < 8 or not f[0].strip().isdigit():
            continue
        title = re.split(r"\s/\s", unicodedata.normalize("NFKC", f[5]).strip())[0].rstrip(". ")
        raw = unicodedata.normalize("NFKC", f[7]).strip().rstrip(".")
        segs = [s for s in re.split(r"\s;\s", raw) if s.strip()]
        pubs = [p for p in (clean_publisher(s) for s in segs) if p]
        years = [int(y) for y in re.findall(r"(1[89]\d\d|20\d\d)", raw)]
        out.append({"bibid": f[0].strip(), "status": f[1].strip() or None,
                    "issns": sorted(set(ISSN.findall(f[2].upper()))), "title": title,
                    "publisher": pubs[-1] if pubs else None, "publisher_raw": raw or None,
                    "place_in_japan": place_in_japan(raw), "first_year": min(years) if years else None,
                    "last_year": max(years) if years else None, "edition": edition})
    return out


def fetch():
    page = requests.get(INDEX, headers=UA, timeout=60)
    page.raise_for_status()
    href = re.search(r'href="(/file/data/sakuin/sakuin_index/(sairokushi_\d{6})\.tsv)"', page.text)
    assert href, "no sairokushi_YYYYMM.tsv link on the NDL index page"
    r = requests.get(BASE + href.group(1), headers=UA, timeout=300)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text, href.group(2)


def load(conn, rows):
    conn.execute(text("TRUNCATE ndl_journal"))
    raw = conn.connection.dbapi_connection.cursor()
    from psycopg2.extras import execute_values
    execute_values(raw, "INSERT INTO ndl_journal (bibid, status, issns, title, publisher, publisher_raw, place_in_japan, "
                        "first_year, last_year, edition) VALUES %s ON CONFLICT (bibid) DO NOTHING",
                   [(r["bibid"], r["status"], r["issns"], r["title"], r["publisher"], r["publisher_raw"],
                     r["place_in_japan"], r["first_year"], r["last_year"], r["edition"]) for r in rows], page_size=2000)


def bind(conn, path):
    rows = [(int(r["source_id"]), r["bibid"], r["matched_by"]) for r in csv.DictReader(open(path, encoding="utf-8"))]
    raw = conn.connection.dbapi_connection.cursor()
    from psycopg2.extras import execute_values
    execute_values(raw, "INSERT INTO source_ndl_journal (source_id, bibid, matched_by) SELECT v.s, v.b, v.m FROM "
                        "(VALUES %s) v(s, b, m) JOIN sources s ON s.id = v.s ON CONFLICT DO NOTHING", rows, page_size=2000)
    return len(rows)


def tie_by_issn(conn):
    return conn.execute(text("""
        INSERT INTO source_ndl_journal (source_id, bibid, matched_by)
        SELECT DISTINCT si.source_id, n.bibid, 'issn'
        FROM ndl_journal n JOIN source_issn si ON si.issn = ANY(n.issns) JOIN sources s ON s.id = si.source_id
        WHERE s.type = 'journal'
        ON CONFLICT DO NOTHING""")).rowcount


def fill(conn, receipt):
    rows = conn.execute(text("""
        SELECT s.id, s.publisher, s.publisher_id, s.institution_id, s.country_code,
               EXISTS (SELECT 1 FROM source_issn si WHERE si.source_id = s.id) AS has_issn,
               n.bibid, n.status, n.issns, n.publisher AS ndl_publisher, n.place_in_japan
        FROM source_ndl_journal t JOIN ndl_journal n ON n.bibid = t.bibid JOIN sources s ON s.id = t.source_id
        WHERE s.type = 'journal'""")).mappings().all()
    by = defaultdict(list)
    for r in rows:
        by[r["id"]].append(r)
    want_issns = {}
    for sid, recs in by.items():
        sets = [set(r["issns"]) for r in recs if r["issns"]]
        if not recs[0]["has_issn"] and sets and set.intersection(*sets):
            want_issns[sid] = sorted(set().union(*sets))
    claimed = defaultdict(set)
    for sid, issns in want_issns.items():
        for i in issns:
            claimed[i].add(sid)
    owned = {r[0] for r in conn.execute(text("SELECT issn FROM source_issn WHERE issn = ANY(:i)"),
                                        {"i": sorted(claimed)})}
    issnl = dict(conn.execute(text("SELECT issn, issn_l FROM issn_to_issnl WHERE issn = ANY(:i) AND issn_l IS NOT NULL"),
                              {"i": sorted(claimed)}).fetchall())
    issn_rows, issnl_rows = [], []
    for sid, issns in want_issns.items():
        if any(i in owned or len(claimed[i]) > 1 for i in issns):
            receipt["held"].append({"source_id": sid, "why": "issn on another source", "issns": issns})
            continue
        lead = next((issnl[i] for i in issns if issnl.get(i) in issns), issns[0])
        issn_rows += [(sid, i) for i in issns]
        issnl_rows.append((sid, lead))
        receipt["issns"].append({"source_id": sid, "issns": issns, "issn_l": lead})
    raw = conn.connection.dbapi_connection.cursor()
    from psycopg2.extras import execute_values
    if issn_rows:
        execute_values(raw, "INSERT INTO source_issn (source_id, issn, is_issn_l) SELECT v.s, v.i, false FROM "
                            "(VALUES %s) v(s, i)", issn_rows, page_size=2000)
        execute_values(raw, "UPDATE sources s SET issn_l = v.l, updated_date = now() FROM (VALUES %s) v(s, l) "
                            "WHERE s.id = v.s AND s.issn_l IS NULL", issnl_rows, page_size=2000)
        ids = [s for s, _ in issnl_rows]
        conn.execute(text("UPDATE source_issn si SET is_issn_l = (si.issn IS NOT DISTINCT FROM s.issn_l) FROM sources s "
                          "WHERE s.id = si.source_id AND si.source_id = ANY(:ids)"), {"ids": ids})
        conn.execute(text("UPDATE sources s SET issns = (SELECT array_agg(issn ORDER BY is_issn_l DESC, issn) "
                          "FROM source_issn WHERE source_id = s.id) WHERE s.id = ANY(:ids)"), {"ids": ids})
    pub_rows, jp = [], []
    for sid, recs in by.items():
        r0 = recs[0]
        best = max(recs, key=lambda r: (r["status"] == "c", r["bibid"]))
        if best["ndl_publisher"] and not (r0["publisher"] or "").strip() and r0["publisher_id"] is None \
                and r0["institution_id"] is None:
            pub_rows.append((sid, best["ndl_publisher"]))
        if r0["country_code"] is None and all(r["place_in_japan"] for r in recs):
            jp.append(sid)
    if pub_rows:
        execute_values(raw, "UPDATE sources s SET publisher = v.p, updated_date = now() FROM (VALUES %s) v(s, p) "
                            "WHERE s.id = v.s AND COALESCE(s.publisher, '') = '' AND s.publisher_id IS NULL "
                            "AND s.institution_id IS NULL", pub_rows, page_size=2000)
        receipt["publisher"] = [{"source_id": s, "publisher": p} for s, p in pub_rows]
    if jp:
        conn.execute(text("UPDATE sources SET country_code = 'JP', country = 'Japan', updated_date = now() "
                          "WHERE id = ANY(:ids) AND country_code IS NULL"), {"ids": jp})
        receipt["country"] = jp


def revert(conn, rec):
    for x in rec["issns"]:
        conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :s AND issn_l = :l"),
                     {"s": x["source_id"], "l": x["issn_l"]})
        conn.execute(text("DELETE FROM source_issn WHERE source_id = :s AND issn = ANY(:i)"),
                     {"s": x["source_id"], "i": x["issns"]})
        conn.execute(text("UPDATE sources s SET issns = (SELECT array_agg(issn ORDER BY is_issn_l DESC, issn) "
                          "FROM source_issn WHERE source_id = s.id) WHERE s.id = :s"), {"s": x["source_id"]})
    for x in rec["publisher"]:
        conn.execute(text("UPDATE sources SET publisher = NULL WHERE id = :s AND publisher = :p"),
                     {"s": x["source_id"], "p": x["publisher"]})
    conn.execute(text("UPDATE sources SET country_code = NULL, country = NULL WHERE id = ANY(:ids) AND country_code = 'JP'"),
                 {"ids": rec["country"]})
    print(f"reverted {len(rec['issns'])} ISSN fills, {len(rec['publisher'])} publishers, {len(rec['country'])} countries")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--bind-csv")
    ap.add_argument("--revert")
    a = ap.parse_args()
    receipt = {"job": "ndl_journals", "run_at": datetime.now(timezone.utc).isoformat(), "dry_run": a.dry_run,
               "issns": [], "publisher": [], "country": [], "held": []}
    try:
        with engine.begin() as conn:
            if a.revert:
                revert(conn, json.load(open(a.revert)))
            else:
                tsv, edition = fetch()
                rows = parse(tsv, edition)
                assert len(rows) > 20000, f"{edition}: only {len(rows)} rows parsed, refusing to replace ndl_journal"
                load(conn, rows)
                receipt["edition"], receipt["ndl_rows"] = edition, len(rows)
                if a.bind_csv:
                    receipt["bound"] = bind(conn, a.bind_csv)
                receipt["tied_by_issn"] = tie_by_issn(conn)
                fill(conn, receipt)
                print(f"{edition}: {len(rows)} NDL records; ties by ISSN +{receipt['tied_by_issn']}, bound "
                      f"{receipt.get('bound', 0)}; filled ISSNs {len(receipt['issns'])}, publisher "
                      f"{len(receipt['publisher'])}, country {len(receipt['country'])}; held {len(receipt['held'])}")
            if a.dry_run:
                raise DryRun()
    except DryRun:
        print("DRY RUN - rolled back")
    if not a.revert:
        print("RECEIPT " + json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
