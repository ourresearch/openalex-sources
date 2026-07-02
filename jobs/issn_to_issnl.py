"""Load the ISSN -> ISSN-L matching table from the ISSN International Centre.

  python -m jobs.issn_to_issnl [--url URL]

The ISSN IC publishes the complete table daily as a zip (~27MB, ~2.5M rows) at a
stable public URL. Each reload TRUNCATEs and bulk-COPYs `issn_to_issnl` in one
transaction, so readers never see a partial table. `sources_lib.resolve_issn_l`
consults this table when minting, falling back to first-ISSN when the map has
no entry.
"""
import argparse
import io
import zipfile

import requests

from db import engine

ISSNL_TABLES_URL = "https://www.issn.org/wp-content/uploads/2014/03/issnltables.zip"


def download(url):
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    names = [n for n in zf.namelist() if "ISSN-to-ISSN-L" in n]
    if not names:
        raise RuntimeError(f"no ISSN-to-ISSN-L file in zip (contents: {zf.namelist()})")
    print(f"downloaded {len(r.content):,} bytes; using {names[0]}", flush=True)
    return zf.open(names[0])


def to_copy_buffer(fileobj):
    """Parse the tab-separated file into a COPY-ready buffer of issn\tissn_l\tnote."""
    buf = io.StringIO()
    rows = 0
    for raw in io.TextIOWrapper(fileobj, encoding="utf-8", errors="replace"):
        parts = raw.rstrip("\n").split("\t")
        if len(parts) < 2:
            continue
        issn, issn_l = parts[0].strip().upper(), parts[1].strip().upper()
        if issn == "ISSN" or not issn or not issn_l:  # header / blanks
            continue
        note = parts[2].strip() if len(parts) > 2 and parts[2].strip() else "\\N"
        buf.write(f"{issn}\t{issn_l}\t{note}\n")
        rows += 1
    buf.seek(0)
    return buf, rows


def load(buf, rows):
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE issn_to_issnl")
            cur.copy_expert(
                "COPY issn_to_issnl (issn, issn_l, note) FROM STDIN", buf
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    print(f"loaded {rows:,} issn -> issn_l rows (DONE)", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=ISSNL_TABLES_URL)
    args = ap.parse_args()
    buf, rows = to_copy_buffer(download(args.url))
    load(buf, rows)


if __name__ == "__main__":
    main()
