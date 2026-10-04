"""Strip ISSN strings and marketing boilerplate from source display names.

OJS Beacon journal titles are whatever the journal typed into its OJS setup, so
#1426's mints carried names like "AI Tech International Journal, ISSN: 3079-4749",
"Ajasraa ISSN 2278-3741 UGC CARE 1" or "Universal Journal of Advanced Studies
P-ISSN -3051-0570 ,E-ISSN -3051-0589 Impact Factor: 6.8" (273 of 10,141 minted
names, audit 2026-10-04). `clean_source_name` removes:

  * ISSN clauses anywhere in the name: bare, prefixed (p-/e-/eISSN/E ISSN-/ISSN (Online)),
    bracketed, with a second bare number ("ISSN: 2306-7012 (Print), 2313-5700 (Online)"),
    "ISSN em fase de registro", XXXX-XXXX placeholders;
  * marketing tails after a separator: Impact Factor, UGC CARE, Scopus, Indexed in,
    SIF, Frequency:, Dates of Publication, Depósito Legal, ", International Peer
    reviewed Journal", parenthetical "(a Monthly, Open Access, Peer Reviewed ...)".

It never touches a bare "Peer Reviewed Journal" inside a title ("Neo Science Peer
Reviewed Journal" stays), never empties a name, and repairs a parenthesis it unbalanced.
`ojs_beacon.clean_title` calls it, so future mints are clean on the way in.

Usage:
  python -m jobs.clean_source_names --dry-run                       # preview (all sources)
  python -m jobs.clean_source_names --dry-run --since 2026-09-28    # only recently created
  python -m jobs.clean_source_names --apply --since 2026-09-28 --receipt data/ojs_beacon/renames-2026-10-04.csv

Only rows with override_timestamp IS NULL are written (curator overrides win, as in
sources_lib.enrich_journal). Receipt = id, old, new. oxjob #1426 follow-up (audit 2026-10-04).
"""
import argparse
import csv
import re
from datetime import date

from sqlalchemy import text

from db import engine

ISSN=r'(?:\d{4}\s?[-–]?\s?\d{3}[\dXx]|X{4}-X{4})'
QUAL=r'(?:\s*\(\s*(?:print|on-?line|onilne|impres[sa]o?|impresa|virtual|eletr[oô]nic[oa]|electr[oó]nic[oa]|en\s+l[ií]nea|p|e|o)\s*\)|\s+on-?line\b)?'
PFX=r'(?:\(\s*[peoPEO]\s*\)\s*|\b(?:p|e|o|print|online)\b[\.\s\-:]*|\b[peoPEO](?=issn|ISSN))?'
TOK=PFX + r'(?:issn-l|issn|essn)\b[^0-9A-Za-zЀ-ӿ؀-ۿ฀-๿]{0,3}(?:\(\s*(?:print|online|impres[so]o?|eletr[oô]nic[oa]|electr[oó]nic[oa]|p|e|o)\s*\)\s*:?\s*|(?:en\s+l[ií]nea|online|impresso?|eletr[oô]nico|electr[oó]nico|print|digital)\s*:?\s*)?[-:–]?\s*\(?' + ISSN + r'\)?' + QUAL
CONN=r'\s*(?:,|;|/|and|e|y|&|en|und|\|)?\s*'
# a second bare ISSN number with qualifier: "ISSN: 2306-7012 (Print), 2313-5700 (Online)"
BARE=r'(?:' + CONN + ISSN + QUAL + r')*'
RULES=[
 # "Purakala with ISSN 0971-2143 is an UGC CARE Journal"
 (r'\s+with\s+' + TOK + r'.*$', ''),
 # "RAUnP - ISSN 1984-4204 - Digital Object Identifier (DOI): http://..."
 (r'[\s,;:|\-–—]+digital\s+object\s+identifier.*$', ''),
 (r'<br\s*/?>', ' '),
 # parenthetical marketing blurbs
 (r'\s*\([^()]*(?:peer[\s\-]*reviewed|open\s+access)[^()]*journal[^()]*\)', ''),
 # bracketed block whose content is only ISSN tokens
 (r'\s*[\(\[]\s*(?:' + TOK + BARE + r'|issn\s+em\s+fase\s+de\s+registro)(?:' + CONN + TOK + BARE + r')*\s*[\)\]]', ''),
 # trailing ISSN clauses after a separator
 (r'[\s,;:|\-–—/]*' + TOK + BARE + r'(?:' + CONN + TOK + BARE + r')*\s*$', ''),
 # leading ISSN clause
 (r'^\s*' + TOK + BARE + r'[\s,;:|\-–—/]+', ''),
 # mid-string ISSN clause between separators
 (r'[\s,;:|\-–—/]+' + TOK + BARE + r'(?:' + CONN + TOK + BARE + r')*(?=[\s,;:|\-–—/]+\S)', ' '),
 # marketing / boilerplate tails introduced by a separator
 (r'[\s,;:|\-–—]+(?:an?\s+)?\b(?:impact\s*factor|ugc[\s\-]*care|frequency\s*:|dates?\s+of\s+publication|indexed\s+(?:in|by)|scopus|sif\s+\d|dep[oó]sito\s+legal|international\s+academic\s+journal$).*$', ''),
 (r'\s*[,;|]+\s*(?:an?\s+)?(?:international\s+)?(?:peer[\s\-]*reviewed|refereed)[^,;|]*journal.*$', ''),
 # pipe-separated junk segments anywhere
 (r'\s*\|\s*(?:frequency|dates?\s+of\s+publication|impact\s*factor)[^|]*', ''),
]

MANUAL = {"Academic Social Research:(P),(E)": "Academic Social Research",
          "Academic Social Research:(P)": "Academic Social Research"}
# legacy catalogue-record names that are junk for other reasons; not this job's to fix
SKIP_IDS = {4306552269, 4306526334}

FLAG = re.compile(r"\bissn\b|essn\b|impact factor|ugc care|scopus|indexed|frequency:|dep[oó]sito legal",
                  re.I)


def clean_source_name(name):
    """The display name without ISSN strings / marketing text; unchanged when nothing matches."""
    if not name:
        return name
    n = name
    for pat, rep in RULES:
        n = re.sub(pat, rep, n, flags=re.I)
    n = re.sub(r"\s+", " ", n).strip(" ,;:|-–—/").strip()
    n = re.sub(r"\(\s*\)|\[\s*\]", "", n).strip(" ,;:|-–—/").strip()
    if n.count("(") > n.count(")") and name.count("(") == name.count(")"):
        n = n[: n.rfind("(")].strip(" ,;:|-–—/").strip()
    if n != name and n.endswith(".") and not re.search(r"\b[A-Z]\.$", n):
        n = n[:-1].strip()
    n = MANUAL.get(n, n)
    if len(n) < 4:
        return name
    return n or name


def run(apply=False, since=None, receipt=None):
    where = "override_timestamp IS NULL"
    params = {}
    if since:
        where += " AND created_date >= :since"
        params["since"] = since
    with engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT id, display_name FROM sources WHERE {where} AND display_name ~* "
            "'issn|essn|impact factor|ugc care|scopus|indexed|frequency:|dep[oó]sito legal' "
            "ORDER BY id"), params).fetchall()
    changes = [(r.id, r.display_name, clean_source_name(r.display_name)) for r in rows if r.id not in SKIP_IDS]
    changes = [c for c in changes if c[2] != c[1]]
    print(f"{len(rows):,} candidate names, {len(changes):,} change; apply={apply}", flush=True)
    for sid, old, new in changes:
        print(f"  {sid}  {old!r}  ->  {new!r}")
    if apply and changes:
        with engine.begin() as conn:
            for sid, old, new in changes:
                conn.execute(text(
                    "UPDATE sources SET display_name = :new, updated_date = now() "
                    "WHERE id = :id AND display_name = :old AND override_timestamp IS NULL"),
                    {"new": new, "old": old, "id": sid})
        print(f"updated {len(changes):,} sources", flush=True)
    if receipt:
        with open(receipt, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["id", "old_display_name", "new_display_name", "applied"])
            for sid, old, new in changes:
                w.writerow([sid, old, new, "yes" if apply else "dry-run"])
        print(f"receipt -> {receipt}")
    return changes


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    ap.add_argument("--since", help="only sources created on/after this date (YYYY-MM-DD)")
    ap.add_argument("--receipt", help="CSV of id, old, new")
    a = ap.parse_args()
    run(apply=a.apply, since=a.since, receipt=a.receipt)


if __name__ == "__main__":
    main()
