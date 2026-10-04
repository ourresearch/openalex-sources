"""OJS journals from the PKP Beacon: sources for the active OJS journals the
registry lacks (oxjob #1426).

The Beacon (Harvard Dataverse doi:10.7910/DVN/OCZNVY, CC0) is the telemetry every
Open Journal Systems install reports: one row per journal ("context") with its
ISSNs, OAI endpoint, record counts and country. Tens of thousands of these
journals have no DOIs and no DOAJ listing, so the Crossref / DOAJ / DataCite
syncs never mint them, and no article can be credited to them.

Two by-hand steps, like fetch_source_list -> load_source_list:

  candidates  read the Beacon, keep ACTIVE journals (responsive, records in the
              current year, has an ISSN) whose ISSNs no source owns, assign each a
              quality tier, and write data/ojs_beacon/candidates-<edition>.csv.
              Reads the DB only. Tier B's title check fetches each candidate's
              ISSN portal record (cached in data/ojs_beacon/issn_portal_titles.json).
  mint        read a candidates CSV and, for the requested tiers, run the shared
              match cascade (sources_lib): ISSN / ISSN-L match -> enrich (attach
              ISSNs, fill a missing homepage / country; never rename); no match ->
              mint a journal with the Beacon's title, ISSNs, homepage and country.
              --dry-run writes nothing and reports the same diff. Every run writes
              a receipt CSV (one row per candidate: outcome + source id).

Quality bar (Casey, 2026-09-29, oxjob #1426):
  A  in DOAJ, or on any source_list other than 'ojs' (the Beacon itself) and the
     national accreditation lists below.
  B  ISSN registered with the ISSN network (issn_to_issnl); the ISSN portal's title
     matches the Beacon title; first record <= 2023; >= 100 records; records this
     year. A journal on a national accreditation list (SINTA, TCI) with a registered
     ISSN qualifies for B whatever its size or age.
  C  the rest: held (not minted) until A and B are verified and the bar revisited.
Feed health is deliberately not part of the bar: it decides whether a journal's
OAI feed gets registered (oxjob #1417), not whether the journal exists.

Name matches. Many OJS journals share a title with another journal (Indonesian
university journals especially: "Jurnal Pendidikan Matematika" exists at dozens
of universities, each with its own ISSN). ISSNs are the identity here, so when
the ISSNs match nothing but the title matches existing journal sources:
  the only namesake, which has no ISSNs and no contradicting homepage host or
  country                                          -> link: our ISSNs, homepage and
                                                      country go onto it (enrich)
  a namesake with no ISSNs, but ambiguous or contradicted -> held for review
  a same-host namesake with OTHER ISSNs (a broken source) -> held for review
  namesakes that all have other ISSNs              -> mint; receipt says 'name_twin'

  python -m jobs.ojs_beacon candidates [--beacon PATH_OR_URL] [--no-portal]
  python -m jobs.ojs_beacon mint --csv data/ojs_beacon/candidates-2026-07-18.csv \
      --tiers A,B [--dry-run] [--limit N] [--receipt PATH]
"""
import argparse
import csv
import html
import io
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse

import requests
from sqlalchemy import text

from db import engine
from jobs.clean_source_names import clean_source_name
from sources_lib import (
    MatchContext,
    enrich_journal,
    match_source,
    mint_source,
    normalize_name,
    park_multi_match,
    recompute_is_ojs,
    recompute_listed_in,
)

DATAVERSE_DATASET = ("https://dataverse.harvard.edu/api/datasets/:persistentId/"
                     "?persistentId=doi:10.7910/DVN/OCZNVY")
DATAVERSE_FILE = "https://dataverse.harvard.edu/api/access/datafile/{id}?format=original"
ISSN_PORTAL = "https://portal.issn.org/resource/ISSN/{issn}"
PORTAL_DELAY = 1.0  # portal.issn.org robots.txt: Crawl-delay: 1
UA = "openalex-sources/ojs_beacon (support@openalex.org)"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "ojs_beacon"
PORTAL_CACHE = OUT_DIR / "issn_portal_titles.json"

ACTIVE_YEAR_COLUMN = "record_count_2025"  # the Beacon edition's "this year" count
B_MIN_RECORDS = 100
B_MAX_FIRST_YEAR = 2023  # >= 3 years of records
TITLE_MIN_SCORE = 0.8
NATIONAL_LIST_PREFIXES = ("sinta-", "tci-")  # accreditation lists that promote to B
NOT_A_QUALITY_LIST = ("ojs",)  # the Beacon itself (oxjob #1424)
SOURCE_FEED = "ojs_beacon"

ISSN_RE = re.compile(r"^\d{4}-\d{3}[\dX]$")

# Beacon country name -> (ISO 3166-1 alpha-2, ISO short name). Covers every name in
# the 2026-07-18 edition; an unknown name leaves the country empty (and is counted).
BEACON_COUNTRY = {
    'Afghanistan': ('AF', 'Afghanistan'),
    'Albania': ('AL', 'Albania'),
    'Algeria': ('DZ', 'Algeria'),
    'Angola': ('AO', 'Angola'),
    'Anguilla (United Kingdom)': ('AI', 'Anguilla'),
    'Argentina': ('AR', 'Argentina'),
    'Armenia': ('AM', 'Armenia'),
    'Australia': ('AU', 'Australia'),
    'Austria': ('AT', 'Austria'),
    'Azerbaijan': ('AZ', 'Azerbaijan'),
    'Bahrain': ('BH', 'Bahrain'),
    'Bangladesh': ('BD', 'Bangladesh'),
    'Barbados': ('BB', 'Barbados'),
    'Belarus': ('BY', 'Belarus'),
    'Belgium': ('BE', 'Belgium'),
    'Belize': ('BZ', 'Belize'),
    'Benin': ('BJ', 'Benin'),
    'Bhutan': ('BT', 'Bhutan'),
    'Bolivia': ('BO', 'Bolivia, Plurinational State of'),
    'Bosnia and Herzegovina': ('BA', 'Bosnia and Herzegovina'),
    'Botswana': ('BW', 'Botswana'),
    'Brazil': ('BR', 'Brazil'),
    'Brunei': ('BN', 'Brunei Darussalam'),
    'Bulgaria': ('BG', 'Bulgaria'),
    'Burkina Faso': ('BF', 'Burkina Faso'),
    'Burundi': ('BI', 'Burundi'),
    'Cambodia': ('KH', 'Cambodia'),
    'Cameroon': ('CM', 'Cameroon'),
    'Canada': ('CA', 'Canada'),
    'Cape Verde': ('CV', 'Cabo Verde'),
    'Chile': ('CL', 'Chile'),
    'China': ('CN', 'China'),
    'Colombia': ('CO', 'Colombia'),
    'Comoros': ('KM', 'Comoros'),
    'Costa Rica': ('CR', 'Costa Rica'),
    'Croatia': ('HR', 'Croatia'),
    'Cuba': ('CU', 'Cuba'),
    'Cyprus': ('CY', 'Cyprus'),
    'Czech Republic': ('CZ', 'Czechia'),
    'Democratic Republic of the Congo': ('CD', 'Congo, The Democratic Republic of the'),
    'Denmark': ('DK', 'Denmark'),
    'Dominican Republic': ('DO', 'Dominican Republic'),
    'East Timor': ('TL', 'Timor-Leste'),
    'Ecuador': ('EC', 'Ecuador'),
    'Egypt': ('EG', 'Egypt'),
    'El Salvador': ('SV', 'El Salvador'),
    'Estonia': ('EE', 'Estonia'),
    'Eswatini': ('SZ', 'Eswatini'),
    'Ethiopia': ('ET', 'Ethiopia'),
    'Fiji': ('FJ', 'Fiji'),
    'Finland': ('FI', 'Finland'),
    'France': ('FR', 'France'),
    'Germany': ('DE', 'Germany'),
    'Ghana': ('GH', 'Ghana'),
    'Greece': ('GR', 'Greece'),
    'Guatemala': ('GT', 'Guatemala'),
    'Guinea': ('GN', 'Guinea'),
    'Guyana': ('GY', 'Guyana'),
    'Honduras': ('HN', 'Honduras'),
    'Hong Kong': ('HK', 'Hong Kong'),
    'Hungary': ('HU', 'Hungary'),
    'Iceland': ('IS', 'Iceland'),
    'India': ('IN', 'India'),
    'Indonesia': ('ID', 'Indonesia'),
    'Iran': ('IR', 'Iran, Islamic Republic of'),
    'Iraq': ('IQ', 'Iraq'),
    'Ireland': ('IE', 'Ireland'),
    'Isle of Man (United Kingdom)': ('IM', 'Isle of Man'),
    'Israel': ('IL', 'Israel'),
    'Italy': ('IT', 'Italy'),
    'Ivory Coast': ('CI', "Côte d'Ivoire"),
    'Japan': ('JP', 'Japan'),
    'Jordan': ('JO', 'Jordan'),
    'Kazakhstan': ('KZ', 'Kazakhstan'),
    'Kenya': ('KE', 'Kenya'),
    'Kuwait': ('KW', 'Kuwait'),
    'Kyrgyzstan': ('KG', 'Kyrgyzstan'),
    'Laos': ('LA', "Lao People's Democratic Republic"),
    'Latvia': ('LV', 'Latvia'),
    'Lebanon': ('LB', 'Lebanon'),
    'Libya': ('LY', 'Libya'),
    'Lithuania': ('LT', 'Lithuania'),
    'Luxembourg': ('LU', 'Luxembourg'),
    'Macau': ('MO', 'Macao'),
    'Madagascar': ('MG', 'Madagascar'),
    'Malawi': ('MW', 'Malawi'),
    'Malaysia': ('MY', 'Malaysia'),
    'Maldives': ('MV', 'Maldives'),
    'Mali': ('ML', 'Mali'),
    'Malta': ('MT', 'Malta'),
    'Marshall Islands': ('MH', 'Marshall Islands'),
    'Mauritius': ('MU', 'Mauritius'),
    'Mexico': ('MX', 'Mexico'),
    'Moldova': ('MD', 'Moldova, Republic of'),
    'Monaco': ('MC', 'Monaco'),
    'Mongolia': ('MN', 'Mongolia'),
    'Montenegro': ('ME', 'Montenegro'),
    'Morocco': ('MA', 'Morocco'),
    'Mozambique': ('MZ', 'Mozambique'),
    'Myanmar': ('MM', 'Myanmar'),
    'Nepal': ('NP', 'Nepal'),
    'Netherlands': ('NL', 'Netherlands'),
    'New Zealand': ('NZ', 'New Zealand'),
    'Nicaragua': ('NI', 'Nicaragua'),
    'Nigeria': ('NG', 'Nigeria'),
    'North Macedonia': ('MK', 'North Macedonia'),
    'Norway': ('NO', 'Norway'),
    'Oman': ('OM', 'Oman'),
    'Pakistan': ('PK', 'Pakistan'),
    'Palau': ('PW', 'Palau'),
    'Palestine': ('PS', 'Palestine, State of'),
    'Panama': ('PA', 'Panama'),
    'Papua New Guinea': ('PG', 'Papua New Guinea'),
    'Paraguay': ('PY', 'Paraguay'),
    'Peru': ('PE', 'Peru'),
    'Philippines': ('PH', 'Philippines'),
    'Poland': ('PL', 'Poland'),
    'Portugal': ('PT', 'Portugal'),
    'Qatar': ('QA', 'Qatar'),
    'Republic of the Congo': ('CG', 'Congo'),
    'Romania': ('RO', 'Romania'),
    'Russia': ('RU', 'Russian Federation'),
    'Rwanda': ('RW', 'Rwanda'),
    'Réunion (France)': ('RE', 'Réunion'),
    'Samoa': ('WS', 'Samoa'),
    'Saudi Arabia': ('SA', 'Saudi Arabia'),
    'Senegal': ('SN', 'Senegal'),
    'Serbia': ('RS', 'Serbia'),
    'Sierra Leone': ('SL', 'Sierra Leone'),
    'Singapore': ('SG', 'Singapore'),
    'Slovakia': ('SK', 'Slovakia'),
    'Slovenia': ('SI', 'Slovenia'),
    'Somalia': ('SO', 'Somalia'),
    'South Africa': ('ZA', 'South Africa'),
    'South Korea': ('KR', 'Korea, Republic of'),
    'Spain': ('ES', 'Spain'),
    'Sri Lanka': ('LK', 'Sri Lanka'),
    'Sudan': ('SD', 'Sudan'),
    'Sweden': ('SE', 'Sweden'),
    'Switzerland': ('CH', 'Switzerland'),
    'Syria': ('SY', 'Syrian Arab Republic'),
    'São Tomé and Príncipe': ('ST', 'Sao Tome and Principe'),
    'Taiwan': ('TW', 'Taiwan, Province of China'),
    'Tanzania': ('TZ', 'Tanzania, United Republic of'),
    'Thailand': ('TH', 'Thailand'),
    'The Gambia': ('GM', 'Gambia'),
    'Togo': ('TG', 'Togo'),
    'Tokelau': ('TK', 'Tokelau'),
    'Trinidad and Tobago': ('TT', 'Trinidad and Tobago'),
    'Tunisia': ('TN', 'Tunisia'),
    'Turkey': ('TR', 'Türkiye'),
    'Uganda': ('UG', 'Uganda'),
    'Ukraine': ('UA', 'Ukraine'),
    'United Arab Emirates': ('AE', 'United Arab Emirates'),
    'United Kingdom': ('GB', 'United Kingdom'),
    'United States': ('US', 'United States'),
    'Uruguay': ('UY', 'Uruguay'),
    'Uzbekistan': ('UZ', 'Uzbekistan'),
    'Vanuatu': ('VU', 'Vanuatu'),
    'Venezuela': ('VE', 'Venezuela, Bolivarian Republic of'),
    'Vietnam': ('VN', 'Viet Nam'),
    'Yemen': ('YE', 'Yemen'),
    'Zambia': ('ZM', 'Zambia'),
    'Zimbabwe': ('ZW', 'Zimbabwe'),
}

CANDIDATE_COLUMNS = [
    "tier", "tier_reason", "context_name", "issns", "homepage_url", "country_code",
    "country", "oai_url", "set_spec", "total_record_count", "active_year_records",
    "first_year", "registered", "in_doaj", "lists", "portal_title", "title_score", "title_check",
    "n_contexts",
]


# --- Beacon -----------------------------------------------------------------

def valid_issn(issn):
    """Format + ISSN check digit (mod 11)."""
    if not ISSN_RE.match(issn):
        return False
    digits = issn.replace("-", "")
    total = sum(int(d) * w for d, w in zip(digits[:7], range(8, 1, -1)))
    check = (11 - total % 11) % 11
    return digits[7] == ("X" if check == 10 else str(check))


def beacon_issns(raw):
    out = []
    for v in re.split(r"[\s;,]+", (raw or "").upper()):
        if valid_issn(v) and v not in out:
            out.append(v)
    return out


def homepage_from_oai(oai_url, set_spec):
    """Journal homepage from the Beacon's OAI URL + context path.

    OJS routes every journal under <base>/<path>: install-wide endpoints are
    <base>/index/oai (optionally with a locale segment), per-journal ones
    <base>/<path>/oai, OJS 2 query-style ones <base>/index.php?journal=...&page=oai.
    """
    oai_url = (oai_url or "").strip()
    path = (set_spec or "").strip()
    if not oai_url or not path or path in ("NA", "index"):
        return None
    m = re.match(r"^(https?://.+?/index\.php)\?journal=[^&]*&page=oai", oai_url, re.I)
    if m:
        return f"{m.group(1)}/{path}"
    m = re.match(r"^(https?://.+?)/[^/?#]+/(?:[a-z]{2}(?:_[A-Za-z]{2})?/)?oai/?$", oai_url)
    if m:
        return f"{m.group(1)}/{path}"
    return None


def clean_title(t):
    t = html.unescape(t or "")
    t = re.sub(r"\s+", " ", t).strip().strip('"').strip()
    t = clean_source_name(t)  # no ISSN strings / marketing text in a source name (audit 2026-10-04)
    return t or None


def download_beacon():
    meta = requests.get(DATAVERSE_DATASET, timeout=60).json()["data"]["latestVersion"]
    fid = next(f["dataFile"]["id"] for f in meta["files"]
               if f["dataFile"].get("filename") == "beacon.tab")
    edition = (meta.get("releaseTime") or "")[:10]
    r = requests.get(DATAVERSE_FILE.format(id=fid), timeout=600)
    r.raise_for_status()
    print(f"downloaded Beacon v{meta.get('versionNumber')} ({edition}), {len(r.content):,} bytes",
          flush=True)
    return r.content.decode("utf-8"), edition


def read_beacon(source):
    """Rows of the Beacon CSV (the Dataverse 'original' of beacon.tab)."""
    edition = None
    if source is None or re.match(r"^https?://", source or ""):
        body, edition = download_beacon()
    else:
        body = Path(source).read_text(encoding="utf-8")
    return list(csv.DictReader(io.StringIO(body))), edition


def active_journals(rows):
    """ACTIVE = responsive endpoint and context, records this year, a valid ISSN
    (the census #1404 'core' definition). Contexts sharing an ISSN (one journal
    reported by two installs, e.g. after a move) collapse into one journal: the
    union of their ISSNs, described by the context with the most records."""
    ctxs = []
    for r in rows:
        issns = beacon_issns(r.get("issn"))
        if not issns:
            continue
        if r.get("unresponsive_endpoint") != "0" or r.get("unresponsive_context") != "0":
            continue
        if int(r.get(ACTIVE_YEAR_COLUMN) or 0) <= 0:
            continue
        ctxs.append((issns, r))

    parent = list(range(len(ctxs)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    first_seen = {}
    for i, (issns, _) in enumerate(ctxs):
        for issn in issns:
            if issn in first_seen:
                parent[find(i)] = find(first_seen[issn])
            else:
                first_seen[issn] = i
    groups = defaultdict(list)
    for i in range(len(ctxs)):
        groups[find(i)].append(i)

    journals = []
    for members in groups.values():
        members.sort(key=lambda i: -int(ctxs[i][1].get("total_record_count") or 0))
        issns = []
        for i in members:
            issns += [x for x in ctxs[i][0] if x not in issns]
        best = ctxs[members[0]][1]
        years = [ctxs[i][1].get("earliest_datestamp") or "" for i in members]
        years = [int(y[:4]) for y in years if re.match(r"^\d{4}", y)]
        cc, country = BEACON_COUNTRY.get(best.get("country") or "", (None, None))
        journals.append({
            "context_name": clean_title(best.get("context_name")),
            "issns": issns,
            "homepage_url": homepage_from_oai(best.get("oai_url"), best.get("set_spec")),
            "country_code": cc,
            "country": country,
            "beacon_country": best.get("country"),
            "oai_url": best.get("oai_url"),
            "set_spec": best.get("set_spec"),
            "total_record_count": int(best.get("total_record_count") or 0),
            "active_year_records": int(best.get(ACTIVE_YEAR_COLUMN) or 0),
            "first_year": min(years) if years else None,
            "n_contexts": len(members),
        })
    return journals


# --- ISSN portal title check --------------------------------------------------

# words that name the genre, not the journal: they carry no evidence either way
TITLE_FILLER = set("""
journal jurnal revista revue rivista zeitschrift zhurnal warasan magazine bulletin
international internasional nasional national online print of the and for in on
de del la le les da do das dos di dan e y et und an a
""".split())


def _title_tokens(t):
    t = re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", t or "")  # portal qualifiers: (Online), (Padang)
    return [w for w in normalize_name(t).split() if w not in TITLE_FILLER]


def _mostly_latin(t):
    letters = [c for c in (t or "") if c.isalpha()]
    return bool(letters) and sum(c < "\u0250" for c in letters) / len(letters) >= 0.5


def _token_hit(w, others):
    # equal, or an abbreviation of at least 3 letters ("rev" / "revista", "hist" / "historia")
    return any(w == o or (min(len(w), len(o)) >= 3 and (o.startswith(w) or w.startswith(o)))
               for o in others)


def _title_variants(t):
    """The title plus its parts: bilingual Beacon titles put the second language
    in parentheses or after ' = ' ('<Thai title> (Journal of ...)')."""
    parts = [t] + re.findall(r"\(([^)]{8,})\)", t or "") + re.split(r"\s+=\s+|\s+/\s+", t or "")
    return [p.strip() for p in dict.fromkeys(parts) if p and p.strip()]


def title_score(beacon, portal):
    """Best _title_score over the Beacon title's variants (None: no variant is in
    the portal title's script)."""
    scores = [_title_score(v, portal) for v in _title_variants(beacon)]
    scores = [x for x in scores if x is not None]
    return max(scores) if scores else None


def _title_score(beacon, portal):
    """How well the ISSN portal's key title supports the Beacon title, in [0, 1];
    None when the two are in different scripts (a transliterated key title can't
    be compared by string). The key title is often a short form of the journal's
    name ('Puca' for 'Puca: Revista de Comunicacao ...', 'Jupiter (Palembang)'),
    so the score is the better of the whole-string ratio and the share of the
    shorter title's distinctive words found in the other."""
    if _mostly_latin(beacon) != _mostly_latin(portal):
        return None
    # the whole key title opens the Beacon title ('XY' / 'XY. Rassegna critica ...'),
    # or one title is the other's acronym ('sdjyqy' / 'Shi dai jiao yu qian yan')
    fa, fb = (normalize_name(re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", t)) for t in (beacon, portal))
    if fa and fb and (fa.startswith(fb + " ") or fb.startswith(fa + " ") or fa == fb):
        return 1.0
    for one, other in ((fa, fb), (fb, fa)):
        words = other.split()
        if " " not in one and len(one) >= 3 and len(words) >= 3 and one == "".join(w[0] for w in words):
            return 1.0
    ta, tb = _title_tokens(beacon), _title_tokens(portal)
    if not ta or not tb:
        return 0.0
    ratio = SequenceMatcher(None, " ".join(ta), " ".join(tb)).ratio()
    short, long_ = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    contain = sum(_token_hit(w, long_) for w in short) / len(short)
    if len(short) == 1 and len(short[0]) < 4:
        contain = 0.0  # one short word ('ijo') is no evidence
    return round(max(ratio, contain), 3)


def portal_title(issn, session):
    """Key title from the ISSN portal record page (<title>ISSN X - Title</title>),
    '' when the portal has no record, None on a fetch failure."""
    for attempt in range(3):
        try:
            r = session.get(ISSN_PORTAL.format(issn=issn), timeout=30)
            if r.status_code == 404:
                return ""
            r.raise_for_status()
            m = re.search(r"<title>\s*ISSN\s+[\dX-]+\s*-\s*(.*?)\s*</title>", r.text, re.S | re.I)
            return clean_title(m.group(1)) if m else ""
        except requests.RequestException:
            time.sleep(5 * (attempt + 1))
    return None


def fill_portal_titles(journals, cache_path=PORTAL_CACHE):
    cache = {}
    if cache_path.exists():
        cache = json.loads(cache_path.read_text(encoding="utf-8"))
    todo = sorted({i for j in journals for i in j["issns"] if i not in cache})
    print(f"ISSN portal: {len(todo)} ISSNs to fetch ({len(cache)} cached)", flush=True)
    session = requests.Session()
    session.headers["User-Agent"] = UA
    for n, issn in enumerate(todo, 1):
        t = portal_title(issn, session)
        if t is not None:
            cache[issn] = t
        if n % 100 == 0 or n == len(todo):
            cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=0, sort_keys=True),
                                  encoding="utf-8")
            print(f"  portal {n}/{len(todo)}", flush=True)
        time.sleep(PORTAL_DELAY)
    return cache


# --- tiers ------------------------------------------------------------------

def registry_facts(conn, issns):
    """Owned ISSNs, registered ISSNs, DOAJ ISSNs and list memberships for these ISSNs."""
    issns = sorted(issns)
    owned = {r[0] for r in conn.execute(text(
        "SELECT issn FROM source_issn WHERE issn = ANY(:i)"), {"i": issns})}
    registered = {r[0] for r in conn.execute(text(
        "SELECT issn FROM issn_to_issnl WHERE issn = ANY(:i)"), {"i": issns})}
    doaj = {r[0] for r in conn.execute(text(
        "SELECT DISTINCT di.issn FROM doaj_journal d CROSS JOIN LATERAL unnest(d.issns) AS di(issn) "
        "WHERE di.issn = ANY(:i)"), {"i": issns})}
    lists = defaultdict(set)
    for issn, list_id in conn.execute(text(
            "SELECT issn, list_id FROM source_list_member "
            "WHERE active AND issn = ANY(:i) AND list_id <> ALL(:skip)"),
            {"i": issns, "skip": list(NOT_A_QUALITY_LIST)}):
        lists[issn].add(list_id)
    return owned, registered, doaj, lists


def assign_tier(j):
    """(tier, reason) under the 2026-09-29 bar; see the module docstring."""
    quality_lists = [x for x in j["lists"] if not x.startswith(NATIONAL_LIST_PREFIXES)]
    national = [x for x in j["lists"] if x.startswith(NATIONAL_LIST_PREFIXES)]
    if j["in_doaj"]:
        return "A", "doaj"
    if quality_lists:
        return "A", "list"
    if not j["registered"]:
        return "C", "issn_not_registered"
    if national:
        return "B", "national_list"
    misses = []
    if j["first_year"] is None or j["first_year"] > B_MAX_FIRST_YEAR:
        misses.append("under_3_years")
    if j["total_record_count"] < B_MIN_RECORDS:
        misses.append(f"under_{B_MIN_RECORDS}_records")
    if j.get("title_check") in (None, "unchecked"):
        misses.append("title_unchecked")
    elif j["title_check"] in ("mismatch", "no_portal_record"):
        misses.append(f"title_{j['title_check']}")
    if misses:
        return "C", "+".join(misses)
    # 'other_script': a transliterated key title can't be compared; ISSN registration,
    # size and age carry it (1 in 75 of a 150-journal sample)
    return "B", "size_age_title" if j["title_check"] == "match" else "size_age_title_other_script"


def read_list_csvs(paths):
    """{issn: {list_id}} from load_source_list-format CSVs (active rows only), for
    lists not yet loaded in the registry; the id is the file name minus its date
    (data/source_lists/sinta-s3-2026-09-29.csv -> sinta-s3)."""
    out = defaultdict(set)
    for p in paths or ():
        list_id = re.sub(r"-\d{4}-\d{2}-\d{2}$", "", Path(p).stem)
        with open(p, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if (r.get("active") or "true").strip().lower() in ("true", "1", "yes", "y"):
                    for issn in beacon_issns(r.get("issns")):
                        out[issn].add(list_id)
    return out


def candidates(beacon=None, portal=True, out=None, list_csvs=()):
    rows, edition = read_beacon(beacon)
    edition = edition or date.today().isoformat()
    journals = active_journals(rows)
    all_issns = {i for j in journals for i in j["issns"]}
    with engine.connect() as conn:
        owned, registered, doaj, lists = registry_facts(conn, all_issns)
    for issn, ids in read_list_csvs(list_csvs).items():
        lists[issn] |= ids
    print(f"Beacon: {len(rows):,} contexts -> {len(journals):,} active journals; "
          f"{sum(1 for j in journals if set(j['issns']) & owned):,} already have a source", flush=True)
    journals = [j for j in journals if not set(j["issns"]) & owned]
    for j in journals:
        j["registered"] = any(i in registered for i in j["issns"])
        j["in_doaj"] = any(i in doaj for i in j["issns"])
        j["lists"] = sorted({x for i in j["issns"] for x in lists.get(i, ())})

    # the title check is B's last criterion: fetch only where it can decide
    need_title = [j for j in journals if assign_tier({**j, "title_check": "match"}) == ("B", "size_age_title")]
    cache = fill_portal_titles(need_title) if portal else (
        json.loads(PORTAL_CACHE.read_text(encoding="utf-8")) if PORTAL_CACHE.exists() else {})
    for j in journals:
        titles = [cache[i] for i in j["issns"] if cache.get(i)]
        j["portal_title"] = " | ".join(titles) or None
        scores = [title_score(j["context_name"], t) for t in titles]
        known = [x for x in scores if x is not None]
        j["title_score"] = max(known) if known else None
        if titles:
            j["title_check"] = ("match" if known and max(known) >= TITLE_MIN_SCORE
                                else "mismatch" if known else "other_script")
        else:
            j["title_check"] = ("no_portal_record" if any(i in cache for i in j["issns"])
                                else "unchecked")
        j["tier"], j["tier_reason"] = assign_tier(j)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = Path(out) if out else OUT_DIR / f"candidates-{edition}.csv"
    journals.sort(key=lambda j: (j["tier"], -j["total_record_count"]))
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CANDIDATE_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for j in journals:
            w.writerow({**j, "issns": ";".join(j["issns"]), "lists": ";".join(j["lists"])})
    by = Counter((j["tier"], j["tier_reason"]) for j in journals)
    recs = Counter()
    for j in journals:
        recs[j["tier"]] += j["total_record_count"]
    print(f"wrote {out}: {len(journals):,} journals with no source")
    for tier in "ABC":
        n = sum(v for (t, _), v in by.items() if t == tier)
        print(f"  tier {tier}: {n:,} journals, {recs[tier]:,} records")
        for (t, reason), v in by.most_common():
            if t == tier:
                print(f"      {reason}: {v:,}")
    unknown = Counter(j["beacon_country"] for j in journals if j["beacon_country"] and not j["country_code"]
                      and j["beacon_country"] != "NA")
    if unknown:
        print(f"  Beacon countries with no ISO mapping: {dict(unknown)}")
    return out


# --- mint -------------------------------------------------------------------

def host(url):
    h = urlparse(url or "").netloc.lower()
    return h[4:] if h.startswith("www.") else h


def name_outcome(twins, meta, src, linked=()):
    """(outcome, source_id) for a journal whose ISSNs match nothing but whose
    title exactly matches existing sources (see the module docstring).

    ISSNs are the identity, so a same-named journal source with OTHER ISSNs is a
    different journal: mint. A same-named journal source with NO ISSNs is most
    likely this journal minted without them (the 2022-10-17 legacy cohort), so
    the Beacon's ISSNs, homepage and country go onto it, provided nothing
    contradicts it: its homepage (if any) is on the same host and its country (if
    any) agrees, and it is the only journal of that name. A same-host source with
    other ISSNs is a broken source, not a second journal: held for review."""
    journals = [s for s in twins if s in src and src[s].type == "journal"]
    our_host = host(meta["homepage_url"])
    same_host = [s for s in journals if our_host and host(src[s].homepage_url) == our_host]
    # linked: namesakes that already took another journal's ISSNs earlier in this run
    issnless = [s for s in journals if not src[s].issns and s not in linked]
    consistent = [s for s in issnless
                  if (not src[s].homepage_url or s in same_host)
                  and (not src[s].country_code or not meta["country_code"]
                       or src[s].country_code == meta["country_code"])]
    if not journals:
        return "mint_name_twin_not_journal", None
    if len(journals) == 1 and consistent:
        return "link_issnless_namesake", consistent[0]
    if issnless:
        return "held_name_issnless", None
    if same_host:
        return "held_same_host_other_issns", None
    return "mint_name_twin", None


def mint(csv_path, tiers=("A", "B"), dry_run=False, limit=None, receipt=None, batch=200):
    with open(csv_path, newline="", encoding="utf-8") as f:
        todo = [r for r in csv.DictReader(f) if r["tier"] in tiers]
    if limit:
        todo = todo[:limit]
    with engine.connect() as conn:
        ctx = MatchContext(conn, name_link=True)
        src = {r.id: r for r in conn.execute(text(
            "SELECT id, type, homepage_url, issns, country_code FROM sources"))}
    print(f"{len(todo):,} candidates in tiers {','.join(tiers)}; dry_run={dry_run}", flush=True)

    counts = Counter()
    out_rows = []
    linked = set()
    conn = engine.connect()
    trans = conn.begin()
    written = 0
    try:
        for r in todo:
            issns = [i for i in r["issns"].split(";") if i]
            title = r["context_name"]
            meta = {"homepage_url": r["homepage_url"] or None,
                    "country_code": r["country_code"] or None,
                    "country": r["country"] or None}
            kind, val = match_source(conn, ctx, issns, title, SOURCE_FEED, dry_run=True)
            outcome, sid, twins = None, None, []
            if kind == "issn":
                outcome, sid = "enrich", val
            elif kind == "issn_multi":
                outcome, twins = "held_issn_multi", val
            elif kind in ("name", "name_multi", "name_parked"):
                twins = [s for s, _ in ctx.name_index.get(normalize_name(title), [])]
                outcome, sid = name_outcome(twins, meta, src, linked)
                if sid:
                    linked.add(sid)
            else:
                outcome = "mint"
            counts[outcome] += 1

            if not dry_run:
                if outcome in ("enrich", "link_issnless_namesake"):
                    res = enrich_journal(conn, ctx, sid, issns, **meta)
                    outcome += "" if res == "updated" else "_unchanged"
                elif outcome == "held_issn_multi":
                    park_multi_match(conn, SOURCE_FEED, issns, twins, title)
                elif outcome.startswith("mint"):
                    sid = mint_source(conn, ctx, title, source_type="journal", issns=issns, **meta)
                written += 1
                if written % batch == 0:
                    trans.commit()
                    trans = conn.begin()
            elif outcome.startswith("mint"):
                ctx.register(-len(out_rows) - 1, issns)  # later rows see the would-be mint
            out_rows.append({"tier": r["tier"], "tier_reason": r["tier_reason"],
                             "context_name": title, "issns": r["issns"], "outcome": outcome,
                             "source_id": sid or "", "matched_ids": ";".join(map(str, twins)),
                             "homepage_url": meta["homepage_url"] or "",
                             "country_code": meta["country_code"] or "",
                             "total_record_count": r["total_record_count"]})
        if dry_run:
            trans.rollback()
        else:
            ojs = recompute_is_ojs(conn)
            listed = recompute_listed_in(conn)
            print(f"is_ojs rows changed: {ojs}; listed_in rows changed: {listed}")
            trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()

    receipt = Path(receipt) if receipt else OUT_DIR / (
        f"receipt-{date.today().isoformat()}-{''.join(tiers)}{'-dry' if dry_run else ''}.csv")
    receipt.parent.mkdir(parents=True, exist_ok=True)
    with open(receipt, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0]) if out_rows else ["outcome"])
        w.writeheader()
        w.writerows(out_rows)
    print(f"outcome summary: {dict(counts)}")
    print(f"receipt: {receipt}")
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("candidates")
    c.add_argument("--beacon", help="Beacon CSV path or URL (default: latest from Dataverse)")
    c.add_argument("--no-portal", action="store_true", help="use the portal cache only, fetch nothing")
    c.add_argument("--out")
    c.add_argument("--lists", nargs="*", default=(),
                   help="list CSVs not yet loaded in the registry (e.g. data/source_lists/sinta-s*-DATE.csv)")
    m = sub.add_parser("mint")
    m.add_argument("--csv", required=True)
    m.add_argument("--tiers", default="A,B")
    m.add_argument("--dry-run", action="store_true")
    m.add_argument("--limit", type=int)
    m.add_argument("--receipt")
    a = ap.parse_args()
    if a.cmd == "candidates":
        candidates(a.beacon, portal=not a.no_portal, out=a.out, list_csvs=a.lists)
    else:
        mint(a.csv, tiers=tuple(t.strip() for t in a.tiers.split(",")), dry_run=a.dry_run,
             limit=a.limit, receipt=a.receipt)


if __name__ == "__main__":
    sys.exit(main())
