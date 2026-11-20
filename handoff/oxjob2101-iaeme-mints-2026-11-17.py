"""oxjob #2101 (support factory #2254): mint IAEME Publication's journals whose articles OpenAlex binds to another
publisher's same-named journal. Written off desk 2026-11-17 (lane #2263); run on desk from the openalex-sources checkout.

The defect (ZD 27210): IAEME Publication (Crossref member 20219, DOI prefix 10.34218) deposits its articles with no ISSN
and an upper-case container title named exactly like an established journal ('INTERNATIONAL JOURNAL OF DISTANCE
EDUCATION TECHNOLOGIES'). OpenAlex has no IAEME record under that name, so walden CreateLocationsWithSources' last cell
(normalized display-name fallback: lower case, letters and digits only, unique names) binds the article to the one
source with that name once case is dropped: IGI Global's S103640237 (W7129014553, W7213343058). Measured on Databricks
17 Nov (locations_w_sources of night 11-17): 350 IAEME Crossref locations on 92 other publishers' journals this way.

The fix: mint each IAEME journal under the name IAEME deposits (upper case), from IAEME's own site. The exact
display-name tier (CTAS, case-sensitive, unique names) then binds IAEME's deposits to it, ahead of the fallback; the
fallback no longer reaches the foreign journal for that name (two sources share the normalized name, name_count = 2).
Measured safe 17 Nov, per name: (a) no registry record holds the exact upper-case name; (b) every non-IAEME location
carrying that exact name is a repository copy bound by its endpoint (98 locations on 18 names; none unbound, none
name-bound), so none moves; (c) no non-IAEME location reaches the foreign journal through the normalized fallback,
so none loses its source. IGI's own 607 locations on S103640237 are ISSN-, exact title-case name- or MAG-bound.

Mints, under the D5 ruling (16 Nov, RULINGS.md § 3 Coverage: add a journal on no list once a third party outside
OpenAlex and the work, ideally the venue's own website, confirms it exists; name and details from that site; ISSN
optional). Witness = IAEME's site, read 17 Nov 09:50-10:20 CT through its public JSON (the HTML is JS-rendered):
https://iaeme.com/api/public/journals (1,758 journals) and /api/public/journals/<ABBR>/editorial-board: each of the 92
is listed under exactly the deposited name, with an editorial board (1-147 members) and 2-8 issues (2022-2026).
IJDET (the ticket's journal): https://iaeme.com/Home/journal/IJDET, started 2023, 6 issues, 5 articles, Chief Editor,
hosts both ticket PDFs (iaeme.com/MasterAdmin/Journal_uploads/IJDET/VOLUME_4_ISSUE_1/IJDET_04_01_001.pdf: 200).
Not added: ISSNs. IAEME's pages show an 'ISSN' for 24 of these; all 24 fail the ISSN check digit and no source holds
them; the deposits carry none, and the exact-name tier needs none. Publisher text 'IAEME Publication' (the site's
citation_publisher meta; OpenAlex has no IAEME publisher record and card D12 is open, so publisher_id stays NULL like
IAEME's 35 existing records). is_oa starts FALSE (mint_source; OA flags are a separate check).
Not minted: 3 titles IAEME deposited in title case, whose exact name a foreign journal already holds (International
Journal of English Studies S2764882319, Journal of Software Engineering S4210212565, International Journal of Cloud
Computing S4210172516; 7 locations): a same-named mint would knock both off the exact tier.

Per row the script re-checks, and SKIPS (logged in the receipt) instead of minting when: a record already holds the
exact name; a record already has the IAEME homepage; or a namesake (registry normalize_name) is already an IAEME record.

Run on desk (dry run by default; ONE transaction):
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources && \
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob2101-iaeme-mints-2026-11-17.py [--execute]
Expect: 92 MINTED, 0 SKIP. The receipt (actions + minted ids) prints as JSON after the RECEIPT marker; save it as
handoff/oxjob2101-iaeme-mints-2026-11-17-receipt.json and in the #2101 job's work/.
Revert: DELETE FROM sources WHERE id = ANY(<receipt minted source_ids>) AND publisher = 'IAEME Publication'
AND homepage_url LIKE 'https://iaeme.com/Home/journal/%' (no ISSN, endpoint or override rows are written); the next
night re-binds the articles as before.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import MatchContext, mint_source, normalize_name

EXECUTE = "--execute" in sys.argv
JOB = 2101
APPROVED = "support factory #2254 (standing authority, reversible registry edit); D5 ruling 2026-11-16 (RULINGS.md § 3 Coverage)"
PUBLISHER = "IAEME Publication"
HOME = "https://iaeme.com/Home/journal/"

# (exact name as IAEME deposits it and lists it on its site, IAEME abbreviation (its URL slug), the foreign journal(s)
#  the deposits sit on today, IAEME locations there 17 Nov, site editorial-board size, site issues)
MINTS = [
    ('INTERNATIONAL JOURNAL OF STRUCTURAL ENGINEERING', 'IJSE', [178805903], 29, 12, 8),
    ('INTERNATIONAL JOURNAL OF COMPUTER APPLICATIONS', 'IJCA', [4210206007], 22, 142, 8),
    ('INTERNATIONAL JOURNAL OF ENGLISH LANGUAGE AND LITERATURE STUDIES', 'IJELLS', [2764394334], 22, 5, 8),
    ('INTERNATIONAL JOURNAL OF INFORMATION SECURITY', 'IJIS', [164062316], 15, 54, 8),
    ('INTERNATIONAL JOURNAL OF CLOUD COMPUTING', 'IJCC', [4210172516], 14, 147, 8),
    ('INTERNATIONAL JOURNAL OF MATHEMATICS', 'IJMM', [105014038], 11, 17, 7),
    ('INTERNATIONAL JOURNAL OF PHYSIOTHERAPY', 'IJPH', [2764442288], 9, 2, 7),
    ('INTERNATIONAL JOURNAL OF TOURISM RESEARCH', 'IJTR', [172501620], 9, 1, 5),
    ('INTERNATIONAL JOURNAL OF COMPUTER SCIENCE AND APPLICATIONS', 'IJCSA', [20945777], 7, 5, 8),
    ('INTERNATIONAL JOURNAL OF THERMAL ENGINEERING', 'IJTE', [4210174147], 7, 21, 8),
    ('INTERNATIONAL JOURNAL OF AGRICULTURAL SCIENCE AND TECHNOLOGY', 'IJAST', [4210206098], 6, 22, 8),
    ('INTERNATIONAL JOURNAL OF GENERAL MEDICINE', 'IJGMED', [123199335], 6, 7, 6),
    ('INTERNATIONAL JOURNAL OF NEUROLOGY', 'IJNUE', [2764769398], 6, 2, 6),
    ('INTERNATIONAL JOURNAL OF PHARMACOLOGY', 'IJPHM', [46065367], 6, 6, 7),
    ('INTERNATIONAL JOURNAL ON DIGITAL LIBRARIES', 'IJDL', [110615584], 6, 1, 5),
    ('JOURNAL OF SOFTWARE ENGINEERING', 'JSE', [4210212565], 6, 56, 7),
    ('INTERNATIONAL JOURNAL OF ACCOUNTING AND FINANCE', 'IJAF', [173177581], 5, 9, 5),
    ('INTERNATIONAL JOURNAL OF ADVANCED RESEARCH IN COMPUTER SCIENCE AND ENGINEERING', 'IJARCSE', [7407056034], 5, 4, 6),
    ('INTERNATIONAL JOURNAL OF COMMERCE AND MANAGEMENT', 'IJCAM', [100820205], 5, 1, 5),
    ('INTERNATIONAL JOURNAL OF DATA ANALYTICS', 'IJDA', [4210195268], 5, 51, 8),
    ('INTERNATIONAL JOURNAL OF ECONOMICS', 'IJECO', [4210231152], 5, 1, 5),
    ('INTERNATIONAL JOURNAL OF MACHINE LEARNING AND CYBERNETICS', 'IJMLC', [2764999920], 5, 17, 7),
    ('INTERNATIONAL SOCIAL SCIENCE JOURNAL', 'ISSJ', [100444555], 5, 2, 5),
    ('JOURNAL OF ARTIFICIAL INTELLIGENCE AND MACHINE LEARNING', 'JAIML', [4394708398], 5, 51, 8),
    ('JOURNAL OF ENVIRONMENTAL MANAGEMENT', 'JEM', [44455300], 5, 2, 6),
    ('INTERNATIONAL JOURNAL OF AEROSPACE ENGINEERING', 'IJASE', [75380005], 4, 24, 7),
    ('INTERNATIONAL JOURNAL OF AVIATION MANAGEMENT', 'IJAM', [4210168010], 4, 8, 7),
    ('INTERNATIONAL JOURNAL OF HEALTHCARE INFORMATION SYSTEMS AND INFORMATICS', 'IJHISI', [139009114], 4, 19, 6),
    ('INTERNATIONAL JOURNAL OF HUMAN RESOURCES DEVELOPMENT AND MANAGEMENT', 'IJHRDM', [84210283], 4, 1, 7),
    ('INTERNATIONAL JOURNAL OF NEUROSURGERY', 'IJNEU', [4210179568], 4, 1, 6),
    ('INTERNATIONAL JOURNAL OF ASTRONOMY AND ASTROPHYSICS', 'IJAA', [2764899421], 3, 2, 6),
    ('INTERNATIONAL JOURNAL OF BUSINESS ANALYTICS', 'IJBAN', [4210200043], 3, 4, 6),
    ('INTERNATIONAL JOURNAL OF BUSINESS INTELLIGENCE RESEARCH', 'IJBIR', [184213297], 3, 19, 8),
    ('INTERNATIONAL JOURNAL OF CARDIOLOGY', 'IJC', [13342628], 3, 1, 8),
    ('INTERNATIONAL JOURNAL OF CUSTOMER RELATIONSHIP MARKETING AND MANAGEMENT', 'IJCRMM', [1034618884], 3, 12, 7),
    ('INTERNATIONAL JOURNAL OF CYBER WARFARE AND TERRORISM', 'IJCWT', [4210168647], 3, 4, 7),
    ('INTERNATIONAL JOURNAL OF DATA SCIENCE AND ANALYTICS', 'IJDSA', [4210195017], 3, 29, 7),
    ('INTERNATIONAL JOURNAL OF GREEN ENERGY', 'IJOGE', [65809584], 3, 2, 6),
    ('INTERNATIONAL JOURNAL OF MEDICAL MICROBIOLOGY', 'IJOMM', [41453107], 3, 4, 7),
    ('INTERNATIONAL JOURNAL OF PHYSIOLOGY', 'IJPHY', [2764881697], 3, 2, 6),
    ('INTERNATIONAL JOURNAL OF PLANT BIOTECHNOLOGY', 'IJPBT', [4387290653], 3, 2, 5),
    ('INTERNATIONAL JOURNAL OF RADIOLOGY', 'IJR', [4210233878], 3, 1, 6),
    ('JOURNAL OF OIL AND GAS TECHNOLOGY', 'JOGT', [2765033345], 3, 11, 7),
    ('JOURNAL OF SOLAR ENERGY', 'JOSE', [4210203647], 3, 20, 6),
    ('INTERNATIONAL JOURNAL OF BUSINESS AND DATA ANALYTICS', 'IJBDA', [4210199297], 2, 10, 6),
    ('INTERNATIONAL JOURNAL OF COMPUTER VISION', 'IJCV', [25538012], 2, 3, 6),
    ('INTERNATIONAL JOURNAL OF DISTANCE EDUCATION TECHNOLOGIES', 'IJDET', [103640237], 2, 1, 6),
    ('INTERNATIONAL JOURNAL OF ECONOMICS AND BUSINESS RESEARCH', 'IJEBR', [187151359], 2, 1, 5),
    ('INTERNATIONAL JOURNAL OF ENGINEERING SCIENCE', 'IJES', [102630943], 2, 1, 6),
    ('INTERNATIONAL JOURNAL OF FISHERIES AND AQUACULTURE', 'IJFAQ', [2764529728], 2, 1, 5),
    ('INTERNATIONAL JOURNAL OF INDUSTRIAL ERGONOMICS', 'IJIER', [92539022], 2, 1, 6),
    ('INTERNATIONAL JOURNAL OF MACHINE INTELLIGENCE', 'IJMI', [4210168556], 2, 2, 8),
    ('INTERNATIONAL JOURNAL OF OBSTETRICS AND GYNAECOLOGY', 'IJOG', [4387290288], 2, 7, 6),
    ('INTERNATIONAL JOURNAL OF OPHTHALMOLOGY', 'IJOP', [2481947471], 2, 1, 6),
    ('INTERNATIONAL JOURNAL OF POLITICS, CULTURE, AND SOCIETY', 'IJPCS', [42865813], 2, 1, 5),
    ('INTERNATIONAL JOURNAL OF PROBABILITY AND STATISTICS', 'IJPST', [4210193220], 2, 1, 8),
    ('INTERNATIONAL JOURNAL OF PUBLIC POLICY', 'IJPP', [188124791], 2, 1, 6),
    ('INTERNATIONAL JOURNAL OF UROLOGY', 'IJU', [134892809], 2, 1, 6),
    ('INTERNATIONAL JOURNAL OF AGRICULTURAL ECONOMICS', 'IJAGRE', [4210207208], 1, 3, 8),
    ('INTERNATIONAL JOURNAL OF ANALYTICAL CHEMISTRY', 'IJAC', [127888171], 1, 3, 5),
    ('INTERNATIONAL JOURNAL OF ARTIFICIAL INTELLIGENCE TOOLS', 'IJAIT', [178780388], 1, 5, 6),
    ('INTERNATIONAL JOURNAL OF AUTOMOBILE ENGINEERING', 'IJAUE', [4404661863], 1, 22, 8),
    ('INTERNATIONAL JOURNAL OF BEHAVIORAL SCIENCES', 'IJOBS', [5407047218], 1, 1, 5),
    ('INTERNATIONAL JOURNAL OF BLOCKCHAINS AND CRYPTOCURRENCIES', 'IJBC', [4210223274], 1, 4, 6),
    ('INTERNATIONAL JOURNAL OF BUSINESS INNOVATION AND RESEARCH', 'IJBIRE', [155657779], 1, 2, 6),
    ('INTERNATIONAL JOURNAL OF CLINICAL PRACTICE', 'IJCP', [22344484], 1, 2, 6),
    ('INTERNATIONAL JOURNAL OF COMPUTATIONAL BIOLOGY', 'IJCB', [4210170868], 1, 3, 8),
    ('INTERNATIONAL JOURNAL OF COMPUTER SCIENCES AND ENGINEERING', 'IJCSE', [4210169770], 1, 3, 6),
    ('INTERNATIONAL JOURNAL OF DATA SCIENCE', 'IJDS', [4210179791], 1, 15, 6),
    ('INTERNATIONAL JOURNAL OF DENTAL SCIENCES', 'IJDENT', [7407058479], 1, 3, 6),
    ('INTERNATIONAL JOURNAL OF E-ENTREPRENEURSHIP AND INNOVATION', 'IJEEI', [4210191280], 1, 1, 6),
    ('INTERNATIONAL JOURNAL OF EMBEDDED SYSTEMS', 'IJEMS', [204369148], 1, 7, 6),
    ('INTERNATIONAL JOURNAL OF ENDOCRINOLOGY', 'IJEN', [123631873], 1, 1, 7),
    ('INTERNATIONAL JOURNAL OF ENTREPRENEURSHIP AND SMALL BUSINESS', 'IJESB', [123914889], 1, 1, 5),
    ('INTERNATIONAL JOURNAL OF GENERATIVE ARTIFICIAL INTELLIGENCE IN BUSINESS', 'IJGAIB', [5407052546], 1, 3, 3),
    ('INTERNATIONAL JOURNAL OF HYPERTENSION', 'IJHY', [52659601], 1, 1, 6),
    ('INTERNATIONAL JOURNAL OF MANUFACTURING, MATERIALS, AND MECHANICAL ENGINEERING', 'IJMMME', [4210226236], 1, 9, 7),
    ('INTERNATIONAL JOURNAL OF NANOTECHNOLOGY', 'IJNT', [200612592], 1, 19, 6),
    ('INTERNATIONAL JOURNAL OF NEMATOLOGY', 'IJNMT', [2764360568], 1, 3, 8),
    ('INTERNATIONAL JOURNAL OF NEPHROLOGY', 'IJNE', [120416549], 1, 1, 7),
    ('INTERNATIONAL JOURNAL OF PAIN RESEARCH AND TREATMENT', 'IJPART', [4210223289], 1, 2, 3),
    ('INTERNATIONAL JOURNAL OF PRODUCTION RESEARCH', 'IJPR', [65690446], 1, 1, 5),
    ('INTERNATIONAL JOURNAL OF RELIABILITY, QUALITY AND SAFETY ENGINEERING', 'IJRQSE', [18122089], 1, 1, 2),
    ('INTERNATIONAL JOURNAL OF RELIGIOUS AND CULTURAL STUDIES', 'IJRCS', [4210225145], 1, 1, 7),
    ('INTERNATIONAL JOURNAL OF SOCIAL ECONOMICS', 'IJSECO', [49131624], 1, 1, 5),
    ('INTERNATIONAL JOURNAL OF SOIL SCIENCE', 'IJSOS', [109079305], 1, 1, 5),
    ('INTERNATIONAL JOURNAL OF VETERINARY SCIENCE AND MEDICINE', 'IJVSM', [2898220704], 1, 2, 6),
    ('JOURNAL OF CARDIOLOGY AND CARDIOVASCULAR MEDICINE', 'JCCM', [4210200178], 1, 1, 2),
    ('JOURNAL OF ENGINEERING DESIGN', 'JED', [2912548], 1, 17, 6),
    ('JOURNAL OF INDUSTRIAL TECHNOLOGY AND INNOVATION', 'JITI', [4387281474], 1, 2, 2),
    ('JOURNAL OF LEARNING DEVELOPMENT IN HIGHER EDUCATION', 'JLDHE', [2764854317], 1, 1, 5),
    ('JOURNAL OF ORGANIZATIONAL BEHAVIOR', 'JOB', [160573970], 1, 2, 6),
]
TICKET_NAME = "INTERNATIONAL JOURNAL OF DISTANCE EDUCATION TECHNOLOGIES"  # ZD 27210

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
           "actions": [], "minted": [], "skipped": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


class DryRun(Exception):
    pass


def main():
    assert len(MINTS) == 92 and len({m[0] for m in MINTS}) == 92 and len({m[1] for m in MINTS}) == 92
    assert any(m[0] == TICKET_NAME for m in MINTS)
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn, name_link=True)
            pubs = {r.id: (r.publisher or "", r.homepage_url or "") for r in conn.execute(
                text("SELECT id, publisher, homepage_url FROM sources WHERE publisher ILIKE '%iaeme%' "
                     "OR homepage_url ILIKE '%iaeme.%'"))}
            for name, abbr, foreign, locs, board, issues in MINTS:
                url = HOME + abbr
                exact = [r[0] for r in conn.execute(text("SELECT id FROM sources WHERE display_name = :n"), {"n": name})]
                if exact:
                    receipt["skipped"].append({"name": name, "why": "exact name already held", "ids": exact})
                    log(f"SKIP {name!r}: exact name already on {exact}")
                    continue
                same_home = [r[0] for r in conn.execute(
                    text("SELECT id FROM sources WHERE lower(rtrim(homepage_url, '/')) = lower(:u)"), {"u": url})]
                if same_home:
                    receipt["skipped"].append({"name": name, "why": "homepage already held", "ids": same_home})
                    log(f"SKIP {name!r}: {url} already on {same_home}")
                    continue
                namesakes = [sid for sid, _ in ctx.name_index.get(normalize_name(name), [])]
                iaeme_twins = [sid for sid in namesakes if sid in pubs]
                if iaeme_twins:
                    receipt["skipped"].append({"name": name, "why": "an IAEME namesake exists", "ids": iaeme_twins})
                    log(f"SKIP {name!r}: IAEME namesake(s) {iaeme_twins}")
                    continue
                sid = mint_source(conn, ctx, name, source_type="journal", issns=(), publisher=PUBLISHER,
                                  homepage_url=url, country_code="IN", country="India")
                row = conn.execute(text("SELECT id, display_name, type, publisher, homepage_url, country_code, is_oa "
                                        "FROM sources WHERE id = :id"), {"id": sid}).mappings().one()
                assert row["display_name"] == name and row["type"] == "journal", dict(row)
                receipt["minted"].append({"source_id": sid, "name": name, "homepage_url": url,
                                          "foreign_namesakes_today": foreign, "iaeme_locations_on_foreign": locs,
                                          "namesakes_in_registry": namesakes, "site_board": board, "site_issues": issues,
                                          "ticket": 27210 if name == TICKET_NAME else None})
                log(f"MINTED S{sid} {name!r} {url} (moves {locs} IAEME locations off {foreign}; board {board}, issues {issues})")
            log(f"minted {len(receipt['minted'])}, skipped {len(receipt['skipped'])}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
