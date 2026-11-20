"""Book platform for Éditions Orbis Tertius (oxjob #1817, support factory #2254; ZD 26966), 2026-11-20.

The press asked for a publisher record "with a publisher-level source for its books" (ZD 26966, 4 Nov). Its books
(Crossref prefix 10.66263, 136 DOIs, all edited-book or monograph) show no source and no publisher: on 20 Nov,
127 of 135 Crossref locations had source_id NULL in openalex.works.locations_w_sources (8 sit on a series).

Policy choice: follows ruling D12 (17 Nov: a publisher ROR doesn't list gets a record when its Crossref membership plus
its own site confirm it) and D5 (16 Nov: a new book press is added when a third party confirms it; the press's own
website is the ideal witness), and #1528 POLICY 2026-10-03 (a book's source is its series, else the publisher's ebook
platform). Witnesses, saved in oxjobs #1817 work/receipts/:
  - Crossref member 55857 'Éditions Orbis Tertius', Saint-Paul-lès-Dax, France, sole prefix 10.66263
    (crossref_member_55857_2026-11-20.json; its 136 records name the publisher 'Éditions Orbis Tertius' (132) or
    'EDITIONS ORBIS TERTIUS' (4): crossref_prefix_10.66263_2026-11-20.json).
  - The press's own site https://www.editionsorbistertius.website/ ("Les Éditions Orbis Tertius sont une infrastructure
    éditoriale pour la science ouverte ..."; collections, DOI and ORCID, open access): orbis_tertius_homepage_2026-11-20.txt
  - No publisher, institution or ebook platform of that name (publishers table, API searches, sources; 20 Nov 17:30 CT).

Publisher P4404685466 'Éditions Orbis Tertius' (FR) is inserted in openalex.publishers.publishers by the same job
(Databricks, receipt in oxjobs #1817 work/). This script mints the platform and hosts it there:
  MINT  'Éditions Orbis Tertius eBooks' (type ebook platform; alt 'Editions Orbis Tertius eBooks' so the 4 unaccented
        deposits share the key), publisher string 'Éditions Orbis Tertius', homepage, FR, publisher_id 4404685466.
Walden's ebook-platform tier (CreateLocationsWithSources cell 11) then binds the NULL-source book locations whose
normalized Crossref publisher is 'éditionsorbistertius' or 'editionsorbistertius'. WORKS RE-STAMP: ~127.

Run on desk from the openalex-sources checkout, outside the End 2 End window; dry run by default, ONE transaction:
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1817-orbis-tertius-2026-11-20.py
    ... --execute
    ... --revert handoff/oxjob1817-orbis-tertius-2026-11-20-receipt.json [--execute]
Revert deletes the minted source if no ISSN, endpoint or merge row has been attached to it since; then
DELETE FROM openalex.publishers.publishers WHERE id = 4404685466 AND created_date = '2026-11-20'.
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import MatchContext, mint_source  # noqa: E402

EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
JOB = 1817
APPROVED = "support factory #2254; RULINGS § 3 D12 (17 Nov) + D5 (16 Nov); standing authority"
RECEIPT_PATH = os.path.join(HERE, "oxjob1817-orbis-tertius-2026-11-20-receipt.json")
PUBLISHER_ID = 4404685466
NAME = "Éditions Orbis Tertius eBooks"
ALT = ["Editions Orbis Tertius eBooks"]
PUBLISHER = "Éditions Orbis Tertius"
HOMEPAGE = "https://www.editionsorbistertius.website/"

KEY_SQL = ("lower(regexp_replace(regexp_replace({x}, '\\s*[eE][bB]ooks$', ''), '[^[:alnum:]]', '', 'g'))")


class DryRun(Exception):
    pass


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT, "job": JOB,
               "approved": APPROVED, "actions": [], "minted": []}

    def log(msg):
        print(msg, flush=True)
        receipt["actions"].append(msg)

    try:
        with engine.begin() as conn:
            if REVERT:
                prior = json.load(open(REVERT, encoding="utf-8"))
                for m in prior["minted"]:
                    sid = m["source_id"]
                    held = conn.execute(text(
                        "SELECT (SELECT count(*) FROM source_issn WHERE source_id = :i) + "
                        "(SELECT count(*) FROM oai_pmh_endpoint WHERE source_id = :i) AS n"), {"i": sid}).scalar()
                    if held:
                        log(f"S{sid}: {held} ISSN/endpoint rows attached since; left in place")
                        continue
                    n = conn.execute(text("DELETE FROM sources WHERE id = :i AND display_name = :n"),
                                     {"i": sid, "n": m["name"]}).rowcount
                    log(f"DELETED S{sid} {m['name']!r} ({n} row)")
            else:
                # the platform tier keys on the name minus 'eBooks', normalized; a key two platforms share binds nothing
                for t in [NAME] + ALT:
                    taken = conn.execute(text(
                        "SELECT id, display_name FROM sources WHERE type = 'ebook platform' AND ("
                        + KEY_SQL.format(x="display_name") + " = " + KEY_SQL.format(x=":n")
                        + " OR EXISTS (SELECT 1 FROM jsonb_array_elements_text(COALESCE(alternate_titles, '[]'::jsonb)) a "
                        "WHERE " + KEY_SQL.format(x="a") + " = " + KEY_SQL.format(x=":n") + "))"),
                        {"n": t}).fetchall()
                    assert not taken, f"{t}: platform key already taken by {taken}"
                same = conn.execute(text(
                    "SELECT id, display_name, type FROM sources WHERE "
                    "lower(regexp_replace(display_name, '[^[:alnum:]]', '', 'g')) IN "
                    "(lower(regexp_replace(:a, '[^[:alnum:]]', '', 'g')), lower(regexp_replace(:b, '[^[:alnum:]]', '', 'g')))"),
                    {"a": NAME, "b": PUBLISHER}).fetchall()
                assert not same, f"a source with the same normalized name exists {same}: edit it instead"
                ctx = MatchContext(conn, name_link=True)
                sid = mint_source(conn, ctx, NAME, source_type="ebook platform", issns=[], publisher=PUBLISHER,
                                  homepage_url=HOMEPAGE, country_code="FR", country="France")
                conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), publisher_id = :p, "
                                  "updated_date = now() WHERE id = :id"),
                             {"a": json.dumps(ALT, ensure_ascii=False), "p": PUBLISHER_ID, "id": sid})
                receipt["minted"].append({"source_id": sid, "name": NAME, "publisher_id": PUBLISHER_ID, "ticket": 26966})
                log(f"MINTED S{sid} ebook platform {NAME!r}, alt {ALT}, publisher_id {PUBLISHER_ID} (ZD 26966)")
                r = conn.execute(text("SELECT id, display_name, type, publisher, publisher_id, institution_id, "
                                      "homepage_url, country_code, is_oa, alternate_titles FROM sources WHERE id = :id"),
                                 {"id": sid}).mappings().first()
                log("row: " + json.dumps({k: (v if isinstance(v, (list, dict, bool, int, type(None))) else str(v))
                                          for k, v in r.items()}, ensure_ascii=False))
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
        path = RECEIPT_PATH.replace("-receipt.json", "-revert-receipt.json") if REVERT else RECEIPT_PATH
        json.dump(receipt, open(path, "w"), ensure_ascii=False, indent=1, default=str)
        print("receipt:", path)
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
