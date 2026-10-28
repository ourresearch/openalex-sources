"""Registry edit for Zendesk ticket 9061 (Inba Kehoe, UVic Libraries ePublishing, 2026-06-10):
Canadian Journal of Program Evaluation moved from University of Toronto Press to UVic's OJS (Kyle: "approve", 2026-10-28).

  CJPE  S2764689634  host P4310320517 University of Toronto Press -> P4310313033 University of Victoria
                     (the publisher entity the other 11 UVic-hosted journals use, not the institution I212119943);
                     homepage utpjournals.press/loi/cjpe -> https://ces.journals.uvic.ca/index.php/cjpe;
                     source_oa_override curated_is_oa = TRUE, no flip year.

Checked 2026-10-28: the UVic archive lists all 105 issues back to Vol. 1 No. 1 (Spring 1986), and the 1986 PDFs
download without a login under CC BY-NC 4.0, so the override covers every year. Not in DOAJ (ISSN 1496-7308).
The OA flags are applied for this source only, with apply_oa_flags' own precedence (curated_is_oa wins), so the
weekly job (Mon 05:55 UTC) computes the same values and changes nothing.

Run from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 <runner> handoff/9061-cjpe-2026-10-28.py [--execute]
where the runner fetches DATABASE_URL from Heroku into the child env only (oxjob #1880 work/run_registry.py).
The receipt (before-state + actions) is printed as JSON after the RECEIPT marker.
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name

EXECUTE = "--execute" in sys.argv
TICKET = 9061
APPROVED = "kyle 2026-10-28"

CJPE = 2764689634
EXPECT_NAME = "Canadian Journal of Program Evaluation"
P_UTP, P_UVIC = 4310320517, 4310313033
HOMEPAGE = "https://ces.journals.uvic.ca/index.php/cjpe"
OA_NOTE = (f"ZD {TICKET}: moved to UVic OJS; full archive (1986-) free under CC BY-NC 4.0, "
           f"checked 2026-10-28 ({APPROVED})")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "ticket": TICKET,
           "before": {}, "actions": [], "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn, sid):
    r = conn.execute(text("SELECT * FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
    return {k: (None if v is None else str(v)) for k, v in r.items()} if r else None


def main():
    try:
        with engine.begin() as conn:
            before = row(conn, CJPE)
            assert before is not None, "source missing"
            receipt["before"][str(CJPE)] = before
            receipt["before"]["source_oa_override"] = [dict(r._mapping) for r in conn.execute(
                text("SELECT * FROM source_oa_override WHERE source_id = :id"), {"id": CJPE})]
            # Merge losers are deleted (post-D1), so a present row is a live source.
            assert normalize_name(before["display_name"]) == normalize_name(EXPECT_NAME), before["display_name"]
            # Drift guard: if someone has already re-hosted it, stop and look rather than overwrite.
            assert before["publisher_id"] in (str(P_UTP), str(P_UVIC)), f"publisher_id drifted: {before['publisher_id']}"
            uvic = conn.execute(text("SELECT publisher, COUNT(*) FROM sources WHERE publisher_id = :p "
                                     "GROUP BY publisher ORDER BY 2 DESC LIMIT 1"),
                                {"p": P_UVIC}).fetchone()
            pub_text = uvic.publisher if uvic and uvic.publisher else "University of Victoria"

            n = conn.execute(text("UPDATE sources SET publisher_id = :p, publisher = :t, homepage_url = :h, "
                                  "updated_date = now() WHERE id = :id"),
                             {"p": P_UVIC, "t": pub_text, "h": HOMEPAGE, "id": CJPE}).rowcount
            assert n == 1
            log(f"UPDATE sources {CJPE}: publisher_id {before['publisher_id']} -> {P_UVIC}, "
                f"publisher {before['publisher']!r} -> {pub_text!r}, homepage_url {before['homepage_url']!r} -> {HOMEPAGE!r}")

            existing = conn.execute(text("SELECT curated_is_oa, curated_flip_year FROM source_oa_override "
                                         "WHERE source_id = :id"), {"id": CJPE}).fetchone()
            if existing is None:
                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) "
                                  "VALUES (:s, TRUE, :n)"), {"s": CJPE, "n": OA_NOTE})
                log(f"INSERT source_oa_override {CJPE}: curated_is_oa TRUE, no flip year")
            else:
                log(f"source_oa_override {CJPE} already exists {tuple(existing)}: left as is")

            # apply_oa_flags for this one source: curated_is_oa TRUE -> is_oa_high_oa_rate TRUE; a TRUE override
            # with no flip year takes the HOAR list start year if listed, else NULL. Then is_oa = any OA signal.
            ov = conn.execute(text("SELECT curated_is_oa, curated_flip_year FROM source_oa_override "
                                   "WHERE source_id = :id"), {"id": CJPE}).fetchone()
            if ov.curated_is_oa:
                hoar_start = conn.execute(text("SELECT MIN(h.start_year) FROM source_issn si "
                                               "JOIN high_oa_rate_issn h ON h.issn_l = si.issn "
                                               "WHERE si.source_id = :id"), {"id": CJPE}).scalar()
                start = ov.curated_flip_year + 1 if ov.curated_flip_year is not None else hoar_start
                conn.execute(text("UPDATE sources SET is_oa_high_oa_rate = TRUE, high_oa_rate_start_year = :y, "
                                  "updated_date = now() WHERE id = :id"), {"y": start, "id": CJPE})
                conn.execute(text("""
                    UPDATE sources SET is_oa = (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
                                                OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE)),
                                       updated_date = now()
                    WHERE id = :id"""), {"id": CJPE})
                log(f"OA flags {CJPE}: is_oa_high_oa_rate TRUE, high_oa_rate_start_year {start}, is_oa recomputed")

            after = row(conn, CJPE)
            receipt["after"][str(CJPE)] = {k: after[k] for k in (
                "display_name", "publisher", "publisher_id", "institution_id", "homepage_url",
                "is_oa", "is_oa_high_oa_rate", "high_oa_rate_start_year", "is_in_doaj", "type")}
            print(json.dumps(receipt["after"], indent=1), flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
