"""Registry edit for Zendesk ticket 26668 (Rolf Kwakkelaar, Elsevier, 2026-10-28):
American Journal of Pharmaceutical Education is a hybrid journal, but every work since 2017 reads gold.

  AJPE  S32993367  source_oa_override curated_is_oa = FALSE (no flip year).

Cause: ISSN-L 0002-9459 is in the legacy high_oa_rate_issn list (start year 2017, served as oa_flip_year 2016), so
walden colours every 2017+ work gold from the source flag alone (3,280 works on 2026-10-28). Not in DOAJ (0002-9459,
1553-6467). Checked 2026-10-28: Crossref holds no Creative Commons licence on any of its 13,319 deposits before
May 2023, when AACP moved AJPE to Elsevier ("predominantly subscription-based", Elsevier press release 2023-05-22);
since then about 110 of 2,067 carry CC BY / BY-NC / BY-NC-ND (the hybrid OA articles). Works up to 2022 keep their
own evidence (almost all have a PubMed Central copy), so FALSE with no year range is the right override.
The OA flags are applied for this source only, with apply_oa_flags' own precedence (curated_is_oa FALSE wins), so
the weekly job (Mon 05:55 UTC) computes the same values and changes nothing.

Run from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 <runner> handoff/26668-ajpe-2026-10-28.py [--execute]
where the runner fetches DATABASE_URL from Heroku into the child env only (oxjob #1899 work/run_registry.py).
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
TICKET = 26668
APPROVED = "kyle (pending)"

AJPE = 32993367
EXPECT_NAME = "American Journal of Pharmaceutical Education"
P_ELSEVIER = 4310320990
OA_NOTE = (f"ZD {TICKET}: hybrid (Elsevier since May 2023; AACP before). Not in DOAJ; no CC licence in Crossref "
           f"before May 2023, ~110 of 2,067 since. high_oa_rate list entry (2017) was wrong. Checked 2026-10-28 "
           f"({APPROVED})")

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
            before = row(conn, AJPE)
            assert before is not None, "source missing"
            receipt["before"][str(AJPE)] = before
            receipt["before"]["source_oa_override"] = [dict(r._mapping) for r in conn.execute(
                text("SELECT * FROM source_oa_override WHERE source_id = :id"), {"id": AJPE})]
            receipt["before"]["high_oa_rate_issn"] = [dict(r._mapping) for r in conn.execute(
                text("SELECT h.* FROM source_issn si JOIN high_oa_rate_issn h ON h.issn_l = si.issn "
                     "WHERE si.source_id = :id"), {"id": AJPE})]
            # Merge losers are deleted (post-D1), so a present row is a live source.
            assert normalize_name(before["display_name"]) == normalize_name(EXPECT_NAME), before["display_name"]
            # Drift guards: still Elsevier-hosted and still not in DOAJ; any existing curation is read, not overwritten.
            assert before["publisher_id"] == str(P_ELSEVIER), f"publisher_id drifted: {before['publisher_id']}"
            assert before["is_in_doaj"] != "True", "now in DOAJ: stop and look"
            assert not receipt["before"]["source_oa_override"], \
                f"override already exists: {receipt['before']['source_oa_override']}"

            conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) "
                              "VALUES (:s, FALSE, :n)"), {"s": AJPE, "n": OA_NOTE})
            log(f"INSERT source_oa_override {AJPE}: curated_is_oa FALSE, no flip year")

            # apply_oa_flags for this one source: curated_is_oa FALSE -> is_oa_high_oa_rate FALSE, start year NULL.
            # Then is_oa = any remaining OA signal (DOAJ, J-STAGE, SciELO: all false here).
            conn.execute(text("UPDATE sources SET is_oa_high_oa_rate = FALSE, high_oa_rate_start_year = NULL, "
                              "updated_date = now() WHERE id = :id"), {"id": AJPE})
            conn.execute(text("""
                UPDATE sources SET is_oa = (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
                                            OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE)),
                                   updated_date = now()
                WHERE id = :id"""), {"id": AJPE})
            log(f"OA flags {AJPE}: is_oa_high_oa_rate FALSE, high_oa_rate_start_year NULL, is_oa recomputed")

            after = row(conn, AJPE)
            receipt["after"][str(AJPE)] = {k: after[k] for k in (
                "display_name", "publisher", "publisher_id", "is_oa", "is_oa_high_oa_rate",
                "high_oa_rate_start_year", "is_in_doaj", "is_in_scielo", "is_fully_open_in_jstage")}
            assert after["is_oa"] == "False", f"is_oa still {after['is_oa']}: another signal is set, stop and look"
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
