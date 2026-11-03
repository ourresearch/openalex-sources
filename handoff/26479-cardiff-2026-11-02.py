"""Registry edit for Zendesk ticket 26479 (Cardiff University Press, Kyle approved 2026-11-02 in the support desk), oxjob #1899.

After the 2026-10-26 TRUE overrides, 286 works under 10.18573 still read hybrid:
  - five CUP journals never overridden: Assuming Gender S4210230471, Asian Literature and Translation S4210189124,
    The British Student Doctor Journal S4210174315, SHARE S4210234459, Studies in Gothic Fiction S4210239808
    -> curated_is_oa TRUE with a flip year just before each journal's first issue (four of them carry high_oa_rate
    list start years 2018-2020 that would otherwise leave their early issues hybrid). A FALSE row stops the run.
  - JOMEC Journal S2764899589: TRUE override but high_oa_rate_start_year 2018, so its 2012-2017 papers (117) stay
    hybrid. JOMEC has been an open access CUP journal since its first issue (2012) -> curated_flip_year 2011
    (start year 2012).
Flags applied per source with apply_oa_flags' own precedence, so the weekly job computes the same values.

Run from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 <runner> handoff/26479-cardiff-2026-11-02.py [--execute]
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
TICKET = 26479
APPROVED = "kyle 2026-11-02 (support desk)"
# name, flip year (= year before the first issue in OpenAlex; None when the source has no high_oa_rate list start year)
FIVE = {
    4210230471: ("Assuming Gender", None),
    4210189124: ("Asian Literature and Translation", 2012),
    4210174315: ("The British Student Doctor Journal", 2016),
    4210234459: ("SHARE Studies in History Archaeology Religion and Conservation", 2013),
    4210239808: ("Studies in Gothic Fiction", 2016),
}
JOMEC, JOMEC_NAME, JOMEC_FLIP = 2764899589, "JOMEC Journal", 2011
PUBLISHER_OA_RULE = "^(mdpi|academic journals|edorium journals)"
ALL = list(FIVE) + [JOMEC]
SHOW = ("display_name", "publisher", "is_oa", "is_oa_high_oa_rate", "high_oa_rate_start_year", "is_in_doaj")

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


def override(conn, sid):
    return conn.execute(text("SELECT curated_is_oa, curated_flip_year FROM source_oa_override WHERE source_id = :id"),
                        {"id": sid}).fetchone()


def apply_flags(conn, sid):
    ov = override(conn, sid)
    assert ov is not None, f"{sid}: no override row"
    if ov.curated_is_oa is False:
        hoar, start = False, None
    else:
        hoar = True
        if ov.curated_flip_year is not None:
            start = ov.curated_flip_year + 1
        else:
            s = row(conn, sid)
            rule = conn.execute(text("SELECT COALESCE(:p ~* :r, FALSE)"),
                                {"p": s["publisher"] or "", "r": PUBLISHER_OA_RULE}).scalar()
            start = None if (rule or s["is_in_scielo"] == "True") else conn.execute(text(
                "SELECT MIN(h.start_year) FROM source_issn si JOIN high_oa_rate_issn h ON h.issn_l = si.issn "
                "WHERE si.source_id = :id"), {"id": sid}).scalar()
    conn.execute(text("UPDATE sources SET is_oa_high_oa_rate = :h, high_oa_rate_start_year = :y, updated_date = now() "
                      "WHERE id = :id"), {"h": hoar, "y": start, "id": sid})
    conn.execute(text("""
        UPDATE sources SET is_oa = (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
                                    OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE)),
                           updated_date = now()
        WHERE id = :id"""), {"id": sid})
    log(f"OA flags {sid}: is_oa_high_oa_rate {hoar}, high_oa_rate_start_year {start}, is_oa recomputed")


def main():
    try:
        with engine.begin() as conn:
            for sid in ALL:
                b = row(conn, sid)
                assert b is not None, f"source {sid} missing"
                b["source_oa_override"] = [dict(r._mapping) for r in conn.execute(
                    text("SELECT * FROM source_oa_override WHERE source_id = :id"), {"id": sid})]
                receipt["before"][str(sid)] = b
                print(sid, {k: b[k] for k in SHOW}, b["source_oa_override"], flush=True)

            for sid, (name, flip) in FIVE.items():
                b = receipt["before"][str(sid)]
                assert normalize_name(b["display_name"]) == normalize_name(name), b["display_name"]
                ov = override(conn, sid)
                if ov is None:
                    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                                      "VALUES (:s, TRUE, :y, :n)"),
                                 {"s": sid, "y": flip, "n": f"ZD {TICKET}: Cardiff University Press journal; every CUP publication "
                                              f"is open access ({APPROVED})"})
                    log(f"INSERT source_oa_override {sid} {name}: curated_is_oa TRUE, flip year {flip}")
                else:
                    assert ov.curated_is_oa is True, f"{sid}: existing FALSE override, stop and look"
                    log(f"{sid} {name}: TRUE override already present (flip {ov.curated_flip_year}), kept")
                apply_flags(conn, sid)

            b = receipt["before"][str(JOMEC)]
            assert normalize_name(b["display_name"]) == normalize_name(JOMEC_NAME), b["display_name"]
            ov = override(conn, JOMEC)
            assert ov is not None and ov.curated_is_oa is True, f"JOMEC: expected the 10-26 TRUE override, got {ov}"
            conn.execute(text("UPDATE source_oa_override SET curated_flip_year = :y, note = COALESCE(note, '') || :n "
                              "WHERE source_id = :id"),
                         {"y": JOMEC_FLIP, "id": JOMEC,
                          "n": f" | ZD {TICKET}: OA since first issue (2012), flip year {JOMEC_FLIP} ({APPROVED})"})
            log(f"UPDATE source_oa_override {JOMEC}: curated_flip_year {ov.curated_flip_year} -> {JOMEC_FLIP}")
            apply_flags(conn, JOMEC)

            for sid in ALL:
                a = row(conn, sid)
                receipt["after"][str(sid)] = {k: a[k] for k in SHOW}
                assert a["is_oa"] == "True", f"{sid} is_oa not true after override"
            for sid, (name, flip) in FIVE.items():
                y = receipt["after"][str(sid)]["high_oa_rate_start_year"]
                want = None if flip is None else str(flip + 1)
                assert y == want, f"{sid}: start year {y}, expected {want}"
            assert receipt["after"][str(JOMEC)]["high_oa_rate_start_year"] == str(JOMEC_FLIP + 1)
            print(json.dumps(receipt["after"], indent=1, ensure_ascii=False), flush=True)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED", flush=True)
    except DryRun:
        print("DRY RUN - rolled back", flush=True)
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
