"""Registry edit for Zendesk ticket 26668 (Rolf Kwakkelaar, Elsevier, Partner), desk 2026-11-04, Kyle's yes in the desk session:
six journals Rolf confirmed 30 Oct are NOT fully OA (IJMM, Revista de Psicodidactica, F&S Science, Energy and Climate Change = regular
hybrid; Allergologia et Immunopathologia = transferred; Heterocycles = not an Elsevier title, not fully OA). All six read is_oa=true at
source level (not in DOAJ), so every work since the flip year reads gold/diamond from the source flag alone.

  source_oa_override curated_is_oa = FALSE (no flip year) on each; apply_oa_flags' precedence recomputed for these sources only.

Run from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 <runner> handoff/26668-elsevier6-2026-11-04.py [--execute]
runner = oxjob #1899 work/run_registry.py. Receipt printed as JSON after the RECEIPT marker.
Revert: DELETE the six source_oa_override rows and re-run jobs.apply_oa_flags.
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
APPROVED = "kyle 2026-11-04 desk"
SOURCES = {131141226: "Indian Journal of Medical Microbiology", 4210205432: "Allergologia et Immunopathologia",
           965846755: "Revista de Psicodidáctica", 4210200594: "F&S Science", 4210178894: "Energy and Climate Change",
           27298842: "Heterocycles"}
OA_NOTE = (f"ZD {TICKET}: publisher (Elsevier, Rolf Kwakkelaar 2026-10-30) confirms not fully OA (hybrid/transferred; Heterocycles "
           f"not Elsevier, not fully OA). Not in DOAJ. ({APPROVED})")

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
            for sid, name in SOURCES.items():
                before = row(conn, sid)
                assert before is not None, f"source {sid} missing"
                receipt["before"][str(sid)] = before
                ov = [dict(r._mapping) for r in conn.execute(text("SELECT * FROM source_oa_override WHERE source_id = :id"), {"id": sid})]
                receipt["before"][f"override_{sid}"] = ov
                assert normalize_name(before["display_name"]) == normalize_name(name), before["display_name"]
                assert before["is_in_doaj"] != "True", f"{sid} now in DOAJ: stop and look"
                assert not ov, f"override already exists on {sid}: {ov}"
                conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, note) VALUES (:s, FALSE, :n)"), {"s": sid, "n": OA_NOTE})
                conn.execute(text("UPDATE sources SET is_oa_high_oa_rate = FALSE, high_oa_rate_start_year = NULL, updated_date = now() WHERE id = :id"), {"id": sid})
                conn.execute(text("""
                    UPDATE sources SET is_oa = (COALESCE(is_in_doaj, FALSE) OR COALESCE(is_fully_open_in_jstage, FALSE)
                                                OR COALESCE(is_in_scielo, FALSE) OR COALESCE(is_oa_high_oa_rate, FALSE)),
                                       updated_date = now()
                    WHERE id = :id"""), {"id": sid})
                log(f"{sid} {name}: INSERT source_oa_override FALSE; is_oa_high_oa_rate FALSE, start NULL, is_oa recomputed")
                after = row(conn, sid)
                receipt["after"][str(sid)] = {k: after[k] for k in ("display_name", "is_oa", "is_oa_high_oa_rate", "high_oa_rate_start_year", "is_in_doaj", "is_in_scielo", "is_fully_open_in_jstage")}
                assert after["is_oa"] == "False", f"{sid} is_oa still {after['is_oa']}: another signal is set, stop and look"
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
