"""Registry fixes for two Diamond Discovery Hub sources found while researching OA start years
(Kyle, 2026-10-23: fix the matching errors).

1. Prolegomena (S1027301186, Zagreb philosophy journal, Hrcak): host is "Croatian Dairy Union"
   (wrong publisher entity) and the online ISSN 1846-0593 (DDH, Hrcak) is missing. No publisher
   entity exists for the Society for the Advancement of Philosophy, so publisher_id is cleared and
   the name recorded; 1846-0593 is on no other source.
2. S4210189331 "Eduquer" holds two Crossref titles: Recherches & educations (1969-0622 /
   1760-7760, 753 DOIs, the journal DDH lists, homepage rechercheseducations.revues.org, most
   alternate titles) and Eduquer (1634-359X / 3038-8392, 142 DOIs). It keeps its id as
   Recherches & educations (curated rename, ISSN-L re-resolved over its own pair); Eduquer's two
   ISSNs move to a newly minted journal source. Works follow their ISSNs at the next End 2 End.

One transaction, dry run by default, receipt after RECEIPT.
    heroku run -a openalex-sources --no-tty "python handoff/ddh-source-fixes-2026-10-23.py [--execute]"
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from sources_lib import (MatchContext, insert_issns, mint_source, normalize_name, recompute_listed_in,
                         refresh_issns_column, resolve_issn_l)

EXECUTE = "--execute" in sys.argv
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "before": {}, "actions": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def snapshot(conn, sid):
    r = conn.execute(text("SELECT * FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
    receipt["before"][str(sid)] = {k: (None if v is None else str(v)) for k, v in r.items()}
    receipt["before"][f"{sid}_issns"] = [x[0] for x in conn.execute(
        text("SELECT issn FROM source_issn WHERE source_id = :id ORDER BY issn"), {"id": sid})]
    return r


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            ctx = MatchContext(conn)
            # 1. Prolegomena
            p = snapshot(conn, 1027301186)
            assert normalize_name(p["display_name"]) == normalize_name("Prolegomena"), p["display_name"]
            owner = conn.execute(text("SELECT source_id FROM source_issn WHERE issn = '1846-0593'")).scalar()
            assert owner in (None, 1027301186), f"1846-0593 owned by {owner}"
            if owner is None:
                insert_issns(conn, 1027301186, ["1846-0593"],
                             resolve_issn_l(conn, ["1333-4395", "1846-0593"], member_only=True))
            conn.execute(text("UPDATE sources SET publisher_id = NULL, publisher = 'Society for the Advancement of Philosophy', "
                              "updated_date = now() WHERE id = 1027301186"))
            log("Prolegomena 1027301186: + ISSN 1846-0593; publisher_id cleared (was Croatian Dairy Union), "
                "publisher = Society for the Advancement of Philosophy")

            # 2. split S4210189331
            e = snapshot(conn, 4210189331)
            assert normalize_name(e["display_name"]) == normalize_name("Eduquer") or e["display_name"] == "Éduquer", e["display_name"]
            have = set(receipt["before"]["4210189331_issns"])
            assert have == {"1634-359X", "1760-7760", "1969-0622", "3038-8392"}, have
            edu = ["1634-359X", "3038-8392"]
            conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = 4210189331"))
            n = conn.execute(text("DELETE FROM source_issn WHERE source_id = 4210189331 AND issn = ANY(:i)"), {"i": edu}).rowcount
            assert n == 2
            rne_l = resolve_issn_l(conn, ["1969-0622", "1760-7760"], member_only=True)
            conn.execute(text("UPDATE sources SET issn_l = :l, display_name = 'Recherches & éducations', "
                              "display_name_before_override = :old, override_timestamp = now(), updated_date = now() "
                              "WHERE id = 4210189331"), {"l": rne_l, "old": e["display_name"]})
            conn.execute(text("UPDATE source_issn si SET is_issn_l = (si.issn = :l) WHERE si.source_id = 4210189331"), {"l": rne_l})
            refresh_issns_column(conn, 4210189331)
            log(f"4210189331 renamed 'Éduquer' -> 'Recherches & éducations' (curated); kept 1969-0622 / 1760-7760, issn_l {rne_l}; "
                f"moved out {edu}")
            for i in edu:
                ctx.issn_to_sid.pop(i, None)
            new = mint_source(conn, ctx, "Éduquer", source_type="journal", issns=edu)
            receipt["minted"] = new
            log(f"MINTED {new} 'Éduquer' {edu}")

            changed = recompute_listed_in(conn)
            for sid in (1027301186, 4210189331, new):
                r = conn.execute(text("SELECT id, display_name, issn_l, issns, listed_in, publisher, publisher_id FROM sources "
                                      "WHERE id = :id"), {"id": sid}).mappings().fetchone()
                print(f"  {dict(r)}", flush=True)
            log(f"listed_in rows changed: {changed}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back (ids above are not reserved)")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
