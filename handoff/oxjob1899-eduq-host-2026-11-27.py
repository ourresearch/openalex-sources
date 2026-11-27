"""Registry fix for ZD 28404 (premium) on oxjob #1899 (support factory #2254), written off-desk 2026-11-27 04:23 CT (#2263).

S4306400638 is named 'Bibliothèque et Archives nationales du Québec (Québec government)' and hosted on I4210163302
'Hôpital en santé mentale Rivière-des-Prairies'. ZD 28404 asks for host BAnQ (I4210159679). Neither is right: the
source's records are EDUQ.info's, the Québec college network's open archive.
  - Live API 27 Nov 04:21 CT: 113 of 118 sampled S4306400638 locations land on eduq.info (handle prefix 11515,
    e.g. W3176133800 https://eduq.info/xmlui/handle/11515/40416), 2 more on www.cdc.qc.ca (#1899 work/eduq_2026-11-27.json).
  - Handle prefix 0.NA/11515 (hdl.handle.net/api/handles/0.NA/11515): its handle server is "serveur handle.net du CDC".
  - https://cdc.qc.ca/fr/apropos/ (27 Nov): "Le CDC gère EDUQ.info, une archive ouverte ..." and "Le CDC, dont la
    gestion est dès lors assurée par le cégep André-Laurendeau"; "Le CDC est installé à l'intérieur du Carrefour de
    l'information et des médias (bibliothèque) du cégep André-Laurendeau".
  - The Centre de documentation collégiale has no ROR record and no OpenAlex institution (ROR search 27 Nov), so the
    host is the institution that manages it: I282831249 Cégep André Laurendeau (ROR https://ror.org/03by7ks57).
  - No other source is EDUQ.info (/sources?search=eduq: only S4306508814 'Educació Química. EduQ', a journal).
Sets: institution_id 4210163302 -> 282831249 (publisher_id NULL, the 26471 set_host_institution pattern);
display_name -> 'EDUQ.info (Centre de documentation collégiale)' (curator override, so the feed's name can't come back;
the old name is NOT added as an alternate title: it was never this archive's name); homepage_url -> https://eduq.info/
if empty.

Run on desk AFTER handoff/oxjob1899-ticket-batch-2026-11-26.py (independent of it; dry run by default; ONE transaction):
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources && \
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/oxjob1899-eduq-host-2026-11-27.py [--execute]
Dry run expect: "HOST 4306400638 I4210163302 -> I282831249", "RENAME ...", "HOMEPAGE ... set", "DRY RUN - rolled back".
Receipt: the JSON after RECEIPT -> handoff/oxjob1899-eduq-host-2026-11-27-receipt.json and #1899 work/.
Works re-stamped: S4306400638 works_count 32,834 (10,142 works list a location on it); pre-clear --add 39,500 (oxpreclear,
recorded by this lane). Rides night 2026-11-28 when run before 19:30 CT 27 Nov.
Revert: UPDATE sources SET institution_id = 4210163302, publisher_id = <before>, display_name = <before display_name>,
display_name_before_override = <before>, override_timestamp = <before>, homepage_url = <before>, updated_date = now()
WHERE id = 4306400638  (every <before> is in the receipt's `before`).
"""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import normalize_name

EXECUTE = "--execute" in sys.argv
JOB = 1899
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); host = the institution that runs the repository (#1899 manifest rule)"

SID = 4306400638
EXPECT_NAME = "Bibliothèque et Archives nationales du Québec (Québec government)"
WAS_INST, NEW_INST = 4210163302, 282831249
NEW_NAME = "EDUQ.info (Centre de documentation collégiale)"
HOMEPAGE = "https://eduq.info/"
COLS = ("id", "display_name", "type", "publisher", "publisher_id", "institution_id", "homepage_url",
        "display_name_before_override", "override_timestamp", "alternate_titles")

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
           "tickets": [28404], "before": {}, "actions": [], "after": {}}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn):
    r = conn.execute(text(f"SELECT {', '.join(COLS)} FROM sources WHERE id = :id"), {"id": SID}).mappings().fetchone()
    assert r is not None, f"source {SID} missing (merged away?)"
    return {k: (None if v is None else str(v)) for k, v in r.items()}


class DryRun(Exception):
    pass


def main():
    try:
        with engine.begin() as conn:
            before = row(conn)
            receipt["before"][str(SID)] = before
            assert before["type"] == "repository", before["type"]
            done_name = normalize_name(before["display_name"]) == normalize_name(NEW_NAME)
            assert done_name or normalize_name(before["display_name"]) == normalize_name(EXPECT_NAME), \
                (SID, before["display_name"], EXPECT_NAME)
            cur = None if before["institution_id"] is None else int(before["institution_id"])
            assert cur in (WAS_INST, NEW_INST), f"{SID}: institution_id drifted: registry {cur}, expected {WAS_INST}"
            assert before["publisher_id"] is None, f"{SID}: publisher_id {before['publisher_id']} set; host would ignore institution"

            if cur == NEW_INST:
                log(f"HOST {SID} already I{NEW_INST}, left")
            else:
                n = conn.execute(text("UPDATE sources SET institution_id = :n, publisher_id = NULL, updated_date = now() "
                                      "WHERE id = :id AND institution_id = :w"),
                                 {"n": NEW_INST, "w": WAS_INST, "id": SID}).rowcount
                assert n == 1, n
                log(f"HOST {SID} I{WAS_INST} -> I{NEW_INST} (Cégep André Laurendeau, ROR 03by7ks57; "
                    "manages the Centre de documentation collégiale, which runs EDUQ.info)")
            if done_name:
                log(f"RENAME {SID} already {NEW_NAME!r}, left")
            else:
                n = conn.execute(text("UPDATE sources SET display_name = :n, display_name_before_override = :o, "
                                      "override_timestamp = now(), updated_date = now() WHERE id = :id"),
                                 {"n": NEW_NAME, "o": before["display_name"], "id": SID}).rowcount
                assert n == 1, n
                log(f"RENAME {SID} {before['display_name']!r} -> {NEW_NAME!r}")
            n = conn.execute(text("UPDATE sources SET homepage_url = :u, updated_date = now() "
                                  "WHERE id = :id AND (homepage_url IS NULL OR homepage_url = '')"),
                             {"u": HOMEPAGE, "id": SID}).rowcount
            log(f"HOMEPAGE {SID} {'set to ' + HOMEPAGE if n else 'already set, left'}")

            after = row(conn)
            receipt["after"][str(SID)] = after
            assert int(after["institution_id"]) == NEW_INST and after["publisher_id"] is None
            assert after["display_name"] == NEW_NAME
            print(f"  {SID}: {after['display_name']!r} inst={after['institution_id']} hp={after['homepage_url']}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
