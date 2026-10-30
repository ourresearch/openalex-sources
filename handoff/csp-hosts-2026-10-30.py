"""Registry edits for Canadian Science Publishing journals (ZD 22906, oxjob #1899; audit 2026-10-30).
Kyle: "right the switch" (run it) and Canadian Journal of Research -> CSP "since CSP owns the backfile", 2026-10-30.

CSP's journals are split across two publisher entities: P4310312929 Canadian Science Publishing and
P4310313060 NRC Research Press (CSP's name before 2010). Checked 2026-10-30 against the CSP journal list
(cdnsciencepub.com/action/showPublications?pubType=journal, 25 titles), Crossref member 155 (prefix 10.1139)
and each journal's about page. Evidence table: ~/MacDemes/research/csp-publisher-sources/audit.csv.

  1 NRC Research Press -> CSP   13 sources (11 current journals + CJ Genetics and Cytology, CJ Applied Physiology)
  2 CMA -> CSP                  S141047287 Journal of Psychiatry and Neuroscience (CSP publishes it since 2025:
                                CSP site + editorial office, Crossref publisher CSP, 10.1139 DOIs from 2025)
  3 NRC (agency) -> CSP         S4210176233 Canadian Journal of Research 1929-50 (Crossref publisher CSP,
                                10.1139 DOIs, same as its CSP-hosted successors); name de-duplicated
  4 CSP -> Elsevier             S4210183195 Geomatica (CSP run ends v75, Dec 2021; Elsevier since 2024,
                                Crossref 1195-1036 publisher Elsevier, 10.1016/j.geomat)
  5 NRC Research Press -> owner S142396420 Canadian Aeronautics and Space Journal -> I4210163105 CASI (Crossref
                                member 3549); S950063636 Journal of Student Science and Technology -> no host
                                (Foundation for Student Science and Technology, Crossref member 5179, has no entity)
  6 homepages                   current journals -> https://cdnsciencepub.com/journal/<code> (old nrc.ca,
                                pubs.nrc-cnrc.gc.ca, pubs.aic.ca, nrcresearchpress.com URLs); CJ Botany -> /loi/cjb;
                                CASJ and JSST cleared (pubs.casi.ca dead; cysjournal.ca and fsst.ca are casino spam)

After this, P4310313060 hosts nothing. Tombstoning it into P4310312929 is a separate Databricks step
(openalex.publishers.publishers.merge_into_id), not done here.

Run from the openalex-sources checkout (dry run by default; ONE transaction):
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py handoff/csp-hosts-2026-10-30.py [--execute]
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
TICKET = 22906
APPROVED = "kyle 2026-10-30"

P_CSP, P_NRCRP, P_NRC, P_CMA, P_ELSEVIER = 4310312929, 4310313060, 4310312892, 4310320072, 4310320990
I_CASI = 4210163105
CDN = "https://cdnsciencepub.com/journal/"

# source id: (expected name, expected current publisher_id, new publisher_id, new institution_id,
#             new homepage (None = leave, "" = clear), new display name or None)
EDITS = {
    9189650:    ("Canadian Journal of Chemistry", P_NRCRP, P_CSP, None, CDN + "cjc", None),
    146329739:  ("Canadian Journal of Zoology", P_NRCRP, P_CSP, None, CDN + "cjz", None),
    173335977:  ("Canadian Journal of Physics", P_NRCRP, P_CSP, None, CDN + "cjp", None),
    82722058:   ("Canadian Journal of Physiology and Pharmacology", P_NRCRP, P_CSP, None, CDN + "cjpp", None),
    115310785:  ("Canadian Journal of Forest Research", P_NRCRP, P_CSP, None, CDN + "cjfr", None),
    4305354:    ("Canadian Journal of Earth Sciences", P_NRCRP, P_CSP, None, CDN + "cjes", None),
    105963835:  ("Canadian Geotechnical Journal", P_NRCRP, P_CSP, None, CDN + "cgj", None),
    10380613:   ("Canadian Journal of Civil Engineering", P_NRCRP, P_CSP, None, CDN + "cjce", None),
    170991295:  ("Genome", P_NRCRP, P_CSP, None, CDN + "gen", None),
    159551863:  ("Biochemistry and Cell Biology", P_NRCRP, P_CSP, None, CDN + "bcb", None),
    173941543:  ("Applied Physiology Nutrition and Metabolism", P_NRCRP, P_CSP, None, CDN + "apnm",
                 "Applied Physiology, Nutrition, and Metabolism"),
    4210187997: ("Canadian Journal of Genetics and Cytology", P_NRCRP, P_CSP, None, None, None),
    184552281:  ("Canadian Journal of Applied Physiology", P_NRCRP, P_CSP, None, None, None),
    141047287:  ("Journal of Psychiatry and Neuroscience", P_CMA, P_CSP, None, CDN + "jpn", None),
    4210176233: ("Canadian Journal of Research/Canadian journal of research", P_NRC, P_CSP, None, None,
                 "Canadian Journal of Research"),
    4210183195: ("GEOMATICA", P_CSP, P_ELSEVIER, None, "https://www.sciencedirect.com/journal/geomatica", "Geomatica"),
    142396420:  ("Canadian aeronautics and space journal", P_NRCRP, None, I_CASI, "",
                 "Canadian Aeronautics and Space Journal"),
    950063636:  ("Journal of Student Science and Technology", P_NRCRP, None, None, "", None),
    # homepage only (already on CSP)
    182870142:  ("Canadian Journal of Microbiology", P_CSP, P_CSP, None, CDN + "cjm", None),
    57183551:   ("Canadian Journal of Fisheries and Aquatic Sciences", P_CSP, P_CSP, None, CDN + "cjfas", None),
    34720450:   ("Canadian Journal of Plant Science", P_CSP, P_CSP, None, CDN + "cjps", None),
    54036783:   ("Canadian Journal of Animal Science", P_CSP, P_CSP, None, CDN + "cjas", None),
    204002359:  ("Canadian Journal of Soil Science", P_CSP, P_CSP, None, CDN + "cjss", None),
    3690700:    ("Transactions of the Canadian Society for Mechanical Engineering", P_CSP, P_CSP, None, CDN + "tcsme", None),
    118554560:  ("Botany", P_CSP, P_CSP, None, CDN + "cjb", None),
    79420083:   ("Environmental Reviews", P_CSP, P_CSP, None, CDN + "er", None),
    4220651326: ("Drone Systems and Applications", P_CSP, P_CSP, None, CDN + "dsa", None),
    4210188060: ("Canadian Journal of Botany", P_CSP, P_CSP, None, "https://cdnsciencepub.com/loi/cjb", None),
}
# publisher text for sources with no host entity (registry `publisher` column)
PUBLISHER_TEXT = {950063636: "Foundation for Student Science and Technology",
                  142396420: "Canadian Aeronautics and Space Institute"}

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "ticket": TICKET,
           "approved_by": APPROVED, "before": {}, "actions": [], "skipped": {}, "after": {}}


class DryRun(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def row(conn, sid):
    r = conn.execute(text("SELECT * FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
    return {k: (None if v is None else str(v)) for k, v in r.items()} if r else None


def publisher_text(conn, pid):
    """The registry's most common `publisher` string among sources already on this entity."""
    return conn.execute(text("SELECT publisher FROM sources WHERE publisher_id = :p AND publisher IS NOT NULL "
                             "GROUP BY publisher ORDER BY COUNT(*) DESC LIMIT 1"), {"p": pid}).scalar()


def add_alt_title(conn, sid, title):
    r = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(r.alternate_titles or [])
    if title and title.strip().lower() not in {t.strip().lower() for t in alts + [r.display_name or ""]}:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB) WHERE id = :id"),
                     {"a": json.dumps(alts + [title]), "id": sid})
        log(f"alternate_titles {sid} += {title!r}")


def main():
    assert not (EXECUTE and APPROVED is None), "set APPROVED before --execute"
    try:
        with engine.begin() as conn:
            texts = {p: publisher_text(conn, p) for p in (P_CSP, P_ELSEVIER)}
            assert texts[P_CSP] and texts[P_ELSEVIER], texts
            log(f"publisher text: CSP {texts[P_CSP]!r}, Elsevier {texts[P_ELSEVIER]!r}")
            for sid, (name, cur_pub, new_pub, new_inst, home, new_name) in EDITS.items():
                before = row(conn, sid)
                assert before is not None, f"source {sid} missing"
                receipt["before"][str(sid)] = before
                # Merge losers are deleted (post-D1), so a present row is a live source.
                assert normalize_name(before["display_name"]) in (normalize_name(name), normalize_name(new_name or name)), \
                    (sid, before["display_name"], name)
                have_pub = None if before["publisher_id"] is None else int(before["publisher_id"])
                # Drift guard: expected old value or already at the new one; anything else, stop and look.
                assert have_pub in (cur_pub, new_pub), f"{sid} publisher_id drifted: {have_pub}"
                if new_pub is None and new_inst is None:
                    assert before["institution_id"] is None, f"{sid} has institution_id {before['institution_id']}"
                sets = {}
                if have_pub != new_pub:
                    sets["publisher_id"] = new_pub
                    sets["publisher"] = texts.get(new_pub) or PUBLISHER_TEXT[sid]
                if new_inst is not None and before["institution_id"] != str(new_inst):
                    sets["institution_id"] = new_inst
                if home is not None and (before["homepage_url"] or "") != home:
                    sets["homepage_url"] = home or None
                if new_name and before["display_name"] != new_name:
                    sets.update(display_name=new_name, display_name_before_override=before["display_name"],
                                override_timestamp=datetime.now(timezone.utc))
                if not sets:
                    receipt["skipped"][str(sid)] = "already done"
                    continue
                cols = ", ".join(f"{k} = :{k}" for k in sets)
                n = conn.execute(text(f"UPDATE sources SET {cols}, updated_date = now() WHERE id = :id"),
                                 {**sets, "id": sid}).rowcount
                assert n == 1, sid
                shown = {k: (str(v) if k == "override_timestamp" else v) for k, v in sets.items()}
                log(f"UPDATE sources {sid} ({name}): {shown}")
                if new_name and before["display_name"] != new_name:
                    add_alt_title(conn, sid, before["display_name"])
                after = row(conn, sid)
                receipt["after"][str(sid)] = {k: after[k] for k in (
                    "display_name", "publisher", "publisher_id", "institution_id", "homepage_url")}
            left = conn.execute(text("SELECT id, display_name FROM sources WHERE publisher_id = :p"),
                                {"p": P_NRCRP}).fetchall()
            log(f"sources still on P{P_NRCRP} NRC Research Press: {[(r.id, r.display_name) for r in left]}")
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
