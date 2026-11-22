"""oxjob #952 (support factory #2254): merge three duplicate DiVA member sources, ONE transaction on openalex-sources.

Written 2026-11-20 16:30 CT by #952's off-desk lane (#2263), which cannot reach the registry. NOT YET RUN. Guards assert
the before-state read on 2026-11-20 (public registry https://user.openalex.org/oaipmh-sets, saved
work/registry_endpoints_2026-11-15.json; Databricks openalex.sources.endpoint_to_source 20 Nov 16:15 CT; live API
/sources), so a changed row aborts the whole transaction.

Run on desk (dry run by default = rollback; read the printed receipt):
    cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py \
        ~/ox/oxjobs/working/repository-source-attribution-bugs/work/registry_fixes_952_diva_twins_2026-11-20.py   # rehearsal
    ... same command + --execute                                                                                   # commit
Save the printed JSON after RECEIPT as work/registry_fixes_952_diva_twins_receipt.json. Timing: any time EXCEPT while an
End 2 End's Sources_Snapshot task is running (05:00 UTC + ~5 min); it rides the next night whose snapshot follows it.

Why: three DiVA members each have two repository sources, one per feed, both serving the member's own
oai:DiVA.org:<code>-* records. The national-feed override rows (openalex.sources.aggregator_source_overrides, #952,
night 2026-11-20) route each member's records to one of them (the winner below); the other keeps the records its own
feed delivered, so the member's DiVA records are split across two sources (sized 20 Nov, live API corpus=all):
  hj  Jönköping: winner S4377196548 (feed 71795982e84f559815e http://hj.diva-portal.org/dice/oai, 14,436 works)
                 loser  S4306402397 (feed 76b7a89050faf3046b9 http://diva-portal.org/oai/hj/OAI, 4,598 works)
  oru Örebro:    winner S4306400154 (feed 077eb075e8431b5d1ea http://oru.diva-portal.org/dice/oai, 16,805 works)
                 loser  S4306400130 (feed e77f9ff197f99acfefa http://diva-portal.org/oai/oru/OAI, 2,182 works)
  sh  Södertörn: winner S4306401896 (feed 380d2f211dbed2147d0 http://diva-portal.org/oai/sh/OAI, 15,485 works)
                 loser  S4306402109 (feed a6b1ec64ce7c20b22cc http://sh.diva-portal.org/dice/oai, 45 works)
Each loser merges into its winner (standard merge = source_merge redirect, as eBiltegia on 15 Nov; the loser's feed
moves with it, so both feeds land on one source). Each winner gets the Borås-style name 'DiVA - <member> (<Swedish
name>)' (Jönköping: 'DiVA - Jönköping University', its Swedish name too), homepage https://<code>.diva-portal.org and country SE. ~6,800 works move (rider 952-diva-member-twins).

Revert: the receipt's `revert` list (one line per step) and `before` rows.
"""
import datetime as dt
import json
import sys
import uuid

sys.path.insert(0, "/Users/jasonpriem/ox/openalex-sources")
from sqlalchemy import text  # noqa: E402

from db import engine  # noqa: E402
from sources_lib import refresh_issns_column, resolve_issn_l  # noqa: E402

EXECUTE = "--execute" in sys.argv
JOB = 952
APPROVED = "support factory #2254 (standing authority); duplicate repository sources merge as eBiltegia (#952, 15 Nov)"

# endpoint id -> (expected pmh_url, expected source_id now)
EXPECT_EP = {
    "71795982e84f559815e": ("http://hj.diva-portal.org/dice/oai", 4377196548),
    "76b7a89050faf3046b9": ("http://diva-portal.org/oai/hj/OAI", 4306402397),
    "077eb075e8431b5d1ea": ("http://oru.diva-portal.org/dice/oai", 4306400154),
    "e77f9ff197f99acfefa": ("http://diva-portal.org/oai/oru/OAI", 4306400130),
    "380d2f211dbed2147d0": ("http://diva-portal.org/oai/sh/OAI", 4306401896),
    "a6b1ec64ce7c20b22cc": ("http://sh.diva-portal.org/dice/oai", 4306402109),
}
# (member code, loser, winner, institution, new winner name)
PAIRS = [
    ("hj", 4306402397, 4377196548, 94616838, "DiVA - Jönköping University"),
    ("oru", 4306400130, 4306400154, 26437253, "DiVA - Örebro University (Örebro universitet)"),
    ("sh", 4306402109, 4306401896, 129826125, "DiVA - Södertörn University (Södertörns högskola)"),
]
SOURCES = sorted({p[1] for p in PAIRS} | {p[2] for p in PAIRS})
EXPECT_NAME = {
    4377196548: "Jonkoping University Library (Jönköping University)",
    4306402397: "Jönköping University Publications (Jönköping University)",
    4306400154: "Örebro University Library (Örebro University)",
    4306400130: "Örebro University (Örebro University)",
    4306401896: "DiVA (Södertörn University)",
    4306402109: "Södertörns högskolas (Södertörn University)",
}

receipt = {"run_id": str(uuid.uuid4()), "oxjob": JOB, "execute": EXECUTE,
           "at": dt.datetime.now(dt.timezone.utc).isoformat(), "before": {}, "after": {}, "actions": [], "revert": []}


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def rows(conn, sql, args):
    return [{k: (None if v is None else str(v)) for k, v in r.items()} for r in conn.execute(text(sql), args).mappings()]


def upd_source(conn, sid, **fields):
    before = conn.execute(text(f"SELECT {', '.join(fields)} FROM sources WHERE id = :id"), {"id": sid}).mappings().one()
    sets = ", ".join(f"{k} = :{k}" for k in fields)
    n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"), {**fields, "id": sid}).rowcount
    assert n == 1, (sid, fields)
    log(f"source {sid}: {dict(before)} -> {fields}")
    back = ", ".join(f"{k} = {('NULL' if v is None else repr(v))}" for k, v in dict(before).items())
    receipt["revert"].append(f"UPDATE sources SET {back} WHERE id = {sid};")


def rename(conn, sid, new_name):
    old = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).one()
    if old.display_name == new_name:
        return
    alts = list(old.alternate_titles or [])
    if old.display_name and old.display_name not in alts:
        alts.append(old.display_name)
    conn.execute(text("UPDATE sources SET display_name = :n, display_name_before_override = :o, override_timestamp = now(), "
                      "alternate_titles = CAST(:a AS JSONB), updated_date = now() WHERE id = :id"),
                 {"n": new_name, "o": old.display_name, "a": json.dumps(alts, ensure_ascii=False), "id": sid})
    log(f"source {sid}: renamed {old.display_name!r} -> {new_name!r}")
    receipt["revert"].append(f"UPDATE sources SET display_name = {old.display_name!r} WHERE id = {sid}; "
                             f"(and drop it from alternate_titles)")


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in openalex-sources handoff/oxjob629-ticket-merges-2026-11-15.py."""
    rws = {r.id: r for r in conn.execute(
        text("SELECT id, display_name, alternate_titles, override_timestamp, issn_l FROM sources WHERE id IN (:l, :w)"),
        {"l": loser, "w": winner})}
    assert loser in rws and winner in rws, (loser, winner)
    assert rws[loser].override_timestamp is None, f"loser {loser} is curator-overridden"
    conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :l"), {"l": loser})
    moved = conn.execute(text("UPDATE source_issn SET source_id = :w, is_issn_l = FALSE WHERE source_id = :l"),
                         {"l": loser, "w": winner}).rowcount
    ep = conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
    dc = conn.execute(text("UPDATE source_datacite_id SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
    bm = conn.execute(text("INSERT INTO ojs_beacon_mint (source_id, edition, receipt, minted_at) "
                           "SELECT :w, edition, receipt, minted_at FROM ojs_beacon_mint WHERE source_id = :l "
                           "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner}).rowcount
    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                      "SELECT :w, curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :l "
                      "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner})
    conn.execute(text("DELETE FROM source_oa_override WHERE source_id = :l"), {"l": loser})
    issns = [r[0] for r in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": winner})]
    if issns and rws[winner].issn_l is None:
        conn.execute(text("UPDATE sources SET issn_l = :l WHERE id = :id"),
                     {"l": resolve_issn_l(conn, issns, member_only=True), "id": winner})
    conn.execute(text("UPDATE source_issn si SET is_issn_l = (si.issn IS NOT DISTINCT FROM s.issn_l) "
                      "FROM sources s WHERE s.id = :id AND si.source_id = :id"), {"id": winner})
    refresh_issns_column(conn, winner)
    conn.execute(text("INSERT INTO source_merge (loser_id, winner_id, rule, source_feed, detail) "
                      "VALUES (:l, :w, :r, 'zendesk', CAST(:d AS JSONB))"),
                 {"l": loser, "w": winner, "r": rule,
                  "d": json.dumps({**detail, "loser_name": rws[loser].display_name}, ensure_ascii=False)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1
    log(f"MERGED {loser} -> {winner} ({rule}): issns {moved}, endpoints {ep}, datacite {dc}, beacon {bm}")
    receipt["revert"].append(f"re-insert source {loser} from receipt.before.sources, move its endpoints back "
                             f"(receipt.before.endpoints), DELETE FROM source_merge WHERE loser_id = {loser};")


class DryRun(Exception):
    pass


def snapshot(conn):
    receipt["before"]["sources"] = rows(conn, "SELECT * FROM sources WHERE id = ANY(:i) ORDER BY id", {"i": SOURCES})
    receipt["before"]["endpoints"] = rows(conn, "SELECT * FROM oai_pmh_endpoint WHERE source_id = ANY(:i) ORDER BY id",
                                          {"i": SOURCES})


def guards(conn):
    got = {r["id"]: r for r in conn.execute(text("SELECT id, pmh_url, source_id FROM oai_pmh_endpoint WHERE id = ANY(:i)"),
                                            {"i": list(EXPECT_EP)}).mappings()}
    for eid, (url, sid) in EXPECT_EP.items():
        assert eid in got, f"endpoint {eid} missing"
        assert got[eid]["pmh_url"] == url, (eid, got[eid]["pmh_url"], url)
        assert got[eid]["source_id"] == sid, (eid, got[eid]["source_id"], sid)
    # every endpoint on these sources is one of the six (nothing else rides the merge)
    others = conn.execute(text("SELECT id, pmh_url, source_id FROM oai_pmh_endpoint WHERE source_id = ANY(:i) "
                               "AND NOT (id = ANY(:e))"), {"i": SOURCES, "e": list(EXPECT_EP)}).fetchall()
    assert not others, f"unexpected endpoints on these sources: {others}"
    have = {r.id: r for r in conn.execute(text("SELECT id, type, display_name, institution_id, issn_l FROM sources "
                                               "WHERE id = ANY(:i)"), {"i": SOURCES})}
    missing = set(SOURCES) - set(have)
    assert not missing, f"sources missing: {missing}"
    for code, loser, winner, inst, _ in PAIRS:
        for sid in (loser, winner):
            assert have[sid].type == "repository", (sid, have[sid].type)
            # names as read on the live API 20 Nov; a differing name is logged, not fatal (the endpoint guard is the key one)
            if have[sid].display_name != EXPECT_NAME[sid]:
                log(f"note: source {sid} name is {have[sid].display_name!r}, API showed {EXPECT_NAME[sid]!r}")
            assert have[sid].institution_id in (inst, None), (sid, have[sid].institution_id, inst)
            assert have[sid].issn_l is None, (sid, have[sid].issn_l)
        merged = conn.execute(text("SELECT loser_id, winner_id FROM source_merge WHERE loser_id = ANY(:i) OR winner_id = :l"),
                              {"i": [loser, winner], "l": loser}).fetchall()
        assert not merged, f"{code}: already in source_merge: {merged}"


def main():
    try:
        with engine.begin() as conn:
            snapshot(conn)
            guards(conn)
            for code, loser, winner, inst, name in PAIRS:
                merge(conn, loser, winner, "zendesk_952_diva_member_duplicate",
                      {"tickets": [8234, 25850, 25853], "oxjob": JOB, "approved_by": APPROVED,
                       "note": f"DiVA member {code}: two repository sources for one repository (one per feed); "
                               f"the national-feed override rows route {code} to {winner}"})
                rename(conn, winner, name)
                fields = {"homepage_url": f"https://{code}.diva-portal.org", "country_code": "SE"}
                if conn.execute(text("SELECT institution_id FROM sources WHERE id = :i"), {"i": winner}).scalar() is None:
                    fields["institution_id"] = inst
                upd_source(conn, winner, **fields)
            receipt["after"]["sources"] = rows(conn, "SELECT id, display_name, type, institution_id, homepage_url, country_code "
                                               "FROM sources WHERE id = ANY(:i) ORDER BY id", {"i": SOURCES})
            receipt["after"]["endpoints"] = rows(conn, "SELECT id, pmh_url, source_id, is_journal_host, status FROM oai_pmh_endpoint "
                                                 "WHERE id = ANY(:e) ORDER BY id", {"e": list(EXPECT_EP)})
            receipt["after"]["source_merge"] = rows(conn, "SELECT loser_id, winner_id, rule FROM source_merge WHERE loser_id = ANY(:i)",
                                                    {"i": [p[1] for p in PAIRS]})
            for code, loser, winner, _, _ in PAIRS:
                assert all(e["source_id"] == str(winner) for e in receipt["after"]["endpoints"]
                           if EXPECT_EP[e["id"]][1] in (loser, winner)), code
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back (pass --execute to commit)")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
