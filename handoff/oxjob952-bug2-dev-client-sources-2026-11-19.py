"""oxjob #952 Bug 2 (support factory #2254): DataCite client-account sources named "-DEV" / "-PROD" that are real
repositories. ONE transaction on openalex-sources. Written 2026-11-19 by #952's off-desk lane (#2263), NOT YET RUN.

Found 19 Nov (receipts work/bug2_dev_sources_2026-11-19.json): every "-DEV" DataCite client that holds works mints
FINDABLE production DOIs that resolve to a live site (DataCite API, 6 sampled DOIs each). The "DEV" is only the client
account's name, so the source is real and misnamed; three of them are the same repository as an OAI-harvested source.

Run on desk (dry run by default = rollback; read the printed receipt):
    cd ~/ox/openalex-sources
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py \
        ~/ox/oxjobs/working/repository-source-attribution-bugs/work/registry_fixes_952_bug2_2026-11-19.py            # rehearsal
    ... same command + --execute                                                                                        # commit
Save the printed JSON after RECEIPT as work/registry_fixes_952_bug2_receipt.json. Timing: any time EXCEPT while an
End 2 End's Sources_Snapshot task runs (05:00 UTC + ~5 min); it rides the next night whose snapshot follows it.
Size for that night's rider/pre-clear: ~64.5K works change a location's source (merges) + ~6.4K works whose source
is renamed = ~71K works re-stamped (no deletions).

Steps (precedent: this job's eBiltegia merge, 15 Nov: a DataCite client source for the same repository as an OAI
source merges into it, standard merge = source_merge redirect):
 1. Merges (DOIs resolve to the repository the OAI source harvests):
    S7407060097 McGill-DEV (client iyfw.kddaax, 10.82308 -> mcgill.scholaris.ca; 55,971 works corpus=all)
        -> S4306401568 eScholarship@McGill (feed 1281ef6cd2377abeb35 https://mcgill.scholaris.ca/server/oai/request)
    S7407060140 UREG-DEV (ltjy.pkxggm, 10.82465 -> uregina.scholaris.ca; 7,551)
        -> S4306402576 oURspace (University of Regina) (feed fa9621202fc0bd3a29d https://uregina.scholaris.ca/...)
    S7407052692 USherbrooke-PROD (scci.llymnz, 10.71892 -> usherbrooke.scholaris.ca; 948)
        -> S4306401321 Knowledge UdeS (feed 4bd6a64098714b9318e https://usherbrooke.scholaris.ca/...)
    Each winner gets its scholaris homepage if it has none.
 2. Renames (no OAI source for the site; name = the DOIs' DataCite publisher / the site's own name; homepage = the
    host the DOIs resolve to):
    S7407051997 AcadiaU-DEV -> 'Scholar (Acadia University)'  https://scholar.acadiau.ca   (5,251 works)
    S7407051303 ILDS-DEV -> 'ILDS Derm Gallery (International League of Dermatological Societies)' https://ildsdermgallery.org (381)
    S7407059868 YGS-DEV -> 'Yukon Geological Survey Publications'  https://data.geology.gov.yk.ca (48)
    S7407060025 CRKN-DEV -> 'Canadian Research Knowledge Network (CRKN)'  (homepage kept) (42)
    S7407053414 MRNF-DEV -> 'Ministère des Ressources naturelles et des Forêts (Québec)'  https://mrnf.gouv.qc.ca (25)
    S7407060626 CoRE-DEV -> 'Canadian Centre of Recovery Excellence'  https://recoveryexcellence.org (20)
    S7407053548 PSC-DEV -> 'Public Safety Canada'  https://www.publicsafety.gc.ca (1)
    S7407060204 AIND Prod Repo -> 'Neuron Morphology Community Portal (Allen Institute for Neural Dynamics)'
        https://morphology.allenneuraldynamics.org (641)
 Not here: the three DataCite TEST client sources (S7407051295, S7407051175, S7407052010) lose their 20 works through
 openalex.works.location_denylist (endpoint oxjob952_datacite_test_clients, written 19 Nov 11:05 CT); the zero-work
 test/dev sources stay (they show nothing). The sync guard (don't mint from test clients) is an openalex-sources code
 change: PLAN.md Handoff.

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
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); #952 eBiltegia precedent (DataCite client source into its OAI source)"

# loser -> (expected name, expected datacite client, winner, expected winner name prefix, winner feed id, homepage)
MERGES = [
    (7407060097, "McGill-DEV", "iyfw.kddaax", 4306401568, "eScholarship@McGill", "1281ef6cd2377abeb35", "https://mcgill.scholaris.ca"),
    (7407060140, "UREG-DEV", "ltjy.pkxggm", 4306402576, "oURspace", "fa9621202fc0bd3a29d", "https://uregina.scholaris.ca"),
    (7407052692, "USherbrooke-PROD", "scci.llymnz", 4306401321, "Knowledge UdeS", "4bd6a64098714b9318e", "https://usherbrooke.scholaris.ca"),
]
# source -> (expected name, expected client, new name, homepage or None to keep)
RENAMES = [
    (7407051997, "AcadiaU-DEV", "krep.szjagu", "Scholar (Acadia University)", "https://scholar.acadiau.ca"),
    (7407051303, "ILDS-DEV", "ezwg.kjqcfa", "ILDS Derm Gallery (International League of Dermatological Societies)", "https://ildsdermgallery.org"),
    (7407059868, "YGS-DEV", "bmwp.qicvpx", "Yukon Geological Survey Publications", "https://data.geology.gov.yk.ca"),
    (7407060025, "CRKN-DEV", "gyxb.cpdymx", "Canadian Research Knowledge Network (CRKN)", None),
    (7407053414, "MRNF-DEV", "xduf.kpprwo", "Ministère des Ressources naturelles et des Forêts (Québec)", "https://mrnf.gouv.qc.ca"),
    (7407060626, "CoRE-DEV", "xflw.rqhvyl", "Canadian Centre of Recovery Excellence", "https://recoveryexcellence.org"),
    (7407053548, "PSC-DEV", "zcut.ceatnc", "Public Safety Canada", "https://www.publicsafety.gc.ca"),
    (7407060204, "AIND Prod Repo", "ohmd.aind", "Neuron Morphology Community Portal (Allen Institute for Neural Dynamics)", "https://morphology.allenneuraldynamics.org"),
]
SOURCES = sorted({m[0] for m in MERGES} | {m[3] for m in MERGES} | {r[0] for r in RENAMES})

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


def refresh_datacite(conn, sid):
    # same as openalex-sources jobs/sync_datacite_clients._refresh_datacite_jsonb (as #954 used it)
    conn.execute(text("UPDATE sources SET datacite_ids = COALESCE((SELECT jsonb_agg(datacite_id ORDER BY datacite_id) "
                      "FROM source_datacite_id WHERE source_id = :id), '[]'::jsonb), "
                      "datacite_id = (SELECT MIN(datacite_id) FROM source_datacite_id WHERE source_id = :id), "
                      "updated_date = now() WHERE id = :id"), {"id": sid})


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
    receipt["revert"].append(f"re-insert source {loser} from receipt.before, move its datacite client back "
                             f"(UPDATE source_datacite_id SET source_id = {loser} WHERE datacite_id = <client>), "
                             f"DELETE FROM source_merge WHERE loser_id = {loser}; refresh datacite_ids on both")


class DryRun(Exception):
    pass


def snapshot(conn):
    receipt["before"]["sources"] = rows(conn, "SELECT * FROM sources WHERE id = ANY(:i) ORDER BY id", {"i": SOURCES})
    receipt["before"]["datacite_links"] = rows(conn, "SELECT * FROM source_datacite_id WHERE source_id = ANY(:i) ORDER BY source_id",
                                               {"i": SOURCES})
    receipt["before"]["endpoints"] = rows(conn, "SELECT id, pmh_url, source_id, status FROM oai_pmh_endpoint WHERE source_id = ANY(:i) ORDER BY id",
                                          {"i": SOURCES})


def guards(conn):
    have = {r.id: r for r in conn.execute(text("SELECT id, display_name, type, override_timestamp FROM sources WHERE id = ANY(:i)"),
                                          {"i": SOURCES})}
    missing = set(SOURCES) - set(have)
    assert not missing, f"sources missing: {missing}"
    for sid in SOURCES:
        assert have[sid].type == "repository", (sid, have[sid].type)
    def client_of(sid):
        return sorted(r[0] for r in conn.execute(text("SELECT datacite_id FROM source_datacite_id WHERE source_id = :s"), {"s": sid}))
    for loser, name, client, winner, wname, feed, _hp in MERGES:
        assert have[loser].display_name == name, (loser, have[loser].display_name, name)
        assert have[loser].override_timestamp is None, f"{loser} is curator-overridden"
        assert client_of(loser) == [client], (loser, client_of(loser), client)
        assert have[winner].display_name.startswith(wname), (winner, have[winner].display_name)
        ep = conn.execute(text("SELECT source_id FROM oai_pmh_endpoint WHERE id = :e"), {"e": feed}).scalar()
        assert ep == winner, (feed, ep, winner)
        n = conn.execute(text("SELECT count(*) FROM oai_pmh_endpoint WHERE source_id = :s"), {"s": loser}).scalar()
        assert n == 0, f"loser {loser} has {n} endpoints"
    for sid, name, client, new, _hp in RENAMES:
        assert have[sid].display_name == name, (sid, have[sid].display_name, name)
        assert have[sid].override_timestamp is None, f"{sid} is curator-overridden"
        assert client_of(sid) == [client], (sid, client_of(sid), client)
        hit = conn.execute(text("SELECT id FROM sources WHERE lower(display_name) = lower(:n) AND id <> :s"), {"n": new, "s": sid}).fetchall()
        assert not hit, f"another source is named {new!r}: {hit}"


def main():
    try:
        with engine.begin() as conn:
            snapshot(conn)
            guards(conn)

            # 1. merges
            for loser, name, client, winner, _wname, feed, hp in MERGES:
                merge(conn, loser, winner, "oxjob952_datacite_client_duplicate",
                      {"oxjob": JOB, "approved_by": APPROVED,
                       "note": f"DataCite client {client} ({name}) mints DOIs that resolve to the repository harvested by feed {feed}"})
                refresh_datacite(conn, winner)
                if conn.execute(text("SELECT homepage_url FROM sources WHERE id = :i"), {"i": winner}).scalar() is None:
                    upd_source(conn, winner, homepage_url=hp)

            # 2. renames
            for sid, _name, _client, new, hp in RENAMES:
                rename(conn, sid, new)
                if hp and conn.execute(text("SELECT homepage_url FROM sources WHERE id = :i"), {"i": sid}).scalar() is None:
                    upd_source(conn, sid, homepage_url=hp)

            survivors = [s for s in SOURCES if s not in {m[0] for m in MERGES}]
            receipt["after"]["sources"] = rows(conn, "SELECT id, display_name, type, institution_id, homepage_url, datacite_ids "
                                               "FROM sources WHERE id = ANY(:i) ORDER BY id", {"i": survivors})
            receipt["after"]["merges"] = rows(conn, "SELECT loser_id, winner_id, rule FROM source_merge WHERE loser_id = ANY(:i)",
                                              {"i": [m[0] for m in MERGES]})
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN: rolled back (pass --execute to commit)")
    print("RECEIPT")
    print(json.dumps(receipt, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
