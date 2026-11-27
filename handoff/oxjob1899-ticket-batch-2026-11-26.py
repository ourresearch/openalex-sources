"""Registry fixes for oxjob #1899 (support factory #2254), written off desk 2026-11-26 21:13 CT (#2263, wake 1899-1126-2100-2).

Data: the manifest (first argument), #1899 work/registry_manifest_1899_2026-11-26.json. Every before-value in it is the
live API's on 26 Nov 21:05-21:11 CT; every target has its witness in the entry's `witness`.
  - 1 merge (ZD 28412): S7407121719 'Revista Interagir' (e-ISSN 3086-8629) -> S4387997840 'Revista Interagir'
    (ISSN-L 1809-5771). Same steps as merge() in handoff/oxjob1899-bmj-society-hosts-2026-11-24.py (desk ran it 24 Nov).
  - 23 entries: 13 repository hosts (premium ZD 28404; BAnQ-named S4306400638 held, see the manifest `held`), Progressive Economy (28403), Stacks Journal (28416), Interagir
    survivor (28412), Confluence name case (28424), Jurnal Identitas homepage (28450), 5 Pluto Journals titles (27630).
Order on desk: FIRST the publishers mint (work/d12_mint_package_2026-11-26.json `mint_sql`, P4404685483 + P4404685484),
then this script. If the mint did not run, set SKIP_MINT_HOSTS = True (S4210176529 and S4404660718 then keep no publisher;
their homepage/name edits still apply).

Guards (an entry whose guard fails is rolled back alone, SAVEPOINT, and listed under `skipped`):
  - identity: registry display_name must match `expect_name` (normalize_name) or already be the target name;
  - drift: every `before` field must still hold the API's value, or already hold the target ("done");
  - repositories: host is institution_id, so the entry may only set institution_id.

Run on desk (dry run by default; ONE transaction):
    cp <this file> ~/ox/openalex-sources/handoff/ && cd ~/ox/openalex-sources && \
    python3 ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/run_registry.py \
      handoff/oxjob1899-ticket-batch-2026-11-26.py \
      ~/ox/oxjobs/working/source-record-corrections-from-tickets/work/registry_manifest_1899_2026-11-26.json [--execute]
Save the JSON after RECEIPT as handoff/oxjob1899-ticket-batch-2026-11-26-receipt.json and in #1899's work/.
Works re-stamped: ~185.5K (repositories 182,930); pre-clear recorded off desk with oxpreclear --add 223000.

Revert: the receipt's `before` holds every touched row. Entries: UPDATE sources SET <field> = <before> for each field in
the entry's decision (alternate_titles back to the before list; display_name_before_override / override_timestamp too).
Merge: re-insert the loser's `before` row (OVERRIDING SYSTEM VALUE), UPDATE source_issn SET source_id = 7407121719,
is_issn_l = TRUE WHERE issn = '3086-8629', UPDATE oai_pmh_endpoint SET source_id = 7407121719 WHERE id IN (<the
receipt's loser _endpoints>), DELETE FROM source_merge WHERE loser_id = 7407121719, refresh_issns_column for both, and
drop the alternate titles the merge added.
"""
import json
import os
import re
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from sqlalchemy import text

from db import engine
from sources_lib import refresh_datacite_column, normalize_name, refresh_issns_column, resolve_issn_l

ARGS = sys.argv[1:]
EXECUTE = "--execute" in ARGS
POSITIONAL = [a for a in ARGS if not a.startswith("--")]
JOB = 1899
APPROVED = "support factory #2254 (Jason 2026-11-15 yes); D12 2026-11-17 (RULINGS.md § 3 Publishers)"

SKIP_MINT_HOSTS = False   # desk: True if the publishers mint (P4404685483, P4404685484) did not run
SKIP_ENTRIES = set()      # desk: e.g. {"S7407077881"} if journals.imist.ma does not print 'Confluence Journal of Multidisciplinary Studies'
MINTED = {4404685483, 4404685484}
SETTABLE = {"publisher_id", "publisher", "institution_id", "homepage_url", "display_name"}

receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "job": JOB, "approved": APPROVED,
           "manifest": POSITIONAL[0] if POSITIONAL else None, "before": {}, "actions": [], "decisions": {},
           "skipped": {}, "after": {}}


class DryRun(Exception):
    pass


class Skip(Exception):
    pass


def log(msg):
    print(msg, flush=True)
    receipt["actions"].append(msg)


def nurl(u):
    if not u:
        return None
    u = re.sub(r"^https?://", "", u.strip().lower())
    return re.sub(r"^www\.", "", u).rstrip("/")


def same(field, a, b):
    if field == "homepage_url":
        return nurl(a) == nurl(b)
    if field == "display_name":
        return (a or "").strip() == (b or "").strip()
    return (a if a not in ("", None) else None) == (b if b not in ("", None) else None)


def snapshot(conn, ids):
    for r in conn.execute(text("SELECT * FROM sources WHERE id = ANY(:i)"), {"i": list(ids)}).mappings():
        row = {k: (None if v is None else str(v)) for k, v in r.items()}
        row["_issns"] = [x[0] for x in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :i"), {"i": r["id"]})]
        row["_endpoints"] = [x[0] for x in conn.execute(text("SELECT id FROM oai_pmh_endpoint WHERE source_id = :i"), {"i": r["id"]})]
        receipt["before"][str(r["id"])] = row


def add_alt_titles(conn, sid, titles):
    row = conn.execute(text("SELECT display_name, alternate_titles FROM sources WHERE id = :id"), {"id": sid}).fetchone()
    alts = list(row.alternate_titles or [])
    have = {t.strip().lower() for t in alts + [row.display_name or ""]}
    gained = []
    for t in titles:
        if t and t.strip() and t.strip().lower() not in have:
            gained.append(t.strip())
            have.add(t.strip().lower())
    if gained:
        conn.execute(text("UPDATE sources SET alternate_titles = CAST(:a AS JSONB), updated_date = now() WHERE id = :id"),
                     {"a": json.dumps(alts + gained, ensure_ascii=False), "id": sid})
        log(f"alternate_titles {sid} += {gained}")
    return gained


def merge(conn, loser, winner, rule, detail):
    """Same steps as merge() in handoff/oxjob1899-bmj-society-hosts-2026-11-24.py (desk ran it 24 Nov, e5a9fd1)."""
    rows = {r.id: r for r in conn.execute(
        text("SELECT id, display_name, alternate_titles, override_timestamp, issn_l FROM sources WHERE id IN (:l, :w)"),
        {"l": loser, "w": winner})}
    if winner not in rows:
        raise Skip(f"winner {winner} missing")
    if loser not in rows:
        done = conn.execute(text("SELECT winner_id FROM source_merge WHERE loser_id = :l"), {"l": loser}).scalar()
        if done == winner:
            log(f"MERGE {loser} -> {winner} already done")
            return "done"
        raise Skip(f"loser {loser} missing and not merged into {winner} (merged into {done})")
    if rows[loser].override_timestamp is not None:
        raise Skip(f"loser {loser} is curator-overridden")
    conn.execute(text("UPDATE sources SET issn_l = NULL WHERE id = :l"), {"l": loser})
    moved = conn.execute(text("UPDATE source_issn SET source_id = :w, is_issn_l = FALSE WHERE source_id = :l"),
                         {"l": loser, "w": winner}).rowcount
    ep = conn.execute(text("UPDATE oai_pmh_endpoint SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
    dc = conn.execute(text("UPDATE source_datacite_id SET source_id = :w WHERE source_id = :l"), {"l": loser, "w": winner}).rowcount
    if dc:
        refresh_datacite_column(conn, winner)
    bm = conn.execute(text("INSERT INTO ojs_beacon_mint (source_id, edition, receipt, minted_at) "
                           "SELECT :w, edition, receipt, minted_at FROM ojs_beacon_mint WHERE source_id = :l "
                           "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner}).rowcount
    conn.execute(text("INSERT INTO source_oa_override (source_id, curated_is_oa, curated_flip_year, note) "
                      "SELECT :w, curated_is_oa, curated_flip_year, note FROM source_oa_override WHERE source_id = :l "
                      "ON CONFLICT DO NOTHING"), {"l": loser, "w": winner})
    conn.execute(text("DELETE FROM source_oa_override WHERE source_id = :l"), {"l": loser})
    add_alt_titles(conn, winner, [rows[loser].display_name] + list(rows[loser].alternate_titles or []))
    issns = [r[0] for r in conn.execute(text("SELECT issn FROM source_issn WHERE source_id = :id"), {"id": winner})]
    if issns and rows[winner].issn_l is None:
        issn_l = resolve_issn_l(conn, issns, member_only=True)
        conn.execute(text("UPDATE sources SET issn_l = :l WHERE id = :id"), {"l": issn_l, "id": winner})
    conn.execute(text("UPDATE source_issn si SET is_issn_l = (si.issn IS NOT DISTINCT FROM s.issn_l) "
                      "FROM sources s WHERE s.id = :id AND si.source_id = :id"), {"id": winner})
    refresh_issns_column(conn, winner)
    conn.execute(text("INSERT INTO source_merge (loser_id, winner_id, rule, source_feed, detail) "
                      "VALUES (:l, :w, :r, 'zendesk', CAST(:d AS JSONB))"),
                 {"l": loser, "w": winner, "r": rule, "d": json.dumps({**detail, "loser_name": rows[loser].display_name},
                                                                        ensure_ascii=False)})
    for t in ("source_works_count", "source_publication_years"):
        conn.execute(text(f"DELETE FROM {t} WHERE source_id = :l"), {"l": loser})
    n = conn.execute(text("DELETE FROM sources WHERE id = :l"), {"l": loser}).rowcount
    assert n == 1
    conn.execute(text("UPDATE sources SET updated_date = now() WHERE id = :w"), {"w": winner})
    log(f"MERGED {loser} -> {winner} ({rule}): issns moved {moved}, endpoints {ep}, datacite {dc}, beacon {bm}")
    return [loser, winner]


def apply_entry(conn, e):
    sid, st, before = e["source_id"], dict(e["set"]), e.get("before", {})
    assert set(st) <= SETTABLE, (e["entry"], set(st) - SETTABLE)
    if e["entry"] in SKIP_ENTRIES:
        raise Skip("desk SKIP flag")
    if SKIP_MINT_HOSTS and st.get("publisher_id") in MINTED:
        st.pop("publisher_id")
        st.pop("publisher", None)
        log(f"{e['entry']}: publisher left (SKIP_MINT_HOSTS)")
    row = conn.execute(text("SELECT id, display_name, type, publisher_id, publisher, institution_id, homepage_url, "
                            "display_name_before_override FROM sources WHERE id = :id FOR UPDATE"), {"id": sid}).mappings().fetchone()
    if row is None:
        raise Skip("source missing (merged or deleted)")
    row = dict(row)
    names = {normalize_name(e["expect_name"])} | ({normalize_name(st["display_name"])} if "display_name" in st else set())
    if normalize_name(row["display_name"]) not in names:
        raise Skip(f"identity: registry name {row['display_name']!r}, expected {e['expect_name']!r}")
    if e["type"] == "repository":
        if row["type"] != "repository" or set(st) != {"institution_id"}:
            raise Skip(f"repository entry must set institution_id only (registry type {row['type']})")
    dec, cols = {}, {}
    for k, v in st.items():
        if k == "publisher":
            continue  # follows publisher_id
        if same(k, row[k], v):
            dec[k] = "done"
            continue
        if k in before and not same(k, row[k], before[k]):
            raise Skip(f"{k} drift: registry {row[k]!r}, API showed {before[k]!r}, target {v!r}")
        cols[k] = v
        dec[k] = [row[k], v]
    if "publisher_id" in cols and "publisher" in st:
        cols["publisher"] = st["publisher"]
    if "display_name" in cols:
        cols["display_name_before_override"] = row["display_name_before_override"] or row["display_name"]
        cols["override_timestamp"] = datetime.now(timezone.utc)
    if cols:
        sets = ", ".join(f"{k} = :{k}" for k in cols)
        n = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :id"), {**cols, "id": sid}).rowcount
        assert n == 1, (sid, n)
        log(f"UPDATE {e['entry']} (tickets {e['tickets']}): " + ", ".join(f"{k} {row.get(k)!r} -> {v!r}" for k, v in cols.items()
                                                                          if k not in ("override_timestamp",)))
    if e.get("alt_add"):
        dec["alternate_titles_added"] = add_alt_titles(conn, sid, e["alt_add"]) or "done"
    return dec


def main():
    man = json.load(open(POSITIONAL[0], encoding="utf-8"))
    entries = man["entries"]
    assert len({e["entry"] for e in entries}) == len(entries), "duplicate entries"
    ids = {e["source_id"] for e in entries} | {m["loser"] for m in man["merges"]} | {m["winner"] for m in man["merges"]}
    try:
        with engine.begin() as conn:
            snapshot(conn, ids)
            for p in {e["set"]["publisher_id"] for e in entries if "publisher_id" in e["set"]} - MINTED:
                used = conn.execute(text("SELECT COUNT(*) FROM sources WHERE publisher_id = :p"), {"p": p}).scalar()
                assert used > 0, f"P{p} hosts no source in the registry: wrong id?"
                log(f"P{p} already hosts {used} registry sources")
            for m in man["merges"]:
                key = f"merge S{m['loser']}->S{m['winner']}"
                try:
                    with conn.begin_nested():
                        receipt["decisions"][key] = merge(conn, m["loser"], m["winner"], "zendesk_1899_same_journal_two_issns",
                                                          {"tickets": m["tickets"], "oxjob": JOB, "approved_by": APPROVED,
                                                           "note": m["note"]})
                except Skip as s:
                    receipt["skipped"][key] = str(s)
                    log(f"SKIP {key}: {s}")
            for e in entries:
                try:
                    with conn.begin_nested():
                        receipt["decisions"][e["entry"]] = apply_entry(conn, e)
                except Skip as s:
                    receipt["skipped"][e["entry"]] = str(s)
                    log(f"SKIP {e['entry']} (tickets {e['tickets']}): {s}")
            for r in conn.execute(text("SELECT id, display_name, type, issn_l, issns, publisher_id, publisher, institution_id, "
                                       "homepage_url, alternate_titles FROM sources WHERE id = ANY(:i) ORDER BY id"),
                                  {"i": list(ids)}).mappings():
                receipt["after"][str(r["id"])] = {k: (None if v is None else str(v)) for k, v in r.items()}
            changed = [k for k, d in receipt["decisions"].items()
                       if (isinstance(d, list)) or (isinstance(d, dict) and any(isinstance(v, list) and v for v in d.values()))]
            receipt["changed"] = changed
            log(f"entries {len(entries)} + merges {len(man['merges'])}: changed {len(changed)}, "
                f"skipped {len(receipt['skipped'])}")
            if not EXECUTE:
                raise DryRun()
        log("COMMITTED")
    except DryRun:
        log("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
