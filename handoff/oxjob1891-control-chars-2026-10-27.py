"""Source names without control characters (oxjob #1891, 2026-10-27).

1,649 sources carried C1 control characters in display_name, most of them MARC "non-sort" marks around a leading
article that a library-catalogue import copied in: The Cryosphere was '\\x98The \\x9ccryosphere' on its page, in every
work's source name, in exports and in search. 144 more carried them in alternate_titles and 78 in the publisher string.
The plan (oxjob1891-control-chars-2026-10-27.json, built by oxjob #1891 work/plan.py from the live rows) applies
sources_lib.clean_control_chars(decode_mojibake=False): MARC marks dropped, a lone Windows-1252 dash or quote becomes
that character, nothing else. No letter or digit changes, so walden's name keys stay the same and no location loses
the source it is linked to by name. Where Crossref's own title for one of the source's ISSNs (crossref_journal)
differs from the cleaned name in letter case only, Crossref's casing (44: 'The Cryosphere').
Left as they are (14): sources whose cleaned name would take another source's unique exact name, or whose
name-linked locations would then match nothing (8,902 locations; #1891 evidence/hold.json).

Guard per row: every field still holds the plan's old value (never overwrites a later edit). Works re-stamp where
their source's name changes (~550K, End 2 End rider 1891-source-name-control-chars).
Revert: python handoff/oxjob1891-control-chars-2026-10-27.py --revert <receipt.json> [--execute]
(sets each field back where it still holds the new value).

One transaction, dry run by default, receipt after RECEIPT.
    python handoff/oxjob1891-control-chars-2026-10-27.py [--execute]
"""
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine
from sources_lib import _CTRL

HERE = os.path.dirname(os.path.abspath(__file__))
PLAN = os.path.join(HERE, "oxjob1891-control-chars-2026-10-27.json")
EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
FIELDS = ("display_name", "alternate_titles", "publisher")
CTRL_SQL = r"[\x01-\x1f\x7f-\x9f]"
receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert": REVERT,
           "changed": {}, "skipped": {}}


class DryRun(Exception):
    pass


def counts(conn):
    return list(conn.execute(text(
        "SELECT COUNT(*) FILTER (WHERE display_name ~ :c), COUNT(*) FILTER (WHERE publisher ~ :c), "
        "COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM jsonb_array_elements_text(COALESCE(alternate_titles, '[]'::jsonb)) t "
        "WHERE t ~ :c)) FROM sources"), {"c": CTRL_SQL}).fetchone())


def steps():
    """(source_id, {field: (expected_now, set_to)}) from the plan, or reversed from a receipt."""
    if REVERT:
        for sid, ch in json.load(open(REVERT, encoding="utf-8"))["changed"].items():
            yield int(sid), {f: (new, old) for f, (old, new) in ch.items()}
    else:
        for p in json.load(open(PLAN, encoding="utf-8")):
            yield p["source_id"], {f: tuple(v) for f, v in p["changes"].items()}


def main():
    plan = list(steps())
    if not REVERT:
        for sid, ch in plan:
            assert set(ch) <= set(FIELDS), sid
            for f, (_, new) in ch.items():
                assert not any(_CTRL.search(v or "") for v in (new if isinstance(new, list) else [new])), (sid, f)
    try:
        with engine.begin() as conn:
            b = counts(conn)
            print(f"before: control chars in display_name {b[0]}, publisher {b[1]}, alternate_titles {b[2]}", flush=True)
            cur = {r.id: {"display_name": r.display_name, "alternate_titles": r.alternate_titles, "publisher": r.publisher}
                   for r in conn.execute(text("SELECT id, display_name, alternate_titles, publisher FROM sources "
                                              "WHERE id = ANY(:i)"), {"i": [s for s, _ in plan]})}
            by_field = Counter()
            for sid, ch in plan:
                now = cur.get(sid)
                if now is None:
                    receipt["skipped"][str(sid)] = "gone"
                    continue
                stale = [f for f, (want, _) in ch.items() if now[f] != want]
                if stale:
                    receipt["skipped"][str(sid)] = f"changed since the plan: {stale}"
                    continue
                sets = ", ".join(f"{f} = CAST(:{f} AS JSONB)" if f == "alternate_titles" else f"{f} = :{f}" for f in ch)
                params = {f: (json.dumps(new, ensure_ascii=False) if f == "alternate_titles" and new is not None else new)
                          for f, (_, new) in ch.items()}
                k = conn.execute(text(f"UPDATE sources SET {sets}, updated_date = now() WHERE id = :s"),
                                 {**params, "s": sid}).rowcount
                assert k == 1, sid
                receipt["changed"][str(sid)] = {f: [want, new] for f, (want, new) in ch.items()}
                by_field.update(ch.keys())
            a = counts(conn)
            print(f"changed {len(receipt['changed'])} of {len(plan)}; skipped {len(receipt['skipped'])}; "
                  f"fields {dict(by_field)}", flush=True)
            print(f"after: control chars in display_name {a[0]}, publisher {a[1]}, alternate_titles {a[2]}", flush=True)
            receipt["before"], receipt["after"] = b, a
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
