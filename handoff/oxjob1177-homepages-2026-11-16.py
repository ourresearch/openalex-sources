"""Stale journal homepages from support tickets (oxjob #1177, support factory, 2026-11-16).

21 sources named by 21 Zendesk tickets that still carry this job in their Oxjob field. Each new homepage was fetched
live on 16 Nov 2026 and serves that journal (receipts: ~/ox/oxjobs/working/source-homepage-url-stale/work/
probe_targets_2026-11-16.txt). The edits are the reviewed #1899 batch-1/batch-2 rows for these tickets (Kyle's yes,
29 Oct: "write all high-confidence fixes"; batch 1 was built but never run), re-checked today, plus:
  - DergiPark S4306401840 (ZD 1950): the portal is the national DergiPark (endpoint dergipark.org.tr/api/public/oai),
    run by TUBITAK ULAKBIM (I4210099755, ROR 017k52s24), not Istanbul University: host, homepage and name fixed.
    WORKS RE-STAMP: ~94K visible works (109K with xpac) with a DergiPark location get the new host embedded.
  - IJIL S4404662214 (ZD 23584): homepage + host UIN KHAS Jember I4405256419 (41 sibling journals use it).
  - Otra Economia S4306521833 + El Arco y la Lira S7407076822 (ZD 23866): host National University of General
    Sarmiento P4310318155, the publisher their UNGS sibling journals already carry (~480 works re-stamp).
  - JGSP S4210206984 (ZD 7766): http:// form, because https://geometry.imbm.bas.bg does not connect.

Guard per field, re-checked inside each UPDATE: the registry still holds the value read on 16 Nov ("expect").
A source whose fields drifted is skipped and reported. Renames also set display_name_before_override and
override_timestamp (as #668 does) so feeds don't put the old name back.

    python handoff/oxjob1177-homepages-2026-11-16.py                         # dry run, rolled back
    python handoff/oxjob1177-homepages-2026-11-16.py --execute               # write + print the receipt
    python handoff/oxjob1177-homepages-2026-11-16.py --revert <receipt.json> [--execute]   # put the old values back

On desk run it through run_registry.py (lanes_registry login, #1831).
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text

from db import engine

EXECUTE = "--execute" in sys.argv
REVERT = sys.argv[sys.argv.index("--revert") + 1] if "--revert" in sys.argv else None
FIELDS = {"homepage_url", "institution_id", "publisher_id", "display_name"}

EDITS = [
    # (source id, tickets, {field: (expected now, new)})
    (4210226059, [1749], {"homepage_url": ("http://www.ijirem.org", "https://www.ijirmps.org/")}),
    (4306530289, [1803], {"homepage_url": ("http://everant.in/index.php/sshj-current-issue",
                                           "https://sshjournal.com/index.php/sshj")}),
    (2739067750, [1880], {"homepage_url": ("http://www.seer.ufs.br/index.php/Ambivalencias/index",
                                           "https://periodicos.ufs.br/Ambivalencias")}),
    (4306401840, [1950], {"homepage_url": ("http://dergipark.gov.tr", "https://dergipark.org.tr/en/"),
                          "institution_id": (67581229, 4210099755),
                          "display_name": ("DergiPark (Istanbul University)", "DergiPark")}),
    (2764855249, [3821], {"homepage_url": ("http://iaesjournal.com/online/index.php/telkomnika",
                                           "https://ijeecs.iaescore.com/index.php/IJEECS")}),
    (2737195927, [4230], {"homepage_url": ("https://www.janusdigital.es/indice.htm",
                                           "https://revistas.udc.es/index.php/janusdigital")}),
    (4210198516, [5201], {"homepage_url": ("http://www.ujpr.org", "https://ujpronline.com/index.php/journal")}),
    (4404675206, [5541], {"homepage_url": ("http://chetumal.tecnm.mx/avacient/index.php/revista/issue/view/13",
                                           "https://avacient.chetumal.tecnm.mx/index.php/revista")}),
    (2480811292, [6218, 6403], {"homepage_url": ("http://www.ecopetrol.com.co/contenido.aspx?catID=140&conID=37780",
                                                 "https://ctyf.journal.ecopetrol.com.co/index.php/ctyf")}),
    (4210231913, [6230], {"homepage_url": ("http://periodicos.univille.br/index.php/Plural_Design/index",
                                           "https://periodicos.univille.br/PL")}),
    (4210206984, [7766], {"homepage_url": ("http://www.bio21.bas.bg/jgsp", "http://geometry.imbm.bas.bg/jgsp/")}),
    (5406994965, [8071], {"homepage_url": (None, "https://www.zkg.de/en/index.html")}),
    (4210205988, [8238], {"homepage_url": ("http://vocesysilencios.uniandes.edu.co/index.php/vys",
                                           "https://revistas.uniandes.edu.co/index.php/vys")}),
    (4210241193, [8239], {"homepage_url": ("http://revistas.uniandes.edu.co/lar",
                                           "https://revistas.uniandes.edu.co/index.php/lar")}),
    (4210212406, [22198], {"homepage_url": ("http://per-aspera.pte.hu", "https://journals.lib.pte.hu/index.php/paaa")}),
    (4210235929, [22896], {"homepage_url": ("http://cristal.ac.za/index.php/cristal/index", "https://cristal.org.za")}),
    (4210229589, [23575], {"homepage_url": ("http://www.fundamentaljournals.org/ijfps/index.html",
                                            "https://www.fundamentaljournals.com/index.php/ijfps")}),
    (4210204480, [23575], {"homepage_url": ("http://fundamentaljournals.org/ijfpss/index.html",
                                            "https://www.fundamentaljournals.com/index.php/ijfpss")}),
    (4404662214, [23584], {"homepage_url": ("http://ejournal.pascasarjana-iainjember.id/index.php/IJIL",
                                            "https://ijil.uinkhas.ac.id/index.php/IJIL"),
                           "institution_id": (None, 4405256419)}),
    (4306521833, [23866], {"publisher_id": (None, 4310318155)}),
    (7407076822, [23866], {"publisher_id": (None, 4310318155)}),
]


class DryRun(Exception):
    pass


def apply(conn, receipt):
    assert len({e[0] for e in EDITS}) == len(EDITS), "duplicate source ids"
    receipt["changed"], receipt["skipped"] = {}, {}
    for sid, tickets, sets in EDITS:
        assert set(sets) <= FIELDS
        cur = conn.execute(text("SELECT display_name, homepage_url, institution_id, publisher_id, "
                                "display_name_before_override, CAST(override_timestamp AS text) AS override_ts "
                                "FROM sources WHERE id = :id"), {"id": sid}).mappings().fetchone()
        drift = {f: cur[f] for f, (old, _) in sets.items() if cur is None or cur[f] != old} if cur else {"row": None}
        if drift:
            receipt["skipped"][f"S{sid}"] = {"tickets": tickets, "registry_now": drift}
            continue
        entry = {"tickets": tickets}
        for f, (old, new) in sets.items():
            n = conn.execute(text(f"UPDATE sources SET {f} = :new, updated_date = now() "
                                  f"WHERE id = :id AND {f} IS NOT DISTINCT FROM :old"),
                             {"new": new, "old": old, "id": sid}).rowcount
            assert n == 1, (sid, f)
            entry[f] = [old, new]
        if "display_name" in sets:
            conn.execute(text("UPDATE sources SET display_name_before_override = :o, override_timestamp = now() "
                              "WHERE id = :id"), {"o": cur["display_name"], "id": sid})
            entry["override_before"] = [cur["display_name_before_override"], cur["override_ts"]]
        receipt["changed"][f"S{sid}"] = entry
    print(f"changed {len(receipt['changed'])} of {len(EDITS)}; skipped {len(receipt['skipped'])}", flush=True)
    for s, why in receipt["skipped"].items():
        print("SKIP", s, why, flush=True)


def revert(conn, receipt):
    prior = json.load(open(REVERT, encoding="utf-8"))
    receipt["reverted"], receipt["skipped"] = 0, {}
    for s, e in prior["changed"].items():
        sid, done = int(s.lstrip("S")), []
        for f in FIELDS & set(e):
            old, new = e[f]
            n = conn.execute(text(f"UPDATE sources SET {f} = :old, updated_date = now() "
                                  f"WHERE id = :id AND {f} IS NOT DISTINCT FROM :new"),
                             {"old": old, "new": new, "id": sid}).rowcount
            (done if n else receipt["skipped"].setdefault(s, [])).append(f)
        if "display_name" in done:
            dbo, ots = e.get("override_before", [None, None])
            conn.execute(text("UPDATE sources SET display_name_before_override = :dbo, override_timestamp = "
                              "CAST(:ots AS timestamptz) WHERE id = :id"), {"dbo": dbo, "ots": ots, "id": sid})
        receipt["reverted"] += bool(done)
    print(f"reverted {receipt['reverted']} of {len(prior['changed'])}; skipped {receipt['skipped']}", flush=True)


def main():
    receipt = {"run_at": datetime.now(timezone.utc).isoformat(), "execute": EXECUTE, "revert_of": REVERT,
               "job": "oxjob #1177 (support factory)"}
    try:
        with engine.begin() as conn:
            if REVERT:
                revert(conn, receipt)
            else:
                apply(conn, receipt)
            if not EXECUTE:
                raise DryRun()
        print("COMMITTED")
    except DryRun:
        print("DRY RUN - rolled back")
    print("RECEIPT")
    print(json.dumps(receipt, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
