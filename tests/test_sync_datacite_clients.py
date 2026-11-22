"""Test-account guard in jobs/sync_datacite_clients.py (oxjob #952 Bug 2 step 3).

The client names are DataCite's own, fetched 20 Nov 2026 from api.datacite.org/clients?query=test|dev|pre-production.
"""
import importlib.util
import sys
import types
import unittest
from collections import namedtuple
from pathlib import Path

ROOT = Path(__file__).parents[1]


def _load(match_calls):
    db = types.ModuleType("db")
    db.engine = None
    lib = types.ModuleType("sources_lib")

    class MatchContext:
        def __init__(self, *a, **k):
            pass

    def match_source(conn, ctx, issns, name, feed, **k):
        match_calls.append(name)
        return "none", None

    lib.MatchContext = MatchContext
    lib.match_source = match_source
    lib.mint_source = lambda *a, **k: 1
    lib.normalize_issns = lambda xs: xs
    lib.park_multi_match = lambda *a, **k: None
    saved = {k: sys.modules.get(k) for k in ("db", "sources_lib")}
    sys.modules["db"], sys.modules["sources_lib"] = db, lib
    try:
        spec = importlib.util.spec_from_file_location("sync_datacite_clients_t", ROOT / "jobs" / "sync_datacite_clients.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    return mod


TEST_ACCOUNTS = [
    ("bl.jisc-mrd", "BL test account"), ("datacite.apbnjd", "Test"), ("datacite.bffquh", "test"),
    ("eblj.clpibt", "hbz Consortium Organisation - Test Repository"), ("eiuc.iikvtp", "Test"),
    ("ethz.test", "Test Datacentre"), ("europ.test", "OP Test account"), ("fgff.urhpbq", "TEST"),
    ("figshare.test", "Figshare test Datacenter"), ("ghsl.test", "Galter Library Pre-Production Test Client"),
    ("labarc.dev-box", "Dev box tool"), ("rads.pcycjg", "Test"), ("snd.test", "test datacentre for SND"),
    ("tsinghua.kuksik", "test"), ("usc.test", "Test Prefix for USC DataCite"), ("sagebio.dev", "Dev"),
]
REAL_CLIENTS = [
    ("alhd.bed", "Build Environment Data_Experiments"), ("arf.data", "Arctic Research Foundation Data Catalogue"),
    ("bl.oxdb", "Oxford University Research Archive"), ("bmwp.qicvpx", "YGS-DEV"), ("cnrst.data", "Dataverse"),
    ("ezwg.kjqcfa", "ILDS-DEV"), ("gyxb.cpdymx", "CRKN-DEV"), ("iyfw.kddaax", "McGill-DEV"),
    ("krep.szjagu", "AcadiaU-DEV"), ("ltjy.pkxggm", "UREG-DEV"), ("hlqc.jslnah", "TUprints"),
    ("kim.openkim", "Open Knowledgebase of Interatomic Models (OpenKIM)"), ("wuua.uljier", "FID BAUdigital"),
    ("rhtq.roztvc", "Artificial Intelligence Tools in Software and Data Engineering (AITSDE)"),
    ("x.y", "Journal of Testing and Evaluation"), ("x.z", "Contest Studies Archive"), ("x.w", "Devon Heritage"),
]


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, clients):
        self.clients = clients

    def execute(self, stmt, params=None):
        sql = str(stmt)
        if sql.startswith("SELECT id, display_name"):
            return FakeResult(self.clients)
        return FakeResult([])

    def begin(self):
        return types.SimpleNamespace(commit=lambda: None, rollback=lambda: None)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class TestClientGuard(unittest.TestCase):
    def test_test_accounts_match(self):
        mod = _load([])
        for cid, name in TEST_ACCOUNTS:
            self.assertTrue(mod.is_test_client(cid, name), (cid, name))

    def test_real_clients_do_not_match(self):
        mod = _load([])
        for cid, name in REAL_CLIENTS:
            self.assertFalse(mod.is_test_client(cid, name), (cid, name))

    def test_run_skips_test_accounts_before_matching(self):
        calls = []
        mod = _load(calls)
        Client = namedtuple("Client", "id display_name issns url client_type provider_name")
        clients = [Client(cid, name, [], None, "repository", "Some Provider") for cid, name in TEST_ACCOUNTS + REAL_CLIENTS]
        mod.engine = types.SimpleNamespace(connect=lambda: FakeConn(clients))
        counts = mod.run(dry_run=True)
        self.assertEqual(counts["skipped_test_client"], len(TEST_ACCOUNTS))
        self.assertEqual(sorted(calls), sorted(name for _, name in REAL_CLIENTS))
        self.assertEqual(counts["added_account_label_name"], 6)  # the six "-DEV" names above


if __name__ == "__main__":
    unittest.main()
