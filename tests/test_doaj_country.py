import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql://localhost/none")
sys.path.insert(0, str(Path(__file__).parents[1]))
import sources_lib  # noqa: E402
from jobs import doaj  # noqa: E402


class CountryIso(unittest.TestCase):
    def test_csv_names(self):
        self.assertEqual(sources_lib.country_iso("Japan"), ("JP", "Japan"))
        self.assertEqual(sources_lib.country_iso("United Kingdom"), ("GB", "United Kingdom"))
        self.assertEqual(sources_lib.country_iso("Iran, Islamic Republic of"), ("IR", "Iran, Islamic Republic of"))
        self.assertEqual(sources_lib.country_iso(" Türkiye "), ("TR", "Türkiye"))
        self.assertEqual(sources_lib.country_iso("Taiwan, Province of China")[0], "TW")

    def test_api_codes(self):
        self.assertEqual(sources_lib.country_iso("GB"), ("GB", "United Kingdom"))
        self.assertEqual(sources_lib.country_iso("id"), ("ID", "Indonesia"))

    def test_not_a_country(self):
        for v in ("Kosovo", "", None, "International organization", "ZZ", "England"):
            self.assertIsNone(sources_lib.country_iso(v))

    def test_table_is_whole(self):
        self.assertEqual(len(sources_lib.ISO_NAME_TO_CODE), 249)
        self.assertEqual(len(sources_lib.ISO_CODE_TO_NAME), 249)


class FakeConn:
    """Returns the candidate rows for the SELECT and records each UPDATE's parameters."""

    PUBLISHERS = [("Big House", "United Kingdom"), ("Big House", "US"), ("Small Press", "Japan"),
                  ("Small Press", "JP")]

    def __init__(self, rows):
        self.rows, self.updates = rows, []

    def execute(self, stmt, params=None):
        if params:  # the UPDATE ... RETURNING id: source 2 was filled by someone else meanwhile
            self.updates.append(params)
            return [SimpleNamespace(id=i) for i in params["ids"] if i != 2]
        if "DISTINCT publisher" in str(stmt):
            return self.PUBLISHERS
        return SimpleNamespace(fetchall=lambda: self.rows)


class FillCountry(unittest.TestCase):
    def test_multinational_publishers(self):
        self.assertEqual(doaj.multinational_publishers(FakeConn([])), {"big house"})

    def test_fills_one_country_only(self):
        small = ["small press"]
        conn = FakeConn([(1, ["Japan"], True, small), (2, ["Japan"], True, small),
                         (3, ["Japan", "China"], True, small), (4, ["Kosovo"], True, small),
                         (5, ["United Kingdom", "GB"], False, [None]), (6, ["United Kingdom"], True, ["big house"])])
        filled = doaj.fill_country(conn)
        self.assertEqual(conn.updates[0]["ids"], [1, 2, 5])
        self.assertEqual(conn.updates[0]["codes"], ["JP", "JP", "GB"])
        self.assertEqual(filled, [(1, "JP", "Japan", True), (5, "GB", "United Kingdom", False)])

    def test_dry_run_writes_nothing(self):
        conn = FakeConn([(1, ["Japan"], True, ["small press"])])
        self.assertEqual(doaj.fill_country(conn, dry_run=True), [(1, "JP", "Japan", True)])
        self.assertEqual(conn.updates, [])


if __name__ == "__main__":
    unittest.main()
