import csv
import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("fill_from_beacon", ROOT / "jobs" / "fill_from_beacon.py")
fill = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = fill
SPEC.loader.exec_module(fill)


def write_csv(rows):
    f = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="")
    w = csv.DictWriter(f, fieldnames=["source_id", "homepage_url", "country_code", "country", "evidence"])
    w.writeheader()
    w.writerows(rows)
    f.close()
    return f.name


class LoadRows(unittest.TestCase):
    def test_valid_rows_and_blank_skips(self):
        rows, errors = fill.load_rows(write_csv([
            {"source_id": "1", "homepage_url": "https://j.example/index.php/a", "country_code": "id", "country": "Indonesia"},
            {"source_id": "2", "homepage_url": "", "country_code": ""},
        ]))
        self.assertEqual(errors, [])
        self.assertEqual(rows, [{"id": 1, "hp": "https://j.example/index.php/a", "cc": "ID", "cn": "Indonesia"}])

    def test_refuses_bad_values(self):
        _, errors = fill.load_rows(write_csv([
            {"source_id": "x", "homepage_url": "", "country_code": ""},
            {"source_id": "3", "homepage_url": "ftp://nope", "country_code": "IDN"},
            {"source_id": "4", "homepage_url": "https://a", "country_code": ""},
            {"source_id": "4", "homepage_url": "https://b", "country_code": ""},
            {"source_id": "5", "homepage_url": "", "country_code": "PE"},
        ]))
        # bad id; bad scheme; IDN: not ISO2 + no name; PE: no name; duplicate id
        self.assertEqual(len(errors), 6)


@unittest.skipUnless(os.environ.get("FILL_TEST_PG_URL"), "set FILL_TEST_PG_URL to a scratch Postgres")
class FillSqlSemantics(unittest.TestCase):
    """FILL_SQL against real Postgres: fills NULLs only, never overwrites."""

    def setUp(self):
        from sqlalchemy import create_engine, text
        self.text = text
        self.engine = create_engine(os.environ["FILL_TEST_PG_URL"], future=True)
        with self.engine.begin() as c:
            c.execute(text("DROP TABLE IF EXISTS sources"))
            c.execute(text("CREATE TABLE sources (id bigint primary key, homepage_url text, "
                           "country_code text, country text, updated_date timestamptz)"))
            c.execute(text(
                "INSERT INTO sources VALUES "
                "(1, NULL, NULL, NULL, '2020-01-01'),"          # both empty -> both fill
                "(2, 'https://curated', 'US', 'United States', '2020-01-01'),"  # curated -> untouched
                "(3, NULL, 'BR', 'Brazil', '2020-01-01'),"      # only homepage fills
                "(4, NULL, NULL, 'Kept Name', '2020-01-01')"    # code fills, existing name kept
            ))

    def run_fill(self, id_, hp, cc, cn):
        with self.engine.begin() as c:
            return c.execute(fill.FILL_SQL, {"id": id_, "hp": hp, "cc": cc, "cn": cn}).scalar()

    def row(self, id_):
        with self.engine.connect() as c:
            return c.execute(self.text("SELECT homepage_url, country_code, country, "
                                       "updated_date > '2021-01-01' FROM sources WHERE id=:i"), {"i": id_}).one()

    def test_fill_only(self):
        self.assertEqual(self.run_fill(1, "https://j1", "ID", "Indonesia"), 1)
        self.assertEqual(tuple(self.row(1)), ("https://j1", "ID", "Indonesia", True))

        self.assertIsNone(self.run_fill(2, "https://other", "RU", "Russian Federation"))
        self.assertEqual(tuple(self.row(2)), ("https://curated", "US", "United States", False))

        self.assertEqual(self.run_fill(3, "https://j3", "RU", "Russian Federation"), 3)
        self.assertEqual(tuple(self.row(3)), ("https://j3", "BR", "Brazil", True))

        self.assertEqual(self.run_fill(4, None, "PE", "Peru"), 4)
        self.assertEqual(tuple(self.row(4)), (None, "PE", "Kept Name", True))


if __name__ == "__main__":
    unittest.main()
