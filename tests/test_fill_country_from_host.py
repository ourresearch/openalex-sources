import os
import sys
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("DATABASE_URL", "postgresql://localhost/none")
sys.path.insert(0, str(Path(__file__).parents[1]))
from jobs import fill_country_from_host as f  # noqa: E402


def cand(**kw):
    return {"id": 1, "host": "P10", "top_cc": "JP", "top_n": 4, "n_with": 4, "doaj": False, **kw}


class HoldReason(unittest.TestCase):
    def test_fills_when_everything_agrees(self):
        self.assertIsNone(f.hold_reason(cand(), set(), ["JP"], (50, {"JP": 45, "US": 5})))

    def test_fills_when_host_record_has_no_country(self):
        self.assertIsNone(f.hold_reason(cand(), set(), [], (0, {})))

    def test_held_list_and_doaj(self):
        self.assertIn("held", f.hold_reason(cand(), {1}, ["JP"], (0, {})))
        self.assertIn("DOAJ", f.hold_reason(cand(doaj=True), set(), ["JP"], (0, {})))

    def test_host_record_names_another_country(self):
        # Bentham: its titles say NL, its own record says AE
        self.assertIn("another country", f.hold_reason(cand(top_cc="NL"), set(), ["AE"], (0, {})))
        # a multi-country publisher record that includes the rule's country is fine
        self.assertIsNone(f.hold_reason(cand(top_cc="NL"), set(), ["AE", "NL"], (0, {})))
        # East Germany counts as Germany
        self.assertIsNone(f.hold_reason(cand(top_cc="DE"), set(), ["DD"], (0, {})))

    def test_china_via_publisher_is_held_institution_is_not(self):
        self.assertIn("CN", f.hold_reason(cand(top_cc="CN"), set(), ["CN"], (0, {})))
        self.assertIsNone(f.hold_reason(cand(top_cc="CN", host="I19820366"), set(), ["CN"], (0, {})))

    def test_authors_elsewhere(self):
        self.assertIn("authors", f.hold_reason(cand(), set(), ["JP"], (100, {"CN": 85, "JP": 10})))
        # under 80%, or too few works with a country: fill
        self.assertIsNone(f.hold_reason(cand(), set(), ["JP"], (100, {"CN": 70, "JP": 30})))
        self.assertIsNone(f.hold_reason(cand(), set(), ["JP"], (19, {"CN": 19})))
        # the authors' top country is the rule's own: fill
        self.assertIsNone(f.hold_reason(cand(), set(), ["JP"], (100, {"JP": 95})))


class AuthorCountries(unittest.TestCase):
    def test_group_by_plus_count_with_a_country(self):
        groups = {"meta": {"count": 26394},
                  "group_by": [{"key": "https://openalex.org/countries/JP", "count": 9512},
                               {"key": "https://openalex.org/countries/US", "count": 97}]}
        with_country = {"meta": {"count": 9635}, "results": []}

        def fake(path, **params):
            return with_country if "!null" in params["filter"] else groups
        with mock.patch.object(f, "api", side_effect=fake):
            self.assertEqual(f.author_countries(4306547168), (9635, {"JP": 9512, "US": 97}))

    def test_no_author_countries(self):
        with mock.patch.object(f, "api", return_value={"meta": {"count": 12}, "group_by": []}):
            self.assertEqual(f.author_countries(5), (0, {}))


class HostCountries(unittest.TestCase):
    def test_publisher_and_institution(self):
        with mock.patch.object(f, "api", return_value={"country_codes": ["AE"]}):
            self.assertEqual(f.host_countries("P4310320079"), ["AE"])
        with mock.patch.object(f, "api", return_value={"country_code": "RU"}):
            self.assertEqual(f.host_countries("I108321963"), ["RU"])
        with mock.patch.object(f, "api", return_value=None):
            self.assertEqual(f.host_countries("P1"), [])


class Holds(unittest.TestCase):
    def test_holds_file_loads(self):
        held = f.load_holds()
        self.assertIn(1296839, held)  # The Open Macromolecules Journal (Bentham), held by #2208
        self.assertGreater(len(held), 500)


if __name__ == "__main__":
    unittest.main()
