import json
import os
import unittest
from collections import Counter, defaultdict
from datetime import date
from unittest.mock import MagicMock, patch


# Importing db constructs an engine but does not connect.  Keep unit tests
# independent of a developer's local environment.
os.environ.setdefault("DATABASE_URL", "postgresql://invalid.invalid/openalex")

from jobs import butler_apc as apc  # noqa: E402


def reference(*rows):
    """Build the two indexes returned by load_v1_reference for tiny fixtures."""
    by_key = defaultdict(list)
    by_issn = defaultdict(list)
    for reference_id, row in enumerate(rows, start=1):
        ref = {
            "reference_id": reference_id,
            "unique_id": row["unique_id"],
            "publisher_key": apc.normalize_publisher(row["publisher"]),
            "apc_year": row["apc_year"],
            "apc_date": row.get("apc_date"),
            "issns": row.get("issns", []),
            "original_prices": row.get("original_prices", {}),
        }
        key = (ref["unique_id"], ref["apc_year"], ref["publisher_key"])
        by_key[key].append(ref)
        for issn in ref["issns"]:
            by_issn[(ref["apc_year"], ref["publisher_key"], issn)].append(ref)
    return {"by_key": by_key, "by_issn": by_issn, "row_count": len(rows)}


def rates_from_usd_values(year, usd_per_unit):
    """Make a complete transitive currency matrix from USD-per-unit factors."""
    return {
        (year, source, target): usd_per_unit[source] / usd_per_unit[target]
        for source in apc.CURRENCIES
        for target in apc.CURRENCIES
    }


def v2_row(**overrides):
    row = {
        "record_id": "1",
        "unique_id": "100",
        "publisher": "Example Publisher",
        "issn1": "1234-5678",
        "issn2": "",
        "issn_l": "1234-5678",
        "journal": "Example Journal",
        "oa_status": "Gold OA",
        "apc_date": "2019-01-01 00:00:00 UTC",
        "apc_source": "Publisher website",
        "apc_year": "2019",
        "data_version": "1",
    }
    for currency in apc.CURRENCIES:
        key = currency.lower()
        row[f"apc_{key}"] = ""
        row[f"apc_{key}_originalORconverted"] = ""
    row.update(overrides)
    return row


class V1ReferenceResolutionTests(unittest.TestCase):
    def test_issn_fallback_handles_upstream_id_remap(self):
        refs = reference({
            "unique_id": 3868,
            "publisher": "MDPI",
            "apc_year": 2020,
            "issns": ["1234-5678"],
            "original_prices": {"CHF": 1000},
        })
        row = v2_row(unique_id="2474", publisher="MDPI", apc_year="2020")
        currencies, basis = apc.resolve_v1_original_currencies(row, refs)
        self.assertEqual(currencies, {"CHF"})
        self.assertEqual(basis, "v1_reference_issn")

    def test_duplicate_key_uses_original_value_vector(self):
        refs = reference(
            {
                "unique_id": 100,
                "publisher": "Example Publisher",
                "apc_year": 2019,
                "original_prices": {"USD": 1000},
            },
            {
                "unique_id": 100,
                "publisher": "Example Publisher",
                "apc_year": 2019,
                "original_prices": {"USD": 2000, "EUR": 1800},
            },
        )
        row = v2_row(apc_usd="2000", apc_eur="1800")
        currencies, basis = apc.resolve_v1_original_currencies(row, refs)
        self.assertEqual(currencies, {"USD", "EUR"})
        self.assertEqual(basis, "v1_reference_key_value_match")

    def test_unreconciled_reference_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "could not reconcile"):
            apc.resolve_v1_original_currencies(v2_row(), reference())


class LegacyConversionInferenceTests(unittest.TestCase):
    def setUp(self):
        # Annual 2019 USD-per-unit factors from the supplied v2 FX matrix.
        self.rates = rates_from_usd_values(2019, {
            "USD": 1.0,
            "EUR": 1.1198337667236917,
            "GBP": 1.276733901970175,
            "JPY": 0.0091756489811084,
            "CHF": 1.0066255179151145,
            "CAD": 0.7537075637850522,
            "AUD": 0.6953557980566871,
        })

    def test_iza_corrected_row_recovers_three_original_currencies(self):
        row = v2_row(
            unique_id="8949",
            apc_usd="1535",
            apc_eur="1250",
            apc_gbp="980",
            apc_jpy="166341.81",
            apc_chf="1515.04",
            apc_cad="2015.76",
            apc_aud="2203.95",
        )
        currencies, basis = apc.infer_corrected_legacy_currencies(
            row, self.rates
        )
        self.assertEqual(currencies, {"USD", "EUR", "GBP"})
        self.assertEqual(basis, "v2_legacy_conversion_inference_confirmed")

    def test_parse_row_uses_inference_when_v1_row_was_unpriced(self):
        row = v2_row(
            unique_id="8949",
            publisher="Springer Nature",
            apc_usd="1535",
            apc_eur="1250",
            apc_gbp="980",
            apc_jpy="166341.81",
            apc_chf="1515.04",
            apc_cad="2015.76",
            apc_aud="2203.95",
        )
        refs = reference({
            "unique_id": 8949,
            "publisher": "Springer Nature",
            "apc_year": 2019,
            "issns": ["1234-5678"],
            "original_prices": {},
        })
        parsed = apc.parse_row_v2(row, refs, self.rates)
        historical = {
            p["currency"]
            for p in json.loads(parsed["prices"])
            if p["historical_original"]
        }
        self.assertEqual(historical, {"USD", "EUR", "GBP"})

    def test_ambiguous_integer_conversions_are_rejected(self):
        equal_rates = {
            (2019, source, target): 1.0
            for source in apc.CURRENCIES
            for target in apc.CURRENCIES
        }
        row = v2_row(**{
            f"apc_{currency.lower()}": "1000"
            for currency in apc.CURRENCIES
        })
        currencies, basis = apc.infer_corrected_legacy_currencies(
            row, equal_rates
        )
        self.assertEqual(currencies, set())
        self.assertEqual(basis, "v2_legacy_conversion_inference_rejected")

    def test_zero_never_claims_local_currency(self):
        row = v2_row(apc_usd="0", apc_eur="0", apc_gbp="0")
        currencies, basis = apc.infer_corrected_legacy_currencies(row, None)
        self.assertEqual(currencies, set())
        self.assertEqual(basis, "v2_legacy_zero_or_unpriced")


class HistoricalPriceTests(unittest.TestCase):
    def test_history_uses_same_winning_row_as_usd_for_year_collision(self):
        def annual_row(order, usd, currency, local):
            return {
                "price_usd": usd,
                "prices": json.dumps([
                    {
                        "currency": currency,
                        "price": local,
                        "original": True,
                        "historical_original": True,
                    },
                ]),
                "apc_year": 2020,
                "apc_order": order,
                "publisher": "Example Publisher",
                "apc_date": f"2020-0{order}-01",
            }

        rows = [
            annual_row(1, 1000, "EUR", 900),
            annual_row(2, 1200, "GBP", 1000),
        ]
        by_year, current, current_prices, historical = apc.build_usd_by_year(
            rows, Counter()
        )
        self.assertEqual(by_year, {"2020": 1200})
        self.assertEqual(current, 1200)
        self.assertEqual(current_prices, [
            {"price": 1000, "currency": "GBP"},
        ])
        self.assertEqual(historical, {
            "2020": [{"price": 1000, "currency": "GBP"}],
        })

    def test_v2_direct_conversion_source_is_historical_not_current(self):
        row = v2_row(
            data_version="2",
            apc_usd="500",
            apc_usd_originalORconverted="converted from AUD",
            apc_aud="735.50",
            apc_aud_originalORconverted="converted from AUD",
        )
        parsed = apc.parse_row_v2(row)
        prices = {p["currency"]: p for p in json.loads(parsed["prices"])}
        self.assertTrue(prices["AUD"]["historical_original"])
        self.assertEqual(
            prices["AUD"]["historical_original_basis"],
            "v2_conversion_source",
        )
        # This is intentionally the pre-phase-2 current-price heuristic.
        self.assertFalse(prices["AUD"]["original"])

    def test_exact_zero_publishes_usd_history_but_preserves_current_prices(self):
        raw_prices = [
            {
                "currency": "USD", "price": 0, "original": True,
                "historical_original": True,
            },
            {
                "currency": "EUR", "price": 0, "original": True,
                "historical_original": True,
            },
        ]
        row = {
            "price_usd": 0,
            "prices": json.dumps(raw_prices),
            "apc_year": 2020,
            "apc_order": None,
            "publisher": "Example Publisher",
            "apc_date": "2020-01-01",
        }
        by_year, current, current_prices, historical = apc.build_usd_by_year(
            [row], Counter()
        )
        self.assertEqual(by_year, {"2020": 0})
        self.assertEqual(current, 0)
        self.assertEqual(current_prices, [
            {"price": 0, "currency": "USD"},
            {"price": 0, "currency": "EUR"},
        ])
        self.assertEqual(historical, {
            "2020": [{"price": 0, "currency": "USD"}],
        })

    def test_fractional_v2_original_is_exact_only_in_historical_output(self):
        row = v2_row(
            data_version="2",
            apc_usd="2887.5",
            apc_usd_originalORconverted="original",
        )
        parsed = apc.parse_row_v2(row)
        _, _, current_prices, historical = apc.build_usd_by_year(
            [parsed], Counter()
        )
        self.assertEqual(current_prices, [
            {"price": 2888, "currency": "USD"},
        ])
        self.assertEqual(historical, {
            "2019": [{"price": 2887.5, "currency": "USD"}],
        })

    def test_fractional_v1_original_retains_exact_historical_price(self):
        row = {
            "unique_id": "5009",
            "Publisher": "Example Publisher",
            "ISSN_1": "1234-5678",
            "ISSN_2": "",
            "Journal": "Example Journal",
            "OA_status": "Gold OA",
            "APC_provided": "yes",
            "APC_order": "1",
            "APC_year": "2019",
            "APC_date": "2019-01-01",
            "APC_source": "Publisher website",
        }
        for currency in apc.CURRENCIES:
            row[f"APC_{currency}"] = ""
            row[f"APC_{currency}-originalORconverted"] = ""
        row["APC_USD"] = "1608.7500"
        row["APC_USD-originalORconverted"] = "original"
        parsed = apc.parse_row(row)
        _, _, current_prices, historical = apc.build_usd_by_year(
            [parsed], Counter()
        )
        self.assertEqual(current_prices, [
            {"price": 1609, "currency": "USD"},
        ])
        self.assertEqual(historical, {
            "2019": [{"price": 1608.75, "currency": "USD"}],
        })

    def test_historical_price_outside_decimal_contract_fails_closed(self):
        for bad_price in (1.23456, float("nan"), 100000000000000.0):
            with self.subTest(bad_price=bad_price):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "historical APC price",
                ):
                    apc._public_historical_prices([{
                        "currency": "USD",
                        "price": bad_price,
                        "historical_original": True,
                    }])

    def test_blank_data_version_never_claims_historical_currency(self):
        row = v2_row(
            data_version="",
            apc_usd="1000",
            apc_usd_originalORconverted="original",
            apc_eur="900",
            apc_eur_originalORconverted="original",
        )
        parsed = apc.parse_row_v2(row)
        prices = json.loads(parsed["prices"])
        self.assertTrue(all(not p["historical_original"] for p in prices))
        self.assertTrue(all(p["original"] for p in prices))


class ApplySafetyTests(unittest.TestCase):
    def test_history_only_carries_local_map_and_dry_run_executes_no_batch(self):
        year = date.today().year - 3
        rows = [{
            "unique_id": 100,
            "journal": "Example Journal",
            "price_usd": 1200,
            "prices": json.dumps([{
                "currency": "EUR",
                "price": 1000,
                "original": True,
                "historical_original": True,
            }]),
            "apc_year": year,
            "apc_order": None,
            "publisher": "Example Publisher",
            "apc_date": f"{year}-01-01",
        }]
        conn = MagicMock()
        with (
            patch.object(apc.engine, "begin") as begin,
            patch.object(apc, "match_rows") as match_rows,
            patch.object(apc.psycopg2.extras, "execute_batch") as execute_batch,
        ):
            begin.return_value.__enter__.return_value = conn
            match_rows.return_value = ({42: rows}, [], Counter())

            apc.apply("butler_v2", rows=rows, dry_run=False)
            execute_batch.assert_called_once()
            sql = execute_batch.call_args.args[1]
            params = execute_batch.call_args.args[2]
            self.assertIn("apc_prices_by_year", sql)
            self.assertNotIn("apc_usd =", sql)
            self.assertEqual(json.loads(params[0]["prices_by_year"]), {
                str(year): [{"price": 1000, "currency": "EUR"}],
            })

            execute_batch.reset_mock()
            match_rows.return_value = ({42: rows}, [], Counter())
            apc.apply("butler_v2", rows=rows, dry_run=True)
            execute_batch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
