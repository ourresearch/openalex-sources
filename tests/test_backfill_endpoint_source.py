import argparse
import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "jobs" / "backfill_endpoint_source.py"
SPEC = importlib.util.spec_from_file_location("backfill_endpoint_source", MODULE_PATH)
backfill = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = backfill
SPEC.loader.exec_module(backfill)


def observation(endpoint_id, canonical=None, matcher=None, legacy=(), pmh_url=None):
    return backfill.Observation(
        endpoint_id=endpoint_id,
        canonical_source_id=canonical,
        source_endpoint_source_id=matcher,
        legacy_source_ids=tuple(legacy),
        pmh_url=pmh_url,
        metadata_prefix="oai_dc",
        ready_to_run=True,
    )


def decision(
    endpoint_id,
    observed,
    action,
    target=None,
    kind="known_correction",
    approval="review://decision",
    supersedes="",
):
    return backfill.ManifestDecision(
        endpoint_id=endpoint_id,
        expected_canonical_source_id=observed.canonical_source_id,
        expected_source_endpoint_source_id=observed.source_endpoint_source_id,
        expected_legacy_source_ids=observed.legacy_source_ids,
        action=action,
        target_source_id=target,
        reviewed_by="reviewer@example",
        approval_ref=approval,
        evidence="reviewed evidence",
        manifest_kind=kind,
        manifest_sha256="a" * 64,
        supersedes_conflict_approval_ref=supersedes,
    )


def protected_fixture():
    values = {
        "b174b390e74e8df2b0a": observation(
            "b174b390e74e8df2b0a", matcher=4406922384, legacy=(4406922384,)
        ),
        "6333d6bcffb78f92839": observation(
            "6333d6bcffb78f92839", matcher=4210203682, legacy=(4210203682,)
        ),
        "b54a0400f7544929302": observation(
            "b54a0400f7544929302", matcher=2764988245, legacy=(2764988245,)
        ),
        "2b57dfbd43207095dbc": observation(
            "2b57dfbd43207095dbc", matcher=4377196246, legacy=(4377196246,)
        ),
        "vzs6na3qd8sizyhrp6u5": observation(
            "vzs6na3qd8sizyhrp6u5",
            matcher=4306402621,
            legacy=(4306402621,),
            pmh_url="https://api.figshare.com/v2/oai",
        ),
        "rqyjzdijqflhirw6ao8v": backfill.Observation(
            endpoint_id="rqyjzdijqflhirw6ao8v",
            canonical_source_id=None,
            source_endpoint_source_id=7407051385,
            legacy_source_ids=(),
            pmh_url="https://api.figshare.com/v2/oai",
            pmh_set="set=portal_959",
            metadata_prefix="oai_dc",
            ready_to_run=False,
        ),
    }
    corrections = {
        "b174b390e74e8df2b0a": decision(
            "b174b390e74e8df2b0a", values["b174b390e74e8df2b0a"], "LEAVE_NULL"
        ),
        "6333d6bcffb78f92839": decision(
            "6333d6bcffb78f92839",
            values["6333d6bcffb78f92839"],
            "SET_SOURCE",
            4377196541,
        ),
        "b54a0400f7544929302": decision(
            "b54a0400f7544929302", values["b54a0400f7544929302"], "LEAVE_NULL"
        ),
        "2b57dfbd43207095dbc": decision(
            "2b57dfbd43207095dbc", values["2b57dfbd43207095dbc"], "ESCALATE"
        ),
        "vzs6na3qd8sizyhrp6u5": decision(
            "vzs6na3qd8sizyhrp6u5", values["vzs6na3qd8sizyhrp6u5"], "LEAVE_NULL"
        ),
        "rqyjzdijqflhirw6ao8v": decision(
            "rqyjzdijqflhirw6ao8v", values["rqyjzdijqflhirw6ao8v"], "LEAVE_NULL"
        ),
    }
    return list(values.values()), corrections


class ManifestTests(unittest.TestCase):
    def test_manifest_hash_and_strict_values(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "direct.csv"
            fields = sorted(backfill.COMMON_MANIFEST_COLUMNS)
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerow(
                    {
                        "endpoint_id": "e1",
                        "expected_canonical_source_id": "",
                        "expected_source_endpoint_source_id": "10",
                        "expected_legacy_source_ids": "[20,30]",
                        "action": "SET_SOURCE",
                        "target_source_id": "10",
                        "reviewed_by": "Rohan",
                        "approval_ref": "notion://approval",
                        "evidence": "specific evidence",
                    }
                )
            rows, digest = backfill.load_manifest(path, "direct_conflict")
            self.assertEqual(rows["e1"].expected_legacy_source_ids, (20, 30))
            self.assertEqual(digest, backfill.sha256_bytes(path.read_bytes()))

    def test_placeholder_approval_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "direct.csv"
            fields = sorted(backfill.COMMON_MANIFEST_COLUMNS)
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerow(
                    {
                        "endpoint_id": "e1",
                        "expected_legacy_source_ids": "[]",
                        "action": "LEAVE_NULL",
                        "reviewed_by": "Rohan",
                        "approval_ref": "TBD",
                        "evidence": "specific evidence",
                    }
                )
            with self.assertRaisesRegex(backfill.PreflightError, "approval_ref"):
                backfill.load_manifest(path, "direct_conflict")

    def test_unsorted_legacy_guard_is_rejected(self):
        with self.assertRaisesRegex(backfill.PreflightError, "must be sorted"):
            backfill.parse_legacy_ids("[20,10]", "e1")


class PlanningTests(unittest.TestCase):
    def full_fixture(self):
        protected, corrections = protected_fixture()
        ordinary = [
            observation("agree", matcher=10, legacy=(10,)),
            observation("matcher_only", matcher=11),
            observation("legacy_only", legacy=(12,)),
            observation("orphan"),
            observation("conflict", matcher=13, legacy=(14,)),
        ]
        conflict_observation = ordinary[-1]
        direct = {
            "conflict": decision(
                "conflict",
                conflict_observation,
                "SET_SOURCE",
                13,
                kind="direct_conflict",
                approval="review://conflict",
            )
        }
        existing = {
            10,
            11,
            12,
            13,
            14,
            4406922384,
            4210203682,
            4377196541,
            2764988245,
            4377196246,
            4306402621,
            7407051385,
        }
        return protected + ordinary, direct, corrections, existing

    def test_candidate_rules_and_correction_overlay(self):
        observations, direct, corrections, existing = self.full_fixture()
        plan = backfill.build_plan(observations, direct, corrections, existing)
        rows = {row.endpoint_id: row for row in plan}

        self.assertEqual((rows["agree"].status, rows["agree"].proposed_source_id), ("UPDATE", 10))
        self.assertEqual(
            (rows["matcher_only"].status, rows["matcher_only"].proposed_source_id),
            ("UPDATE", 11),
        )
        self.assertEqual(rows["legacy_only"].status, "REMAINING_NULL")
        self.assertEqual(rows["orphan"].status, "REMAINING_NULL")
        self.assertEqual((rows["conflict"].status, rows["conflict"].proposed_source_id), ("UPDATE", 13))

        # Ordinary legacy evidence would copy all six without this overlay.
        self.assertEqual(rows["b174b390e74e8df2b0a"].status, "REMAINING_NULL")
        self.assertEqual(rows["b54a0400f7544929302"].status, "REMAINING_NULL")
        self.assertEqual(rows["2b57dfbd43207095dbc"].status, "REMAINING_NULL")
        self.assertEqual(rows["2b57dfbd43207095dbc"].disposition, "ESCALATE")
        self.assertEqual(rows["6333d6bcffb78f92839"].proposed_source_id, 4377196541)
        self.assertEqual(rows["vzs6na3qd8sizyhrp6u5"].status, "REMAINING_NULL")
        self.assertEqual(rows["rqyjzdijqflhirw6ao8v"].status, "REMAINING_NULL")

    def test_missing_protected_decision_fails_closed(self):
        observations, direct, corrections, existing = self.full_fixture()
        corrections.pop("b174b390e74e8df2b0a")
        with self.assertRaisesRegex(backfill.PreflightError, "protected endpoints"):
            backfill.build_plan(observations, direct, corrections, existing)

    def test_internet_archive_target_is_pinned(self):
        observations, direct, corrections, existing = self.full_fixture()
        row = next(value for value in observations if value.endpoint_id == "6333d6bcffb78f92839")
        corrections["6333d6bcffb78f92839"] = decision(
            row.endpoint_id, row, "SET_SOURCE", 4210203682
        )
        with self.assertRaisesRegex(backfill.PreflightError, "target must be 4377196541"):
            backfill.build_plan(observations, direct, corrections, existing)

    def test_configuration_risk_requires_explicit_decision(self):
        observations, direct, corrections, existing = self.full_fixture()
        observations.append(
            observation(
                "future_setless_figshare",
                matcher=999,
                legacy=(999,),
                pmh_url="https://api.figshare.com/v2/oai",
            )
        )
        existing.add(999)
        with self.assertRaisesRegex(backfill.PreflightError, "configuration-risk"):
            backfill.build_plan(observations, direct, corrections, existing)

    def test_verified_global_figshare_agreement_remains_automatic(self):
        observations, direct, corrections, existing = self.full_fixture()
        observations.append(
            observation(
                "verified_global_figshare",
                matcher=backfill.FIGSHARE_GLOBAL_SOURCE_ID,
                legacy=(backfill.FIGSHARE_GLOBAL_SOURCE_ID,),
                pmh_url="http://api.figshare.com/v2/oai",
            )
        )
        existing.add(backfill.FIGSHARE_GLOBAL_SOURCE_ID)
        plan = backfill.build_plan(observations, direct, corrections, existing)
        row = next(
            value for value in plan if value.endpoint_id == "verified_global_figshare"
        )
        self.assertEqual(row.status, "UPDATE")
        self.assertEqual(row.proposed_source_id, backfill.FIGSHARE_GLOBAL_SOURCE_ID)
        self.assertIn(
            "setless_figshare_aggregate",
            next(
                value["signals"]
                for value in backfill.semantic_risk_rows(observations, corrections)
                if value["endpoint_id"] == "verified_global_figshare"
            ),
        )

    def test_figshare_and_pmh_set_configuration_signals(self):
        setless = observation(
            "figshare",
            matcher=1,
            legacy=(1,),
            pmh_url="https://api.figshare.com/v2/oai/",
        )
        scoped = backfill.Observation(
            **{**setless.__dict__, "endpoint_id": "scoped", "pmh_set": "portal_959"}
        )
        malformed = backfill.Observation(
            **{**setless.__dict__, "endpoint_id": "malformed", "pmh_set": "set=portal_959"}
        )
        self.assertEqual(
            backfill.configuration_risk_signals(setless),
            ["setless_figshare_aggregate"],
        )
        self.assertEqual(backfill.configuration_risk_signals(scoped), [])
        self.assertEqual(
            backfill.configuration_risk_signals(malformed),
            ["pmh_set_contains_parameter_name"],
        )
        trusted = observation(
            "trusted",
            matcher=backfill.FIGSHARE_GLOBAL_SOURCE_ID,
            legacy=(backfill.FIGSHARE_GLOBAL_SOURCE_ID,),
            pmh_url="http://api.figshare.com/v2/oai",
        )
        self.assertFalse(backfill.requires_configuration_decision(trusted))
        self.assertTrue(backfill.requires_configuration_decision(setless))

    def test_semantic_report_includes_configuration_signals(self):
        observations, _, corrections, _ = self.full_fixture()
        signals = {
            row["endpoint_id"]: row["signals"]
            for row in backfill.semantic_risk_rows(observations, corrections)
        }
        self.assertIn("setless_figshare_aggregate", signals["vzs6na3qd8sizyhrp6u5"])
        self.assertIn(
            "pmh_set_contains_parameter_name", signals["rqyjzdijqflhirw6ao8v"]
        )

    def test_direct_manifest_must_match_query_exactly(self):
        observations, direct, corrections, existing = self.full_fixture()
        direct.clear()
        with self.assertRaisesRegex(backfill.PreflightError, "manifest parity failed"):
            backfill.build_plan(observations, direct, corrections, existing)

    def test_stale_exact_guard_is_rejected(self):
        observations, direct, corrections, existing = self.full_fixture()
        conflict = next(value for value in observations if value.endpoint_id == "conflict")
        direct["conflict"] = backfill.ManifestDecision(
            **{
                **direct["conflict"].__dict__,
                "expected_source_endpoint_source_id": conflict.source_endpoint_source_id + 1,
            }
        )
        with self.assertRaisesRegex(backfill.PreflightError, "stale exact-value guard"):
            backfill.build_plan(observations, direct, corrections, existing)

    def test_set_source_manifest_accepts_its_approved_post_state_on_rerun(self):
        observations, direct, corrections, existing = self.full_fixture()
        conflict_index = next(
            index for index, value in enumerate(observations) if value.endpoint_id == "conflict"
        )
        observations[conflict_index] = observation(
            "conflict", canonical=13, matcher=13, legacy=(14,)
        )
        archive_index = next(
            index
            for index, value in enumerate(observations)
            if value.endpoint_id == "6333d6bcffb78f92839"
        )
        observations[archive_index] = observation(
            "6333d6bcffb78f92839",
            canonical=4377196541,
            matcher=4210203682,
            legacy=(4210203682,),
        )

        plan = backfill.build_plan(observations, direct, corrections, existing)
        rows = {row.endpoint_id: row for row in plan}
        self.assertEqual(rows["conflict"].status, "NOOP")
        self.assertEqual(rows["6333d6bcffb78f92839"].status, "NOOP")

    def test_reviewed_set_source_can_correct_a_nonnull_canonical_value(self):
        observations, direct, corrections, existing = self.full_fixture()
        archive_index = next(
            index
            for index, value in enumerate(observations)
            if value.endpoint_id == "6333d6bcffb78f92839"
        )
        observations[archive_index] = observation(
            "6333d6bcffb78f92839",
            canonical=4210203682,
            matcher=4210203682,
            legacy=(4210203682,),
        )
        corrections["6333d6bcffb78f92839"] = decision(
            "6333d6bcffb78f92839",
            observations[archive_index],
            "SET_SOURCE",
            4377196541,
        )

        plan = backfill.build_plan(observations, direct, corrections, existing)
        row = next(
            value for value in plan if value.endpoint_id == "6333d6bcffb78f92839"
        )
        self.assertEqual(row.before_source_id, 4210203682)
        self.assertEqual(row.proposed_source_id, 4377196541)
        self.assertEqual(row.status, "UPDATE")

    def test_non_set_source_manifest_keeps_strict_canonical_guard(self):
        observations, direct, corrections, existing = self.full_fixture()
        datacite_index = next(
            index
            for index, value in enumerate(observations)
            if value.endpoint_id == "b174b390e74e8df2b0a"
        )
        observations[datacite_index] = observation(
            "b174b390e74e8df2b0a",
            canonical=4406922384,
            matcher=4406922384,
            legacy=(4406922384,),
        )
        with self.assertRaisesRegex(backfill.PreflightError, "stale exact-value guard"):
            backfill.build_plan(observations, direct, corrections, existing)

    def test_missing_target_source_is_blocked(self):
        observations, direct, corrections, existing = self.full_fixture()
        existing.remove(4377196541)
        plan = backfill.build_plan(observations, direct, corrections, existing)
        row = next(value for value in plan if value.endpoint_id == "6333d6bcffb78f92839")
        self.assertEqual(row.status, "BLOCKED")

    def test_plan_hash_is_deterministic_and_manifest_bound(self):
        observations, direct, corrections, existing = self.full_fixture()
        plan = backfill.build_plan(observations, direct, corrections, existing)
        payload_a = backfill.plan_payload(plan, "a" * 64, "b" * 64)
        payload_b = backfill.plan_payload(list(reversed(plan)), "a" * 64, "b" * 64)
        # build_plan is sorted; callers must not reorder its result before hashing.
        self.assertNotEqual(
            backfill.calculate_plan_hash(payload_a),
            backfill.calculate_plan_hash(payload_b),
        )
        self.assertEqual(
            backfill.calculate_plan_hash(payload_a),
            backfill.calculate_plan_hash(backfill.plan_payload(plan, "a" * 64, "b" * 64)),
        )
        self.assertNotEqual(
            backfill.calculate_plan_hash(payload_a),
            backfill.calculate_plan_hash(backfill.plan_payload(plan, "c" * 64, "b" * 64)),
        )

    def test_plan_hash_binds_endpoint_identity_fields(self):
        observations, direct, corrections, existing = self.full_fixture()
        plan = backfill.build_plan(observations, direct, corrections, existing)
        first = plan[0]
        changed = backfill.PlanRow(
            **{**first.__dict__, "pmh_url": "https://changed.example/oai"}
        )
        changed_plan = [changed, *plan[1:]]
        self.assertNotEqual(
            backfill.calculate_plan_hash(backfill.plan_payload(plan, "a" * 64, "b" * 64)),
            backfill.calculate_plan_hash(
                backfill.plan_payload(changed_plan, "a" * 64, "b" * 64)
            ),
        )

    def test_runtime_exact_guard_detects_drift(self):
        planned = backfill.PlanRow(
            endpoint_id="e1",
            before_source_id=None,
            observed_source_endpoint_source_id=10,
            observed_legacy_source_ids=(10,),
            proposed_source_id=10,
            reason="legacy_agreement",
            disposition="AUTO_SET",
            status="UPDATE",
        )
        changed = observation("e1", matcher=11, legacy=(10,))
        with self.assertRaisesRegex(backfill.ExecutionDrift, "exact guard drift"):
            backfill.assert_runtime_observation(planned, changed)

    def test_runtime_exact_guard_detects_endpoint_identity_drift(self):
        planned = backfill.PlanRow(
            endpoint_id="e1",
            before_source_id=None,
            observed_source_endpoint_source_id=10,
            observed_legacy_source_ids=(10,),
            proposed_source_id=10,
            reason="legacy_agreement",
            disposition="AUTO_SET",
            status="UPDATE",
            pmh_url="https://old.example/oai",
            pmh_set="journal-a",
            metadata_prefix="oai_dc",
        )
        changed = observation(
            "e1", matcher=10, legacy=(10,), pmh_url="https://new.example/oai"
        )
        with self.assertRaisesRegex(backfill.ExecutionDrift, "exact guard drift"):
            backfill.assert_runtime_observation(planned, changed)

    def test_execution_scope_hash_binds_selected_endpoint_ids(self):
        rows = [
            backfill.PlanRow(
                endpoint_id=endpoint_id,
                before_source_id=None,
                observed_source_endpoint_source_id=10,
                observed_legacy_source_ids=(10,),
                proposed_source_id=10,
                reason="legacy_agreement",
                disposition="AUTO_SET",
                status="UPDATE",
            )
            for endpoint_id in ("e1", "e2")
        ]
        one = backfill.execution_scope_payload("a" * 64, rows[:1], False)
        two = backfill.execution_scope_payload("a" * 64, rows, False)
        full = backfill.execution_scope_payload("a" * 64, rows, True)
        self.assertNotEqual(
            backfill.calculate_execution_scope_hash(one),
            backfill.calculate_execution_scope_hash(two),
        )
        self.assertNotEqual(
            backfill.calculate_execution_scope_hash(two),
            backfill.calculate_execution_scope_hash(full),
        )

    def test_review_package_is_complete_and_omits_evidence_text(self):
        observations, direct, corrections, existing = self.full_fixture()
        plan = backfill.build_plan(observations, direct, corrections, existing)
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "package"
            plan_hash, counts = backfill.write_review_package(
                output,
                observations,
                plan,
                direct,
                corrections,
                {"database_name": "scratch", "snapshot_id": "1:2:"},
                "a" * 64,
                "b" * 64,
            )
            expected = {
                "candidate_backfill.csv",
                "direct_conflicts.csv",
                "known_corrections.csv",
                "remaining_nulls.csv",
                "semantic_risk_report.csv",
                "counts.json",
                "plan.json",
                "run_metadata.json",
            }
            self.assertEqual({path.name for path in output.iterdir()}, expected)
            self.assertEqual(json.loads((output / "plan.json").read_text())["plan_sha256"], plan_hash)
            metadata = json.loads((output / "run_metadata.json").read_text())
            self.assertEqual(
                metadata["remaining_nulls_sha256"],
                backfill.sha256_bytes((output / "remaining_nulls.csv").read_bytes()),
            )
            self.assertEqual(counts["endpoint_rows"], len(observations))
            self.assertNotIn("reviewed evidence", (output / "known_corrections.csv").read_text())


class ExecutionGateTests(unittest.TestCase):
    def namespace(self, **overrides):
        values = {
            "execute": False,
            "approved_plan_hash": None,
            "approved_scope_hash": None,
            "expected_database": None,
            "run_id": None,
            "executed_by": None,
            "approval_ref": None,
            "writer_pause_ref": None,
            "endpoint_id": None,
            "full_run": False,
            "batch_size": 100,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_execute_requires_review_and_pause_fields(self):
        with self.assertRaisesRegex(backfill.PreflightError, "execute mode requires"):
            backfill.validate_execute_args(self.namespace(execute=True))

    def test_write_selection_requires_canary_or_full_run(self):
        plan = [
            backfill.PlanRow(
                endpoint_id="e1",
                before_source_id=None,
                observed_source_endpoint_source_id=10,
                observed_legacy_source_ids=(10,),
                proposed_source_id=10,
                reason="legacy_agreement",
                disposition="AUTO_SET",
                status="UPDATE",
            )
        ]
        with self.assertRaisesRegex(backfill.PreflightError, "explicit canary"):
            backfill.choose_execution_rows(plan, self.namespace(execute=True))
        selected = backfill.choose_execution_rows(
            plan, self.namespace(execute=True, endpoint_id=["e1"])
        )
        self.assertEqual([row.endpoint_id for row in selected], ["e1"])


if __name__ == "__main__":
    unittest.main()
