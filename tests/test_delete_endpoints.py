import argparse
import contextlib
import csv
import importlib.util
import io
import json
import os
import re
import sys
import tempfile
import unittest
import uuid
from dataclasses import replace
from pathlib import Path


ROOT = Path(__file__).parents[1]
MODULE_PATH = ROOT / "jobs" / "delete_endpoints.py"
MIGRATION_PATH = ROOT / "migrations" / "025_endpoint_deletion_audit.sql"
SPEC = importlib.util.spec_from_file_location("delete_endpoints", MODULE_PATH)
delete_endpoints = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = delete_endpoints
SPEC.loader.exec_module(delete_endpoints)


def manifest_row(
    endpoint_id="e1",
    reason="reviewed duplicate endpoint",
    review_ref="review://endpoint/e1",
    expected_source_id="10",
    expected_pmh_url="https://example.org/oai",
):
    return {
        "endpoint_id": endpoint_id,
        "reason": reason,
        "review_ref": review_ref,
        "expected_source_id": expected_source_id,
        "expected_pmh_url": expected_pmh_url,
    }


def write_manifest(path, rows, fieldnames=None):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames or delete_endpoints.MANIFEST_COLUMNS
        )
        writer.writeheader()
        writer.writerows(rows)


def observation(
    endpoint_id="e1",
    pmh_url="https://example.org/oai",
    pmh_set="set-a",
    source_id=10,
    legacy_source_id=11,
    repo_record_count=2,
):
    return delete_endpoints.Observation(
        endpoint_id=endpoint_id,
        pmh_url=pmh_url,
        pmh_set=pmh_set,
        source_id=source_id,
        legacy_source_endpoint_source_id=legacy_source_id,
        repo_record_count=repo_record_count,
    )


def decision(
    endpoint_id="e1",
    expected_source_id=10,
    expected_pmh_url="https://example.org/oai",
):
    return delete_endpoints.ManifestRow(
        endpoint_id=endpoint_id,
        reason="reviewed duplicate endpoint",
        review_ref=f"review://endpoint/{endpoint_id}",
        expected_source_id=expected_source_id,
        expected_pmh_url=expected_pmh_url,
    )


class ManifestTests(unittest.TestCase):
    def test_manifest_uses_raw_hash_and_parses_nullable_guards(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.csv"
            write_manifest(
                path,
                [
                    manifest_row(),
                    manifest_row(
                        endpoint_id="e-null",
                        review_ref="review://endpoint/e-null",
                        expected_source_id="NULL",
                        expected_pmh_url="",
                    ),
                ],
            )
            rows, digest = delete_endpoints.load_manifest(path)
            self.assertEqual(digest, delete_endpoints.sha256_bytes(path.read_bytes()))
            self.assertEqual(rows["e1"].expected_source_id, 10)
            self.assertIsNone(rows["e-null"].expected_source_id)
            self.assertIsNone(rows["e-null"].expected_pmh_url)

    def test_manifest_header_must_be_exact_and_unique(self):
        with tempfile.TemporaryDirectory() as temp:
            missing = Path(temp) / "missing.csv"
            write_manifest(
                missing,
                [
                    {
                        key: value
                        for key, value in manifest_row().items()
                        if key != "reason"
                    }
                ],
                [
                    column
                    for column in delete_endpoints.MANIFEST_COLUMNS
                    if column != "reason"
                ],
            )
            with self.assertRaisesRegex(
                delete_endpoints.PreflightError, "columns mismatch"
            ):
                delete_endpoints.load_manifest(missing)

            duplicate = Path(temp) / "duplicate.csv"
            duplicate.write_text(
                "endpoint_id,reason,review_ref,expected_source_id,expected_pmh_url,reason\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                delete_endpoints.PreflightError, "duplicate columns"
            ):
                delete_endpoints.load_manifest(duplicate)

    def test_manifest_refuses_duplicate_ids_placeholders_and_invalid_integers(self):
        cases = [
            ([manifest_row(), manifest_row()], "duplicate endpoint_id"),
            ([manifest_row(reason="TBD")], "reason"),
            ([manifest_row(review_ref="pending")], "review_ref"),
            ([manifest_row(expected_source_id="ten")], "must be an integer"),
        ]
        with tempfile.TemporaryDirectory() as temp:
            for index, (rows, error) in enumerate(cases):
                with self.subTest(error=error):
                    path = Path(temp) / f"case-{index}.csv"
                    write_manifest(path, rows)
                    with self.assertRaisesRegex(delete_endpoints.PreflightError, error):
                        delete_endpoints.load_manifest(path)

    def test_empty_manifest_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "empty.csv"
            write_manifest(path, [])
            with self.assertRaisesRegex(
                delete_endpoints.PreflightError, "at least one"
            ):
                delete_endpoints.load_manifest(path)

    def test_truncated_row_cannot_silently_turn_guards_into_null(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "truncated.csv"
            path.write_text(
                "endpoint_id,reason,review_ref,expected_source_id,expected_pmh_url\n"
                "e1,reviewed deletion,review://endpoint/e1\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                delete_endpoints.PreflightError, "missing cells"
            ):
                delete_endpoints.load_manifest(path)


class PlanningTests(unittest.TestCase):
    def test_plan_is_sorted_and_hash_binds_manifest_and_every_plan_field(self):
        manifest = {"e2": decision("e2"), "e1": decision("e1")}
        observations = [observation("e2"), observation("e1")]
        plan = delete_endpoints.build_plan(manifest, observations)
        self.assertEqual([row.endpoint_id for row in plan], ["e1", "e2"])

        payload = delete_endpoints.plan_payload(plan, "a" * 64)
        digest = delete_endpoints.calculate_plan_hash(payload)
        self.assertEqual(
            digest,
            delete_endpoints.calculate_plan_hash(
                delete_endpoints.plan_payload(list(reversed(plan)), "a" * 64)
            ),
        )
        self.assertNotEqual(
            digest,
            delete_endpoints.calculate_plan_hash(
                delete_endpoints.plan_payload(plan, "b" * 64)
            ),
        )

        changes = {
            "endpoint_id": "e0",
            "reason": "different reviewed reason",
            "review_ref": "review://different",
            "expected_source_id": 20,
            "expected_pmh_url": "https://expected.example/oai",
            "pmh_url": "https://observed.example/oai",
            "pmh_set": "different-set",
            "source_id_at_delete": 20,
            "legacy_source_endpoint_source_id": 21,
            "repo_record_count_at_delete": 3,
            "status": "KEEP",
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                changed = [replace(plan[0], **{field: value}), plan[1]]
                changed_digest = delete_endpoints.calculate_plan_hash(
                    delete_endpoints.plan_payload(changed, "a" * 64)
                )
                self.assertNotEqual(digest, changed_digest)

    def test_plan_refuses_missing_endpoint_and_expected_guard_mismatches(self):
        with self.assertRaisesRegex(
            delete_endpoints.PreflightError, "missing_endpoints"
        ):
            delete_endpoints.build_plan({"e1": decision()}, [])
        with self.assertRaisesRegex(delete_endpoints.PreflightError, "source/url"):
            delete_endpoints.build_plan(
                {"e1": decision(expected_source_id=99)}, [observation()]
            )
        with self.assertRaisesRegex(delete_endpoints.PreflightError, "source/url"):
            delete_endpoints.build_plan(
                {"e1": decision(expected_pmh_url="https://wrong.example/oai")},
                [observation()],
            )

    def test_runtime_guard_detects_every_observed_field(self):
        planned = delete_endpoints.build_plan({"e1": decision()}, [observation()])[0]
        changes = {
            "endpoint_id": "different",
            "pmh_url": "https://changed.example/oai",
            "pmh_set": "changed-set",
            "source_id": 20,
            "legacy_source_endpoint_source_id": 21,
            "repo_record_count": 3,
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                with self.assertRaisesRegex(
                    delete_endpoints.ExecutionDrift, "exact guard"
                ):
                    delete_endpoints.assert_runtime_observation(
                        planned, replace(observation(), **{field: value})
                    )


class ExecutionGateTests(unittest.TestCase):
    def namespace(self, **overrides):
        values = {
            "execute": False,
            "approved_plan_hash": None,
            "expected_database": None,
            "run_id": None,
            "executed_by": None,
            "approval_ref": None,
            "batch_size": 100,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def valid_execute_namespace(self, **overrides):
        values = {
            "execute": True,
            "approved_plan_hash": "a" * 64,
            "expected_database": "scratch",
            "run_id": str(uuid.uuid4()),
            "executed_by": "operator@example.org",
            "approval_ref": "review://execution/approved",
        }
        values.update(overrides)
        return self.namespace(**values)

    def test_dry_run_rejects_execute_only_arguments(self):
        with self.assertRaisesRegex(
            delete_endpoints.PreflightError, "without --execute"
        ):
            delete_endpoints.validate_execute_args(
                self.namespace(approved_plan_hash="a" * 64)
            )

    def test_execute_requires_all_review_gates(self):
        with self.assertRaisesRegex(
            delete_endpoints.PreflightError, "execute mode requires"
        ):
            delete_endpoints.validate_execute_args(self.namespace(execute=True))

    def test_execute_validates_hash_uuid_and_review_values(self):
        invalid = [
            ({"approved_plan_hash": "abc"}, "SHA-256"),
            ({"run_id": "not-a-uuid"}, "UUID"),
            ({"executed_by": "TBD"}, "executed_by"),
            ({"approval_ref": "pending"}, "approval_ref"),
            ({"expected_database": "unknown"}, "expected_database"),
        ]
        for overrides, error in invalid:
            with self.subTest(error=error):
                with self.assertRaisesRegex(delete_endpoints.PreflightError, error):
                    delete_endpoints.validate_execute_args(
                        self.valid_execute_namespace(**overrides)
                    )

    def test_batch_size_is_capped_at_500(self):
        for accepted in (1, 100, 500):
            delete_endpoints.validate_execute_args(self.namespace(batch_size=accepted))
        for refused in (0, -1, 501):
            with self.assertRaisesRegex(
                delete_endpoints.PreflightError, "between 1 and 500"
            ):
                delete_endpoints.validate_execute_args(
                    self.namespace(batch_size=refused)
                )


TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


@unittest.skipUnless(
    TEST_DATABASE_URL,
    "set TEST_DATABASE_URL to a disposable PostgreSQL database",
)
class PostgresIntegrationTests(unittest.TestCase):
    def setUp(self):
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        url = TEST_DATABASE_URL.replace("postgres://", "postgresql://", 1)
        self.schema = f"endpoint_delete_test_{uuid.uuid4().hex}"
        self.admin_engine = create_engine(url, poolclass=NullPool, future=True)
        with self.admin_engine.begin() as conn:
            conn.exec_driver_sql(f'CREATE SCHEMA "{self.schema}"')
        self.addCleanup(self._drop_schema)
        self.engine = create_engine(
            url,
            poolclass=NullPool,
            future=True,
            connect_args={"options": f"-csearch_path={self.schema}"},
        )
        self.addCleanup(self.engine.dispose)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE sources (id BIGINT PRIMARY KEY, endpoint_id TEXT)"
            )
            conn.exec_driver_sql(
                "CREATE TABLE endpoint ("
                "id TEXT PRIMARY KEY, pmh_url TEXT, pmh_set TEXT, "
                "source_id BIGINT REFERENCES sources(id) ON DELETE RESTRICT)"
            )
            conn.exec_driver_sql(
                "CREATE TABLE source_endpoint ("
                "endpoint_id TEXT PRIMARY KEY REFERENCES endpoint(id), "
                "source_id BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE)"
            )
            conn.exec_driver_sql(MIGRATION_PATH.read_text(encoding="utf-8"))

    def _drop_schema(self):
        try:
            with self.admin_engine.begin() as conn:
                conn.exec_driver_sql(f'DROP SCHEMA "{self.schema}" CASCADE')
        finally:
            self.admin_engine.dispose()

    def database_name(self):
        from sqlalchemy import text

        with self.engine.connect() as conn:
            return conn.execute(text("SELECT current_database()")).scalar_one()

    def test_dry_run_execute_receipt_and_immutable_triggers(self):
        from sqlalchemy import text
        from sqlalchemy.exc import DBAPIError

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO sources (id, endpoint_id) VALUES "
                    "(1, 'delete-me'), (2, 'delete-me'), (3, 'control'), "
                    "(101, NULL), (202, NULL), (303, NULL), (404, NULL)"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO endpoint (id, pmh_url, pmh_set, source_id) VALUES "
                    "('delete-me', 'https://repo.example/oai', 'set-1', 101), "
                    "('control', 'https://control.example/oai', NULL, 202)"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO source_endpoint (endpoint_id, source_id) "
                    "VALUES ('delete-me', 303), ('control', 404)"
                )
            )

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.csv"
            write_manifest(
                path,
                [
                    manifest_row(
                        endpoint_id="delete-me",
                        reason="reviewed obsolete endpoint",
                        review_ref="review://row/delete-me",
                        expected_source_id="101",
                        expected_pmh_url="https://repo.example/oai",
                    )
                ],
            )
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                result = delete_endpoints.main(
                    ["--manifest", str(path)], engine=self.engine
                )
            self.assertEqual(result, 0, stderr.getvalue())
            self.assertIn("DRY RUN ONLY", stdout.getvalue())
            match = re.search(r"PLAN SHA-256: ([0-9a-f]{64})", stdout.getvalue())
            self.assertIsNotNone(match)
            plan_hash = match.group(1)
            with self.engine.connect() as conn:
                self.assertEqual(
                    conn.execute(text("SELECT count(*) FROM endpoint")).scalar_one(), 2
                )
                self.assertEqual(
                    conn.execute(
                        text("SELECT count(*) FROM source_endpoint")
                    ).scalar_one(),
                    2,
                )
                self.assertEqual(
                    conn.execute(
                        text("SELECT count(*) FROM endpoint_deletion_audit")
                    ).scalar_one(),
                    0,
                )

            run_id = str(uuid.uuid4())
            stdout = io.StringIO()
            stderr = io.StringIO()
            args = [
                "--manifest",
                str(path),
                "--execute",
                "--approved-plan-hash",
                plan_hash,
                "--expected-database",
                self.database_name(),
                "--run-id",
                run_id,
                "--executed-by",
                "operator@example.org",
                "--approval-ref",
                "review://execution/approved",
            ]
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                result = delete_endpoints.main(args, engine=self.engine)
            self.assertEqual(result, 0, stderr.getvalue())
            self.assertIn("EXECUTE COMPLETE: 1/1", stdout.getvalue())

            with self.engine.connect() as conn:
                self.assertIsNone(
                    conn.execute(
                        text("SELECT id FROM endpoint WHERE id = 'delete-me'")
                    ).one_or_none()
                )
                self.assertIsNone(
                    conn.execute(
                        text(
                            "SELECT endpoint_id FROM source_endpoint "
                            "WHERE endpoint_id = 'delete-me'"
                        )
                    ).one_or_none()
                )
                self.assertEqual(
                    conn.execute(
                        text(
                            "SELECT count(*) FROM sources WHERE endpoint_id = 'delete-me'"
                        )
                    ).scalar_one(),
                    2,
                )
                receipt = (
                    conn.execute(
                        text(
                            "SELECT * FROM endpoint_deletion_audit "
                            "WHERE run_id = CAST(:run_id AS uuid)"
                        ),
                        {"run_id": run_id},
                    )
                    .mappings()
                    .one()
                )
            self.assertEqual(receipt["endpoint_id"], "delete-me")
            self.assertEqual(receipt["pmh_url"], "https://repo.example/oai")
            self.assertEqual(receipt["pmh_set"], "set-1")
            self.assertEqual(receipt["source_id_at_delete"], 101)
            self.assertEqual(receipt["legacy_source_endpoint_source_id"], 303)
            self.assertEqual(receipt["repo_record_count_at_delete"], 2)
            self.assertEqual(receipt["reason"], "reviewed obsolete endpoint")
            self.assertEqual(
                receipt["manifest_hash"],
                delete_endpoints.sha256_bytes(path.read_bytes()),
            )
            self.assertEqual(receipt["executed_by"], "operator@example.org")
            self.assertIsNotNone(receipt["executed_at"])
            receipt_review = json.loads(receipt["review_ref"])
            self.assertEqual(receipt_review["plan_sha256"], plan_hash)
            self.assertEqual(
                receipt_review["approval_ref"], "review://execution/approved"
            )
            self.assertEqual(receipt_review["row_review_ref"], "review://row/delete-me")

            mutations = [
                "UPDATE endpoint_deletion_audit SET reason = 'changed'",
                "DELETE FROM endpoint_deletion_audit",
                "TRUNCATE endpoint_deletion_audit",
            ]
            for statement in mutations:
                with self.subTest(statement=statement):
                    with self.assertRaises(DBAPIError):
                        with self.engine.begin() as conn:
                            conn.exec_driver_sql(statement)
            with self.engine.connect() as conn:
                self.assertEqual(
                    conn.execute(
                        text("SELECT count(*) FROM endpoint_deletion_audit")
                    ).scalar_one(),
                    1,
                )
                self.assertEqual(
                    conn.execute(
                        text("SELECT count(*) FROM endpoint WHERE id = 'control'")
                    ).scalar_one(),
                    1,
                )

    def test_drift_rolls_back_the_entire_current_batch(self):
        from sqlalchemy import text

        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO sources (id, endpoint_id) VALUES "
                "(1, NULL), (2, NULL), (11, NULL), (22, NULL)"
            )
            conn.exec_driver_sql(
                "INSERT INTO endpoint (id, pmh_url, pmh_set, source_id) VALUES "
                "('e1', 'https://e1.example/oai', 'old', 1), "
                "('e2', 'https://e2.example/oai', 'old', 2)"
            )
            conn.exec_driver_sql(
                "INSERT INTO source_endpoint (endpoint_id, source_id) "
                "VALUES ('e1', 11), ('e2', 22)"
            )
        manifest = {
            "e1": decision("e1", 1, "https://e1.example/oai"),
            "e2": decision("e2", 2, "https://e2.example/oai"),
        }
        observations, _ = delete_endpoints.read_snapshot(self.engine, manifest)
        plan = delete_endpoints.build_plan(manifest, observations)
        plan_hash = delete_endpoints.calculate_plan_hash(
            delete_endpoints.plan_payload(plan, "c" * 64)
        )
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE endpoint SET pmh_set = 'drifted' WHERE id = 'e2'")
            )
        run_id = str(uuid.uuid4())
        args = argparse.Namespace(
            batch_size=2,
            expected_database=self.database_name(),
            run_id=run_id,
            executed_by="operator@example.org",
            approval_ref="review://execution/approved",
            approved_plan_hash=plan_hash,
        )
        with self.assertRaisesRegex(
            delete_endpoints.ExecutionDrift, "exact guard drift"
        ):
            delete_endpoints.execute_batches(self.engine, plan, args, "c" * 64)
        with self.engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("SELECT count(*) FROM endpoint")).scalar_one(), 2
            )
            self.assertEqual(
                conn.execute(text("SELECT count(*) FROM source_endpoint")).scalar_one(),
                2,
            )
            self.assertEqual(
                conn.execute(
                    text(
                        "SELECT count(*) FROM endpoint_deletion_audit "
                        "WHERE run_id = CAST(:run_id AS uuid)"
                    ),
                    {"run_id": run_id},
                ).scalar_one(),
                0,
            )

    def test_active_sources_writer_refuses_batch_before_receipts_or_deletes(self):
        from sqlalchemy import text

        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO sources (id, endpoint_id) VALUES (1, NULL), (11, NULL)"
            )
            conn.exec_driver_sql(
                "INSERT INTO endpoint (id, pmh_url, pmh_set, source_id) "
                "VALUES ('e1', 'https://e1.example/oai', NULL, 1)"
            )
            conn.exec_driver_sql(
                "INSERT INTO source_endpoint (endpoint_id, source_id) VALUES ('e1', 11)"
            )
        manifest = {"e1": decision("e1", 1, "https://e1.example/oai")}
        observations, _ = delete_endpoints.read_snapshot(self.engine, manifest)
        plan = delete_endpoints.build_plan(manifest, observations)
        plan_hash = delete_endpoints.calculate_plan_hash(
            delete_endpoints.plan_payload(plan, "d" * 64)
        )
        run_id = str(uuid.uuid4())
        args = argparse.Namespace(
            batch_size=1,
            expected_database=self.database_name(),
            run_id=run_id,
            executed_by="operator@example.org",
            approval_ref="review://execution/approved",
            approved_plan_hash=plan_hash,
        )

        with self.engine.connect() as blocker:
            transaction = blocker.begin()
            blocker.exec_driver_sql("LOCK TABLE sources IN ROW EXCLUSIVE MODE")
            try:
                with self.assertRaisesRegex(
                    delete_endpoints.ExecutionDrift,
                    "could not lock sources against writes",
                ):
                    delete_endpoints.execute_batches(self.engine, plan, args, "d" * 64)
            finally:
                transaction.rollback()

        with self.engine.connect() as conn:
            self.assertEqual(
                conn.execute(text("SELECT count(*) FROM endpoint")).scalar_one(), 1
            )
            self.assertEqual(
                conn.execute(text("SELECT count(*) FROM source_endpoint")).scalar_one(),
                1,
            )
            self.assertEqual(
                conn.execute(
                    text(
                        "SELECT count(*) FROM endpoint_deletion_audit "
                        "WHERE run_id = CAST(:run_id AS uuid)"
                    ),
                    {"run_id": run_id},
                ).scalar_one(),
                0,
            )


if __name__ == "__main__":
    unittest.main()
