"""Plan and execute reviewed endpoint deletions for oxjob 83.13.

The default invocation is a database-read-only dry run. It prints the complete
fresh plan and the SHA-256 digest that must be approved before execution::

    python -m jobs.delete_endpoints --manifest /safe/local/endpoint-deletions.csv

``--execute`` additionally requires the approved plan hash, exact database name,
run UUID, actor, and execution approval reference. The manifest is the execution
scope. Every execution rebuilds the plan from a fresh database snapshot, deletes
``source_endpoint`` before ``endpoint``, and writes one durable audit receipt per
endpoint in the same transaction. Batches commit independently and may contain at
most 500 endpoints. Each write batch also takes a short-lived PostgreSQL SHARE
lock on ``sources`` so the legacy repository-record count cannot change between
its runtime guard and commit.

The manifest columns are ``endpoint_id``, ``reason``, ``review_ref``,
``expected_source_id``, and ``expected_pmh_url``. Blank or ``NULL`` in either
expected field means SQL NULL. Importing or deploying this module does not connect
to Postgres or run a job.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, cast


AUDIT_TABLE = "endpoint_deletion_audit"
PLAN_SCHEMA_VERSION = 1
MANIFEST_COLUMNS = (
    "endpoint_id",
    "reason",
    "review_ref",
    "expected_source_id",
    "expected_pmh_url",
)
PLACEHOLDER_VALUES = {"", "todo", "tbd", "unknown", "pending", "n/a"}


class PreflightError(RuntimeError):
    """The manifest, schema, or execution gates are not safe to use."""


class ExecutionDrift(RuntimeError):
    """The database changed after the reviewed plan was produced."""


@dataclass(frozen=True)
class ManifestRow:
    endpoint_id: str
    reason: str
    review_ref: str
    expected_source_id: Optional[int]
    expected_pmh_url: Optional[str]


@dataclass(frozen=True)
class Observation:
    endpoint_id: str
    pmh_url: Optional[str]
    pmh_set: Optional[str]
    source_id: Optional[int]
    legacy_source_endpoint_source_id: Optional[int]
    repo_record_count: int


@dataclass(frozen=True)
class PlanRow:
    endpoint_id: str
    reason: str
    review_ref: str
    expected_source_id: Optional[int]
    expected_pmh_url: Optional[str]
    pmh_url: Optional[str]
    pmh_set: Optional[str]
    source_id_at_delete: Optional[int]
    legacy_source_endpoint_source_id: Optional[int]
    repo_record_count_at_delete: int
    status: str = "DELETE"


SNAPSHOT_SQL = """
SELECT
    e.id AS endpoint_id,
    e.pmh_url,
    e.pmh_set,
    e.source_id,
    (
        SELECT se.source_id
        FROM source_endpoint se
        WHERE se.endpoint_id = e.id
    ) AS legacy_source_endpoint_source_id,
    (
        SELECT count(*)
        FROM sources s
        WHERE s.endpoint_id = e.id
    ) AS repo_record_count
FROM endpoint e
WHERE e.id = ANY(CAST(:endpoint_ids AS text[]))
ORDER BY e.id
"""


LOCK_ENDPOINT_SQL = """
SELECT
    e.id AS endpoint_id,
    e.pmh_url,
    e.pmh_set,
    e.source_id,
    (
        SELECT count(*)
        FROM sources s
        WHERE s.endpoint_id = e.id
    ) AS repo_record_count
FROM endpoint e
WHERE e.id = :endpoint_id
FOR UPDATE OF e
"""


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def require_review_value(value: Any, field: str, endpoint_id: str) -> str:
    cleaned = str(value or "").strip()
    if cleaned.lower() in PLACEHOLDER_VALUES:
        raise PreflightError(f"{endpoint_id}: {field} must be a real reviewed value")
    return cleaned


def parse_optional_int(raw: Any, field: str, endpoint_id: str) -> Optional[int]:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value or value.upper() == "NULL":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise PreflightError(
            f"{endpoint_id}: {field} must be an integer or blank/NULL, got {value!r}"
        ) from exc


def parse_optional_text(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value or value.upper() == "NULL":
        return None
    return value


def load_manifest(path: Path) -> tuple[dict[str, ManifestRow], str]:
    raw_bytes = path.read_bytes()
    digest = sha256_bytes(raw_bytes)
    try:
        text_value = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise PreflightError(f"{path}: manifest must be UTF-8") from exc

    reader = csv.DictReader(io.StringIO(text_value, newline=""))
    if reader.fieldnames is None:
        raise PreflightError(f"{path}: manifest has no header")
    duplicate_headers = sorted(
        name for name in set(reader.fieldnames) if reader.fieldnames.count(name) > 1
    )
    if duplicate_headers:
        raise PreflightError(f"{path}: duplicate columns {duplicate_headers}")
    expected_columns = set(MANIFEST_COLUMNS)
    actual_columns = set(reader.fieldnames)
    missing_columns = sorted(expected_columns - actual_columns)
    unexpected_columns = sorted(actual_columns - expected_columns)
    if missing_columns or unexpected_columns:
        raise PreflightError(
            f"{path}: manifest columns mismatch; missing={missing_columns}, "
            f"unexpected={unexpected_columns}"
        )

    decisions: dict[str, ManifestRow] = {}
    try:
        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise PreflightError(
                    f"{path}:{line_number}: row has values beyond the declared columns"
                )
            missing_cells = [
                column for column in MANIFEST_COLUMNS if row.get(column) is None
            ]
            if missing_cells:
                raise PreflightError(
                    f"{path}:{line_number}: row is missing cells for {missing_cells}"
                )
            endpoint_id = (row.get("endpoint_id") or "").strip()
            if not endpoint_id:
                raise PreflightError(f"{path}:{line_number}: endpoint_id is required")
            if endpoint_id in decisions:
                raise PreflightError(
                    f"{path}:{line_number}: duplicate endpoint_id {endpoint_id}"
                )
            decisions[endpoint_id] = ManifestRow(
                endpoint_id=endpoint_id,
                reason=require_review_value(row.get("reason"), "reason", endpoint_id),
                review_ref=require_review_value(
                    row.get("review_ref"), "review_ref", endpoint_id
                ),
                expected_source_id=parse_optional_int(
                    row.get("expected_source_id"), "expected_source_id", endpoint_id
                ),
                expected_pmh_url=parse_optional_text(row.get("expected_pmh_url")),
            )
    except csv.Error as exc:
        raise PreflightError(f"{path}: invalid CSV: {exc}") from exc
    if not decisions:
        raise PreflightError(f"{path}: manifest must contain at least one endpoint")
    return decisions, digest


def row_mapping(row: Any) -> Mapping[str, Any]:
    if hasattr(row, "_mapping"):
        return cast(Mapping[str, Any], row._mapping)
    if isinstance(row, Mapping):
        return row
    raise TypeError(f"unsupported database row type {type(row)!r}")


def observation_from_row(row: Any) -> Observation:
    mapped = row_mapping(row)
    count = int(mapped["repo_record_count"])
    if count < 0:
        raise PreflightError("database returned a negative repository record count")
    return Observation(
        endpoint_id=str(mapped["endpoint_id"]),
        pmh_url=mapped.get("pmh_url"),
        pmh_set=mapped.get("pmh_set"),
        source_id=None if mapped.get("source_id") is None else int(mapped["source_id"]),
        legacy_source_endpoint_source_id=(
            None
            if mapped.get("legacy_source_endpoint_source_id") is None
            else int(mapped["legacy_source_endpoint_source_id"])
        ),
        repo_record_count=count,
    )


def build_plan(
    manifest: Mapping[str, ManifestRow], observations: Sequence[Observation]
) -> list[PlanRow]:
    observed_by_id = {row.endpoint_id: row for row in observations}
    if len(observed_by_id) != len(observations):
        raise PreflightError("snapshot contains duplicate endpoint IDs")
    manifest_ids = set(manifest)
    observed_ids = set(observed_by_id)
    if manifest_ids != observed_ids:
        raise PreflightError(
            "manifest/snapshot parity failed: "
            f"missing_endpoints={sorted(manifest_ids - observed_ids)}, "
            f"unexpected_endpoints={sorted(observed_ids - manifest_ids)}"
        )

    plan: list[PlanRow] = []
    for endpoint_id in sorted(manifest):
        decision = manifest[endpoint_id]
        observed = observed_by_id[endpoint_id]
        expected = (decision.expected_source_id, decision.expected_pmh_url)
        # The manifest parser strips edge whitespace; compare the observed URL the
        # same way so a legacy row whose pmh_url carries a trailing space can be
        # matched (1 such row in the 2026-09-01 batch). Identity is by endpoint_id.
        actual = (observed.source_id, parse_optional_text(observed.pmh_url))
        if expected != actual:
            raise PreflightError(
                f"{endpoint_id}: stale exact-value guard; expected source/url={expected!r}, "
                f"observed={actual!r}"
            )
        plan.append(
            PlanRow(
                endpoint_id=endpoint_id,
                reason=decision.reason,
                review_ref=decision.review_ref,
                expected_source_id=decision.expected_source_id,
                expected_pmh_url=decision.expected_pmh_url,
                pmh_url=observed.pmh_url,
                pmh_set=observed.pmh_set,
                source_id_at_delete=observed.source_id,
                legacy_source_endpoint_source_id=(
                    observed.legacy_source_endpoint_source_id
                ),
                repo_record_count_at_delete=observed.repo_record_count,
            )
        )
    return plan


def plan_payload(plan: Sequence[PlanRow], manifest_sha256: str) -> dict[str, Any]:
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "manifest_sha256": manifest_sha256,
        "rows": [
            asdict(row) for row in sorted(plan, key=lambda value: value.endpoint_id)
        ],
    }


def calculate_plan_hash(payload: Mapping[str, Any]) -> str:
    return sha256_bytes(canonical_json(payload).encode("utf-8"))


def lazy_engine() -> Any:
    # Keep pure imports/tests independent of DATABASE_URL and application deps.
    from db import engine

    return engine


def verify_operational_schema(conn: Any) -> None:
    from sqlalchemy import text

    required_types = {
        ("endpoint", "id"): "text",
        ("endpoint", "pmh_url"): "text",
        ("endpoint", "pmh_set"): "text",
        ("endpoint", "source_id"): "bigint",
        ("source_endpoint", "endpoint_id"): "text",
        ("source_endpoint", "source_id"): "bigint",
        ("sources", "endpoint_id"): "text",
    }
    columns = {
        (row["table_name"], row["column_name"]): row
        for row in conn.execute(
            text(
                "SELECT table_name, column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema = current_schema() "
                "AND table_name IN ('endpoint', 'source_endpoint', 'sources')"
            )
        ).mappings()
    }
    missing = sorted(set(required_types) - set(columns))
    if missing:
        raise PreflightError(
            f"endpoint deletion requires current-main operational columns; missing={missing}"
        )
    wrong_types = {
        f"{table}.{column}": columns[(table, column)]["data_type"]
        for (table, column), expected in required_types.items()
        if columns[(table, column)]["data_type"] != expected
    }
    if wrong_types:
        raise PreflightError(
            f"endpoint deletion found unexpected column types {wrong_types}"
        )
    if columns[("source_endpoint", "source_id")]["is_nullable"] != "NO":
        raise PreflightError("source_endpoint.source_id must be NOT NULL")

    unique_endpoint = conn.execute(
        text(
            "SELECT EXISTS ("
            " SELECT 1 FROM pg_constraint c"
            " WHERE c.conrelid = 'source_endpoint'::regclass"
            "   AND c.contype IN ('p', 'u')"
            "   AND ("
            "     SELECT array_agg(a.attname ORDER BY key_column.ordinality)"
            "     FROM unnest(c.conkey) WITH ORDINALITY AS key_column(attnum, ordinality)"
            "     JOIN pg_attribute a"
            "       ON a.attrelid = c.conrelid AND a.attnum = key_column.attnum"
            "   ) = ARRAY['endpoint_id']::name[]"
            ")"
        )
    ).scalar_one()
    if not unique_endpoint:
        raise PreflightError("source_endpoint.endpoint_id must be unique")

    endpoint_fk = conn.execute(
        text(
            "SELECT EXISTS ("
            " SELECT 1 FROM pg_constraint c"
            " JOIN pg_attribute a ON a.attrelid = c.conrelid"
            "   AND a.attnum = c.conkey[1]"
            " JOIN pg_attribute ra ON ra.attrelid = c.confrelid"
            "   AND ra.attnum = c.confkey[1]"
            " WHERE c.conrelid = 'source_endpoint'::regclass"
            "   AND c.contype = 'f'"
            "   AND c.confrelid = 'endpoint'::regclass"
            "   AND a.attname = 'endpoint_id' AND ra.attname = 'id'"
            "   AND c.confdeltype IN ('a', 'r')"
            "   AND array_length(c.conkey, 1) = 1"
            "   AND array_length(c.confkey, 1) = 1"
            ")"
        )
    ).scalar_one()
    if not endpoint_fk:
        raise PreflightError(
            "source_endpoint.endpoint_id must reference endpoint.id with NO ACTION/RESTRICT"
        )


def read_snapshot(
    engine: Any, manifest: Mapping[str, ManifestRow]
) -> tuple[list[Observation], dict[str, Any]]:
    from sqlalchemy import text

    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            conn.exec_driver_sql(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            verify_operational_schema(conn)
            identity = (
                conn.execute(
                    text(
                        "SELECT current_database() AS database_name, "
                        "txid_current_snapshot()::text AS snapshot_id"
                    )
                )
                .mappings()
                .one()
            )
            rows = conn.execute(
                text(SNAPSHOT_SQL), {"endpoint_ids": sorted(manifest)}
            ).fetchall()
            observations = [observation_from_row(row) for row in rows]
            metadata = dict(identity)
        finally:
            transaction.rollback()
    return observations, metadata


def validate_audit_table(conn: Any) -> None:
    from sqlalchemy import text

    table_exists = conn.execute(
        text("SELECT to_regclass(current_schema() || '.endpoint_deletion_audit')")
    ).scalar_one_or_none()
    if table_exists is None:
        raise PreflightError(f"required audit table {AUDIT_TABLE} does not exist")

    required_types = {
        "run_id": "uuid",
        "endpoint_id": "text",
        "pmh_url": "text",
        "pmh_set": "text",
        "source_id_at_delete": "bigint",
        "legacy_source_endpoint_source_id": "bigint",
        "repo_record_count_at_delete": "bigint",
        "reason": "text",
        "manifest_hash": "text",
        "review_ref": "text",
        "executed_by": "text",
        "executed_at": "timestamp with time zone",
    }
    columns = {
        row["column_name"]: row
        for row in conn.execute(
            text(
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_schema = current_schema() "
                "AND table_name = 'endpoint_deletion_audit'"
            )
        ).mappings()
    }
    missing = sorted(set(required_types) - set(columns))
    if missing:
        raise PreflightError(f"{AUDIT_TABLE} is missing columns {missing}")
    wrong_types = {
        name: columns[name]["data_type"]
        for name, expected in required_types.items()
        if columns[name]["data_type"] != expected
    }
    if wrong_types:
        raise PreflightError(
            f"{AUDIT_TABLE} has unexpected column types {wrong_types}; "
            f"expected {required_types}"
        )
    nullable_columns = {
        "pmh_url",
        "pmh_set",
        "source_id_at_delete",
        "legacy_source_endpoint_source_id",
    }
    for name in set(required_types) - nullable_columns:
        if columns[name]["is_nullable"] != "NO":
            raise PreflightError(f"{AUDIT_TABLE}.{name} must be NOT NULL")
    for name in nullable_columns:
        if columns[name]["is_nullable"] != "YES":
            raise PreflightError(f"{AUDIT_TABLE}.{name} must permit NULL")
    if not columns["executed_at"]["column_default"]:
        raise PreflightError(f"{AUDIT_TABLE}.executed_at must have a database default")

    unique_run_endpoint = conn.execute(
        text(
            "SELECT EXISTS ("
            " SELECT 1 FROM pg_constraint c"
            " WHERE c.conrelid = 'endpoint_deletion_audit'::regclass"
            "   AND c.contype IN ('p', 'u')"
            "   AND ("
            "     SELECT array_agg(a.attname ORDER BY key_column.ordinality)"
            "     FROM unnest(c.conkey) WITH ORDINALITY AS key_column(attnum, ordinality)"
            "     JOIN pg_attribute a"
            "       ON a.attrelid = c.conrelid AND a.attnum = key_column.attnum"
            "   ) = ARRAY['run_id', 'endpoint_id']::name[]"
            ")"
        )
    ).scalar_one()
    if not unique_run_endpoint:
        raise PreflightError(
            f"{AUDIT_TABLE} must have a UNIQUE or PRIMARY KEY on (run_id, endpoint_id)"
        )

    trigger_rows = {
        row["tgname"]: row
        for row in conn.execute(
            text(
                "SELECT t.tgname, t.tgtype, t.tgenabled, p.proname "
                "FROM pg_trigger t "
                "JOIN pg_proc p ON p.oid = t.tgfoid "
                "WHERE t.tgrelid = 'endpoint_deletion_audit'::regclass "
                "AND NOT t.tgisinternal"
            )
        ).mappings()
    }
    expected_triggers = {
        "endpoint_deletion_audit_no_row_mutation": 27,
        "endpoint_deletion_audit_no_truncate": 34,
    }
    for name, trigger_type in expected_triggers.items():
        trigger = trigger_rows.get(name)
        if (
            trigger is None
            or int(trigger["tgtype"]) != trigger_type
            or trigger["tgenabled"] not in {"O", "A"}
            or trigger["proname"] != "reject_endpoint_deletion_audit_mutation"
        ):
            raise PreflightError(
                f"{AUDIT_TABLE} immutable trigger {name} is absent or disabled"
            )


def read_locked_observation(conn: Any, endpoint_id: str) -> Observation:
    from sqlalchemy import text

    endpoint_row = conn.execute(
        text(LOCK_ENDPOINT_SQL), {"endpoint_id": endpoint_id}
    ).one_or_none()
    if endpoint_row is None:
        raise ExecutionDrift(f"{endpoint_id}: endpoint disappeared")
    endpoint = row_mapping(endpoint_row)
    child_row = conn.execute(
        text(
            "SELECT source_id FROM source_endpoint "
            "WHERE endpoint_id = :endpoint_id FOR UPDATE"
        ),
        {"endpoint_id": endpoint_id},
    ).one_or_none()
    child_source_id = (
        None if child_row is None else int(row_mapping(child_row)["source_id"])
    )
    return Observation(
        endpoint_id=str(endpoint["endpoint_id"]),
        pmh_url=endpoint.get("pmh_url"),
        pmh_set=endpoint.get("pmh_set"),
        source_id=(
            None if endpoint.get("source_id") is None else int(endpoint["source_id"])
        ),
        legacy_source_endpoint_source_id=child_source_id,
        repo_record_count=int(endpoint["repo_record_count"]),
    )


def assert_runtime_observation(expected: PlanRow, actual: Observation) -> None:
    expected_values = (
        expected.endpoint_id,
        expected.pmh_url,
        expected.pmh_set,
        expected.source_id_at_delete,
        expected.legacy_source_endpoint_source_id,
        expected.repo_record_count_at_delete,
    )
    actual_values = (
        actual.endpoint_id,
        actual.pmh_url,
        actual.pmh_set,
        actual.source_id,
        actual.legacy_source_endpoint_source_id,
        actual.repo_record_count,
    )
    if expected_values != actual_values:
        raise ExecutionDrift(
            f"{expected.endpoint_id}: exact guard drift; expected={expected_values!r}, "
            f"observed={actual_values!r}"
        )


def validate_batch_size(batch_size: int) -> None:
    if batch_size <= 0 or batch_size > 500:
        raise PreflightError("--batch-size must be between 1 and 500")


def receipt_review_ref(args: argparse.Namespace, planned: PlanRow) -> str:
    return canonical_json(
        {
            "approval_ref": args.approval_ref,
            "plan_sha256": args.approved_plan_hash.lower(),
            "row_review_ref": planned.review_ref,
        }
    )


def execute_batches(
    engine: Any,
    rows: Sequence[PlanRow],
    args: argparse.Namespace,
    manifest_sha256: str,
) -> dict[str, Any]:
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    validate_batch_size(args.batch_size)
    ordered_rows = sorted(rows, key=lambda value: value.endpoint_id)
    committed_ids: list[str] = []
    for offset in range(0, len(ordered_rows), args.batch_size):
        batch = ordered_rows[offset : offset + args.batch_size]
        batch_ids: list[str] = []
        with engine.begin() as conn:
            conn.exec_driver_sql("SET LOCAL lock_timeout = '5s'")
            conn.exec_driver_sql("SET LOCAL statement_timeout = '60s'")
            actual_database = conn.execute(
                text("SELECT current_database()")
            ).scalar_one()
            if actual_database != args.expected_database:
                raise PreflightError(
                    f"database mismatch inside batch: expected {args.expected_database!r}, "
                    f"connected to {actual_database!r}"
                )
            verify_operational_schema(conn)
            validate_audit_table(conn)
            # sources.endpoint_id has no FK to endpoint on current main. A SHARE
            # table lock conflicts with INSERT/UPDATE/DELETE writers, making the
            # count receipt stable through this batch's guarded deletes. The 5s
            # lock timeout above makes active writers a refusal, not a long wait.
            try:
                conn.exec_driver_sql("LOCK TABLE sources IN SHARE MODE")
            except DBAPIError as exc:
                raise ExecutionDrift(
                    "could not lock sources against writes; no rows in this batch committed"
                ) from exc
            for planned in batch:
                actual = read_locked_observation(conn, planned.endpoint_id)
                assert_runtime_observation(planned, actual)

                audit_result = conn.execute(
                    text(
                        f"INSERT INTO {AUDIT_TABLE} ("
                        "run_id, endpoint_id, pmh_url, pmh_set, source_id_at_delete, "
                        "legacy_source_endpoint_source_id, repo_record_count_at_delete, "
                        "reason, manifest_hash, review_ref, executed_by"
                        ") VALUES ("
                        ":run_id, :endpoint_id, :pmh_url, :pmh_set, :source_id, "
                        ":legacy_source_id, :repo_record_count, :reason, :manifest_hash, "
                        ":review_ref, :executed_by"
                        ")"
                    ),
                    {
                        "run_id": args.run_id,
                        "endpoint_id": planned.endpoint_id,
                        "pmh_url": planned.pmh_url,
                        "pmh_set": planned.pmh_set,
                        "source_id": planned.source_id_at_delete,
                        "legacy_source_id": planned.legacy_source_endpoint_source_id,
                        "repo_record_count": planned.repo_record_count_at_delete,
                        "reason": planned.reason,
                        "manifest_hash": manifest_sha256,
                        "review_ref": receipt_review_ref(args, planned),
                        "executed_by": args.executed_by,
                    },
                )
                if audit_result.rowcount != 1:
                    raise ExecutionDrift(
                        f"{planned.endpoint_id}: audit INSERT affected "
                        f"{audit_result.rowcount}, expected 1"
                    )

                expected_child_count = (
                    0 if planned.legacy_source_endpoint_source_id is None else 1
                )
                child_result = conn.execute(
                    text(
                        "DELETE FROM source_endpoint "
                        "WHERE endpoint_id = :endpoint_id "
                        "AND source_id IS NOT DISTINCT FROM :legacy_source_id"
                    ),
                    {
                        "endpoint_id": planned.endpoint_id,
                        "legacy_source_id": planned.legacy_source_endpoint_source_id,
                    },
                )
                if child_result.rowcount != expected_child_count:
                    raise ExecutionDrift(
                        f"{planned.endpoint_id}: guarded source_endpoint DELETE affected "
                        f"{child_result.rowcount}, expected {expected_child_count}"
                    )

                endpoint_result = conn.execute(
                    text(
                        "DELETE FROM endpoint AS e "
                        "WHERE e.id = :endpoint_id "
                        "AND e.source_id IS NOT DISTINCT FROM :source_id "
                        "AND e.pmh_url IS NOT DISTINCT FROM :pmh_url "
                        "AND e.pmh_set IS NOT DISTINCT FROM :pmh_set "
                        "AND NOT EXISTS ("
                        "  SELECT 1 FROM source_endpoint se WHERE se.endpoint_id = e.id"
                        ") "
                        "AND ("
                        "  SELECT count(*) FROM sources s WHERE s.endpoint_id = e.id"
                        ") = :repo_record_count"
                    ),
                    {
                        "endpoint_id": planned.endpoint_id,
                        "source_id": planned.source_id_at_delete,
                        "pmh_url": planned.pmh_url,
                        "pmh_set": planned.pmh_set,
                        "repo_record_count": planned.repo_record_count_at_delete,
                    },
                )
                if endpoint_result.rowcount != 1:
                    raise ExecutionDrift(
                        f"{planned.endpoint_id}: guarded endpoint DELETE affected "
                        f"{endpoint_result.rowcount}, expected 1"
                    )
                batch_ids.append(planned.endpoint_id)

        committed_ids.extend(batch_ids)
        print(
            f"BATCH COMMITTED: {len(batch_ids)} endpoint(s); "
            f"total={len(committed_ids)}/{len(ordered_rows)}"
        )

    return {
        "run_id": args.run_id,
        "selected": len(ordered_rows),
        "committed": len(committed_ids),
        "committed_endpoint_ids": committed_ids,
        "status": "COMPLETE",
    }


def valid_uuid(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a UUID") from exc
    return str(parsed)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--execute", action="store_true", help="delete; default is database read-only"
    )
    parser.add_argument("--approved-plan-hash")
    parser.add_argument("--expected-database")
    parser.add_argument("--run-id", type=valid_uuid)
    parser.add_argument("--executed-by")
    parser.add_argument("--approval-ref")
    parser.add_argument("--batch-size", type=int, default=100)
    return parser


def validate_sha256(value: str, flag: str) -> None:
    if len(value) != 64 or any(
        char not in "0123456789abcdef" for char in value.lower()
    ):
        raise PreflightError(f"{flag} must be a SHA-256 hex digest")


def validate_execute_args(args: argparse.Namespace) -> None:
    validate_batch_size(args.batch_size)
    if not args.execute:
        execute_only = {
            "approved_plan_hash": args.approved_plan_hash,
            "expected_database": args.expected_database,
            "run_id": args.run_id,
            "executed_by": args.executed_by,
            "approval_ref": args.approval_ref,
        }
        supplied = sorted(
            key for key, value in execute_only.items() if value not in (None, False, [])
        )
        if supplied:
            raise PreflightError(
                f"execute-only arguments supplied without --execute: {supplied}"
            )
        return

    required = {
        "--approved-plan-hash": args.approved_plan_hash,
        "--expected-database": args.expected_database,
        "--run-id": args.run_id,
        "--executed-by": args.executed_by,
        "--approval-ref": args.approval_ref,
    }
    missing = [flag for flag, value in required.items() if not value]
    if missing:
        raise PreflightError(f"execute mode requires {', '.join(missing)}")
    validate_sha256(args.approved_plan_hash, "--approved-plan-hash")
    require_review_value(args.expected_database, "expected_database", "execution")
    require_review_value(args.executed_by, "executed_by", "execution")
    require_review_value(args.approval_ref, "approval_ref", "execution")
    try:
        args.run_id = str(uuid.UUID(str(args.run_id)))
    except ValueError as exc:
        raise PreflightError("--run-id must be a UUID") from exc


def main(argv: Optional[Sequence[str]] = None, engine: Any = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate_execute_args(args)
        manifest, manifest_sha256 = load_manifest(args.manifest)
        database_engine = engine or lazy_engine()
        observations, metadata = read_snapshot(database_engine, manifest)
        plan = build_plan(manifest, observations)
        payload = plan_payload(plan, manifest_sha256)
        plan_sha256 = calculate_plan_hash(payload)

        print("PLAN:")
        print(json.dumps(payload, indent=2, sort_keys=True))
        print(f"PLAN SHA-256: {plan_sha256}")
        print(f"DATABASE: {metadata['database_name']}")
        print(f"SNAPSHOT: {metadata['snapshot_id']}")
        if not args.execute:
            print("DRY RUN ONLY: no rows changed")
            return 0

        if plan_sha256.lower() != args.approved_plan_hash.lower():
            raise PreflightError(
                "fresh plan hash does not match --approved-plan-hash; review the new plan"
            )
        if metadata.get("database_name") != args.expected_database:
            raise PreflightError(
                f"database mismatch: expected {args.expected_database!r}, "
                f"connected to {metadata.get('database_name')!r}"
            )
        progress = execute_batches(database_engine, plan, args, manifest_sha256)
        print(f"EXECUTE COMPLETE: {progress['committed']}/{progress['selected']}")
        return 0
    except (OSError, PreflightError, ExecutionDrift) as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
