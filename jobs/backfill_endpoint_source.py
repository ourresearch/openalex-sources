"""Plan and execute the reviewed endpoint.source_id backfill for oxjob 83.13.

The normal invocation is read-only with respect to Postgres and writes a review
package only to a caller-selected local directory::

    python -m jobs.backfill_endpoint_source \
      --direct-conflicts /safe/local/direct_conflicts.csv \
      --known-corrections /safe/local/known_corrections.csv \
      --output-dir /safe/local/backfill-review-20260820

``--execute`` is deliberately difficult to invoke accidentally. It additionally
requires approved full-plan and execution-scope hashes, database name, run UUID,
actor, approval reference, writer-pause reference, and either explicit canary IDs
or ``--full-run``.
The database audit table must already exist with the reviewed contract described
by this module's audit-schema checks. Input manifests and generated packages are
operational evidence: keep them outside the application checkout and do not commit
them. Importing or deploying this module does not connect to Postgres or run a job.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence, cast


AUDIT_TABLE = "endpoint_source_backfill_audit"
PLAN_SCHEMA_VERSION = 1

COMMON_MANIFEST_COLUMNS = {
    "endpoint_id",
    "expected_canonical_source_id",
    "expected_source_endpoint_source_id",
    "expected_legacy_source_ids",
    "action",
    "target_source_id",
    "reviewed_by",
    "approval_ref",
    "evidence",
}
CORRECTION_ONLY_COLUMN = "supersedes_conflict_approval_ref"
DIRECT_ACTIONS = {"SET_SOURCE", "LEAVE_NULL", "ESCALATE"}
CORRECTION_ACTIONS = {"SET_SOURCE", "LEAVE_NULL", "HARD_DELETE", "ESCALATE"}
PLACEHOLDER_VALUES = {"", "todo", "tbd", "unknown", "pending", "n/a"}

# These are fail-closed identity canaries, not embedded disposition rows. A local,
# reviewed correction manifest must still carry the exact observations, action,
# reviewer, approval reference, and evidence for every one. Keeping the IDs here
# prevents a missing overlay from silently canonizing the four already-verified
# bad/special bindings during the ordinary copy pass.
PROTECTED_CORRECTION_ENDPOINTS: Mapping[str, Mapping[str, Any]] = {
    "b174b390e74e8df2b0a": {
        "label": "DataCite global OAI previously bound to Open MIND",
        "allowed_actions": {"LEAVE_NULL", "HARD_DELETE", "ESCALATE"},
    },
    "6333d6bcffb78f92839": {
        "label": "Internet Archive OAI previously bound to Kew",
        "allowed_actions": {"SET_SOURCE"},
        "required_target": 4377196541,
    },
    "b54a0400f7544929302": {
        "label": "UMich DLPS aggregate previously bound to Lincoln journal",
        "allowed_actions": {"LEAVE_NULL", "HARD_DELETE", "ESCALATE"},
    },
    "2b57dfbd43207095dbc": {
        "label": "CISION content-class exclusion",
        "allowed_actions": {"HARD_DELETE", "ESCALATE"},
    },
}


class PreflightError(RuntimeError):
    """The review inputs or observed state are not safe to use."""


class ExecutionDrift(RuntimeError):
    """The database changed after the reviewed plan was produced."""


@dataclass(frozen=True)
class Observation:
    endpoint_id: str
    canonical_source_id: Optional[int]
    source_endpoint_source_id: Optional[int]
    legacy_source_ids: tuple[int, ...]
    pmh_url: Optional[str] = None
    pmh_set: Optional[str] = None
    metadata_prefix: Optional[str] = None
    ready_to_run: Optional[bool] = None


@dataclass(frozen=True)
class ManifestDecision:
    endpoint_id: str
    expected_canonical_source_id: Optional[int]
    expected_source_endpoint_source_id: Optional[int]
    expected_legacy_source_ids: tuple[int, ...]
    action: str
    target_source_id: Optional[int]
    reviewed_by: str
    approval_ref: str
    evidence: str
    manifest_kind: str
    manifest_sha256: str
    supersedes_conflict_approval_ref: str = ""


@dataclass(frozen=True)
class PlanRow:
    endpoint_id: str
    before_source_id: Optional[int]
    observed_source_endpoint_source_id: Optional[int]
    observed_legacy_source_ids: tuple[int, ...]
    proposed_source_id: Optional[int]
    reason: str
    disposition: str
    status: str
    manifest_kind: str = ""
    manifest_approval_ref: str = ""
    pmh_url: Optional[str] = None
    pmh_set: Optional[str] = None
    metadata_prefix: Optional[str] = None
    ready_to_run: Optional[bool] = None


SNAPSHOT_SQL = """
SELECT
    e.id AS endpoint_id,
    e.source_id AS canonical_source_id,
    se.source_id AS source_endpoint_source_id,
    ARRAY(
        SELECT s.id
        FROM sources s
        WHERE s.endpoint_id = e.id
        ORDER BY s.id
    ) AS legacy_source_ids,
    e.pmh_url,
    e.pmh_set,
    e.metadata_prefix,
    e.ready_to_run
FROM endpoint e
LEFT JOIN source_endpoint se ON se.endpoint_id = e.id
ORDER BY e.id
"""

ONE_OBSERVATION_SQL = """
SELECT
    e.id AS endpoint_id,
    e.source_id AS canonical_source_id,
    se.source_id AS source_endpoint_source_id,
    ARRAY(
        SELECT s.id
        FROM sources s
        WHERE s.endpoint_id = e.id
        ORDER BY s.id
    ) AS legacy_source_ids,
    e.pmh_url,
    e.pmh_set,
    e.metadata_prefix,
    e.ready_to_run
FROM endpoint e
LEFT JOIN source_endpoint se ON se.endpoint_id = e.id
WHERE e.id = :endpoint_id
FOR UPDATE OF e
"""


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


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


def parse_legacy_ids(raw: Any, endpoint_id: str) -> tuple[int, ...]:
    value = str(raw or "").strip()
    if not value or value.upper() == "NULL":
        return ()
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise PreflightError(
            f"{endpoint_id}: expected_legacy_source_ids must be a JSON array, got {value!r}"
        ) from exc
    if not isinstance(decoded, list) or any(isinstance(v, bool) or not isinstance(v, int) for v in decoded):
        raise PreflightError(
            f"{endpoint_id}: expected_legacy_source_ids must be a JSON array of integers"
        )
    if len(decoded) != len(set(decoded)):
        raise PreflightError(f"{endpoint_id}: expected_legacy_source_ids contains duplicates")
    if decoded != sorted(decoded):
        raise PreflightError(f"{endpoint_id}: expected_legacy_source_ids must be sorted")
    return tuple(decoded)


def require_review_value(value: str, field: str, endpoint_id: str) -> str:
    cleaned = (value or "").strip()
    if cleaned.lower() in PLACEHOLDER_VALUES:
        raise PreflightError(f"{endpoint_id}: {field} must be a real reviewed value")
    return cleaned


def load_manifest(path: Path, manifest_kind: str) -> tuple[dict[str, ManifestDecision], str]:
    if manifest_kind not in {"direct_conflict", "known_correction"}:
        raise ValueError(f"unknown manifest kind {manifest_kind}")
    raw_bytes = path.read_bytes()
    digest = sha256_bytes(raw_bytes)
    try:
        text_value = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise PreflightError(f"{path}: manifest must be UTF-8") from exc

    reader = csv.DictReader(io.StringIO(text_value, newline=""))
    if reader.fieldnames is None:
        raise PreflightError(f"{path}: manifest has no header")
    required = set(COMMON_MANIFEST_COLUMNS)
    if manifest_kind == "known_correction":
        required.add(CORRECTION_ONLY_COLUMN)
    missing_columns = sorted(required - set(reader.fieldnames))
    if missing_columns:
        raise PreflightError(f"{path}: missing columns {missing_columns}")

    allowed_actions = DIRECT_ACTIONS if manifest_kind == "direct_conflict" else CORRECTION_ACTIONS
    decisions: dict[str, ManifestDecision] = {}
    for line_number, row in enumerate(reader, start=2):
        endpoint_id = (row.get("endpoint_id") or "").strip()
        if not endpoint_id:
            raise PreflightError(f"{path}:{line_number}: endpoint_id is required")
        if endpoint_id in decisions:
            raise PreflightError(f"{path}:{line_number}: duplicate endpoint_id {endpoint_id}")
        action = (row.get("action") or "").strip().upper()
        if action not in allowed_actions:
            raise PreflightError(
                f"{path}:{line_number}: action must be one of {sorted(allowed_actions)}, got {action!r}"
            )
        target = parse_optional_int(row.get("target_source_id"), "target_source_id", endpoint_id)
        if action == "SET_SOURCE" and target is None:
            raise PreflightError(f"{endpoint_id}: SET_SOURCE requires target_source_id")
        if action != "SET_SOURCE" and target is not None:
            raise PreflightError(f"{endpoint_id}: {action} requires a blank target_source_id")

        decisions[endpoint_id] = ManifestDecision(
            endpoint_id=endpoint_id,
            expected_canonical_source_id=parse_optional_int(
                row.get("expected_canonical_source_id"),
                "expected_canonical_source_id",
                endpoint_id,
            ),
            expected_source_endpoint_source_id=parse_optional_int(
                row.get("expected_source_endpoint_source_id"),
                "expected_source_endpoint_source_id",
                endpoint_id,
            ),
            expected_legacy_source_ids=parse_legacy_ids(
                row.get("expected_legacy_source_ids"), endpoint_id
            ),
            action=action,
            target_source_id=target,
            reviewed_by=require_review_value(row.get("reviewed_by") or "", "reviewed_by", endpoint_id),
            approval_ref=require_review_value(row.get("approval_ref") or "", "approval_ref", endpoint_id),
            evidence=require_review_value(row.get("evidence") or "", "evidence", endpoint_id),
            manifest_kind=manifest_kind,
            manifest_sha256=digest,
            supersedes_conflict_approval_ref=(
                row.get(CORRECTION_ONLY_COLUMN) or ""
            ).strip(),
        )
    return decisions, digest


def row_mapping(row: Any) -> Mapping[str, Any]:
    if hasattr(row, "_mapping"):
        return cast(Mapping[str, Any], row._mapping)
    if isinstance(row, Mapping):
        return row
    raise TypeError(f"unsupported database row type {type(row)!r}")


def observation_from_row(row: Any) -> Observation:
    mapped = row_mapping(row)
    return Observation(
        endpoint_id=str(mapped["endpoint_id"]),
        canonical_source_id=(
            None if mapped["canonical_source_id"] is None else int(mapped["canonical_source_id"])
        ),
        source_endpoint_source_id=(
            None
            if mapped["source_endpoint_source_id"] is None
            else int(mapped["source_endpoint_source_id"])
        ),
        legacy_source_ids=tuple(int(value) for value in (mapped["legacy_source_ids"] or ())),
        pmh_url=mapped.get("pmh_url"),
        pmh_set=mapped.get("pmh_set"),
        metadata_prefix=mapped.get("metadata_prefix"),
        ready_to_run=mapped.get("ready_to_run"),
    )


def is_direct_conflict(observation: Observation) -> bool:
    return (
        observation.source_endpoint_source_id is not None
        and bool(observation.legacy_source_ids)
        and observation.legacy_source_ids != (observation.source_endpoint_source_id,)
    )


def assert_decision_matches(decision: ManifestDecision, observation: Observation) -> None:
    expected = (
        decision.expected_canonical_source_id,
        decision.expected_source_endpoint_source_id,
        decision.expected_legacy_source_ids,
    )
    actual = (
        observation.canonical_source_id,
        observation.source_endpoint_source_id,
        observation.legacy_source_ids,
    )
    if expected != actual:
        raise PreflightError(
            f"{decision.manifest_kind} {decision.endpoint_id}: stale exact-value guard; "
            f"expected canonical/matcher/legacy={expected!r}, observed={actual!r}"
        )


def validate_manifest_parity(
    observations: Sequence[Observation],
    direct: Mapping[str, ManifestDecision],
    corrections: Mapping[str, ManifestDecision],
) -> None:
    by_id = {row.endpoint_id: row for row in observations}
    if len(by_id) != len(observations):
        raise PreflightError("snapshot contains duplicate endpoint IDs")

    actual_conflicts = {row.endpoint_id for row in observations if is_direct_conflict(row)}
    direct_ids = set(direct)
    if actual_conflicts != direct_ids:
        raise PreflightError(
            "direct-conflict manifest parity failed: "
            f"missing={sorted(actual_conflicts - direct_ids)}, extra={sorted(direct_ids - actual_conflicts)}"
        )

    missing_protected = set(PROTECTED_CORRECTION_ENDPOINTS) - set(corrections)
    if missing_protected:
        labels = [
            f"{endpoint_id} ({PROTECTED_CORRECTION_ENDPOINTS[endpoint_id]['label']})"
            for endpoint_id in sorted(missing_protected)
        ]
        raise PreflightError(
            "known-correction manifest must explicitly adjudicate protected endpoints: "
            + ", ".join(labels)
        )

    for decision in list(direct.values()) + list(corrections.values()):
        observation = by_id.get(decision.endpoint_id)
        if observation is None:
            raise PreflightError(
                f"{decision.manifest_kind} {decision.endpoint_id}: endpoint is absent from snapshot"
            )
        assert_decision_matches(decision, observation)

    for endpoint_id, correction in corrections.items():
        conflict = direct.get(endpoint_id)
        supersedes = correction.supersedes_conflict_approval_ref
        if conflict:
            if supersedes != conflict.approval_ref:
                raise PreflightError(
                    f"known_correction {endpoint_id}: supersedes_conflict_approval_ref must equal "
                    f"the direct decision approval_ref {conflict.approval_ref!r}"
                )
        elif supersedes:
            raise PreflightError(
                f"known_correction {endpoint_id}: supersedes_conflict_approval_ref is set but "
                "the endpoint is not a direct conflict"
            )

    for endpoint_id, safety in PROTECTED_CORRECTION_ENDPOINTS.items():
        decision = corrections[endpoint_id]
        if decision.action not in safety["allowed_actions"]:
            raise PreflightError(
                f"known_correction {endpoint_id} ({safety['label']}): action {decision.action} "
                f"is not allowed; choose one of {sorted(safety['allowed_actions'])}"
            )
        required_target = safety.get("required_target")
        if required_target is not None and decision.target_source_id != required_target:
            raise PreflightError(
                f"known_correction {endpoint_id} ({safety['label']}): target must be {required_target}"
            )


def base_decision(observation: Observation) -> tuple[Optional[int], str, str]:
    se_id = observation.source_endpoint_source_id
    legacy_ids = observation.legacy_source_ids
    if se_id is not None and legacy_ids == (se_id,):
        return se_id, "legacy_agreement", "AUTO_SET"
    if se_id is not None and not legacy_ids:
        return se_id, "source_endpoint_only", "AUTO_SET"
    if se_id is None and legacy_ids:
        return None, "legacy_source_side_only_requires_review", "REVIEW"
    if se_id is not None and legacy_ids:
        return None, "direct_conflict_requires_manifest", "REVIEW"
    return None, "no_legacy_relationship", "REVIEW"


def apply_manifest_decision(
    decision: ManifestDecision,
) -> tuple[Optional[int], str, str]:
    if decision.action == "SET_SOURCE":
        return (
            decision.target_source_id,
            f"{decision.manifest_kind}:set_source",
            "SET_SOURCE",
        )
    return None, f"{decision.manifest_kind}:{decision.action.lower()}", decision.action


def build_plan(
    observations: Sequence[Observation],
    direct: Mapping[str, ManifestDecision],
    corrections: Mapping[str, ManifestDecision],
    existing_source_ids: set[int],
) -> list[PlanRow]:
    validate_manifest_parity(observations, direct, corrections)
    plan: list[PlanRow] = []
    for observation in sorted(observations, key=lambda value: value.endpoint_id):
        proposed, reason, disposition = base_decision(observation)
        manifest_kind = ""
        manifest_approval_ref = ""

        direct_decision = direct.get(observation.endpoint_id)
        if direct_decision:
            proposed, reason, disposition = apply_manifest_decision(direct_decision)
            manifest_kind = direct_decision.manifest_kind
            manifest_approval_ref = direct_decision.approval_ref

        correction = corrections.get(observation.endpoint_id)
        if correction:
            proposed, reason, disposition = apply_manifest_decision(correction)
            manifest_kind = correction.manifest_kind
            manifest_approval_ref = correction.approval_ref

        if proposed is not None and proposed not in existing_source_ids:
            status = "BLOCKED"
            reason = f"{reason}:target_source_missing"
        elif proposed is None:
            if observation.canonical_source_id is None:
                status = "REMAINING_NULL"
            else:
                # This job never silently clears an already-populated canonical
                # value. A distinct reviewed correction contract would be needed.
                status = "BLOCKED"
                reason = f"{reason}:canonical_nonnull_requires_review"
        elif observation.canonical_source_id == proposed:
            status = "NOOP"
        elif observation.canonical_source_id is None:
            status = "UPDATE"
        elif manifest_kind and disposition == "SET_SOURCE":
            status = "UPDATE"
        else:
            status = "BLOCKED"
            reason = f"{reason}:canonical_conflict_requires_manifest"

        plan.append(
            PlanRow(
                endpoint_id=observation.endpoint_id,
                before_source_id=observation.canonical_source_id,
                observed_source_endpoint_source_id=observation.source_endpoint_source_id,
                observed_legacy_source_ids=observation.legacy_source_ids,
                proposed_source_id=proposed,
                reason=reason,
                disposition=disposition,
                status=status,
                manifest_kind=manifest_kind,
                manifest_approval_ref=manifest_approval_ref,
                pmh_url=observation.pmh_url,
                pmh_set=observation.pmh_set,
                metadata_prefix=observation.metadata_prefix,
                ready_to_run=observation.ready_to_run,
            )
        )
    return plan


def plan_payload(
    plan: Sequence[PlanRow], direct_sha256: str, corrections_sha256: str
) -> dict[str, Any]:
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "direct_conflict_manifest_sha256": direct_sha256,
        "known_correction_manifest_sha256": corrections_sha256,
        "rows": [
            {
                "endpoint_id": row.endpoint_id,
                "before_source_id": row.before_source_id,
                "observed_source_endpoint_source_id": row.observed_source_endpoint_source_id,
                "observed_legacy_source_ids": list(row.observed_legacy_source_ids),
                "proposed_source_id": row.proposed_source_id,
                "reason": row.reason,
                "disposition": row.disposition,
                "status": row.status,
                "manifest_kind": row.manifest_kind,
                "manifest_approval_ref": row.manifest_approval_ref,
                "pmh_url": row.pmh_url,
                "pmh_set": row.pmh_set,
                "metadata_prefix": row.metadata_prefix,
            }
            for row in plan
        ],
    }


def calculate_plan_hash(payload: Mapping[str, Any]) -> str:
    return sha256_bytes(canonical_json(payload).encode("utf-8"))


def execution_scope_payload(
    plan_sha256: str, rows: Sequence[PlanRow], full_run: bool
) -> dict[str, Any]:
    return {
        "plan_sha256": plan_sha256,
        "mode": "FULL_RUN" if full_run else "EXPLICIT_ENDPOINTS",
        "endpoint_ids": sorted(row.endpoint_id for row in rows),
    }


def calculate_execution_scope_hash(payload: Mapping[str, Any]) -> str:
    return sha256_bytes(canonical_json(payload).encode("utf-8"))


def manifest_combined_hash(direct_sha256: str, corrections_sha256: str) -> str:
    return sha256_bytes(f"{direct_sha256}\n{corrections_sha256}\n".encode("ascii"))


def counts_for_plan(observations: Sequence[Observation], plan: Sequence[PlanRow]) -> dict[str, Any]:
    status_counts = Counter(row.status for row in plan)
    reason_counts = Counter(row.reason for row in plan)
    relationship_counts: Counter[str] = Counter()
    for row in observations:
        if is_direct_conflict(row):
            relationship_counts["direct_conflict"] += 1
        elif row.source_endpoint_source_id is not None and row.legacy_source_ids == (
            row.source_endpoint_source_id,
        ):
            relationship_counts["legacy_agreement"] += 1
        elif row.source_endpoint_source_id is not None and not row.legacy_source_ids:
            relationship_counts["source_endpoint_only"] += 1
        elif row.source_endpoint_source_id is None and row.legacy_source_ids:
            relationship_counts["legacy_source_side_only"] += 1
        else:
            relationship_counts["neither_legacy_store"] += 1
    return {
        "endpoint_rows": len(observations),
        "already_populated_canonical": sum(
            row.canonical_source_id is not None for row in observations
        ),
        "status_counts": dict(sorted(status_counts.items())),
        "relationship_counts": dict(sorted(relationship_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
    }


def csv_scalar(value: Any) -> Any:
    if isinstance(value, tuple):
        return json.dumps(list(value), separators=(",", ":"))
    if value is None:
        return ""
    return value


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: csv_scalar(row.get(key)) for key in fieldnames})
    temporary.replace(path)


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def prepare_output_directory(path: Path) -> None:
    if path.exists() and any(path.iterdir()):
        raise PreflightError(f"output directory must be absent or empty: {path}")
    path.mkdir(parents=True, exist_ok=True)


def decision_report_row(
    observation: Observation, decision: ManifestDecision
) -> dict[str, Any]:
    return {
        "endpoint_id": observation.endpoint_id,
        "observed_canonical_source_id": observation.canonical_source_id,
        "observed_source_endpoint_source_id": observation.source_endpoint_source_id,
        "observed_legacy_source_ids": observation.legacy_source_ids,
        "action": decision.action,
        "target_source_id": decision.target_source_id,
        "reviewed_by": decision.reviewed_by,
        "approval_ref": decision.approval_ref,
        "evidence_sha256": sha256_bytes(decision.evidence.encode("utf-8")),
        "manifest_sha256": decision.manifest_sha256,
        "supersedes_conflict_approval_ref": decision.supersedes_conflict_approval_ref,
    }


def semantic_risk_rows(
    observations: Sequence[Observation], corrections: Mapping[str, ManifestDecision]
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for observation in observations:
        signals: list[str] = []
        if observation.endpoint_id in corrections:
            signals.append("reviewed_known_correction")
        if len(observation.legacy_source_ids) > 1:
            signals.append("multiple_legacy_source_side_pointers")
        url = (observation.pmh_url or "").lower()
        if "oai.datacite.org" in url:
            signals.append("known_global_datacite_service")
        if "archive.org/services/oai" in url:
            signals.append("known_aggregate_internet_archive_service")
        if signals:
            results.append(
                {
                    "endpoint_id": observation.endpoint_id,
                    "signals": ";".join(signals),
                    "pmh_url": observation.pmh_url,
                    "advisory_only": True,
                }
            )
    return results


def write_review_package(
    output_dir: Path,
    observations: Sequence[Observation],
    plan: Sequence[PlanRow],
    direct: Mapping[str, ManifestDecision],
    corrections: Mapping[str, ManifestDecision],
    metadata: Mapping[str, Any],
    direct_sha256: str,
    corrections_sha256: str,
) -> tuple[str, dict[str, Any]]:
    prepare_output_directory(output_dir)
    payload = plan_payload(plan, direct_sha256, corrections_sha256)
    plan_hash = calculate_plan_hash(payload)
    counts = counts_for_plan(observations, plan)
    by_id = {row.endpoint_id: row for row in observations}

    plan_fields = [
        "endpoint_id",
        "before_source_id",
        "observed_source_endpoint_source_id",
        "observed_legacy_source_ids",
        "proposed_source_id",
        "reason",
        "disposition",
        "status",
        "manifest_kind",
        "manifest_approval_ref",
        "pmh_url",
        "pmh_set",
        "metadata_prefix",
        "ready_to_run",
    ]
    write_csv(
        output_dir / "candidate_backfill.csv",
        plan_fields,
        (asdict(row) for row in plan if row.proposed_source_id is not None),
    )
    decision_fields = [
        "endpoint_id",
        "observed_canonical_source_id",
        "observed_source_endpoint_source_id",
        "observed_legacy_source_ids",
        "action",
        "target_source_id",
        "reviewed_by",
        "approval_ref",
        "evidence_sha256",
        "manifest_sha256",
        "supersedes_conflict_approval_ref",
    ]
    write_csv(
        output_dir / "direct_conflicts.csv",
        decision_fields,
        (decision_report_row(by_id[endpoint_id], decision) for endpoint_id, decision in sorted(direct.items())),
    )
    write_csv(
        output_dir / "known_corrections.csv",
        decision_fields,
        (
            decision_report_row(by_id[endpoint_id], decision)
            for endpoint_id, decision in sorted(corrections.items())
        ),
    )
    write_csv(
        output_dir / "remaining_nulls.csv",
        plan_fields,
        (asdict(row) for row in plan if row.proposed_source_id is None),
    )
    write_csv(
        output_dir / "semantic_risk_report.csv",
        ["endpoint_id", "signals", "pmh_url", "advisory_only"],
        semantic_risk_rows(observations, corrections),
    )
    write_json(output_dir / "counts.json", counts)
    write_json(
        output_dir / "plan.json",
        {"plan_sha256": plan_hash, "plan": payload},
    )
    package_metadata = {
        **dict(metadata),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "DRY_RUN",
        "plan_schema_version": PLAN_SCHEMA_VERSION,
        "plan_sha256": plan_hash,
        "direct_conflict_manifest_sha256": direct_sha256,
        "known_correction_manifest_sha256": corrections_sha256,
        "combined_manifest_sha256": manifest_combined_hash(
            direct_sha256, corrections_sha256
        ),
        "input_manifests_copied_to_package": False,
    }
    write_json(output_dir / "run_metadata.json", package_metadata)
    return plan_hash, counts


def lazy_engine() -> Any:
    # Imported only when the command actually needs a database. Unit tests and
    # static imports therefore do not require DATABASE_URL or application deps.
    from db import engine

    return engine


def verify_022_schema(conn: Any) -> dict[str, Any]:
    from sqlalchemy import text

    column = conn.execute(
        text(
            "SELECT data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = 'endpoint' "
            "AND column_name = 'source_id'"
        )
    ).mappings().one_or_none()
    if not column or column["data_type"] != "bigint" or column["is_nullable"] != "YES":
        raise PreflightError("migration 022 nullable BIGINT endpoint.source_id is not present")

    fk_ok = conn.execute(
        text(
            "SELECT EXISTS ("
            " SELECT 1 FROM pg_constraint c"
            " JOIN pg_class t ON t.oid = c.conrelid"
            " JOIN pg_attribute a ON a.attrelid = c.conrelid"
            "   AND a.attnum = c.conkey[1]"
            " JOIN pg_attribute ra ON ra.attrelid = c.confrelid"
            "   AND ra.attnum = c.confkey[1]"
            " WHERE t.oid = 'endpoint'::regclass"
            "   AND c.conname = 'endpoint_source_id_fkey'"
            "   AND c.contype = 'f' AND c.confdeltype = 'r'"
            "   AND c.confrelid = 'sources'::regclass"
            "   AND a.attname = 'source_id' AND ra.attname = 'id'"
            "   AND array_length(c.conkey, 1) = 1"
            "   AND array_length(c.confkey, 1) = 1"
            ")"
        )
    ).scalar_one()
    if not fk_ok:
        raise PreflightError("endpoint_source_id_fkey with ON DELETE RESTRICT is absent")

    index_ok = conn.execute(
        text(
            "SELECT to_regclass(current_schema() || '.idx_endpoint_source_id') IS NOT NULL"
        )
    ).scalar_one()
    if not index_ok:
        raise PreflightError("idx_endpoint_source_id is absent")

    migration = conn.execute(
        text("SELECT applied_at FROM schema_migrations WHERE version = '022'")
    ).scalar_one_or_none()
    if migration is None:
        raise PreflightError("schema_migrations does not contain 022")
    return {"migration_022_applied_at": str(migration)}


def read_snapshot(
    engine: Any,
    direct: Mapping[str, ManifestDecision],
    corrections: Mapping[str, ManifestDecision],
) -> tuple[list[Observation], set[int], dict[str, Any]]:
    from sqlalchemy import text

    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            conn.exec_driver_sql(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
            )
            schema_metadata = verify_022_schema(conn)
            identity = conn.execute(
                text(
                    "SELECT current_database() AS database_name, "
                    "txid_current_snapshot()::text AS snapshot_id"
                )
            ).mappings().one()
            rows = conn.execute(text(SNAPSHOT_SQL)).fetchall()
            observations = [observation_from_row(row) for row in rows]
            targets = all_potential_targets(observations, direct, corrections)
            if targets:
                existing_source_ids = {
                    int(value)
                    for value in conn.execute(
                        text("SELECT id FROM sources WHERE id = ANY(:ids)"),
                        {"ids": sorted(targets)},
                    ).scalars()
                }
            else:
                existing_source_ids = set()
            metadata = {**dict(identity), **schema_metadata}
        finally:
            transaction.rollback()
    return observations, existing_source_ids, metadata


def all_potential_targets(
    observations: Sequence[Observation],
    direct: Mapping[str, ManifestDecision],
    corrections: Mapping[str, ManifestDecision],
) -> set[int]:
    targets: set[int] = set()
    for observation in observations:
        if observation.source_endpoint_source_id is not None:
            targets.add(observation.source_endpoint_source_id)
        targets.update(observation.legacy_source_ids)
        if observation.canonical_source_id is not None:
            targets.add(observation.canonical_source_id)
    for decision in list(direct.values()) + list(corrections.values()):
        if decision.target_source_id is not None:
            targets.add(decision.target_source_id)
    return targets


def validate_audit_table(conn: Any) -> None:
    from sqlalchemy import text

    table_exists = conn.execute(
        text("SELECT to_regclass(current_schema() || '.endpoint_source_backfill_audit')")
    ).scalar_one_or_none()
    if table_exists is None:
        raise PreflightError(
            f"required reviewed audit table {AUDIT_TABLE} does not exist"
        )
    required_types = {
        "run_id": "uuid",
        "endpoint_id": "text",
        "before_source_id": "bigint",
        "after_source_id": "bigint",
        "observed_source_endpoint_source_id": "bigint",
        "observed_legacy_source_ids": "jsonb",
        "reason": "text",
        "manifest_hash": "text",
        "plan_hash": "text",
        "scope_hash": "text",
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
                "AND table_name = 'endpoint_source_backfill_audit'"
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
    for name in (
        "run_id",
        "endpoint_id",
        "after_source_id",
        "observed_legacy_source_ids",
        "reason",
        "manifest_hash",
        "plan_hash",
        "scope_hash",
        "review_ref",
        "executed_by",
        "executed_at",
    ):
        if columns[name]["is_nullable"] != "NO":
            raise PreflightError(f"{AUDIT_TABLE}.{name} must be NOT NULL")
    if not columns["executed_at"]["column_default"]:
        raise PreflightError(f"{AUDIT_TABLE}.executed_at must have a database default")

    unique_run_endpoint = conn.execute(
        text(
            "SELECT EXISTS ("
            " SELECT 1 FROM pg_constraint c"
            " WHERE c.conrelid = 'endpoint_source_backfill_audit'::regclass"
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


def assert_runtime_observation(expected: PlanRow, actual: Observation) -> None:
    expected_values = (
        expected.before_source_id,
        expected.observed_source_endpoint_source_id,
        expected.observed_legacy_source_ids,
        expected.pmh_url,
        expected.pmh_set,
        expected.metadata_prefix,
    )
    actual_values = (
        actual.canonical_source_id,
        actual.source_endpoint_source_id,
        actual.legacy_source_ids,
        actual.pmh_url,
        actual.pmh_set,
        actual.metadata_prefix,
    )
    if expected_values != actual_values:
        raise ExecutionDrift(
            f"{expected.endpoint_id}: exact guard drift; expected canonical/matcher/legacy="
            f"{expected_values!r}, observed={actual_values!r}"
        )


def choose_execution_rows(plan: Sequence[PlanRow], args: argparse.Namespace) -> list[PlanRow]:
    rows = [row for row in plan if row.status == "UPDATE"]
    requested_ids = set(args.endpoint_id or ())
    if args.full_run and requested_ids:
        raise PreflightError("--full-run cannot be combined with --endpoint-id")
    if not args.full_run and not requested_ids:
        raise PreflightError(
            "scope requires explicit canary --endpoint-id values or --full-run"
        )
    if requested_ids:
        available = {row.endpoint_id for row in rows}
        missing = sorted(requested_ids - available)
        if missing:
            raise PreflightError(f"requested endpoint IDs are not UPDATE candidates: {missing}")
        rows = [row for row in rows if row.endpoint_id in requested_ids]
    return rows


def execute_batches(
    engine: Any,
    rows: Sequence[PlanRow],
    args: argparse.Namespace,
    combined_manifest_sha256: str,
    output_dir: Path,
) -> dict[str, Any]:
    from sqlalchemy import text

    if args.batch_size <= 0 or args.batch_size > 500:
        raise PreflightError("--batch-size must be between 1 and 500")

    progress: dict[str, Any] = {
        "run_id": args.run_id,
        "selected": len(rows),
        "committed": 0,
        "committed_endpoint_ids": [],
        "status": "STARTED",
    }
    write_json(output_dir / "execution_progress.json", progress)

    for offset in range(0, len(rows), args.batch_size):
        batch = rows[offset : offset + args.batch_size]
        committed_ids: list[str] = []
        with engine.begin() as conn:
            conn.exec_driver_sql("SET LOCAL lock_timeout = '5s'")
            conn.exec_driver_sql("SET LOCAL statement_timeout = '60s'")
            validate_audit_table(conn)
            for planned in batch:
                raw = conn.execute(
                    text(ONE_OBSERVATION_SQL), {"endpoint_id": planned.endpoint_id}
                ).one_or_none()
                if raw is None:
                    raise ExecutionDrift(f"{planned.endpoint_id}: endpoint disappeared")
                actual = observation_from_row(raw)
                assert_runtime_observation(planned, actual)
                if planned.proposed_source_id is None:
                    raise PreflightError(
                        f"{planned.endpoint_id}: UPDATE candidate has no proposed Source"
                    )
                target_exists = conn.execute(
                    text("SELECT EXISTS (SELECT 1 FROM sources WHERE id = :source_id)"),
                    {"source_id": planned.proposed_source_id},
                ).scalar_one()
                if not target_exists:
                    raise ExecutionDrift(
                        f"{planned.endpoint_id}: target Source {planned.proposed_source_id} disappeared"
                    )

                row_review_ref = planned.manifest_approval_ref or args.approval_ref
                review_ref = (
                    f"plan={args.approval_ref};scope_sha256={args.approved_scope_hash};"
                    f"row={row_review_ref}"
                )
                audit_result = conn.execute(
                    text(
                        f"INSERT INTO {AUDIT_TABLE} ("
                        "run_id, endpoint_id, before_source_id, after_source_id, "
                        "observed_source_endpoint_source_id, observed_legacy_source_ids, "
                        "reason, manifest_hash, plan_hash, scope_hash, review_ref, executed_by"
                        ") VALUES ("
                        ":run_id, :endpoint_id, :before_source_id, :after_source_id, "
                        ":se_source_id, CAST(:legacy_source_ids AS jsonb), :reason, "
                        ":manifest_hash, :plan_hash, :scope_hash, :review_ref, :executed_by"
                        ")"
                    ),
                    {
                        "run_id": args.run_id,
                        "endpoint_id": planned.endpoint_id,
                        "before_source_id": planned.before_source_id,
                        "after_source_id": planned.proposed_source_id,
                        "se_source_id": planned.observed_source_endpoint_source_id,
                        "legacy_source_ids": json.dumps(
                            list(planned.observed_legacy_source_ids), separators=(",", ":")
                        ),
                        "reason": planned.reason,
                        "manifest_hash": combined_manifest_sha256,
                        "plan_hash": args.approved_plan_hash,
                        "scope_hash": args.approved_scope_hash,
                        "review_ref": review_ref,
                        "executed_by": args.executed_by,
                    },
                )
                if audit_result.rowcount != 1:
                    raise ExecutionDrift(
                        f"{planned.endpoint_id}: audit INSERT affected "
                        f"{audit_result.rowcount}, expected 1"
                    )
                result = conn.execute(
                    text(
                        "UPDATE endpoint SET source_id = :after_source_id "
                        "WHERE id = :endpoint_id "
                        "AND source_id IS NOT DISTINCT FROM :before_source_id "
                        "AND (SELECT se.source_id FROM source_endpoint se "
                        "     WHERE se.endpoint_id = :endpoint_id) "
                        "    IS NOT DISTINCT FROM :se_source_id "
                        "AND to_jsonb(ARRAY("
                        "     SELECT s.id FROM sources s "
                        "     WHERE s.endpoint_id = :endpoint_id ORDER BY s.id"
                        ")) = CAST(:legacy_source_ids AS jsonb) "
                        "AND pmh_url IS NOT DISTINCT FROM :pmh_url "
                        "AND pmh_set IS NOT DISTINCT FROM :pmh_set "
                        "AND metadata_prefix IS NOT DISTINCT FROM :metadata_prefix "
                        "AND EXISTS (SELECT 1 FROM sources target "
                        "            WHERE target.id = :after_source_id)"
                    ),
                    {
                        "endpoint_id": planned.endpoint_id,
                        "before_source_id": planned.before_source_id,
                        "after_source_id": planned.proposed_source_id,
                        "se_source_id": planned.observed_source_endpoint_source_id,
                        "legacy_source_ids": json.dumps(
                            list(planned.observed_legacy_source_ids), separators=(",", ":")
                        ),
                        "pmh_url": planned.pmh_url,
                        "pmh_set": planned.pmh_set,
                        "metadata_prefix": planned.metadata_prefix,
                    },
                )
                if result.rowcount != 1:
                    raise ExecutionDrift(
                        f"{planned.endpoint_id}: guarded UPDATE affected {result.rowcount}, expected 1"
                    )
                committed_ids.append(planned.endpoint_id)

        progress["committed_endpoint_ids"].extend(committed_ids)
        progress["committed"] = len(progress["committed_endpoint_ids"])
        progress["status"] = "IN_PROGRESS"
        write_json(output_dir / "execution_progress.json", progress)

    progress["status"] = "COMPLETE"
    write_json(output_dir / "execution_progress.json", progress)
    return progress


def valid_uuid(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a UUID") from exc
    return str(parsed)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--direct-conflicts", type=Path, required=True)
    parser.add_argument("--known-corrections", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true", help="write; default is database read-only")
    parser.add_argument("--approved-plan-hash")
    parser.add_argument("--approved-scope-hash")
    parser.add_argument("--expected-database")
    parser.add_argument("--run-id", type=valid_uuid)
    parser.add_argument("--executed-by")
    parser.add_argument("--approval-ref")
    parser.add_argument("--writer-pause-ref")
    parser.add_argument(
        "--endpoint-id",
        action="append",
        help="select this explicit canary endpoint; repeatable and usable in dry-run to hash scope",
    )
    parser.add_argument(
        "--full-run",
        action="store_true",
        help="select every UPDATE candidate; usable in dry-run to hash the full scope",
    )
    parser.add_argument("--batch-size", type=int, default=100)
    return parser


def validate_execute_args(args: argparse.Namespace) -> None:
    if not args.execute:
        execute_only = {
            "approved_plan_hash": args.approved_plan_hash,
            "approved_scope_hash": args.approved_scope_hash,
            "expected_database": args.expected_database,
            "run_id": args.run_id,
            "executed_by": args.executed_by,
            "approval_ref": args.approval_ref,
            "writer_pause_ref": args.writer_pause_ref,
        }
        supplied = sorted(key for key, value in execute_only.items() if value not in (None, False, []))
        if supplied:
            raise PreflightError(
                f"execute-only arguments supplied without --execute: {supplied}"
            )
        return

    required = {
        "--approved-plan-hash": args.approved_plan_hash,
        "--approved-scope-hash": args.approved_scope_hash,
        "--expected-database": args.expected_database,
        "--run-id": args.run_id,
        "--executed-by": args.executed_by,
        "--approval-ref": args.approval_ref,
        "--writer-pause-ref": args.writer_pause_ref,
    }
    missing = [flag for flag, value in required.items() if not value]
    if missing:
        raise PreflightError(f"execute mode requires {', '.join(missing)}")
    require_review_value(args.executed_by, "executed_by", "execution")
    require_review_value(args.approval_ref, "approval_ref", "execution")
    require_review_value(args.writer_pause_ref, "writer_pause_ref", "execution")
    if len(args.approved_plan_hash) != 64 or any(
        char not in "0123456789abcdef" for char in args.approved_plan_hash.lower()
    ):
        raise PreflightError("--approved-plan-hash must be a SHA-256 hex digest")
    if len(args.approved_scope_hash) != 64 or any(
        char not in "0123456789abcdef" for char in args.approved_scope_hash.lower()
    ):
        raise PreflightError("--approved-scope-hash must be a SHA-256 hex digest")


def main(argv: Optional[Sequence[str]] = None, engine: Any = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        validate_execute_args(args)
        direct, direct_sha256 = load_manifest(args.direct_conflicts, "direct_conflict")
        corrections, corrections_sha256 = load_manifest(
            args.known_corrections, "known_correction"
        )
        database_engine = engine or lazy_engine()
        observations, existing_source_ids, metadata = read_snapshot(
            database_engine, direct, corrections
        )

        # Reassert parity before planning; build_plan repeats this deliberately so
        # direct use of that pure function remains fail-closed in tests/tools.
        validate_manifest_parity(observations, direct, corrections)
        plan = build_plan(observations, direct, corrections, existing_source_ids)
        plan_hash, counts = write_review_package(
            args.output_dir,
            observations,
            plan,
            direct,
            corrections,
            metadata,
            direct_sha256,
            corrections_sha256,
        )

        print(f"DRY-RUN PACKAGE: {args.output_dir}")
        print(f"PLAN SHA-256: {plan_hash}")
        print(canonical_json(counts))
        selected: Optional[list[PlanRow]] = None
        scope_hash: Optional[str] = None
        if args.endpoint_id or args.full_run:
            selected = choose_execution_rows(plan, args)
            scope_payload = execution_scope_payload(plan_hash, selected, args.full_run)
            scope_hash = calculate_execution_scope_hash(scope_payload)
            write_json(
                args.output_dir / "execution_scope.json",
                {"scope_sha256": scope_hash, "scope": scope_payload},
            )
            print(f"EXECUTION SCOPE SHA-256: {scope_hash}")
        if not args.execute:
            return 0

        if plan_hash.lower() != args.approved_plan_hash.lower():
            raise PreflightError(
                "fresh plan hash does not match --approved-plan-hash; review the new package"
            )
        if metadata.get("database_name") != args.expected_database:
            raise PreflightError(
                f"database mismatch: expected {args.expected_database!r}, "
                f"connected to {metadata.get('database_name')!r}"
            )
        blocked = [row.endpoint_id for row in plan if row.status == "BLOCKED"]
        if blocked:
            raise PreflightError(f"plan contains BLOCKED rows: {blocked[:20]}")
        if selected is None or scope_hash is None:
            raise PreflightError("execute mode requires an explicit hashed scope")
        if scope_hash.lower() != args.approved_scope_hash.lower():
            raise PreflightError(
                "fresh execution scope hash does not match --approved-scope-hash"
            )
        execute_metadata = {
            **dict(metadata),
            "mode": "EXECUTE",
            "plan_sha256": plan_hash,
            "scope_sha256": scope_hash,
            "run_id": args.run_id,
            "executed_by": args.executed_by,
            "approval_ref": args.approval_ref,
            "writer_pause_ref": args.writer_pause_ref,
            "selected_endpoint_ids": [row.endpoint_id for row in selected],
        }
        write_json(args.output_dir / "execution_metadata.json", execute_metadata)
        progress = execute_batches(
            database_engine,
            selected,
            args,
            manifest_combined_hash(direct_sha256, corrections_sha256),
            args.output_dir,
        )
        print(f"EXECUTE COMPLETE: {progress['committed']}/{progress['selected']}")
        return 0
    except (OSError, PreflightError, ExecutionDrift) as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
