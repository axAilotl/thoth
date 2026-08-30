"""SQLite persistence for artifact classification policies and decisions.

Stores durable state inside the existing ``MetadataDB``; no new network or
external dependencies.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Mapping
from typing import Any

from .artifact_classification import (
    AlternativeProjection,
    RoutingDecision,
    RoutingPolicy,
    policy_from_mapping,
    policy_to_mapping,
)
from .metadata_db import MetadataDB
from .time_utils import utc_now_iso as _now_iso


class ClassificationStoreError(RuntimeError):
    """Raised when classification persistence fails."""


def _new_id() -> str:
    return str(uuid.uuid4())


def _json_text(value: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(value or {}), ensure_ascii=False, sort_keys=True)


def _json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _clean_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class ClassificationStore:
    """Repository for routing policy revisions and person decisions."""

    def __init__(self, db: MetadataDB | None = None):
        self.db = db or _metadata_db()
        self.ensure_tables()

    def ensure_tables(self) -> None:
        """Create classification tables in the existing SQLite database."""
        try:
            with self.db._get_connection() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS routing_policy_revisions (
                        revision_id TEXT PRIMARY KEY,
                        version INTEGER NOT NULL UNIQUE,
                        status TEXT NOT NULL
                            CHECK (status IN ('proposed', 'active', 'rolled_back', 'superseded')),
                        actor TEXT,
                        reason TEXT,
                        policy_json TEXT NOT NULL,
                        metrics_json TEXT NOT NULL DEFAULT '{}',
                        previous_revision_id TEXT,
                        created_at TEXT NOT NULL,
                        activated_at TEXT,
                        provenance_json TEXT NOT NULL DEFAULT '{}'
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS routing_decisions (
                        decision_id TEXT PRIMARY KEY,
                        artifact_id TEXT NOT NULL,
                        artifact_type TEXT NOT NULL,
                        source TEXT NOT NULL,
                        revision_id TEXT,
                        proposed_projection_id TEXT,
                        actual_projection_id TEXT,
                        action TEXT NOT NULL
                            CHECK (action IN ('approve', 'reject', 'correct', 'figure_out', 'auto_route')),
                        actor TEXT,
                        reason TEXT,
                        features_json TEXT NOT NULL DEFAULT '{}',
                        alternatives_json TEXT NOT NULL DEFAULT '[]',
                        confidence REAL NOT NULL DEFAULT 0.0,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_routing_decisions_artifact
                    ON routing_decisions(artifact_id)
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_routing_decisions_revision
                    ON routing_decisions(revision_id)
                    """
                )
                conn.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_routing_decisions_created
                    ON routing_decisions(created_at)
                    """
                )
        except Exception as exc:
            raise ClassificationStoreError(
                "failed to ensure classification tables"
            ) from exc

    def seed_initial_policy(
        self,
        policy: RoutingPolicy,
        *,
        actor: str | None = None,
        reason: str | None = None,
    ) -> RoutingPolicy:
        """Insert the first active revision if the table is empty."""
        try:
            with self.db._get_connection() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) AS count FROM routing_policy_revisions"
                ).fetchone()
                if row and row["count"]:
                    return policy
                self._insert_revision(
                    conn,
                    policy,
                    status="active",
                    metrics={},
                    provenance_event={
                        "action": "seed",
                        "actor": actor,
                        "reason": reason,
                        "at": _now_iso(),
                    },
                )
                return policy
        except Exception as exc:
            raise ClassificationStoreError("failed to seed initial policy") from exc

    def get_revision(self, revision_id: str) -> RoutingPolicy | None:
        """Load one policy revision by id."""
        try:
            with self.db._get_connection() as conn:
                row = conn.execute(
                    "SELECT policy_json FROM routing_policy_revisions WHERE revision_id = ?",
                    (str(revision_id),),
                ).fetchone()
                if row is None:
                    return None
                mapping = _json_object(row["policy_json"])
                return policy_from_mapping(
                    mapping,
                    revision_id=mapping.get("revision_id", revision_id),
                    version=mapping.get("version", 1),
                    previous_revision_id=mapping.get("previous_revision_id"),
                    actor=mapping.get("actor"),
                    reason=mapping.get("reason"),
                )
        except Exception as exc:
            raise ClassificationStoreError(
                f"failed to load revision {revision_id}"
            ) from exc

    def get_active_revision(self) -> RoutingPolicy | None:
        """Load the currently active policy revision."""
        try:
            with self.db._get_connection() as conn:
                row = conn.execute(
                    """
                    SELECT revision_id, policy_json
                    FROM routing_policy_revisions
                    WHERE status = 'active'
                    ORDER BY version DESC
                    LIMIT 1
                    """
                ).fetchone()
                if row is None:
                    return None
                mapping = _json_object(row["policy_json"])
                return policy_from_mapping(
                    mapping,
                    revision_id=mapping.get("revision_id", row["revision_id"]),
                    version=mapping.get("version", 1),
                    previous_revision_id=mapping.get("previous_revision_id"),
                    actor=mapping.get("actor"),
                    reason=mapping.get("reason"),
                )
        except Exception as exc:
            raise ClassificationStoreError(
                "failed to load active routing policy"
            ) from exc

    def save_proposed_revision(
        self,
        policy: RoutingPolicy,
        metrics: Mapping[str, Any],
        *,
        actor: str | None,
        reason: str | None,
    ) -> RoutingPolicy:
        """Persist a proposed policy revision."""
        try:
            with self.db._get_connection() as conn:
                self._insert_revision(
                    conn,
                    policy,
                    status="proposed",
                    metrics=metrics,
                    provenance_event={
                        "action": "propose",
                        "actor": actor,
                        "reason": reason,
                        "at": _now_iso(),
                    },
                )
                return policy
        except Exception as exc:
            raise ClassificationStoreError(
                f"failed to save proposed revision {policy.revision_id}"
            ) from exc

    def activate_revision(
        self,
        revision_id: str,
        *,
        actor: str,
        reason: str,
        metrics: Mapping[str, Any] | None = None,
    ) -> RoutingPolicy:
        """Activate a proposed revision and supersede the current active one."""
        clean_actor = _clean_optional(actor)
        clean_reason = _clean_optional(reason)
        if not clean_actor or not clean_reason:
            raise ClassificationStoreError(
                "activation requires actor and reason"
            )
        try:
            with self.db._get_connection() as conn:
                proposed = conn.execute(
                    "SELECT revision_id, policy_json FROM routing_policy_revisions "
                    "WHERE revision_id = ? AND status = 'proposed'",
                    (revision_id,),
                ).fetchone()
                if proposed is None:
                    raise ClassificationStoreError(
                        f"proposed revision not found: {revision_id}"
                    )
                now = _now_iso()
                conn.execute(
                    """
                    UPDATE routing_policy_revisions
                    SET status = 'superseded',
                        provenance_json = json_insert(
                            provenance_json,
                            '$.events',
                            COALESCE(json_extract(provenance_json, '$.events'), json_array())
                        )
                    WHERE status = 'active'
                    """
                )
                # SQLite's json_insert into an array via expression above is
                # awkward; use a simpler provenance append by loading and
                # dumping the JSON.
                self._append_provenance(
                    conn,
                    revision_id,
                    {
                        "action": "activate",
                        "actor": clean_actor,
                        "reason": clean_reason,
                        "at": now,
                        "metrics": dict(metrics or {}),
                    },
                )
                conn.execute(
                    """
                    UPDATE routing_policy_revisions
                    SET status = 'active',
                        activated_at = ?,
                        metrics_json = ?
                    WHERE revision_id = ?
                    """,
                    (now, _json_text(metrics), revision_id),
                )
                mapping = _json_object(proposed["policy_json"])
                return policy_from_mapping(
                    mapping,
                    revision_id=mapping.get("revision_id", revision_id),
                    version=mapping.get("version", 1),
                    previous_revision_id=mapping.get("previous_revision_id"),
                    actor=mapping.get("actor"),
                    reason=mapping.get("reason"),
                )
        except ClassificationStoreError:
            raise
        except Exception as exc:
            raise ClassificationStoreError(
                f"failed to activate revision {revision_id}"
            ) from exc

    def rollback_active_revision(
        self,
        *,
        actor: str,
        reason: str,
    ) -> RoutingPolicy:
        """Roll back the active revision to its predecessor."""
        clean_actor = _clean_optional(actor)
        clean_reason = _clean_optional(reason)
        if not clean_actor or not clean_reason:
            raise ClassificationStoreError(
                "rollback requires actor and reason"
            )
        try:
            with self.db._get_connection() as conn:
                active_row = conn.execute(
                    """
                    SELECT revision_id, previous_revision_id
                    FROM routing_policy_revisions
                    WHERE status = 'active'
                    ORDER BY version DESC
                    LIMIT 1
                    """
                ).fetchone()
                if active_row is None:
                    raise ClassificationStoreError("no active revision to roll back")
                previous_id = active_row["previous_revision_id"]
                if not previous_id:
                    raise ClassificationStoreError(
                        "active revision has no predecessor"
                    )
                previous_row = conn.execute(
                    """
                    SELECT revision_id, policy_json
                    FROM routing_policy_revisions
                    WHERE revision_id = ? AND status IN ('superseded', 'rolled_back')
                    ORDER BY version DESC
                    LIMIT 1
                    """,
                    (previous_id,),
                ).fetchone()
                if previous_row is None:
                    raise ClassificationStoreError(
                        f"previous revision not found: {previous_id}"
                    )
                now = _now_iso()
                self._append_provenance(
                    conn,
                    active_row["revision_id"],
                    {
                        "action": "rollback",
                        "actor": clean_actor,
                        "reason": clean_reason,
                        "at": now,
                    },
                )
                conn.execute(
                    """
                    UPDATE routing_policy_revisions
                    SET status = 'rolled_back'
                    WHERE revision_id = ?
                    """,
                    (active_row["revision_id"],),
                )
                self._append_provenance(
                    conn,
                    previous_id,
                    {
                        "action": "reactivate",
                        "actor": clean_actor,
                        "reason": clean_reason,
                        "at": now,
                    },
                )
                conn.execute(
                    """
                    UPDATE routing_policy_revisions
                    SET status = 'active', activated_at = ?
                    WHERE revision_id = ?
                    """,
                    (now, previous_id),
                )
                mapping = _json_object(previous_row["policy_json"])
                return policy_from_mapping(
                    mapping,
                    revision_id=mapping.get("revision_id", previous_id),
                    version=mapping.get("version", 1),
                    previous_revision_id=mapping.get("previous_revision_id"),
                    actor=mapping.get("actor"),
                    reason=mapping.get("reason"),
                )
        except ClassificationStoreError:
            raise
        except Exception as exc:
            raise ClassificationStoreError("failed to roll back active revision") from exc

    def record_decision(self, decision: RoutingDecision) -> RoutingDecision:
        """Persist a routing review decision."""
        try:
            with self.db._get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO routing_decisions (
                        decision_id, artifact_id, artifact_type, source,
                        revision_id, proposed_projection_id, actual_projection_id,
                        action, actor, reason, features_json, alternatives_json,
                        confidence, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        decision.decision_id,
                        decision.artifact_id,
                        decision.artifact_type,
                        decision.source,
                        decision.revision_id,
                        decision.proposed_projection_id,
                        decision.actual_projection_id,
                        decision.action,
                        decision.actor,
                        decision.reason,
                        _json_text(decision.features),
                        _json_text(
                            {
                                "alternatives": [
                                    {
                                        "projection_id": alt.projection_id,
                                        "confidence": alt.confidence,
                                        "rule_id": alt.rule_id,
                                    }
                                    for alt in decision.alternatives
                                ]
                            }
                        ),
                        decision.confidence,
                        decision.created_at,
                    ),
                )
                return decision
        except Exception as exc:
            raise ClassificationStoreError(
                f"failed to record decision {decision.decision_id}"
            ) from exc

    def list_decisions(
        self,
        *,
        artifact_id: str | None = None,
        action: str | None = None,
        limit: int = 1000,
    ) -> list[RoutingDecision]:
        """Return durable routing decisions, newest first."""
        try:
            where: list[str] = []
            params: list[Any] = []
            if artifact_id:
                where.append("artifact_id = ?")
                params.append(artifact_id)
            if action:
                where.append("action = ?")
                params.append(action)
            query = "SELECT * FROM routing_decisions"
            if where:
                query += " WHERE " + " AND ".join(where)
            query += " ORDER BY created_at DESC, decision_id DESC LIMIT ?"
            params.append(max(1, int(limit)))
            with self.db._get_connection() as conn:
                rows = conn.execute(query, tuple(params)).fetchall()
                return [self._decision_from_row(row) for row in rows]
        except Exception as exc:
            raise ClassificationStoreError("failed to list routing decisions") from exc

    def _insert_revision(
        self,
        conn: sqlite3.Connection,
        policy: RoutingPolicy,
        *,
        status: str,
        metrics: Mapping[str, Any],
        provenance_event: Mapping[str, Any],
    ) -> None:
        now = _now_iso()
        provenance = {"events": [dict(provenance_event)]}
        conn.execute(
            """
            INSERT INTO routing_policy_revisions (
                revision_id, version, status, actor, reason, policy_json,
                metrics_json, previous_revision_id, created_at, activated_at,
                provenance_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                policy.revision_id,
                policy.version,
                status,
                policy.actor,
                policy.reason,
                _json_text(policy_to_mapping(policy)),
                _json_text(metrics),
                policy.previous_revision_id,
                policy.created_at or now,
                now if status == "active" else None,
                _json_text(provenance),
            ),
        )

    def _append_provenance(
        self,
        conn: sqlite3.Connection,
        revision_id: str,
        event: Mapping[str, Any],
    ) -> None:
        row = conn.execute(
            "SELECT provenance_json FROM routing_policy_revisions WHERE revision_id = ?",
            (revision_id,),
        ).fetchone()
        if row is None:
            return
        provenance = _json_object(row["provenance_json"])
        events = provenance.get("events")
        if not isinstance(events, list):
            events = []
        events.append(dict(event))
        provenance["events"] = events
        conn.execute(
            "UPDATE routing_policy_revisions SET provenance_json = ? WHERE revision_id = ?",
            (_json_text(provenance), revision_id),
        )

    def _decision_from_row(self, row: sqlite3.Row) -> RoutingDecision:
        features = _json_object(row["features_json"])
        alternatives_payload = _json_object(row["alternatives_json"])
        alternatives = alternatives_payload.get("alternatives")
        if not isinstance(alternatives, list):
            alternatives = []
        return RoutingDecision(
            decision_id=row["decision_id"],
            artifact_id=row["artifact_id"],
            artifact_type=row["artifact_type"],
            source=row["source"],
            revision_id=row["revision_id"],
            proposed_projection_id=row["proposed_projection_id"],
            actual_projection_id=row["actual_projection_id"],
            action=row["action"],
            actor=row["actor"],
            reason=row["reason"],
            features=features,
            alternatives=tuple(
                AlternativeProjection(
                    projection_id=alt["projection_id"],
                    confidence=alt.get("confidence", 0.0),
                    rule_id=alt.get("rule_id"),
                )
                for alt in alternatives
            ),
            confidence=row["confidence"],
            created_at=row["created_at"],
        )


def _metadata_db() -> MetadataDB:
    from .metadata_db import get_metadata_db

    return get_metadata_db()
