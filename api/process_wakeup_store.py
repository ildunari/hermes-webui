"""WebUI-local durable state for server-initiated process wakeups.

A process wakeup is accepted only after its local worker thread has actually
begun.  Until then the complete launch payload remains ``pending`` in this
SQLite store and WebUI startup can resume it.  ``started`` and ``completed``
records make Agent-row acknowledgement replayable without treating a pre-start
receipt as acceptance.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SCHEMA_VERSION = 2
_COMPLETED_RETENTION_SECONDS = 30 * 24 * 60 * 60
_COMPLETED_RETENTION_COUNT = 2048


@dataclass(frozen=True)
class ProcessWakeupRecord:
    wakeup_id: str
    session_id: str
    state: str
    payload: dict[str, Any]
    stream_id: str = ""
    created_at: float = 0.0
    started_at: float | None = None
    completed_at: float | None = None
    agent_acknowledged: bool = False


def _database_path() -> Path:
    # Resolve at call time so isolated test/runtime state-dir overrides are
    # honored rather than captured when this module is imported.
    from api import config

    state_dir = Path(config.STATE_DIR)
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir / "pending_process_wakeups.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_database_path(), timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS process_wakeups (
               wakeup_id TEXT PRIMARY KEY,
               session_id TEXT NOT NULL,
               state TEXT NOT NULL CHECK (state IN ('pending', 'started', 'completed')),
               payload_json TEXT NOT NULL,
               request_fingerprint TEXT NOT NULL,
               stream_id TEXT NOT NULL DEFAULT '',
               created_at REAL NOT NULL,
               updated_at REAL NOT NULL,
               started_at REAL,
               completed_at REAL,
               agent_acknowledged INTEGER NOT NULL DEFAULT 0
           )"""
    )
    columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(process_wakeups)")
    }
    if "agent_acknowledged" not in columns:
        conn.execute(
            "ALTER TABLE process_wakeups ADD COLUMN "
            "agent_acknowledged INTEGER NOT NULL DEFAULT 0"
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS process_wakeups_state_created_idx "
        "ON process_wakeups(state, created_at)"
    )
    conn.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")
    return conn


def _canonical_payload(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _request_fingerprint(payload: dict[str, Any]) -> str:
    # Restored Agent events may add transport-only fields (for example
    # ``restored``).  Idempotency is scoped to the behavior-changing launch
    # request while the first full payload remains the recovery authority.
    identity = {
        "wakeup_id": str(payload.get("wakeup_id") or ""),
        "session_id": str(payload.get("session_id") or ""),
        "message": str(payload.get("message") or ""),
        "source": str(payload.get("source") or "process_wakeup"),
        "attachments": list(payload.get("attachments") or []),
        "workspace": str(payload.get("workspace") or ""),
        "model": str(payload.get("model") or ""),
        "model_provider": str(payload.get("model_provider") or ""),
        "backend_is_gateway": bool(payload.get("backend_is_gateway")),
        "durable_event": _normalized_durable_event(payload.get("durable_event")),
    }
    return hashlib.sha256(_canonical_payload(identity).encode("utf-8")).hexdigest()


def _row_to_record(row: sqlite3.Row | None) -> ProcessWakeupRecord | None:
    if row is None:
        return None
    try:
        payload = json.loads(row["payload_json"])
    except (TypeError, ValueError, json.JSONDecodeError):
        payload = {}
    return ProcessWakeupRecord(
        wakeup_id=str(row["wakeup_id"]),
        session_id=str(row["session_id"]),
        state=str(row["state"]),
        payload=payload if isinstance(payload, dict) else {},
        stream_id=str(row["stream_id"] or ""),
        created_at=float(row["created_at"] or 0.0),
        started_at=(float(row["started_at"]) if row["started_at"] is not None else None),
        completed_at=(
            float(row["completed_at"]) if row["completed_at"] is not None else None
        ),
        agent_acknowledged=bool(row["agent_acknowledged"]),
    )


def get_process_wakeup(wakeup_id: str) -> ProcessWakeupRecord | None:
    wakeup_id = str(wakeup_id or "").strip()
    if not wakeup_id:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM process_wakeups WHERE wakeup_id=?", (wakeup_id,)
        ).fetchone()
    return _row_to_record(row)


def _normalized_durable_event(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    normalized = dict(value)
    # Restoration/claim annotations describe transport, not the delegated work.
    for key in ("restored", "delivery_claim", "delivery_claimed_at"):
        normalized.pop(key, None)
    return normalized


def validate_process_wakeup_replay(
    wakeup_id: str,
    *,
    session_id: str,
    message: str,
    source: str,
    durable_event: dict[str, Any] | None,
) -> ProcessWakeupRecord | None:
    """Validate request identity before any pending/accepted replay shortcut."""
    record = get_process_wakeup(wakeup_id)
    if record is None:
        return None
    payload = record.payload
    same_request = (
        record.session_id == str(session_id or "").strip()
        and str(payload.get("message") or "").strip() == str(message or "").strip()
        and str(payload.get("source") or "process_wakeup").strip()
        == (str(source or "process_wakeup").strip() or "process_wakeup")
        and _normalized_durable_event(payload.get("durable_event"))
        == _normalized_durable_event(durable_event)
    )
    if not same_request:
        raise ValueError("process wakeup id conflicts with an existing request")
    return record


def reserve_pending_process_wakeup(payload: dict[str, Any]) -> ProcessWakeupRecord:
    """Persist a complete launch payload before any worker can be created.

    Replays return the existing state. Reusing an id for a behaviorally different
    request fails closed instead of launching or acknowledging the wrong work.
    """
    if not isinstance(payload, dict):
        raise ValueError("process wakeup payload must be an object")
    wakeup_id = str(payload.get("wakeup_id") or "").strip()
    session_id = str(payload.get("session_id") or "").strip()
    message = str(payload.get("message") or "").strip()
    if not wakeup_id or not session_id or not message:
        raise ValueError("process wakeup id, session id, and message are required")
    persisted_payload = dict(payload)
    persisted_payload.update(
        {"wakeup_id": wakeup_id, "session_id": session_id, "message": message}
    )
    payload_json = _canonical_payload(persisted_payload)
    fingerprint = _request_fingerprint(persisted_payload)
    now = time.time()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM process_wakeups WHERE wakeup_id=?", (wakeup_id,)
        ).fetchone()
        if row is None:
            conn.execute(
                """INSERT INTO process_wakeups
                   (wakeup_id, session_id, state, payload_json, request_fingerprint,
                    stream_id, created_at, updated_at)
                   VALUES (?, ?, 'pending', ?, ?, '', ?, ?)""",
                (wakeup_id, session_id, payload_json, fingerprint, now, now),
            )
            row = conn.execute(
                "SELECT * FROM process_wakeups WHERE wakeup_id=?", (wakeup_id,)
            ).fetchone()
        elif (
            str(row["session_id"]) != session_id
            or str(row["request_fingerprint"]) != fingerprint
        ):
            raise ValueError("process wakeup id conflicts with an existing request")
        conn.commit()
    record = _row_to_record(row)
    if record is None:  # pragma: no cover - defensive SQLite invariant
        raise RuntimeError("failed to persist process wakeup")
    return record


def set_pending_process_wakeup_stream(wakeup_id: str, stream_id: str) -> bool:
    wakeup_id = str(wakeup_id or "").strip()
    stream_id = str(stream_id or "").strip()
    if not wakeup_id or not stream_id:
        return False
    now = time.time()
    with _connect() as conn:
        changed = conn.execute(
            """UPDATE process_wakeups SET stream_id=?, updated_at=?
               WHERE wakeup_id=? AND state='pending'""",
            (stream_id, now, wakeup_id),
        ).rowcount
        conn.commit()
    return changed == 1


def clear_pending_process_wakeup_stream(wakeup_id: str, stream_id: str) -> bool:
    with _connect() as conn:
        changed = conn.execute(
            """UPDATE process_wakeups SET stream_id='', updated_at=?
               WHERE wakeup_id=? AND state='pending' AND stream_id=?""",
            (time.time(), str(wakeup_id or ""), str(stream_id or "")),
        ).rowcount
        conn.commit()
    return changed == 1


def mark_process_wakeup_started(wakeup_id: str, stream_id: str) -> bool:
    """Transition pending→started from inside the running worker thread."""
    now = time.time()
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        changed = conn.execute(
            """UPDATE process_wakeups
               SET state='started', stream_id=?, started_at=?, updated_at=?
               WHERE wakeup_id=? AND state='pending'""",
            (str(stream_id or ""), now, now, str(wakeup_id or "")),
        ).rowcount
        conn.commit()
    return changed == 1


def mark_process_wakeup_completed(wakeup_id: str, stream_id: str) -> bool:
    """Transition started→completed after the accepted worker returns."""
    now = time.time()
    with _connect() as conn:
        changed = conn.execute(
            """UPDATE process_wakeups
               SET state='completed', completed_at=?, updated_at=?
               WHERE wakeup_id=? AND state='started' AND stream_id=?""",
            (now, now, str(wakeup_id or ""), str(stream_id or "")),
        ).rowcount
        conn.commit()
    if changed:
        prune_completed_process_wakeups()
    return changed == 1


def mark_process_wakeup_agent_acknowledged(wakeup_id: str) -> bool:
    """Record that the owning Agent row is durably delivered."""
    with _connect() as conn:
        changed = conn.execute(
            """UPDATE process_wakeups
               SET agent_acknowledged=1, updated_at=?
               WHERE wakeup_id=? AND state IN ('started', 'completed')""",
            (time.time(), str(wakeup_id or "")),
        ).rowcount
        conn.commit()
    if changed:
        prune_completed_process_wakeups()
    return changed == 1


def list_pending_process_wakeups() -> list[ProcessWakeupRecord]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM process_wakeups WHERE state='pending' ORDER BY created_at, wakeup_id"
        ).fetchall()
    return [record for row in rows if (record := _row_to_record(row)) is not None]


def process_wakeup_is_accepted(wakeup_id: str) -> bool:
    record = get_process_wakeup(wakeup_id)
    return bool(record and record.state in {"started", "completed"})


def prune_completed_process_wakeups() -> int:
    """Bound terminal history while retaining recent replay receipts."""
    cutoff = time.time() - _COMPLETED_RETENTION_SECONDS
    with _connect() as conn:
        old = conn.execute(
            """DELETE FROM process_wakeups
               WHERE state='completed' AND agent_acknowledged=1 AND completed_at < ?""",
            (cutoff,),
        ).rowcount
        overflow = conn.execute(
            """DELETE FROM process_wakeups
               WHERE wakeup_id IN (
                   SELECT wakeup_id FROM process_wakeups
                   WHERE state='completed' AND agent_acknowledged=1
                   ORDER BY completed_at DESC
                   LIMIT -1 OFFSET ?
               )""",
            (_COMPLETED_RETENTION_COUNT,),
        ).rowcount
        conn.commit()
    return int(old or 0) + int(overflow or 0)
