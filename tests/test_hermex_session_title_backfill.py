"""Regression coverage for Hermex sidebar session-title repair.

Hermex renders the Hermes WebUI sidebar. Desktop/Craft sessions can arrive from
state.db with framework placeholder titles (or NULL), while delegated subagents
are background workers rather than user-facing sidebar conversations.
"""
from __future__ import annotations

import sqlite3


def _make_state_db(path):
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT,
            title TEXT,
            model TEXT,
            message_count INTEGER,
            started_at REAL,
            ended_at REAL,
            end_reason TEXT,
            parent_session_id TEXT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            role TEXT,
            content TEXT,
            timestamp REAL
        );
        """
    )
    conn.commit()
    return conn


def test_default_agent_projection_hides_subagents_but_diagnostic_query_keeps_them(tmp_path):
    from api.agent_sessions import read_importable_agent_session_rows

    db_path = tmp_path / "state.db"
    conn = _make_state_db(db_path)
    try:
        conn.executemany(
            "INSERT INTO sessions (id, source, title, model, message_count, started_at) VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("desktop-1", "desktop", None, "gpt-test", 2, 10.0),
                ("worker-1", "subagent", None, "gpt-test", 2, 11.0),
            ],
        )
        conn.executemany(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
            [
                ("desktop-1", "user", "Repair desktop title", 10.0),
                ("desktop-1", "assistant", "Working on it", 10.1),
                ("worker-1", "user", "Worker task", 11.0),
                ("worker-1", "assistant", "Worker result", 11.1),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    default_rows = read_importable_agent_session_rows(db_path, limit=None)
    diagnostic_rows = read_importable_agent_session_rows(
        db_path,
        limit=None,
        exclude_sources=None,
    )

    assert [row["id"] for row in default_rows] == ["desktop-1"]
    assert {row["id"] for row in diagnostic_rows} == {"desktop-1", "worker-1"}


def test_desktop_title_backfill_only_targets_framework_placeholders():
    import api.models as models

    assert models._is_repairable_agent_session_title("desktop", None) is True
    assert models._is_repairable_agent_session_title("desktop", "Desktop Session") is True
    assert models._is_repairable_agent_session_title("craft-agent", "Session") is True
    assert models._is_repairable_agent_session_title("desktop", "Harness feature completion review") is False
    assert models._is_repairable_agent_session_title("subagent", None) is False
    assert models._is_repairable_agent_session_title("telegram", "Session") is False


def test_desktop_title_backfill_persists_model_title_only_while_placeholder(monkeypatch):
    import api.models as models
    import api.state_sync as state_sync
    import api.streaming as streaming

    class FakeDB:
        def __init__(self, title=None):
            self.title = title
            self.writes = []
            self.closed = False

        def get_session_title(self, _session_id):
            return self.title

        def set_session_title(self, session_id, title):
            self.writes.append((session_id, title))
            self.title = title
            return True

        def close(self):
            self.closed = True

    db = FakeDB()
    cleared = []
    monkeypatch.setattr(
        models,
        "get_state_db_session_messages",
        lambda _sid, profile=None: [
            {"role": "user", "content": "Fix the Hermex session list"},
            {"role": "assistant", "content": "I found the title repair path."},
        ],
    )
    monkeypatch.setattr(
        streaming,
        "generate_session_title_for_session",
        lambda _session: ("Hermex session title repair", "llm_aux", ""),
    )
    monkeypatch.setattr(state_sync, "_get_state_db", lambda profile=None: db)
    monkeypatch.setattr(models, "clear_cli_sessions_cache", lambda: cleared.append(True))

    models._backfill_agent_session_title(
        session_id="desktop-1",
        profile="coding",
        source="desktop",
        known_title="Desktop Session",
    )

    assert db.writes == [("desktop-1", "Hermex session title repair")]
    assert db.closed is True
    assert cleared == [True]


def test_desktop_title_backfill_never_overwrites_a_real_title(monkeypatch):
    import api.models as models
    import api.state_sync as state_sync
    import api.streaming as streaming

    class FakeDB:
        def __init__(self):
            self.writes = []
            self.closed = False

        def get_session_title(self, _session_id):
            return "Existing manual title"

        def set_session_title(self, session_id, title):
            self.writes.append((session_id, title))
            return True

        def close(self):
            self.closed = True

    db = FakeDB()
    monkeypatch.setattr(
        models,
        "get_state_db_session_messages",
        lambda _sid, profile=None: [
            {"role": "user", "content": "Fix the Hermex session list"},
            {"role": "assistant", "content": "I found the title repair path."},
        ],
    )
    monkeypatch.setattr(
        streaming,
        "generate_session_title_for_session",
        lambda _session: ("Should not overwrite", "llm_aux", ""),
    )
    monkeypatch.setattr(state_sync, "_get_state_db", lambda profile=None: db)

    models._backfill_agent_session_title(
        session_id="desktop-1",
        profile="coding",
        source="desktop",
        known_title="Desktop Session",
    )

    assert db.writes == []
    assert db.closed is True


def test_title_backfill_queue_has_a_global_concurrency_cap(monkeypatch):
    import api.models as models

    monkeypatch.setattr(models, "_AGENT_TITLE_BACKFILL_MAX_CONCURRENT", 2)
    with models._AGENT_TITLE_BACKFILL_LOCK:
        models._AGENT_TITLE_BACKFILL_INFLIGHT.clear()
        models._AGENT_TITLE_BACKFILL_INFLIGHT.update({("coding", "one"), ("coding", "two")})
    try:
        models._queue_agent_session_title_backfill(
            session_id="three",
            profile="coding",
            source="desktop",
            title="Desktop Session",
        )
        with models._AGENT_TITLE_BACKFILL_LOCK:
            assert ("coding", "three") not in models._AGENT_TITLE_BACKFILL_INFLIGHT
    finally:
        with models._AGENT_TITLE_BACKFILL_LOCK:
            models._AGENT_TITLE_BACKFILL_INFLIGHT.clear()
