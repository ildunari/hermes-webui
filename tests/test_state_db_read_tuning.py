import sqlite3

from api import agent_sessions


def test_state_db_read_tuning_is_opt_in(monkeypatch, tmp_path):
    db_path = tmp_path / "state.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE sessions (id TEXT)")

    for name in (
        "HERMES_WEBUI_STATEDB_BUSY_MS",
        "HERMES_WEBUI_STATEDB_MMAP_MB",
        "HERMES_WEBUI_STATEDB_CACHE_MB",
        "HERMES_WEBUI_STATEDB_QUERY_ONLY",
    ):
        monkeypatch.delenv(name, raising=False)

    with agent_sessions.open_state_db_readonly(db_path) as conn:
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 0


def test_state_db_read_tuning_applies_requested_pragmas(monkeypatch, tmp_path):
    db_path = tmp_path / "state.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE sessions (id TEXT)")

    monkeypatch.setenv("HERMES_WEBUI_STATEDB_BUSY_MS", "1234")
    monkeypatch.setenv("HERMES_WEBUI_STATEDB_MMAP_MB", "16")
    monkeypatch.setenv("HERMES_WEBUI_STATEDB_CACHE_MB", "8")
    monkeypatch.setenv("HERMES_WEBUI_STATEDB_QUERY_ONLY", "true")

    with agent_sessions.open_state_db_readonly(db_path) as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 1234
        assert conn.execute("PRAGMA mmap_size").fetchone()[0] == 16 * 1024 * 1024
        assert conn.execute("PRAGMA cache_size").fetchone()[0] == -(8 * 1024)
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 1
