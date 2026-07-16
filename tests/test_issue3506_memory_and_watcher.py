"""Regression tests for issue #3506 — WebUI memory growth and idle CPU.

A user on a large install (615 sessions / 40k messages in state.db) reported the
WebUI Python process climbing from ~100 MB to ~1.5 GB RSS over 3 days and holding
180%+ CPU at idle. Three independent contributors were confirmed in the code:

  1. ``api.session_lifecycle._sessions`` grew without bound — keys were inserted
     on ``register_agent`` / ``mark_turn_completed`` but never deleted, so every
     unique session_id the WebUI ever touched leaked a permanent entry.
  2. ``SESSION_AGENT_CACHE_MAX`` / ``SESSIONS_MAX`` were hard-coded with no way
     for an operator to tune the dominant RSS lever without editing source.
  3. ``GatewayWatcher`` re-ran an expensive per-session ``MAX(messages.timestamp)``
     aggregation over an oversampled candidate set every 5s, forever, even when
     nothing in the sidebar-visible session set had changed.

These tests pin the fixes for all three:
  * ``session_lifecycle.discard_session`` bounds the dict, safely.
  * ``config._env_int`` makes the caps env-overridable with safe fallback.
  * ``gateway_watcher._cheap_change_fingerprint`` is a small change signal
    that avoids scanning the full messages table on every idle poll.
"""
from __future__ import annotations

import importlib
import sqlite3
import time
from pathlib import Path


# ─────────────────────────── Fix 1: lifecycle leak ───────────────────────────

def _fresh_lifecycle():
    lifecycle = importlib.import_module("api.session_lifecycle")
    lifecycle = importlib.reload(lifecycle)
    reset = getattr(lifecycle, "_reset_for_tests", None)
    if callable(reset):
        reset()
    return lifecycle


class _Agent:
    def commit_memory_session(self):  # pragma: no cover - not exercised here
        pass


def test_discard_session_removes_clean_entry():
    """A registered-then-completed-then-committed session must be evictable."""
    lc = _fresh_lifecycle()
    agent = _Agent()
    sid = "clean-session"

    lc.register_agent(sid, agent)
    gen = lc.mark_turn_completed(sid, agent=agent)
    # Simulate a successful commit catching up to the latest generation.
    with lc._condition:
        lc._sessions[sid]["committed_generation"] = gen

    assert sid in lc._sessions
    assert lc.has_uncommitted_work(sid) is False
    assert lc.discard_session(sid) is True
    assert sid not in lc._sessions, "clean entry must be removed to bound growth"


def test_discard_session_preserves_uncommitted_work():
    """A session with pending memory work must NOT be discarded (stays retryable)."""
    lc = _fresh_lifecycle()
    agent = _Agent()
    sid = "dirty-session"

    lc.register_agent(sid, agent)
    lc.mark_turn_completed(sid, agent=agent)  # generation > committed_generation

    assert lc.has_uncommitted_work(sid) is True
    assert lc.discard_session(sid) is False
    assert sid in lc._sessions, "dirty entry must be preserved so commit can retry"


def test_discard_session_preserves_in_flight_commit():
    """An in-flight commit must block discard to avoid racing the committer."""
    lc = _fresh_lifecycle()
    agent = _Agent()
    sid = "in-flight-session"

    lc.register_agent(sid, agent)
    gen = lc.mark_turn_completed(sid, agent=agent)
    with lc._condition:
        lc._sessions[sid]["committed_generation"] = gen  # clean...
        lc._sessions[sid]["in_flight"] = True             # ...but a commit is running

    assert lc.discard_session(sid) is False
    assert sid in lc._sessions


def test_discard_session_absent_key_is_noop_success():
    lc = _fresh_lifecycle()
    assert lc.discard_session("never-seen") is True
    assert lc.discard_session("") is False


def test_lifecycle_dict_is_bounded_under_churn():
    """Register/complete/commit/discard across many sessions must not accumulate."""
    lc = _fresh_lifecycle()
    for i in range(500):
        sid = f"churn-{i}"
        agent = _Agent()
        lc.register_agent(sid, agent)
        gen = lc.mark_turn_completed(sid, agent=agent)
        with lc._condition:
            lc._sessions[sid]["committed_generation"] = gen
        lc.unregister_agent(sid)
        assert lc.discard_session(sid) is True
    assert len(lc._sessions) == 0, "dict must not grow unbounded across session churn"


# ─────────────────────────── Fix 2: tunable caps ─────────────────────────────

def test_env_int_reads_valid_override(monkeypatch):
    cfg = importlib.import_module("api.config")
    monkeypatch.setenv("HERMES_TEST_CAP", "12")
    assert cfg._env_int("HERMES_TEST_CAP", 99) == 12


def test_env_int_falls_back_on_bad_input(monkeypatch):
    cfg = importlib.import_module("api.config")
    monkeypatch.setenv("HERMES_TEST_CAP", "not-a-number")
    assert cfg._env_int("HERMES_TEST_CAP", 99) == 99
    monkeypatch.setenv("HERMES_TEST_CAP", "")
    assert cfg._env_int("HERMES_TEST_CAP", 99) == 99
    monkeypatch.delenv("HERMES_TEST_CAP", raising=False)
    assert cfg._env_int("HERMES_TEST_CAP", 99) == 99


def test_env_int_rejects_below_minimum(monkeypatch):
    cfg = importlib.import_module("api.config")
    monkeypatch.setenv("HERMES_TEST_CAP", "0")
    # A 0 or negative cap would disable the bound entirely — must fall back.
    assert cfg._env_int("HERMES_TEST_CAP", 99) == 99
    monkeypatch.setenv("HERMES_TEST_CAP", "-5")
    assert cfg._env_int("HERMES_TEST_CAP", 99) == 99


def test_agent_cache_max_default_is_bounded():
    cfg = importlib.import_module("api.config")
    # Default must remain a sane, modest bound (each entry pins a full transcript).
    assert isinstance(cfg.SESSION_AGENT_CACHE_MAX, int)
    assert 1 <= cfg.SESSION_AGENT_CACHE_MAX <= 50
    assert isinstance(cfg.SESSIONS_MAX, int)
    assert cfg.SESSIONS_MAX >= 1


# ─────────────────────── Fix 3: cheap watcher fingerprint ────────────────────

def _make_db(tmp_path: Path):
    db = tmp_path / "state.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            session_source TEXT,
            model TEXT,
            started_at REAL NOT NULL,
            ended_at REAL,
            end_reason TEXT,
            parent_session_id TEXT,
            message_count INTEGER DEFAULT 0,
            title TEXT,
            archived INTEGER DEFAULT 0
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT,
            timestamp REAL NOT NULL
        );
        """
    )
    conn.commit()
    return db, conn


def _add_session(conn, sid, source="telegram", mc=2, started=None, title="Chat"):
    started = started or time.time()
    conn.execute(
        "INSERT OR REPLACE INTO sessions (id, source, model, started_at, message_count, title) "
        "VALUES (?, ?, 'm', ?, ?, ?)",
        (sid, source, started, mc, title),
    )
    for i in range(mc):
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', 'x', ?)",
            (sid, started + i),
        )
    conn.commit()


def test_cheap_fingerprint_stable_and_sensitive(tmp_path):
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "tg1", "telegram", mc=2)
    _add_session(conn, "dc1", "discord", mc=3)

    fp1 = gw._cheap_change_fingerprint(db)
    fp2 = gw._cheap_change_fingerprint(db)
    assert fp1 is not None
    assert fp1 == fp2, "fingerprint must be stable when nothing changes"

    # New message in a visible session bumps message_count -> fingerprint changes.
    _add_session(conn, "tg1", "telegram", mc=3)
    fp3 = gw._cheap_change_fingerprint(db)
    assert fp3 != fp1, "fingerprint must change when a visible session gains a message"

    # New session appears -> fingerprint changes.
    _add_session(conn, "tg2", "telegram", mc=1)
    fp4 = gw._cheap_change_fingerprint(db)
    assert fp4 != fp3


def test_cheap_fingerprint_excludes_default_sources_and_their_messages(tmp_path):
    """cron/subagent/webui churn is wholly outside the default projection."""
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "tg1", "telegram", mc=2)
    fp1 = gw._cheap_change_fingerprint(db)

    # A cron session row with no messages is outside the sidebar projection, so
    # the sessions aggregate should not move.
    conn.execute(
        "INSERT INTO sessions (id, source, model, started_at, message_count, title) "
        "VALUES ('cron1', 'cron', 'm', ?, 0, 'cron')",
        (time.time(),),
    )
    conn.commit()
    fp2 = gw._cheap_change_fingerprint(db)
    assert fp2 == fp1, "excluded-source session rows must not move the sessions aggregate"

    # Message aggregates are joined through included sessions, not global.
    conn.execute(
        "INSERT INTO messages (session_id, role, content, timestamp) "
        "VALUES ('cron1', 'user', 'x', ?)",
        (time.time(),),
    )
    conn.commit()
    fp3 = gw._cheap_change_fingerprint(db)
    assert fp3 == fp2

    for source in ("subagent", "webui"):
        conn.execute(
            "INSERT INTO sessions (id, source, model, started_at, message_count, title) "
            "VALUES (?, ?, 'm', ?, 0, ?)",
            (source, source, time.time(), source),
        )
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', 'x', ?)",
            (source, time.time()),
        )
    conn.commit()
    assert gw._cheap_change_fingerprint(db) == fp2


def test_cheap_fingerprint_detects_source_change(tmp_path):
    """A source retag changes the projection's derived source_label / visibility,
    so the cheap fingerprint MUST change even though no displayed field moved."""
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "s1", "telegram", mc=2)
    fp1 = gw._cheap_change_fingerprint(db)

    conn.execute("UPDATE sessions SET source = 'discord' WHERE id = 's1'")
    conn.commit()
    fp2 = gw._cheap_change_fingerprint(db)
    assert fp2 != fp1, "a source change alters projected metadata and must be detected"


def test_cheap_fingerprint_detects_same_count_message_rewrite(tmp_path):
    """Regression (#3536 review): SessionDB.replace_messages (/retry, /undo,
    /compress) deletes + reinserts a transcript with NEW timestamps but can leave
    sessions.message_count UNCHANGED. The projection's last_activity
    (MAX(messages.timestamp)) moves, so the cheap fingerprint MUST still change
    even though every sessions-table column is identical — otherwise the watcher
    skips a re-projection and other tabs show stale last_activity ordering."""
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "s1", "telegram", mc=3)
    fp1 = gw._cheap_change_fingerprint(db)

    # Simulate replace_messages: same count (3), brand-new timestamps, no change
    # to ANY sessions-table column (message_count stays 3).
    conn.execute("DELETE FROM messages WHERE session_id = 's1'")
    base = time.time() + 10_000  # strictly later than the originals
    for i in range(3):
        conn.execute(
            "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', 'rewritten', ?)",
            ("s1", base + i),
        )
    conn.commit()
    # sessions table is byte-identical to before; only messages moved.
    assert conn.execute("SELECT message_count FROM sessions WHERE id='s1'").fetchone()[0] == 3
    fp2 = gw._cheap_change_fingerprint(db)
    assert fp2 != fp1, (
        "delete+insert transcript rewrites advance MAX(messages.id) and must "
        "invalidate the fingerprint even when message_count is unchanged"
    )


def test_cheap_fingerprint_detects_same_row_timestamp_update(tmp_path):
    """An in-place timestamp update changes projected last_activity."""
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "s1", "telegram", mc=3)
    fp1 = gw._cheap_change_fingerprint(db)

    conn.execute(
        "UPDATE messages SET content = 'rewritten', timestamp = timestamp + 1000 "
        "WHERE session_id = 's1'"
    )
    conn.commit()
    fp2 = gw._cheap_change_fingerprint(db)
    assert fp2 != fp1


def test_cheap_fingerprint_detects_lagging_message_count_append(tmp_path):
    """MAX(messages.id) catches new messages before sessions.message_count updates."""
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "s1", "telegram", mc=1)
    fp1 = gw._cheap_change_fingerprint(db)

    conn.execute(
        "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', 'late', ?)",
        ("s1", time.time() + 1000),
    )
    conn.commit()
    assert conn.execute("SELECT message_count FROM sessions WHERE id='s1'").fetchone()[0] == 1
    fp2 = gw._cheap_change_fingerprint(db)
    assert fp2 != fp1


def test_cheap_fingerprint_detects_message_delete_or_prune(tmp_path):
    """COUNT(messages) catches deletes/prunes even when MAX(messages.id) is unchanged."""
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "s1", "telegram", mc=3)
    fp1 = gw._cheap_change_fingerprint(db)

    conn.execute(
        "DELETE FROM messages WHERE id = (SELECT MIN(id) FROM messages WHERE session_id = 's1')"
    )
    conn.commit()
    fp2 = gw._cheap_change_fingerprint(db)
    assert fp2 != fp1


def test_cheap_fingerprint_tracks_projection_metadata_updates(tmp_path):
    """Lineage/visibility fields can alter collapse output and must invalidate."""
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "s1", "telegram", mc=2)
    fp0 = gw._cheap_change_fingerprint(db)

    conn.execute("UPDATE sessions SET parent_session_id = 'p-root' WHERE id = 's1'")
    conn.commit()
    fp1 = gw._cheap_change_fingerprint(db)
    assert fp1 != fp0

    conn.execute("UPDATE sessions SET end_reason = 'compressed' WHERE id = 's1'")
    conn.commit()
    fp2 = gw._cheap_change_fingerprint(db)
    assert fp2 != fp1

    conn.execute("UPDATE sessions SET ended_at = 1234567890.0 WHERE id = 's1'")
    conn.commit()
    fp3 = gw._cheap_change_fingerprint(db)
    assert fp3 != fp2


def test_cheap_fingerprint_handles_missing_db(tmp_path):
    gw = importlib.import_module("api.gateway_watcher")
    missing = tmp_path / "nope.db"
    # No exception, returns None so the caller falls back to the full read.
    assert gw._cheap_change_fingerprint(missing) is None


def test_cheap_fingerprint_handles_missing_optional_columns(tmp_path):
    """Older agent schemas without archived/ended_at must still produce a fingerprint."""
    gw = importlib.import_module("api.gateway_watcher")
    db = tmp_path / "old.db"
    conn = sqlite3.connect(str(db))
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            model TEXT,
            started_at REAL NOT NULL,
            message_count INTEGER DEFAULT 0,
            title TEXT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT,
            timestamp REAL NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO sessions (id, source, model, started_at, message_count, title) "
        "VALUES ('s1', 'telegram', 'm', ?, 2, 't')",
        (time.time(),),
    )
    conn.commit()
    fp = gw._cheap_change_fingerprint(db)
    assert fp is not None and isinstance(fp, str)


def test_cheap_fingerprint_returns_none_without_source_column(tmp_path):
    """A pre-source-tracking schema must return None (forces safe full read)."""
    gw = importlib.import_module("api.gateway_watcher")
    db = tmp_path / "ancient.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, started_at REAL)")
    conn.commit()
    assert gw._cheap_change_fingerprint(db) is None


def test_gateway_watcher_poll_interval_env_override(tmp_path, monkeypatch):
    gw = importlib.import_module("api.gateway_watcher")
    monkeypatch.setenv("HERMES_WEBUI_POLL_INTERVAL", "2.5")
    watcher = gw.GatewayWatcher(state_db_path=tmp_path / "state.db")
    assert watcher._poll_interval == 2.5

    monkeypatch.setenv("HERMES_WEBUI_POLL_INTERVAL", "0.1")
    watcher = gw.GatewayWatcher(state_db_path=tmp_path / "state.db")
    assert watcher._poll_interval == 1.0

    monkeypatch.setenv("HERMES_WEBUI_POLL_INTERVAL", "not-a-number")
    watcher = gw.GatewayWatcher(state_db_path=tmp_path / "state.db")
    assert watcher._poll_interval == gw.GatewayWatcher.POLL_INTERVAL


def test_gateway_watcher_idle_backoff_env_override(tmp_path, monkeypatch):
    gw = importlib.import_module("api.gateway_watcher")
    monkeypatch.setenv("HERMES_WEBUI_POLL_IDLE_BACKOFF", "7.5")
    watcher = gw.GatewayWatcher(state_db_path=tmp_path / "state.db")
    assert watcher._idle_backoff_multiplier == 7.5

    monkeypatch.setenv("HERMES_WEBUI_POLL_IDLE_BACKOFF", "0")
    watcher = gw.GatewayWatcher(state_db_path=tmp_path / "state.db")
    assert watcher._idle_backoff_multiplier == 1.0

    monkeypatch.setenv("HERMES_WEBUI_POLL_IDLE_BACKOFF", "invalid")
    watcher = gw.GatewayWatcher(state_db_path=tmp_path / "state.db")
    assert watcher._idle_backoff_multiplier == watcher.IDLE_BACKOFF_MULTIPLIER


def test_poll_loop_skips_projection_when_unchanged(tmp_path, monkeypatch):
    """The poll body must call the expensive projection only when the cheap fp changes."""
    gw = importlib.import_module("api.gateway_watcher")
    db, conn = _make_db(tmp_path)
    _add_session(conn, "tg1", "telegram", mc=2)

    monkeypatch.setattr(gw, "_get_state_db_path", lambda: db)

    calls = {"n": 0}
    real = gw._get_agent_sessions_from_db

    def counting(db_path=None):
        calls["n"] += 1
        return real(db_path)

    monkeypatch.setattr(gw, "_get_agent_sessions_from_db", counting)

    w = gw.GatewayWatcher()

    # Run the change-detection body directly (one iteration) without the thread.
    def one_iteration():
        db_path = w._state_db_path
        cheap_fp = gw._cheap_change_fingerprint(db_path) if db_path.exists() else ''
        if cheap_fp is not None and cheap_fp == w._last_cheap_fp:
            return
        sessions = gw._get_agent_sessions_from_db(db_path)
        if cheap_fp is not None:
            w._last_cheap_fp = cheap_fp
        _ = gw._snapshot_hash(sessions)

    one_iteration()  # first poll: must read
    assert calls["n"] == 1
    one_iteration()  # unchanged: must skip
    one_iteration()  # still unchanged: must skip
    assert calls["n"] == 1, "expensive projection must not run while state is unchanged"

    _add_session(conn, "tg1", "telegram", mc=3)  # a real change
    one_iteration()
    assert calls["n"] == 2, "expensive projection must run again after a real change"


def test_lru_eviction_skips_active_runs():
    """Regression (#3536 review round 2): lowering SESSION_AGENT_CACHE_MAX (50→25)
    makes LRU agent-cache eviction more likely to fire, so the eviction loop must
    NOT close an agent whose worker is still live. The loop must consult the
    ACTIVE_RUNS registry (worker lifecycle — survives a cancel/reconnect that
    drops STREAMS) and skip those session_ids, deferring (temporarily exceeding
    the cap) if every over-cap entry is active. Source-contract test: the deep
    streaming function isn't unit-invokable, so pin the invariant in source."""
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "api" / "streaming.py").read_text()
    idx = src.index("while len(SESSION_AGENT_CACHE) > SESSION_AGENT_CACHE_MAX:")
    block = src[idx - 1600:idx + 700]
    # The eviction path must build an active-session set from ACTIVE_RUNS...
    assert "ACTIVE_RUNS" in block and "_active_sids" in block, (
        "eviction must consult ACTIVE_RUNS to find live workers"
    )
    # ...skip active session_ids when choosing what to evict...
    assert "_sid not in _active_sids" in block, (
        "eviction must skip sessions with a live run"
    )
    # ...and defer (break) rather than evict when all over-cap entries are active.
    assert "all over-cap entries are active; defer" in block, (
        "eviction must defer (temporarily exceed cap) rather than close a live agent"
    )
    # The unconditional popitem(last=False) that closed the LRU agent regardless
    # of liveness must be gone from this block.
    assert "popitem(last=False)" not in block, (
        "the liveness-blind popitem eviction must be replaced"
    )
