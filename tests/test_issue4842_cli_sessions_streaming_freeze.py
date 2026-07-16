"""Behavioral test for #4842: the CLI/cron sidebar projection cache must not be
re-run on every poll while a turn is streaming.

Root cause: ``_CLI_SESSIONS_CACHE`` is keyed (via ``_resolve_cli_sessions_context``
-> ``_sqlite_file_stat_cache_key`` -> ``_sqlite_content_fingerprint``) on
``MAX(rowid) FROM messages``. During a live turn the gateway writes a message row
per streamed delta, so that fingerprint advances on essentially every
``/api/sessions`` poll, busting the cache and re-running the expensive CLI/cron
candidate-join + projection (and the lineage-metadata pass) on every poll — the
multi-second ``get_cli_sessions`` in #4842.

Fix: while a stream in the cache's profile is active, the cache key folds in a
stable streaming-freeze marker (keyed only on that profile's active stream ids)
instead of the volatile content fingerprint, and the cache TTL widens. Other
profiles remain independently fresh; unknown ownership freezes conservatively
for active-run safety.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api import models as M


def _set_active_streams(monkeypatch, ids):
    monkeypatch.setattr(M, "_active_stream_ids", lambda: set(ids))


def test_freeze_marker_none_when_idle(monkeypatch):
    _set_active_streams(monkeypatch, [])
    assert M._cli_sessions_streaming_freeze_marker() is None


def test_freeze_marker_stable_for_same_streams(monkeypatch):
    _set_active_streams(monkeypatch, ["sA", "sB"])
    m1 = M._cli_sessions_streaming_freeze_marker()
    m2 = M._cli_sessions_streaming_freeze_marker()
    assert m1 is not None and m1 == m2
    # order-independent
    _set_active_streams(monkeypatch, ["sB", "sA"])
    assert M._cli_sessions_streaming_freeze_marker() == m1


def test_freeze_marker_changes_when_stream_set_changes(monkeypatch):
    _set_active_streams(monkeypatch, ["sA"])
    m1 = M._cli_sessions_streaming_freeze_marker()
    _set_active_streams(monkeypatch, ["sA", "sB"])
    m2 = M._cli_sessions_streaming_freeze_marker()
    assert m1 != m2
    _set_active_streams(monkeypatch, [])
    assert M._cli_sessions_streaming_freeze_marker() is None


def test_cache_key_stable_across_message_writes_while_streaming(monkeypatch, tmp_path):
    """THE core guarantee: with a stream active, the CLI cache key must NOT change
    when the state.db content fingerprint advances (a new streamed message row).
    Before the fix the key folded in the live fingerprint and changed every write."""
    db = tmp_path / "state.db"
    db.write_bytes(b"")  # exists; fingerprint reader degrades gracefully

    monkeypatch.setattr(M, "_default_claude_code_projects_dir", lambda: tmp_path / "projects")
    # Simulate the volatile fingerprint advancing on each streamed message.
    fp = {"v": 0}
    monkeypatch.setattr(M, "_sqlite_file_stat_cache_key", lambda p: ("fp", fp["v"]))

    # Active stream -> key should be frozen (independent of fp).
    _set_active_streams(monkeypatch, ["live-stream-1"])
    _, _, _, key_a = M._resolve_cli_sessions_context(None)
    fp["v"] = 1  # a streamed message row landed
    _, _, _, key_b = M._resolve_cli_sessions_context(None)
    fp["v"] = 2  # another
    _, _, _, key_c = M._resolve_cli_sessions_context(None)
    assert key_a == key_b == key_c, (
        "CLI cache key changed across message writes while streaming — the heavy "
        "projection would re-run on every poll (#4842 regression)"
    )

    # Idle -> key tracks the fingerprint again (so genuine new rows show up).
    _set_active_streams(monkeypatch, [])
    fp["v"] = 10
    _, _, _, key_idle1 = M._resolve_cli_sessions_context(None)
    fp["v"] = 11
    _, _, _, key_idle2 = M._resolve_cli_sessions_context(None)
    assert key_idle1 != key_idle2, (
        "When idle the CLI cache key must still advance with the content "
        "fingerprint so newly-committed sessions are not served stale"
    )


def test_streaming_ttl_wider_than_idle(monkeypatch):
    _set_active_streams(monkeypatch, [])
    idle = M._cli_sessions_cache_ttl_seconds()
    _set_active_streams(monkeypatch, ["s1"])
    streaming = M._cli_sessions_cache_ttl_seconds()
    assert streaming > idle, (
        "streaming TTL must exceed idle TTL so the fixed poll cadence does not "
        "force a rebuild on every poll (#4842)"
    )


def test_streaming_freeze_and_ttl_are_profile_local(monkeypatch):
    from api import config as cfg

    _set_active_streams(monkeypatch, ["alpha-stream"])
    monkeypatch.setattr(
        cfg,
        "STREAM_SESSION_OWNER_PROFILES",
        {"alpha-stream": "alpha"},
        raising=False,
    )
    monkeypatch.setattr(cfg, "STREAM_SESSION_OWNERS", {}, raising=False)

    assert M._cli_sessions_streaming_freeze_marker("alpha") is not None
    assert M._cli_sessions_streaming_freeze_marker("beta") is None
    assert M._cli_sessions_cache_ttl_seconds("alpha") == M._CLI_SESSIONS_CACHE_STREAMING_TTL_SECONDS
    assert M._cli_sessions_cache_ttl_seconds("beta") == M._CLI_SESSIONS_CACHE_TTL_SECONDS
    assert M._cli_sessions_streaming_freeze_marker(all_profiles=True) is not None


def test_route_cache_freeze_is_profile_local(monkeypatch):
    from api import config as cfg
    from api import route_session_list_cache as route_cache

    monkeypatch.setattr(route_cache, "_session_list_cache_active_stream_ids", lambda: {"alpha-stream"})
    monkeypatch.setattr(
        cfg,
        "STREAM_SESSION_OWNER_PROFILES",
        {"alpha-stream": "alpha"},
        raising=False,
    )
    monkeypatch.setattr(cfg, "STREAM_SESSION_OWNERS", {}, raising=False)
    alpha_key = route_cache._session_list_cache_key("alpha", False, True, False, False)
    beta_key = route_cache._session_list_cache_key("beta", False, True, False, False)

    assert route_cache._session_list_cache_streaming_freeze_marker(alpha_key) is not None
    assert route_cache._session_list_cache_streaming_freeze_marker(beta_key) is None


def test_unknown_stream_owner_freezes_conservatively(monkeypatch):
    from api import config as cfg

    _set_active_streams(monkeypatch, ["registration-window"])
    monkeypatch.setattr(cfg, "STREAM_SESSION_OWNER_PROFILES", {}, raising=False)
    monkeypatch.setattr(cfg, "STREAM_SESSION_OWNERS", {}, raising=False)
    assert M._cli_sessions_streaming_freeze_marker("beta") is not None


def test_structural_change_listener_clears_cli_cache(monkeypatch):
    """While streaming, the CLI cache is frozen and no longer self-invalidates via
    the content fingerprint. A structural mutation (cron completion / new / renamed
    / archived session) must therefore clear the CLI cache directly so the change
    surfaces promptly instead of lagging up to the streaming TTL. Those structural
    signals fire the session-list-changed listener; per-token message writes never
    do — that is exactly what makes the freeze safe."""
    from api import routes as R

    cleared = {"n": 0}
    monkeypatch.setattr(M, "clear_cli_sessions_cache", lambda: cleared.__setitem__("n", cleared["n"] + 1))
    # The route module imports the symbol lazily inside the listener, so patching
    # api.models.clear_cli_sessions_cache is what the listener resolves.
    R._on_session_list_changed("default")
    assert cleared["n"] >= 1, (
        "_on_session_list_changed must clear the CLI/cron projection cache so a "
        "structural mutation isn't masked by the streaming freeze (#4842)"
    )

