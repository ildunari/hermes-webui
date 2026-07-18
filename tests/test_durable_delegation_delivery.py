from __future__ import annotations

import json
import queue
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest


pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _agent_delivery_module():
    return pytest.importorskip(
        "tools.async_delegation", reason="Hermes Agent durable-delivery API is unavailable"
    )


@contextmanager
def _agent_home(home: Path):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    token = set_hermes_home_override(home)
    try:
        yield
    finally:
        reset_hermes_home_override(token)


def _seed_durable_row(
    home: Path,
    evt: dict,
    *,
    claim: str | None = None,
    claim_age: float = 0.0,
) -> None:
    mod = _agent_delivery_module()
    home.mkdir(parents=True, exist_ok=True)
    with _agent_home(home):
        conn = mod._connect()
        try:
            now = float(evt.get("completed_at") or time.time())
            conn.execute(
                """INSERT OR REPLACE INTO async_delegations
                   (delegation_id, origin_session, origin_ui_session_id,
                    parent_session_id, state, dispatched_at, completed_at,
                    updated_at, event_json, result_json, delivery_state,
                    delivery_attempts, delivery_claim, delivery_claimed_at)
                   VALUES (?, ?, ?, NULL, 'completed', ?, ?, ?, ?, '{}',
                           'pending', 0, ?, ?)""",
                (
                    evt["delegation_id"],
                    evt["session_key"],
                    evt.get("origin_ui_session_id", ""),
                    now - 1,
                    now,
                    now,
                    json.dumps(evt),
                    claim,
                    now - claim_age if claim else None,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def _delivery_row(home: Path, delegation_id: str) -> tuple:
    with sqlite3.connect(home / "state.db") as conn:
        return conn.execute(
            """SELECT delivery_state, delivery_attempts, delivery_claim
               FROM async_delegations WHERE delegation_id=?""",
            (delegation_id,),
        ).fetchone()


def _make_webui_session(monkeypatch, tmp_path: Path, sid: str, profile: str):
    from api import config, models

    state_dir = tmp_path / "webui"
    session_dir = state_dir / "sessions"
    session_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(config, "STATE_DIR", state_dir)
    monkeypatch.setattr(models, "SESSION_DIR", session_dir)
    with models.LOCK:
        models.SESSIONS.clear()
    session = models.Session(
        session_id=sid,
        profile=profile,
        workspace=str(tmp_path),
        model="test/model",
    )
    session.save(skip_index=True)
    return session


def _clear_delivery_memory(bp, cfg, sid: str, delegation_id: str) -> None:
    with cfg.BG_TASK_COMPLETE_EVENTS_SEEN_LOCK:
        cfg.BG_TASK_COMPLETE_EVENTS_SEEN.pop(sid, None)
    with cfg.DEFERRED_PROCESS_WAKEUPS_LOCK:
        cfg.DEFERRED_PROCESS_WAKEUPS.pop(sid, None)
    cfg.PENDING_BG_TASK_COMPLETIONS.discard(sid)
    try:
        from tools.process_registry import process_registry

        with process_registry._lock:
            process_registry._completion_consumed.discard(delegation_id)
    except Exception:
        pass
    retry_lock = getattr(bp, "_DURABLE_RETRY_LOCK", None)
    retry_pending = getattr(bp, "_DURABLE_RETRY_PENDING", None)
    if retry_lock is not None and retry_pending is not None:
        with retry_lock:
            retry_pending.discard(delegation_id)


def _event(sid: str, delegation_id: str, *, restored: bool = False) -> dict:
    return {
        "type": "async_delegation",
        "delegation_id": delegation_id,
        "session_key": sid,
        "origin_ui_session_id": sid,
        "status": "completed",
        "goal": "durable delivery regression",
        "summary": f"result for {delegation_id}",
        "completed_at": time.time(),
        "restored": restored,
    }


def _install_start_turn(monkeypatch, responses):
    import api.routes as routes

    calls: list[dict] = []
    done = threading.Event()
    response_iter = iter(responses)

    def _start(session_id, message, *, source="process_wakeup", **kwargs):
        from api.process_wakeup_store import (
            get_process_wakeup,
            mark_process_wakeup_started,
            reserve_pending_process_wakeup,
        )

        wakeup_id = str(kwargs.get("process_wakeup_id") or "").strip()
        existing = get_process_wakeup(wakeup_id) if wakeup_id else None
        if existing and existing.state in {"started", "completed"}:
            return {
                "_status": 200,
                "stream_id": "already-accepted",
                "already_accepted": True,
            }
        calls.append(
            {
                "session_id": session_id,
                "message": message,
                "source": source,
                **kwargs,
            }
        )
        done.set()
        try:
            status = next(response_iter)
        except StopIteration:
            status = 200
        if isinstance(status, dict):
            response = dict(status)
        else:
            response = {"_status": status, "stream_id": f"stream-{len(calls)}"}
        if int(response.get("_status", 200) or 200) < 400 and wakeup_id:
            reserve_pending_process_wakeup(
                {
                    "wakeup_id": wakeup_id,
                    "session_id": session_id,
                    "message": message,
                    "source": source,
                    "durable_event": kwargs.get("process_wakeup_payload") or {},
                }
            )
            assert mark_process_wakeup_started(
                wakeup_id, str(response.get("stream_id") or "fake-stream")
            )
        return response

    monkeypatch.setattr(routes, "start_session_turn", _start)
    return calls, done


def _wait_until(predicate, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


def test_idle_delivery_is_exact_once_and_uses_owning_profile_state_db(monkeypatch, tmp_path):
    from api import background_process as bp, config as cfg, profiles
    from api.process_wakeup_store import get_process_wakeup

    sid = "durable-profile-owner"
    delegation_id = "deleg-profile-owner"
    profile = "alpha"
    alpha_home = tmp_path / "homes" / "alpha"
    beta_home = tmp_path / "homes" / "beta"
    evt = _event(sid, delegation_id, restored=True)
    _make_webui_session(monkeypatch, tmp_path, sid, profile)
    _seed_durable_row(alpha_home, evt)
    _seed_durable_row(beta_home, evt)
    monkeypatch.setattr(
        profiles,
        "get_hermes_home_for_profile",
        lambda name: alpha_home if name == profile else beta_home,
    )
    monkeypatch.setenv("HERMES_HOME", str(beta_home))
    calls, done = _install_start_turn(monkeypatch, [200])

    try:
        bp._process_one(dict(evt))
        assert done.wait(2.0)
        assert _wait_until(lambda: _delivery_row(alpha_home, delegation_id)[0] == "delivered")
        assert _delivery_row(beta_home, delegation_id)[0] == "pending"
        accepted = get_process_wakeup(delegation_id)
        assert accepted is not None and accepted.state == "started"
        assert accepted.agent_acknowledged is True

        # Simulate a process restart: all volatile dedupe is gone. A delivered
        # durable row must not restore, and replaying a stale queue copy must not
        # start a second turn.
        _clear_delivery_memory(bp, cfg, sid, delegation_id)
        restored = queue.Queue()
        with _agent_home(alpha_home):
            assert _agent_delivery_module().restore_undelivered_completions(restored) == 0
        bp._process_one(dict(evt))
        time.sleep(0.15)
        assert len(calls) == 1
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_active_turn_keeps_row_pending_until_deferred_delivery_survives_restart(
    monkeypatch, tmp_path
):
    from api import background_process as bp, config as cfg, profiles

    sid = "durable-active-restart"
    delegation_id = "deleg-active-restart"
    profile = "alpha"
    home = tmp_path / "homes" / profile
    evt = _event(sid, delegation_id, restored=True)
    _make_webui_session(monkeypatch, tmp_path, sid, profile)
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    calls, done = _install_start_turn(monkeypatch, [200])
    active_stream = "stream-active-durable"
    cfg.ACTIVE_RUNS[active_stream] = {"session_id": sid}

    try:
        bp._process_one(dict(evt))
        assert _delivery_row(home, delegation_id)[0] == "pending"
        assert cfg.DEFERRED_PROCESS_WAKEUPS.get(sid)
        assert not done.is_set()

        # Crash: the volatile deferred copy disappears. The durable row must
        # restore and deliver in the new process instead of being lost.
        cfg.ACTIVE_RUNS.pop(active_stream, None)
        _clear_delivery_memory(bp, cfg, sid, delegation_id)
        restored = queue.Queue()
        with _agent_home(home):
            assert _agent_delivery_module().restore_undelivered_completions(restored) == 1
        bp._process_one(restored.get_nowait())
        assert done.wait(2.0)
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "delivered")
        assert len(calls) == 1
    finally:
        cfg.ACTIVE_RUNS.pop(active_stream, None)
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_failed_server_delivery_retries_without_restart(monkeypatch, tmp_path):
    from api import background_process as bp, config as cfg, profiles

    sid = "durable-retry-no-restart"
    delegation_id = "deleg-retry-no-restart"
    profile = "alpha"
    home = tmp_path / "homes" / profile
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, profile)
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    calls, _done = _install_start_turn(monkeypatch, [503, 200])

    try:
        bp._process_one(dict(evt))
        assert _wait_until(lambda: len(calls) >= 2, timeout=4.0), calls
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "delivered")
        assert len(calls) == 2
        assert _delivery_row(home, delegation_id)[1] >= 2
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_409_defer_does_not_ack_volatile_copy_and_later_delivery_acks(
    monkeypatch, tmp_path
):
    from api import background_process as bp, config as cfg, profiles

    sid = "durable-409-defer"
    delegation_id = "deleg-409-defer"
    profile = "alpha"
    home = tmp_path / "homes" / profile
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, profile)
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    calls, _done = _install_start_turn(monkeypatch, [409, 200])

    try:
        bp._process_one(dict(evt))
        assert _wait_until(lambda: len(calls) == 1)
        assert _wait_until(lambda: bool(cfg.DEFERRED_PROCESS_WAKEUPS.get(sid)))
        assert _delivery_row(home, delegation_id)[0] == "pending"
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[2] is None)

        assert bp.drain_deferred_wakeups_for_session(sid) == 1
        assert _wait_until(lambda: len(calls) == 2)
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "delivered")
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_acknowledgement_failure_retries_ack_without_duplicate_turn(monkeypatch, tmp_path):
    from api import background_process as bp, config as cfg, profiles

    mod = _agent_delivery_module()
    sid = "durable-ack-retry"
    delegation_id = "deleg-ack-retry"
    profile = "alpha"
    home = tmp_path / "homes" / profile
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, profile)
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    calls, _done = _install_start_turn(monkeypatch, [200])
    real_complete = mod.complete_event_delivery
    complete_attempts = []

    def _fail_once(event, claim_id):
        complete_attempts.append(claim_id)
        if len(complete_attempts) == 1:
            return None
        return real_complete(event, claim_id)

    monkeypatch.setattr(mod, "complete_event_delivery", _fail_once)
    try:
        bp._process_one(dict(evt))
        assert _wait_until(lambda: len(complete_attempts) >= 2, timeout=4.0)
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "delivered")
        assert len(calls) == 1
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_ack_failure_then_restart_uses_durable_webui_receipt_exact_once(
    monkeypatch, tmp_path
):
    from api import background_process as bp, config as cfg, models, profiles
    from api.process_wakeup_store import get_process_wakeup

    mod = _agent_delivery_module()
    sid = "durable-ack-restart"
    delegation_id = "deleg-ack-restart"
    profile = "alpha"
    home = tmp_path / "homes" / profile
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, profile)
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    calls, _done = _install_start_turn(monkeypatch, [200])
    real_complete = mod.complete_event_delivery
    monkeypatch.setattr(mod, "complete_event_delivery", lambda _evt, _claim: None)
    monkeypatch.setattr(bp, "_schedule_durable_retry", lambda *_a, **_kw: False)

    try:
        bp._process_one(dict(evt))
        assert _wait_until(lambda: len(calls) == 1)
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "pending")
        assert _wait_until(
            lambda: bool(
                (record := get_process_wakeup(delegation_id))
                and record.state == "started"
            )
        )

        # New process: volatile dedupe is gone, while both SQLite and the WebUI
        # sidecar receipt survive. Replay must only acknowledge the row.
        _clear_delivery_memory(bp, cfg, sid, delegation_id)
        models.SESSIONS.pop(sid, None)
        monkeypatch.setattr(mod, "complete_event_delivery", real_complete)
        replay = dict(evt)
        replay["restored"] = True
        bp._process_one(replay)
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "delivered")
        assert len(calls) == 1
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_start_session_turn_short_circuits_only_from_started_durable_record(
    monkeypatch, tmp_path
):
    from api import models, routes
    from api.process_wakeup_store import (
        mark_process_wakeup_started,
        reserve_pending_process_wakeup,
    )

    sid = "durable-route-receipt"
    delegation_id = "deleg-route-receipt"
    _make_webui_session(monkeypatch, tmp_path, sid, "alpha")
    reserve_pending_process_wakeup(
        {
            "wakeup_id": delegation_id,
            "session_id": sid,
            "message": "must not start a duplicate turn",
            "source": "process_wakeup",
        }
    )
    assert mark_process_wakeup_started(delegation_id, "started-stream")
    models.SESSIONS.pop(sid, None)

    response = routes.start_session_turn(
        sid,
        "must not start a duplicate turn",
        process_wakeup_id=delegation_id,
    )
    assert response["_status"] == 200
    assert response["already_accepted"] is True
    assert response["session_id"] == sid
    assert response["process_wakeup_state"] == "started"


def test_streaming_drain_never_accepts_durable_event_before_turn_persistence(
    monkeypatch, tmp_path
):
    from api import config as cfg, profiles, streaming
    from tools.process_registry import process_registry

    sid = "durable-streaming-drain"
    delegation_id = "deleg-streaming-drain"
    profile = "alpha"
    home = tmp_path / "homes" / profile
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, profile)
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)

    with process_registry._lock:
        process_registry._completion_consumed.discard(delegation_id)
    process_registry.completion_queue.put(dict(evt))
    try:
        # The streaming drain must not create a local-only prompt/ack window.
        # It hands the event back to the background path without consuming it.
        assert streaming._drain_webui_process_notifications(sid) == []
        queued_ids = []
        while True:
            try:
                queued_ids.append(
                    process_registry.completion_queue.get_nowait().get("delegation_id")
                )
            except queue.Empty:
                break
        assert delegation_id in queued_ids
        assert _delivery_row(home, delegation_id)[0] == "pending"
        assert not process_registry.is_completion_consumed(delegation_id)

        # Simulate a crash after the drain returned but before the background
        # consumer saw its queue copy. The durable Agent row must restore.
        restored = queue.Queue()
        with _agent_home(home):
            assert _agent_delivery_module().restore_undelivered_completions(restored) == 1
        assert restored.get_nowait()["delegation_id"] == delegation_id
    finally:
        with process_registry._lock:
            process_registry._completion_consumed.discard(delegation_id)
        with cfg.BG_TASK_COMPLETE_EVENTS_SEEN_LOCK:
            cfg.BG_TASK_COMPLETE_EVENTS_SEEN.pop(sid, None)
        while True:
            try:
                process_registry.completion_queue.get_nowait()
            except queue.Empty:
                break


def test_async_event_without_positive_webui_owner_is_left_for_rightful_consumer(
    monkeypatch, tmp_path
):
    from api import background_process as bp, config as cfg, profiles

    sid = "not-a-webui-session"
    delegation_id = "deleg-cli-owned"
    home = tmp_path / "homes" / "alpha"
    evt = _event(sid, delegation_id, restored=True)
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    calls, _done = _install_start_turn(monkeypatch, [200])

    try:
        bp._process_one(dict(evt))
        time.sleep(0.1)
        assert calls == []
        assert _delivery_row(home, delegation_id)[0] == "pending"
        assert _delivery_row(home, delegation_id)[2] is None
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_streaming_handoff_preserves_claimed_event_without_injection(monkeypatch, tmp_path):
    from api import profiles, streaming
    from tools.process_registry import process_registry

    sid = "durable-claim-denied"
    delegation_id = "deleg-claim-denied"
    profile = "alpha"
    home = tmp_path / "homes" / profile
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, profile)
    _seed_durable_row(home, evt, claim="other-consumer")
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    with process_registry._lock:
        process_registry._completion_consumed.discard(delegation_id)
    process_registry.completion_queue.put(dict(evt))

    try:
        assert streaming._drain_webui_process_notifications(sid) == []
        assert _delivery_row(home, delegation_id)[0] == "pending"
        assert _delivery_row(home, delegation_id)[2] == "other-consumer"
        assert not process_registry.is_completion_consumed(delegation_id)
        queued_ids = []
        while True:
            try:
                queued_ids.append(
                    process_registry.completion_queue.get_nowait().get("delegation_id")
                )
            except queue.Empty:
                break
        assert delegation_id in queued_ids
    finally:
        with process_registry._lock:
            process_registry._completion_consumed.discard(delegation_id)
        while True:
            try:
                process_registry.completion_queue.get_nowait()
            except queue.Empty:
                break


def test_claim_contention_retries_until_stale_claim_takeover_without_restart(
    monkeypatch, tmp_path
):
    from api import background_process as bp, config as cfg, profiles

    sid = "durable-stale-claim-retry"
    delegation_id = "deleg-stale-claim-retry"
    home = tmp_path / "homes" / "alpha"
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, "alpha")
    # Agent takeover threshold is 300s. Keep it briefly live so the first claim
    # is denied, then let the configured retry cross the stale boundary.
    _seed_durable_row(home, evt, claim="crashed-consumer", claim_age=299.92)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    monkeypatch.setenv("HERMES_WEBUI_DURABLE_CLAIM_RETRY_DELAY_SECS", "0.02")
    calls, _done = _install_start_turn(monkeypatch, [200])

    try:
        bp._process_one(dict(evt))
        assert _wait_until(lambda: len(calls) == 1, timeout=2.0), calls
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "delivered")
        assert _delivery_row(home, delegation_id)[1] >= 1
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_deferred_claim_contention_schedules_idle_retry_without_new_teardown(
    monkeypatch, tmp_path
):
    from api import background_process as bp, config as cfg, profiles

    sid = "durable-deferred-stale-claim"
    delegation_id = "deleg-deferred-stale-claim"
    home = tmp_path / "homes" / "alpha"
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, "alpha")
    _seed_durable_row(home, evt, claim="crashed-consumer", claim_age=299.92)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    monkeypatch.setenv("HERMES_WEBUI_DURABLE_CLAIM_RETRY_DELAY_SECS", "0.02")
    calls, _done = _install_start_turn(monkeypatch, [200])
    assert bp.record_deferred_wakeup(
        sid,
        delegation_id,
        "[ASYNC DELEGATION COMPLETE] deferred result",
        durable_event=evt,
    )

    try:
        assert bp.drain_deferred_wakeups_for_session(sid) == 0
        assert _wait_until(lambda: len(calls) == 1, timeout=2.0), calls
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "delivered")
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_partial_agent_delivery_api_retries_after_api_recovers_without_restart(
    monkeypatch, tmp_path
):
    from api import background_process as bp, config as cfg, profiles

    mod = _agent_delivery_module()
    sid = "durable-partial-api-retry"
    delegation_id = "deleg-partial-api-retry"
    home = tmp_path / "homes" / "alpha"
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, "alpha")
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    monkeypatch.setenv("HERMES_WEBUI_DURABLE_CLAIM_RETRY_DELAY_SECS", "0.02")
    calls, _done = _install_start_turn(monkeypatch, [200])
    real_release = mod.release_event_delivery
    monkeypatch.setattr(mod, "release_event_delivery", None)

    try:
        bp._process_one(dict(evt))
        assert calls == []
        assert _delivery_row(home, delegation_id)[0] == "pending"
        # Repair the partially-upgraded Agent module in this same process. The
        # scheduled copy must retry without depending on startup restoration.
        monkeypatch.setattr(mod, "release_event_delivery", real_release)
        assert _wait_until(lambda: len(calls) == 1, timeout=2.0), calls
        assert _wait_until(lambda: _delivery_row(home, delegation_id)[0] == "delivered")
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_ownerless_event_has_no_elapsed_timer_churn(monkeypatch, tmp_path):
    from api import background_process as bp, config as cfg, profiles

    sid = "foreign-cli-session"
    delegation_id = "deleg-ownerless-no-churn"
    home = tmp_path / "homes" / "alpha"
    evt = _event(sid, delegation_id, restored=True)
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    scheduled = []

    class RecordingTimer:
        def __init__(self, *args, **kwargs):
            scheduled.append((args, kwargs))
            self.daemon = False

        def start(self):
            return None

    monkeypatch.setattr(bp.threading, "Timer", RecordingTimer)
    try:
        bp._process_one(dict(evt))
        time.sleep(0.35)
        assert scheduled == []
        assert _delivery_row(home, delegation_id) == ("pending", 0, None)
        with bp._DURABLE_RETRY_LOCK:
            assert delegation_id not in bp._DURABLE_RETRY_PENDING
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def test_paused_wakeup_keeps_real_row_pending_with_slow_retry(monkeypatch, tmp_path):
    from api import background_process as bp, config as cfg, profiles
    from api.process_wakeup_store import get_process_wakeup

    sid = "durable-paused-row"
    delegation_id = "deleg-paused-row"
    home = tmp_path / "homes" / "alpha"
    evt = _event(sid, delegation_id)
    _make_webui_session(monkeypatch, tmp_path, sid, "alpha")
    _seed_durable_row(home, evt)
    monkeypatch.setattr(profiles, "get_hermes_home_for_profile", lambda _name: home)
    monkeypatch.setattr(bp, "_DURABLE_PAUSED_RETRY_DELAY_SECS", 0.5)
    calls, _done = _install_start_turn(
        monkeypatch,
        [{"_status": 409, "error": "process_wakeup_paused"}],
    )

    try:
        bp._process_one(dict(evt))
        assert _wait_until(lambda: len(calls) == 1)
        time.sleep(0.2)
        assert len(calls) == 1
        assert _delivery_row(home, delegation_id)[0] == "pending"
        assert _delivery_row(home, delegation_id)[2] is None
        assert get_process_wakeup(delegation_id) is None
        with bp._DURABLE_RETRY_LOCK:
            assert delegation_id in bp._DURABLE_RETRY_PENDING
    finally:
        _clear_delivery_memory(bp, cfg, sid, delegation_id)


def _install_production_wakeup_route(monkeypatch, tmp_path, sid, delegation_id):
    from api import routes, turn_journal

    session = _make_webui_session(monkeypatch, tmp_path, sid, "alpha")
    executions = []
    monkeypatch.setattr(routes, "ensure_agent_runtime_current", lambda: None)
    monkeypatch.setattr(routes, "webui_gateway_chat_enabled", lambda _cfg: False)
    monkeypatch.setattr(routes, "get_config", lambda: {})
    monkeypatch.setattr(routes, "set_last_workspace", lambda _workspace: None)
    monkeypatch.setattr(
        turn_journal, "append_turn_journal_event", lambda *_args, **_kwargs: {}
    )
    monkeypatch.setattr(
        routes,
        "_run_agent_streaming",
        lambda *args, **kwargs: executions.append((args, kwargs)),
    )
    kwargs = {
        "msg": "[ASYNC DELEGATION COMPLETE] production recovery payload",
        "attachments": [],
        "workspace": str(tmp_path),
        "model": "test/model",
        "model_provider": "test",
        "source": "process_wakeup",
        "process_wakeup_id": delegation_id,
        "process_wakeup_payload": {
            "type": "async_delegation",
            "delegation_id": delegation_id,
            "session_key": sid,
            "summary": "full durable event payload",
        },
    }
    return routes, session, executions, kwargs


def test_production_crash_before_worker_runs_is_recovered_once_on_startup(
    monkeypatch, tmp_path
):
    from api import durable_delegation, models
    from api.process_wakeup_store import get_process_wakeup

    sid = "durable-prestart-crash"
    delegation_id = "deleg-prestart-crash"
    routes, session, executions, kwargs = _install_production_wakeup_route(
        monkeypatch, tmp_path, sid, delegation_id
    )
    real_thread = threading.Thread

    class CrashBeforeSchedulingThread:
        def __init__(self, *, target, args, kwargs, daemon):
            self.target = target

        def start(self):
            # Models a hard process exit after Thread construction/start return,
            # before the target gets a scheduling slice.
            return None

    monkeypatch.setattr(routes.threading, "Thread", CrashBeforeSchedulingThread)
    monkeypatch.setattr(routes, "_PROCESS_WAKEUP_WORKER_START_TIMEOUT_SECONDS", 0.01)
    response = routes._start_chat_stream_for_session(session, **kwargs)
    assert response["_status"] == 503
    pending = get_process_wakeup(delegation_id)
    assert pending is not None and pending.state == "pending"
    assert pending.payload["durable_event"]["summary"] == "full durable event payload"
    assert executions == []

    # New process: volatile stream/cache state is gone, while SQLite and the
    # session sidecar survive. Startup launches the persisted payload once.
    with routes.STREAMS_LOCK:
        routes.STREAMS.clear()
    models.SESSIONS.pop(sid, None)
    monkeypatch.setattr(routes.threading, "Thread", real_thread)
    assert durable_delegation.recover_pending_webui_process_wakeups() == 1
    assert _wait_until(
        lambda: bool(
            (record := get_process_wakeup(delegation_id))
            and record.state == "completed"
        )
    )
    assert len(executions) == 1
    assert durable_delegation.recover_pending_webui_process_wakeups() == 0
    assert len(executions) == 1


def test_production_thread_start_exception_leaves_recoverable_pending_work(
    monkeypatch, tmp_path
):
    from api import durable_delegation
    from api.process_wakeup_store import get_process_wakeup

    sid = "durable-thread-start-error"
    delegation_id = "deleg-thread-start-error"
    routes, session, executions, kwargs = _install_production_wakeup_route(
        monkeypatch, tmp_path, sid, delegation_id
    )
    real_thread = threading.Thread

    class FailingStartThread:
        def __init__(self, *, target, args, kwargs, daemon):
            pass

        def start(self):
            raise RuntimeError("thread scheduler unavailable")

    monkeypatch.setattr(routes.threading, "Thread", FailingStartThread)
    response = routes._start_chat_stream_for_session(session, **kwargs)
    assert response["_status"] == 503
    pending = get_process_wakeup(delegation_id)
    assert pending is not None and pending.state == "pending"
    assert pending.stream_id == ""
    assert executions == []

    monkeypatch.setattr(routes.threading, "Thread", real_thread)
    assert durable_delegation.recover_pending_webui_process_wakeups() == 1
    assert _wait_until(lambda: len(executions) == 1)
    assert _wait_until(
        lambda: bool(
            (record := get_process_wakeup(delegation_id))
            and record.state == "completed"
        )
    )
    completed = get_process_wakeup(delegation_id)
    assert completed is not None and completed.state == "completed"


def test_process_wakeup_forces_webui_local_path_without_runner_journal(
    monkeypatch, tmp_path
):
    from api.process_wakeup_store import get_process_wakeup

    sid = "durable-force-local"
    delegation_id = "deleg-force-local"
    routes, session, executions, kwargs = _install_production_wakeup_route(
        monkeypatch, tmp_path, sid, delegation_id
    )
    monkeypatch.setattr("api.runtime_adapter.runtime_adapter_enabled", lambda: False)
    monkeypatch.setattr("api.runtime_adapter.runtime_adapter_runner_enabled", lambda: True)
    monkeypatch.setattr(
        routes,
        "_runtime_runner_client_factory",
        lambda: (_ for _ in ()).throw(AssertionError("runner must not be called")),
    )

    response = routes._start_run(
        session,
        route="start_session_turn",
        normalized_model=False,
        **kwargs,
    )
    assert response["process_wakeup_state"] == "started"
    assert _wait_until(lambda: len(executions) == 1)
    assert _wait_until(
        lambda: bool(
            (record := get_process_wakeup(delegation_id))
            and record.state == "completed"
        )
    )
    completed = get_process_wakeup(delegation_id)
    assert completed is not None and completed.state == "completed"


def test_real_startup_restores_named_profile_rows_and_dedupes_aliases(
    monkeypatch, tmp_path
):
    from api import background_process as bp, durable_delegation, profiles
    from tools.process_registry import process_registry

    root_home = tmp_path / "hermes"
    alpha_home = root_home / "profiles" / "alpha"
    beta_home = root_home / "profiles" / "beta"
    root_evt = _event("startup-root", "deleg-startup-root", restored=True)
    alpha_evt = _event("startup-alpha", "deleg-startup-alpha", restored=True)
    beta_evt = _event("startup-beta", "deleg-startup-beta", restored=True)
    _seed_durable_row(root_home, root_evt)
    _seed_durable_row(alpha_home, alpha_evt)
    _seed_durable_row(beta_home, beta_evt)

    homes = {
        "default": root_home,
        "renamed-root": root_home,
        "alpha": alpha_home,
        "beta": beta_home,
    }
    monkeypatch.setattr(profiles, "get_active_profile_name", lambda: "alpha")
    monkeypatch.setattr(
        profiles,
        "get_hermes_home_for_profile",
        lambda name: homes[name],
    )
    monkeypatch.setattr(
        profiles,
        "list_profiles_api",
        lambda: [
            {"name": "renamed-root", "path": str(root_home), "is_default": True},
            {"name": "alpha", "path": str(alpha_home)},
            {"name": "beta", "path": str(beta_home)},
        ],
    )

    class NonRunningThread:
        def __init__(self, *args, **kwargs):
            self.daemon = kwargs.get("daemon", False)
            self.started = False

        def is_alive(self):
            return False

        def start(self):
            self.started = True

    monkeypatch.setattr(bp.threading, "Thread", NonRunningThread)
    monkeypatch.setattr(bp, "_DRAIN_THREAD", None)
    local_recovery_calls = []
    monkeypatch.setattr(
        durable_delegation,
        "recover_pending_webui_process_wakeups",
        lambda: local_recovery_calls.append(True) or 0,
    )

    preserved = []
    while True:
        try:
            preserved.append(process_registry.completion_queue.get_nowait())
        except queue.Empty:
            break
    # Agent's singleton can already have restored the root DB. The real WebUI
    # startup path must keep one copy while discovering named homes and aliases.
    process_registry.completion_queue.put(dict(root_evt))
    try:
        assert bp.start_drain_thread() is True
        restored = []
        while True:
            try:
                restored.append(process_registry.completion_queue.get_nowait())
            except queue.Empty:
                break
        ids = [item.get("delegation_id") for item in restored]
        assert ids.count("deleg-startup-root") == 1
        assert ids.count("deleg-startup-alpha") == 1
        assert ids.count("deleg-startup-beta") == 1
        assert local_recovery_calls == [True]
    finally:
        while True:
            try:
                process_registry.completion_queue.get_nowait()
            except queue.Empty:
                break
        for item in preserved:
            process_registry.completion_queue.put(item)


def test_wakeup_id_replay_rejects_different_request_before_acceptance_shortcut(
    monkeypatch, tmp_path
):
    from api.process_wakeup_store import get_process_wakeup

    sid = "durable-replay-conflict"
    delegation_id = "deleg-replay-conflict"
    routes, session, executions, kwargs = _install_production_wakeup_route(
        monkeypatch, tmp_path, sid, delegation_id
    )
    response = routes._start_chat_stream_for_session(session, **kwargs)
    assert response.get("stream_id")
    assert _wait_until(lambda: len(executions) == 1)
    assert _wait_until(
        lambda: bool(
            (record := get_process_wakeup(delegation_id))
            and record.state == "completed"
        )
    )

    conflicting_event = dict(kwargs["process_wakeup_payload"])
    conflicting_event["summary"] = "different delegated result"
    direct_kwargs = dict(kwargs)
    direct_kwargs["process_wakeup_payload"] = conflicting_event
    direct = routes._start_chat_stream_for_session(session, **direct_kwargs)
    assert direct["_status"] == 409
    assert "conflicts" in direct["error"]

    entrypoint = routes.start_session_turn(
        sid,
        kwargs["msg"],
        process_wakeup_id=delegation_id,
        process_wakeup_payload=conflicting_event,
    )
    assert entrypoint["_status"] == 409
    assert "conflicts" in entrypoint["error"]
    assert len(executions) == 1
