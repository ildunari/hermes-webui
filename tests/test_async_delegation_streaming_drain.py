import queue
import time


def test_streaming_next_turn_drain_hands_async_delegation_to_background_path(
    monkeypatch, tmp_path
):
    from api import models
    from api.streaming import _drain_webui_process_notifications
    pytest = __import__("pytest")
    pytest.importorskip("tools.process_registry", reason="hermes-agent not installed")
    from tools.process_registry import process_registry

    sid = "sess-streaming-async-delegation"
    deleg_id = "deleg-streaming-drain-1"
    session_dir = tmp_path / "sessions"
    session_dir.mkdir()
    monkeypatch.setattr(models, "SESSION_DIR", session_dir)
    monkeypatch.setattr(models, "SESSIONS", {})
    monkeypatch.setattr(models, "SESSION_META_CACHE", {}, raising=False)
    models.Session(session_id=sid, profile="default").save(skip_index=True)
    evt = {
        "type": "async_delegation",
        "delegation_id": deleg_id,
        "session_key": sid,
        "status": "completed",
        "goal": "streaming drain smoke",
        "summary": "DELEGATE_REENTRY_SMOKE_OK streaming-drain",
        "completed_at": time.time(),
    }

    with process_registry._lock:
        process_registry._completion_consumed.discard(deleg_id)
    process_registry.completion_queue.put(evt)
    try:
        assert _drain_webui_process_notifications(sid) == []
        queued_ids = []
        while True:
            try:
                queued_ids.append(
                    process_registry.completion_queue.get_nowait().get("delegation_id")
                )
            except queue.Empty:
                break
        assert deleg_id in queued_ids
        assert not process_registry.is_completion_consumed(deleg_id)
        with __import__("pytest").raises(queue.Empty):
            process_registry.completion_queue.get_nowait()
    finally:
        with process_registry._lock:
            process_registry._completion_consumed.discard(deleg_id)
        # Remove our event if an assertion failed before the drain consumed it.
        kept = []
        while True:
            try:
                item = process_registry.completion_queue.get_nowait()
            except Exception:
                break
            if not (isinstance(item, dict) and item.get("delegation_id") == deleg_id):
                kept.append(item)
        for item in kept:
            process_registry.completion_queue.put(item)
