import time


def test_streaming_next_turn_drain_accepts_async_delegation_events():
    from api.streaming import _drain_webui_process_notifications
    pytest = __import__("pytest")
    pytest.importorskip("tools.process_registry", reason="hermes-agent not installed")
    from tools.process_registry import process_registry

    sid = "sess-streaming-async-delegation"
    deleg_id = "deleg-streaming-drain-1"
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
        notifications = _drain_webui_process_notifications(sid)
        assert len(notifications) == 1
        assert "ASYNC DELEGATION COMPLETE" in notifications[0]
        assert deleg_id in notifications[0]
        assert "DELEGATE_REENTRY_SMOKE_OK" in notifications[0]
        assert process_registry.is_completion_consumed(deleg_id)
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
