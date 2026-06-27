from types import SimpleNamespace


def test_process_wakeup_user_prompt_hidden_from_display_merge():
    from api.streaming import _merge_display_messages_after_agent_result

    previous = [
        {"role": "user", "content": "start"},
        {"role": "assistant", "content": "ok"},
    ]
    wakeup = "[ASYNC DELEGATION COMPLETE — deleg_test]\n--- RESULT ---\nfinished"
    result_messages = previous + [
        {"role": "user", "content": wakeup},
        {"role": "assistant", "content": "I saw the subagent result and handled it."},
    ]

    merged = _merge_display_messages_after_agent_result(
        previous,
        previous,
        result_messages,
        wakeup,
        source="process_wakeup",
    )

    assert [m["role"] for m in merged] == ["user", "assistant", "assistant"]
    assert all("ASYNC DELEGATION" not in str(m.get("content", "")) for m in merged)
    assert merged[-1]["content"] == "I saw the subagent result and handled it."


def test_process_wakeup_context_backfill_does_not_surface_internal_user_row():
    from api.streaming import _merge_display_messages_after_agent_result

    visible = [
        {"role": "user", "content": "start"},
        {"role": "assistant", "content": "ok"},
    ]
    hidden_wakeup = {
        "role": "user",
        "content": "[IMPORTANT: Background process proc_1 completed (exit_code=0).",
        "_source": "process_wakeup",
    }
    previous_context = visible + [hidden_wakeup]
    result_messages = previous_context + [
        {"role": "user", "content": "next normal turn"},
        {"role": "assistant", "content": "normal answer"},
    ]

    merged = _merge_display_messages_after_agent_result(
        visible,
        previous_context,
        result_messages,
        "next normal turn",
        source="webui",
    )

    assert all("Background process" not in str(m.get("content", "")) for m in merged)
    assert [m["content"] for m in merged[-2:]] == ["next normal turn", "normal answer"]



def test_api_visible_messages_filter_existing_process_wakeup_rows():
    from api.routes import _visible_messages_for_client

    leaked = {
        "role": "user",
        "_source": "process_wakeup",
        "content": "[IMPORTANT: Background process proc_3e2cfd521406 completed (exit_code=1).\nCommand: ssh ...",
    }
    visible = [
        {"role": "assistant", "content": "Same stale Cairo failure, already bypassed."},
        leaked,
        {"role": "assistant", "content": "I handled the background result."},
    ]

    filtered = _visible_messages_for_client(visible)

    assert leaked not in filtered
    assert [m["content"] for m in filtered] == [
        "Same stale Cairo failure, already bypassed.",
        "I handled the background result.",
    ]


def test_process_wakeup_pending_prompt_not_materialized_on_error():
    from api.streaming import _materialize_pending_user_turn_before_error

    session = SimpleNamespace(
        pending_user_message="[ASYNC DELEGATION COMPLETE — deleg_test]\nresult",
        pending_user_source="process_wakeup",
        pending_started_at=123.0,
        pending_attachments=[],
        messages=[],
        context_messages=[],
    )

    assert _materialize_pending_user_turn_before_error(session) is False
    assert session.messages == []


def test_api_visible_messages_filter_legacy_watch_overflow_rows():
    from api.routes import _visible_messages_for_client

    rows = [
        {"role": "assistant", "content": "before"},
        {"role": "user", "content": "[IMPORTANT: Watch-pattern overflow: >3 notifications in 15s]"},
        {"role": "user", "content": "[IMPORTANT: Watch patterns disabled for process proc_x]"},
        {"role": "assistant", "content": "after"},
    ]

    filtered = _visible_messages_for_client(rows)

    assert [m["content"] for m in filtered] == ["before", "after"]


def test_api_pending_process_wakeup_hidden_but_active_stream_preserved():
    from api.routes import _pending_user_message_for_client, _pending_user_source_for_client

    session = SimpleNamespace(
        active_stream_id="stream_1",
        pending_user_message="[ASYNC DELEGATION BATCH COMPLETE — deleg_x]\nresult",
        pending_user_source="process_wakeup",
    )

    assert session.active_stream_id == "stream_1"
    assert _pending_user_message_for_client(session) is None
    assert _pending_user_source_for_client(session) is None


def test_process_wakeup_does_not_set_provisional_title_for_untitled_session(monkeypatch):
    import api.routes as routes

    saved = []
    session = SimpleNamespace(
        workspace=None,
        model=None,
        model_provider=None,
        active_stream_id=None,
        pending_user_message=None,
        pending_attachments=None,
        pending_started_at=None,
        pending_user_source=None,
        title="Untitled",
        messages=[],
        save=lambda: saved.append(True),
    )
    monkeypatch.setattr(routes, "get_webui_session_save_mode", lambda: "eager")

    routes._prepare_chat_start_session_for_stream(
        session,
        msg="[IMPORTANT: Background process proc_x completed (exit_code=0).\nOutput:\nOK]",
        attachments=[],
        workspace="/tmp",
        model="gpt-test",
        model_provider="test",
        stream_id="stream_x",
        started_at=123.0,
        source="process_wakeup",
    )

    assert session.title == "Untitled"
    assert session.messages == []
    assert saved == [True]


def test_visible_filter_before_window_does_not_underfill_limited_tail():
    from api.routes import _message_window_for_display, _visible_messages_for_client

    raw = [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "_source": "process_wakeup", "content": "[ASYNC DELEGATION COMPLETE — deleg_x]"},
        {"role": "assistant", "content": "a2"},
        {"role": "user", "content": "u3"},
    ]

    display = _visible_messages_for_client(raw)
    window, offset = _message_window_for_display(display, msg_limit=4)

    assert [m["content"] for m in window] == ["u1", "a1", "a2", "u3"]
    assert offset == 0




def test_cli_import_existing_session_echo_filters_internal_wakeup_rows():
    from api.routes import _visible_messages_for_client

    existing_messages = [
        {"role": "user", "content": "real"},
        {"role": "user", "_source": "process_wakeup", "content": "[IMPORTANT: Background process proc_x completed]"},
    ]

    assert _visible_messages_for_client(existing_messages) == [{"role": "user", "content": "real"}]


def test_frontend_pending_session_message_hides_internal_wakeup_prompt():
    from pathlib import Path

    source = Path("static/ui.js").read_text()
    helper_idx = source.find("function _isInternalWakeupPendingSession")
    pending_idx = source.find("function getPendingSessionMessage")

    assert helper_idx != -1
    assert pending_idx != -1
    assert helper_idx < pending_idx
    snippet = source[pending_idx:pending_idx + 500]
    assert "_isInternalWakeupPendingSession(session,text)" in snippet
    assert "return null" in snippet
    assert "process_wakeup" in source[helper_idx:helper_idx + 500]
    assert "[ASYNC DELEGATION" in source[helper_idx:helper_idx + 500]
