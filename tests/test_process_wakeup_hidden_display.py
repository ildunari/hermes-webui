"""Regression coverage for the durable process-wakeup transcript contract.

Process wakeups are synthetic user turns, not hidden model-only scaffolding.  The
backend must persist and return them with ``_source: process_wakeup`` so they
remain a chronological turn boundary.  The frontend renders persisted wakeups
as compact status rows rather than human-authored bubbles, while still hiding a
pending wakeup placeholder to avoid showing the same in-flight turn twice.
"""

from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
UI_JS = ROOT / "static" / "ui.js"


def test_process_wakeup_user_prompt_persists_as_synthetic_display_boundary():
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

    assert [m["role"] for m in merged] == ["user", "assistant", "user", "assistant"]
    assert merged[-2]["content"] == wakeup
    assert merged[-2]["_source"] == "process_wakeup"
    assert merged[-1]["content"] == "I saw the subagent result and handled it."


def test_process_wakeup_context_backfill_restores_synthetic_turn_in_order():
    from api.streaming import _merge_display_messages_after_agent_result

    visible = [
        {"role": "user", "content": "start"},
        {"role": "assistant", "content": "ok"},
    ]
    wakeup = {
        "role": "user",
        "content": "[IMPORTANT: Background process proc_1 completed (exit_code=0).",
        "_source": "process_wakeup",
    }
    previous_context = visible + [wakeup]
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

    assert merged[2] == wakeup
    assert [m["content"] for m in merged[-2:]] == ["next normal turn", "normal answer"]


def test_api_message_window_keeps_existing_process_wakeup_rows_visible():
    from api.routes import _message_window_for_display

    wakeup = {
        "role": "user",
        "_source": "process_wakeup",
        "content": "[IMPORTANT: Background process proc_3 completed (exit_code=1).",
    }
    messages = [
        {"role": "assistant", "content": "before"},
        wakeup,
        {"role": "assistant", "content": "after"},
    ]

    window, offset = _message_window_for_display(messages)

    assert window == messages
    assert window[1] is wakeup
    assert offset == 0


def test_process_wakeup_pending_prompt_materializes_on_error_with_source():
    from api.streaming import _materialize_pending_user_turn_before_error

    session = SimpleNamespace(
        pending_user_message="[ASYNC DELEGATION COMPLETE — deleg_test]\nresult",
        pending_user_source="process_wakeup",
        pending_started_at=123.0,
        pending_attachments=[],
        messages=[],
        context_messages=[],
    )

    assert _materialize_pending_user_turn_before_error(session) is True
    assert session.messages == [
        {
            "role": "user",
            "content": "[ASYNC DELEGATION COMPLETE — deleg_test]\nresult",
            "timestamp": 123,
            "_recovered": True,
            "_source": "process_wakeup",
        }
    ]


def test_frontend_pending_guard_recognizes_legacy_watch_overflow_prompts():
    source = UI_JS.read_text(encoding="utf-8")
    helper_idx = source.find("function _isInternalWakeupPendingSession")
    pending_idx = source.find("function getPendingSessionMessage")

    assert helper_idx != -1
    assert pending_idx != -1
    assert helper_idx < pending_idx
    helper = source[helper_idx:pending_idx]
    assert "[IMPORTANT: Watch-pattern" in helper
    assert "[IMPORTANT: Watch patterns" in helper


def test_api_pending_process_wakeup_metadata_and_active_stream_are_preserved(monkeypatch):
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
        title="Existing title",
        messages=[],
        save=lambda: saved.append(True),
    )
    monkeypatch.setattr(routes, "get_webui_session_save_mode", lambda: "deferred")
    wakeup = "[ASYNC DELEGATION BATCH COMPLETE — deleg_x]\nresult"

    routes._prepare_chat_start_session_for_stream(
        session,
        msg=wakeup,
        attachments=[],
        workspace="/tmp",
        model="gpt-test",
        model_provider="test",
        stream_id="stream_1",
        started_at=123.0,
        source="process_wakeup",
    )

    assert session.active_stream_id == "stream_1"
    assert session.pending_user_message == wakeup
    assert session.pending_user_source == "process_wakeup"
    assert saved == [True]


def test_process_wakeup_uses_normal_provisional_title_path_and_eager_checkpoint(monkeypatch):
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
        truncation_watermark=None,
        save=lambda: saved.append(True),
    )
    monkeypatch.setattr(routes, "get_webui_session_save_mode", lambda: "eager")
    wakeup = "[IMPORTANT: Background process proc_x completed (exit_code=0).\nOutput:\nOK]"

    routes._prepare_chat_start_session_for_stream(
        session,
        msg=wakeup,
        attachments=[],
        workspace="/tmp",
        model="gpt-test",
        model_provider="test",
        stream_id="stream_x",
        started_at=123.0,
        source="process_wakeup",
    )

    assert session.title != "Untitled"
    assert session.messages[0]["content"] == wakeup
    assert session.messages[0]["_source"] == "process_wakeup"
    assert saved == [True]


def test_process_wakeup_counts_as_renderable_in_limited_api_tail():
    from api.routes import _message_window_for_display

    messages = [
        {"role": "user", "content": "u1"},
        {"role": "assistant", "content": "a1"},
        {"role": "user", "_source": "process_wakeup", "content": "[ASYNC DELEGATION COMPLETE — deleg_x]"},
        {"role": "assistant", "content": "a2"},
        {"role": "user", "content": "u3"},
    ]

    window, offset = _message_window_for_display(messages, msg_limit=4)

    assert [m["content"] for m in window] == ["a1", "[ASYNC DELEGATION COMPLETE — deleg_x]", "a2", "u3"]
    assert window[1]["_source"] == "process_wakeup"
    assert offset == 1


def test_imported_existing_session_echo_keeps_synthetic_wakeup_boundary():
    from api.routes import _message_window_for_display

    existing_messages = [
        {"role": "user", "content": "real"},
        {
            "role": "user",
            "_source": "process_wakeup",
            "content": "[IMPORTANT: Background process proc_x completed]",
        },
    ]

    window, offset = _message_window_for_display(existing_messages)

    assert window == existing_messages
    assert window[-1]["_source"] == "process_wakeup"
    assert offset == 0


def test_frontend_pending_safeguard_hides_duplicate_but_persisted_row_is_compact():
    source = UI_JS.read_text(encoding="utf-8")
    helper_idx = source.find("function _isInternalWakeupPendingSession")
    pending_idx = source.find("function getPendingSessionMessage")

    assert helper_idx != -1
    assert pending_idx != -1
    assert helper_idx < pending_idx
    pending_snippet = source[pending_idx:pending_idx + 500]
    assert "_isInternalWakeupPendingSession(session,text)" in pending_snippet
    assert "return null" in pending_snippet
    assert "process_wakeup" in source[helper_idx:pending_idx]
    assert "[ASYNC DELEGATION" in source[helper_idx:pending_idx]

    render_marker = source.find("const isProcessWakeup=")
    process_branch = source.find("if(isProcessWakeup)", render_marker)
    human_branch = source.find("if(isUser)", render_marker)
    assert render_marker != -1
    assert process_branch != -1
    assert human_branch != -1
    assert process_branch < human_branch
    assert "process-wakeup-row" in source[process_branch:human_branch]
