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
