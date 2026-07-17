"""Regression coverage for #3929 restart-time partial-work recovery."""

import json
import time

import pytest

import api.models as models
from api.models import (
    Session,
    _append_journaled_partial_output,
    _apply_core_sync_or_error_marker,
)
from api.run_journal import append_run_event
from api.streaming import _sanitize_messages_for_api


@pytest.fixture(autouse=True)
def _isolate_session_state(tmp_path, monkeypatch):
    session_dir = tmp_path / "sessions"
    session_dir.mkdir()
    monkeypatch.setattr(models, "SESSION_DIR", session_dir)
    monkeypatch.setattr(models, "SESSION_INDEX_FILE", session_dir / "_index.json")
    models.SESSIONS.clear()
    yield
    models.SESSIONS.clear()


def test_restart_recovery_preserves_display_reasoning_without_provider_replay(
    tmp_path,
):
    """A dead live turn must recover all user-visible work from its journal.

    Display-only reasoning belongs in the settled Worklog after reload, but it
    must not leak into the model-facing history used by the next turn.
    """
    session_id = "issue3929_restart"
    stream_id = "stream_partial_work"
    previous_messages = [
        {"role": "user", "content": "Inspect the failure"},
        {"role": "assistant", "content": "I will trace it."},
    ]
    session = Session(
        session_id=session_id,
        title="Partial work recovery",
        messages=[dict(message) for message in previous_messages],
        context_messages=[dict(message) for message in previous_messages],
        pending_user_message="Continue until the root cause is clear",
        pending_started_at=time.time() - 120,
        active_stream_id=stream_id,
    )

    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "Checking the first failure boundary."},
    )
    append_run_event(
        session_id,
        stream_id,
        "token",
        {"text": "The first boundary is the live stream."},
    )
    append_run_event(
        session_id,
        stream_id,
        "tool",
        {
            "name": "terminal",
            "preview": "rg STREAM_PARTIAL_TEXT api/streaming.py",
            "args": {"command": "rg STREAM_PARTIAL_TEXT api/streaming.py"},
        },
    )
    append_run_event(
        session_id,
        stream_id,
        "tool_complete",
        {"name": "terminal", "duration": 0.4, "is_error": False},
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "Comparing the journal with the session sidecar."},
    )
    append_run_event(
        session_id,
        stream_id,
        "token",
        {"text": "The journal still has the emitted work."},
    )

    core_path = tmp_path / "missing-core-transcript.json"
    assert _apply_core_sync_or_error_marker(
        session,
        core_path,
        stream_id_for_recheck=stream_id,
    ) is True

    models.SESSIONS.clear()
    reloaded = models.get_session(session_id)
    recovered = [
        message for message in reloaded.messages
        if message.get("_recovered_from_run_journal")
    ]

    assert [message.get("content") for message in recovered] == [
        "The first boundary is the live stream.",
        "The journal still has the emitted work.",
    ]
    assert [message.get("reasoning") for message in recovered] == [
        "Checking the first failure boundary.",
        "Comparing the journal with the session sidecar.",
    ]
    assert reloaded.tool_calls[0]["name"] == "terminal"
    assert reloaded.tool_calls[0]["done"] is True
    assert any(
        message.get("_error")
        and message.get("type") == "interrupted"
        and "partial output above was recovered" in message.get("content", "")
        for message in reloaded.messages
    )

    serialized_context = json.dumps(
        reloaded.context_messages, ensure_ascii=False,
    )
    assert "Checking the first failure boundary" not in serialized_context
    assert "Comparing the journal with the session sidecar" not in serialized_context
    sanitized = _sanitize_messages_for_api(reloaded.context_messages)
    assert all("reasoning" not in message for message in sanitized)


def test_core_sync_keeps_pending_owner_for_reasoning_only_partial(tmp_path):
    """Reasoning-only live work still proves the pending user turn had activity."""
    session_id = "issue3929_reasoning_only"
    stream_id = "stream_reasoning_only"
    session = Session(
        session_id=session_id,
        title="Reasoning-only recovery",
        messages=[],
        context_messages=[
            {"role": "user", "content": "Earlier question"},
            {"role": "assistant", "content": "Earlier answer"},
        ],
        pending_user_message="Continue the investigation",
        pending_started_at=time.time() - 90,
        active_stream_id=stream_id,
    )
    core_path = tmp_path / "core-transcript.json"
    core_path.write_text(json.dumps({
        "messages": [
            {"role": "user", "content": "Earlier question"},
            {"role": "assistant", "content": "Earlier answer"},
        ],
    }), encoding="utf-8")
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "The worker reached a visible Thinking step before restart."},
    )

    assert _apply_core_sync_or_error_marker(
        session,
        core_path,
        stream_id_for_recheck=stream_id,
    ) is True

    recovered_users = [message for message in session.messages if message.get("_recovered")]
    assert [message.get("content") for message in recovered_users] == [
        "Continue the investigation",
    ]
    recovered_reasoning = [
        message for message in session.messages
        if message.get("_recovered_from_run_journal") and message.get("reasoning")
    ]
    assert [message.get("reasoning") for message in recovered_reasoning] == [
        "The worker reached a visible Thinking step before restart.",
    ]
    assert any(
        message.get("_error")
        and "partial output above was recovered" in message.get("content", "")
        for message in session.messages
    )
    assert not any(
        "visible Thinking step" in json.dumps(message, ensure_ascii=False)
        for message in session.context_messages
    )


def test_reasoning_backfill_is_idempotent_for_existing_recovered_text():
    session_id = "issue3929_reasoning_dedupe"
    stream_id = "stream_reasoning_dedupe"
    recovered_text = "The visible partial was already restored."
    session = Session(
        session_id=session_id,
        title="Reasoning backfill",
        messages=[
            {"role": "user", "content": "Continue"},
            {"role": "assistant", "content": recovered_text},
        ],
        context_messages=[
            {"role": "user", "content": "Continue"},
            {"role": "assistant", "content": recovered_text},
        ],
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "Backfill this Thinking row once."},
    )
    append_run_event(session_id, stream_id, "token", {"text": recovered_text})

    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is True
    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is False

    matching = [
        message for message in session.messages
        if message.get("role") == "assistant" and message.get("content") == recovered_text
    ]
    assert len(matching) == 1
    assert matching[0].get("reasoning") == "Backfill this Thinking row once."
    assert "Backfill this Thinking row once" not in json.dumps(
        session.context_messages, ensure_ascii=False,
    )


def test_reasoning_backfill_does_not_claim_matching_content_from_prior_turn():
    session_id = "issue3929_reasoning_current_turn_scope"
    stream_id = "stream_reasoning_current_turn_scope"
    repeated_text = "The same sufficiently long partial output appears again."
    prior_assistant = {"role": "assistant", "content": repeated_text}
    session = Session(
        session_id=session_id,
        title="Current-turn reasoning scope",
        messages=[
            {"role": "user", "content": "Earlier request"},
            prior_assistant,
            {"role": "user", "content": "Current request"},
        ],
        context_messages=[
            {"role": "user", "content": "Earlier request"},
            {"role": "assistant", "content": repeated_text},
            {"role": "user", "content": "Current request"},
        ],
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "This Thinking belongs only to the current request."},
    )
    append_run_event(session_id, stream_id, "token", {"text": repeated_text})

    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is True

    assert prior_assistant.get("reasoning") is None
    current_rows = [
        message for message in session.messages[3:]
        if message.get("content") == repeated_text
    ]
    assert len(current_rows) == 1
    assert current_rows[0].get("reasoning") == (
        "This Thinking belongs only to the current request."
    )
    assert "This Thinking belongs only to the current request" not in json.dumps(
        session.context_messages, ensure_ascii=False,
    )


def test_reasoning_backfill_accepts_core_row_before_recovered_owner_echo():
    session_id = "issue3929_reasoning_core_owner_echo"
    stream_id = "stream_reasoning_core_owner_echo"
    pending_text = "Current request"
    recovered_text = "The core transcript already contains this partial output."
    core_assistant = {"role": "assistant", "content": recovered_text}
    session = Session(
        session_id=session_id,
        title="Core owner echo",
        messages=[
            {"role": "user", "content": pending_text},
            core_assistant,
            {"role": "user", "content": pending_text, "_recovered": True},
        ],
        context_messages=[
            {"role": "user", "content": pending_text},
            {"role": "assistant", "content": recovered_text},
        ],
        pending_user_message=pending_text,
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "Attach this Thinking to the current core row."},
    )
    append_run_event(session_id, stream_id, "token", {"text": recovered_text})

    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is True

    assert core_assistant.get("reasoning") == (
        "Attach this Thinking to the current core row."
    )
    matching_rows = [
        message for message in session.messages
        if message.get("content") == recovered_text
    ]
    assert len(matching_rows) == 1
    assert "Attach this Thinking to the current core row" not in json.dumps(
        session.context_messages, ensure_ascii=False,
    )


def test_reasoning_only_recovery_is_idempotent_across_replays():
    session_id = "issue3929_reasoning_only_dedupe"
    stream_id = "stream_reasoning_only_dedupe"
    session = Session(
        session_id=session_id,
        title="Reasoning-only replay",
        messages=[{"role": "user", "content": "Continue"}],
        context_messages=[{"role": "user", "content": "Continue"}],
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "Restore this Thinking row once."},
    )

    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is True
    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is False

    reasoning_rows = [
        message for message in session.messages
        if message.get("reasoning") == "Restore this Thinking row once."
    ]
    assert len(reasoning_rows) == 1


def test_retry_growth_attaches_later_tool_to_reasoning_segment():
    session_id = "issue3929_reasoning_then_tool_growth"
    stream_id = "stream_reasoning_then_tool_growth"
    session = Session(
        session_id=session_id,
        title="Reasoning then tool growth",
        messages=[{"role": "user", "content": "Keep checking"}],
        context_messages=[{"role": "user", "content": "Keep checking"}],
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "Inspect the first boundary."},
    )

    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is True
    reasoning_idx = next(
        idx for idx, message in enumerate(session.messages)
        if message.get("reasoning") == "Inspect the first boundary."
    )

    append_run_event(
        session_id,
        stream_id,
        "tool",
        {"name": "terminal", "preview": "inspect the next boundary"},
    )

    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is True
    reasoning_rows = [
        message for message in session.messages
        if message.get("reasoning") == "Inspect the first boundary."
    ]
    assert len(reasoning_rows) == 1
    assert session.tool_calls[0]["assistant_msg_idx"] == reasoning_idx


def test_identical_reasoning_segments_around_tool_remain_distinct():
    session_id = "issue3929_identical_reasoning_segments"
    stream_id = "stream_identical_reasoning_segments"
    repeated_reasoning = "Checking the same condition again."
    session = Session(
        session_id=session_id,
        title="Repeated Thinking segments",
        messages=[{"role": "user", "content": "Check twice"}],
        context_messages=[{"role": "user", "content": "Check twice"}],
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": repeated_reasoning},
    )
    append_run_event(
        session_id,
        stream_id,
        "tool",
        {"name": "terminal", "preview": "first check"},
    )
    append_run_event(
        session_id,
        stream_id,
        "tool_complete",
        {"name": "terminal", "is_error": False},
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": repeated_reasoning},
    )

    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is True

    reasoning_rows = [
        (idx, message)
        for idx, message in enumerate(session.messages)
        if message.get("reasoning") == repeated_reasoning
    ]
    assert len(reasoning_rows) == 2
    assert reasoning_rows[0][0] != reasoning_rows[1][0]
    assert session.tool_calls[0]["assistant_msg_idx"] == reasoning_rows[0][0]


def test_identical_content_segments_claim_distinct_rows_on_replay():
    session_id = "issue3929_identical_content_segments"
    stream_id = "stream_identical_content_segments"
    repeated_content = "The same sufficiently long process update appears twice."
    session = Session(
        session_id=session_id,
        title="Repeated process segments",
        messages=[{"role": "user", "content": "Check twice"}],
        context_messages=[{"role": "user", "content": "Check twice"}],
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "First Thinking segment."},
    )
    append_run_event(
        session_id,
        stream_id,
        "token",
        {"text": repeated_content},
    )
    append_run_event(
        session_id,
        stream_id,
        "tool",
        {"name": "terminal", "preview": "separate the segments"},
    )
    append_run_event(
        session_id,
        stream_id,
        "tool_complete",
        {"name": "terminal", "is_error": False},
    )
    append_run_event(
        session_id,
        stream_id,
        "reasoning",
        {"text": "Second Thinking segment."},
    )
    append_run_event(
        session_id,
        stream_id,
        "token",
        {"text": repeated_content},
    )

    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is True
    assert _append_journaled_partial_output(
        session, stream_id, dedupe_existing=True,
    ) is False

    matching_rows = [
        message for message in session.messages
        if message.get("content") == repeated_content
    ]
    assert [message.get("reasoning") for message in matching_rows] == [
        "First Thinking segment.",
        "Second Thinking segment.",
    ]
