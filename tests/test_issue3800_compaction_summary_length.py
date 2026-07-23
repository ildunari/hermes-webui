"""
Regression coverage for compaction summary normalization (#3800).
"""

import sys
import types

from api.routes import _handle_session_compress, get_session
from api.streaming import _compact_summary_text
from tests.test_sprint46 import (
    _FakeAgent,
    _FakeHandler,
    _install_fake_compression_runtime,
    _make_session,
)


def _install_manual_summary(monkeypatch, summary_text):
    # The route (local carry f594a4ca) no longer forwards the plugin's
    # ``reference_message`` verbatim — it always synthesizes its own
    # "[CONTEXT COMPACTION — REFERENCE ONLY] ..." string from
    # summary["headline"]/["token_line"]/["note"] plus a fixed trailer. Only a
    # non-blank ``headline`` actually survives into that synthesized string,
    # so route this test's summary text through "headline" (when non-blank)
    # to keep exercising the same normalization/length behavior the test was
    # written to cover.
    stripped = (summary_text or "").strip()
    payload = {"headline": summary_text} if stripped else {}
    agent_module = types.ModuleType("agent")
    agent_module.__path__ = []
    feedback_module = types.ModuleType("agent.manual_compression_feedback")
    feedback_module.summarize_manual_compression = (
        lambda original_messages, compressed_messages, before_count, after_count: dict(payload)
    )
    monkeypatch.setitem(sys.modules, "agent", agent_module)
    monkeypatch.setitem(sys.modules, "agent.manual_compression_feedback", feedback_module)


def _run_manual_compaction_summary(monkeypatch, cleanup_test_sessions, summary_text):
    sid = _make_session()
    cleanup_test_sessions.append(sid)

    _install_fake_compression_runtime(monkeypatch, _FakeAgent)
    _install_manual_summary(monkeypatch, summary_text)

    handler = _FakeHandler()
    _handle_session_compress(handler, {"session_id": sid})

    assert handler.status == 200
    payload = handler.payload()
    return payload["session"]["compression_anchor_summary"], get_session(sid)


def test_manual_and_streaming_compaction_preserve_long_summaries(
    monkeypatch, cleanup_test_sessions
):
    long_summary = ("Alpha summary line.\n" + "Long detail " * 40 + "tail").strip()

    route_summary, stored_session = _run_manual_compaction_summary(
        monkeypatch, cleanup_test_sessions, long_summary
    )

    assert route_summary is not None
    assert route_summary == stored_session.compression_anchor_summary
    # The synthesized reference message embeds the (whitespace-collapsed)
    # long headline verbatim rather than truncating it.
    assert _compact_summary_text(long_summary) in route_summary
    assert route_summary.startswith("[CONTEXT COMPACTION — REFERENCE ONLY]")
    assert route_summary.endswith("Compression completed.")
    assert len(route_summary) > 320


def test_manual_and_streaming_compaction_normalize_blank_summaries(
    monkeypatch, cleanup_test_sessions
):
    blank_summary = " \n\t  "

    route_summary, stored_session = _run_manual_compaction_summary(
        monkeypatch, cleanup_test_sessions, blank_summary
    )

    # A blank/whitespace-only plugin summary carries no usable headline, so
    # the route falls back to its default "Context compressed" headline
    # instead of surfacing an empty summary — the compaction card always
    # shows the synthesized token-accounting text now, never None.
    assert route_summary is not None
    assert route_summary == stored_session.compression_anchor_summary
    assert route_summary.startswith("[CONTEXT COMPACTION — REFERENCE ONLY] Context compressed")
    assert route_summary.endswith("Compression completed.")
