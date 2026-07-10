"""Internal agent sessions stay out of ordinary sidebar projections.

Direct session/detail and source-filtered diagnostic paths remain separate from this
sidebar-only policy, and branch/compression lineage sources remain visible.
"""

import api.models as models
import api.routes as routes


INTERNAL_SOURCES = ("subagent", "tool", "smoke-test")


def _row(source: str, sid: str | None = None) -> dict:
    return {
        "session_id": sid or f"{source}-session",
        "title": f"{source} session",
        "source": source,
        "raw_source": source,
        "source_tag": source,
        "session_source": "other",
        "message_count": 2,
        "actual_message_count": 2,
    }


def test_internal_sources_are_hidden_from_default_sidebar_policy():
    for source in INTERNAL_SOURCES:
        assert models._hide_from_default_sidebar(_row(source)) is True
        conflicting = {**_row("webui", f"stale-{source}"), "raw_source": source}
        assert models._hide_from_default_sidebar(conflicting) is True
        assert routes._is_internal_agent_execution_row(conflicting) is True


def test_branch_and_compression_lineage_sources_remain_visible():
    for source in ("webui", "fork", "cli", "tui"):
        row = _row(source)
        row["session_source"] = "fork" if source == "fork" else source
        assert models._hide_from_default_sidebar(row) is False


def test_internal_source_diagnostic_filter_bypasses_only_requested_source():
    all_rows = [_row(source) for source in INTERNAL_SOURCES]
    for source in INTERNAL_SOURCES:
        rows = routes._dedupe_cli_sidebar_sessions_for_api(
            all_rows,
            set(),
            diagnostic_source_filter=source,
        )
        assert [row["session_id"] for row in rows] == [f"{source}-session"]


def test_non_diagnostic_sidebar_projection_drops_internal_sources():
    rows = routes._dedupe_cli_sidebar_sessions_for_api(
        [_row(source) for source in INTERNAL_SOURCES],
        set(),
    )
    assert rows == []


def test_session_list_builder_hides_sidecar_internal_rows_but_keeps_lineage(monkeypatch):
    rows = [_row(source) for source in INTERNAL_SOURCES]
    rows += [
        _row("webui", "normal-webui"),
        {**_row("webui", "compression-tip"), "parent_session_id": "normal-webui", "_lineage_root_id": "normal-webui"},
        {**_row("fork", "branch-child"), "session_source": "fork", "parent_session_id": "normal-webui"},
    ]
    monkeypatch.setattr(routes, "all_sessions", lambda **_kwargs: list(rows))
    monkeypatch.setattr(routes, "_reconcile_stale_stream_state_for_session_rows", lambda _rows: False)
    monkeypatch.setattr(routes, "_prune_orphaned_webui_zero_message_sessions", lambda value, **_kwargs: value)
    monkeypatch.setattr(routes, "_enrich_sidebar_lineage_metadata", lambda _rows: None)

    payload = routes._build_session_list_cache_payload(
        active_profile="default",
        all_profiles=True,
        show_cli_sessions=False,
        show_previous_messaging_sessions=True,
        show_cron_sessions=False,
        visible_only=True,
    )

    assert {row["session_id"] for row in payload["sessions"]} == {
        "normal-webui",
        "compression-tip",
        "branch-child",
    }


def test_session_list_builder_explicit_internal_source_filter_is_diagnostic(monkeypatch):
    rows = [_row(source) for source in INTERNAL_SOURCES]
    rows.append(_row("webui", "normal-webui"))
    monkeypatch.setattr(routes, "all_sessions", lambda **_kwargs: list(rows))
    monkeypatch.setattr(routes, "_reconcile_stale_stream_state_for_session_rows", lambda _rows: False)
    monkeypatch.setattr(routes, "_prune_orphaned_webui_zero_message_sessions", lambda value, **_kwargs: value)
    monkeypatch.setattr(routes, "_enrich_sidebar_lineage_metadata", lambda _rows: None)

    payload = routes._build_session_list_cache_payload(
        active_profile="default",
        all_profiles=True,
        show_cli_sessions=False,
        show_previous_messaging_sessions=True,
        show_cron_sessions=False,
        visible_only=True,
        source_filter="subagent",
    )

    assert {row["session_id"] for row in payload["sessions"]} == {"subagent-session"}
