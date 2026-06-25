"""Regression coverage for Kosta WebUI source visibility defaults."""

from __future__ import annotations


def test_default_session_list_hides_messaging_and_cron(monkeypatch):
    import api.routes as routes

    webui_rows = [
        {
            "session_id": "webui-1",
            "source": "webui",
            "session_source": "webui",
            "message_count": 1,
            "last_message_at": 50,
            "updated_at": 50,
        }
    ]
    agent_rows = [
        {
            "session_id": "cli-1",
            "source": "cli",
            "raw_source": "cli",
            "message_count": 1,
            "actual_message_count": 1,
            "actual_user_message_count": 1,
            "title": "Useful CLI task",
            "last_message_at": 40,
            "updated_at": 40,
        },
        {
            "session_id": "telegram-1",
            "source": "telegram",
            "raw_source": "telegram",
            "message_count": 1,
            "actual_message_count": 1,
            "last_message_at": 30,
            "updated_at": 30,
        },
        {
            "session_id": "imessage-1",
            "source": "bluebubbles",
            "raw_source": "bluebubbles",
            "message_count": 1,
            "actual_message_count": 1,
            "last_message_at": 20,
            "updated_at": 20,
        },
        {
            "session_id": "cron-1",
            "source": "cron",
            "raw_source": "cron",
            "message_count": 1,
            "actual_message_count": 1,
            "last_message_at": 10,
            "updated_at": 10,
        },
    ]

    monkeypatch.setattr(routes, "all_sessions", lambda **_kwargs: webui_rows)
    monkeypatch.setattr(routes, "get_cli_sessions", lambda **_kwargs: agent_rows)
    monkeypatch.setattr(routes, "agent_session_rows_existing", lambda ids, profile=None: set(ids))
    monkeypatch.setattr(routes, "_load_gateway_session_identity_map", lambda: {})
    monkeypatch.setattr(routes, "_is_isolated_profile_mode", lambda: True)
    monkeypatch.setattr(routes, "_profiles_match", lambda session_profile, active_profile: True)
    monkeypatch.setattr(routes, "_enrich_sidebar_lineage_metadata", lambda rows: None)

    payload = routes._build_session_list_cache_payload(
        active_profile="default",
        all_profiles=False,
        show_cli_sessions=True,
        show_messaging_sessions=False,
        show_previous_messaging_sessions=False,
        show_cron_sessions=False,
        visible_only=True,
    )

    assert [row["session_id"] for row in payload["sessions"]] == ["webui-1", "cli-1"]


def test_source_classifier_treats_bluebubbles_and_webhook_as_messaging():
    from api.agent_sessions import MESSAGING_SOURCES, normalize_agent_session_source

    assert "bluebubbles" in MESSAGING_SOURCES
    assert "webhook" in MESSAGING_SOURCES
    assert normalize_agent_session_source("bluebubbles")["session_source"] == "messaging"
    assert normalize_agent_session_source("webhook")["session_source"] == "messaging"
