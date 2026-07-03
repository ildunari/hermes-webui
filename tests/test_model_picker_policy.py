import api.config as config


def test_model_picker_policy_hides_noise_and_collapses_custom_atomic():
    cfg = {
        "providers": {
            "atomic": {"name": "atomic"},
        },
        "model_picker": {
            "hidden_providers": [
                "anthropic",
                "google",
                "copilot",
                "copilot-acp",
                "x-ai",
                "studio",
            ],
            "visible_models": {
                "vibeproxy": ["claude-opus-4-8"],
                "zai": ["glm-5.2"],
            },
        },
    }
    groups = [
        {
            "provider": "VibeProxy",
            "provider_id": "vibeproxy",
            "models": [
                {"id": "claude-fable-5", "label": "claude-fable-5"},
                {"id": "claude-opus-4-8", "label": "claude-opus-4-8"},
            ],
        },
        {
            "provider": "atomic",
            "provider_id": "custom:atomic",
            "models": [
                {"id": "@custom:atomic:qwopus-atomic", "label": "Qwopus Atomic"},
            ],
        },
        {
            "provider": "Anthropic",
            "provider_id": "anthropic",
            "models": [{"id": "@anthropic:claude-opus-4-8", "label": "Claude Opus"}],
        },
        {
            "provider": "Gemini",
            "provider_id": "gemini",
            "models": [{"id": "@gemini:gemini-3.5-flash", "label": "Gemini"}],
        },
        {
            "provider": "xAI API",
            "provider_id": "xai",
            "models": [{"id": "@xai:grok-4.3", "label": "Grok"}],
        },
        {
            "provider": "zAI",
            "provider_id": "zai",
            "models": [
                {"id": "@zai:glm-5.2", "label": "GLM 5.2"},
                {"id": "@zai:glm-4.6", "label": "GLM 4.6"},
            ],
        },
    ]

    filtered = config._filter_model_picker_groups_by_policy(groups, cfg)

    provider_ids = [group["provider_id"] for group in filtered]
    assert provider_ids == ["vibeproxy", "atomic", "zai"]
    assert [group["provider"] for group in filtered] == ["VibeProxy", "Atomic", "zAI"]
    assert filtered[0]["models"] == [
        {"id": "claude-opus-4-8", "label": "claude-opus-4-8"}
    ]
    assert filtered[1]["models"] == [
        {"id": "@atomic:qwopus-atomic", "label": "Qwopus Atomic"}
    ]
    assert filtered[2]["models"] == [
        {"id": "@zai:glm-5.2", "label": "GLM 5.2"}
    ]
