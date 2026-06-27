from __future__ import annotations

import api.config as config
import api.routes as routes


def test_moa_virtual_group_is_added_from_config():
    groups = [{"provider": "OpenAI Codex", "provider_id": "openai-codex", "models": [{"id": "gpt-5.5", "label": "GPT 5.5"}]}]
    config._append_moa_virtual_group(
        groups,
        {
            "moa": {
                "default_preset": "default",
                "presets": {
                    "default": {"reference_models": []},
                    "review": {"reference_models": []},
                },
            }
        },
    )

    moa_group = next(group for group in groups if group.get("provider_id") == "moa")
    assert moa_group["provider"] == "Mixture of Agents"
    assert [model["id"] for model in moa_group["models"]] == ["default", "review"]


def test_moa_virtual_group_is_not_duplicated():
    groups = [{"provider": "Mixture of Agents", "provider_id": "moa", "models": [{"id": "default", "label": "default"}]}]
    config._append_moa_virtual_group(groups, {"moa": {"presets": {"default": {}}}})
    assert len([group for group in groups if group.get("provider_id") == "moa"]) == 1


def test_webui_moa_command_strips_prefix_and_forces_prompt():
    assert routes._webui_moa_command_prompt("/moa solve this") == (True, "solve this")
    assert routes._webui_moa_command_prompt("  /moa\nsolve this\n") == (True, "solve this")
    assert routes._webui_moa_command_prompt("/model gpt-5.5") == (False, "/model gpt-5.5")


def test_webui_moa_command_without_prompt_is_detected():
    assert routes._webui_moa_command_prompt("/moa") == (True, "")
