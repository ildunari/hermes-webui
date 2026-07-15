"""Regression coverage for #6068 per-turn used-model footer instrumentation."""

from pathlib import Path

from api.models import Session


REPO = Path(__file__).resolve().parents[1]
STREAMING_PY = (REPO / "api" / "streaming.py").read_text(encoding="utf-8")
MODELS_PY = (REPO / "api" / "models.py").read_text(encoding="utf-8")
UI_JS = (REPO / "static" / "ui.js").read_text(encoding="utf-8")
STYLE_CSS = (REPO / "static" / "style.css").read_text(encoding="utf-8")


def test_streaming_stamps_used_model_on_assistant_message_and_usage_payload():
    assert "_dm['_usedModel'] = _used_model" in STREAMING_PY
    assert "_used_model = resolved_model or model" in STREAMING_PY
    assert "usage['used_model'] = _used_model" in STREAMING_PY


def test_models_allowlist_round_trips_used_model_across_save_reload():
    assert '"_usedModel"' in MODELS_PY
    assert "_usedModel" in MODELS_PY.split("_SESSION_MESSAGE_DISPLAY_METADATA_KEYS")[1].split(")")[0]

    session = Session(session_id="6068usedmodel", title="Used model")
    session.messages = [
        {
            "role": "assistant",
            "content": "done",
            "_firstTokenMs": 250,
            "_usedModel": "gpt-5-mini",
        },
    ]
    session.save()

    reloaded = Session.load("6068usedmodel")
    assert reloaded.messages[-1]["_usedModel"] == "gpt-5-mini"
    assert reloaded.messages[-1]["_firstTokenMs"] == 250


def test_settled_footer_renders_used_model_chip_and_suppresses_gateway_duplicate():
    assert "function _usedModelTurnChipLabel" in UI_JS
    assert "msg-used-model-inline" in UI_JS
    assert "_usedModelTurnChipLabel(msg)" in UI_JS
    assert "routing.used_model" in UI_JS
    assert "msg._usedModel" in UI_JS
    assert "_compactComposerModelChipLabel(usedModel,getModelLabel(usedModel))" in UI_JS
    assert ".msg-used-model-inline" in STYLE_CSS


def test_transparent_turn_footer_includes_model_between_duration_and_ttft():
    assert "function _transparentTurnFooterHtml(durationText, modelText, ttftText, tokensText, statusText)" in UI_JS
    assert 'class="lf-model"' in UI_JS
    assert "modelText=_usedModelTurnChipLabel(msg)" in UI_JS
    assert ".transparent-turn-footer .lf-model" in STYLE_CSS