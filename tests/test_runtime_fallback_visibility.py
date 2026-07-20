from __future__ import annotations

import json
import subprocess
import threading
from pathlib import Path
from types import SimpleNamespace

from api.runtime_routing import attach_runtime_routing_summary, normalize_runtime_routing_payload
from api import gateway_chat
from api.gateway_chat import _gateway_runtime_routing_event
from api.streaming import (
    _build_runtime_routing_event_callback,
    _event_callback_for_cached_agent,
)


REPO = Path(__file__).resolve().parents[1]


ROUTE_EVENT = {
    "schema_version": 1,
    "state": "fallback_activated",
    "selected": {"model": "primary-model", "provider": "primary"},
    "runtime": {"model": "backup-model", "provider": "backup"},
    "fallback": {"active": True, "reason": "primary quota exhausted", "chain_index": 1},
}


def test_runtime_routing_contract_normalizes_without_mutating_selected_model():
    raw = {**ROUTE_EVENT, "ignored_secret": "must-not-cross", "selected": dict(ROUTE_EVENT["selected"])}
    normalized = normalize_runtime_routing_payload(raw)

    assert normalized == ROUTE_EVENT
    assert raw["selected"]["model"] == "primary-model"
    assert "ignored_secret" not in normalized


def test_runtime_routing_contract_rejects_unknown_schema_and_state():
    assert normalize_runtime_routing_payload({**ROUTE_EVENT, "schema_version": 2}) is None
    assert normalize_runtime_routing_payload({**ROUTE_EVENT, "state": "mystery"}) is None


def test_settled_runtime_summary_attaches_to_session_and_completed_assistant():
    session = SimpleNamespace(
        model="primary-model",
        model_provider="primary",
        messages=[
            {"role": "user", "content": "work"},
            {"role": "assistant", "content": "done"},
        ]
    )
    finished = {**ROUTE_EVENT, "state": "finished"}

    attach_runtime_routing_summary(session, finished)

    assert session.runtime_routing == finished
    assert session.messages[-1]["_runtimeRouting"] == finished
    assert (session.model, session.model_provider) == ("primary-model", "primary")


def test_gateway_runtime_routing_translates_direct_and_nested_runs_payloads():
    assert _gateway_runtime_routing_event(ROUTE_EVENT) == ROUTE_EVENT
    assert _gateway_runtime_routing_event({"event": "runtime.routing", "payload": ROUTE_EVENT}) == ROUTE_EVENT


def test_gateway_runs_stream_relays_runtime_routing_as_local_event(monkeypatch):
    class Response:
        def __init__(self, body=b"", lines=()):
            self.body = body
            self.lines = list(lines)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, _limit=-1):
            return self.body

        def __iter__(self):
            return iter(self.lines)

    responses = iter(
        [
            Response(json.dumps({"run_id": "gateway-run-1"}).encode()),
            Response(
                lines=[
                    b"event: runtime.routing\n",
                    f"data: {json.dumps(ROUTE_EVENT)}\n".encode(),
                    b"event: run.completed\n",
                    b'data: {"output":"done"}\n',
                    b"data: [DONE]\n",
                ]
            ),
        ]
    )
    monkeypatch.setattr(gateway_chat.urllib.request, "urlopen", lambda *_args, **_kwargs: next(responses))
    emitted = []

    text, _usage = gateway_chat._run_gateway_runs_api_streaming(
        "session-1",
        "work",
        "primary-model",
        "/tmp",
        "stream-1",
        "http://gateway.invalid",
        "",
        [],
        {},
        put_gateway_event=lambda event, payload: emitted.append((event, payload)),
        cancel_event=threading.Event(),
        session=SimpleNamespace(context_messages=[]),
    )

    assert text == "done"
    assert ("runtime_routing", ROUTE_EVENT) in emitted


def test_direct_streaming_event_callback_emits_route_and_preserves_independent_callback():
    emitted = []
    prior = []
    latest = [None]
    current = _build_runtime_routing_event_callback(
        lambda event, payload: emitted.append((event, payload)), latest
    )
    combined = _event_callback_for_cached_agent(
        lambda name, payload: prior.append((name, payload)), current
    )

    combined("unrelated:event", {"value": 1})
    combined("runtime:route", ROUTE_EVENT)

    assert prior == [("unrelated:event", {"value": 1}), ("runtime:route", ROUTE_EVENT)]
    assert emitted == [("runtime_routing", ROUTE_EVENT)]
    assert latest[0] == ROUTE_EVENT


def test_browser_runtime_presentation_distinguishes_running_and_last_used():
    ui_js = (REPO / "static" / "ui.js").read_text(encoding="utf-8")
    start = ui_js.index("function _runtimeRoutingPresentation(")
    end = ui_js.index("\nfunction ", start + 10)
    function_source = ui_js[start:end]
    script = f"""
const getModelLabel=(id)=>String(id||'');
const _compactComposerModelChipLabel=(id,label)=>String(label||id||'');
{function_source}
const fallback={json.dumps(ROUTE_EVENT)};
const finished={{...fallback,state:'finished'}};
console.log(JSON.stringify([
  _runtimeRoutingPresentation(fallback),
  _runtimeRoutingPresentation(finished),
]));
"""
    result = subprocess.run(["node", "-e", script], cwd=REPO, text=True, capture_output=True, check=True)
    running, settled = json.loads(result.stdout)
    assert running["phase"] == "Running"
    assert running["runtimeLabel"] == "backup-model via Backup"
    assert settled["phase"] == "Last used"
