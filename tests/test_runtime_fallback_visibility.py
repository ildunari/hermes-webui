from __future__ import annotations

from collections import OrderedDict
import json
import subprocess
import threading
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

from api import gateway_chat, models, routes, streaming
from api.config import STREAMS, create_stream_channel
from api.gateway_chat import _gateway_runtime_routing_event
from api.runtime_routing import (
    attach_runtime_routing_summary,
    normalize_runtime_routing_payload,
    settle_runtime_routing_summary,
)
from api.streaming import (
    _build_runtime_routing_event_callback,
    _event_callback_for_cached_agent,
)
from tests.test_zh_hant_locale import locale_block, value_map


REPO = Path(__file__).resolve().parents[1]


ROUTE_EVENT = {
    "schema_version": 1,
    "state": "fallback_activated",
    "selected": {"model": "primary-model", "provider": "primary"},
    "runtime": {"model": "backup-model", "provider": "backup"},
    "fallback": {"active": True, "reason": "rate_limit", "chain_index": 1},
}


def _js_function_source(path: Path, name: str) -> str:
    """Extract a named vanilla-JS function, including its balanced body."""
    source = path.read_text(encoding="utf-8")
    start = source.index(f"function {name}(")
    paren = source.index("(", start)
    paren_depth = 0
    body_start = None
    for idx in range(paren, len(source)):
        if source[idx] == "(":
            paren_depth += 1
        elif source[idx] == ")":
            paren_depth -= 1
            if paren_depth == 0:
                body_start = source.index("{", idx)
                break
    assert body_start is not None
    depth = 0
    quote = None
    escaped = False
    line_comment = False
    block_comment = False
    idx = body_start
    while idx < len(source):
        char = source[idx]
        nxt = source[idx + 1] if idx + 1 < len(source) else ""
        if line_comment:
            if char == "\n":
                line_comment = False
            idx += 1
            continue
        if block_comment:
            if char == "*" and nxt == "/":
                block_comment = False
                idx += 2
                continue
            idx += 1
            continue
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            idx += 1
            continue
        if char == "/" and nxt == "/":
            line_comment = True
            idx += 2
            continue
        if char == "/" and nxt == "*":
            block_comment = True
            idx += 2
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start : idx + 1]
        idx += 1
    raise AssertionError(f"unterminated JS function {name}")


def test_runtime_routing_contract_normalizes_without_mutating_selected_model():
    raw = {**ROUTE_EVENT, "ignored_secret": "must-not-cross", "selected": dict(ROUTE_EVENT["selected"])}
    normalized = normalize_runtime_routing_payload(raw)

    assert normalized == ROUTE_EVENT
    assert raw["selected"]["model"] == "primary-model"
    assert "ignored_secret" not in normalized


def test_runtime_routing_contract_rejects_unknown_schema_state_and_reason_enum():
    assert normalize_runtime_routing_payload({**ROUTE_EVENT, "schema_version": 2}) is None
    assert normalize_runtime_routing_payload({**ROUTE_EVENT, "schema_version": True}) is None
    assert normalize_runtime_routing_payload({**ROUTE_EVENT, "state": "mystery"}) is None
    assert normalize_runtime_routing_payload(
        {**ROUTE_EVENT, "fallback": {"active": True, "reason": "primary quota exhausted", "chain_index": 1}}
    ) is None


def test_runtime_routing_contract_rejects_missing_or_malformed_required_fields():
    malformed = [
        {key: value for key, value in ROUTE_EVENT.items() if key != "selected"},
        {key: value for key, value in ROUTE_EVENT.items() if key != "runtime"},
        {key: value for key, value in ROUTE_EVENT.items() if key != "fallback"},
        {**ROUTE_EVENT, "selected": None},
        {**ROUTE_EVENT, "runtime": []},
        {**ROUTE_EVENT, "fallback": None},
        {**ROUTE_EVENT, "selected": {"model": "primary-model"}},
        {**ROUTE_EVENT, "selected": {"provider": "primary"}},
        {**ROUTE_EVENT, "runtime": {"model": "backup-model"}},
        {**ROUTE_EVENT, "runtime": {"provider": "backup"}},
        {**ROUTE_EVENT, "selected": {"model": "", "provider": "primary"}},
        {**ROUTE_EVENT, "runtime": {"model": "backup-model", "provider": "  "}},
        {**ROUTE_EVENT, "selected": {"model": "\x00\x1b", "provider": "primary"}},
        {**ROUTE_EVENT, "fallback": {"reason": "rate_limit", "chain_index": 1}},
        {**ROUTE_EVENT, "fallback": {"active": "false", "reason": "rate_limit", "chain_index": 1}},
        {**ROUTE_EVENT, "fallback": {"active": True, "reason": "", "chain_index": 1}},
        {**ROUTE_EVENT, "fallback": {"active": True, "reason": "rate_limit"}},
    ]
    for payload in malformed:
        assert normalize_runtime_routing_payload(payload) is None


def test_runtime_routing_contract_rejects_non_integer_or_out_of_bounds_chain_index():
    for chain_index in (True, 1.5, "1", -1, 101, 10**100):
        payload = {
            **ROUTE_EVENT,
            "fallback": {"active": True, "reason": "rate_limit", "chain_index": chain_index},
        }
        assert normalize_runtime_routing_payload(payload) is None

    boundary = {
        **ROUTE_EVENT,
        "fallback": {"active": True, "reason": "rate_limit", "chain_index": 100},
    }
    assert normalize_runtime_routing_payload(boundary) == boundary


def test_runtime_routing_contract_scrubs_hostile_allowed_text_before_persistence():
    hostile = {
        **ROUTE_EVENT,
        "selected": {
            "model": "primary\nAuthorization: Bearer abcdefghijklmnopqrstuvwxyz",
            "provider": "https://user:pass@example.invalid/path?api_key=secret",
        },
        "runtime": {
            "model": "sk-abcdefghijklmnopqrstuvwxyz",
            "provider": "backup\x00\x1bprovider password=hunter2",
        },
        "fallback": {
            "active": True,
            "reason": "rate_limit",
            "chain_index": 1,
        },
    }

    normalized = normalize_runtime_routing_payload(hostile)
    assert normalized is not None
    serialized = json.dumps(normalized).lower()

    assert normalized["fallback"]["reason"] == "rate_limit"
    for leaked in ("abcdefghijklmnopqrstuvwxyz", "hunter2", "user:pass", "api_key=secret", "\x1b"):
        assert leaked not in serialized
    assert "[redacted" in serialized


def test_settled_runtime_summary_attaches_to_session_and_completed_assistant():
    session = SimpleNamespace(
        model="primary-model",
        model_provider="primary",
        messages=[
            {"role": "user", "content": "work"},
            {"role": "assistant", "content": "done"},
        ],
    )
    finished = {**ROUTE_EVENT, "state": "finished"}

    attach_runtime_routing_summary(session, finished)

    assert session.runtime_routing == finished
    assert session.messages[-1]["_runtimeRouting"] == finished
    assert (session.model, session.model_provider) == ("primary-model", "primary")


def test_success_without_routing_clears_summary_but_preserves_historical_message_metadata():
    old = {**ROUTE_EVENT, "state": "finished"}
    session = SimpleNamespace(
        runtime_routing=old,
        messages=[{"role": "assistant", "content": "old", "_runtimeRouting": old}],
    )

    settle_runtime_routing_summary(session, None)

    assert session.runtime_routing is None
    assert session.messages[0]["_runtimeRouting"] == old


def test_gateway_runtime_routing_translates_exact_upstream_and_compat_envelopes():
    producer = {
        "event": "runtime.routing",
        "run_id": "gateway-run-1",
        "timestamp": 0,
        "routing": ROUTE_EVENT,
    }
    assert _gateway_runtime_routing_event(producer) == ROUTE_EVENT
    assert _gateway_runtime_routing_event(ROUTE_EVENT) == ROUTE_EVENT
    assert _gateway_runtime_routing_event({"event": "runtime.routing", "payload": ROUTE_EVENT}) == ROUTE_EVENT
    assert _gateway_runtime_routing_event({"event": "run.completed", "runtime_routing": ROUTE_EVENT}) == ROUTE_EVENT


def _runs_stream(monkeypatch, lines):
    class Response:
        def __init__(self, body=b"", stream_lines=()):
            self.body = body
            self.lines = list(stream_lines)

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
            Response(stream_lines=lines),
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
    return text, emitted


def test_gateway_runs_stream_relays_exact_producer_envelope(monkeypatch):
    producer = {
        "event": "runtime.routing",
        "run_id": "gateway-run-1",
        "timestamp": 0,
        "routing": ROUTE_EVENT,
    }
    text, emitted = _runs_stream(
        monkeypatch,
        [
            b"event: runtime.routing\n",
            f"data: {json.dumps(producer)}\n".encode(),
            b"event: run.completed\n",
            b'data: {"event":"run.completed","output":"done"}\n',
            b"data: [DONE]\n",
        ],
    )
    assert text == "done"
    assert emitted.count(("runtime_routing", ROUTE_EVENT)) == 1


def test_gateway_runs_stream_consumes_completed_route_when_live_event_is_absent(monkeypatch):
    completed = {"event": "run.completed", "output": "done", "runtime_routing": ROUTE_EVENT}
    text, emitted = _runs_stream(
        monkeypatch,
        [
            b"event: run.completed\n",
            f"data: {json.dumps(completed)}\n".encode(),
            b"data: [DONE]\n",
        ],
    )
    assert text == "done"
    assert emitted == [("runtime_routing", ROUTE_EVENT)]


def test_legacy_gateway_stream_consumes_completed_route_when_live_event_is_absent(tmp_path, monkeypatch):
    session_dir = tmp_path / "sessions"
    session_dir.mkdir()
    monkeypatch.setattr(models, "SESSION_DIR", session_dir)
    monkeypatch.setattr(models, "SESSION_INDEX_FILE", session_dir / "_index.json")
    monkeypatch.setattr(models, "SESSIONS", OrderedDict())

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def __iter__(self):
            yield b'data: {"choices":[{"delta":{"content":"done"}}]}\n\n'
            completed = {"event": "run.completed", "runtime_routing": ROUTE_EVENT}
            yield f"data: {json.dumps(completed)}\n\n".encode()
            yield b"data: [DONE]\n\n"

    monkeypatch.setenv("HERMES_WEBUI_GATEWAY_BASE_URL", "http://gateway.invalid")
    monkeypatch.setattr(streaming, "_load_webui_prefill_context", lambda _cfg: {"status": "not_configured"})
    monkeypatch.setattr(streaming, "_prefill_messages_with_webui_context", lambda _ctx, _cfg: [])
    monkeypatch.setattr(gateway_chat, "gateway_approval_unavailable_reason", lambda *_args: None)
    monkeypatch.setattr(gateway_chat.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())

    session = models.new_session()
    stream_id = "legacy-runtime-route"
    session.active_stream_id = stream_id
    session.pending_user_message = "work"
    session.pending_attachments = []
    session.pending_started_at = 1
    session.save()
    channel = create_stream_channel()
    subscriber = channel.subscribe()
    STREAMS[stream_id] = channel

    gateway_chat._run_gateway_chat_streaming(
        session.session_id, "work", "primary-model", str(tmp_path), stream_id, []
    )

    events = []
    while not subscriber.empty():
        item = subscriber.get_nowait()
        events.append((item[0], item[1]))
    assert ("runtime_routing", ROUTE_EVENT) in events
    saved = models.get_session(session.session_id)
    assert saved.runtime_routing == ROUTE_EVENT
    assert saved.messages[-1]["_runtimeRouting"] == ROUTE_EVENT


def test_cached_agent_callback_preserves_base_callback_across_four_reuses_without_nesting():
    prior = []
    callback = lambda name, payload: prior.append((name, payload))
    per_turn = []

    for turn in range(4):
        emitted = []
        current = _build_runtime_routing_event_callback(
            lambda event, payload, emitted=emitted: emitted.append((event, payload)), [None]
        )
        callback = _event_callback_for_cached_agent(callback, current)
        callback("unrelated:event", {"turn": turn})
        callback("runtime:route", ROUTE_EVENT)
        per_turn.append(emitted)

    assert prior == [item for turn in range(4) for item in (
        ("unrelated:event", {"turn": turn}),
        ("runtime:route", ROUTE_EVENT),
    )]
    assert per_turn == [[("runtime_routing", ROUTE_EVENT)] for _ in range(4)]


def test_cached_agent_replaces_initial_webui_callback_and_old_turn_closures_receive_nothing():
    per_turn = []
    callback = None

    for _turn in range(4):
        emitted = []
        current = _build_runtime_routing_event_callback(
            lambda event, payload, emitted=emitted: emitted.append((event, payload)), [None]
        )
        callback = current if callback is None else _event_callback_for_cached_agent(callback, current)
        callback("runtime:route", ROUTE_EVENT)
        per_turn.append(emitted)

    assert per_turn == [[("runtime_routing", ROUTE_EVENT)] for _ in range(4)]


def test_session_model_or_provider_change_clears_summary_not_message_history(tmp_path, monkeypatch):
    session_dir = tmp_path / "sessions"
    session_dir.mkdir()
    monkeypatch.setattr(models, "SESSION_DIR", session_dir)
    monkeypatch.setattr(models, "SESSION_INDEX_FILE", session_dir / "_index.json")
    monkeypatch.setattr(models, "SESSIONS", OrderedDict())
    session = models.new_session(model="same-model", model_provider="primary")
    finished = {**ROUTE_EVENT, "state": "finished"}
    session.runtime_routing = finished
    session.messages = [{"role": "assistant", "content": "old", "_runtimeRouting": finished}]
    session.save()

    body = json.dumps({
        "session_id": session.session_id,
        "model": "same-model",
        "model_provider": "backup",
    }).encode()
    captured = {}
    monkeypatch.setattr(routes, "j", lambda _handler, payload, *args, **kwargs: captured.update(payload) or True)
    monkeypatch.setattr(routes, "bad", lambda _handler, message, *args, **kwargs: (_ for _ in ()).throw(AssertionError(message)))
    monkeypatch.setattr(routes, "_resolve_context_length_for_session_model", lambda *_args, **_kwargs: 123)
    monkeypatch.setattr("api.config._evict_session_agent", lambda _sid: None)
    handler = SimpleNamespace(headers={"Content-Length": str(len(body))}, rfile=BytesIO(body))

    routes.handle_post(handler, SimpleNamespace(path="/api/session/update"))

    updated = models.get_session(session.session_id)
    assert updated.model_provider == "backup"
    assert updated.runtime_routing is None
    assert updated.messages[0]["_runtimeRouting"] == finished


def test_browser_event_lifecycle_replay_replacement_cleanup_and_selected_control_are_observable():
    ui_path = REPO / "static" / "ui.js"
    messages_path = REPO / "static" / "messages.js"
    functions = "\n".join(
        _js_function_source(path, name)
        for path, name in (
            (ui_path, "_runtimeRoutingPresentation"),
            (ui_path, "_latestRuntimeRoutingForSession"),
            (ui_path, "syncModelChip"),
            (messages_path, "_applyRuntimeRoutingForStream"),
            (messages_path, "_clearRuntimeRoutingForStream"),
            (messages_path, "_handleRuntimeRoutingEvent"),
        )
    )
    script = f"""
const route={json.dumps(ROUTE_EVENT)};
const finished={{...route,state:'finished'}};
const labels={{
  runtime_route_running:'Running',runtime_route_last_used:'Last response',runtime_route_selected_via:'Selected via {{0}}',runtime_route_via:'{{0}} via {{1}}',
  runtime_route_fallback:'Fallback',runtime_route_fallback_index:'Fallback {{0}}',
  runtime_route_primary_restored:'Primary restored',runtime_route_title:'Selected {{0}}; {{1}} {{2}}{{3}}'
}};
function t(key,...args){{let out=String(labels[key]||key);args.forEach((v,i)=>{{out=out.split('{{'+i+'}}').join(v??'');}});return out;}}
const classList={{contains:()=>false,toggle:()=>{{}}}};
const elements={{
 modelSelect:{{value:'primary-model'}},composerModelChip:{{title:'',classList}},
 composerModelLabel:{{textContent:''}},composerMobileModelLabel:{{textContent:''}},
 composerMobileModelAction:{{classList}},composerModelRuntime:{{textContent:''}},composerModelDropdown:{{classList}}
}};
function $(id){{return elements[id]||null;}}
const INFLIGHT={{}};
const S={{session:{{session_id:'sid',runtime_routing:finished,messages:[]}},messages:[],activeStreamId:'stream-1',_bootReady:true}};
const window={{}};
const getModelLabel=id=>id==='primary-model'?'Primary selected':id;
const _compactComposerModelChipLabel=(id,label)=>String(label||id||'');
const _selectedModelOption=()=>({{textContent:'Primary selected'}});
const _latestGatewayRoutingForSession=()=>({{used_model:'backup-model',used_provider:'backup'}});
const _gatewayRoutingLabel=()=>'(backup)';
let chipSyncs=0,persists=0;
{functions}
const realSync=syncModelChip;
syncModelChip=()=>{{chipSyncs++;realSync();}};
const event={{data:JSON.stringify(route)}};
const providerOnlyPresentation=_runtimeRoutingPresentation({{...route,runtime:{{model:'primary-model',provider:'backup'}}}});
const first=_handleRuntimeRoutingEvent('sid','stream-1',event,()=>persists++);
const replay=_handleRuntimeRoutingEvent('sid','stream-1',event,()=>persists++);
const selectedAfterReplay=elements.modelSelect.value;
const mainLabel=elements.composerModelLabel.textContent;
const runtimeLabel=elements.composerModelRuntime.textContent;
const staleClear=_clearRuntimeRoutingForStream('sid','older-stream');
const stillRunning=_latestRuntimeRoutingForSession(S.session).state;
const terminalClear=_clearRuntimeRoutingForStream('sid','stream-1');
const settledAfterTerminal=_runtimeRoutingPresentation(_latestRuntimeRoutingForSession(S.session));
INFLIGHT.sid={{streamId:'stream-2',messages:[],toolCalls:[]}};
S.activeStreamId='stream-2';
const replacementEvent={{data:JSON.stringify({{...route,runtime:{{model:'new-backup',provider:'backup'}}}})}};
const replacementApplied=_handleRuntimeRoutingEvent('sid','stream-2',replacementEvent,()=>persists++);
const staleOwnerCleanup=_clearRuntimeRoutingForStream('sid','stream-1');
const replacementModel=INFLIGHT.sid.runtimeRouting.runtime.model;
delete INFLIGHT.sid.runtimeRouting;
delete S.session.runtime_routing;
delete S.session.runtime_routing_live;
S.session.messages=[{{role:'assistant',_runtimeRouting:finished}},{{role:'assistant',content:'new legacy response'}}];
const ignoresOlderAssistant=_latestRuntimeRoutingForSession(S.session)===null;
console.log(JSON.stringify({{first,replay,persists,selectedAfterReplay,mainLabel,runtimeLabel,staleClear,stillRunning,
 terminalClear,settledAfterTerminal,replacementApplied,staleOwnerCleanup,replacementModel,
 liveOwner:S.session.runtime_routing_live_stream_id,chipSyncs,ignoresOlderAssistant,providerOnlyPresentation}}));
"""
    result = subprocess.run(["node", "-e", script], cwd=REPO, text=True, capture_output=True, check=True)
    observed = json.loads(result.stdout)

    assert observed["first"] is True and observed["replay"] is True
    assert observed["persists"] == 3
    assert observed["selectedAfterReplay"] == "primary-model"
    assert observed["mainLabel"] == "Primary selected"
    assert observed["runtimeLabel"] == "Selected via Primary · Running backup-model via Backup"
    assert observed["staleClear"] is False and observed["stillRunning"] == "fallback_activated"
    assert observed["terminalClear"] is True
    assert observed["settledAfterTerminal"]["phase"] == "Last response"
    assert observed["replacementApplied"] is True and observed["staleOwnerCleanup"] is False
    assert observed["replacementModel"] == "new-backup" and observed["liveOwner"] == "stream-2"
    assert observed["ignoresOlderAssistant"] is True
    assert observed["providerOnlyPresentation"]["selectedRouteLabel"] == "Selected via Primary"
    assert observed["providerOnlyPresentation"]["runtimeLabel"] == "Primary selected via Backup"


def test_runtime_copy_uses_i18n_fallback_and_narrow_footer_hides_secondary_route():
    ui = (REPO / "static" / "ui.js").read_text(encoding="utf-8")
    css = (REPO / "static" / "style.css").read_text(encoding="utf-8")
    i18n = (REPO / "static" / "i18n.js").read_text(encoding="utf-8")

    for key in (
        "runtime_route_running",
        "runtime_route_last_used",
        "runtime_route_selected_via",
        "runtime_route_via",
        "runtime_route_fallback",
        "runtime_route_primary_restored",
        "runtime_route_title",
    ):
        assert key in i18n and f"t('{key}'" in ui
    assert ".composer-model-runtime" in css
    phone_block = css[css.index("@media(max-width:640px)") :]
    assert ".composer-model-runtime" in phone_block and "display:none" in phone_block


def test_runtime_route_copy_is_translated_in_every_non_english_locale():
    src = (REPO / "static" / "i18n.js").read_text(encoding="utf-8")
    english = value_map(locale_block(src, "\n  en: {"))
    runtime_keys = {key for key in english if key.startswith("runtime_route_")}
    expected_running = {
        "it": "'In esecuzione'",
        "ja": "'実行中'",
        "ru": "'Выполняется'",
        "es": "'En ejecución'",
        "de": "'Wird ausgeführt'",
        "zh": "'运行中'",
        "zh-Hant": "'執行中'",
        "pt": "'Em execução'",
        "ko": "'실행 중'",
        "fr": "'En cours'",
        "cs": "'Probíhá'",
        "tr": "'Çalışıyor'",
        "pl": "'W toku'",
        "vi": "'Đang chạy'",
    }

    for locale, translated_running in expected_running.items():
        marker = f"\n  '{locale}': {{" if "-" in locale else f"\n  {locale}: {{"
        values = value_map(locale_block(src, marker))
        assert runtime_keys <= values.keys(), f"{locale} is missing runtime route translations"
        assert values["runtime_route_running"] == translated_running
        assert values["runtime_route_running"] != english["runtime_route_running"]
