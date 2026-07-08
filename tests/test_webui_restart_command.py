import json
import pathlib
import sys
import time
import types

import api.models as models
from api.models import Session
import api.routes as routes


REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


def test_webui_restart_command_is_exact_slash_only():
    assert routes._webui_restart_command_scope('/restart-hermes') == 'hermes'
    assert routes._webui_restart_command_scope('/restart_hermes') == 'hermes'
    assert routes._webui_restart_command_scope('/restart-gateways') == 'gateways'
    assert routes._webui_restart_command_scope('/restart-hermes now') is None
    assert routes._webui_restart_command_scope('please /restart-hermes') is None


def test_webui_restart_stream_uses_detached_helper_completion_marker(monkeypatch, tmp_path):
    session_dir = tmp_path / 'sessions'
    session_dir.mkdir()
    monkeypatch.setattr(models, 'SESSION_DIR', session_dir)
    monkeypatch.setattr(models, 'SESSION_INDEX_FILE', session_dir / '_index.json')
    monkeypatch.setattr(routes, 'SESSION_DIR', session_dir)
    state_dir = tmp_path / 'state'
    monkeypatch.setattr(routes, 'STATE_DIR', state_dir)
    models.SESSIONS.clear()
    routes.STREAMS.clear()

    calls = []

    def fake_enqueue(scope, *, delay, completion_marker):
        calls.append({'scope': scope, 'delay': delay, 'completion_marker': completion_marker})
        marker = {
            'status': 'complete',
            'scope': scope,
            'exit_code': 0,
            'message': 'Hermes restart complete from test',
        }
        with open(completion_marker, 'w', encoding='utf-8') as fh:
            json.dump(marker, fh)
        return 'Queued Hermes restart helper'

    hermes_cli = types.ModuleType('hermes_cli')
    restart_surfaces = types.ModuleType('hermes_cli.restart_surfaces')
    restart_surfaces.enqueue_detached_restart = fake_enqueue
    monkeypatch.setitem(sys.modules, 'hermes_cli', hermes_cli)
    monkeypatch.setitem(sys.modules, 'hermes_cli.restart_surfaces', restart_surfaces)

    s = Session(session_id='restart-test', title='Untitled', messages=[])
    s.save()

    response = routes._start_webui_detached_restart_stream(
        s,
        msg='/restart-hermes',
        scope='hermes',
    )

    assert response['session_id'] == 'restart-test'
    assert response['stream_id'].startswith('restart_')

    deadline = time.time() + 5
    completion = None
    while time.time() < deadline:
        completion = routes._read_webui_restart_completion(response['stream_id'])
        if completion:
            break
        time.sleep(0.05)

    assert calls == [
        {
            'scope': 'hermes',
            'delay': 1.0,
            'completion_marker': str(routes._webui_restart_marker_path(response['stream_id'])),
        }
    ]
    assert completion['status'] == 'complete'
    assert completion['message'] == 'Hermes restart complete from test'

    deadline = time.time() + 5
    while time.time() < deadline:
        loaded = Session.load('restart-test')
        if loaded and loaded.active_stream_id is None:
            break
        time.sleep(0.05)
    loaded = Session.load('restart-test')
    assert loaded.active_stream_id is None
    assert loaded.pending_user_message is None


def test_frontend_consumes_restart_completion_from_stream_status():
    src = REPO_ROOT / 'static' / 'messages.js'
    text = src.read_text(encoding='utf-8')
    assert 'st&&st.restart_completion' in text
    assert '_finishRestartCompletion(source, st.restart_completion)' in text
    assert "Hermes restart finished." in text
