"""Regression checks for configurable pinned session limits."""

import json
import pathlib
import shutil
import subprocess
import urllib.error
import urllib.request

import pytest

from tests._pytest_port import BASE, TEST_STATE_DIR

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONFIG_PY = (ROOT / "api" / "config.py").read_text(encoding="utf-8")
ROUTES_PY = (ROOT / "api" / "routes.py").read_text(encoding="utf-8")
INDEX_HTML = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
PANELS_JS = (ROOT / "static" / "panels.js").read_text(encoding="utf-8")
BOOT_JS = (ROOT / "static" / "boot.js").read_text(encoding="utf-8")
SESSIONS_JS = (ROOT / "static" / "sessions.js").read_text(encoding="utf-8")


def post(path, body=None):
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read()), r.status
    except urllib.error.HTTPError as e:
        return json.loads(e.read()), e.code


def make_session(created, title):
    payload = {
        "title": title,
        "messages": [{"role": "user", "content": "keep this conversation handy"}],
        "model": "test/pin-limit-setting",
    }
    d, status = post("/api/session/import", payload)
    assert status == 200
    sid = d["session"]["session_id"]
    created.append(sid)
    return sid


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        ({}, 0),
        ({"pinned_sessions_limit": 0}, 0),
        ({"pinned_sessions_limit": "7"}, 7),
        ({"pinned_sessions_limit": None}, 0),
        ({"pinned_sessions_limit": "bad"}, 0),
        ({"pinned_sessions_limit": "++1"}, 0),
        ({"pinned_sessions_limit": -1}, 0),
        ({"pinned_sessions_limit": 100}, 0),
    ],
)
def test_load_settings_normalizes_missing_and_malformed_pin_limits(
    tmp_path, monkeypatch, stored, expected
):
    from api import config

    settings_file = tmp_path / "settings.json"
    settings_file.write_text(json.dumps(stored), encoding="utf-8")
    monkeypatch.setattr(config, "SETTINGS_FILE", settings_file)
    assert config.load_settings()["pinned_sessions_limit"] == expected


def test_browser_pin_limit_normalizer_preserves_finite_limit_on_invalid_input():
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for the browser pin-limit helper test")
    start = PANELS_JS.index("function _normalizePinnedSessionsLimit")
    end = PANELS_JS.index("\n}\n\nfunction _schedulePreferencesAutosave", start) + 2
    helper = PANELS_JS[start:end]
    payload_start = PANELS_JS.index("function _preferencesPayloadFromUi")
    payload_end = PANELS_JS.index("\n}\n\nfunction _speechPreferencesPayloadFromUi", payload_start) + 2
    payload_function = PANELS_JS[payload_start:payload_end]
    script = helper + payload_function + """
const window={_pinnedSessionsLimit:3};
let fieldValue='';
function $(id){ return id==='settingsPinnedSessionsLimit'?{value:fieldValue}:null; }
function _speechPreferencesPayloadFromUi(){ return {}; }
const actual = [
  _normalizePinnedSessionsLimit(0, 3),
  _normalizePinnedSessionsLimit('7', 3),
  _normalizePinnedSessionsLimit('', 3),
  _normalizePinnedSessionsLimit('-1', 3),
  _normalizePinnedSessionsLimit('1.5', 3),
  _normalizePinnedSessionsLimit('100', 3),
  _normalizePinnedSessionsLimit('bad', 3),
  _normalizePinnedSessionsLimit(undefined),
];
const payloads=['0','7','','-1','1.5','1e2','100','bad'].map(value=>{
  fieldValue=value;
  return _preferencesPayloadFromUi().pinned_sessions_limit;
});
process.stdout.write(JSON.stringify({actual,payloads}));
"""
    result = subprocess.run([node, "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    output = json.loads(result.stdout)
    assert output["actual"] == [0, 7, 3, 3, 3, 3, 3, 0]
    assert output["payloads"] == [0, 7, 3, 3, 3, 3, 3, 3]


def test_pin_limit_setting_is_exposed_and_wired_through_ui():
    assert '"pinned_sessions_limit": 0' in CONFIG_PY
    assert "normalize_pinned_sessions_limit(" in CONFIG_PY
    assert "api_config.normalize_pinned_sessions_limit(" in ROUTES_PY
    assert '"pinned_sessions_limit": (0, 99)' in CONFIG_PY
    assert 'id="settingsPinnedSessionsLimit"' in INDEX_HTML
    assert 'type="number"' in INDEX_HTML
    assert 'min="0"' in INDEX_HTML
    assert 'max="99"' in INDEX_HTML
    assert "unlimited by default" in INDEX_HTML
    assert "payload.pinned_sessions_limit=_normalizePinnedSessionsLimit(" in PANELS_JS
    assert "settings.pinned_sessions_limit" in PANELS_JS
    assert "function _normalizePinnedSessionsLimit(value,fallback=0)" in PANELS_JS
    assert "pinnedSessionsLimit=parseInt(s.pinned_sessions_limit,10)" in BOOT_JS
    assert "window._pinnedSessionsLimit=0" in BOOT_JS
    assert "pinned_sessions_limit||3" not in BOOT_JS
    assert "settingsPinnedSessionsLimit')||{}).value,10)||3" not in PANELS_JS
    assert "function _getPinnedSessionsLimit()" in SESSIONS_JS
    assert "function _pinnedSessionsLimit()" not in SESSIONS_JS
    assert "_pinnedSessionCount()>=_getPinnedSessionsLimit()" not in SESSIONS_JS
    assert "await api('/api/session/pin'" in SESSIONS_JS


def test_absent_pin_limit_defaults_to_unlimited_at_runtime():
    created = []
    settings_file = TEST_STATE_DIR / "settings.json"
    original = settings_file.read_bytes() if settings_file.exists() else None
    try:
        settings_file.parent.mkdir(parents=True, exist_ok=True)
        settings_file.write_text("{}", encoding="utf-8")
        pinned = [make_session(created, f"Default unlimited pin {i}") for i in range(4)]
        for sid in pinned:
            data, status = post("/api/session/pin", {"session_id": sid, "pinned": True})
            assert status == 200
            assert data["session"]["pinned"] is True
    finally:
        if original is None:
            settings_file.unlink(missing_ok=True)
        else:
            settings_file.write_bytes(original)
        for sid in created:
            post("/api/session/delete", {"session_id": sid})


def test_settings_api_persists_integer_pin_limit_and_rejects_invalid_values():
    try:
        d, status = post("/api/settings", {"pinned_sessions_limit": 5})
        assert status == 200
        assert d["pinned_sessions_limit"] == 5

        d, status = post("/api/settings", {"pinned_sessions_limit": "7"})
        assert status == 200
        assert d["pinned_sessions_limit"] == 7

        for invalid in (1.5, True, "", "bad", "++1", "--1", "+-1", "-+1"):
            d, status = post("/api/settings", {"pinned_sessions_limit": invalid})
            assert status == 200
            assert d["pinned_sessions_limit"] == 7

        d, status = post("/api/settings", {"pinned_sessions_limit": 0})
        assert status == 200
        assert d["pinned_sessions_limit"] == 0

        d, status = post("/api/settings", {"pinned_sessions_limit": 100})
        assert status == 200
        assert d["pinned_sessions_limit"] == 0
    finally:
        post("/api/settings", {"pinned_sessions_limit": 0})


def test_session_pin_endpoint_uses_configured_limit():
    created = []
    try:
        d, status = post("/api/settings", {"pinned_sessions_limit": 4})
        assert status == 200
        assert d["pinned_sessions_limit"] == 4

        pinned = [make_session(created, f"Configured pin cap {i}") for i in range(4)]
        for sid in pinned:
            d, status = post("/api/session/pin", {"session_id": sid, "pinned": True})
            assert status == 200
            assert d["session"]["pinned"] is True

        fifth = make_session(created, "Configured pin cap overflow")
        d, status = post("/api/session/pin", {"session_id": fifth, "pinned": True})
        assert status == 400
        assert "4 sessions" in d.get("error", "")
    finally:
        post("/api/settings", {"pinned_sessions_limit": 0})
        for sid in created:
            post("/api/session/delete", {"session_id": sid})


def test_session_pin_endpoint_allows_unlimited_pins_when_limit_is_zero():
    created = []
    try:
        d, status = post("/api/settings", {"pinned_sessions_limit": 0})
        assert status == 200
        assert d["pinned_sessions_limit"] == 0

        pinned = [make_session(created, f"Unlimited pin {i}") for i in range(5)]
        for sid in pinned:
            d, status = post("/api/session/pin", {"session_id": sid, "pinned": True})
            assert status == 200
            assert d["session"]["pinned"] is True
    finally:
        post("/api/settings", {"pinned_sessions_limit": 0})
        for sid in created:
            post("/api/session/delete", {"session_id": sid})
