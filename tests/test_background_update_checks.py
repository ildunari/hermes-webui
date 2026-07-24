import threading
import time

from api import updates


def test_background_update_refresh_collapses_concurrent_calls(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    calls = []

    def _check(**kwargs):
        calls.append(kwargs)
        started.set()
        assert release.wait(timeout=2)

    monkeypatch.setattr(updates, "check_for_updates", _check)
    monkeypatch.setattr(updates, "_background_check_in_progress", False)

    assert updates.refresh_update_status_async(force=True, channel="stable") is True
    assert started.wait(timeout=1)
    assert updates.refresh_update_status_async(force=True, channel="stable") is False
    release.set()

    deadline = time.monotonic() + 2
    while updates._background_check_in_progress:
        assert time.monotonic() < deadline
        time.sleep(0.01)

    assert calls == [
        {"force": True, "include_agent": True, "channel": "stable"}
    ]
