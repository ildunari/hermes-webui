import threading
import time

from api import profiles


def test_deferred_skill_stats_return_stale_then_refresh(monkeypatch, tmp_path):
    profile_dir = tmp_path / "coding"
    skills_dir = profile_dir / "skills"
    skills_dir.mkdir(parents=True)
    (profile_dir / "config.yaml").write_text("skills: {}\n", encoding="utf-8")

    started = threading.Event()
    release = threading.Event()

    def _compute(_profile_dir):
        started.set()
        assert release.wait(timeout=2)
        return (7, 9)

    monkeypatch.setenv("HERMES_WEBUI_DEFER_SKILL_STATS", "true")
    monkeypatch.setattr(profiles, "_compute_profile_skills_stats", _compute)
    profiles._SKILLS_STATS_CACHE.clear()
    profiles._SKILLS_STATS_REFRESHING.clear()

    assert profiles._get_profile_skills_stats(profile_dir) == (0, 0)
    assert started.wait(timeout=1)
    # A concurrent poll does not start a duplicate worker and remains instant.
    assert profiles._get_profile_skills_stats(profile_dir) == (0, 0)

    release.set()
    deadline = time.monotonic() + 2
    while profile_dir.resolve() not in profiles._SKILLS_STATS_CACHE:
        assert time.monotonic() < deadline
        time.sleep(0.01)

    assert profiles._get_profile_skills_stats(profile_dir) == (7, 9)
