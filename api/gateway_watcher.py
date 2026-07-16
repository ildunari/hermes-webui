"""
Hermes Web UI -- Gateway session watcher.

Background daemon thread that polls state.db every 5 seconds for changes
to gateway sessions (telegram, discord, slack, etc.). When changes are
detected, it pushes notifications to all subscribed SSE clients.

This enables real-time session list updates in the sidebar without
requiring any changes to hermes-agent.
"""
import hashlib
import logging
import os
import queue
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path

from api.config import HOME
from api.agent_sessions import open_state_db_readonly, read_importable_agent_session_rows

logger = logging.getLogger(__name__)


# ── State hash tracking ─────────────────────────────────────────────────────

def _snapshot_hash(sessions: list) -> str:
    """Create a lightweight hash of session IDs and timestamps for change detection."""
    key = '|'.join(
        f"{s['session_id']}:{s.get('updated_at', 0)}:{s.get('message_count', 0)}"
        for s in sorted(sessions, key=lambda x: x['session_id'])
    )
    return hashlib.md5(key.encode(), usedforsecurity=False).hexdigest()


# Sources excluded from the WebUI sidebar projection. Must match the default
# ``exclude_sources`` used by ``read_importable_agent_session_rows`` so the
# sessions-table aggregate tracks the same row set as the expensive projection.
_WATCHER_EXCLUDED_SOURCES = ("cron", "subagent", "webui")


# Every sessions-table field read by read_importable_agent_session_rows(),
# including lineage/visibility inputs whose changes can alter which row survives.
_WATCHER_PROJECTED_SESSION_COLUMNS = (
    "id", "title", "model", "message_count", "started_at", "source",
    "session_source", "user_id", "chat_id", "chat_type", "thread_id",
    "session_key", "origin_chat_id", "origin_user_id", "platform",
    "parent_session_id", "ended_at", "end_reason",
)


def _cheap_change_fingerprint(db_path: Path) -> str | None:
    """Hash the complete sidebar projection input, cheaper than projecting it.

    This scan intentionally preserves correctness rather than relying on rowid or
    global aggregates: in-place session updates and rewrites of an older
    transcript must invalidate too. It reads all projected session fields and
    one grouped message aggregate (count, user count, max timestamp) per included
    session, but skips candidate ordering, lineage collapse, Python mapping, and
    repeated correlated lookups performed by the full projection.
    """
    try:
        with closing(open_state_db_readonly(db_path)) as conn:
            session_cols = {
                row[1] for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
            }
            if "source" not in session_cols:
                return None
            selectable = [
                column for column in _WATCHER_PROJECTED_SESSION_COLUMNS
                if column in session_cols
            ]
            placeholders = ", ".join("?" for _ in _WATCHER_EXCLUDED_SOURCES)
            params = list(_WATCHER_EXCLUDED_SOURCES)
            h = hashlib.md5(usedforsecurity=False)
            session_rows = conn.execute(
                f"SELECT {', '.join(selectable)} FROM sessions "
                f"WHERE source IS NOT NULL AND source NOT IN ({placeholders}) "
                "ORDER BY id",
                params,
            ).fetchall()
            for row in session_rows:
                h.update(repr(row).encode("utf-8", "replace"))
                h.update(b"\x1e")

            tables = {
                row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "messages" not in tables:
                return h.hexdigest()
            message_cols = {
                row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()
            }
            if "session_id" not in message_cols:
                return h.hexdigest()
            count_col = "id" if "id" in message_cols else "session_id"
            user_count_expr = (
                "COUNT(CASE WHEN LOWER(m.role) = 'user' THEN 1 END)"
                if "role" in message_cols
                else f"COUNT(m.{count_col})"
            )
            max_timestamp_expr = (
                "COALESCE(MAX(m.timestamp), 0)" if "timestamp" in message_cols else "0"
            )
            message_rows = conn.execute(
                f"SELECT s.id, COUNT(m.{count_col}), {user_count_expr}, "
                f"{max_timestamp_expr} "
                "FROM sessions s LEFT JOIN messages m ON m.session_id = s.id "
                f"WHERE s.source IS NOT NULL AND s.source NOT IN ({placeholders}) "
                "GROUP BY s.id ORDER BY s.id",
                params,
            ).fetchall()
            for row in message_rows:
                h.update(repr(row).encode("utf-8", "replace"))
                h.update(b"\x1e")
            return h.hexdigest()
    except sqlite3.Error:
        # Unknown/locked schema: force the caller to run the authoritative full
        # projection rather than trusting an incomplete fingerprint.
        return None
    except Exception:
        return None


def _positive_float_from_env(name: str, default: float, *, minimum: float) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return float(default)
    try:
        return max(minimum, float(raw))
    except (TypeError, ValueError):
        return float(default)


# ── DB resolution (shared pattern with state_sync.py) ──────────────────────

def _get_state_db_path(hermes_home: Path | None = None) -> Path:
    """Resolve state.db path for the active profile."""
    if hermes_home is not None:
        return Path(hermes_home).expanduser().resolve() / 'state.db'
    try:
        from api.profiles import get_active_hermes_home
        hermes_home = Path(get_active_hermes_home()).expanduser().resolve()
    except Exception:
        hermes_home = Path(os.getenv('HERMES_HOME', str(HOME / '.hermes'))).expanduser().resolve()
    return hermes_home / 'state.db'


def _get_agent_sessions_from_db(db_path: Path | None = None) -> list:
    """Read all non-webui sessions from state.db.
    Returns list of session dicts, or empty list on any error.
    """
    db_path = Path(db_path) if db_path is not None else _get_state_db_path()
    if not db_path.exists():
        return []

    try:
        sessions = []
        for row in read_importable_agent_session_rows(db_path, limit=200, log=logger):
            sessions.append({
                'session_id': row['id'],
                'title': row['title'] or 'Agent Session',
                'model': row['model'] or None,
                'message_count': row['message_count'] or row['actual_message_count'] or 0,
                'created_at': row['started_at'],
                'updated_at': row['last_activity'] or row['started_at'],
                'source': row['source'] or 'cli',
                'raw_source': row.get('raw_source'),
                'session_source': row.get('session_source'),
                'source_label': row.get('source_label'),
            })
        return sessions
    except Exception:
        return []


# ── GatewayWatcher ──────────────────────────────────────────────────────────

class GatewayWatcher:
    """Background thread that polls state.db for agent session changes.

    Usage:
        watcher = GatewayWatcher()
        watcher.start()
        q = watcher.subscribe()
        # ... receive change events via q.get() ...
        watcher.unsubscribe(q)
        watcher.stop()
    """

    # HERMES_WEBUI_POLL_INTERVAL overrides this per watcher instance; minimum 1s.
    POLL_INTERVAL = 5.0  # seconds between polls
    IDLE_BACKOFF_MULTIPLIER = 4.0
    SUBSCRIBER_TIMEOUT = 30  # seconds before sending keepalive comment

    def __init__(
        self,
        *,
        hermes_home: Path | None = None,
        profile_name: str | None = None,
        state_db_path: Path | None = None,
    ):
        self._subscribers: list[queue.Queue] = []
        self._sub_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._subscriber_wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._hermes_home = Path(hermes_home).expanduser().resolve() if hermes_home else None
        self._state_db_path = (
            Path(state_db_path).expanduser().resolve()
            if state_db_path is not None
            else _get_state_db_path(self._hermes_home) if self._hermes_home is not None else _get_state_db_path()
        )
        self.profile_name = profile_name or ""
        self._poll_interval = _positive_float_from_env(
            "HERMES_WEBUI_POLL_INTERVAL", self.POLL_INTERVAL, minimum=1.0
        )
        self._idle_backoff_multiplier = _positive_float_from_env(
            "HERMES_WEBUI_POLL_IDLE_BACKOFF",
            self.IDLE_BACKOFF_MULTIPLIER,
            minimum=1.0,
        )
        self._last_hash: str = ''
        self._last_sessions: list = []
        # Cheap sessions-only fingerprint from the previous poll. When it is
        # unchanged we skip the expensive messages-JOIN projection entirely
        # (issue #3506). Empty string forces the first poll to run the full read.
        self._last_cheap_fp: str = ''

    def start(self):
        """Start the watcher daemon thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name='gateway-watcher')
        self._thread.start()

    def is_alive(self) -> bool:
        """Return True when the poll thread is running.

        Public accessor used by ``/api/sessions/gateway/stream`` probe mode and
        the live SSE handler to detect a watcher instance whose poll thread
        died silently (e.g. uncaught exception in ``_poll_loop``).  Callers
        use this to decide whether to return 503 and trigger the client-side
        polling fallback, instead of handing out an SSE connection that would
        never emit events.
        """
        t = self._thread
        return t is not None and t.is_alive()

    def stop(self):
        """Stop the watcher thread."""
        self._stop_event.set()
        self._subscriber_wake_event.set()
        # Wake up any subscribers
        with self._sub_lock:
            for q in self._subscribers:
                try:
                    q.put_nowait(None)  # sentinel
                except queue.Full:
                    # Never block profile switching behind a slow SSE client.
                    # Drop one stale event and replace it with the terminal marker.
                    try:
                        q.get_nowait()
                        q.put_nowait(None)
                    except (queue.Empty, queue.Full):
                        pass
                except Exception:
                    logger.debug("Failed to send sentinel to subscriber")
        if self._thread:
            self._thread.join(timeout=3)
            if not self._thread.is_alive():
                self._thread = None

    def subscribe(self) -> queue.Queue:
        """Subscribe to change events. Returns a queue.Queue.
        Events are dicts: {'type': 'sessions_changed', 'sessions': [...]}
        A None sentinel means the watcher is stopping.
        """
        q = queue.Queue(maxsize=10)
        with self._sub_lock:
            self._subscribers.append(q)
            self._subscriber_wake_event.set()
            # Stop-race safety: if stop() already ran (set _stop_event and drained
            # the then-current subscriber list) before we appended, this queue would
            # never receive the sentinel and the SSE loop would hang open with
            # keepalives but no events. Enqueue the sentinel ourselves so the handler
            # closes and reconnects to the live registry watcher. (#3629 / Codex gate)
            if self._stop_event.is_set():
                try:
                    q.put_nowait(None)
                except Exception:
                    logger.debug("Failed to send stop sentinel to late subscriber")
        return q

    def unsubscribe(self, q: queue.Queue):
        """Remove a subscriber queue."""
        with self._sub_lock:
            try:
                self._subscribers.remove(q)
            except ValueError:
                pass

    def _has_subscribers(self) -> bool:
        with self._sub_lock:
            return bool(self._subscribers)

    def _notify_subscribers(self, sessions: list):
        """Push change event to all subscribers."""
        event = {
            'type': 'sessions_changed',
            'sessions': sessions,
        }
        with self._sub_lock:
            dead = []
            for q in self._subscribers:
                try:
                    q.put_nowait(event)
                except queue.Full:
                    dead.append(q)  # remove slow consumers
                except Exception:
                    dead.append(q)
            for q in dead:
                try:
                    self._subscribers.remove(q)
                except ValueError:
                    pass
                # Send a None sentinel so the SSE handler unblocks, closes,
                # and lets the browser's EventSource auto-reconnect.
                try:
                    q.put_nowait(None)
                except Exception:
                    logger.debug("Failed to send sentinel to dead subscriber")

    def _poll_loop(self):
        """Main polling loop. Runs in a daemon thread."""
        while not self._stop_event.is_set():
            has_subscribers = self._has_subscribers()
            try:
                # Phase 1: cheap sessions-only fingerprint. The expensive
                # messages-JOIN projection (_get_agent_sessions_from_db) only
                # runs when this fingerprint actually changes, so an idle server
                # with a large state.db stops re-aggregating tens of thousands
                # of message rows every 5 seconds (issue #3506). A None
                # fingerprint (error / unreadable db) forces the full read so we
                # never silently skip a real change.
                db_path = self._state_db_path
                cheap_fp = _cheap_change_fingerprint(db_path) if db_path.exists() else ''
                if cheap_fp is not None and cheap_fp == self._last_cheap_fp:
                    # Nothing changed in the sidebar-visible session set; skip
                    # the expensive projection and the notify entirely.
                    pass
                else:
                    # Phase 2: only now pay for the full projection.
                    sessions = _get_agent_sessions_from_db(db_path)
                    current_hash = _snapshot_hash(sessions)
                    if cheap_fp is not None:
                        self._last_cheap_fp = cheap_fp

                    if current_hash != self._last_hash:
                        self._last_hash = current_hash
                        self._last_sessions = sessions
                        self._notify_subscribers(sessions)
            except Exception:
                logger.debug("Error in gateway watcher poll loop", exc_info=True)

            # Sleep in small increments so we can stop promptly
            sleep_for = (
                self._poll_interval
                if has_subscribers
                else self._poll_interval * self._idle_backoff_multiplier
            )
            for _ in range(max(1, int(sleep_for * 10))):
                if self._stop_event.is_set():
                    return
                if self._subscriber_wake_event.is_set():
                    self._subscriber_wake_event.clear()
                    break
                time.sleep(0.1)


# ── Module-level watcher registry ──────────────────────────────────────────

_watchers: dict[str, GatewayWatcher] = {}
_watcher_lock = threading.Lock()
_retiring_watchers: set[GatewayWatcher] = set()
_retiring_watcher_lock = threading.Lock()


def _retire_watcher_async(watcher: GatewayWatcher | None) -> None:
    """Stop a replaced watcher off the profile-switch request thread.

    Keep a strong reference until ``stop`` has joined (or timed out) so a live
    retiring watcher/thread is never lost merely because the registry swapped.
    """
    if watcher is None:
        return
    with _retiring_watcher_lock:
        if watcher in _retiring_watchers:
            return
        _retiring_watchers.add(watcher)

    def _retire() -> None:
        try:
            watcher.stop()
        finally:
            with _retiring_watcher_lock:
                _retiring_watchers.discard(watcher)

    threading.Thread(
        target=_retire,
        daemon=True,
        name=f"gateway-watcher-retire-{watcher.profile_name or 'default'}",
    ).start()

def _resolve_watcher_target(
    *,
    profile_name: str | None = None,
    hermes_home: Path | None = None,
) -> tuple[str, Path | None]:
    """Resolve the watcher profile/home pair for the current request context."""
    resolved_profile = str(profile_name or "").strip()
    resolved_home = Path(hermes_home).expanduser().resolve() if hermes_home is not None else None

    try:
        from api.profiles import get_active_profile_name, get_hermes_home_for_profile

        if not resolved_profile:
            resolved_profile = get_active_profile_name() or "default"
        if resolved_home is None and resolved_profile:
            resolved_home = Path(get_hermes_home_for_profile(resolved_profile)).expanduser().resolve()
    except Exception:
        if resolved_home is None:
            try:
                resolved_home = _get_state_db_path().parent.resolve()
            except Exception:
                resolved_home = None

    return resolved_profile, resolved_home


def _watcher_registry_key(profile_name: str | None = None, hermes_home: Path | None = None) -> str:
    """Return the stable registry key for a watcher target."""
    if hermes_home is not None:
        return str(Path(hermes_home).expanduser().resolve())
    return str(profile_name or "").strip() or "__default__"


def _watcher_has_subscribers(watcher: GatewayWatcher) -> bool:
    subscribers = getattr(watcher, "_subscribers", None)
    sub_lock = getattr(watcher, "_sub_lock", None)
    if subscribers is None or sub_lock is None:
        return False
    with sub_lock:
        return bool(subscribers)


def _pop_idle_watchers_locked(*, exclude_key: str) -> list[GatewayWatcher]:
    stale: list[GatewayWatcher] = []
    for key, watcher in list(_watchers.items()):
        if key == exclude_key or _watcher_has_subscribers(watcher):
            continue
        if _watchers.get(key) is watcher:
            stale.append(_watchers.pop(key))
    return stale


def start_watcher(*, profile_name: str | None = None, hermes_home: Path | None = None):
    """Start the watcher for the resolved profile home (idempotent)."""
    resolved_profile, resolved_home = _resolve_watcher_target(
        profile_name=profile_name,
        hermes_home=hermes_home,
    )
    key = _watcher_registry_key(resolved_profile, resolved_home)
    retiring = None
    with _watcher_lock:
        watcher = _watchers.get(key)
        if watcher is None or not watcher.is_alive():
            retiring = watcher
            watcher = GatewayWatcher(profile_name=resolved_profile, hermes_home=resolved_home)
            watcher.start()
            _watchers[key] = watcher
    _retire_watcher_async(retiring)
    return watcher


def stop_watcher(*, profile_name: str | None = None, hermes_home: Path | None = None):
    """Stop either one profile watcher or the entire registry."""
    with _watcher_lock:
        if profile_name is None and hermes_home is None:
            watchers = list(_watchers.values())
            _watchers.clear()
        else:
            resolved_profile, resolved_home = _resolve_watcher_target(
                profile_name=profile_name,
                hermes_home=hermes_home,
            )
            key = _watcher_registry_key(resolved_profile, resolved_home)
            watcher = _watchers.pop(key, None)
            watchers = [watcher] if watcher is not None else []
    for watcher in watchers:
        watcher.stop()


def restart_watcher_for_profile(name: str):
    """Restart only the watcher pinned to the target profile home."""
    from api.profiles import get_hermes_home_for_profile

    hermes_home = Path(get_hermes_home_for_profile(name)).expanduser().resolve()
    key = _watcher_registry_key(name, hermes_home)
    watcher = GatewayWatcher(profile_name=name, hermes_home=hermes_home)
    watcher.start()
    with _watcher_lock:
        existing = _watchers.pop(key, None)
        stale_watchers = [] if existing is not None else _pop_idle_watchers_locked(exclude_key=key)
        _watchers[key] = watcher
    for old_watcher in ([existing] if existing is not None else stale_watchers):
        _retire_watcher_async(old_watcher)
    return watcher


def get_watcher(*, profile_name: str | None = None, hermes_home: Path | None = None) -> GatewayWatcher | None:
    """Get or lazily start the watcher for the resolved request profile."""
    resolved_profile, resolved_home = _resolve_watcher_target(
        profile_name=profile_name,
        hermes_home=hermes_home,
    )
    key = _watcher_registry_key(resolved_profile, resolved_home)
    with _watcher_lock:
        watcher = _watchers.get(key)
    if watcher is None or not watcher.is_alive():
        watcher = start_watcher(profile_name=resolved_profile, hermes_home=resolved_home)
    return watcher
