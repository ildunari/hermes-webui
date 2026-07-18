"""Profile-scoped durable delivery for WebUI async-delegation completions.

The Agent completion queue is process-wide. The next-turn streaming drain never
claims async-delegation events; it requeues them for the idle/background drain,
which is the single durable claim/ack/release boundary.
"""

from __future__ import annotations

import importlib
import logging
import queue
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WebUIDeliveryOwner:
    session_id: str
    profile_name: str
    profile_home: Path


@dataclass(frozen=True)
class DurableDeliveryClaim:
    event: dict
    owner: WebUIDeliveryOwner
    claim_id: str = ""
    durable: bool = False


@dataclass(frozen=True)
class _AgentDeliveryAPI:
    module: Any
    claim: Any
    complete: Any
    release: Any
    get: Any = None


def _event_id(evt: dict) -> str:
    return str((evt or {}).get("delegation_id") or "")


def restore_all_profile_durable_delegations(target_queue=None) -> int:
    """Restore pending Agent delegation rows from every available profile home.

    Agent's process-registry singleton restores only the ``HERMES_HOME`` that
    was active when it was imported.  WebUI is multi-profile in one process, so
    startup must explicitly visit every profile database before its sole drain
    thread starts.  Homes are canonicalized to collapse the root profile's
    ``default`` and renamed aliases; delegation ids are also deduplicated
    against both earlier profile scans and events already restored by Agent.
    """
    try:
        from api.profiles import (
            get_active_profile_name,
            get_hermes_home_for_profile,
            list_profiles_api,
        )
        from hermes_constants import (
            reset_hermes_home_override,
            set_hermes_home_override,
        )
        from tools.async_delegation import restore_undelivered_completions
        from tools.process_registry import process_registry
    except (ImportError, AttributeError):
        return 0

    destination = target_queue or getattr(process_registry, "completion_queue", None)
    if destination is None:
        return 0

    existing_ids: set[str] = set()
    # Startup runs before WebUI's consumer thread, but Agent may already have
    # populated this queue while constructing its singleton.  Snapshot through
    # queue.Queue's mutex when available instead of destructively draining it.
    mutex = getattr(destination, "mutex", None)
    queued = getattr(destination, "queue", None)
    if mutex is not None and queued is not None:
        with mutex:
            existing_ids.update(
                _event_id(item) for item in list(queued) if isinstance(item, dict)
            )

    candidates: list[tuple[str, Path]] = []

    def _add_candidate(name: str, raw_home: Any = None) -> None:
        clean_name = str(name or "").strip() or "default"
        try:
            home = Path(
                raw_home if raw_home else get_hermes_home_for_profile(clean_name)
            ).expanduser()
        except Exception:
            logger.warning(
                "Cannot resolve profile %r during durable delegation restore",
                clean_name,
                exc_info=True,
            )
            return
        candidates.append((clean_name, home))

    _add_candidate("default")
    try:
        _add_candidate(get_active_profile_name())
    except Exception:
        logger.debug("Cannot resolve active profile during delegation restore", exc_info=True)
    try:
        rows = list_profiles_api()
    except Exception:
        logger.warning("Cannot enumerate profiles for durable delegation restore", exc_info=True)
        rows = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        _add_candidate(str(row.get("name") or ""), row.get("path"))

    seen_homes: set[str] = set()
    restored = 0
    for profile_name, home in candidates:
        try:
            home_key = str(home.resolve(strict=False))
        except OSError:
            home_key = str(home.absolute())
        if home_key in seen_homes:
            continue
        seen_homes.add(home_key)

        staged: queue.Queue = queue.Queue()
        token = set_hermes_home_override(home)
        try:
            restore_undelivered_completions(staged)
        except Exception:
            logger.warning(
                "Failed to restore durable delegations for profile %s (%s)",
                profile_name,
                home,
                exc_info=True,
            )
            continue
        finally:
            reset_hermes_home_override(token)

        while True:
            try:
                event = staged.get_nowait()
            except queue.Empty:
                break
            if not isinstance(event, dict):
                continue
            event_id = _event_id(event)
            if event_id and event_id in existing_ids:
                continue
            if event_id:
                existing_ids.add(event_id)
            destination.put(event)
            restored += 1
    if restored:
        logger.info(
            "Restored %d pending durable delegation(s) across %d profile home(s)",
            restored,
            len(seen_homes),
        )
    return restored


def resolve_webui_delivery_owner(
    evt: dict,
    *,
    expected_session_id: str | None = None,
) -> WebUIDeliveryOwner | None:
    """Return the positively-proven WebUI session/profile that owns ``evt``.

    A process-local ``PROCESS_SESSION_INDEX`` registration proves ownership for
    live events.  Restored events have no such registration after restart, so a
    real WebUI session sidecar whose id exactly matches ``session_key`` is the
    durable proof.  Merely receiving an arbitrary non-empty ``session_key`` is
    never enough.  When Agent supplies ``origin_ui_session_id`` it must also
    agree, closing the cross-UI collision path for newly-created events.
    """
    if not isinstance(evt, dict) or evt.get("type") != "async_delegation":
        return None
    event_session_key = str(evt.get("session_key") or "").strip()
    expected = str(expected_session_id or "").strip()
    if not event_session_key or (expected and expected != event_session_key):
        return None
    session_id = expected or event_session_key
    origin_ui_session_id = str(evt.get("origin_ui_session_id") or "").strip()
    if origin_ui_session_id and origin_ui_session_id != session_id:
        return None

    process_registration_proves_owner = False
    try:
        from api import config as cfg

        with cfg.PROCESS_SESSION_INDEX_LOCK:
            process_registration_proves_owner = (
                str(cfg.PROCESS_SESSION_INDEX.get(event_session_key) or "") == session_id
            )
    except Exception:
        logger.debug("Failed to inspect WebUI process-session ownership", exc_info=True)

    session = None
    try:
        from api.models import get_session

        session = get_session(session_id, metadata_only=True)
        if str(getattr(session, "session_id", "") or "") != session_id:
            session = None
    except (KeyError, ValueError, OSError):
        session = None
    except Exception:
        logger.debug(
            "Failed to resolve WebUI session owner for async delegation %s",
            _event_id(evt),
            exc_info=True,
        )
        session = None

    if session is None and not process_registration_proves_owner:
        return None
    # Durable Agent rows must always be scoped from the persisted owning
    # session. A live registration alone is sufficient for legacy/non-durable
    # events, but it cannot safely choose among profile state.db files.
    if session is None and evt.get("restored"):
        return None

    profile_name = str(getattr(session, "profile", "") or "").strip() or "default"
    try:
        from api.profiles import get_hermes_home_for_profile

        profile_home = Path(get_hermes_home_for_profile(profile_name)).expanduser()
    except Exception:
        logger.warning(
            "Cannot resolve owning profile for async delegation %s session %s",
            _event_id(evt),
            session_id,
            exc_info=True,
        )
        return None
    return WebUIDeliveryOwner(session_id, profile_name, profile_home)


def _agent_delivery_api() -> _AgentDeliveryAPI | None | bool:
    """Return full Agent delivery API, ``False`` for legacy, ``None`` partial.

    All three state transitions are one compatibility unit.  Treating a module
    with claim but no acknowledgement/release as legacy would claim a durable
    row that WebUI can neither finish nor safely retry.
    """
    try:
        module = importlib.import_module("tools.async_delegation")
    except ImportError:
        return False
    claim = getattr(module, "claim_event_delivery", None)
    complete = getattr(module, "complete_event_delivery", None)
    release = getattr(module, "release_event_delivery", None)
    present = tuple(callable(fn) for fn in (claim, complete, release))
    if not any(present):
        return False
    if not all(present):
        logger.warning("Hermes Agent durable-delivery API is partial; refusing delivery")
        return None
    getter = getattr(module, "get_durable_delegation", None)
    return _AgentDeliveryAPI(
        module=module,
        claim=claim,
        complete=complete,
        release=release,
        get=getter if callable(getter) else None,
    )


@contextmanager
def _owning_agent_home(owner: WebUIDeliveryOwner) -> Iterator[None]:
    """Pin Agent ``get_hermes_home()`` to the session owner's profile home."""
    from api.profiles import profile_scope_for_detached_worker

    with profile_scope_for_detached_worker(
        owner.profile_name,
        "WebUI durable delegation delivery",
        logger_override=logger,
    ):
        try:
            from hermes_constants import (
                reset_hermes_home_override,
                set_hermes_home_override,
            )
        except ImportError:
            # Older Agents do not expose a context-local override. Named profile
            # scope still mirrors HERMES_HOME; root profile remains its normal
            # process home in those versions.
            yield
            return
        token = set_hermes_home_override(owner.profile_home)
        try:
            yield
        finally:
            reset_hermes_home_override(token)


def claim_webui_delivery(
    evt: dict,
    *,
    expected_session_id: str | None = None,
) -> DurableDeliveryClaim | None:
    """Positively resolve and claim one async-delegation event for WebUI.

    ``None`` means WebUI must not inject the event.  A returned claim with
    ``durable=False`` is a legacy Agent event and preserves historical in-memory
    notification semantics.
    """
    owner = resolve_webui_delivery_owner(evt, expected_session_id=expected_session_id)
    if owner is None:
        return None
    api = _agent_delivery_api()
    if api is False:
        return DurableDeliveryClaim(dict(evt), owner)
    if not isinstance(api, _AgentDeliveryAPI):
        return None

    delegation_id = _event_id(evt)
    if not delegation_id:
        return DurableDeliveryClaim(dict(evt), owner)
    try:
        with _owning_agent_home(owner):
            durable_row = api.get(delegation_id) if api.get is not None else None
            # A restored event necessarily came from a durable database. If the
            # row is absent in this profile, this is the wrong profile/owner;
            # claim_completion_delivery's legacy-row success must not bypass it.
            if evt.get("restored") and api.get is not None and durable_row is None:
                return None
            # A live event with no row was emitted by a legacy/non-durable
            # producer sharing this newer consumer module. Preserve historical
            # one-shot process notification semantics; do not manufacture a
            # claim token whose acknowledgement can never update a row.
            if api.get is not None and durable_row is None:
                return DurableDeliveryClaim(dict(evt), owner)
            claim_id = api.claim(evt, f"webui:{owner.session_id}")
    except Exception:
        logger.warning(
            "Durable async-delegation claim failed for %s in profile %s",
            delegation_id,
            owner.profile_name,
            exc_info=True,
        )
        return None
    if claim_id is None:
        return None
    return DurableDeliveryClaim(
        dict(evt),
        owner,
        claim_id=str(claim_id or ""),
        durable=bool(claim_id),
    )


def complete_webui_delivery(claim: DurableDeliveryClaim | None) -> bool:
    """Acknowledge a claimed row and positively verify its delivered state."""
    if claim is None or not claim.durable:
        return True
    api = _agent_delivery_api()
    if not isinstance(api, _AgentDeliveryAPI):
        return False
    delegation_id = _event_id(claim.event)
    try:
        with _owning_agent_home(claim.owner):
            api.complete(claim.event, claim.claim_id)
            if api.get is None:
                # Older complete_event_delivery has no result contract. The call
                # returned successfully, which is the strongest available proof.
                return True
            row = api.get(delegation_id)
            return bool(row and row.get("delivery_state") == "delivered")
    except Exception:
        logger.warning(
            "Durable async-delegation acknowledgement failed for %s in profile %s",
            delegation_id,
            claim.owner.profile_name,
            exc_info=True,
        )
        return False


def recover_pending_webui_process_wakeups() -> int:
    """Resume every WebUI-local wakeup that never reached worker start.

    This runs before Agent rows are restored into the background drain. A
    pending record owns the complete resolved launch payload, so recovery does
    not depend on the Agent claim still being held or on a runner journal.
    ``started``/``completed`` rows are deliberately not relaunched.
    """
    from api.process_wakeup_store import list_pending_process_wakeups

    try:
        records = list_pending_process_wakeups()
    except Exception:
        logger.warning("Failed to enumerate pending WebUI process wakeups", exc_info=True)
        return 0
    resumed = 0
    for record in records:
        try:
            from api.routes import resume_pending_process_wakeup

            response = resume_pending_process_wakeup(record)
            status = int((response or {}).get("_status", 200) or 200)
            if status < 400:
                resumed += 1
            else:
                logger.warning(
                    "Pending WebUI process wakeup %s was not resumed: status=%s error=%r",
                    record.wakeup_id,
                    status,
                    (response or {}).get("error"),
                )
        except Exception:
            logger.warning(
                "Failed to resume pending WebUI process wakeup %s",
                record.wakeup_id,
                exc_info=True,
            )
    return resumed


def release_webui_delivery(claim: DurableDeliveryClaim | None) -> bool:
    """Release a claim after failed/volatile delivery so it can be retried."""
    if claim is None or not claim.durable:
        return True
    api = _agent_delivery_api()
    if not isinstance(api, _AgentDeliveryAPI):
        return False
    try:
        with _owning_agent_home(claim.owner):
            api.release(claim.event, claim.claim_id)
        return True
    except Exception:
        logger.warning(
            "Durable async-delegation claim release failed for %s in profile %s",
            _event_id(claim.event),
            claim.owner.profile_name,
            exc_info=True,
        )
        return False
