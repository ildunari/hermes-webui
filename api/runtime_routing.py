"""Display-safe normalization for Hermes runtime route lifecycle events."""

from __future__ import annotations

from typing import Any


_RUNTIME_ROUTING_STATES = frozenset(
    {"started", "fallback_activated", "primary_restored", "finished"}
)


def _clean_text(value: Any, limit: int = 240) -> str:
    return str(value or "").strip()[:limit]


def _route_lane(value: Any) -> dict[str, str]:
    source = value if isinstance(value, dict) else {}
    return {
        "model": _clean_text(source.get("model")),
        "provider": _clean_text(source.get("provider"), 120),
    }


def normalize_runtime_routing_payload(payload: Any) -> dict[str, Any] | None:
    """Return the committed schema-v1 presentation payload, or ``None``.

    The allowlist deliberately drops unknown fields so callback or Gateway data
    cannot leak credentials/provider internals into the journal or browser.
    """
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        return None
    state = _clean_text(payload.get("state"), 40).lower()
    if state not in _RUNTIME_ROUTING_STATES:
        return None
    selected = _route_lane(payload.get("selected"))
    runtime = _route_lane(payload.get("runtime"))
    raw_fallback = payload.get("fallback")
    fallback_source: dict[str, Any] = raw_fallback if isinstance(raw_fallback, dict) else {}
    try:
        chain_index = int(fallback_source.get("chain_index") or 0)
    except (TypeError, ValueError):
        chain_index = 0
    return {
        "schema_version": 1,
        "state": state,
        "selected": selected,
        "runtime": runtime,
        "fallback": {
            "active": bool(fallback_source.get("active")),
            "reason": _clean_text(fallback_source.get("reason"), 500),
            "chain_index": max(0, chain_index),
        },
    }


def attach_runtime_routing_summary(session: Any, payload: Any) -> dict[str, Any] | None:
    """Attach one settled route summary to the session and last assistant row."""
    normalized = normalize_runtime_routing_payload(payload)
    if normalized is None:
        return None
    session.runtime_routing = normalized
    for message in reversed(getattr(session, "messages", None) or []):
        if isinstance(message, dict) and message.get("role") == "assistant":
            message["_runtimeRouting"] = normalized
            break
    return normalized
