"""Display-safe normalization for Hermes runtime route lifecycle events."""

from __future__ import annotations

import re
from typing import Any


_RUNTIME_ROUTING_STATES = frozenset(
    {"started", "fallback_activated", "primary_restored", "finished"}
)
_RUNTIME_ROUTING_REASONS = frozenset(
    {
        "rate_limit",
        "billing",
        "authentication",
        "provider_unavailable",
        "timeout",
        "upstream_error",
        "context_limit",
        "non_retryable_error",
        "unknown",
    }
)
# Keep Desktop's strict integer contract while bounding hostile telemetry. A
# maximum of 100 is deliberately generous for any practical fallback chain.
_MAX_RUNTIME_ROUTING_CHAIN_INDEX = 100
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_URL_RE = re.compile(r"(?i)\b(?:https?|wss?)://[^\s<>'\"]+")
_BEARER_RE = re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(?:api[_-]?key|access[_-]?token|auth(?:orization)?|password|secret)"
    r"\s*[:=]\s*[^\s,;]+"
)
_SECRET_TOKEN_RE = re.compile(
    r"\b(?:sk|pk|rk|ghp|github_pat|xox[baprs])-[_A-Za-z0-9]{12,}\b",
    re.IGNORECASE,
)


def _clean_text(value: Any, limit: int = 240) -> str:
    text = str(value or "")
    text = _CONTROL_CHARS_RE.sub("", text)
    text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = _URL_RE.sub("[redacted-url]", text)
    text = _BEARER_RE.sub("[redacted-secret]", text)
    text = _SECRET_ASSIGNMENT_RE.sub("[redacted-secret]", text)
    text = _SECRET_TOKEN_RE.sub("[redacted-secret]", text)
    return " ".join(text.split()).strip()[:limit]


def _route_lane(value: Any) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    model = value.get("model")
    provider = value.get("provider")
    if not isinstance(model, str) or not model.strip():
        return None
    if not isinstance(provider, str) or not provider.strip():
        return None
    cleaned_model = _clean_text(model)
    cleaned_provider = _clean_text(provider, 120)
    if not cleaned_model or not cleaned_provider:
        return None
    return {"model": cleaned_model, "provider": cleaned_provider}


def normalize_runtime_routing_payload(payload: Any) -> dict[str, Any] | None:
    """Return a strict, display-safe schema-v1 payload, or ``None``.

    The allowlist deliberately drops unknown fields so callback or Gateway data
    cannot leak credentials/provider internals into the journal or browser.
    Malformed required fields are rejected rather than coerced.
    """
    if (
        not isinstance(payload, dict)
        or isinstance(payload.get("schema_version"), bool)
        or payload.get("schema_version") != 1
    ):
        return None
    raw_state = payload.get("state")
    if not isinstance(raw_state, str) or not raw_state.strip():
        return None
    state = _clean_text(raw_state, 40).lower()
    if state not in _RUNTIME_ROUTING_STATES:
        return None
    selected = _route_lane(payload.get("selected"))
    runtime = _route_lane(payload.get("runtime"))
    if selected is None or runtime is None:
        return None
    fallback = payload.get("fallback")
    if not isinstance(fallback, dict):
        return None
    active = fallback.get("active")
    raw_reason = fallback.get("reason")
    chain_index = fallback.get("chain_index")
    if not isinstance(active, bool):
        return None
    if not isinstance(raw_reason, str) or not raw_reason.strip():
        return None
    reason = _clean_text(raw_reason, 40).lower()
    if reason not in _RUNTIME_ROUTING_REASONS:
        return None
    if (
        isinstance(chain_index, bool)
        or not isinstance(chain_index, int)
        or not 0 <= chain_index <= _MAX_RUNTIME_ROUTING_CHAIN_INDEX
    ):
        return None
    return {
        "schema_version": 1,
        "state": state,
        "selected": selected,
        "runtime": runtime,
        "fallback": {
            "active": active,
            "reason": reason,
            "chain_index": chain_index,
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


def settle_runtime_routing_summary(session: Any, payload: Any) -> dict[str, Any] | None:
    """Set the latest-response summary, clearing stale legacy-backend state.

    Per-message routing metadata is historical evidence and is deliberately left
    untouched when a successful response has no schema-v1 routing payload.
    """
    normalized = normalize_runtime_routing_payload(payload)
    if normalized is None:
        session.runtime_routing = None
        return None
    return attach_runtime_routing_summary(session, normalized)
