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
    reason = _clean_text(fallback_source.get("reason"), 40).lower()
    if reason not in _RUNTIME_ROUTING_REASONS:
        reason = "unknown"
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
            "reason": reason,
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
