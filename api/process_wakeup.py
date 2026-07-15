"""Classification helpers for durable synthetic process-wakeup prompts.

Hermes state.db message rows do not carry WebUI's ``_source`` metadata.  These
anchored prefixes are the narrow compatibility seam used to recover provenance
for prompts emitted by the process registry.  Keep this list aligned with the
legacy pending-placeholder guard in ``static/ui.js``.
"""

from __future__ import annotations


PROCESS_WAKEUP_CONTROL_PREFIXES = (
    "[IMPORTANT: Background process ",
    "[ASYNC DELEGATION ",
    # Legacy process-registry watch-overflow/disable notifications.  These were
    # already filtered as internal pending placeholders by the frontend.
    "[IMPORTANT: Watch-pattern ",
    "[IMPORTANT: Watch patterns ",
)


def is_process_wakeup_control_text(value: object) -> bool:
    """Return whether *value* starts with a deterministic Hermes wakeup marker.

    This intentionally uses anchored prefixes rather than substring matching:
    ordinary user prose that quotes or discusses a marker later in the message
    must remain human-authored transcript content.
    """
    if not isinstance(value, str):
        return False
    return value.lstrip().startswith(PROCESS_WAKEUP_CONTROL_PREFIXES)
