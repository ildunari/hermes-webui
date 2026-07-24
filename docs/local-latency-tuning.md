# Local sidebar and profile-switch latency tuning

This fork carries three opt-in backend changes for Kosta's shared WebUI,
Desktop dashboard, and Hermex/iOS session/profile surfaces:

- `HERMES_WEBUI_STATEDB_BUSY_MS`, `HERMES_WEBUI_STATEDB_MMAP_MB`,
  `HERMES_WEBUI_STATEDB_CACHE_MB`, and
  `HERMES_WEBUI_STATEDB_QUERY_ONLY` tune only WebUI read connections to the
  live Hermes `state.db`. They do not write, checkpoint, vacuum, or migrate it.
- `HERMES_WEBUI_DEFER_SKILL_STATS=1` replaces the recursive per-request skill
  mtime walk with `config.yaml` plus top-level `skills/` stats. Parsed counts
  refresh in one background worker per profile and may lag by the existing
  five-minute TTL after a nested-only edit.
- `HERMES_WEBUI_BACKGROUND_UPDATE_CHECKS=1` makes GET and POST update-check
  requests return cached status immediately while one coalesced daemon thread
  performs the existing network/git refresh. A forced manual check still forces
  that background refresh.

All variables default off, preserving upstream behavior when unset. The
existing `HERMES_WEBUI_STATE_DB_OVERRIDE_TOP_N` default/cap is unchanged.

Mac Studio activation values:

```text
HERMES_WEBUI_STATEDB_BUSY_MS=3000
HERMES_WEBUI_STATEDB_MMAP_MB=2048
HERMES_WEBUI_STATEDB_CACHE_MB=16
HERMES_WEBUI_STATEDB_QUERY_ONLY=1
HERMES_WEBUI_DEFER_SKILL_STATS=1
HERMES_WEBUI_BACKGROUND_UPDATE_CHECKS=1
```

Rollback is to remove those six environment variables and safely restart the
affected backend. No database cleanup or reversal is required.
