# Core Bridge: the text-protocol fallback for existing Core tools

Base: `27ee321c` plus the uncommitted OpenCode-jobs work (same shared tree). Main process +
shared contract + renderer Settings + tests. The MCP Core connector is untouched and remains
canonical.

## Change

A conversation that cannot reach the MCP Core connector can still run the *existing* Core
tools through an explicit text protocol carried by the ordinary conversation:

final assistant answer → `<COS_CORE_CALL>` (≤8 calls) → real kernel registrar →
`<COS_CORE_RESULT>` → new after-turn outbox input → same conversation, next turn.

- `src/shared/core-bridge.ts` (new): the protocol. One exact call block, strict zod shape,
  bounded body; results frame per-call `ok`/`content`/`error` with head/tail clipping
  (reusing `clipStreamText`) and a halving loop under a 60,000-char budget inside the
  outbox's 96,000 ceiling. A `<COS_CORE_RESULT>` can never parse as a request (own-opening-tag
  match only), and result markers, second blocks or nested markers are rejected as invalid.
  Also owns `CORE_BRIDGE_INSTRUCTIONS`, the bounded paragraph added to eligible openings.
  Its request example now carries read's exact `{"paths":["/<root>/folder"]}` array shape
  plus one bounded line naming every bridge tool's real argument keys (strict zod refuses
  wrong/extra keys); the first live bridge run (same day) showed a model guessing the
  singular `"path"` from a bare `args:{}` example and eating a full `INVALID_ARGUMENTS`
  round-trip before retrying with `paths`.
- `src/main/core-bridge.ts` (new): the decisions. Detection is the recorder's new
  final-answer listener (one event per final `assistant_message` actually written — no
  streaming text, no replays, no tool results). Gates read live: `coreBridge.enabled`, the
  session's own current conversation, worker/helper origins unsupported; then the durable
  `core-bridge-requests` ledger is reserved *before* execution (a crash can lose a request,
  never re-run a patch/command). Repeat UUIDs ignored; invalid blocks answered once per
  message with a rejected result. Per-session sequential batches.
- Kernel: first-class `invokeLocal` on the registrar + `LocalCallSeed` on `dispatch` — a
  top-level (never nested) dispatch whose caller is seeded with the exact session/conversation
  and the `core_bridge` transport. Seeded calls skip the request-id ingress join, and count as
  neither tunnel proof (`noteConnectorUse`) nor connector use (`surfaceToolCallAt`/
  transport identity). Every other dispatcher guard runs unchanged.
- Recording: `CallAttribution`/`attributionMethod` gained `core_bridge` (label "the local
  Core Bridge"); bridge rows are filed on the exact session with event source `app`, no
  request id, and are excluded from every "this chat made an MCP call" join, so bridge
  execution grants no silence/Goal recovery authority.
- Bridge surface: `read`, `view_image`, `save_image`, `find`, `apply_patch`, `exec_command`,
  `write_stdin` only. `agents`/`session_finish`/`update_plan` refused by name
  (MCP-turn contracts); code-mode `exec` not registered here; unknown/Desktop/Plugins names
  get the surface's own unknown-tool refusal. Writes/commands additionally need
  `coreBridge.allowActions` (explicit guard, default off, repaired off while the bridge is
  off — same shape and same rules as the control API pair).
- Delivery: `sendDesktopInput` with `mode:'after-turn'`, `authoredSource:'none'` — the
  OpenCode bridge lesson; no second send path, no retry timer, result follows Compact & Resume.
- Prompt: `prepareSessionPrompt` appends the bridge paragraph inside the existing
  COS_CONTEXT frame only while enabled; `currentCoreInstructions` and MCP initialize never
  mention it.
- Settings: `coreBridge { enabled, allowActions }` in config schema + three-way merge +
  preload patch type + the two For developers checkboxes ("Local Core bridge", "Allow bridge
  actions", disabled until the bridge is on).
- AGENTS.md: §3 baseline row, §4 owner row, new §6 contract section, §12 attribution note.

## Validation

- `npm run typecheck` clean; `npm run build` clean.
- `test/core-bridge.test.ts` (25): real recorder → real bridge → real kernel → real outbox.
  Covers: final-answer execution of the canonical `read` (with `core_bridge` attribution,
  source `app`), streaming/normal/result/worker/disabled silence, duplicate UUID (same
  message, new message, ledger across a fresh install), sequential batches (real child
  processes appending in order), >8 rejected, per-call refusals for
  unknown/lifecycle/Desktop/Plugins names, sandbox outside-roots and traversal refusals,
  disabled-read existing answers, Read-only (no write tool even offered), command allowlist
  `COMMAND_NOT_ALLOWED`, the bridge action guard, relative paths resolved against the
  originating session's project, bounded output with omission counts, after-turn new-input
  delivery after the turn ended, originating-session-only delivery, and two unchanged-native
  fences (an exact-request MCP call still files as `request_id`/`mcp`; the switches load off
  and the guard never outlives the bridge).
- `test/session-prompt.test.ts` (9): the bridge paragraph appears in the opening frame only
  while enabled, after the complete normal Core instructions; MCP instructions unchanged.
- Existing suites with the change: `test/mcp.test.ts`, `correlation`, `call-context`,
  `mcp-inflight`, `mcp-tool-declarations` (237 passed); input/outbox family
  (`session-input`, `session-input-delivery`, `input-delivery-integration`,
  `finish-input-integration`), `config`, the three control-api suites and both OpenCode
  suites (741 passed); recorder identity family (`backend-recording`,
  `session-response-identity`, `recorder-final-identity`, `attribution-repair`,
  `chronology`, `session`) (269 passed).
- The 5 failures in `test/mcp.test.ts`/`test/code-mode-mcp.test.ts` (process-custody cases)
  were re-reproduced on the stashed clean base tree — pre-existing on this machine, not from
  this change.

## Follow-up, same day: guided args, and result delivery that actually reaches the page

- **Guided arguments.** The instructions showed only a bare `args:{}` example; the first
  live run produced `read` with the singular `"path"` and the kernel's strict zod guard
  refused it (`INVALID_ARGUMENTS`). `CORE_BRIDGE_INSTRUCTIONS` now shows read's exact
  `{"paths":["/<root>/folder"]}` array plus one bounded line naming every bridge tool's
  real argument keys, taken from the real `tools-core.ts` schemas.
- **Delivery deadlock fixed.** The result row was enqueued as a plain after-turn input
  *after* its source turn's `turn_end` was already recorded, so the outbox's
  later-completion rule (`eligibleStageEnd`) never released it and the bridge's own
  recorded calls were vetoing `readCompletedFinal` and advancing `lastToolCallAt` — the
  answer sat queued until an unrelated user message created a second completion. Fixes:
  `FinalAssistantMessage` now carries the final's canonical `turnId`; the bridge names
  that exact source turn through the outbox's existing `queuedTurn` mechanism
  (`sendDesktopInput`/`enqueueInput` `waitFor` parameter); and the store no longer moves
  the chat work clock or vetoes the completed-final check for `core_bridge` rows (they
  are the answer being prepared, never fresh chat work).
- Tests: `test/core-bridge.test.ts` grew a `delivery` describe — the positive case
  (source turn completed, row carries `queuedTurn`, `pendingBrowserInputs` offers it,
  `claimBrowserInput` hands it out) and the neighbouring negative (a still-active source
  turn is never interrupted). 27/27 in the file; 838 in the input/finish/goal/recorder
  net; 779 in bridge/usage/handoff/finish/startup; the 3 `test/mcp.test.ts` process
  failures are the pre-existing Homebrew `libsimdutf` breakage noted above.

## Known limits

- Workers/helpers unsupported by design (origin gate); their final answers execute nothing.
- A delivery failure (outbox refuses the result row) leaves the work recorded and the answer
  unqueued; the ledger keeps the request from re-executing, so the model must issue a new
  request id.
- Code-mode `exec` is not offered through the bridge; the native Core connector remains the
  way to use it. Image tool output is named as omitted in the result, never carried as base64.
