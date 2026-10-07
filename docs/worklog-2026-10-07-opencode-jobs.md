# OpenCode jobs through the local control API

Base: `27ee321c`, branch `main` (shared tree). Main process + shared contract + tests.

## Change

A conversation can now delegate a task to the local OpenCode CLI and get the answer back in
that same chat, with no new MCP tool and no browser-side parsing:

ChatGPT conversation → explicit controller request → `POST /v1/opencode/jobs` →
`opencode run` (argv only) → approved-root folder → `<COS_LOCAL_RESULT>` payload →
existing outbox (`sendDesktopInput`, after-turn row) → original conversation.

- `src/shared/local-task.ts` (new): the `<COS_LOCAL_TASK>`/`<COS_LOCAL_RESULT>` protocol.
  `parseLocalTask` matches only task blocks, so a result can never parse as a task;
  `localResultPayload` frames the result and clips stdout/stderr head/tail inside a
  60,000-character budget (under the outbox's 96,000 ceiling), halving until it fits;
  `clipStreamText` is the one clipper both the payload and the job view use.
- `src/main/opencode.ts` (new): the executor and its bounded process-memory registry
  (4 concurrent jobs, 64 terminal retained; a restart is a lost listener, like
  browser-control's claims). Spawn is argv-only through `prepareCommand` (`shell:false`,
  POSIX process-group leader); flags verified against the installed OpenCode 1.18.34
  (`run --dir <real> --format json [--model <slug>] -- <task>`). Admission: session exists,
  not a worker/helper chat, has a conversation; `cwd` through `resolvePath` against the
  approved roots and must be a folder; task plain text, non-flag-shaped, marker-free
  (≤16,000 chars); model a `provider/model` slug or null for OpenCode's own default
  (OpenCode keeps its credentials; this app never touches provider keys). Output collected
  in the terminal tools' `HeadTailBuffer` (1 MiB per stream). Cancel kills the tree via
  `terminateProcessTree` and answers the settled truth. Terminal jobs deliver one
  `<COS_LOCAL_RESULT>` to the originating session as an after-turn row — never interrupts an
  answer, queues behind a busy chat, follows a Compact & Resume rebind — recording
  `resultInputId` or a bounded `resultError`; no retry timer.
- Control API: `POST /v1/opencode/jobs` (202; optional caller UUID for at-most-once replay,
  409 on a same-id different request), `GET /v1/opencode/jobs/{id}` (allowlisted projection,
  virtual cwd only, redacted channels), `POST /v1/opencode/jobs/{id}/cancel`. All POSTs are
  action paths behind `controlApi.allowActions` (same 403-before-reading refusal), permits
  rechecked at the last point before spawn/kill; shutdown's process-cleanup phase stops the
  runtime (`index.ts`).
- `src/shared/control-api.ts`: routes + `ControlApiOpenCodeJob{,Created,Cancel}` types;
  health lists the new routes. AGENTS.md §4/§18 updated to match.

## Validation

- `npm run typecheck` clean.
- New suites: `test/opencode.test.ts` (27: argv shape with the default command, `shell:false`,
  task verbatim as one argument, sandbox refusals incl. symlink escape, session gates,
  task/model fences, replay/conflict, 5th-job cap, payload/view caps on fake and real
  chatty children, spawn-error and nonzero-exit results, real-process cancel of a live
  child, two concurrent jobs, shutdown sweep isolation, protocol parsing incl. result-never-a-task)
  and `test/control-api-opencode.test.ts` (20: both switches, method/path answers, id
  spelling, create/get/cancel over HTTP, originating-session-only delivery through the
  outbox with the right wake URL, switch-flip mid-job, caps over HTTP, result-echo task
  refused).
- Existing control API family updated for the route lists: `test/control-api{,-reads,-actions}.test.ts`
  all green (169 with the new suites).
- Full `vitest run` (excl. `computer`/`mcp-shutdown`): 7945 passed; the 6 remaining failures
  (`test/exec.test.ts` PATH count, `test/mcp.test.ts`, `test/code-mode-mcp.test.ts`,
  `test/agents.test.ts` process-custody cases) were reproduced on a stashed clean tree —
  pre-existing on this machine, not from this change. `verify:privacy` passes.
- Live flags checked against the installed OpenCode 1.18.34 (`opencode run --help`), and
  `--` separator behaviour probed with a missing `--dir` (no model call spent).

## Known limits

- Job records live in process memory: an app restart loses running and terminal jobs
  (documented in AGENTS.md §18); the controller sees a dropped listener, like browser-control.
- Result delivery is once at job end; a refused enqueue (deleted/blocked chat, queue full)
  leaves `resultError` on the job — the controller can re-send the retained bytes through
  `POST /v1/inputs`.
- Windows npm `.cmd` installs of OpenCode go through the existing PowerShell shim launcher
  like every other spawned command; the native binary is spawned directly.
