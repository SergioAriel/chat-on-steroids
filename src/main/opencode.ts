/**
 * The OpenCode executor: one bounded registry of local jobs spawned for a CoS conversation.
 *
 * A job is requested only through the local control API's action routes
 * (`src/main/control-actions.ts`); no MCP tool and no browser-side parsing can start one. It
 * runs `opencode run` as a direct child process — argv only, never a shell — inside a folder
 * that the app's approved roots already contain, with a model the caller names (OpenCode
 * keeps its own provider configuration and credentials; this app never touches them).
 *
 * When the process ends, the structured result is delivered to the *originating* session
 * through the one existing send path, `sendDesktopInput`: an after-turn outbox row, so it
 * never interrupts the answer ChatGPT is writing and it queues behind a busy chat by the
 * outbox's own rules. There is no second delivery mechanism and no retry timer: the job
 * record keeps `resultInputId`/`resultError`, and a controller that needs the bytes can read
 * them from the job or send them itself through `POST /v1/inputs`.
 *
 * Jobs live in process memory, like browser-control's pending claims: the app terminating
 * takes them down (the shutdown sequence stops this runtime with the other owned processes)
 * and nothing is resurrected from disk afterwards. A controller sees that as a lost listener.
 */

import { spawn, type ChildProcess } from 'node:child_process';
import { promises as fs } from 'node:fs';
import { randomUUID } from 'node:crypto';
import type { ControlApiOpenCodeJob } from '../shared/control-api.js';
import {
  clipStreamText,
  containsLocalProtocolMarkers,
  localResultPayload,
  LOCAL_RESULT_BUDGET
} from '../shared/local-task.js';
import { HeadTailBuffer } from './codex/head-tail-buffer.js';
import { UNIFIED_EXEC_OUTPUT_MAX_BYTES } from './codex/unified-exec-constants.js';
import { prepareCommand, terminateProcessTree } from './exec.js';
import { getConfig } from './config.js';
import { logInfo, logWarn, redact } from './logger.js';
import { redactSecretText } from './redaction.js';
import { SandboxError, resolvePath } from './sandbox.js';
import { readSession } from './session/read-model.js';
import { sendDesktopInput } from './session/start-input.js';
import type { InputArgs } from './session/input.js';

export type OpenCodeJobStatus = 'running' | 'completed' | 'failed' | 'cancelled';

/** A refusal the control API answers with its code; nothing else in this module throws typed. */
export class OpenCodeJobError extends Error {
  constructor(readonly code: string, message: string, readonly detail?: string) {
    super(message);
    this.name = 'OpenCodeJobError';
  }
}

/** At most this many processes at once; a slot frees when a job turns terminal. */
const MAX_RUNNING_JOBS = 4;
/** Terminal jobs kept for `GET /v1/opencode/jobs/{id}`, the oldest evicted first. */
const MAX_RETAINED_JOBS = 64;
/** The command runs a task of up to this many characters, matching the objective bound. */
const MAX_TASK_CHARS = 16_000;
/** Model ids are provider slugs such as `openai/gpt-5.6`; this keeps them inert argv. */
const MODEL_ID = /^[A-Za-z0-9][A-Za-z0-9._/:+-]{0,199}$/;
/** How long cancel waits for the exit it asked for before answering the current truth. */
const CANCEL_SETTLE_MS = 5_000;

export interface OpenCodeJobRecord {
  jobId: string;
  /** The session that asked for the work; the result is delivered to it and nothing else. */
  sessionId: string;
  cwdReal: string;
  /** The sandbox's virtual spelling, the only path form this module publishes. */
  cwdVirtual: string;
  model: string | null;
  task: string;
  status: OpenCodeJobStatus;
  pid: number | null;
  startedAt: number;
  finishedAt: number | null;
  exitCode: number | null;
  stdoutBuffer: HeadTailBuffer;
  stderrBuffer: HeadTailBuffer;
  stdout: string;
  stderr: string;
  resultInputId: string | null;
  resultError: string | null;
  /** Exact request bytes, compared before a replayed id is answered. */
  requestFingerprint: string;
  cancelRequested: boolean;
  finalized: boolean;
  /** The registry generation that created this record; a sweep orphans late finalizes. */
  epoch: number;
  /** Settles once, when the process reached its terminal state. */
  exit: Promise<void>;
}

export interface OpenCodeJobRequest {
  /** Caller-supplied id for at-most-once creation; generated when omitted. */
  id?: string;
  sessionId: string;
  /** Virtual (`/root/...`) or native spelling; the sandbox decides what it names. */
  cwd: string;
  task: string;
  /** OpenCode model id in `provider/model` form, or null for OpenCode's own default. */
  model: string | null;
}

/** Rechecked right before the process is created and before a cancel is carried out. */
export interface OpenCodeJobPermits {
  permitted: () => boolean;
}

// --------------------------------------------------------------------- state

const running = new Map<string, OpenCodeJobRecord>();
const terminal: OpenCodeJobRecord[] = [];
let command: { file: string; args: string[] } = { file: 'opencode', args: [] };
let stopped = false;
/**
 * Admission owns two process-wide invariants: a caller-supplied id starts at most one job,
 * and no more than MAX_RUNNING_JOBS are registered. Session/cwd validation may run in
 * parallel, but the final id/capacity/permission check plus spawn+registration is one short
 * serialized section so two requests cannot both observe the same pre-admission state.
 */
let admissionTail: Promise<void> = Promise.resolve();
/**
 * Bumped whenever the registry is swept (a reset, or the app's shutdown). A finalize that
 * lands after the sweep belongs to a registry that no longer lists the record, so it settles
 * its promise and touches no shared state — late exits cannot pollute the next generation.
 */
let registryEpoch = 0;

/** Test seam: a stand-in executable for real-child behaviour without an OpenCode install. */
export function setOpenCodeCommandForTests(next: { file: string; args: string[] }): void {
  command = next;
}

export function resetOpenCodeForTests(): void {
  sweepRuntime();
  command = { file: 'opencode', args: [] };
  stopped = false;
  admissionTail = Promise.resolve();
}

/** Shutdown: no new admission, and every owned process goes with the app. */
export function stopOpenCodeRuntime(): Promise<void> {
  return sweepRuntime();
}

function sweepRuntime(): Promise<void> {
  stopped = true;
  registryEpoch += 1;
  const records = [...running.values()];
  running.clear();
  terminal.length = 0;
  for (const record of records) {
    record.cancelRequested = true;
    if (record.status === 'running' && record.pid !== null) void terminateProcessTree(record.pid).catch(() => undefined);
  }
  // The exit handlers may still try to deliver; the outbox's own shutdown guard refuses it.
  return Promise.allSettled(records.map((record) => record.exit)).then(() => undefined);
}

// ---------------------------------------------------------------- admission

const fingerprintOf = (request: OpenCodeJobRequest): string =>
  JSON.stringify([request.sessionId, request.cwd, request.task, request.model]);

function byId(jobId: string): OpenCodeJobRecord | undefined {
  return running.get(jobId) ?? terminal.find((record) => record.jobId === jobId);
}

function serializeAdmission<T>(operation: () => Promise<T> | T): Promise<T> {
  const run = admissionTail.then(operation, operation);
  admissionTail = run.then(() => undefined, () => undefined);
  return run;
}

function validateTask(task: string): void {
  if (task.trim() === '') throw new OpenCodeJobError('task_refused', 'The task is empty');
  if (task.length > MAX_TASK_CHARS) {
    throw new OpenCodeJobError('task_refused', `The task is longer than ${MAX_TASK_CHARS} characters`);
  }
  if (task.includes('\0')) throw new OpenCodeJobError('task_refused', 'The task contains a null byte');
  if (task.trimStart().startsWith('-')) {
    throw new OpenCodeJobError('task_refused', 'The task starts with "-" and would read as a command flag');
  }
  // Neither a returned result nor a nested task block may ride inside a task: a result
  // echoed into a new job is the loop this protocol must never close on itself.
  if (containsLocalProtocolMarkers(task)) {
    throw new OpenCodeJobError('task_refused', 'The task contains COS_LOCAL_TASK or COS_LOCAL_RESULT protocol markers');
  }
}

function validateModel(model: string | null): void {
  if (model !== null && !MODEL_ID.test(model)) {
    throw new OpenCodeJobError('model_refused', 'The model id is not a provider/model slug');
  }
}

async function validateSession(sessionId: string): Promise<void> {
  const session = await readSession(sessionId);
  if (!session) throw new OpenCodeJobError('session_not_found', 'No such session');
  if (session.origin?.kind === 'worker' || session.origin?.kind === 'helper') {
    throw new OpenCodeJobError('session_not_controllable', 'A worker or helper chat belongs to its run');
  }
  if (!session.conversationId) {
    throw new OpenCodeJobError('no_chat', 'This session has no ChatGPT conversation to receive the result');
  }
}

/** The one workspace authority: the sandbox's approved roots, virtual and native spellings alike. */
async function resolveWorkspace(cwd: string): Promise<{ real: string; virtual: string }> {
  let resolved;
  try {
    resolved = await resolvePath(getConfig().roots, cwd);
  } catch (error) {
    if (error instanceof SandboxError) throw new OpenCodeJobError('cwd_refused', 'The folder was refused', error.message);
    throw error;
  }
  const stat = await fs.stat(resolved.real).catch(() => null);
  if (!stat?.isDirectory()) {
    throw new OpenCodeJobError('cwd_refused', 'The path is not a folder');
  }
  return { real: resolved.real, virtual: resolved.virtual };
}

// ------------------------------------------------------------------ process

/** Spawns without a shell. Only argv crosses the boundary; the task is one argument. */
function spawnProcess(cwdReal: string, task: string, model: string | null): ChildProcess {
  const args = [...command.args, 'run', '--dir', cwdReal, '--format', 'json'];
  if (model !== null) args.push('--model', model);
  // The separator keeps a task that begins with "-" a message, not a flag; a leading-dash
  // task is refused at admission as well.
  args.push('--', task);
  const prepared = prepareCommand(command.file, args, cwdReal);
  return spawn(prepared.file, prepared.args, {
    cwd: cwdReal,
    env: prepared.env,
    windowsHide: true,
    shell: false,
    // A POSIX group leader lets cancellation reach descendants, as with the terminal tools.
    detached: process.platform !== 'win32',
    stdio: ['ignore', 'pipe', 'pipe']
  });
}

function finalize(record: OpenCodeJobRecord, exitCode: number | null, spawnError: string | null, settle: () => void): void {
  if (record.finalized) return;
  record.finalized = true;
  record.pid = null;
  if (record.epoch !== registryEpoch) {
    // The registry was swept while this process was dying; nothing of it remains to publish.
    settle();
    return;
  }
  record.finishedAt = Date.now();
  record.exitCode = spawnError === null ? exitCode : null;
  record.status = record.cancelRequested
    ? 'cancelled'
    : spawnError !== null || exitCode === null
      ? 'failed'
      : exitCode === 0
        ? 'completed'
        : 'failed';
  record.stdout = record.stdoutBuffer.toBytesWithOmissionMarker().toString('utf8');
  record.stderr = spawnError === null
    ? record.stderrBuffer.toBytesWithOmissionMarker().toString('utf8')
    // A process that never started has no streams; name that where the reader of a failed
    // job looks, keeping both channels honest about what was captured.
    : `Failed to start opencode: ${spawnError}`;
  running.delete(record.jobId);
  terminal.push(record);
  while (terminal.length > MAX_RETAINED_JOBS) terminal.shift();
  logInfo(`opencode job ${record.jobId} ${record.status}` + (record.exitCode !== null ? ` (exit ${record.exitCode})` : ''));
  void deliverResult(record);
  settle();
}

async function deliverResult(record: OpenCodeJobRecord): Promise<void> {
  if (stopped) {
    record.resultError = 'The app is shutting down';
    return;
  }
  const text = localResultPayload({
    jobId: record.jobId,
    status: record.status,
    exitCode: record.exitCode,
    stdout: record.stdout,
    stderr: record.stderr,
    stdoutOmittedBytes: record.stdoutBuffer.omittedBytes(),
    stderrOmittedBytes: record.stderrBuffer.omittedBytes()
  });
  // After-turn: the outbox's own queue semantics deliver at the chat's boundary, so the
  // result never interrupts an answer, and it queues behind a busy conversation instead of
  // being refused as a second in-flight message.
  const input: InputArgs = {
    id: randomUUID(),
    sessionId: record.sessionId,
    text,
    mode: 'after-turn',
    dueAt: Date.now(),
    model: null,
    reasoningEffort: null,
    authoredSource: 'none'
  };
  try {
    const row = await sendDesktopInput(input);
    record.resultInputId = row.id;
    logInfo(`opencode job ${record.jobId} result queued for session ${record.sessionId} as ${row.id}`);
  } catch (error) {
    record.resultError = (error instanceof Error ? error.message : String(error)).slice(0, 200);
    logWarn(`opencode job ${record.jobId} result was not delivered: ${redact(record.resultError)}`);
  }
}

// -------------------------------------------------------------------- routes

export async function startOpenCodeJob(
  request: OpenCodeJobRequest,
  permits: OpenCodeJobPermits
): Promise<{ job: OpenCodeJobRecord; replayed: boolean }> {
  if (stopped) throw new OpenCodeJobError('shutting_down', 'The app is shutting down');
  if (request.id !== undefined) {
    const existing = byId(request.id);
    if (existing) {
      if (existing.requestFingerprint !== fingerprintOf(request)) {
        throw new OpenCodeJobError('id_conflict', 'This id belongs to a different job request');
      }
      return { job: existing, replayed: true };
    }
  }
  validateTask(request.task);
  validateModel(request.model);
  await validateSession(request.sessionId);
  const workspace = await resolveWorkspace(request.cwd);
  return serializeAdmission(() => {
    // Another request with the same caller id may have completed validation while this one
    // did. Re-check ownership inside the admission section before any process is created.
    if (request.id !== undefined) {
      const existing = byId(request.id);
      if (existing) {
        if (existing.requestFingerprint !== fingerprintOf(request)) {
          throw new OpenCodeJobError('id_conflict', 'This id belongs to a different job request');
        }
        return { job: existing, replayed: true };
      }
    }
    if (running.size >= MAX_RUNNING_JOBS) {
      throw new OpenCodeJobError('too_many_jobs', `${MAX_RUNNING_JOBS} OpenCode jobs are already running`);
    }
    // The switch can flip while the lookups above ran; this is the last point before the spawn.
    if (!permits.permitted()) throw new OpenCodeJobError('actions_disabled', 'Actions are switched off');

    let settle!: () => void;
    const exit = new Promise<void>((resolve) => { settle = resolve; });
    const record: OpenCodeJobRecord = {
      jobId: request.id ?? randomUUID(),
      sessionId: request.sessionId,
      cwdReal: workspace.real,
      cwdVirtual: workspace.virtual,
      model: request.model,
      task: request.task,
      status: 'running',
      pid: null,
      startedAt: Date.now(),
      finishedAt: null,
      exitCode: null,
      stdoutBuffer: new HeadTailBuffer(UNIFIED_EXEC_OUTPUT_MAX_BYTES),
      stderrBuffer: new HeadTailBuffer(UNIFIED_EXEC_OUTPUT_MAX_BYTES),
      stdout: '',
      stderr: '',
      resultInputId: null,
      resultError: null,
      requestFingerprint: fingerprintOf(request),
      cancelRequested: false,
      finalized: false,
      epoch: registryEpoch,
      exit
    };

    // A synchronous spawn refusal (bad argv, unwritable cwd) is a job that failed to start,
    // reported through the same failed state an early OS error produces.
    let child: ChildProcess;
    try {
      child = spawnProcess(workspace.real, request.task, request.model);
    } catch (error) {
      finalize(record, null, (error as Error).message, settle);
      return { job: record, replayed: false };
    }
    record.pid = child.pid ?? null;
    running.set(record.jobId, record);

    // stdout and stderr stay apart: the payload reports each channel as it happened.
    for (const [stream, buffer] of [[child.stdout, record.stdoutBuffer], [child.stderr, record.stderrBuffer]] as const) {
      if (!stream) continue;
      stream.on('data', (chunk: Buffer) => buffer.pushChunk(chunk));
    }
    let spawnError: string | null = null;
    // A spawn failure (an executable that is not installed) reports through `error`; `close`
    // may or may not follow it, so both settle through the same once-guard in `finalize`.
    child.once('error', (error: Error) => {
      spawnError = error.message;
      finalize(record, null, spawnError, settle);
    });
    // `close`, not `exit`: the streams have flushed, so the collected output is whole.
    child.once('close', (code) => finalize(record, code, spawnError, settle));
    logInfo(`opencode job ${record.jobId} started for session ${record.sessionId} in ${redact(workspace.virtual)}`);
    return { job: record, replayed: false };
  });
}

export async function cancelOpenCodeJob(
  jobId: string,
  permits: OpenCodeJobPermits
): Promise<{ cancelled: boolean; job: OpenCodeJobRecord }> {
  const record = byId(jobId);
  if (!record) throw new OpenCodeJobError('job_not_found', 'No such OpenCode job');
  if (record.status !== 'running') return { cancelled: false, job: record };
  if (!permits.permitted()) throw new OpenCodeJobError('actions_disabled', 'Actions are switched off');
  record.cancelRequested = true;
  // The exit handler mutates the record behind TypeScript's back, so re-read rather than
  // let the earlier `status` narrowing decide what the settled record says.
  const statusNow = (): OpenCodeJobStatus => record.status;
  if (record.pid !== null) {
    // Only a live record asks the OS to kill; a pid read after its process ended could name
    // a different process by then. terminateProcessTree is a no-op for a group already gone.
    await terminateProcessTree(record.pid);
  }
  // The kill was delivered; the exit lands within moments. Answer what actually happened,
  // never a hope: a job that somehow outlives the settle window still reads as running.
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, CANCEL_SETTLE_MS);
    record.exit.then(() => { clearTimeout(timer); resolve(); }, () => { clearTimeout(timer); resolve(); });
  });
  return { cancelled: statusNow() === 'cancelled', job: record };
}

// -------------------------------------------------------------- projection

/**
 * One captured channel for the wire: redacted first, then cut, like every other free-text
 * view of this API. The stored bytes stay untouched; `chars` is the stored length.
 */
function streamText(record: OpenCodeJobRecord, channel: 'stdout' | 'stderr'): ControlApiOpenCodeJob['stdout'] {
  const raw = record[channel];
  const budget = channel === 'stdout'
    ? { head: LOCAL_RESULT_BUDGET.stdoutHead, tail: LOCAL_RESULT_BUDGET.stdoutTail }
    : { head: LOCAL_RESULT_BUDGET.stderrHead, tail: LOCAL_RESULT_BUDGET.stderrTail };
  const buffer = channel === 'stdout' ? record.stdoutBuffer : record.stderrBuffer;
  const { text, omitted } = clipStreamText(redactSecretText(raw), budget.head, budget.tail);
  const truncated = buffer.omittedBytes() > 0 || omitted > 0;
  return { text, chars: raw.length, truncated };
}

/** An allowlist projection, like every other control API view: named fields only. */
export function projectOpenCodeJob(record: OpenCodeJobRecord): ControlApiOpenCodeJob {
  return {
    jobId: record.jobId,
    sessionId: record.sessionId,
    status: record.status,
    pid: record.status === 'running' ? record.pid : null,
    startedAt: record.startedAt,
    finishedAt: record.finishedAt,
    exitCode: record.exitCode,
    model: record.model,
    cwd: record.cwdVirtual,
    stdout: streamText(record, 'stdout'),
    stderr: streamText(record, 'stderr'),
    resultInputId: record.resultInputId,
    resultError: record.resultError === null ? null : redactSecretText(record.resultError).slice(0, 200)
  };
}

/** Null when no job has this id. */
export function openCodeJobView(jobId: string): ControlApiOpenCodeJob | null {
  const record = byId(jobId);
  return record ? projectOpenCodeJob(record) : null;
}
