/**
 * The Core Bridge: the non-MCP fallback that executes the *existing* Core tools for a
 * conversation that cannot reach the MCP Core connector.
 *
 * The protocol is in `src/shared/core-bridge.ts`; this module owns the decisions:
 *
 *   · The recorder reports each final assistant answer of a conversation
 *     (`setFinalAssistantListener`); nothing else can start a bridge request. Streaming
 *     text, tool results and user messages never reach this path, so nothing in them
 *     executes, and a `<COS_CORE_RESULT>` can never parse as a request.
 *   · Every gate is read live per answer: the two settings switches, the session's own
 *     conversation and its origin (workers and helpers stay unsupported), and a durable
 *     executed-request ledger that is reserved *before* execution — a crash between reserve
 *     and execution can lose one request, never re-run a batch that may have applied a
 *     patch or started a command.
 *   · Execution reuses the real kernel: a fresh `createRegistrar(null, liveContext, 'core')`
 *     plus `registerCoreTools`, invoked through the first-class `invokeLocal` entry so schema
 *     validation, live capabilities, the sandbox, Read-only, the command launch policy, the
 *     output caps and every block/compaction/lifecycle guard run exactly as for an MCP call.
 *     No filesystem, command or permission logic exists here. Calls run sequentially and are
 *     recorded with attribution `core_bridge` — never as an MCP request.
 *   · The result is a `<COS_CORE_RESULT>` delivered as a *new* outbox input to the same
 *     session, like the OpenCode bridge before it: the assistant turn that requested the
 *     work has already ended, so the answer belongs to the next turn, and after-turn
 *     delivery never interrupts a generating one. The row names that exact source turn
 *     (the outbox's `queuedTurn`), because its completion is already recorded — waiting
 *     for a *later* completion would park the answer until an unrelated turn finished.
 *
 * Batches for one session run in order; nothing here retries or runs on a timer.
 */

import { randomUUID } from 'node:crypto';
import {
  coreBridgeContentNote,
  coreBridgeResult,
  COS_CORE_CALL_OPEN,
  parseCoreBridgeCall,
  type CoreBridgeCallResult
} from '../shared/core-bridge.js';
import { effectiveCapabilities, getConfig } from './config.js';
import { readDurable, writeDurableNow } from './durable.js';
import { logInfo, logWarn, redact } from './logger.js';
import { createRegistrar, type ToolResult } from './mcp/kernel.js';
import { registerCoreTools } from './mcp/tools-core.js';
import { withManagedSkills } from './skill-access.js';
import { setFinalAssistantListener, type FinalAssistantMessage } from './session/recorder.js';
import { getSession } from './session/store.js';
import { sendDesktopInput } from './session/start-input.js';
import type { InputArgs } from './session/input.js';

/**
 * The Core tools the bridge offers. The registrar decides what is *registered*; this set
 * decides what the text transport accepts. The MCP-turn lifecycle tools are excluded: their
 * contracts are request-shaped (`session_finish` holds an MCP boundary, `agents` drives the
 * broker's worker bootstraps, `update_plan` belongs to a tool turn), and code-mode `exec` is
 * not registered here at all — the native Core connector remains the way to use it.
 */
const BRIDGE_TOOLS: ReadonlySet<string> = new Set([
  'read', 'view_image', 'save_image', 'find', 'apply_patch', 'exec_command', 'write_stdin'
]);
/** Mutating tools, which additionally need the bridge's own explicit action guard. */
const BRIDGE_ACTION_TOOLS: ReadonlySet<string> = new Set(['apply_patch', 'exec_command', 'write_stdin']);

/** The executed-request ledger: a durable "this request already ran" receipt. */
const REQUEST_LEDGER = 'core-bridge-requests';
const LEDGER_MAX_ENTRIES = 500;
const LEDGER_TTL_MS = 7 * 24 * 60 * 60_000;

let ledger: Record<string, number> | null = null;
/** One chain per session, so two finals of one chat execute and deliver in order. */
const sessionChains = new Map<string, Promise<unknown>>();

export function installCoreBridge(): void {
  setFinalAssistantListener(handleFinalAnswer);
  logInfo('Core Bridge installed: watching final answers for explicit COS_CORE_CALL requests');
}

export function resetCoreBridgeForTests(): void {
  ledger = null;
  sessionChains.clear();
}

async function loadLedger(): Promise<Record<string, number>> {
  if (ledger) return ledger;
  const stored = await readDurable<Record<string, number>>(REQUEST_LEDGER);
  ledger = prune(stored && typeof stored === 'object' ? { ...stored } : {});
  return ledger;
}

function prune(entries: Record<string, number>): Record<string, number> {
  const cutoff = Date.now() - LEDGER_TTL_MS;
  const kept = Object.fromEntries(
    Object.entries(entries)
      .filter(([, at]) => typeof at === 'number' && at >= cutoff)
      .sort((a, b) => a[1]! - b[1]!)
      .slice(-LEDGER_MAX_ENTRIES)
  );
  return kept;
}

/** Reserves before execution. False means one of the keys already ran and nothing may repeat. */
async function reserve(keys: readonly string[]): Promise<boolean> {
  const entries = await loadLedger();
  if (keys.some((key) => entries[key] !== undefined)) return false;
  const now = Date.now();
  for (const key of keys) entries[key] = now;
  const pruned = prune(entries);
  ledger = pruned;
  await writeDurableNow(REQUEST_LEDGER, pruned);
  return true;
}

/** The answer, as a new input to the session that asked. Delivery belongs to the outbox. */
async function deliverResult(sessionId: string, text: string, source: { conversationId: string; turnId: string } | null): Promise<void> {
  const input: InputArgs = {
    id: randomUUID(),
    sessionId,
    text,
    mode: 'after-turn',
    dueAt: Date.now(),
    model: null,
    reasoningEffort: null,
    authoredSource: 'none'
  };
  try {
    // The row waits for the exact turn whose final asked for this work: that completion
    // is already recorded by the time the batch ran, so naming it (the existing
    // queuedTurn reference) is what lets it release the head. Without it the row would
    // sit behind the later-completion rule until an unrelated next turn finished.
    const row = await sendDesktopInput(input, source ?? undefined);
    logInfo(`Core Bridge result queued for session ${sessionId} as input ${row.id}`);
  } catch (error) {
    // The work ran and is recorded; only the answer row failed. Never re-execute to retry it.
    logWarn(`Core Bridge result was not delivered: ${redact(error instanceof Error ? error.message : String(error))}`);
  }
}

function callResult(tool: string, result: ToolResult): CoreBridgeCallResult {
  const content = result.content.map((part) =>
    part.type === 'text' ? part.text : coreBridgeContentNote(Math.floor(part.data.length * 3 / 4)));
  if (result.isError) {
    const first = content.find((text) => text.length > 0) ?? '';
    return { tool, ok: false, ...(first ? { error: first } : { content }) };
  }
  return { tool, ok: true, content };
}

/**
 * The one decision the bridge makes about a batch: refuse the whole request, or run each
 * call through the live registrar. Per-call refusals keep the rest of a batch useful.
 */
async function executeBatch(message: FinalAssistantMessage, requestId: string, calls: ReadonlyArray<{ tool: string; args: Record<string, unknown> }>): Promise<void> {
  const config = getConfig();
  const registrar = createRegistrar(null, withManagedSkills({
    roots: config.roots,
    caps: effectiveCapabilities(config),
    readOnly: config.readOnly,
    privacyScreenshots: config.ui.privacyScreenshots
  }), 'core');
  registerCoreTools(registrar);
  const registered = new Set(registrar.registered());
  const results: CoreBridgeCallResult[] = [];
  for (const call of calls) {
    if (!registered.has(call.tool)) {
      results.push({ tool: call.tool, ok: false, error: 'UNKNOWN_TOOL: this tool is not available on this connector.' });
      continue;
    }
    if (!BRIDGE_TOOLS.has(call.tool)) {
      results.push({
        tool: call.tool,
        ok: false,
        error: `BRIDGE_TOOL_REFUSED: ${call.tool} is not offered through the text bridge; it belongs to the native Core connector's request contract. No action was taken.`
      });
      continue;
    }
    if (BRIDGE_ACTION_TOOLS.has(call.tool) && config.coreBridge.allowActions !== true) {
      results.push({
        tool: call.tool,
        ok: false,
        error: `BRIDGE_ACTIONS_DISABLED: ${call.tool} needs the "Allow bridge actions" switch in Settings → General → For developers. No file or command was changed.`
      });
      continue;
    }
    // The real dispatch: schema validation, live capabilities, sandbox, Read-only, command
    // policy, output caps and every conversation guard, recorded as core_bridge traffic.
    const result = await registrar.invokeLocal(call.tool, call.args, {
      conversationId: message.conversationId,
      sessionId: message.sessionId,
      transport: 'core_bridge'
    });
    results.push(callResult(call.tool, result));
  }
  await deliverResult(message.sessionId, coreBridgeResult({ id: requestId, status: 'completed', results }),
    message.turnId ? { conversationId: message.conversationId, turnId: message.turnId } : null);
}

async function handleFinalAnswer(message: FinalAssistantMessage): Promise<void> {
  const config = getConfig();
  if (config.coreBridge.enabled !== true) return;
  // The cheap negative path: an ordinary answer names no protocol marker at all.
  if (!message.text.includes(COS_CORE_CALL_OPEN)) return;
  // The caller is the exact originating session and its current conversation. A worker or
  // helper chat belongs to its run and stays unsupported, silently like every other skip.
  const session = await getSession(message.sessionId);
  if (!session || session.conversationId !== message.conversationId) return;
  if (session.origin?.kind === 'worker' || session.origin?.kind === 'helper') return;
  const parsed = parseCoreBridgeCall(message.text);
  if (parsed.kind === 'none') return;

  // One chain per session: batches of the same conversation run in order, and each result
  // is queued behind the previous one by the outbox as well. The chain entry leaves with its
  // last batch; a map entry per quiet session is still bounded, but there is no reason to keep it.
  const prior = sessionChains.get(message.sessionId) ?? Promise.resolve();
  const run = prior.then(() => processParsed(message, parsed), () => processParsed(message, parsed));
  const tail = run.then(() => undefined, () => undefined);
  sessionChains.set(message.sessionId, tail);
  void tail.then(() => {
    if (sessionChains.get(message.sessionId) === tail) sessionChains.delete(message.sessionId);
  });
  await run;
}

async function processParsed(message: FinalAssistantMessage, parsed: Exclude<Awaited<ReturnType<typeof parseCoreBridgeCall>>, { kind: 'none' }>): Promise<void> {
  if (parsed.kind === 'request') {
    // Request ids are supplied by the model, so their uniqueness is only meaningful inside
    // the session that owns the final answer. A global UUID key would let one conversation
    // accidentally suppress another conversation that happened to reuse the same id.
    const keys = [`req:${message.sessionId}:${parsed.request.id.toLowerCase()}`];
    if (!(await reserve(keys))) {
      logInfo(`Core Bridge: request ${parsed.request.id} of session ${message.sessionId} already ran; ignoring the repeat`);
      return;
    }
    logInfo(`Core Bridge: executing ${parsed.request.calls.length} call(s) for session ${message.sessionId} (request ${parsed.request.id})`);
    await executeBatch(message, parsed.request.id, parsed.request.calls);
    return;
  }
  // An invalid block is answered, not executed, so the model is not left waiting — but only
  // once per message identity, and never for chats the bridge does not serve (those were
  // gated before the parse reached here). Only the message is reserved: a corrected request
  // that reuses the id is a new message and must be allowed to run.
  if (!(await reserve([`msg:${message.sessionId}:${message.messageId}`]))) return;
  logInfo(`Core Bridge: rejected a malformed request of session ${message.sessionId}`);
  await deliverResult(
    message.sessionId,
    coreBridgeResult({ id: parsed.id, status: 'rejected', results: [], error: parsed.reason }),
    message.turnId ? { conversationId: message.conversationId, turnId: message.turnId } : null
  );
}
