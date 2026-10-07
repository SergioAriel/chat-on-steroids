/**
 * The textual Core Bridge protocol: one explicit request block a ChatGPT answer can carry,
 * and one result block the app sends back as a new conversation turn.
 *
 * This is a *fallback* transport for the existing Core tools, not a second implementation of
 * them. ChatGPT cannot use the MCP Core connector in some conversations; when the user has
 * switched the bridge on, the model may ask for local work with exactly one
 * `<COS_CORE_CALL>` JSON block in its final answer, and `src/main/core-bridge.ts` executes
 * those calls through the real kernel and returns a `<COS_CORE_RESULT>` through the outbox.
 *
 * The fences that keep this from turning into a loop:
 *   · a request is recognized only by its own `<COS_CORE_CALL>` opening tag, so a
 *     `<COS_CORE_RESULT>` block can never parse as a request;
 *   · a message carrying result markers, a second call marker, or markers nested inside the
 *     block body is rejected rather than guessed at;
 *   · both blocks are bounded, and the result builder clips each channel's text so a whole
 *     result always fits the outbox's message ceiling.
 */

import { z } from 'zod';
import { clipStreamText } from './local-task.js';

export const COS_CORE_CALL_OPEN = '<COS_CORE_CALL>';
export const COS_CORE_CALL_CLOSE = '</COS_CORE_CALL>';
export const COS_CORE_RESULT_OPEN = '<COS_CORE_RESULT>';
export const COS_CORE_RESULT_CLOSE = '</COS_CORE_RESULT>';

/** A batch runs at most this many calls, sequentially. */
export const CORE_BRIDGE_MAX_CALLS = 8;
/** The request block is JSON, and this is its whole size ceiling. */
export const CORE_BRIDGE_MAX_REQUEST_CHARS = 24_000;

const callSchema = z
  .object({
    tool: z.string().min(1).max(64),
    args: z.record(z.string(), z.unknown())
  })
  .strict();
const requestSchema = z
  .object({
    id: z.string().uuid(),
    calls: z.array(callSchema).min(1).max(CORE_BRIDGE_MAX_CALLS)
  })
  .strict();

export interface CoreBridgeCall {
  tool: string;
  args: Record<string, unknown>;
}

export interface CoreBridgeRequest {
  id: string;
  calls: CoreBridgeCall[];
}

export type CoreBridgeParse =
  | { kind: 'none' }
  | { kind: 'request'; request: CoreBridgeRequest }
  | { kind: 'invalid'; id: string | null; reason: string };

/**
 * Reads the protocol content of one final assistant answer.
 *
 * `none` is the ordinary case — no call marker at all — and costs one `includes`. Anything
 * else is executable only when one complete, well-formed block owns the whole answer apart
 * from surrounding whitespace: no prose around it, no result markers, no second call marker,
 * no marker inside the block body. An invalid executable block keeps
 * its id when the id itself parsed, so the caller can answer the model with it.
 */
export function parseCoreBridgeCall(text: string): CoreBridgeParse {
  if (!text.includes(COS_CORE_CALL_OPEN)) return { kind: 'none' };
  const framed = text.trim();
  // Executable protocol must own the final answer. Mentions in explanations,
  // documentation, quoted examples, or prose around a block are not requests.
  if (!framed.startsWith(COS_CORE_CALL_OPEN)) return { kind: 'none' };
  if (text.includes(COS_CORE_RESULT_OPEN) || text.includes(COS_CORE_RESULT_CLOSE)) {
    return { kind: 'invalid', id: null, reason: 'a COS_CORE_RESULT block is not a request and cannot start one; send a fresh COS_CORE_CALL if local work is still needed' };
  }
  const first = framed.indexOf(COS_CORE_CALL_OPEN);
  const afterFirst = first + COS_CORE_CALL_OPEN.length;
  if (framed.indexOf(COS_CORE_CALL_OPEN, afterFirst) !== -1) {
    return { kind: 'invalid', id: null, reason: 'more than one COS_CORE_CALL block is ambiguous; send exactly one' };
  }
  const close = framed.indexOf(COS_CORE_CALL_CLOSE, afterFirst);
  if (close === -1) {
    return { kind: 'invalid', id: null, reason: 'the COS_CORE_CALL block has no closing tag' };
  }
  const afterClose = close + COS_CORE_CALL_CLOSE.length;
  if (framed.slice(afterClose).trim().length > 0) return { kind: 'none' };
  if (framed.indexOf(COS_CORE_CALL_CLOSE, afterClose) !== -1) {
    return { kind: 'invalid', id: null, reason: 'more than one closing COS_CORE_CALL tag is ambiguous; send exactly one block' };
  }
  const body = framed.slice(afterFirst, close);
  if (body.length > CORE_BRIDGE_MAX_REQUEST_CHARS) {
    return { kind: 'invalid', id: null, reason: `the COS_CORE_CALL block is longer than ${CORE_BRIDGE_MAX_REQUEST_CHARS} characters` };
  }
  if (body.includes(COS_CORE_CALL_OPEN) || body.includes(COS_CORE_CALL_CLOSE) ||
      body.includes(COS_CORE_RESULT_OPEN) || body.includes(COS_CORE_RESULT_CLOSE)) {
    return { kind: 'invalid', id: null, reason: 'protocol markers are not allowed inside the COS_CORE_CALL block' };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return { kind: 'invalid', id: null, reason: 'the COS_CORE_CALL block is not valid JSON' };
  }
  const shaped = requestSchema.safeParse(parsed);
  if (!shaped.success) {
    // Keep a valid id so the model can match the answer to what it sent.
    const id = z.object({ id: z.string().uuid() }).passthrough().safeParse(parsed);
    const issue = shaped.error.issues[0];
    return {
      kind: 'invalid',
      id: id.success ? id.data.id : null,
      reason: `the COS_CORE_CALL body is not a valid request: ${issue?.path.map(String).join('.') || 'body'}: ${issue?.message ?? ''}`.trim()
    };
  }
  return { kind: 'request', request: shaped.data };
}

/** One executed call, as the result reports it. */
export interface CoreBridgeCallResult {
  tool: string;
  ok: boolean;
  /** Text content parts, in order. Image blocks become an explicit omission note. */
  content?: string[];
  /** The refusal or failure text, when the call did not succeed. */
  error?: string;
}

export interface CoreBridgeResultPayload {
  id: string | null;
  /** `completed`: the batch ran and each call reports its own outcome. `rejected`: nothing executed. */
  status: 'completed' | 'rejected';
  results: CoreBridgeCallResult[];
  /** Why the request was rejected, when it was. */
  error?: string;
}

/** Per-call and whole-payload caps, under the outbox's 96,000-character message ceiling. */
export interface CoreBridgeResultBudget {
  maxChars: number;
  perCallHead: number;
  perCallTail: number;
}

export const CORE_BRIDGE_RESULT_BUDGET: CoreBridgeResultBudget = {
  maxChars: 60_000,
  perCallHead: 10_000,
  perCallTail: 2_000
};

/** Image blocks cannot cross a text channel; say so instead of dropping them silently. */
export function coreBridgeContentNote(bytes: number): string {
  return `[image block omitted: ${bytes} bytes — image results do not cross the text bridge; use the native Core connector for images]`;
}

/**
 * Frames one result payload. Each call's content is clipped head/tail to the budget and the
 * frame is rebuilt with halved budgets until the whole payload fits, so escaped JSON can
 * never push it past the message ceiling.
 */
export function coreBridgeResult(
  payload: CoreBridgeResultPayload,
  budget: CoreBridgeResultBudget = CORE_BRIDGE_RESULT_BUDGET
): string {
  const assemble = (head: number, tail: number): string => {
    const results = payload.results.map((result) => {
      if (result.content === undefined) return { tool: result.tool, ok: result.ok, ...(result.error ? { error: result.error } : {}) };
      const { text, omitted } = clipStreamText(result.content.join('\n'), head, tail);
      const body: Record<string, unknown> = { tool: result.tool, ok: result.ok, content: [text] };
      if (omitted > 0) body.omitted = omitted;
      if (result.error !== undefined) body.error = result.error;
      return body;
    });
    const frame: Record<string, unknown> = {
      id: payload.id,
      status: payload.status,
      results,
      ...(payload.error ? { error: payload.error } : {})
    };
    return `${COS_CORE_RESULT_OPEN}${JSON.stringify(frame)}${COS_CORE_RESULT_CLOSE}`;
  };
  let head = budget.perCallHead;
  let tail = budget.perCallTail;
  let framed = assemble(head, tail);
  while (framed.length > budget.maxChars && head + tail > 512) {
    head = Math.floor(head / 2);
    tail = Math.floor(tail / 2);
    framed = assemble(head, tail);
  }
  return framed;
}

/**
 * The bridge paragraph added to a new chat's COS_CONTEXT frame, only while the bridge is
 * enabled. It supplements the normal Core instructions; it never replaces them.
 */
export const CORE_BRIDGE_INSTRUCTIONS = [
  '# Local Core Bridge (fallback)',
  `Local Core Bridge is available. When local files, search, patching or terminal access is needed and no native Core tool is available in this conversation, request it using exactly one ${COS_CORE_CALL_OPEN} JSON block as the entire final answer apart from surrounding whitespace, shaped like {"id":"<uuid>","calls":[{"tool":"read","args":{"paths":["/<root>/folder"]}}]}. Do not put prose before or after the block.`,
  'Use the bridge only when the native Core tools are not available in this conversation; call the tools directly whenever they are.',
  'Each tool\'s args are validated strictly and a wrong or extra key fails the call, so use their exact names: read takes "paths" — an array of one or more paths, never a singular "path" — plus optional "start_line"/"end_line"/"max_bytes"; find takes "query" with optional "path" and "mode":"name"|"content"; view_image and save_image take a singular "path"; apply_patch takes the whole patch as one "patch" string; exec_command takes exactly one of "cmd" or "cmds" plus optional "workdir"; write_stdin takes numeric "session_id" plus optional "chars".',
  `Prefer one bounded batch of related reads (at most ${CORE_BRIDGE_MAX_CALLS} calls); calls run sequentially and each reports its own outcome. Available bridge tools: read, view_image, save_image, find, apply_patch, exec_command, write_stdin, with the app's live permissions deciding each call and refusals naming what to ask the user for.`,
  'Patching and terminal calls additionally need the app\'s bridge action permission; a BRIDGE_ACTIONS_DISABLED refusal means the user has not granted it.',
  `Never echo a ${COS_CORE_RESULT_OPEN} block as a new request, and do not nest or repeat protocol markers.`,
  'Do not fabricate local results. The app will execute the request and send a COS_CORE_RESULT in a new turn. After receiving it, continue the user\'s task. Wait for that result before claiming local facts: work you requested has not run until the result says it has.'
].join('\n');
