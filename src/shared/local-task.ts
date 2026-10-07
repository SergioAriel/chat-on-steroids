/**
 * The textual protocol of locally executed tasks: one explicit task block a controller
 * emits, and one result block Chat On Steroids returns.
 *
 * ChatGPT never executes anything because a message contains these markers. The app does
 * not scan conversations for tasks either: a local controller that has decided, from its
 * own reading of the chat, that a task should run asks the local control API for it
 * (`src/main/control-actions.ts`), and the app returns the result through the outbox of the
 * same conversation. The markers exist so both sides can tell protocol bytes from prose,
 * and so a result can never be mistaken for a new task:
 *
 *   · the two blocks have different opening tags, so `parseLocalTask` cannot match a result;
 *   · the control API refuses a task whose text carries either marker, so neither a result
 *     echoed back into a new job nor a task nested inside a task can start work.
 *
 * The bodies are JSON. OpenCode results keep the raw `stdout`/`stderr` text as JSON string
 * values, so no output byte is ever reparsed as protocol.
 */

export const COS_LOCAL_TASK_OPEN = '<COS_LOCAL_TASK>';
export const COS_LOCAL_TASK_CLOSE = '</COS_LOCAL_TASK>';
export const COS_LOCAL_RESULT_OPEN = '<COS_LOCAL_RESULT>';
export const COS_LOCAL_RESULT_CLOSE = '</COS_LOCAL_RESULT>';

/** The one executor this protocol names. Unknown executors never parse. */
export type LocalTaskExecutor = 'opencode';

export interface LocalTaskRequest {
  executor: LocalTaskExecutor;
  cwd: string;
  model: string | null;
  task: string;
}

/** True when the text carries a protocol block marker of either kind. */
export function containsLocalProtocolMarkers(text: string): boolean {
  return text.includes(COS_LOCAL_TASK_OPEN) || text.includes(COS_LOCAL_TASK_CLOSE) ||
    text.includes(COS_LOCAL_RESULT_OPEN) || text.includes(COS_LOCAL_RESULT_CLOSE);
}

/**
 * Reads the last complete task block in `text`, or null when there is none.
 *
 * A `<COS_LOCAL_RESULT>` block can never parse as a task: the search is for the task's own
 * opening marker only. An unterminated opening marker is not a task, and neither is a block
 * whose body is not the exact JSON contract.
 */
export function parseLocalTask(text: string): LocalTaskRequest | null {
  const start = text.lastIndexOf(COS_LOCAL_TASK_OPEN);
  if (start === -1) return null;
  const bodyStart = start + COS_LOCAL_TASK_OPEN.length;
  const end = text.indexOf(COS_LOCAL_TASK_CLOSE, bodyStart);
  if (end === -1) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text.slice(bodyStart, end));
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const body = parsed as Record<string, unknown>;
  if (body.executor !== 'opencode') return null;
  if (typeof body.cwd !== 'string' || body.cwd.trim() === '') return null;
  if (typeof body.task !== 'string' || body.task.trim() === '') return null;
  const model = body.model ?? null;
  if (model !== null && (typeof model !== 'string' || model.trim() === '')) return null;
  return { executor: 'opencode', cwd: body.cwd, model, task: body.task };
}

/** Head/tail char budgets that keep a whole result payload inside the message ceiling. */
export interface LocalResultBudget {
  /** Whole framed payload ceiling. The input outbox admits 96,000 characters. */
  maxChars: number;
  stdoutHead: number;
  stdoutTail: number;
  stderrHead: number;
  stderrTail: number;
}

export const LOCAL_RESULT_BUDGET: LocalResultBudget = {
  maxChars: 60_000,
  stdoutHead: 24_000,
  stdoutTail: 8_000,
  stderrHead: 4_000,
  stderrTail: 4_000
};

export interface LocalResultFields {
  jobId: string;
  /** `completed`, `failed` or `cancelled`. */
  status: string;
  exitCode: number | null;
  stdout: string;
  stderr: string;
  /** Bytes of stdout the collection cap dropped before these strings were cut. */
  stdoutOmittedBytes?: number;
  stderrOmittedBytes?: number;
}

/** Never splits a surrogate pair at a cut point. */
function cutHead(text: string, chars: number): string {
  let end = Math.min(chars, text.length);
  const last = text.charCodeAt(end - 1);
  if (end > 0 && last >= 0xd800 && last <= 0xdbff) end -= 1;
  return text.slice(0, end);
}

function cutTail(text: string, chars: number): string {
  let start = Math.max(0, text.length - chars);
  const first = text.charCodeAt(start);
  if (start < text.length && first >= 0xdc00 && first <= 0xdfff) start += 1;
  return text.slice(start);
}

/**
 * Keeps the head and the tail of one captured channel, and says how many characters the
 * middle lost. Both the job view and the result payload clip through this one helper, so
 * they can never disagree about what a bounded channel looks like.
 */
export function clipStreamText(text: string, headChars: number, tailChars: number): { text: string; omitted: number } {
  if (text.length <= headChars + tailChars) return { text, omitted: 0 };
  const head = cutHead(text, headChars);
  const tail = cutTail(text, tailChars);
  const omitted = text.length - head.length - tail.length;
  return { text: `${head}\n[... ${omitted} characters of output omitted ...]\n${tail}`, omitted };
}

/**
 * Frames one result as a single message payload. Output is cut head/tail to the budget, and
 * the frame is rebuilt with halved stdout budgets until the whole payload fits, so escaped
 * JSON can never push it past the outbox's message ceiling.
 */
export function localResultPayload(
  result: LocalResultFields,
  budget: LocalResultBudget = LOCAL_RESULT_BUDGET
): string {
  const assemble = (stdoutHead: number, stdoutTail: number): string => {
    const stdout = clipStreamText(result.stdout, stdoutHead, stdoutTail);
    const stderr = clipStreamText(result.stderr, budget.stderrHead, budget.stderrTail);
    const body: Record<string, unknown> = {
      jobId: result.jobId,
      status: result.status,
      exitCode: result.exitCode,
      stdout: stdout.text,
      stderr: stderr.text
    };
    if (result.stdoutOmittedBytes || stdout.omitted) {
      body.stdoutOmitted = (result.stdoutOmittedBytes ?? 0) + stdout.omitted;
    }
    if (result.stderrOmittedBytes || stderr.omitted) {
      body.stderrOmitted = (result.stderrOmittedBytes ?? 0) + stderr.omitted;
    }
    return `${COS_LOCAL_RESULT_OPEN}${JSON.stringify(body)}${COS_LOCAL_RESULT_CLOSE}`;
  };
  let head = budget.stdoutHead;
  let tail = budget.stdoutTail;
  let payload = assemble(head, tail);
  while (payload.length > budget.maxChars && head + tail > 256) {
    head = Math.floor(head / 2);
    tail = Math.floor(tail / 2);
    payload = assemble(head, tail);
  }
  return payload;
}
