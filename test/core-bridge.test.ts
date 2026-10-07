/**
 * The Core Bridge: the text-protocol fallback that runs the *existing* Core tools for a
 * conversation whose final answer carries exactly one explicit `<COS_CORE_CALL>`.
 *
 * Everything runs the real pipeline: the real recorder decides a final answer was written,
 * the real bridge gates parse it, the real kernel registrar executes the calls (real
 * filesystem, real child processes) and the real outbox carries the `<COS_CORE_RESULT>` back
 * as a new input to the originating session. Only the delivery transports that would leave
 * the process are mocked, exactly as in the control API suites.
 */

import { promises as fs } from 'node:fs';
import path from 'node:path';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { defaultConfig, initConfigPath, loadConfig, saveConfig } from '../src/main/config.js';
import { initDurableStore, resetDurableForTests, writeDurableNow } from '../src/main/durable.js';
import { addProject, assignSessionProject } from '../src/main/projects.js';
import { installCoreBridge, resetCoreBridgeForTests } from '../src/main/core-bridge.js';
import { recordChatObservations, resetRecorderForTests, recordToolCall } from '../src/main/session/recorder.js';
import { createSession, initSessionStore, readEvents, resetSessionStoreForTests } from '../src/main/session/store.js';
import { listInputs, pendingBrowserInputs, claimBrowserInput, resetInputForTests } from '../src/main/session/input.js';
import { resetInputStartupForTests } from '../src/main/session/start-input.js';
import { resetWorkspaces } from '../src/main/workspace.js';
import { unifiedExecManager } from '../src/main/codex/manager.js';
import { makeTempDir, removeTempDir, writeTree } from './helpers.js';
import { coreBridgeResult, parseCoreBridgeCall, CORE_BRIDGE_INSTRUCTIONS, COS_CORE_CALL_OPEN, COS_CORE_RESULT_CLOSE, COS_CORE_RESULT_OPEN } from '../src/shared/core-bridge.js';

vi.mock('electron', () => ({
  app: { on: vi.fn(), getPath: () => '', getVersion: vi.fn(() => '0.0.0'), getAppPath: () => process.cwd(), isPackaged: false },
  safeStorage: {
    isAsyncEncryptionAvailable: vi.fn(async () => true),
    getSelectedStorageBackend: vi.fn(() => 'gnome_libsecret'),
    encryptStringAsync: vi.fn(async (value: string) => Buffer.from(value, 'utf8')),
    decryptStringAsync: vi.fn(async (buffer: Buffer) => ({ result: buffer.toString('utf8'), shouldReEncrypt: false }))
  },
  BrowserWindow: class {},
  clipboard: { readText: () => '', writeText: () => undefined },
  shell: { openExternal: vi.fn(async () => undefined), openPath: vi.fn(async () => '') },
  nativeTheme: { themeSource: 'system' }
}));
vi.mock('../src/main/connection.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/main/connection.js')>();
  return {
    ...actual,
    connect: vi.fn(async () => undefined),
    getStatus: () => ({ ...actual.getStatus(), state: 'connected' as const }),
    onStatusChange: () => () => undefined
  };
});
vi.mock('../src/main/bridge.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/main/bridge.js')>();
  return { ...actual, startBridge: vi.fn(async () => true) };
});
vi.mock('../src/main/browser-startup.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/main/browser-startup.js')>();
  return { ...actual, wakeBrowserUrl: vi.fn(async () => undefined) };
});

let dir: string;
let outside: string;
const CHAT = 'core-bridge-chat';
let session: { id: string };

/** Base config: one approved root, bridge on, its action guard on. */
async function resetConfig(over: Record<string, unknown> = {}): Promise<void> {
  await saveConfig({
    ...defaultConfig(),
    roots: [{ name: 'workspace', path: dir }],
    coreBridge: { enabled: true, allowActions: true },
    ...over
  } as never);
}

const callBlock = (id: string, calls: Array<{ tool: string; args?: Record<string, unknown> }>) =>
  `${COS_CORE_CALL_OPEN}${JSON.stringify({ id, calls: calls.map((call) => ({ tool: call.tool, args: call.args ?? {} })) })}${'</COS_CORE_CALL>'}`;

/** One final answer, exactly as the recorder observes one. */
async function finalAnswer(text: string, options: { messageId?: string; sessionId?: string; conversationId?: string; turnEnd?: boolean } = {}): Promise<void> {
  const messageId = options.messageId ?? `assistant-${Math.random().toString(36).slice(2, 10)}`;
  const items: Array<Record<string, unknown>> = [
    { kind: 'assistant_message', time: Date.now(), messageId, text, state: 'final' }
  ];
  if (options.turnEnd) items.push({ kind: 'turn_end', time: Date.now() + 1, turnId: 'turn-1', outcome: 'completed' });
  await recordChatObservations(options.conversationId ?? CHAT, items as never);
}

async function resetOutbox(): Promise<void> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      await writeDurableNow('session-input', []);
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EPERM' || attempt >= 5) throw error;
      await new Promise((resolve) => setTimeout(resolve, 50 * attempt));
    }
  }
  resetInputForTests();
}

/** The result rows the bridge produced, as the outbox holds them. */
async function bridgeRows() {
  return (await listInputs()).filter((row) => row.text.startsWith(COS_CORE_RESULT_OPEN));
}

async function rowFor(requestId: string | null) {
  let found: Awaited<ReturnType<typeof bridgeRows>>[number] | undefined;
  await vi.waitFor(async () => {
    found = (await bridgeRows()).find((row) => {
      const body = row.text.slice(COS_CORE_RESULT_OPEN.length, -COS_CORE_RESULT_CLOSE.length);
      try { return JSON.parse(body).id === requestId; } catch { return false; }
    });
    expect(found).toBeDefined();
  });
  return found!;
}

/** The bridge result of one row, parsed. */
function payloadOf(row: { text: string }) {
  expect(row.text.startsWith(COS_CORE_RESULT_OPEN)).toBe(true);
  expect(row.text.endsWith(COS_CORE_RESULT_CLOSE)).toBe(true);
  return JSON.parse(row.text.slice(COS_CORE_RESULT_OPEN.length, -COS_CORE_RESULT_CLOSE.length)) as {
    id: string | null;
    status: 'completed' | 'rejected';
    results: Array<{ tool: string; ok: boolean; content?: string[]; error?: string; omitted?: number }>;
    error?: string;
  };
}

/** The tool calls the session recorded, newest last. */
async function recordedToolCalls(sessionId: string) {
  return (await readEvents(sessionId)).filter((event) => event.kind === 'tool_call');
}

beforeAll(async () => {
  dir = await makeTempDir('clf-core-bridge-');
  outside = await makeTempDir('clf-core-bridge-outside-');
  await writeTree(dir, {
    'notes.txt': ['note line 1', 'note line 2', 'note line 3'].join('\n') + '\n',
    'big.txt': Array.from({ length: 9_000 }, (_, i) => `filler line ${i} ${'x'.repeat(50)}`).join('\n') + '\nEND-OF-BIG\n',
    'project-a/own.txt': 'A-OWNED-NOTE\n',
    'project-b/own.txt': 'B-OWNED-NOTE\n'
  });
  await writeTree(outside, { 'secret.txt': 'hunter2\n' });
  initConfigPath(dir);
  initDurableStore(dir);
  initSessionStore(dir);
  installCoreBridge();
});

beforeEach(async () => {
  resetRecorderForTests();
  resetCoreBridgeForTests();
  resetWorkspaces();
  await resetConfig();
  await resetOutbox();
  session = await createSession({ title: 'Bridge chat', conversationId: CHAT }) as { id: string };
});

afterAll(async () => {
  await unifiedExecManager.terminateAllProcesses();
  resetInputStartupForTests();
  await resetOutbox();
  resetInputForTests();
  resetSessionStoreForTests();
  resetRecorderForTests();
  resetDurableForTests();
  await removeTempDir(dir);
  await removeTempDir(outside);
});

describe('the protocol module', () => {
  it('parses one standalone block and ignores protocol mentioned inside prose', () => {
    const id = '12345678-1234-4123-8123-123456789abc';
    expect(parseCoreBridgeCall('a normal answer')).toEqual({ kind: 'none' });
    const parsed = parseCoreBridgeCall(`\n  ${callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }])}\n`);
    expect(parsed).toEqual({
      kind: 'request',
      request: { id, calls: [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }] }
    });
    expect(parseCoreBridgeCall(`before\n${callBlock(id, [{ tool: 'read' }])}`)).toEqual({ kind: 'none' });
    expect(parseCoreBridgeCall(`${callBlock(id, [{ tool: 'read' }])}\nafter`)).toEqual({ kind: 'none' });

    const result = coreBridgeResult({ id, status: 'completed', results: [{ tool: 'read', ok: true, content: ['done'] }] });
    // A pure result carries no request marker: silence, never a request. A result followed by
    // a call is prose/mixed protocol rather than a standalone executable request, so ignore it.
    expect(parseCoreBridgeCall(result)).toEqual({ kind: 'none' });
    expect(parseCoreBridgeCall(`${result}\n${callBlock(id, [{ tool: 'read' }])}`)).toEqual({ kind: 'none' });
    expect(parseCoreBridgeCall(`${callBlock(id, [{ tool: 'read' }])} and again ${callBlock(id, [{ tool: 'read' }])}`).kind).toBe('invalid');
    expect(parseCoreBridgeCall(`${COS_CORE_CALL_OPEN}{"id":"${id}","calls":[]}`).kind).toBe('invalid');
    expect(parseCoreBridgeCall(`${COS_CORE_CALL_OPEN}not json${'</COS_CORE_CALL>'}`).kind).toBe('invalid');
    expect(parseCoreBridgeCall(`${COS_CORE_CALL_OPEN}{"id":"${id}","calls":[{"tool":"read","args":{"p":"${COS_CORE_CALL_OPEN}"}}]}${'</COS_CORE_CALL>'}`).kind).toBe('invalid');
    expect(parseCoreBridgeCall(`${COS_CORE_CALL_OPEN}{"id":"${id}","extra":1,"calls":[{"tool":"read","args":{}}]}${'</COS_CORE_CALL>'}`).kind).toBe('invalid');
    expect(parseCoreBridgeCall(`${COS_CORE_CALL_OPEN}{"id":"not-a-uuid","calls":[{"tool":"read","args":{}}]}${'</COS_CORE_CALL>'}`).kind).toBe('invalid');
    expect(parseCoreBridgeCall(`${COS_CORE_CALL_OPEN}{"id":"${id}","calls":Array.from({length: 9}, () => ({ tool: 'read', args: {} }))}${'</COS_CORE_CALL>'}`).kind).toBe('invalid');
    const oversized = `${COS_CORE_CALL_OPEN}{"id":"${id}","calls":[{"tool":"read","args":{"p":"${'x'.repeat(30_000)}"}}]}${'</COS_CORE_CALL>'}`;
    expect(parseCoreBridgeCall(oversized).kind).toBe('invalid');
  });

  it('keeps a result payload inside the message ceiling, with the omission said out loud', () => {
    const id = '12345678-1234-4123-8123-123456789abc';
    const framed = coreBridgeResult({
      id, status: 'completed',
      results: [{ tool: 'read', ok: true, content: ['HEAD\n' + 'x'.repeat(500_000) + '\nTAIL'] }]
    });
    expect(framed.length).toBeLessThanOrEqual(60_000);
    const body = JSON.parse(framed.slice(COS_CORE_RESULT_OPEN.length, -COS_CORE_RESULT_CLOSE.length));
    expect(body.results[0].content[0].startsWith('HEAD')).toBe(true);
    expect(body.results[0].content[0]).toContain('characters of output omitted');
    expect(body.results[0].content[0].endsWith('TAIL')).toBe(true);
    expect(body.results[0].omitted).toBeGreaterThan(400_000);
  });

  it('states the fallback rules the prompt repeats, including the never-echo rule', () => {
    expect(CORE_BRIDGE_INSTRUCTIONS).toContain('Local Core Bridge is available');
    expect(CORE_BRIDGE_INSTRUCTIONS).toContain('no native Core tool is available');
    expect(CORE_BRIDGE_INSTRUCTIONS).toContain('at most 8 calls');
    expect(CORE_BRIDGE_INSTRUCTIONS).toContain('Never echo a <COS_CORE_RESULT>');
    expect(CORE_BRIDGE_INSTRUCTIONS).toContain('Do not fabricate local results');
    // The request example and its argument cheat sheet name read's plural "paths" array —
    // a model shown only args:{} guesses the singular "path" and the call is rejected.
    expect(CORE_BRIDGE_INSTRUCTIONS).toContain('"args":{"paths":["/<root>/folder"]}');
    expect(CORE_BRIDGE_INSTRUCTIONS).toContain('never a singular "path"');
    expect(CORE_BRIDGE_INSTRUCTIONS).toContain('exec_command takes exactly one of "cmd" or "cmds"');
  });
});

describe('detection', () => {
  it('executes a final answer with one valid call through the canonical read tool', async () => {
    const id = '11111111-1111-4111-8111-111111111111';
    await finalAnswer(callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]));

    const row = await rowFor(id);
    expect(row.sessionId).toBe(session.id);
    expect(row.conversationId).toBe(CHAT);
    const payload = payloadOf(row);
    expect(payload.status).toBe('completed');
    expect(payload.results).toHaveLength(1);
    expect(payload.results[0]).toMatchObject({ tool: 'read', ok: true });
    expect(payload.results[0]!.content![0]).toContain('note line 2');

    const calls = await recordedToolCalls(session.id);
    expect(calls).toHaveLength(1);
    const call = calls[0]!.call;
    expect(call.attribution).toBe('core_bridge');
    expect(call.attributionMethod).toBe('core_bridge');
    expect(call.conversationId).toBe(CHAT);
    expect(call.requestId).toBeNull();
    // App-executed work in that chat, never filed as MCP transport traffic.
    expect(calls[0]!.source).toBe('app');
  });

  it('does not execute protocol examples or mentions embedded in prose', async () => {
    const id = '11111111-1111-4111-8111-111111111112';
    await finalAnswer(`For example, a request would look like ${callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }])}, but this answer is only explanatory.`);

    await new Promise((resolve) => setTimeout(resolve, 100));
    expect((await bridgeRows()).some((row) => row.text.includes(id))).toBe(false);
    expect(await recordedToolCalls(session.id)).toHaveLength(0);
  });

  it('scopes request ids to the session that owns them', async () => {
    const id = '12121212-1212-4121-8121-121212121212';
    const secondChat = 'core-bridge-chat-two';
    const second = await createSession({ title: 'Second bridge chat', conversationId: secondChat }) as { id: string };
    const request = callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]);

    await finalAnswer(request, { messageId: 'same-id-first', conversationId: CHAT });
    await finalAnswer(request, { messageId: 'same-id-second', conversationId: secondChat });

    for (let attempt = 0; attempt < 30; attempt += 1) {
      if ((await bridgeRows()).filter((row) => payloadOf(row).id === id).length === 2) break;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const rows = (await bridgeRows()).filter((row) => payloadOf(row).id === id);
    expect(rows).toHaveLength(2);
    expect(new Set(rows.map((row) => row.sessionId))).toEqual(new Set([session.id, second.id]));
    expect(await recordedToolCalls(session.id)).toHaveLength(1);
    expect(await recordedToolCalls(second.id)).toHaveLength(1);
  });

  it('does not execute streaming assistant text', async () => {
    const id = '22222222-2222-4222-8222-222222222222';
    await recordChatObservations(CHAT, [
      { kind: 'assistant_message', time: Date.now(), messageId: 'partial', text: callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]), state: 'streaming' }
    ]);
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(await bridgeRows()).toEqual([]);
    expect(await recordedToolCalls(session.id)).toEqual([]);
  });

  it('does nothing for a normal assistant answer', async () => {
    await finalAnswer('The work is done; the tests pass.');
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(await bridgeRows()).toEqual([]);
    expect(await recordedToolCalls(session.id)).toEqual([]);
  });

  it('never executes from a result message or from mixed protocol/prose', async () => {
    const id = '33333333-3333-4333-8333-333333333333';
    const result = coreBridgeResult({ id: 'other', status: 'completed', results: [{ tool: 'read', ok: true, content: ['done'] }] });
    // A pure result quote carries no call marker at all: silence.
    await finalAnswer(`Here is what came back:\n${result}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(await bridgeRows()).toEqual([]);

    // Mixed protocol/prose is not an executable request: ignore it, never execute it.
    await finalAnswer(`${result}\n${callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }])}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(await bridgeRows()).toEqual([]);
    expect(await recordedToolCalls(session.id)).toEqual([]);
  });

  it('does nothing while the bridge is disabled', async () => {
    await saveConfig({ ...defaultConfig(), roots: [{ name: 'workspace', path: dir }], coreBridge: { enabled: false, allowActions: false } } as never);
    const id = '44444444-4444-4444-8444-444444444444';
    await finalAnswer(callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]));
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(await bridgeRows()).toEqual([]);
    expect(await recordedToolCalls(session.id)).toEqual([]);
  });

  it('ignores worker and helper chats', async () => {
    const worker = await createSession({
      title: 'Worker', conversationId: 'core-bridge-worker',
      origin: { kind: 'worker', fromSessionId: session.id, agentId: 'worker-1', task: 't' } as never
    });
    const id = '55555555-5555-4555-8555-555555555555';
    await finalAnswer(callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]),
      { conversationId: 'core-bridge-worker', messageId: 'worker-final' });
    // The worker's session id must own the observation; re-point it through the same chat.
    const recorded = await recordChatObservations('core-bridge-worker', [
      { kind: 'assistant_message', time: Date.now(), messageId: 'worker-final-2', text: callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]), state: 'final' }
    ]);
    expect(recorded.sessionId).toBe(worker.id);
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(await bridgeRows()).toEqual([]);
    expect(await recordedToolCalls(worker.id)).toEqual([]);
  });

  it('executes a request once per request UUID, and never re-fires an unchanged message', async () => {
    const id = '66666666-6666-4666-8666-666666666666';
    const text = callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]);
    await finalAnswer(text, { messageId: 'stable-final' });
    await rowFor(id);

    // The same message observed again wrote nothing new: the listener never fired.
    await recordChatObservations(CHAT, [
      { kind: 'assistant_message', time: Date.now(), messageId: 'stable-final', text, state: 'final' }
    ]);
    // A different message repeating the same request UUID is refused by the durable ledger.
    await finalAnswer(text, { messageId: 'retry-final' });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect((await bridgeRows()).filter((row) => payloadOf(row).id === id)).toHaveLength(1);
    expect((await recordedToolCalls(session.id)).length).toBe(1);

    // The ledger survives the process state: a fresh install of the same listener still refuses.
    resetCoreBridgeForTests();
    await finalAnswer(text, { messageId: 'third-final' });
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect((await bridgeRows()).filter((row) => payloadOf(row).id === id)).toHaveLength(1);
  });
});

describe('delivery', () => {
  it('names the turn it answers, so its already-recorded completion releases the head', async () => {
    // The exact stuck sequence observed live (2026-10-07): the final carrying the request
    // is recorded, the batch runs, and the result lands with createdAt after that final's
    // turn_end. Without the source turn named, the outbox waited for a *later* completion
    // and the answer only moved with the user's next message.
    const id = 'b0b0b0b0-b0b0-4b0b-8b0b-b0b0b0b0b0b0';
    const base = Date.now();
    await recordChatObservations(CHAT, [
      { kind: 'user_message', time: base, messageId: 'q-src', text: 'look at the local notes' },
      { kind: 'turn_start', time: base + 1, turnId: 'turn-src' },
      { kind: 'assistant_message', time: base + 2, messageId: 'a-src', turnId: 'turn-src', text: callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]), state: 'final' },
      { kind: 'turn_end', time: base + 3, turnId: 'turn-src', outcome: 'completed' }
    ] as never);
    const row = await rowFor(id);

    expect(row.mode).toBe('after-turn');
    expect(row.queuedTurn).toEqual({ conversationId: CHAT, turnId: 'turn-src' });
    // The bridge's own recorded calls run after that final; they must not veto its
    // completion or the head's delivery — the answer is deliverable right now.
    expect((await recordedToolCalls(session.id)).length).toBe(1);
    expect((await pendingBrowserInputs()).some((item) => item.id === row.id)).toBe(true);
    expect(await claimBrowserInput(row.id, 'bridge-document', CHAT, true)).not.toBeNull();
    expect((await listInputs()).find((item) => item.id === row.id)?.state).toBe('browser');
  });

  it('never interrupts its still-active source turn', async () => {
    const id = 'c1c1c1c1-c1c1-4c1c-8c1c-c1c1c1c1c1c1';
    const base = Date.now();
    await recordChatObservations(CHAT, [
      { kind: 'user_message', time: base, messageId: 'q-live', text: 'look at the local notes' },
      { kind: 'turn_start', time: base + 1, turnId: 'turn-live' },
      { kind: 'assistant_message', time: base + 2, messageId: 'a-live', turnId: 'turn-live', text: callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]), state: 'final' }
    ] as never);
    const row = await rowFor(id);

    expect(row.queuedTurn).toEqual({ conversationId: CHAT, turnId: 'turn-live' });
    // The source turn has no end yet: claimed delivery waits for it.
    expect((await pendingBrowserInputs()).some((item) => item.id === row.id)).toBe(false);
    expect(await claimBrowserInput(row.id, 'bridge-document', CHAT, true)).toBeNull();
  });
});

describe('a batch', () => {
  it('runs its calls sequentially, in the order requested', async () => {
    const id = '77777777-7777-4777-8777-777777777777';
    const log = path.join(dir, 'sequence.log');
    await finalAnswer(callBlock(id, [
      { tool: 'exec_command', args: { cmds: [`printf first >> ${log}`] } },
      { tool: 'exec_command', args: { cmds: [`printf second >> ${log}`] } }
    ]));
    await rowFor(id);
    expect(await fs.readFile(log, 'utf8')).toBe('firstsecond');
    const calls = await recordedToolCalls(session.id);
    expect(calls.map((event) => event.call.tool)).toEqual(['exec_command', 'exec_command']);
    for (const event of calls) {
      expect(event.call.attribution).toBe('core_bridge');
      expect(event.source).toBe('app');
    }
  });

  it('is refused whole when it asks for more than the maximum', async () => {
    const id = '88888888-8888-4888-8888-888888888888';
    const calls = Array.from({ length: 9 }, () => ({ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }));
    await finalAnswer(callBlock(id, calls));
    const row = await rowFor(id);
    expect(payloadOf(row).status).toBe('rejected');
    expect(await recordedToolCalls(session.id)).toEqual([]);
  });

  it('answers each refused call without refusing the rest', async () => {
    const id = '99999999-9999-4999-8999-999999999999';
    // The finish tool switch on, so session_finish is genuinely registered and the refusal
    // is the bridge's own — not an unknown-tool answer.
    await resetConfig({ ui: { ...defaultConfig().ui, finishTool: true } });
    await finalAnswer(callBlock(id, [
      { tool: 'read', args: { paths: ['/workspace/notes.txt'] } },
      { tool: 'computer', args: {} },
      { tool: 'agents', args: { action: 'status' } },
      { tool: 'session_finish', args: {} },
      { tool: 'update_plan', args: { steps: [] } }
    ]));
    const row = await rowFor(id);
    const payload = payloadOf(row);
    expect(payload.status).toBe('completed');
    const byTool = new Map(payload.results.map((result) => [result.tool, result]));
    expect(byTool.get('read')!.ok).toBe(true);
    expect(byTool.get('computer')!.error).toContain('UNKNOWN_TOOL');
    expect(byTool.get('agents')!.error).toContain('BRIDGE_TOOL_REFUSED');
    expect(byTool.get('session_finish')!.error).toContain('BRIDGE_TOOL_REFUSED');
    expect(byTool.get('update_plan')!.error).toContain('BRIDGE_TOOL_REFUSED');
    // Only the read ran.
    expect((await recordedToolCalls(session.id)).map((event) => event.call.tool)).toEqual(['read']);
  });

  it('keeps Desktop and Plugins tool names unreachable', async () => {
    const id = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
    await finalAnswer(callBlock(id, [
      { tool: 'browser_snapshot', args: {} },
      { tool: 'exec', args: { code: '1' } }
    ]));
    const row = await rowFor(id);
    const payload = payloadOf(row);
    expect(payload.results.map((result) => result.ok)).toEqual([false, false]);
    expect(await recordedToolCalls(session.id)).toEqual([]);
  });
});

describe('the existing guards', () => {
  it('keeps the sandbox as the workspace authority: outside the roots is refused', async () => {
    const id = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
    await finalAnswer(callBlock(id, [
      { tool: 'read', args: { paths: [path.join(outside, 'secret.txt')] } },
      { tool: 'read', args: { paths: ['/workspace/../escape.txt'] } }
    ]));
    const row = await rowFor(id);
    const payload = payloadOf(row);
    expect(payload.results.map((result) => result.ok)).toEqual([false, false]);
    expect(payload.results[0]!.error).toContain('not inside an approved folder');
    expect(payload.results[1]!.error).toContain('traversal');
    // Refusals are ordinary tool outcomes and are recorded, exactly as MCP refusals are.
    const calls = await recordedToolCalls(session.id);
    expect(calls.map((event) => event.call.outcome)).toEqual(['tool_rejected', 'tool_rejected']);
    expect(row.text).not.toContain('hunter2');
  });

  it('returns the existing Core answer when the read capability is disabled', async () => {
    const caps = { ...defaultConfig().capabilities, read: false, browse: true, metadata: false };
    await saveConfig({ ...defaultConfig(), roots: [{ name: 'workspace', path: dir }], capabilities: caps, coreBridge: { enabled: true, allowActions: true } } as never);
    const id = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
    await finalAnswer(callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]));
    const row = await rowFor(id);
    const payload = payloadOf(row);
    // The existing per-path answer names the missing permission and returns no content lines.
    const first = payload.results[0]!;
    expect(first.error ?? first.content![0]).toContain('file contents need the Read files permission');
    expect(first.content?.[0] ?? first.error ?? '').not.toContain('note line 2');
    // With every reading permission off, a fresh Core surface does not register `read` at all.
    await saveConfig({ ...defaultConfig(), roots: [{ name: 'workspace', path: dir }], capabilities: { ...caps, browse: false }, coreBridge: { enabled: true, allowActions: true } } as never);
    const secondId = 'cdcdcdcd-cdcd-4cdc-8cdc-cdcdcdcdcdcd';
    await finalAnswer(callBlock(secondId, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]));
    const second = payloadOf(await rowFor(secondId));
    expect(second.results[0]!.ok).toBe(false);
    expect(second.results[0]!.error).toContain('UNKNOWN_TOOL');
    expect(second.results[0]!.error).not.toContain('Read files permission');
  });

  it('lets Read-only keep writes and commands from even being offered', async () => {
    const id = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
    await resetConfig({ readOnly: true });
    const before = await fs.readFile(path.join(dir, 'notes.txt'), 'utf8');
    await finalAnswer(callBlock(id, [
      { tool: 'read', args: { paths: ['/workspace/notes.txt'] } },
      { tool: 'apply_patch', args: { patch: '*** Begin Patch\n*** Update File: /workspace/notes.txt\n@@\n-note line 1\n+changed\n*** End Patch' } },
      { tool: 'exec_command', args: { cmds: ['printf nope >> /workspace/notes.txt'] } }
    ]));
    const row = await rowFor(id);
    const payload = payloadOf(row);
    expect(payload.results[0]!.ok).toBe(true);
    for (const refused of payload.results.slice(1)) {
      expect(refused.ok).toBe(false);
      expect(refused.error).toContain('not available on this connector');
    }
    expect(await fs.readFile(path.join(dir, 'notes.txt'), 'utf8')).toBe(before);
    expect((await recordedToolCalls(session.id)).map((event) => event.call.tool)).toEqual(['read']);
  });

  it('still enforces the command launch policy', async () => {
    const id = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee';
    await resetConfig({ commandAllowlist: { enabled: true, mode: 'allow', rules: ['git status'] } });
    await finalAnswer(callBlock(id, [{ tool: 'exec_command', args: { cmds: ['echo hello'] } }]));
    const row = await rowFor(id);
    const payload = payloadOf(row);
    expect(payload.results[0]!.ok).toBe(false);
    expect(payload.results[0]!.error).toContain('COMMAND_NOT_ALLOWED');
  });

  it('refuses patching and commands until its own action guard is on', async () => {
    const id = 'ffffffff-ffff-4fff-8fff-ffffffffffff';
    await saveConfig({ ...defaultConfig(), roots: [{ name: 'workspace', path: dir }], coreBridge: { enabled: true, allowActions: false } } as never);
    await finalAnswer(callBlock(id, [
      { tool: 'read', args: { paths: ['/workspace/notes.txt'] } },
      { tool: 'apply_patch', args: { patch: '*** Begin Patch\n*** Update File: /workspace/notes.txt\n@@\n-note line 1\n+changed\n*** End Patch' } },
      { tool: 'exec_command', args: { cmds: ['echo hello'] } }
    ]));
    const row = await rowFor(id);
    const payload = payloadOf(row);
    expect(payload.results[0]!.ok).toBe(true);
    expect(payload.results[1]!.error).toContain('BRIDGE_ACTIONS_DISABLED');
    expect(payload.results[2]!.error).toContain('BRIDGE_ACTIONS_DISABLED');
    expect(await fs.readFile(path.join(dir, 'notes.txt'), 'utf8')).toContain('note line 1');
    expect((await recordedToolCalls(session.id)).map((event) => event.call.tool)).toEqual(['read']);
  });

  it('resolves relative paths against the originating session\'s selected project', async () => {
    const projectA = await addProject(path.join(dir, 'project-a'));
    const projectB = await addProject(path.join(dir, 'project-b'));
    const chatA = await createSession({ title: 'A', conversationId: 'bridge-chat-a' });
    const chatB = await createSession({ title: 'B', conversationId: 'bridge-chat-b' });
    await assignSessionProject(chatA.id, projectA.id);
    await assignSessionProject(chatB.id, projectB.id);

    const id = '10101010-1010-4101-8101-101010101010';
    await finalAnswer(callBlock(id, [{ tool: 'read', args: { paths: ['own.txt'] } }]),
      { sessionId: chatA.id, conversationId: 'bridge-chat-a' });
    const row = await rowFor(id);
    const payload = payloadOf(row);
    expect(payload.results[0]!.ok).toBe(true);
    expect(payload.results[0]!.content![0]).toContain('A-OWNED-NOTE');
    expect(payload.results[0]!.content![0]).not.toContain('B-OWNED-NOTE');
    const calls = await recordedToolCalls(chatA.id);
    expect(calls).toHaveLength(1);
    expect(await recordedToolCalls(chatB.id)).toEqual([]);
  });

  it('keeps its result bounded however large the local output', async () => {
    const id = '12121212-1212-4121-8121-121212121212';
    await finalAnswer(callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/big.txt'] } }]));
    const row = await rowFor(id);
    expect(row.text.length).toBeLessThanOrEqual(60_000);
    const payload = payloadOf(row);
    expect(payload.results[0]!.ok).toBe(true);
    expect(payload.results[0]!.content![0]).toContain('characters of output omitted');
  });
});

describe('the result delivery', () => {
  it('is a new after-turn input of the originating session once the answer ended', async () => {
    const id = '13131313-1313-4131-8131-131313131313';
    await finalAnswer(callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]), { turnEnd: true });
    const row = await rowFor(id);
    expect(row).toMatchObject({
      sessionId: session.id,
      conversationId: CHAT,
      state: 'queued',
      mode: 'after-turn',
      authoredSource: 'none'
    });
    // A new turn's message, not an injection into the turn that already ended.
    expect(row.toolTurnId).toBeUndefined();
    expect(row.transportIntent ?? null).toBeNull();
    expect(row.opening).toBeUndefined();
  });

  it('reaches only the session that asked, never a neighbour', async () => {
    const neighbour = await createSession({ title: 'Neighbour', conversationId: 'bridge-neighbour' });
    const id = '14141414-1414-4141-8141-141414141414';
    await finalAnswer(callBlock(id, [{ tool: 'read', args: { paths: ['/workspace/notes.txt'] } }]));
    await rowFor(id);
    const rows = await listInputs();
    expect(rows.filter((row) => row.sessionId === neighbour.id)).toEqual([]);
    expect(await recordedToolCalls(neighbour.id)).toEqual([]);
  });

});

describe('unchanged native behaviour', () => {
  it('still files an ordinary exact-request call as MCP traffic', async () => {
    await recordToolCall({
      conversationId: CHAT,
      requestId: 'wfr_bridge_regression',
      sessionId: session.id,
      tool: 'read',
      args: { paths: ['/workspace/notes.txt'] },
      content: [{ type: 'text', text: 'ok' }],
      outcome: 'ok',
      durationMs: 3,
      startedAt: Date.now()
    });
    const calls = await recordedToolCalls(session.id);
    expect(calls.at(-1)!.source).toBe('mcp');
    expect(calls.at(-1)!.call.attribution).toBe('request_id');
    expect(calls.at(-1)!.call.attributionMethod).toBe('request_id');
    expect(calls.at(-1)!.call.requestId).toBe('wfr_bridge_regression');
  });

  it('loads the switches off by default, and the action guard never outlives the bridge', async () => {
    expect(defaultConfig().coreBridge).toEqual({ enabled: false, allowActions: false });
    await saveConfig({ ...defaultConfig(), roots: [{ name: 'workspace', path: dir }], coreBridge: { enabled: false, allowActions: true } } as never);
    expect((await import('../src/main/config.js')).getConfig().coreBridge).toEqual({ enabled: false, allowActions: false });
    // A damaged stored value repairs field-wise to off, like the control API pair.
    await fs.writeFile(path.join(dir, 'config.json'), JSON.stringify({
      ...defaultConfig(), roots: [{ name: 'workspace', path: dir }],
      coreBridge: { enabled: 'yes', allowActions: true }
    }));
    await loadConfig();
    expect((await import('../src/main/config.js')).getConfig().coreBridge).toEqual({ enabled: false, allowActions: false });
  });
});
