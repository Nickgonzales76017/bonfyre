import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { fabricEstateSummary, handleFabricHook, hookEvidence, runFabricHookCli } from '../hook_bridge.mjs';

const fixture = () => {
  const home = mkdtempSync(join(tmpdir(), 'fabric-hook-test-'));
  const policyDir = join(home, '.bonfyre/dev-fabric');
  mkdirSync(policyDir, { recursive: true });
  writeFileSync(join(policyDir, 'fabric_policy.json'), JSON.stringify({ max_peer_depth: 1,
    forbidden_command_patterns: ['(^|\\\\s)git\\\\s+reset\\\\s+--hard', '(^|\\\\s)rm\\\\s+-rf'] }));
  return home;
};
const offline = { token: '', request: () => { throw new Error('must not contact live broker'); } };

test('PreToolUse respects peer depth and installed policy regex, including its double-escaped whitespace', async () => {
  const home = fixture();
  try {
    const opts = { home, ...offline, env: { BONFYRE_PEER_DEPTH: '1' } };
    const peer = await handleFabricHook(JSON.stringify({ hook_event_name: 'PreToolUse', tool_name: 'bonfyre-dev-fabric.duel' }), opts);
    expect(peer.hookSpecificOutput.permissionDecision).toBe('deny');
    expect(peer.hookSpecificOutput.permissionDecisionReason).toContain('recursion');
    const destructive = await handleFabricHook(JSON.stringify({ hook_event_name: 'PreToolUse', tool_name: 'Bash', tool_input: { command: 'git reset --hard' } }),
      { ...opts, env: { BONFYRE_PEER_DEPTH: '0' } });
    expect(destructive.hookSpecificOutput.permissionDecision).toBe('deny');
    expect(await handleFabricHook(JSON.stringify({ hook_event_name: 'PreToolUse', tool_name: 'Bash', input: { cmd: 'git status --short' } }),
      { ...opts, env: { BONFYRE_PEER_DEPTH: '0' } })).toBeNull();
  } finally { rmSync(home, { recursive: true, force: true }); }
});

test('missing or invalid policy fails closed for PreToolUse', async () => {
  const opts = { home: '/nonexistent', ...offline, env: {} };
  const result = await handleFabricHook(JSON.stringify({ hook_event_name: 'PreToolUse', tool_name: 'Bash' }), opts);
  expect(result.hookSpecificOutput.permissionDecision).toBe('deny');
  expect(result.hookSpecificOutput.permissionDecisionReason).toContain('policy unavailable');
});

test('hook telemetry is local, authenticated and best effort; source and event are derived', async () => {
  const seen = [];
  const request = async (url, opts) => { seen.push({ url, opts }); return Response.json({ accepted: true, seq: 1 }, { status: 202 }); };
  const output = await handleFabricHook(JSON.stringify({ event: 'PostToolUse', turn_id: 'turn', tool_name: 'Read' }),
    { token: 'fixture-secret', env: {}, request });
  expect(output).toBeNull();
  expect(seen).toHaveLength(1);
  expect(seen[0].url).toBe('http://127.0.0.1:3053/hook');
  expect(seen[0].opts.headers.authorization).toBe('Bearer fixture-secret');
  expect(JSON.parse(seen[0].opts.body)).toMatchObject({ source: 'codex', kind: 'PostToolUse' });
  expect(await handleFabricHook('{"event":"PostToolUse"}', { token: 'x', env: { BONFYRE_FABRIC_URL: 'http://remote.example' }, request })).toBeNull();
  expect(seen).toHaveLength(1);
});

test('hook telemetry retains only known identities and nonnegative counts, never private bodies', async () => {
  const seen = [];
  const request = async (_url, opts) => { seen.push(JSON.parse(opts.body)); return Response.json({accepted: true}, {status: 202}); };
  const payload = { event: 'PostToolUse', turn_id: 'turn-42', session_id: 's123',
    request_id: 'req-44', tool_name: 'Bash', prompt: 'PRIVATE_USER_PROMPT',
    cwd: '/Users/private/project', tool_input: { command: 'SECRET_COMMAND' },
    tool_response: 'SECRET_OUTPUT', env: { TOKEN: 'PRIVATE_TOKEN' },
    usage: { input_tokens: 10, output_tokens: 3, cache_read_input_tokens: 8,
      cache_creation_input_tokens: -1, extra: 'SECRET_USAGE_BODY' } };
  await handleFabricHook(JSON.stringify(payload), { token: 'fixture-secret', env: {}, request });
  expect(seen).toEqual([{ source: 'codex', kind: 'PostToolUse', session_id: 's123',
    turn_id: 'turn-42', request_id: 'req-44', tool_name: 'Bash',
    usage: { input_tokens: 10, cache_read_input_tokens: 8, output_tokens: 3 } }]);
  for (const privateValue of ['PRIVATE_USER_PROMPT','SECRET_COMMAND','SECRET_OUTPUT',
    'PRIVATE_TOKEN','SECRET_USAGE_BODY','/Users/private/project'])
    expect(JSON.stringify(seen[0])).not.toContain(privateValue);
  expect(hookEvidence({ source: 'claude', kind: 'UntrustedPrompt', tool_name: 'Bash' })).toBeNull();
  expect(hookEvidence({ source: 'claude', kind: 'Stop', session_id: 'bad/path',
    usage: { input_tokens: -1, output_tokens: Number.MAX_SAFE_INTEGER + 1 } }))
    .toEqual({ source: 'claude', kind: 'Stop' });
});

test('prompt fingerprint, compaction and stop guard preserve event output contracts', async () => {
  const prompt = 'test mission';
  const hash = createHash('sha256').update(prompt).digest('hex').slice(0, 16);
  const result = await handleFabricHook(JSON.stringify({ event: 'UserPromptSubmit', prompt }), offline);
  expect(result.hookSpecificOutput.additionalContext).toContain(hash);
  expect((await handleFabricHook('{"event":"PreCompact"}', offline)).hookSpecificOutput.additionalContext).toContain('receipts');
  const run = async () => ({ exit_code: 0, stdout: ' M file one\n?? file two\n' });
  const blocked = await handleFabricHook(JSON.stringify({ event: 'Stop', last_assistant_message: 'done' }), { ...offline, run });
  expect(blocked.decision).toBe('block');
  expect(await handleFabricHook(JSON.stringify({ event: 'Stop', last_assistant_message: 'tests passed' }), { ...offline, run })).toBeNull();
  expect(await handleFabricHook(JSON.stringify({ event: 'Stop', stop_hook_active: true }), { ...offline, run })).toBeNull();
});

test('session context uses sanitized Git status and read-only estate summary', async () => {
  let args;
  const run = async (argv, options) => { args = { argv, options }; return { exit_code: 0, stdout: ' M file\n' }; };
  const result = await handleFabricHook(JSON.stringify({ event: 'SessionStart', cwd: '/tmp/project' }),
    { ...offline, env: { GIT_DIR: '/different/repo', GIT_CONFIG_COUNT: '99' }, run, estateSummary: () => '{"world":{}}' });
  expect(result.hookSpecificOutput.additionalContext).toContain('dirty_count=1');
  expect(result.hookSpecificOutput.additionalContext).toContain('{"world":{}}');
  expect(args.argv).toEqual(['/usr/bin/git', '--no-optional-locks', 'status', '--short']);
  expect(args.options.cwd).toBe('/tmp/project');
  expect(args.options.env.GIT_DIR).toBeUndefined();
  expect(args.options.env.GIT_CONFIG_COUNT).toBe('1');
});

test('estate summary performs legacy status queries without a Python launcher', () => {
  const home = mkdtempSync(join(tmpdir(), 'fabric-estate-test-'));
  try {
    const world = join(home, '.bonfyre/estate-compiler'), operator = join(home, '.bonfyre/operator');
    mkdirSync(world, { recursive: true }); mkdirSync(operator, { recursive: true });
    const estateDb = new Database(join(world, 'estate.sqlite3'));
    estateDb.exec('CREATE TABLE entities(id TEXT); CREATE TABLE signals(state TEXT); CREATE TABLE missions(id TEXT,family TEXT,title TEXT,priority INTEGER,state TEXT,operator_goal_id TEXT,last_error TEXT,updated_at REAL); CREATE TABLE locks(acquired_at REAL);');
    estateDb.query('INSERT INTO entities VALUES(?)').run('e1'); estateDb.close();
    const operatorDb = new Database(join(operator, 'operator.sqlite3'));
    operatorDb.exec('CREATE TABLE goals(id TEXT,title TEXT,priority INTEGER,state TEXT,source TEXT,updated_at REAL);');
    operatorDb.close();
    const status = JSON.parse(fabricEstateSummary(home));
    expect(status.world.entities).toBe(1);
    expect(status.operator.running).toEqual([]);
    expect(fabricEstateSummary('/nonexistent')).toBe('estate status unavailable');
  } finally { rmSync(home, { recursive: true, force: true }); }
});

test('CLI reads bounded stdin and emits local denial on malformed oversized input', async () => {
  const output = [];
  expect(await runFabricHookCli({ input: new Blob(['{"event":"PostToolUse"}']), output: text => output.push(text), ...offline })).toBe(0);
  expect(output).toEqual([]);
  expect(await runFabricHookCli({ input: new Blob(['x'.repeat(1_000_001)]), output: text => output.push(text), ...offline })).toBe(1);
  expect(JSON.parse(output.at(-1)).hookSpecificOutput.permissionDecision).toBe('deny');
});
