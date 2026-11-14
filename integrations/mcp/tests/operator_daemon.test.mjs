import { test, expect } from 'bun:test';
import { createOperatorDaemon, createOperatorTransport } from '../operator_daemon.mjs';
import { createOperatorStore } from '../operator_store.mjs';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const objectSchema = { type: 'object', properties: { value: { type: 'integer' } }, required: ['value'], additionalProperties: false };
const finishSchema = { type: 'object', properties: {
  disposition: { type: 'string', enum: ['resolved', 'blocked', 'retry', 'deferred'] },
  summary: { type: 'string' }, evidence: { type: 'array', items: { type: 'string' } },
  new_goals: { type: 'array', maxItems: 16, items: { type: 'object', properties: {
    title: { type: 'string' }, details: { type: 'string' }, priority: { type: 'integer' }, dedupe_key: { type: 'string' },
  }, required: ['title'], additionalProperties: false } },
}, required: ['disposition', 'summary'], additionalProperties: false };
const call = (name, args, id) => ({ type: 'function_call', name, arguments: JSON.stringify(args), call_id: id });
const answer = (id, output, usage = { input_tokens: 3, output_tokens: 2 }) => ({ id, status: 'completed', output, usage });
const finish = (id = 'finish') => call('operator_finish', { disposition: 'resolved', summary: 'verified', evidence: ['fixture'] }, id);

function fixture({ responses = [answer('r1', [finish()])], policy: overrides = {}, paused = false,
  onExecute = async () => ({ ok: true }), clock = { seconds: 1000 } } = {}) {
  const calls = { store: [], response: [], effects: [], receipts: [], deadlines: [] };
  const policy = { autonomous: true, mode: 'power', allow_provider_requests: true,
    approved_provider_models: ['fixture-model'], max_rounds_per_cycle: 8,
    max_tool_calls_per_cycle: 80, cycle_timeout_seconds: 100, max_cycles_per_hour: 6, ...overrides };
  const config = { model: 'fixture-model', paused: () => paused, loadPolicy: () => policy };
  const goal = { id: 'goal-1', title: 'Fixture goal', details: 'exact', priority: 50, attempts: 0, source: 'fixture' };
  const run = { id: 'run-1', started_at: 1000, usage_json: '{}' };
  let claimed = false;
  let usage = { input_tokens: 0, output_tokens: 0, total_tokens: 0, response_count: 0 };
  const store = {
    metaGet(...args) { calls.store.push(['metaGet', ...args]); return '0'; },
    ingestEvents(...args) { calls.store.push(['ingestEvents', ...args]); },
    budgetOk() { calls.store.push(['budgetOk']); return { okay: true }; },
    cyclesLastHour() { calls.store.push(['cyclesLastHour']); return 0; },
    reconcileOrphans() { calls.store.push(['reconcileOrphans']); return 0; },
    claimGoal() { calls.store.push(['claimGoal']); if (claimed) return null; claimed = true; return { goal, run }; },
    recordResponse(value) {
      calls.store.push(['recordResponse', value]);
      const input = value.usage.input_tokens ?? 0, output = value.usage.output_tokens ?? 0;
      usage = { input_tokens: usage.input_tokens + input, output_tokens: usage.output_tokens + output,
        total_tokens: usage.total_tokens + input + output, response_count: usage.response_count + 1 };
    },
    recordAction(value) { calls.store.push(['recordAction', value]); },
    addGoal(value) { calls.store.push(['addGoal', value]); return value; },
    finishGoal(value) { calls.store.push(['finishGoal', value]); return {
      goal: { ...goal, state: value.disposition === 'resolved' ? 'resolved' : 'retry' },
      run: { ...run, state: value.disposition, summary: value.summary, usage_json: JSON.stringify(usage) },
    }; },
    failRun(value) { calls.store.push(['failRun', value]); },
  };
  const definitions = [
    { name: 'read_fixture', parameters: objectSchema },
    { name: 'write_fixture', parameters: objectSchema },
    { name: 'operator_finish', parameters: finishSchema },
    { name: 'operator_add_goal', parameters: { type: 'object', properties: { title: { type: 'string' } }, required: ['title'], additionalProperties: false } },
  ];
  const catalog = Object.fromEntries(definitions.map(item => [item.name,
    { annotations: { readOnlyHint: item.name === 'read_fixture' } }]));
  const tools = {
    async catalog() { calls.effects.push(['catalog']); return { tools: definitions, catalog }; },
    async execute(runId, name, args) { calls.effects.push(['execute', name, args]); return onExecute(runId, name, args); },
  };
  let index = 0;
  const transport = {
    async events(after) { calls.effects.push(['events', after]); return { events: [], next_after: after }; },
    async response(payload) { calls.response.push(payload); return responses[index++] ?? answer(`r${index}`, []); },
    setDeadline(value) { calls.deadlines.push(value); },
    redact(value) { return String(value).replaceAll('fixture-secret', '[REDACTED]'); },
  };
  const daemon = createOperatorDaemon({ config, store, tools, transport, now: () => clock.seconds,
    onReceipt: receipt => { calls.receipts.push(receipt); } });
  return { daemon, calls, config, store, tools, transport, clock, setPaused(value) { paused = value; } };
}

test('STOP and disabled policy cause zero store, catalog, event, or API calls', async () => {
  for (const input of [{ paused: true }, { policy: { autonomous: false } }]) {
    const f = fixture(input);
    expect((await f.daemon.once()).state).toBe(input.paused ? 'paused' : 'disabled');
    expect(f.calls.store).toEqual([]);
    expect(f.calls.effects).toEqual([]);
    expect(f.calls.response).toEqual([]);
  }
});

test('terminal-only resolution consumes no tool-effect slot', async () => {
  const f = fixture();
  const result = await f.daemon.once();
  expect(result.state).toBe('completed');
  expect(result.receipt.tool_calls).toBe(0);
  expect(f.calls.effects.filter(item => item[0] === 'execute')).toEqual([]);
});

test('every model response repeats instructions, links previous id, and aggregates per-response usage', async () => {
  const f = fixture({ responses: [
    answer('r1', [call('read_fixture', { value: 7 }, 'c1')], { input_tokens: 11, output_tokens: 2 }),
    answer('r2', [finish()], { input_tokens: 5, output_tokens: 3 }),
  ] });
  const result = await f.daemon.once();
  expect(result.state).toBe('completed');
  expect(f.calls.response).toHaveLength(2);
  expect(f.calls.response[0].instructions).toContain('delegated authority');
  expect(f.calls.response[1].instructions).toBe(f.calls.response[0].instructions);
  expect(f.calls.response[1].previous_response_id).toBe('r1');
  expect(f.calls.response[1].input).toEqual([{ type: 'function_call_output', call_id: 'c1', output: '{"ok":true}' }]);
  expect(result.receipt.usage).toEqual({ input_tokens: 16, output_tokens: 5, total_tokens: 21, response_count: 2 });
  expect(f.calls.store.filter(item => item[0] === 'recordResponse').map(item => item[1].response_id)).toEqual(['r1', 'r2']);
});

test('unknown and invalid-schema calls are refused without tool effects; duplicate IDs fail the cycle', async () => {
  const f = fixture({ responses: [
    answer('r1', [call('unknown_fixture', {}, 'u'), call('read_fixture', { value: 'seven' }, 'bad')]),
    answer('r2', [finish()]),
  ] });
  expect((await f.daemon.once()).state).toBe('completed');
  expect(f.calls.effects.filter(item => item[0] === 'execute')).toEqual([]);
  expect(f.calls.store.filter(item => item[0] === 'recordAction').map(item => item[1].state)).toEqual(['refused', 'refused']);
  const duplicate = fixture({ responses: [answer('r1', [call('read_fixture', { value: 1 }, 'same'), call('read_fixture', { value: 2 }, 'same')])] });
  const outcome = await duplicate.daemon.once();
  expect(outcome.state).toBe('failed');
  expect(outcome.error).toContain('duplicate function call identity');
  expect(duplicate.calls.effects.filter(item => item[0] === 'execute')).toEqual([]);
  expect(duplicate.calls.store.some(item => item[0] === 'failRun')).toBe(true);
});

test('at most eight reads overlap; writes start after reads and retain model order', async () => {
  let active = 0, peak = 0;
  const sequence = [];
  const calls = Array.from({ length: 10 }, (_, index) => call('read_fixture', { value: index }, `read-${index}`));
  calls.push(call('write_fixture', { value: 10 }, 'write-10'), call('write_fixture', { value: 11 }, 'write-11'));
  const f = fixture({ responses: [answer('r1', calls), answer('r2', [finish()])],
    onExecute: async (_runId, name, args) => {
      sequence.push(`start:${name}:${args.value}`);
      active++; peak = Math.max(peak, active);
      await new Promise(resolve => setTimeout(resolve, 2));
      active--; sequence.push(`end:${name}:${args.value}`);
      return { ok: true };
    } });
  expect((await f.daemon.once()).state).toBe('completed');
  expect(peak).toBe(8);
  const firstWrite = sequence.findIndex(item => item.startsWith('start:write_fixture'));
  expect(firstWrite).toBeGreaterThan(sequence.findLastIndex(item => item.startsWith('end:read_fixture')));
  expect(sequence.filter(item => item.startsWith('start:write_fixture'))).toEqual(['start:write_fixture:10', 'start:write_fixture:11']);
});

test('deadline, midcycle STOP, and tool failure become retryable failures without trailing effects', async () => {
  const deadline = fixture({ responses: [answer('r1', [call('write_fixture', { value: 1 }, 'c1')])],
    clock: { seconds: 1000 } });
  deadline.transport.response = async () => { deadline.clock.seconds = 1101; return answer('r1', [call('write_fixture', { value: 1 }, 'c1')]); };
  expect((await deadline.daemon.once()).state).toBe('failed');
  expect(deadline.calls.effects.filter(item => item[0] === 'execute')).toEqual([]);
  expect(deadline.calls.store.some(item => item[0] === 'failRun')).toBe(true);

  const paused = fixture({ responses: [answer('r1', [call('write_fixture', { value: 1 }, 'c1')])] });
  paused.transport.response = async () => { paused.setPaused(true); return answer('r1', [call('write_fixture', { value: 1 }, 'c1')]); };
  expect((await paused.daemon.once()).state).toBe('failed');
  expect(paused.calls.effects.filter(item => item[0] === 'execute')).toEqual([]);
  expect(paused.calls.store.some(item => item[0] === 'failRun')).toBe(true);

  const failed = fixture({ responses: [answer('r1', [call('write_fixture', { value: 1 }, 'c1')])],
    onExecute: async () => { throw new Error('fixture-secret tool outage'); } });
  const outcome = await failed.daemon.once();
  expect(outcome.state).toBe('failed');
  expect(outcome.error).not.toContain('fixture-secret');
  expect(failed.calls.store.some(item => item[0] === 'failRun')).toBe(true);
});

test('tool-call budget and no-finish response produce retry, never synthetic resolution', async () => {
  const f = fixture({ policy: { max_tool_calls_per_cycle: 1 }, responses: [answer('r1', [
    call('read_fixture', { value: 1 }, 'c1'), call('write_fixture', { value: 2 }, 'c2'),
  ]), answer('r2', [call('write_fixture', { value: 3 }, 'c3')])] });
  const outcome = await f.daemon.once();
  expect(outcome.state).toBe('completed');
  expect(outcome.receipt.disposition).toBe('retry');
  expect(outcome.receipt.tool_calls).toBe(1);
  expect(f.calls.effects.filter(item => item[0] === 'execute').map(item => item[1])).toEqual(['read_fixture']);
  expect(f.calls.response).toHaveLength(2);
  expect(f.calls.store.find(item => item[0] === 'finishGoal')[1].disposition).toBe('retry');
  const noCalls = fixture({ responses: [answer('r1', [])] });
  expect((await noCalls.daemon.once()).receipt.disposition).toBe('retry');
});

test('transport redacts auth/API failures and refuses MCP isError without exposing body', async () => {
  const config = { token: 'fixture-token-secret', gateway: 'http://127.0.0.1:13052', model: 'fixture', env: {} };
  const api = createOperatorTransport(config, { secrets: async () => ({ OPENAI_API_KEY: 'fixture-api-secret' }),
    request: async (_url, options) => {
      if (options.headers.authorization === 'Bearer fixture-api-secret')
        return Response.json({ error: { code: 'insufficient_quota', message: 'fixture-api-secret' } }, { status: 401 });
      const id = JSON.parse(options.body).id;
      return Response.json({ jsonrpc: '2.0', id, result: { isError: true, content: [{ type: 'text', text: 'fixture-token-secret' }] } });
    } });
  await expect(api.response({ model: 'fixture', input: 'safe' }, 5)).rejects.toThrow('operator HTTP 401: insufficient_quota');
  await expect(api.mcpCall('project_status', {})).rejects.toThrow('MCP tool project_status refused');
  expect(api.redact('fixture-api-secret fixture-token-secret')).toBe('[REDACTED] [REDACTED]');
});

test('temporary real SQLite store stays absent under STOP, then accounts two responses and resolves one goal', async () => {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-operator-daemon-fixture-'));
  const home = join(base, 'operator');
  const store = createOperatorStore({ home, now: () => 1000 });
  const policy = { autonomous: true, allow_provider_requests: true, approved_provider_models: ['fixture'],
    daily_cycle_budget: 1, max_cycles_per_hour: 6, max_rounds_per_cycle: 4,
    max_tool_calls_per_cycle: 8, cycle_timeout_seconds: 60 };
  let paused = true, index = 0, requests = 0;
  const config = { model: 'fixture', paused: () => paused, loadPolicy: () => policy };
  const transport = { events: async () => ({ events: [], next_after: 0 }),
    response: async () => { requests++; return [
      answer('real-r1', [call('read_fixture', { value: 1 }, 'real-c1')], { input_tokens: 4, output_tokens: 2 }),
      answer('real-r2', [finish()], { input_tokens: 6, output_tokens: 3 }),
    ][index++]; }, redact: value => String(value), setDeadline() {} };
  const tools = { catalog: async () => ({ tools: [
    { name: 'read_fixture', parameters: objectSchema }, { name: 'operator_finish', parameters: finishSchema },
  ], catalog: { read_fixture: { annotations: { readOnlyHint: true } } } }),
  execute: async () => ({ result: 'read' }) };
  const daemon = createOperatorDaemon({ config, store, tools, transport, now: () => 1000 });
  try {
    expect((await daemon.once()).state).toBe('paused');
    expect(existsSync(home)).toBe(false);
    expect(requests).toBe(0);
    store.addGoal({ title: 'Real fixture goal', details: 'exact', source: 'fixture', dedupe_key: 'fixture:real' });
    paused = false;
    const outcome = await daemon.once();
    expect(outcome.state).toBe('completed');
    expect(outcome.receipt.disposition).toBe('resolved');
    expect(outcome.receipt.usage).toEqual({ input_tokens: 10, output_tokens: 5, total_tokens: 15, response_count: 2 });
    expect(store.getGoal(outcome.receipt.goal.id).state).toBe('resolved');
  } finally { store.close(); rmSync(base, { recursive: true, force: true }); }
});

test('a lock naming a reused pid is stale, not another daemon', async () => {
  const { operatorDaemonAlive } = await import('../operator_daemon.mjs');
  const stranger = Bun.spawn(['sleep', '30']);
  try {
    expect(operatorDaemonAlive(stranger.pid)).toBe(false);
    expect(operatorDaemonAlive(stranger.pid, { spawn: () => ({ exitCode: 0, stdout: Buffer.from('bun /x/integrations/mcp/operator_daemon.mjs') }) })).toBe(true);
  } finally { stranger.kill(); await stranger.exited; }
  expect(operatorDaemonAlive(2 ** 22 + 12345)).toBe(false);
});
test('direct operator action intents carry a fresh daily limit through the native audit owner', async () => {
  const f = fixture({ responses: [
    answer('r1', [call('unknown_fixture', {}, 'unknown'), call('operator_add_goal', { title: 'Follow-up fixture' }, 'follow')]),
    answer('r2', [finish()]),
  ] });
  const previous = f.config.loadPolicy;
  let limit = 8;
  f.config.loadPolicy = () => ({ ...previous(), daily_tool_call_budget: limit });
  const originalResponse = f.transport.response.bind(f.transport);
  f.transport.response = async input => {
    const result = await originalResponse(input);
    if (result.id === 'r1') limit = 2;
    return result;
  };
  const budgetChecks = [];
  f.store.budgetOk = policy => { budgetChecks.push(policy.daily_tool_call_budget); return { okay: true }; };
  f.store.recordAction = action => { f.calls.store.push(['recordAction', action]); return 1; };
  f.store.completeAction = action => f.calls.store.push(['completeAction', action]);
  expect((await f.daemon.once()).state).toBe('completed');
  const intents = f.calls.store.filter(row => row[0] === 'recordAction').map(row => row[1]);
  expect(intents.map(row => row.state)).toEqual(['refused', 'started']);
  expect(intents.map(row => row.policy.daily_tool_call_budget)).toEqual([2, 2]);
  expect(budgetChecks).toContain(2);
});

test('a tightened daily limit stops queued tool dispatch without a second provider response', async () => {
  const f = fixture({ responses: [answer('r1', [call('write_fixture', { value: 4 }, 'write-4')])] });
  const previous = f.config.loadPolicy;
  let limit = 8;
  f.config.loadPolicy = () => ({ ...previous(), daily_tool_call_budget: limit });
  f.store.budgetOk = policy => ({ okay: policy.daily_tool_call_budget > 0 });
  const originalResponse = f.transport.response.bind(f.transport);
  f.transport.response = async input => {
    const result = await originalResponse(input);
    limit = 0;
    return result;
  };
  const result = await f.daemon.once();
  expect(result.state).toBe('failed');
  expect(result.error).toContain('model-token budget exhausted');
  expect(f.calls.response).toHaveLength(1);
  expect(f.calls.effects.filter(row => row[0] === 'execute')).toEqual([]);
  expect(f.calls.store.some(row => row[0] === 'failRun')).toBe(true);
});

test('provider quarantine or withdrawn model permission stops in-flight cycles before effects or another model request', async () => {
  for (const reason of ['quarantine', 'policy']) {
    const f = fixture({ responses: [answer('r1', [call('write_fixture', { value: 5 }, 'effect')])] });
    let withdrawn = false;
    const prior = f.config.loadPolicy;
    f.config.loadPolicy = () => ({ ...prior(), allow_provider_requests: reason !== 'policy' || !withdrawn });
    f.store.providerStatus = () => withdrawn && reason === 'quarantine'
      ? { state: 'quarantined', cause: 'insufficient_quota', admitted: false } : { state: 'unverified' };
    const previousResponse = f.transport.response.bind(f.transport);
    f.transport.response = async input => {
      const result = await previousResponse(input);
      withdrawn = true;
      return result;
    };
    const result = await f.daemon.once();
    expect(result.state).toBe('failed');
    expect(result.error).toContain('provider admission withdrawn');
    expect(f.calls.response).toHaveLength(1);
    expect(f.calls.effects.filter(row => row[0] === 'execute')).toEqual([]);
    expect(f.calls.store.some(row => row[0] === 'failRun')).toBe(true);
  }
});

test('asynchronous MCP preflight rechecks STOP, provider permission, quarantine and budgets before a claim', async () => {
  for (const scenario of ['paused', 'provider_not_admitted', 'provider_quarantined', 'budget_exhausted']) {
    const f = fixture();
    let budgetAllowed = true, quarantined = false;
    f.store.providerStatus = () => quarantined ? { state: 'quarantined', cause: 'insufficient_quota' } : { state: 'unverified' };
    f.store.budgetOk = () => ({ okay: budgetAllowed });
    const catalog = f.tools.catalog.bind(f.tools);
    f.tools.catalog = async () => {
      const result = await catalog();
      if (scenario === 'paused') f.setPaused(true);
      if (scenario === 'provider_not_admitted') f.config.loadPolicy().allow_provider_requests = false;
      if (scenario === 'provider_quarantined') quarantined = true;
      if (scenario === 'budget_exhausted') budgetAllowed = false;
      return result;
    };
    expect((await f.daemon.once()).state).toBe(scenario);
    expect(f.calls.store.some(row => row[0] === 'claimGoal')).toBe(false);
    expect(f.calls.response).toEqual([]);
    expect(f.calls.effects.filter(row => row[0] === 'execute')).toEqual([]);
  }
});
test('operator_finish imports a reviewed follow-up batch through the existing atomic store', async () => {
  const f = fixture({ responses: [answer('r1', [call('operator_finish', {
    disposition: 'resolved', summary: 'complete', evidence: ['fixture'],
    new_goals: [{ title: 'Inspect source A' }, { title: 'Inspect source B', details: 'exact', priority: 61 }],
  }, 'finish-batch')])], policy: { max_followup_goals_per_cycle: 2 } });
  const imports = [];
  f.store.importGoals = (batch, options) => { imports.push({ batch, options }); return { created: 2 }; };
  expect((await f.daemon.once()).state).toBe('completed');
  expect(imports).toHaveLength(1);
  expect(imports[0].options).toEqual({ apply: true });
  expect(imports[0].batch.map(goal => goal.title)).toEqual(['Inspect source A', 'Inspect source B']);
  expect(imports[0].batch.map(goal => goal.details)).toEqual(['', 'exact']);
  expect(imports[0].batch.every(goal => goal.source === 'run:run-1' && goal.recurring === false && goal.interval_seconds === 0)).toBe(true);
  expect(f.calls.store.filter(item => item[0] === 'addGoal')).toHaveLength(0);
});
test('an oversized or disabled finish batch is refused before any new goal is written', async () => {
  for (const [limit, count] of [[0, 1], [1, 2], [16, 17]]) {
    const f = fixture({ responses: [answer('r1', [call('operator_finish', {
      disposition: 'resolved', summary: 'unreviewed', new_goals: Array.from({ length: count }, (_, index) => ({ title: 'New ' + index })),
    }, 'finish-batch')])], policy: { max_followup_goals_per_cycle: limit } });
    f.store.importGoals = (...args) => f.calls.store.push(['importGoals', args]);
    const result = await f.daemon.once();
    expect(result.state).toBe('failed');
    expect(f.calls.store.some(item => item[0] === 'importGoals' || item[0] === 'addGoal')).toBe(false);
    expect(f.calls.store.some(item => item[0] === 'finishGoal')).toBe(false);
  }
});
test('direct follow-up tools and terminal goals share one per-cycle admission budget', async () => {
  const f = fixture({ responses: [
    answer('r1', [call('operator_add_goal', { title: 'First' }, 'g1'),
      call('operator_add_goal', { title: 'Second' }, 'g2')]),
    answer('r2', [call('operator_finish', { disposition: 'resolved', summary: 'later',
      new_goals: [{ title: 'Third' }] }, 'terminal')]),
  ], policy: { max_followup_goals_per_cycle: 2 } });
  f.store.importGoals = (batch, options) => f.calls.store.push(['importGoals', batch, options]);
  f.store.completeAction = (value) => f.calls.store.push(['completeAction', value]);
  const result = await f.daemon.once();
  expect(result.state).toBe('failed');
  expect(result.error).toContain('follow-up goal admission limit');
  expect(f.calls.store.filter(item => item[0] === 'addGoal')).toHaveLength(2);
  expect(f.calls.store.some(item => item[0] === 'importGoals')).toBe(false);
  expect(f.calls.store.some(item => item[0] === 'finishGoal')).toBe(false);
});
test('tightening a follow-up policy during a model response forbids the terminal batch', async () => {
  const f = fixture({ responses: [answer('r1', [call('operator_finish', {
    disposition: 'resolved', summary: 'fixture', new_goals: [{ title: 'Should stay absent' }],
  }, 'terminal')])] });
  const originalPolicy = f.config.loadPolicy;
  let limit = 16;
  f.config.loadPolicy = () => ({ ...originalPolicy(), max_followup_goals_per_cycle: limit });
  const originalResponse = f.transport.response;
  f.transport.response = async (...args) => { const value = await originalResponse(...args); limit = 0; return value; };
  f.store.importGoals = (batch, options) => f.calls.store.push(['importGoals', batch, options]);
  const result = await f.daemon.once();
  expect(result.state).toBe('failed');
  expect(f.calls.store.some(item => item[0] === 'importGoals')).toBe(false);
});
