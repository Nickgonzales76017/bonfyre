import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createOperatorDaemon } from '../operator_daemon.mjs';
import { createOperatorStore } from '../operator_store.mjs';
import { createOperatorTools } from '../operator_tools.mjs';
import { CATALOG } from '../project_node.mjs';

const modelCall = (name, args, callId) => ({ type: 'function_call', name, arguments: JSON.stringify(args), call_id: callId });
const fixtureTime = 1791000000;
const fixturePolicy = { autonomous: true, mode: 'power', allow_provider_requests: true,
  approved_provider_models: ['fixture'], max_cycles_per_hour: 6,
  max_rounds_per_cycle: 4, max_tool_calls_per_cycle: 8, cycle_timeout_seconds: 60 };

function realReadFixture({ paused = false, onReceipt = () => {}, policy: overridePolicy = {}, secondResponse = null } = {}) {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-operator-read-'));
  const home = join(base, 'operator');
  const store = createOperatorStore({ home, now: () => fixtureTime });
  const requests = [], effects = [];
  const tools = createOperatorTools({
    listTools: async () => [CATALOG.tools.find(item => item.name === 'project_status')],
    mcpCall: async (name, args) => {
      effects.push([name, args]);
      return { root: '/fixture/repo', clean: true, branch: 'fixture' };
    },
    recordAction: store.recordAction, completeAction: store.completeAction, now: () => fixtureTime,
  });
  const transport = {
    events: async () => ({ events: [], next_after: 0 }),
    response: async payload => {
      requests.push(payload);
      if (requests.length === 1) return { id: 'read-response', status: 'completed',
        usage: { input_tokens: 11, output_tokens: 7 }, output: [modelCall('project_status', {}, 'read-call')] };
      if (secondResponse) return secondResponse;
      return { id: 'finish-response', status: 'completed',
        usage: { input_tokens: 3, output_tokens: 2 }, output: [modelCall('operator_finish',
          { disposition: 'resolved', summary: 'Fixture status was clean.', evidence: ['project_status fixture'] }, 'finish-call')] };
    },
    setDeadline() {}, redact: value => String(value),
  };
  const daemon = createOperatorDaemon({ config: { model: 'fixture', paused: () => paused, loadPolicy: () => ({ ...fixturePolicy, ...overridePolicy }) },
    store, tools, transport, now: () => fixtureTime, onReceipt });
  return { base, home, store, requests, effects, daemon,
    cleanup() { store.close(); rmSync(base, { recursive: true, force: true }); } };
}

test('real store and canonical project_status owner carry a two-response read into verified resolution', async () => {
  const f = realReadFixture();
  try {
    const goal = f.store.addGoal({ title: 'Read fixture', details: 'Observe only', source: 'fixture', dedupe_key: 'fixture:read' });
    const outcome = await f.daemon.once();
    expect(outcome.state).toBe('completed');
    expect(f.effects).toEqual([['project_status', {}]]);
    expect(f.store.getGoal(goal.id).state).toBe('resolved');
    expect(f.requests).toHaveLength(2);
    expect(f.requests[1].previous_response_id).toBe('read-response');
    expect(f.requests[1].instructions).toBe(f.requests[0].instructions);
    expect(f.requests[1].input).toEqual([{ type: 'function_call_output', call_id: 'read-call',
      output: JSON.stringify({ root: '/fixture/repo', clean: true, branch: 'fixture' }) }]);
    expect(outcome.receipt.usage.response_count).toBe(2);
    const db = new Database(f.store.path, { readonly: true });
    try { expect(db.query('SELECT cycles,tool_calls,input_tokens,output_tokens FROM budgets').get())
      .toEqual({ cycles: 1, tool_calls: 1, input_tokens: 14, output_tokens: 9 }); }
    finally { db.close(); }
  } finally { f.cleanup(); }
});

test('the last admitted tool slot does not prevent an exact terminal model response', async () => {
  const f = realReadFixture({ policy: { daily_cycle_budget: 1, daily_tool_call_budget: 1 } });
  try {
    const goal = f.store.addGoal({ title: 'Close audited read', details: 'One effect slot',
      source: 'fixture', dedupe_key: 'fixture:budget-finish' });
    const outcome = await f.daemon.once();
    expect(outcome.state).toBe('completed');
    expect(f.requests).toHaveLength(2);
    expect(f.effects).toEqual([['project_status', {}]]);
    expect(f.store.getGoal(goal.id).state).toBe('resolved');
    const db = new Database(f.store.path, { readonly: true });
    try {
      expect(db.query('SELECT cycles,tool_calls FROM budgets').get()).toEqual({ cycles: 1, tool_calls: 1 });
      expect(db.query('SELECT tool,state FROM actions ORDER BY seq').all())
        .toEqual([{ tool: 'project_status', state: 'passed' }]);
    } finally { db.close(); }
    expect((await f.daemon.once()).state).toBe('budget_exhausted');
    expect(f.effects).toHaveLength(1);
  } finally { f.cleanup(); }
});

test('per-cycle tool ceiling permits one terminal response without another effect', async () => {
  const f = realReadFixture({ policy: { max_tool_calls_per_cycle: 1, daily_tool_call_budget: 1 } });
  try {
    const goal = f.store.addGoal({ title: 'Close at tool ceiling', details: 'One read',
      source: 'fixture', dedupe_key: 'fixture:cycle-ceiling' });
    const result = await f.daemon.once();
    expect(result.state).toBe('completed');
    expect(f.requests).toHaveLength(2);
    expect(f.effects).toEqual([['project_status', {}]]);
    expect(result.receipt.tool_calls).toBe(1);
    expect(f.store.getGoal(goal.id).state).toBe('resolved');
  } finally { f.cleanup(); }
});

test('per-cycle ceiling refuses another tool and never loops model responses', async () => {
  const f = realReadFixture({ policy: { max_tool_calls_per_cycle: 1, daily_tool_call_budget: 1 },
    secondResponse: { id: 'extra-response', status: 'completed',
      usage: { input_tokens: 3, output_tokens: 2 },
      output: [modelCall('project_status', {}, 'extra-read')] } });
  try {
    const goal = f.store.addGoal({ title: 'Refuse extra read', details: 'One read only',
      source: 'fixture', dedupe_key: 'fixture:cycle-refusal' });
    const result = await f.daemon.once();
    expect(result.state).toBe('completed');
    expect(f.requests).toHaveLength(2);
    expect(f.effects).toEqual([['project_status', {}]]);
    expect(result.receipt.tool_calls).toBe(1);
    expect(f.store.getGoal(goal.id).state).toBe('retry');
  } finally { f.cleanup(); }
});

test('paused real store performs no database creation, catalog call, or API request', async () => {
  const f = realReadFixture({ paused: true });
  try {
    expect((await f.daemon.once()).state).toBe('paused');
    expect(existsSync(f.store.path)).toBe(false);
    expect(f.requests).toEqual([]);
    expect(f.effects).toEqual([]);
  } finally { f.cleanup(); }
});

test('failed file-receipt projection does not replay a resolved goal or its read', async () => {
  const f = realReadFixture({ onReceipt: () => { throw new Error('fixture disk full'); } });
  try {
    const goal = f.store.addGoal({ title: 'Projection fixture', details: 'No replay', source: 'fixture', dedupe_key: 'fixture:projection' });
    const first = await f.daemon.once();
    expect(first.state).toBe('completed_receipt_failed');
    expect(first.receipt.receipt_projection_error).toContain('fixture disk full');
    expect(f.store.getGoal(goal.id).state).toBe('resolved');
    expect((await f.daemon.once()).state).toBe('idle');
    expect(f.requests).toHaveLength(2);
    expect(f.effects).toEqual([['project_status', {}]]);
  } finally { f.cleanup(); }
});

test('uncertain MCP effect blocks stored run and emitted receipt despite model resolution claim', async () => {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-operator-uncertain-'));
  const store = createOperatorStore({ home: join(base, 'operator'), now: () => 1000 });
  const receipts = [], requests = [], effects = [];
  const policy = { autonomous: true, mode: 'power', allow_provider_requests: true,
    approved_provider_models: ['fixture'], max_cycles_per_hour: 6,
    max_rounds_per_cycle: 4, max_tool_calls_per_cycle: 8, cycle_timeout_seconds: 60 };
  try {
    const goal = store.addGoal({ title: 'Uncertain effect fixture', details: 'Do not auto-resolve',
      source: 'fixture', dedupe_key: 'fixture:uncertain-effect' });
    const tools = createOperatorTools({
      listTools: async () => [{ name: 'bonfyre_moe_generate' }],
      mcpCall: async (name, args) => {
        effects.push([name, args]);
        // This is an effect-capable profile; the MCP owner reports an error
        // after invocation, so the daemon cannot prove what happened.
        return { isError: true, content: [{ type: 'text', text: 'fixture failure after invocation' }] };
      },
      recordAction: store.recordAction, completeAction: store.completeAction, now: () => 1000,
    });
    const transport = {
      events: async () => ({ events: [], next_after: 0 }),
      response: async payload => {
        requests.push(payload);
        if (requests.length === 1) return { id: 'response-1', status: 'completed', usage: { input_tokens: 5, output_tokens: 2 },
          output: [modelCall('bonfyre_moe_generate', { prompt: 'fixture only' }, 'call-1')] };
        return { id: 'response-2', status: 'completed', usage: { input_tokens: 3, output_tokens: 2 },
          output: [modelCall('operator_finish', { disposition: 'resolved', summary: 'The model claims success.', evidence: ['claim only'] }, 'call-2')] };
      },
      setDeadline() {}, redact: value => String(value),
    };
    const daemon = createOperatorDaemon({ config: { model: 'fixture', paused: () => false, loadPolicy: () => policy },
      store, tools, transport, now: () => 1000, onReceipt: receipt => { receipts.push(receipt); } });
    const result = await daemon.once();
    expect(result.state).toBe('completed');
    expect(requests).toHaveLength(2);
    expect(effects).toEqual([['bonfyre_moe_generate', { prompt: 'fixture only' }]]);
    expect(store.getGoal(goal.id).state).toBe('blocked');
    const action = store.getRun(result.receipt.run_id);
    expect(action.state).toBe('blocked');
    expect(action.summary).toContain('requires reconciliation');
    expect(receipts).toHaveLength(1);
    expect(receipts[0].disposition).toBe('blocked');
    expect(receipts[0].summary).toContain('requires reconciliation');
    expect(receipts[0].goal.state).toBe('blocked');
    expect(receipts[0].usage).toEqual({ input_tokens: 8, output_tokens: 4, total_tokens: 12, response_count: 2 });
  } finally { store.close(); rmSync(base, { recursive: true, force: true }); }
});
