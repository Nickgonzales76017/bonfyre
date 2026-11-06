// The operator stopped on 2026-10-05 after 20,777 runs that each failed on
// "no credits remaining": nothing told it the provider was gone, and the legacy
// loop retried seed goals hundreds of times past their cap. A spent goal is not
// claimed again, and a credit failure quarantines the provider through the
// transport that saw it, after one call.
import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createOperatorDaemon, createOperatorTransport } from '../operator_daemon.mjs';
import { createOperatorStore } from '../operator_store.mjs';

const finishSchema = { type: 'object', properties: { disposition: { type: 'string' }, summary: { type: 'string' } }, required: ['disposition', 'summary'] };
const readSchema = { type: 'object', properties: { path: { type: 'string' } } };

function withStore(fn) {
  const home = mkdtempSync(join(tmpdir(), 'operator-local-'));
  let now = 1800000000;
  const store = createOperatorStore({ home, now: () => now });
  return Promise.resolve(fn({ store, advance: s => { now += s; }, clock: () => now }))
    .finally(() => { store.close(); rmSync(home, { recursive: true, force: true }); });
}

const policy = extra => ({ autonomous: true, mode: 'power', max_cycles_per_hour: 6, max_goal_attempts: 6,
  cycle_timeout_seconds: 60, max_rounds_per_cycle: 4, max_tool_calls_per_cycle: 8, ...extra });
const catalog = { async catalog() { return {
  tools: [{ type: 'function', name: 'project_read', parameters: readSchema }, { type: 'function', name: 'project_apply_low_risk_patch', parameters: readSchema },
    { type: 'function', name: 'operator_finish', parameters: finishSchema }, { type: 'function', name: 'operator_add_goal', parameters: finishSchema }],
  catalog: { project_read: { annotations: { readOnlyHint: true } }, project_apply_low_risk_patch: { annotations: { readOnlyHint: false } } } }; },
  async execute(_run, name) { return { read: name }; } };

test('a goal that has used its attempts is not claimed again', () => withStore(({ store }) => {
  const spent = store.addGoal({ title: 'spent', details: 'x', source: 'seed', dedupe_key: 'spent' });
  // The legacy Python loop left seed goals in retry with hundreds of attempts.
  const legacy = new Database(store.path);
  legacy.run("UPDATE goals SET state='retry',attempts=540 WHERE id=?", [spent.id]); legacy.close();
  expect(store.getGoal(spent.id)).toMatchObject({ state: 'retry', attempts: 540 });
  expect(store.selectGoal(policy())).toBeNull();
  expect(store.claimGoal({ policy: policy() })).toBeNull();
  expect(store.selectGoal(policy({ max_goal_attempts: 541 }))).not.toBeNull();
}));

test('a credit failure seen by the paid transport quarantines it, and the next cycle does not call it', () => withStore(async ({ store }) => {
  let paid = 0;
  const transport = createOperatorTransport({ token: 't', gateway: 'http://127.0.0.1:1', env: {}, userHome: '/nonexistent' }, {
    secrets: async () => ({ OPENAI_API_KEY: 'k' }),
    request: async () => { paid++; return new Response(JSON.stringify({ error: { code: 'insufficient_quota' } }), { status: 429 }); } });
  transport.events = async after => ({ events: [], next_after: after });
  store.addGoal({ title: 'paid', details: 'x', source: 'fixture', dedupe_key: 'paid' });
  const live = { autonomous: true, mode: 'power', max_cycles_per_hour: 6, cycle_timeout_seconds: 60, max_rounds_per_cycle: 2,
    max_tool_calls_per_cycle: 4, allow_provider_requests: true, approved_provider_models: ['gpt-5'] };
  const daemon = createOperatorDaemon({ config: { model: 'gpt-5', paused: () => false, loadPolicy: () => live }, store, tools: catalog, transport });
  expect((await daemon.once()).state).toBe('failed');
  expect(store.providerStatus('openai')).toMatchObject({ state: 'quarantined', cause: 'insufficient_quota', retry_at: 0 });
  store.addGoal({ title: 'next', details: 'x', source: 'fixture', dedupe_key: 'next' });
  expect((await daemon.once()).state).toBe('provider_quarantined');
  expect(paid).toBe(1);
}));

test('malformed and empty provider failure bodies retain quarantine status without leaking response bytes', async () => {
  for (const [status, body, cause] of [
    [429, '<html>fixture-private-body</html>', 'rate_limited'],
    [429, null, 'rate_limited'],
    [401, null, 'invalid_api_key'],
  ]) {
    const transport = createOperatorTransport({ token: 't', gateway: 'http://127.0.0.1:1', env: {} }, {
      secrets: async () => ({ OPENAI_API_KEY: 'fixture-key' }),
      request: async () => new Response(body, { status }),
    });
    let failure;
    try { await transport.response({ model: 'fixture' }, 5); }
    catch (error) { failure = error; }
    expect(failure?.http_status).toBe(status);
    expect(failure?.provider_failure).toMatchObject({ provider: 'openai', cause });
    expect(String(failure?.message)).not.toContain('fixture-private-body');
  }
});

test('oversized or interrupted 429 response bodies preserve quarantine and never expose bytes', async () => {
  const huge = new Uint8Array(8 * 1024 * 1024 + 1);
  const broken = new ReadableStream({
    pull(controller) { controller.error(new Error('fixture-private-body')); },
  });
  for (const body of [huge, broken]) {
    const transport = createOperatorTransport({ token: 't', gateway: 'http://127.0.0.1:1', env: {} }, {
      secrets: async () => ({ OPENAI_API_KEY: 'fixture-key' }),
      request: async () => new Response(body, { status: 429 }),
    });
    let failure;
    try { await transport.response({ model: 'fixture' }, 5); }
    catch (error) { failure = error; }
    expect(failure?.http_status).toBe(429);
    expect(failure?.provider_failure).toMatchObject({ provider: 'openai', cause: 'rate_limited' });
    expect(String(failure?.message)).not.toContain('fixture-private-body');
  }
});

test('a non-JSON 429 quarantines once and never sends a second paid request', () => withStore(async ({ store }) => {
  let paid = 0;
  const transport = createOperatorTransport({ token: 't', gateway: 'http://127.0.0.1:1', env: {} }, {
    secrets: async () => ({ OPENAI_API_KEY: 'k' }),
    request: async () => { paid++; return new Response('fixture-private-body', { status: 429 }); },
  });
  transport.events = async after => ({ events: [], next_after: after });
  store.addGoal({ title: 'provider error body', source: 'fixture', dedupe_key: 'bad-body' });
  const live = policy({ allow_provider_requests: true, approved_provider_models: ['gpt-5'] });
  const daemon = createOperatorDaemon({ config: { model: 'gpt-5', paused: () => false, loadPolicy: () => live }, store, tools: catalog, transport });
  expect((await daemon.once()).state).toBe('failed');
  expect(store.providerStatus('openai')).toMatchObject({ state: 'quarantined', cause: 'rate_limited' });
  expect((await daemon.once()).state).toBe('provider_quarantined');
  expect(paid).toBe(1);
}));

test('async secret resolution cannot bypass STOP, provider quarantine or tightened budget', async () => {
  for (const change of ['stop', 'quarantine', 'budget']) {
    await withStore(async ({ store }) => {
      let paused = false, paid = 0, secretLoads = 0;
      const live = policy({ allow_provider_requests: true, approved_provider_models: ['gpt-5'] });
      const goal = store.addGoal({ title: 'async admission', source: 'fixture', dedupe_key: `async-admission:${change}` });
      const transport = createOperatorTransport({ token: 't', gateway: 'http://127.0.0.1:1', env: {} }, {
        secrets: async () => {
          secretLoads++;
          await Promise.resolve();
          if (change === 'stop') paused = true;
          if (change === 'quarantine') store.quarantineProvider('openai', 'insufficient_quota');
          if (change === 'budget') live.daily_input_token_budget = 0;
          return { OPENAI_API_KEY: 'fixture-key' };
        },
        request: async () => { paid++; return Response.json({ id: 'unexpected', output: [] }); },
      });
      transport.events = async after => ({ events: [], next_after: after });
      const daemon = createOperatorDaemon({
        config: { model: 'gpt-5', paused: () => paused, loadPolicy: () => live },
        store, tools: catalog, transport,
      });
      const result = await daemon.once();
      expect(result.state).toBe('failed');
      expect(secretLoads).toBe(1);
      expect(paid).toBe(0);
      expect(store.getGoal(goal.id).state).not.toBe('resolved');
    });
  }
});
