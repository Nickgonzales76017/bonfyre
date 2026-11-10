import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHypervisorStore } from '../hypervisor_store.mjs';
import { createHypervisorContext } from '../hypervisor_context.mjs';

const fixtures = [];
afterEach(() => { for (const { home, store } of fixtures.splice(0)) { store.close(); rmSync(home, { recursive: true, force: true }); } });
function fixture(options = {}) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-context-'));
  const store = createHypervisorStore({ home, checkStop: options.checkStop });
  fixtures.push({ home, store });
  const mission = { id: 'context-fixture', goal: 'Preserve `x < y`, 版本, and https://example.test exactly.',
    constraints: ['Never omit NOT or negation.'], tasks: [{ id: 'A', title: 'Observe', capability: 'agent.reviewer', dependencies: [] }] };
  store.createMission({ mission, spec: '# Fixture', graph: { tasks: mission.tasks } });
  const context = createHypervisorContext({ store, owners: { recent_receipts: async () => ({ head: 'stable' }) }, ...options });
  return { home, store, mission, context, request: { mission_id: mission.id, mission, goal: mission.goal, target: 'codex', budget_tokens: 12000 } };
}

test('unchanged source bodies share exact CAS bytes while each purpose-bound packet has its own receipt', async () => {
  const { store, context, request } = fixture();
  const first = await context.compile(request), second = await context.compile(request);
  expect(first.id).not.toBe(second.id);
  expect(first.representation_digest).toBe(second.representation_digest);
  expect(first.storage.new_body_bytes).toBeGreaterThan(0);
  expect(second.storage.new_body_bytes).toBe(0);
  expect(second.storage.reused_body_bytes).toBe(second.storage.source_bytes);
  expect(second.sections.map(s => s.artifact)).toEqual(first.sections.map(s => s.artifact));
  expect(context.renderForProvider(second, 'codex')).toContain(request.goal);
  expect(second.budget_verified).toBe(false);
  expect(second.token_count).toBeNull();
  expect(store.verifyReceiptChain().verified).toBe(true);
  expect(() => context.renderForProvider(second, 'claude')).toThrow('purpose');
  const third = await context.compile({ ...request, target: 'claude' });
  expect(third.representation_digest).not.toBe(first.representation_digest);
  expect(third.sections.map(s => s.artifact)).toEqual(first.sections.map(s => s.artifact));
});

test('required semantics never truncate; optional bodies become explicit exact references', async () => {
  const { context, request } = fixture({ owners: { memory: async () => '🧬not optional truth\n'.repeat(4000), recent_receipts: async () => ({ head: 'stable' }) } });
  const packet = await context.compile({ ...request, budget_tokens: 1800 });
  expect(packet.sections.find(section => section.name === 'memory').mode).toBe('reference');
  const rendered = context.renderForProvider(packet, 'codex');
  expect(rendered).toContain('not resident in this cut');
  expect(rendered).toContain('Never omit NOT or negation.');
  expect(packet.token_estimate).toBeLessThanOrEqual(1800);
  const reference = packet.sections.find(section => section.name === 'memory');
  expect(readFileSync(reference.artifact.path, 'utf8')).toBe('🧬not optional truth\n'.repeat(4000));
  await expect(context.compile({ ...request, budget_tokens: 1 })).rejects.toThrow('required context');
});

test('complete rendered envelope is accounted by the declared counter, not just source sections', async () => {
  const { context, request } = fixture({ countTokens: text => Buffer.byteLength(text), tokenizerPassport: 'fixture-byte-counter-not-a-model' });
  const packet = await context.compile({ ...request, budget_tokens: 8000 });
  expect(packet.accounting).toBe('tokenizer-exact');
  expect(packet.token_count).toBe(Buffer.byteLength(context.renderForProvider(packet, 'codex')));
  expect(packet.token_count).toBeLessThanOrEqual(packet.budget_tokens);
});

test('render refuses changed CAS or packet bytes rather than reusing stale context', async () => {
  const { context, request, store } = fixture();
  const packet = await context.compile(request), body = packet.sections[0].artifact;
  writeFileSync(body.path, 'forged');
  expect(() => context.renderForProvider(packet, 'codex')).toThrow('body digest');
  expect(() => store.putContextBody(request.goal)).toThrow('body digest');
  writeFileSync(packet.path, '{}');
  expect(() => store.getContext(packet.id)).toThrow('packet digest');
});

test('STOP and cancellation prevent publication even after concurrent observations finish', async () => {
  let stopped = false;
  const { store, context, request } = fixture({ checkStop: () => { if (stopped) throw new Error('STOP'); },
    owners: { memory: async () => { stopped = true; return 'observed'; } } });
  const before = store.summary().receipts;
  await expect(context.compile(request)).rejects.toThrow('STOP');
  expect(store.summary().receipts).toBe(before);
  stopped = false;
  const cancelled = new AbortController(); cancelled.abort();
  await expect(context.compile({ ...request, signal: cancelled.signal })).rejects.toThrow();
  expect(store.summary().receipts).toBe(before);
});

test('mission start binds a persisted packet and outstanding attempts cannot be overwritten', async () => {
  const { store, context, request, mission } = fixture();
  const packet = await context.compile(request);
  expect(store.beginMission({ mission_id: mission.id, context_packet_id: packet.id }).state).toBe('running');
  expect(() => store.beginMission({ mission_id: mission.id, context_packet_id: packet.id })).toThrow('outstanding');
  expect(store.getMissionRuntime(mission.id).packet.digest).toBe(packet.digest);
});
