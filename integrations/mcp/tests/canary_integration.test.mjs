import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createCanaryServer } from '../canary_server.mjs';

test('concrete default canary owners start lazily and persist fallback/outcome/receipt transitions without a provider', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bonfyre-canary-integrated-'));
  const home = join(directory, 'owner'), root = resolve(new URL('../../..', import.meta.url).pathname);
  const node = await createCanaryServer({ token: 'fixture-only', home, root });
  try {
    expect(existsSync(join(home, 'canary.sqlite3'))).toBe(false);
    const health = await node.handle(new Request('http://fixture/health'));
    expect((await health.json()).tools).toBe(12);
    expect(existsSync(join(home, 'canary.sqlite3'))).toBe(false);
    const decision = await node.call('canary_route_decide', { mission_id: 'fixture-mission', capability: 'fixture.read', effect_class: 'READ_ONLY' });
    expect(decision.mode).toBe('fallback');
    expect(decision.selected_provider).toBe('bonfyre-native');
    expect(decision.reason).toBe('no_enabled_policy');
    expect(decision.receipt.digest).toMatch(/^[a-f0-9]{64}$/);
    const outcome = await node.call('canary_outcome_record', { decision_id: decision.id, state: 'fallback', latency_ms: 1.5, payload: { fixture: true } });
    expect(outcome.policy_suspended).toBe(false);
    expect((await node.call('canary_decision_list')).decisions).toHaveLength(1);
    expect((await node.call('canary_outcome_list')).outcomes[0].payload).toEqual({ fixture: true });
    expect(await node.call('canary_receipt_verify')).toMatchObject({ verified: true, receipts: 2, failures: [] });
    await expect(node.call('canary_policy_upsert', { capability: 'fixture.read', provider: 'fixture-provider', fallback_provider: 'bonfyre-native', percentage: 10, effect_classes: ['READ_ONLY'], enable: true })).rejects.toThrow();
    expect((await node.call('canary_policy_list')).policies).toEqual([]);
  } finally { node.close(); rmSync(directory, { recursive: true, force: true }); }
});

test('concrete canary decisions and hash chain survive an owner restart', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-canary-reopen-'));
  const config = { token: 'fixture-only', home, root: resolve(new URL('../../..', import.meta.url).pathname) };
  let node = await createCanaryServer(config);
  try {
    const decision = await node.call('canary_route_decide', { mission_id: 'restart', capability: 'fixture.read', effect_class: 'PURE' });
    node.close(); node = await createCanaryServer(config);
    expect((await node.call('canary_decision_list')).decisions[0].id).toBe(decision.id);
    expect(await node.call('canary_receipt_verify')).toMatchObject({ verified: true, receipts: 1, head: decision.receipt.digest });
  } finally { node.close(); rmSync(home, { recursive: true, force: true }); }
});
