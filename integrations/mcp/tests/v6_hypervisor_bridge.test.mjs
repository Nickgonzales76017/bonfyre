import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createV6Bfp } from '../v6_bfp.mjs';
import { createV6HypervisorBridge } from '../v6_hypervisor_bridge.mjs';
import { createV6Missions } from '../v6_missions.mjs';
import { createV6Store } from '../v6_store.mjs';
import { createHypervisorStore } from '../hypervisor_store.mjs';

const fixtures = [];
afterEach(() => { for (const item of fixtures.splice(0)) { item.v6.close(); rmSync(item.root, { recursive: true, force: true }); } });
function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-v6-hypervisor-'));
  const hypervisorHome = join(root, 'hypervisor');
  const v6 = createV6Store({ home: join(hypervisorHome, 'recursive-hypervisor-v6'), root, now: () => 1800000000.25 });
  v6.writeJson(join(v6.home, 'estate/estate.json'), { apps: [] });
  writeFileSync(join(hypervisorHome, 'ecosystem_registry.json'), JSON.stringify({ capability_contracts: [], projects: [] }));
  const bfp = createV6Bfp({ root, home: v6.home });
  const value = { root, hypervisorHome, v6, bfp }; fixtures.push(value); return value;
}

test('V6 default compiler bridge verifies actual Bun-owned spec, graph, plan and receipts, but never claims canary activation', async () => {
  const state = fixture();
  const bridge = createV6HypervisorBridge({ v6Store: state.v6 });
  try {
    const owner = createV6Missions({ store: state.v6, bfp: state.bfp, compileMission: bridge.compileMission });
    const result = await owner.activate();
    expect(result.summary).toMatchObject({ cataloged: 10, hypervisor_compiled: 10, active: 0, candidate: 10, canary_runs_passed: 0 });
    expect(result.catalog.hypervisor_compilation.every(item => item.compiled && item.probe.evidence.execution_plan_authority === 'candidate-plan-only')).toBe(true);
    expect(result.safe_runs.every(item => item.outcome.command_canary.state === 'not_executed')).toBe(true);
    expect(result.catalog.missions.every(item => item.activation.external_effect_executed === false)).toBe(true);
    expect(state.v6.verifyReceipts().verified).toBe(true);
    bridge.close();
    const durable = createHypervisorStore({ home: state.hypervisorHome });
    try {
      expect(durable.verifyReceiptChain()).toMatchObject({ verified: true, receipts: 20 });
      expect(durable.summary().missions.compiled).toBe(10);
    } finally { durable.close(); }
    expect(() => bridge.compileMission({ mission: { id: 'production.fixture', effect_class: 'READ_ONLY', capabilities: [] },
      prompt: 'Build a fixture mission', intent: { digest: 'a'.repeat(64) } })).toThrow('closed');
  } finally { bridge.close(); }
});

test('bridge refuses forged stored spec instead of laundering a compile proof', () => {
  const state = fixture();
  const bridge = createV6HypervisorBridge({ v6Store: state.v6,
    createStore: options => {
      const store = createHypervisorStore(options);
      return { ...store, createMission(args) {
        const result = store.createMission(args);
        writeFileSync(result.spec_path, '# forged\n');
        return result;
      } };
    } });
  try {
    expect(() => bridge.compileMission({ mission: { id: 'production.fixture', effect_class: 'READ_ONLY', capabilities: [] },
      prompt: 'Build a fixture mission', intent: { digest: 'a'.repeat(64) } })).toThrow('spec mismatch');
  } finally { bridge.close(); }
});
