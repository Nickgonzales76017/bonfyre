import { createHash } from 'node:crypto';
import { closeSync, constants, fstatSync, lstatSync, openSync, readSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { canonicalJson } from './canonical_json.mjs';
import { createHypervisorStore } from './hypervisor_store.mjs';
import { buildMissionGraph, createHypervisorMissions, renderMissionSpec } from './hypervisor_missions.mjs';
import { compileTaskPlan, missionPlanContext } from './mission_plan.mjs';

const MAX_DOCUMENT_BYTES = 8 * 1024 * 1024;
const SHA = /^[a-f0-9]{64}$/;
const sha = value => createHash('sha256').update(value).digest('hex');
function readPinned(path) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const first = fstatSync(fd), named = lstatSync(path);
    if (!first.isFile() || !named.isFile() || named.isSymbolicLink() || first.dev !== named.dev || first.ino !== named.ino ||
      first.size > MAX_DOCUMENT_BYTES) throw new Error('hypervisor artifact is not a bounded regular file');
    const bytes = Buffer.alloc(first.size); let offset = 0;
    while (offset < bytes.length) {
      const count = readSync(fd, bytes, offset, bytes.length - offset, null);
      if (!count) throw new Error('hypervisor artifact truncated');
      offset += count;
    }
    const after = fstatSync(fd), namedAfter = lstatSync(path);
    if (first.size !== after.size || first.mtimeMs !== after.mtimeMs || first.ctimeMs !== after.ctimeMs ||
      first.dev !== namedAfter.dev || first.ino !== namedAfter.ino) throw new Error('hypervisor artifact changed while reading');
    return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } finally { closeSync(fd); }
}

// The home is derived from the trusted V6 store configuration, never an MCP argument.
export function createV6HypervisorBridge({ v6Store, hypervisorHome = null, hypervisorDbPath = null, registryPath = null,
  createStore = createHypervisorStore, checkStop = () => {} } = {}) {
  if (!v6Store?.home || typeof v6Store.digest !== 'function') throw new Error('V6 store required for compiler bridge');
  hypervisorHome ??= dirname(v6Store.home);
  hypervisorDbPath ??= join(hypervisorHome, 'hypervisor.sqlite3');
  registryPath ??= join(hypervisorHome, 'ecosystem_registry.json');
  let store, compiler, closed = false;
  function owner() {
    if (closed) throw new Error('V6 hypervisor compiler bridge is closed');
    if (!store) {
      checkStop();
      store = createStore({ home: hypervisorHome, dbPath: hypervisorDbPath, registryPath, checkStop });
      compiler = createHypervisorMissions({ store, checkStop });
    }
    return compiler;
  }
  function compileMission({ mission, prompt, intent }) {
    checkStop();
    if (!mission || typeof mission.id !== 'string' || typeof mission.effect_class !== 'string' ||
      typeof prompt !== 'string' || !SHA.test(intent?.digest)) throw new Error('invalid V6 compile request');
    const result = owner().compile(prompt, { requested_capabilities: mission.capabilities });
    checkStop();
    const persisted = store.getMission(result.mission.id);
    if (persisted.id !== result.mission.id || persisted.goal !== prompt ||
      canonicalJson(persisted) !== canonicalJson(result.mission)) throw new Error('hypervisor mission row differs from compilation');
    const plan = compileTaskPlan(persisted.tasks, { context: missionPlanContext(persisted) });
    if (persisted.execution_plan?.digest !== plan.digest || persisted.execution_plan?.authority !== 'candidate-plan-only')
      throw new Error('hypervisor execution plan identity mismatch');
    const spec = readPinned(result.spec_path), graphSource = readPinned(result.graph_path), missionSource = readPinned(join(result.mission_dir, 'mission.json'));
    if (spec !== renderMissionSpec(persisted)) throw new Error('hypervisor spec mismatch');
    const graph = JSON.parse(graphSource), expectedGraph = buildMissionGraph(persisted.tasks);
    if (!expectedGraph.valid || canonicalJson(graph) !== canonicalJson(expectedGraph)) throw new Error('hypervisor graph mismatch');
    if (canonicalJson(JSON.parse(missionSource)) !== canonicalJson(persisted)) throw new Error('hypervisor mission artifact mismatch');
    const receipts = store.verifyReceiptChain();
    if (receipts.verified !== true || !SHA.test(receipts.head)) throw new Error('hypervisor receipt chain failed');
    checkStop();
    const evidence = { source_mission_id: mission.id, compiled_mission_id: persisted.id,
      compiled_goal_sha256: sha(Buffer.from(prompt, 'utf8')), execution_plan_digest: plan.digest,
      spec_sha256: sha(Buffer.from(spec, 'utf8')), graph_sha256: sha(Buffer.from(graphSource, 'utf8')),
      mission_sha256: sha(Buffer.from(missionSource, 'utf8')), receipt_head: receipts.head,
      receipt_count: receipts.receipts, compiler: 'bonfyre.hypervisor-missions.bun.v1',
      execution_plan_authority: 'candidate-plan-only', external_effect_executed: false };
    return { executed: true, verified: true, state: 'passed', kind: 'compile', mission_id: mission.id,
      effect_class: mission.effect_class, intent_digest: intent.digest, external_effect_executed: false,
      evidence, evidence_digest: v6Store.digest(evidence) };
  }
  return { compileMission, close() { if (!closed) store?.close(); store = null; compiler = null; closed = true; } };
}
