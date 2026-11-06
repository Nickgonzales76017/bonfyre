#!/usr/bin/env bun
// Exact body-residency witness, not a model-token or distributed latency claim.
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative, resolve } from 'node:path';
import { createHypervisorStore } from '../hypervisor_store.mjs';
import { createHypervisorContext } from '../hypervisor_context.mjs';

const root = resolve(import.meta.dir, '../../..');
const files = ['integrations/mcp/hypervisor_store.mjs', 'integrations/mcp/hypervisor_schema.sql', 'integrations/mcp/hypervisor_context.mjs',
  'integrations/mcp/mission_plan.mjs', 'integrations/mcp/canonical_json.mjs', 'integrations/mcp/tests/hypervisor_context_benchmark.mjs'];
const sha = text => createHash('sha256').update(text).digest('hex');
const fingerprint = () => Object.fromEntries(files.map(path => [path, sha(readFileSync(join(root, path)))]));
export async function contextResidencyWitness() {
  const sources = fingerprint(), home = mkdtempSync(join(tmpdir(), 'bonfyre-context-residency-'));
  const store = createHypervisorStore({ home });
  const mission = { id: 'residency-fixture', goal: 'Preserve exact task and context identity', constraints: ['No text rewriting'],
    tasks: Array.from({ length: 64 }, (_, i) => ({ id: `T${i}`, title: `Observe ${i}`, capability: 'agent.reviewer', dependencies: [] })) };
  const memory = 'immutable observation: must NOT authorize effects; 🧬\n'.repeat(20000);
  const context = createHypervisorContext({ store, owners: { memory: async () => memory, recent_receipts: async () => ({ head: 'fixed-fixture-observation' }) } });
  try {
    store.createMission({ mission, spec: 'residency fixture', graph: { tasks: mission.tasks } });
    const samples = [], packets = [];
    for (let i = 0; i < 12; i++) {
      const started = performance.now();
      const packet = await context.compile({ mission_id: mission.id, mission, goal: mission.goal, target: i < 11 ? 'codex' : 'claude',
        budget_tokens: 6000, extra_evidence: i === 10 ? [{ changed: 'one new observation' }] : [] });
      const rendered = context.renderForProvider(packet, packet.target);
      if (!rendered.includes(mission.goal) || packet.sections.find(s => s.name === 'memory').mode !== 'reference') throw new Error('context witness selection failure');
      if (store.readContextBody(packet.sections.find(s => s.name === 'memory').artifact) !== memory) throw new Error('context source parity failure');
      samples.push({ cut: i, elapsed_ms: performance.now() - started, ...packet.storage, representation_digest: packet.representation_digest });
      packets.push(packet);
    }
    if (packets.slice(0, 10).some(packet => packet.representation_digest !== packets[0].representation_digest)) throw new Error('unstable unchanged representation');
    if (packets[10].representation_digest === packets[0].representation_digest || packets[11].representation_digest === packets[0].representation_digest)
      throw new Error('evidence or purpose failed to invalidate context');
    const bodies = join(store.home, 'context', 'bodies'), names = readdirSync(bodies);
    const residentBodyBytes = names.reduce((sum, name) => sum + statSync(join(bodies, name)).size, 0);
    const sourceBytesAcrossCuts = samples.reduce((sum, sample) => sum + sample.source_bytes, 0);
    const newBodyBytes = samples.reduce((sum, sample) => sum + sample.new_body_bytes, 0);
    if (JSON.stringify(sources) !== JSON.stringify(fingerprint())) throw new Error('implementation changed during witness');
    return { schema: 'bonfyre.context-residency-witness.v1', state: 'passed', generated_at: new Date().toISOString(), sources,
      scope: 'Twelve real disposable-store compiles; 64 tasks and a repeated synthetic UTF-8 body. Exact raw source equality verified on every cut.',
      limitations: ['Byte-residency measurement, not production latency or model token savings.', 'No distributed deployment, compression ratio, or exact tokenizer budget claim.',
        'All rendered bodies included in resident bytes; SQLite, receipt and packet manifest overhead excluded.'],
      runtime: { bun: Bun.version, platform: process.platform, arch: process.arch }, sources_unchanged: true,
      cuts: packets.length, body_files: names.length, raw_source_bytes_across_cuts: sourceBytesAcrossCuts,
      unique_source_bytes_written: newBodyBytes, resident_source_and_rendered_bytes: residentBodyBytes,
      avoided_repeated_source_write_bytes: sourceBytesAcrossCuts - newBodyBytes,
      source_write_reduction_fraction: 1 - newBodyBytes / sourceBytesAcrossCuts,
      exact_body_parity: true, purpose_and_evidence_invalidate: true, chain_verified: store.verifyReceiptChain().verified, samples };
  } finally { store.close(); rmSync(home, { recursive: true, force: true }); }
}
if (import.meta.main) {
  const args = process.argv.slice(2);
  if (args.length !== 2 || args[0] !== '--output') throw new Error('usage: benchmark --output workspace/report.json');
  const path = resolve(args[1]), rel = relative(root, path);
  if (!rel || rel.startsWith('..') || rel.startsWith('/')) throw new Error('report must be inside workspace');
  const report = await contextResidencyWitness();
  writeFileSync(path, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify({ state: report.state, cuts: report.cuts, source_write_reduction_fraction: report.source_write_reduction_fraction,
    raw_source_bytes_across_cuts: report.raw_source_bytes_across_cuts, resident_source_and_rendered_bytes: report.resident_source_and_rendered_bytes }));
}
