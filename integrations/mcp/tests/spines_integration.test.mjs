import { expect, test } from 'bun:test';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSpinesStore } from '../spines_store.mjs';
import { createSpinesServer } from '../spines_server.mjs';
const nativeSpines = async (port, command) => {
  const child = Bun.spawn([new URL('../../../cmd/BonfyreMcp/bonfyre-mcp', import.meta.url).pathname, 'spines', command],
    { env: { ...process.env, BONFYRE_SPINES_URL: `http://127.0.0.1:${port}`, BONFYRE_MCP_TOKEN_FILE: tokenFile }, stdout: 'pipe' });
  const text = await new Response(child.stdout).text(); expect(await child.exited).toBe(0); return JSON.parse(text);
};
let tokenFile;
const binaries = Object.fromEntries(Object.entries({ deltaKernel: 'bonfyre-delta-kernel', helixVm: 'bonfyre-helix-vm', kernelProbe: 'bonfyre-kernel-probe' })
  .map(([key, name]) => [key, join(homedir(), '.local/bin', name)]));
test.skipIf(!Object.values(binaries).every(existsSync))('actual C and default Wasm engines compose through CLI/MCP owner without Python or live state', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-spines-integrated-'));
  const store = createSpinesStore({ home, projectRoot: home, ...binaries });
  const owner = await createSpinesServer({ token: 'fixture-token', store, stopPaths: [] });
  tokenFile = join(home, 'token'); writeFileSync(tokenFile, 'fixture-token\n');
  const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: owner.handle });
  try {
    const r5 = await nativeSpines(server.port, 'r5-smoke');
    expect(r5.state).toBe('passed'); expect(r5.second.receipt.state).toBe('passed'); expect(r5.parity.equal).toBe(true);
    const r6 = await nativeSpines(server.port, 'r6-smoke');
    expect(r6.state).toBe('passed'); expect(r6.reduction.witness.result).toBe(5);
    expect(r6.reduction.backend).toBe('bonfyre-helix-wasm-r7');
    expect(r6.reduction.probe.execution_kind).toBe('in_process_wasm');
    expect(r6.authority.decision).toBe('candidate_only'); expect(r6.crux.authoritative).toBe(false);
    const bootstrap = await owner.call('mission_r5_bootstrap', { mission: { id: 'mission-fixture', tasks: [{ id: 'task-one' }] } });
    expect(bootstrap.state).toBe('passed');
    const reasoning = await owner.call('mission_r6_reason', { mission: { id: 'mission-fixture', tasks: [{ id: 'task-one' }] }, token_estimate: 40 });
    expect(reasoning.state).toBe('passed'); expect(reasoning.reduction.witness.result).toBe(41);
    const denied = await owner.call('mission_r6_authority', { mission: { id: 'mission-fixture' }, effect_class: 'EXTERNAL_IRREVERSIBLE_EFFECT' });
    expect(denied.state).toBe('blocked'); expect(denied.authority.zig_executed).toBe(false);
    const phenotype = await owner.call('r7_phenotype_compile', { mission_id: 'mission-fixture', model_family: 'BonfyreQwenFPQ', token_count: 40 });
    expect(phenotype.weights.verified).toBe(false); expect(phenotype.weights.representation).toBe('unknown');
    expect(phenotype.weights.representation_owner).toBe('ModelRepresentationPassport');
    const kernel = await owner.call('r7_kernel_parity', {}); expect(kernel.state).toBe('passed');
    const placement = await owner.call('r7_placement_compile', { mission_id: 'mission-fixture', phenotype_id: phenotype.phenotype_id, privacy_class: 'internal', effect_class: 'CANDIDATE_WRITE', remote_allowed: false });
    expect(placement.selected_node).toBe('local');
    const fuse = await owner.call('r7_fuse_compile', { mission_id: 'mission-fixture', phenotype_id: phenotype.phenotype_id, placement_id: placement.placement_id });
    expect(fuse.fuse.activation).toBe('candidate'); expect(fuse.fuse.requires_authority).toBe(true);
    expect(fuse.axon.weight_representation).toBe('unresolved'); expect(fuse.axon.representation_verified).toBe(false);
    const finalized = await owner.call('mission_spines_finalize', { mission: { id: 'mission-fixture' } });
    expect(finalized.verified).toBe(true); expect(store.verifyReceipts().verified).toBe(true);
    const response = await owner.handle(new Request('http://local/mcp', { method: 'POST', headers: { authorization: 'Bearer fixture-token' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 'read', method: 'tools/call', params: { name: 'r7_nodes_list', arguments: {} } }) }));
    expect((await response.json()).result.isError).toBe(false);
  } finally { server.stop(true); owner.close(); rmSync(home, { recursive: true, force: true }); }
});
