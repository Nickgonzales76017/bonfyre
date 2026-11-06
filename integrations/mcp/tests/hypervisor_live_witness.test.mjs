import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HYPERVISOR_TOOLS } from '../hypervisor_catalog.mjs';
import { collectHypervisorWitness, snapshotHypervisorState } from './hypervisor_live_witness.mjs';

const homes = [];
afterEach(() => { for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true }); });
function fixture() {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-live-test-')); homes.push(home);
  const db = new Database(join(home, 'hypervisor.sqlite3'), { create: true });
  db.exec(readFileSync(new URL('../hypervisor_schema.sql', import.meta.url), 'utf8'));
  db.query(`INSERT INTO receipts(id,state,path,digest,payload_json,created_at) VALUES(?,?,?,?,?,?)`).run(
    'receipt-fixture', 'passed', join(home, 'receipts', 'missing.json'), 'a'.repeat(64), '{}', 1791000000);
  db.query(`INSERT INTO capabilities(id,description,effect_class) VALUES('agent.executor','executor','write')`).run();
  db.query(`INSERT INTO providers(id,repo,homepage,license,integration,mode,updated_at) VALUES(?,?,?,?,?,?,?)`).run(
    'fixture', 'local/fixture', 'https://example.test', 'MIT', 'native', 'candidate', 1791000000);
  db.query(`INSERT INTO provider_capabilities(provider_id,capability_id,updated_at) VALUES('fixture','agent.executor',?)`).run(1791000000);
  db.close();
  return home;
}

test('online snapshot covers all fourteen tables and explicitly records absent owned files', () => {
  const home = fixture(), snapshot = snapshotHypervisorState(home);
  expect(Object.keys(snapshot.tables)).toHaveLength(14);
  expect(snapshot.tables.receipts.rows).toBe(1);
  expect(snapshot.files[join(realpathSync(home), 'receipts', 'missing.json')]).toEqual({ state: 'missing' });
  expect(snapshot.files[join(realpathSync(home), 'context')]).toEqual({ state: 'missing_directory' });
  expect(snapshot.sample.capability).toBe('agent.executor');
  expect(snapshot.source_unchanged).toBe(true);
});

test('read-only RPC witness normalizes corrected annotation and never invokes a write tool', async () => {
  const home = fixture(), invoked = [];
  const legacyTools = structuredClone(HYPERVISOR_TOOLS);
  legacyTools.find(tool => tool.name === 'ecosystem_discover').annotations.readOnlyHint = true;
  const replies = {
    initialize: { serverInfo: { name: 'bonfyre-agent-hypervisor', version: '5.0.0' } },
    'tools/list': { tools: legacyTools }, 'prompts/list': { prompts: [] },
    ecosystem_providers: { providers: [] }, session_list: { sessions: [] },
    provider_compare: { capability: 'agent.executor', ranking: [], winner: null },
    replay_verify: { verified: true, receipts: 0, head: null, failures: [] },
  };
  const request = async (_url, options) => {
    const body = JSON.parse(options.body), name = body.method === 'tools/call' ? body.params.name : body.method;
    invoked.push(name);
    const answer = replies[name];
    if (!answer) throw new Error(`unreviewed fixture operation ${name}`);
    const result = body.method === 'tools/call' ? { isError: false, structuredContent: answer } : answer;
    return Response.json({ jsonrpc: '2.0', id: body.id, result });
  };
  const witness = await collectHypervisorWitness({ token: 'fixture-token', home, request });
  expect(witness.runtime).toBe('python');
  expect(witness.catalog.tool_count).toBe(33);
  expect(witness.catalog.read_annotation_corrections).toContainEqual({ tool: 'ecosystem_discover', from: true, to: false });
  expect(witness.reads.mission_status).toEqual({ state: 'absent_sample' });
  expect(invoked).toEqual(['initialize', 'tools/list', 'prompts/list', 'ecosystem_providers', 'session_list',
    'provider_compare', 'replay_verify']);
});

test('mission read projection refuses a candidate that drops historical receipt rows', async () => {
  const home = fixture(), db = new Database(join(home, 'hypervisor.sqlite3'));
  db.query(`INSERT INTO missions(id,goal,state,spec_path,mission_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)`).run(
    'mission-fixture', 'fixture goal', 'compiled', join(home, 'missions', 'mission-fixture', 'SPEC.md'),
    JSON.stringify({ id: 'mission-fixture', goal: 'fixture goal' }), 1791000000, 1791000000);
  db.close();
  const request = async (_url, options) => {
    const body = JSON.parse(options.body), name = body.method === 'tools/call' ? body.params.name : body.method;
    const values = { initialize: { serverInfo: { name: 'bonfyre-agent-hypervisor', version: '6.1.0' } },
      'tools/list': { tools: HYPERVISOR_TOOLS }, 'prompts/list': { prompts: [] },
      ecosystem_providers: { providers: [] }, session_list: { sessions: [] },
      provider_compare: { capability: 'agent.executor', ranking: [], winner: null },
      mission_status: { id: 'mission-fixture', state: 'compiled', tasks: [], mission: {}, route: {}, active: false } };
    const result = body.method === 'tools/call' ? { isError: false, structuredContent: values[name] } : values[name];
    return Response.json({ jsonrpc: '2.0', id: body.id, result });
  };
  await expect(collectHypervisorWitness({ token: 'fixture-token', home, request })).rejects.toThrow('lost tasks or receipts');
});
