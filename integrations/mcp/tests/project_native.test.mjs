// What this does not prove:
// - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, mkdir, writeFile, readFile, readdir, unlink, copyFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createProjectNative } from '../project_native.mjs';
import { createProjectNode } from '../project_node.mjs';
import { projectRun } from '../project_runtime.mjs';

const families = {
  'bonfyre-cms': 'BonfyreCMS', 'bonfyre-graph': 'BonfyreArtifactGraph', 'bonfyre-flow': 'BonfyreFlow', 'bonfyre-discipl': 'BonfyreDisCIPL', 'bonfyre-qwen-fpq': 'BonfyreQwenFPQ',
  'bonfyre-economy': 'BonfyreEconomy', 'bonfyre-finance': 'BonfyreFinance', 'bonfyre-frappe-compiler': 'BonfyreFrappeCompiler', 'bonfyre-project': 'BonfyreProject', 'bonfyre-gen': 'BonfyreGen', 'bonfyre-lifecycle': 'BonfyreLifecycle', 'bonfyre-web-address': 'BonfyreWebAddress',
};
const response = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json' } });

async function fixture({ runtimeResponse, httpResponse, env = {} } = {}) {
  const base = await mkdtemp(path.join(tmpdir(), 'project-native-'));
  const root = path.join(base, 'root'), home = path.join(base, 'home');
  const managed = path.join(home, '.bonfyre', 'bin'), control = path.join(home, '.bonfyre', 'moe');
  await mkdir(managed, { recursive: true }); await mkdir(root, { recursive: true });
  await mkdir(path.join(control, 'content-types'), { recursive: true });
  await writeFile(path.join(control, 'content-types', 'cms_generation.json'), '{}');
  await writeFile(path.join(managed, '.families.json'), JSON.stringify(families));
  await writeFile(path.join(managed, '.capabilities.json'), JSON.stringify({ 'bonfyre-cms': { family: 'BonfyreCMS', shape: 'cli', estate: 'content', subcommands: ['schema'] } }));
  for (const name of Object.keys(families)) await writeFile(path.join(managed, name), Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 0]), { mode: 0o755 });
  const calls = [], requests = [];
  const run = async (argv, options) => {
    calls.push({ argv, options });
    const stdout = await runtimeResponse?.(argv, options) ?? '{}';
    return { argv, exit_code: 0, stdout, stderr: '', duration_ms: 1 };
  };
  const request = async (url, options) => {
    requests.push({ url: String(url), options });
    return httpResponse?.(String(url), options) || response({});
  };
  return { root, home, managed, control, calls, requests, project: createProjectNative({ root, home, env, run, request }) };
}

test('native inventory and invoke refuse executable Python launchers without executing classification', async () => {
  const f = await fixture();
  await writeFile(path.join(f.managed, 'bonfyre-python'), '#!/usr/bin/env python3\nprint(1)\n', { mode: 0o755 });
  await writeFile(path.join(f.managed, '.families.json'), JSON.stringify({ ...families, 'bonfyre-python': 'BonfyrePython' }));
  await writeFile(path.join(f.managed, 'bonfyre-project.bak20260901'), Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 0]), { mode: 0o755 });
  await writeFile(path.join(f.managed, 'bonfyre-cms.patched2'), Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 0]), { mode: 0o755 });
  await writeFile(path.join(f.managed, 'bonfyre-api-selftest.archive'), Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 0]), { mode: 0o755 });
  const inventory = await f.project.call('native_tool_inventory');
  expect(inventory.tools.some((tool) => tool.binary.includes('.bak'))).toBe(false);
  expect(inventory.tools.some((tool) => /\.(?:patched\d*|archive)$/.test(tool.binary))).toBe(false);
  expect(inventory.tools.some((tool) => tool.family === 'BonfyreCMS')).toBe(true);
  expect(inventory.tools.some((tool) => tool.family === 'BonfyrePython')).toBe(false);
  expect(inventory.refused.some((row) => row.binary.endsWith('bonfyre-python'))).toBe(true);
  expect(f.calls.length).toBe(0);
  await expect(f.project.call('native_tool_invoke', { family: 'BonfyrePython', args: [] })).rejects.toThrow('no native executable');
  const called = await f.project.call('native_tool_invoke', { family: 'BonfyreCMS', args: ['schema', 'list'] });
  expect(called.status).toBe('passed');
  expect(called.capabilities).toContain('cmd:schema');
  expect(f.calls.length).toBe(1);
});

test('estate capability read composes native tools, models, adapters and fleet without provider execution', async () => {
  const f = await fixture();
  const registryDir = path.join(f.home, '.local', 'share', 'bonfyre');
  await mkdir(registryDir, { recursive: true });
  const registry = new Database(path.join(registryDir, 'capability.db'));
  registry.exec(`CREATE TABLE capabilities(
    id TEXT PRIMARY KEY,name TEXT NOT NULL,description TEXT NOT NULL,tags TEXT,binary TEXT NOT NULL,command TEXT,model_id TEXT,
    hardware_tier TEXT NOT NULL DEFAULT 'cpu',cost_estimate REAL NOT NULL DEFAULT 0.0,latency_tier TEXT NOT NULL DEFAULT 'fast',
    stage_class TEXT,artifact_out TEXT,keywords TEXT,source TEXT NOT NULL DEFAULT 'manual',updated INTEGER NOT NULL)`);
  const insert = registry.prepare('INSERT INTO capabilities(id,name,description,binary,cost_estimate,latency_tier,stage_class,artifact_out,source,updated) VALUES(?,?,?,?,?,?,?,?,?,?)');
  insert.run('ready','Ready','fixture','bonfyre-ready',0,'fast','runtime','Receipt','builtin',1);
  insert.run('partial','Partial','fixture','bonfyre-partial',-1,'unmeasured','','','manual',1);
  insert.run('mixed','Mixed','fixture','bonfyre-mixed',1.25,'batch','transform','','catalog',1);
  registry.close();
  const capabilities = await f.project.call('estate_capabilities', { refresh: true });
  expect(capabilities.schema_version).toBe('bonfyre.estate.capabilities.v1');
  expect(Array.isArray(capabilities.native_tools)).toBe(true);
  expect(Array.isArray(capabilities.models)).toBe(true);
  expect(Array.isArray(capabilities.adapters)).toBe(true);
  expect(capabilities.adapters.every((adapter) => adapter.state === 'unavailable')).toBe(true);
  expect(Array.isArray(capabilities.fleet.providers)).toBe(true);
  expect(capabilities.capability_registry).toMatchObject({
    state: 'available', registry_records: 3,
    metadata: { stage_classified: 2, stage_unclassified: 1, artifact_named: 1, artifact_missing: 2,
      latency_measured: 2, latency_unmeasured: 1, cost_recorded: 2, cost_unmeasured: 1 },
    stages: { transform: 1, runtime: 1, unclassified: 1 },
    sources: { builtin: 1, catalog: 1, manual: 1 },
  });
  expect(f.calls).toHaveLength(0);
});
test('managed executable provenance never borrows a different checkout artifact', async () => {
  const f = await fixture();
  const alternative = path.join(f.root, 'cmd', 'BonfyreCMS', 'bonfyre-cms-helper');
  await mkdir(path.dirname(alternative), { recursive: true });
  await writeFile(alternative, Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 0]), { mode: 0o755 });
  const entry = (await f.project.call('native_tool_inventory')).tools.find((t) => t.family === 'BonfyreCMS');
  expect(entry.checkout_comparison).toBe('checkout_missing');
  expect(entry.checkout_binary).toBeNull();
  const result = await f.project.call('native_tool_invoke', { family: 'BonfyreCMS', args: ['schema', 'list'] });
  expect(result.checkout_comparison).toBe('checkout_missing');
  expect(f.calls.at(-1).argv[0]).toBe(path.join(f.managed, 'bonfyre-cms'));
});

test('native inventory and invoked receipts identify managed versus checkout drift without changing the selected owner', async () => {
  const f = await fixture();
  const source = path.join(f.root, 'cmd', 'BonfyreCMS', 'bonfyre-cms');
  await mkdir(path.dirname(source), { recursive: true });
  await writeFile(source, await readFile(path.join(f.managed, 'bonfyre-cms')), { mode: 0o755 });
  const before = (await f.project.call('native_tool_inventory')).tools.find((t) => t.family === 'BonfyreCMS');
  expect(before.checkout_comparison).toBe('not_compared');
  expect(before.binary_sha256_at_readback).toBeNull();
  expect(before.checkout_sha256_at_readback).toBeNull();
  expect(before.binary_bytes).toBe(before.checkout_bytes);
  expect(before.source_build_freshness).toBe('not_attested');
  const invoked = await f.project.call('native_tool_invoke', { family: 'BonfyreCMS', args: ['schema', 'list'] });
  expect(invoked.checkout_comparison).toBe('matched');
  expect(invoked.binary_sha256_at_readback).toMatch(/^[a-f0-9]{64}$/);
  expect(invoked.checkout_sha256_at_readback).toBe(invoked.binary_sha256_at_readback);
  expect(invoked.binary).toBe(path.join(f.managed, 'bonfyre-cms'));
  expect(f.calls.at(-1).argv[0]).toBe(path.join(f.managed, 'bonfyre-cms'));
  await writeFile(source, Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 0, 1, 2, 3]));
  const after = (await f.project.call('native_tool_inventory')).tools.find((t) => t.family === 'BonfyreCMS');
  expect(after.checkout_comparison).toBe('different_size');
  expect(after.checkout_bytes).not.toBe(after.binary_bytes);
  const stale = await f.project.call('native_tool_invoke', { family: 'BonfyreCMS', args: ['schema', 'list'] });
  expect(stale.checkout_comparison).toBe('diverged');
  expect(f.calls.at(-1).argv[0]).toBe(path.join(f.managed, 'bonfyre-cms'));
  const missing = (await f.project.call('native_tool_inventory')).tools.find((t) => t.family === 'BonfyreFinance');
  expect(missing.checkout_comparison).toBe('checkout_missing');
});

test('checkout-only native binaries remain callable without a false managed parity claim', async () => {
  const f = await fixture();
  const source = path.join(f.root, 'cmd', 'BonfyreWitness', 'bonfyre-witness');
  await mkdir(path.dirname(source), { recursive: true });
  await writeFile(source, Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 0]), { mode: 0o755 });
  const item = (await f.project.call('native_tool_inventory')).tools.find((t) => t.family === 'BonfyreWitness');
  expect(item.source).toBe('checkout');
  expect(item.checkout_comparison).toBe('checkout_only');
  const result = await f.project.call('native_tool_invoke', { family: 'BonfyreWitness', args: ['status'] });
  expect(result.checkout_comparison).toBe('checkout_only');
  expect(f.calls.at(-1).argv[0]).toBe(source);
});

test('ExperienceMap MCP reuses its retained corpus without overriding an explicit operator selection', async () => {
  const setup = async (f) => {
    const binary = path.join(f.managed, 'bonfyre-experience-map');
    await writeFile(binary, Buffer.from([0xcf, 0xfa, 0xed, 0xfe, 0]), { mode: 0o755 });
    await writeFile(path.join(f.managed, '.families.json'), JSON.stringify({ ...families, 'bonfyre-experience-map': 'BonfyreExperienceMap' }));
    const corpus = path.join(f.root, 'cmd', 'BonfyreExperienceMap', 'generated');
    await mkdir(corpus, { recursive: true });
    await writeFile(path.join(corpus, 'manifest.json'), '{"schema":"bonfyre.experience-map.v1"}');
    await writeFile(path.join(corpus, 'all_bindings.tsv'), 'kind\tid\tnote\n');
    return corpus;
  };
  const f = await fixture();
  const corpus = await setup(f);
  const bound = await f.project.call('native_tool_invoke', { family: 'BonfyreExperienceMap', args: ['stats'] });
  expect(f.calls.at(-1).options.env.BONFYRE_EXPERIENCE_MAP_ROOT).toBe(corpus);
  expect(bound.corpus_binding).toEqual({ path: corpus, source: 'checkout-generated-corpus', release_match: 'not_attested', scope: 'read-only reference lookup' });
  await unlink(path.join(corpus, 'all_bindings.tsv'));
  const missing = await f.project.call('native_tool_invoke', { family: 'BonfyreExperienceMap', args: ['stats'] });
  expect(missing.corpus_binding).toBeNull();
  expect(f.calls.at(-1).options.env.BONFYRE_EXPERIENCE_MAP_ROOT).toBeUndefined();
  const own = await fixture({ env: { BONFYRE_EXPERIENCE_MAP_ROOT: '/operator-chosen-release' } });
  await setup(own);
  const selected = await own.project.call('native_tool_invoke', { family: 'BonfyreExperienceMap', args: ['stats'] });
  expect(selected.corpus_binding).toBeNull();
  expect(own.calls.at(-1).options.env.BONFYRE_EXPERIENCE_MAP_ROOT).toBe('/operator-chosen-release');
});

test('default Project MCP dispatch binds the concrete native owner without mocks', async () => {
  const f = await fixture();
  const node = await createProjectNode({ root: f.root, home: f.home, token: 'fixture-token', run: async () => { throw new Error('unexpected execution'); }, request: async () => { throw new Error('unexpected request'); } });
  const inventory = await node.call('native_tool_inventory', {});
  expect(inventory.tools.some((tool) => tool.family === 'BonfyreCMS')).toBe(true);
});

test('filtered native discovery pages summaries without executing owners or losing full provenance', async () => {
  const f = await fixture();
  const node = await createProjectNode({ root: f.root, home: f.home, token: 'fixture-token', run: async () => { throw new Error('unexpected execution'); } });
  const all = await node.call('native_tool_inventory', {});
  const first = await node.call('native_tool_inventory', { detail: 'summary', limit: 2 });
  expect(first).toMatchObject({ count: all.count, matched: all.count, offset: 0, returned: 2, next_offset: 2 });
  expect(first.performance).toMatchObject({
    classification_concurrency: 16,
    managed_candidates: expect.any(Number),
    checkout_candidates: expect.any(Number),
    duration_ms: expect.any(Number),
    managed_classification_ms: expect.any(Number),
    checkout_classification_ms: expect.any(Number),
  });
  expect(first.tools[0]).not.toHaveProperty('binary');
  const second = await node.call('native_tool_inventory', { detail: 'summary', limit: 2, offset: first.next_offset });
  expect(second.tools.map(x => x.family)).toEqual(all.tools.slice(2, 4).map(x => x.family));
  const selected = await node.call('native_tool_inventory', { query: 'CMD:SCHEMA', detail: 'summary' });
  expect(selected).toMatchObject({ matched: 1, returned: 1, next_offset: null });
  expect(selected.tools[0].family).toBe('BonfyreCMS');
  const full = await node.call('native_tool_inventory', { query: 'BonfyreCMS', detail: 'full' });
  expect(full.tools).toEqual(all.tools.filter(x => x.family === 'BonfyreCMS'));
  expect((await node.call('native_tool_inventory', { query: 'no-such-owner' })).tools).toEqual([]);
  expect((await node.call('native_tool_inventory', { detail: 'summary', offset: 999 })).next_offset).toBeNull();
  for (const args of [{ detail: 'invalid' }, { limit: 0 }, { offset: -1 }, { query: 'x'.repeat(257) }, { query: [] }, { unknown: true }])
    await expect(node.call('native_tool_inventory', args)).rejects.toThrow();
  expect(f.calls).toHaveLength(0);
  await writeFile(path.join(f.managed, '.capabilities.json'), JSON.stringify({ 'bonfyre-cms': { family: 'BonfyreCMS', subcommands: ['new-verb'] } }));
  expect((await node.call('native_tool_inventory', { query: 'cmd:new-verb', detail: 'summary' })).tools[0].family).toBe('BonfyreCMS');
  expect((await node.call('native_tool_inventory', { query: 'cmd:schema' })).matched).toBe(0);
});

test('MCP command discovery reads one native CLI catalog without duplicating routes or granting execution', async () => {
  const catalog = { query:null, summary:{total:2,ready:2,missing:0},
    sections:[{name:'Websites and content',total:2,ready:2,missing:0,commands:[
      {command:'site',binary:'bonfyre-site',module:'BonfyreWebAddress',tier:'start',ready:true},
      {command:'api',binary:'bonfyre-api',module:'BonfyreAPI',tier:'everyday',ready:true}]}]};
  const f = await fixture({runtimeResponse:async argv => {
    if (argv[1] === 'list' && argv[2] === '--json') return JSON.stringify(catalog);
    throw new Error('unrelated native process invoked');
  }});
  const cli = path.join(f.managed, 'bonfyre');
  await writeFile(cli, Buffer.from([0xcf,0xfa,0xed,0xfe,0]), {mode:0o755});
  const surface = await f.project.call('native_tool_inventory', {surface:'commands'});
  expect(surface).toMatchObject({schema_version:'bonfyre.native.command.catalog.v1',
    surface:'commands',scope:'public',owner:'BonfyreCLI',summary:catalog.summary,sections:catalog.sections});
  expect(surface.binary_sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(f.calls).toHaveLength(1);
  expect(f.calls[0].argv).toEqual([cli,'list','--json']);
  const all = await f.project.call('native_tool_inventory', {surface:'commands',query:'site',all:true});
  expect(all.scope).toBe('all');
  expect(f.calls[1].argv).toEqual([cli,'list','--json','--all','site']);
  await expect(f.project.call('native_tool_inventory', {surface:'commands',limit:5})).rejects.toThrow('pagination');
  await expect(f.project.call('native_tool_inventory', {surface:'commands',all:'yes'})).rejects.toThrow('boolean');
  await expect(f.project.call('native_tool_inventory', {all:true})).rejects.toThrow('only valid');
  expect(f.calls).toHaveLength(2);
});

test('MCP command catalog marks managed drift and selects the tested checkout for read-only discovery', async () => {
  const f = await fixture({runtimeResponse:async argv => JSON.stringify({
    query:null,summary:{total:1,ready:1,missing:0},
    sections:[{name:'Websites and content',total:1,ready:1,missing:0,commands:[{command:'site',tier:'start',ready:true}]}],
  })});
  const managed = path.join(f.managed, 'bonfyre');
  const checkout = path.join(f.root, 'cmd', 'BonfyreCLI', 'bonfyre');
  await mkdir(path.dirname(checkout), {recursive:true});
  await writeFile(managed, Buffer.from([0xcf,0xfa,0xed,0xfe,0]), {mode:0o755});
  await writeFile(checkout, Buffer.from([0xcf,0xfa,0xed,0xfe,1]), {mode:0o755});
  const catalog = await f.project.call('native_tool_inventory', {surface:'commands'});
  expect(catalog).toMatchObject({selected_source:'checkout',managed_checkout_drift:'diverged',
    source_build_freshness:'not_attested'});
  expect(catalog.managed_binary_sha256).not.toBe(catalog.checkout_binary_sha256);
  expect(f.calls[0].argv).toEqual([checkout,'list','--json']);
});

test('managed family manifest cannot redirect invocation outside its binary directory', async () => {
  const f = await fixture();
  await writeFile(path.join(f.managed, '.families.json'), JSON.stringify({ '../outside': 'BonfyreOutside' }));
  await expect(f.project.call('native_tool_invoke', { family: 'BonfyreOutside' })).rejects.toThrow('no native executable');
  expect(f.calls).toHaveLength(0);
});

test('financial routes never trust a caller-supplied local-ready bit', async () => {
  const f = await fixture();
  const route = await f.project.call('financial_membrane', { action: 'route', recipe: 'demo', _local_model_ready: true });
  expect(route.basis).toContain('not a provider booking');
  expect(f.calls.find(({ argv }) => argv.includes('route'))?.options.env.BONFYRE_ECONOMY_LOCAL_MODEL_READY).toBeUndefined();
  await expect(f.project.call('financial_membrane', { action: 'set_cap', recipe: 'demo', usd_cap: Number.NaN })).rejects.toThrow();
  await f.project.call('financial_membrane', { action: 'set_cap', recipe: 'demo', usd_cap: 1.25 });
  expect(f.calls.some(({ argv }) => argv.includes('1.250000'))).toBe(true);
});

test('read facades preserve native owners while making mutation verbs unreachable', async () => {
  const f = await fixture({ runtimeResponse: async () => JSON.stringify({ id: 7 }) });
  const finance = await f.project.call('financial_read', { action: 'status' });
  expect(finance.basis).toContain('operating-book control');
  const financeCalls = f.calls.length;
  await expect(f.project.call('financial_read', { action: 'set_cap', recipe: 'demo', usd_cap: 1 })).rejects.toThrow('status or route');
  expect(f.calls).toHaveLength(financeCalls);
  const cms = await f.project.call('cms_read', { action: 'get', content_type: 'article', namespace: 'desk', entry_id: 7 });
  expect(cms.result.id).toBe(7);
  expect(cms.forbidden_inference).toContain('not a change');
  const cmsCalls = f.calls.length;
  await expect(f.project.call('cms_read', { action: 'create', content_type: 'article', namespace: 'desk', attributes: { title: 'x' } })).rejects.toThrow('list_types, list, or get');
  expect(f.calls).toHaveLength(cmsCalls);
});

test('CMS writes preserve native namespace and mutation boundary', async () => {
  const f = await fixture({ runtimeResponse: async () => JSON.stringify({ id: 41 }) });
  const record = await f.project.call('cms_record', { action: 'create', content_type: 'article', namespace: 'desk', attributes: { title: 'x' } });
  expect(record.result.id).toBe(41);
  expect(record.authority_boundary).toContain('does not publish');
  expect(f.calls[0].argv).toContain(f.control + '/cms.db');
  expect(f.calls[0].argv).toContain('desk');
});

test('Frappe compiler preserves local lineage and optional native CMS registration', async () => {
  const f = await fixture({ runtimeResponse: async (argv) => {
    const index = argv.indexOf('--emit-cms-schema');
    if (index !== -1) await writeFile(argv[index + 1], '{}');
    return '{}';
  } });
  await mkdir(path.join(f.root, 'integrations', 'frappe-bench', 'apps', 'crm'), { recursive: true });
  const result = await f.project.call('frappe_compile', { lineage: 'crm', phase: 1, cms_doctype: 'Customer' });
  expect(result.cms_projection.state).toBe('registered_in_native_cms');
  expect(result.artifacts.at(-1).kind).toBe('NativeCMSSchema');
  expect(f.calls.some(({ argv }) => argv.includes('schema') && argv.includes('apply'))).toBe(true);
});

test('ForeignTwin observation uses public-right native owner only', async () => {
  const f = await fixture({ runtimeResponse: async (argv) => {
    await writeFile(argv[argv.indexOf('--receipt') + 1], JSON.stringify({ verdict: 'observed' }));
    await writeFile(argv[argv.indexOf('--body') + 1], 'body');
    return '{}';
  } });
  await expect(f.project.call('foreign_twin_observe', { twin_id: 't', url: 'http://127.0.0.1/a' })).rejects.toThrow('loopback');
  for (const twin_id of ['a/../b', 'a/b', '..']) await expect(f.project.call('foreign_twin_observe', { twin_id, url: 'https://example.com/a' })).rejects.toThrow();
  const result = await f.project.call('foreign_twin_observe', { twin_id: 't', url: 'https://example.com/a' });
  expect(result.receipt.verdict).toBe('observed');
  expect(result.rights.not_granted).toContain('act');
  expect(f.calls[0].argv).toContain('public');
});

test('public ForeignTwin release composes native source refs and separate grant, geo and feed discovery candidates', async () => {
  const council = 'https://cityofdallas.legistar.com/MeetingDetail.aspx?ID=1362423';
  const recording = 'https://dallastx.new.swagit.com/videos/402001';
  const gen = 'foreign-v1-verified-aa01';
  const item = (url, source, family) => ({
    url, subject_id: 'foreign_twin:' + source,
    release: { source_contract: { source, family }, stages: {
      frontier: { state: 'current', admitted: 1 }, archive: { state: 'empty', items: 0 },
      public_program: { state: 'unbound', bindings: 0 } },
      readiness: { media: { state: 'unbound' }, rights: { state: 'unbound' } },
      facets: { media: { state: 'unbound' } } }
  });
  const councilItem = item(council, 'council-agenda', 'civic');
  const mediaItem = item(recording, 'council-recording', 'media');
  const grantsItem = item('https://grants.gov/opportunity', 'grant-search-contract', 'funding');
  const seen = [];
  const f = await fixture({ httpResponse: (uri, options) => {
    seen.push([uri, options]);
    const request = new URL(uri);
    const q = request.searchParams.get('q') || '';
    const page = Number(request.pathname.split('/page/')[1] || 0);
    let records = [], next = null, total = 0;
    if (q === 'cityofdallas.legistar.com') {
      records = page ? [councilItem] : []; next = page ? null : 1; total = 1;
    } else if (q === 'dallastx.new.swagit.com') {
      records = [mediaItem]; total = 1;
    } else if (q === 'grants') { records = [grantsItem]; total = 1; }
    else if (q === 'dallas') { records = [councilItem]; total = 1; }
    return response({ schema: 'bonfyre.foreign.twins.v2', state: 'current',
      catalogue_generation: gen, offset: page, limit: 200,
      query: q, total, estate_total: 2442, twins: records, next_offset: next,
      release_summary: { source_contracts: 2442, frontier_current: 2293, archive_current: 0 } });
  } });
  const result = await f.project.call('foreign_twin_release', {
    case_urls: [council, recording, council],
    related_queries: ['grants', 'dallas'],
  });
  expect(result.case.state).toBe('released_sources_resolved');
  expect(result.case.source_refs).toEqual(['council-agenda', 'council-recording']);
  expect(result.case.exact_sources[0].readiness.rights.state).toBe('unbound');
  expect(result.case.source_collection_draft.source_refs).toEqual(result.case.source_refs);
  expect(result.case.selection_effect).toBe('none');
  expect(result.context_candidates[0].relation).toBe('candidate_only');
  expect(result.context_candidates[0].matching_released_sources[0].source).toBe('grant-search-contract');
  expect(result.release_summary.source_contracts).toBe(2442);
  expect(result.local_project_store).toBe('separate_unless_explicitly_bound');
  expect(result.readback).toHaveLength(5);
  expect(result.readback.every(row => /^[a-f0-9]{64}$/.test(row.sha256))).toBe(true);
  expect(f.calls).toHaveLength(0);
  expect(seen.every(([uri, options]) => uri.startsWith('https://aurekai.work/foreign/twins') &&
    options.method === 'GET' && !options.headers.Authorization)).toBe(true);
  expect(seen.some(([uri]) => uri.includes('/page/1') &&
    uri.includes('snapshot=' + gen))).toBe(true);
  await rm(path.dirname(f.root), { recursive: true, force: true });
});

test('public ForeignTwin release refuses mixed generations, invalid sources and unsupported catalogue outputs', async () => {
  const meeting = 'https://cityofdallas.legistar.com/MeetingDetail.aspx?ID=1362423';
  const f = await fixture({ httpResponse: uri => response({
    schema: 'bonfyre.foreign.twins.v2', state: 'current',
    catalogue_generation: new URL(uri).searchParams.get('q') === 'dallas' ? 'foreign-v1' : 'foreign-v2',
    offset: 0, total: 0, estate_total: 2442, twins: [], next_offset: null,
    release_summary: { source_contracts: 2442 }
  }) });
  await expect(f.project.call('foreign_twin_release', { query: 'dallas', case_urls: [meeting] }))
    .rejects.toThrow('generation changed');
  for (const url of ['http://example.org/a', 'https://user:pass@example.org/a',
    'https://example.org:8443/a', 'https://example.org/a#section']) {
    await expect(f.project.call('foreign_twin_release', { case_urls: [url] })).rejects.toThrow();
  }
  await expect(f.project.call('foreign_twin_release', { related_queries: ['x'.repeat(81)] })).rejects.toThrow();
  const bad = await fixture({ httpResponse: () => response({ schema: 'unexpected', state: 'current', twins: [] }) });
  await expect(bad.project.call('foreign_twin_release', { query: 'dallas' })).rejects.toThrow('unexpected release contract');
  await rm(path.dirname(f.root), { recursive: true, force: true });
  await rm(path.dirname(bad.root), { recursive: true, force: true });
});

test('public ForeignTwin release reports missing sources without elevating candidate relationships', async () => {
  const f = await fixture({ httpResponse: () => response({
    schema: 'bonfyre.foreign.twins.v2', state: 'current',
    catalogue_generation: 'foreign-v1', offset: 0, total: 0, estate_total: 2442,
    twins: [], next_offset: null, release_summary: { source_contracts: 2442 }
  }) });
  const result = await f.project.call('foreign_twin_release', {
    case_urls: ['https://dallastx.new.swagit.com/videos/402001'],
    related_queries: ['grants', 'council district', 'news'],
  });
  expect(result.case.state).toBe('incomplete');
  expect(result.case.source_refs).toEqual([]);
  expect(result.case.missing).toHaveLength(1);
  expect(result.context_candidates.every(x => x.relation === 'candidate_only')).toBe(true);
  await rm(path.dirname(f.root), { recursive: true, force: true });
});
test('ForeignTwin case reads Grants.gov prospect observations through the existing funding owner', async () => {
  const grants = { data: { hitCount: 2, oppHits: [
    { id: 'opportunity-streets', title: 'Safe Streets Roads for All', agency: 'DOT', number: 'SS4A-1', oppStatus: 'posted' },
    { id: 'unrelated', title: 'Healthcare workforce', agency: 'HHS', number: 'HHS-2', oppStatus: 'forecasted' },
  ] } };
  const post = [];
  const f = await fixture({ httpResponse: (uri, req) => {
    if (uri === 'https://api.grants.gov/v1/api/search2') {
      post.push(req);
      return response(grants);
    }
    return response({ schema: 'bonfyre.foreign.twins.v2', state: 'current',
      catalogue_generation: 'foreign-v1', offset: 0, total: 0, estate_total: 2442,
      release_summary: { source_contracts: 2442 }, twins: [], next_offset: null });
  } });
  const result = await f.project.call('foreign_twin_release', {
    query: 'dallas', grant_terms: ['SS4A'],
  });
  expect(result.funding_observations).toHaveLength(1);
  expect(result.funding_observations[0].observation_state).toBe('observed');
  expect(result.funding_observations[0].query).toBe('Safe Streets Roads for All');
  expect(result.funding_observations[0].related_to_case).toBe('unverified');
  expect(result.funding_observations[0].eligibility).toBe('not_determined');
  expect(result.funding_observations[0].application).toBe('not_submitted');
  expect(result.funding_observations[0].response_digest).toMatch(/^sha256:[a-f0-9]{64}$/);
  expect(result.funding_observations[0].returned[0].opportunity_id).toBe('opportunity-streets');
  expect(post).toHaveLength(1);
  expect(JSON.parse(post[0].body).keyword).toBe('Safe Streets Roads for All');
  expect(f.calls).toHaveLength(0);
  const noFunding = await fixture({ httpResponse: (uri) =>
    uri === 'https://api.grants.gov/v1/api/search2' ? response({ error: 'maintenance' }, 503) :
      response({ schema: 'bonfyre.foreign.twins.v2', state: 'current',
        catalogue_generation: 'foreign-v1', offset: 0, total: 0, estate_total: 2442,
        twins: [], next_offset: null }) });
  const unavailable = await noFunding.project.call('foreign_twin_release', {
    query: 'dallas', grant_terms: ['SS4A'],
  });
  expect(unavailable.funding_observations[0].observation_state).toBe('unavailable');
  expect(unavailable.funding_observations[0].returned).toEqual([]);
  await expect(f.project.call('foreign_twin_release', { query: 'dallas', grant_terms: ['x'.repeat(101)] })).rejects.toThrow();
  await rm(path.dirname(f.root), { recursive: true, force: true });
  await rm(path.dirname(noFunding.root), { recursive: true, force: true });
});
test('ForeignTwin composes official civic agenda, funding and geographic candidates without awarding or geocoding', async () => {
  const item = { EventItemId: 1234, EventItemAgendaNumber: '14', EventItemMatterFile: '26-2820A',
    EventItemTitle: 'Apply for a $400,000 grant at 7601 Greenville Avenue for a city park',
    EventItemActionName: null, EventItemPassedFlag: null };
  const seen = [];
  const f = await fixture({ httpResponse: (uri, options) => {
    seen.push(uri);
    if (uri === 'https://webapi.legistar.com/v1/dallas/events/4346/eventitems') return response([item]);
    if (uri === 'https://api.grants.gov/v1/api/search2') return response({ data: { hitCount: 0, oppHits: [] } });
    return response({ schema: 'bonfyre.foreign.twins.v2', state: 'current', catalogue_generation: 'foreign-v1',
      offset: 0, total: 0, estate_total: 2442, release_summary: { source_contracts: 2442 }, twins: [], next_offset: null });
  } });
  const result = await f.project.call('foreign_twin_release', { query: 'dallas', civic_event_id: 4346, grant_terms: ['park'] });
  expect(result.civic_agenda.state).toBe('observed');
  expect(result.civic_agenda.items).toHaveLength(1);
  expect(result.civic_agenda.items[0].funding_amount_mentions).toEqual(['$400,000']);
  expect(result.civic_agenda.items[0].location_text_candidates).toEqual(['7601 Greenville Avenue']);
  expect(result.civic_agenda.items[0].funding_state).toBe('amount_not_award');
  expect(result.civic_agenda.items[0].geo_state).toBe('place_not_geocoded');
  expect(result.civic_agenda.video_alignment).toBe('not_verified');
  expect(result.civic_agenda.response_sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(result.funding_observations[0].eligibility).toBe('not_determined');
  expect(seen).toContain('https://webapi.legistar.com/v1/dallas/events/4346/eventitems');
  await expect(f.project.call('foreign_twin_release', { query: 'dallas', civic_event_id: -1 })).rejects.toThrow();
  const missing = await fixture({ httpResponse: uri => uri.includes('legistar.com') ? response({}, 503) :
    response({ schema: 'bonfyre.foreign.twins.v2', state: 'current', catalogue_generation: 'foreign-v1',
      offset: 0, total: 0, estate_total: 2442, twins: [], next_offset: null }) });
  const unavailable = await missing.project.call('foreign_twin_release', { query: 'dallas', civic_event_id: 4346 });
  expect(unavailable.civic_agenda.state).toBe('unavailable');
  expect(unavailable.civic_agenda.items).toEqual([]);
  await rm(path.dirname(f.root), { recursive: true, force: true });
  await rm(path.dirname(missing.root), { recursive: true, force: true });
});
test('official Dallas meeting HTML supplies agenda items when the JSON API is down, without inventing item IDs', async () => {
  const meeting='https://cityofdallas.legistar.com/MeetingDetail.aspx?GUID=proof&ID=1362423&Search=';
  const html='<html><table id="ctl00_ContentPlaceHolder1_gridMain_ctl00">'+
    '<tr><td>26-2777A</td><td>1</td><td>2.</td><td></td><td>CONSENT AGENDA</td>'+
    '<td>Authorize $1,128,369.00 grant &amp; $227,026.00 match</td><td></td><td></td></tr>'+
    '<tr><td>26-2917A</td><td>1</td><td>29.</td><td></td><td>CONSENT AGENDA</td>'+
    '<td>Approve work at 405 Long Creek Road and $250,000.00</td><td></td><td></td></tr></table></html>';
  const seen=[];
  const f=await fixture({httpResponse:(uri,options)=>{
    seen.push({uri,accept:options?.headers?.Accept});
    if(uri==='https://webapi.legistar.com/v1/dallas/events/1362423/eventitems')
      return response({error:'provider_unavailable'},500);
    if(uri===meeting)return new Response(html,{headers:{'content-type':'text/html'}});
    return response({schema:'bonfyre.foreign.twins.v2',state:'current',
      catalogue_generation:'foreign-v1',offset:0,total:0,estate_total:2442,
      release_summary:{source_contracts:2442},twins:[],next_offset:null});
  }});
  try {
    const r=await f.project.call('foreign_twin_release',{
      query:'dallas',case_urls:[meeting],civic_event_id:1362423});
    expect(r.civic_agenda.state).toBe('observed');
    expect(r.civic_agenda.evidence_route).toBe('official_meeting_html_fallback');
    expect(r.civic_agenda.item_count).toBe(2);
    expect(r.civic_agenda.response_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(r.civic_agenda.items[0].item_id).toBeNull();
    expect(r.civic_agenda.items[0].identity_kind).toBe('official_html_agenda_number_and_file');
    expect(r.civic_agenda.items[0].file).toBe('26-2777A');
    expect(r.civic_agenda.items[0].funding_amount_mentions).toEqual(['$1,128,369.00','$227,026.00']);
    expect(r.civic_agenda.items[1].location_text_candidates).toEqual(['405 Long Creek Road']);
    expect(r.civic_agenda.items[0].official_action).toBeNull();
    expect(r.civic_agenda.video_alignment).toBe('not_verified');
    expect(seen.some(x=>x.uri===meeting&&x.accept==='text/html')).toBe(true);
    const unmatched=await f.project.call('foreign_twin_release',{
      query:'dallas',case_urls:[meeting],civic_event_id:1362424});
    expect(unmatched.civic_agenda.state).toBe('unavailable');
    expect(unmatched.civic_agenda.items).toEqual([]);
  } finally {await rm(path.dirname(f.root),{recursive:true,force:true})}
});
test('ForeignTwin observation and later native lookup use the same retained store', async () => {
  for (const configured of [false, true]) {
    const configuredRoot = path.join(await mkdtemp(path.join(tmpdir(), 'foreign-root-')), 'retained');
    const f = await fixture({ env: configured ? { BONFYRE_FOREIGN_ROOT: configuredRoot } : {}, runtimeResponse: async (argv) => {
      if (argv.includes('--receipt')) {
        await writeFile(argv[argv.indexOf('--receipt') + 1], '{"verdict":"observed"}');
        await writeFile(argv[argv.indexOf('--body') + 1], 'retained source');
      }
      return '{}';
    } });
    const expectedRoot = configured ? configuredRoot : path.join(f.home, '.bonfyre', 'foreign');
    const observed = await f.project.call('foreign_twin_observe', { twin_id: 'client-catalogue', url: 'https://example.com/catalogue' });
    await f.project.call('native_tool_invoke', { family: 'BonfyreProject', args: ['foreign', 'resolve', 'https://example.com/catalogue'] });
    expect(path.dirname(observed.receipt_path)).toBe(path.join(expectedRoot, 'receipts'));
    expect(path.dirname(observed.retained_body_path)).toBe(path.join(expectedRoot, 'bodies'));
    expect(await readFile(observed.retained_body_path, 'utf8')).toBe('retained source');
    for (const call of f.calls) expect(call.options.env.BONFYRE_FOREIGN_ROOT).toBe(expectedRoot);
    expect(f.calls[1].argv.slice(1)).toEqual(['foreign', 'resolve', 'https://example.com/catalogue']);
  }
});

test('ForeignTwin roots expand the configured home and refuse cwd-dependent paths', async () => {
  const f = await fixture({ env: { BONFYRE_FOREIGN_ROOT: '~/client-evidence' } });
  await f.project.call('native_tool_invoke', { family: 'BonfyreProject', args: ['foreign', 'conflicts'] });
  expect(f.calls[0].options.env.BONFYRE_FOREIGN_ROOT).toBe(path.join(f.home, 'client-evidence'));
  await expect(fixture({ env: { BONFYRE_FOREIGN_ROOT: 'client-evidence' } })).rejects.toThrow('must be absolute');
});

test('ForeignTwin native release sees the exact CMS owner store, and explicit overrides remain isolated', async () => {
  const f = await fixture();
  const sql = `CREATE TABLE internet_source(
    url TEXT, status TEXT, source TEXT, family TEXT, carries TEXT, body_regime TEXT, tier TEXT,
    last_seen TEXT, cadence_seconds INTEGER, place_field TEXT, media_field TEXT,
    rights_field TEXT, publisher_ident TEXT);`;
  try {
    await copyFile(new URL('../../../cmd/BonfyreProject/bonfyre-project', import.meta.url),
      path.join(f.managed, 'bonfyre-project'));
    const sourceDb = path.join(f.control, 'cms.db');
    const ownerDb = new Database(sourceDb);
    try {
      ownerDb.exec(sql);
      ownerDb.query('INSERT INTO internet_source VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(
        'https://civic.example.test/agenda','published','council-agenda','civic-meeting',
        'agenda','REMOTE','WARM','',86400,'','','','City of Example');
    } finally { ownerDb.close(); }
    const project = createProjectNative({ root: f.root, home: f.home, env: {}, run: projectRun });
    const result = await project.call('native_tool_invoke', { family: 'BonfyreProject',
      args: ['foreign','twins','--query','civic.example.test','--page','0','--limit','10'] });
    expect(result.status).toBe('passed');
    const release = JSON.parse(result.stdout);
    expect(release.schema).toBe('bonfyre.foreign.twins.v2');
    expect(release.estate_total).toBe(1);
    expect(release.total).toBe(1);
    expect(release.twins[0].release.source_contract.source).toBe('council-agenda');
    expect(release.twins[0].release.stages.archive.state).toBe('empty');
    expect(release.twins[0].release.stages.public_program.state).toBe('unbound');
    const separateDb = path.join(f.home,'alternate-cms.db');
    const separate = new Database(separateDb);
    try { separate.exec(sql); } finally { separate.close(); }
    const other = createProjectNative({root:f.root,home:f.home,
      env:{ BONFYRE_CMS_DB: separateDb },run:projectRun});
    const otherResult = await other.call('native_tool_invoke', {family:'BonfyreProject',
      args:['foreign','twins','--query','civic.example.test','--page','0','--limit','10']});
    if (otherResult.status !== 'passed') throw new Error('alternate CMS native result: ' + JSON.stringify({exit:otherResult.exit_code,stdout:otherResult.stdout,stderr:otherResult.stderr}));
    expect(JSON.parse(otherResult.stdout).estate_total).toBe(0);
    const stub=await fixture({env:{BONFYRE_CMS_DB:'~/configured/cms.db',
      BONFYRE_CMS_SCHEMAS:'~/configured/schemas'}});
    try {
      await stub.project.call('native_tool_invoke',{family:'BonfyreProject',args:['foreign','conflicts']});
      expect(stub.calls[0].options.env.BONFYRE_CMS_DB).toBe(path.join(stub.home,'configured/cms.db'));
      expect(stub.calls[0].options.env.BONFYRE_CMS_SCHEMAS).toBe(path.join(stub.home,'configured/schemas'));
    } finally { await rm(path.dirname(stub.root),{recursive:true,force:true}); }
    await expect(fixture({env:{BONFYRE_CMS_DB:'relative-cms.db'}})).rejects.toThrow('must be absolute');
  } finally { await rm(path.dirname(f.root), { recursive: true, force: true }); }
});
test('compiled Project owner resolves retained evidence through the MCP root binding', async () => {
  const f = await fixture();
  try {
    // This executes the native owner, so a transport-only environment assertion
    // cannot pass while the C command still reads a different receipts setting.
    await copyFile(new URL('../../../cmd/BonfyreProject/bonfyre-project', import.meta.url), path.join(f.managed, 'bonfyre-project'));
    const retained = path.join(f.home, '.bonfyre', 'foreign', 'receipts');
    await mkdir(retained, { recursive: true });
    const url = 'https://example.com/client-catalogue';
    await writeFile(path.join(retained, 'client.json'), JSON.stringify({
      schema: 'bonfyre-foreign-observation.v1', twin_id: 'client-catalogue', source_url: url,
      observed_at: '2026-10-07T00:00:00Z', observe_right: 'public', policy_decision: 'allow',
      fetch_attempted: true, transport_exit: 0, http_status: 200, body_bytes: 4,
      content_sha256: '230d8358dc8e8890b4c58deeb62912ee2f20357ae92a5cc861b98e68fe31acb5',
      observation_kind: 'source_observed', availability_claim: false,
    }));
    const project = createProjectNative({ root: f.root, home: f.home, env: {}, run: projectRun });
    const result = await project.call('native_tool_invoke', { family: 'BonfyreProject', args: ['foreign', 'history', url] });
    expect(result.stderr).toBe('');
    expect(result.status).toBe('passed');
    const history = JSON.parse(result.stdout);
    expect(history.source_url).toBe(url);
    expect(history.observations).toBe(1);
    expect(history.history).toHaveLength(1);
    const separateReceipts = path.join(f.home, 'explicit-receipts');
    await mkdir(separateReceipts);
    const configured = createProjectNative({ root: f.root, home: f.home,
      env: { BONFYRE_FOREIGN_RECEIPTS: separateReceipts }, run: projectRun });
    const absent = await configured.call('native_tool_invoke', { family: 'BonfyreProject', args: ['foreign', 'history', url] });
    expect(absent.exit_code).toBe(1);
    expect(JSON.parse(absent.stdout).observations).toBe(0);
    const overlong = await projectRun([path.join(f.managed, 'bonfyre-project'), 'foreign', 'history', url],
      { env: { BONFYRE_FOREIGN_ROOT: '/' + 'x'.repeat(5000) } });
    expect(overlong.exit_code).toBe(2);
    expect(overlong.stderr).toContain('cannot derive BONFYRE_FOREIGN_RECEIPTS');
  } finally { await rm(path.dirname(f.root), { recursive: true, force: true }); }
});

test('ForeignTwin observation honors explicitly separated receipt and body stores', async () => {
  const base = await mkdtemp(path.join(tmpdir(), 'foreign-separated-'));
  const receipts = path.join(base, 'receipts'), bodies = path.join(base, 'bodies');
  const f = await fixture({ env: { BONFYRE_FOREIGN_RECEIPTS: receipts, BONFYRE_FOREIGN_BODIES: bodies }, runtimeResponse: async argv => {
    await writeFile(argv[argv.indexOf('--receipt') + 1], '{"verdict":"observed"}');
    await writeFile(argv[argv.indexOf('--body') + 1], 'retained source');
    return '{}';
  } });
  try {
    const result = await f.project.call('foreign_twin_observe', { twin_id: 'client', url: 'https://example.com/client' });
    expect(path.dirname(result.receipt_path)).toBe(receipts);
    expect(path.dirname(result.retained_body_path)).toBe(bodies);
    expect(f.calls[0].options.env.BONFYRE_FOREIGN_RECEIPTS).toBe(receipts);
    expect(f.calls[0].options.env.BONFYRE_FOREIGN_BODIES).toBe(bodies);
  } finally {
    await rm(base, { recursive: true, force: true });
    await rm(path.dirname(f.root), { recursive: true, force: true });
  }
});

test('Matrix remains unconfigured instead of silently opening another store, and honors declared remote route', async () => {
  const first = await fixture();
  const unconfigured = await first.project.call('matrix_address_resolve', { host: 'Example.com', path: '/a' });
  expect(unconfigured.status).toBe('unconfigured');
  expect(first.calls.length).toBe(0);
  const key = path.join(first.home, 'ssh.key'); await writeFile(key, 'fixture');
  const remote = await fixture({ env: { BONFYRE_MATRIX_REMOTE_SSH_TARGET: 'user@matrix.example', BONFYRE_MATRIX_REMOTE_SSH_KEY: key, BONFYRE_MATRIX_REMOTE_DB: '/srv/matrix.db' }, runtimeResponse: async () => '{"verdict":"resolved"}\n' });
  const resolved = await remote.project.call('matrix_address_resolve', { host: 'Example.com', path: '/a' });
  expect(resolved.status).toBe('resolved');
  expect(resolved.backend.kind).toBe('configured-remote-native-owner');
  expect(remote.calls[0].argv[0]).toBe('ssh');
  expect(remote.calls[0].argv.at(-1)).toContain("'/srv/matrix.db'");
  expect(remote.calls[0].argv.at(-1)).toContain("'/usr/local/bin/bonfyre-web-address'");
  await expect(remote.project.call('matrix_closure_compile', { host: 'example.com', path: '/../x' })).rejects.toThrow();
});

test('remote Matrix shell transport preserves metacharacters as one literal address argument', async () => {
  const seed = await fixture();
  const key = path.join(seed.home, 'ssh.key'); await writeFile(key, 'fixture');
  const f = await fixture({ env: { BONFYRE_MATRIX_REMOTE_SSH_TARGET: 'user@matrix.example', BONFYRE_MATRIX_REMOTE_SSH_KEY: key, BONFYRE_MATRIX_REMOTE_DB: '/srv/matrix.db' }, runtimeResponse: async () => '{"verdict":"resolved"}\n' });
  const attack = '/foo?x=1&/usr/bin/id';
  const result = await f.project.call('matrix_address_resolve', { host: 'example.com', path: attack });
  expect(result.status).toBe('resolved');
  const remoteCommand = f.calls[0].argv.at(-1);
  expect(remoteCommand).toContain(`'${attack}'`);
  expect(f.calls[0].argv).toHaveLength(11);
  const parsed = Bun.spawnSync(['/bin/sh', '-c', `set -- ${remoteCommand}; printf '%s\\n' "$@"`]);
  expect(parsed.exitCode).toBe(0);
  const words = parsed.stdout.toString().trimEnd().split('\n');
  expect(words[words.indexOf('--path') + 1]).toBe(attack);
  expect(parsed.stdout.toString()).not.toContain('uid=');
});

test('MoE generation persists the four existing control-plane projections before final manifest', async () => {
  const f = await fixture({ httpResponse: (url) => url.endsWith('/v1/chat/completions') ? response({ model: 'local-model', choices: [{ message: { content: 'memo' } }], usage: { completion_tokens: 2 } }) : response({}), runtimeResponse: async (argv) => {
    if (argv.includes('list')) return JSON.stringify({ data: [] });
    if (argv.includes('create')) return JSON.stringify({ id: 7 });
    return '{}';
  } });
  const result = await f.project.call('bonfyre_moe_generate', { prompt: 'draft' });
  expect(result.control_plane.cms_lambda_tensor.entry_id).toBe(7);
  expect(result.control_plane.artifact_graph.operator).toContain(result.receipt_id);
  expect(result.control_plane.workgraph.status).toBe('defined');
  expect(result.control_plane.actor_graph.status).toBe('refreshed');
  expect(result.control_plane_manifest_persisted).toBe(true);
  expect(JSON.parse(await readFile(result.control_plane.receipt_path, 'utf8')).control_plane).toEqual(result.control_plane);
});

test('QUIC generation preserves local client receipt and same durable projection contract', async () => {
  const f = await fixture({ runtimeResponse: async (argv) => {
    const index = argv.indexOf('--json');
    if (index !== -1) await writeFile(argv[index + 1], JSON.stringify({ last_response: JSON.stringify({ success: true, text: 'code', tokens_generated: 3 }) }));
    if (argv.includes('list')) return JSON.stringify({ data: [] });
    if (argv.includes('create')) return JSON.stringify({ id: 8 });
    return '{}';
  } });
  const result = await f.project.call('bonfyre_quic_fpq_generate', { prompt: 'code' });
  expect(result.control_plane.cms_lambda_tensor.entry_id).toBe(8);
  expect(result.endpoint).toBe('quic://127.0.0.1:9443');
  expect(result.usage.completion_tokens).toBe(3);
});

test('institutional preparation composes local health, native recipe/economy, and persisted generation without effects', async () => {
  const f = await fixture({ httpResponse: (url) => url.endsWith('/health') ? response({ status: 'ok' }) : url.endsWith('/v1/models') ? response({ data: [{ id: 'local' }] }) : response({ model: 'local', choices: [{ message: { content: 'candidate' } }] }), runtimeResponse: async (argv) => argv.includes('list') ? JSON.stringify({ data: [] }) : argv.includes('create') ? JSON.stringify({ id: 9 }) : '{}' });
  const result = await f.project.call('institutional_prepare', { subject: 's', objective: 'study', model_route: 'moe' });
  expect(result.local_runtime.status).toBe('passed');
  expect(result.local_generation.control_plane_manifest_persisted).toBe(true);
  expect(result.authority.effect_authority).toBe('not_present');
  expect(f.calls.find(({ argv }) => argv.includes('route'))?.options.env.BONFYRE_ECONOMY_LOCAL_MODEL_READY).toBe('1');
});

test('capability protocol only records independently verified facilitator responses', async () => {
  const f = await fixture();
  const unverified = await f.project.call('capability_fulfillment', { action: 'verify_payment', capability: 'test', exchange: 'e', payment_reference: 'p' });
  expect(unverified.verification.verified).toBe(false);
  expect(f.calls.some(({ argv }) => argv.includes('rail-verified'))).toBe(false);
  const quote = await fixture({ httpResponse: () => response({ protocol: 'MPP', capability: 'test', priced: true, amount: '1' }) });
  const opened = await quote.project.call('capability_fulfillment', { action: 'open_quote', capability: 'test', exchange: 'e' });
  expect(opened.public_mpp_quote.quote_sha256).toHaveLength(64);
  expect(opened.authority.not_granted).toContain('charge');
  expect(quote.calls.some(({ argv }) => argv.includes('mpp-quote-sha256:' + opened.public_mpp_quote.quote_sha256))).toBe(true);
});

test('configured facilitator can advance only after HTTP success and a verified answer', async () => {
  const env = { BONFYRE_X402_FACILITATOR: 'https://facilitator.example' };
  const refused = await fixture({ env, httpResponse: () => response({ verified: true }, 403) });
  const noStep = await refused.project.call('capability_fulfillment', { action: 'verify_payment', capability: 'test', exchange: 'e', payment_reference: 'secret-payment' });
  expect(noStep.verification.verified).toBe(false);
  expect(refused.calls.some(({ argv }) => argv.includes('rail-verified'))).toBe(false);
  const accepted = await fixture({ env, httpResponse: () => response({ verified: true, note: 'café' }) });
  const stepped = await accepted.project.call('capability_fulfillment', { action: 'verify_payment', capability: 'test', exchange: 'e', payment_reference: 'secret-payment' });
  expect(stepped.verification.verified).toBe(true);
  expect(stepped.verification.answer_digest).toHaveLength(64);
  expect(accepted.calls.some(({ argv }) => argv.includes('rail-verified'))).toBe(true);
  expect(JSON.stringify(stepped)).not.toContain('secret-payment');
  const nonIntegral = await fixture({ env, httpResponse: () => response({ verified: true, fractional_score: 0.5 }) });
  const refusedNumber = await nonIntegral.project.call('capability_fulfillment', { action: 'verify_payment', capability: 'test', exchange: 'e', payment_reference: 'p' });
  expect(refusedNumber.verification.verified).toBe(false);
  expect(nonIntegral.calls.some(({ argv }) => argv.includes('rail-verified'))).toBe(false);
  for (const answer of [{ verified: false }, { verified: 'false' }, { verified: 1 }, { isValid: 'true' }]) {
    const adversarial = await fixture({ env, httpResponse: () => response(answer) });
    const refusal = await adversarial.project.call('capability_fulfillment', { action: 'verify_payment', capability: 'test', exchange: 'e', payment_reference: 'p' });
    expect(refusal.verification.verified).toBe(false);
    expect(adversarial.calls.some(({ argv }) => argv.includes('rail-verified'))).toBe(false);
  }
});

test('declared local Matrix store owns all read cuts, with no default store substitution', async () => {
  const base = await fixture();
  const db = path.join(base.home, 'matrix.db'); await writeFile(db, 'fixture');
  const f = await fixture({ env: { BONFYRE_MATRIX_DB: db }, runtimeResponse: async (argv) => argv.includes('resolve') ? '{"verdict":"resolved"}\n' : '{"subject":"s"}\n' });
  const result = await f.project.call('matrix_address_resolve', { host: 'example.com' });
  const constellation = await f.project.call('matrix_subject_constellation', { subject: 's' });
  const closure = await f.project.call('matrix_closure_compile', { host: 'example.com', format: 'ro-crate' });
  expect(result.backend.database).toBe(db);
  expect(constellation.status).toBe('available');
  expect(closure.status).toBe('available');
  expect(f.calls.every(({ argv }) => argv.includes(db))).toBe(true);
});

test('generation refuses malformed response and cannot publish a final manifest when a projection fails', async () => {
  const malformed = await fixture({ httpResponse: () => response({ model: 'local', choices: [] }) });
  await expect(malformed.project.call('bonfyre_moe_generate', { prompt: 'p' })).rejects.toThrow('no completion content');
  const failing = await fixture({ httpResponse: () => response({ model: 'local', choices: [{ message: { content: 'candidate' } }] }), runtimeResponse: async (argv) => {
    if (argv.includes('list')) return JSON.stringify({ data: [] });
    if (argv.includes('create')) return JSON.stringify({ id: 1 });
    if (argv.includes('add-op')) throw new Error('graph unavailable');
    return '{}';
  } });
  await expect(failing.project.call('bonfyre_moe_generate', { prompt: 'p' })).rejects.toThrow('projection failed at graph_add-op');
  const receiptDir = path.join(failing.control, 'receipts');
  const retained = (await readdir(receiptDir)).filter((name) => name.endsWith('.json'));
  expect(retained).toHaveLength(1);
  const pending = JSON.parse(await readFile(path.join(receiptDir, retained[0]), 'utf8'));
  expect(pending.output).toBe('candidate');
  expect(pending.status).toBe('projection_failed');
  expect(pending.generation_status).toBe('passed');
  expect(pending.control_plane_manifest_persisted).toBe(false);
  expect(pending.control_plane).toBeUndefined();
  expect(pending.projection_progress.completed.cms.entry_id).toBe(1);
  expect(pending.projection_progress.failed.stage).toBe('graph_add-op');
  const requestsBeforeRetry = failing.requests.length;
  await expect(failing.project.call('bonfyre_moe_generate', { prompt: 'p' })).rejects.toThrow('manual projection reconciliation required');
  expect(failing.requests).toHaveLength(requestsBeforeRetry);
});

test('failed pre-projection receipt can resume without re-requesting model output', async () => {
  const f = await fixture({ httpResponse: () => response({ model: 'local', choices: [{ message: { content: 'retained output' } }] }), runtimeResponse: async (argv) => {
    if (argv.includes('list')) return JSON.stringify({ data: [] });
    if (argv.includes('create')) return JSON.stringify({ id: 2 });
    return '{}';
  } });
  const schema = path.join(f.control, 'content-types', 'cms_generation.json');
  await unlink(schema);
  await expect(f.project.call('bonfyre_moe_generate', { prompt: 'p' })).rejects.toThrow('projection failed at cms_schema');
  const count = f.requests.length;
  await writeFile(schema, '{}');
  const resumed = await f.project.call('bonfyre_moe_generate', { prompt: 'p' });
  expect(resumed.output).toBe('retained output');
  expect(resumed.control_plane_manifest_persisted).toBe(true);
  expect(f.requests).toHaveLength(count);
});

test('a throwing native runner leaves a durable uncertain attempt and forbids model replay', async () => {
  const f = await fixture({ httpResponse: () => response({ model: 'local', choices: [{ message: { content: 'retained output' } }] }), runtimeResponse: async () => { throw new Error('runner lost child result'); } });
  await expect(f.project.call('bonfyre_moe_generate', { prompt: 'p' })).rejects.toThrow('projection failed at cms_schema');
  const receiptDir = path.join(f.control, 'receipts');
  const file = path.join(receiptDir, (await readdir(receiptDir)).find((name) => name.endsWith('.json')));
  const retained = JSON.parse(await readFile(file, 'utf8'));
  expect(retained.output).toBe('retained output');
  expect(retained.control_plane_manifest_persisted).toBe(false);
  expect(retained.projection_progress.attempts).toHaveLength(1);
  expect(retained.projection_progress.attempts[0].status).toBe('outcome_uncertain');
  const count = f.requests.length;
  await expect(f.project.call('bonfyre_moe_generate', { prompt: 'p' })).rejects.toThrow('manual projection reconciliation required');
  expect(f.requests).toHaveLength(count);

  // Crash shape: the process died before it could write projection_failed.
  retained.status = 'projection_pending';
  delete retained.projection_progress.failed;
  await writeFile(file, `${JSON.stringify(retained)}\n`);
  await expect(f.project.call('bonfyre_moe_generate', { prompt: 'p' })).rejects.toThrow('manual projection reconciliation required');
  expect(f.requests).toHaveLength(count);
});

test('HTTP generation enforces response byte budget before consuming the whole stream', async () => {
  const f = await fixture({ httpResponse: () => new Response(new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(2_000_001)); controller.close(); } })) });
  await expect(f.project.call('bonfyre_moe_generate', { prompt: 'p' })).rejects.toThrow('response exceeds byte limit');
  expect(f.calls).toHaveLength(0);
});
