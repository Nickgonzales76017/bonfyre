import { test, expect } from 'bun:test';
import { CATALOG, createProjectNode } from '../project_node.mjs';
import { mkdtempSync, rmSync, writeFileSync, mkdirSync, existsSync, renameSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SPINES_PROJECT_TOOLS } from '../project_spines.mjs';
import { V6_PROJECT_TOOLS } from '../project_v6.mjs';
import { FABRIC_PROJECT_TOOLS } from '../project_fabric.mjs';
import { PRODUCTS_PROJECT_TOOLS } from '../project_products.mjs';
import { bindCapabilityPackSchemas, capabilityPackAllows } from '../capability_packs.mjs';
import { buildControlPlaneApp } from '../control_plane_app_build.mjs';
import { PROJECT_MCP_SOURCE_SET, compileProjectMcpLaunchAgent } from '../project_service_profiles.mjs';

const make = async options => {
  const calls = [];
  const owners = Object.fromEntries(['reads', 'operator', 'native', 'spines', 'v6', 'fabric', 'products'].map(owner => [owner,
    async (name, args) => { calls.push({ owner, name, args }); return { owner, name, args }; }]));
  const { owners: ownerOverrides = {}, ...nodeOptions } = options || {};
  const node = await createProjectNode({ root: '/fixture/project', token: 'fixture-credential', owners: { ...owners, ...ownerOverrides }, ...nodeOptions });
  const rpc = async (method, params = {}, token = 'fixture-credential') => {
    const response = await node.handle(new Request('http://127.0.0.1/mcp', {
      method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 42, method, params }),
    }));
    return { status: response.status, body: response.status === 202 ? null : await response.json() };
  };
  return { node, calls, rpc };
};
function required(schema) {
  if (Object.hasOwn(schema, 'const')) return schema.const;
  if (schema.enum) return schema.enum[0];
  switch (schema.type) {
    case 'object': return Object.fromEntries((schema.required || []).map(key => [key, required(schema.properties[key])]));
    case 'array': return [];
    case 'integer': case 'number': return schema.minimum ?? 1;
    case 'boolean': return false;
    default: return schema.pattern?.includes('bonfyre') ? 'bonfyre-fixture' : 'fixture';
  }
}

test('Project, Spines, V6, Fabric and Products catalogs have exactly one owner without schema duplication', async () => {
  const { calls, rpc } = await make();
  const firstPage = (await rpc('tools/list')).body.result;
  const listed = [...firstPage.tools];
  let cursor = firstPage.nextCursor;
  while (cursor) {
    const page = (await rpc('tools/list', { cursor })).body.result;
    listed.push(...page.tools);
    cursor = page.nextCursor;
  }
  const expected = 48 + SPINES_PROJECT_TOOLS.length + V6_PROJECT_TOOLS.length + FABRIC_PROJECT_TOOLS.length + PRODUCTS_PROJECT_TOOLS.length;
  expect(expected).toBe(105);
  expect(firstPage.tools).toHaveLength(100);
  expect(firstPage.nextCursor).toBe('100');
  expect(listed).toHaveLength(expected);
  expect(new Set(listed.map(t => t.name)).size).toBe(expected);
  const canonicalPacks = ['build', 'operate', 'work', 'model', 'commerce', 'evidence', 'connect', 'institution', 'account'];
  expect(listed.find(t => t.name === 'mcp_capability_lens').inputSchema.properties.pack.enum).toEqual(canonicalPacks);
  expect(listed.find(t => t.name === 'mcp_pack_snapshot').inputSchema.properties.pack.enum).toEqual(canonicalPacks);
  expect(listed.find(t => t.name === 'mcp_product_plan').inputSchema.properties.pack.enum).toEqual(['auto', ...canonicalPacks]);
  expect(listed.find(t => t.name === 'mcp_portfolio_plan').inputSchema.properties.packs.items.enum).toEqual(canonicalPacks);
  const rebound = bindCapabilityPackSchemas([{
    name: 'mcp_product_plan',
    inputSchema: { type: 'object', properties: { pack: { type: 'string', enum: ['stale'] } } },
  }]);
  expect(rebound[0].inputSchema.properties.pack.enum).toEqual(['auto', ...canonicalPacks]);
  const reboundPortfolio = bindCapabilityPackSchemas([{
    name: 'mcp_portfolio_plan',
    inputSchema: { type: 'object', properties: { packs: { type: 'array', items: { type: 'string', enum: ['stale'] } } } },
  }]);
  expect(reboundPortfolio[0].inputSchema.properties.packs.items.enum).toEqual(canonicalPacks);
  const untouched = { name: 'project_status', inputSchema: { type: 'object', properties: {} } };
  expect(bindCapabilityPackSchemas([untouched])[0]).toBe(untouched);
  for (const tool of CATALOG.tools) {
    expect(listed.find(t => t.name === tool.name)['x-bonfyre-owner']).toBeUndefined();
    if (tool['x-bonfyre-owner'] === 'node') continue;
    const answer = (await rpc('tools/call', { name: tool.name, arguments: required(tool.inputSchema) })).body.result;
    expect(answer.isError).toBe(false);
    expect(answer.structuredContent.owner).toBe(tool['x-bonfyre-owner']);
  }
  expect(calls).toHaveLength(CATALOG.tools.filter((tool) => tool['x-bonfyre-owner'] !== 'node').length);
  expect(listed.find(t => t.name === 'project_test_profile').inputSchema.properties.profile.enum).not.toContain('python-compile');
  expect(listed.find(t => t.name === 'project_test_profile').inputSchema.properties.profile.enum).toContain('cloudflare-bridge');
  expect(listed.find(t => t.name === 'project_test_profile').annotations.readOnlyHint).toBe(false);
  expect(listed.find(t => t.name === 'mcp_capability_lens')._meta.ui.resourceUri).toBe('ui://aurekai/project-control-plane/v1.html');
  expect(listed.find(t => t.name === 'mcp_product_plan')._meta.ui.resourceUri).toBe('ui://aurekai/project-control-plane/v1.html');
  expect(listed.find(t => t.name === 'mcp_portfolio_plan')._meta.ui.resourceUri).toBe('ui://aurekai/project-control-plane/v1.html');
  expect(CATALOG.tools.find(t => t.name === 'mcp_capability_lens')).not.toHaveProperty('_meta');
  expect(CATALOG.tools.find(t => t.name === 'mcp_product_plan')).not.toHaveProperty('_meta');
  expect(CATALOG.tools.find(t => t.name === 'mcp_portfolio_plan')).not.toHaveProperty('_meta');
  const lensSchema = listed.find(t => t.name === 'mcp_capability_lens').inputSchema.properties.pack.enum;
  expect(lensSchema).toContain('account');
});

test('deployment capability pack narrows standard tools/list and refuses out-of-pack calls before dispatch', async () => {
  const { calls, rpc, node } = await make({ env: { BONFYRE_PROJECT_MCP_PACK: 'account' } });
  const listed = (await rpc('tools/list')).body.result.tools;
  expect(listed.length).toBeGreaterThan(0);
  expect(listed.every((tool) => capabilityPackAllows(tool, 'account'))).toBe(true);
  expect(listed.map((tool) => tool.name)).toContain('mcp_capability_lens');
  expect(listed.map((tool) => tool.name)).toContain('mcp_read_bundle');
  expect(listed.map((tool) => tool.name)).toContain('mcp_pack_snapshot');
  expect(listed.map((tool) => tool.name)).toContain('mcp_runtime_identity');
  expect(listed.map((tool) => tool.name)).toContain('mcp_product_plan');
  expect(listed.map((tool) => tool.name)).toContain('mcp_portfolio_plan');
  expect(listed.map((tool) => tool.name)).toContain('cms_record');
  expect(listed.map((tool) => tool.name)).not.toContain('project_status');
  expect(listed.map((tool) => tool.name)).not.toContain('workspace_list');
  const allowed = (await rpc('tools/call', { name: 'cms_record', arguments: { action: 'list_types' } })).body.result;
  expect(allowed.isError).toBe(false);
  const lens = (await rpc('tools/call', { name: 'mcp_capability_lens', arguments: { pack: 'account' } })).body.result;
  expect(lens.isError).toBe(false);
  expect(lens.structuredContent.args.pack).toBe('account');
  const runtime = (await rpc('tools/call', { name: 'mcp_runtime_identity', arguments: {} })).body.result;
  expect(runtime.isError).toBe(false);
  expect(runtime.structuredContent).toMatchObject({ state: 'source_unavailable', capability_pack: 'account', launched_source_sha256: null });
  const before = calls.length;
  const blocked = (await rpc('tools/call', { name: 'project_status', arguments: {} })).body.result;
  expect(blocked.isError).toBe(true);
  expect(calls).toHaveLength(before);
  expect(await (await node.handle(new Request('http://localhost/health'))).json()).toMatchObject({
    capability_pack: 'account', advertised_tools: listed.length,
    runtime_identity: { state: 'source_unavailable', capability_pack: 'account' },
  });
  await expect(make({ env: { BONFYRE_PROJECT_MCP_PACK: 'unknown' } })).rejects.toThrow('capability pack');
});

test('read bundle concurrently dispatches only validated read tools inside the active aperture', async () => {
  const { calls, rpc } = await make();
  const bundled = (await rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { name: 'project_status', arguments: {} },
    { name: 'native_tool_inventory', arguments: { detail: 'summary', limit: 2 } },
  ], max_concurrency: 8 } })).body.result;
  expect(bundled.isError).toBe(false);
  expect(bundled.structuredContent).toMatchObject({ owner: 'node', count: 2, passed: 2, failed: 0 });
  expect(bundled.structuredContent.results.map((item) => item.name)).toEqual(['project_status', 'native_tool_inventory']);
  expect(new Set(calls.slice(-2).map((call) => call.owner))).toEqual(new Set(['reads', 'native']));
  expect(bundled.structuredContent.performance).toMatchObject({ max_concurrency: 8, owner_invocations: 2, deduplicated_reads: 0 });
  expect(bundled.structuredContent.performance.wall_duration_ms).toBeGreaterThanOrEqual(0);

  const dedupeBefore = calls.length;
  const coalesced = (await rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { id: 'left', name: 'project_status', arguments: {} },
    { id: 'right', name: 'project_status', arguments: {} },
  ], max_concurrency: 2 } })).body.result;
  expect(coalesced.isError).toBe(false);
  expect(coalesced.structuredContent.results.map((item) => [item.id, item.shared])).toEqual([['left', false], ['right', true]]);
  expect(coalesced.structuredContent.performance).toMatchObject({ owner_invocations: 1, deduplicated_reads: 1 });
  expect(calls).toHaveLength(dedupeBefore + 1);

  let releaseRead, enteredRead;
  const readGate = new Promise((resolve) => { releaseRead = resolve; });
  const readEntered = new Promise((resolve) => { enteredRead = resolve; });
  let overlappingReads = 0;
  const overlap = await make({ owners: {
    reads: async (name, args) => {
      overlappingReads++;
      enteredRead();
      await readGate;
      return { owner: 'reads', name, args, invocation: overlappingReads };
    },
  } });
  const firstOverlap = overlap.rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { name: 'project_status', arguments: {} },
  ] } });
  await readEntered;
  const secondOverlap = overlap.rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { name: 'project_status', arguments: {} },
  ] } });
  await new Promise((resolve) => setTimeout(resolve, 10));
  expect(overlappingReads).toBe(1);
  releaseRead();
  const [firstOverlapResult, secondOverlapResult] = await Promise.all([firstOverlap, secondOverlap]);
  expect(firstOverlapResult.body.result.structuredContent.performance).toMatchObject({
    owner_invocations: 1, cross_bundle_coalesced_reads: 0,
  });
  expect(secondOverlapResult.body.result.structuredContent.performance).toMatchObject({
    owner_invocations: 0, deduplicated_reads: 1, cross_bundle_coalesced_reads: 1,
  });
  expect(secondOverlapResult.body.result.structuredContent.results[0].shared).toBe(true);
  const afterOverlap = await overlap.rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { name: 'project_status', arguments: {} },
  ] } });
  expect(afterOverlap.body.result.isError).toBe(false);
  expect(overlappingReads).toBe(2);
  expect(afterOverlap.body.result.structuredContent.performance.cross_bundle_coalesced_reads).toBe(0);

  const duplicateIdBefore = calls.length;
  const duplicateIds = (await rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { id: 'same', name: 'project_status', arguments: {} },
    { id: 'same', name: 'native_tool_inventory', arguments: {} },
  ] } })).body.result;
  expect(duplicateIds.isError).toBe(true);
  expect(duplicateIds.structuredContent.error).toContain('call ids must be unique');
  expect(calls).toHaveLength(duplicateIdBefore);

  const before = calls.length;
  const refused = (await rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { name: 'operator_pause', arguments: {} },
    { name: 'mcp_read_bundle', arguments: {} },
    { name: 'project_status', arguments: { cwd: '/elsewhere' } },
  ] } })).body.result;
  expect(refused.isError).toBe(false);
  expect(refused.structuredContent).toMatchObject({ count: 3, passed: 0, failed: 3 });
  expect(refused.structuredContent.results[0].error).toContain('read-only');
  expect(refused.structuredContent.results[1].error).toContain('unknown read tool');
  expect(refused.structuredContent.results[2].error).toContain('arguments.cwd is not allowed');
  expect(calls).toHaveLength(before);

  const account = await make({ env: { BONFYRE_PROJECT_MCP_PACK: 'account' } });
  const allowed = (await account.rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { name: 'matrix_address_resolve', arguments: { host: 'aurekai.live' } },
  ] } })).body.result;
  expect(allowed.structuredContent).toMatchObject({ passed: 1, failed: 0 });
  const accountBefore = account.calls.length;
  const outOfPack = (await account.rpc('tools/call', { name: 'mcp_read_bundle', arguments: { calls: [
    { name: 'project_status', arguments: {} },
  ] } })).body.result;
  expect(outOfPack.structuredContent).toMatchObject({ passed: 0, failed: 1 });
  expect(account.calls).toHaveLength(accountBefore);

  const operateSnapshot = (await rpc('tools/call', { name: 'mcp_pack_snapshot', arguments: { pack: 'operate' } })).body.result;
  expect(operateSnapshot.isError).toBe(false);
  expect(operateSnapshot.structuredContent).toMatchObject({ pack: 'operate', count: 5, passed: 5, failed: 0 });
  expect(Object.keys(operateSnapshot.structuredContent.results).sort()).toEqual(
    ['estate_capabilities', 'fabric_status', 'operator_backlog_audit', 'organ_drift_status', 'project_status']);
  expect(operateSnapshot.structuredContent.pack_plan.snapshot_reads).toHaveLength(5);
  expect(operateSnapshot.structuredContent.runtime_identity).toMatchObject({ state: 'source_unavailable', launched_source_sha256: null });
  expect(operateSnapshot.structuredContent.readiness).toMatchObject({
    snapshot: 'available', runtime: 'source_unavailable', effect_authority: 'not_inferred', snapshot_reads: 5, required_context: [],
  });
  expect(operateSnapshot.structuredContent.performance).toHaveProperty('wall_duration_ms');

  const commerceSnapshot = (await rpc('tools/call', { name: 'mcp_pack_snapshot', arguments: { pack: 'commerce' } })).body.result;
  expect(commerceSnapshot.isError).toBe(false);
  expect(commerceSnapshot.structuredContent).toMatchObject({ pack: 'commerce', count: 2, passed: 2, failed: 0 });
  expect(Object.keys(commerceSnapshot.structuredContent.results).sort()).toEqual(['commercial_state', 'financial_read']);
  expect(commerceSnapshot.structuredContent.performance).toHaveProperty('owner_invocations', 2);
  expect(commerceSnapshot.structuredContent.readiness).toMatchObject({ snapshot: 'available', snapshot_reads: 2, required_context: [] });

  const emptyAccountSnapshot = (await account.rpc('tools/call', { name: 'mcp_pack_snapshot', arguments: { pack: 'account' } })).body.result;
  expect(emptyAccountSnapshot.isError).toBe(false);
  expect(emptyAccountSnapshot.structuredContent).toMatchObject({ pack: 'account', count: 0, passed: 0, failed: 0, results: {} });
  expect(emptyAccountSnapshot.structuredContent.pack_plan.snapshot_reads).toEqual([]);
  expect(emptyAccountSnapshot.structuredContent.readiness).toMatchObject({
    snapshot: 'context_required',
    effect_authority: 'not_inferred',
    snapshot_reads: 0,
    required_context: ['matrix_address_resolve', 'workspace_list', 'bonfyre_admin_status'],
    next_effects: ['workspace_preview', 'workspace_update'],
  });

  const wrongPack = (await account.rpc('tools/call', { name: 'mcp_pack_snapshot', arguments: { pack: 'build' } })).body.result;
  expect(wrongPack.isError).toBe(true);
  expect(wrongPack.structuredContent.error).toContain('must match the active capability aperture');

  const operatePlan = (await rpc('tools/call', { name: 'mcp_product_plan', arguments: {
    pack: 'operate', intent: 'operator status', limit: 8,
  } })).body.result;
  expect(operatePlan.isError).toBe(false);
  expect(operatePlan.structuredContent).toMatchObject({
    schema_version: 'bonfyre.mcp.product-plan.v1',
    pack: 'operate',
    intent: 'operator status',
    readiness: { snapshot: 'available' },
    performance: { external_round_trips: 1, composed_client_operations: 2, round_trips_avoided: 1 },
  });
  expect(Array.isArray(operatePlan.structuredContent.route.shortlist)).toBe(true);
  expect(operatePlan.structuredContent.lens.args.pack).toBe('operate');
  expect(operatePlan.structuredContent.snapshot.pack).toBe('operate');

  const accountPlan = (await account.rpc('tools/call', { name: 'mcp_product_plan', arguments: {
    pack: 'account', intent: 'continue saved work',
  } })).body.result;
  expect(accountPlan.isError).toBe(false);
  expect(accountPlan.structuredContent.readiness).toMatchObject({
    snapshot: 'context_required',
    required_context: ['matrix_address_resolve', 'workspace_list', 'bonfyre_admin_status'],
  });
  expect(accountPlan.structuredContent.route.handoffs).toEqual(expect.arrayContaining([
    expect.objectContaining({ name: 'workspace_list', carrier: 'account-mcp', kind: 'read', required: true }),
    expect.objectContaining({ name: 'workspace_update', carrier: 'account-mcp', kind: 'effect', required: false }),
  ]));

  const autoModelPlan = (await rpc('tools/call', { name: 'mcp_product_plan', arguments: {
    pack: 'auto', intent: 'local model generation inference',
  } })).body.result;
  expect(autoModelPlan.isError).toBe(false);
  expect(autoModelPlan.structuredContent).toMatchObject({
    pack: 'model', requested_pack: 'auto',
    routing_selection: { state: 'selected', selected_pack: 'model' },
  });
  expect(autoModelPlan.structuredContent.routing_selection.candidates[0].matched_terms).toEqual(
    expect.arrayContaining(['model', 'generation', 'inference']));

  const autoWorkPlan = (await rpc('tools/call', { name: 'mcp_product_plan', arguments: {
    intent: 'background subagents outputs inbox notifications',
  } })).body.result;
  expect(autoWorkPlan.isError).toBe(false);
  expect(autoWorkPlan.structuredContent).toMatchObject({
    pack: 'work',
    routing_selection: { state: 'selected', selected_pack: 'work' },
    readiness: {
      snapshot: 'context_required',
      required_context: ['fabric_activity_read'],
    },
  });
  expect(autoWorkPlan.structuredContent.route.handoffs).toEqual(expect.arrayContaining([
    expect.objectContaining({ name: 'fabric_activity_read', carrier: 'account-activity', kind: 'read', required: true }),
    expect.objectContaining({ name: 'notification', carrier: 'account-activity', kind: 'effect', required: false }),
    expect.objectContaining({ name: 'model-verify-transfer', carrier: 'account-activity', kind: 'effect', required: false }),
  ]));

  const autoEvidencePlan = (await rpc('tools/call', { name: 'mcp_product_plan', arguments: {
    intent: 'verify evidence provenance receipt parity proof',
  } })).body.result;
  expect(autoEvidencePlan.isError).toBe(false);
  expect(autoEvidencePlan.structuredContent).toMatchObject({
    pack: 'evidence',
    routing_selection: { state: 'selected', selected_pack: 'evidence' },
    readiness: { snapshot: 'available' },
  });

  const autoInstitutionPlan = (await rpc('tools/call', { name: 'mcp_product_plan', arguments: {
    intent: 'institutional training frappe cms prepare',
  } })).body.result;
  expect(autoInstitutionPlan.isError).toBe(false);
  expect(autoInstitutionPlan.structuredContent).toMatchObject({
    pack: 'institution',
    routing_selection: { state: 'selected', selected_pack: 'institution' },
    readiness: { snapshot: 'available' },
  });

  const autoAccountPlan = (await account.rpc('tools/call', { name: 'mcp_product_plan', arguments: {
    intent: 'saved draft editor revision',
  } })).body.result;
  expect(autoAccountPlan.isError).toBe(false);
  expect(autoAccountPlan.structuredContent).toMatchObject({
    pack: 'account', requested_pack: 'auto',
    routing_selection: { state: 'aperture_constrained', selected_pack: 'account' },
  });

  const unrouted = (await rpc('tools/call', { name: 'mcp_product_plan', arguments: {
    intent: 'flibbertigibbet',
  } })).body.result;
  expect(unrouted.isError).toBe(false);
  expect(unrouted.structuredContent).toMatchObject({ pack: null, readiness: { snapshot: 'pack_required' },
    routing_selection: { state: 'pack_required', selected_pack: null } });

  const wrongPlan = (await account.rpc('tools/call', { name: 'mcp_product_plan', arguments: { pack: 'build' } })).body.result;
  expect(wrongPlan.isError).toBe(true);
  expect(wrongPlan.structuredContent.error).toContain('must match the active capability aperture');

  const portfolio = (await rpc('tools/call', { name: 'mcp_portfolio_plan', arguments: {
    intent: 'model institutional generation finance', max_packs: 3,
  } })).body.result;
  expect(portfolio.isError).toBe(false);
  expect(portfolio.structuredContent).toMatchObject({
    schema_version: 'bonfyre.mcp.portfolio-plan.v1',
    selected_packs: ['model', 'commerce', 'institution'],
    routing_selection: { state: 'selected', selected_packs: ['model', 'commerce', 'institution'] },
    performance: {
      external_round_trips: 1, composed_pack_plans: 3, round_trips_avoided: 2,
      context_economics: { full_catalog_tools: 105 },
    },
    readiness: { effect_authority: 'not_inferred', runtime: 'source_unavailable', runtime_current: false },
  });
  expect(portfolio.structuredContent.plans).toHaveLength(3);
  expect(portfolio.structuredContent.plans.every((plan) => !Object.hasOwn(plan.snapshot || {}, 'results'))).toBe(true);
  const contextEconomics = portfolio.structuredContent.performance.context_economics;
  expect(contextEconomics.portfolio_tools).toBeLessThan(contextEconomics.full_catalog_tools);
  expect(contextEconomics.tools_avoided).toBe(contextEconomics.full_catalog_tools - contextEconomics.portfolio_tools);
  expect(contextEconomics.portfolio_public_bytes).toBeLessThan(contextEconomics.full_catalog_public_bytes);
  expect(contextEconomics.public_bytes_avoided).toBe(contextEconomics.full_catalog_public_bytes - contextEconomics.portfolio_public_bytes);
  expect(contextEconomics.schema_reduction_percent).toBeGreaterThan(0);
  expect(contextEconomics.measurement).toContain('unique public MCP tool definitions');
  expect(portfolio.structuredContent.next_effects).toEqual(expect.arrayContaining([
    'institutional_prepare', 'bonfyre_moe_generate', 'capability_fulfillment',
  ]));

  const explicitPortfolio = (await rpc('tools/call', { name: 'mcp_portfolio_plan', arguments: {
    intent: 'audit build proof', packs: ['evidence', 'build'], max_packs: 2,
  } })).body.result;
  expect(explicitPortfolio.isError).toBe(false);
  expect(explicitPortfolio.structuredContent.selected_packs).toEqual(['evidence', 'build']);
  expect(explicitPortfolio.structuredContent.routing_selection.state).toBe('explicit');

  const accountPortfolio = (await account.rpc('tools/call', { name: 'mcp_portfolio_plan', arguments: {
    intent: 'saved draft editor', packs: ['account'],
  } })).body.result;
  expect(accountPortfolio.isError).toBe(false);
  expect(accountPortfolio.structuredContent).toMatchObject({
    selected_packs: ['account'],
    routing_selection: { state: 'aperture_constrained' },
    readiness: { state: 'context_required' },
  });
  const wrongPortfolio = (await account.rpc('tools/call', { name: 'mcp_portfolio_plan', arguments: {
    intent: 'build source', packs: ['build'],
  } })).body.result;
  expect(wrongPortfolio.isError).toBe(true);
  expect(wrongPortfolio.structuredContent.error).toContain('active capability aperture');

  const emptyPortfolio = (await rpc('tools/call', { name: 'mcp_portfolio_plan', arguments: {
    intent: 'flibbertigibbet',
  } })).body.result;
  expect(emptyPortfolio.isError).toBe(false);
  expect(emptyPortfolio.structuredContent).toMatchObject({
    selected_packs: [],
    routing_selection: { state: 'packs_required', selected_packs: [] },
    readiness: { state: 'packs_required', effect_authority: 'not_inferred' },
  });
});

test('Project product profile binds connect aperture to deterministic source identity', () => {
  const base = mkdtempSync(join(tmpdir(), 'bf-project-product-profile-'));
  const root = join(base, 'repo'), home = join(base, 'home'), fakeBun = join(base, 'bun');
  mkdirSync(home, { recursive: true });
  writeFileSync(fakeBun, 'fixture bun');
  for (const relative of PROJECT_MCP_SOURCE_SET) {
    const file = join(root, relative);
    mkdirSync(join(file, '..'), { recursive: true });
    writeFileSync(file, `fixture:${relative}\n`);
  }
  try {
    const first = compileProjectMcpLaunchAgent('connect', { root, home, bun: fakeBun });
    expect(first).toMatchObject({
      endpoint: 'http://127.0.0.1:3066',
      capability_pack: 'connect',
      deployment_state: 'candidate_only',
    });
    expect(first.source_identity.aggregate_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(first.source_identity.files).toHaveLength(PROJECT_MCP_SOURCE_SET.length);
    expect(first.launch_agent.EnvironmentVariables.BONFYRE_PROJECT_MCP_PACK).toBe('connect');
    expect(first.launch_agent.EnvironmentVariables.BONFYRE_PROJECT_MCP_SOURCE_SHA256).toBe(first.source_identity.aggregate_sha256);
    expect(first.launch_agent.EnvironmentVariables.BONFYRE_MCP_TOKEN_FILE).toBe(join(home, '.bonfyre/mcp/project-connect.token'));
    writeFileSync(join(root, PROJECT_MCP_SOURCE_SET[0]), 'changed candidate source\n');
    const second = compileProjectMcpLaunchAgent('connect', { root, home, bun: fakeBun });
    expect(second.source_identity.aggregate_sha256).not.toBe(first.source_identity.aggregate_sha256);
  } finally { rmSync(base, { recursive: true, force: true }); }
});

test('bearer, schema and unknown-name refusals happen before any owner invocation', async () => {
  const { calls, rpc } = await make();
  expect((await rpc('tools/call', { name: 'project_status' }, 'wrong')).status).toBe(401);
  for (const params of [
    { name: 'unknown' }, { name: 'project_status', arguments: { cwd: '/elsewhere' } },
    { name: 'project_search', arguments: { query: 12 } },
    { name: 'project_apply_patch', arguments: { patch: 'x' } },
    { name: 'project_diff', arguments: { staged: 'false' } },
    { name: 'project_test_profile', arguments: { profile: 'python-compile', paths: ['x.py'] } },
  ]) expect((await rpc('tools/call', params)).body.result.isError).toBe(true);
  expect(calls).toHaveLength(0);
});

test('resources bind to the same owners and prompts never execute tools', async () => {
  const { calls, rpc } = await make();
  const resources = (await rpc('resources/list')).body.result.resources;
  expect(resources).toHaveLength(11);
  expect(resources.map(resource => resource.uri)).toContain('ui://aurekai/project-control-plane/v1.html');
  expect(resources.map(resource => resource.uri)).toContain('bonfyre://mcp/service-profiles');
  expect(resources.map(resource => resource.uri)).toContain('bonfyre://mcp/products');
  expect(resources.map(resource => resource.uri)).toContain('bonfyre://mcp/runtime');
  expect((await rpc('resources/templates/list')).body.result.resourceTemplates).toHaveLength(5);
  for (const uri of ['bonfyre://atlas/summary', 'bonfyre://native/inventory', 'bonfyre://operator/status',
    'bonfyre://models/status', 'bonfyre://mcp/runtime', 'bonfyre://mcp/toolsets', 'bonfyre://estate/capabilities',
    'bonfyre://mcp/toolsets/account', 'bonfyre://mcp/snapshots/account', 'bonfyre://architecture/cmd-project', 'bonfyre://matrix/subject/example']) {
    const answer = await rpc('resources/read', { uri });
    expect(answer.body.result.contents[0].uri).toBe(uri);
    expect(answer.body.result.contents[0].mimeType).toBe('application/json');
  }
  expect(calls.map(c => c.name)).toEqual(['architecture_atlas', 'native_tool_inventory', 'operator_status',
    'bonfyre_moe_status', 'mcp_capability_lens', 'estate_capabilities', 'mcp_capability_lens', 'architecture_atlas', 'matrix_subject_constellation']);
  expect(calls.find(c => c.name === 'mcp_capability_lens' && c.args.pack === 'account')).toBeDefined();
  expect((await rpc('resources/read', { uri: 'bonfyre://mcp/toolsets/unknown' })).body.error).toBeDefined();
  const app = await rpc('resources/read', { uri: 'ui://aurekai/project-control-plane/v1.html' });
  expect(app.body).toHaveProperty('result.contents.0.mimeType', 'text/html;profile=mcp-app');
  expect(app.body.result.contents[0].text.startsWith('<!doctype html>')).toBe(true);
  expect(Buffer.byteLength(app.body.result.contents[0].text)).toBeLessThan(512 * 1024);
  const built = await buildControlPlaneApp({ stdout: true });
  expect(built.bytes).toBeLessThan(512 * 1024);
  expect(built.html).toContain('Refresh pack');
  expect(built.html).toContain('Plan objective');
  expect(built.html).toContain('mcp_product_plan');
  expect(built.html).toContain('mcp_portfolio_plan');
  expect(built.html).not.toContain('mcp_runtime_identity');
  for (const definition of CATALOG.prompts) {
    const arguments_ = Object.fromEntries(definition.arguments.map(a => [a.name, 'fixture']));
    if (definition.name === 'product_pack_workflow') arguments_.pack = 'build';
    const answer = await rpc('prompts/get', { name: definition.name, arguments: arguments_ });
    expect(answer.body.result.messages[0].role).toBe('user');
    if (definition.name === 'product_pack_workflow') {
      expect(answer.body.result.messages[0].content.text).toContain('mcp_product_plan');
      expect(answer.body.result.messages[0].content.text).toContain('snapshot readiness');
    }
  }
  const packCompletion = (await rpc('completion/complete', {
    ref: { type: 'ref/prompt', name: 'product_pack_workflow' },
    argument: { name: 'pack', value: '' },
  })).body.result.completion;
  expect(packCompletion.values).toEqual(['build', 'operate', 'work', 'model', 'commerce', 'evidence', 'connect', 'institution', 'account']);
  expect(packCompletion.values).not.toContain('workspace');
  const legacyPackPrompt = await rpc('prompts/get', {
    name: 'product_pack_workflow', arguments: { pack: 'workspace', objective: 'Continue a saved draft' },
  });
  expect(legacyPackPrompt.body.error).toBeDefined();
  expect(calls).toHaveLength(9);
  expect((await rpc('resources/read', { uri: 'file:///etc/passwd' })).body.error).toBeDefined();
});

test('HTTP workflow exposes batched source reads, conditional refresh and path preflight through existing tools', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-project-workflow-'));
  const root = join(home, 'repo'); mkdirSync(root);
  Bun.spawnSync(['git', 'init', '-q', root]);
  writeFileSync(join(root, 'owner.mjs'), 'export const value = 1;\n');
  writeFileSync(join(root, 'test.mjs'), 'test source\n');
  for (const relative of PROJECT_MCP_SOURCE_SET) {
    const file = join(root, relative);
    mkdirSync(join(file, '..'), { recursive: true });
    writeFileSync(file, `fixture:${relative}\n`);
  }
  // Retain executable lookup, not the host's grants, policy, credentials or stores.
  // This suite also runs inside the production MCP process's test profile.
  const env = {
    PATH: process.env.PATH, HOME: home, TMPDIR: home,
    BONFYRE_OPERATOR_HOME: join(home, 'operator'),
    BONFYRE_OPERATOR_POLICY: join(home, 'operator', 'policy.json'),
    BONFYRE_MCP_APPROVAL_FILE: join(home, 'fixture-approval.token'),
    BONFYRE_MCP_PROJECT_WRITE_ENABLED: '0', BONFYRE_MCP_AUTONOMOUS_WRITE_ENABLED: '0',
    BONFYRE_MCP_AUTO_COMMIT_ENABLED: '0',
  };
  const productByPort = new Map([
    ['3061', 'build'], ['3062', 'operate'], ['3063', 'model'], ['3064', 'commerce'],
    ['3065', 'account'], ['3066', 'connect'], ['3067', 'evidence'], ['3068', 'institution'],
    ['3069', 'work'],
  ]);
  let listenerMode = 'healthy', productHealthCalls = 0;
  const productRequest = async (url) => {
    productHealthCalls++;
    const target = new URL(String(url));
    const expectedPack = productByPort.get(target.port);
    if (target.hostname !== '127.0.0.1' || target.pathname !== '/health' || !expectedPack) throw new Error('unexpected fixture request');
    if (listenerMode === 'hang') return new Promise(() => {});
    return new Response(JSON.stringify({
      status: 'ok',
      service: 'bonfyre-project-mcp',
      runtime: 'bun',
      capability_pack: listenerMode === 'mismatch' ? 'wrong-product' : expectedPack,
      advertised_tools: 9,
      runtime_identity: { state: 'current', current_source_sha256: 'a'.repeat(64) },
    }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  const node = await createProjectNode({ root, home, token: 'fixture-token', env, request: productRequest });
  const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: node.handle });
  let id = 0;
  const rpcEnvelope = async (method, params) => await (await fetch(`${server.url}mcp`, { method: 'POST',
    headers: { authorization: 'Bearer fixture-token', 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }) })).json();
  const rpc = async (method, params) => (await rpcEnvelope(method, params)).result;
  try {
    const health = await (await fetch(`${server.url}health`)).json();
    expect(health).toMatchObject({
      write_enabled: false, autonomous_write_enabled: false, operator_paused: false,
    });
    const listed = await rpc('tools/list', {});
    expect(listed.tools.find(t => t.name === 'project_read').inputSchema.properties.files.maxItems).toBe(16);
    const lens = await rpc('tools/call', { name: 'mcp_capability_lens', arguments: { query: 'project', detail: 'summary', limit: 5 } });
    expect(lens.isError).toBe(false);
    expect(lens.structuredContent.performance.catalog_public_bytes).toBeGreaterThan(lens.structuredContent.performance.selected_payload_bytes);
    expect(lens.structuredContent.performance.payload_reduction_percent).toBeGreaterThan(0);
    expect(lens.structuredContent.performance.payload_reduction_percent).toBeLessThanOrEqual(100);
    const workspaceAlias = await rpc('tools/call', { name: 'mcp_capability_lens', arguments: { pack: 'workspace', limit: 5 } });
    expect(workspaceAlias.isError).toBe(true);
    const serviceProfiles = await rpc('resources/read', { uri: 'bonfyre://mcp/service-profiles' });
    const serviceProfileBody = JSON.parse(serviceProfiles.contents[0].text);
    expect(serviceProfileBody.deployment_state).toBe('candidate_only');
    expect(serviceProfileBody.source_identity.aggregate_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(Object.keys(serviceProfileBody.profiles)).toEqual(['build', 'operate', 'work', 'model', 'commerce', 'account', 'connect', 'evidence', 'institution']);
    expect(serviceProfileBody.contract_validation).toMatchObject({ ok: true, packs: 9, issues: [] });
    expect(serviceProfileBody.portfolio).toMatchObject({
      products: 9,
      local_snapshot_products: 6,
      continuation_required_products: 3,
      context_required_products: 0,
      deployment: { missing: 9 },
      listeners: { healthy: 9 },
      healthy_listeners: 9,
    });
    expect(serviceProfileBody.profiles.connect.listener).toMatchObject({
      state: 'healthy', service: 'bonfyre-project-mcp', runtime: 'bun', capability_pack: 'connect',
      advertised_tools: 9, runtime_identity_state: 'current',
    });
    expect(serviceProfileBody.profiles.work).toMatchObject({ endpoint: 'http://127.0.0.1:3069', capability_pack: 'work' });
    expect(serviceProfileBody.profiles.work.capability_summary).toMatchObject({
      context_mode: 'continuation-required',
      required_handoff_count: 1,
      required_handoffs: ['fabric_activity_read'],
    });
    expect(serviceProfileBody.profiles.connect.capability_summary).toMatchObject({
      context_mode: 'continuation-required',
      required_handoff_count: 2,
      required_handoffs: ['bonfyre_admin_credentials_status', 'bonfyre_admin_external_descriptors'],
    });
    expect(serviceProfileBody.profiles.evidence.capability_summary.context_mode).toBe('local-snapshot');
    expect(serviceProfileBody.profiles.connect).toMatchObject({ endpoint: 'http://127.0.0.1:3066', capability_pack: 'connect' });
    expect(serviceProfileBody.profiles.evidence).toMatchObject({ endpoint: 'http://127.0.0.1:3067', capability_pack: 'evidence' });
    expect(serviceProfileBody.profiles.institution).toMatchObject({ endpoint: 'http://127.0.0.1:3068', capability_pack: 'institution' });
    expect(serviceProfileBody.profiles.connect).not.toHaveProperty('source_identity');
    expect(serviceProfileBody.profiles.connect.source_identity_sha256).toBe(serviceProfileBody.source_identity.aggregate_sha256);
    expect(serviceProfileBody.profiles.connect.pack_contract_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(serviceProfileBody.profiles.connect.toolset_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(serviceProfileBody.profiles.connect.capability_cache_key).toBe(
      `${serviceProfileBody.profiles.connect.pack_contract_sha256}:${serviceProfileBody.profiles.connect.toolset_sha256}`);
    expect(serviceProfileBody.profiles.connect.release_cache_key).toBe(
      `${serviceProfileBody.source_identity.aggregate_sha256}:${serviceProfileBody.profiles.connect.pack_contract_sha256}:${serviceProfileBody.profiles.connect.toolset_sha256}`);
    expect(serviceProfileBody.profiles.connect.capability_summary.toolset_sha256).toBe(serviceProfileBody.profiles.connect.toolset_sha256);
    expect(serviceProfileBody.profiles.connect.capability_summary.pack_contract_sha256).toBe(serviceProfileBody.profiles.connect.pack_contract_sha256);
    expect(serviceProfileBody.profiles.connect.capability_summary.capability_cache_key).toBe(serviceProfileBody.profiles.connect.capability_cache_key);
    expect(serviceProfileBody.profiles.connect.client_cache_key).toBe(serviceProfileBody.profiles.connect.release_cache_key);
    expect(lens.structuredContent.available_packs.evidence.contract_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(lens.structuredContent.available_packs.evidence.contract_sha256)
      .not.toBe(lens.structuredContent.available_packs.connect.contract_sha256);
    expect(serviceProfileBody.profiles.connect.connection_descriptor).toMatchObject({
      schema_version: 'bonfyre.project-mcp.connection.v1',
      product: 'connect',
      transport: 'http-jsonrpc',
      mcp_url: 'http://127.0.0.1:3066/mcp',
      health_url: 'http://127.0.0.1:3066/health',
      authentication: { type: 'bearer-token-file', token_file: serviceProfileBody.profiles.connect.token_file },
      capability_pack: 'connect',
      source_identity_sha256: serviceProfileBody.source_identity.aggregate_sha256,
      pack_contract_sha256: serviceProfileBody.profiles.connect.pack_contract_sha256,
      toolset_sha256: serviceProfileBody.profiles.connect.toolset_sha256,
      capability_cache_key: serviceProfileBody.profiles.connect.capability_cache_key,
      release_cache_key: serviceProfileBody.profiles.connect.release_cache_key,
      client_cache_key: serviceProfileBody.profiles.connect.release_cache_key,
      app_resource_uri: 'ui://aurekai/project-control-plane/v1.html',
      entry_tool: 'mcp_product_plan',
      routing_mode: 'aperture-constrained-auto',
      pack_argument_required: false,
    });
    expect(serviceProfileBody.profiles.connect.deployment_preview).toMatchObject({
      state: 'missing', token_file_present: false, installed: null,
    });
    expect(serviceProfileBody.profiles.connect.deployment_preview.changes).toContain('launch_agent_missing');
    const afterPortfolioListenerCalls = productHealthCalls;
    const singleWork = JSON.parse((await rpc('resources/read', { uri: 'bonfyre://mcp/products/work' })).contents[0].text);
    expect(productHealthCalls - afterPortfolioListenerCalls).toBe(1);
    expect(singleWork).toMatchObject({
      schema_version: 'bonfyre.project-mcp.product-manifest.v1',
      product: 'work',
      source_identity_sha256: serviceProfileBody.source_identity.aggregate_sha256,
      contract_validation: { ok: true, packs: 9, issues: [] },
      profile: {
        endpoint: 'http://127.0.0.1:3069',
        capability_pack: 'work',
        listener: { state: 'healthy', capability_pack: 'work' },
        capability_summary: {
          context_mode: 'continuation-required',
          required_handoffs: ['fabric_activity_read'],
        },
      },
    });
    expect(singleWork.profile).not.toHaveProperty('source_identity');
    expect(singleWork.profile.release_cache_key).toBe(
      `${singleWork.source_identity_sha256}:${singleWork.profile.pack_contract_sha256}:${singleWork.profile.toolset_sha256}`);
    expect(JSON.stringify(singleWork).length).toBeLessThan(JSON.stringify(serviceProfileBody).length);
    expect((await rpcEnvelope('resources/read', { uri: 'bonfyre://mcp/products/unknown' })).error).toBeDefined();
    listenerMode = 'mismatch';
    const mismatchProfiles = JSON.parse((await rpc('resources/read', { uri: 'bonfyre://mcp/products' })).contents[0].text);
    expect(mismatchProfiles.portfolio.listeners).toEqual({ identity_mismatch: 9 });
    expect(mismatchProfiles.profiles.connect.listener).toMatchObject({
      state: 'identity_mismatch', capability_pack: 'wrong-product',
    });
    listenerMode = 'hang';
    const listenerStarted = performance.now();
    const timedOutProfiles = JSON.parse((await rpc('resources/read', { uri: 'bonfyre://mcp/products' })).contents[0].text);
    expect(timedOutProfiles.portfolio.listeners).toEqual({ timeout: 9 });
    expect(performance.now() - listenerStarted).toBeLessThan(1000);
    listenerMode = 'healthy';
    const connectProfile = serviceProfileBody.profiles.connect;
    mkdirSync(join(home, '.bonfyre/mcp'), { recursive: true });
    mkdirSync(join(home, 'Library/LaunchAgents'), { recursive: true });
    writeFileSync(connectProfile.token_file, 'fixture-secret-never-return');
    const connectPlist = `<?xml version="1.0"?><plist><dict>
<key>Label</key><string>${connectProfile.deployment_preview.candidate.label}</string>
<key>BONFYRE_PROJECT_MCP_PACK</key><string>${connectProfile.capability_pack}</string>
<key>BONFYRE_PROJECT_MCP_PORT</key><string>3066</string>
<key>BONFYRE_PROJECT_MCP_SOURCE_SHA256</key><string>${serviceProfileBody.source_identity.aggregate_sha256}</string>
<key>BONFYRE_MCP_TOKEN_FILE</key><string>${connectProfile.token_file}</string>
</dict></plist>`;
    writeFileSync(connectProfile.deployment_preview.plist_path, connectPlist);
    const currentProfiles = JSON.parse((await rpc('resources/read', { uri: 'bonfyre://mcp/service-profiles' })).contents[0].text);
    expect(currentProfiles.profiles.connect.deployment_preview).toMatchObject({ state: 'current', changes: [], token_file_present: true });
    expect(JSON.stringify(currentProfiles)).not.toContain('fixture-secret-never-return');
    writeFileSync(connectProfile.deployment_preview.plist_path,
      connectPlist.replace(serviceProfileBody.source_identity.aggregate_sha256, '0'.repeat(64)));
    const driftedProfiles = JSON.parse((await rpc('resources/read', { uri: 'bonfyre://mcp/service-profiles' })).contents[0].text);
    expect(driftedProfiles.profiles.connect.deployment_preview.state).toBe('drifted');
    expect(driftedProfiles.profiles.connect.deployment_preview.changes).toContain('source_sha256');
    expect(serviceProfileBody.profiles.connect.capability_summary.tools).toBeGreaterThan(0);
    expect(serviceProfileBody.profiles.connect.capability_summary.read_tools
      + serviceProfileBody.profiles.connect.capability_summary.effect_tools).toBe(serviceProfileBody.profiles.connect.capability_summary.tools);
    expect(serviceProfileBody.profiles.connect.capability_summary.model_visible_bytes).toBeGreaterThan(0);
    expect(serviceProfileBody.profiles.connect.capability_summary.model_visible_bytes)
      .toBeLessThan(serviceProfileBody.profiles.connect.capability_summary.full_admin_model_visible_bytes);
    expect(serviceProfileBody.profiles.connect.capability_summary.model_visible_bytes_avoided).toBe(
      serviceProfileBody.profiles.connect.capability_summary.full_admin_model_visible_bytes
      - serviceProfileBody.profiles.connect.capability_summary.model_visible_bytes);
    expect(serviceProfileBody.profiles.connect.capability_summary.context_reduction_percent).toBeGreaterThan(0);
    const economics = serviceProfileBody.portfolio.context_economics;
    expect(economics.smallest_product_model_visible_bytes).toBeLessThanOrEqual(economics.average_product_model_visible_bytes);
    expect(economics.average_product_model_visible_bytes).toBeLessThanOrEqual(economics.largest_product_model_visible_bytes);
    expect(economics.largest_product_model_visible_bytes).toBeLessThan(economics.full_admin_model_visible_bytes);
    expect(economics.best_context_reduction_percent).toBeGreaterThanOrEqual(economics.worst_context_reduction_percent);
    expect(economics.worst_context_reduction_percent).toBeGreaterThan(0);
    expect(Array.isArray(serviceProfileBody.profiles.connect.capability_summary.handoffs)).toBe(true);
    const batch = await rpc('tools/call', { name: 'project_read', arguments: { files: [{ path: 'owner.mjs' }, { path: 'test.mjs' }] } });
    expect(batch.isError).toBe(false);
    expect(batch.structuredContent.files).toHaveLength(2);
    const unchanged = await rpc('tools/call', { name: 'project_read', arguments: { files: batch.structuredContent.files.map(f => ({ path: f.path, if_sha256: f.sha256 })) } });
    expect(unchanged.structuredContent.files.every(f => f.not_modified && !f.lines)).toBe(true);
    const preflight = await rpc('tools/call', { name: 'project_patch_preview', arguments: { paths: ['owner.mjs', 'test.mjs'] } });
    expect(preflight.structuredContent).toMatchObject({ kind: 'path_preflight', manual_write_enabled: false, low_risk_paths_eligible: false });
    expect(preflight.structuredContent.metadata.files[0].sha256).toBe(batch.structuredContent.files[0].sha256);
    Bun.spawnSync(['git', '-C', root, 'add', '--all']);
    const committed = Bun.spawnSync(['git', '-C', root, '-c', 'core.hooksPath=/dev/null', '-c', 'user.name=Fixture',
      '-c', 'user.email=fixture@example.test', 'commit', '-qm', 'fixture']);
    expect(committed.exitCode).toBe(0);
    const renamed = join(root, 'renamed test.mjs');
    renameSync(join(root, 'test.mjs'), renamed);
    Bun.spawnSync(['git', '-C', root, 'add', '--all']);
    writeFileSync(renamed, 'changed after staged rename\n');
    writeFileSync(join(root, 'untracked only.txt'), 'untracked\n');
    const status = await rpc('tools/call', { name: 'project_status', arguments: {} });
    expect(status.isError).toBe(false);
    expect(status.structuredContent.clean).toBe(false);
    expect(status.structuredContent.performance).toEqual({
      path: 'porcelain-v2', git_processes: 1, fast_path: true, fallback_reason: null,
    });
    expect(status.structuredContent.staged.sample).toEqual([{ status: 'R100', path: 'test.mjs\trenamed test.mjs' }]);
    expect(status.structuredContent.working.sample).toEqual([{ status: 'M', path: 'renamed test.mjs' }]);
    expect(existsSync(join(home, 'operator'))).toBe(false);
    for (const args of [{}, { path: 'owner.mjs', files: [{ path: 'test.mjs' }] }, { files: [{ path: '../outside' }] }])
      expect((await rpc('tools/call', { name: 'project_read', arguments: args })).isError).toBe(true);
    const prompt = await rpc('prompts/get', { name: 'native_delivery_sprint', arguments: { outcome: 'Make source review faster' } });
    expect(prompt.messages[0].content.text).toContain('before implementing');
    expect(prompt.messages[0].content.text).toContain('if_sha256');
  } finally { server.stop(true); rmSync(home, { recursive: true, force: true }); }
});
test('initialization, notifications, health and unavailable local model stay distinct', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-project-transport-'));
  try {
    const { node, rpc } = await make({ home, env: {} });
    const initialized = (await rpc('initialize')).body.result;
    expect(initialized.protocolVersion).toBe('2025-06-18');
    expect(initialized.serverInfo.version).toBe('3.0.0');
    expect((await rpc('notifications/initialized')).status).toBe(202);
    const health = await (await node.handle(new Request('http://localhost/health'))).json();
    expect(health.runtime).toBe('bun');
    expect(health.write_enabled).toBe(false);
    expect(health.operator_paused).toBe(false);
    const unavailable = await make({ owners: { reads: async () => ({}), operator: async () => ({}),
      native: async () => { throw new Error('local model unavailable'); } } });
    const resource = (await unavailable.rpc('resources/read', { uri: 'bonfyre://models/status' })).body.result;
    expect(JSON.parse(resource.contents[0].text).status).toBe('unavailable');
  } finally { rmSync(home, { recursive: true, force: true }); }
});
