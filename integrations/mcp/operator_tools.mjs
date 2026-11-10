import { readFileSync } from 'node:fs';
import { CATALOG, validateArguments } from './project_node.mjs';

const richCatalog = JSON.parse(readFileSync(new URL('../../mcp/rich-catalog.json', import.meta.url), 'utf8'));
const matrixCatalog = JSON.parse(readFileSync(new URL('./matrix_tools.json', import.meta.url), 'utf8'));
const schema = (properties = {}, required = []) => ({ type: 'object', properties, required, additionalProperties: false });
// The recursive overlay declares these locally rather than in a JSON catalogue.
// Keep the trusted schemas here; never accept a remote tools/list schema as authority.
const localTools = [
  { name: 'recursive_closure_frontier', description: 'Compile a five-plane preparation frontier, without joined authority.', inputSchema: schema({ host: { type: 'string' }, format: { type: 'string', enum: ['ro-crate', 'zarr'] } }) },
  { name: 'foreign_resolution', description: 'Read a foreign observation, its history, or conflicts.', inputSchema: schema({ action: { type: 'string', enum: ['resolve', 'history', 'conflicts'] }, source_url: { type: 'string' } }, ['action']) },
  { name: 'surface_project', description: 'Read public surface projections from WebAddress.', inputSchema: schema({ action: { type: 'string', enum: ['catalogue', 'read'] }, pattern: { type: 'string' }, selector: { type: 'string' } }, ['action']) },
  { name: 'cms_record', description: 'Read or write this deployment CMS store; a save is not publication.', inputSchema: schema({ action: { type: 'string', enum: ['schemas', 'list', 'get', 'create', 'update', 'delete'] }, content_type: { type: 'string' }, namespace: { type: 'string' }, entry_id: { type: 'string' }, attributes: { type: 'object' }, limit: { type: 'integer' } }, ['action']) },
  { name: 'capability_circuit_compile', description: 'Compile a priced-capability demand into a non-authorizing route.', inputSchema: schema({ demand: { type: 'string' }, budget_usd: { type: 'number' } }, ['demand']) },
  { name: 'decision_tool_shortlist', description: 'Return a typed, non-effecting tool proposal; execution never invokes a suggested tool.', inputSchema: schema({ intent: { type: 'string' }, shortlist: { type: 'array', items: { type: 'string' } }, information_policy_class: { type: 'string' }, execute: { type: 'boolean' }, provider: { type: 'string', enum: ['local', 'clef-flash', 'clef', 'jev'] } }, ['intent', 'shortlist', 'information_policy_class']) },
];
const canonical = new Map([...richCatalog.tools, ...CATALOG.tools, ...matrixCatalog.tools, ...localTools].map((tool) => [tool.name, tool]));
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const READ_ONLY = new Set([
  'architecture_atlas', 'bonfyre_moe_status', 'matrix_address_resolve',
  'matrix_closure_compile', 'matrix_subject_constellation', 'native_tool_inventory',
  'operator_status', 'operator_backlog_audit', 'project_diff', 'project_patch_preview', 'project_read',
  'project_search', 'project_status', 'project_tree', 'mesh_status', 'project_recent_deltas',
  'studio_client_list', 'studio_fleming_query', 'membrane_state', 'institutional_measure',
  'model_local_inventory', 'model_local_generate', 'estate_query', 'finance_book',
  'protocol_fetch', 'desk_open_decisions', 'studio_membrane_status', 'runtime_get',
  'runtime_preview', 'oracle_service_status', 'oracle_command_describe',
  'oracle_command_probe', 'oracle_profile_receipts', 'task_status', 'artifact_search',
  'artifact_read', 'release_attest', 'mcp_call_history', 'recursive_closure_frontier',
  'foreign_resolution', 'surface_project', 'training_public_programs',
]);
const PREPARE = new Set([
  'bonfyre_moe_generate', 'bonfyre_quic_fpq_generate', 'foreign_twin_observe',
  'frappe_compile', 'institutional_prepare', 'project_test_profile',
  'foreign_discover', 'sitemap_delta', 'capability_circuit_compile',
  'decision_tool_shortlist', 'studio_fleming_watch', 'studio_fleming_opportunity',
]);
const DENIED_DIRECT = new Set([
  'native_tool_invoke', 'operator_goal_add', 'operator_pause', 'operator_resume',
  'project_apply_patch', 'project_commit_receipt', 'project_revert_receipt',
]);
const POWER_EFFECT = new Set(['oracle_profile_run', 'oracle_deep_flow', 'task_start']);
const CONTROLLED_EFFECT = new Map([
  ['finance_price_set', 'allow_financial_book_mutations'],
  ['studio_membrane_budget_set', 'allow_financial_book_mutations'],
  ['studio_economy_cost_record', 'allow_financial_book_mutations'],
  ['studio_work_claim', 'allow_work_queue_transitions'],
  ['studio_work_complete', 'allow_work_queue_transitions'],
  ['studio_client_register', 'allow_client_registration'], ['operator_goal_supersede', 'allow_operator_backlog_compaction'],
]);
const TEST_PROFILES = new Set([
  'git-diff-check', 'bonfyre-zig-build', 'bonfyre-final', 'cmd-make', 'training-mcp-smoke', 'cloudflare-bridge',
  'bun-syntax', 'shell-syntax', 'node-syntax', 'php-lint',
]);

const FINISH_TOOL = {
  type: 'function', name: 'operator_finish',
  description: 'Finish the current autonomous goal with exact disposition, evidence, and any new goals.',
  parameters: { type: 'object', properties: {
    disposition: { type: 'string', enum: ['resolved', 'blocked', 'deferred', 'retry'] },
    summary: { type: 'string' },
    evidence: { type: 'array', items: { type: 'string' } },
    new_goals: { type: 'array', maxItems: 16, items: { type: 'object', properties: {
      title: { type: 'string' }, details: { type: 'string' }, priority: { type: 'integer' }, dedupe_key: { type: 'string' },
    }, required: ['title'], additionalProperties: false } },
    retry_after_seconds: { type: 'integer', minimum: 0 },
  }, required: ['disposition', 'summary'], additionalProperties: false }, strict: false,
};
const ADD_GOAL_TOOL = {
  type: 'function', name: 'operator_add_goal',
  description: 'Add a durable follow-up goal to the autonomous queue.',
  parameters: { type: 'object', properties: {
    title: { type: 'string' }, details: { type: 'string' }, priority: { type: 'integer', minimum: 1, maximum: 100 }, dedupe_key: { type: 'string' },
  }, required: ['title'], additionalProperties: false }, strict: false,
};

const powerMode = (policy) => ['power', 'deploy'].includes(policy.mode);
function admission(name, args, policy) {
  if (!canonical.has(name)) return 'unknown tool';
  if (READ_ONLY.has(name)) return null;
  if (DENIED_DIRECT.has(name)) return 'tool requires a dedicated owner or operator boundary';
  if (name === 'project_apply_low_risk_patch') {
    if (!powerMode(policy) || policy.auto_apply_low_risk_patches !== true) return 'low-risk patch application is disabled by policy';
    return null;
  }
  if (CONTROLLED_EFFECT.has(name)) {
    return powerMode(policy) && policy[CONTROLLED_EFFECT.get(name)] === true ? null : 'material effect is not delegated by operator policy';
  }
  if (name === 'financial_membrane') {
    const action = args.action ?? 'status';
    if (['status', 'route'].includes(action)) return null;
    return powerMode(policy) && policy.allow_financial_book_mutations === true ? null : 'financial book mutation is not delegated';
  }
  if (name === 'cms_record') {
    if (['schemas', 'list', 'get'].includes(args.action)) return null;
    return powerMode(policy) && policy.allow_cms_record_mutations === true ? null : 'CMS mutation is not delegated';
  }
  if (name === 'capability_fulfillment') {
    if ((args.action ?? 'open_quote') === 'status') return null;
    return powerMode(policy) && policy.allow_capability_protocol_transitions === true ? null : 'capability protocol transition is not delegated';
  }
  if (name === 'oracle_deep_flow') {
    return powerMode(policy) && Array.isArray(policy.oracle_operator_flows) && policy.oracle_operator_flows.includes(args.flow) ? null : 'operator policy does not allow this flow';
  }
  if (name === 'task_start') {
    if (!powerMode(policy)) return 'task start requires power or deploy mode';
    if (args.kind === 'flow' && (!Array.isArray(policy.oracle_operator_flows) || !policy.oracle_operator_flows.includes(args.name))) return 'operator policy does not allow this flow';
    return args.kind === 'profile' || args.kind === 'flow' ? null : 'task kind must be profile or flow';
  }
  if (POWER_EFFECT.has(name)) return powerMode(policy) ? null : 'Oracle execution requires power or deploy mode';
  if (PREPARE.has(name)) {
    if (!powerMode(policy)) return 'preparation and tests require power or deploy mode';
    if (name === 'project_test_profile' && !TEST_PROFILES.has(args.profile)) return 'unknown test profile';
    return null;
  }
  return 'tool has no autonomous effect admission';
}

function toolResult(value) {
  if (object(value) && value.isError === true) return { value, failed: true };
  if (object(value) && Object.hasOwn(value, 'structuredContent')) return toolResult(value.structuredContent);
  if (object(value) && Array.isArray(value.content) && value.content.length === 1 && value.content[0]?.type === 'text') {
    try { return toolResult(JSON.parse(value.content[0].text)); } catch { return { value, failed: true }; }
  }
  if (!object(value)) return { value, failed: value == null };
  const directFailure = Boolean(value.error) || ['failed', 'error', 'timed_out', 'blocked', 'outcome_uncertain'].includes(value.state)
    || ['failed', 'error', 'timed_out', 'blocked', 'projection_failed'].includes(value.status)
    || (Array.isArray(value.errors) && value.errors.length > 0)
    || (Object.hasOwn(value, 'exit_code') && value.exit_code !== 0)
    || (Object.hasOwn(value, 'native_exit') && value.native_exit !== 0)
    || (Object.hasOwn(value, 'http_status') && (value.http_status < 200 || value.http_status >= 400))
    || (object(value.result) && ((Object.hasOwn(value.result, 'exit_code') && value.result.exit_code !== 0)
      || Boolean(value.result.error) || (Array.isArray(value.result.errors) && value.result.errors.length > 0)));
  return { value, failed: directFailure };
}

function checkProfile(profile, metadata) {
  if (!TEST_PROFILES.has(profile)) throw new Error(`unknown required test profile: ${profile}`);
  if (profile === 'cmd-make' && !/^Bonfyre[A-Za-z0-9_-]+$/.test(metadata.target || '')) throw new Error('invalid native command test target');
}

export function requiredTests(metadata, policy) {
  if (!object(metadata) || !Array.isArray(metadata.files)) throw new Error('patch preview lacks touched-file metadata');
  const paths = metadata.files.map((item) => item?.path);
  if (!paths.length || paths.some((file) => typeof file !== 'string' || !file || file.startsWith('/') || file.includes('..'))) throw new Error('patch preview has invalid touched paths');
  if (paths.some((file) => file.endsWith('.py'))) throw new Error('Python source changes have no retired operator test owner');
  const defaults = policy.default_tests ?? ['git-diff-check'];
  if (!Array.isArray(defaults) || defaults.some((profile) => typeof profile !== 'string')) throw new Error('default_tests must be a profile list');
  const tests = defaults.map((profile) => ({ profile }));
  const bunPaths = paths.filter((file) => /\.(?:[cm]?js|[cm]?ts|jsx|tsx)$/.test(file));
  if (bunPaths.length) tests.push({ profile: 'bun-syntax', paths: bunPaths, timeout_seconds: 300 });
  const shellPaths = paths.filter((file) => file.endsWith('.sh'));
  if (shellPaths.length) tests.push({ profile: 'shell-syntax', paths: shellPaths, timeout_seconds: 300 });
  if (paths.some((file) => file.startsWith('bonfyre-zig/'))) tests.push({ profile: 'bonfyre-zig-build', timeout_seconds: 1800 });
  const families = [...new Set(paths.filter((file) => file.startsWith('cmd/')).map((file) => file.split('/')[1]))].sort();
  for (const target of families) tests.push({ profile: 'cmd-make', target, timeout_seconds: 1200 });
  const seen = new Set();
  return tests.filter((test) => {
    checkProfile(test.profile, test);
    const key = JSON.stringify(test);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export function createOperatorTools({ mcpCall, listTools, recordAction, completeAction, now = () => Date.now() / 1000 }) {
  if (![mcpCall, listTools, recordAction, completeAction, now].every((part) => typeof part === 'function')) throw new TypeError('operator tools require callable MCP, catalog, durable action audit, and clock owners');
  let observed = null;
  async function catalog(policy) {
    if (!object(policy)) throw new Error('operator policy must be an object');
    const listed = await listTools();
    const items = Array.isArray(listed) ? listed : listed?.tools;
    if (!Array.isArray(items)) throw new Error('MCP tool list must be an array');
    observed = new Set();
    const tools = [], descriptions = Object.create(null);
    for (const item of items) {
      if (!object(item) || typeof item.name !== 'string' || observed.has(item.name)) continue;
      const owned = canonical.get(item.name);
      if (!owned) continue; // A model-facing tool declaration never creates a new owner.
      observed.add(item.name);
      const dynamicReadOrPreparation = ['financial_membrane', 'cms_record', 'capability_fulfillment'].includes(item.name);
      const flowAvailable = item.name === 'oracle_deep_flow' && powerMode(policy)
        && Array.isArray(policy.oracle_operator_flows) && policy.oracle_operator_flows.length > 0;
      const taskAvailable = item.name === 'task_start' && powerMode(policy);
      const profileAvailable = item.name === 'project_test_profile' && powerMode(policy);
      if (admission(item.name, {}, policy) && !dynamicReadOrPreparation && !flowAvailable && !taskAvailable && !profileAvailable) continue;
      tools.push({ type: 'function', name: item.name, description: owned.description || '', parameters: owned.inputSchema, strict: false });
      descriptions[item.name] = { ...owned, annotations: { ...owned.annotations, readOnlyHint: READ_ONLY.has(item.name) } };
    }
    tools.push(FINISH_TOOL, ADD_GOAL_TOOL);
    return { tools, catalog: descriptions };
  }
  async function execute(runId, name, args, policy) {
    const started = now();
    let invoked = false;
    let seq;
    try {
      seq = await recordAction({ run_id: runId, tool: name, args, state: 'started', result: null, started_at: started, policy });
      if (!Number.isSafeInteger(seq) || seq < 1) throw new Error('action ledger did not return a sequence');
    } catch { return { error: 'action audit unavailable; MCP effect refused' }; }
    const audit = async (state, result) => {
      try { await completeAction({ seq, state, result }); return result; }
      catch { return { error: 'action completion audit unavailable', outcome_uncertain: true }; }
    };
    try {
      if (!object(policy) || !object(args)) return audit('blocked', { error: 'invalid operator policy or tool arguments' });
      if (!observed) await catalog(policy);
      if (!observed.has(name) || !canonical.has(name)) return audit('blocked', { error: `unknown or unobserved MCP tool: ${name}` });
      validateArguments(args, canonical.get(name).inputSchema);
      const refused = admission(name, args, policy);
      if (refused) return audit('blocked', { error: refused });
      if (name === 'project_apply_low_risk_patch') {
        invoked = true; // The composite may cross its effect boundary before an unexpected throw.
        return await applyLowRisk(runId, name, args, policy, audit);
      }
      invoked = true;
      const { value, failed } = toolResult(await mcpCall(name, args));
      return audit(failed ? (READ_ONLY.has(name) ? 'failed' : 'outcome_uncertain') : 'passed', value);
    } catch (error) {
      return audit(invoked && !READ_ONLY.has(name) ? 'outcome_uncertain' : 'failed', { error: `${error?.name || 'Error'}: ${error?.message || error}` });
    }
  }

  async function applyLowRisk(runId, name, args, policy, audit) {
    const previewReceipt = toolResult(await mcpCall('project_patch_preview', { patch: args.patch }));
    const preview = previewReceipt.value;
    if (previewReceipt.failed || !object(preview) || preview.valid !== true) return audit('blocked', { applied: false, preview, reason: 'patch validation failed' });
    if (preview.low_risk_eligible !== true) return audit('blocked', { applied: false, preview, reason: 'low-risk policy rejected patch' });
    let tests;
    try { tests = requiredTests(preview.metadata, policy); }
    catch (error) { return audit('blocked', { applied: false, preview, reason: String(error.message || error) }); }
    let appliedReceipt;
    try { appliedReceipt = toolResult(await mcpCall(name, args)); }
    catch (error) { return audit('outcome_uncertain', { applied: null, verified: false, outcome_uncertain: true, preview, error: String(error.message || error) }); }
    const applied = appliedReceipt.value;
    if (appliedReceipt.failed || !object(applied) || applied.applied !== true) return audit('outcome_uncertain', { applied: false, preview, apply: applied, outcome_uncertain: true });
    const receiptId = applied.receipt_id;
    if (typeof receiptId !== 'string' || !/^patch-[0-9]+-[a-f0-9]{12}$/.test(receiptId)) return audit('outcome_uncertain', { applied: true, verified: false, preview, apply: applied, outcome_uncertain: true, reason: 'applied patch lacks a valid rollback receipt' });
    const testReceipts = [];
    let allPassed = true;
    for (const test of tests) {
      let item;
      try { item = toolResult(await mcpCall('project_test_profile', test)); }
      catch (error) { item = { value: { error: String(error.message || error) }, failed: true }; }
      testReceipts.push({ request: test, result: item.value });
      if (item.failed || !object(item.value) || !object(item.value.result) || item.value.result.exit_code !== 0) { allPassed = false; break; }
    }
    if (!allPassed) {
      let rollback;
      try { rollback = toolResult(await mcpCall('project_revert_receipt', { receipt_id: receiptId })); }
      catch (error) { rollback = { value: { error: String(error.message || error) }, failed: true }; }
      const reverted = !rollback.failed && rollback.value?.reverted === true;
      return audit(reverted ? 'reverted' : 'outcome_uncertain', { applied: !reverted, reverted, verified: false, receipt_id: receiptId, preview, tests: testReceipts, revert: rollback.value, ...(reverted ? {} : { requires_manual_recovery: true, outcome_uncertain: true }) });
    }
    const verified = { applied: true, verified: true, receipt_id: receiptId, preview, tests: testReceipts, apply: applied };
    if (policy.auto_commit === true) {
      let commit;
      try { commit = toolResult(await mcpCall('project_commit_receipt', { receipt_id: receiptId })); }
      catch (error) { commit = { value: { error: String(error.message || error) }, failed: true }; }
      verified.commit = commit.value;
      verified.committed = !commit.failed && commit.value?.committed === true;
      if (!verified.committed) return audit('outcome_uncertain', { ...verified, requires_manual_recovery: true, outcome_uncertain: true });
    }
    return audit('passed', verified);
  }
  return Object.freeze({ catalog, execute, requiredTests });
}
