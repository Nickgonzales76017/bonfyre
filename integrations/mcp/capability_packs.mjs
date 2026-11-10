import { createHash } from 'node:crypto';

const GLOBAL_PACK_TOOLS = Object.freeze(['mcp_runtime_identity', 'mcp_product_plan', 'mcp_portfolio_plan']);

export const CAPABILITY_PACKS = Object.freeze({
  build: {
    route_terms: ['build', 'source', 'code', 'repo', 'repository', 'project', 'architecture', 'implement', 'implementation', 'test', 'verify', 'verification', 'debug', 'bug', 'patch', 'diff'],
    description: 'Project inspection, architecture, native discovery, implementation, verification and task reconciliation.',
    prefixes: ['project_', 'architecture_', 'native_tool_', 'task_'],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'implementation_relay', 'fabric_status', 'recent_receipts', 'estate_capabilities', 'organ_drift_status', 'organ_drift_sync'],
    entry_reads: ['mcp_capability_lens', 'project_status', 'estate_capabilities', 'organ_drift_status'],
    snapshot_reads: [{ name: 'project_status', arguments: {} }, { name: 'estate_capabilities', arguments: {} }, { name: 'organ_drift_status', arguments: {} }],
    primary_effects: ['implementation_relay', 'organ_drift_sync'],
    outcome: 'Understand the checkout, choose an owner, make a bounded change, verify it, and reconcile receipts.',
  },
  operate: {
    route_terms: ['operate', 'operator', 'status', 'health', 'service', 'task', 'queue', 'live', 'drift', 'pause', 'stop', 'receipt', 'runtime', 'backlog', 'cleanup', 'compact', 'supersede'],
    description: 'Live operator, task, service, backlog hygiene and cumulative estate health without widening authority.',
    prefixes: ['operator_', 'task_'],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'project_status', 'fabric_status', 'spines_status', 'v6_status', 'recent_receipts', 'estate_capabilities', 'organ_drift_status', 'organ_drift_sync', 'operator_backlog_audit', 'operator_goal_supersede'],
    entry_reads: ['project_status', 'fabric_status', 'estate_capabilities', 'organ_drift_status', 'operator_backlog_audit'],
    snapshot_reads: [{ name: 'project_status', arguments: {} }, { name: 'fabric_status', arguments: {} }, { name: 'estate_capabilities', arguments: {} }, { name: 'organ_drift_status', arguments: {} }, { name: 'operator_backlog_audit', arguments: { state: 'queued', limit: 50 } }],
    primary_effects: ['operator_goal_add', 'operator_goal_supersede', 'organ_drift_sync'],
    outcome: 'See what is live, paused, drifting or backlogged; review exact never-started queued goals; retire obsolete work without deleting history.',
  },
  work: {
    route_terms: ['work', 'activity', 'output', 'outputs', 'inbox', 'subagent', 'subagents', 'background', 'processing', 'notification', 'notifications', 'recipient', 'continuation', 'push'],
    description: 'Current work, outputs and recipient attention with account-scoped continuation into the existing Activity/Inbox carrier.',
    prefixes: [],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'fabric_status', 'recent_receipts'],
    entry_reads: ['fabric_status', 'recent_receipts'],
    snapshot_reads: [{ name: 'fabric_status', arguments: {} }, { name: 'recent_receipts', arguments: { limit: 20 } }],
    primary_effects: [],
    handoffs: [
      { name: 'fabric_activity_read', carrier: 'account-activity', owner: 'AurekaiActivity/WebAddress+Queue+Fabric', kind: 'read', required: true },
      { name: 'notification', carrier: 'account-activity', owner: 'AurekaiActivity/Fabric', kind: 'effect', required: false },
      { name: 'model-review', carrier: 'account-activity', owner: 'AurekaiActivity/ModelCommons', kind: 'effect', required: false },
      { name: 'model-submit', carrier: 'account-activity', owner: 'AurekaiActivity/ModelCommons', kind: 'effect', required: false },
      { name: 'model-reconcile', carrier: 'account-activity', owner: 'AurekaiActivity/ModelCommons', kind: 'effect', required: false },
      { name: 'model-verify-transfer', carrier: 'account-activity', owner: 'AurekaiActivity/FabricOutputTransfer', kind: 'effect', required: false },
      { name: 'push-subscribe', carrier: 'account-activity', owner: 'AurekaiActivity/FabricPush', kind: 'effect', required: false },
      { name: 'push-unsubscribe', carrier: 'account-activity', owner: 'AurekaiActivity/FabricPush', kind: 'effect', required: false },
    ],
    outcome: 'Keep current work, subagent progress, retained outputs and recipient attention visible while preserving the exact account/destination boundary for private activity.',
  },
  model: {
    route_terms: ['model', 'generation', 'generate', 'inference', 'llm', 'moe', 'fpq', 'gpu', 'placement', 'token', 'kernel'],
    description: 'Local model inventory, generation, placement and bounded institutional preparation.',
    prefixes: ['bonfyre_moe_', 'bonfyre_quic_', 'r7_model_', 'mission_r7_'],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'institutional_prepare', 'estate_capabilities', 'native_tool_inventory', 'financial_membrane', 'financial_read'],
    entry_reads: ['estate_capabilities', 'bonfyre_moe_status', 'r7_model_inventory', 'financial_read'],
    snapshot_reads: [{ name: 'estate_capabilities', arguments: {} }, { name: 'bonfyre_moe_status', arguments: {} }, { name: 'financial_read', arguments: { action: 'status' } }],
    primary_effects: ['institutional_prepare', 'bonfyre_moe_generate', 'bonfyre_quic_fpq_generate'],
    outcome: 'Choose an admitted local model path with explicit availability, cost context, receipts, and bounded execution.',
  },
  commerce: {
    route_terms: ['commerce', 'commercial', 'bookkeeping', 'books', 'finance', 'financial', 'payment', 'price', 'pricing', 'quote', 'deal', 'manifest', 'invoice', 'cost'],
    description: 'Commercial position, bookkeeping, financial membranes, priced capability exchange and sourced intake.',
    prefixes: ['books_'],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'commercial_state', 'financial_membrane', 'financial_read', 'capability_fulfillment', 'manifest_lot', 'cms_record', 'cms_read', 'foreign_twin_release', 'recent_receipts'],
    entry_reads: ['commercial_state', 'financial_read', 'recent_receipts'],
    snapshot_reads: [{ name: 'commercial_state', arguments: { view: 'relationships' } }, { name: 'financial_read', arguments: { action: 'status' } }],
    primary_effects: ['capability_fulfillment', 'cms_record', 'books_book'],
    outcome: 'Move from sourced demand to priced, evidenced, reviewable work without confusing offers, replies, payments, or delivery.',
  },
  evidence: {
    route_terms: ['evidence', 'proof', 'provenance', 'verify', 'verification', 'receipt', 'receipts', 'claim', 'replay', 'parity', 'closure', 'trace', 'audit', 'witness'],
    description: 'Evidence intake, provenance, replay/parity verification, proof sealing and retained receipt inspection.',
    prefixes: ['r5_', 'r6_', 'r8_', 'spines_', 'foreign_twin_', 'matrix_'],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'recent_receipts', 'estate_recall', 'architecture_atlas'],
    entry_reads: ['recent_receipts', 'spines_status', 'estate_recall', 'architecture_atlas'],
    snapshot_reads: [{ name: 'recent_receipts', arguments: { limit: 20 } }, { name: 'spines_status', arguments: {} }],
    primary_effects: ['foreign_twin_observe', 'r6_crux_seal', 'r5_flux_append'],
    outcome: 'Move from an observed claim or retained artifact to explicit provenance, parity and proof state without mistaking proximity for evidence.',
  },
  connect: {
    route_terms: ['connect', 'connection', 'provider', 'credential', 'oauth', 'api', 'external', 'foreign', 'descriptor', 'address', 'integration'],
    description: 'External-provider and account connection planning across credentials, descriptors, addresses and lawful ForeignTwin observation.',
    prefixes: ['foreign_twin_', 'matrix_'],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'estate_capabilities'],
    entry_reads: ['estate_capabilities', 'matrix_address_resolve'],
    snapshot_reads: [{ name: 'estate_capabilities', arguments: {} }],
    primary_effects: ['foreign_twin_observe'],
    handoffs: [
      { name: 'bonfyre_admin_credentials_status', carrier: 'account-mcp', owner: 'CredentialBoundary/OAuth', kind: 'read', required: true },
      { name: 'bonfyre_admin_external_descriptors', carrier: 'account-mcp', owner: 'MachineMembrane/OAuth', kind: 'read', required: true },
      { name: 'workspace_list', carrier: 'account-mcp', owner: 'AccountWorkspaceMcp/OAuth', kind: 'read', required: false },
      { name: 'bonfyre_admin_credential_verify', carrier: 'account-mcp', owner: 'CredentialBoundary/OAuth', kind: 'effect', required: false },
    ],
    outcome: 'See what an external provider needs, what is already ready, where it is addressed, and what can be observed next without exposing secrets or widening authority.',
  },
  institution: {
    route_terms: ['institution', 'institutional', 'training', 'programme', 'program', 'frappe', 'cms', 'governance', 'participation', 'public', 'authority', 'prepare'],
    description: 'Institutional preparation, native CMS/Frappe projection, participation addressing and explicit admission boundaries.',
    prefixes: ['matrix_', 'training_', 'frappe_', 'cms_'],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'institutional_prepare', 'estate_capabilities', 'financial_read', 'recent_receipts'],
    entry_reads: ['estate_capabilities', 'financial_read', 'training_public_programs'],
    snapshot_reads: [{ name: 'estate_capabilities', arguments: {} }, { name: 'financial_read', arguments: { action: 'status' } }],
    primary_effects: ['institutional_prepare', 'frappe_compile', 'cms_record'],
    handoffs: [
      { name: 'bonfyre_admin_status', carrier: 'account-mcp', owner: 'BonfyreAuth/OAuth', kind: 'read', required: false },
      { name: 'bonfyre_admin_institution_grant', carrier: 'account-mcp', owner: 'BonfyreAuth/OAuth', kind: 'effect', required: false },
      { name: 'bonfyre_admin_institution_revoke', carrier: 'account-mcp', owner: 'BonfyreAuth/OAuth', kind: 'effect', required: false },
    ],
    outcome: 'Prepare and operate institution-shaped work while keeping admission, publication, finance and external effects independently owned.',
  },
  account: {
    route_terms: ['account', 'workspace', 'draft', 'editor', 'revision', 'destination', 'saved', 'profile', 'user', 'organization', 'org'],
    description: 'Account and destination context plus handoff metadata; authenticated saved-draft operations remain owned by AccountWorkspaceMcp/OAuth.',
    prefixes: [],
    names: ['mcp_capability_lens', 'mcp_read_bundle', 'mcp_pack_snapshot', 'cms_record', 'matrix_address_resolve', 'matrix_subject_constellation'],
    entry_reads: ['matrix_address_resolve'],
    snapshot_reads: [],
    primary_effects: [],
    handoffs: [
      { name: 'workspace_list', carrier: 'account-mcp', owner: 'AccountWorkspaceMcp/OAuth', kind: 'read', required: true },
      { name: 'bonfyre_admin_status', carrier: 'account-mcp', owner: 'BonfyreAuth/OAuth', kind: 'read', required: true },
      { name: 'workspace_preview', carrier: 'account-mcp', owner: 'AccountWorkspaceMcp/OAuth', kind: 'effect', required: false },
      { name: 'workspace_update', carrier: 'account-mcp', owner: 'AccountWorkspaceMcp/OAuth', kind: 'effect', required: false },
    ],
    outcome: 'Continue the same account-owned saved work in the right editor while preserving destination, revision, and authority.',
  },
});

export const CAPABILITY_PACK_NAMES = Object.freeze(Object.keys(CAPABILITY_PACKS));

const PACK_SCHEMA_TOOLS = new Set(['mcp_capability_lens', 'mcp_pack_snapshot', 'mcp_product_plan', 'mcp_portfolio_plan']);

export function bindCapabilityPackSchemas(catalogTools = []) {
  if (!Array.isArray(catalogTools)) throw new Error('catalogTools must be an array');
  return catalogTools.map((tool) => {
    if (!PACK_SCHEMA_TOOLS.has(tool?.name)) return tool;
    if (tool.name === 'mcp_portfolio_plan' && tool.inputSchema?.properties?.packs?.items) {
      return {
        ...tool,
        inputSchema: {
          ...tool.inputSchema,
          properties: {
            ...tool.inputSchema.properties,
            packs: {
              ...tool.inputSchema.properties.packs,
              items: { ...tool.inputSchema.properties.packs.items, enum: [...CAPABILITY_PACK_NAMES] },
            },
          },
        },
      };
    }
    if (!tool.inputSchema?.properties?.pack) return tool;
    const values = tool.name === 'mcp_product_plan'
      ? ['auto', ...CAPABILITY_PACK_NAMES]
      : [...CAPABILITY_PACK_NAMES];
    return {
      ...tool,
      inputSchema: {
        ...tool.inputSchema,
        properties: {
          ...tool.inputSchema.properties,
          pack: { ...tool.inputSchema.properties.pack, enum: values },
        },
      },
    };
  });
}

export function capabilityPackAllows(toolOrName, pack) {
  if (!pack) return true;
  const definition = CAPABILITY_PACKS[pack];
  if (!definition) return false;
  const name = typeof toolOrName === 'string' ? toolOrName : toolOrName?.name;
  return typeof name === 'string' && (GLOBAL_PACK_TOOLS.includes(name)
    || definition.names.includes(name)
    || definition.prefixes.some((prefix) => name.startsWith(prefix)));
}

export function validateCapabilityPacks(catalogTools = []) {
  if (!Array.isArray(catalogTools)) throw new Error('catalogTools must be an array');
  const byName = new Map(catalogTools.map((tool) => [tool.name, tool]));
  const issues = [];
  const check = (pack, role, name, requireReadOnly = false) => {
    const tool = byName.get(name);
    if (!tool) {
      issues.push({ pack, role, name, issue: 'missing_tool' });
      return;
    }
    if (!capabilityPackAllows(tool, pack)) {
      issues.push({ pack, role, name, issue: 'not_reachable_in_pack' });
      return;
    }
    if (requireReadOnly && tool.annotations?.readOnlyHint !== true) {
      issues.push({ pack, role, name, issue: 'entry_read_is_not_read_only' });
    }
  };
  for (const pack of CAPABILITY_PACK_NAMES) {
    const definition = CAPABILITY_PACKS[pack];
    for (const name of definition.entry_reads || []) check(pack, 'entry_read', name, true);
    for (const item of definition.snapshot_reads || []) check(pack, 'snapshot_read', item.name, true);
    for (const name of definition.primary_effects || []) check(pack, 'primary_effect', name, false);
    for (const handoff of definition.handoffs || []) {
      if (!handoff || typeof handoff.name !== 'string' || !handoff.name
        || typeof handoff.carrier !== 'string' || !handoff.carrier
        || typeof handoff.owner !== 'string' || !handoff.owner
        || !['read', 'effect'].includes(handoff.kind)
        || typeof handoff.required !== 'boolean') {
        issues.push({ pack, role: 'handoff', name: handoff?.name || null, issue: 'invalid_handoff' });
      }
    }
  }
  return {
    schema_version: 'bonfyre.mcp.capability-pack-validation.v1',
    ok: issues.length === 0,
    packs: CAPABILITY_PACK_NAMES.length,
    tools: byName.size,
    issues,
  };
}

const ROUTE_TERM_RE = /[a-z0-9]+/g;

function routingTerms(value) {
  return [...new Set(String(value || '').toLowerCase().match(ROUTE_TERM_RE) || [])];
}

export function rankCapabilityPacks(intent) {
  const terms = routingTerms(intent);
  const ranked = CAPABILITY_PACK_NAMES.map((pack, order) => {
    const definition = CAPABILITY_PACKS[pack];
    const declared = new Set(definition.route_terms || []);
    const matched_terms = terms.filter(term => declared.has(term));
    const exact_pack_name = terms.includes(pack);
    const score = matched_terms.length + (exact_pack_name ? 3 : 0);
    return { pack, score, matched_terms, exact_pack_name, order };
  }).sort((left, right) => right.score - left.score || left.order - right.order);
  return ranked.map(({ order: _order, ...item }) => item);
}

function stable(value) {
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stable(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function packContractBody(pack) {
  if (!Object.hasOwn(CAPABILITY_PACKS, pack)) return null;
  const value = CAPABILITY_PACKS[pack];
  return {
    schema_version: 'bonfyre.mcp.capability-pack-contract.v1',
    id: pack,
    description: value.description,
    route_terms: [...(value.route_terms || [])],
    prefixes: [...value.prefixes],
    names: [...new Set([...GLOBAL_PACK_TOOLS, ...value.names])],
    entry_reads: [...(value.entry_reads || [])],
    snapshot_reads: (value.snapshot_reads || []).map((item) => ({ name: item.name, arguments: { ...(item.arguments || {}) } })),
    primary_effects: [...(value.primary_effects || [])],
    handoffs: (value.handoffs || []).map((item) => ({ ...item })),
    outcome: value.outcome || null,
  };
}

export function capabilityPackContractSha256(pack) {
  const body = packContractBody(pack);
  return body ? createHash('sha256').update(stable(body)).digest('hex') : null;
}

export function capabilityPack(pack) {
  const body = packContractBody(pack);
  return body ? { ...body, contract_sha256: createHash('sha256').update(stable(body)).digest('hex') } : null;
}
