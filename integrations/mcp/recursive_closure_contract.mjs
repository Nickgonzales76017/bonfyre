/*
 * RecursiveMatrixClosure MCP contract.
 *
 * This module intentionally owns no institutional state. It is the regression
 * seam between the rich mesh gateway and the older project-node MCP: callers
 * inject owner reads and this compiler exposes the smallest explicit five-plane
 * frontier an agent may use without turning a joined read into authority.
 */

export const CLOSURE_PLANES = Object.freeze([
  'exact_semantic',
  'material_external',
  'latent_model',
  'execution',
  'economic',
]);

export const CLOSURE_RESOURCES = Object.freeze([
  {
    uri: 'bonfyre://closure/frontier',
    name: 'Recursive institutional closure frontier',
    description: 'Owner-backed Matrix, membrane, model, work and finance frontier. A preparation read, never effect authority.',
    mimeType: 'application/json',
  },
  {
    uri: 'bonfyre://closure/operating-contract',
    name: 'Recursive closure operating contract',
    description: 'The non-escalation law an MCP agent must preserve across observation, planning, work, money and effects.',
    mimeType: 'application/json',
  },
]);

export const CLOSURE_RESOURCE_TEMPLATES = Object.freeze([
  {
    uriTemplate: 'bonfyre://matrix/subject/{id}',
    name: 'Matrix subject constellation',
    description: 'Exact native addresses/presences for one semantic subject; never a copied object or authority grant.',
    mimeType: 'application/json',
  },
  {
    uriTemplate: 'bonfyre://matrix/closure/{host}/{format}',
    name: 'Recursive Matrix closure',
    description: 'Sparse RO-Crate or Zarr v3 cut for one controlled Matrix host.',
    mimeType: 'application/json',
  },
]);

export const CLOSURE_PROMPTS = Object.freeze([
  {
    name: 'institutional_circuit',
    description: 'Ground an institutional objective in Atlas, Matrix, evidence, Work, capability/provider and finance owners before action.',
    arguments: [
      { name: 'subject', description: 'Stable semantic subject or safe working id', required: true },
      { name: 'objective', description: 'Concrete outcome to prepare', required: true },
    ],
  },
  {
    name: 'evidence_bound_intake',
    description: 'Discover and observe a foreign source while keeping retained bytes, candidate meaning, proof and authority distinct.',
    arguments: [
      { name: 'url', description: 'Public HTTP(S) source', required: true },
      { name: 'need', description: 'Exact evidence/capability gap', required: true },
    ],
  },
  {
    name: 'recursive_closure_stress',
    description: 'Stress one real subject across exact, material, latent, execution, economic and carrier planes, then recurse only from admitted deltas.',
    arguments: [
      { name: 'subject', description: 'Exact subject', required: true },
      { name: 'objective', description: 'Outcome or stress scenario', required: true },
    ],
  },
  {
    name: 'native_delivery_sprint',
    description: 'Turn a product gap into a verified native change using current owners, receipts and narrow tests rather than a new compatibility layer.',
    arguments: [
      { name: 'objective', description: 'Observable outcome', required: true },
    ],
  },
]);

export const OPERATING_CONTRACT = Object.freeze({
  schema_version: 'bonfyre.recursive-closure.contract.v1',
  laws: [
    'A Matrix address or closure is not viewer, effect, provider, evidence, publication or financial authority.',
    'A retained foreign observation is not canonical truth, availability, an external account or a right to act.',
    'A CMS save is not publication, guest readability, Frappe execution, financial settlement or external effect.',
    'A budget, price or quote is not cash, committed capital, verified payment, settlement or delivery.',
    'A capability advertisement is not provider availability or agreement to perform it for this caller.',
    'Approximate/latent output may rank candidates but cannot satisfy an exact semantic, policy, authority or payment constraint.',
    'Human submit lines remain effect boundaries; an MCP write verb cannot cross them by implication.',
    'A derived closure may add only owner-receipted deltas and references; it never mints a replacement semantic identity.',
  ],
  recursion: [
    'exact semantic cut',
    'material/body references',
    'latent/model passports',
    'execution/work/provider/resource closure',
    'economic closure',
    'surface/carrier projection',
    'real occurrence/evidence delta',
    'recompile only affected closures',
  ],
});

function available(value) {
  if (value === undefined || value === null) return false;
  if (typeof value !== 'object') return true;
  return value.status !== 'unavailable' && !value.error;
}

export function compileClosureFrontier({
  subject = null,
  actor = null,
  matrix,
  atlas,
  membrane,
  models,
  work,
  finance,
  receipts = [],
} = {}) {
  const ownerReads = {
    exact_semantic: matrix ?? { status: 'unresolved', owner: 'BonfyreMatrix' },
    material_external: membrane ?? { status: 'unresolved', owner: 'MachineMembrane/ObjectFabric' },
    latent_model: models ?? { status: 'unresolved', owner: 'LatentCoordinateFabric/Model estate' },
    execution: work ?? { status: 'unresolved', owner: 'WorkGraph/Provider/Resource/Authority' },
    economic: finance ?? { status: 'unresolved', owner: 'FinancialMetabolism/MoneyGraph' },
  };
  const resolved = CLOSURE_PLANES.filter(name => available(ownerReads[name]));
  const unresolved = CLOSURE_PLANES.filter(name => !available(ownerReads[name]));

  return {
    schema_version: 'bonfyre.recursive-closure.frontier.v1',
    subject,
    actor,
    architecture: atlas ?? { status: 'unresolved', owner: 'ArchitectureAtlas' },
    planes: ownerReads,
    resolved_planes: resolved,
    unresolved_planes: unresolved,
    receipts: Array.isArray(receipts) ? receipts : [],
    operating_contract: OPERATING_CONTRACT,
    forbidden_inference: 'Joining owner reads into this frontier does not join their authority. Every write/effect must still be admitted by the owner of that transition.',
  };
}

export function promptMessage(name, args = {}) {
  const subject = String(args.subject || '').trim();
  const objective = String(args.objective || '').trim();
  if (name === 'institutional_circuit') {
    if (!subject || !objective) throw new Error('institutional_circuit requires subject and objective');
    return `Subject: ${subject}\nObjective: ${objective}\nRead Atlas and the exact Matrix closure first. Resolve material, latent/model, Work/provider/resource and economic planes only through their owners. Keep observed fact, proposal, authority blocker, capability demand, commitment, occurrence and economic consequence distinct. Prefer existing native organs and receipts. Preparation never implies outreach, publication, payment, settlement, human submit or foreign mutation.`;
  }
  if (name === 'evidence_bound_intake') {
    const url = String(args.url || '').trim();
    const need = String(args.need || '').trim();
    if (!/^https?:\/\//.test(url) || !need) throw new Error('evidence_bound_intake requires public http(s) url and need');
    return `Need: ${need}\nSource: ${url}\nDiscover the cheapest deterministic carrier first. Retain permitted bytes and a ForeignTwin receipt before parsing/model interpretation. Report observation, candidate meaning, missing proof and next lawful transition separately. Never promote the source to canonical truth, identity, payment state or effect authority.`;
  }
  if (name === 'recursive_closure_stress') {
    if (!subject || !objective) throw new Error('recursive_closure_stress requires subject and objective');
    return `Subject: ${subject}\nStress objective: ${objective}\nCompile one RecursiveMatrixClosure. Walk exact semantic -> material/body -> latent/model -> Work/provider/resource -> economic -> surface/carrier -> occurrence/evidence delta. Use reuse/index/latent/micro-model/deeper-model ordering before expensive cognition. Project protocols from one state rather than copying state into integrations. Recurse only from admitted deltas and preserve unresolved planes explicitly.`;
  }
  if (name === 'native_delivery_sprint') {
    if (!objective) throw new Error('native_delivery_sprint requires objective');
    return `Observable outcome: ${objective}\nConsult Atlas and native inventory before editing. Search/read actual owners and tests, compose existing Canon/Hash/Graph/Index/Stitch/Wire/DisCIPL/WorkGraph organs instead of minting a second owner, preview changes, run the narrowest verification, and retain receipt/revert coordinates. A passing patch is not deployment or product evidence until the relevant owner witness passes.`;
  }
  throw new Error(`unknown recursive closure prompt: ${name}`);
}

export function contractCoverage({ tools = [], resources = [], resourceTemplates = [], prompts = [] } = {}) {
  const names = new Set(tools.map(item => item?.name).filter(Boolean));
  const resourceUris = new Set(resources.map(item => item?.uri).filter(Boolean));
  const templates = new Set(resourceTemplates.map(item => item?.uriTemplate).filter(Boolean));
  const promptNames = new Set(prompts.map(item => item?.name).filter(Boolean));
  const requiredTools = [
    'architecture_atlas',
    'matrix_closure_compile',
    'matrix_subject_constellation',
  ];
  const requiredPrompts = CLOSURE_PROMPTS.map(item => item.name);
  return {
    required_tools: Object.fromEntries(requiredTools.map(name => [name, names.has(name)])),
    closure_frontier_resource: resourceUris.has('bonfyre://closure/frontier'),
    matrix_subject_template: templates.has('bonfyre://matrix/subject/{id}'),
    matrix_closure_template: templates.has('bonfyre://matrix/closure/{host}/{format}'),
    required_prompts: Object.fromEntries(requiredPrompts.map(name => [name, promptNames.has(name)])),
  };
}
