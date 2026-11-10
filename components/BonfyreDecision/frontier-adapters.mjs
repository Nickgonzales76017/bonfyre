// Exact-owner adapters: these only construct diagnostic cases, never execute tools or effects.
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { compileDecisionCase, decisionDigest, decisionPlanePolicy } from './decision-plane.mjs';

const sha = bytes => createHash('sha256').update(bytes).digest('hex');
function boundedChunks(value, maximumBytes) {
  const bytes = Buffer.from(value, 'utf8'), chunks = [];
  for (let start = 0; start < bytes.length;) {
    let end = Math.min(start + maximumBytes, bytes.length);
    while (end < bytes.length && (bytes[end] & 0xc0) === 0x80) end--;
    if (end <= start) throw new Error('decision state byte budget is too small for UTF-8');
    chunks.push(bytes.subarray(start, end).toString('utf8'));
    start = end;
  }
  return chunks;
}

export function contextPassageCases(manifest, materialRows, purpose, policy = decisionPlanePolicy()) {
  if (manifest?.schema !== 'bonfyre.context-cut.v1' ||
      !['ADMIT', 'SHADOW'].includes(manifest.rank_gate_state) ||
      !Array.isArray(manifest.selected_candidates) || !manifest.selected_candidates.length ||
      !manifest.context_sha256?.startsWith('sha256:') ||
      typeof purpose !== 'string' || !purpose.trim() || manifest.purpose !== purpose)
    throw new Error('a receipted ContextCut and explicit purpose are required');
  const byIdentity = new Map();
  for (const row of materialRows) {
    if (!row?.candidate_identity || byIdentity.has(row.candidate_identity))
      throw new Error('material manifest has missing or duplicate candidate identity');
    byIdentity.set(row.candidate_identity, row);
  }
  return manifest.selected_candidates.flatMap(candidate => {
    const material = byIdentity.get(candidate.identity);
    if (!material || material.receipt_digest !== candidate.receipt_digest ||
        material.information_policy_class !== manifest.information_policy_class ||
        `sha256:${sha(readFileSync(material.body_path))}` !== candidate.content_sha256)
      throw new Error(`ContextCut material receipt/body mismatch: ${candidate.identity}`);
    // Never ask a provider to judge a silently truncated body. Every chunk is
    // independently receipted; a body can only be omitted when all its chunks
    // pass the conservative omission test.
    const chunks = boundedChunks(readFileSync(material.body_path, 'utf8'),
      Math.floor(policy.max_state_bytes / 2));
    if (!chunks.length) throw new Error(`ContextCut material is empty: ${candidate.identity}`);
    return chunks.map((chunk, index) => {
      const id = chunks.length === 1 ? candidate.identity : `${candidate.identity}#${index + 1}/${chunks.length}`;
      return compileDecisionCase({ program: 'context_passage', owner: 'ContextCut',
        subject_id: `${manifest.context_sha256}:${id}`,
        mask_digest: decisionDigest({ frontier: manifest.frontier, candidate: candidate.identity,
          receipt: candidate.receipt_digest, body: candidate.content_sha256, purpose,
          chunk_index: index, chunk_count: chunks.length, chunk_sha256: sha(Buffer.from(chunk)) }),
        gate_state: manifest.rank_gate_state,
        information_policy_class: manifest.information_policy_class,
        frontier: [{ id, description: `${candidate.candidate_kind} chunk ${index + 1}/${chunks.length}`,
          owner_ref: candidate.owner_ref }],
        state: { purpose, passage: { id, source_candidate_id: candidate.identity,
          chunk_index: index, chunk_count: chunks.length, text: chunk,
          content_sha256: candidate.content_sha256 } } }, policy);
    });
  });
}

export function contextCasesFromManifest(manifest, policy = decisionPlanePolicy()) {
  const frontier = manifest?.decision_frontier;
  if (frontier?.schema !== 'bonfyre.decision-frontier.v1' ||
      frontier.state !== 'reconstructable' || frontier.owner !== 'ContextCut' ||
      typeof frontier.material_manifest !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(frontier.material_manifest_sha256 || ''))
    throw new Error('ContextCut has no reconstructable decision frontier');
  const bytes = readFileSync(frontier.material_manifest);
  if (`sha256:${sha(bytes)}` !== frontier.material_manifest_sha256)
    throw new Error('ContextCut decision material manifest changed');
  const recipe = { context: manifest.context_sha256, materials: frontier.material_manifest_sha256,
    purpose: manifest.purpose, candidates: manifest.selected_candidates.map(candidate =>
      [candidate.identity, candidate.content_sha256, candidate.receipt_digest]) };
  if (`sha256:${sha(Buffer.from(JSON.stringify(recipe)))}` !== frontier.recipe_sha256)
    throw new Error('ContextCut decision frontier recipe changed');
  const cases = contextPassageCases(manifest, bytes.toString('utf8').split(/\r?\n/)
    .filter(Boolean).map(line => JSON.parse(line)), manifest.purpose, policy);
  if (new Set(cases.map(item => item.state.passage.source_candidate_id)).size !== frontier.selected_bodies)
    throw new Error('ContextCut decision frontier is incomplete');
  return cases;
}

export function mcpToolCase(toolList, shortlist, intent, informationPolicyClass, policy = decisionPlanePolicy()) {
  const tools = toolList?.tools;
  if (!Array.isArray(tools) || !Array.isArray(shortlist) || !shortlist.length ||
      typeof intent !== 'string' || !intent.trim() ||
      typeof informationPolicyClass !== 'string' || !informationPolicyClass)
    throw new Error('MCP tool list, exact shortlist, intent, and information policy are required');
  const byName = new Map();
  for (const tool of tools) {
    if (typeof tool.name !== 'string' || byName.has(tool.name))
      throw new Error('MCP tool catalogue has duplicate or missing names');
    byName.set(tool.name, tool);
  }
  const frontier = shortlist.map(id => {
    const tool = byName.get(id);
    if (!tool || tool.annotations?.readOnlyHint !== true)
      throw new Error(`MCP shortlist is not an exact read-only tool: ${id}`);
    return { id, description: tool.description || id, owner_ref: `MCP:${id}` };
  });
  return compileDecisionCase({ program: 'tool_shortlist', owner: 'MCP',
    subject_id: `mcp-intent:sha256:${decisionDigest(intent)}`,
    mask_digest: decisionDigest({ catalogue: tools.map(tool => [tool.name, tool.description,
      tool.annotations?.readOnlyHint]), shortlist }), gate_state: 'ADMIT',
    information_policy_class: informationPolicyClass, frontier, state: { intent } }, policy);
}

export function flowWorkCase(work, candidateRoles, exactReadDigest, policy = decisionPlanePolicy()) {
  if (!work || typeof work !== 'object' || Array.isArray(work) ||
      typeof work.id !== 'string' || !work.id ||
      !/^[a-f0-9]{64}$/.test(exactReadDigest || ''))
    throw new Error('Flow work and exact read digest are required');
  return compileDecisionCase({ program: 'work_triage', owner: 'Flow', subject_id: work.id,
    mask_digest: exactReadDigest, gate_state: 'ADMIT',
    information_policy_class: work.information_policy_class,
    frontier: candidateRoles, state: { work } }, policy);
}

if (import.meta.main) {
  try {
    const [kind, input] = process.argv.slice(2);
    if (kind !== 'context' || !input)
      throw new Error('usage: frontier-adapters.mjs context <context-cut.json>');
    for (const item of contextCasesFromManifest(JSON.parse(readFileSync(input, 'utf8'))))
      console.log(JSON.stringify(item));
  } catch (error) { console.error(`decision frontier: ${error.message}`); process.exitCode = 2; }
}
