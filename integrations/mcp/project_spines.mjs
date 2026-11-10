// One catalog/semantic owner across machines. This bridge grants no placement,
// tenant, or effect authority; it only binds the administrative Project aperture
// to the configured Spines owner. Public/self/institution apertures stay closed.
import { readFileSync } from 'node:fs';
import { createProjectOwner } from './project_owner.mjs';

const catalog = JSON.parse(readFileSync(new URL('./spines_catalog.json', import.meta.url), 'utf8'));
const proofWritingReads = new Set(['spines_verify', 'r5_affected_closure', 'r5_parity_verify']);
export const SPINES_PROJECT_TOOLS = catalog.tools.map(tool => ({ ...tool,
  annotations: { ...tool.annotations, ...(proofWritingReads.has(tool.name) ? { readOnlyHint: false } : {}) },
  'x-bonfyre-owner': 'spines' }));
export function createProjectSpines(options = {}) {
  return createProjectOwner({ ...options, name: 'spines', tools: SPINES_PROJECT_TOOLS,
    urlEnv: 'BONFYRE_SPINES_URL', tokenFileEnv: 'BONFYRE_SPINES_TOKEN_FILE', defaultUrl: 'http://127.0.0.1:3057' });
}
