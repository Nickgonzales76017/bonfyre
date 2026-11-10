import { V6_TOOLS } from './v6_catalog.mjs';
import { createProjectOwner } from './project_owner.mjs';

export const V6_PROJECT_TOOLS = V6_TOOLS.map(tool => ({ ...tool, 'x-bonfyre-owner': 'v6' }));

export function createProjectV6(options = {}) {
  return createProjectOwner({ ...options, name: 'v6', tools: V6_PROJECT_TOOLS,
    urlEnv: 'BONFYRE_V6_URL', tokenFileEnv: 'BONFYRE_V6_TOKEN_FILE', defaultUrl: 'http://127.0.0.1:3058' });
}
