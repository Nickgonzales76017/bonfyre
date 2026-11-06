// Catalog-bound Project -> owner bridge. Deployment chooses the endpoint and
// dedicated remote credential; caller arguments cannot change either.
import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createOwnedMcpCall, mcpEndpoint } from './owned_mcp_client.mjs';

export function createProjectOwner({ name, tools, urlEnv, tokenFileEnv, defaultUrl,
  home = homedir(), env = process.env, token, request = fetch, stopExemptTools = [] } = {}) {
  if (typeof name !== 'string' || !name || !Array.isArray(tools) || !tools.length ||
      !tools.every(tool => tool?.['x-bonfyre-owner'] === name && typeof tool.name === 'string') ||
      typeof urlEnv !== 'string' || typeof tokenFileEnv !== 'string' || typeof defaultUrl !== 'string')
    throw new Error('invalid Project owner binding');
  const definitions = new Map(tools.map(tool => [tool.name, tool]));
  if (definitions.size !== tools.length) throw new Error('duplicate Project owner operation');
  if (!Array.isArray(stopExemptTools) || stopExemptTools.some(operation => !definitions.has(operation)))
    throw new Error('invalid Project STOP exception');
  const stopExempt = new Set(stopExemptTools);
  let ownerCall;
  return async (operation, args) => {
    const definition = definitions.get(operation);
    if (!definition) throw new Error(`unbound ${name === 'spines' ? 'Spines' : name.toUpperCase()} operation`);
    if (definition.annotations?.readOnlyHint !== true) {
      if (env.BONFYRE_MCP_PROJECT_WRITE_ENABLED !== '1') throw new Error('project write gate disabled');
      if (!stopExempt.has(operation) && existsSync(join(env.BONFYRE_OPERATOR_HOME || join(home, '.bonfyre/operator'), 'STOP')))
        throw new Error(`STOP active; ${name === 'spines' ? 'Spines' : name.toUpperCase()} dispatch refused`);
    }
    if (!ownerCall) {
      const endpoint = mcpEndpoint(`${(env[urlEnv] || defaultUrl).replace(/\/$/, '')}/mcp`);
      if (new URL(endpoint).protocol === 'https:' && !env[tokenFileEnv])
        throw new Error(`remote ${name === 'spines' ? 'Spines' : name.toUpperCase()} requires its own configured credential file`);
      const credential = env[tokenFileEnv] ? readFileSync(env[tokenFileEnv], 'utf8').trim() : token;
      ownerCall = createOwnedMcpCall({ endpoint, token: credential, tools: [...definitions.keys()], request });
    }
    return await ownerCall(operation, args);
  };
}
