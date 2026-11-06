// Project exposes the existing delivery lifecycle without creating another queue.
import { readFileSync } from 'node:fs';
import { createProjectOwner } from './project_owner.mjs';

const names = ['fabric_status', 'implementation_relay', 'task_status', 'task_cancel', 'recent_receipts'];
const catalog = JSON.parse(readFileSync(new URL('./fabric_catalog.json', import.meta.url), 'utf8'));
export const FABRIC_PROJECT_TOOLS = names.map(name => {
  const tool = catalog.tools.find(tool => tool.name === name);
  if (!tool) throw new Error(`missing Fabric delivery operation: ${name}`);
  return { ...tool, 'x-bonfyre-owner': 'fabric' };
});

export function createProjectFabric(options = {}) {
  return createProjectOwner({ ...options, name: 'fabric', tools: FABRIC_PROJECT_TOOLS,
    urlEnv: 'BONFYRE_FABRIC_URL', tokenFileEnv: 'BONFYRE_FABRIC_TOKEN_FILE',
    defaultUrl: 'http://127.0.0.1:3053', stopExemptTools: ['task_cancel'] });
}
