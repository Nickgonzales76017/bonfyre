// Read-only local HTTP measurement. No model, patch, service restart or production mutation.
import { createProjectNode } from '../project_node.mjs';
import { randomUUID, createHash } from 'node:crypto';
import { homedir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { readFileSync } from 'node:fs';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const token = randomUUID();
const node = await createProjectNode({ root, home: homedir(), token });
const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: node.handle });
let id = 0;
async function call(name, args) {
  const start = performance.now();
  const response = await fetch(`${server.url}mcp`, { method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: ++id, method: 'tools/call', params: { name, arguments: args } }) });
  const bytes = await response.text(), result = JSON.parse(bytes).result;
  if (!response.ok || !result || result.isError) throw new Error(`measurement failed for ${name}`);
  return { bytes: Buffer.byteLength(bytes), ms: performance.now() - start, value: result.structuredContent };
}
try {
  const full = await call('native_tool_inventory', {});
  const summary = await call('native_tool_inventory', { detail: 'summary', limit: 20 });
  const targeted = await call('native_tool_inventory', { query: 'BonfyreCMS', detail: 'summary' });
  const paths = ['project_reads.mjs', 'project_native.mjs', 'project_operator.mjs', 'project_node.mjs',
    'tests/project_reads.test.mjs', 'tests/project_native.test.mjs', 'tests/project_operator.test.mjs', 'tests/project_node.test.mjs']
    .map(p => `integrations/mcp/${p}`);
  const files = paths.map(path => ({ path, start_line: 1, end_line: 100 }));
  const individual = [];
  for (const file of files) individual.push(await call('project_read', file));
  const batch = await call('project_read', { files });
  if (batch.value.files.some((file, i) => file.sha256 !== individual[i].value.sha256)) throw new Error('source changed during measurement');
  const unchanged = await call('project_read', { files: batch.value.files.map(file => ({ path: file.path, if_sha256: file.sha256 })) });
  if (!unchanged.value.files.every(file => file.not_modified)) throw new Error('source changed during refresh');
  const measure = ({ bytes, ms }) => ({ response_bytes: bytes, local_http_ms: Number(ms.toFixed(3)) });
  const sources = Object.fromEntries(paths.map(path => [path, createHash('sha256').update(readFileSync(`${root}${path}`)).digest('hex')]));
  console.log(JSON.stringify({ schema: 'bonfyre.mcp-workflow-measurement.v1', observed_at: new Date().toISOString(),
    scope: 'One local authenticated HTTP sample per operation; current implementations only. Timing is not a before/after latency benchmark.',
    inventory: { legacy_full: { ...measure(full), count: full.value.count }, first_summary_page: { ...measure(summary), returned: summary.value.returned,
      next_offset: summary.value.next_offset }, selected_family: { ...measure(targeted), matched: targeted.value.matched } },
    reads: { files: files.length, individual: { requests: files.length, response_bytes: individual.reduce((sum, x) => sum + x.bytes, 0) },
      batch: { requests: 1, ...measure(batch) }, unchanged_refresh: { requests: 1, ...measure(unchanged) } }, sources,
    cannot_infer: 'These measurements do not establish remote latency, deployment, complete chat call arguments, atomic multi-file snapshots or write authority.' }, null, 2));
} finally { server.stop(true); }
