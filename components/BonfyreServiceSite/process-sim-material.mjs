// The simulator's package: the engine, its worker and page code, and one real
// day of arrivals retained from NYC Open Data with the query that produced it.
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';

export function simulationFiles(tracePath) {
  const hash = body => createHash('sha256').update(body).digest('hex');
  const trace = JSON.parse(readFileSync(tracePath, 'utf8'));
  if (!trace.source?.response_sha256 || !Array.isArray(trace.minutes) || trace.minutes.length !== trace.source.rows) throw Error('The retained arrivals do not match their record.');
  const files = { 'sample-311.json': Buffer.from(JSON.stringify(trace)) };
  for (const name of ['process-sim.mjs', 'process-sim-worker.mjs', 'process-sim-client.mjs', 'instrument-store.mjs']) files[name] = readFileSync(join(import.meta.dir, name));
  files['instrument-local-store.mjs'] = readFileSync(join(import.meta.dir, '../../integrations/browser/extension/store.mjs'));
  const identity = Object.fromEntries(Object.keys(files).sort().map(name => [name, hash(files[name])]));
  const asset_prefix = `v-${hash(JSON.stringify(identity))}/`;
  const { minutes, ...about } = trace;
  return { sample: { ...about, arrivals: minutes.length }, asset_prefix, files: Object.fromEntries(Object.entries(files).map(([name, body]) => [asset_prefix + name, body])) };
}
