// The pricing desk's package: its code and the capabilities this estate's desk
// lists with no asking price, retained with what each address answered.
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
export function pricingFiles(samplePath) {
  const hash = body => createHash('sha256').update(body).digest('hex'), sample = JSON.parse(readFileSync(samplePath, 'utf8'));
  if (!sample.capabilities?.length || !sample.source?.sha256) throw Error('The retained desk snapshot is empty or has no receipt.');
  const files = { 'sample-capabilities.json': Buffer.from(JSON.stringify(sample)) };
  for (const name of ['pricing-desk.mjs', 'pricing-desk-client.mjs', 'instrument-store.mjs']) files[name] = readFileSync(join(import.meta.dir, name));
  files['instrument-local-store.mjs'] = readFileSync(join(import.meta.dir, '../../integrations/browser/extension/store.mjs'));
  const identity = Object.fromEntries(Object.keys(files).sort().map(name => [name, hash(files[name])]));
  const asset_prefix = `v-${hash(JSON.stringify(identity))}/`;
  return { sample: { capabilities: sample.capabilities.length, fetched_at: sample.source.fetched_at }, asset_prefix, files: Object.fromEntries(Object.entries(files).map(([name, body]) => [asset_prefix + name, body])) };
}
