// The opportunity desk's package: its code and four public requests for
// proposals as this estate retained them, reduced to their plain text, with
// the address, time and digest of each observation.
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { plainText } from './opportunity-desk.mjs';
const DOCS = { 'achievement-first-website-rfp': 'Achievement First: website redesign', 'stc-website-rfp': 'Southern Tier Central: social media and website', 'tjpdc-website-rfp': 'Thomas Jefferson PDC: website management', 'arc-wordpress-rfp804v2': 'Atlanta Regional Commission: WordPress support' };
export function opportunityFiles(packDir) {
  const hash = body => createHash('sha256').update(body).digest('hex'), texts = {}, sources = {};
  for (const [id, title] of Object.entries(DOCS)) {
    const body = readFileSync(join(packDir, id + '.html')), receipt = JSON.parse(readFileSync(join(packDir, id + '.receipt.json'), 'utf8'));
    if (receipt.content_sha256 !== hash(body)) throw Error(`${id} no longer matches the digest it was observed with.`);
    texts[id] = plainText(body.toString('utf8')); sources[id] = { title, url: receipt.source_url, observed_at: receipt.observed_at, sha256: receipt.content_sha256 };
  }
  const files = { 'sample-rfps.json': Buffer.from(JSON.stringify({ texts, sources })) };
  for (const name of ['opportunity-desk.mjs', 'opportunity-desk-client.mjs', 'opportunity-selection.mjs', 'opportunity-source-review.mjs', 'instrument-store.mjs', 'plan-network.mjs']) files[name] = readFileSync(join(import.meta.dir, name));
  files['instrument-local-store.mjs'] = readFileSync(join(import.meta.dir, '../../integrations/browser/extension/store.mjs'));
  const identity = Object.fromEntries(Object.keys(files).sort().map(name => [name, hash(files[name])]));
  const asset_prefix = `v-${hash(JSON.stringify(identity))}/`;
  return { sample: { documents: Object.keys(DOCS).length }, asset_prefix, files: Object.fromEntries(Object.entries(files).map(([name, body]) => [asset_prefix + name, body])) };
}
