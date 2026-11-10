// The research desk's package: its code and one retained search, with a few
// decisions and quotations already made so the desk opens on work in progress.
// Each quotation is found in its record's abstract here; if a record's words
// ever differ, the build stops instead of shipping a quotation that is not there.
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { excerpt, verify } from './synthesis.mjs';

const WORKED = {
  claims: [['c1', 'Wellbeing improves after the move to four days'], ['c2', 'Productivity holds or rises'], ['c3', 'Results depend on how management runs the change']],
  decisions: { 'oa:W3190201265': { status: 'include', design: 'interviews or qualitative', n: 45 }, 'oa:W4411105170': { status: 'include', design: 'interviews or qualitative', n: 20 },
    'oa:W7108442760': { status: 'include', design: 'survey' }, 'oa:W4410222780': { status: 'include', design: 'longitudinal or panel', n: 200 }, 'oa:W66110143': { status: 'exclude', reason: 'Not about the question' } },
  quotes: [['oa:W4411105170', 'Individual and organisational well-being variables reported a positive impact.', 'c1', 'supports'],
    ['oa:W3190201265', 'the promise of a four-day week attracted employee favour and individualised benefits', 'c1', 'supports'],
    ['oa:W7108442760', 'the findings reveal no significant impact of working hours on employees’ happiness scores', 'c1', 'against'],
    ['oa:W3190201265', 'entrenched managerialist practices of performance measurement, monitoring and productivity pressures were intensified', 'c3', 'supports'],
    ['oa:W4410222780', 'organisations that fail to reduce stress are more likely to revert', 'c3', 'supports']],
};

export function synthesisFiles(samplePath) {
  const hash = body => createHash('sha256').update(body).digest('hex');
  const sample = JSON.parse(readFileSync(samplePath, 'utf8'));
  const excerpts = WORKED.quotes.map(([id, text, claim, direction]) => {
    const r = sample.records.find(x => x.id === id), at = r?.abstract.indexOf(text) ?? -1;
    if (at < 0) throw Error(`The worked example quotes ${id}, but its abstract does not contain: ${text}`);
    const e = excerpt(r, at, at + text.length, { claim, direction }); if (!verify(sample.records, e)) throw Error(`Quotation from ${id} does not match its source.`); return e; });
  for (const id of Object.keys(WORKED.decisions)) if (!sample.records.some(r => r.id === id)) throw Error(`The worked example screens ${id}, which is not in the retained search.`);
  const project = { question: sample.question, search: sample.search, retrieved: sample.retrieved, identified: sample.identified, duplicates: sample.duplicates, records: sample.records,
    decisions: WORKED.decisions, claims: WORKED.claims.map(([id, text]) => ({ id, text })), excerpts };
  const files = { 'sample-project.json': Buffer.from(JSON.stringify(project)) };
  for (const name of ['synthesis.mjs', 'synthesis-client.mjs', 'campaign-composer.mjs', 'campaign-composer-client.mjs', 'instrument-store.mjs']) files[name] = readFileSync(join(import.meta.dir, name));
  files['instrument-local-store.mjs'] = readFileSync(join(import.meta.dir, '../../integrations/browser/extension/store.mjs'));
  const identity = Object.fromEntries(Object.keys(files).sort().map(name => [name, hash(files[name])]));
  const asset_prefix = `v-${hash(JSON.stringify(identity))}/`;
  return { sample: { question: sample.question, records: sample.records.length, retrieved: sample.retrieved.map(({ url, fetched_at }) => ({ url, fetched_at })) }, asset_prefix,
    files: Object.fromEntries(Object.entries(files).map(([name, body]) => [asset_prefix + name, body])) };
}
