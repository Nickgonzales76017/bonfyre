// What this does not prove:
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
import { test, expect, afterAll } from 'bun:test';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { workerSource } from '../lead-worker.mjs';
import { catalogueReviewForm, reviewRequest } from '../catalogue-review.mjs';
import { catalogueFiles } from '../catalogue-material.mjs';
import { prepareCatalogueRow } from '../catalogue-baseline.mjs';
import { baseline, makeDraft, restoreDraft } from '../catalogue-model.mjs';
import { preparationFields } from '../catalogue-preparation.mjs';
import { syncLeads } from '../leads.mjs';
import { onLead } from '../../../packs/aurekai-work-samples/site/lead.mjs';

const scratch = mkdtempSync(join(tmpdir(), 'catalogue-review-'));
afterAll(() => rmSync(scratch, { recursive: true, force: true }));
const repo = resolve(import.meta.dir, '../../..');
const input = JSON.parse(Buffer.from(Bun.gunzipSync(readFileSync(join(repo, 'packs/aurekai-work-samples/site/catalogue-input.json.gz')))).toString());
const p = input.rows.find(p => p.store === 'red3-display');
const form = 'catalogue-product-workspace', page = '/catalogue/';
const contract = catalogueReviewForm(catalogueFiles(input.rows, input.stores), page);
const draft = () => ({ schema: 'service-site.product-draft.v1', state: 'draft', product_id: p.id, store: p.store, source_url: p.source_url, source_sha256: p.source_sha256, observed_at: p.observed_at, saved_at: '2026-10-07T00:00:00Z', changes: [{ field: 'description', before: '', after: 'A review test, not product copy.' }] });
let serial = 0;
async function fixture() {
  const path = join(scratch, 'worker-' + serial++ + '.mjs');
  writeFileSync(path, workerSource('aurekai-work-samples', { [form]: contract }));
  const worker = await import(path), values = new Map();
  return { values, worker, env: { LEADS: { get: async key => values.get(key) ?? null, put: async (key, value) => values.set(key, value) }, ASSETS: { fetch: () => new Response('not found', { status: 404 }) } } };
}
const request = (body, origin = 'https://example.test', route = form) => new Request('https://example.test/api/lead/' + route, { method: 'POST', headers: { origin, 'content-type': 'application/json' }, body });
const body = value => reviewRequest(form, 'Integration test', 'test@example.test', value ?? draft(), page);

test('a saved product review returns one stable receipt across repeated submission', async () => {
  const { worker, env, values } = await fixture();
  const first = await worker.default.fetch(request(body()), env), one = await first.json();
  expect(first.status).toBe(201); expect(one.receipt).toMatch(/^lead:catalogue:[a-f0-9]{64}$/);
  const retry = await worker.default.fetch(request(body()), env);
  expect(retry.status).toBe(200); expect((await retry.json()).receipt).toBe(one.receipt);
  expect([...values.keys()].filter(k => k.startsWith('lead:'))).toHaveLength(1);
  expect([...values].filter(([k]) => k.startsWith('count:')).every(([, v]) => v === '1')).toBe(true);
});

test('invalid source identities, duplicate fields, cross-origin requests and excess bytes store nothing', async () => {
  const { worker, env, values } = await fixture();
  for (const value of [{ ...draft(), source_sha256: '0'.repeat(64) }, { ...draft(), product_id: '99999999999' }, { ...draft(), source_url: 'https://other.test/products/a/' }, { ...draft(), changes: [] }]) {
    expect((await worker.default.fetch(request(body(value)), env)).status).toBe(400);
  }
  const duplicate = JSON.parse(body()); duplicate.fields.push(duplicate.fields[0]);
  expect((await worker.default.fetch(request(JSON.stringify(duplicate)), env)).status).toBe(400);
  expect((await worker.default.fetch(request(body(), 'https://other.test'), env)).status).toBe(403);
  expect((await worker.default.fetch(request('x'.repeat(24001)), env)).status).toBe(413);
  expect((await worker.default.fetch(request(body(), 'https://example.test', 'missing'), env)).status).toBe(404);
  expect(values.size).toBe(0);
});

test('receipt is absent when storage fails, and retry safely retains the draft', async () => {
  const { worker, env, values } = await fixture();
  const missing = await worker.default.fetch(request(body()), {});
  expect(missing.status).toBe(503); expect((await missing.json()).receipt).toBeUndefined();
  const broken = { LEADS: { ...env.LEADS, put: async (key, value) => { if (key.startsWith('lead:')) throw new Error('interrupted write'); values.set(key, value); } } };
  await expect(worker.default.fetch(request(body()), broken)).rejects.toThrow('interrupted write');
  expect([...values.keys()].filter(k => k.startsWith('lead:'))).toHaveLength(0);
  expect((await worker.default.fetch(request(body()), env)).status).toBe(201);
});

test('intake refuses product addresses and options that native review cannot accept', async () => {
  const { worker, env, values } = await fixture();
  for (const value of [
    { ...draft(), source_url: p.source_url + '?different-product=1' },
    { ...draft(), changes: [{ field: 'sku:999999999999999999', before: '', after: 'FAKE' }] },
    { ...draft(), changes: [{ field: 'price:999999999999999999', before: '1.00', after: '2.00' }] },
    { ...draft(), changes: [{ field: 'sku:' + p.variants[0].id, before: '', after: 'x'.repeat(256) }] },
    { ...draft(), changes: [{ field: 'tags', before: '', after: 'x'.repeat(4001) }] }
  ]) {
    const response = await worker.default.fetch(request(body(value)), env);
    expect(response.status).toBe(400);
    expect((await response.json()).receipt).toBeUndefined();
  }
  expect(values.size).toBe(0);
});

test('a retained option change passes both generated intake and native review', async () => {
  const { worker, env, values } = await fixture();
  const value = { ...draft(), changes: [{ field: 'sku:' + p.variants[0].id, before: p.variants[0].sku ?? '', after: 'REVIEW-ONLY-SKU' }] };
  const response = await worker.default.fetch(request(body(value)), env);
  expect(response.status).toBe(201);
  const receipt = (await response.json()).receipt;
  const result = await onLead({ key: receipt, ...JSON.parse(values.get(receipt)) });
  expect(result.facts.product_id).toBe(p.id);
  expect(result.facts.merchant_published).toBe(false);
});

test('invented original values are rejected before storage and by the native lead hook', async () => {
  const { worker, env, values } = await fixture();
  for (const field of ['title', 'description', 'tags', 'sku:' + p.variants[0].id, 'price:' + p.variants[0].id]) {
    const value = { ...draft(), changes: [{ field, before: 'invented original', after: field.startsWith('price:') ? '20.00' : 'Proposed edit' }] };
    const response = await worker.default.fetch(request(body(value)), env);
    expect(response.status).toBe(400);
    expect((await response.json()).receipt).toBeUndefined();
    await expect(onLead({ key: 'test:forged', form, fields: [{ label: 'Product draft', value: JSON.stringify(value) }] })).rejects.toThrow('original');
  }
  expect(values.size).toBe(0);
});

test('a browser-shaped draft with a nonempty source description reaches native review', async () => {
  const { worker, env, values } = await fixture();
  const source = prepareCatalogueRow(input.rows.find(row => row.description_html?.length > 50));
  const original = baseline(source), edited = structuredClone(original);
  edited.description += '\nProposed café description.';
  edited.title += ' review'; edited.tags += ', review';
  edited.variants[0].sku = 'REVIEW'; edited.variants[0].price = '123.45';
  const value = makeDraft(source, original, edited);
  const response = await worker.default.fetch(request(body(value)), env);
  expect(response.status).toBe(201);
  const receipt = (await response.json()).receipt;
  const native = await onLead({ key: receipt, ...JSON.parse(values.get(receipt)) });
  expect(native.facts.original_fields_verified).toBe(true);
  expect(native.facts.change_count).toBe(5);
  expect(native.brief).toContain('Verified original:');
  expect(native.facts.changes_approved).toBe(false);
});

test('review travels through generated worker, existing lead sync, site hook and compiled CMS without truncation', async () => {
  const { worker, env, values } = await fixture();
  const large = draft(); large.changes[0].after = 'A long submitted description. '.repeat(300);
  large.changes.push(...preparationFields.map(({field}) => ({field, before: '', after: 'Proposed '+field})));
  const response = await worker.default.fetch(request(body(large)), env);
  expect(response.status).toBe(201);
  const receipt = (await response.json()).receipt;
  const schemas = join(repo, 'cmd/BonfyreCMS/content-types');
  const cms = args => {
    const out = Bun.spawnSync([process.env.BONFYRE_CMS_BINARY ?? join(homedir(), '.bonfyre/bin/bonfyre-cms'), ...args, '--db', join(scratch, 'roundtrip.db'), '--schemas', schemas], { stdout: 'pipe', stderr: 'pipe' });
    if (out.exitCode !== 0) throw new Error(out.stderr.toString());
    const data = JSON.parse(out.stdout.toString().trim() || '{}'); if (data.error) throw Error(data.error); return data;
  };
  for (const type of ['service_lead', 'outbound_message']) cms(['schema', 'apply', join(schemas, type + '.json')]);
  const sites = [{ site_id: 'aurekai-work-samples', pages_project: 'test', business: { name: 'Test operator' }, contact: { email: 'operator@example.test' } }];
  const heldLeads = async () => [{ key: receipt, ...JSON.parse(values.get(receipt)) }];
  const result = await syncLeads({ sites, cms, heldLeads, enrich: (_, lead) => onLead(lead) });
  expect(result.errors).toEqual([]); expect(result.prepared).toBe(1);
  const lead = cms(['entry', 'list', 'service_lead', '--ns', 'service-site']).data[0].attributes;
  expect(JSON.parse(JSON.parse(lead.fields_json).find(f => f.label === 'Product draft').value)).toEqual(large);
  expect(JSON.parse(lead.facts_json).intake_receipt).toBe(receipt);
  expect(JSON.parse(lead.facts_json).merchant_published).toBe(false);
  expect(JSON.parse(lead.facts_json).preparation_count).toBe(6);
  const message = cms(['entry', 'list', 'outbound_message', '--ns', 'service-site']).data[0].attributes;
  expect(message.state).toBe('composed'); expect(message.body).toContain('Proposed:');
  expect(message.subject_line).toContain(p.title); expect(message.body).not.toContain('service-site.product-draft.v1');
  expect((await syncLeads({ sites, cms, heldLeads, enrich: (_, lead) => onLead(lead) })).prepared).toBe(0);
});

test('preparation proposals restore and reach review without claiming merchant originals', async () => {
  const source = prepareCatalogueRow(p), original = baseline(source), edited = structuredClone(original);
  for (const {field} of preparationFields) edited[field] = 'Proposed '+field;
  const value = makeDraft(source, original, edited);
  expect(restoreDraft(source, original, value)).toEqual(edited);
  const {worker,env,values} = await fixture();
  const response = await worker.default.fetch(request(body(value)),env);
  expect(response.status).toBe(201);
  const receipt = (await response.json()).receipt;
  const native = await onLead({key:receipt,...JSON.parse(values.get(receipt))});
  expect(native.facts.original_fields_verified).toBe(false);
  expect(native.facts.preparation_count).toBe(6);
  expect(native.brief).not.toContain('Verified original:');
  for (const change of [
    {field:'prep:seo_title',before:'claimed merchant title',after:'Proposed'},
    {field:'prep:seo_title',before:'',after:'x'.repeat(256)},
    {field:'prep:unknown',before:'',after:'Proposed'},
    {field:'toString',before:'',after:'Proposed'}
  ]) {
    const invalid = {...value,changes:[change]};
    expect((await worker.default.fetch(request(body(invalid)),env)).status).toBe(400);
    expect(() => restoreDraft(source,original,invalid)).toThrow();
  }
});

// Receipt memory is local acknowledgement history, never a review or publication decision.
test('review version includes nested changes, source, form and saved revision', async () => {
 const {reviewVersion}=await import('../catalogue-review.mjs');
 const one=draft(),key=await reviewVersion(form,one);
 for(const change of [{...one,store:'other'},{...one,product_id:'9999'},{...one,source_sha256:'0'.repeat(64)},{...one,saved_at:'2026-10-08T00:00:00Z'},{...one,changes:[{...one.changes[0],after:'Different copy'}]},{...one,changes:[{...one.changes[0],before:'Different original'}]}])expect(await reviewVersion(form,change)).not.toBe(key);
 expect(await reviewVersion('different-form',one)).not.toBe(key);
 expect(await reviewVersion(form,JSON.parse(JSON.stringify(one)))).toBe(key);
 const two={...one,changes:[...one.changes,{field:'prep:questions',before:'',after:'Confirm dimensions'}]};
 expect(await reviewVersion(form,two)).toBe(await reviewVersion(form,{...two,changes:two.changes.toReversed()}));
});
test('local receipt validates its exact version and keeps no contact details', async () => {
 const {reviewVersion,localReviewReceipt,storedReviewReceipt}=await import('../catalogue-review.mjs');
 const version=await reviewVersion(form,draft()),reference='lead:catalogue:'+'a'.repeat(64),record=localReviewReceipt(version,reference,'2026-10-08T00:00:00Z');
 expect(storedReviewReceipt(version,record)).toEqual(record);
 for(const bad of [{...record,version:'f'.repeat(64)},{...record,reference:'approved'},{...record,recorded_at:'yesterday'},{...record,schema:'different'}])expect(storedReviewReceipt(version,bad)).toBeNull();
 expect(Object.keys(record).sort()).toEqual(['recorded_at','reference','schema','version']);
 expect(()=>localReviewReceipt(version,'fake')).toThrow('No valid review receipt');
});
