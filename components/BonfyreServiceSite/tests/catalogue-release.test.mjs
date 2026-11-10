import { test, expect } from 'bun:test';
import { prepareCatalogueRelease, reviewCatalogueRelease, verifyCatalogueRelease, releaseChecks, catalogueRoomImpact } from '../catalogue-release.mjs';
import { catalogueFiles } from '../catalogue-material.mjs';
import { releaseFixture, fixtureCatalogue, product, material } from './catalogue-release-fixture.mjs';
import { readFileSync } from 'node:fs';
import { plannerFiles } from '../planner-material.mjs';
import { startingPlan } from '../planner-model.mjs';
import { baseline, makeDraft, draftKey } from '../catalogue-model.mjs';

const prepare = (f = releaseFixture()) => prepareCatalogueRelease({ ...f, id: 'release-test-001', title: 'October price review', createdAt: '2026-10-17T12:00:00Z' });
test('retained merchant product and option identities connect each room placement to the proposed price', async () => {
  const f = releaseFixture(), before = structuredClone(f), value = await prepare(f), room = value.impact.impacted[0];
  expect(room.items).toHaveLength(2); expect(room.proposed_cents).toBe(80000); expect(room.exceeds_budget).toBe(true);
  expect(room.delta_cents).toBe(80000 - 2 * fixtureCatalogue.products[0].price_cents);
  expect(value.entries[0].draft.source_url).toBe(product.source_url); expect(f).toEqual(before);
  expect(await verifyCatalogueRelease(JSON.parse(JSON.stringify(value)))).toEqual(value);
  expect(releaseChecks(value)).toHaveLength(2);
});
test('similar names, another merchant and another option do not create a price dependency', () => {
  const f = releaseFixture(), draft = f.records[0].value;
  for (const different of [{ ...draft, source_url: 'https://other.example/products/same' }, { ...draft, product_id: '99999999' },
    { ...draft, changes: draft.changes.map(c => ({ ...c, field: 'price:99999999' })) }]) {
    expect(catalogueRoomImpact([different], f.records.slice(1), f.catalogues).impacted).toEqual([]);
  }
});
test('unknown room collections and different product observations block review instead of inventing totals', async () => {
  const f = releaseFixture(), missing = await prepare({ ...f, catalogues: [] });
  expect(missing.impact.unresolved).toHaveLength(1);
  await expect(reviewCatalogueRelease(missing, missing, '')).rejects.toThrow('source checks');
  const draft = { ...f.records[0].value, source_sha256: 'b'.repeat(64) };
  const result = catalogueRoomImpact([draft], f.records.slice(1), f.catalogues).impacted[0];
  expect(result.proposed_cents).toBeNull(); expect(result.delta_cents).toBeNull(); expect(result.blockers).toHaveLength(1);
});
test('a changed room, new room, or revised product requires a new review; refresh removes the old annotation', async () => {
  const f = releaseFixture(), saved = await prepare(f);
  const reviewed = await reviewCatalogueRelease(saved, saved, 'Checked the affected fixture budget.');
  expect(reviewed.review.sha256).toBe(saved.sha256); expect(reviewed.publication).toBeNull();
  for (const change of ['room', 'product', 'new-room']) {
    const next = structuredClone(f);
    if (change === 'new-room') { const row = structuredClone(next.records[1]); row.value.plan.id = 'another-room-001'; row.key = `fixture-plan:planner-rooms:${row.value.plan.id}`; next.records.push(row); }
    else next.records[change === 'room' ? 1 : 0].revision++;
    const current = await prepare(next);
    await expect(reviewCatalogueRelease(reviewed, current, '')).rejects.toThrow('changed after staging');
    expect(current.state).toBe('staged'); expect(current.review).toBeNull();
  }
});
test('altered release content, duplicate dependencies and unvalidated source changes are refused', async () => {
  const f = releaseFixture(), saved = await prepare(f);
  const altered = structuredClone(saved); altered.entries[0].draft.changes[0].after = '1.00';
  await expect(verifyCatalogueRelease(altered)).rejects.toThrow('digest');
  const duplicate = structuredClone(saved); duplicate.entries.push(duplicate.entries[0]); expect(() => releaseChecks(duplicate)).toThrow();
  for (const mutate of [d => d.source_url = 'https://other.example/product', d => d.changes[0].before = '1.00', d => d.changes[0].field = 'price:999999']) {
    const next = structuredClone(f); mutate(next.records[0].value); await expect(prepare(next)).rejects.toThrow();
  }
});
test('catalogue packaging retains the fixture owner and release modules; altered fixture digests refuse', () => {
  expect(material.fixture_catalogues).toEqual([fixtureCatalogue]);
  for (const name of ['catalogue-release.mjs', 'catalogue-release-view.mjs', 'planner-model.mjs']) expect(material.files[name].length).toBeGreaterThan(100);
  expect(() => catalogueFiles([product], material.stores, { fixtureCatalogues: [{ ...fixtureCatalogue, currency: 'EUR' }] })).toThrow('digest');
});
test('the public easel shared by catalogue and room retains its different capture hashes and cannot finish review', async () => {
  const dir = new URL('../../../packs/aurekai-work-samples/site/',import.meta.url);
  const catalogue = plannerFiles(JSON.parse(readFileSync(new URL('fixture-planner.json',dir)))).catalogue;
  const input = JSON.parse(Buffer.from(Bun.gunzipSync(readFileSync(new URL('catalogue-input.json.gz',dir)))).toString());
  const packed = catalogueFiles(input.rows,input.stores);
  const products = Object.entries(packed.files).filter(([name]) => name.endsWith('.json.gz')).flatMap(([,body]) => JSON.parse(Buffer.from(Bun.gunzipSync(body)).toString()));
  const easel = products.find(p => p.store === 'red3-display'), fixture = catalogue.products.find(p => p.product_id === easel.id);
  expect(fixture.source_url).toBe(easel.source_url); expect(fixture.source_sha256).not.toBe(easel.source_sha256);
  const original = baseline(easel), edit = structuredClone(original); edit.variants.find(v=>v.id===fixture.variant_id).price='100.00';
  const plan = startingPlan(catalogue,'compact','retained-room-001'), key = draftKey(easel);
  const value = await prepareCatalogueRelease({title:'Retained source comparison',keys:[key],products:[easel],catalogues:[catalogue],records:[
    {key,revision:1,value:makeDraft(easel,original,edit)},
    {key:`fixture-plan:planner-red3-fixtures:${plan.id}`,revision:1,value:{plan,receipt:null}}
  ]});
  expect(value.impact.impacted).toHaveLength(1);expect(value.impact.impacted[0].proposed_cents).toBeNull();
  expect(value.impact.impacted[0].blockers).toContain('The product draft and room use different source observations.');
  await expect(reviewCatalogueRelease(value,value,'')).rejects.toThrow('source checks');
});

test('merchant alphanumeric product and variant IDs retain exact room identity',async()=>{
 const f=releaseFixture(),p=structuredClone(product),cat=structuredClone(fixtureCatalogue);p.id='W002337865';p.variants[0].id='W002337865';
 const original=baseline(p),edited=structuredClone(original);edited.variants[0].price='400.00';
 f.products=[p];f.keys=[draftKey(p)];f.records[0]={key:draftKey(p),revision:1,value:makeDraft(p,original,edited)};
 cat.products[0].product_id=p.id;cat.products[0].variant_id=p.variants[0].id;f.catalogues=[cat];
 const value=await prepare(f);expect(value.impact.impacted[0].items).toHaveLength(2);expect(value.impact.impacted[0].proposed_cents).toBe(80000);
 expect((await verifyCatalogueRelease(value)).entries[0].draft.product_id).toBe('W002337865');
 expect((await reviewCatalogueRelease(value,value,'Exact merchant identifiers checked')).state).toBe('reviewed');
 for(const bad of ['bad:identity','../other','x'.repeat(129)]){
  const changed=structuredClone(value);changed.entries[0].draft.product_id=bad;changed.entries[0].key='draft:catalogue:'+p.store+':'+bad;
  expect(()=>releaseChecks(changed)).toThrow('Invalid saved release');
 }
});
