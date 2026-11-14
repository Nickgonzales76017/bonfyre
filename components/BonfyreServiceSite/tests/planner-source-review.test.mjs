import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {plannerFiles} from '../planner-material.mjs';
import {startingPlan,validatePlan} from '../planner-model.mjs';
import {retainedFixtureText,selectPlannerPassage,adoptPlannerPassage} from '../planner-source-view.mjs';
import {verifyFixtureCatalogue} from '../planner-catalogue.mjs';

const input=JSON.parse(readFileSync('packs/aurekai-work-samples/site/fixture-planner.json','utf8'));
const packed=plannerFiles(input),catalogue=packed.catalogue;
const plan=()=>startingPlan(catalogue,'compact','planner-review-001');
const product=catalogue.products[0],body=retainedFixtureText(product);
const at=body.indexOf('Source URL: ');
const choice=p=>selectPlannerPassage(p,catalogue,product.id,at,at+33);

test('planner packages its real source review, shared selection owner and digest-bound catalogue',async()=>{
  await verifyFixtureCatalogue(catalogue);
  for(const filename of ['planner-source-view.mjs','planner-source-review.mjs','opportunity-selection.mjs','planner-catalogue.mjs'])
    expect(packed.files[packed.asset_prefix+filename]).toEqual(packed.files[filename]);
  const corrupted=structuredClone(catalogue);corrupted.products[0].price_cents++;
  await expect(verifyFixtureCatalogue(corrupted)).rejects.toThrow('digest');
});

test('exact user-selected retained passage becomes unsaved notes with exact source provenance',()=>{
  const original=plan(),saved=structuredClone(original),selected=choice(original);
  expect(selected.text).toBe(body.slice(at,at+33));
  expect(selected.source_sha256).toBe(product.source_sha256);
  const candidate=adoptPlannerPassage(original,catalogue,selected);
  expect(candidate.notes).toContain(selected.text);
  expect(candidate.notes).toContain(product.source_sha256);
  expect(candidate.notes).toContain(catalogue.digest);
  expect(candidate.notes).toContain(product.source_url);
  expect(candidate.catalogue).toBe(catalogue.digest);
  expect(validatePlan(candidate,catalogue)).toEqual(candidate);
  expect(original).toEqual(saved);
  expect(Object.hasOwn(candidate,'receipt')).toBe(false);
});

test('stale, cross-plan, altered product and edited selection refuse without plan mutation',()=>{
  const original=plan(),selected=choice(original),before=structuredClone(original);
  expect(()=>adoptPlannerPassage({...original,notes:'changed'},catalogue,selected)).toThrow('plan changed');
  expect(()=>adoptPlannerPassage({...original,id:'different-project'},catalogue,selected)).toThrow('plan changed');
  expect(()=>adoptPlannerPassage(original,catalogue,{...selected,text:'invented words'})).toThrow('passage changed');
  const changedSource=structuredClone(catalogue);changedSource.products[0].source_sha256='f'.repeat(64);
  expect(()=>adoptPlannerPassage(original,changedSource,selected)).toThrow('identity changed');
  expect(original).toEqual(before);
});

test('selected source coordinates and plan notes remain bounded',()=>{
  const original=plan();
  expect(()=>selectPlannerPassage(original,catalogue,product.id,-1,3)).toThrow();
  expect(()=>selectPlannerPassage(original,catalogue,'missing',0,2)).toThrow();
  const selected=choice(original);
  const full={...original,notes:'a'.repeat(1490)};
  const held=choice(full);
  expect(()=>adoptPlannerPassage(full,catalogue,held)).toThrow('limit');
  expect(adoptPlannerPassage(original,catalogue,selected).notes.length).toBeLessThanOrEqual(1500);
});
