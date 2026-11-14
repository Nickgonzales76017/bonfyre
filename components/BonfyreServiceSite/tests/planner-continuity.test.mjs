import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {plannerFiles} from '../planner-material.mjs';
import {startingPlan,validatePlan,assessPlan} from '../planner-model.mjs';
import {reviewPlanRefresh,useReviewedRefresh,resolveMissingPlanOptions,refreshBrief} from '../planner-continuity.mjs';
import {verifyFixtureCatalogue} from '../planner-catalogue.mjs';

const source=JSON.parse(readFileSync('packs/aurekai-work-samples/site/fixture-planner.json','utf8'));
const current=plannerFiles(source).catalogue,owner={validatePlan,assessPlan};
const release=update=>{const input=structuredClone(source);update(input);return plannerFiles(input).catalogue;};
const plan=previous=>startingPlan(previous,'compact','planner-continuity-001');

test('reconcile source price change without overwriting original placements or notes',()=>{
 const earlier=release(input=>{input.products[0].price_cents-=700;});
 const original=plan(earlier);original.notes='Keep aisle clearance';
 const old=structuredClone(original);
 const review=reviewPlanRefresh(original,earlier,current,owner);
 expect(review.status).toBe('needs_review');
 expect(review.changes[0].fields).toContain('price_cents');
 expect(review.subtotal_delta_cents).toBe(700);
 expect(review.candidate.notes).toBe('Keep aisle clearance');
 expect(review.candidate.items).toEqual(original.items);
 expect(original).toEqual(old);
 const result=useReviewedRefresh(original,earlier,current,review,owner);
 expect(result.plan.catalogue).toBe(current.digest);
 expect(result.receipt).toBeNull();
 expect(refreshBrief(review)).toContain('Listed subtotal');
});

test('missing exact variant cannot be silently substituted despite matching display names',()=>{
 const earlier=release(input=>{input.products[0].variant_id='99998';});
 const original=plan(earlier);
 const review=reviewPlanRefresh(original,earlier,current,owner);
 expect(review.status).toBe('needs_selection');
 expect(review.candidate).toBeNull();
 expect(()=>useReviewedRefresh(original,earlier,current,review,owner)).toThrow('Resolve missing');
 expect(()=>resolveMissingPlanOptions(original,earlier,current,review,{},owner)).toThrow('explicit choice');
 const missing=review.missing[0].fixture;
 expect(()=>resolveMissingPlanOptions(original,earlier,current,review,{[missing]:'replace:unknown'},owner)).toThrow('current product');
 const replacement=resolveMissingPlanOptions(original,earlier,current,review,{[missing]:'replace:'+current.products[0].id},owner);
 expect(replacement.plan.catalogue).toBe(current.digest);
 expect(replacement.plan.items[0].product).toBe(current.products[0].id);
 expect(replacement.receipt).toBeNull();
 const removed=resolveMissingPlanOptions(original,earlier,current,review,{[missing]:'remove'},owner);
 expect(removed.plan.items.length).toBe(original.items.length-1);
 expect(original.catalogue).toBe(earlier.digest);
});

test('stale plan, conflicting catalogue, and changed source identity refuse all actions',async()=>{
 const earlier=release(input=>{input.products[0].price_cents-=200;});
 const original=plan(earlier),review=reviewPlanRefresh(original,earlier,current,owner);
 expect(()=>useReviewedRefresh({...original,notes:'edited'},earlier,current,review,owner)).toThrow('after review');
 const later=release(input=>{input.products[0].price_cents+=300;});
 expect(()=>useReviewedRefresh(original,earlier,later,review,owner)).toThrow('after review');
 const otherMerchant=release(input=>{input.id='other-merchant';});
 expect(()=>reviewPlanRefresh(original,earlier,otherMerchant,owner)).toThrow('another merchant');
 const forged=structuredClone(earlier);forged.products[0].price_cents++;
 await expect(verifyFixtureCatalogue(forged)).rejects.toThrow('digest');
});

test('explicit removal cannot empty plan; invalid changes never mutate source plan',()=>{
 const earlier=release(input=>{
   for(const p of input.products)p.variant_id=String(Number(p.variant_id)+800000);
 });
 const original=plan(earlier),snapshot=structuredClone(original);
 const review=reviewPlanRefresh(original,earlier,current,owner);
 expect(review.missing.length).toBeGreaterThan(0);
 const allRemove=Object.fromEntries(review.missing.map(r=>[r.fixture,'remove']));
 expect(()=>resolveMissingPlanOptions(original,earlier,current,review,allRemove,owner)).toThrow('Choose between one and');
 expect(original).toEqual(snapshot);
});
