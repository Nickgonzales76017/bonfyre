import {test,expect} from 'bun:test';
import {startingPlan,validatePlan,assessPlan,planItemLimit,planRequest,planReviewForm} from '../planner-model.mjs';
import {editFixtures,moveFixtures,displayFootprint} from '../planner-spatial-model.mjs';
import {prepareCatalogueRelease,verifyCatalogueRelease,reviewCatalogueRelease,releaseChecks} from '../catalogue-release.mjs';
import {fixtureCatalogue,releaseFixture} from './catalogue-release-fixture.mjs';
const room=(count=40)=>({...startingPlan(fixtureCatalogue,'compact','scale-room-0001'),width:40,depth:20,budget:1400000,
 items:Array.from({length:count},(_,i)=>({key:'placement-'+i,product:'table',x:1+i%10*3.25,y:1+Math.floor(i/10)*2.25,rotation:0}))});
const source=plan=>{const s=releaseFixture();s.records[1]={key:'fixture-plan:planner-rooms:'+plan.id,revision:3,value:{plan,receipt:null}};return {...s,id:'scale-release-001',title:'Showroom price revision'};};
test('forty distinct placements retain geometry, exact subtotal and budget through edits',()=>{
 const p=room(),a=assessPlan(p,fixtureCatalogue);
 expect(a.lines).toHaveLength(40);expect(a.subtotal).toBe(1200000);expect(a.issues).toEqual([]);
 const moved=moveFixtures(p,fixtureCatalogue,['placement-39'],.25,.25);
 expect(moved.items[39]).toMatchObject({x:30.5,y:8});expect(p.items[39].x).toBe(30.25);
 const rotated=editFixtures(moved,fixtureCatalogue,['placement-39'],'rotate');
 expect(rotated.items[39].rotation).toBe(90);expect(assessPlan(rotated,fixtureCatalogue).subtotal).toBe(1200000);
 const collision=structuredClone(p);collision.items[39].x=1;collision.items[39].y=1;
 expect(assessPlan(collision,fixtureCatalogue).issues.join(' ')).toContain('overlaps');
});
test('additions and bulk duplication share the sixty-four placement boundary without partial changes',()=>{
 const p=room(63);expect(planItemLimit).toBe(64);
 const full=editFixtures(p,fixtureCatalogue,['placement-0'],'duplicate',()=> 'last-fixture');
 expect(full.items).toHaveLength(64);expect(validatePlan(full,fixtureCatalogue)).toEqual(full);
 expect(()=>editFixtures(full,fixtureCatalogue,['placement-0'],'duplicate')).toThrow('64 fixtures');
 expect(()=>editFixtures(p,fixtureCatalogue,['placement-0','placement-1'],'duplicate')).toThrow('64 fixtures');
 expect(p.items).toHaveLength(63);expect(()=>validatePlan(room(65),fixtureCatalogue)).toThrow('64 fixtures');
});
test('all sixty-four placements fit the existing intake field and encoded link bounds',()=>{
 const p=room(64);p.notes='x'.repeat(1500);p.title='x'.repeat(100);
 p.items=p.items.map((i,n)=>({...i,key:'x'.repeat(36)+String(n).padStart(4,'0')}));
 const request=planRequest(p,fixtureCatalogue,{form:'planner-rooms',page:'product-room',name:'Reviewer',email:'reviewer@example.test'});
 const limits=planReviewForm(fixtureCatalogue,'product-room');
 expect(JSON.parse(request).fields[2].value.length).toBeLessThan(limits.fields['Fixture plan'].max_length);
 expect(new TextEncoder().encode(request).length).toBeLessThan(limits.max_bytes);
 expect(Buffer.from(JSON.stringify(p)).toString('base64url').length).toBeLessThan(22000);
});
test('unsafe large subtotals refuse rounded totals',()=>{
 const catalogue=structuredClone(fixtureCatalogue);catalogue.products[0].price_cents=Number.MAX_SAFE_INTEGER;
 expect(()=>assessPlan(room(),catalogue)).toThrow('precision');
});
test('forty repeated merchant options each contribute to retained release price and budget impact',async()=>{
 const s=source(room()),value=await prepareCatalogueRelease(s),impact=value.impact.impacted[0];
 expect(value.impact.unresolved).toEqual([]);expect(impact.items).toHaveLength(40);
 expect(impact).toMatchObject({before_cents:1200000,proposed_cents:1600000,delta_cents:400000,budget_cents:1400000,exceeds_budget:true});
 expect(releaseChecks(value)).toHaveLength(2);
 expect(await verifyCatalogueRelease(JSON.parse(JSON.stringify(value)))).toEqual(value);
 const reviewed=await reviewCatalogueRelease(value,value,'All forty placements checked.');
 expect(reviewed.state).toBe('reviewed');expect(reviewed.publication).toBeNull();
});
test('changed large-room layout or evidence invalidates review instead of retaining approval',async()=>{
 const s=source(room()),saved=await prepareCatalogueRelease(s);s.records[1].revision++;s.records[1].value.plan.items[39].x+=.25;
 const current=await prepareCatalogueRelease(s);
 await expect(reviewCatalogueRelease(saved,current,'Checked')).rejects.toThrow('changed after staging');
 const wrong=structuredClone(fixtureCatalogue);wrong.products[0].source_sha256='b'.repeat(64);
 const blocked=await prepareCatalogueRelease({...s,catalogues:[wrong]});
 expect(blocked.impact.impacted[0].proposed_cents).toBeNull();
 await expect(reviewCatalogueRelease(blocked,blocked,'Checked')).rejects.toThrow('source checks');
});

test('listed dimensions determine the visible footprint even without an allowance',()=>{
 const p=fixtureCatalogue.products[0];expect(displayFootprint(p,{rotation:0})).toEqual({w:3,h:2,known:true});
 expect(displayFootprint({...p,allowance:{width:12,depth:12}},{rotation:90})).toEqual({w:2,h:3,known:true});
 expect(displayFootprint({...p,dimensions:null,allowance:{width:12,depth:24}},{rotation:0})).toEqual({w:1,h:2,known:false});
});
