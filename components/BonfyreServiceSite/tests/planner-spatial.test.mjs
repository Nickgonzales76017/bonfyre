import {test,expect} from 'bun:test';
import {readFileSync} from 'node:fs';
import {plannerFiles} from '../planner-material.mjs';
import {startingPlan,assessPlan} from '../planner-model.mjs';
import {moveFixtures,editFixtures} from '../planner-spatial-model.mjs';
const {catalogue}=plannerFiles(JSON.parse(readFileSync('packs/aurekai-work-samples/site/fixture-planner.json')));
test('group movement snaps once and preserves offsets at the bounds',()=>{
 const plan=startingPlan(catalogue),keys=['rack-1','table-1'];const moved=moveFixtures(plan,catalogue,keys,1.13,.37);
 expect(moved.items[0].x).toBe(2.25);expect(moved.items[1].x).toBe(10.25);expect(moved.items[0].y).toBe(1.25);
 const bounded=moveFixtures(plan,catalogue,keys,-10,70);expect(bounded.items[0].x).toBe(0);expect(bounded.items[1].x).toBe(8);expect(bounded.items[0].y).toBe(60);expect(plan.items[0].x).toBe(1);
});
test('duplication preserves catalogue identity and adds exact option cost',()=>{
 const plan=startingPlan(catalogue);let id=0;const next=editFixtures(plan,catalogue,['rack-1','table-1'],'duplicate',()=>`copy-${++id}`);
 expect(next.items.length).toBe(6);expect(new Set(next.items.map(i=>i.key)).size).toBe(6);expect(assessPlan(next,catalogue).subtotal-assessPlan(plan,catalogue).subtotal).toBe(62300);
 expect(()=>editFixtures({...plan,items:Array.from({length:64},(_,i)=>({...plan.items[0],key:`rack-${i}`}))},catalogue,['rack-1'],'duplicate')).toThrow('64 fixtures');
});
test('alignment and distribution retain existing contract and deletion cannot empty a plan',()=>{
 const plan=startingPlan(catalogue),keys=plan.items.map(i=>i.key);
 const aligned=editFixtures(plan,catalogue,keys,'left');expect(aligned.items.every(i=>i.x===1)).toBe(true);
 const spaced=editFixtures(plan,catalogue,keys,'distribute-x');expect(spaced.items.map(i=>i.x)).toEqual([1,4.25,7.75,11]);
 expect(editFixtures(plan,catalogue,['counter-1'],'rotate').items[2].rotation).toBe(90);
 expect(()=>editFixtures(plan,catalogue,keys,'remove')).toThrow('at least one');
});

test('planner modules share content-addressed paths without changing catalogue identity',()=>{
 const input=JSON.parse(readFileSync('packs/aurekai-work-samples/site/fixture-planner.json')),packed=plannerFiles(input);
 expect(packed.asset_prefix).toMatch(/^v-[a-f0-9]{64}\/$/);
 expect(packed.catalogue.digest).toBe(catalogue.digest);
 for(const name of ['planner-client.mjs','planner-spatial.mjs','planner-spatial-model.mjs','store.mjs','vendor/konva-10.9.0/konva.min.js'])expect(packed.files[packed.asset_prefix+name]).toEqual(packed.files[name]);
});
