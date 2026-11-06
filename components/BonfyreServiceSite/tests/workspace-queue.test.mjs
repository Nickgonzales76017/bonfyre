import {test,expect} from 'bun:test';
import {queueState,queueSearch,queueRows} from '../workspace-queue.mjs';
const id='025908d2-ae5f-46e3-9fb6-3abb2a665c35';
test('queue links preserve a selected request and bounded view state',()=>{
  const state={filter:'mine',query:'Shoulders & travel',layout:'board',sort:'oldest',selected:id};
  expect(queueState(queueSearch(state))).toEqual(state);
  expect(queueState('?filter=admin&layout=inject&sort=bad&request=%22%3E&q='+('x'.repeat(500)))).toEqual({filter:'all',layout:'list',sort:'newest',selected:'',query:'x'.repeat(300)});
});
test('personal queue excludes another worker and filters delivered routine names without mutating records',()=>{
  const make=(id,state,worker,submittedAt,delivery)=>({id,state,worker,submittedAt,brief:{title:'Session',details:'Travel'},delivery});
  const rows=[make('a','running','coach-a','2026-10-14'),make('b','running','coach-b','2026-10-15'),make('c','complete','coach-a','2026-10-13',{routine:{name:'Evening mobility'}})];
  expect(queueRows(rows,{...queueState(''),filter:'mine'},'coach-a').map(r=>r.id)).toEqual(['a']);
  expect(queueRows(rows,{...queueState(''),query:'mobility'},'coach-a').map(r=>r.id)).toEqual(['c']);
  expect(queueRows(rows,{...queueState(''),sort:'oldest'},'coach-a').map(r=>r.id)).toEqual(['c','a','b']);
  expect(rows.map(r=>r.id)).toEqual(['a','b','c']);
});
