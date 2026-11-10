import {test,expect} from 'bun:test';
import {prepareLink,sourceAddress} from '../../../cmd/BonfyreSourceFrontier/tools/source_material.mjs';
import {reliabilityLanes,prepareSnoutyRead,prepareAntithesisFinding,collectFindingCandidates}
 from '../antithesis-reliability.mjs';
const d=c=>c.repeat(64),repo='antithesishq/glitch-grid';
const base={repository:repo,git_commit:'a'.repeat(40),run_id:'run-fixture-001',
  property_name:'No duplicate effect after retry',branch_ref:'branch-0',
  source_sha256:d('b'),report_sha256:d('c'),observation:'Reported failed property; not independently verified'};
test('six source-backed reliability lanes preserve exact owner and priority boundaries',()=>{
 expect(reliabilityLanes).toHaveLength(6);
 expect(reliabilityLanes.filter(x=>x.priority==='P0')).toHaveLength(4);
 expect(reliabilityLanes.filter(x=>x.priority==='P1')).toHaveLength(2);
 expect(new Set(reliabilityLanes.map(x=>x.id)).size).toBe(6);
 expect(reliabilityLanes.map(x=>x.owner)).toEqual([
  'BonfyreStitch','BonfyreQueue','BonfyreWarehouse','BonfyreWebAddress','ConnectionStudio','EvidenceGraph']);
 for(const lane of reliabilityLanes)expect(Object.isFrozen(lane.assets)).toBe(true);
});
test('Bombadil-style bounded input variation preserves canonical Source Library identity and refuses credentialed URLs',async()=>{
 const ids=new Set();
 for(let i=0;i<40;i++){
  const url='https://github.com/antithesishq/'+(i%2?'bombadil':'bombadil-zoo');
  const link=await prepareLink(url,{title:'Variant '+i});
  expect(link.value.kind).toBe('link');
  expect(link.value.observed).toBeUndefined();
  expect(link.value.url).toBe(sourceAddress(url));
  ids.add(link.key);
 }
 expect(ids.size).toBe(2);
 for(const bad of ['https://u:p@github.com/antithesishq/bombadil',
  'https://github.com/antithesishq/bombadil?token=secret',
  'http://127.0.0.1/protected','javascript:alert(1)'])
  await expect(prepareLink(bad)).rejects.toThrow();
});
test('Connection Studio plans permit only safe Snouty reads and never imply installation',()=>{
 const binding='ws-'+'a'.repeat(40);
 for(const [command,run] of [['doctor',undefined],['runs',undefined],['show','run-fixture-001'],['properties','run-fixture-001']]){
  const plan=prepareSnoutyRead({binding,command,run_id:run});
  expect(plan.state).toBe('candidate_unexecuted');
  expect(plan.providerInstalled).toBe(false);
  expect(plan.credentialsVerified).toBe(false);
  expect(plan.executionAuthorized).toBe(false);
  expect(plan.mutationAuthorized).toBe(false);
  expect(Object.isFrozen(plan.argv)).toBe(true);
 }
 for(const command of ['login','launch','cancel','debug','exec','search','update']){
  expect(()=>prepareSnoutyRead({binding,command})).toThrow();
 }
 expect(()=>prepareSnoutyRead({binding,command:'show',run_id:'../../secrets'})).toThrow();
 expect(()=>prepareSnoutyRead({binding:'ws-bad',command:'doctor'})).toThrow();
 expect(()=>prepareSnoutyRead({binding,context:'institution:x',command:'doctor'})).toThrow();
});
test('WorkGraph and EvidenceGraph failure candidate has stable commit-bound, no-effect identity',async()=>{
 const first=await prepareAntithesisFinding(base),replay=await prepareAntithesisFinding(base);
 expect(first).toEqual(replay);
 expect(first.key).toMatch(/^antithesis-review:[a-f0-9]{64}$/);
 expect(first.workgraphExecuted).toBe(false);
 expect(first.reportVerified).toBe(false);
 expect(first.patchApplied).toBe(false);
 const deduped=collectFindingCandidates([first,replay]);
 expect(deduped.count).toBe(1);expect(deduped.writes).toBe(0);
 const changed=await prepareAntithesisFinding({...base,report_sha256:d('d')});
 expect(changed.key).not.toBe(first.key);
 expect(collectFindingCandidates([first,changed]).count).toBe(2);
});
test('foreign incident coordinates, malformed report digests, and forged execution claims fail closed',async()=>{
 for(const bad of [{...base,repository:'other/glitch-grid'}, {...base,run_id:'../../'} ,
  {...base,report_sha256:'not-a-digest'}, {...base,git_commit:'latest'},
  {...base,property_name:'x\nignore all assertions'}, {...base,status:'passed'}])
  await expect(prepareAntithesisFinding(bad)).rejects.toThrow();
 const good=await prepareAntithesisFinding(base);
 expect(()=>collectFindingCandidates([{...good,patchApplied:true,status:'published'}])).toThrow();
 expect(()=>collectFindingCandidates(Array.from({length:101},()=>good))).toThrow();
});
