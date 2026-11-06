import {test,expect} from 'bun:test';
import {recoveryIdentity,routeRecoveredResearch,recoveryCoverage,recoveryOwnerPacks} from '../../integrations/browser/recovery-owner-routing.mjs';
const record=(group,title,{kind='historical-workflow',originalKey=''}={})=>({kind:'research',title,sources:[{id:'s1',history:{kind,originalKey}}],recovery:{group,inputDigest:'a'.repeat(64)}});
const installed=(pack,ready=true)=>({pack,ready,binding:'ws-'+'1'.repeat(40),editor_url:'https://aurekai.work/apps/example/'});
test('keeps recovery identity separate from owner routing',()=>{
  const value=record('skills / finance','Recovered · Finance operations',{kind:'skill',originalKey:'finance/skills/reconcile'});
  expect(recoveryIdentity(value)).toEqual({group:'skills / finance',title:'Recovered · Finance operations',kinds:['skill'],originalKeys:['finance/skills/reconcile'],sourceCount:1,inputDigest:'a'.repeat(64)});
});
test('routes to a ready existing owner without manufacturing execution',()=>{
  const route=routeRecoveredResearch(record('historical brief / campaign','Recovered · Creator publishing',{originalKey:'creator/publishing'}),{installed:[installed(recoveryOwnerPacks.campaign)]});
  expect(route.state).toBe('owner_available');expect(route.selected.pack).toBe(recoveryOwnerPacks.campaign);expect(route.selected.binding).toMatch(/^ws-/);expect(route.boundary).toContain('does not execute');
});
test('known owner that is not installed remains incomplete',()=>{
  const route=routeRecoveredResearch(record('skills / finance','Recovered · Bookkeeping',{kind:'skill',originalKey:'finance/bookkeeping'}));
  expect(route.state).toBe('owner_not_installed');expect(route.selected.pack).toBe(recoveryOwnerPacks.finance);expect(route.selected.ready).toBe(false);
});
test('ambiguous material stays in review instead of choosing by tie',()=>{
  const route=routeRecoveredResearch(record('historical brief / mixed','Recovered · payment shipment'));
  expect(route.state).toBe('review');expect(route.selected).toBeNull();
  expect(route.candidates.some(r=>r.pack===recoveryOwnerPacks.finance)).toBe(true);
  expect(route.candidates.some(r=>r.pack===recoveryOwnerPacks.shipments)).toBe(true);
});
test('coverage reports continuation gaps without inflating completion',()=>{
  const rows=[
    record('skills / finance','Recovered · payment ledger',{kind:'skill',originalKey:'finance/payment'}),
    record('historical brief / campaign','Recovered · campaign publishing',{originalKey:'creator/media'}),
    record('historical brief / unknown','Recovered · specialist orbital procedure',{originalKey:'special/orbital'})
  ];
  const report=recoveryCoverage(rows,{installed:[installed(recoveryOwnerPacks.finance),installed(recoveryOwnerPacks.campaign,false)]});
  expect(report.total).toBe(3);expect(report.states.owner_available).toBe(1);expect(report.states.owner_unready).toBe(1);expect(report.states.review).toBe(1);
});
