import { test, expect } from 'bun:test';
import { mkdtempSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { measureZeroClose } from '../tools/zero_close.mjs';

const sha=v=>createHash('sha256').update(v).digest('hex');
const stages=[
  ['observed_request','DemandBook'],['specified','ProgramFabric'],['capability_closed','SupplyMatch'],
  ['proposal_delivered','CommitmentGraph'],['authority_granted','AuthorityGraph'],['work_completed','WorkGraph'],
  ['verified','EvidenceGraph'],['delivered','PublicationProgram'],['settled','MoneyGraph'],['retained','BonfyreProject']
];
const event=(id,i,extra={})=>({schema:'bonfyre-zero-close-event.v1',occurrence_id:id,
  stage:stages[i][0],owner:stages[i][1],basis_sha256:sha('basis:'+id),
  evidence_sha256:sha(id+':'+i),recorded_at:`2026-10-05T1${i}:00:00Z`,source_kind:'classified',...extra});

test('closure is contiguous and later receipts cannot skip a missing stage',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'zero-close-')),input=join(dir,'events.jsonl'),details=join(dir,'state.jsonl');
  try{
    const rows=[];
    for(let i=0;i<10;i++)rows.push(event('request:full',i,i===0?
      {advertised_amount_microunits:'300000000',advertised_currency:'USD',cost_microunits:'500000'}:
      i===8?{revenue_microunits:'300000000',revenue_currency:'USD',human_seconds:60}:{}));
    rows.push(event('request:gap',0),event('request:gap',1),event('request:gap',2),event('request:gap',8,{revenue_microunits:'999000000',revenue_currency:'USD'}));
    rows.push({...event('request:bad-owner',0),owner:'MoneyGraph'});
    rows.push(event('request:full',9));
    writeFileSync(input,rows.map(JSON.stringify).join('\n')+'\n');
    const report=await measureZeroClose(input,{details});
    expect(report).toMatchObject({requests:2,settled:1,retained:1,invalid_events:1,duplicate_events:1,
      revenue_microunits:'300000000',cost_microunits:'500000',human_seconds:60});
    expect(report.stages.retained).toEqual({exact:1,at_least:1});
    expect(report.stages.capability_closed.at_least).toBe(2);
    expect(report.next_stage_blockers.proposal_delivered).toBe(1);
    expect(report.contribution_margin_microunits).toBe('299500000');
    const state=readFileSync(details,'utf8').trim().split('\n').map(JSON.parse);
    expect(state.find(r=>r.occurrence_id==='request:gap')).toMatchObject({depth:2,stage:'capability_closed',revenue_microunits:'0'});
  }finally{rmSync(dir,{recursive:true,force:true})}
});

test('settlement requires an economic owner and exact integer revenue',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'zero-close-invalid-')),input=join(dir,'events.jsonl');
  try{
    const rows=[
      {...event('request:a',8,{revenue_microunits:'1',revenue_currency:'USD'}),owner:'WorkGraph'},
      event('request:b',8,{revenue_microunits:'1.5',revenue_currency:'USD'}),
      event('request:c',8,{revenue_microunits:'9007199254740993000',revenue_currency:'USD'}),
    ];
    writeFileSync(input,rows.map(JSON.stringify).join('\n')+'\n');
    const report=await measureZeroClose(input);
    expect(report.requests).toBe(1);
    expect(report.invalid_events).toBe(2);
    expect(report.settled).toBe(0);
  }finally{rmSync(dir,{recursive:true,force:true})}
});

test('100k observed requests stream without pretending they closed',async()=>{
  const dir=mkdtempSync(join(tmpdir(),'zero-close-scale-')),input=join(dir,'events.jsonl');
  try{
    const rows=Array.from({length:100000},(_,i)=>JSON.stringify(event('scale:'+String(i).padStart(6,'0'),0,
      {advertised_amount_microunits:'300000000',advertised_currency:'USD'}))).join('\n')+'\n';
    writeFileSync(input,rows);
    const start=performance.now(),report=await measureZeroClose(input),elapsed=performance.now()-start;
    expect(report.requests).toBe(100000);expect(report.settled).toBe(0);
    expect(report.next_stage_blockers.specified).toBe(100000);
    console.log(JSON.stringify({witness:'zero_close_100k',requests:report.requests,elapsed_ms:elapsed}));
  }finally{rmSync(dir,{recursive:true,force:true})}
},120000);
