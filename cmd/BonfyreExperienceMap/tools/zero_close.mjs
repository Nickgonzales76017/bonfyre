#!/usr/bin/env bun
import { createReadStream, createWriteStream, existsSync, renameSync, rmSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { createInterface } from 'node:readline';

const STAGES = [
  'observed_request','specified','capability_closed','proposal_delivered','authority_granted',
  'work_completed','verified','delivered','settled','retained'
];
const OWNER = {
  observed_request:new Set(['ForeignTwinGraph','DemandBook','SourceFrontier']),
  specified:new Set(['DemandBook','ProgramFabric']),
  capability_closed:new Set(['SupplyMatch','CapabilityClosureGraph']),
  proposal_delivered:new Set(['CommitmentGraph','CommerceChain','MoneyGraph']),
  authority_granted:new Set(['AuthorityGraph','CredentialBoundary']),
  work_completed:new Set(['WorkGraph','EffectKernel']),
  verified:new Set(['EvidenceGraph','RoundTripTwin']),
  delivered:new Set(['PublicationProgram','RoundTripTwin']),
  settled:new Set(['MoneyGraph','FinancialMetabolism','CommerceChain']),
  retained:new Set(['BonfyreProject','ForeignTwinGraph','ArtifactGraph'])
};
const SHA=/^[a-f0-9]{64}$/;
const ID=/^[a-z0-9][a-z0-9:._/-]{2,240}$/i;
const MONEY=/^(0|[1-9][0-9]{0,20})$/;
const stageIndex=new Map(STAGES.map((name,i)=>[name,i]));
const digest=v=>createHash('sha256').update(v).digest('hex');
const money=(v,label)=>{const s=String(v??'');if(!MONEY.test(s))throw Error(`invalid ${label}`);return BigInt(s)};
const safeInt=(v,label,max=Number.MAX_SAFE_INTEGER)=>{if(!Number.isSafeInteger(v)||v<0||v>max)throw Error(`invalid ${label}`);return v};
const add=(m,k,n=1)=>m.set(k,(m.get(k)||0)+n);
const fresh=id=>({occurrence_id:id,events:new Map(),owners:new Set(),source_kind:'',advertised:0n,advertised_currency:'',revenue:0n,revenue_currency:'',cost:0n,human:0,first_at:'',last_at:''});

function validate(row){
  if(!row||typeof row!=='object'||Array.isArray(row)||row.schema!=='bonfyre-zero-close-event.v1'||
    typeof row.occurrence_id!=='string'||!ID.test(row.occurrence_id)||
    typeof row.stage!=='string'||!stageIndex.has(row.stage)||
    typeof row.owner!=='string'||!OWNER[row.stage].has(row.owner)||
    typeof row.evidence_sha256!=='string'||!SHA.test(row.evidence_sha256)||
    typeof row.basis_sha256!=='string'||!SHA.test(row.basis_sha256)||
    typeof row.recorded_at!=='string'||!Number.isFinite(Date.parse(row.recorded_at)))
    throw Error('invalid zero-close receipt');
  if(row.source_kind!=null&&(typeof row.source_kind!=='string'||row.source_kind.length>120))throw Error('invalid source kind');
  if(row.human_seconds!=null)safeInt(row.human_seconds,'human seconds',31536000);
  if(row.cost_microunits!=null)money(row.cost_microunits,'cost');
  if(row.advertised_amount_microunits!=null)money(row.advertised_amount_microunits,'advertised amount');
  if(row.revenue_microunits!=null)money(row.revenue_microunits,'revenue');
  for(const k of ['advertised_currency','revenue_currency'])if(row[k]!=null&&!/^[A-Z]{3}$/.test(row[k]))throw Error(`invalid ${k}`);
  if(row.stage==='settled'&&row.revenue_microunits==null)throw Error('settled receipt requires revenue');
  return row;
}

export async function measureZeroClose(path,{details=''}={}){
  const byId=new Map(), seen=new Set(), stats={lines:0,admitted:0,invalid:0,duplicates:0};
  const input=createInterface({input:createReadStream(path,{encoding:'utf8'}),crlfDelay:Infinity});
  for await(const line of input){
    if(!line.trim())continue; stats.lines++;
    let row; try{row=validate(JSON.parse(line))}catch{stats.invalid++;continue}
    const ek=[row.occurrence_id,row.stage,row.evidence_sha256].join('\x1f');
    if(seen.has(ek)){stats.duplicates++;continue} seen.add(ek);
    const s=byId.get(row.occurrence_id)||fresh(row.occurrence_id), at=Date.parse(row.recorded_at), prior=s.events.get(row.stage);
    if(!prior||at<prior.at)s.events.set(row.stage,{...row,at});
    s.owners.add(row.owner); if(row.source_kind&&!s.source_kind)s.source_kind=row.source_kind;
    if(!s.first_at||row.recorded_at<s.first_at)s.first_at=row.recorded_at;
    if(!s.last_at||row.recorded_at>s.last_at)s.last_at=row.recorded_at;
    if(row.human_seconds!=null)s.human+=row.human_seconds;
    if(row.cost_microunits!=null)s.cost+=money(row.cost_microunits,'cost');
    if(row.stage==='observed_request'&&row.advertised_amount_microunits!=null){
      const n=money(row.advertised_amount_microunits,'advertised');
      if(!s.advertised_currency||s.advertised_currency===row.advertised_currency){s.advertised=n;s.advertised_currency=row.advertised_currency||''}
    }
    if(row.stage==='settled'){
      const n=money(row.revenue_microunits,'revenue');
      if(!s.revenue_currency||s.revenue_currency===row.revenue_currency){s.revenue=n;s.revenue_currency=row.revenue_currency||''}
    }
    byId.set(row.occurrence_id,s);stats.admitted++;
  }

  const exact=Array(STAGES.length).fill(0),atLeast=Array(STAGES.length).fill(0),blockers=new Map(),sources=new Map(),rows=[];
  let revenue=0n,cost=0n,human=0,settled=0,retained=0;
  for(const s of byId.values()){
    let depth=-1;for(let i=0;i<STAGES.length;i++){if(!s.events.has(STAGES[i]))break;depth=i}
    const next=depth+1<STAGES.length?STAGES[depth+1]:'';
    if(depth>=0){exact[depth]++;for(let i=0;i<=depth;i++)atLeast[i]++}
    if(next)add(blockers,next);if(s.source_kind)add(sources,s.source_kind);
    if(depth>=8){settled++;revenue+=s.revenue}if(depth>=9)retained++;
    cost+=s.cost;human+=s.human;
    rows.push({schema:'bonfyre-zero-close-state.v1',occurrence_id:s.occurrence_id,depth:Math.max(depth,0),
      stage:depth>=0?STAGES[depth]:'invalid',next_required_stage:next,source_kind:s.source_kind,
      owners_observed:[...s.owners].sort(),human_seconds:s.human,cost_microunits:s.cost.toString(),
      advertised_amount_microunits:s.advertised.toString(),advertised_currency:s.advertised_currency,
      revenue_microunits:depth>=8?s.revenue.toString():'0',revenue_currency:depth>=8?s.revenue_currency:'',
      first_at:s.first_at,last_at:s.last_at});
  }
  rows.sort((a,b)=>a.occurrence_id.localeCompare(b.occurrence_id));
  if(details){
    const tmp=`${details}.tmp-${process.pid}`,out=createWriteStream(tmp,{flags:'wx',mode:0o600});
    try{for(const row of rows)if(!out.write(JSON.stringify(row)+'\n'))await once(out,'drain');out.end();await once(out,'close');renameSync(tmp,details)}
    catch(e){out.destroy();rmSync(tmp,{force:true});throw e}
  }
  return {schema:'bonfyre-zero-close-report.v1',requests:byId.size,events_read:stats.lines,events_admitted:stats.admitted,
    invalid_events:stats.invalid,duplicate_events:stats.duplicates,
    stages:Object.fromEntries(STAGES.map((name,i)=>[name,{exact:exact[i],at_least:atLeast[i]}])),
    next_stage_blockers:Object.fromEntries([...blockers].sort((a,b)=>b[1]-a[1]||a[0].localeCompare(b[0]))),
    source_kinds:Object.fromEntries([...sources].sort((a,b)=>b[1]-a[1]||a[0].localeCompare(b[0]))),
    settled,retained,human_seconds:human,revenue_microunits:revenue.toString(),cost_microunits:cost.toString(),
    contribution_margin_microunits:(revenue-cost).toString(),settlement_ratio:{numerator:settled,denominator:byId.size},
    retained_ratio:{numerator:retained,denominator:byId.size},
    evidence_digest:digest(rows.map(r=>[r.occurrence_id,r.stage,r.next_required_stage].join('\x1f')).join('\n')),
    cannot_infer:['observed compensation -> collectible revenue','a later stage without every prior independent stage -> closure',
      'settled amount -> profit without measured cost','synthetic scale -> live marketplace reach, permission to contact, or customer acceptance']};
}

if(import.meta.main){
  try{
    const [input,details]=process.argv.slice(2);
    if(!input||!existsSync(input))throw Error('usage: zero_close.mjs EVENTS.jsonl [DETAILS.jsonl]');
    console.log(JSON.stringify(await measureZeroClose(input,{details}),null,2));
  }catch(error){console.error(`zero-close: ${error.message}`);process.exitCode=1}
}
