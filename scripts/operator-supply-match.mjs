#!/usr/bin/env bun
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {createWriteStream} from 'node:fs';
import {once} from 'node:events';
import {homedir} from 'node:os';

const sha=v=>createHash('sha256').update(v).digest('hex');
const canonical=v=>JSON.stringify(v,Object.keys(v).sort());
const parsed=v=>{try{return JSON.parse(v)}catch{return null}};
const tokens=v=>String(v??'').toLowerCase().split(/[^a-z0-9]+/).filter(x=>x.length>2&&!['hybrid','remote','united','states'].includes(x));
const cms=(bin,db,schemas,type,ns)=>{
  const p=spawnSync(bin,['entry','list',type,'--ns',ns,'--limit','5000','--db',db,'--schemas',schemas],
    {encoding:'utf8',timeout:30000,maxBuffer:8*1024*1024});
  if(p.status!==0)throw Error(p.stderr||p.stdout);
  return (JSON.parse(p.stdout).data??[]).map(x=>x.attributes??x);
};
const rateValue=r=>r&&Number.isFinite(Number(r.amount))?Number(r.amount):null;
const standingSatisfied=(required,actual={})=>{
  if(!required||typeof required!=='object'||Array.isArray(required))return true;
  for(const [key,want] of Object.entries(required)){
    if(key.endsWith('_min')){
      const have=Number(actual[key.slice(0,-4)]);
      if(!Number.isFinite(have)||have<Number(want))return false;
    }else if(key.endsWith('_contains')){
      const have=actual[key.slice(0,-9)];
      if(Array.isArray(have)){if(!have.includes(want))return false}
      else if(typeof have==='string'){if(!have.split(/[;,|]/).map(x=>x.trim()).includes(want))return false}
      else return false;
    }else if(actual[key]!==want)return false;
  }
  return true;
};

export function matchOperatorSupply(needs,resources,{limit=12}={}){
  const summaries=[],byResource=new Map();
  for(const need of needs){
    const wanted=parsed(need.wanted);if(!wanted?.workflow)continue;
    const asks=new Set([wanted.workflow,...(Array.isArray(wanted.secondary)?wanted.secondary:[])]);
    const geoRelevant=wanted.human_presence_required!==false&&wanted.search_scope!=='national'&&wanted.fulfillment_mode!=='remote_account';
    const geoWanted=new Set(tokens(wanted.operator_locality??wanted.locality)),candidates=[];
    for(const resource of resources){
      const f=parsed(resource.functions);if(!f||!Array.isArray(f.workflow_families))continue;
      if(!standingSatisfied(wanted.required_evidence,f.standing??{}))continue;
      const matched=f.workflow_families.filter(x=>asks.has(x));if(!matched.length)continue;
      const primary=matched.includes(wanted.workflow),geo=tokens(resource.grouping);
      const geoHits=geoRelevant?geo.filter(x=>geoWanted.has(x)).length:0;
      const remote=/remote/i.test(String(f.availability??''))||wanted.fulfillment_mode==='remote_account'||wanted.search_scope==='national';
      const rating=f.rating&&Number.isFinite(Number(f.rating.value))?Number(f.rating.value):0;
      let score=(primary?100:55)+Math.min(30,(matched.length-1)*12)+(geoHits?20:0)+(remote?8:0)+Math.min(10,rating*2);
      if(['labor_market_rate','job_posting_rate'].includes(f.source_kind))score-=35;
      candidates.push({resource_id:resource.ext_id,name:resource.name,url:resource.url,source:resource.related,grouping:resource.grouping,
        matched_workflows:matched,workflow_families:f.workflow_families,rate:f.rate??null,rating:f.rating??null,
        availability:f.availability??'',contact_mode:f.contact_mode??'unknown',standing:f.standing??{},body_sha256:f.body_sha256??'',score});
    }
    candidates.sort((a,b)=>b.score-a.score||(rateValue(a.rate)??1e12)-(rateValue(b.rate)??1e12)||a.name.localeCompare(b.name));
    const chosen=candidates.slice(0,limit);
    summaries.push({need_id:need.need_id,project_id:need.project_id,workflow:wanted.workflow,
      operator_locality:wanted.operator_locality??wanted.locality??'',inventory_locality:wanted.inventory_locality??'',
      search_scope:wanted.search_scope??'local',fulfillment_mode:wanted.fulfillment_mode??'human_local',
      verification_mode:wanted.verification_mode??'observed_profile',human_presence_required:wanted.human_presence_required!==false,
      candidates:chosen.length,top:chosen.slice(0,8)});
    for(const c of chosen){
      const x=byResource.get(c.resource_id)??{resource:c,needs:[]};
      x.needs.push({need_id:need.need_id,project_id:need.project_id,score:c.score,matched_workflows:c.matched_workflows,
        search_scope:wanted.search_scope??'local',fulfillment_mode:wanted.fulfillment_mode??'human_local',
        verification_mode:wanted.verification_mode??'observed_profile',human_presence_required:wanted.human_presence_required!==false});
      if(c.score>x.resource.score)x.resource=c;
      byResource.set(c.resource_id,x);
    }
  }
  const receipts=[];
  for(const [resourceId,x] of [...byResource].sort(([a],[b])=>a.localeCompare(b))){
    x.needs.sort((a,b)=>a.need_id.localeCompare(b.need_id));
    const basis={resource_id:resourceId,resource_url:x.resource.url,resource_grouping:x.resource.grouping,
      resource_body_sha256:x.resource.body_sha256,resource_standing:x.resource.standing,matched_needs:x.needs};
    const basis_sha256=sha(canonical(basis)),evidence_sha256=sha(canonical({...basis,rate:x.resource.rate,rating:x.resource.rating}));
    receipts.push({schema:'bonfyre-owner-participation.v1',occurrence_id:resourceId,axis:'capability',state:'eligible',
      owner:'SupplyMatch',from_owner:'DemandBook',to_owner:'SupplyMatch',basis_sha256,evidence_sha256,
      produced_by:'operator-supply-match',recorded_at:new Date().toISOString(),matched_needs:x.needs,
      observed_rate:x.resource.rate,observed_rating:x.resource.rating,resource_url:x.resource.url,
      cannot_infer:['eligible -> selected, contacted, engaged, hired, authorized, bound or activated',
        'observed public rate -> accepted quote or final cost','workflow overlap -> adequate performance for the actual customer']});
  }
  return {receipts,summaries};
}

if(import.meta.main){
  try{
    const argv=process.argv.slice(2),arg=n=>{const i=argv.indexOf(n);return i>=0?argv[i+1]:''};
    const home=homedir(),bin=arg('--cms')||home+'/.bonfyre/bin/bonfyre-cms',
      db=arg('--db')||home+'/.bonfyre/estate-local/cms.db',
      schemas=arg('--schemas')||home+'/Developer/Bonfyre/cmd/BonfyreCMS/content-types',
      needsNs=arg('--needs-ns')||'craigslist-zero-close',supplyNs=arg('--supply-ns')||'operator-supply',
      output=arg('--output'),summaryPath=arg('--summary'),limit=Number(arg('--limit')||12);
    if(!output||!summaryPath||!Number.isSafeInteger(limit)||limit<1||limit>100)throw Error('usage: operator-supply-match.mjs --output OUT.jsonl --summary SUMMARY.json [--limit N]');
    const needs=cms(bin,db,schemas,'team_need',needsNs).filter(x=>x.filled==='unfilled'&&x.status!=='deleted');
    const resources=cms(bin,db,schemas,'external_resource',supplyNs).filter(x=>x.status!=='deleted');
    const result=matchOperatorSupply(needs,resources,{limit});
    const out=createWriteStream(output,{flags:'w',mode:0o600});
    for(const row of result.receipts)if(!out.write(JSON.stringify(row)+'\n'))await once(out,'drain');
    out.end();await once(out,'close');
    await Bun.write(summaryPath,JSON.stringify({schema:'bonfyre-operator-supply-match.v1',needs:needs.length,resources:resources.length,
      eligible_resources:result.receipts.length,summary:result.summaries},null,2)+'\n');
    console.log(JSON.stringify({needs:needs.length,resources:resources.length,eligible_resources:result.receipts.length,output,summary:summaryPath}));
  }catch(e){console.error(e.message);process.exitCode=1}
}
