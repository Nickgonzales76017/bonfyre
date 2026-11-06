#!/usr/bin/env bun
// Already-observed material only. A compute plan is not a claim of execution.
import { readFileSync, existsSync } from 'node:fs';
import { Database } from 'bun:sqlite';

const typed={event:'events',place:'places',video:'video',audio:'recordings',photo:'pictures',post:'conversation',story:'publications',publication:'publications'};
const grammar={events:['stage','agenda'],places:['map'],pictures:['gallery'],recordings:['listen'],video:['watch'],conversation:['channels','community'],publications:['context']};
export function computePlan(counts, relationCount, sourceRoots) {
  const total=Object.values(counts).reduce((a,b)=>a+b,0), n=k=>counts[k]||0, jobs=[];
  const add=(job,fanout,operation,backend_candidate)=>jobs.push({job,fanout,operation,backend_candidate});
  add('temporal-phase',n('events'),'classify event phase against now','cpu');
  add('spatial-neighborhood',n('places')*Math.max(1,n('events')),'distance/window fanout',n('places')>=32?'vgpu':'cpu');
  const media=n('pictures')+n('recordings')+n('video');
  add('media-similarity',media*Math.max(0,media-1)/2,'pairwise similarity / clustering',media>=24?'vgpu':'cpu');
  add('conversation-cluster',n('conversation'),'group/thread affinity',n('conversation')>=64?'vgpu':'cpu');
  add('source-diversity',sourceRoots,'publisher/protocol balancing','cpu');
  add('relation-expand',relationCount,'typed relation fanout','cpu');
  add('scene-score',total,'grammar evidence accumulation',total>=128?'vgpu':'cpu');
  return jobs;
}
export function relationRows(path, itemKeys) {
  if(!path||!existsSync(path)||!itemKeys.length)return [];
  const db=new Database(path,{readonly:true});
  try {
    const marks=itemKeys.map(()=>'?').join(',');
    return db.query(`SELECT src,relation,dst,evidence FROM relation WHERE src IN (${marks})`).all(...itemKeys);
  } catch { return []; }
  finally { db.close(); }
}
export function compileStage(items, {subject='stage:public',limit=2048,frontierRootCount=0,relations=[]}={}) {
  const selected=items.slice(0,Math.max(1,limit)), material={},sources=new Set(),identities=new Set();
  for(const item of selected){
    const family=typed[String(item.kind||'').toLowerCase()]||'publications';
    (material[family]??=[]).push(item);
    if(item.source)sources.add(String(item.source));
    if(item.ident)identities.add(String(item.ident));
  }
  const counts=Object.fromEntries(Object.entries(material).map(([k,v])=>[k,v.length]));
  const grammars=['events','places','pictures','recordings','video','conversation','publications'].flatMap(f=>counts[f]?grammar[f]:[]);
  const stageable=(counts.events||0)>0;
  return {schema:'bonfyre.stage-closure.v1',subject,stageable,typed_counts:counts,source_count:sources.size,identity_count:identities.size,
    frontier_root_count:frontierRootCount,grammars:stageable?grammars:grammars.filter(x=>x!=='stage'&&x!=='agenda'),material,relations,
    compute_plan:computePlan(counts,relations.length,frontierRootCount||sources.size),
    cannot_infer:['event material -> attendance, availability, capacity or invitation','place relation -> presence or access','offer-like words -> offer or commitment','gpu candidate -> gpu execution','source hierarchy -> authority or endorsement']};
}
if(import.meta.main){
  const args={items:'','frontier-hierarchy':'','latent-db':'',subject:'stage:public',limit:'2048'};
  for(let i=2;i<process.argv.length;i++){const key=process.argv[i].slice(2);if(!Object.hasOwn(args,key))throw Error(`unknown option ${process.argv[i]}`);args[key]=process.argv[++i]}
  if(!args.items)throw Error('--items is required');
  const items=readFileSync(args.items,'utf8').split(/\r?\n/).filter(Boolean).slice(0,Math.max(1,Number(args.limit))).map(JSON.parse);
  const frontier=args['frontier-hierarchy']&&existsSync(args['frontier-hierarchy'])?JSON.parse(readFileSync(args['frontier-hierarchy'],'utf8')):{};
  const keys=[...new Set(items.flatMap(item=>{const key=String(item.key||''),source=String(item.source||'');return key?[key,source?`${source}:${key}`:key]:[]}))];
  const relations=relationRows(args['latent-db'],keys);
  console.log(JSON.stringify(compileStage(items,{subject:args.subject,limit:Number(args.limit),frontierRootCount:Number(frontier.root_count||0),relations})));
}
