#!/usr/bin/env bun
// Bounded read projection over one SourceFrontier owner, not a second ledger.
import { Database } from 'bun:sqlite';

const placeholders = n => Array(n).fill('?').join(',');
const batches = (values,size=500) => Array.from({length:Math.ceil(values.length/size)},(_,i)=>values.slice(i*size,(i+1)*size));
const group = (rows,key) => { const out=new Map(); for(const row of rows){const id=row[key];if(!out.has(id))out.set(id,[]);out.get(id).push(row)}return out };
export function compileHierarchy(db,{state='',roots=100,channels=64,identity='',host=''}={}) {
  const rootLimit=Math.max(1,Math.min(Number(roots),1000)),channelLimit=Math.max(1,Math.min(Number(channels),500));
  db.exec('BEGIN');
  try{
    const rootRows=db.query(`SELECT r.root_id,r.host,COALESCE(r.name,'') AS name,COALESCE(r.identity,'') AS identity,
      COUNT(DISTINCT c.family_id) AS family_count,COUNT(DISTINCT c.channel_id) AS channel_count,
      SUM(c.state IN ('admitted','active')) AS admitted_count,SUM(c.virtual) AS virtual_count,COALESCE(SUM(c.contributions),0) AS contributions
      FROM root r JOIN channel c ON c.root_id=r.root_id
      WHERE (?='' OR c.state=?) AND (?='' OR COALESCE(NULLIF(c.identity,''),r.identity,'')=?) AND (?='' OR r.host=?)
      GROUP BY r.root_id ORDER BY SUM(c.state IN ('admitted','active')) DESC,COALESCE(SUM(c.contributions),0) DESC,
      COUNT(DISTINCT c.channel_id) DESC,r.host LIMIT ?`).all(state,state,identity,identity,host,host,rootLimit);
    if(!rootRows.length)return {schema:'bonfyre-source-hierarchy.v1',filters:{state,identity,host},root_count:0,roots:[],cannot_infer:'source hierarchy is topology and provenance, not endorsement, authority, identity equivalence, availability, or permission to fetch'};
    const rootIds=rootRows.map(x=>x.root_id);
    const originRows=[],families=[];
    for(const rootsBatch of batches(rootIds)){
      const rootMarks=placeholders(rootsBatch.length);
      originRows.push(...db.query(`SELECT root_id,label,listings FROM root_origin WHERE root_id IN (${rootMarks}) ORDER BY label`).all(...rootsBatch));
      families.push(...db.query(`SELECT f.family_id,f.root_id,f.protocol,COUNT(c.channel_id) AS channel_count,
      SUM(c.virtual) AS virtual_count,COALESCE(SUM(c.contributions),0) AS contributions
      FROM family f JOIN channel c ON c.family_id=f.family_id
      WHERE f.root_id IN (${rootMarks}) AND (?='' OR c.state=?)
      GROUP BY f.family_id ORDER BY COUNT(c.channel_id) DESC,f.protocol`).all(...rootsBatch,state,state));
    }
    const origins=group(originRows,'root_id');
    const byRoot=group(families,'root_id');
    const familyIds=families.map(x=>x.family_id), byFamily=new Map();
    if(familyIds.length){
      const selected=[];
      for(const familyBatch of batches(familyIds)){
        const familyMarks=placeholders(familyBatch.length);
        selected.push(...db.query(`SELECT * FROM (SELECT c.channel_id,c.family_id,c.address,c.exact_address,c.state,c.material,c.protocol,c.virtual,
        c.census_source,c.identity,c.discovered_from,c.contributions,c.last_contributed,c.truth_level,c.rights,c.credential,
        ROW_NUMBER() OVER (PARTITION BY c.family_id ORDER BY c.contributions DESC,c.virtual ASC,c.first_seen ASC,c.channel_id) AS ordinal
        FROM channel c WHERE c.family_id IN (${familyMarks}) AND (?='' OR c.state=?)) WHERE ordinal<=?
        ORDER BY family_id,ordinal`).all(...familyBatch,state,state,channelLimit));
      }
      const channelIds=selected.map(x=>x.channel_id), originByChannel=new Map(), readersByChannel=new Map();
      if(channelIds.length){
        for(const channelBatch of batches(channelIds)){
          const channelMarks=placeholders(channelBatch.length);
          for(const row of db.query(`SELECT channel_id,label FROM channel_origin WHERE channel_id IN (${channelMarks}) ORDER BY label`).all(...channelBatch)){
          if(!originByChannel.has(row.channel_id))originByChannel.set(row.channel_id,[]);originByChannel.get(row.channel_id).push(row.label);
          }
          for(const row of db.query(`SELECT channel_id,census_source AS source,material FROM channel_census WHERE channel_id IN (${channelMarks}) ORDER BY census_source`).all(...channelBatch)){
          if(!readersByChannel.has(row.channel_id))readersByChannel.set(row.channel_id,[]);readersByChannel.get(row.channel_id).push({source:row.source,material:row.material});
          }
        }
      }
      for(const row of selected){
        const channel={channel_id:row.channel_id,address:row.address,exact_address:row.exact_address,state:row.state,material:row.material,protocol:row.protocol,
          virtual:Boolean(row.virtual),reader:row.census_source||'',readers:readersByChannel.get(row.channel_id)||[],identity:row.identity||'',
          discovered_from:row.discovered_from||'',contributions:row.contributions,last_contributed:row.last_contributed||'',truth_level:row.truth_level||'',
          rights:row.rights||'',credential:row.credential||'',origins:originByChannel.get(row.channel_id)||[]};
        if(!byFamily.has(row.family_id))byFamily.set(row.family_id,[]);byFamily.get(row.family_id).push(channel);
      }
    }
    const output=rootRows.map(row=>({
      ...row,origins:(origins.get(row.root_id)||[]).map(({label,listings})=>({label,listings})),
      families:(byRoot.get(row.root_id)||[]).map(f=>({family_id:f.family_id,protocol:f.protocol,channel_count:f.channel_count,
        virtual_count:f.virtual_count,contributions:f.contributions,visible_channels:(byFamily.get(f.family_id)||[]).length,channels:byFamily.get(f.family_id)||[]})),
    }));
    return {schema:'bonfyre-source-hierarchy.v1',filters:{state,identity,host},root_count:output.length,roots:output,
      cannot_infer:'source hierarchy is topology and provenance, not endorsement, authority, identity equivalence, availability, or permission to fetch'};
  }finally{db.exec('COMMIT')}
}
if(import.meta.main){
  const args={db:'',state:'',roots:'100',channels:'64',identity:'',host:''};
  for(let i=2;i<process.argv.length;i++){const key=process.argv[i].slice(2);if(!Object.hasOwn(args,key))throw Error(`unknown option ${process.argv[i]}`);args[key]=process.argv[++i]}
  if(!args.db)throw Error('--db is required');
  const db=new Database(args.db,{readonly:true});
  try{console.log(JSON.stringify(compileHierarchy(db,args)))}finally{db.close()}
}
