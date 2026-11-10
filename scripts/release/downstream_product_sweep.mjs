#!/usr/bin/env bun
// Bounded source-to-application deployment sweep. Published contracts only.
// This is not operator resume, a provider install, customer write, or public release.
import {Database} from 'bun:sqlite';
import {readFileSync,existsSync,lstatSync,mkdirSync,writeFileSync,renameSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {join,dirname,resolve} from 'node:path';
import {homedir} from 'node:os';
const root=resolve(import.meta.dir,'../..'),home=homedir(),estate=join(home,'.bonfyre');
const native=join(estate,'bin/bonfyre-surface-scene'),schemas=join(root,'cmd/BonfyreCMS/content-types');
const sourceDb=join(estate,'estate-local/cms.db'),operatorDb=join(estate,'operator/operator.sqlite3');
const glyphSource=join(root,'cmd/BonfyreWebAddress/src/glyph_set.h');
const icons={Newsroom:'book',PublicationFeed:'list',Community:'group',Listen:'headphones',Watch:'play',Weather:'world',Board:'board',Other:'apps'};
const clean=v=>String(v??'').replace(/[\t\r\n\0]+/g,' ').slice(0,600);
const fingerprint=v=>createHash('sha256').update(String(v)).digest('hex');
const regular=p=>{if(!p.startsWith('/')||!existsSync(p)||!lstatSync(p).isFile()||lstatSync(p).isSymbolicLink())throw Error('Missing regular file '+p);return p;};
const parse=v=>{const x=JSON.parse(v||'[]');if(!Array.isArray(x)||x.length>512)throw Error('Unbounded CMS references');return x;};
function options(args){
 const o={apply:false,index:false,live:false,site:'',maximum:32,report:''};
 for(let i=0;i<args.length;i++){
  const k=args[i],v=args[i+1];
  if(k==='--apply')o.apply=true;
  else if(k==='--index-account')o.index=true;else if(k==='--live-check')o.live=true;
  else if(k==='--site'){o.site=v;i++;}
  else if(k==='--max-sites'){o.maximum=Number(v);i++;}
  else if(k==='--report'){o.report=v;i++;}
  else throw Error('Unsupported option '+k);
 }
 if(o.index&&!o.apply)throw Error('Account indexing requires --apply');
 if(o.site&&!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,119}$/.test(o.site))throw Error('Invalid site');
 if(!Number.isSafeInteger(o.maximum)||o.maximum<1||o.maximum>128)throw Error('Bound the sweep to 1..128 sites');
 if(o.report&&!o.report.startsWith(estate+'/'))throw Error('Report must be under private Bonfyre root');
 return o;
}
export function planProductSweep({collections,programs,bindings,glyphs}){
 for(const glyph of Object.values(icons))if(!glyphs.includes('BF_ICON_'+glyph.toUpperCase()))throw Error('Missing compiled native glyph '+glyph);
 const source=new Map();for(const row of collections){if(source.has(row.collection_id))throw Error('Ambiguous source collection '+row.collection_id);source.set(row.collection_id,row);}
 const byBinding=new Map();for(const row of bindings){if(byBinding.has(row.binding_id))throw Error('Ambiguous application binding '+row.binding_id);byBinding.set(row.binding_id,row);}
 const sites=new Set();return programs.map(p=>{if(sites.has(p.site_id))throw Error('Ambiguous published site '+p.site_id);sites.add(p.site_id);
  const declared=parse(p.source_collections),referenced=parse(p.application_bindings),unknown=referenced.filter(x=>typeof x==='string'&&!byBinding.has(x));const material=referenced.map(x=>typeof x==='string'?byBinding.get(x):x).filter(Boolean);
  const slots=material.map(x=>({kind:x.application||x.application_kind||'Other',id:x.slot||x.slot_id||x.binding_id||'',collection:x.collection||x.source_collection||''}));
  const crossSite=material.filter(x=>x.site_id&&x.site_id!==p.site_id).map(x=>'binding:'+x.binding_id);const missing=[...new Set([...declared,...slots.map(x=>x.collection).filter(Boolean)].filter(id=>!source.has(id)||source.get(id).status!=='published').concat(unknown.map(x=>'binding:'+x),crossSite))];
  const unbound=declared.filter(id=>!slots.some(x=>x.collection===id));
  return {site:p.site_id,declared:declared.length,slots:slots.length,missing,unbound,ready:slots.length>0&&missing.length===0&&unbound.length===0,material:slots};
 }).sort((a,b)=>a.site.localeCompare(b.site));
}
function snapshot(site,maximum) {
 regular(sourceDb);regular(operatorDb);regular(glyphSource);
 const db=new Database(sourceDb,{readonly:true}),operator=new Database(operatorDb,{readonly:true});
 try{
  const collections=db.query("SELECT collection_id,status FROM source_collection WHERE namespace='root'").all();
  const programs=db.query("SELECT site_id,source_collections,application_bindings FROM site_program WHERE namespace='root' AND status='published' ORDER BY site_id").all().filter(x=>!site||x.site_id===site);
  if(programs.length>maximum)throw Error('Unbounded published site count');
  const bindings=db.query("SELECT binding_id,site_id,application_kind,source_collection,status FROM application_binding WHERE namespace='root'").all();
  const goals=operator.query("SELECT state,count(*) AS n FROM goals GROUP BY state").all();
  const proof=fingerprint(programs.map(x=>x.site_id+' '+x.source_collections+' '+x.application_bindings).join('\n')+collections.map(x=>x.collection_id+' '+x.status).sort().join('\n'));
  return {plan:planProductSweep({collections,programs,bindings,glyphs:readFileSync(glyphSource,'utf8')}),proof,goals};
 }finally{db.close();operator.close();}
}
function exec(argv,ms=45000){
 const p=Bun.spawnSync(argv,{cwd:root,stdout:'pipe',stderr:'pipe',timeout:ms});
 return {ok:p.exitCode===0,uncertain:!!p.timedOut,stdout:p.stdout.toString(),stderr:p.stderr.toString().slice(0,600)};
}
function present(snap,results){
 const rows=[['kind','site','icon','state','collections','slots','unbound','missing','detail']];
 rows.push(['estate','operator','board','STOP preserved','','','','',snap.goals.map(x=>x.state+':'+x.n).join(' ')]);
 for(const p of snap.plan){
  const outcome=results.find(x=>x.site===p.site);
  rows.push(['site',p.site,'board',outcome?.state||(p.ready?'eligible':'held'),p.declared,p.slots,p.unbound.length,p.missing.length,clean('missing:'+p.missing.join(',')+' unbound:'+p.unbound.join(','))]);
  for(const slot of p.material)rows.push(['app',p.site,icons[slot.kind]||icons.Other,'declared',slot.collection?1:0,1,'','',slot.kind+':'+slot.id+':'+slot.collection]);
 }
 for(const r of results)rows.push(['receipt',r.site,'board',r.state,'','','','',r.digest||r.error||'']);
 rows.push(['snapshot','owner','book','verified','','','','',snap.proof]);
 return rows.map(row=>row.map(clean).join('\t')).join('\n')+'\n';
}
export function main(argv=process.argv.slice(2)){
 const o=options(argv),before=snapshot(o.site,o.maximum),out=[];let failed=false;
 if(o.apply){
   regular(join(estate,'operator/STOP'));regular(native);regular(join(schemas,'surface_scene.json'));
  for(const p of before.plan){
   if(!p.ready){out.push({site:p.site,state:'held_unresolved_source'});continue;}
   const base=['--db',sourceDb,'--ns','root','--site',p.site,'--actor-scope','public','--schemas',schemas,'--scene-schema',join(schemas,'surface_scene.json')];
   const compiled=exec([native,'compile',...base]);
   let c;try{c=JSON.parse(compiled.stdout)}catch{}
   if(!compiled.ok||c?.focus?.site_id!==p.site||!c?.lenses?.length){failed=true;out.push({site:p.site,state:compiled.uncertain?'compile_uncertain':'compile_refused',error:compiled.stderr});break;}
   const synced=exec([native,'sync',...base]);
   let v;try{v=JSON.parse(synced.stdout)}catch{}
   if(!synced.ok||v?.site_id!==p.site||v?.state!=='materialized'||!/^[a-f0-9]{64}$/.test(v?.source_digest||'')){failed=true;out.push({site:p.site,state:synced.uncertain?'outcome_uncertain':'sync_refused',error:synced.stderr});break;}
   out.push({site:p.site,state:'scene_materialized',digest:v.source_digest});
  }
  if(o.index&&!failed&&out.some(x=>x.state==='scene_materialized')){
   const q=join(estate,'bin/bonfyre-queue'),sources=[['operator',operatorDb],['fabric',join(estate,'dev-fabric/fabric.sqlite3')],
    ['workgraph',join(estate,'edge-workgraph/workgraph.db')],['workgraph',join(estate,'estate/native-architecture-workgraph.db')],['workgraph',join(estate,'estate-fabric/fabric.db')]];
   try{
    regular(q);for(const [,path] of sources)regular(path);
    const indexed=exec([q,'account-work','index','--db',join(estate,'state/account-work.db'),'--subject','user:19','--scope','self','--owner','user:19',
     ...sources.flatMap(([key,value])=>['--source',key+'='+value])],45000);
    if(!indexed.ok){failed=true;out.push({site:'user:19',state:indexed.uncertain?'account_index_uncertain':'account_index_failed',error:indexed.stderr});}
    else out.push({site:'user:19',state:'account_index_updated',digest:fingerprint(indexed.stdout)});
   }catch(e){failed=true;out.push({site:'user:19',state:'account_index_preflight_failed',error:e.message});}
  }
 }
 if(o.live){
  try{
   const bun=regular(join(home,'.bun/bin/bun')),qa=regular(join(root,'scripts/live_account_readback.mjs'));
   const witnessed=exec([bun,'--no-env-file','--no-install',qa,'--run'],195000);
   let proof;try{proof=JSON.parse(witnessed.stdout.split('\n').filter(Boolean).at(-1));}catch{}
   const accepted=witnessed.ok&&proof?.passed===true&&proof?.principal===19&&proof?.createdAccounts===0;
   if(!accepted)failed=true;
   out.push({site:'user:19',state:accepted?'signed_in_four_apex_verified':witnessed.uncertain?'live_readback_uncertain':'live_readback_failed',error:witnessed.stderr});
  }catch(error){failed=true;out.push({site:'user:19',state:'live_readback_unavailable',error:error.message});}
 }
 const after=snapshot(o.site,o.maximum);
 if(after.proof!==before.proof){failed=true;out.push({site:'estate',state:'source_changed_during_sweep'});}
 const data=present(after,out);
 if(o.report){mkdirSync(dirname(o.report),{recursive:true,mode:0o700});const temp=o.report+'.tmp-'+process.pid;writeFileSync(temp,data,{mode:0o600,flag:'wx'});renameSync(temp,o.report);}
 else process.stdout.write(data);
 if(failed)process.exitCode=1;
}
if(import.meta.main)try{main();}catch(e){console.error('product sweep refused: '+e.message);process.exitCode=1;}
