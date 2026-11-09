#!/usr/bin/env -S bun run
// Source-pinned, review-held Journal draft admission. Default mode is read-only.
// Rows are created through BonfyreCMS, never by writing its SQLite tables.
import {createHash} from 'node:crypto';
import {existsSync,lstatSync,readFileSync,statfsSync} from 'node:fs';
import {resolve,join,dirname,isAbsolute} from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {isDeepStrictEqual} from 'node:util';

const NAMESPACE='aurekai-journal';
const JSON_FIELDS=new Set(['semantic_tags','projection_contract']);
const sha=s=>createHash('sha256').update(s).digest('hex');
export class Held extends Error {}

// Quoted multiline bodies, escaped quotes and UTF-8, without parser dependencies.
export function delimited(source,separator=','){
  if(typeof source!=='string'||source.length>16*1024*1024)throw new Held('Invalid or oversized manifest');
  const lines=[];let row=[],field='',quoted=false;
  for(let i=0;i<source.length;i++){
    const c=source[i];
    if(quoted){if(c==='"'&&source[i+1]==='"'){field+='"';i++;}
      else if(c==='"')quoted=false;else field+=c;continue;}
    if(c==='"'){if(field)throw new Held('Malformed quoted manifest');quoted=true;}
    else if(c===separator){row.push(field);field='';}
    else if(c==='\n'){row.push(field);lines.push(row);row=[];field='';}
    else if(c!=='\r')field+=c;
  }
  if(quoted)throw new Held('Unclosed CSV quoted field');
  if(row.length||field){row.push(field);lines.push(row);}
  const header=lines.shift();
  if(!header?.length||new Set(header).size!==header.length)throw new Held('Missing or duplicated manifest headers');
  if(!lines.length||lines.some(r=>r.length!==header.length))throw new Held('Empty or ragged manifest');
  return lines.map(parts=>Object.fromEntries(header.map((name,i)=>[name,parts[i]])));
}
function rows(path,separator=','){return delimited(readFileSync(path,'utf8'),separator);}

export function typedRecord(row){const value={};for(const [key,raw] of Object.entries(row)){
  if(key==='namespace'||raw==='')continue;
  if(key==='position'){
    if(!/^\d+$/u.test(raw)||!Number.isSafeInteger(Number(raw)))throw new Held('Invalid fragment position');
    value[key]=Number(raw);
  }else if(JSON_FIELDS.has(key)){
    try{value[key]=JSON.parse(raw);}catch{throw new Held('Invalid typed contract: '+key);}
  }else value[key]=raw;
}return value;}

export function validateBundle(root,bundle){
  const pages=rows(join(bundle,'cms-site-page.csv')),
        fragments=rows(join(bundle,'cms-site-fragment.csv')),
        receipts=rows(join(bundle,'source-receipts.tsv'),'\t');
  for(const [kind,data,key] of [['site_page',pages,'route'],['site_fragment',fragments,'coordinate']]){
    const ids=data.map(row=>row[key]);
    if(ids.some(x=>!x)||new Set(ids).size!==ids.length)throw new Held('Duplicate or missing '+kind+' identity');
    for(const row of data){
      if(row.namespace!==NAMESPACE||row.status!=='draft')throw new Held('Unsafe namespace or publication state: '+kind);
      typedRecord(row);
    }
  }
  const routes=new Set(pages.map(p=>p.route));
  if(fragments.some(f=>!routes.has(f.page)||f.audience!=='public'))throw new Held('Orphaned or nonpublic fragment');
  const digests=new Map();
  for(const frag of fragments){
    const ref=frag.source_ref||'';
    if(!ref.startsWith('editorial/')||isAbsolute(ref)||ref.split('/').includes('..'))throw new Held('Source escaped editorial root: '+ref);
    const source=resolve(root,ref);
    if(!source.startsWith(resolve(root,'editorial')+'/')||!existsSync(source)||lstatSync(source).isSymbolicLink())throw new Held('Missing or symlinked source: '+ref);
    if(!digests.has(ref))digests.set(ref,sha(readFileSync(source)));
    if(frag.source_digest!==digests.get(ref))throw new Held('Stale original-source digest: '+ref);
  }
  const identities=new Set();
  for(const r of receipts){
    if(!r.subject_id||identities.has(r.subject_id)||r.status!=='draft'||digests.get(r.source_ref)!==r.source_sha256)
      throw new Held('Unverified or repeated source receipt: '+(r.subject_id||''));
    identities.add(r.subject_id);
  }
  for(const page of pages.filter(p=>p.editorial_kind==='article')){
    const slug=page.route.startsWith('/blog/')?page.route.slice(6,-1):'';
    if(!slug||page.route!=='/blog/'+slug+'/')throw new Held('Noncanonical article route');
    const related=fragments.filter(f=>f.page===page.route);
    for(const kind of ['article-body','article-credit','article-presentation'])
      if(related.filter(f=>f.kind===kind).length!==1)throw new Held('Incomplete article '+kind+': '+slug);
    const presentation=typedRecord(related.find(f=>f.kind==='article-presentation')).projection_contract;
    if(!presentation||presentation.review_required!==true||presentation.subject_id!=='post:'+slug||
       presentation.authored_source_sha256!==digests.get('editorial/posts/'+slug+'.md'))
      throw new Held('Unreviewed or unbound art direction: '+slug);
    if(!identities.has('post:'+slug))throw new Held('Missing original-source receipt: '+slug);
  }
  if(receipts.length!==pages.length)throw new Held('Source receipt / page count differs');
  return [['site_page','route',pages],['site_fragment','coordinate',fragments]];
}

async function sqliteReader(){
  if(typeof Bun!=='undefined'){
    const {Database}=await import('bun:sqlite');
    return {open:db=>new Database(db,{readonly:true}),all:(db,sql,...args)=>db.query(sql).all(...args)};
  }
  const {DatabaseSync}=await import('node:sqlite');
  return {open:db=>new DatabaseSync(db,{readOnly:true}),all:(db,sql,...args)=>db.prepare(sql).all(...args)};
}
export async function dbRows(path,kind,key,provider){
  provider ??= await sqliteReader();
  if(!existsSync(path))throw new Held('Canonical CMS database unavailable');
  const db=provider.open(path);
  try{
    const columns=new Set(provider.all(db,'PRAGMA table_info('+kind+')').map(x=>x.name));
    if(!['namespace','status',key].every(x=>columns.has(x)))throw new Held('Incompatible CMS table: '+kind);
    return provider.all(db,'SELECT * FROM '+kind+' WHERE namespace=?',NAMESPACE);
  }finally{db.close();}
}
export function sameRecord(expected,current){
  const typed=typedRecord(expected);
  return Object.entries(expected).every(([name,value])=>{
    if(name==='namespace')return true;
    let existing=current[name];
    if(value==='')return existing===null||existing===undefined||existing==='';
    if(JSON_FIELDS.has(name)&&typeof existing==='string'){
      try{existing=JSON.parse(existing);}catch{return false;}
      return isDeepStrictEqual(existing,typed[name]);
    }
    if(name==='position')return Number(existing)===typed[name];
    return existing===typed[name];
  });
}
export async function createPlan(db,groups,provider){
  const plan=[];
  for(const [kind,key,expected] of groups){
    const current=new Map();
    for(const row of await dbRows(db,kind,key,provider)){
      const array=current.get(row[key])||[];array.push(row);current.set(row[key],array);
    }
    for(const candidate of expected){
      const found=current.get(candidate[key])||[];
      const action=found.length>1?'DUPLICATE':!found.length?'MISSING':found[0].status!=='draft'?'PROTECTED':
        sameRecord(candidate,found[0])?'IDENTICAL':'DRIFT';
      plan.push({kind,key,identity:candidate[key],action,candidate});
    }
  }
  const digest=sha(JSON.stringify(plan.map(x=>[x.kind,x.identity,x.action,typedRecord(x.candidate)])));
  return {plan,digest};
}
function freeBytes(location){const fs=statfsSync(location);return Number(fs.bavail)*Number(fs.bsize);}
function parseArgs(argv){
  const options={root:'.',bundle:'build/publication',schemas:'cmd/BonfyreCMS/content-types',
    cms:'cmd/BonfyreCMS/bonfyre-cms',batchSize:50,minimumFreeMib:2048,applyDrafts:false,expectedPlan:''};
  for(let i=0;i<argv.length;i++){
    const arg=argv[i];if(arg==='--apply-drafts'){options.applyDrafts=true;continue;}
    const name=({'--root':'root','--bundle':'bundle','--db':'db','--schemas':'schemas','--cms':'cms',
      '--batch-size':'batchSize','--minimum-free-mib':'minimumFreeMib','--expected-plan':'expectedPlan'})[arg];
    if(!name||i+1===argv.length)throw new Held('Invalid command option: '+arg);
    const raw=argv[++i];options[name]=name==='batchSize'||name==='minimumFreeMib'?Number(raw):raw;
  }
  if(!options.db)throw new Held('--db is required');return options;
}
export async function execute(opts){
  const root=resolve(opts.root),bundle=resolve(opts.bundle),db=resolve(opts.db);
  const groups=validateBundle(root,bundle),{plan,digest}=await createPlan(db,groups);
  const statuses=['MISSING','IDENTICAL','DRIFT','PROTECTED','DUPLICATE'];
  const counts=Object.fromEntries(statuses.map(status=>[status,plan.filter(x=>x.action===status).length]));
  console.log('NATIVE_JOURNAL_DRAFT_PLAN\t'+digest);
  console.log(statuses.map(x=>x+'='+counts[x]).join('\t'));
  for(const item of plan)if(item.action!=='IDENTICAL')console.log([item.action,item.kind,item.identity].join('\t'));
  if(!opts.applyDrafts)return ['DRIFT','PROTECTED','DUPLICATE'].some(s=>counts[s])?2:0;
  if(!opts.expectedPlan||opts.expectedPlan!==digest)throw new Held('Apply refused: exact current plan digest is required');
  if(['DRIFT','PROTECTED','DUPLICATE'].some(s=>counts[s]))throw new Held('Apply refused: existing record needs owner review');
  if(!Number.isInteger(opts.batchSize)||opts.batchSize<1||opts.batchSize>100||
     !Number.isInteger(opts.minimumFreeMib)||opts.minimumFreeMib<0)throw new Held('Invalid bounded resource settings');
  const floor=opts.minimumFreeMib*1048576;
  if(freeBytes(dirname(db))<floor)throw new Held('Apply refused: insufficient free-space reservation');
  const cms=resolve(opts.cms),schemas=resolve(opts.schemas);
  if(!existsSync(cms)||!existsSync(schemas))throw new Held('Native CMS binary or schema directory unavailable');
  let created=0;
  for(const item of plan){
    if(item.action!=='MISSING'||created>=opts.batchSize)continue;
    if(freeBytes(dirname(db))<floor)throw new Held('Draft admission stopped at free-space reservation');
    const payload=JSON.stringify(typedRecord(item.candidate));
    if(Buffer.byteLength(payload)>256*1024)throw new Held('Oversize native draft: '+item.identity);
    const current=await dbRows(db,item.kind,item.key);
    if(current.some(r=>r[item.key]===item.identity))throw new Held('Concurrent CMS arrival: '+item.identity);
    const result=spawnSync(cms,['entry','create',item.kind,payload,'--ns',NAMESPACE,'--db',db,'--schemas',schemas],
      {encoding:'utf8',timeout:30000,maxBuffer:1024*1024});
    if(result.error||result.status!==0)throw new Held('Native CMS refused '+item.kind+':'+item.identity+' '+String(result.stderr||result.error||'').slice(-250));
    created++;console.log(['CREATED',item.kind,item.identity].join('\t'));
  }
  console.log('DRAFT_ADMISSION\twritten='+created+'\tremaining='+(counts.MISSING-created)+'\tpublication=held');
  return 0;
}
if(process.argv[1]&&resolve(process.argv[1])===resolve(fileURLToPath(import.meta.url))){
  try{const exit=await execute(parseArgs(process.argv.slice(2)));process.exitCode=exit;}
  catch(error){console.error('HELD\t'+String(error.message||error));process.exitCode=2;}
}
