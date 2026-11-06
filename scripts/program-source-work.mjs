#!/usr/bin/env bun
// Programs retain source-work associations; SourceFrontier owns every run and result.
import {Database} from 'bun:sqlite';
import {createHash} from 'node:crypto';
import {existsSync,readSync,writeSync} from 'node:fs';
import {isAbsolute} from 'node:path';
import {encode,decode} from '../integrations/browser/workspace-msgpack.mjs';
import {WebWork} from '../cmd/BonfyreSourceFrontier/tools/web_work.mjs';
import {webSpec,WebRefusal} from '../cmd/BonfyreSourceFrontier/tools/web_protocol.mjs';
import {policy} from './compile-program-fabric.mjs';
import {ProgramRefusal,admittedBinding,readProgramSourceContext} from './program-workspace-owner.mjs';

export const programSourceOperations=Object.freeze({
  'source-status':'read','source-read':'read','source-item':'read','source-recover':'read',
  'source-plan':'write','source-start':'write','source-decision':'write'
});
const BASE=['expectedBinding','instance_id'];
const FIELDS={
  'source-status':BASE,
  'source-read':[...BASE,'job_id'],
  'source-item':[...BASE,'job_id','item_id'],
  'source-recover':[...BASE,'request_id'],
  'source-plan':[...BASE,'collection_id','collection_revision','demand_id','request_id','provider','mode','query','limit','characters'],
  'source-start':[...BASE,'job_id','digest','revision'],
  'source-decision':[...BASE,'job_id','item_id','revision','decision','notes']
};
const TABLE='_program_web_work';
const LIMIT=128;
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value)&&!(value instanceof Uint8Array);
const canonical=value=>Array.isArray(value)?value.map(canonical):object(value)?Object.fromEntries(Object.keys(value).sort().map(k=>[k,canonical(value[k])])):value;
const hash=value=>createHash('sha256').update(value).digest('hex');
const digest=value=>hash(encode(canonical(value)));
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const requestKey=value=>typeof value==='string'&&/^[A-Za-z0-9_.:-]{8,100}$/.test(value);
const token=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$/.test(value);
const equal=(a,b)=>object(a)&&Object.keys(a).sort().join(',')==='actor,scope,subject'&&a.actor===b.actor&&a.subject===b.subject&&a.scope===b.scope;
const refuse=(ok,code,message)=>{if(!ok)throw new ProgramRefusal(code,message,4);};
const revision=value=>Number.isSafeInteger(value)&&value>0;

function requestFor(operation,input,binding){
  refuse(Object.hasOwn(programSourceOperations,operation),'invalid_operation','Choose a supported program source operation.');
  refuse(object(input)&&Object.keys(input).sort().join(',')===[...FIELDS[operation]].sort().join(','),'invalid_fields','The source request contains unsupported or missing fields.');
  refuse(equal(input.expectedBinding,binding),'binding_changed','The account destination changed. Reopen this program before continuing.');
  refuse(token(input.instance_id),'invalid_instance_id','Choose an exact program in this workspace.');
  if(Object.hasOwn(input,'job_id'))refuse(hex(input.job_id),'invalid_job','Choose an exact retained source run.');
  if(Object.hasOwn(input,'item_id'))refuse(hex(input.item_id),'invalid_item','Choose an exact retained source result.');
  if(Object.hasOwn(input,'request_id'))refuse(requestKey(input.request_id),'invalid_request','Retain the request identity for this source plan.');
  if(Object.hasOwn(input,'revision'))refuse(revision(input.revision),'invalid_revision','Read the current source work before changing it.');
  if(Object.hasOwn(input,'digest'))refuse(hex(input.digest),'invalid_digest','Use the reviewed source request digest.');
  return input;
}
function tableExists(db){return !!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(TABLE);}
function ensureTable(db){
  db.exec(`CREATE TABLE IF NOT EXISTS ${TABLE}(
    namespace TEXT NOT NULL,instance_id TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,
    collection_id TEXT NOT NULL,demand_id TEXT NOT NULL,collection_revision TEXT NOT NULL,
    request_id TEXT NOT NULL,web_request_id TEXT NOT NULL,request_digest TEXT NOT NULL,job_digest TEXT NOT NULL,
    job_id TEXT,actor TEXT NOT NULL,created_at TEXT NOT NULL,
    PRIMARY KEY(namespace,instance_id,request_id),UNIQUE(namespace,instance_id,job_id))`);
}
function coordinates(binding,instance){return [policy.namespace,binding.subject,binding.scope,instance];}
function rows(db,binding,instance){
  if(!tableExists(db))return [];
  const found=db.query(`SELECT * FROM ${TABLE} WHERE namespace=? AND subject=? AND scope=? AND instance_id=? ORDER BY created_at DESC,request_id LIMIT ?`).all(...coordinates(binding,instance),LIMIT+1);
  refuse(found.length<=LIMIT,'capacity','This program has reached its retained research limit.');
  return found;
}
function byRequest(db,binding,instance,id){
  if(!tableExists(db))return null;
  return db.query(`SELECT * FROM ${TABLE} WHERE namespace=? AND subject=? AND scope=? AND instance_id=? AND request_id=?`).get(...coordinates(binding,instance),id)??null;
}
function collectionView(context){
  const collection=context.source_collection,demands=context.source_demands;
  return {collection_id:collection.collection_id,revision:digest([1,collection,demands]),demands};
}
function relationView(row,binding,job=null){
  const id=job?.id??row.job_id??null,context=binding.scope==='self'?'self':binding.subject;
  return {request_id:row.request_id,job_id:id,digest:row.job_digest,demand_id:row.demand_id,
    collection_id:row.collection_id,created_at:row.created_at,
    href:id?'/sources/web?'+new URLSearchParams({context,job:id}):null};
}
function verifiedJob(job,row,binding){
  refuse(object(job)&&equal(job.binding,binding)&&hex(job.id)&&job.digest===row.job_digest&&(!row.job_id||job.id===row.job_id),
    'source_identity_changed','The retained source run does not match this program request.');
  return job;
}
function recoverJob(owner,row,binding){
  const job=owner.recover(row.web_request_id);
  if(!job){refuse(!row.job_id,'source_unavailable','The previously retained source run is unavailable. Reconcile its original owner.');return null;}
  return verifiedJob(job,row,binding);
}
function findJob(db,owner,binding,instance,id){
  const retained=rows(db,binding,instance),direct=retained.find(row=>row.job_id===id);
  if(direct)return {row:direct,job:verifiedJob(owner.get(id),direct,binding)};
  // A process may stop after SourceFrontier retained the plan but before the
  // association gained its native ID. Recovery only reads that exact request.
  for(const row of retained.filter(row=>!row.job_id)){
    const job=recoverJob(owner,row,binding);
    if(job?.id===id)return {row,job};
  }
  throw new ProgramRefusal('source_not_linked','This source run is not attached to the selected program.',4);
}
function confirmJob(db,binding,instance,row,job){
  db.transaction(()=>{
    readProgramSourceContext(db,binding,instance,{write:true});
    const current=byRequest(db,binding,instance,row.request_id);
    refuse(current&&current.request_digest===row.request_digest&&current.job_digest===job.digest&&(!current.job_id||current.job_id===job.id),
      'source_relation_changed','The program source request changed. Reopen its retained work.');
    if(!current.job_id)db.query(`UPDATE ${TABLE} SET job_id=? WHERE namespace=? AND subject=? AND scope=? AND instance_id=? AND request_id=? AND job_id IS NULL`)
      .run(job.id,...coordinates(binding,instance),row.request_id);
  }).immediate();
  return {...row,job_id:job.id};
}
function planInput(context,request){
  const collection=collectionView(context);
  refuse(request.collection_id===collection.collection_id&&request.collection_revision===collection.revision,
    'source_review_changed','The program source collection changed. Review its current demands.');
  const demand=collection.demands.find(row=>row.id===request.demand_id);
  refuse(!!demand,'source_demand_unavailable','Choose a source demand declared by this program.');
  refuse(['public','account'].includes(request.mode),'invalid_mode','Choose public access or this destination’s stored provider key.');
  const title=(context.instance.template_id+' · '+demand.id).slice(0,500);
  const objective=('Research sources for '+context.instance.template_id+', demand '+demand.id+'. Required: '+demand.need.join(', ')+'. Excluded: '+demand.forbid.join(', ')+'.').slice(0,800);
  const spec=webSpec({provider:request.provider,operation:'search',objective,query:request.query,limit:request.limit,characters:request.characters});
  return {title,spec,mode:request.mode};
}
function configuredWeb(config){
  return {credentialBinary:process.env.BONFYRE_WEB_CREDENTIALS_BINARY,credentialDB:process.env.BONFYRE_WEB_CREDENTIALS_DB,
    credentialKey:process.env.BONFYRE_WEB_CREDENTIALS_KEY,credentialProviders:process.env.BONFYRE_WEB_CREDENTIALS_PROVIDERS,
    canAdminister:false,...config};
}

// The caller supplies identity already resolved by WebAddress. Public request
// fields may identify work but never select a principal or database.
export async function operateProgramSourceWork({operation,binding,db,request,sourceFrontierDB,webConfig={},clock=Date.now,
  createWebWork=(path,options)=>new WebWork(path,options)}={}){
  binding=admittedBinding(binding);
  request=requestFor(operation,request,binding);
  const writing=programSourceOperations[operation]==='write',instance=request.instance_id;
  const initial=readProgramSourceContext(db,binding,instance,{write:writing});
  refuse(typeof sourceFrontierDB==='string'&&isAbsolute(sourceFrontierDB)&&existsSync(sourceFrontierDB),
    'source_owner_unavailable','The retained source owner is unavailable. Existing program work is preserved.');
  const owner=createWebWork(sourceFrontierDB,{...binding,clock,config:configuredWeb(webConfig)});
  const response=value=>({binding,operation,instance_id:instance,...value});
  try{
    if(operation==='source-status'){
      const web=owner.connectionStatus();
      refuse(equal(web.binding,binding),'source_identity_changed','The source destination changed during this read.');
      return response({collection:collectionView(initial),web,runs:rows(db,binding,instance).map(row=>relationView(row,binding))});
    }
    if(operation==='source-recover'){
      const row=byRequest(db,binding,instance,request.request_id);
      if(!row)return response({relation:null,job:null});
      const job=recoverJob(owner,row,binding);
      return response({relation:relationView(row,binding,job),job});
    }
    if(operation==='source-plan'){
      // Store only identity/digest before asking SourceFrontier to retain a plan.
      // The intent makes the cross-owner acknowledgement gap recoverable.
      const input=planInput(initial,request),requestDigest=digest([1,request]),jobDigest=hash(encode([input.title,input.spec,input.mode]));
      let row;
      db.transaction(()=>{
        const current=readProgramSourceContext(db,binding,instance,{write:true});
        planInput(current,request);
        ensureTable(db);row=byRequest(db,binding,instance,request.request_id);
        if(row){refuse(row.request_digest===requestDigest&&row.job_digest===jobDigest,'request_conflict','This request identity already names different research.');return;}
        refuse(rows(db,binding,instance).length<LIMIT,'capacity','This program has reached its retained research limit.');
        const webRequest='program:'+digest([1,policy.namespace,binding.subject,binding.scope,instance,request.request_id]);
        db.query(`INSERT INTO ${TABLE}(namespace,instance_id,subject,scope,collection_id,demand_id,collection_revision,request_id,web_request_id,request_digest,job_digest,job_id,actor,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL,?,?)`)
          .run(policy.namespace,instance,binding.subject,binding.scope,request.collection_id,request.demand_id,request.collection_revision,request.request_id,webRequest,requestDigest,jobDigest,binding.actor,new Date(clock()).toISOString());
        row=byRequest(db,binding,instance,request.request_id);
      }).immediate();
      const job=verifiedJob(owner.plan({...input,requestId:row.web_request_id}),row,binding);
      row=confirmJob(db,binding,instance,row,job);
      return response({relation:relationView(row,binding,job),job});
    }
    const found=findJob(db,owner,binding,instance,request.job_id);
    let {row,job}=found;
    if(operation==='source-read')return response({relation:relationView(row,binding,job),job});
    if(operation==='source-item'){
      const item=owner.item(job.id,request.item_id);
      refuse(item?.id===request.item_id&&typeof item.body==='string'&&hex(item.bodySHA256)&&hash(item.body)===item.bodySHA256&&!item.preview,
        'source_item_changed','The retained source text does not match its digest.');
      return response({relation:relationView(row,binding,job),item});
    }
    row=confirmJob(db,binding,instance,row,job);
    if(operation==='source-start'){
      const current=readProgramSourceContext(db,binding,instance,{write:true});
      // An already-started run is read back even after a collection edit. A new
      // dispatch still requires the exact collection reviewed with its plan.
      if(job.state==='planned')refuse(collectionView(current).revision===row.collection_revision,
        'source_review_changed','The program source collection changed. Save a new reviewed plan before starting.');
      refuse(request.digest===row.job_digest,'source_review_changed','The source request digest changed after review.');
      job=verifiedJob(await owner.start({id:job.id,digest:request.digest,revision:request.revision}),row,binding);
    }else{
      refuse(['unreviewed','shortlist','discard'].includes(request.decision)&&typeof request.notes==='string'&&request.notes.length<=2000,
        'invalid_decision','Choose a source review decision and a note of at most 2,000 characters.');
      job=verifiedJob(owner.decide({id:job.id,item:request.item_id,revision:request.revision,decision:request.decision,notes:request.notes}),row,binding);
    }
    // Recheck the account-owned Program after the native wait. The result remains
    // in SourceFrontier if another editor changed or removed this association.
    readProgramSourceContext(db,binding,instance);
    return response({relation:relationView(row,binding,job),job});
  }finally{owner.close();}
}

function cliOptions(argv){
  const fields=new Map([['--operation','operation'],['--actor','actor'],['--subject','subject'],['--scope','scope'],['--db','db'],['--source-db','sourceFrontierDB']]),options={};
  for(let i=0;i<argv.length;i+=2){const key=fields.get(argv[i]);refuse(key&&i+1<argv.length&&!Object.hasOwn(options,key),'invalid_cli','Use the configured program source owner arguments.');options[key]=argv[i+1];}
  refuse(Object.keys(options).length===fields.size,'invalid_cli','The program source owner configuration is incomplete.');
  return options;
}
function stdin(){
  const parts=[];let total=0;
  for(;;){const block=Buffer.alloc(4096),n=readSync(0,block,0,block.length,null);if(!n)break;total+=n;refuse(total<=32768,'invalid_packet','Submit a source request within 32 KB.');parts.push(block.subarray(0,n));}
  refuse(total>0,'invalid_packet','Submit the reviewed source request.');return Buffer.concat(parts);
}
function packetOut(value){const bytes=encode(value);let at=0;while(at<bytes.length)at+=writeSync(1,bytes,at,bytes.length-at);}
async function main(){
  let db;
  try{
    const options=cliOptions(process.argv.slice(2));
    refuse(Object.hasOwn(programSourceOperations,options.operation),'invalid_operation','Choose a supported program source operation.');
    refuse(isAbsolute(options.db)&&existsSync(options.db),'owner_unavailable','The canonical program store is unavailable.');
    const value=decode(stdin());refuse(Array.isArray(value)&&value.length===2&&value[0]===1&&object(value[1]),'invalid_packet','Use the current program source request format.');
    const binding=admittedBinding(options);
    db=new Database(options.db,programSourceOperations[options.operation]==='write'?{readwrite:true,create:false}:{readonly:true,create:false});
    db.exec('PRAGMA busy_timeout=2000');
    packetOut([1,true,await operateProgramSourceWork({operation:options.operation,binding,db,request:value[1],sourceFrontierDB:options.sourceFrontierDB})]);
  }catch(error){
    const known=error instanceof ProgramRefusal||error instanceof WebRefusal;
    packetOut([1,false,{code:known?error.code:'source_unavailable',message:known?error.message:'The source outcome could not be confirmed. Reopen the same program request before trying again.'}]);
  }finally{db?.close();}
}
if(import.meta.main)await main();
