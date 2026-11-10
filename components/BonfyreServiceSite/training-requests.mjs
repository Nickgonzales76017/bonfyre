import {mkdirSync,readFileSync,writeFileSync,renameSync,readdirSync,openSync,fsyncSync,closeSync} from 'node:fs';
import {resolve,join,dirname} from 'node:path';
import {pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
import {randomBytes,createHash,timingSafeEqual} from 'node:crypto';
import {checkRoutine} from './training-model.mjs';
import {requestRoutine} from './training-draft.mjs';
const idPattern=/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
const hash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const field=(value,name,max)=>{if(typeof value!=='string'||!value.trim()||value.length>max||/[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(value))throw Error(`${name} is invalid.`);return value.trim();};
const closed=(value,allowed)=>{if(!value||typeof value!=='object'||Object.keys(value).some(k=>!allowed.includes(k)))throw Error('Unexpected request field.');};
function atom(value){if(typeof value!=='string'||!/^[a-zA-Z0-9:_-]{1,100}$/.test(value))throw Error('Invalid worker name.');return value;}
function atomic(path,value){const temporary=path+'.'+randomBytes(8).toString('hex');const fd=openSync(temporary,'wx',0o600);try{writeFileSync(fd,JSON.stringify(value,null,2)+'\n');fsyncSync(fd);}finally{closeSync(fd);}renameSync(temporary,path);const parent=openSync(dirname(path),'r');try{fsyncSync(parent);}finally{closeSync(parent);}}
export function requestStore(home,material,{binary=resolve(import.meta.dir,'../../cmd/BonfyreQueue/bonfyre-queue'),programResolver=()=>null}={}){
  home=resolve(home);mkdirSync(home,{recursive:true,mode:0o700});const db=join(home,'work.db');
  const queue=(...args)=>{try{return JSON.parse(execFileSync(binary,[...args,'--db',db],{encoding:'utf8',timeout:10000,maxBuffer:128*1024}));}catch(error){let result;try{result=JSON.parse(error.stdout);}catch{}throw Object.assign(Error(result?.error_message||'The native work owner refused this operation.'),result?.result==='conflict'?{status:409}:{});}};
  const directory=id=>{if(!idPattern.test(id))throw Error('Invalid request ID.');return join(home,id);};
  const load=id=>{const dir=directory(id);try{return {dir,record:JSON.parse(readFileSync(join(dir,'request.json'),'utf8'))};}catch{throw Error('Request not found.');}};
  const mission=id=>'training-request-'+id;
  function inspect(id){
    const {dir,record}=load(id),node=queue('mission',mission(id));
    if(node.node_id!=='job'||node.operator_id!=='queue.training-program'||node.family!==`training-${record.brief.creatorId}`||node.input_uri!==pathToFileURL(join(dir,'request.json')).href)throw Error('Native work does not match this request.');
    return {dir,record,node};
  }
  function publicStatus(id){
    const {dir,record,node}=inspect(id);let delivery=null;
    if(node.status==='complete'){
      const name=node.output_uri.slice(pathToFileURL(dir).href.length+1);if(!/^delivery-[a-f0-9]{64}\.json$/.test(name))throw Error('Delivery reference does not match.');
      const file=join(dir,name);if(node.output_uri!==pathToFileURL(file).href)throw Error('Delivery reference does not match.');
      delivery=JSON.parse(readFileSync(file,'utf8'));if(hash(delivery.routine)!==delivery.sha256||`delivery-${hash(delivery)}.json`!==name)throw Error('Delivery changed after completion.');checkRoutine(delivery.routine);
    }
    const status=queue('status',mission(id),'job');
    return {id,creatorId:record.brief.creatorId,brief:record.brief,submittedAt:record.submittedAt,state:node.status==='running'&&status.lease_expires_at_ms<=Date.now()?'attention':node.status,receiptId:node.receipt_id,delivery};
  }
  function submit(input){
    closed(input,['id','creatorId','programId','title','details']);const dir=directory(input.id),creator=material.creators.find(c=>c.id===input.creatorId),program=creator?.routines.find(r=>r.id===input.programId)??(creator?programResolver(creator.id,input.programId):null);
    if(!creator||!program)throw Error('Choose a program from this studio.');
    checkRoutine(program);if(program.source?.creator!==creator.id)throw Error('Program creator does not match.');
    const brief={creatorId:creator.id,programId:program.id,programVersion:program.source.version,title:field(input.title,'Request title',160),details:field(input.details,'Request details',4000)};
    let record;
    try{mkdirSync(dir,{mode:0o700});record={schema:'bonfyre.training-request.v1',id:input.id,brief,briefSha256:hash(brief),programSnapshot:structuredClone(program),submittedAt:new Date().toISOString(),accessToken:randomBytes(32).toString('hex')};atomic(join(dir,'request.json'),record);}
    catch(error){if(error.code!=='EEXIST')throw error;record=load(input.id).record;if(record.briefSha256!==hash(brief))throw Error('This request ID already names a different brief.');}
    queue('enqueue','training-program',pathToFileURL(join(dir,'request.json')).href,'--mission',mission(input.id),'--family',`training-${creator.id}`,'--if-absent','--retries','3');
    return {...publicStatus(input.id),accessToken:record.accessToken};
  }
  function read(id,token){
    const {record}=load(id);if(typeof token!=='string'||!/^[a-f0-9]{64}$/.test(token)||!timingSafeEqual(Buffer.from(token),Buffer.from(record.accessToken)))throw Error('Request receipt is not valid.');
    return publicStatus(id);
  }
  function accept(id,worker,leaseMs=86400000){
    atom(worker);if(!Number.isSafeInteger(leaseMs)||leaseMs<1000||leaseMs>86400000)throw Error('Choose a claim duration up to 24 hours.');
    const {dir,record}=inspect(id);const claim=queue('claim',mission(id),'job',worker,'--family',`training-${record.brief.creatorId}`,'--lease-ms',String(leaseMs));
    atomic(join(dir,'claim.json'),claim);return {id,worker,state:claim.status,leaseExpiresAt:claim.lease_expires_at_ms};
  }
  function ownedClaim(dir,expectedWorker){const claim=JSON.parse(readFileSync(join(dir,'claim.json'),'utf8'));if(expectedWorker!==undefined&&claim.worker_id!==atom(expectedWorker))throw Error('This request is assigned to another coach.');return claim;}
  function renew(id,expectedWorker){const {dir}=inspect(id),claim=ownedClaim(dir,expectedWorker);const result=queue('renew',mission(id),'job',claim.worker_id,claim.claim_token,'--lease-ms','86400000');return {id,state:result.status,leaseExpiresAt:result.lease_expires_at_ms};}
  function deliver(id,raw,expectedWorker,expectedRevision){
    const {dir,record,node}=inspect(id);if(node.status!=='running')throw Error('Claim the request before delivering it.');
    const claim=ownedClaim(dir,expectedWorker);
    const routine=requestRoutine(raw,id,record.brief.creatorId);
    checkRoutine(routine);const delivery={routine,sha256:hash(routine),deliveredAt:new Date().toISOString()},file=join(dir,`delivery-${hash(delivery)}.json`);
    // Immutable output paths prevent a racing or stale worker from replacing an acknowledged delivery.
    if(!readdirSync(dir).includes(file.slice(dir.length+1)))atomic(file,delivery);
    queue('ack',mission(id),'job',claim.worker_id,claim.claim_token,'--output-uri',pathToFileURL(file).href,...(expectedRevision===undefined?[]:['--checkpoint-revision',String(expectedRevision)]));
    return publicStatus(id);
  }
  function list(creatorId){if(!material.creators.some(c=>c.id===creatorId))throw Error('Unknown studio.');return readdirSync(home).filter(id=>idPattern.test(id)).map(id=>{const {record}=load(id);return record.brief.creatorId===creatorId?publicStatus(id):null;}).filter(Boolean);}
  function creatorRead(id,creatorId){const {record}=inspect(id);if(record.brief.creatorId!==creatorId)throw Error('Request not found.');const status=queue('status',mission(id),'job');return {...publicStatus(id),worker:status.worker_id||null,leaseExpiresAt:status.lease_expires_at_ms};}
  function draftRead(id,worker){
    const {dir,record}=inspect(id);ownedClaim(dir,worker);const saved=queue('checkpoint-read',mission(id),'job');let routine=null;
    if(saved.revision){
      const name=saved.artifact_uri.slice(pathToFileURL(dir).href.length+1);
      if(!/^draft-[a-f0-9]{64}\.json$/.test(name)||saved.artifact_uri!==pathToFileURL(join(dir,name)).href)throw Error('Draft reference does not match.');
      const artifact=JSON.parse(readFileSync(join(dir,name),'utf8'));
      if(artifact.requestId!==id||`draft-${hash(artifact)}.json`!==name)throw Error('Saved draft changed.');
      routine=requestRoutine(artifact.routine,id,record.brief.creatorId,{draft:true});
    }
    const original=record.programSnapshot??material.creators.find(c=>c.id===record.brief.creatorId)?.routines.find(r=>r.id===record.brief.programId&&r.source.version===record.brief.programVersion);
    return {revision:saved.revision,savedAt:saved.created_at_ms||null,routine,sourceProgram:original?requestRoutine(original,id,record.brief.creatorId):null};
  }
  function saveDraft(id,raw,worker,expectedRevision){
    if(!Number.isSafeInteger(expectedRevision)||expectedRevision<0)throw Error('Draft revision is invalid.');
    const {dir,record}=inspect(id),claim=ownedClaim(dir,worker),routine=requestRoutine(raw,id,record.brief.creatorId,{draft:true});
    const artifact={requestId:id,routine},file=join(dir,`draft-${hash(artifact)}.json`);atomic(file,artifact);
    const result=queue('checkpoint-save',mission(id),'job',claim.worker_id,claim.claim_token,'--expected-revision',String(expectedRevision),'--artifact-uri',pathToFileURL(file).href);
    return {revision:result.revision,savedAt:result.created_at_ms,routine};
  }
  function deliverDraft(id,worker,revision){
    if(!Number.isSafeInteger(revision)||revision<1)throw Error('Save and review a draft before delivering.');
    const draft=draftRead(id,worker);
    if(draft.revision!==revision)throw Object.assign(Error('A newer draft is saved. Reload it before delivering.'),{status:409});
    return deliver(id,draft.routine,worker,revision);
  }
  return {submit,read,accept,renew,deliver,list,creatorRead,draftRead,saveDraft,deliverDraft};
}
