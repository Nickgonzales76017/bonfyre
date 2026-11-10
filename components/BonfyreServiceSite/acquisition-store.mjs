import {mkdirSync,existsSync,openSync,closeSync,fsyncSync,writeFileSync,readFileSync,readdirSync,fstatSync,linkSync,unlinkSync,constants} from 'node:fs';
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';

const LIMIT=2*1024*1024, FAMILY='acquisition-workspace', TYPE='acquisition-workspace';
const fail=(status,message,extra={})=>Object.assign(Error(message),{status,...extra});
const digest=value=>createHash('sha256').update(value).digest('hex');
const orgKey=id=>{if(typeof id!=='string'||!/^[a-z0-9][a-z0-9_-]{0,63}$/.test(id))throw fail(400,'Invalid organization identifier.');return id;};
function readBytes(file,limit){
  const fd=openSync(file,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{const stat=fstatSync(fd);if(!stat.isFile()||stat.size>limit)throw Error('Invalid artifact size.');return readFileSync(fd);}finally{closeSync(fd);}
}
// Publish complete bytes without replacing an existing content-addressed artifact.
function immutable(file,bytes){
  const temporary=`${file}.${randomUUID()}.tmp`,fd=openSync(temporary,'wx',0o600);
  try{writeFileSync(fd,bytes);fsyncSync(fd);}finally{closeSync(fd);}
  try{
    try{linkSync(temporary,file);}catch(error){if(error.code!=='EEXIST')throw error;if(!readBytes(file,bytes.length).equals(Buffer.from(bytes)))throw fail(503,'Stored acquisition artifact has changed.');}
    const directory=openSync(resolve(file,'..'),'r');try{fsyncSync(directory);}finally{closeSync(directory);}
  }finally{unlinkSync(temporary);}
}
function jsonClone(value){
  let nodes=0;
  function visit(item,depth){
    if(++nodes>200000||depth>48)throw Error('Snapshot is too complex.');
    if(item===null||typeof item==='string'||typeof item==='boolean')return;
    if(typeof item==='number'&&Number.isFinite(item))return;
    if(typeof item!=='object'||(!Array.isArray(item)&&Object.getPrototypeOf(item)!==Object.prototype&&Object.getPrototypeOf(item)!==null))throw Error('Snapshot must contain JSON values.');
    for(const child of Object.values(item))visit(child,depth+1);
  }
  if(!value||typeof value!=='object'||Array.isArray(value))throw Error('A workspace snapshot is required.');
  visit(value,0);const bytes=JSON.stringify(value);
  if(Buffer.byteLength(bytes)>LIMIT)throw Error('Workspace snapshot exceeds 2 MiB.');
  return JSON.parse(bytes);
}

/** Native WorkGraph owns revisions; files contain immutable artifacts and lease credentials only. */
export function acquisitionStore(home,{validate,hydrate,binary=resolve(import.meta.dir,'../../cmd/BonfyreQueue/bonfyre-queue'),leaseMs=60000}={}){
  if(typeof validate!=='function'||(hydrate!==undefined&&typeof hydrate!=='function'))throw Error('An acquisition workspace validator is required.');
  if(!Number.isSafeInteger(leaseMs)||leaseMs<100||leaseMs>86400000)throw Error('Invalid acquisition lease duration.');
  home=resolve(home);mkdirSync(home,{recursive:true,mode:0o700});
  const db=join(home,'work.db'),root=join(home,'acquisition');
  const mission=org=>`acquisition-workspace-${org}`;
  function queue(...args){
    try{return JSON.parse(execFileSync(binary,[...args,'--db',db],{encoding:'utf8',timeout:10000,maxBuffer:128*1024,stdio:['ignore','pipe','pipe']}));}
    catch(error){let result;try{result=JSON.parse(String(error.stdout));}catch{}
      const conflict=['conflict','stale_claim','not_eligible'].includes(result?.result);
      throw fail(conflict?409:503,conflict?'Workspace changed or is being saved. Reload before saving.':'The native acquisition store refused this operation.',{nativeResult:result?.result,nativeCode:result?.error_code});
    }
  }
  function snapshot(value,status=400){
    try{let next=jsonClone(value);if(hydrate)next=hydrate(next)??next;const result=validate(next);if(result===false||typeof result?.then==='function')throw Error('Synchronous workspace validation is required.');return jsonClone(next);}
    catch(error){throw fail(status,status===400?`Invalid acquisition workspace: ${error.message}`:'Stored acquisition workspace is invalid.');}
  }
  const inputURI=org=>`acquisition-workspace://${org}/itad/v1`;
  function identity(org){
    const node=queue('mission',mission(org));
    if(node.node_id!=='job'||node.operator_id!==`queue.${TYPE}`||node.family!==FAMILY||node.input_uri!==inputURI(org))throw fail(503,'Acquisition workspace identity does not match.');
  }
  function read(org){
    orgKey(org);if(!existsSync(db)){
      // A lost native database must not make existing artifact history look like a fresh workspace.
      if(existsSync(root)&&readdirSync(root,{withFileTypes:true}).some(entry=>entry.isDirectory()&&readdirSync(join(root,entry.name)).some(name=>/^(snapshot-|acquisition-)/.test(name))))throw fail(503,'The native acquisition database is missing.');
      return null;
    }
    let checkpoint;
    try{checkpoint=queue('checkpoint-read',mission(org),'job');}catch(error){if(error.nativeResult==='not_found')return null;throw error;}
    identity(org);
    if(checkpoint.revision===0)return null;
    try{
      if(!Number.isSafeInteger(checkpoint.revision)||checkpoint.revision<1)throw Error('Invalid revision.');
      const name=checkpoint.artifact_uri.match(/\/snapshot-([a-f0-9]{64})\.json$/);
      if(!name)throw Error('Invalid artifact.');
      const file=join(root,org,`snapshot-${name[1]}.json`);
      if(checkpoint.artifact_uri!==pathToFileURL(file).href)throw Error('Artifact outside workspace.');
      const bytes=readBytes(file,LIMIT+4096);
      if(digest(bytes)!==name[1])throw Error('Artifact changed.');
      const artifact=JSON.parse(bytes);
      if(artifact.schema!=='acquisition-workspace/v1'||artifact.orgId!==org||artifact.expectedRevision!==checkpoint.revision-1||!validActor(artifact.actor))throw Error('Artifact identity changed.');
      return {revision:checkpoint.revision,value:snapshot(artifact.value,503)};
    }catch{throw fail(503,'Stored acquisition workspace is missing, changed, or invalid.');}
  }
  function validActor(actor){return typeof actor==='string'&&actor.trim().length>0&&actor.length<=512&&!/[\u0000-\u001f\u007f]/.test(actor);}
  function claim(org){
    const id=mission(org),directory=join(root,org);
    let status=queue('status',id,'job');
    if(status.status==='running'&&status.lease_expires_at_ms<=Date.now()){
      try{queue('reap-expired',id);}catch(error){if(error.nativeCode!=='no_expired_leases')throw error;}
      status=queue('status',id,'job');
    }
    let credential;
    if(status.status==='running'){
      if(!/^acquisition-[a-f0-9-]{36}$/.test(status.worker_id))throw fail(503,'Acquisition workspace has an unexpected lease owner.');
      try{credential=JSON.parse(readBytes(join(directory,`${status.worker_id}.json`),4096));}
      catch(error){if(error.code==='ENOENT')throw fail(409,'Workspace lease is being initialized. Retry saving.');throw fail(503,'Acquisition lease credential is invalid.');}
      if(credential.worker_id!==status.worker_id||!/^[a-f0-9]{64}$/.test(credential.claim_token))throw fail(503,'Acquisition lease credential is invalid.');
      queue('renew',id,'job',credential.worker_id,credential.claim_token,'--lease-ms',String(leaseMs));
    }else{
      credential=queue('claim',id,'job',`acquisition-${randomUUID()}`,'--family',FAMILY,'--lease-ms',String(leaseMs));
      immutable(join(directory,`${credential.worker_id}.json`),JSON.stringify({worker_id:credential.worker_id,claim_token:credential.claim_token}));
    }
    return credential;
  }
  function write(org,actor,expectedRevision,value){
    orgKey(org);
    if(!validActor(actor))throw fail(400,'Invalid acquisition actor.');
    if(!Number.isSafeInteger(expectedRevision)||expectedRevision<0||expectedRevision>=Number.MAX_SAFE_INTEGER)throw fail(400,'Invalid expected workspace revision.');
    const next=snapshot(value),previous=read(org);
    if((previous?.revision??0)!==expectedRevision)throw fail(409,'A newer workspace is saved. Reload before saving.');
    mkdirSync(join(root,org),{recursive:true,mode:0o700});
    queue('enqueue',TYPE,inputURI(org),'--mission',mission(org),'--family',FAMILY,'--if-absent','--retries','1000000');
    identity(org);
    const credential=claim(org),bytes=JSON.stringify({schema:'acquisition-workspace/v1',orgId:org,actor,expectedRevision,value:next});
    const file=join(root,org,`snapshot-${digest(bytes)}.json`);
    immutable(file,bytes);
    const checkpoint=queue('checkpoint-save',mission(org),'job',credential.worker_id,credential.claim_token,'--expected-revision',String(expectedRevision),'--artifact-uri',pathToFileURL(file).href);
    return {revision:checkpoint.revision,value:next};
  }
  return {read,write};
}
