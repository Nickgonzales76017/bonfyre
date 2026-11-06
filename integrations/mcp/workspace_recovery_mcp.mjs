// Account MCP transport for the same reviewed recovery used by Production Studio.
// The current WebAddress session and CMS transaction own destination admission.
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {encode,decode} from '../browser/workspace-msgpack.mjs';
import {prepareRecord,restoreRecord,blobTransport,digest} from '../browser/workspace-value.mjs';
import {verifyProductionDestination} from '../browser/workspace-destinations.mjs';
import {inspectRecoveryMaterial,previewRecoveryWorkflow,commitRecoveryWorkflow,recoverySchema} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace-recovery.mjs';
import {validateRecoveryInstrument} from '../../cmd/BonfyreSurfaceScene/runtime/authored-instrument-validation.mjs';

const maxMaterial=512*1024,maxReview=1024*1024;
const b64Schema=max=>({type:'string',minLength:4,maxLength:4*Math.ceil(max/3),pattern:'^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$'});
const hash={type:'string',pattern:'^[a-f0-9]{64}$'},binding={type:'string',pattern:'^ws-[a-f0-9]{40}$'};
const context={type:'string',pattern:'^(?:self|(?:institution|household):[a-z0-9-]{1,63})$'};
const target={binding,context,release:hash};
const tool=(name,description,properties,write=false)=>({name,description,inputSchema:{type:'object',additionalProperties:false,properties,required:Object.keys(properties)},annotations:{readOnlyHint:!write,destructiveHint:write,openWorldHint:false}});
export const RECOVERY_MCP_TOOLS=[
 tool('workspace_recovery_preview','Inspect one source-backed native recovery MessagePack packet and prepare its exact composition and child records for the currently admitted profile or organization. Requires an installed compatible Production Studio release. Returns the review packet, source lineage, record identities and conflicts. This does not save drafts or execute historical actions.',{...target,material_base64:b64Schema(maxMaterial)}),
 tool('workspace_recovery_import','Attach the exact workspace_recovery_preview to the selected account through the existing native atomic record writer. Rechecks current destination access, editor compatibility, source digest and every child record. Retrying the same review reconciles existing work; it never replaces edited records. This creates editable native work, not external sends, purchases, approvals or publications.',{...target,review_base64:b64Schema(maxReview),preview_sha256:hash,request_id:{type:'string',pattern:'^[a-z0-9][a-z0-9_-]{0,78}$'}},true)
];
const fail=(message,status=400)=>{throw Object.assign(Error(message),{status});};
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const valueDigest=async value=>digest(await prepareRecord(value).encodeWith(async blob=>digest(await blob.arrayBuffer())));
function unpack(text,max){
 if(typeof text!=='string'||text.length>4*Math.ceil(max/3)||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(text))fail('Provide a bounded native MessagePack packet.');
 const bytes=Buffer.from(text,'base64');if(!bytes.length||bytes.length>max||bytes.toString('base64')!==text)fail('The recovery packet is not canonical base64.');
 return decode(bytes,{maxBytes:max});
}
const sameTarget=(left,right)=>left&&['context','id','release','entry'].every(key=>left[key]===right[key])&&Object.keys(left).length===4;
const runtime='../../cmd/BonfyreSurfaceScene/runtime/';
const profileFiles=[['compiler_sha256',runtime+'authored-workspace-recovery.mjs'],['validator_sha256',runtime+'authored-instrument-validation.mjs'],['checks_sha256',runtime+'authored-data-checks.mjs'],['composition_sha256',runtime+'authored-workspace.mjs'],['renderer_sha256',runtime+'renderers/workspace-composition.mjs'],['value_codec_sha256','../browser/workspace-value.mjs'],['msgpack_codec_sha256','../browser/workspace-msgpack.mjs']];
async function readProfile(){
 return Object.fromEntries([['schema',recoverySchema],...await Promise.all(profileFiles.map(async([key,path])=>[key,sha(await readFile(new URL(path,import.meta.url)))]))]);
}
// A mutable checkout cannot make an old imported evaluator advertise a new
// profile. After any source change the process must load that code afresh.
const startupProfile=Object.freeze(await readProfile());
export async function deployedRecoveryProfile(){
 const current=await readProfile();
 if(Object.keys(startupProfile).some(key=>startupProfile[key]!==current[key]))fail('Recovery source changed after this service started. Restart the verified service before importing.',503);
 return {...startupProfile};
}
export function parseRecoveryProfile(text){
 if(typeof text!=='string'||Buffer.byteLength(text)>2048||!text.endsWith('\n'))fail('This editor has no supported recovery profile.',409);
 const rows=text.slice(0,-1).split('\n'),expected=['schema',...profileFiles.map(([key])=>key)];
 if(rows.length!==expected.length)fail('The editor recovery profile is incomplete.',409);
 const profile={};for(const [i,line]of rows.entries()){const fields=line.split('\t');if(fields.length!==2||fields[0]!==expected[i]||i===0&&fields[1]!==recoverySchema||i>0&&!/^[a-f0-9]{64}$/.test(fields[1]))fail('The editor recovery profile changed.',409);profile[fields[0]]=fields[1];}return profile;
}
async function textResponse(response){
 if(!response.ok){await response.body?.cancel();fail('The current editor does not provide the reviewed recovery capability.',response.status===403?403:409);}
 const reader=response.body?.getReader();if(!reader)fail('The editor recovery profile is unavailable.',502);
 const chunks=[];let total=0;try{for(;;){const row=await reader.read();if(row.done)break;if((total+=row.value.length)>2048)fail('The editor recovery profile exceeds its bound.',502);chunks.push(row.value);}}
 catch(error){await reader.cancel().catch(()=>{});throw error;}finally{reader.releaseLock();}
 try{return new TextDecoder('utf-8',{fatal:true}).decode(Buffer.concat(chunks));}catch{fail('The editor recovery profile is not valid text.',502);}
}
export async function callRecoveryMcp(name,args,{exchange,readExact,writeBatch,requestOwner,link}){
 if(!RECOVERY_MCP_TOOLS.some(t=>t.name===name))fail('Unknown recovery operation.');
 args=structuredClone(args);
 const target={context:args.context,id:args.binding,release:args.release,entry:'instruments/production/'},profile=await deployedRecoveryProfile();
 async function verifyTarget(offered){
  if(!sameTarget(offered,target))fail('The reviewed recovery destination changed.',409);
  const admitted=await verifyProductionDestination(target,{request:requestOwner});
  const actual=parseRecoveryProfile(await textResponse(await requestOwner(`/apps/${target.id}/instruments/RECOVERY.tsv`,{method:'GET',redirect:'error',cache:'no-store',signal:AbortSignal.timeout(15000),headers:{Accept:'text/plain'}})));
  if(Object.keys(profile).some(key=>profile[key]!==actual[key]))fail('The installed editor and account recovery service use different validators. Update the editor before importing.',409);
  return admitted;
 }
 const path=(operation,parameters={})=>`/apps/${target.id}/_records?${new URLSearchParams({operation,...parameters})}`;
 const files=blobTransport((operation,packet,parameters={})=>exchange(path(operation,parameters),packet));
 const nativeRow=async row=>row===null?null:row[2]===null?{key:row[0],revision:row[1],deleted:true}:{key:row[0],revision:row[1],value:await restoreRecord(row[2],files.get)};
 const targetStore={
  readVersion:async key=>nativeRow(await readExact(target.id,key)),
  saveLocalBatch:async changes=>{
   if(name!=='workspace_recovery_import')fail('A recovery preview cannot write records.');
   const rows=[];for(const change of changes){if(change.expectedRevision!==0||Object.keys(change).sort().join('|')!=='expectedRevision|key|value')fail('Recovery only creates the reviewed new records.');rows.push([change.key,0,0,await prepareRecord(change.value).encodeWith(files.put)]);}
   const result=await writeBatch(target.id,[1,args.request_id,rows]);
   if(result[2]||result[4].length!==rows.length)fail('The native recovery receipt is incomplete.',502);
   return Promise.all(result[4].map(nativeRow));
  }
 };
 const options={target,targetStore,verifyTarget,digestValue:valueDigest,validateInstrument:validateRecoveryInstrument};
 try{
  if(name==='workspace_recovery_preview'){
   // Check account admission before spending work on private material validation.
   await verifyTarget(target);
   const material=await inspectRecoveryMaterial(unpack(args.material_base64,maxMaterial),options);
   if(material.workflows.length!==1)fail('Preview one complete workflow per account operation. Use the native editor for a larger reviewed batch.');
   const workflow=material.workflows[0],plan=await previewRecoveryWorkflow(workflow,options),bytes=encode(plan,{maxBytes:maxReview});
   return{state:'recovery_preview',binding:target.id,context:target.context,release:target.release,preview_sha256:plan.digest,review_base64:Buffer.from(bytes).toString('base64'),disposition:plan.state,conflicts:plan.conflicts,summary:plan.summary,source:workflow.production.recoveryOrigin,records:await Promise.all(workflow.changes.map(async c=>({key:c.key,expected_revision:0,value_sha256:await valueDigest(c.value)}))),saved_work_url:link(`/apps/${target.id}/_work`)};
  }
  const plan=unpack(args.review_base64,maxReview);
  if(!sameTarget(plan?.target,target)||plan.digest!==args.preview_sha256)fail('The confirmed review or account differs from the requested import.',409);
  const result=await commitRecoveryWorkflow(plan,options),key=result.production.key,revision=result.production.revision;
  return{state:'native_work_attached',disposition:result.state,binding:target.id,context:target.context,release:target.release,request_id:args.request_id,production:{key,revision,value_sha256:await valueDigest(result.production.value)},records:await Promise.all(result.records.map(async row=>({key:row.key,revision:row.revision,value_sha256:await valueDigest(row.value)}))),source:result.production.value.recoveryOrigin,editor_url:link(`/apps/${target.id}/instruments/production/?${new URLSearchParams({record:key,revision:String(revision)})}`),saved_work_url:link(`/apps/${target.id}/_work`),external_actions_executed:false};
 }catch(error){
  if(!error.status)error.status=error.code==='recovery_conflict'||error.code==='recovery_admission_changed'?409:error.code==='recovery_uncertain'||error.code==='recovery_unconfirmed'?502:400;
  if(name==='workspace_recovery_import'){error.request_id=args.request_id;error.reconcile='Keep this exact review and reconcile its composition and every child before retrying.';}throw error;
 }
}
