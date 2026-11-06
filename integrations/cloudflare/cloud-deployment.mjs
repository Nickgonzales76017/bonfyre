import {account} from '../../components/BonfyreServiceSite/cloud.mjs';
import {createHmac,createHash} from 'node:crypto';
import {mkdirSync,writeFileSync,readFileSync} from 'node:fs';
import {homedir} from 'node:os';
import {join} from 'node:path';

export const SCRIPT='bonfyre-power-user-continuation',QUEUE='bonfyre-power-user-workgraph',WORKFLOW='bonfyre-workgraph-activation',DLQ=QUEUE+'-dead-letter';
export const stateRoot=()=>join(homedir(),'.bonfyre','edge-workgraph');
export function cloudOwner() {
  const cloud=account();
  // Purpose-separated transport key. The administrative token stays at the account owner.
  const secret=createHmac('sha256',cloud.env.CLOUDFLARE_API_TOKEN).update('Bonfyre/WorkGraph/transport/v1').digest('hex');
  return {cloud,secret};
}
export async function inventory(cloud) {
  const queues=[];
  for(let page=1;page<=20;page++) {
    const r=await cloud.api('GET',`/queues?per_page=100&page=${page}`);queues.push(...r.result);
    if(page>=(r.result_info?.total_pages??1))return queues;
  }
  throw Error('queue inventory is incomplete');
}
export async function ensureQueue(cloud,name) {
  let rows=(await inventory(cloud)).filter(q=>q.queue_name===name);
  if(rows.length>1)throw Error('ambiguous queue');
  if(!rows.length)rows=[(await cloud.api('POST','/queues',{queue_name:name,settings:{message_retention_period:86400}})).result];
  return rows[0];
}
export async function provision(cloud) {
  const queue=await ensureQueue(cloud,QUEUE),dead=await ensureQueue(cloud,DLQ);
  const current=(await cloud.api('GET',`/queues/${queue.queue_id}`)).result;
  if(!current.consumers?.length)await cloud.api('POST',`/queues/${queue.queue_id}/consumers`,{type:'http_pull',dead_letter_queue:DLQ,settings:{batch_size:1,max_retries:5,retry_delay:30,visibility_timeout_ms:60000}});
  const result=(await cloud.api('GET',`/queues/${queue.queue_id}`)).result;
  if(result.consumers?.length!==1||result.consumers[0].type!=='http_pull'||result.consumers[0].dead_letter_queue!==DLQ)throw Error('existing queue consumer needs explicit reconciliation');
  return {queue:result,deadLetter:dead};
}
async function upload(cloud,secret,{bindWorkflow,watch=false}) {
  const modules=['workflow-worker.mjs','workflow-run.mjs','workflow-contract.mjs','workgraph-envelope.mjs','bounded-body.mjs'];
  const settings=(await cloud.api('GET',`/workers/scripts/${SCRIPT}/settings`)).result;
  const owned=new Set(['WORKGRAPH_QUEUE','WORKGRAPH_FLOW','EDGE_SHARED_SECRET','SOURCE_WATCH_ENABLED','SOURCE_URL','SOURCE_MISSION']);
  const bindings=(settings.bindings??[]).filter(x=>!owned.has(x.name));
  if(bindings.some(x=>x.type==='secret_text'))throw Error('unrelated secret binding requires preservation review');
  bindings.push({type:'queue',name:'WORKGRAPH_QUEUE',queue_name:QUEUE},{type:'secret_text',name:'EDGE_SHARED_SECRET',text:secret},
    {type:'plain_text',name:'SOURCE_WATCH_ENABLED',text:watch?'true':'false'},
    {type:'plain_text',name:'SOURCE_URL',text:'https://aurekai-work-samples.pages.dev/'},
    {type:'plain_text',name:'SOURCE_MISSION',text:'source:work-samples'});
  if(bindWorkflow)bindings.push({type:'workflow',name:'WORKGRAPH_FLOW',workflow_name:WORKFLOW,class_name:'WorkgraphActivation'});
  const form=new FormData();
  form.set('metadata',JSON.stringify({main_module:'workflow-worker.mjs',compatibility_date:settings.compatibility_date??'2026-07-26',compatibility_flags:settings.compatibility_flags??[],bindings}));
  const hashes={};
  for(const name of modules){const source=readFileSync(join(import.meta.dir,name),'utf8');hashes[name]=createHash('sha256').update(source).digest('hex');form.set(name,new File([source],name,{type:'application/javascript+module'}));}
  const r=await fetch(`https://api.cloudflare.com/client/v4/accounts/${cloud.env.CLOUDFLARE_ACCOUNT_ID}/workers/scripts/${SCRIPT}`,{method:'PUT',headers:{authorization:`Bearer ${cloud.env.CLOUDFLARE_API_TOKEN}`},body:form,signal:AbortSignal.timeout(30000)});
  const response=await r.json();
  if(!r.ok||response.success!==true)throw Error(`Worker upload refused: HTTP ${r.status}, codes ${(response.errors??[]).map(x=>x.code).join(',')}`);
  return {hashes,deployment:response.result?.deployment_id??response.result?.version_id??null};
}
export async function deploy({watch=false}={}) {
  const {cloud,secret}=cloudOwner(),root=stateRoot(),backup=join(root,'rollback-'+Date.now());
  mkdirSync(backup,{recursive:true,mode:0o700});
  const old={settings:(await cloud.api('GET',`/workers/scripts/${SCRIPT}/settings`)).result,schedules:(await cloud.api('GET',`/workers/scripts/${SCRIPT}/schedules`)).result};
  writeFileSync(join(backup,'worker.multipart'),await cloud.api('GET',`/workers/scripts/${SCRIPT}`,null,true),{mode:0o600});
  writeFileSync(join(backup,'settings.json'),JSON.stringify(old),{mode:0o600});
  const provisioned=await provision(cloud);
  await upload(cloud,secret,{bindWorkflow:false});
  const workflow=(await cloud.api('PUT',`/workflows/${WORKFLOW}`,{script_name:SCRIPT,class_name:'WorkgraphActivation'})).result;
  const installed=await upload(cloud,secret,{bindWorkflow:true,watch});
  const settings=(await cloud.api('GET',`/workers/scripts/${SCRIPT}/settings`)).result;
  const readback=(await cloud.api('GET',`/workflows/${WORKFLOW}`)).result;
  if(readback.script_name!==SCRIPT||readback.class_name!=='WorkgraphActivation'||!settings.bindings.some(x=>x.name==='WORKGRAPH_FLOW'))throw Error('deployment readback mismatch');
  const receipt={at:new Date().toISOString(),account:cloud.label,script:SCRIPT,workflow:WORKFLOW,workflowId:workflow.id,queueId:provisioned.queue.queue_id,consumer:provisioned.queue.consumers[0],deadLetterId:provisioned.deadLetter.queue_id,...installed,watch,backup};
  writeFileSync(join(root,'deployment.json'),JSON.stringify(receipt,null,2)+'\n',{mode:0o600});
  return receipt;
}
export async function setWatch(enabled) {
  const {cloud,secret}=cloudOwner(),watch=enabled===true,file=join(stateRoot(),'deployment.json');
  const prior=JSON.parse(readFileSync(file,'utf8')),installed=await upload(cloud,secret,{bindWorkflow:true,watch});
  const settings=(await cloud.api('GET',`/workers/scripts/${SCRIPT}/settings`)).result;
  if(settings.bindings.find(b=>b.name==='SOURCE_WATCH_ENABLED')?.text!==(watch?'true':'false'))throw Error('watch state readback mismatch');
  const result={...prior,...installed,watch,at:new Date().toISOString()};
  writeFileSync(file,JSON.stringify(result,null,2)+'\n',{mode:0o600});return result;
}
if(import.meta.main) {
  if(process.argv[2]!=='deploy')throw Error('usage: cloud-deployment.mjs deploy [--watch]');
  try {console.log(JSON.stringify(await deploy({watch:process.argv.includes('--watch')})));}
  catch(error) {console.error(error.message);process.exitCode=1;}
}
