import {account} from '../../components/BonfyreServiceSite/cloud.mjs';
import {SCRIPT,QUEUE,WORKFLOW,DLQ,stateRoot} from './cloud-deployment.mjs';
import {readFileSync,writeFileSync,mkdirSync,existsSync} from 'node:fs';
import {join} from 'node:path';
import {line} from '../experience-handoffs/contract.mjs';

const root=stateRoot();
const knownInstances=['w-b2938124b4d4ce939048e24a6fa65e29f04ab15e9366eb5a',
  'w-333ddcdf5bdc98b9bc3026ab856c728c87b09530ca5c1404'];

// Read-only Cloudflare account owner. Never serialize settings values, credentials,
// instance inputs, event signatures, transport packets or native lease secrets.
export async function liveReadback({cloud=account(),instances=knownInstances}={}){
  const installed=JSON.parse(readFileSync(join(root,'deployment.json'),'utf8'));
  const [queue,settings,schedules,workflow]=await Promise.all([
    cloud.api('GET','/queues/'+installed.queueId),
    cloud.api('GET','/workers/scripts/'+SCRIPT+'/settings'),
    cloud.api('GET','/workers/scripts/'+SCRIPT+'/schedules'),
    cloud.api('GET','/workflows/'+WORKFLOW)
  ]);
  const q=queue.result,c=(q.consumers??[]).map(x=>({
    type:x.type,deadLetterQueue:x.dead_letter_queue,
    batchSize:x.settings?.batch_size,maxRetries:x.settings?.max_retries
  }));
  const bindings=new Map((settings.result?.bindings??[]).map(x=>[x.name,x]));
  const ownedSource=bindings.get('SOURCE_URL')?.text==='https://aurekai-work-samples.pages.dev/'&&
    bindings.get('SOURCE_MISSION')?.text==='source:work-samples';
  const watch=bindings.get('SOURCE_WATCH_ENABLED')?.text==='true';
  const flow=workflow.result;
  const statuses=[];
  for(const id of instances){
    if(!/^w-[a-f0-9]{48}$/.test(id))throw Error('Invalid known workflow identity.');
    const instance=(await cloud.api('GET','/workflows/'+WORKFLOW+'/instances/'+id)).result;
    statuses.push({id,status:instance.status,success:instance.success===true,
      verified:instance.steps?.some(s=>s.name.startsWith('verify-native-completion')&&s.success===true)===true});
  }
  const result={
    account:cloud.label,checkedAt:new Date().toISOString(),worker:SCRIPT,
    queue:QUEUE,queueId:q.queue_id,consumer:c,
    workflow:WORKFLOW,workflowBound:flow?.script_name===SCRIPT&&flow?.class_name==='WorkgraphActivation'&&bindings.has('WORKGRAPH_FLOW'),
    ownedSource,watch,schedules:schedules.result?.schedules?.map(x=>x.cron)??[],instances:statuses
  };
  result.valid=result.queueId===installed.queueId&&c.length===1&&c[0].type==='http_pull'&&
    c[0].deadLetterQueue===DLQ&&result.workflowBound&&ownedSource&&watch&&
    result.schedules.includes('*/15 * * * *')&&statuses.every(x=>x.status==='complete'&&x.success&&x.verified);
  return result;
}
export function retainReadback(result){
  const text=['cloudflare_federation_readback v1\n',line('checked',result.checkedAt),
    line('account',result.account),line('worker',result.worker),
    line('workflow',result.workflow),line('queue',result.queue,result.queueId),
    line('consumer',result.consumer[0]?.type??'missing',result.consumer[0]?.deadLetterQueue??'missing'),
    line('scope',result.ownedSource?'owned_work_samples':'refused'),
    line('watch',result.watch?'enabled':'disabled'),
    ...result.schedules.map(x=>line('schedule',x)),
    ...result.instances.map(x=>line('completion',x.id,x.status,x.success&&x.verified?'verified':'unverified')),
    line('readback',result.valid?'passed':'refused')].join('');
  mkdirSync(root,{recursive:true,mode:0o700});
  const file=join(root,'last-live-readback.yaff');
  writeFileSync(file,text,{mode:0o600});
  return file;
}
if(import.meta.main){
  try{const result=await liveReadback();const path=retainReadback(result);
    console.log(JSON.stringify({...result,retainedAt:path}));if(!result.valid)process.exitCode=1;}
  catch(error){console.error('Read-only Cloudflare verification failed: '+error.message);process.exitCode=1;}
}
