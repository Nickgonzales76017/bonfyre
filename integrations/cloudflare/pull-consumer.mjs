import {cloudOwner,stateRoot,WORKFLOW} from './cloud-deployment.mjs';
import {checkPacket,packetFor,workflowId,signCompletion,hash} from './workflow-contract.mjs';
import {admission} from './workgraph-admission.mjs';
import {nonceStore} from './native-nonce-store.mjs';
import {queueOwner,nativeMission} from './native-queue-adapter.mjs';
import {materializeSourceExperience} from './source-experience-projection.mjs';
import {prepareSourceExperienceHandoffs} from './source-experience-admission.mjs';
import {readFileSync,writeFileSync,mkdirSync,existsSync,createReadStream} from 'node:fs';
import {createHash} from 'node:crypto';
import {join,resolve} from 'node:path';

const repo=resolve(import.meta.dir,'../..');
export const SOURCE='https://aurekai-work-samples.pages.dev/';
const sourceAllowed=mission=>/^source:work-samples(?::canary-[0-9]+)?$/.test(mission);
export async function fileDigest(path) {const h=createHash('sha256');for await(const b of createReadStream(path,{highWaterMark:1048576}))h.update(b);return h.digest('hex');}
export async function observeSource({root=stateRoot(),expected=null}={}) {
  const dir=join(root,'source');mkdirSync(dir,{recursive:true,mode:0o700});
  if(expected) {
    const kept=join(dir,expected+'.html'),receipt=join(dir,expected+'.receipt.json');
    if(existsSync(kept)&&existsSync(receipt)&&await fileDigest(kept)===expected) {
      const r=JSON.parse(readFileSync(receipt,'utf8'));
      if(r.source_url===SOURCE&&r.content_sha256===expected&&r.policy_decision==='allow')return {digest:expected,body:kept,receipt,reused:true};
    }
  }
  const stem='read-'+process.pid+'-'+Date.now(),body=join(dir,stem+'.html'),receipt=join(dir,stem+'.receipt.json');
  const p=Bun.spawn([join(repo,'cmd/BonfyreProject/bonfyre-project'),'foreign','observe','edge-work-samples',SOURCE,'--receipt',receipt,'--body',body,'--max-bytes','1048576'],{cwd:repo,env:{...process.env,PATH:'/Users/nickgonzales/.bonfyre/bin:/Users/nickgonzales/.bun/bin:/opt/homebrew/bin:/usr/bin:/bin'},stdout:'ignore',stderr:'ignore'});
  const timer=setTimeout(()=>p.kill(),30000);let exit;try{exit=await p.exited;}finally{clearTimeout(timer);}
  if(exit!==0||!existsSync(body)||!existsSync(receipt))throw Error('native source observation failed');
  const r=JSON.parse(readFileSync(receipt,'utf8')),digest=await fileDigest(body);
  if(r.policy_decision!=='allow'||r.http_status!==200||r.source_url!==SOURCE||r.content_sha256!==digest)throw Error('source observation readback failed');
  if(expected&&digest!==expected)throw Error('source revision changed; review a new activation');
  // Content-addressed public material is shared across retries; no per-request corpus copy.
  const kept=join(dir,digest+'.html'),record=join(dir,digest+'.receipt.json');
  if(!existsSync(kept))await Bun.write(kept,Bun.file(body));
  if(!existsSync(record))writeFileSync(record,JSON.stringify({...r,body_path:kept})+'\n',{mode:0o600});
  return {digest,body:kept,receipt:record,reused:false};
}
export async function activate({canary=false}={}) {
  const {cloud,secret}=cloudOwner(),source=await observeSource();
  const mission=canary?'source:work-samples:canary-'+Date.now():'source:work-samples';
  const {id,packet}=await packetFor({mission,digest:source.digest},secret);
  let status;try {status=(await cloud.api('POST',`/workflows/${WORKFLOW}/instances`,{instance_id:id,params:packet})).result;}
  catch(error) {const old=(await cloud.api('GET',`/workflows/${WORKFLOW}/instances/${id}`)).result;if(!old)throw error;status=old;}
  if(status.id&&status.id!==id)throw Error('Cloudflare assigned a different instance identity');
  const result={id,mission,digest:source.digest,source,created:status};
  writeFileSync(join(stateRoot(),'last-activation.json'),JSON.stringify(result,null,2)+'\n',{mode:0o600});
  return result;
}
function decode(message) {
  if(typeof message.body!=='string'||message.body.length>8192)throw Error('invalid cloud body');
  const type=message.metadata?.['CF-Content-Type'];
  if(type==='text')return message.body;
  if(type==='json'||type==='bytes')return Buffer.from(message.body,'base64').toString('utf8');
  throw Error('unrecognized queue content type');
}
export async function consumeOne({injectFailure=false,worker='edge-native-'+process.pid}={}) {
  const {cloud,secret}=cloudOwner(),root=stateRoot(),deployment=JSON.parse(readFileSync(join(root,'deployment.json'),'utf8'));
  const db=process.env.BONFYRE_WORKGRAPH_DB||join(root,'workgraph.db');
  const owner=queueOwner({binary:join(repo,'cmd/BonfyreQueue/bonfyre-queue'),db});
  const replay=nonceStore(join(root,'transport.db'));
  const receive=admission({secret,replay,enqueue:owner.enqueue,maxAgeMs:86400000});
  const path=`/queues/${deployment.queueId}/messages`;
  const pulled=(await cloud.api('POST',path+'/pull',{batch_size:1,visibility_timeout_ms:60000})).result;
  const result={at:new Date().toISOString(),pulled:pulled.messages?.length??0,backlog:pulled.message_backlog_count,completed:[],retried:[]};
  try {
    for(const message of pulled.messages??[]) {
      let claim,intent;
      try {
        const wire=decode(message),packet=JSON.parse(wire);intent=await checkPacket(packet,secret);
        if(!sourceAllowed(intent.mission))throw Error('unapproved source operation');
        const admitted=await receive(wire);
        if(admitted.status==='pending')throw Error('native admission pending');
        const id=nativeMission(intent);let row=(await owner.read(id))[0];
        if(row.status!=='complete') {
          if(row.status==='running') {
            // Reap only this mission; never take another worker's unexpired claim.
            try {await owner.run(['reap-expired',id]);}catch{}
          }
          [claim]=await owner.run(['claim',id,'job',worker,'--family','source-sync','--lease-ms','60000']);
          if(injectFailure&&intent.mission.includes(':canary-')&&!existsSync(join(root,id+'.fault-injected'))) {
            writeFileSync(join(root,id+'.fault-injected'),'canary transient failure\n',{mode:0o600});
            throw Error('canary transient failure before source verification');
          }
          const source=await observeSource({expected:intent.digest});
          const projection=materializeSourceExperience({source,root});
          const output=projection.uri;
          const checkpoint=(await owner.run(['checkpoint-read',id,'job']))[0];
          const saved=(await owner.run(['checkpoint-save',id,'job',worker,claim.claim_token,'--expected-revision',String(checkpoint.revision),'--artifact-uri',output]))[0];
          await owner.run(['ack',id,'job',worker,claim.claim_token,'--checkpoint-revision',String(saved.revision),'--output-uri',output]);
          claim=null;row=(await owner.read(id))[0];
        }
        if(row.status!=='complete'||!row.output_uri||row.input_uri!=='urn:sha256:'+intent.digest)throw Error('native completion readback failed');
        const checkpoint=(await owner.run(['checkpoint-read',id,'job']))[0];
        if(checkpoint.revision<1||checkpoint.artifact_uri!==row.output_uri)throw Error('completion checkpoint does not match output');
        // Persist the exact native readback; an empty optional receipt_id is never invented.
        const readback=JSON.stringify({schema:'bonfyre.workgraph.completion-readback.v1',row,checkpoint});
        const receiptDigest=createHash('sha256').update(readback).digest('hex'),receipts=join(root,'receipts');
        mkdirSync(receipts,{recursive:true,mode:0o700});
        const receiptFile=join(receipts,receiptDigest+'.json');
        if(!existsSync(receiptFile))writeFileSync(receiptFile,readback,{mode:0o600});
        const flow=await workflowId(intent),complete={version:1,workflow:flow,mission:intent.mission,digest:intent.digest,nativeMission:id,node:'job',receipt:'urn:sha256:'+receiptDigest,output:row.output_uri,status:'complete',at:Date.now()};
        const signed=await signCompletion(complete,secret);
        let current=(await cloud.api('GET',`/workflows/${WORKFLOW}/instances/${flow}`)).result;
        if(current.status!=='complete')await cloud.api('POST',`/workflows/${WORKFLOW}/instances/${flow}/events/native-completion`,signed);
        for(let n=0;n<5&&current.status!=='complete';n++){await Bun.sleep(350);current=(await cloud.api('GET',`/workflows/${WORKFLOW}/instances/${flow}`)).result;}
        const last=current.steps?.find(s=>s.name.startsWith('verify-native-completion')&&s.success);
        const witnessed=typeof last?.output==='string'?JSON.parse(last.output):last?.output;
        if(current.status!=='complete'||witnessed?.receipt!==complete.receipt||witnessed?.nativeMission!==id||witnessed?.digest!==intent.digest)throw Error('cloud completion readback pending or mismatched');
        await cloud.api('POST',path+'/ack',{acks:[{lease_id:message.lease_id}],retries:[]});
        result.completed.push({...complete,cloudAttempts:message.attempts,nativeAttempt:row.attempt});
      } catch(error) {
        if(claim?.claim_token)try {await owner.run(['fail',claim.mission_id,claim.node_id,claim.worker_id,claim.claim_token,'--class','transient','--message','edge source operation needs retry']);}catch{}
        await cloud.api('POST',path+'/ack',{acks:[],retries:[{lease_id:message.lease_id,delay_seconds:injectFailure?1:30}]});
        result.retried.push({mission:intent?.mission??null,cloudAttempts:message.attempts,error:String(error.message).slice(0,250)});
      }
    }
  } finally {replay.close();}
  writeFileSync(join(root,'last-consume.json'),JSON.stringify(result,null,2)+'\n',{mode:0o600});
  if(result.completed.length)writeFileSync(join(root,'last-completed.json'),JSON.stringify(result,null,2)+'\n',{mode:0o600});
  return result;
}
if(import.meta.main) {
  const command=process.argv[2];
  if(!['activate','consume'].includes(command))throw Error('usage: pull-consumer.mjs activate [--canary] | consume [--canary-failure]');
  try {
    const result=command==='activate'?await activate({canary:process.argv.includes('--canary')}):await consumeOne({injectFailure:process.argv.includes('--canary-failure')});
    console.log(JSON.stringify(result));
    if(result.retried?.length)process.exitCode=1;
  }
  catch(error) {console.error(error.message);process.exitCode=1;}
}
