#!/usr/bin/env bun
// WebAddress supplies authenticated coordinates through argv. This adapter talks
// to the existing process, whose account-aware owners admit effects. It never
// opens a task DB, invents authority or resumes STOP.
import {readFileSync} from 'node:fs';
import {homedir} from 'node:os';
import {join} from 'node:path';
import {encode} from '../browser/workspace-msgpack.mjs';
import {readFabricPacket} from './fabric_activity.mjs';

export async function callFabricActivity(input,{host='127.0.0.1',port=3053,token,request=fetch}={}) {
  if(!['127.0.0.1','::1','localhost'].includes(host)||!Number.isSafeInteger(port)||port<1||port>65535||typeof token!=='string'||!token)throw new Error('Fabric native connection unavailable');
  const url=`http://${host==='::1'?'[::1]':host}:${port}/activity`;
  const response=await request(url,{method:'POST',redirect:'error',cache:'no-store',signal:AbortSignal.timeout(8000),headers:{Authorization:`Bearer ${token}`,Accept:'application/msgpack','Content-Type':'application/msgpack'},body:encode([1,input],{maxBytes:65536})});
  if(response.headers.get('content-type')?.split(';')[0]!=='application/msgpack'){try{response.body?.cancel()?.catch(()=>{});}catch{}throw new Error('Fabric native representation unavailable');}
  const packet=await readFabricPacket(response.body);
  if(!Array.isArray(packet)||packet.length!==3||packet[0]!==1||typeof packet[1]!=='boolean'||response.ok!==packet[1])throw new Error('Fabric native reply incomplete');
  return packet;
}
async function main(){
  let packet;
  try{
    const args=process.argv.slice(2),input={};
    if(args.length%2)throw new Error('invalid argv');
    for(let i=0;i<args.length;i+=2){const key=args[i].slice(2);if(args[i]!==`--${key}`||!['operation','actor','subject','scope','task'].includes(key)||Object.hasOwn(input,key)||!args[i+1]&&key!=='task')throw new Error('invalid argv');input[key]=args[i+1];}
    if(input.task==='')delete input.task;
    const reads=['read','model-read','model-capabilities','push-status'];
    const actions=['notification','model-review','model-submit','model-cancel','model-reconcile','model-verify-transfer','push-subscribe','push-unsubscribe','push-ack','push-resolve'];
    if(![...reads,...actions].includes(input.operation)||!input.actor||!input.subject||!input.scope||
       input.task&&!['read','model-read'].includes(input.operation)||input.operation==='model-read'&&!input.task)throw new Error('invalid native coordinates');
    if(actions.includes(input.operation)){
      const body=await readFabricPacket(Bun.stdin.stream(),{limit:65536,timeoutMs:2000});
      if(!Array.isArray(body)||body.length!==2||body[0]!==1)throw new Error('invalid native request');input.payload=body[1];
    }
    const token=readFileSync(process.env.BONFYRE_MCP_TOKEN_FILE||join(homedir(),'.bonfyre/mcp/token'),'utf8').trim();
    packet=await callFabricActivity(input,{token,host:process.env.BONFYRE_FABRIC_HOST||'127.0.0.1',port:Number(process.env.BONFYRE_FABRIC_PORT||3053)});
  }catch{packet=[1,false,{code:'unavailable',message:'The existing Fabric process is unavailable. Work and notification actions were not replayed.'}];}
  await Bun.write(Bun.stdout,encode(packet));
}
if(import.meta.main)await main();
