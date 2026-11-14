#!/usr/bin/env bun
import { readSync } from 'node:fs';
import { decode } from '../../integrations/browser/workspace-msgpack.mjs';

// Server-bound transport: no browser-supplied session, actor, worker or argv.
const response=(status,value)=>{
  process.stdout.write(JSON.stringify({schema:'bonfyre.command.dispatch.v1',status,...value})+'\n');
};
// The WebAddress pipe and direct CLI both have the same hard input bound.
function boundedStdin(){
  const parts=[];let total=0;
  for(;;){
    const chunk=Buffer.allocUnsafe(512),count=readSync(0,chunk,0,chunk.length,null);
    if(!count)break;
    total+=count;
    if(total>8192)throw Object.assign(Error('Command packet exceeds 8192 bytes'),{status:400});
    parts.push(chunk.subarray(0,count));
  }
  return new Uint8Array(Buffer.concat(parts,total));
}
let owner;
const accountBound=process.argv.length===3&&process.argv[2]==='--account-bound';
try {
  if(process.argv.length>3||(process.argv.length===3&&!accountBound))
    throw Object.assign(Error('Unsupported native command transport'),{status:400});
  const bytes=boundedStdin();
  if(bytes.length<2||bytes.length>8192)
    throw Object.assign(Error('Invalid command intent size'),{status:400});
  let request,token;
  try {
  if(accountBound){
    const packet=decode(bytes,{maxBytes:8192});
    if(!Array.isArray(packet)||packet.length!==3||packet[0]!==1||
       typeof packet[1]!=='string'||packet[1].length<8||packet[1].length>2048||
       !(packet[2] instanceof Uint8Array)||packet[2].length<2||packet[2].length>4096)
      throw Object.assign(Error('Invalid bound account session'),{status:400});
    token=packet[1];
    request=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(packet[2]));
  }else{
    request=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
    token=request?.token;
  }
  }catch{throw Object.assign(Error('Invalid command intent encoding'),{status:400});}
  const keys=accountBound?
    ['command','context','program','reviewId','sourceSha256']:
    ['command','context','program','reviewId','sourceSha256','token'];
  if(!request||Array.isArray(request)||typeof request!=='object'||
     Object.keys(request).sort().join(',')!==keys.sort().join(',')||
     keys.some(key=>typeof request[key]!=='string'||request[key].length===0||
       request[key].length>512))
    throw Object.assign(Error('Exact command intent required'),{status:400});
  const env=process.env;
  // Pin this subprocess to the same immutable Queue cut as its public owner.
  if(accountBound&&env.BONFYRE_NATIVE_COMMAND_QUEUE_BIN)
    process.env.BONFYRE_QUEUE_BIN=env.BONFYRE_NATIVE_COMMAND_QUEUE_BIN;
  const {openNativeCommandOwner}=await import('./command_owner.mjs');
  owner=openNativeCommandOwner({
    home:env.BONFYRE_COMMAND_REVIEW_HOME,
    authDb:env.BONFYRE_NATIVE_COMMAND_AUTH_DB||env.BONFYRE_AUTH_DB,authBinary:env.BONFYRE_AUTH_BINARY,
    cmsDb:env.BONFYRE_CMS_DB,cmsBinary:env.BONFYRE_CMS_BINARY,
    cmsSchemas:env.BONFYRE_NATIVE_COMMAND_CMS_SCHEMAS||env.BONFYRE_CMS_SCHEMAS,
  });
  const result=owner.dispatch({...request,token});
  response(202,{result});
}catch(error){
  const status=[400,401,403,404,409].includes(error?.status)?error.status:503;
  const message=status===503?'Native command execution owner unavailable':
    String(error.message||'Effect admission refused').slice(0,240);
  response(status,{admitted:false,error:message});
  if(accountBound)process.exitCode=status===400?4:[401,403,404,409].includes(status)?3:6;
}finally{owner?.close();}
