#!/usr/bin/env bun
import { openNativeCommandOwner } from './command_owner.mjs';

// Private native transport: session and intent IDs arrive on stdin, never in
// shell argv, environment configuration, URLs or command logs.
const response=(status,value)=>{
  process.stdout.write(JSON.stringify({schema:'bonfyre.command.dispatch.v1',status,...value})+'\n');
};
let owner;
try {
  const raw=await Bun.stdin.text();
  if(raw.length<2||raw.length>8192)throw Object.assign(Error('Invalid command intent size'),{status:400});
  const request=JSON.parse(raw);
  const keys=['command','context','program','reviewId','sourceSha256','token'];
  if(!request||Array.isArray(request)||typeof request!=='object'||
     Object.keys(request).sort().join(',')!==keys.sort().join(',')||
     keys.some(key=>typeof request[key]!=='string'||request[key].length===0||
       request[key].length>512))
    throw Object.assign(Error('Exact command intent required'),{status:400});
  const env=process.env;
  owner=openNativeCommandOwner({
    home:env.BONFYRE_COMMAND_REVIEW_HOME,
    authDb:env.BONFYRE_AUTH_DB,authBinary:env.BONFYRE_AUTH_BINARY,
    cmsDb:env.BONFYRE_CMS_DB,cmsBinary:env.BONFYRE_CMS_BINARY,
    cmsSchemas:env.BONFYRE_CMS_SCHEMAS,
  });
  const result=owner.dispatch(request);
  response(202,{result});
}catch(error){
  const status=[400,401,403,404,409].includes(error?.status)?error.status:503;
  const message=status===503?'Native command execution owner unavailable':
    String(error.message||'Effect admission refused').slice(0,240);
  response(status,{admitted:false,error:message});
}finally{owner?.close();}
