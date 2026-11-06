// Account MCP carrier for the existing WebAddress/CMS workspace owner.
// No database, copied draft, inferred actor, or alternate grant evaluator.
import {createHash} from 'node:crypto';
import {encode,decode,maxPacket} from '../browser/workspace-msgpack.mjs';
import {validateArguments} from './http_boundary.mjs';
import {blobTransport} from '../browser/workspace-value.mjs';
import {WORKBOOK_MCP_TOOLS,callWorkbookMcp} from './workbook_mcp.mjs';
import {RECOVERY_MCP_TOOLS,callRecoveryMcp} from './workspace_recovery_mcp.mjs';

const string=(maxLength=511)=>({type:'string',minLength:1,maxLength});
const binding={type:'string',pattern:'^ws-[a-f0-9]{40}$'};
const hash={type:'string',pattern:'^[a-f0-9]{64}$'};
const revision={type:'integer',minimum:1,maximum:2147483646};
const edits={type:'array',minItems:1,maxItems:32,items:{type:'object',additionalProperties:false,required:['path','value'],properties:{
  path:{type:'array',minItems:1,maxItems:16,items:{anyOf:[string(128),{type:'integer',minimum:0,maximum:19999}]}},
  value:{type:['string','number','boolean','null']}
}}};
const change={binding,key:string(),expected_revision:revision,sha256:hash,edits};
const tool=(name,description,properties,required=[],write=false)=>({name,description,inputSchema:{type:'object',additionalProperties:false,properties,required},annotations:{readOnlyHint:!write,destructiveHint:write,openWorldHint:false}});
export const WORKSPACE_MCP_TOOLS=[
  ...WORKBOOK_MCP_TOOLS,
  ...RECOVERY_MCP_TOOLS,
  tool('workspace_list','List installed product editors in one current account destination. Use self or an explicitly chosen institution/household context. Returns real bindings and editor links; inactive or unavailable releases are marked. No account, installation or permission is created.',{context:string(95),pack:string(95)}),
  tool('workspace_records','Read one native saved draft by key, or one bounded page of record identities. Exact reads include plain draft values and file references without downloading media. Retain revision and sha256 for reviewed changes. Use the native cursor and epoch for subsequent pages; records remain owned by their existing product.',{binding,key:string(),prefix:{type:'string',maxLength:511},cursor:{type:'string',maxLength:511},epoch:{type:'integer',minimum:0,maximum:2147483646}},['binding']),
  tool('workspace_views','Resolve the existing compatible editors for one saved record and its current release/revision. Links open that same record, with current account access checked again. This does not copy it into another organization or product.',{binding,key:string(),revision,release:hash},['binding','key','revision','release']),
  tool('workspace_preview','Preview changes to existing scalar fields of a native draft against its exact revision and SHA-256. Preserves file references and record structure. Returns before/after values and a preview digest. Product validation and external actions remain in the owning editor; this preview does not save or execute them.',change,Object.keys(change)),
  tool('workspace_update','Apply the exact workspace_preview changes to one saved draft through its existing native owner. Requires the preview digest and a caller-retained request_id. Current account grants and expected revision are rechecked. Reopen the owning editor for domain validation; this saves draft fields, not orders, publications, grants or payments. On an uncertain response, read the record to reconcile before retrying.',{...change,preview_sha256:hash,request_id:{type:'string',pattern:'^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,78}$'}},[...Object.keys(change),'preview_sha256','request_id'],true)
];
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const fail=(message,status=400)=>{throw Object.assign(Error(message),{status});};
const uint=n=>Number.isInteger(n)&&n>=0&&n<2147483647;
const validBinding=id=>typeof id==='string'&&/^ws-[a-f0-9]{40}$/.test(id);
const validHash=h=>typeof h==='string'&&/^[a-f0-9]{64}$/.test(h);
const arr=(v,n)=>Array.isArray(v)&&v.length===n;
const scalar=v=>v===null||['string','boolean'].includes(typeof v)||typeof v==='number'&&Number.isFinite(v);
const kind=v=>v===null?'null':typeof v;
function bounds(value,schema){
  if(schema.anyOf){
    for(const option of schema.anyOf){try{validateArguments(value,option);bounds(value,option);return;}catch{}}
    fail('Unsupported draft field path.');
  }
  if(typeof value==='string' && (schema.minLength!==undefined&&value.length<schema.minLength || schema.maxLength!==undefined&&Buffer.byteLength(value)>schema.maxLength || schema.pattern&&!new RegExp(schema.pattern).test(value)))fail('A workspace argument is outside its declared bound.');
  if(Array.isArray(value)){
    if(schema.minItems!==undefined&&value.length<schema.minItems||schema.maxItems!==undefined&&value.length>schema.maxItems)fail('A workspace list is outside its declared bound.');
    if(schema.items)for(const item of value)bounds(item,schema.items);
  }else if(value&&typeof value==='object')for(const [key,item]of Object.entries(value))if(schema.properties?.[key])bounds(item,schema.properties[key]);
}

function page(value){
  if(!arr(value,5)||value[0]!==1||!uint(value[1])||typeof value[2]!=='boolean'||typeof value[3]!=='string'||!Array.isArray(value[4])||value[4].length>1000)fail('Invalid native record page.',502);
  const seen=new Set();
  for(const row of value[4]){
    if(!arr(row,3)||typeof row[0]!=='string'||!row[0]||seen.has(row[0])||!uint(row[1])||row[1]<1||row[2]!==null&&!(row[2] instanceof Uint8Array))fail('Invalid native record identity.',502);
    seen.add(row[0]);
  }
  return value;
}
function envelope(body){
  const value=decode(body);
  if(!arr(value,3)||![2,3].includes(value[0])||!Array.isArray(value[2])||value[2].length>64)fail('Unsupported native draft representation.',502);
  for(const ref of value[2])if(!arr(ref,value[0]===3?7:6)||!Array.isArray(ref[0])||!validHash(ref[1])||!Number.isSafeInteger(ref[2])||ref[2]<0)fail('Invalid native file reference.',502);
  return value;
}
function jsonValue(value){
  if(value instanceof Uint8Array)return {$binary:{sha256:sha(value),bytes:value.length}};
  if(Array.isArray(value))return value.map(jsonValue);
  if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,v])=>[key,jsonValue(v)]));
  return value;
}
function describe(row,full=false){
  const [key,revision,body]=row;
  if(body===null)return {key,revision,deleted:true};
  const result={key,revision,sha256:sha(body),bytes:body.length};
  if(full){const [version,value,refs]=envelope(body);result.value=jsonValue(value);result.files=refs.map(ref=>({path:ref[0],sha256:ref[1],bytes:ref[2],mime:ref[3],name:ref[4],last_modified:ref[5],kind:version===3?ref[6]:0}));}
  return result;
}

export function createWorkspaceMcp({ownerUrl='http://127.0.0.1:28901',publicOrigin='https://aurekai.work',request=fetch}={}){
  let workbookActive=0,recoveryActive=0;
  const base=new URL(ownerUrl),publicBase=new URL(publicOrigin);
  const local=['127.0.0.1','localhost','[::1]'].includes(base.hostname);
  if(base.username||base.password||base.pathname!=='/'||base.search||base.hash||!(base.protocol==='https:'||base.protocol==='http:'&&local))throw Error('Use a fixed HTTPS or loopback WebAddress origin.');
  if(publicBase.username||publicBase.password||publicBase.pathname!=='/'||publicBase.search||publicBase.hash||!(publicBase.protocol==='https:'||publicBase.protocol==='http:'&&['127.0.0.1','localhost','[::1]'].includes(publicBase.hostname)))throw Error('Invalid public workspace origin.');
  const link=path=>new URL(path,publicBase).href;
  function requestOwner(session,path,options={}){
    if(typeof session!=='string'||!session||/[\r\n]/.test(session))fail('A current account session is required.',403);
    const url=new URL(path,base);if(url.origin!==base.origin||url.username||url.password||!url.pathname.startsWith('/apps/'))fail('Use the fixed native workspace owner.');
    const headers=new Headers(options.headers);headers.set('Authorization','Bearer '+session);
    return request(url,{...options,headers,redirect:'error',cache:'no-store',credentials:'omit',signal:options.signal??AbortSignal.timeout(20000)});
  }
  async function exchange(session,path,packet=null){
    if(typeof session!=='string'||!session||/[\r\n]/.test(session))fail('A current account session is required.',403);
    let response;const isRead=packet===null,body=isRead?undefined:encode(packet);
    // Only retry an idempotent GET; an uncertain POST must never replay.
    for(let attempt=0;attempt<(isRead?2:1);attempt++){
      try{response=await request(new URL(path,base),{method:isRead?'GET':'POST',redirect:'error',cache:'no-store',signal:AbortSignal.timeout(20000),headers:{Authorization:`Bearer ${session}`,Accept:'application/msgpack',...(isRead?{}:{Origin:base.origin,'Content-Type':'application/msgpack'})},body});}
      catch{if(isRead&&!attempt)continue;fail('The workspace owner response was not received. Reconcile current saved state before repeating a write.',502);}
      if(isRead&&!attempt&&(response.status===502||response.status===503)){await response.body?.cancel().catch(()=>{});continue;}
      break;
    }
    if(!response.ok){await response.body?.cancel();fail(response.status===409?'The draft or installation changed. Re-read it before editing.':response.status===403?'Current account access does not allow this workspace operation.':'The native workspace did not confirm this operation.',response.status);}
    if(response.headers.get('content-type')?.split(';')[0]!=='application/msgpack'){await response.body?.cancel();fail('The workspace owner returned an unexpected representation.',502);}
    const reader=response.body?.getReader();if(!reader)fail('The native workspace response is empty.',502);
    const chunks=[];let total=0;
    try{for(;;){const part=await reader.read();if(part.done)break;if((total+=part.value.length)>maxPacket)fail('The native workspace response exceeds its bound.',502);chunks.push(part.value);}}
    catch(error){await reader.cancel().catch(()=>{});throw error;}finally{reader.releaseLock();}
    const bytes=new Uint8Array(total);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}return decode(bytes);
  }
  const recordPath=(id,params)=>`/apps/${id}/_records?${new URLSearchParams(params)}`;
  async function exact(session,id,key){
    const result=page(await exchange(session,recordPath(id,{operation:'get',prefix:key})));
    if(result[2]||result[4].length>1||result[4].some(r=>r[0]!==key))fail('The native lookup changed record identity.',502);
    return result[4][0]??null;
  }
  async function preview(session,args){
    const row=await exact(session,args.binding,args.key);
    if(!row||row[2]===null||row[1]!==args.expected_revision||sha(row[2])!==args.sha256)fail('The selected draft is no longer the reviewed version.',409);
    const value=envelope(row[2]),changes=[],seen=new Set();
    for(const edit of args.edits){
      const path=edit.path,key=JSON.stringify(path);
      if(!scalar(edit.value)||seen.has(key)||path.some(p=>['__proto__','constructor','prototype'].includes(p)))fail('Choose distinct existing scalar draft fields.');seen.add(key);
      if(value[2].some(ref=>JSON.stringify(ref[0])===key))fail('File references must be changed in their owning editor.');
      let parent=value[1];
      for(let n=0;n<path.length;n++){
        const part=path[n];
        if(!parent||typeof parent!=='object'||parent instanceof Uint8Array||!Object.hasOwn(parent,part)|| (Array.isArray(parent)?!Number.isInteger(part)||part<0:typeof part!=='string'))fail('The selected draft field does not exist.');
        if(n===path.length-1){const before=parent[part];if(!scalar(before)||kind(before)!==kind(edit.value))fail('Preserve the existing scalar field type.');changes.push({path,before,after:edit.value});parent[part]=edit.value;}
        else parent=parent[part];
      }
    }
    const body=encode(value,{maxBytes:256*1024});
    const preview_sha256=sha(encode([1,args.binding,args.key,args.expected_revision,args.sha256,body]));
    return {row,body,result:{binding:args.binding,key:args.key,expected_revision:args.expected_revision,source_sha256:args.sha256,preview_sha256,changes,file_references_preserved:value[2].length,saved_work_url:link(`/apps/${args.binding}/_work`),state:'draft_preview'}};
  }
  async function commit(session,args,body){
    // The native request ledger and revision transaction own commit/replay.
    // No transparent retry: a lost response must be reconciled by exact read.
    let result;
    try{
      result=page(await exchange(session,recordPath(args.binding,{operation:'write'}),[1,args.request_id,[[args.key,args.expected_revision,0,body]]]));
      const row=result[4][0];
      if(result[2]||result[4].length!==1||row[0]!==args.key||row[1]!==args.expected_revision+1||!(row[2] instanceof Uint8Array)||sha(row[2])!==sha(body))fail('The native save receipt does not match the requested draft; reconcile its current version.',502);
    }
    catch(error){error.request_id=args.request_id;error.reconcile='Read the saved record before retrying; a failed response alone does not prove no write.';throw error;}
    return {binding:args.binding,request_id:args.request_id,record:describe(result[4][0],true),state:'draft_saved',saved_work_url:link(`/apps/${args.binding}/_work`)};
  }
  return async function call(name,args={},session){
    const definition=WORKSPACE_MCP_TOOLS.find(t=>t.name===name);if(!definition)fail('Unknown workspace operation.');
    validateArguments(args,definition.inputSchema);
    bounds(args,definition.inputSchema);
    if(args.binding&&!validBinding(args.binding))fail('Invalid workspace binding.');
    if(RECOVERY_MCP_TOOLS.some(tool=>tool.name===name)){
      if(recoveryActive>=2)fail('The recovery service is busy. Continue after the current operation finishes.',503);
      recoveryActive++;
      try{return await callRecoveryMcp(name,args,{
        exchange:(path,packet=null)=>exchange(session,path,packet),
        readExact:(id,key)=>exact(session,id,key),
        writeBatch:async(id,packet)=>page(await exchange(session,recordPath(id,{operation:'write'}),packet)),
        requestOwner:(path,options)=>requestOwner(session,path,options),link
      });}finally{recoveryActive--;}
    }
    if(WORKBOOK_MCP_TOOLS.some(tool=>tool.name===name)){
      if(workbookActive>=2)fail('The workbook service is busy. Retry after a current operation finishes.',503);
      workbookActive++;
      try{
        const files=blobTransport((operation,packet,parameters={})=>exchange(session,recordPath(args.binding,{operation,...parameters}),packet));
        return await callWorkbookMcp(name,args,{read:()=>exact(session,args.binding,args.key),files,commit:body=>commit(session,args,body),savedWorkUrl:link(`/apps/${args.binding}/_work`)});
      }finally{workbookActive--;}
    }
    if(name==='workspace_list'){
      const result=await exchange(session,'/apps/_bindings?'+new URLSearchParams({context:args.context??'self',...(args.pack?{pack:args.pack}:{})}));
      if(!arr(result,4)||result[0]!==1||typeof result[1]!=='string'||!['self','institution','household'].includes(result[2])||!Array.isArray(result[3])||result[3].length>64)fail('Invalid native workspace directory.',502);
      const workspaces=result[3].map(r=>{
        if(!arr(r,6)||!validBinding(r[0])||!validHash(r[3])||!r.slice(1,5).every(x=>typeof x==='string')||!/^([A-Za-z0-9_-]+\/)+$/.test(r[2])||typeof r[5]!=='boolean')fail('Invalid native editor binding.',502);
        return {binding:r[0],pack:r[1],release:r[3],label:r[4],ready:r[5],editor_url:r[5]?link('/apps/'+r[0]+'/'+r[2]):null,saved_work_url:r[5]?link('/apps/'+r[0]+'/_work'):null};
      });return {destination:result[1],scope:result[2],workspaces};
    }
    if(name==='workspace_records'){
      if(args.key!==undefined&&['prefix','cursor','epoch'].some(k=>args[k]!==undefined))fail('Use an exact key or page coordinates, not both.');
      if(args.cursor&&!Number.isInteger(args.epoch))fail('Continue a page with its observed epoch.');
      if(args.key!==undefined){const row=await exact(session,args.binding,args.key);return {binding:args.binding,record:row?describe(row,true):null};}
      const result=page(await exchange(session,recordPath(args.binding,{operation:'read',prefix:args.prefix??'',cursor:args.cursor??'',epoch:String(args.epoch??-1)})));
      let cursor=args.cursor??'';for(const row of result[4]){if(!row[0].startsWith(args.prefix??'')||row[0]<=cursor||row[2]===null)fail('The native record page is inconsistent.',502);cursor=row[0];}
      if(args.epoch!==undefined&&result[1]!==args.epoch||result[4].length&&result[3]!==cursor||result[2]&&!result[4].length)fail('The native record page changed.',409);
      return {binding:args.binding,epoch:result[1],more:result[2],cursor:result[3],records:result[4].map(r=>describe(r))};
    }
    if(name==='workspace_views'){
      const result=await exchange(session,`/apps/${args.binding}/_views?${new URLSearchParams({key:args.key,revision:String(args.revision),release:args.release,repr:'msgpack'})}`);
      if(!arr(result,9)||result[0]!==1||result[1]!==args.binding||result[5]!==args.key||result[6]!==args.revision||!validHash(result[7])||!Array.isArray(result[8])||result[8].length>64)fail('Invalid native record continuation.',502);
      const views=result[8].map(r=>{if(!arr(r,6)||!validBinding(r[0])||!validHash(r[4])||!r.every(x=>typeof x==='string')||!r[5].startsWith(`/apps/${r[0]}/_open?`))fail('Invalid native continuation link.',502);return {binding:r[0],pack:r[1],label:r[2],view:r[3],release:r[4],url:link(r[5])};});
      return {binding:args.binding,destination:result[2],scope:result[3],key:args.key,revision:args.revision,sha256:result[7],views};
    }
    const proposed=await preview(session,args);
    if(name==='workspace_preview')return proposed.result;
    if(args.preview_sha256!==proposed.result.preview_sha256)fail('The confirmed preview does not match this change.',409);
    return commit(session,args,proposed.body);
  };
}
