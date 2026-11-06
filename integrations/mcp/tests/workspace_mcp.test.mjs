import {test,expect,beforeAll,afterAll} from 'bun:test';
import {workspaceFixture,root} from '../../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {freePort,pause} from '../../../cmd/BonfyreWebAddress/tests/auth_continuation_fixture.mjs';
import {createWorkspaceMcp} from '../workspace_mcp.mjs';
import {writeFileSync,openSync,closeSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';

let f,catalogue,rooms,call,gateway,gatewayBase;
const session=user=>decodeURIComponent(user.cookie.slice(user.cookie.indexOf('=')+1));
const key='fixture-plan:mcp-shared';
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
beforeAll(async()=>{
  f=await workspaceFixture();
  catalogue=await f.install('catalogue','institution:workspace-team');rooms=await f.install('rooms','institution:workspace-team');
  call=createWorkspaceMcp({ownerUrl:f.base,publicOrigin:f.base});
  await f.store(catalogue).saveLocal(key,{plan:{id:'mcp-shared',title:'Original plan'},quantity:2,attachment:new Blob(['retained original model'],{type:'model/gltf-binary'})});
  const port=await freePort(),tokenfile=join(f.home,'project.token');writeFileSync(tokenfile,'fixture-project-token');
  const log=openSync(join(f.home,'gateway.log'),'w',0o600);
  gateway=spawn(process.execPath,[join(root,'integrations/mcp/bonfyre_oauth_gateway.mjs')],{env:{...process.env,
    BONFYRE_AUTH_BINARY:f.auth,BONFYRE_AUTH_DB:f.authdb,BONFYRE_CMS_BINARY:f.cms,BONFYRE_CMS_DB:f.db,
    BONFYRE_UPPER90_BINARY:join(root,'cmd/BonfyreUpper90/bonfyre-upper90'),BONFYRE_UPPER90_DB:join(f.home,'upper90.db'),
    BONFYRE_PROJECT_MCP_TOKEN_FILE:tokenfile,BONFYRE_PROJECT_MCP_URL:'http://127.0.0.1:1/mcp',BONFYRE_OAUTH_MCP_PORT:String(port),
    BONFYRE_WORKSPACE_OWNER_URL:f.base,BONFYRE_WORKSPACE_PUBLIC_ORIGIN:f.base},stdio:['ignore',log,log]});closeSync(log);
  gatewayBase=`http://127.0.0.1:${port}`;
  for(let n=0;n<100;n++){if(gateway.exitCode!==null)throw Error(readFileSync(join(f.home,'gateway.log'),'utf8'));try{if((await fetch(gatewayBase+'/auth.md')).ok)return;}catch{}await pause(50);}
  throw Error('Workspace MCP fixture gateway did not start.');
},60000);
afterAll(async()=>{if(gateway&&gateway.exitCode===null){gateway.kill();await Promise.race([new Promise(r=>gateway.once('exit',r)),pause(2000)]);if(gateway.exitCode===null)gateway.kill('SIGKILL');}await f?.close();});
async function mcp(name,args,user=f.owner){
  const response=await fetch(gatewayBase+'/mcp',{method:'POST',headers:{Authorization:'Bearer '+session(user),'Content-Type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name,arguments:args}})});
  expect(response.status).toBe(200);return (await response.json()).result;
}
async function current(){return (await call('workspace_records',{binding:catalogue.id,key},session(f.owner))).record;}
function proposal(record,title){return {binding:catalogue.id,key,expected_revision:record.revision,sha256:record.sha256,edits:[{path:['plan','title'],value:title}]};}

test('the account MCP directory and native UI resolve the same installed products',async()=>{
  const result=await mcp('workspace_list',{context:'institution:workspace-team'});
  expect(result.isError).toBe(false);expect(result.structuredContent.destination).toBe('institution:workspace-team');
  expect(result.structuredContent.workspaces.map(w=>w.binding).sort()).toEqual([catalogue.id,rooms.id].sort());
  for(const w of result.structuredContent.workspaces){expect(w.ready).toBe(true);expect((await f.raw(new URL(w.editor_url).pathname)).status).toBe(200);}
  const denied=await mcp('workspace_list',{context:'institution:workspace-team'},f.other);expect(denied.isError).toBe(true);expect(denied.structuredContent.status).toBe(403);
  expect((await f.raw('/apps/_bindings',null)).status).toBe(403);
},30000);

test('exact records retain source hashes and compatible cross-product links',async()=>{
  const result=await mcp('workspace_records',{binding:catalogue.id,key});expect(result.isError).toBe(false);
  const r=result.structuredContent.record;expect(r.value.plan.title).toBe('Original plan');expect(r.files).toHaveLength(1);expect(r.files[0].sha256).toBe(sha('retained original model'));
  const views=await mcp('workspace_views',{binding:catalogue.id,key,revision:r.revision,release:f.release});expect(views.isError).toBe(false);
  expect(new Set(views.structuredContent.views.map(v=>v.binding))).toEqual(new Set([catalogue.id,rooms.id]));
  for(const view of views.structuredContent.views){const url=new URL(view.url);expect((await f.raw(url.pathname+url.search)).status).toBe(303);}
  const page=await mcp('workspace_records',{binding:rooms.id,prefix:'fixture-plan:'});expect(page.structuredContent.records[0].sha256).toBe(r.sha256);expect(page.structuredContent.records[0].value).toBeUndefined();
},30000);

test('preview changes nothing; confirmed native save reopens in the other product and retains media',async()=>{
  const before=await current(),args=proposal(before,'Reviewed shared plan');
  const preview=await mcp('workspace_preview',args);expect(preview.isError).toBe(false);
  expect((await current()).sha256).toBe(before.sha256);
  expect(preview.structuredContent.changes).toEqual([{path:['plan','title'],before:'Original plan',after:'Reviewed shared plan'}]);
  const saved=await mcp('workspace_update',{...args,preview_sha256:preview.structuredContent.preview_sha256,request_id:'fixture-reviewed-save'});
  expect(saved.isError).toBe(false);expect(saved.structuredContent.record.revision).toBe(before.revision+1);
  const reopened=await f.store(rooms).readVersion(key);expect(reopened.value.plan.title).toBe('Reviewed shared plan');expect(await reopened.value.attachment.text()).toBe('retained original model');
  expect(saved.structuredContent.record.files).toEqual(before.files);
},30000);

test('stale previews, altered confirmation and read-only accounts cannot overwrite drafts',async()=>{
  const before=await current(),args=proposal(before,'Candidate change'),preview=await call('workspace_preview',args,session(f.reader));
  await expect(call('workspace_update',{...args,preview_sha256:preview.preview_sha256,request_id:'reader-refused'},session(f.reader))).rejects.toMatchObject({status:403});
  await expect(call('workspace_update',{...args,preview_sha256:'0'.repeat(64),request_id:'changed-preview'},session(f.owner))).rejects.toMatchObject({status:409});
  const competing=await f.store(rooms).readVersion(key);competing.value.plan.title='Changed in the room editor';await f.store(rooms).saveLocal(key,competing.value,competing.revision);
  const stale=await mcp('workspace_update',{...args,preview_sha256:preview.preview_sha256,request_id:'stale-preview'});expect(stale.isError).toBe(true);expect(stale.structuredContent.status).toBe(409);
  expect((await current()).value.plan.title).toBe('Changed in the room editor');
},30000);

test('scope, file-reference, field-shape and bounds refusals precede writes',async()=>{
  const before=await current(),args=proposal(before,'Safe text');
  for(const edits of [[{path:['attachment'],value:null}],[{path:['plan'],value:'replace object'}],[{path:['absent'],value:'new'}],[{path:['quantity'],value:'wrong type'}],[],Array.from({length:33},()=>({path:['quantity'],value:3})),[{path:['__proto__','polluted'],value:true}],[{path:[],value:1}]])await expect(call('workspace_preview',{...args,edits},session(f.owner))).rejects.toThrow();
  await expect(call('workspace_records',{binding:catalogue.id,key,actor:'user:1'},session(f.owner))).rejects.toThrow();
  await expect(call('workspace_records',{binding:catalogue.id,cursor:key},session(f.owner))).rejects.toThrow();
  await expect(call('workspace_records',{binding:catalogue.id,key},session(f.other))).rejects.toMatchObject({status:403});
  expect((await current()).sha256).toBe(before.sha256);
},30000);

test('revocation between preview and commit and inactive bindings are checked by native owners',async()=>{
  const before=await current(),args=proposal(before,'Revoked writer'),preview=await call('workspace_preview',args,session(f.writer));
  f.authsql.run('UPDATE institution_grants SET active=0 WHERE user_id=? AND institution=?',f.writer.id,'workspace-team');
  await expect(call('workspace_update',{...args,preview_sha256:preview.preview_sha256,request_id:'revoked-writer'},session(f.writer))).rejects.toMatchObject({status:403});
  await f.action('deactivate','catalogue','institution:workspace-team');
  await expect(call('workspace_records',{binding:catalogue.id,key},session(f.owner))).rejects.toMatchObject({status:404});
  expect((await call('workspace_records',{binding:rooms.id,key},session(f.owner))).record.sha256).toBe(before.sha256);
  await f.action('activate','catalogue','institution:workspace-team');
},30000);

test('workspace records retry an interrupted owner read but never need a second write',async()=>{
  let reads=0;
  const retry=createWorkspaceMcp({ownerUrl:f.base,publicOrigin:f.base,request:async(url,options)=>{
    if(options.method==='GET'&&++reads===1)throw Error('transient transport failure');
    return fetch(url,options);
  }});
  const record=(await retry('workspace_records',{binding:catalogue.id,key},session(f.owner))).record;
  expect(record.key).toBe(key);expect(reads).toBe(2);
},30000);

test('a lost response after a real native commit preserves its request identity and never repeats the write',async()=>{
  const before=await current(),args=proposal(before,'Saved despite a lost response'),preview=await call('workspace_preview',args,session(f.owner));let posts=0;
  const interrupted=createWorkspaceMcp({ownerUrl:f.base,publicOrigin:f.base,request:async(url,options)=>{
    const response=await fetch(url,options);if(options.method==='POST'){posts++;await response.arrayBuffer();throw Error('lost response');}return response;
  }});
  await expect(interrupted('workspace_update',{...args,preview_sha256:preview.preview_sha256,request_id:'lost-response'},session(f.owner))).rejects.toMatchObject({status:502,request_id:'lost-response'});
  expect(posts).toBe(1);const saved=await current();expect(saved.revision).toBe(before.revision+1);expect(saved.value.plan.title).toBe('Saved despite a lost response');
},30000);

test('audio and recurring-program drafts use the same account carrier and their own record families',async()=>{
  for(const [pack,family,entry,release,draftKey,value,path,next]of [
    ['audio','creator-audio','audio/',process.env.BONFYRE_AUDIO_WORKSPACE_RELEASE,'draft:creator:generated-tones-v1:imports',{title:'Recording session',samples:new Uint8Array([1,3,5]).buffer},['title'],'Reviewed recording'],
    ['programs','recurring-programs','programs/',process.env.BONFYRE_PROGRAM_WORKSPACE_RELEASE,'draft:recurring-mcp',{name:'Monthly kit',terms:{quantity:2}},['terms','quantity'],3]
  ]){
    expect(release).toMatch(/^[a-f0-9]{64}$/);
    f.pack(pack,pack);f.add('pack_demand',{pack,demand_kind:'workspace',needs:family,preferred_sources:release,fields:entry,status:'published'});
    f.add('pack_demand',{pack,demand_kind:'workspace-record',needs:family,fields:'draft:',status:'published'});
    const installed=await f.install(pack,'institution:workspace-team'),store=f.store(installed);
    await store.saveLocal(draftKey,value);
    const r=(await call('workspace_records',{binding:installed.id,key:draftKey},session(f.owner))).record;
    const args={binding:installed.id,key:draftKey,expected_revision:r.revision,sha256:r.sha256,edits:[{path,value:next}]};
    const preview=await mcp('workspace_preview',args);expect(preview.isError).toBe(false);
    const saved=await mcp('workspace_update',{...args,preview_sha256:preview.structuredContent.preview_sha256,request_id:'product-'+pack});expect(saved.isError).toBe(false);
    const reopened=await store.readVersion(draftKey);expect(path.reduce((v,p)=>v[p],reopened.value)).toBe(next);
    if(pack==='audio')expect([...new Uint8Array(reopened.value.samples)]).toEqual([1,3,5]);
    const views=await mcp('workspace_views',{binding:installed.id,key:draftKey,revision:reopened.revision,release});expect(views.isError).toBe(false);expect(views.structuredContent.views).toHaveLength(1);
    // The same account does not make unrelated product record families join.
    expect((await call('workspace_records',{binding:rooms.id,key:draftKey},session(f.owner))).record).toBeNull();
  }
},30000);

test('personal and household destinations keep their own access even for a Bonfyre administrator',async()=>{
  const own=await f.install('catalogue','self'),household=await f.install('catalogue','household:workspace-home');
  await f.store(own).saveLocal('fixture-plan:personal',{plan:{id:'personal',title:'Personal plan'}});
  const listed=await mcp('workspace_list',{});expect(listed.isError).toBe(false);expect(listed.structuredContent.scope).toBe('self');expect(listed.structuredContent.workspaces.map(w=>w.binding)).toEqual([own.id]);
  const shared=await mcp('workspace_list',{context:'household:workspace-home'});expect(shared.isError).toBe(false);expect(shared.structuredContent.workspaces.map(w=>w.binding)).toEqual([household.id]);
  f.grant(f.other.id,'bonfyre','admin');
  const denied=await mcp('workspace_records',{binding:own.id,key:'fixture-plan:personal'},f.other);expect(denied.isError).toBe(true);expect(denied.structuredContent.status).toBe(403);
  const unrelated=await mcp('workspace_list',{context:'household:workspace-home'},f.other);expect(unrelated.isError).toBe(true);expect(unrelated.structuredContent.status).toBe(403);
  f.authsql.run('UPDATE institution_grants SET active=0 WHERE user_id=? AND institution=?',f.other.id,'bonfyre');
},30000);
