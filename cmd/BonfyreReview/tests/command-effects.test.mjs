import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, copyFileSync, writeFileSync, rmSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { spawnSync } from 'node:child_process';
import { packFixture, repo } from '../../BonfyreCMS/tests/pack_lifecycle_fixture.mjs';
import { nativeReview } from '../review.mjs';
import { commandBindings, reviewedCommandEffects, readCommandIntent } from '../command_effects.mjs';
import { queue as nativeQueue } from '../../../components/BonfyreControlPlane/obligation-work.mjs';

const sha=v=>createHash('sha256').update(v).digest('hex');
const program=(command,pack='media-pack')=>({
  program_id:'media-'+command, application_kind:'native-command',action:command,
  consumes:{kind:'source-library'},produces:{kind:'workspace-record'},
  carrier_candidates:[{kind:'native-workgraph',type:command,operator_id:'queue.'+command}],
  capability_requirements:{installed_pack:pack,workspace_family:'media-studio'},
  authority_requirement:'actor+pack+independent-review',
  transition_conditions:{review:{experience:'media-review',institution:'media-team',
    approved_family:'media-reviewed',executor:'BonfyreQueue',
    artifact_kind:'immutable-review-decision'}},
  failure_projection:'refuse-with-owner',status:'published',
});
const content=fields=>'command_effect_intent v1\n'+[
  'command','program','pack','pack_digest','subject','workspace_binding',
  'workspace_release','operator','input_sha256'
].map(key=>'  '+key+' '+fields[key]).join('\n')+'\n';

function fixture(run) {
  const f=packFixture();
  let review;
  try {
    for(const type of ['application_binding','capability_program'])
      copyFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),
        join(f.schemas,type+'.json'));
    const migrated=spawnSync(f.binary,['schema','migrate','--db',f.db,
      '--schemas',f.schemas],{encoding:'utf8'});
    if(migrated.status!==0)throw Error('CMS schema migration: '+migrated.stderr);
    f.pack('media-pack');
    const release=sha('media-workspace-release');
    f.add('pack_demand',{pack:'media-pack',demand_kind:'workspace',needs:'media-studio',
      preferred_sources:release,fields:'media-editor/',status:'published'});
    f.add('capability_program',program('transcribe'));
    const plan=f.call('plan','media-pack',{actor:'user:1'});
    if(plan.code!==0)throw Error('plan: '+plan.raw+' '+plan.stderr);
    const installed=f.call('install','media-pack',{actor:'user:1',digest:plan.digest});
    if(installed.code!==0)throw Error('install: '+installed.raw+' '+installed.stderr);
    const activated=f.call('activate','media-pack',{actor:'user:1',
      revision:Number(installed.revision),digest:installed.pinned_digest});
    if(activated.code!==0||activated.state!=='active')
      throw Error('activate: '+activated.raw+' '+activated.stderr);
    const sourceRoot=join(f.home,'source');mkdirSync(sourceRoot);
    const grants=new Map([['author',{userId:1,actor:'actor:author'}],
      ['reviewer',{userId:2,actor:'actor:reviewer'}]]);
    const auth={
      principal:token=>{const user=grants.get(token);if(!user)throw Error('revoked ActorClosure');return user;},
      allowed:(token,institution,role)=>grants.has(token)&&institution==='media-team'&&
        ['read','write'].includes(role),
      members:()=>({members:[],next_user_id:null})
    };
    const home=join(f.home,'reviews');
    review=nativeReview({home,auth,experiences:[{
      experience:'media-review',institution:'media-team',approvedFamily:'media-reviewed',
      sourceRoot}],queue:nativeQueue});
    const workspaceCurrent=({pack,subject})=>{
      const row=f.sql.query(`SELECT w.binding_id AS binding,w.release,a.status
         FROM _application_workspace w JOIN application_binding a ON a.id=w.cms_id
         JOIN pack_installation i ON i.id=w.installation_id
         WHERE i.pack=? AND i.installed_for=?`).get(pack,subject);
      return row??null;
    };
    const packCurrent=({pack,subject,scope,actor})=>{
      const result=f.call('current',pack,{subject,scope,actor});
      if(result.code!==0)return null;
      return {state:result.state,digest:result.digest,
        pinned_digest:result.pinned_digest};
    };
    const programs=f.sql.query('SELECT * FROM capability_program').all().map(row=>({
      ...row,carrier_candidates:JSON.parse(row.carrier_candidates),
      capability_requirements:JSON.parse(row.capability_requirements),
      transition_conditions:JSON.parse(row.transition_conditions)
    }));
    const workerState={state:'ready'};
    const workerCurrent=({operator,program,packDigest,workspaceBinding})=>({
      state:workerState.state,operator_id:operator,program_id:program,
      workspace_binding:workspaceBinding,
      pack_digest:plan.digest,binary_sha256:sha('native-worker-fixture')
    });
    const binder=()=>reviewedCommandEffects({programs,review,auth,packCurrent,
      workspaceCurrent,workerCurrent,queue:nativeQueue,queueDb:join(home,'work.db')});
    return run({f,auth,grants,review,binder,programs,workerState,
      workspaceCurrent,packCurrent,sourceRoot,release,plan,content});
  }finally{review?.close();f.close();}
}
test('CMS capability + PackGraph + independent approval admit the exact native Queue operator',()=>fixture(
  ({f,review,binder,workspaceCurrent,packCurrent,sourceRoot,release,plan,workerState,grants})=>{
    const current=packCurrent({pack:'media-pack',subject:'user:1',scope:'self',actor:'user:1'});
    const workspace=workspaceCurrent({pack:'media-pack',subject:'user:1'});
    expect(current.state).toBe('active');
    expect(workspace.status).toBe('active');
    expect(workspace.release).toBe(release);
    const fields={command:'transcribe',program:'media-transcribe',pack:'media-pack',
      pack_digest:current.digest,subject:'user:1',workspace_binding:workspace.binding,
      workspace_release:workspace.release,operator:'queue.transcribe',
      input_sha256:sha('source-library-input')};
    const original=content(fields),file=join(sourceRoot,'intent.yaff');
    writeFileSync(file,original);
    const submitted=review.submit({token:'author',experience:'media-review',
      reviewId:'media-case',sourceUri:pathToFileURL(file).href,
      sourceSha256:sha(original),requestKey:'submit-media'});
    expect(submitted.state).toBe('pending');
    expect(()=>binder().dispatch({token:'author',command:'transcribe',
      program:'media-transcribe',context:'self',reviewId:'media-case',
      sourceSha256:sha(original)})).toThrow();
    expect(()=>review.decide({token:'author',reviewId:'media-case',action:'approve',
      expectedRevision:1,requestKey:'self-approval'})).toThrow();
    const approved=review.decide({token:'reviewer',reviewId:'media-case',
      action:'approve',expectedRevision:1,requestKey:'reviewer-approval'});
    expect(approved.state).toBe('approved');
    expect(approved.downstream.at(-1).admitted).toBe(true);
    const request={token:'author',command:'transcribe',program:'media-transcribe',
      context:'self',reviewId:'media-case',sourceSha256:sha(original)};
    const admitted=binder().dispatch(request);
    expect(admitted).toMatchObject({admitted:true,executed:false,
      command:'transcribe',program:'media-transcribe',pack:'media-pack',
      subject:'user:1',operator_id:'queue.transcribe',
      pack_digest:current.digest,workspace_binding:workspace.binding});
    expect(admitted.review_mission).toBe(approved.downstream.at(-1).mission);
    expect(binder().dispatch(request).mission_id).toBe(admitted.mission_id);
    // No CLI lookup, random argv or caller's own approval can redirect the job.
    expect(()=>binder().dispatch({...request,command:'pay'})).toThrow();
    expect(()=>binder().dispatch({...request,program:'media-narrate'})).toThrow();
    expect(()=>binder().dispatch({...request,context:'institution:other'})).toThrow();
    expect(()=>binder().dispatch({...request,sourceSha256:sha('other')})).toThrow();
    workerState.state='offline';
    expect(()=>binder().dispatch(request)).toThrow();
    workerState.state='ready';
    grants.delete('author');
    expect(()=>binder().dispatch(request)).toThrow();
    grants.set('author',{userId:1,actor:'actor:author'});
    // Native CMS retained-source digests invalidate installed capability.
    f.sql.query("UPDATE pack_demand SET needs='changed' WHERE pack='media-pack'").run();
    expect(()=>binder().dispatch(request)).toThrow();
    f.sql.query("UPDATE pack_demand SET needs='media-studio' WHERE pack='media-pack'").run();
    // An old approval does not authorize a changed source intent.
    writeFileSync(file,original.replace('queue.transcribe','queue.narrate'));
    expect(()=>binder().dispatch(request)).toThrow();
  }
));
test('declarations must be unique, typed and independently reviewed',()=>{
  const base=program('transcribe');
  expect(commandBindings([base]).get('transcribe')).toMatchObject({
    command:'transcribe',pack:'media-pack',operator:'queue.transcribe',
    executor:'BonfyreQueue',approvedFamily:'media-reviewed'});
  expect(()=>commandBindings([base,base])).toThrow(/Ambiguous/);
  expect(()=>commandBindings([{...base,authority_requirement:'api-key'}])).toThrow();
  expect(()=>commandBindings([{...base,carrier_candidates:[{kind:'native-workgraph',
    type:'pay',operator_id:'queue.pay'}]}])).toThrow();
  expect(()=>commandBindings([{...base,transition_conditions:{review:{
    ...base.transition_conditions.review,executor:'local-shell'}}}])).toThrow();
  expect(commandBindings([{...base,status:'draft'}]).size).toBe(0);
});
test('source proof rejects aliases, reordered fields and unpinned bytes',()=>fixture(
  ({sourceRoot,content})=>{
    const candidate={command:'transcribe',program:'media-transcribe',
      pack:'media-pack',pack_digest:sha('pack'),subject:'user:1',
      workspace_binding:'ws-'+sha('workspace').slice(0,40),
      workspace_release:sha('release'),operator:'queue.transcribe',
      input_sha256:sha('source')};
    const file=join(sourceRoot,'standalone.yaff'),bytes=content(candidate),uri=pathToFileURL(file).href;
    writeFileSync(file,bytes);
    expect(readCommandIntent(uri,sha(bytes))).toEqual(candidate);
    expect(()=>readCommandIntent(uri,sha('different'))).toThrow();
    writeFileSync(file,bytes.replace('  operator','  obsolete'));
    expect(()=>readCommandIntent(uri,sha(bytes))).toThrow();
    const rearranged=bytes.replace('  command transcribe\n  program media-transcribe\n',
      '  program media-transcribe\n  command transcribe\n');
    writeFileSync(file,rearranged);
    expect(()=>readCommandIntent(uri,sha(rearranged))).toThrow();
  }
));
test('account-bound native command transport rejects forged browser authority before owner access',async()=>{
  const {encode}=await import('../../../integrations/browser/workspace-msgpack.mjs');
  const intent={command:'transcribe',context:'self',program:'media-transcribe',
    reviewId:'review-case',sourceSha256:sha('review-source')};
  const call=payload=>{
    const result=Bun.spawnSync([process.execPath,join(import.meta.dir,'../command_dispatch.mjs'),'--account-bound'],
      {stdin:encode(payload),stdout:'pipe',stderr:'pipe',env:{...process.env,
        BONFYRE_COMMAND_REVIEW_HOME:'',BONFYRE_AUTH_DB:'',BONFYRE_CMS_DB:''}});
    return {code:result.exitCode,response:JSON.parse(result.stdout.toString())};
  };
  expect(call([1,'server-session-123',new TextEncoder().encode(JSON.stringify(intent))])).toMatchObject({
    code:6,response:{status:503,admitted:false}});
  for(const forged of [{...intent,token:'browser-forgery'}, {...intent,extra:'effect'},
    {...intent,program:42}])
    expect(call([1,'server-session-123',new TextEncoder().encode(JSON.stringify(forged))])).toMatchObject({
      code:4,response:{status:400,admitted:false}});
  expect(call([1,'',new TextEncoder().encode(JSON.stringify(intent))]).code).toBe(4);
  expect(call([1,'server-session-123',new TextEncoder().encode('{}')]).code).toBe(4);
  expect(call([1,'server-session-123',new TextEncoder().encode('not-json')])).toMatchObject({
    code:4,response:{status:400,admitted:false}});
  const oversized=Bun.spawnSync([process.execPath,join(import.meta.dir,'../command_dispatch.mjs'),'--account-bound'],
    {stdin:Buffer.alloc(8193),stdout:'pipe',stderr:'pipe'});
  expect(oversized.exitCode).toBe(4);
  expect(JSON.parse(oversized.stdout.toString()).status).toBe(400);
});
test('source-owned native worker registration ties a real digest to active PackGraph and Queue',async()=>{
  const {registerNativeCommandWorker,readNativeCommandWorker}=await import('../command_owner.mjs');
  const {chmodSync,appendFileSync,symlinkSync,unlinkSync,existsSync,readFileSync}=await import('node:fs');
  const f=packFixture();
  try {
    for(const type of ['application_binding','capability_program'])
      copyFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),
        join(f.schemas,type+'.json'));
    const migrated=spawnSync(f.binary,['schema','migrate','--db',f.db,'--schemas',f.schemas],{encoding:'utf8'});
    expect(migrated.status).toBe(0);
    f.pack('media-pack');
    const release=sha('media-workspace-release');
    f.add('pack_demand',{pack:'media-pack',demand_kind:'workspace',needs:'media-studio',
      preferred_sources:release,fields:'media-editor/',status:'published'});
    f.add('capability_program',program('transcribe'));
    const plan=f.call('plan','media-pack',{actor:'user:1'});
    expect(plan.code).toBe(0);
    const installed=f.call('install','media-pack',{actor:'user:1',digest:plan.digest});
    expect(installed.code).toBe(0);
    const active=f.call('activate','media-pack',{actor:'user:1',
      revision:Number(installed.revision),digest:installed.pinned_digest});
    expect(active.state).toBe('active');
    const workspace=f.sql.query('SELECT binding_id FROM _application_workspace LIMIT 1').get()?.binding_id;
    expect(workspace).toMatch(/^ws-[a-f0-9]{40}$/);
    const queueDb=join(f.home,'work.db'),binRoot=join(f.home,'bin');
    mkdirSync(binRoot);
    const binary=join(binRoot,'bonfyre-transcribe');
    copyFileSync(f.binary,binary);chmodSync(binary,0o700);
    const digest=createHash('sha256').update(readFileSync(binary)).digest('hex');
    nativeQueue(['enqueue','review-decision','fixture://review','--mission','review-fixture',
      '--family','media-reviewed'],queueDb);
    const args={cmsDb:f.db,cmsBinary:f.binary,cmsSchemas:f.schemas,queueDb,binRoot,
      command:'transcribe',programId:'media-transcribe',workspaceBinding:workspace,expectedSha256:digest};
    expect(registerNativeCommandWorker(args)).toMatchObject({
      registered:true,executed:false,operator_id:'queue.transcribe',pack_digest:plan.digest});
    expect(registerNativeCommandWorker(args).worker_sha256).toBe(digest);
    const expected={queueDb,operator:'queue.transcribe',program:'media-transcribe',
      packDigest:plan.digest,workspaceBinding:workspace,artifactKind:'immutable-review-decision'};
    expect(readNativeCommandWorker(expected)).toMatchObject({state:'ready',binary_sha256:digest});
    expect(readNativeCommandWorker({...expected,packDigest:sha('different')})).toBeNull();
    expect(readNativeCommandWorker({...expected,artifactKind:'mutable'})).toBeNull();
    expect(()=>registerNativeCommandWorker({...args,workspaceBinding:'ws-'+sha('wrong').slice(0,40)})).toThrow();
    const cliEnv={...process.env,BONFYRE_COMMAND_REVIEW_HOME:f.home,
      BONFYRE_CMS_DB:f.db,BONFYRE_CMS_BINARY:f.binary,BONFYRE_CMS_SCHEMAS:f.schemas,
      BONFYRE_INSTALLED_NATIVE_BIN_DIR:binRoot};
    const installPacket={command:args.command,programId:args.programId,
      workspaceBinding:args.workspaceBinding,expectedSha256:args.expectedSha256};
    const install=packet=>Bun.spawnSync([process.execPath,join(import.meta.dir,'../command_owner.mjs'),'--register'],
      {stdin:Buffer.from(JSON.stringify(packet)),stdout:'pipe',stderr:'pipe',env:cliEnv});
    const admitted=install(installPacket);
    expect(admitted.exitCode).toBe(0);
    expect(JSON.parse(admitted.stdout.toString())).toMatchObject({
      registered:true,executed:false,operator_id:'queue.transcribe'});
    expect(install({...installPacket,binRoot:'/tmp'}).exitCode).toBe(3);
    expect(install({...installPacket,workspaceBinding:'ws-'+sha('unbound').slice(0,40)}).exitCode).toBe(3);
    appendFileSync(binary,'changed');
    expect(readNativeCommandWorker(expected)).toBeNull();
    expect(()=>registerNativeCommandWorker(args)).toThrow('digest changed');
    const changed=createHash('sha256').update(readFileSync(binary)).digest('hex');
    expect(()=>registerNativeCommandWorker({...args,expectedSha256:changed})).toThrow('new reviewed release');
    unlinkSync(binary);symlinkSync(f.binary,binary);
    expect(()=>registerNativeCommandWorker({...args,expectedSha256:digest})).toThrow();
    expect(readNativeCommandWorker(expected)).toBeNull();
    expect(existsSync(queueDb)).toBe(true);
  }finally{f.close();}
});

test('frozen public release retains the complete native command and Queue import closure',async()=>{
  const {freezePrograms}=await import('../../../scripts/release/public_surface_rollout.mjs');
  const {tmpdir}=await import('node:os');
  const stage=mkdtempSync(join(tmpdir(),'bonfyre-native-command-cut-'));
  try {
    const frozen=freezePrograms(repo,stage);
    for(const part of ['cmd/BonfyreReview/command_dispatch.mjs','cmd/BonfyreReview/command_owner.mjs',
      'cmd/BonfyreQueue/bonfyre-queue','schemas/control/protocol-library.cue'])
      expect(frozen.inputs.some(entry=>entry.path===part)).toBe(true);
    const owner=await import(pathToFileURL(join(stage,'native/cmd/BonfyreReview/command_owner.mjs')).href);
    expect(typeof owner.openNativeCommandOwner).toBe('function');
    expect(typeof owner.registerNativeCommandWorker).toBe('function');
    expect(typeof owner.readNativeCommandWorker).toBe('function');
  } finally {rmSync(stage,{recursive:true,force:true});}
});

test('real Auth + PackGraph + two-person review + registered worker admit a scoped Queue/Work mission',async()=>{
  const {encode}=await import('../../../integrations/browser/workspace-msgpack.mjs');
  const {readFileSync,chmodSync}=await import('node:fs');
  const {Database}=await import('bun:sqlite');
  const {accountAuth}=await import('../../../components/BonfyreServiceSite/account-auth.mjs');
  const {registerNativeCommandWorker}=await import('../command_owner.mjs');
  const f=packFixture();
  let review;
  try{
    for(const type of ['application_binding','capability_program'])
      copyFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),join(f.schemas,type+'.json'));
    const migrate=spawnSync(f.binary,['schema','migrate','--db',f.db,'--schemas',f.schemas],{encoding:'utf8'});
    expect(migrate.status).toBe(0);
    const sourceRoot=join(f.home,'source');mkdirSync(sourceRoot);
    const declared=program('transcribe');
    declared.transition_conditions.review.source_root=sourceRoot;
    f.pack('media-pack');
    const release=sha('media-workspace-release');
    f.add('pack_demand',{pack:'media-pack',demand_kind:'workspace',needs:'media-studio',
      preferred_sources:release,fields:'media-editor/',status:'published'});
    f.add('capability_program',declared);
    const plan=f.call('plan','media-pack',{actor:'user:1'});
    expect(plan.code).toBe(0);
    const installed=f.call('install','media-pack',{actor:'user:1',digest:plan.digest});
    expect(installed.code).toBe(0);
    const active=f.call('activate','media-pack',{actor:'user:1',
      revision:Number(installed.revision),digest:installed.pinned_digest});
    expect(active.state).toBe('active');
    const workspace=f.sql.query('SELECT binding_id,release FROM _application_workspace LIMIT 1').get();
    expect(workspace.release).toBe(release);
    const authDb=join(f.home,'auth.db'),authBinary=join(repo,'cmd/BonfyreAuth/bonfyre-auth');
    const authRun=(args,input)=>{
      const c=Bun.spawnSync([authBinary,'--db',authDb,...args],
        {stdin:input?Buffer.from(input):undefined,stdout:'pipe',stderr:'pipe',timeout:10000});
      if(c.exitCode!==0)throw Error('Native Auth fixture: '+args[0]+': '+c.stderr.toString().slice(0,160));
      return c.stdout.toString();
    };
    const login=(name,index)=>{
      const email=name+'@example.test',password='isolated-native-command-fixture';
      authRun(['http-signup'],JSON.stringify({name,email,password}));
      const token=JSON.parse(authRun(['http-login'],JSON.stringify({email,password}))).session.token;
      authRun(['actor-bind','--user-id',String(index),'--actor','actor:'+name]);
      authRun(['institution-grant','--user-id',String(index),'--institution','media-team','--role','write']);
      return token;
    };
    const author=login('author',1),reviewer=login('reviewer',2),auth=accountAuth(authDb,{binary:authBinary});
    expect(auth.principal(author)).toMatchObject({userId:1,actor:'actor:author'});
    expect(auth.principal(reviewer)).toMatchObject({userId:2,actor:'actor:reviewer'});
    const home=join(f.home,'reviews'),queueDb=join(home,'work.db'),binRoot=join(f.home,'bin');
    mkdirSync(home);mkdirSync(binRoot);
    const executable=join(binRoot,'bonfyre-transcribe');
    copyFileSync(f.binary,executable);chmodSync(executable,0o700);
    const binarySha=sha(readFileSync(executable));
    nativeQueue(['enqueue','review-decision','fixture://bootstrap','--mission',
      'review-workgraph-bootstrap','--family','media-reviewed'],queueDb);
    expect(registerNativeCommandWorker({cmsDb:f.db,cmsBinary:f.binary,cmsSchemas:f.schemas,
      queueDb,binRoot,command:'transcribe',programId:'media-transcribe',
      workspaceBinding:workspace.binding_id,expectedSha256:binarySha})).toMatchObject({
        registered:true,executed:false});
    const original=content({command:'transcribe',program:'media-transcribe',pack:'media-pack',
      pack_digest:plan.digest,subject:'user:1',workspace_binding:workspace.binding_id,
      workspace_release:workspace.release,operator:'queue.transcribe',
      input_sha256:sha('retained-input')});
    const file=join(sourceRoot,'intent.yaff');writeFileSync(file,original);
    const tokenConfig={home,auth,experiences:[{experience:'media-review',institution:'media-team',
      approvedFamily:'media-reviewed',sourceRoot}],queue:nativeQueue};
    review=nativeReview(tokenConfig);
    const pending=review.submit({token:author,experience:'media-review',
      reviewId:'real-native-case',sourceUri:pathToFileURL(file).href,
      sourceSha256:sha(original),requestKey:'real-submit'});
    expect(pending.state).toBe('pending');
    expect(()=>review.decide({token:author,reviewId:'real-native-case',action:'approve',
      expectedRevision:1,requestKey:'self-approval'})).toThrow();
    const approved=review.decide({token:reviewer,reviewId:'real-native-case',
      action:'approve',expectedRevision:1,requestKey:'independent-approval'});
    expect(approved.state).toBe('approved');
    expect(approved.downstream.at(-1).admitted).toBe(true);
    const intent={command:'transcribe',program:'media-transcribe',context:'self',
      reviewId:'real-native-case',sourceSha256:sha(original)};
    const env={...process.env,BONFYRE_COMMAND_REVIEW_HOME:home,
      BONFYRE_NATIVE_COMMAND_AUTH_DB:authDb,BONFYRE_AUTH_BINARY:authBinary,
      BONFYRE_CMS_DB:f.db,BONFYRE_CMS_BINARY:f.binary,
      BONFYRE_NATIVE_COMMAND_CMS_SCHEMAS:f.schemas,
      BONFYRE_NATIVE_COMMAND_QUEUE_BIN:join(repo,'cmd/BonfyreQueue/bonfyre-queue')};
    const submit=(session,request=intent)=>Bun.spawnSync(
      [process.execPath,join(repo,'cmd/BonfyreReview/command_dispatch.mjs'),'--account-bound'],
      {stdin:Buffer.from(encode([1,session,new TextEncoder().encode(JSON.stringify(request))])),
        stdout:'pipe',stderr:'pipe',env,timeout:15000});
    const done=submit(author);
    expect(done.exitCode).toBe(0);
    const response=JSON.parse(done.stdout.toString());
    expect(response.status).toBe(202);
    expect(response.result).toMatchObject({admitted:true,executed:false,
      operator_id:'queue.transcribe',subject:'user:1'});
    expect(JSON.parse(submit(author).stdout.toString()).result.mission_id)
      .toBe(response.result.mission_id);
    expect(submit(reviewer).exitCode).toBe(3);
    const index=join(f.home,'account-work.db');
    const projected=Bun.spawnSync([join(repo,'cmd/BonfyreQueue/bonfyre-queue'),
      'account-work','index','--db',index,'--subject','user:1','--scope','self',
      '--owner','user:1','--source','workgraph='+queueDb],{stdout:'pipe',stderr:'pipe',timeout:15000});
    expect(projected.exitCode).toBe(0);
    const indexDb=new Database(index,{readonly:true});
    try {
      expect(indexDb.query('SELECT source FROM account_work WHERE id=?').get(
        'account-workgraph-'+response.result.mission_id)?.source).toBe('workgraph');
    }finally{indexDb.close();}
    authRun(['actor-revoke','--user-id','1']);
    expect(submit(author).exitCode).toBe(3);
    expect(nativeQueue(['mission',response.result.mission_id],queueDb)).toMatchObject({
      operator_id:'queue.transcribe'});
  }finally{review?.close();f.close();}
});
