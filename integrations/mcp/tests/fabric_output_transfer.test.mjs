import {test,beforeAll,afterAll,afterEach} from 'bun:test';
import assert from 'node:assert/strict';
import {spawn,spawnSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,readdirSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Database} from 'bun:sqlite';
import {createFabricStore} from '../fabric_store.mjs';
import {createFabricModelWork} from '../fabric_model.mjs';
import {createFabricOutputTransfer,outputTransferFromEnvironment} from '../fabric_output_transfer.mjs';

const candidate=fileURLToPath(new URL('../../../',import.meta.url));
const cleanups=[];let build,binary;
beforeAll(()=>{
  if(process.env.SWARM_TEST_BINARY){binary=process.env.SWARM_TEST_BINARY;return;}
  build=mkdtempSync(join(tmpdir(),'fabric-transfer-build-'));binary=join(build,'bonfyre-swarm');
  const lib=join(candidate,'lib/libbonfyre');
  const argv=['-O1','-g','-Wall','-Wextra','-std=c11','-D_POSIX_C_SOURCE=200809L','-D_DEFAULT_SOURCE','-DBONFYRE_SHA256_PORTABLE',
    '-I'+join(lib,'include'),...(process.env.SWARM_TEST_CFLAGS||'').split(/\s+/).filter(Boolean),
    join(candidate,'cmd/BonfyreSwarm/src/main.c'),join(lib,'src/bf_sha256.c'),join(lib,'bf_crypto.c'),join(lib,'bf_msgpack.c'),
    ...(process.env.SWARM_TEST_LDFLAGS||'-lsqlite3').split(/\s+/).filter(Boolean),'-o',binary];
  const result=spawnSync(process.env.CC||'cc',argv,{encoding:'utf8',timeout:60000});assert.equal(result.status,0,result.stderr);
});
afterAll(()=>{if(build)rmSync(build,{recursive:true,force:true});});
afterEach(async()=>{for(const release of cleanups.splice(0).reverse())await release();});
const alice={actor:'user:1',subject:'institution:studio',scope:'institution'},
  coworker={actor:'user:2',subject:'institution:studio',scope:'institution'},
  other={actor:'user:1',subject:'institution:other',scope:'institution'};

function fixture(options={}) {
  const root=mkdtempSync(join(tmpdir(),'fabric-transfer-')),projectRoot=join(root,'project'),home=join(root,'owner');mkdirSync(projectRoot);
  const store=createFabricStore({home,projectRoot});
  const taskId=store.createScopedTask({kind:'model',title:'Retained local result',prompt:'fixture prompt',requestId:'model-request-001'},alice).task_id;
  store.claimTask(taskId);
  const text='An actual retained output from the Fabric owner.\n'.repeat(700),output=store.saveReceipt(taskId,'model-output',text,'txt');
  store.saveReceipt(taskId,'model-result',{state:'completed',digest:output.sha256,bytes:Buffer.byteLength(text)},'msgpack');
  store.updateTask(taskId,{state:'completed'});
  const checks=[],commands=[];let allowed=true,paused=false;
  const authorizeAccount=async binding=>{checks.push(binding);return options.authorize?options.authorize(binding):allowed;};
  const admit=()=>{if(paused)throw Object.assign(new Error('paused'),{code:'admission'});};
  const model=createFabricModelWork({store,native:{},meter:{},authorizeAccount,admit,enqueue(){},execution(){return {owned:false,queued:false};}});
  const index=join(root,'swarm.db');
  const args={store,outputArtifact:model.outputArtifact,authorizeAccount,admit,binary,index,
    spawnImpl:(command,argv,spawnOptions)=>{commands.push([command,...argv]);return options.spawn?options.spawn(command,argv,spawnOptions):spawn(command,argv,spawnOptions);}};
  const transfer=createFabricOutputTransfer(args);
  cleanups.push(async()=>{await transfer.close();await model.close();store.close();rmSync(root,{recursive:true,force:true});});
  const input=(changes={})=>({expectedBinding:alice,requestId:'local-transfer-001',taskId,outputId:output.receipt_id,...changes});
  return {root,home,store,taskId,output,index,args,transfer,input,commands,checks,text,
    allow:value=>{allowed=value;},pause:value=>{paused=value;}};
}

test('real native two-process transfer verifies retained output, scoped receipts and exact replay',async()=>{
  const f=fixture(),first=await f.transfer.verify(alice,f.input());
  assert.equal(first.transfer.state,'verified');assert.equal(first.transfer.sha256,f.output.sha256);
  assert.equal(first.transfer.bytes,Buffer.byteLength(f.text));assert.equal(first.transfer.senderConfirmed,true);assert.equal(first.transfer.receiverConfirmed,true);
  assert.equal(first.transfer.localOnly,true);assert.equal(first.transfer.verificationOnly,true);assert.equal(f.commands.length,2);
  const replay=await f.transfer.verify(alice,f.input());assert.equal(replay.replayed,true);assert.deepEqual(replay.transfer,first.transfer);assert.equal(f.commands.length,2);
  const restarted=createFabricOutputTransfer(f.args);try{assert.deepEqual((await restarted.verify(alice,f.input())).transfer,first.transfer);}finally{await restarted.close();}
  const db=new Database(f.index,{readonly:true});try{assert.equal(db.query('SELECT count(*) AS n FROM swarm_scoped_transfers').get().n,2);}finally{db.close();}
  assert.deepEqual(readdirSync(join(f.home,'local-transfer-verifications')).map(path=>path.split('.').at(-1)),['msgpack']);
  const rendered=JSON.stringify(first);for(const secret of [f.root,f.output.path,'--key-file','recipient','private prompt'])assert.equal(rendered.includes(secret),false,secret);
  assert.ok(f.checks.some(row=>row.permission==='write'));assert.ok(f.checks.some(row=>row.permission==='read'));
});

test('concurrent exact requests share one native transfer; changed output cannot reuse request identity',async()=>{
  const f=fixture(),[one,two]=await Promise.all([f.transfer.verify(alice,f.input()),f.transfer.verify(alice,f.input())]);
  assert.equal(one.transfer.state,'verified');assert.equal(two.transfer.id,one.transfer.id);assert.equal(f.commands.length,2);
  await assert.rejects(()=>f.transfer.verify(alice,f.input({outputId:'a'.repeat(24)})),{code:'conflict'});
  assert.equal(f.commands.length,2);
});

test('native authority, initiating actor and exact destination precede native process creation',async()=>{
  const f=fixture();
  for(const binding of [other,coworker])await assert.rejects(()=>f.transfer.verify(binding,f.input({expectedBinding:binding})),{code:'not_found'});
  await assert.rejects(()=>f.transfer.verify(alice,f.input({expectedBinding:other})),{code:'conflict'});
  f.allow(false);await assert.rejects(()=>f.transfer.verify(alice,f.input()),{code:'admission'});f.allow(true);
  f.pause(true);await assert.rejects(()=>f.transfer.verify(alice,f.input()),{code:'admission'});
  assert.equal(f.commands.length,0);
});

test('browser-supplied transport fields and unavailable output never become a native grant',async()=>{
  const f=fixture();
  for(const extra of [{path:'/tmp/private'},{host:'example.invalid'},{key:'secret'},{sha256:'a'.repeat(64)},{recipient:coworker}])
    await assert.rejects(()=>f.transfer.verify(alice,{...f.input(),...extra}),{code:'invalid'});
  await assert.rejects(()=>f.transfer.verify(alice,f.input({outputId:'b'.repeat(24)})),{code:'not_found'});
  writeFileSync(f.output.path,'tampered owner output');await assert.rejects(()=>f.transfer.verify(alice,f.input()));assert.equal(f.commands.length,0);
});

test('revocation after native sender readiness prevents receiver, records uncertainty and never retransmits',async()=>{
  let allowed=true;
  const f=fixture({authorize:()=>allowed,spawn:(command,args,options)=>{const child=spawn(command,args,options);if(args[0]==='scoped-seed')allowed=false;return child;}});
  await assert.rejects(()=>f.transfer.verify(alice,f.input()),{code:'admission'});
  assert.equal(f.commands.length,1);allowed=true;
  const replay=await f.transfer.verify(alice,f.input());assert.equal(replay.replayed,true);assert.equal(replay.transfer.state,'uncertain');assert.equal(f.commands.length,1);
  assert.equal(readdirSync(join(f.home,'local-transfer-verifications')).some(path=>path.endsWith('.key')),false);
});

test('failed native receiver stays uncertain across owner restart and does not claim delivery',async()=>{
  const f=fixture({spawn:(command,args,options)=>args[0]==='scoped-fetch'?spawn('/usr/bin/false',[],options):spawn(command,args,options)});
  const first=await f.transfer.verify(alice,f.input());assert.equal(first.transfer.state,'uncertain');
  await f.transfer.close();
  const restarted=createFabricOutputTransfer(f.args);try{
    const replay=await restarted.verify(alice,f.input());assert.equal(replay.transfer.state,'uncertain');assert.equal(replay.transfer.senderConfirmed,false);
    assert.equal(replay.transfer.receiverConfirmed,false);assert.equal(f.commands.length,2);
  }finally{await restarted.close();}
});

test('owner close terminates and joins an in-flight native sender before returning',async()=>{
  let ready;
  const started=new Promise(resolve=>{ready=resolve;});
  const f=fixture({spawn:(command,args,options)=>{const child=spawn(command,args,options);if(args[0]==='scoped-seed')ready(child);return child;}});
  const pending=f.transfer.verify(alice,f.input());pending.catch(()=>{});
  const child=await started;await f.transfer.close();await assert.rejects(()=>pending,{code:'unavailable'});
  assert.ok(child.exitCode!==null||child.signalCode!==null);assert.equal(f.commands.length,1);
  assert.equal(readdirSync(join(f.home,'local-transfer-verifications')).some(path=>/\.(key|received)$/.test(path)),false);
});

test('unconfigured helper cannot invent a carrier or alternate native index',async()=>{
  const unavailable=outputTransferFromEnvironment({HOME:'/does-not-exist'},{root:'/does-not-exist'});
  assert.equal(unavailable.configured,false);await assert.rejects(()=>unavailable.verify(alice,{}),{code:'unavailable'});await unavailable.close();
  const f=fixture(),configured=outputTransferFromEnvironment({BONFYRE_SWARM_BINARY:binary,BONFYRE_SWARM_INDEX:f.index},f.args);
  assert.equal(configured.configured,true);await configured.close();
});
