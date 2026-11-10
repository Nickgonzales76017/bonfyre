// Actual Queue, WorkGraph, Credentials and BrowserHost in an isolated home.
// No provider is contacted; synthetic keys never enter an installed store.
import {test,expect,afterEach,spyOn} from 'bun:test';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync,existsSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Database} from 'bun:sqlite';
import * as en from '../onboarding/enrollment.mjs';
import {writeCheckpointMaterial} from '../onboarding/checkpoint-material.mjs';
import {createHost} from '../host/bonfyre-browser-host.mjs';
const homes=[];
afterEach(()=>{for(const home of homes.splice(0))rmSync(home,{recursive:true,force:true});});
const review=view=>({work_id:view.item.work_id,revision:view.revision});
function fixture(count=4){
  const home=mkdtempSync(join(tmpdir(),'enrollment-continuation-'));homes.push(home);
  const providers=join(home,'providers.json'),key=join(home,'credentials.key'),root=join(home,'enrollment');mkdirSync(root);
  const env={...process.env,HOME:home,BONFYRE_ENROLLMENT_HOME:root,BONFYRE_ENROLLMENT_DB:join(root,'queue.db'),BONFYRE_CREDENTIAL_PROVIDERS:providers,BONFYRE_CREDENTIALS_PROVIDERS:providers,BONFYRE_CREDENTIALS_DB:join(home,'credentials.db'),BONFYRE_CREDENTIALS_KEY:key};
  const c=en.config(env);
  expect(existsSync(c.queue)).toBe(true);expect(existsSync(c.credentials)).toBe(true);
  writeFileSync(providers,JSON.stringify([{provider:'fixture',name:'Fixture',purpose:'isolated test',get_it:'https://provider.example.invalid/',fields:[{field:'token',label:'Token',kind:'secret',sensitive:true,required:true,generator:'',help:'fixture'}],apply:{headers:[],query:[]}}]));
  const native=(...args)=>{const p=Bun.spawnSync([c.queue,...args,'--db',c.db],{stdout:'pipe',stderr:'pipe'});expect(p.exitCode).toBe(0);return JSON.parse(p.stdout.toString().trim().split('\n').at(-1));};
  const rows=[];
  for(let i=1;i<=count;i++)for(const identity of ['gmail','proton']){const id=String(i).padStart(3,'0'),item={work_id:id+'-'+identity,identity,account:en.IDENTITIES[identity],platform:i===3?'Fixture 100%_ literal':'Fixture '+id,signup_url:'https://provider.example.invalid/login',credential_provider:'fixture'};rows.push(['enroll-'+item.work_id,'account.enrollment',JSON.stringify(item),'enrollment-'+identity,500,40,600000].join('\t'));}
  const batch=join(home,'seed.tsv');writeFileSync(batch,rows.join('\n')+'\n');native('enqueue-batch',batch);
  const sql=(statement,...args)=>{const db=new Database(c.db);try{return db.query(statement).all(...args);}finally{db.close();}};
  const state=()=>sql('SELECT mission_id,status,attempt FROM workgraph_nodes ORDER BY mission_id');
  return {c,env,home,native,sql,state,key};
}
test('read-only selection pages the real queue with exact identity and escaped search',()=>{
  const {c,state}=fixture(43),before=state();
  const first=en.list('gmail',{},c);expect(first.items).toHaveLength(20);expect(first.cursor).toBe('020-gmail');expect(first.more).toBe(true);
  const second=en.list('gmail',{cursor:first.cursor},c),third=en.list('gmail',{cursor:second.cursor},c);
  expect(second.items[0].item.work_id).toBe('021-gmail');expect(third.items).toHaveLength(3);expect(third.cursor).toBeNull();
  expect(new Set([...first.items,...second.items,...third.items].map(row=>row.item.work_id)).size).toBe(43);
  expect(en.list('gmail',{search:'100%_'},c).items.map(row=>row.item.work_id)).toEqual(['003-gmail']);
  expect(en.list('gmail',{search:'%_missing'},c).items).toEqual([]);
  expect(()=>en.list('gmail',{cursor:'020-proton'},c)).toThrow(/another enrollment identity/);
  expect(()=>en.list('gmail',{cursor:0},c)).toThrow(/supported enrollment filter/);
  expect(()=>en.list('gmail',{search:'x'.repeat(101)},c)).toThrow(/100 characters/);
  expect(en.inspect('gmail','003-gmail',c)).toMatchObject({read_only:true,revision:0,status:'ready'});
  expect(()=>en.inspect('gmail','003-proton',c)).toThrow(/another enrollment identity/);
  expect(existsSync(join(c.root,'leases'))).toBe(false);expect(state()).toEqual(before);
});
test('an unseeded read does not create a queue or lease directory',()=>{
  const home=mkdtempSync(join(tmpdir(),'enrollment-unseeded-'));homes.push(home);const c=en.config({...process.env,HOME:home,BONFYRE_ENROLLMENT_HOME:join(home,'absent')});
  expect(en.list('gmail',{},c)).toMatchObject({seeded:false,items:[]});expect(existsSync(c.root)).toBe(false);
  expect(()=>en.inspect('gmail','001-gmail',c)).toThrow(/unavailable/);expect(existsSync(c.root)).toBe(false);
});
test('continue claims the selected reviewed work and refuses another held setup',()=>{
  const {c,state}=fixture();const opened=en.continueWork('gmail',review(en.inspect('gmail','003-gmail',c)),c);
  expect(opened).toMatchObject({how:'continued',read_only:false,status:'running',item:{work_id:'003-gmail'}});
  expect(state().find(row=>row.mission_id==='enroll-001-gmail').status).toBe('ready');
  expect(()=>en.continueWork('gmail',review(en.inspect('gmail','001-gmail',c)),c)).toThrow(/Another setup is held/);
  expect(en.resume('gmail',c).item.work_id).toBe('003-gmail');
  const stale=review(opened);en.record('gmail','existing_access_checked','fixture review',c,stale);
  expect(()=>en.continueWork('gmail',stale,c)).toThrow(/stale/);expect(en.inspect('gmail','003-gmail',c).revision).toBe(1);
});
test('checkpoint changes during exact claim or renew retain the hold but require a fresh review',()=>{
  for(const operation of ['claim','renew']){
    const {c,state}=fixture(),workId='003-gmail',mission='enroll-'+workId;
    if(operation==='renew')en.continueWork('gmail',review(en.inspect('gmail',workId,c)),c);
    const reviewed=review(en.inspect('gmail',workId,c)),spawn=Bun.spawnSync.bind(Bun);let advanced=false;
    const intercept=spyOn(Bun,'spawnSync').mockImplementation((args,options)=>{
      const result=spawn(args,options);
      if(!advanced&&args[0]===c.queue&&args[1]===operation&&args[2]===mission){
        advanced=true;expect(result.exitCode).toBe(0);
        const held=JSON.parse(result.stdout.toString().trim().split('\n').at(-1));
        const artifact=writeCheckpointMaterial(c.root,workId,1,{stages:{account_bound:{at:new Date().toISOString(),note:'Concurrent native checkpoint',evidence_kind:'human_assertion'}},notes:[]});
        // Only response timing is intercepted. The claim/renew and competing
        // checkpoint CAS all run through the actual native Queue executable.
        const saved=spawn([c.queue,'checkpoint-save',mission,'job',args[4],operation==='claim'?held.claim_token:args[5],
          '--expected-revision','0','--artifact-uri',artifact,'--db',c.db],{stdout:'pipe',stderr:'pipe'});
        expect(saved.exitCode).toBe(0);expect(JSON.parse(saved.stdout.toString().trim().split('\n').at(-1)).revision).toBe(1);
      }
      return result;
    });
    let opened;
    try{opened=en.continueWork('gmail',reviewed,c);}finally{intercept.mockRestore();}
    expect(advanced).toBe(true);
    expect(opened).toMatchObject({how:'review-required',read_only:true,review_required:true,reviewed_revision:0,revision:1,status:'running',item:{work_id:workId}});
    expect(opened.stages.account_bound.note).toBe('Concurrent native checkpoint');
    const lease=JSON.parse(readFileSync(join(c.root,'leases/gmail.json'),'utf8'));
    expect(lease.mission).toBe(mission);expect(lease.token).toBeTruthy();
    expect(state().find(row=>row.mission_id===mission).status).toBe('running');
    expect(state().find(row=>row.mission_id==='enroll-001-gmail').status).toBe('ready');
    expect(()=>en.record('gmail','scope_confirmed','stale review must fail',c,reviewed)).toThrow(/stale/);
    expect(en.inspect('gmail',workId,c).revision).toBe(1);
    const continued=en.continueWork('gmail',review(opened),c);
    expect(continued).toMatchObject({how:'continued',read_only:false,revision:1,item:{work_id:workId}});
    expect(continued.review_required).not.toBe(true);
    expect(JSON.parse(readFileSync(join(c.root,'leases/gmail.json'),'utf8')).token).toBe(lease.token);
  }
});
test('a lapsed hold restores the same checkpoint for review and reclaims only that selected mission',()=>{
  const {c,sql,state}=fixture();let opened=en.continueWork('gmail',review(en.inspect('gmail','003-gmail',c)),c);
  en.record('gmail','account_bound','intended fixture account',c,review(opened));
  sql("UPDATE workgraph_nodes SET lease_expires_at_ms=1 WHERE mission_id='enroll-003-gmail'");
  const restored=en.resume('gmail',c);expect(restored).toMatchObject({read_only:true,revision:1,item:{work_id:'003-gmail'},stages:{account_bound:{evidence_kind:'human_assertion'}}});
  expect(existsSync(join(c.root,'leases/gmail.json'))).toBe(false);expect(state().find(row=>row.mission_id==='enroll-001-gmail').status).toBe('ready');
  opened=en.continueWork('gmail',review(restored),c);expect(opened.item.work_id).toBe('003-gmail');expect(opened.revision).toBe(1);
  expect(state().find(row=>row.mission_id==='enroll-001-gmail').status).toBe('ready');
});
test('wait and decision inspection cannot clear a native hold or advance another task',()=>{
  const {c,state,sql}=fixture();let opened=en.continueWork('gmail',review(en.inspect('gmail','001-gmail',c)),c);
  en.wait('gmail','email','waiting fixture',c,review(opened));
  const waiting=en.inspect('gmail','001-gmail',c),before=state();expect(waiting).toMatchObject({status:'retry_wait',read_only:true,resumable:false});expect(waiting.notes.at(-1).note).toBe('waiting fixture');
  expect(en.list('gmail',{filter:'waiting'},c).items.map(row=>row.item.work_id)).toEqual(['001-gmail']);
  expect(()=>en.continueWork('gmail',review(waiting),c)).toThrow(/not ready/);expect(state()).toEqual(before);
  opened=en.continueWork('gmail',review(en.inspect('gmail','002-gmail',c)),c);en.decide('gmail','terms','read terms',c,review(opened));
  const decision=en.inspect('gmail','002-gmail',c);expect(decision).toMatchObject({status:'dead_letter',needs_decision:true,resumable:false});
  expect(()=>en.continueWork('gmail',review(decision),c)).toThrow(/not ready/);
  expect(en.list('gmail',{filter:'decisions'},c).items.map(row=>row.item.work_id)).toEqual(['002-gmail']);
  sql("UPDATE workgraph_nodes SET next_attempt_at_ms=0 WHERE mission_id='enroll-001-gmail'");
  expect(en.list('gmail',{filter:'ready'},c).items.some(row=>row.item.work_id==='001-gmail')).toBe(true);
});
test('a missing or mismatched checkpoint prevents continuation before a new native hold',()=>{
  const {c,sql,state}=fixture();const opened=en.continueWork('gmail',review(en.inspect('gmail','001-gmail',c)),c);
  en.record('gmail','account_bound','fixture',c,review(opened));
  const pointer=sql("SELECT artifact_uri FROM workgraph_checkpoints WHERE mission_id='enroll-001-gmail' AND revision=1")[0].artifact_uri;
  sql("UPDATE workgraph_checkpoints SET revision=2 WHERE mission_id='enroll-001-gmail' AND revision=1");
  expect(()=>en.inspect('gmail','001-gmail',c)).toThrow(/material revision/);
  sql("UPDATE workgraph_checkpoints SET revision=1 WHERE mission_id='enroll-001-gmail' AND revision=2");rmSync(new URL(pointer));
  const before=state();expect(()=>en.continueWork('gmail',{work_id:'001-gmail',revision:1},c)).toThrow();expect(state()).toEqual(before);
});
test('the native queue identity and provider address must agree before any setup is opened',()=>{
  const {c,sql,state}=fixture(),before=state();
  sql("UPDATE workgraph_nodes SET input_uri=json_set(input_uri,'$.account','another@example.invalid') WHERE mission_id='enroll-001-gmail'");
  expect(()=>en.inspect('gmail','001-gmail',c)).toThrow(/disagrees with its identity/);
  expect(()=>en.continueWork('gmail',{work_id:'001-gmail',revision:0},c)).toThrow(/disagrees/);
  sql("UPDATE workgraph_nodes SET input_uri=json_set(input_uri,'$.signup_url','https://user:password@example.invalid') WHERE mission_id='enroll-002-gmail'");
  expect(()=>en.inspect('gmail','002-gmail',c)).toThrow(/provider address/);expect(state()).toEqual(before);
});
test('real Credentials seals only the selected account and reopening exposes metadata only',()=>{
  const {c,key}=fixture();expect(Bun.spawnSync([c.credentials,'init-key','--key',key]).exitCode).toBe(0);
  const opened=en.continueWork('proton',review(en.inspect('proton','003-proton',c)),c),secret='synthetic-continuation-key-7391';
  en.seal('proton','fixture','token',secret,c,review(opened));
  const reopened=en.inspect('proton','003-proton',c);expect(reopened.revision).toBe(1);expect(reopened.missing).toContain('credential_verified');expect(JSON.stringify(reopened)).not.toContain(secret);
  const db=new Database(c.credentialsDb,{readonly:true});try{expect(db.query('SELECT account,provider,field FROM sealed_field').all()).toEqual([{account:en.IDENTITIES.proton,provider:'fixture',field:'token'}]);}finally{db.close();}
  expect(readFileSync(c.db).includes(Buffer.from(secret))).toBe(false);
});
test('BrowserHost inspects without effect and requires exact reviewed confirmation to continue',async()=>{
  const {c,env,state}=fixture(),host=createHost({env,tools:{call:async()=>{throw Error('No provider tools may run');}}}),before=state();
  const listed=await host.answer({operation:'enroll_list',identity:'gmail',filter:'ready'});expect(listed.ok).toBe(true);expect(listed.result.items).toHaveLength(4);
  const inspected=await host.answer({operation:'enroll_inspect',identity:'gmail',work_id:'003-gmail'});expect(inspected.result.read_only).toBe(true);expect(state()).toEqual(before);
  expect((await host.answer({operation:'enroll_continue',identity:'gmail',expected:review(inspected.result)})).refused).toBe('not_confirmed');expect(state()).toEqual(before);
  const continued=await host.answer({operation:'enroll_continue',identity:'gmail',expected:review(inspected.result),confirmed:true});expect(continued.ok).toBe(true);expect(continued.result.item.work_id).toBe('003-gmail');
  expect((await host.answer({operation:'enroll_inspect',identity:'gmail',work_id:'003-proton'})).ok).toBe(false);
  expect(en.resume('gmail',c).item.work_id).toBe('003-gmail');
});
