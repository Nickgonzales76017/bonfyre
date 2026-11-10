// Real native owners in a throwaway home. Provider declarations and all pages are fixtures.
// Run with Bun after building BonfyreQueue and BonfyreCredentials; no external accounts are touched.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,writeFileSync,readFileSync,statSync,mkdirSync,rmSync,readdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Database} from 'bun:sqlite';
import * as en from '../onboarding/enrollment.mjs';
import {createHost} from '../host/bonfyre-browser-host.mjs';
const home=mkdtempSync(join(tmpdir(),'bonfyre-enrollment-'));
const providers=join(home,'providers.json'),key=join(home,'credentials.key');
const env={...process.env,HOME:home,BONFYRE_ENROLLMENT_HOME:join(home,'.bonfyre/onboarding/native'),BONFYRE_ENROLLMENT_DB:join(home,'.bonfyre/onboarding/native/enrollment.db'),BONFYRE_ENROLLMENT_LEGACY:join(home,'.bonfyre/onboarding'),BONFYRE_CREDENTIAL_PROVIDERS:providers,BONFYRE_CREDENTIALS_PROVIDERS:providers,BONFYRE_CREDENTIALS_DB:join(home,'credentials.db'),BONFYRE_CREDENTIALS_KEY:key};
const c=en.config(env);
const reviewed=identity=>{const item=en.resume(identity,c)??en.next(identity,c);return{work_id:item.item.work_id,revision:item.revision};};
afterAll(()=>rmSync(home,{recursive:true,force:true}));
beforeAll(()=>{
  writeFileSync(providers,JSON.stringify([{provider:'fixture-provider',name:'Fixture',purpose:'isolated test',get_it:'https://provider.example.invalid/',fields:[{field:'token',label:'Token',kind:'secret',sensitive:true,required:true,generator:'',help:'fixture'}],apply:{headers:[],query:[]},cannot_infer:'No external provider was contacted.'}]));
  expect(Bun.spawnSync([c.credentials,'init-key','--key',key]).exitCode).toBe(0);
  const columns=['work_id','source_id','platform','signup_url','identity','credential_account','api_gate','supplied_state','account_group','source_tier','credential_provider'];
  const rows=[];
  for(let i=1;i<=200;i++)for(const identity of ['gmail','proton']){const id=String(i).padStart(3,'0');rows.push([`${id}-${identity}`,id,`Fixture ${id}`,'https://provider.example.invalid/login',identity,en.IDENTITIES[identity],'fixture','lookup_required',id,'1','fixture-provider']);}
  c.ledger=join(home,'ledger.tsv');writeFileSync(c.ledger,[columns,...rows].map(r=>r.join('\t')).join('\n')+'\n');
  mkdirSync(join(home,'.bonfyre/onboarding'),{recursive:true});
  writeFileSync(join(home,'.bonfyre/onboarding/progress.tsv'),'gmail\t002\tvisited\t\n');
  writeFileSync(join(home,'.bonfyre/onboarding/cursor.tsv'),'gmail\t3\nproton\t1\n');
});
test('400 native missions are seeded once without resetting existing work',()=>{
  expect(en.seed(c).added).toBe(400);expect(en.seed(c)).toMatchObject({added:0,kept:400});
  expect(en.status(c).by_identity).toEqual({gmail:{ready:200},proton:{ready:200}});
});
test('legacy visits are retained only as notes, never native milestones',()=>{
  expect(en.migrate(c)).toMatchObject({carried:1,items:['002-gmail'],cursor_ignored:true});
  expect(JSON.parse(readFileSync(join(c.root,'checkpoints/002-gmail.json'),'utf8'))).toMatchObject({stages:{},legacy:{launcher_status:'visited'}});
});
test('native claim survives panel loss without taking another item',()=>{
  const a=en.next('gmail',c);expect(en.resume('gmail',c).item.work_id).toBe(a.item.work_id);expect(en.next('gmail',c).item.work_id).toBe(a.item.work_id);
  expect(en.next('proton',c).item.identity).toBe('proton');expect(statSync(join(c.root,'leases/gmail.json')).mode&0o777).toBe(0o600);
});
test('completion and machine-owned assertions refuse before any milestone exists',()=>{
  expect(()=>en.done('proton',c,reviewed('proton'))).toThrow(/not complete/);
  for(const stage of ['credential_sealed','credential_verified','consumer_bound'])expect(()=>en.record('gmail',stage,'clicked',c,reviewed('gmail'))).toThrow(/native owner/);
});
test('carrier checks the reviewed native item and revision, not a freshly assumed revision',()=>{
  const old=reviewed('gmail');en.record('gmail','existing_access_checked','fixture review',c,old);
  expect(()=>en.record('gmail','account_bound','stale panel',c,old)).toThrow(/stale/);
  expect(()=>en.record('gmail','account_bound','missing review',c)).toThrow(/reviewed revision/);
  const next=reviewed('gmail');expect(next.revision).toBe(old.revision+1);
  expect(()=>en.record('gmail','account_bound','wrong item',c,{...next,work_id:'200-gmail'})).toThrow(/stale/);
  const saved=en.record('gmail','account_bound','fixture identity observation',c,next);expect(saved.revision).toBe(next.revision+1);
});
test('seal requires the exact native provider binding and keeps value out of checkpoint material',()=>{
  const secret='fixture-secret-9876-abcd';
  expect(()=>en.seal('gmail','unrelated','token',secret,c,reviewed('gmail'))).toThrow(/provider binding/);
  en.seal('gmail','fixture-provider','token',secret,c,reviewed('gmail'));
  const store=new Database(env.BONFYRE_CREDENTIALS_DB,{readonly:true});
  expect(store.query("SELECT account FROM sealed_field WHERE provider='fixture-provider'").all()).toEqual([{account:en.IDENTITIES.gmail}]);store.close();
  const lease=JSON.parse(readFileSync(join(c.root,'leases/gmail.json'),'utf8')),dir=join(c.root,'checkpoints',lease.mission.replace('enroll-',''));
  const material=readdirSync(dir);expect(material.some(f=>f.endsWith('.mpk'))).toBe(true);
  for(const f of material)expect(readFileSync(join(dir,f)).includes(Buffer.from(secret))).toBe(false);
  expect(()=>en.done('gmail',c,reviewed('gmail'))).toThrow(/credential_verified/);
});
test('waiting and decision release a lease without pretending enrollment is complete',()=>{
  const before=reviewed('proton').work_id;en.wait('proton','email','fixture email pending',c,reviewed('proton'));
  expect(en.next('proton',c).item.work_id).not.toBe(before);en.decide('proton','captcha','fixture decision',c,reviewed('proton'));
  const status=en.status(c);expect(status.decisions).toHaveLength(1);expect(status.by_identity.gmail.done??0).toBe(0);
  expect(status.by_identity.proton.waiting).toBe(1);expect(status.by_identity.proton['needs a decision']).toBe(1);
  en.seed(c);expect(en.status(c).by_identity).toEqual(status.by_identity);
});
test('browser carrier requires confirmation and the reviewed native coordinates for mutations',async()=>{
  const host=createHost({env,tools:{call:async()=>({})}}),review=reviewed('gmail');
  expect((await host.answer({operation:'enroll_record',identity:'gmail',stage:'scope_confirmed',expected:review})).refused).toBe('not_confirmed');
  expect((await host.answer({operation:'enroll_record',identity:'gmail',stage:'scope_confirmed',confirmed:true})).ok).toBe(false);
  const saved=await host.answer({operation:'enroll_record',identity:'gmail',stage:'scope_confirmed',expected:review,confirmed:true});expect(saved.ok).toBe(true);
  expect((await host.answer({operation:'enroll_done',identity:'gmail',expected:reviewed('gmail'),confirmed:true})).ok).toBe(false);
  expect((await host.answer({operation:'enroll_resume',identity:'toString'})).ok).toBe(false);
});
