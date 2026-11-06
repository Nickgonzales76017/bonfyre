// Exercise the exact failure: a published definition moves ahead of an active
// installation. Repair must retain newer tools, old source controls and records.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {homedir} from 'node:os';
import {createHash} from 'node:crypto';
import {workspaceFixture} from './workspace_fixture.mjs';
import {materializeReferenceRepair} from '../../../scripts/materialize_reference_repair.mjs';
import {materializeAuthoredWorkspace} from '../../../scripts/materialize_authored_workspace.mjs';
import {loadWorkspaceFiles} from '../../../scripts/workspace_material.mjs';
import {projectConnectedInstrumentsSetup,publishSetups} from '../../../scripts/publish_experience_setups.mjs';
import {historyFixture} from '../../../scripts/tests/retained-material-fixture.mjs';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';
const pack='experience.connected-instruments.workspace';
const old='c70d97e7004b69149941f1805f16d7f556792110b51977befc39bd663983e654';
const repaired='d8b2d86be17aed532bf84cffb9fb6c75efe1b8c4e3ca1375e4c2730a72b0c25a';
const advanced='ebc9fc98b1b555bd5507344ec263df256249b485e4ca52a0ca2338603586626d';
const keys=['8740bef9171e488a1b73ee26159271beca762ff1','8a2ef2875cc19b091a91f61f7579b266d451a08c','f5cac78045477b052e2cd67f6f4ecbfc4f3c2c1d'].map(k=>'draft:instruments:research:history-'+k);
const hash=b=>createHash('sha256').update(b).digest('hex');
const previous=process.env.BONFYRE_WORKSPACE_RELEASE;
let f,binding,material,original,seeded;
const exact=(row,release=old)=>'/apps/'+binding.id+'/_open?'+new URLSearchParams({view:'research',key:row.key,revision:'1',release,digest:hash(encode([2,row.value,[]]))});
function publish(release){
 const rows=projectConnectedInstrumentsSetup(release),version=rows.find(r=>r.type==='pack_version').value;
 f.add('pack_version',version);
 for(const [type,where,value]of [['pack_head',"channel='stable'",{version:version.version}],['pack_demand',"demand_kind='workspace'",{preferred_sources:release}]]){
  const row=f.sql.query('SELECT id FROM '+type+' WHERE pack=? AND '+where).get(pack);
  f.run(f.cms,['entry','update',type,String(row.id),JSON.stringify(value),'--db',f.db,'--schemas',join(f.home,'schemas')]);
 }
}
beforeAll(async()=>{
 const root=join(homedir(),'.bonfyre/workspace-packages');material=await materializeReferenceRepair({sourceRelease:advanced,root});
 process.env.BONFYRE_WORKSPACE_RELEASE=repaired;f=await workspaceFixture();
 publishSetups({records:projectConnectedInstrumentsSetup(repaired),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});binding=await f.install(pack);
 const history=await historyFixture();seeded=history.rows.filter(r=>keys.includes(r.key)||r.key.includes('history-contract-'));
 for(const row of seeded)await f.store(binding).saveLocal(row.key,row.value,0);
 original=f.sql.query('SELECT * FROM _workspace_record ORDER BY scope_key,record_key').all();
},60000);
afterAll(async()=>{await f?.close();if(previous===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=previous;});
test('combined package keeps every new production tool and changes only the reference-reader seams',async()=>{
 const root=join(homedir(),'.bonfyre/workspace-packages'),a=await loadWorkspaceFiles(root,advanced),b=await loadWorkspaceFiles(root,material.id);
 const changed=[...a].filter(([p,v])=>!v.equals(b.get(p))).map(([p])=>p).sort();expect(changed).toEqual(['SOURCE.tsv','instruments/app.mjs','instruments/research-view.mjs']);
 for(const p of ['instruments/checks-view.mjs','instruments/production-recovery.mjs','instruments/production-runtime/authored-workspace-recovery.mjs','instruments/RECOVERY.tsv','CONTINUATIONS.tsv'])expect(b.get(p)).toEqual(a.get(p));
 const ordinary=await materializeAuthoredWorkspace({root}),normal=await loadWorkspaceFiles(root,ordinary.id);
 expect(normal.get('instruments/research-view.mjs').toString()).toContain('createMaterialReader');expect(normal.has('bridge/retained-material-view.mjs')).toBe(true);expect(normal.has('instruments/checks-view.mjs')).toBe(true);
 expect(b.get('instruments/research-view.mjs').toString()).toContain('createMaterialReader');console.log('MERGED_VERIFIED_PACKAGE',material.id);
});
test('reproduces stale installation, then installs the reconciled package through its owner with every record intact',async()=>{
 publish(advanced);f.add('pack_demand',{pack,demand_kind:'workspace-record',needs:'connected-instruments',fields:'draft:instruments:checks:',status:'published'});
 const broken=await f.raw('/apps/_bindings?context=self&pack='+pack),rows=decode(new Uint8Array(await broken.arrayBuffer()));expect(rows[3].find(r=>r[0]===binding.id)[5]).toBe(false);
 expect((await f.raw(binding.path)).status).toBe(409);
 publish(material.id);const after=await f.install(pack);expect(after.id).toBe(binding.id);
 const ready=decode(new Uint8Array(await (await f.raw('/apps/_bindings?context=self&pack='+pack)).arrayBuffer()));expect(ready[3].find(r=>r[0]===binding.id)[5]).toBe(true);
 expect(f.sql.query('SELECT * FROM _workspace_record ORDER BY scope_key,record_key').all()).toEqual(original);
 for(const row of seeded)expect((await f.store(binding).readVersion(row.key)).value).toEqual(row.value);
},60000);
test('all three original links offer a current exact continuation rather than a dead end',async()=>{
 for(const key of keys){const row=seeded.find(r=>r.key===key),r=await f.raw(exact(row)),body=await r.text();expect(r.status).toBe(409);expect(body).toContain('Your saved work is unchanged');expect(r.headers.get('cache-control')).toContain('no-store');
  const next=body.match(/data-work-current href="([^"]+)"/)?.[1].replaceAll('&amp;','&');expect(next).toBeTruthy();const u=new URL(next,f.base);expect(u.searchParams.get('release')).toBe(material.id);expect(u.searchParams.get('key')).toBe(key);expect(u.searchParams.get('digest')).toBe(hash(encode([2,row.value,[]])));
  const continuation=await f.raw(next);expect(continuation.status).toBe(303);const editor=await f.raw(continuation.headers.get('location'));expect(editor.status).toBe(200);expect(await editor.text()).toContain('bonfyre-workspace-store');
 }
});
test('release recovery never changes revisions, content checks or destination access',async()=>{
 const row=seeded.find(r=>r.key===keys[0]),url=new URL(exact(row),f.base);
 for(const[k,v]of [['digest','0'.repeat(64)],['revision','9']]){const bad=new URL(url);bad.searchParams.set(k,v);const r=await f.raw(bad.pathname+bad.search);expect(r.status).toBe(409);expect(await r.text()).not.toContain('data-work-current');}
 const foreign=await f.raw(url.pathname+url.search,f.other);expect(foreign.status).toBe(403);expect(await foreign.text()).not.toContain(row.value.title);
 const anonymous=await f.raw(url.pathname+url.search,null);expect(anonymous.status).toBe(303);expect(new URL(anonymous.headers.get('location'),f.base).searchParams.get('next')).toBe(url.pathname+url.search);
 expect(f.sql.query('SELECT * FROM _workspace_record ORDER BY scope_key,record_key').all()).toEqual(original);
});
test('Chrome follows an old shared link into the recovered setup and opens its complete contract',async()=>{
 await withChrome(async({open,events})=>{const tab=await open(),[name,...parts]=f.owner.cookie.split('=');await tab.page('Network.setCookie',{name,value:parts.join('='),url:f.base,httpOnly:true});
  const evaluate=async expression=>{const r=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(r.exceptionDetails)throw Error(r.exceptionDetails.exception?.description||r.exceptionDetails.text);return r.result.value;};
  const wait=async expression=>{for(let i=0;i<180;i++){if(await evaluate(expression))return;await Bun.sleep(60);}throw Error('Not reached: '+expression+' / '+await evaluate('document.body.innerText.slice(-1600)'));};
  await tab.go(f.base+exact(seeded.find(r=>r.key===keys[0])));await wait(`document.querySelector('[data-work-current]')`);await evaluate(`document.querySelector('[data-work-current]').click()`);
  await wait(`document.querySelector('#original-source [data-original-start]')&&document.querySelector('#status')?.textContent.includes('Opened exact saved revision')`);expect(await evaluate(`document.querySelector('#original-source').textContent.includes('RewardsOps:')`)).toBe(true);
  await evaluate(`Array.from(document.querySelectorAll('#original-source button')).find(b=>b.textContent==='Shared production contract').click()`);await wait(`document.querySelector('.rm-dialog')?.dataset.verified==='true'`);
  expect(await evaluate(`document.body.innerText.includes('/Users/')`)).toBe(false);expect(events.filter(e=>e.method==='Runtime.exceptionThrown')).toEqual([]);
 },{timeoutMs:45000,window:'1440,1000'});
 expect(f.sql.query('SELECT * FROM _workspace_record ORDER BY scope_key,record_key').all()).toEqual(original);
},60000);
