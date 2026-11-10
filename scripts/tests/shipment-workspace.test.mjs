import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,rmSync,writeFileSync,mkdirSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {shipmentWorkspaceMaterial,shipmentWorkspaceSetup,shipmentPack} from '../materialize_shipment_workspace.mjs';
import {publishSetups} from '../publish_experience_setups.mjs';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {sampleFiles,SAMPLE_SHIPMENTS} from '../../components/BonfyreServiceSite/shipment-material.mjs';
import {appendFiles,snapshotOf,restoreSnapshot,evaluateBatch,reviewedArchive,shipmentList,reviewChoices,batchReports} from '../../integrations/browser/shipment-native-model.mjs';
import {withChrome} from '../../integrations/browser/capture/chrome-session.mjs';
let f,pack,binding,org,temp;const saved=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
const facts=()=>appendFiles([],Object.entries(sampleFiles()).map(([name,b])=>new File([b],name,{lastModified:0})));
const sampleState=async()=>({shipmentsText:SAMPLE_SHIPMENTS,shipments:shipmentList(SAMPLE_SHIPMENTS),files:await facts(),chosen:{}});
beforeAll(async()=>{
 temp=mkdtempSync(join(tmpdir(),'recovered-shipments-'));pack=await shipmentWorkspaceMaterial({root:join(temp,'packages')});process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');f=await workspaceFixture();
 const setups=shipmentWorkspaceSetup(pack.id),result=publishSetups({records:setups,binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});expect(result.created).toBe(setups.length);binding=await f.install(shipmentPack);org=await f.install(shipmentPack,'institution:workspace-team');
},60000);
afterAll(async()=>{await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});for(const[k,v]of Object.entries(saved))if(v===undefined)delete process.env[k];else process.env[k]=v;});
test('the recovered checker still produces its original eight-file result and exact-byte archive',async()=>{
 const state=await sampleState(),result=evaluateBatch(state);expect(result.counts).toEqual({ready:5,duplicate:1,unmatched:1,'date-needs-review':1,'type-needs-review':0});expect(result.missing).toHaveLength(2);
 const bytes=new Uint8Array(await (await reviewedArchive(state)).arrayBuffer());expect(new DataView(bytes.buffer).getUint32(0,true)).toBe(0x04034b50);expect(new TextDecoder().decode(bytes)).toContain('missing-delivery-proof.csv');
 const restored=await restoreSnapshot(snapshotOf(state));expect(evaluateBatch(restored)).toEqual(result);for(let i=0;i<8;i++)expect(new Uint8Array(await restored.files[i].body.arrayBuffer())).toEqual(new Uint8Array(await state.files[i].body.arrayBuffer()));
});
test('bounds, bad dates, traversal, fake file hashes and cancellation refuse before adopting a batch',async()=>{
 const state=await sampleState();expect(()=>shipmentList('shipment_id\n../target\n')).toThrow();expect(()=>shipmentList('shipment_id\nx\nx\n')).toThrow();
 expect(()=>reviewChoices({[state.files[0].key]:{date:'2026-02-31'}},state.files,state.shipments)).toThrow();expect(()=>reviewChoices({[state.files[0].key]:{document_type:'verified-delivery'}},state.files,state.shipments)).toThrow();
 const bad=snapshotOf(state);bad.files[0].sha256='a'.repeat(64);await expect(restoreSnapshot(bad)).rejects.toThrow(/digest/);await expect(restoreSnapshot(snapshotOf(state),{alive:()=>false})).rejects.toThrow(/access/);
 await expect(appendFiles([],Array.from({length:49},(_,i)=>new File(['x'],i+'.pdf')))).rejects.toThrow(/48/);await expect(appendFiles([],[new File(['x'],'../bad.pdf')])).rejects.toThrow(/plain file/);
 const unsafe=await appendFiles([],[new File(['DEMO-1001'], '=formula.jpg')]);const report=batchReports({...state,files:unsafe,chosen:{}});expect(report.checked).toContain("'=formula.jpg");
});
test('current account stores exact originals; metadata listing does not retrieve bodies; foreign access and stale revisions fail',async()=>{
 const value={name:'Native eight-file check',state:snapshotOf(await sampleState())},store=f.store(binding);await store.saveLocal('shipment-batch:roundtrip',value,0);
 const read=await store.readVersion('shipment-batch:roundtrip');expect(read.revision).toBe(1);expect(read.value.state.files).toHaveLength(8);const resumed=await restoreSnapshot(read.value.state);expect(evaluateBatch(resumed).counts.ready).toBe(5);
 const metadata=await store.readMetadata('shipment-batch:');expect(metadata.rows[0].fileCount).toBe(8);expect(metadata.rows[0].value.state.files.every(x=>x.body===null)).toBe(true);
 await expect(f.store(binding,f.other).readVersion('shipment-batch:roundtrip')).rejects.toMatchObject({status:403});await expect(store.saveLocal('shipment-batch:roundtrip',value,0)).rejects.toMatchObject({status:409});
 await expect(store.saveLocal('draft:catalogue:foreign',value,0)).rejects.toThrow();expect((await f.raw(binding.path,null)).status).toBe(303);
 const orgStore=f.store(org);await orgStore.saveLocal('shipment-batch:shared',value,0);await f.store(org,f.writer).saveLocal('shipment-batch:shared',{...value,name:'Writer revision'},1);await expect(f.store(org,f.reader).saveLocal('shipment-batch:shared',value,2)).rejects.toMatchObject({status:403});expect((await f.store(org,f.reader).readVersion('shipment-batch:shared')).revision).toBe(2);
},60000);
async function browser(work,{width=1440,init=''}={}){
 await withChrome(async({open,events})=>{const tab=await open(),[name,...parts]=f.owner.cookie.split('=');await tab.page('Network.setCookie',{name,value:parts.join('=') ,url:f.base,httpOnly:true});if(width<800)await tab.page('Emulation.setDeviceMetricsOverride',{width,height:900,deviceScaleFactor:1,mobile:true});if(init)await tab.page('Page.addScriptToEvaluateOnNewDocument',{source:init});
 const run=async expression=>{const v=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(v.exceptionDetails)throw Error(v.exceptionDetails.exception?.description||v.exceptionDetails.text);return v.result.value;};
 const wait=async expression=>{for(let i=0;i<160;i++){if(await run(expression))return;await Bun.sleep(50);}throw Error('Browser did not reach '+expression+' / '+await run('document.body.innerText.slice(-1600)'));};
 const click=async label=>{console.log('SHIPMENT_BROWSER_ACTION',label);await run(`(()=>{const b=[...document.querySelectorAll('button')].find(n=>n.textContent.trim()===${JSON.stringify(label)}&&n.getClientRects().length&&!n.disabled);if(!b)throw Error('Missing enabled button: '+${JSON.stringify(label)});b.click();})()`);console.log('SHIPMENT_BROWSER_DISPATCHED',label);};
 await tab.go(f.base+binding.path);await wait(`document.querySelector('[data-shipment-workspace]')?.dataset.ready==='true'`);await work({tab,run,wait,click,events});expect(events.filter(e=>e.method==='Runtime.exceptionThrown')).toEqual([]);
 },{timeoutMs:80000,window:width+',1000'});
}
test('real browser reads files, corrects a date, saves/reopens originals, compares the current native record and exports',()=>browser(async({tab,run,wait,click})=>{
 await click('Try the eight-file example');await wait(`document.querySelectorAll('.ac-row').length===8`);expect(await run(`Array.from(document.querySelectorAll('.sw-stats dd')).map(x=>x.textContent)`)).toEqual(['8','5','3','2']);
 await run(`Array.from(document.querySelectorAll('.ac-title')).find(x=>x.textContent.includes('label_unclear.pdf')).click()`);await wait(`document.querySelector('[aria-label="Reviewed date"]')`);
 await run(`document.querySelector('[aria-label="Reviewed date"]').value='2026-10-03'`);await click('Apply reviewed fields');expect(await run(`document.querySelectorAll('.sw-stats dd')[1].textContent`)).toBe('6');
 await run(`document.querySelector('[data-is=name]').value='Browser shipment review'`);await click('Save');await wait(`document.querySelector('[data-is=said]')?.textContent.includes('Saved "Browser shipment review"')`);
 const key=await run(`new URL(location.href).searchParams.get('record')`),row=await f.store(binding).readVersion(key);expect(row.value.state.files).toHaveLength(8);expect(Object.values(row.value.state.chosen)[0].date).toBe('2026-10-03');
 await tab.go(f.base+binding.path+'?record='+encodeURIComponent(key));await wait(`document.querySelector('[data-is=said]')?.textContent.includes('Opened "Browser shipment review"')`);expect(await run(`document.querySelectorAll('.ac-row').length`)).toBe(8);
 await click('Checked handoff');await click('Build checked archive');await wait(`document.querySelector('.sw-notice').textContent.includes('Checked archive created')`);
 await click('Review files');await click('Find an action ⌘ K');expect(await run(`document.querySelector('.ak-command-dialog').open`)).toBe(true);await tab.page('Input.dispatchKeyEvent',{type:'keyDown',key:'Escape',code:'Escape'});await tab.page('Input.dispatchKeyEvent',{type:'keyUp',key:'Escape',code:'Escape'});
 expect(await run(`document.documentElement.scrollWidth<=innerWidth+1`)).toBe(true);const out=process.env.BONFYRE_WORKFLOW_SCREENSHOTS;if(out){mkdirSync(out,{recursive:true});const shot=await tab.page('Page.captureScreenshot',{format:'png'});writeFileSync(join(out,'shipment-desktop.png'),Buffer.from(shot.data,'base64'));}
}),120000);
test('lost acknowledgement checks the exact committed batch without duplicating originals or writes',()=>browser(async({run,wait,click})=>{
 await click('Try the eight-file example');await wait(`document.querySelectorAll('.ac-row').length===8`);await run(`document.querySelector('[data-is=name]').value='Lost acknowledgement';window.fixtureDropSave=true;`);await click('Save');await wait(`document.querySelector('[data-is=said]').textContent.includes('could not be confirmed')`);
 const count=f.sql.query('SELECT count(*) n FROM _workspace_record').get().n;await click('Check saved result');await wait(`document.querySelector('[data-is=said]').textContent.includes('Saved "Lost acknowledgement"')`);expect(f.sql.query('SELECT count(*) n FROM _workspace_record').get().n).toBe(count);expect(await run('window.fixtureWrites')).toBe(1);
},{init:`(()=>{const original=fetch.bind(window);window.fixtureWrites=0;window.fetch=async(...args)=>{const u=new URL(String(args[0]),location.href),r=await original(...args);if(u.pathname.endsWith('/_records')&&u.searchParams.get('operation')==='write'){window.fixtureWrites++;if(window.fixtureDropSave){window.fixtureDropSave=false;await r.arrayBuffer();return new Response('Fixture lost acknowledgement',{status:503});}}return r;};})();`}),120000);
test('mobile selection and account change keep private originals out of the next context',()=>browser(async({run,wait,click,tab})=>{
 await click('Try the eight-file example');await wait(`document.querySelectorAll('.ac-row').length===8`);await run(`document.querySelector('.ac-check input').click()`);await click('Assign selected to a shipment');await wait(`document.querySelector('.sw-dialog')?.open`);await click('Keep current batch');expect(await run(`document.documentElement.scrollWidth<=innerWidth+1`)).toBe(true);
 const out=process.env.BONFYRE_WORKFLOW_SCREENSHOTS;if(out){mkdirSync(out,{recursive:true});const shot=await tab.page('Page.captureScreenshot',{format:'png'});writeFileSync(join(out,'shipment-phone.png'),Buffer.from(shot.data,'base64'));}
 await run(`dispatchEvent(new Event('bonfyre:account-changed'))`);await wait(`document.body.innerText.includes('Private batch contents were cleared')`);expect(await run(`document.querySelectorAll('.ac-row,.sw-correction,.instrument-store').length`)).toBe(0);
},{width:390}),120000);
