// Actual research editor and native Auth/CMS/WebAddress owners in a throwaway
// estate. BONFYRE_TEST_REPO locates native fixtures/binaries when reviewing an
// unapplied candidate. It never changes their source or the live estate.
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { synthesisFiles } from '../synthesis-material.mjs';
import { render } from '../sections/synthesis.mjs';

const repo = resolve(process.env.BONFYRE_TEST_REPO ?? join(import.meta.dir, '../../..'));
const fromRepo = path => import(pathToFileURL(join(repo, path)).href);
const { workspaceFixture } = await fromRepo('cmd/BonfyreWebAddress/tests/workspace_fixture.mjs');
const { retainWorkspaceFiles } = await fromRepo('scripts/materialize_workspace_package.mjs');
const { continuationManifest } = await fromRepo('scripts/workspace_continuation.mjs');
const { withChrome } = await fromRepo('integrations/browser/capture/chrome-session.mjs');
const temp = mkdtempSync(join(tmpdir(), 'campaign-native-'));
const previous = Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE', 'BONFYRE_WORKSPACE_PACKAGES'].map(k => [k, process.env[k]]));
const hash = body => createHash('sha256').update(body).digest('hex');
const seen = [];
let f;
try {
  // Retain the real synthesis package through the existing release owner.
  // This fixture installs only research; it does not claim a full gallery build.
  const material = synthesisFiles(join(repo, 'packs/aurekai-work-samples/site/synthesis-four-day-week-20261019.json'));
  const esc = text => String(text).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const section = render({id:'research',title:'Research desk',data:'/data/research/',...material},{},{esc});
  const files = new Map([['research/index.html', Buffer.from('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"><title>Research desk</title></head><body>'
    + section.replaceAll('/data/research/' + material.asset_prefix, './') + '</body></html>')]]);
  for (const [path,body] of Object.entries(material.files)) files.set('research/' + path.slice(material.asset_prefix.length), body);
  for (const name of ['workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs']) files.set('bridge/'+name,readFileSync(join(repo,'integrations/browser',name)));
  files.set('CONTINUATIONS.tsv',continuationManifest([{view:'review',label:'Research review',entry:'research/',prefix:'review-project:',title:'name',query:'record=$key'}],files));
  const pack = await retainWorkspaceFiles(files,{root:join(temp,'packages')});
  process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');
  f=await workspaceFixture();
  const packName='campaign-research-fixture';f.pack(packName,'Campaign research fixture');
  f.add('pack_demand',{pack:packName,demand_kind:'workspace',needs:'spaces',preferred_sources:pack.id,fields:'research/',status:'published'});
  f.add('pack_demand',{pack:packName,demand_kind:'workspace-record',needs:'spaces',fields:'review-project:',status:'published'});
  const binding=await f.install(packName,'institution:workspace-team');
  const ownerStore=f.store(binding),writerStore=f.store(binding,f.writer);
  const run=async(tab,expression)=>{
    const r=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});
    if(r.exceptionDetails)throw Error(r.exceptionDetails.exception?.description??r.exceptionDetails.text);
    return r.result.value;
  };
  const setup=`const q=s=>document.querySelector('[data-ui="'+s+'"]'),c=s=>document.querySelector('[data-c="'+s+'"]'),i=s=>document.querySelector('[data-is="'+s+'"]');
    const wait=async f=>{const end=Date.now()+12000;while(!f()){if(Date.now()>end)throw Error('Native editor wait timed out');await new Promise(r=>setTimeout(r,30));}};
    const change=(el,value)=>{el.value=value;el.dispatchEvent(new Event('input',{bubbles:true}));};`;
  const ready=tab=>run(tab,`(async()=>{${setup}await wait(()=>i('save')&&!q('ready').hidden);})()`);
  const openPost=tab=>run(tab,`(async()=>{${setup}document.querySelector('[data-post]').click();await wait(()=>!q('compose').hidden);})()`);
  const cookie=async(tab,user)=>{
    const [name,...value]=user.cookie.split('=');
    await tab.page('Network.setCookie',{name,value:value.join('='),domain:'127.0.0.1',path:'/',httpOnly:true});
  };
  let key, savedURL;
  await withChrome(async({open,until,events})=>{
    const tab=await open();await cookie(tab,f.owner);
    await tab.page('Page.addScriptToEvaluateOnNewDocument',{source:`localStorage.setItem('aurekai-research-desk-v1','{"private":"previous-account-sentinel"}');window.legacyReads=[];const original=Storage.prototype.getItem;Storage.prototype.getItem=function(key){if(key==='aurekai-research-desk-v1')window.legacyReads.push(key);return original.call(this,key);};`});
    await tab.go(new URL(binding.path,f.base).href);await ready(tab);await openPost(tab);
    assert.equal(await run(tab,`document.querySelector('.instrument-store .is-where').textContent`),'Saving to this Aurekai destination');
    const saved=await run(tab,`(async()=>{${setup}change(c('quote'),'Native campaign wording');document.querySelector('[data-pick="story"]').click();document.querySelector('[data-theme="paper"]').click();change(c('scale'),'0.75');change(i('name'),'Native campaign');i('save').click();await wait(()=>document.querySelector('[data-save-state="saved"]'));return {key:new URL(location.href).searchParams.get('record'),url:location.href,legacyReads:window.legacyReads};})()`);
    key=saved.key;savedURL=saved.url;assert.deepEqual(saved.legacyReads,[]);
    const row=await writerStore.readVersion(key);assert.equal(row.revision,1);assert.equal(row.value.state.campaigns[0].content.quote,'Native campaign wording');assert.equal(row.value.state.campaigns[0].overrides.story.theme,'paper');
    seen.push('native editor saves campaign state; second member reads the same version; legacy browser draft remains unread');
    await tab.go(savedURL);await ready(tab);
    await run(tab,`(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));})()`);await openPost(tab);
    assert.equal(await run(tab,`document.querySelector('[data-c="quote"]').value`),'Native campaign wording');
    assert.equal(await run(tab,`document.querySelector('[data-c="scale"]').value`),'0.75');seen.push('native continuation URL reopens edited artboard and settings');
    // The writer's actual commit invalidates the original editor revision.
    row.value.state.campaigns[0].content.quote='Other member revision';await writerStore.saveLocal(key,row.value,row.revision);
    await run(tab,`(async()=>{${setup}change(c('quote'),'My unsaved revision');i('save').click();await wait(()=>document.querySelector('[data-save-state="conflict"]'));})()`);
    assert.equal(await run(tab,`document.querySelector('[data-c="quote"]').value`),'My unsaved revision');assert.equal((await ownerStore.readVersion(key)).value.state.campaigns[0].content.quote,'Other member revision');
    seen.push('native stale save preserves both the committed artboard and unsaved editor');
    // Save a copy, lose its response after the real native commit, then reconcile.
    await tab.page('Fetch.enable',{patterns:[{urlPattern:'*'+binding.endpoint+'?operation=write*',requestStage:'Response'}]});
    const eventStart=events.length;
    await run(tab,`document.querySelector('[data-is="copy"]').click()`);
    const paused=await until((event,index)=>index>=eventStart&&event.sessionId===tab.sessionId&&event.method==='Fetch.requestPaused'&&event.params.request.method==='POST','Save did not reach the native response boundary');
    assert.equal(paused.params.responseStatusCode,200);
    await tab.page('Fetch.failRequest',{requestId:paused.params.requestId,errorReason:'ConnectionClosed'});
    await tab.page('Fetch.disable');
    await run(tab,`(async()=>{${setup}await wait(()=>document.querySelector('[data-save-state="uncertain"]'));})()`);
    const beforeCheck=await ownerStore.readLocal('review-project:');assert.equal(beforeCheck.length,2);
    await run(tab,`(async()=>{${setup}i('check').click();await wait(()=>document.querySelector('[data-save-state="saved"]'));})()`);
    const afterCheck=await ownerStore.readLocal('review-project:');assert.equal(afterCheck.length,2);
    const copyKey=await run(tab,`new URL(location.href).searchParams.get('record')`);
    assert.notEqual(copyKey,key);assert.equal((await ownerStore.readVersion(copyKey)).revision,1);
    assert.equal((await ownerStore.readVersion(copyKey)).value.state.campaigns[0].content.quote,'My unsaved revision');
    seen.push('lost native save acknowledgement reconciles the exact copy at version one without repeating the write');
    // Revocation is checked at effect time, including on an already-open editor.
    f.run(f.auth,['--db',f.authdb,'institution-revoke','--user-id',String(f.owner.id),'--institution','workspace-team']);
    await run(tab,`(async()=>{${setup}change(c('quote'),'After revocation');i('save').click();await wait(()=>document.querySelector('[data-save-state="refused"]'));})()`);
    assert.equal(await run(tab,`document.querySelector('[data-c="quote"]').value`),'After revocation');
    assert.equal((await writerStore.readVersion(copyKey)).revision,1);seen.push('revoked member cannot save from an already-open editor; local work remains');
  },{timeoutMs:90000});
  // A fresh reader browser uses the same saved record and real native authority.
  await withChrome(async({open})=>{
    const tab=await open();await cookie(tab,f.reader);await tab.go(savedURL);await ready(tab);
    await run(tab,`(async()=>{${setup}await wait(()=>/Opened/.test(i('said').textContent));})()`);await openPost(tab);
    assert.equal(await run(tab,`document.querySelector('[data-c="quote"]').value`),'Other member revision');
    await run(tab,`(async()=>{${setup}change(c('quote'),'Reader attempt');i('save').click();await wait(()=>document.querySelector('[data-save-state="refused"]'));})()`);
    assert.equal(await run(tab,`document.querySelector('[data-c="quote"]').value`),'Reader attempt');
    assert.equal((await writerStore.readVersion(key)).revision,2);seen.push('reader reopens shared artboard but cannot save its edits');
  },{timeoutMs:45000});
  await assert.rejects(f.store(binding,f.other).readVersion(key),e=>e.status===403);
  await assert.rejects(f.store(binding,f.reader).saveLocal(key,{},2),e=>e.status===403);
  await assert.rejects(writerStore.saveLocal('plan:outside-research',{},0),e=>[400,403].includes(e.status));
  assert.equal((await f.raw(binding.path,f.other)).status,403);
  const anonymous=await f.raw(binding.path,null);
  assert.ok([303,401,403].includes(anonymous.status));
  if(anonymous.status===303)assert.equal(new URL(anonymous.headers.get('location'),f.base).pathname,'/login');
  seen.push('outsider, anonymous page access and foreign record prefix are refused by native owners');
  const binaries=Object.fromEntries(['web','cms','auth'].map(k=>[k,{path:f[k],sha256:hash(readFileSync(f[k]))}]));
  console.log(JSON.stringify({passed:seen.length,checks:seen,release:pack.id,binaries,scope:'Candidate research package on real native owners in an isolated throwaway estate; no live-account or public-release claim.'},null,2));
} finally {
  await f?.close();rmSync(temp,{recursive:true,force:true});
  for(const[k,v]of Object.entries(previous))if(v===undefined)delete process.env[k];else process.env[k]=v;
}
