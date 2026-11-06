// Existing adoption and save carriers against real isolated native owners. The
// small pages are transport fixtures, not catalogue geometry or public release proofs.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {readFileSync,mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {workspaceFixture} from '../../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {retainWorkspaceFiles} from '../../../scripts/materialize_workspace_package.mjs';
import {continuationManifest} from '../../../scripts/workspace_continuation.mjs';
import {prepareWorkspaceAdoption} from '../workspace-adoption.mjs';
import {planAdoptionSelection} from '../workspace-adoption-review.mjs';
import {openDestinationStore} from '../workspace-store.mjs';
import {withChrome} from '../capture/chrome-session.mjs';
let temp,f,binding,release,source;
const savedEnv=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
const hash=s=>createHash('sha256').update(s).digest('hex');
function material(tag){
  const scope=hash(tag),digest=hash('fixture logical model identity:'+tag),bodyKey=`product-model:${scope}:body:${digest}`;
  const root={key:`product-model:${scope}:binding:${encodeURIComponent(tag)}`,revision:7,value:{scope,productId:tag,digest,unit:'metres',name:'model.gltf',variant:null}};
  const body={key:bodyKey,revision:2,value:{files:[{name:'model.gltf',blob:new File(['{"asset":{"version":"2.0"}}'],'model.gltf',{type:'model/gltf+json',lastModified:1000})}]}};
  return {root,body,bodyKey};
}
beforeAll(async()=>{
  temp=mkdtempSync(join(tmpdir(),'adoption-closure-native-'));const root=join(temp,'packages');
  const html=Buffer.from('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Isolated adoption</title><meta name="bonfyre-workspace-store" content="/apps/__WORKSPACE__/_records"><meta name="bonfyre-workspace-context" content="__DESTINATION__"><meta name="bonfyre-workspace-release" content="__RELEASE__"></head><body><nav data-native-workspace aria-label="Workspace"></nav><h1>Adoption fixture</h1><input aria-label="Unsaved note" id="unsaved"><script type="module" src="../bridge/workspace-adoption.mjs"></script></body></html>');
  const files=new Map([['catalogue/index.html',html],['product-room/index.html',html]]);
  for(const name of ['workspace-adoption.mjs','workspace-adoption-review.mjs','workspace-store.mjs','workspace-msgpack.mjs','workspace-value.mjs','workspace-continuation.mjs','interaction-collection.mjs','record-comparison.mjs'])files.set('bridge/'+name,readFileSync(new URL('../'+name,import.meta.url)));
  files.set('CONTINUATIONS.tsv',continuationManifest([{view:'room',label:'Room plan',entry:'product-room/',prefix:'fixture-plan:',title:'plan.title',query:'record=$key'},{view:'product',label:'Product draft',entry:'catalogue/',prefix:'draft:catalogue:',title:'title',query:'record=$key'}],files));
  release=await retainWorkspaceFiles(files,{root});process.env.BONFYRE_WORKSPACE_RELEASE=release.id;process.env.BONFYRE_WORKSPACE_PACKAGES=root;
  f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');
  const facade=readFileSync(new URL('../extension/store.mjs',import.meta.url));
  source=Bun.serve({hostname:'127.0.0.1',port:0,fetch(request){return new URL(request.url).pathname==='/store.mjs'?new Response(facade,{headers:{'Content-Type':'text/javascript'}}):new Response('<!doctype html><html><head><title>Isolated original work</title></head><body><script type="module" src="/store.mjs"></script></body></html>',{headers:{'Content-Type':'text/html'}});}});
},60000);
afterAll(async()=>{source?.stop(true);await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});for(const[k,v]of Object.entries(savedEnv))if(v===undefined)delete process.env[k];else process.env[k]=v;});
const prepare=(rows,store=f.store(binding))=>prepareWorkspaceAdoption(rows,{readVersion:k=>store.readVersion(k)});
test('the native transaction adopts a binding and required original files together',async()=>{
  const m=material('native-new'),sourceRows=[m.root,m.body],original=structuredClone(sourceRows),store=f.store(binding);
  const plan=planAdoptionSelection(await prepare(sourceRows),[m.root.key]);await store.saveLocalBatch(plan.changes);
  const other=f.store(binding,f.writer),body=await other.readVersion(m.bodyKey),root=await other.readVersion(m.root.key);
  expect(root.revision).toBe(1);expect(root.value.digest).toBe(m.root.value.digest);expect(body.revision).toBe(1);expect(await body.value.files[0].blob.text()).toBe(await original[1].value.files[0].blob.text());expect(body.value.files[0].blob.name).toBe('model.gltf');expect(sourceRows[0].revision).toBe(7);
});
test('existing native bodies are checked without a duplicate body revision',async()=>{
  const m=material('native-existing'),store=f.store(binding);await store.saveLocal(m.bodyKey,m.body.value,0);
  const plan=planAdoptionSelection(await prepare([m.root]),[m.root.key]);expect(plan.checks).toBe(1);await store.saveLocalBatch(plan.changes);expect((await store.readVersion(m.bodyKey)).revision).toBe(1);
});
test('a dependency removed after preview aborts the entire native adoption',async()=>{
  const m=material('native-removed'),store=f.store(binding);await store.saveLocal(m.bodyKey,m.body.value,0);
  const plan=planAdoptionSelection(await prepare([m.root]),[m.root.key]);await f.store(binding,f.writer).deleteLocal(m.bodyKey);
  await expect(store.saveLocalBatch(plan.changes)).rejects.toMatchObject({status:409});expect(await store.readVersion(m.root.key)).toBeNull();expect((await store.readVersion(m.bodyKey)).deleted).toBe(true);
});
test('reader and unrelated native users cannot apply an adoption plan',async()=>{
  const m=material('native-private'),plan=planAdoptionSelection(await prepare([m.root,m.body]),[m.root.key]);
  for(const user of [f.reader,f.other])await expect(f.store(binding,user).saveLocalBatch(plan.changes)).rejects.toMatchObject({status:403});
  expect(await f.store(binding).readVersion(m.root.key)).toBeNull();
});
test('a lost accepted response replays the same native batch without duplicate revisions',async()=>{
  const m=material('native-lost');let lost=false;
  const store=openDestinationStore(binding.endpoint,{origin:f.base,request:async(url,options)=>{const response=await f.raw(url,f.owner,options);if(new URL(url).searchParams.get('operation')==='write'&&response.ok&&!lost){lost=true;await response.arrayBuffer();throw Error('Fixture lost reply');}return response;}});
  const review=await prepare([m.root,m.body],store),plan=planAdoptionSelection(review,[m.root.key]);
  await expect(store.saveLocalBatch(plan.changes)).rejects.toThrow('Fixture lost reply');await store.saveLocalBatch(planAdoptionSelection(review,[m.root.key]).changes);
  expect((await f.store(binding).readVersion(m.root.key)).revision).toBe(1);expect((await f.store(binding).readVersion(m.bodyKey)).revision).toBe(1);
});
test('Chrome source selection, dependency preview, native adoption and exact continuation work together',async()=>{
  const m=material('browser-café'),originalURL=`http://127.0.0.1:${source.port}/catalogue/`,roomKey='fixture-plan:browser-exact';
  await withChrome(async({open,send,events})=>{
    const src=await open(),dest=await open();
    const evalFor=page=>async expression=>{const r=await page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true,userGesture:true});if(r.exceptionDetails)throw Error(r.exceptionDetails.exception?.description??r.exceptionDetails.text);return r.result.value;};
    const es=evalFor(src.page),ed=evalFor(dest.page);
    const until=async(fn,predicate=x=>x)=>{for(let i=0;i<200;i++){const value=await fn();if(predicate(value))return value;await Bun.sleep(50);}throw Error('Browser condition did not arrive.');};
    const click=(ev,label)=>ev(`(()=>{const b=[...document.querySelectorAll('button')].find(b=>b.textContent.trim()===${JSON.stringify(label)}&&!b.disabled&&!b.hidden);if(!b)throw Error('Missing enabled button');b.click();return true;})()`);
    await src.go(originalURL);
    const original=await es(`(async()=>{const s=await import('/store.mjs');const value={title:'reviewed',file:new File(['file-bytes'],'original.txt',{lastModified:123})};const save=s.saveLocal('draft:catalogue:browser-snapshot',value,0);value.title='later';await save;const snapshot=(await s.readLocal('draft:catalogue:browser-snapshot'))[0];await s.saveLocal(${JSON.stringify(m.root.key)},${JSON.stringify(m.root.value)},0);await s.saveLocal(${JSON.stringify(m.bodyKey)},{files:[{name:'model.gltf',blob:new File(['{"asset":{"version":"2.0"}}'],'model.gltf',{type:'model/gltf+json',lastModified:1000})}]},0);await s.saveLocal(${JSON.stringify(roomKey)},{plan:{id:'browser-plan',title:'A kept room',items:[{key:'unchanged-part',product:'exact-SKU',x:2.25}]},receipt:{reference:'source-only'}},0);return {title:snapshot.value.title,file:snapshot.value.file.name,rows:(await s.readLocal()).map(r=>[r.key,r.revision])};})()`);
    expect(original.title).toBe('reviewed');expect(original.file).toBe('original.txt');
    const[name,...rest]=f.owner.cookie.split(';')[0].split('=');await dest.page('Network.setCookie',{name,value:rest.join('='),domain:new URL(f.base).hostname,path:'/',httpOnly:true});await dest.go(new URL(binding.path,f.base).href);
    await until(()=>ed(`!!document.querySelector('[data-adopt-browser]')`));
    await ed(`(async()=>{document.querySelector('#unsaved').value='leave this alone';document.querySelector('[data-adopt-browser]').remove();const base=document.querySelector('meta[name="bonfyre-workspace-store"]').content.replace('_records','bridge/');const a=await import(base+'workspace-adoption.mjs');a.installWorkspaceAdoption({sources:[['Isolated source',${JSON.stringify(originalURL)}]]});})()`);
    await click(ed,'Bring browser drafts');await click(ed,'Open Isolated source');
    const target=await until(async()=>{const r=await send('Target.getTargets');return r.targetInfos.find(t=>t.url.startsWith(originalURL)&&t.url.includes('aurekai-transfer='));});
    const{sessionId}=await send('Target.attachToTarget',{targetId:target.targetId,flatten:true});const pp=(method,params)=>send(method,params,sessionId);await pp('Runtime.enable');const ep=evalFor(pp);
    await until(()=>ep(`!![...document.querySelectorAll('button')].find(b=>b.textContent==='Show my saved drafts')`));await click(ep,'Show my saved drafts');
    await until(()=>ep(`document.querySelectorAll('input[type=checkbox]').length===4`));
    await ep(`for(const i of document.querySelectorAll('input[type=checkbox]'))if(${JSON.stringify([m.root.key,m.bodyKey,roomKey])}.includes(i.value))i.click();`);await click(ep,'Send selected work for review');
    await until(()=>ed(`document.querySelector('[data-adoption-status]')?.textContent.includes('Source versions')`));
    expect(await f.store(binding).readVersion(m.root.key)).toBeNull();
    expect(await ed(`(()=>{const box=document.querySelector('input[value="'+${JSON.stringify(m.bodyKey)}+'"]');if(!box||!box.checked||box.disabled)throw Error('The required body must be visibly selected before this test changes it.');box.click();return !box.checked;})()`)).toBe(true);
    expect(await ed(`document.querySelectorAll('.ac-collection input[type=checkbox]:checked').length`)).toBe(2);
    expect(await ed(`document.querySelector('[data-adoption-dependencies]').textContent.includes(${JSON.stringify(m.bodyKey)})`)).toBe(true);
    await click(ed,'Adopt selected drafts');await until(()=>ed(`document.querySelector('[data-adoption-status]')?.textContent.includes('3 drafts adopted')`));
    const room=await f.store(binding,f.writer).readVersion(roomKey);expect(room.value.plan.items[0].x).toBe(2.25);expect(room.value.receipt).toBeNull();
    expect((await f.store(binding).readVersion(m.bodyKey)).value.files[0].blob.name).toBe('model.gltf');
    const link=await ed(`document.querySelector('[data-adopted-continuation]').getAttribute('href')`),views=await f.raw(link),text=await views.text();expect(views.status).toBe(200);
    const next=text.match(/data-compatible-view href="([^"]+)"/)?.[1].replaceAll('&amp;','&');expect(next).toBeDefined();const redirect=await f.raw(next);expect(redirect.status).toBe(303);expect(new URL(redirect.headers.get('location'),f.base).searchParams.get('record')).toBe(roomKey);
    expect(await ed(`document.querySelector('#unsaved').value`)).toBe('leave this alone');
    expect(await es(`(async()=>{const s=await import('/store.mjs');return (await s.readLocal()).map(r=>[r.key,r.revision]);})()`)).toEqual(original.rows);
    expect(events.filter(e=>e.method==='Runtime.exceptionThrown').length).toBe(0);
  },{timeoutMs:60000});
},90000);
