import { test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { build } from '../build.mjs';
import { materializeLocalWorkspacePackage } from '../../../scripts/materialize_workspace_package.mjs';
import { workspaceFixture } from '../../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import { openDestinationStore } from '../../../integrations/browser/workspace-store.mjs';
import { prepareCatalogueRelease, saveCatalogueRelease, reviewCatalogueRelease, releasePrefix, verifyCatalogueRelease } from '../catalogue-release.mjs';
import { releaseFixture, fixtureCatalogue, product } from './catalogue-release-fixture.mjs';

let temp, f, binding, pack, store;
const source = releaseFixture(), savedEnv = Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(key => [key,process.env[key]]));
const prepare = async (id, rows) => prepareCatalogueRelease({ ...source, records: rows ?? await store.readLocal(), id, title: 'Fixture price release', createdAt: '2026-10-17T12:00:00Z' });
const checks = [];
beforeAll(async () => {
  temp = mkdtempSync(join(tmpdir(),'catalogue-release-')); const site = join(temp,'site'), out = join(temp,'public'); mkdirSync(site);
  const content = JSON.parse(readFileSync(new URL('./fixtures/sample-lawn/content.json',import.meta.url)));
  content.leads = 'server';
  content.pages = [
    {path:'catalogue',title:'Catalogue',menu:'Catalogue',layout:'workspace',sections:[{kind:'catalogue',id:'products',title:'Fixture catalogue',material:'products'}]},
    {path:'product-room',title:'Rooms',menu:'Rooms',layout:'workspace',sections:[{kind:'planner',id:'rooms',title:'Fixture rooms',material:'rooms',presentation:{brand:'Fixture store',intro:'Plan your showroom.',descriptor:'Fixtures',accent:'#34523d',planning_url:'https://fixture.example/planning',presets:{compact:'Compact'}}}]}
  ];
  writeFileSync(join(site,'content.json'),JSON.stringify(content));
  writeFileSync(join(site,'material.mjs'),`import {material,fixtureInput} from ${JSON.stringify(new URL('./catalogue-release-fixture.mjs',import.meta.url).href)};\nimport {plannerFiles} from ${JSON.stringify(new URL('../planner-material.mjs',import.meta.url).href)};\nexport const products=()=>material;export const rooms=()=>plannerFiles(fixtureInput);\n`);
  await build(site,out);
  const compiled=await Bun.build({entrypoints:[join(out,'data/products/catalogue-client.mjs')],target:'browser',external:['/apps/workspace-store.mjs']});
  if(!compiled.success)throw new AggregateError(compiled.logs,'Catalogue modules did not build.');
  const files = new Map();
  function gather(dir, prefix='') { for (const entry of readdirSync(dir,{withFileTypes:true})) {
    const path=prefix+entry.name; if(entry.isDirectory()){gather(join(dir,entry.name),path+'/');continue;}
    if (!/\.(?:html|mjs|js|css|gz|txt)$/.test(path)) continue;
    let bytes=readFileSync(join(dir,entry.name));
    if (/\.(?:html|mjs|js|css)$/.test(path)) bytes=Buffer.from(bytes.toString().replace(/(["'])\/(data|assets|catalogue|product-room)\//g,'$1/bonfyre/spaces/$2/').replaceAll('href="/#','href="/bonfyre/spaces/#').replaceAll('href="/"','href="/bonfyre/spaces/"'));
    files.set(path,bytes);
  } }
  gather(out);
  process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');
  pack=await materializeLocalWorkspacePackage(files,{root:process.env.BONFYRE_WORKSPACE_PACKAGES});process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;
  f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');store=f.store(binding);
  await store.saveLocalBatch(source.records.map(row=>({key:row.key,value:row.value,expectedRevision:0})));
},60000);
afterAll(async()=>{
  if(f && process.env.CATALOGUE_RELEASE_PROOF && checks.length===5){mkdirSync(process.env.CATALOGUE_RELEASE_PROOF,{recursive:true});writeFileSync(join(process.env.CATALOGUE_RELEASE_PROOF,'native.json'),JSON.stringify({package:pack.id,source:pack.sourceId,checks,browser_verified:false,live_deployment:false,scope:'Real isolated Auth, CMS, WebAddress and store transport; fictional merchant fixture.'},null,2)+'\n');}
  await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});
  for(const[key,value]of Object.entries(savedEnv))if(value===undefined)delete process.env[key];else process.env[key]=value;
});
test('the actual catalogue release view, fixture sources and native continuation ship in the bound package',async()=>{
  const response=await f.raw(binding.path),html=await response.text();expect(response.status).toBe(200);
  expect(html).toContain('data-workspace-view="releases"');expect(html).toContain('data-ui="releases"');expect(html).toContain(fixtureCatalogue.digest);
  expect(html).toContain(binding.endpoint);expect(html).not.toContain('__WORKSPACE__');
  for(const file of ['catalogue-release.mjs','catalogue-release-view.mjs','planner-model.mjs','store.mjs'])expect((await f.raw(`/apps/${binding.id}/data/products/${file}`)).status).toBe(200);
  const value=await prepare('native-release-001');await saveCatalogueRelease(store,value);
  const view=await f.raw(`/apps/${binding.id}/_views?`+new URLSearchParams({key:releasePrefix+value.id,revision:'1',release:pack.id}));expect(view.status).toBe(200);
  const target=(await view.text()).match(/data-compatible-view href="([^"]+)"/)?.[1].replaceAll('&amp;','&');expect(target).toBeDefined();
  const continued=await f.raw(target);expect(continued.status).toBe(303);const url=new URL(continued.headers.get('location'),f.base);
  expect(url.searchParams.get('release')).toBe(releasePrefix+value.id);expect(url.searchParams.get('workspace')).toBe('releases');
  checks.push('built catalogue release UI assets, retained fixture sources and exact native continuation');
});
test('another authorized member reviews and reopens the same release without changing product or room drafts',async()=>{
  const writer=f.store(binding,f.writer),row=(await writer.readLocal(releasePrefix+'native-release-001'))[0];
  const current=await prepare(row.value.id),value=await reviewCatalogueRelease(row.value,current,'Fixture quantities and revised budget checked.');
  await saveCatalogueRelease(writer,value,row.revision);
  const read=(await f.store(binding,f.reader).readLocal(row.key))[0];expect(read.revision).toBe(2);expect(await verifyCatalogueRelease(JSON.parse(JSON.stringify(read.value)))).toEqual(value);
  expect(read.value.impact.impacted[0].proposed_cents).toBe(80000);expect(read.value.publication).toBeNull();
  for(const original of source.records){const kept=(await store.readLocal(original.key))[0];expect(kept.revision).toBe(1);expect(kept.value).toEqual(original.value);}
  checks.push('shared release review, exact JSON round trip, budget impact and unchanged source drafts');
});
test('a concurrent source edit atomically refuses staging and stale review revisions cannot overwrite newer notes',async()=>{
  const proposed=await prepare('native-stale-001'),room=(await store.readLocal(source.records[1].key))[0];
  await store.saveLocal(room.key,{...room.value,plan:{...room.value.plan,notes:'Another member changed the room.'}},room.revision);
  await expect(saveCatalogueRelease(store,proposed)).rejects.toMatchObject({status:409});expect(await store.readLocal(releasePrefix+proposed.id)).toEqual([]);
  const refreshed=await prepare(proposed.id);await saveCatalogueRelease(store,refreshed);
  const reviewed=await reviewCatalogueRelease(refreshed,refreshed,'Checked');await saveCatalogueRelease(store,reviewed,1);
  await expect(saveCatalogueRelease(store,reviewed,1)).rejects.toMatchObject({status:409});
  const productRow=(await store.readLocal(source.keys[0]))[0],staged=await prepare('native-stale-002');
  await store.saveLocal(productRow.key,productRow.value,productRow.revision);
  await expect(saveCatalogueRelease(store,staged)).rejects.toMatchObject({status:409});expect(await store.readLocal(releasePrefix+staged.id)).toEqual([]);
  checks.push('atomic product and room revision checks; stale review refusal');
});
test('a response lost after commit can retry the same native request without duplicating a release',async()=>{
  let lose=true;
  const uncertain=openDestinationStore(binding.endpoint,{origin:f.base,request:async(url,options)=>{
    const response=await f.raw(url,f.owner,options);
    if(lose&&options.method==='POST'&&new URL(url).searchParams.get('operation')==='write'){lose=false;await response.arrayBuffer();throw Error('Response lost after commit');}
    return response;
  }});
  const value=await prepare('native-recovery-001');await expect(saveCatalogueRelease(uncertain,value)).rejects.toThrow('Response lost');
  const recovered=await saveCatalogueRelease(uncertain,value);expect(recovered.revision).toBe(1);expect(await store.readLocal(releasePrefix+value.id)).toHaveLength(1);
  checks.push('lost native response and idempotent recovery of the exact release save');
});
test('readers, revoked writers and unrelated accounts cannot save releases; another destination remains separate',async()=>{
  const value=await prepare('native-denied-001');
  await expect(saveCatalogueRelease(f.store(binding,f.reader),value)).rejects.toMatchObject({status:403});
  await expect(f.store(binding,f.other).readLocal(releasePrefix)).rejects.toMatchObject({status:403});
  const personal=await f.install('catalogue','self',f.other);expect(await f.store(personal,f.other).readLocal(releasePrefix)).toEqual([]);
  f.grant(f.writer.id,'workspace-team','read');await expect(saveCatalogueRelease(f.store(binding,f.writer),value)).rejects.toMatchObject({status:403});
  expect(await store.readLocal(releasePrefix+value.id)).toEqual([]);
  checks.push('current roles, revoked writer and destination isolation');
});
