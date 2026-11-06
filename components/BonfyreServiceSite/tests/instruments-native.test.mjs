// The five instruments installed as a native Aurekai workspace, against real
// Auth, CMS and WebAddress binaries in a throwaway estate. Work saved by one
// member reopens for another through the record store, a continuation link
// opens the exact record in the editor that made it, and a real browser saves
// and reopens through the page. What this does not prove: anything on the
// public site or the live estate; no live account is used.
import { test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { workspaceFixture } from '../../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import { projectInstrumentsWorkspaceSetup, publishSetups } from '../../../scripts/publish_experience_setups.mjs';
import { readWorkspaceFile } from '../../../scripts/materialize_workspace_package.mjs';
import { instrumentsPackage, INSTRUMENTS } from '../instruments-package.mjs';
import { withChrome } from '../../../integrations/browser/capture/chrome-session.mjs';

let temp, pack, f, binding;
const packName = 'experience.skill-instruments.workspace';
const previous = Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE', 'BONFYRE_WORKSPACE_PACKAGES'].map(k => [k, process.env[k]]));
beforeAll(async () => {
  temp = mkdtempSync(join(tmpdir(), 'instruments-native-')); const packageRoot = join(temp, 'packages');
  pack = await instrumentsPackage({ packageRoot });
  process.env.BONFYRE_WORKSPACE_RELEASE = pack.id; process.env.BONFYRE_WORKSPACE_PACKAGES = packageRoot;
  f = await workspaceFixture();
  const definition = { records: projectInstrumentsWorkspaceSetup(pack.id), binary: f.cms, db: f.db, schemas: join(f.home, 'schemas'), apply: true };
  expect(publishSetups(definition).created).toBe(10); expect(publishSetups(definition).created).toBe(0);
  binding = await f.install(packName, 'institution:workspace-team');
}, 120000);
afterAll(async () => {
  await f?.close(); if (temp) rmSync(temp, { recursive: true, force: true });
  for (const [key, value] of Object.entries(previous)) if (value === undefined) delete process.env[key]; else process.env[key] = value;
});
const base = () => binding.path.replace(/instruments\/$/, '');

test('the installed release serves every instrument bound to the destination record store', async () => {
  expect((await f.raw(binding.path)).status).toBe(200);
  for (const it of INSTRUMENTS) {
    const page = await f.raw(base() + it.page + '/'), html = await page.text();
    expect(page.status).toBe(200); expect(html).toContain(`content="${binding.endpoint}"`); expect(html).not.toContain('__WORKSPACE__');
    const client = html.match(/src="\.\/([a-z-]+-client\.mjs)"/)?.[1]; expect(client).toBeDefined();
    expect((await f.raw(base() + it.page + '/' + client)).status).toBe(200);
    expect((await f.raw(base() + it.page + '/instrument-store.mjs')).status).toBe(200);
  }
  expect((await f.raw(base() + 'bridge/workspace-store.mjs')).status).toBe(200);
  expect((await f.raw(base() + 'schema/vendor/sql.js-1.12.0/sql-wasm.wasm')).status).toBe(200);
  const manifest = (await readWorkspaceFile(process.env.BONFYRE_WORKSPACE_PACKAGES, pack.id, 'CONTINUATIONS.tsv')).toString();
  for (const it of INSTRUMENTS) expect(manifest).toContain(`${it.page}/\t${it.prefix}\tname\trecord=$key`);
});

test('a member saves, another reopens and revises, a stale save and a reader are refused', async () => {
  const key = 'plan:launch', value = { name: 'Launch', saved_at: '2026-10-19T12:00:00Z', state: { name: 'Launch', start: '2026-10-20', tasks: [{ id: 'a', name: 'A', optimistic: 1, likely: 2, pessimistic: 3, deps: [] }] } };
  expect((await f.store(binding).saveLocal(key, value, 0)).revision).toBe(1);
  const writer = f.store(binding, f.writer), read = (await writer.readLocal('plan:'))[0];
  expect(read.value.state.tasks[0].likely).toBe(2);
  read.value.state.tasks[0].likely = 5; read.value.state.tasks[0].pessimistic = 8; expect((await writer.saveLocal(key, read.value, read.revision)).revision).toBe(2);
  await expect(f.store(binding).saveLocal(key, value, 1)).rejects.toMatchObject({ status: 409 });
  await expect(f.store(binding, f.reader).saveLocal(key, value, 2)).rejects.toMatchObject({ status: 403 });
  expect((await f.store(binding, f.reader).readLocal('plan:'))[0].value.state.tasks[0].likely).toBe(5);
  await expect(f.store(binding).saveLocal('draft:catalogue:foreign', {}, 0)).rejects.toThrow();
  expect((await f.raw(binding.path, f.other)).status).toBe(403);
});

test('a database file saved by the schema workspace comes back byte for byte', async () => {
  const bytes = Uint8Array.from({ length: 300000 }, (_, i) => (i * 7) & 255);
  const db = new Blob([bytes], { type: 'application/vnd.sqlite3' });
  await f.store(binding).saveLocal('schema-plan:big', { name: 'Big', saved_at: 'x', state: { file: 'big.sqlite', changes: [], layout: {}, database: db } }, 0);
  const back = (await f.store(binding, f.writer).readLocal('schema-plan:'))[0].value.state.database;
  expect(new Uint8Array(await back.arrayBuffer())).toEqual(bytes);
  expect(f.sql.query('SELECT MAX(length(body)) AS n FROM _workspace_body').get().n).toBeLessThan(256 * 1024);
});

test('a continuation link opens the saved record in the editor that made it', async () => {
  const views = await f.raw(`/apps/${binding.id}/_views?` + new URLSearchParams({ key: 'plan:launch', revision: '2', release: pack.id }));
  expect(views.status).toBe(200);
  const target = (await views.text()).match(/data-compatible-view href="([^"]+)"/)?.[1].replaceAll('&amp;', '&'); expect(target).toBeDefined();
  const continued = await f.raw(target); expect(continued.status).toBe(303);
  const address = new URL(continued.headers.get('location'), f.base);
  expect(address.pathname).toBe(base() + 'plan/'); expect(address.searchParams.get('record')).toBe('plan:launch');
});

test('in a real browser the planner opens the continued record and saves a new version through the page', async () => {
  const origin = new URL(f.base), cookie = f.owner.cookie.split(';')[0], [cname, ...rest] = cookie.split('=');
  await withChrome(async ({ open }) => {
    const { page, go } = await open();
    await page('Network.setCookie', { name: cname, value: rest.join('='), domain: origin.hostname, path: '/', httpOnly: true });
    await go(new URL(base() + 'plan/?record=plan:launch', f.base).href);
    const ev = async expression => { const r = await page('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }); if (r.exceptionDetails) throw Error(r.exceptionDetails.exception?.description ?? r.exceptionDetails.text); return r.result.value; };
    const until = async (expr, test) => { for (let i = 0; i < 150; i++) { const v = await ev(expr); if (test(v)) return v; await Bun.sleep(100); } throw Error('never: ' + await ev(expr)); };
    expect(await until(`document.querySelector('.instrument-store .is-where')?.textContent ?? ''`, v => v !== '')).toBe('Saving to this Aurekai destination');
    expect(await until(`document.querySelector('[data-is="said"]').textContent`, v => v.startsWith('Opened'))).toContain('version 2');
    expect(await ev(`document.querySelectorAll('.dp-bar[data-bar]').length`)).toBe(1);
    expect(await ev(`document.querySelector('[data-ui="problems"]').textContent`)).toBe('');
    await ev(`document.querySelector('[data-is="save"]').click()`);
    expect(await until(`document.querySelector('[data-is="said"]').textContent`, v => v.startsWith('Saved'))).toContain('version 3');
  }, { timeoutMs: 60000 });
  const reopened = (await f.store(binding, f.writer).readLocal('plan:launch'))[0];
  expect(reopened.revision).toBe(3); expect(reopened.value.state.tasks[0].likely).toBe(5);
}, 90000);

// Small callbacks isolate the shared lifecycle; requests use real native owners.
async function lifecyclePage({user=f.owner,publicMode=false,width=1440}={}){
  const {createRequire}=await import('node:module');
  const repo=join(import.meta.dir,'../../..');
  const puppeteer=createRequire(join(repo,'tools/creator-ingest/package.json'))('puppeteer');
  const browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-background-networking']});
  const page=await browser.newPage();page.setDefaultTimeout(12000);await page.setViewport({width,height:1050});
  const [name,...rest]=user.cookie.split('=');await page.setCookie({name,value:rest.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  await page.goto(f.base+'/healthz');
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  async function mount({state={tasks:[{name:'Unfamiliar intake',likely:2}],marker:'original'},prefix='plan:',scratch=false}={}){
    return page.evaluate(async({endpoint,module,publicMode,state,prefix,scratch})=>{
      document.body.replaceChildren();document.querySelector('meta[name=bonfyre-workspace-store]')?.remove();
      if(!publicMode){const meta=document.createElement('meta');meta.name='bonfyre-workspace-store';meta.content=endpoint;document.head.append(meta);}
      const section=document.createElement('section');section.id='lifecycle-test';document.body.append(section);
      window.instrumentValue=state;const input=document.createElement('input');input.id='instrument-marker';input.value=state.marker??'';
      input.oninput=()=>window.instrumentValue.marker=input.value;section.append(input);
      const m=await import(module);window.instrumentAPI=await m.attachInstrumentStore({prefix,label:'Test plan',mount:section,get:()=>window.instrumentValue,set:value=>{window.instrumentValue=value;input.value=value.marker??'';}});
      if(scratch)window.scratch=await m.openInstrumentScratch('aurekai-lifecycle-test',null);
      return {native:window.instrumentAPI.native};
    },{endpoint:binding.endpoint,module:base()+'plan/instrument-store.mjs',publicMode,state,prefix,scratch});
  }
  async function close(){await browser.close();}
  return {page,mount,errors,close};
}
const lifecycle='#lifecycle-test';
const field=key=>`${lifecycle} [data-is=${key}]`;
async function title(page,text){await page.$eval(field('name'),(node,text)=>{node.value=text;node.dispatchEvent(new Event('input',{bubbles:true}));},text);}
async function clickSave(page){await page.click(field('save'));await page.waitForFunction(()=>document.querySelector('#lifecycle-test').querySelector('[data-is=said]').textContent.startsWith('Saved'));}
async function currentKey(page){await page.waitForFunction(()=>!document.querySelector('#lifecycle-test [data-is=save]').disabled);return page.evaluate(()=>new URL(location.href).searchParams.get('record'));}

test('all six installed editors ignore origin-wide legacy private scratch and render their native save controls',async()=>{
  const p=await lifecyclePage();try{
    await p.page.evaluateOnNewDocument(()=>{
      window.legacyTouches=[];const get=Storage.prototype.getItem,set=Storage.prototype.setItem;
      const keys=['aurekai-process-sim-v1','aurekai-research-desk-v1','aurekai-schema-lab-v1','aurekai-dependency-planner-v1','aurekai-pricing-desk-v1','aurekai-opportunity-desk-v1'];
      for(const key of keys)set.call(localStorage,key,'{"private":"previous-account-DO-NOT-READ"}');
      Storage.prototype.getItem=function(key){if(keys.includes(key)||String(key).startsWith('aurekai-instrument:'))window.legacyTouches.push(['read',key]);return get.call(this,key);};
      Storage.prototype.setItem=function(key,value){if(keys.includes(key)||String(key).startsWith('aurekai-instrument:'))window.legacyTouches.push(['write',key]);return set.call(this,key,value);};
    });
    for(const it of INSTRUMENTS){
      await p.page.goto(f.base+base()+it.page+'/',{waitUntil:'networkidle0'});
      await p.page.waitForSelector('.instrument-store [data-is=save]');
      expect(await p.page.$eval('.instrument-store .is-where',n=>n.textContent)).toBe('Saving to this Aurekai destination');
      expect(await p.page.evaluate(()=>window.legacyTouches)).toEqual([]);
      expect(await p.page.$eval('body',n=>n.textContent.includes('previous-account-DO-NOT-READ'))).toBe(false);
      expect(await p.page.$eval('.instrument-store [data-is=copy]',n=>n.disabled)).toBe(false);
    }
    expect(p.errors).toEqual([]);
  }finally{await p.close();}
},90000);

test('same names receive distinct identities; renaming and double clicking never duplicate a loaded record',async()=>{
  const p=await lifecyclePage();try{await p.mount();await title(p.page,'Same name');await clickSave(p.page);const first=await currentKey(p.page);
    await p.page.click(field('copy'));await p.page.waitForFunction(key=>new URL(location.href).searchParams.get('record')!==key,{},first);const second=await currentKey(p.page);
    expect(first).not.toBe(second);expect((await f.store(binding).readVersion(first)).revision).toBe(1);
    await title(p.page,'Renamed');await p.page.evaluate(()=>{const b=document.querySelector('#lifecycle-test [data-is=save]');b.click();b.click();});
    await p.page.waitForFunction(()=>document.querySelector('#lifecycle-test [data-is=said]').textContent.includes('version 2'));
    expect(await currentKey(p.page)).toBe(second);expect((await f.store(binding).readVersion(second)).value.name).toBe('Renamed');expect((await f.store(binding).readVersion(second)).revision).toBe(2);
  }finally{await p.close();}
},30000);

test('metadata refresh cannot advance the revision of an unrefreshed editor; conflict preserves both versions',async()=>{
  const p=await lifecyclePage();try{await p.mount();await title(p.page,'Conflict case');await clickSave(p.page);const key=await currentKey(p.page),old=await f.store(binding).readVersion(key);
    await f.store(binding,f.writer).saveLocal(key,{...old.value,state:{...old.value.state,marker:'other writer'}},old.revision);
    await p.page.evaluate(()=>window.instrumentValue.marker='my unsaved edit');await p.page.click(field('refresh'));
    await p.page.evaluate(()=>window.instrumentAPI.list());await p.page.click(field('save'));
    await p.page.waitForFunction(()=>document.querySelector('#lifecycle-test .instrument-store').dataset.saveState==='conflict');
    expect(await p.page.evaluate(()=>window.instrumentValue.marker)).toBe('my unsaved edit');expect((await f.store(binding).readVersion(key)).value.state.marker).toBe('other writer');
    await p.page.click(field('copy'));await p.page.waitForFunction(key=>new URL(location.href).searchParams.get('record')!==key,{},key);
    expect((await f.store(binding).readVersion(await currentKey(p.page))).value.state.marker).toBe('my unsaved edit');expect((await f.store(binding).readVersion(key)).revision).toBe(2);
  }finally{await p.close();}
},30000);

test('lost acknowledgement is reconciled by exact saved contents without repeating the write',async()=>{
  const p=await lifecyclePage();let cdp,writes=0;try{await p.mount();await title(p.page,'Lost response');
    cdp=await p.page.createCDPSession();await cdp.send('Fetch.enable',{patterns:[{urlPattern:'*'+binding.endpoint+'?operation=write*',requestStage:'Response'}]});
    cdp.on('Fetch.requestPaused',async e=>{try{if(e.request.method==='POST'&&e.responseStatusCode===200){writes++;await cdp.send('Fetch.failRequest',{requestId:e.requestId,errorReason:'ConnectionClosed'});}else await cdp.send('Fetch.continueResponse',{requestId:e.requestId});}catch{}});
    await p.page.click(field('save'));await p.page.waitForFunction(()=>document.querySelector('#lifecycle-test .instrument-store').dataset.saveState==='uncertain');
    expect(writes).toBe(1);expect(await p.page.$eval(field('retry'),n=>n.hidden)).toBe(true);
    await p.page.click(field('check'));await p.page.waitForFunction(()=>document.querySelector('#lifecycle-test .instrument-store').dataset.saveState==='saved');
    expect(writes).toBe(1);expect((await f.store(binding).readVersion(await currentKey(p.page))).revision).toBe(1);
  }finally{await cdp?.detach();await p.close();}
},30000);

test('failed-before-commit requires readback and an explicit retry of the reviewed snapshot',async()=>{
  const p=await lifecyclePage();let blocked=true,writes=0;try{await p.mount();await title(p.page,'Exact retry');await p.page.setRequestInterception(true);
    p.page.on('request',r=>{if(r.method()==='POST'&&r.url().includes('operation=write')){writes++;if(blocked){blocked=false;void r.abort();return;}}void r.continue();});
    await p.page.click(field('save'));await p.page.waitForFunction(()=>document.querySelector('#lifecycle-test .instrument-store').dataset.saveState==='uncertain');
    await p.page.evaluate(()=>window.instrumentValue.marker='newer unsaved edit');await p.page.click(field('check'));await p.page.waitForFunction(()=>!document.querySelector('#lifecycle-test [data-is=retry]').hidden);
    expect(writes).toBe(1);await p.page.click(field('retry'));await p.page.waitForFunction(()=>document.querySelector('#lifecycle-test .instrument-store').dataset.saveState==='saved');
    expect(writes).toBe(2);expect((await f.store(binding).readVersion(await currentKey(p.page))).value.state.marker).toBe('original');expect(await p.page.evaluate(()=>window.instrumentValue.marker)).toBe('newer unsaved edit');
    expect(await p.page.$eval(field('said'),n=>n.textContent)).toContain('Newer edits are still unsaved');
  }finally{await p.close();}
},30000);

test('opening work reviews discarded edits and updates the exact continuation address',async()=>{
  const p=await lifecyclePage();try{await p.mount();await title(p.page,'First');await clickSave(p.page);const first=await currentKey(p.page);
    await p.page.evaluate(()=>window.instrumentValue.marker='second');await title(p.page,'Second');await p.page.click(field('copy'));await p.page.waitForFunction(key=>new URL(location.href).searchParams.get('record')!==key,{},first);const second=await currentKey(p.page);
    await p.page.evaluate(()=>window.instrumentValue.marker='unsaved');await p.page.select(field('list'),first);await p.page.waitForSelector('.instrument-discard[open]');await p.page.click('.instrument-discard [data-is=keep]');
    expect(await currentKey(p.page)).toBe(second);expect(await p.page.evaluate(()=>window.instrumentValue.marker)).toBe('unsaved');
    await p.page.select(field('list'),first);await p.page.waitForSelector('.instrument-discard[open]');await p.page.click('.instrument-discard [data-is=discard]');
    await p.page.waitForFunction(key=>new URL(location.href).searchParams.get('record')===key,{},first);expect(await p.page.evaluate(()=>window.instrumentValue.marker)).toBe('original');
  }finally{await p.close();}
},30000);

test('saved-work listing never downloads every attached database; only the selected record is hydrated',async()=>{
  const p=await lifecyclePage();try{let downloads=0;p.page.on('request',r=>{if(r.url().includes('operation=blob-get'))downloads++;});
    await p.mount({prefix:'schema-plan:'});expect(downloads).toBe(0);
    await p.page.evaluate(()=>window.instrumentAPI.open('schema-plan:big'));
    expect(downloads).toBeGreaterThan(0);expect(await p.page.evaluate(()=>window.instrumentValue.database instanceof Blob)).toBe(true);
  }finally{await p.close();}
},30000);

test('public named legacy drafts migrate once to the existing structured store and retain binary files',async()=>{
  const p=await lifecyclePage({publicMode:true,width:390});try{
    const legacy=JSON.stringify({'plan:older-local':{name:'Older local',saved_at:'2026-10-18',state:{marker:'retained legacy'}}});
    await p.page.evaluate(legacy=>localStorage.setItem('aurekai-instrument:plan:',legacy),legacy);await p.mount();
    await p.page.evaluate(()=>window.instrumentAPI.open('plan:older-local'));expect(await p.page.evaluate(()=>window.instrumentValue.marker)).toBe('retained legacy');
    await p.page.evaluate(()=>window.instrumentValue.database=new Blob([new Uint8Array([12,24,36])],{type:'application/vnd.sqlite3'}));await clickSave(p.page);
    expect(await p.page.evaluate(()=>localStorage.getItem('aurekai-instrument:plan:'))).toBe(legacy);
    await p.page.evaluate(()=>window.instrumentValue={marker:'temporary'});await p.page.evaluate(()=>{void window.instrumentAPI.open('plan:older-local');});await p.page.waitForSelector('.instrument-discard[open]');await p.page.click('.instrument-discard [data-is=discard]');
    await p.page.waitForFunction(()=>window.instrumentValue.database instanceof Blob);expect(await p.page.evaluate(async()=>Array.from(new Uint8Array(await window.instrumentValue.database.arrayBuffer())))).toEqual([12,24,36]);
    expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  }finally{await p.close();}
},30000);

// Actual installed sales/product page, exact source imports and prepared delivery.
async function opportunityPage() {
  const p = await lifecyclePage();
  await p.page.goto(f.base + base() + "bids/", { waitUntil: "networkidle0" });
  await p.page.waitForSelector("[data-select-req]");
  await p.page.waitForSelector(".instrument-store [data-is=save]");
  return p;
}
async function saveOpportunity(page, name) {
  await page.$eval(".instrument-store [data-is=name]", (n, name) => {
    n.value = name;
    n.dispatchEvent(new Event("input", { bubbles: !0 }));
  }, name);
  await page.click(".instrument-store [data-is=save]");
  await page.waitForFunction(() => document.querySelector(".instrument-store")?.dataset.saveState === "saved");
  return page.evaluate(() => new URL(location.href).searchParams.get("record"));
}
async function selectFirstRequirements(page, count = 2) {
  for (let n = 0;n < count; n++)
    await page.$$eval("[data-select-req]", (nodes, index) => nodes[index].click(), n);
}
async function reviewDelivery(page) {
  await page.click("[data-prepare-plan]");
  await page.waitForSelector("dialog[data-delivery-plan][open]");
  await page.evaluate(() => {
    const dialog = document.querySelector("dialog[data-delivery-plan]");
    dialog.querySelector("[data-plan-start]").value = "2026-11-02";
    for (const [i, row] of [...dialog.querySelectorAll("[data-plan-task]")].entries()) {
      for (const n of row.querySelectorAll("[data-duration]"))
        n.value = String({ optimistic: 1, likely: 2, pessimistic: 3 }[n.dataset.duration] + i);
      row.querySelector("[data-plan-owner]").value = "Reviewer";
      if (i)
        row.querySelector("[data-plan-deps] option").selected = !0;
    }
    dialog.querySelector("[data-plan-start]").dispatchEvent(new Event("input", { bubbles: !0 }));
  });
  await page.click("[data-plan-review]");
  await page.waitForSelector("dialog[data-delivery-plan][data-state=reviewed]");
}
test("opportunity starts with unknown scores; exact same-name revisions survive reviewed bulk changes and reopen",async () => {
  const p = await opportunityPage();
  try {
    await p.page.click("[data-tab=compare]");
    expect(await p.page.$$eval("[data-review-score]", (nodes) => nodes.every((n) => n.value === ""))).toBe(!0);
    expect(await p.page.$eval("[data-ui=view]", (n) => n.textContent)).toContain("before choosing a leader");
    await p.page.focus("[data-tab=compare]");
    await p.page.keyboard.press("End");
    expect(await p.page.$eval("[data-tab=discovery]", (n) => n.getAttribute("aria-selected"))).toBe("true");
    expect(await p.page.$$eval("[data-op]", (nodes) => nodes.every((n) => n.value === ""))).toBe(!0);
    await p.page.click("[data-tab=grid]");
    const first = "The supplier must preserve the original workshop records for independent review.", second = "The supplier must preserve the revised workshop records for independent review.";
    for (const source of [first, second]) {
      await p.page.evaluate((source) => {
        const transfer = new DataTransfer;
        transfer.items.add(new File([source], "same-request.txt", { type: "text/plain" }));
        const input = document.querySelector("[data-ui=add-doc]");
        input.files = transfer.files;
        input.dispatchEvent(new Event("change", { bubbles: !0 }));
      }, source);
      await p.page.waitForFunction((source) => [...document.querySelectorAll("[data-focus]")].some((n) => n.textContent === source), {}, source);
    }
    const sourceIds = await p.page.$$eval("[data-filter=source] option", (nodes) => nodes.filter((n) => n.textContent === "same-request.txt").map((n) => n.value));
    expect(new Set(sourceIds).size).toBe(2);
    await selectFirstRequirements(p.page);
    await p.page.click("[data-review-selected]");
    expect(await p.page.$$eval("[data-set=coverage]", (nodes) => nodes.every((n) => n.value === ""))).toBe(!0);
    await p.page.click("[data-apply-reviewed]");
    expect(await p.page.$$eval("[data-set=coverage]", (nodes) => nodes.filter((n) => n.value === "covered").length)).toBe(2);
    await p.page.click("[data-undo-bulk]");
    expect(await p.page.$$eval("[data-set=coverage]", (nodes) => nodes.every((n) => n.value === ""))).toBe(!0);
    const key = await saveOpportunity(p.page, "Exact imported requirements"), saved = await f.store(binding).readVersion(key);
    expect(Object.values(saved.value.state.own).map((o) => o.text).sort()).toEqual([first, second].sort());
    await p.page.reload({ waitUntil: "networkidle0" });
    await p.page.waitForFunction(() => document.querySelector("[data-is=said]")?.textContent.includes("Opened"));
    expect(await p.page.$$eval("[data-filter=source] option", (nodes) => nodes.filter((n) => n.textContent === "same-request.txt").length)).toBe(2);
    await p.page.focus(".od-separator");
    const width = await p.page.$eval(".od-separator", (n) => Number(n.getAttribute("aria-valuenow")));
    await p.page.keyboard.press("ArrowLeft");
    expect(await p.page.$eval(".od-separator", (n) => Number(n.getAttribute("aria-valuenow")))).toBeGreaterThan(width);
    expect(p.errors).toEqual([]);
  } finally {
    await p.close();
  }
},60000);
test("reviewed opportunity prepares one native plan with exact quotes and opens the existing planner on phone and desktop",async () => {
  const p = await opportunityPage();
  try {
    const key = await saveOpportunity(p.page, "Buyer delivery"), original = await f.store(binding).readVersion(key);
    await selectFirstRequirements(p.page);
    await reviewDelivery(p.page);
    expect(await p.page.$eval("[data-plan-summary]", (n) => n.textContent)).toContain("5.00 working days");
    for (const width of [1440, 390]) {
      await p.page.setViewport({ width, height: 1000 });
      expect(await p.page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(!0);
      expect(await p.page.$eval("dialog[data-delivery-plan]", (n) => n.scrollWidth <= n.clientWidth + 1)).toBe(!0);
    }
    await p.page.click("[data-plan-commit]");
    await p.page.waitForSelector("dialog[data-state=saved] [data-open-delivery-plan]");
    const href = await p.page.$eval("[data-open-delivery-plan]", (n) => n.href), planKey = new URL(href).searchParams.get("record"), saved = await f.store(binding).readVersion(planKey);
    expect(saved.revision).toBe(1);
    expect(saved.value.state.tasks).toHaveLength(2);
    expect(saved.value.state.tasks[0].requirement.quote).toBe(original.value.state.reqs[0].text);
    expect(saved.value.state.tasks[1].deps).toEqual(["requirement-1"]);
    expect((await f.store(binding).readVersion(key)).revision).toBe(original.revision);
    await p.page.goto(href, { waitUntil: "networkidle0" });
    await p.page.waitForFunction(() => document.querySelector("[data-is=said]")?.textContent.includes("Opened"));
    expect(await p.page.$$(".dp-bar[data-bar]")).toHaveLength(2);
    expect(await p.page.$eval("[data-ui=problems]", (n) => n.textContent)).toBe("");
    expect(p.errors).toEqual([]);
  } finally {
    await p.close();
  }
},60000);
test("a lost native plan acknowledgement is read back without a second write",async () => {
  const p = await opportunityPage();
  let cdp, writes = 0;
  try {
    const key = await saveOpportunity(p.page, "Lost plan reply");
    await selectFirstRequirements(p.page, 1);
    await reviewDelivery(p.page);
    cdp = await p.page.createCDPSession();
    await cdp.send("Fetch.enable", { patterns: [{ urlPattern: "*" + binding.endpoint + "?operation=write*", requestStage: "Response" }] });
    cdp.on("Fetch.requestPaused", async (e) => {
      try {
        if (e.request.method === "POST" && e.responseStatusCode === 200) {
          writes++;
          await cdp.send("Fetch.failRequest", { requestId: e.requestId, errorReason: "ConnectionClosed" });
        } else
          await cdp.send("Fetch.continueResponse", { requestId: e.requestId });
      } catch {}
    });
    await p.page.click("[data-plan-commit]");
    await p.page.waitForSelector("dialog[data-state=uncertain]");
    expect(writes).toBe(1);
    await p.page.click("[data-plan-check]");
    await p.page.waitForSelector("dialog[data-state=saved]");
    expect(writes).toBe(1);
    expect((await f.store(binding).readVersion(key)).revision).toBe(1);
    expect(p.errors).toEqual([]);
  } finally {
    await cdp?.detach();
    await p.close();
  }
},60000);
test("a changed source revision refuses the whole plan handoff and preserves the other writer",async () => {
  const p = await opportunityPage();
  try {
    const key = await saveOpportunity(p.page, "Concurrent buyer review");
    await selectFirstRequirements(p.page, 1);
    await reviewDelivery(p.page);
    const old = await f.store(binding).readVersion(key), beforePlans = await f.store(binding).readLocal("plan:");
    await f.store(binding, f.writer).saveLocal(key, { ...old.value, name: "Other writer kept" }, old.revision);
    await p.page.click("[data-plan-commit]");
    await p.page.waitForSelector("dialog[data-state=uncertain]");
    expect(await f.store(binding).readLocal("plan:")).toEqual(beforePlans);
    expect((await f.store(binding).readVersion(key)).value.name).toBe("Other writer kept");
    await p.page.click("[data-plan-check]");
    await p.page.waitForFunction(() => document.querySelector("[data-plan-status]").textContent.includes("source review changed"));
    expect(await p.page.$("[data-open-delivery-plan]")).toBeNull();
    expect(await p.page.$eval("[data-plan-retry]", (n) => n.hidden)).toBe(!0);
    expect(p.errors).toEqual([]);
  } finally {
    await p.close();
  }
},60000);

test('unstructured brief selections become saved source-linked requirements and a native delivery plan',async()=>{
 const p=await opportunityPage();
 const text='Coffee ☕ and repairs.\r\nTurnaround: 48 hours.\r\nReuse the catalogue.';
 const quotes=['Turnaround: 48 hours.','Reuse the catalogue.'];
 try {
  const count=await p.page.$$eval('[data-focus]',nodes=>nodes.length);
  await p.page.evaluate(text=>{
    const transfer=new DataTransfer();transfer.items.add(new File([text],'short-brief.txt',{type:'text/plain'}));
    const input=document.querySelector('[data-ui=add-doc]');input.files=transfer.files;input.dispatchEvent(new Event('change',{bubbles:true}));
  },text);
  await p.page.waitForFunction(()=>document.querySelector('[data-ui=message]').textContent.includes('Select passages'));
  expect(await p.page.$$eval('[data-focus]',nodes=>nodes.length)).toBe(count);
  await p.page.click('[data-ui=read-sources]');
  const source=await p.page.$eval('[data-source-document]',n=>[...n.options].find(o=>o.textContent==='short-brief.txt').value);
  await p.page.select('[data-source-document]',source);
  expect(await p.page.$eval('[data-source-reader]',n=>n.textContent)).toBe(text);
  for(const quote of quotes) {
    await p.page.$eval('[data-source-find]',(n,q)=>{n.value=q;n.dispatchEvent(new Event('input',{bubbles:true}));},quote);
    await p.page.click('[data-source-find-next]');await p.page.click('[data-source-stage]');
  }
  expect(await p.page.$eval('[data-source-accept]',n=>n.disabled)).toBe(true);
  expect(await p.page.$$eval('[data-focus]',nodes=>nodes.length)).toBe(count);
  await p.page.select('[data-source-bulk-priority]','must');
  await p.page.select('[data-passage-priority="1"]','should');
  for(const width of [1440,390]) {
    await p.page.setViewport({width,height:1000});
    expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
    expect(await p.page.$eval('dialog[data-source-selection]',n=>n.scrollWidth<=n.clientWidth+1)).toBe(true);
    if(process.env.BONFYRE_SOURCE_SELECTION_PROOF){
      const {mkdirSync}=await import('node:fs');mkdirSync(process.env.BONFYRE_SOURCE_SELECTION_PROOF,{recursive:true});
      await p.page.screenshot({path:join(process.env.BONFYRE_SOURCE_SELECTION_PROOF,'source-selection-'+width+'.png'),fullPage:true});
    }
  }
  await p.page.setViewport({width:1440,height:1000});
  await p.page.click('[data-source-accept]');
  expect(await p.page.$('dialog[data-source-selection]')).toBeNull();
  const key=await saveOpportunity(p.page,'Short workshop brief'),saved=await f.store(binding).readVersion(key);
  const selected=saved.value.state.reqs.filter(r=>r.source===source);
  expect(selected.map(r=>r.text)).toEqual(quotes);
  expect(selected.map(r=>r.priority)).toEqual(['must','should']);
  expect(selected.every(r=>r.coverage==='')).toBe(true);
  expect(saved.value.state.own[source].text).toBe(text);
  for(const r of selected)expect(text.slice(r.start,r.end)).toBe(r.text);
  await p.page.reload({waitUntil:'networkidle0'});
  await p.page.waitForFunction(()=>document.querySelector('[data-is=said]')?.textContent.includes('Opened'));
  for(const quote of quotes)await p.page.$$eval('[data-select-req]',(nodes,quote)=>{
    const row=nodes.find(n=>n.closest('tr').querySelector('[data-focus]').textContent===quote);row.click();
  },quote);
  await reviewDelivery(p.page);await p.page.click('[data-plan-commit]');
  await p.page.waitForSelector('dialog[data-state=saved] [data-open-delivery-plan]');
  const href=await p.page.$eval('[data-open-delivery-plan]',n=>n.href),planKey=new URL(href).searchParams.get('record');
  const plan=await f.store(binding).readVersion(planKey);
  expect(plan.value.state.tasks.map(t=>t.requirement.quote)).toEqual(quotes);
  expect((await f.store(binding).readVersion(key)).revision).toBe(saved.revision);
  await p.page.goto(href,{waitUntil:'networkidle0'});
  await p.page.waitForFunction(()=>document.querySelector('[data-is=said]')?.textContent.includes('Opened'));
  expect(await p.page.$$('.dp-bar[data-bar]')).toHaveLength(2);
  expect(p.errors).toEqual([]);
 }finally{await p.close();}
},60000);

test('transient gestures do not warn about unsaved record edits; actual edits and reversals are compared',async()=>{
 const p=await lifecyclePage();try{
  await p.mount();await title(p.page,'Transient controls');await clickSave(p.page);
  const key=await currentKey(p.page);
  await p.page.evaluate(()=>{const b=document.createElement('button');b.id='transient-selection';b.textContent='Select a row';document.querySelector('#lifecycle-test').append(b);});
  await p.page.click('#transient-selection');
  await p.page.waitForFunction(()=>{const e=new Event('beforeunload',{cancelable:true});window.dispatchEvent(e);return !e.defaultPrevented;});
  expect((await p.page.evaluate(()=>window.instrumentAPI.checkpoint())).key).toBe(key);
  await p.page.$eval('#instrument-marker',n=>{n.value='changed';n.dispatchEvent(new Event('input',{bubbles:true}));});
  expect(await p.page.evaluate(()=>{const e=new Event('beforeunload',{cancelable:true});window.dispatchEvent(e);return e.defaultPrevented;})).toBe(true);
  expect((await f.store(binding).readVersion(key)).value.state.marker).toBe('original');
  await p.page.$eval('#instrument-marker',n=>{n.value='original';n.dispatchEvent(new Event('input',{bubbles:true}));});
  await p.page.waitForFunction(()=>{const e=new Event('beforeunload',{cancelable:true});window.dispatchEvent(e);return !e.defaultPrevented;});
  expect((await f.store(binding).readVersion(key)).revision).toBe(1);
 }finally{await p.close();}
},30000);
