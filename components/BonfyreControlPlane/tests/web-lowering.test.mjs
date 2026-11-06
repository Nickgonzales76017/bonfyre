import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { liveSurfaceEnabled, surface, surfaceHtml } from './live-surface.mjs';

const repo = resolve(import.meta.dir, '../../..');
const home = readFileSync(resolve(repo, 'site/matrix/surfaces/home.html'), 'utf8');
const kit = readFileSync(resolve(repo, 'site/matrix/surfaces/_kit.js'), 'utf8');
const main = readFileSync(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'), 'utf8');
const live = test.skipIf(!liveSurfaceEnabled);

live('same address answers human and machine', async () => {
  const path = '/_surface/external-estate?q=weather&extent=card';
  const human = await surfaceHtml(path);
  const machine = await surface(path);
  expect(human.status).toBe(200);
  expect(machine.status).toBe(200);
  expect(human.body).toContain('<dl class="surface-record"');
  expect(machine.body.records.length).toBeGreaterThan(0);
});

live('search narrows and compounds with slice', async () => {
  const wide = await surface('/_surface/external-estate?q=weather');
  const narrow = await surface('/_surface/external-estate?q=weather&estate=~Public');
  expect(narrow.body.count).toBeGreaterThan(0);
  expect(narrow.body.count).toBeLessThanOrEqual(wide.body.count);
});

live('every surface offers search', async () => {
  for (const name of ['worlds', 'internet-census', 'presentation-templates']) {
    expect((await surface(`/_surface/${name}?q=a`)).status).toBe(200);
  }
});

test('public search is a form and not a record renderer', () => {
  expect(home).toContain('hx-get="/_surface/external-estate"');
  const script = [...home.matchAll(/<script(?![^>]*\bsrc=)[^>]*>(.*?)<\/script>/gs)]
    .map(match => match[1]).join('\n');
  for (const handwritten of ["createElement('article')", 'DOMParser', 'prepareRoute']) {
    expect(script).not.toContain(handwritten);
  }
});

test('public page does not call the unserved legacy endpoint', () => {
  expect([...home.matchAll(/\/external-estate\/v1\/[a-z/]*/g)]).toEqual([]);
});

live('a record carries its own actionable affordance', async () => {
  const response = await surfaceHtml('/_surface/external-estate?q=weather&extent=card');
  expect(response.status).toBe(200);
  const acts = [...response.body.matchAll(/<a class="surface-act" href="([^"]+)"/g)]
    .map(match => match[1]);
  expect(acts.length).toBeGreaterThan(0);
  for (const href of acts) {
    expect(href.startsWith('/_surface/intake?url=')).toBe(true);
    expect(href).toContain('%3A%2F%2F');
  }
});

test('affordance template drops an unserved field', () => {
  expect(main).toContain('drops the whole affordance');
  expect(main).toContain('checked before');
});

test('kit asks estate whether a person is signed in', () => {
  expect(kit).toContain('K.signedIn');
  expect(kit).toMatch(/K\.session\s*=\s*(?:async\s*)?\(/);
  const start = kit.indexOf('K.note = (res, what)');
  const end = kit.indexOf('\n  };', start);
  expect(start).toBeGreaterThanOrEqual(0);
  expect(end).toBeGreaterThan(start);
  const note = kit.slice(start, end);
  expect(note).not.toContain('K.token()');
  expect(note).toContain('K.session()');
});

test('same-origin kit fetches carry cookies and foreign fetches do not', () => {
  const fetches = [...kit.matchAll(/await fetch\((.{0,220}?)\);/gs)].map(match => match[1]);
  expect(fetches.length).toBeGreaterThan(0);
  for (const call of fetches) {
    if (call.trimStart().startsWith("'/")) expect(call).toContain('credentials');
    else if (!call.includes('same-origin')) expect(call).not.toContain('credentials');
  }
});

test('browser storage token is not a session', () => {
  expect(kit).toContain('not a session');
  expect(kit).toContain('K.signedIn = async () => (await K.session()).valid');
});

live('limit narrows the actual answer', async () => {
  for (const limit of [3, 7]) {
    const { status, body } = await surface(`/_surface/external-estate?q=weather&limit=${limit}`);
    expect(status).toBe(200);
    expect(body.count).toBe(limit);
  }
});

live('limit cannot raise extent ceiling', async () => {
  const { status, body } = await surface('/_surface/external-estate?extent=chip&limit=500');
  expect(status).toBe(200);
  expect(body.count).toBeLessThanOrEqual(1);
});

test('public page does not expose internal density vocabulary', () => {
  expect(home).not.toContain('extent');
  expect(home.includes('limit=9') || home.includes('name="limit"')).toBe(true);
});


test('session owner coalesces only in-flight identity and rejects superseded readback',async()=>{
 const {runInNewContext}=await import('node:vm');
 const start=kit.indexOf('  K._session = null;'),end=kit.indexOf('  /* One fetch, three outcomes',start);expect(start).toBeGreaterThan(0);expect(end).toBeGreaterThan(start);
 const calls=[],deliveries=[],events=new Map();const context={AbortController,TextDecoder,setTimeout,clearTimeout,window:{addEventListener:(name,fn)=>events.set(name,fn)},document:{hidden:false,addEventListener:()=>{}},fetch:(path,options)=>{calls.push({path,options});return new Promise(resolve=>deliveries.push(resolve));}};
 runInNewContext('const K={};'+kit.slice(start,end)+';globalThis.sessionKit=K;',context);
 const current=context.sessionKit,a=current.session(),b=current.session();expect(a).toBe(b);expect(calls).toHaveLength(1);expect(calls[0].path).toBe('/account');expect(calls[0].options.credentials).toBe('same-origin');expect(calls[0].options.cache).toBe('no-store');expect(calls[0].options.redirect).toBe('error');
 const answer=id=>new Response(JSON.stringify({valid:true,user:{id,name:'Fixture account',email:'fixture@example.test',tier:'free'}}),{headers:{'content-type':'application/json'}});
 deliveries[0](answer(41));expect((await a).user.id).toBe(41);
 const changed=current.session();expect(calls).toHaveLength(2);events.get('pagehide')();deliveries[1](answer(99));expect((await changed).state).toBe('superseded');
 const latest=current.session();expect(calls).toHaveLength(3);deliveries[2](new Response('',{status:401}));expect((await latest).state).toBe('anonymous');
});
