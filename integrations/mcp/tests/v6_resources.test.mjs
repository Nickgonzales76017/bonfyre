import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, copyFileSync, chmodSync, symlinkSync, existsSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { createHash } from 'node:crypto';
import { createV6Resources, V6_PROVIDERS, V6_BACKENDS } from '../v6_resources.mjs';
import { digestValue, canonicalJson, parsedDocument } from '../canonical_json.mjs';
import { createV6Store } from '../v6_store.mjs';

const homes=[];
afterEach(()=>{for(const home of homes.splice(0))rmSync(home,{recursive:true,force:true});});
const digest=value=>digestValue(value,{ascii:false});
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
function fixture(extra={}) {
  const dir=realpathSync(mkdtempSync(join(tmpdir(),'bonfyre-v6-resources-')));homes.push(dir);
  const root=join(dir,'project'),home=join(dir,'v6'),userHome=join(dir,'user'),bin=join(dir,'bin');
  for(const path of [root,userHome,bin])mkdirSync(path,{recursive:true});
  const resources=[],backends=[],receipts=[],meta=new Map();
  const store={home,digest,parseDocument:(source,omitKey)=>parsedDocument(source,{ascii:false,omitKey}),
    upsertResource:doc=>resources.push(doc),upsertBackend:doc=>backends.push(doc),setMeta:(key,value)=>meta.set(key,value),
    receipt:(kind,state,payload)=>receipts.push({kind,state,payload}),writeJson:(path,doc)=>{mkdirSync(dirname(path),{recursive:true});writeFileSync(path,canonicalJson(doc,{ascii:false}));}};
  const calls=[],requests=[];
  const env={PATH:bin,BONFYRE_V61_SUBSTRATE_ROOT:join(dir,'substrates'),...extra.env};
  const owner=createV6Resources({store,root,home,userHome,env,now:()=>1700000000,
    run:async(argv,options)=>{calls.push({argv,options});return extra.run?extra.run(argv,options):{exit_code:0,stdout:'ok',stderr:'',duration_ms:1};},
    request:async(url,options)=>{requests.push({url:String(url),options});return extra.request?extra.request(url,options):Response.json({ok:true});}});
  return {dir,root,home,userHome,bin,store,resources,backends,receipts,meta,calls,requests,env,owner};
}
function native(f,name){const target=join(f.bin,name);copyFileSync('/bin/echo',target);chmodSync(target,0o700);return target;}
function manifest(f,id='feldera',change={}) {
  const base=join(f.env.BONFYRE_V61_SUBSTRATE_ROOT,id);mkdirSync(base,{recursive:true});
  const bytes=Buffer.from('semantic fixture bytes');writeFileSync(join(base,'proof.json'),bytes);
  const body={schema:'bonfyre.substrate-activation.v1',id,state:'active',evidence_class:'live-upstream',
    upstream:{url:'https://example.test/source',identity:'commit-fixture'},semantic_probe:{state:'passed',kind:'fixture',output_digest:hash(bytes)},
    commands:[{exit_code:0}],execution_mode:'upstream-library-adapter',artifacts:[{path:'proof.json',bytes:bytes.length,sha256:hash(bytes)}],completed_at:1699999999,...change};
  const path=join(base,'activation.json');writeFileSync(path,canonicalJson({...body,manifest_digest:digest(body)},{ascii:false}));return {path,base,body};
}
test('constructor is lazy and every historical provider/backend descriptor survives',()=>{
  const f=fixture();expect(existsSync(f.home)).toBe(false);expect(f.calls).toHaveLength(0);expect(f.requests).toHaveLength(0);
  expect(Object.keys(V6_PROVIDERS)).toHaveLength(39);expect(Object.keys(V6_BACKENDS)).toHaveLength(14);
  expect(V6_BACKENDS.hydro.stage).toBe('DistributedIR→placement');expect(V6_PROVIDERS.oracle[2]).toContain('deployment.oracle');
});
test('discovery preserves precedence, names and projections without secrets or effects when probes disabled',async()=>{
  const f=fixture({env:{OPENAI_API_KEY:'process-secret',BONFYRE_PRIVATE_TOKEN:'private-secret'}});
  writeFileSync(join(f.root,'.env'),'OPENAI_API_KEY=file-secret\nexport GEMINI_API_KEY="google-secret"\nUNMAPPED_SECRET=unmapped-secret\nBAD_SECRET=$(do-not-run)\n');
  mkdirSync(join(f.root,'node_modules'),{recursive:true});writeFileSync(join(f.root,'node_modules','.env'),'IGNORED_SECRET=never\n');
  mkdirSync(join(f.root,'Aurekai/generated/integration-repos/adapter'),{recursive:true});
  const result=await f.owner.discoverResources({probe_external:false});
  expect(result.summary.providers).toBe(39);expect(result.summary.configured).toBe(2);expect(result.summary.integrations).toBe(1);
  const google=result.resources.find(r=>r.provider==='google-ai');expect(google.evidence.env_names).toEqual(['GEMINI_API_KEY']);
  expect(result.resources.some(r=>r.id==='env-unmapped:UNMAPPED_SECRET')).toBe(true);expect(result.resources.some(r=>r.id.includes('IGNORED_SECRET'))).toBe(false);
  expect(JSON.stringify(result)).not.toContain('process-secret');expect(JSON.stringify(result)).not.toContain('file-secret');expect(JSON.stringify(result)).not.toContain('google-secret');
  expect(f.calls).toHaveLength(0);expect(f.requests).toHaveLength(0);expect(f.receipts[0].kind).toBe('v6.1.resource-discovery');
});
test('CLI probe output is hashed not retained; local runpod configuration does not prove remote reachability',async()=>{
  const f=fixture({run:async()=>({exit_code:0,stdout:'token=do-not-retain',stderr:'secret=also-private',duration_ms:3})});
  native(f,'gh');native(f,'runpodctl');
  const result=await f.owner.discoverResources();const github=result.resources.find(r=>r.provider==='github'),runpod=result.resources.find(r=>r.provider==='runpod');
  expect(github.state).toBe('reachable');expect(github.evidence.probe.stdout).toBe('');expect(github.evidence.probe.stdout_sha256).toBe(hash('token=do-not-retain'));
  expect(runpod.state).toBe('configured');expect(runpod.evidence.probe.semantic_scope).toBe('local-configuration-only');
  expect(JSON.stringify(result)).not.toContain('do-not-retain');expect(JSON.stringify(result)).not.toContain('also-private');
  expect(f.calls.every(c=>c.options.maxOutputBytes===2097152&&c.options.timeoutMs===15000)).toBe(true);
});
test('Python and unknown shell trampolines are refused, direct Node tooling stays executable',async()=>{
  const f=fixture();writeFileSync(join(f.bin,'gh'),'#!/usr/bin/env python3\n');chmodSync(join(f.bin,'gh'),0o700);
  writeFileSync(join(f.bin,'aws'),'#!/bin/sh\nexec python3 impl.py\n');chmodSync(join(f.bin,'aws'),0o700);
  native(f,'node');writeFileSync(join(f.bin,'wrangler'),'#!/usr/bin/env node\nconsole.log("test")');chmodSync(join(f.bin,'wrangler'),0o700);
  const result=await f.owner.discoverResources();expect(result.resources.find(r=>r.provider==='github').evidence.probe.refused).toBeDefined();
  expect(result.resources.find(r=>r.provider==='aws').state).toBe('configured');expect(f.calls).toHaveLength(1);
  expect(f.calls[0].argv).toEqual([join(f.bin,'node'),join(f.bin,'wrangler'),'whoami']);
});
test('HTTP activation strips query credentials, refuses redirects, and propagates process-over-file precedence',async()=>{
  const f=fixture({env:{GEMINI_API_KEY:'private-google',OPENAI_API_KEY:'process-openai'}});
  writeFileSync(join(f.root,'.env'),'OPENAI_API_KEY=file-openai');const graph=await f.owner.discoverResources({probe_external:false});
  const result=await f.owner.activateProviders({graph});expect(result.summary.validated).toBe(2);
  const openai=f.requests.find(r=>r.url.includes('openai.com'));expect(openai.options.headers.Authorization).toBe('Bearer process-openai');
  expect(f.requests.every(r=>r.options.redirect==='error'&&r.options.method==='GET')).toBe(true);
  const google=result.results.find(r=>r.provider==='google-ai');expect(google.http_probe.endpoint).toBe('https://generativelanguage.googleapis.com/v1beta/models');
  expect(JSON.stringify(result)).not.toContain('private-google');expect(JSON.stringify(result)).not.toContain('process-openai');
});
test('Cloudflare account-owned tokens use account verification and require active status, not HTTP 200 alone',async()=>{
  const account='a'.repeat(32),token='cfat-sensitive-token';
  const f=fixture({env:{CLOUDFLARE_API_TOKEN:token,CLOUDFLARE_ACCOUNT_ID:account},
    request:async(url)=>Response.json({success:true,result:{id:'b'.repeat(32),status:'active'}})});
  const graph=await f.owner.discoverResources({probe_external:false}),result=await f.owner.activateProviders({graph});
  const row=result.results.find(x=>x.provider==='cloudflare');
  expect(row.state).toBe('validated');expect(row.http_probe.ok).toBe(true);
  expect(row.http_probe.endpoint).toBe(`https://api.cloudflare.com/client/v4/accounts/${account}/tokens/verify`);
  expect(f.requests).toHaveLength(1);
  expect(f.requests[0].options.headers.Authorization).toBe(`Bearer ${token}`);
  expect(JSON.stringify(result)).not.toContain(token);
});

test('Cloudflare verification refuses disabled, malformed, or invalid account credentials',async()=>{
  const account='c'.repeat(32);
  const f=fixture({env:{CLOUDFLARE_API_TOKEN:'private-token',CLOUDFLARE_ACCOUNT_ID:account},
    request:async()=>Response.json({success:true,result:{status:'disabled'}})});
  const graph=await f.owner.discoverResources({probe_external:false}),result=await f.owner.activateProviders({graph});
  const row=result.results.find(x=>x.provider==='cloudflare');
  expect(row.state).toBe('configured');expect(row.http_probe.ok).toBe(false);
  expect(row.http_probe.error).toBe('TokenInactiveOrMalformed');
  expect(JSON.stringify(result)).not.toContain('private-token');
  const malformed=fixture({env:{CLOUDFLARE_API_TOKEN:'private-token',CLOUDFLARE_ACCOUNT_ID:'../unexpected'},
    request:async()=>{throw Error('invalid account id must never be requested')}});
  const next=await malformed.owner.activateProviders({graph:await malformed.owner.discoverResources({probe_external:false})});
  expect(next.results.find(x=>x.provider==='cloudflare').state).toBe('configured');
  expect(malformed.requests).toHaveLength(0);
});

test('Cloudflare user-owned tokens retain user verification and reject non-token HTTP success',async()=>{
  const f=fixture({env:{CLOUDFLARE_API_TOKEN:'private-user-token'},
    request:async()=>Response.json({ok:true})});
  const result=await f.owner.activateProviders({graph:await f.owner.discoverResources({probe_external:false})});
  const token=result.results.find(x=>x.provider==='cloudflare');
  expect(token.http_probe.endpoint).toBe('https://api.cloudflare.com/client/v4/user/tokens/verify');
  expect(token.http_probe.ok).toBe(false);expect(token.state).toBe('configured');
  expect(token.http_probe.error).toBe('TokenInactiveOrMalformed');
});

test('failed and oversized HTTP observations do not preserve an old validated claim',async()=>{
  const f=fixture({env:{OPENAI_API_KEY:'private'},request:async()=>new Response('small',{headers:{'content-length':String(3*1024*1024)}})});
  const graph=await f.owner.discoverResources({probe_external:false});graph.resources.find(r=>r.provider==='openai').state='validated';
  const result=await f.owner.activateProviders({graph});const observed=result.results.find(r=>r.provider==='openai');
  expect(observed.state).toBe('configured');expect(observed.http_probe.ok).toBe(false);expect(observed.http_probe.body_sha256).toBe(null);
  expect(result.state).toBe('partial');
});
test('HTTP error messages never escape credentials and unapproved custom health URLs are refused',async()=>{
  const f=fixture({env:{OPENAI_API_KEY:'private-secret',POSTHOG_API_KEY:'key',POSTHOG_HOST:'http://127.0.0.1:1234'},request:async()=>{throw Error('Authorization Bearer private-secret');}});
  const graph=await f.owner.discoverResources({probe_external:false});const result=await f.owner.activateProviders({graph});
  expect(f.requests).toHaveLength(1);expect(JSON.stringify(result)).not.toContain('private-secret');expect(result.results.find(r=>r.provider==='posthog').http_probe.ok).toBe(false);
});
test('manifest verification binds Unicode and raw integral float spellings to historical digest',async()=>{
  const f=fixture(),m=manifest(f,'feldera',{description:'Bonfÿre 🜁'});
  let raw=readFileSync(m.path,'utf8').replace('"completed_at":1699999999','"completed_at":1699999999.0');
  const parsed=parsedDocument(raw,{ascii:false,omitKey:'manifest_digest'});raw=raw.replace(JSON.parse(raw).manifest_digest,parsed.calculated);writeFileSync(m.path,raw);
  const verified=await f.owner.verifyActivationManifest(m.path,{expectedId:'feldera'});expect(verified.valid).toBe(true);expect(verified.manifest_digest).toBe(parsed.calculated);
  writeFileSync(join(m.base,'proof.json'),'tampered');expect((await f.owner.verifyActivationManifest(m.path,{expectedId:'feldera'})).valid).toBe(false);
});
test('manifest refuses synthetic, wrong id, future clock, duplicate and escaping artifacts',async()=>{
  const f=fixture();let m=manifest(f,'feldera',{evidence_class:'synthetic'});
  expect((await f.owner.verifyActivationManifest(m.path)).failures).toContain('synthetic-evidence-not-live');
  // No caller can ask for synthetic evidence to be accepted.
  expect((await f.owner.verifyActivationManifest(m.path,{allowSynthetic:true})).valid).toBe(false);
  m=manifest(f,'feldera',{completed_at:1700000301});expect((await f.owner.verifyActivationManifest(m.path,{expectedId:'egglog'})).failures).toContain('id');
  expect((await f.owner.verifyActivationManifest(m.path)).failures).toContain('completed-at');
  const outside=join(f.dir,'outside');writeFileSync(outside,'outside');
  m=manifest(f,'feldera',{artifacts:[{path:outside,bytes:7,sha256:hash('outside')}]});
  expect((await f.owner.verifyActivationManifest(m.path)).failures).toContain('artifact-0:escape');
  symlinkSync(outside,join(m.base,'link'));
  m=manifest(f,'feldera',{artifacts:[{path:'link',bytes:7,sha256:hash('outside')}]});
  expect((await f.owner.verifyActivationManifest(m.path)).valid).toBe(false);
  m=manifest(f);m.body.artifacts.push({...m.body.artifacts[0]});writeFileSync(m.path,canonicalJson({...m.body,manifest_digest:digest(m.body)},{ascii:false}));
  expect((await f.owner.verifyActivationManifest(m.path)).failures).toContain('artifact-1:duplicate');
  m=manifest(f,'feldera',{commands:[{exit_code:0,cancelled:true}]});
  expect((await f.owner.verifyActivationManifest(m.path)).failures).toContain('no-successful-command');
});
test('all backends discovered; exact names only; live manifest activation is distinct from help/build success',async()=>{
  const f=fixture();manifest(f);manifest(f,'egglog',{evidence_class:'synthetic'});native(f,'hvm4');
  mkdirSync(join(f.root,'integrations/balancer'),{recursive:true});mkdirSync(join(f.root,'integrations/daft_extension'),{recursive:true});
  const result=await f.owner.discoverBackends();expect(result.backends).toHaveLength(14);
  expect(result.backends.find(b=>b.id==='feldera').state).toBe('active');expect(result.backends.find(b=>b.id==='egglog').state).toBe('missing');
  expect(result.backends.find(b=>b.id==='lance').sources).toEqual([]);expect(result.backends.find(b=>b.id==='daft').sources).toEqual([]);
  expect(result.backends.find(b=>b.id==='hvm4').state).toBe('source-backed');expect(result.synthetic_evidence_allowed).toBe(false);
  expect(f.receipts.at(-1).state).toBe('failed');expect(f.calls).toHaveLength(1);
});
test('explicit HVM source build preserves package identity and offline flags, without model/network execution',async()=>{
  const f=fixture({run:async argv=>({exit_code:0,stdout:argv.includes('metadata')?JSON.stringify({packages:[{name:'hvm4'}]}):'build fixture',stderr:''})});
  native(f,'cargo');const source=join(f.root,'vendor/hvm4');mkdirSync(source,{recursive:true});writeFileSync(join(source,'Cargo.toml'),'[package]\nname="hvm4"');
  const result=await f.owner.discoverBackends({build:true});const hvm=result.backends.find(b=>b.id==='hvm4');
  expect(hvm.source_activation.executed).toBe(true);expect(hvm.source_activation.selected_package).toBe('hvm4');expect(hvm.state).toBe('source-backed');
  expect(f.calls).toHaveLength(3);expect(f.calls.every(c=>c.argv.includes('--offline')&&c.argv.includes('--locked'))).toBe(true);
  expect(f.requests).toHaveLength(0);
});
test('cancellation refuses before effects; environment symlinks are not followed',async()=>{
  const f=fixture();const controller=new AbortController();controller.abort();
  await expect(f.owner.discoverResources({signal:controller.signal})).rejects.toThrow();
  await expect(f.owner.discoverBackends({signal:controller.signal})).rejects.toThrow();
  expect(f.calls).toHaveLength(0);expect(f.receipts).toHaveLength(0);expect(existsSync(f.home)).toBe(false);
  const outside=join(f.dir,'outside.env');writeFileSync(outside,'OPENAI_API_KEY=should-not-read');symlinkSync(outside,join(f.root,'.env'));
  const result=await f.owner.discoverResources({probe_external:false});expect(result.summary.configured).toBe(0);
});
test('late cancellation does not persist a partial discovery or proceed to the next process',async()=>{
  const controller=new AbortController();
  const f=fixture({run:async()=>{controller.abort();return {exit_code:0,stdout:'ignored'};}});
  native(f,'gh');native(f,'wrangler');
  await expect(f.owner.discoverResources({signal:controller.signal})).rejects.toThrow();
  expect(f.calls).toHaveLength(1);expect(f.resources).toHaveLength(0);expect(f.receipts).toHaveLength(0);
  expect(existsSync(f.home)).toBe(false);
});
test('chunked oversized HTTP response is cancelled and cannot become validation evidence',async()=>{
  let cancelled=false;
  const f=fixture({env:{OPENAI_API_KEY:'secret'},request:async()=>new Response(new ReadableStream({
    pull(controller){controller.enqueue(new Uint8Array(1024*1024));},cancel(){cancelled=true;}
  }))});
  const graph=await f.owner.discoverResources({probe_external:false});
  const result=await f.owner.activateProviders({graph});
  expect(cancelled).toBe(true);expect(result.summary.validated).toBe(0);
  expect(result.results.find(r=>r.provider==='openai').http_probe.body_sha256).toBe(null);
});
test('real disposable durable store retains all projections and a verifiable receipt chain after reopen',async()=>{
  const f=fixture({env:{OPENAI_API_KEY:'never-persist-this'}});
  const store=createV6Store({home:f.home,root:f.root,env:f.env,now:()=>1700000000});
  const owner=createV6Resources({store,root:f.root,userHome:f.userHome,env:f.env,now:()=>1700000000,
    run:async()=>{throw Error('unexpected process');},request:async()=>Response.json({ok:true})});
  try {
    const graph=await owner.discoverResources({probe_external:false});
    await owner.activateProviders({graph});await owner.discoverBackends();
    expect(store.db.query('SELECT COUNT(*) AS n FROM resources').get().n).toBeGreaterThanOrEqual(39);
    expect(store.db.query('SELECT COUNT(*) AS n FROM backends').get().n).toBe(14);
    expect(store.getMeta('provider_activation_summary').validated).toBe(1);
    expect(store.verifyReceipts()).toMatchObject({verified:true,receipts:3,failures:[]});
    expect(JSON.stringify(store.db.query('SELECT * FROM resources').all())).not.toContain('never-persist-this');
  } finally {store.close();}
  const reopened=createV6Store({home:f.home,root:f.root});
  try {expect(reopened.verifyReceipts()).toMatchObject({verified:true,receipts:3});}
  finally {reopened.close();}
});
