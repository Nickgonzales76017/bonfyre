import {test,expect,beforeAll} from 'bun:test';
import {mkdtempSync,rmSync,readdirSync} from 'node:fs';
import {tmpdir,homedir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
import {materializeConnectedInstruments,projectConnectedInstrumentMaterial} from '../materialize_workspace_package.mjs';
import {setupOptions,setupRecords,projectConnectedInstrumentsSetup} from '../publish_experience_setups.mjs';
const commit='396bcaf02f24bd96c1789aee5410d111edbf0818';let originals;
beforeAll(async()=>{originals=await loadWorkspaceFiles(join(homedir(),'.bonfyre/workspace-packages'),'fad60f319bc25583dce678605babb7c4946c52d343ae8c005458a0c163b7acfb');});
const git=body=>createHash('sha1').update(`blob ${body.length}\0`).update(body).digest('hex');
function fixture(change=()=>{}){
  const rows=[...originals].map(([path,body])=>({path,mode:'100644',type:'blob',sha:git(body),size:body.length}));
  const state={rows,truncated:false,corrupt:null,requests:[]};change(state);
  const fetcher=async url=>{
    state.requests.push(url);const path=new URL(url).pathname;
    if(path.endsWith('/git/trees/'+commit))return Response.json({sha:'a'.repeat(40),truncated:false,tree:[{path:'site',mode:'040000',type:'tree',sha:'b'.repeat(40)}]});
    if(path.endsWith('/git/trees/'+'b'.repeat(40)))return Response.json({sha:'b'.repeat(40),truncated:false,tree:[{path:'instruments',mode:'040000',type:'tree',sha:'c'.repeat(40)}]});
    if(path.endsWith('/git/trees/'+'c'.repeat(40)))return Response.json({sha:'c'.repeat(40),truncated:state.truncated,tree:state.rows});
    const name=path.split('/site/instruments/')[1],body=originals.get(name);if(!body)return new Response('',{status:404});
    return new Response(state.corrupt===name?Buffer.alloc(body.length,65):body);
  };return{state,fetcher};
}
async function isolated(work){const root=mkdtempSync(join(tmpdir(),'connected-package-test-'));try{return await work(root);}finally{rmSync(root,{recursive:true,force:true});}}
test('exact Git blob bytes and native projection are deterministic without changing source material',async()=>isolated(async root=>{
  const f=fixture(),first=await materializeConnectedInstruments({commit,root,fetcher:f.fetcher}),again=await materializeConnectedInstruments({commit,root,fetcher:f.fetcher});
  expect(first.id).toBe(again.id);const kept=await loadWorkspaceFiles(root,first.sourceId);for(const[key,body]of originals)expect(kept.get(key)).toEqual(body);
  const files=await loadWorkspaceFiles(root,first.id);expect(files.has('bridge/publication-picker.mjs')).toBe(true);expect(files.get('bridge/publication-research-client.mjs').toString()).toContain("from './publication-picker.mjs'");
  expect(f.state.requests.every(url=>url.includes(commit)||url.includes('/git/trees/'))).toBe(true);
}));
test('truncated trees and symlink members cannot publish a partial package',async()=>{
  for(const change of [s=>s.truncated=true,s=>s.rows[0].mode='120000'])await isolated(async root=>{const f=fixture(change);await expect(materializeConnectedInstruments({commit,root,fetcher:f.fetcher})).rejects.toThrow();expect(readdirSync(root)).toEqual([]);});
});
test('duplicate paths and path traversal refuse before any raw source fetch',async()=>{
  for(const change of [s=>s.rows.push(s.rows[0]),s=>s.rows[0].path='../escape.mjs'])await isolated(async root=>{const f=fixture(change);await expect(materializeConnectedInstruments({commit,root,fetcher:f.fetcher})).rejects.toThrow();expect(f.state.requests.length).toBe(3);expect(readdirSync(root)).toEqual([]);});
});
test('corrupt bytes cannot be admitted under the reviewed Git blob identity',async()=>isolated(async root=>{const f=fixture(s=>s.corrupt='working-record.mjs');await expect(materializeConnectedInstruments({commit,root,fetcher:f.fetcher})).rejects.toThrow('bytes changed');expect(readdirSync(root)).toEqual([]);}));
test('unexpected browser caches block native packaging rather than leaking destination work',async()=>isolated(async root=>{const source=new Map(originals);source.set('new-cache.mjs',Buffer.from('localStorage.setItem("secret",value)'));await expect(projectConnectedInstrumentMaterial(source,{commit,sourceTree:'c'.repeat(40),root})).rejects.toThrow('browser persistence');expect(readdirSync(root)).toEqual([]);}));
test('publication preserves all four record kinds and their explicit incoming handoff family',()=>{
  const id='d'.repeat(64),opts=setupOptions(['--connected-instruments-workspace',id]);expect(opts.apply).toBeUndefined();expect(setupRecords(opts)).toEqual(projectConnectedInstrumentsSetup(id));
  const rows=setupRecords(opts);expect(rows.filter(x=>x.value.demand_kind==='workspace-record').map(x=>x.value.fields)).toEqual(['traces','research','schema','product','incoming'].map(k=>'draft:instruments:'+k+':'));
  expect(rows.at(-1).type).toBe('pack_head');expect(rows.some(x=>/grant|role|credential/.test(x.type))).toBe(false);
});
test('mixed package selectors and unpinned source coordinates are refused',async()=>{
  expect(()=>setupOptions(['--connected-instruments-workspace','d'.repeat(64),'--finance-workspace','e'.repeat(64)])).toThrow();
  const f=fixture();await expect(materializeConnectedInstruments({commit:'main',fetcher:f.fetcher})).rejects.toThrow('exact');expect(f.state.requests.length).toBe(0);
});
