import {test,expect} from 'bun:test';
import {mkdtempSync,rmSync,readdirSync,readFileSync,existsSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {publishWorkbench,PUBLIC_ORIGIN} from '../publish-workbench.mjs';
async function fixture(fn){const dir=mkdtempSync(join(tmpdir(),'workbench-release-test-')),files=new Map(),calls=[];let production='prior';
 const cloud={api:async(method,path)=>{expect(method).toBe('GET');expect(path).toBe('/pages/projects/aurekai-work-samples');return {result:{canonical_deployment:{id:production}}};}};
 const request=async url=>{expect(url.startsWith(PUBLIC_ORIGIN+'/')).toBe(true);const name=url.slice(PUBLIC_ORIGIN.length+1);return files.has(name)?new Response(files.get(name)):new Response('missing',{status:404});};
 const publisher=async(path,project,label,options)=>{calls.push({project,label,options});expect(existsSync(join(path,'_worker.js'))).toBe(false);for(const name of readdirSync(path))files.set(name,readFileSync(join(path,name)));};
 const options={cloud,request,publisher,receiptDirectory:dir};try{await fn({dir,files,calls,options,setProduction:value=>production=value});}finally{rmSync(dir,{recursive:true,force:true});}}
test('checking an unpublished projection never invokes publication',async()=>fixture(async({dir,calls,options})=>{
 const r=await publishWorkbench(options);expect(r.state).toBe('READY_TO_PUBLISH');expect(r.published).toBe(false);expect(calls).toHaveLength(0);expect(existsSync(join(dir,'workbench-publication.yaff'))).toBe(false);
}));
test('explicit publication uses the existing project and repeats without deployment',async()=>fixture(async({dir,calls,options})=>{
 const r=await publishWorkbench({...options,apply:true});expect(r.state).toBe('PUBLISHED');expect(r.files).toBe(20);expect(calls).toEqual([{project:'aurekai-work-samples',label:undefined,options:{previewBranch:'browser-workbench'}}]);
 expect((await publishWorkbench(options)).state).toBe('ALREADY_PUBLISHED');expect(calls).toHaveLength(1);expect(readFileSync(join(dir,'workbench-publication.yaff'),'utf8')).toContain(r.release);
}));
test('mismatched served source cannot earn a completed publication receipt',async()=>fixture(async({dir,files,options})=>{
 const publisher=async(...args)=>{await options.publisher(...args);files.set('program-editor.mjs',Buffer.from('wrong bytes'));};
 await expect(publishWorkbench({...options,apply:true,publisher})).rejects.toThrow('differs: program-editor.mjs');expect(existsSync(join(dir,'workbench-publication.yaff'))).toBe(false);
}));
test('a concurrent production change requires reconciliation before success is recorded',async()=>fixture(async({dir,options,setProduction})=>{
 const publisher=async(...args)=>{await options.publisher(...args);setProduction('changed');};
 await expect(publishWorkbench({...options,apply:true,publisher})).rejects.toThrow('Production changed');expect(existsSync(join(dir,'workbench-publication.yaff'))).toBe(false);
}));

// Public browser execution is a separate opt-in witness using the owned URL.
