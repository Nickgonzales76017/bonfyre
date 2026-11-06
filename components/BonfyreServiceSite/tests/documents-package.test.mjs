import { test, expect } from 'bun:test';
import { mkdtempSync, readFileSync, existsSync, readdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { packageDocuments, documentPackageSources } from '../document-package.mjs';

test('source package compiles and serves with its own binaries, then preserves removal after restart', async () => {
  const temporary=mkdtempSync(join(tmpdir(),'aurekai-document-package-'));
  const root=join(temporary,'source'), port=22000+Math.floor(Math.random()*18000), origin='http://127.0.0.1:'+port;
  let server;
  const launch=async()=>{
    server=Bun.spawn([process.execPath,'run.mjs','serve',String(port)],{cwd:root,stdout:'pipe',stderr:'pipe'});
    let page;
    for(let i=0;i<300;i++) {
      if(server.exitCode!==null) throw Error('Package server exited: '+await new Response(server.stderr).text());
      try { const r=await fetch(origin);if(r.ok){page=await r.text();break;} } catch {}
      await Bun.sleep(100);
    }
    if(!page) throw Error('Package server did not become ready.');
    return page.match(/name="document-session" content="([^"]+)"/)[1];
  };
  const stop=async()=>{server.kill('SIGTERM');await server.exited;server=null;};
  const api=async(token,path,body)=> {
    const response=await fetch(origin+path,{method:body?'POST':'GET',headers:{'x-document-session':token,...(body?{'content-type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})});
    return {status:response.status,data:await response.json()};
  };
  try {
    const manifest=packageDocuments(root);
    expect(manifest.files.map(f=>f.path)).toEqual(documentPackageSources);
    for(const row of manifest.files) expect(createHash('sha256').update(readFileSync(join(root,row.path))).digest('hex')).toBe(row.sha256);
    expect(()=>packageDocuments(root)).toThrow('never overwritten');
    expect(existsSync(join(root,'.bonfyre'))).toBe(false);
    expect(existsSync(join(root,'packs/aurekai-work-samples/household/.documents'))).toBe(false);
    expect(readdirSync(root).sort()).toEqual(['.gitignore','LICENSE','README.txt','cmd','components','integrations','lib','manifest.json','packs','run.mjs'].sort());
    const build=Bun.spawn([process.execPath,'run.mjs','build'],{cwd:root,stdout:'pipe',stderr:'pipe'});
    const [out,err,code]=await Promise.all([new Response(build.stdout).text(),new Response(build.stderr).text(),build.exited]);
    if(code)throw Error(err);
    expect(JSON.parse(out).binaries.map(b=>b.name)).toEqual(['bonfyre-embed','bonfyre-vec']);
    let token=await launch();
    const found=await api(token,'/api/search',{query:'Where is the dishwasher receipt?',mode:'semantic'});
    expect(found.status).toBe(200);
    expect(found.data.results[0].document_id).toBe('appliance-receipts');
    const hit=found.data.results[0], source=(await api(token,'/api/source?id='+hit.document_id)).data;
    expect(source.digest).toBe(hit.digest);
    expect(source.text).toContain(hit.text);
    expect(found.data).not.toHaveProperty('answer');
    expect((await api(token,'/api/search',{query:'submarine insurance policy',mode:'phrase'})).data.results).toEqual([]);
    expect((await api(token,'/api/search',{query:''})).status).toBe(400);
    for(const asset of ['/documents.mjs','/documents.css','/store.mjs']) expect((await fetch(origin+asset)).status).toBe(200);
    expect((await api(token,'/api/remove',{id:hit.document_id,digest:'wrong'})).status).toBe(400);
    expect((await api(token,'/api/remove',{id:hit.document_id,digest:hit.digest})).status).toBe(200);
    await stop();token=await launch();
    expect((await api(token,'/api/documents')).data.documents.find(d=>d.id===hit.document_id).indexed).toBe(false);
    expect((await api(token,'/api/search',{query:'Where is the dishwasher receipt?',mode:'semantic'})).data.results.some(d=>d.document_id===hit.document_id)).toBe(false);
    await stop();
    // Taking away this package's native command must fail; never fall back to the user's installation.
    rmSync(join(root,'bin/bonfyre-embed'));
    const missing=Bun.spawn([process.execPath,'run.mjs','serve',String(port)],{cwd:root,stdout:'pipe',stderr:'pipe'});
    const [missingErr,missingCode]=await Promise.all([new Response(missing.stderr).text(),missing.exited]);
    expect(missingCode).not.toBe(0);
    expect(missingErr).toContain('build first');
  } finally {if(server)await stop();rmSync(temporary,{recursive:true,force:true});}
},120000);

