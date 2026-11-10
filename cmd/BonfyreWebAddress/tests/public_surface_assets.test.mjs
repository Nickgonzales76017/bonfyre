import {test,expect} from 'bun:test';
import {join,dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {fixture,root} from './auth_continuation_fixture.mjs';
import {browserReadback} from '../../../scripts/release/public_surface_rollout.mjs';
test('every frozen public browser module is served byte-exactly by the actual native HTTP owner',async()=>{
 const client=process.env.BONFYRE_TEST_PUBLIC_BROWSER||join(root,'integrations/browser'),prior=process.env.BONFYRE_WORKSPACE_CLIENT;
 let f;try{
  process.env.BONFYRE_WORKSPACE_CLIENT=client;f=await fixture();
  const raw=(path,options={})=>fetch(f.base+path,{...options,headers:{Host:"aurekai.work",...options.headers},redirect:"manual",signal:AbortSignal.timeout(10000)});
  const result=await browserReadback(f.base,null,dirname(client),async(base,path)=>{
   const r=await raw(path),bytes=Buffer.from(await r.arrayBuffer());
   return {status:r.status,type:r.headers.get('content-type'),sha256:createHash('sha256').update(bytes).digest('hex')};
  });
  expect(result.length).toBeGreaterThan(20);
  for(const name of ['authentication-capability-surface.mjs','provider-auth-capability.mjs']){
   expect(result.find(r=>r.route==='/apps/'+name)?.status).toBe(200);
   expect((await raw('/apps/'+name,{method:'POST',body:'x'})).status).toBe(405);
  }
  expect((await raw('/apps/undeclared-private-module.mjs')).status).not.toBe(200);
 }finally{await f?.close();if(prior===undefined)delete process.env.BONFYRE_WORKSPACE_CLIENT;else process.env.BONFYRE_WORKSPACE_CLIENT=prior;}
},60000);
