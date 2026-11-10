// Isolated Auth, CMS and WebAddress. No live accounts, grants or customer writes.
import {packSetupFixture,root} from './pack_setup_fixture.mjs';
import {copyFileSync,readFileSync} from 'node:fs';
import {join} from 'node:path';
import {openDestinationStore} from '../../../integrations/browser/workspace-store.mjs';
export {root};
export async function workspaceFixture({extraPrefixes=[],beforeServe}={}){
 const release=process.env.BONFYRE_WORKSPACE_RELEASE;
 if(!/^[a-f0-9]{64}$/.test(release??''))throw Error('Set BONFYRE_WORKSPACE_RELEASE to a verified materialized editor release.');
 const previous=process.env.BONFYRE_WORKSPACE_CLIENT;process.env.BONFYRE_WORKSPACE_CLIENT=process.env.BONFYRE_TEST_WORKSPACE_CLIENT||join(root,'integrations/browser');
 let f;try{f=await packSetupFixture({beforeServe});}finally{if(previous===undefined)delete process.env.BONFYRE_WORKSPACE_CLIENT;else process.env.BONFYRE_WORKSPACE_CLIENT=previous;}
 try{
  const schemas=join(f.home,'schemas');
  for(const type of ['application_binding','household'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types/'+type+'.json'),join(schemas,type+'.json'));
  f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
  // Browser requests use loopback, not the Host override in HTTP-only checks.
  f.run(f.web,['domain','--db',join(f.home,'matrix.db'),'--host','127.0.0.1','--territory','work','--scope','public','--world','CANDIDATE','--repr','HTML','--evidence','fixture:browser-loopback']);
  function declare(id,title,entry){
   f.pack(id,title);f.add('pack_demand',{pack:id,demand_kind:'workspace',needs:'spaces',preferred_sources:release,fields:entry,status:'published'});
   for(const fields of ['draft:catalogue:','review:catalogue:','release:catalogue:','fixture-plan:','product-model:',...extraPrefixes])f.add('pack_demand',{pack:id,demand_kind:'workspace-record',needs:'spaces',fields,status:'published'});
  }
  declare('catalogue','Catalogue and product studio','catalogue/');declare('rooms','Room design studio','product-room/');
  const owner=await f.login('workspace-owner@example.test'),writer=await f.login('workspace-writer@example.test'),reader=await f.login('workspace-reader@example.test'),other=await f.login('workspace-other@example.test');
  for(const [user,role]of [[owner,'admin'],[writer,'write'],[reader,'read']])f.grant(user.id,'workspace-team',role);
  f.add('household',{household_id:'workspace-home',label:'Fixture household',steward:'user:'+owner.id,opened_at:'2026-10-15',cannot_infer:'Fictional household for an isolated test.',status:'published'});
  const page=(pack,context='self',user=owner)=>f.request('/packs/setup?'+new URLSearchParams({pack,context}),{cookie:user.cookie});
  async function action(operation,pack,context='self',user=owner){
   const current=await page(pack,context,user);if(current.status!==200)throw Error('Cannot review configuration: '+current.status+' '+current.body.slice(-600));
   const response=await f.request('/packs/setup',{cookie:user.cookie,form:f.form(current.body,operation)});
   if(response.status!==303)throw Error('Configuration '+operation+' failed: '+response.status+' '+response.body.slice(-1500));
   return response;
  }
  async function install(pack,context='self',user=owner){
   await action('install',pack,context,user);await action('activate',pack,context,user);
   const current=await page(pack,context,user),entry=current.body.match(/data-pack-editor href="([^"]*)"/);
   if(!entry)throw Error('Installed configuration did not open its specialized editor: '+current.body.slice(-1000));
   const path=entry[1].replaceAll('&amp;','&');return {path,id:path.split('/')[2],endpoint:'/apps/'+path.split('/')[2]+'/_records',context,pack};
  }
  function request(path,user=owner,options={}){
   return fetch(new URL(path,f.base),{...options,headers:{...(user?{Cookie:user.cookie}:{}),...(options.body?{Origin:f.base}:{}),...options.headers},redirect:'manual',signal:AbortSignal.timeout(20000)});
  }
  const store=(binding,user=owner)=>openDestinationStore(binding.endpoint,{origin:f.base,request:(url,options)=>request(url,user,options)});
  return {...f,release,owner,writer,reader,other,page,action,install,raw:request,store,serverLog:()=>readFileSync(f.log,'utf8')};
 }catch(error){await f.close();throw error;}
}
