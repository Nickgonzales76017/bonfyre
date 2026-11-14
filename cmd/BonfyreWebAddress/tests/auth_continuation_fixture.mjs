// Isolated native owners. No live credentials, sources, account grants or models.
import {mkdtempSync,mkdirSync,copyFileSync,writeFileSync,readFileSync,rmSync,openSync,closeSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {execFileSync,spawn} from 'node:child_process';
import {createServer} from 'node:net';
import {createHash} from 'node:crypto';
export const root=resolve(import.meta.dir,'../../..');
export const password='continuation-fixture-passphrase';
export const decode=s=>s.replaceAll('&amp;','&').replaceAll('&quot;','"').replaceAll('&#39;',"'").replaceAll('&lt;','<').replaceAll('&gt;','>');
export const hidden=html=>decode(html.match(/name="next" value="([^"]*)"/)?.[1]??'');
export const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
export async function freePort(){const server=createServer();await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});const port=server.address().port;await new Promise(resolve=>server.close(resolve));return port;}
export async function fixture({beforeServe}={}){
  const home=mkdtempSync(join(tmpdir(),'aurekai-continuation-'));
  // Release checks may pin copied binaries while another source lane continues.
  const web=process.env.BONFYRE_TEST_WEB_BINARY??join(root,'cmd/BonfyreWebAddress/bonfyre-web-address'),cms=process.env.BONFYRE_TEST_CMS_BINARY??join(root,'cmd/BonfyreCMS/bonfyre-cms'),auth=process.env.BONFYRE_TEST_AUTH_BINARY??join(root,'cmd/BonfyreAuth/bonfyre-auth');
  const schemas=join(home,'schemas'),savedSchemas=join(home,'saved-types'),foreign=join(home,'foreign'),db=join(home,'cms.db'),authdb=join(home,'auth.db');
  for(const dir of [schemas,savedSchemas,join(foreign,'receipts'),join(foreign,'bodies')])mkdirSync(dir,{recursive:true});
  for(const type of ['world','world_fragment','internet_source','publication_occurrence','content_flag','site_page','object_contract'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
  copyFileSync(join(root,'cmd/BonfyreCMS/content-types/saved_item.json'),join(savedSchemas,'saved_item.json'));
  const env={...process.env,BONFYRE_AUTH_BINARY:auth,BONFYRE_CMS_BINARY:cms,BONFYRE_CMS_DB:db,BONFYRE_CMS_SCHEMAS:schemas,BONFYRE_SAVED_ITEM_SCHEMAS:savedSchemas,BONFYRE_FOREIGN_ROOT:foreign,BONFYRE_FACT_DELTAS:'',BONFYRE_WORKSPACE_CLIENT:process.env.BONFYRE_TEST_PUBLIC_BROWSER??join(root,'integrations/browser')};
  const run=(bin,args,options={})=>execFileSync(bin,args,{encoding:'utf8',stdio:['pipe','pipe','pipe'],env,timeout:20000,...options});
  const content=(...args)=>run(cms,[...args,'--db',db,'--schemas',schemas]);
  const entry=(type,value)=>content('entry','create',type,JSON.stringify(value),'--ns','root');
  let server;
  const close=async()=>{if(server&&server.exitCode===null){try{process.kill(-server.pid,'SIGTERM');}catch{}await Promise.race([new Promise(resolve=>server.once('exit',resolve)),pause(1500)]);if(server.exitCode===null)try{process.kill(-server.pid,'SIGKILL');}catch{}}rmSync(home,{recursive:true,force:true});};
  try{
    content('schema','migrate');
    entry('internet_source',{family:'news',source:'newsdesk',label:'Fixture news desk',says:'Fixture material',url:'https://newsdesk.test/rss',twin:'newsdesk-twin',state:'observed',extract:'rss:item',title_field:'title',meta_field:'',link_field:'feed:auto',image_field:'',image_base:'',sub_field:'',body_regime:'retained-public',status:'published',tier:'warm',kind:'post'});
    const source=join(foreign,'bodies/newsdesk.body');
    const rss='<?xml version="1.0"?><rss><channel><item><title>City opens a new library</title><link>https://newsdesk.test/library</link><pubDate>Tue, 15 Sep 2026 10:00:00 GMT</pubDate><description>A public source for the continuation fixture.</description></item></channel></rss>';
    writeFileSync(source,rss);writeFileSync(join(foreign,'receipts/newsdesk-twin.json'),JSON.stringify({body_path:source}));
    if(beforeServe)await beforeServe({home,env,run,db,authdb,web,cms,auth});
    run(web,['bootstrap-owned','--db',join(home,'matrix.db')]);
    const port=await freePort(),log=join(home,'server.log'),fd=openSync(log,'w',0o600);
    server=spawn(web,['serve','--db',join(home,'matrix.db'),'--auth-db',authdb,'--port',String(port),'--site-root',process.env.BONFYRE_TEST_SITE_ROOT??join(root,'site/matrix')],{env,stdio:['ignore',fd,fd],detached:true});closeSync(fd);
    let spawnError;server.once('error',error=>{spawnError=error;});
    const base=`http://127.0.0.1:${port}`;
    async function request(path,{host='aurekai.life',cookie='',form,method=form===undefined?'GET':'POST',headers={}}={}){
      const body=form===undefined?undefined:typeof form==='string'?form:new URLSearchParams(form).toString();
      const response=await fetch(base+path,{method,headers:{Host:host,Accept:'text/html',...(cookie?{Cookie:cookie}:{}),...(body===undefined?{}:{Origin:'http://'+host,'Content-Type':'application/x-www-form-urlencoded'}),...headers},body,redirect:'manual',signal:AbortSignal.timeout(10000)});
      return {status:response.status,headers:response.headers,body:await response.text()};
    }
    let ready=false;
    for(let i=0;i<100;i++){if(spawnError)throw spawnError;if(server.exitCode!==null)throw Error('Native test server exited.');try{const response=await request('/healthz');if(response.status===200){ready=true;break;}}catch{}await pause(50);}
    if(!ready)throw Error('Native test server did not become ready.');
    const page=await request('/discover'),item=page.body.match(/href="\/o\/(newsdesk\/[a-f0-9]{24})"/)?.[1];
    if(!item)throw Error('Retained fixture item did not appear in native discovery.');
    function authored(){
      for(const [route,operation,fields,label] of [['/login','Session.open','email,password','Sign in'],['/signup','Account.create','name,email,password','Create account']]){
        entry('object_contract',{object_name:label,applies_to_kind:'identity_root',birth_operation:operation,birth_mode:'request',collection_lens:'account',human_fields:fields,submit_to:route,submit_method:'POST',action_label:label,status:'published'});
        entry('site_page',{route,title:'Authored '+label,heading:'Authored '+label,aperture_subject:'identity_root:bonfyre',aperture_operation:operation,machine_view:'deferred',status:'published'});
      }
    }
    return {home,db,authdb,web,cms,auth,base,port,log,item,path:'/o/'+item,source,rss,request,authored,close,run,
      fingerprint:createHash('sha256').update(readFileSync(web)).digest('hex')};
  }catch(error){await close();throw error;}
}
