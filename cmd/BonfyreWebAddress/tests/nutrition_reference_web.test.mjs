import {test,expect} from 'bun:test';
import {Database} from 'bun:sqlite';
import {mkdtempSync,mkdirSync,readFileSync,copyFileSync,writeFileSync,rmSync,existsSync} from 'node:fs';
import {tmpdir,homedir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';
const root=fileURLToPath(new URL('../../../',import.meta.url));
const web=join(root,'cmd/BonfyreWebAddress/bonfyre-web-address'),recipe=join(root,'cmd/BonfyreRecipe/build/bonfyre-recipe'),cms=join(root,'cmd/BonfyreCMS/bonfyre-cms'),auth=join(root,'cmd/BonfyreAuth/bonfyre-auth');
test.skipIf(process.env.BONFYRE_NUTRITION_REFERENCE_WITNESS!=='1')('retained USDA reference formulation reaches Chrome, the existing CMS form and the private Desk',async()=>{
 const pack=join(homedir(),'.bonfyre/estate-local/recipe-material.db'),nutrition=join(homedir(),'.bonfyre/recipes.db');
 expect(existsSync(pack)&&existsSync(nutrition)).toBe(true);const hash=p=>createHash('sha256').update(readFileSync(p)).digest('hex'),before=hash(pack);
 const t=mkdtempSync(join(tmpdir(),'bf-nutrition-browser-')),db=join(t,'cms.db'),schemas=join(t,'schemas'),authdb=join(t,'auth.db');let server;
 const invoke=(bin,args)=>{const r=spawnSync(bin,args,{cwd:root,env:{...process.env},encoding:'utf8',timeout:30000,maxBuffer:2000000});expect(r.status).toBe(0);return r.stdout};
 try{
  mkdirSync(schemas);copyFileSync(join(root,'cmd/BonfyreCMS/content-types/wiki_page.json'),join(schemas,'wiki_page.json'));invoke(cms,['schema','migrate','--db',db,'--schemas',schemas]);
  const password='disposable-reference-witness';invoke(auth,['--db',authdb,'signup','--email','reference@example.test','--name','Reference','--password',password,'--tier','pro']);
  const token=JSON.parse(invoke(auth,['--db',authdb,'login','--email','reference@example.test','--password',password])).session.token;
  invoke(web,['bootstrap-owned','--db',join(t,'matrix.db')]);
  const reservation=Bun.serve({hostname:'127.0.0.1',port:0,fetch(){return new Response('reserved')}}),port=reservation.port;reservation.stop(true);
  server=Bun.spawn([web,'serve','--db',join(t,'matrix.db'),'--auth-db',authdb,'--port',String(port),'--site-root',join(root,'site/matrix')],{cwd:root,env:{...process.env,HOME:t,BONFYRE_RECIPE_MATERIAL:pack,BONFYRE_NUTRITION_DB:nutrition,BONFYRE_CMS_DB:db,BONFYRE_CMS_SCHEMAS:schemas,BONFYRE_CMS_BINARY:cms,BONFYRE_AUTH_BINARY:auth,BONFYRE_ADDRESS_INDEX:join(t,'unbound-address.db'),BONFYRE_SUBJECT_FABRIC_DB:join(t,'fabric.db'),BONFYRE_BROWSER_QUEUE_DB:join(t,'queue.db'),BONFYRE_PUBLIC_PROJECTION_CACHE_DB:''},stdout:'ignore',stderr:'ignore'});
  const get=async p=>fetch(`http://127.0.0.1:${port}${p}`,{headers:{Host:'aurekai.life',Accept:'text/html',Authorization:'Bearer '+token}});
  let ready=false;for(let i=0;i<150;i++){try{if((await get('/healthz')).status===200){ready=true;break}}catch{}await Bun.sleep(50)}expect(ready).toBe(true);
  const path='/recipes/plan?items='+encodeURIComponent('prep:oat-bite-variations*1'),html=await(await get(path)).text();
  expect(html).toContain('334.794 kcal');expect(html).toContain('Additional nutrient measures');expect(html).toContain('Save private working plan');
  const text=await(await get(path+'&format=brief')).text(),cli=invoke(recipe,['corpus','week',pack,'prep:oat-bite-variations*1','brief']);expect(text).toBe(cli);expect(Buffer.byteLength(text)).toBeLessThan(48000);
  expect(text).toContain('1089\tIron, Fe');expect(text).toContain('reference_scenario');expect(text).toContain('Unassigned binder remainder');
  await withChrome(async({open})=>{
   const {page,go}=await open(),evaluate=async expression=>{const r=await page('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});expect(r.exceptionDetails).toBeUndefined();return r.result.value};
   await page('Network.setCookie',{name:'bonfyre_session',value:token,url:`http://aurekai.life:${port}/`,httpOnly:true,sameSite:'Lax'});await go(`http://aurekai.life:${port}${path}`);
   expect(await evaluate('document.querySelector("textarea[name=content]").value')).toBe(text);
   await evaluate('document.querySelector("[data-nutrition-owner] details").open=true;true');
   if(process.env.BONFYRE_NUTRITION_SCREENSHOT){const image=await page('Page.captureScreenshot',{format:'png'});writeFileSync(process.env.BONFYRE_NUTRITION_SCREENSHOT,Buffer.from(image.data,'base64'));}
   await evaluate('const input=document.querySelector("input[name=title]");input.value="USDA formulation witness";input.form.requestSubmit();true');
   let saved;for(let i=0;i<150;i++){const d=new Database(db,{readonly:true});try{saved=d.query('SELECT title,content,namespace FROM wiki_page').get()}finally{d.close()}if(saved)break;await Bun.sleep(50)}
   if(!saved)throw Error('Native CMS save failed: '+await evaluate('document.body.textContent.slice(-12000)'));
   expect(saved?.title).toBe('USDA formulation witness');expect(saved.namespace).not.toBe('root');expect(saved.content.replaceAll('\r\n','\n')).toBe(text);
   expect(await(await get('/studio/')).text()).toContain('USDA formulation witness');
  },{timeoutMs:45000,flags:['--host-resolver-rules=MAP aurekai.life 127.0.0.1','--no-proxy-server']});
  expect(hash(pack)).toBe(before);console.log(JSON.stringify({scope:'actual retained USDA data and authored reference scenario; disposable CMS/auth',brief_bytes:Buffer.byteLength(text),source_unchanged:true}));
 }finally{if(server){server.kill();await server.exited}rmSync(t,{recursive:true,force:true})}
},90000);
