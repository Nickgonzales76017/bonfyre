import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { withChrome } from '../../../integrations/browser/capture/chrome-session.mjs';
const root=fileURLToPath(new URL('../../../',import.meta.url));
const web=resolve(process.argv[2]??join(root,'cmd/BonfyreWebAddress/bonfyre-web-address'));
const recipe=join(root,'cmd/BonfyreRecipe/build/bonfyre-recipe');
const temp=mkdtempSync(join(tmpdir(),'bonfyre-recipe-web-'));
let server,passed=0;
const checked=(name,fn)=>{fn();passed++;console.log('PASS '+name)};
const run=(bin,args)=>{const r=spawnSync(bin,args,{encoding:'utf8',timeout:20000,maxBuffer:2000000});assert.equal(r.status,0,r.stderr);return r.stdout};
try {
 const pack=process.env.BONFYRE_RECIPE_TEST_MATERIAL??join(temp,'recipes.db'),matrix=join(temp,'matrix.db');
 if(!process.env.BONFYRE_RECIPE_TEST_MATERIAL)run(recipe,['corpus','factor',join(root,'cmd/BonfyreRecipe/examples/preparation-family.jsonl'),pack]);
 run(web,['bootstrap-owned','--db',matrix]);
 const reserve=Bun.serve({hostname:'127.0.0.1',port:0,fetch(){return new Response('reserved')}}),port=reserve.port;
 reserve.stop(true);
 server=Bun.spawn([web,'serve','--db',matrix,'--auth-db',join(temp,'auth.db'),'--port',String(port),'--site-root',join(root,'site/matrix')],{cwd:root,env:{...process.env,BONFYRE_RECIPE_MATERIAL:pack,BONFYRE_CMS_DB:join(temp,'cms.db'),BONFYRE_FOREIGN_ROOT:join(temp,'foreign'),BONFYRE_FOREIGN_RECEIPTS:join(temp,'foreign/receipts'),BONFYRE_SUBJECT_FABRIC_DB:join(temp,'fabric.db'),BONFYRE_PUBLIC_PROJECTION_CACHE_DB:''},stdout:'ignore',stderr:'ignore'});
 const base=`http://127.0.0.1:${port}`,get=async path=>{const r=await fetch(base+path,{headers:{Host:'aurekai.life',Accept:'text/html'}});return{status:r.status,text:await r.text(),type:r.headers.get('content-type')}};
 let ready=false;
 for(let i=0;i<150;i++){try{const r=await get('/healthz');if(r.status===200){ready=true;break}}catch{}if(server.exitCode!==null)break;await Bun.sleep(50)}
 assert(ready,'native WebAddress did not become ready');
 const index=await get('/recipes');
 checked('normal server returns the recipe collection',()=>{assert.equal(index.status,200);assert.match(index.text,/Ten oat-bite variations/)});
 checked('collection uses existing native site styling',()=>assert.match(index.text,/assets\/matrix\.css/));
 const page=await get('/recipes?id=prep%3Aoat-bite-variations');
 checked('detail contains the native batch result',()=>{assert.equal(page.status,200);assert.match(page.text,/10 distinct variants/);assert.match(page.text,/arithmetic remainder: 2 tbsp/)});
 const two=await get('/recipes?id=prep%3Aoat-bite-variations&copies=2');
 checked('same address recalculates two sets',()=>{assert.equal(two.status,200);assert.match(two.text,/2 declared-capacity base batch/);assert.match(two.text,/arithmetic remainder: 4 tbsp/)});
 const list=await get('/recipes?id=prep%3Aoat-bite-variations&copies=2&format=list');
 checked('HTTP list and native CLI are byte identical',()=>assert.equal(list.text,run(recipe,['corpus','list',pack,'prep:oat-bite-variations','2'])));
 checked('list has its actual content type',()=>assert.match(list.type,/tab-separated-values/));
 const brief=await get('/recipes?id=prep%3Aoat-bite-variations&copies=2&format=brief');
 checked('helper brief and CLI share the actual quantity plan',()=>{assert.equal(brief.status,200);assert.match(brief.type,/text\/plain/);assert.equal(brief.text,run(recipe,['corpus','brief',pack,'prep:oat-bite-variations','2']));assert.match(brief.text,/Base batches at declared capacity: 2/);assert.match(brief.text,/Unassigned remainder: 4\/1 tbsp/)});
 checked('helper brief preserves all ten variants and scales their additions',()=>{assert.equal((brief.text.match(/^\d+\. .*\(2 portion\(s\)\)/gm)||[]).length,10);assert.match(brief.text,/Total addition for this variant: Mexican Vanilla = 6\/1 drop/i);assert.match(brief.text,/REVIEW BEFORE PREPARING OR STORING/)});
 const noBrief=await get('/recipes?id=missing&format=brief');checked('a missing record cannot issue a successful helper brief',()=>assert.equal(noBrief.status,422));
 const card=await get('/flow/at?i=4');
 checked('the existing mixed feed contains the same programme',()=>{assert.equal(card.status,200);assert.match(card.text,/One preparation, distinct variations/);assert.match(card.text,/\/recipes\?id=prep%3Aoat-bite-variations/)});
 for(const copies of ['0','-1','101','abc']){const r=await get('/recipes?id=prep%3Aoat-bite-variations&copies='+copies);checked('bad quantity '+copies+' is refused before rendering',()=>assert.equal(r.status,400))}
 const missing=await get('/recipes?id=missing');checked('missing material does not become a successful empty recipe',()=>assert.equal(missing.status,422));
 const search=await get('/recipes?q=oat');checked('full-text selection reaches the same recipe view',()=>assert.match(search.text,/Ten oat-bite/));
 const quote=await get('/recipes?q=%22');checked('malformed search is a controlled refusal',()=>assert.equal(quote.status,422));
 await withChrome(async({open})=>{
  const {page:cdp,go}=await open();
  await go(`http://aurekai.life:${port}/recipes?id=prep%3Aoat-bite-variations`);
  const evaluate=async expression=>{const r=await cdp('Runtime.evaluate',{expression,returnByValue:true});assert(!r.exceptionDetails,JSON.stringify(r.exceptionDetails));return r.result.value};
  let loaded=false;
  for(let i=0;i<150;i++){loaded=await evaluate('document.readyState==="complete"&&!!document.querySelector(".subj-name")');if(loaded)break;await Bun.sleep(50)}
  assert(loaded,await evaluate('location.href+" "+document.body?.textContent.slice(0,500)'));
  const before=await evaluate('({title:document.querySelector(".subj-name")?.textContent,rows:document.querySelectorAll("tbody tr").length,variants:document.querySelectorAll("article.subj ol li").length,styles:[...document.styleSheets].filter(s=>s.href?.endsWith("/assets/matrix.css")).length})');
  checked('Chrome renders the actual native record',()=>{assert.match(before.title,/Ten oat-bite/);assert.equal(before.rows,8);assert.equal(before.variants,10);assert.equal(before.styles,1)});
  await evaluate('document.querySelector("input[name=copies]").value="2";document.querySelector("input[name=copies]").form.requestSubmit();true');
  let changed=false;
  for(let i=0;i<100;i++){try{changed=await evaluate('location.search.includes("copies=2")&&document.body.textContent.includes("arithmetic remainder: 4 tbsp")');if(changed)break}catch{}await Bun.sleep(50)}
  checked('normal browser form navigation recalculates through the native owner',()=>assert(changed));
  await evaluate('[...document.querySelectorAll("a")].find(a=>a.href.includes("format=brief")).click();true');
  let briefLoaded=false;
  for(let i=0;i<100;i++){try{briefLoaded=await evaluate('location.search.includes("format=brief")&&document.body.textContent.includes("PREPARATION BRIEF")&&document.body.textContent.includes("Sets: 2")');if(briefLoaded)break}catch{}await Bun.sleep(50)}
  checked('the helper action opens the real native brief in Chrome',()=>assert(briefLoaded));
  const screenshot=await cdp('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});
  if(process.env.BONFYRE_RECIPE_SCREENSHOT)writeFileSync(process.env.BONFYRE_RECIPE_SCREENSHOT,Buffer.from(screenshot.data,'base64'));
 },{timeoutMs:25000,flags:['--host-resolver-rules=MAP aurekai.life 127.0.0.1','--no-proxy-server']});
 console.log(`${passed} native HTTP/browser checks passed; no native host, personal profile or live estate was changed.`);
} finally {if(server){server.kill();await server.exited}rmSync(temp,{recursive:true,force:true})}
