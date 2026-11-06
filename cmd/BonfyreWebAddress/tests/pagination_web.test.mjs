import assert from 'node:assert/strict';
import { Database } from 'bun:sqlite';
import { mkdtempSync,readFileSync,writeFileSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { withChrome } from '../../../integrations/browser/capture/chrome-session.mjs';
const root=fileURLToPath(new URL('../../../',import.meta.url));
const compress=join(root,'cmd/BonfyreCompress/bonfyre-compress'),recipe=join(root,'cmd/BonfyreRecipe/build/bonfyre-recipe');
const web=join(root,'cmd/BonfyreWebAddress/bonfyre-web-address'),temp=mkdtempSync(join(tmpdir(),'bf-source-pagination-'));
let server,passed=0;
const check=(name,fn)=>{fn();passed++;console.log('PASS '+name)};
const run=(bin,args)=>{const r=spawnSync(bin,args,{encoding:'utf8',timeout:20000,maxBuffer:2000000});assert.equal(r.status,0,r.stderr);return r.stdout;};
try {
 const rows=Array.from({length:127},(_,i)=>({recipe_id:`source-${i}`,title:`Retained preparation ${i}`,schema:'bonfyre-household-recipe.v1',ingredients:[],instructions:'Source fixture: no cooking performed.'}));
 const source=join(temp,'source.jsonl'),pack=join(temp,'source.db'),matrix=join(temp,'matrix.db');
 const raw=rows.map(r=>JSON.stringify(r)).join('\n')+'\n';writeFileSync(source,raw);
 run(compress,['factor-records',source,pack]);
 check('Compress output is read exactly by the Recipe owner',()=>assert.equal(run(recipe,['corpus','export',pack]),raw));
 run(web,['bootstrap-owned','--db',matrix]);
 const reserve=Bun.serve({hostname:'127.0.0.1',port:0,fetch(){return new Response('reserved')}}),port=reserve.port;reserve.stop(true);
 server=Bun.spawn([web,'serve','--db',matrix,'--auth-db',join(temp,'auth.db'),'--port',String(port),'--site-root',join(root,'site/matrix')],{cwd:root,env:{...process.env,BONFYRE_RECIPE_MATERIAL:pack,BONFYRE_CMS_DB:join(temp,'cms.db'),BONFYRE_FOREIGN_ROOT:join(temp,'foreign'),BONFYRE_FOREIGN_RECEIPTS:join(temp,'foreign/receipts'),BONFYRE_SUBJECT_FABRIC_DB:join(temp,'fabric.db'),BONFYRE_PUBLIC_PROJECTION_CACHE_DB:''},stdout:'ignore',stderr:'ignore'});
 const get=async path=>{const r=await fetch(`http://127.0.0.1:${port}${path}`,{headers:{Host:'aurekai.life',Accept:'text/html'}});return{status:r.status,text:await r.text()}};
 let ready=false;for(let i=0;i<150;i++){try{if((await get('/healthz')).status===200){ready=true;break}}catch{}if(server.exitCode!==null)break;await Bun.sleep(50)}assert(ready);
 const keys=s=>[...s.matchAll(/href="\/recipes\?id=([^"]+)"/g)].map(m=>decodeURIComponent(m[1]));
 const all=[];
 for(const [offset,expected]of [[0,60],[60,60],[120,7]]){const page=await get(`/recipes?offset=${offset}`);check(`page at ${offset} returns the next ${expected} verified records`,()=>{assert.equal(page.status,200);assert.equal(keys(page.text).length,expected);all.push(...keys(page.text));assert.equal(page.text.includes('More recipes'),offset<120)})}
 check('paging covers every source once without loss or overlap',()=>assert.deepEqual(all,rows.map(r=>r.recipe_id)));
 const query=await get('/recipes?q=Retained&offset=60');check('search and pagination use the same record identities',()=>assert.deepEqual(keys(query.text),rows.slice(60,120).map(r=>r.recipe_id)));
 for(const offset of ['-1','1000001','abc','1oops']){const page=await get(`/recipes?offset=${offset}`);check('offset '+offset+' is refused',()=>assert.equal(page.status,400))}
 const end=await get('/recipes?offset=127');check('end of collection has no fabricated next page',()=>{assert.equal(end.status,200);assert.deepEqual(keys(end.text),[]);assert(!end.text.includes('More recipes'))});
 const d=new Database(pack);d.exec("UPDATE record SET title='invented display' WHERE seq=1");d.close();
 const bad=await get('/recipes');check('collection verifies source bytes before trusting display metadata',()=>{assert.equal(bad.status,422);assert(!bad.text.includes('invented display'))});
 const restore=new Database(pack);restore.query('UPDATE record SET title=? WHERE seq=1').run(rows[0].title);restore.close();
 await withChrome(async({open})=>{
   const {page,go}=await open();await go(`http://aurekai.life:${port}/recipes`);
   const evaluate=async expression=>{const r=await page('Runtime.evaluate',{expression,returnByValue:true});assert(!r.exceptionDetails);return r.result.value};
   let loaded=false;for(let i=0;i<150;i++){try{loaded=await evaluate('[...document.querySelectorAll("a")].some(a=>a.textContent==="More recipes")');if(loaded)break}catch{}await Bun.sleep(50)}assert(loaded);
   await evaluate('[...document.querySelectorAll("a")].find(a=>a.textContent==="More recipes").click();true');
   let moved=false;for(let i=0;i<150;i++){try{moved=await evaluate('location.search.includes("offset=60")&&!!document.querySelector("a[href=\\"/recipes?id=source-60\\"]")');if(moved)break}catch{}await Bun.sleep(50)}
   check('ordinary browser navigation reaches the next native page',()=>assert(moved));
 },{timeoutMs:25000,flags:['--host-resolver-rules=MAP aurekai.life 127.0.0.1','--no-proxy-server']});
 check('source input remains unchanged',()=>assert.equal(readFileSync(source,'utf8'),raw));
 console.log(`${passed} source-to-native-page checks passed.`);
}finally{if(server){server.kill();await server.exited}rmSync(temp,{recursive:true,force:true})}
