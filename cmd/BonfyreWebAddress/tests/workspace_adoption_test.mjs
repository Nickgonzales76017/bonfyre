// The source browser and native destination are separate isolated origins.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFileSync,mkdirSync,writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {workspaceFixture,root} from './workspace_fixture.mjs';
const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
const sourceModule=readFileSync(join(root,'integrations/browser/extension/store.mjs'));
const source=createServer((req,res)=>{const path=new URL(req.url,'http://localhost').pathname;if(path==='/store.mjs'){res.writeHead(200,{'Content-Type':'text/javascript'});res.end(sourceModule);}else if(path==='/catalogue/'){res.writeHead(200,{'Content-Type':'text/html'});res.end('<!doctype html><html><head><title>Isolated saved source</title></head><body><h1>Original browser work</h1><script type="module" src="/store.mjs"></script></body></html>');}else{res.writeHead(404);res.end();}});
await new Promise(resolve=>source.listen(0,'127.0.0.1',resolve));const sourceURL='http://127.0.0.1:'+source.address().port+'/catalogue/';
const f=await workspaceFixture(),checks=[],errors=[];let browser,page,original;
const check=(label,truth)=>{assert.ok(truth,label);checks.push(label);console.log('PASS '+label);};
const button=async(p,label)=>{const handle=await p.waitForFunction(label=>[...document.querySelectorAll('button')].find(b=>b.textContent.trim()===label&&b.getClientRects().length&&!b.disabled),{},label);await handle.asElement().click();await handle.dispose();};
async function sourceRecords(){return original.evaluate(async()=>{const {readLocal}=await import('/store.mjs');const rows=await readLocal();return rows.map(row=>({key:row.key,revision:row.revision,title:row.value.title,plan:row.value.plan,receipt:row.value.receipt,bytes:row.value.blob?.size}));});}
async function transfer(){
 await button(page,'Bring browser drafts');const opened=browser.waitForTarget(t=>t.url().startsWith(sourceURL)&&t.url().includes('aurekai-transfer='));await button(page,'Open Isolated source');
 const popup=await (await opened).page();popup.setDefaultTimeout(30000);await button(popup,'Show my saved drafts');await popup.waitForSelector('dialog input[type="checkbox"]');
 for(const input of await popup.$$('dialog input[type="checkbox"]'))await input.click();await button(popup,'Send selected work for review');
 await page.waitForFunction(()=>document.querySelector('dialog[aria-label="Adopt browser drafts"] [role="status"]')?.textContent.includes('Source versions'));
 return popup;
}
try{
 const config=await f.install('catalogue','institution:workspace-team'),native=f.store(config);
 await native.saveLocal('draft:catalogue:conflict',{title:'Destination original'},0);
 browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox']});
 original=await browser.newPage();await original.goto(sourceURL);
 await original.evaluate(async()=>{const {saveLocal}=await import('/store.mjs');await saveLocal('draft:catalogue:conflict',{title:'Source original'},0);await saveLocal('fixture-plan:adoption',{plan:{id:'prior-plan',title:'Original plan',catalogue:'source-coordinate',items:[{key:'stable-fixture',product:'W002337865',x:2.25,y:1,rotation:0}]},receipt:{reference:'historical-source-only'}},0);await saveLocal('product-model:adoption:body',{blob:new File([new Uint8Array(300000).fill(73)],'source-model.glb',{type:'model/gltf-binary',lastModified:1000})},0);});
 const before=await sourceRecords();page=await browser.newPage();page.setDefaultTimeout(30000);page.on('pageerror',e=>errors.push(e.message));
 const [name,...value]=f.owner.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.goto(f.base+config.path);await page.waitForSelector('[data-adopt-browser]');
 // The production allowlisted source list is replaced only in this isolated test.
 await page.evaluate(async(sourceURL)=>{document.querySelector('[data-adopt-browser]').remove();const base=document.querySelector('meta[name="bonfyre-workspace-store"]').content.replace('_records','bridge/');const {installWorkspaceAdoption}=await import(base+'workspace-adoption.mjs');installWorkspaceAdoption({sources:[['Isolated source',sourceURL]]});},sourceURL);
 await transfer();
 check('source confirmation alone cannot change destination drafts',(await native.readLocal()).length===1);
 check('a conflicting destination draft is not selected implicitly',await page.$eval('dialog input[value="draft:catalogue:conflict"]',input=>!input.checked));
 check('new source drafts are offered with their exact identities',await page.$eval('dialog input[value="fixture-plan:adoption"]',input=>input.checked));
 await button(page,'Adopt selected drafts');await page.waitForFunction(()=>document.querySelector('dialog[aria-label="Adopt browser drafts"] [role="status"]')?.textContent.includes('2 drafts adopted'));
 const adopted=(await native.readLocal('fixture-plan:'))[0];
 check('the selected source plan retains every fixture and source coordinate',adopted.value.plan.id==='prior-plan'&&adopted.value.plan.items[0].key==='stable-fixture'&&adopted.value.plan.items[0].x===2.25&&adopted.value.plan.catalogue==='source-coordinate');
 check('a source submission receipt does not mark the recipient draft as sent',adopted.value.receipt===null);
 check('an unselected destination conflict stays unchanged',(await native.readLocal('draft:catalogue:conflict'))[0].value.title==='Destination original');
 const model=(await native.readLocal('product-model:'))[0].value.blob;
 check('explicitly selected model files retain their bytes and file identity',model instanceof File&&model.name==='source-model.glb'&&model.size===300000&&model.lastModified===1000&&new Uint8Array(await model.arrayBuffer()).every(b=>b===73));
 assert.deepEqual(await sourceRecords(),before);check('source browser records and their revisions were not modified',true);
 await button(page,'Close');await transfer();
 check('identical adopted work is recognized instead of duplicated',await page.$eval('dialog input[value="fixture-plan:adoption"]',input=>input.disabled));
 await page.click('dialog input[value="draft:catalogue:conflict"]');
 await native.saveLocal('draft:catalogue:conflict',{title:'Concurrent member edit'},1);
 await button(page,'Adopt selected drafts');await page.waitForFunction(()=>document.querySelector('dialog[aria-label="Adopt browser drafts"] [role="status"]')?.textContent.includes('another tab'));
 check('a stale adoption review cannot overwrite a concurrent member',(await native.readLocal('draft:catalogue:conflict'))[0].value.title==='Concurrent member edit');
 await button(page,'Close');await transfer();await page.click('dialog input[value="draft:catalogue:conflict"]');await button(page,'Adopt selected drafts');await page.waitForFunction(()=>document.querySelector('dialog[aria-label="Adopt browser drafts"] [role="status"]')?.textContent.includes('1 drafts adopted'));
 check('an explicitly re-reviewed conflict can be adopted into that destination',(await native.readLocal('draft:catalogue:conflict'))[0].value.title==='Source original');
 assert.deepEqual(await sourceRecords(),before);check('successful replacement still preserves original source work',true);
 check('the two-origin consent and adoption journey has no script exceptions',errors.length===0);
 const out=join(root,'cmd/BonfyreExperienceStage/verification/workspace-adoption-20261015');mkdirSync(out,{recursive:true});await page.screenshot({path:join(out,'adoption.png'),fullPage:true});writeFileSync(join(out,'RESULTS.txt'),'WORKSPACE_ADOPTION_PASS\t'+checks.length+'\nrelease\t'+f.release+'\n'+checks.map(c=>'PASS\t'+c+'\n').join(''));
 console.log('WORKSPACE_ADOPTION_PASS\t'+checks.length);
}catch(error){console.error('Adoption browser errors',errors);for(const p of await browser?.pages()??[])console.error(new URL(p.url()).pathname,await p.$$eval('dialog [role="status"]',nodes=>nodes.map(n=>n.textContent)));throw error;}finally{await browser?.close();await f.close();await new Promise(resolve=>source.close(resolve));}
