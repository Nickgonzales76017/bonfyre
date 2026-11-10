import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,rmSync} from 'node:fs';
import {resolve,join,sep} from 'node:path';
import {tmpdir} from 'node:os';
const live=process.argv.includes('--live'),root=resolve(process.env.PLANNER_BUILD??'packs/aurekai-work-samples/site/dist');
const out=resolve(process.env.PLANNER_SPATIAL_PROOF??`packs/no-simulation/product-maturity-20261015/planner/${live?'live-spatial':'local-spatial'}`);mkdirSync(out,{recursive:true});
const profile=mkdtempSync(join(tmpdir(),'planner-projects-'));
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer');
let browser;
const server=live?null:Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
 if(!['GET','HEAD'].includes(request.method))return new Response('No writes',{status:405});
 const url=new URL(request.url);let file=resolve(root,'.'+decodeURIComponent(url.pathname));
 if(file!==root&&!file.startsWith(root+sep))return new Response('Not found',{status:404});
 if(url.pathname.endsWith('/'))file=join(file,'index.html');
 const content=Bun.file(file);return await content.exists()?new Response(content,{headers:{'Cache-Control':'no-store'}}):new Response('Not found',{status:404});
}});
const origin=live?'https://aurekai-work-samples.pages.dev':`http://127.0.0.1:${server.port}`;
const report={origin,checks:[],errors:[],writes:[],status:'running'},check=(name,value)=>{assert(value,name);report.checks.push(name);console.log('PASS '+name);};
const $=key=>`[data-ui="${key}"]`;
try{
 browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:profile});
 const page=await browser.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>report.errors.push(e.message));page.on('request',r=>{if(!['GET','HEAD'].includes(r.method()))report.writes.push({url:r.url(),method:r.method()});});
 const ready=()=>page.waitForSelector('[data-ui=spatial][data-ready=true]');
 const value=name=>page.$eval($(name),n=>n.value);
 const select=async(key,shift=false)=>{if(shift)await page.keyboard.down('Shift');await page.click(`[data-select="${key}"]`);if(shift)await page.keyboard.up('Shift');};
 const plan=async()=>{await page.click($('share'));const url=await value('share-url'),encoded=new URL(url).hash.slice(6);return JSON.parse(Buffer.from(encoded,'base64url').toString());};
 const drag=async(key,dx,dy)=>{
  const box=await page.evaluate(key=>{const host=document.querySelector('[data-ui=spatial]');host.scrollIntoView({block:'center',behavior:'instant'});const stage=Konva.stages.find(s=>s.container()===host),node=stage.findOne('#'+key),rect=node.findOne('.footprint').getClientRect(),b=host.getBoundingClientRect();return{x:b.x+rect.x+rect.width/2,y:b.y+rect.y+rect.height/2,scale:node.getAbsoluteScale().x};},key);
  await page.mouse.move(box.x,box.y);await page.mouse.down();await page.mouse.move(box.x+dx*box.scale,box.y+dy*box.scale,{steps:15});await page.mouse.up();
 };
 await page.setViewport({width:1560,height:1100});await page.goto(origin+'/red3-planner/',{waitUntil:'networkidle2'});await page.waitForSelector('[data-planner][data-ready=true]');await ready();
 check('self-hosted spatial canvas opens with the existing four fixtures',await page.$$('[data-select]').then(a=>a.length===4)&&await page.$eval($('canvas'),n=>n.hidden));
 await page.screenshot({path:join(out,'initial-desktop.png'),fullPage:true});
 await drag('rack-1',1.25,.5);let current=await plan();
 report.dragged=current.items;check('pointer drag snaps and updates the saved proposal coordinates',current.items[0].x===2.25&&current.items[0].y===1.5);
 await page.click($('undo'));current=await plan();check('undo restores the full prior arrangement',current.items[0].x===1&&current.items[0].y===1);
 await page.click($('redo'));current=await plan();check('redo restores exact moved coordinates',current.items[0].x===2.25&&current.items[0].y===1.5);
 await select('rack-1');await select('table-1',true);check('Shift selection is reflected in both list and canvas',await page.$eval($('selected-count'),n=>n.textContent)==='2 selected'&&await page.$$('[data-select][aria-pressed=true]').then(a=>a.length===2));
 await drag('rack-1',1,.75);current=await plan();check('group drag preserves relative fixture placement',current.items[0].x===3.25&&current.items[0].y===2.25&&current.items[1].x===10&&current.items[1].y===1.75);
 await page.click('[data-spatial-action=left]');current=await plan();check('alignment updates both selected fixtures',current.items[0].x===3.25&&current.items[1].x===3.25);
 await page.click('[data-spatial-action=duplicate]');current=await plan();check('duplication creates new fixture identities and exact supplier costs',current.items.length===6&&new Set(current.items.map(i=>i.key)).size===6&&await page.$eval($('subtotal'),n=>n.textContent)==='$1,791.50');
 await page.click('[data-spatial-action=rotate]');current=await plan();check('rotation applies to the selected duplicate group',current.items.slice(4).every(i=>i.rotation===90));
 await page.focus($('spatial'));await page.keyboard.press('ArrowRight');current=await plan();check('keyboard movement retains quarter-foot precision',current.items.slice(4).every(i=>i.x===4));
 await page.click($('zoom-in'));check('zoom uses the same editable scene',await page.$eval($('zoom-level'),n=>n.value)==='125%');await page.click($('fit-layout'));
 await page.click($('pan'));const host=await page.$eval($('spatial'),n=>{const b=n.getBoundingClientRect();return{x:b.x+20,y:b.y+20};});
 await page.mouse.move(host.x,host.y);await page.mouse.down();await page.mouse.move(host.x+35,host.y+25,{steps:5});await page.mouse.up();await page.click($('pan'));await page.click($('fit-layout'));
 check('panning changes only the view',JSON.stringify((await plan()).items)===JSON.stringify(current.items));
 await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-ui=save-state]').textContent==='Saved in this browser');
 await page.reload({waitUntil:'networkidle2'});await ready();const restored=await plan();check('all spatial edits survive reload through the existing record',JSON.stringify(restored.items)===JSON.stringify(current.items));
 await page.goto(origin+'/',{waitUntil:'networkidle2'});await page.goBack({waitUntil:'networkidle2'});await ready();check('browser Back restores the spatial editor and saved proposal',JSON.stringify((await plan()).items)===JSON.stringify(current.items));
 const downloads=join(out,'downloads');mkdirSync(downloads,{recursive:true});const cdp=await page.createCDPSession();await cdp.send('Page.setDownloadBehavior',{behavior:'allow',downloadPath:downloads});await page.click($('layout-image'));
 let png;for(let i=0;i<30;i++){try{png=readFileSync(join(downloads,'fixture-layout.png'));break;}catch{await new Promise(r=>setTimeout(r,100));}}
 check('layout export is a real PNG with doubled canvas resolution',png?.subarray(1,4).toString()==='PNG'&&png.readUInt32BE(16)>500&&png.readUInt32BE(20)>=680);
 await page.focus($('spatial'));await page.keyboard.down('Control');await page.keyboard.press('a');await page.keyboard.up('Control');await page.keyboard.press('Delete');check('deleting every fixture is refused without clearing the plan',await page.$eval($('error'),n=>n.textContent.includes('at least one'))&&(await plan()).items.length===6);
 for(const width of [1440,390,320]){await page.setViewport({width,height:1000});await new Promise(r=>setTimeout(r,350));check(`spatial workspace fits ${width}px`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));if(width!==320)await page.screenshot({path:join(out,`edited-${width}.png`),fullPage:true});}
 await page.setViewport({width:390,height:1000});await select('counter-1');
 const touch=await page.evaluate(()=>{const host=document.querySelector('[data-ui=spatial]');host.scrollIntoView({block:'center',behavior:'instant'});const stage=Konva.stages.find(s=>s.container()===host),node=stage.findOne('#counter-1'),r=node.findOne('.footprint').getClientRect(),b=host.getBoundingClientRect();return{x:b.x+r.x+r.width/2,y:b.y+r.y+r.height/2,scale:node.getAbsoluteScale().x};});
 await cdp.send('Emulation.setTouchEmulationEnabled',{enabled:true,maxTouchPoints:1});await cdp.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:touch.x,y:touch.y,id:1}]});
 for(let i=1;i<=8;i++)await cdp.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x:touch.x+touch.scale*i/8,y:touch.y,id:1}]});
 await cdp.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});await cdp.send('Emulation.setTouchEmulationEnabled',{enabled:false});
 check('touch dragging on a phone updates the same proposal', (await plan()).items.find(i=>i.key==='counter-1').x===11);
 await page.setViewport({width:1440,height:1000});await page.waitForFunction(()=>{const host=document.querySelector('[data-ui=spatial]'),stage=Konva.stages.find(s=>s.container()===host);return stage.width()===Math.max(240,host.clientWidth);});
 const box=await page.evaluate(()=>{const host=document.querySelector('[data-ui=spatial]');host.scrollIntoView({block:'center',behavior:'instant'});const stage=Konva.stages.find(s=>s.container()===host),node=stage.findOne('#rack-1'),world=node.getParent(),pos=world.getAbsolutePosition(),scale=world.getAbsoluteScale().x,b=host.getBoundingClientRect();return{x:b.x+pos.x+.25*scale,y:b.y+pos.y+.25*scale,endX:b.x+pos.x+9*scale,endY:b.y+pos.y+6*scale};});
 await page.mouse.move(box.x,box.y);await page.mouse.down();await page.mouse.move(box.endX,box.endY,{steps:12});await page.mouse.up();
 check('marquee selects the fixtures inside its visible region',await page.$eval($('selected-count'),n=>n.textContent)==='4 selected');
 await page.click($('review'));check('review includes spatial edits and retained supplier identifiers',await page.$eval($('brief'),n=>n.textContent.includes('rotation 90°')&&n.textContent.includes('option 10749')&&n.textContent.includes('depth is missing')));await page.click($('close-review'));
 const recovery=await browser.newPage();await recovery.setRequestInterception(true);let blocked=true;recovery.on('request',request=>blocked&&request.url().includes('/konva.min.js')?request.abort():request.continue());
 await recovery.goto(origin+'/red3-planner/',{waitUntil:'networkidle2'});await recovery.waitForSelector('[data-ui=spatial-retry]:not([hidden])');check('library failure retains the SVG and precise editing controls',await recovery.$eval('[data-ui=canvas]',n=>!n.hidden));
 blocked=false;await recovery.click('[data-ui=spatial-retry]');await recovery.waitForSelector('[data-ui=spatial][data-ready=true]');check('canvas retry restores the existing saved layout',await recovery.$$('[data-select]').then(a=>a.length===6));await recovery.close();
 check('no browser exceptions',report.errors.length===0);check('no remote writes or submissions',report.writes.length===0);report.status='passed';
}catch(error){report.status='failed';report.error=error.stack;process.exitCode=1;try{await (await browser.pages()).at(-1).screenshot({path:join(out,'failure.png'),fullPage:true});}catch{}}
finally{await browser?.close();server?.stop(true);rmSync(profile,{recursive:true,force:true});writeFileSync(join(out,'verification.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));}
