// Drive the same catalogue-to-room journey on the build and its public URL.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {resolve,extname} from 'node:path';
import {checkProductStudio,checkRoomStudio} from './spaces-studio-checks.mjs';
const {chromium}=await import(process.env.PLAYWRIGHT_MODULE||'playwright');
const output=resolve(process.env.SPACES_EVIDENCE||'spaces-evidence');await mkdir(output,{recursive:true});
let server,origin=process.env.SPACES_ORIGIN;
if(!origin){const root=resolve('site');server=createServer(async(req,res)=>{try{let path=new URL(req.url,'http://localhost').pathname;if(!path.startsWith('/bonfyre/'))throw Error();path=path.slice('/bonfyre'.length);const file=resolve(root,'.'+path+(path.endsWith('/')?'index.html':''));if(!file.startsWith(root+'/'))throw Error();const body=await readFile(file),types={'.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.html':'text/html','.glb':'model/gltf-binary','.wasm':'application/wasm','.png':'image/png'};res.writeHead(200,{'Content-Type':types[extname(file)]||'application/octet-stream'});res.end(body);}catch{res.writeHead(404);res.end('Not found');}});await new Promise(r=>server.listen(0,'127.0.0.1',r));origin='http://127.0.0.1:'+server.address().port;}
const browser=await chromium.launch({executablePath:process.env.CHROME_PATH||'/usr/bin/google-chrome',headless:true,args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
const checks=[],errors=[],requests=[],posts=[],check=(name,truth)=>{assert.ok(truth,name);checks.push(name);console.log('PASS '+name);};let page;
try{page=await browser.newPage({viewport:{width:1440,height:1050}});page.setDefaultTimeout(30000);page.on('pageerror',e=>errors.push(e.message));page.on('response',r=>{if(r.status()>=400)requests.push(r.status()+' '+r.url());});page.on('request',r=>{if(r.method()==='POST')posts.push(r.url());});page.on('console',m=>{if(m.type()==='error'&&/Shader Error|VALIDATE_STATUS|WebGLProgram/.test(m.text()))errors.push(m.text());});
 await page.goto(origin+'/bonfyre/spaces/catalogue/?store=wayfair-reference&workspace=inventory');
 await page.waitForSelector('[data-ui="inventory"][data-ready="true"]');
 // AG Grid paints row content after its host has become ready.
 const lamp=page.locator('.ag-row [col-id="title"]').filter({hasText:'Bonsall'}).first();await lamp.waitFor({state:'visible'});
 check('hosted catalogue contains the retained lamp',await lamp.innerText().then(s=>s.includes('Bonsall')));
 await lamp.click();await page.locator('[data-grid-action="inspect-3d"]').click();await page.waitForSelector('.cr-dialog[data-ready="true"]');
 check('actual model renderer opens from the catalogue grid',await page.locator('.cr-dialog canvas').count()>0);
 if(process.env.SPACES_STUDIO_REQUIRED)await checkProductStudio(page,check,output);
 await page.getByRole('spinbutton',{name:'Quantity to place'}).fill('2');await page.getByRole('spinbutton',{name:'Quantity to place'}).dispatchEvent('input');
 check('two retained units cost 139.98',await page.locator('.cr-status').innerText().then(s=>s.includes('139.98')));
 await page.screenshot({path:output+'/catalogue.png',fullPage:true});
 if(server&&!process.env.SPACES_STUDIO_REQUIRED){await page.locator('.cr-dialog .ps-viewport').screenshot({path:resolve('site/spaces/assets/product-models/lamp-thumbnail.png')});}
 await page.getByRole('button',{name:'Place in a room',exact:true}).click();await page.waitForSelector('[data-planner][data-ready="true"]');
 check('same saved work opens on the public room route',page.url().includes('/bonfyre/spaces/product-room/')&&page.url().includes('project='));
 check('room subtotal uses retained option cost',await page.locator('[data-ui="subtotal"]').innerText().then(s=>s.includes('139.98')));
 await page.waitForSelector('.ps-viewport canvas');await page.waitForTimeout(1500);await page.screenshot({path:output+'/room.png',fullPage:true});
 if(process.env.SPACES_STUDIO_REQUIRED)await checkRoomStudio(page,check,output);
 const title='Published browser check';await page.locator('[data-ui="title"]').fill(title);await page.locator('[data-ui="title"]').dispatchEvent('input');await page.locator('[data-ui="save"]').click();await page.waitForFunction(()=>document.querySelector('[data-ui="save-state"]')?.textContent.includes('Saved'));
 await page.reload();await page.waitForSelector('[data-planner][data-ready="true"]');check('saved plan survives a hosted reload',await page.locator('[data-ui="title"]').inputValue()===title);
 await page.getByRole('button',{name:'2D plan',exact:true}).click();check('2D editor remains usable',await page.locator('.fp-board').isVisible());
 await page.locator('[data-ui="review"]').click();check('existing review retains the exact supplier SKU',await page.locator('[data-ui="brief"]').innerText().then(s=>s.includes('W002337865')&&s.includes('139.98')));
 check('disconnected public release does not collect contact details',await page.locator('[data-ui="submit-form"]').isHidden());await page.locator('[data-ui="close-review"]').click();
 await page.setViewportSize({width:390,height:844});check('room fits 390px without document overflow',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:output+'/room-mobile.png',fullPage:true});
 check('configuration and review preparation send no POST requests',posts.length===0);
 if(!server){
  await page.setViewportSize({width:1440,height:1050});await page.goto(origin+'/bonfyre/#experiences');
  await page.waitForFunction(()=>document.querySelectorAll('[data-demo-list] a[data-demo][href*="/bonfyre/spaces/"]').length===2);
  check('both existing public showcase entries open the new release',await page.locator('[data-demo-list] a[data-demo][href*="/bonfyre/spaces/"]').count()===2);
  const captures=page.locator('#demo-showcase img[src*="/spaces/shots/"]');
  assert.equal(await captures.count(),2,'Both authored studio captures must reach the carousel.');
  // These images are intentionally lazy-loaded in a horizontal carousel.
  // Scroll the same visible cards a visitor uses; do not force eager loading.
  for(let i=0;i<2;i++){
   const image=captures.nth(i);await image.scrollIntoViewIfNeeded();
   const src=await image.getAttribute('src');
   await page.waitForFunction(src=>[...document.querySelectorAll('#demo-showcase img')].some(i=>i.src===src&&i.complete&&i.naturalWidth>0),src);
  }
  check('showcase uses two loaded captures of the implemented studio',await captures.evaluateAll(images=>images.length===2&&images.every(i=>i.complete&&i.naturalWidth>0)));
  await page.locator('#experiences').screenshot({path:output+'/showcase.png'});
 }
 check('no application script exceptions',errors.length===0);await writeFile(output+'/RESULTS.txt','origin '+origin+'\nchecks '+checks.length+'\n'+checks.map(s=>'PASS '+s).join('\n')+'\n');
}catch(error){console.error('Browser diagnostics',errors,requests);await writeFile(output+'/FAILURE.txt',String(error)+'\n'+errors.join('\n')+'\n'+requests.join('\n'));if(page){await writeFile(output+'/failure.html',await page.content());await page.screenshot({path:output+'/failure.png',fullPage:true}).catch(()=>{});}throw error;
}finally{await browser.close();server?.close();}
