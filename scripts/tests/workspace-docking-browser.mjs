import assert from 'node:assert/strict';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createServer} from 'node:http';
import {resolve,extname} from 'node:path';
const {chromium}=await import(process.env.PLAYWRIGHT_MODULE||'playwright');
const root=resolve('site'),out=resolve(process.env.WORKSPACE_EVIDENCE||'workspace-evidence');await mkdir(out,{recursive:true});
const server=createServer(async(req,res)=>{try{const path=decodeURIComponent(new URL(req.url,'http://localhost').pathname);if(req.method!=='GET'||!path.startsWith('/bonfyre/'))throw Error();const file=resolve(root,'.'+path.slice(8)+(path.endsWith('/')?'index.html':''));if(!file.startsWith(root+'/'))throw Error();const bytes=await readFile(file);res.writeHead(200,{'Content-Type':({'.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.html':'text/html','.wasm':'application/wasm','.glb':'model/gltf-binary'})[extname(file)]||'application/octet-stream'});res.end(bytes);}catch{res.writeHead(404);res.end('Not found');}});await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin=process.env.WORKSPACE_ORIGIN||'http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true,executablePath:process.env.CHROME_PATH,args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});let page;const checks=[];
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
try{
 page=await browser.newPage({viewport:{width:1560,height:1100}});page.setDefaultTimeout(15000);await page.goto(origin+'/bonfyre/spaces/product-room/');await page.waitForSelector('[data-planner][data-ready=true] .bf-workspace[data-ready=true]');
 await page.locator('[data-ui=notes]').fill('Keep this exact unsaved field while arranging panels.');await page.evaluate(()=>window.retainedNotes=document.querySelector('[data-ui=notes]'));
 const details=page.getByRole('tab',{name:'Space & project',exact:true}),canvas=page.getByRole('tab',{name:'Room canvas',exact:true});
 await details.dragTo(canvas);
 await page.waitForFunction(()=>document.querySelector('[data-tab-panel-id=details]').getAttribute('aria-controls')===document.querySelector('[data-tab-panel-id=canvas]').getAttribute('aria-controls'));
 check('pointer dragging moves the real project editor into the canvas tab group',true);
 check('docking retains the original input and its unsaved content',await page.evaluate(()=>retainedNotes===document.querySelector('[data-ui=notes]')&&retainedNotes.value==='Keep this exact unsaved field while arranging panels.'));
 await page.getByRole('button',{name:'Close Space & project',exact:true}).click();await page.waitForSelector('[data-tab-panel-id=details]',{state:'detached'});
 check('closing a pane parks its original editor without discarding values',await page.evaluate(()=>retainedNotes.isConnected&&retainedNotes.closest('.bf-workspace-parking')!==null));
 await page.getByRole('combobox',{name:'Open workspace panel',exact:true}).selectOption('details');await page.locator('[data-ui=notes]').waitFor({state:'visible'});
 check('reopening a closed pane restores that same unsaved editor',await page.evaluate(()=>retainedNotes===document.querySelector('[data-ui=notes]')&&retainedNotes.value==='Keep this exact unsaved field while arranging panels.'));
 await page.getByRole('combobox',{name:'Workspace arrangement',exact:true}).selectOption('design');await page.waitForTimeout(200);
 const before=await page.locator('.bf-workspace-content[data-binding=canvas]').boundingBox();
 const sashes=page.locator('.bf-workspace .dv-sash');let handle;
 for(let i=0;i<await sashes.count();i++){const box=await sashes.nth(i).boundingBox();if(box&&box.height>100&&box.width<20){handle=box;break;}}
 assert.ok(handle,'A real split handle is available');await page.mouse.move(handle.x+handle.width/2,handle.y+80);await page.mouse.down();await page.mouse.move(handle.x+handle.width/2-100,handle.y+80,{steps:12});await page.mouse.up();
 await page.waitForFunction(w=>Math.abs(document.querySelector('.bf-workspace-content[data-binding=canvas]').getBoundingClientRect().width-w)>50,before.width);
 check('pointer resizing reallocates the live canvas and inspector panes',true);
 await page.locator('[data-ui=save]').click();await page.waitForFunction(()=>document.querySelector('[data-ui=save-state]').textContent.includes('Saved'));await page.reload();await page.waitForSelector('[data-planner][data-ready=true] .bf-workspace[data-ready=true]');
 check('docked edits still save and reopen through the original project store',await page.locator('[data-ui=notes]').inputValue()==='Keep this exact unsaved field while arranging panels.');
 await writeFile(out+'/DOCKING.txt',checks.map(c=>'PASS '+c).join('\n')+'\n');
}catch(error){await writeFile(out+'/DOCKING-FAILURE.txt',error.stack);await page?.screenshot({path:out+'/docking-failure.png',fullPage:true}).catch(()=>{});throw error;}finally{await browser.close();server.close();}
