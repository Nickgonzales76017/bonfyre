import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
const delay=page=>page.waitForTimeout(1600);
const waitDimensions=page=>page.waitForFunction(()=>document.querySelectorAll('.ps-workbench .ps-dimension').length===3);
export async function checkProductStudio(page,check,output){
 await page.waitForSelector('.ps-workbench[data-studio-tools="2"]');await delay(page);
 const studio=page.locator('.ps-workbench');
 check('camera, part and dimension tools are installed in the actual product view',await studio.locator('.ps-stage-tools').isVisible());
 await studio.getByRole('button',{name:'Dimensions',exact:true}).click();await waitDimensions(page);
 check('model dimensions are projected onto the rendered object',await studio.locator('.ps-dimension').count()===3&&await studio.locator('.ps-dimension').first().innerText().then(t=>t.includes('cm')));
 const before=await studio.locator('.ps-viewport').screenshot();
 await studio.getByRole('combobox',{name:'Studio lighting'}).selectOption('evening');await delay(page);
 const evening=await studio.locator('.ps-viewport').screenshot();check('lighting control changes actual rendered pixels',!before.equals(evening));
 await studio.getByRole('combobox',{name:'Studio lighting'}).selectOption('studio');
 await studio.getByRole('button',{name:'Parts',exact:true}).click();
 const part=studio.getByRole('combobox',{name:'Inspect model part'}),count=await part.locator('option').count();check('source model exposes separate mesh parts',count>2);
 await part.selectOption('0');await studio.getByRole('button',{name:'Isolate selected part',exact:true}).click();await delay(page);
 await page.screenshot({path:output+'/part-inspection.png',fullPage:true});
 await studio.getByRole('button',{name:'Show complete product',exact:true}).click();
 const full=await studio.locator('.ps-viewport').screenshot(),slider=studio.getByRole('slider',{name:'Exploded model inspection'});
 await slider.focus();await slider.press('Home');for(let i=0;i<13;i++)await slider.press('ArrowRight');await delay(page);
 const exploded=await studio.locator('.ps-viewport').screenshot();check('exploded inspection moves actual visible parts',!full.equals(exploded)&&await studio.getAttribute('data-explode')==='0.65');
 await page.screenshot({path:output+'/exploded-product.png',fullPage:true});
 await studio.getByRole('button',{name:'Show complete product',exact:true}).click();check('complete product is recoverable after isolation and exploded inspection',await part.inputValue()===''&&await studio.getAttribute('data-explode')==='0');
 await studio.getByRole('button',{name:'Parts',exact:true}).click();
 await studio.getByRole('button',{name:'Expand workspace',exact:true}).click();check('expanded canvas fills the viewport',await studio.boundingBox().then(b=>b.width>1300&&b.height>850));
 await studio.getByRole('button',{name:'Close expanded view',exact:true}).click();
 await page.screenshot({path:output+'/studio-product.png',fullPage:true});
}
async function planState(page){return page.evaluate(async()=>{const config=JSON.parse(document.querySelector('[data-planner-config]').textContent),entry=[...document.scripts].find(s=>s.src.endsWith('/planner-client.mjs')).src,{readLocal}=await import(new URL('store.mjs',entry).href),id=new URL(location.href).searchParams.get('project');return (await readLocal('fixture-plan:'+config.form+':'+id)).find(r=>r.key.endsWith(':'+id))?.value.plan;});}
async function save(page){await page.locator('[data-ui="save"]').click();await page.waitForFunction(()=>document.querySelector('[data-ui="save-state"]').textContent.includes('Saved'));}
export async function checkRoomStudio(page,check,output){
 await page.waitForSelector('.ps-workbench[data-studio-tools="2"][data-models="1"]');await delay(page);
 const studio=page.locator('.ps-workbench'),saved=await planState(page);assert.ok(saved);
 await page.locator('[data-select]').first().click();await studio.getByRole('button',{name:'Focus selection',exact:true}).click();await delay(page);
 const builds=await studio.getAttribute('data-instance-builds');
 await page.locator('[data-select]').last().click();await page.locator('[data-select]').first().click();
 check('changing selection reuses existing model instances',await studio.getAttribute('data-instance-builds')===builds);
 await studio.getByRole('button',{name:'Move fixture',exact:true}).click();await studio.locator('.ps-viewport').focus();await page.keyboard.press('ArrowRight');await save(page);
 const moved=await planState(page);check('helper-backed movement commits a quarter-foot change to the original plan',moved.id===saved.id&&moved.items[0].key===saved.items[0].key&&moved.items[0].x===saved.items[0].x+.25);
 check('movement does not rebuild the source model instances',await studio.getAttribute('data-instance-builds')===builds);
 await studio.locator('.ps-viewport').focus();await page.keyboard.press('Control+z');await save(page);
 const undone=await planState(page);check('existing undo reverses the 3D operation',undone.items[0].x===saved.items[0].x&&undone.items[0].key===saved.items[0].key);
 // A real canvas pointer gesture, not a call to the internal placement function.
 await studio.getByRole('button',{name:'Move fixture',exact:true}).click();await studio.getByRole('button',{name:'Focus selection',exact:true}).click();await delay(page);
 const canvas=studio.locator('.ps-viewport canvas').first();await canvas.scrollIntoViewIfNeeded();const b=await canvas.boundingBox(),cx=b.x+b.width/2,cy=b.y+b.height/2;
 await page.mouse.click(cx,cy);await page.mouse.move(cx,cy);await page.mouse.down();await page.mouse.move(cx+65,cy+28,{steps:12});await page.mouse.up();
 await page.waitForFunction(()=>document.querySelector('[data-ui="save-state"]').textContent.includes('Changes not saved'));await save(page);
 const dragged=await planState(page),changed=dragged.items.filter((item,i)=>item.x!==saved.items[i].x||item.y!==saved.items[i].y);
 check('pointer dragging changes exactly one existing fixture on the quarter-foot grid',changed.length===1&&Number.isInteger(changed[0].x*4)&&Number.isInteger(changed[0].y*4)&&dragged.items.every((item,i)=>item.key===saved.items[i].key&&item.product===saved.items[i].product));
 await studio.locator('.ps-viewport').focus();await page.keyboard.press('Control+z');await save(page);check('pointer placement uses the existing undo history',JSON.stringify((await planState(page)).items)===JSON.stringify(saved.items));
 await studio.getByRole('button',{name:'Dimensions',exact:true}).click();await waitDimensions(page);check('room selection receives model-bound dimension labels',await studio.locator('.ps-dimension').count()===3);
 await studio.getByRole('combobox',{name:'Studio lighting'}).selectOption('daylight');await delay(page);await page.screenshot({path:output+'/studio-room-selection.png',fullPage:true});
 await studio.getByRole('button',{name:'Expand workspace',exact:true}).click();await delay(page);await page.screenshot({path:output+'/studio-room-expanded.png',fullPage:true});
 await studio.getByRole('button',{name:'Close expanded view',exact:true}).click();
 check('inspection and lighting leave saved price inputs and source identity unchanged',JSON.stringify((await planState(page)).items)===JSON.stringify(saved.items)&&(await planState(page)).catalogue===saved.catalogue);
 await page.setViewportSize({width:390,height:844});await delay(page);check('3D room and tools fit a phone-width viewport',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:output+'/studio-room-mobile.png',fullPage:true});await page.setViewportSize({width:1440,height:1050});
 await writeFile(output+'/STUDIO.txt','Helper controls exercised on rendered media and existing saved-plan records.\n');
}
