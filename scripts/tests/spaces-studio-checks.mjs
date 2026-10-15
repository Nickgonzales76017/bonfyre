import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
const delay=page=>page.waitForTimeout(1600);
export async function checkProductStudio(page,check,output){
 await page.waitForSelector('.ps-workbench[data-studio-tools="2"]');await delay(page);
 const studio=page.locator('.ps-workbench');
 check('camera, part and dimension tools are installed in the actual product view',await studio.locator('.ps-stage-tools').isVisible());
 await studio.getByRole('button',{name:'Dimensions',exact:true}).click();
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
 const full=await studio.locator('.ps-viewport').screenshot();
 await studio.getByRole('slider',{name:'Exploded model inspection'}).fill('0.65');await studio.getByRole('slider',{name:'Exploded model inspection'}).dispatchEvent('input');await delay(page);
 const exploded=await studio.locator('.ps-viewport').screenshot();check('exploded inspection moves actual visible parts',!full.equals(exploded)&&await studio.getAttribute('data-explode')==='0.65');
 await studio.getByRole('button',{name:'Show complete product',exact:true}).click();check('complete product is recoverable after isolation and exploded inspection',await part.inputValue()===''&&await studio.getAttribute('data-explode')==='0');
 await studio.getByRole('button',{name:'Parts',exact:true}).click();
 await studio.getByRole('button',{name:'Expand workspace',exact:true}).click();check('expanded canvas fills the viewport',await studio.boundingBox().then(b=>b.width>1300&&b.height>850));
 await studio.getByRole('button',{name:'Close expanded view',exact:true}).click();
 await page.screenshot({path:output+'/studio-product.png',fullPage:true});
}
async function planState(page){return page.evaluate(async()=>{const config=JSON.parse(document.querySelector('[data-planner-config]').textContent),entry=[...document.scripts].find(s=>s.src.endsWith('/planner-client.mjs')).src,{readLocal}=await import(new URL('store.mjs',entry).href),id=new URL(location.href).searchParams.get('project');return (await readLocal('fixture-plan:'+config.form+':'+id)).find(r=>r.key.endsWith(':'+id))?.value.plan;});}
export async function checkRoomStudio(page,check,output){
 await page.waitForSelector('.ps-workbench[data-studio-tools="2"][data-models="1"]');await delay(page);
 const studio=page.locator('.ps-workbench'),saved=await planState(page);assert.ok(saved);
 await page.locator('[data-select]').first().click();await studio.getByRole('button',{name:'Focus selection',exact:true}).click();await delay(page);
 const builds=await studio.getAttribute('data-instance-builds');
 await page.locator('[data-select]').last().click();await page.locator('[data-select]').first().click();
 check('changing selection reuses existing model instances',await studio.getAttribute('data-instance-builds')===builds);
 await studio.getByRole('button',{name:'Move fixture',exact:true}).click();await studio.locator('.ps-viewport').focus();await page.keyboard.press('ArrowRight');
 await page.locator('[data-ui="save"]').click();await page.waitForFunction(()=>document.querySelector('[data-ui="save-state"]').textContent.includes('Saved'));
 const moved=await planState(page);check('helper-backed movement commits a quarter-foot change to the original plan',moved.id===saved.id&&moved.items[0].key===saved.items[0].key&&moved.items[0].x===saved.items[0].x+.25);
 check('movement does not rebuild the source model instances',await studio.getAttribute('data-instance-builds')===builds);
 await page.locator('[data-ui="undo"]').click();await page.locator('[data-ui="save"]').click();await page.waitForFunction(()=>document.querySelector('[data-ui="save-state"]').textContent.includes('Saved'));
 const undone=await planState(page);check('existing undo reverses the 3D operation',undone.items[0].x===saved.items[0].x&&undone.items[0].key===saved.items[0].key);
 await studio.getByRole('button',{name:'Dimensions',exact:true}).click();check('room selection receives model-bound dimension labels',await studio.locator('.ps-dimension').count()===3);
 await studio.getByRole('combobox',{name:'Studio lighting'}).selectOption('daylight');await delay(page);await page.screenshot({path:output+'/studio-room-selection.png',fullPage:true});
 await studio.getByRole('button',{name:'Expand workspace',exact:true}).click();await delay(page);await page.screenshot({path:output+'/studio-room-expanded.png',fullPage:true});
 await studio.getByRole('button',{name:'Close expanded view',exact:true}).click();
 check('inspection and lighting leave saved price inputs and source identity unchanged',JSON.stringify((await planState(page)).items)===JSON.stringify(saved.items)&&(await planState(page)).catalogue===saved.catalogue);
 await writeFile(output+'/STUDIO.txt','Helper controls exercised on rendered media and existing saved-plan records.\n');
}
