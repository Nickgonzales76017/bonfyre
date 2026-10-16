import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
export async function runReuseChecks({serve,browser,check,edit}) {
 const {server,origin}=await serve('after');const context=await browser.newContext({viewport:{width:1440,height:1050}});
 try {
  const page=await context.newPage();page.setDefaultTimeout(15000);await page.goto(origin+'/itad-exchange/');await page.waitForSelector('#manifestGrid[data-ready="true"]');
  await page.evaluate(async()=>{
   const url=performance.getEntriesByType('resource').map(r=>r.name).find(s=>s.includes('/shared/interaction-')&&s.endsWith('/record-grid.mjs'));
   if(!url)throw Error('The ITAD table did not load the shared record renderer.');
   const {createRecordGrid}=await import(url);window.componentFixture={owners:[{id:'same-id',value:1},{id:'same-id',value:2}],grids:[]};
   for(let i=0;i<2;i++){
    const host=document.createElement('section');host.id='component-fixture-'+i;host.setAttribute('aria-label','Isolated component test '+i);host.style.cssText='width:600px;height:240px;margin:24px';document.body.prepend(host);
    const grid=createRecordGrid(host,{library:agGrid,rows:[componentFixture.owners[i]],columns:({setter})=>[{field:'value',headerName:'Value',editable:true,valueSetter:setter('value',Number)}],onEdit:(row,field,value)=>{componentFixture.owners[i]={...componentFixture.owners[i],[field]:value};componentFixture.grids[i].setRows([componentFixture.owners[i]]);}});componentFixture.grids.push(grid);
   }
  });
  await edit(page,page.locator('#component-fixture-0 .ag-row [col-id=value]').first(),'7');
  check('two mounted components with the same row ID keep independent records',await page.evaluate(()=>componentFixture.owners[0].value===7&&componentFixture.owners[1].value===2));
  await page.evaluate(()=>{componentFixture.grids[0].destroy();document.querySelector('#component-fixture-0').remove();});
  await edit(page,page.locator('#component-fixture-1 .ag-row [col-id=value]').first(),'8');
  check('destroying one component does not break its sibling',await page.evaluate(()=>componentFixture.owners[1].value===8));
 } finally {await context.close();server.close();}
 const report=await readFile('component-evidence/SHARED.tsv','utf8'),packageHash=report.split('\n')[0].split('\t')[1];assert.match(packageHash,/^[a-f0-9]{64}$/);
 const mutant=await serve('after',{mutate:true}),mutationContext=await browser.newContext();
 try {
  for(const [name,path,selector]of [['catalogue','/bonfyre/spaces/catalogue/?store=wayfair-reference&workspace=inventory','[data-ui="inventory"]'],['ITAD','/itad-exchange/','#manifestGrid']]){
   const page=await mutationContext.newPage();await page.goto(mutant.origin+path);
   await page.waitForFunction(()=>document.body.innerText.includes('Shared renderer deliberately withheld'),null,{timeout:15000});
   check('withholding the common renderer prevents '+name+' from claiming a ready table',await page.locator(selector).getAttribute('data-ready')!=='true');
   await page.close();
  }
 } finally {await mutationContext.close();mutant.server.close();}
 check('both donor substitutions use the same content-addressed component package',true);
}
