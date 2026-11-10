import {test} from 'node:test';
import assert from 'node:assert/strict';
import {runInNewContext} from 'node:vm';
import {readFileSync,mkdtempSync,mkdirSync,copyFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {capturePage} from '../extension/capture.mjs';
import {observe,cmsProjection,heldWorkEnvelope,compareObservations} from '../extension/observation.mjs';
const catalog=JSON.parse(readFileSync(new URL('../extension/catalog.json',import.meta.url)));
const id='7350123456789012345',url='https://www.tiktok.com/@fixture/video/'+id;
const item=()=>({id,author:{uniqueId:'fixture',id:'private-unneeded-id'},desc:'Ten variations from one base',createTime:1710000000,video:{duration:42,downloadAddr:'https://media.test/?token=DO_NOT_KEEP'},stats:{playCount:37},secret:false,forFriend:false});
const payload=x=>({__DEFAULT_SCOPE__:{'webapp.video-detail':{statusCode:0,itemInfo:{itemStruct:x}},session:{token:'DO_NOT_KEEP'}}});
function capture(data,{legacy=false,address=url,structured=[]}={}){
 const context={URL,location:new URL(address),window:{getSelection:()=>''},getComputedStyle:()=>({visibility:'visible'}),document:{title:'Source fixture',documentElement:{dataset:{}},activeElement:null,querySelector:()=>null,querySelectorAll:s=>s==='script[type="application/ld+json"]'?structured.map(v=>({textContent:JSON.stringify(v)})):[],getElementById:n=>n===(legacy?'SIGI_STATE':'__UNIVERSAL_DATA_FOR_REHYDRATION__')?{textContent:typeof data==='string'?data:JSON.stringify(data)}:null}};
 Object.defineProperty(context.document,'cookie',{get(){throw Error('cookies must not be read')}});
 return JSON.parse(JSON.stringify(runInNewContext('('+capturePage.toString()+')()',context)));
}
const recipe=()=>({'@type':'Recipe',name:'Source oat bites',recipeIngredient:['1 1/2 cups oats','salt to taste'],recipeYield:'10 portions',recipeInstructions:[{'@type':'HowToSection',itemListElement:[{'@type':'HowToStep',text:'Mix the declared ingredients.'}]}],nutrition:{'@type':'NutritionInformation',servingSize:'one portion',proteinContent:'7 g',calories:'140 kcal'}});
const recipeRaw=()=>({url:'https://recipes.example/oats',title:'Source oat bites',captured_at:'2026-10-04T00:00:00Z',structured:[recipe()],term_snippets:[]});
test('current TikTok hydration preserves long post identity without rounding',()=>{const c=capture(payload(item()));assert.equal(c.platform_post.id,id);assert.equal(c.platform_post.author,'fixture');assert.equal(c.platform_post.duration_seconds,'42')});
test('legacy ItemModule supplies the same current-post fields',()=>{const x=item();x.author='fixture';assert.equal(capture({ItemModule:{[id]:x}},{legacy:true}).platform_post.description,x.desc)});
test('unrelated hydration, download tokens and account identifiers never leave capture',()=>{const c=JSON.stringify(capture(payload(item())));assert(!c.includes('DO_NOT_KEEP'));assert(!c.includes('private-unneeded-id'))});
test('another post cannot be substituted for the selected URL',()=>{const x=item();x.id='42';assert.equal(capture(payload(x)).platform_post.status,'identity_mismatch')});
test('another creator cannot be substituted for the current creator',()=>{const x=item();x.author.uniqueId='other';assert.equal(capture(payload(x)).platform_post.status,'author_mismatch')});
test('explicitly restricted metadata is not published as an observation',()=>{const x=item();x.forFriend=true;const p=capture(payload(x)).platform_post;assert.equal(p.status,'restricted');assert.equal(p.description,undefined)});
test('lookalike hostname does not trigger platform capture',()=>assert.equal(capture(payload(item()),{address:'https://www.tiktok.com.evil.test/@fixture/video/'+id}).platform_post,null));
test('invalid and oversized hydration has explicit status',()=>{assert.equal(capture('{').platform_post.status,'invalid_payload');assert.equal(capture(' '.repeat(1048577)).platform_post.status,'too_large')});
test('recipe ingredient wording, nested instructions and nutrient serving basis survive',async()=>{const o=await observe(recipeRaw(),catalog);assert.deepEqual(o.recipes[0].ingredients,recipe().recipeIngredient);assert.equal(o.recipes[0].instructions[0],'Mix the declared ingredients.');assert.equal(o.recipes[0].nutrition[0].basis,'one portion');assert.equal(o.offers.length,0)});
test('missing nutrient basis is not synthesized from recipe yield',async()=>{const r=recipeRaw();delete r.structured[0].nutrition.servingSize;assert((await observe(r,catalog)).recipes[0].nutrition.every(c=>c.basis==='not_stated'))});
test('boolean nutrition values are not converted into quantities',async()=>{const r=recipeRaw();r.structured[0].nutrition.proteinContent=true;assert(!(await observe(r,catalog)).recipes[0].nutrition.some(c=>c.field==='proteinContent'))});
test('noncommercial source goes to existing note type rather than service_offer',async()=>{const p=cmsProjection(await observe(recipeRaw(),catalog));assert.equal(p.content_type,'wiki_page');assert.equal(p.record.status,'draft');assert(p.record.content.includes('7 g'))});
test('source work uses the existing hold operation with recipe evidence retained',async()=>{const p=heldWorkEnvelope(await observe(recipeRaw(),catalog),'Review this recipe');assert.equal(p.operation,'hold');assert.equal(JSON.parse(p.selection).recipes[0].nutrition[0].standing,'source_claim_not_calculated')});
test('TikTok caption stays media metadata rather than invented recipe steps',async()=>{const o=await observe(capture(payload(item())),catalog);assert.equal(o.media[0].platform_id,id);assert.equal(o.recipes.length,0);assert.equal(cmsProjection(o).content_type,'wiki_page')});
test('nutrient claim differences are visible while timestamp-only changes are not',async()=>{const r=recipeRaw(),a=await observe(r,catalog);r.captured_at='2026-10-04T01:00:00Z';assert.equal(compareObservations(a,await observe(r,catalog)).changes.length,0);r.structured[0].nutrition.proteinContent='9 g';assert.equal(compareObservations(a,await observe(r,catalog)).changes[0].field,'recipes')});
test('Chrome runs capture on an intercepted current-post fixture; native CMS keeps its projection',{skip:typeof Bun==='undefined'},async()=>{
 const {withChrome}=await import('../capture/chrome-session.mjs');
 let captured;
 const html='<!doctype html><html><head><title>Controlled source fixture</title><script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">'+JSON.stringify(payload(item()))+'</script><script type="application/ld+json">'+JSON.stringify(recipe())+'</script></head><body><h1>Controlled fixture</h1><input value="DO_NOT_KEEP"></body></html>';
 await withChrome(async({open,until})=>{
  const {page,sessionId}=await open();await page('Fetch.enable',{patterns:[{urlPattern:'*',requestStage:'Request'}]});
  const navigation=page('Page.navigate',{url});const event=await until(e=>e.sessionId===sessionId&&e.method==='Fetch.requestPaused');
  await page('Fetch.fulfillRequest',{requestId:event.params.requestId,responseCode:200,responseHeaders:[{name:'Content-Type',value:'text/html; charset=utf-8'}],body:Buffer.from(html).toString('base64')});await navigation;
  await until(e=>e.sessionId===sessionId&&e.method==='Page.loadEventFired');
  const answer=await page('Runtime.evaluate',{expression:'('+capturePage.toString()+')()',returnByValue:true});assert(!answer.exceptionDetails);captured=answer.result.value;
 },{timeoutMs:20000});
 assert.equal(captured.platform_post.id,id);assert(!JSON.stringify(captured).includes('DO_NOT_KEEP'));
 const o=await observe(captured,catalog);assert.equal(o.recipes.length,1);assert.equal(o.media.length,1);
 const p=cmsProjection(o),root=fileURLToPath(new URL('../../../',import.meta.url)),tmp=mkdtempSync(join(tmpdir(),'bf-source-projection-'));
 try{mkdirSync(tmp+'/schemas');copyFileSync(root+'cmd/BonfyreCMS/content-types/wiki_page.json',tmp+'/schemas/wiki_page.json');
  const cli=root+'cmd/BonfyreCMS/bonfyre-cms';
  for(const args of [['schema','migrate','--db',tmp+'/cms.db','--schemas',tmp+'/schemas'],['entry','create','wiki_page',JSON.stringify(p.record),'--ns','source-witness','--db',tmp+'/cms.db','--schemas',tmp+'/schemas']]){
   const result=spawnSync(cli,args,{encoding:'utf8',timeout:15000});assert.equal(result.status,0,result.stderr);
  }
  const {Database}=await import('bun:sqlite');const db=new Database(tmp+'/cms.db',{readonly:true});
  try{const row=db.query('SELECT namespace,content,status FROM wiki_page').get();assert.equal(row.namespace,'source-witness');assert.equal(row.status,'draft');assert(row.content.includes(id));assert(row.content.includes('one portion'));}finally{db.close()}
 }finally{rmSync(tmp,{recursive:true,force:true})}
});
