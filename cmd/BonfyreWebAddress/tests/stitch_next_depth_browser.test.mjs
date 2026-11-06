// Chromium acceptance for owner-backed catalogue and exact CMS review, no release.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
const root=resolve(fileURLToPath(new URL('../../../',import.meta.url))),read=p=>readFileSync(join(root,p),'utf8');
const web='cmd/BonfyreWebAddress/src/';
function literal(path,contains,starts){
 const source=read(path),strings=[...source.matchAll(/fputs\(\s*("(?:\\.|[^"\\])*")\s*,\s*stdout\)/g)]
  .map(m=>m[1]).filter(s=>s.includes(contains)).map(s=>JSON.parse(s)).filter(s=>s.startsWith(starts));
 assert.equal(strings.length,1,`Expected one exact native literal ${contains}`);return strings[0];
}
const core=web+'page_core_rendering.h';
const controls=literal(core,'data-media-catalogue','<style>'),script=literal(core,'data-media-search','<script>');
assert(core&&read(core).includes('const char *media_kind = item->kind == BF_PAGE_MATERIAL_VIDEO'));
assert(read(core).includes('item->kind == BF_PAGE_MATERIAL_AUDIO')&&read(core).includes('Terms: '));
const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
const chrome=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage','--disable-background-networking']});
const failures=[];
try{
 const catalog=items=>`${controls}<div class="video-grid">${items.map(([id,kind,name])=>`<article class="page-media-card" data-media-card data-media-kind="${kind}" id="${id}"><a class="video" href="/o/retained/${id}">${name}</a><div class="media-catalogue-proof">Source: retained · Terms: source-stated</div><a data-item-continue href="/feed-use?context=self&amp;item=retained%2F${id}">Use in my apps</a></article>`).join('')}</div></section>${script}`;
 const page=await chrome.newPage();page.on('pageerror',e=>failures.push(e.message));
 await page.setContent(catalog([['film','video','Retained short film'],['audio','audio','Field recording'],['photo','photo','Picture'],['essay','other','Source essay']])+catalog([['other','video','Second owner film']]));
 assert.equal(await page.$$eval('[data-media-catalogue]:first-of-type [data-media-card]:not([hidden])',a=>a.length),4);
 await page.click('[data-media-catalogue]:first-of-type [data-media-kind="video"]');
 assert.deepEqual(await page.$$eval('[data-media-catalogue]:first-of-type [data-media-card]:not([hidden])',a=>a.map(x=>x.id)),['film']);
 assert.equal(await page.$$eval('[data-media-catalogue]:nth-of-type(2) [data-media-card]:not([hidden])',a=>a.length),1);
 await page.click('[data-media-catalogue]:first-of-type [data-media-layout=list]');
 assert.equal(await page.$eval('[data-media-catalogue]:first-of-type',x=>x.dataset.mediaView),'list');
 await page.type('[data-media-catalogue]:first-of-type [data-media-search]','no results');
 assert.equal(await page.$$eval('[data-media-catalogue]:first-of-type [data-media-card]:not([hidden])',a=>a.length),0);
 assert.equal(await page.$eval('#film a[data-item-continue]',a=>a.getAttribute('href')),'/feed-use?context=self&item=retained%2Ffilm');
 await page.setViewport({width:390,height:840});
 assert(await page.$eval('[data-media-catalogue]:first-of-type .media-catalogue-controls',x=>x.getBoundingClientRect().width<=innerWidth));
 await page.close();
 // Real in-owner CMS compositor, native snapshot in memory, interception never reaches network.
 const source=read(web+'cms_page_composer_markup.h');
 const cut=source.slice(source.indexOf('static const char *cms_page_composer_markup =')+'static const char *cms_page_composer_markup ='.length,source.lastIndexOf('\n;'));
 const markup=cut.split('\n').map(x=>x.trim()).filter(x=>x.startsWith('"')).map(x=>JSON.parse(x)).join('');
 let snapshot={schema:'bonfyre.page-snapshot.v1',id:12,digest:'a'.repeat(64),snapshot:{route:'/stitch',page:{title:'Source page',heading:'Source page',status:'draft'},fragments:[{coordinate:'one',position:0,heading:'Authored material',body:'Body retained',source_ref:'source:exact',source_digest:'sha256:exact',status:'draft'}]}};
 const writes=[],cms=await chrome.newPage();cms.on('pageerror',e=>failures.push(e.message));
 await cms.setRequestInterception(true);cms.on('request',async req=>{const url=new URL(req.url());
  if(url.pathname==='/cms/compose')return req.respond({status:200,contentType:'text/html',body:markup});
  if(url.pathname==='/cms/compose/pages')return req.respond({status:200,contentType:'application/json',body:JSON.stringify({pages:[],has_more:false})});
  if(url.pathname==='/cms/compose/page'&&req.method()==='GET')return req.respond({status:200,contentType:'application/json',body:JSON.stringify(snapshot)});
  if(url.pathname==='/cms/compose/page'&&req.method()==='POST'){const submitted=JSON.parse(req.postData());writes.push(submitted);
   assert.equal(submitted.expected_digest,snapshot.digest,'must retain the original readback revision');
   snapshot={...snapshot,digest:'b'.repeat(64),snapshot:{route:'/stitch',page:submitted.page,fragments:submitted.fragments}};
   return req.respond({status:200,contentType:'application/json',body:JSON.stringify({committed:true,digest:snapshot.digest})});}
  return req.abort();});
 await cms.goto('https://aurekai.work/cms/compose?route=%2Fstitch',{waitUntil:'domcontentloaded'});
 await cms.waitForSelector('#canvas section .cms-edit-section');
 await cms.click('#canvas section .cms-edit-section');
 assert.equal(await cms.$eval('#fields h2',n=>n.textContent),'Section 1','canvas opens the actual CMS section inspector');
 await cms.click('#duplicate');
 assert.equal(await cms.$$eval('#canvas section',nodes=>nodes.length),2,'structural duplication adds one draft section');
 assert.equal(await cms.$$eval('#canvas section .cms-projection-proof',nodes=>nodes.length),1,'duplicate cannot inherit provenance');
 assert.equal(await cms.$eval('#canvas section.selected',n=>n.textContent.includes('Authored material')),true);
 await cms.click('#up');
 assert.equal(await cms.$eval('#outline button:nth-child(2)',n=>n.textContent.includes('Authored material')),true);
 await cms.click('#undo');
 await cms.click('#outline button:first-child');
 await cms.waitForSelector('#canvas section');await cms.focus('#field-heading');
 await cms.type('#field-heading',' updated');await cms.click('#save');
 assert.equal(await cms.$eval('#cms-review',d=>d.open),true);assert.equal(writes.length,0,'review is not a write');
 assert(await cms.$eval('#cms-review-count',n=>n.textContent.includes('changed field')));
 assert(await cms.$eval('#cms-review-items',n=>n.textContent.includes('Page introduction · heading')));
 // A changed draft while the dialog is open must not silently commit the old candidate.
 await cms.evaluate(()=>{const f=document.getElementById('field-heading');f.value+=' changed';f.dispatchEvent(new Event('input',{bubbles:true}));});
 await cms.click('#cms-review-confirm');
 assert.equal(writes.length,0,'changed draft refuses stale reviewed candidate');
 assert(await cms.$eval('#cms-review-status',n=>n.textContent.includes('changed')));
 await cms.click('#cms-review-cancel');await cms.click('#save');
 assert.equal(writes.length,0,'second review still performs no request');
 await cms.click('#cms-review-confirm');
 await cms.waitForFunction(()=>document.getElementById('dirty').textContent==='Saved version');
 assert.equal(writes.length,1,'one explicit confirmed CMS write');
 assert.equal(writes[0].page.heading,'Source page updated changed');
 assert.equal(await cms.$eval('#cms-review',d=>d.open),false);
 assert.deepEqual(failures,[]);await cms.close();
 console.log('Stitch deep cinema grid + exact CMS approval review: PASS two independent catalogues, rights vocabulary, version-bound no-write review, one confirmed owner save');
}finally{await chrome.close();}
