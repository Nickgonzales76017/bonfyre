// Chromium smoke of exact native Stitch owner literals, no external mutation.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
const root=resolve(fileURLToPath(new URL('../../../',import.meta.url))),read=p=>readFileSync(join(root,p),'utf8');
const web='cmd/BonfyreWebAddress/src/';
function exact(path,has,starts){
 const source=read(path);
 const matches=[...source.matchAll(/fputs\(\s*("(?:\\.|[^"\\])*")\s*,\s*stdout\)/g)]
  .map(m=>m[1]).filter(s=>s.includes(has)).map(s=>JSON.parse(s)).filter(s=>s.startsWith(starts));
 assert.equal(matches.length,1,`Native ${has}: expected one exact literal, found ${matches.length}`);return matches[0];
}
const feedPath=web+'page_core_rendering.h',cinemaPath=web+'account_world_cinema_web.h';
const fc=exact(feedPath,'data-feed-tools','<section'),fs=exact(feedPath,'data-feed-search','<script>');
const cc=exact(cinemaPath,'data-cinema-tools','<div'),cs=exact(cinemaPath,'data-cinema-search','<script>');
const mc=exact(feedPath,'data-map-tools','<div'),ms=exact(feedPath,'data-map-search','<script>');
assert(read(feedPath).includes('data-map-point=')&&read(feedPath).includes('data-locate-point='));
const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
const browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage','--disable-background-networking']});
const errors=[];
const pageFor=async html=>{const p=await browser.newPage();p.on('pageerror',e=>errors.push(e.message));await p.setViewport({width:1200,height:850});await p.setContent(html,{waitUntil:'domcontentloaded'});await p.addStyleTag({content:read('site/matrix/page-components.css')});await p.addStyleTag({content:read('site/matrix/world.css')});return p;};
try{
 // Living Feeds: source cards and separate feed widgets, no synthetic material.
 const feed=items=>`<section>${fc}<div class="feed">${items.map(([kind,title])=>`<article class="tile ${kind}"><b>${title}</b><span class="by">Retained publisher</span></article>`).join('')}</div>${fs}</section>`;
 {const p=await pageFor(feed([['is-story','Research article'],['is-video','Recording'],['is-picture','Photograph']])+feed([['is-story','Other feed']]));
  await p.click('section:first-child [data-feed-mode=is-video]');
  assert.equal(await p.$$eval('section:first-child .feed article:not([hidden])',x=>x.length),1);
  assert.equal(await p.$$eval('section:nth-child(2) .feed article:not([hidden])',x=>x.length),1);
  await p.type('section:first-child [data-feed-search]','no matching title');
  assert.equal(await p.$$eval('section:first-child .feed article:not([hidden])',x=>x.length),0);await p.close();}
 // Cinema: playback classification supplied by native rights owner, not guessed.
 {const html=`<section data-world-programme>${cc}<h3 class="aw-month">November</h3><article data-screening="s1" data-state="announced" data-shown="media"><h3>Public domain film</h3></article><article data-screening="s2" data-state="announced" data-shown="link"><h3>Linked film</h3></article><h3 class="aw-month">December</h3><article data-screening="s3" data-state="cancelled" data-shown="link"><h3>Cancelled film</h3></article></section>${cs}`;
  const p=await pageFor(html);await p.click('[data-cinema-filter=media]');
  assert.deepEqual(await p.$$eval('article[data-screening]:not([hidden])',x=>x.map(n=>n.dataset.screening)),['s1']);
  assert.equal(await p.$$eval('.aw-month:not([hidden])',x=>x.length),1);
  await p.click('[data-cinema-filter=cancelled]');
  assert.deepEqual(await p.$$eval('article[data-screening]:not([hidden])',x=>x.map(n=>n.dataset.screening)),['s3']);
  await p.click('[data-cinema-filter=all]');await p.type('[data-cinema-search]','Linked');
  assert.deepEqual(await p.$$eval('article[data-screening]:not([hidden])',x=>x.map(n=>n.dataset.screening)),['s2']);await p.close();}
 // Creator cinema / listening commons: the exact native filter body, independently scoped and write-free.
 {const native=read(web+"page_media_spaces.h");
  const part=native.slice(native.indexOf("static void page_media_space_script("),native.indexOf("static int page_emit_media_spaces("));
  const script=[...part.matchAll(/"(?:\\.|[^"\\])*"/g)].map(m=>JSON.parse(m[0])).join("");
  assert(script.startsWith("<script>")&&script.endsWith("</script>"),"exact native media-space script");
  const cards=entries=>`<section data-media-space-catalogue><div class="media-space-controls"><label><input data-media-space-search></label><div><button data-media-space-filter="all" aria-pressed="true">All</button><button data-media-space-filter="weekend" aria-pressed="false">Weekend</button></div><output data-media-space-count></output></div><ol>${entries.map(([id,title,weekend])=>`<li data-media-space-card data-weekend="${weekend}" id="${id}">${title}</li>`).join("")}</ol></section>${script}`;
  const p=await pageFor(cards([["s1","Saturday Matinee","1"],["s2","Tuesday Cinema","0"]])+cards([["s3","Independent Radio","0"]]));
  const writes=[];p.on("request",request=>{if(request.method()!=="GET")writes.push(request.url())});
  await p.click("section:first-of-type [data-media-space-filter=weekend]");
  assert.deepEqual(await p.$$eval("section:first-of-type [data-media-space-card]:not([hidden])",nodes=>nodes.map(n=>n.id)),["s1"]);
  assert.deepEqual(await p.$$eval("section:nth-of-type(2) [data-media-space-card]:not([hidden])",nodes=>nodes.map(n=>n.id)),["s3"]);
  await p.type("section:first-of-type [data-media-space-search]","not here");
  assert.equal(await p.$$eval("section:first-of-type [data-media-space-card]:not([hidden])",nodes=>nodes.length),0);
  await p.setViewport({width:390,height:820});
  assert(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  assert.deepEqual(writes,[]);await p.close();}
 // Spatial: list coordinate and SVG point are the same native ordinal.
 {const html=`<figure class="map with-list">${mc}<div class="plate"><svg><g class="point" data-map-point="0"><circle/></g><circle class="point" data-map-point="1"/></svg></div><ol class="places-list"><li data-map-point="0"><button data-locate-point="0" aria-pressed="false">Locate</button>Reserve <a href="/o/nature/1">Source</a></li><li data-map-point="1"><button data-locate-point="1" aria-pressed="false">Locate</button>Civic hall <a href="/o/civic/2">Source</a></li></ol><aside data-map-inspector hidden><div data-map-inspector-body></div><button type="button" data-map-clear>Clear map focus</button></aside></figure>${ms}`;
  const p=await pageFor(html);await p.click('[data-locate-point="1"]');
  assert.equal(await p.$eval('svg [data-map-point="1"]',n=>n.dataset.selected),'true');
  assert.equal(await p.$eval('[data-map-inspector]',n=>n.hidden),false,'Source-backed inspector opens from exact list selection');
  assert.equal(await p.$eval('[data-map-inspector-body] a',n=>n.getAttribute('href')),'/o/civic/2');
  await p.keyboard.press('Escape');
  assert.equal(await p.$eval('[data-map-inspector]',n=>n.hidden),true,'Escape dismisses the inspected place');
  assert.equal(await p.$eval('svg [data-map-point="1"]',n=>n.dataset.selected),'false');
  await p.click('[data-locate-point="1"]');await p.click('[data-map-clear]');
  assert.equal(await p.$eval('[data-map-inspector]',n=>n.hidden),true,'Clear map focus dismisses without losing positions');
  await p.type('[data-map-search]','Reserve');
  assert.deepEqual(await p.$$eval('.places-list>li:not([hidden])',x=>x.map(n=>n.dataset.mapPoint)),['0']);
  assert.equal(await p.$$eval('svg [data-map-point]',x=>x.length),2,'Search cannot erase the map');await p.close();}
 // The native World reader now renders a real spatial scene from released DOM pieces.
 {const source=read(web+'account_world_room.h');
  const material=(a,b)=>[...source.slice(source.indexOf('static void '+a),b?source.indexOf('static void '+b):source.indexOf('#endif')).matchAll(/"(?:\\.|[^"\\])*"/g)].map(x=>JSON.parse(x[0])).join('');
  const stage=material('world_room_begin','world_room_end'),script=material('world_room_end');
  assert(stage.includes('data-world-room')&&script.includes('createShader')&&script.includes('uniformMatrix4fv'));
  const html=stage+'<section class="aw-pieces" id="world-room-readable"><article class="aw-piece" id="piece-1"><h2>Night Station</h2><p class="aw-note">Published source retained</p></article><article class="aw-piece" id="piece-2"><h2>Field Recordings</h2><a href="/o/fixture/123">Original</a></article><article class="aw-piece aw-gone" id="piece-3"><h2>Withdrawn reference</h2></article></section>'+script;
  const p=await pageFor(html),writes=[];p.on('request',r=>{if(r.method()!=='GET')writes.push(r.method())});
  assert.equal(await p.$eval('[data-room-items]',n=>n.querySelectorAll('button').length),2,'unavailable material does not become a spatial object');
  await p.click('[data-room-next]');
  assert.equal(await p.$eval('[data-world-room]',n=>n.dataset.roomSelected),'1');
  assert.equal(await p.$eval('[data-room-jump]',n=>n.getAttribute('href')),'#piece-2');
  assert.equal(await p.$eval('[data-room-label]',n=>n.textContent),'Field Recordings');
  const gl=await p.evaluate(()=>({available:!!document.querySelector('[data-world-room] canvas').getContext('webgl'),fallback:document.querySelector('[data-world-room]').dataset.roomFallback}));
  if(gl.available&&!gl.fallback){await p.focus('[data-world-room] canvas');await p.keyboard.press('ArrowLeft');
    assert.equal(await p.$eval('[data-world-room]',n=>n.dataset.roomSelected),'0');
    await p.keyboard.press('Escape');assert.equal(await p.$eval(':focus',n=>n.getAttribute('data-room-exit')),'');
    await p.click('[data-room-near]');await p.click('[data-room-far]');}
  else assert(await p.$eval('[data-room-status]',n=>n.textContent.includes('unavailable')),'software renderer has an accessible fallback');
  await p.setViewport({width:390,height:800});
  assert(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'3D room fits mobile without horizontal overflow');
  assert.deepEqual(writes,[],'room navigation makes no account writes');
  await p.close();}
 // Atelier: read the complete native composer literal and inspect one exact saved snapshot.
 {const source=read(web+'cms_page_composer_markup.h');
  const cut=source.slice(source.indexOf('static const char *cms_page_composer_markup =')+'static const char *cms_page_composer_markup ='.length,source.lastIndexOf('\n;'));
  const html=cut.split('\n').map(x=>x.trim()).filter(x=>x.startsWith('"')).map(x=>JSON.parse(x)).join('');
  assert(html.includes('data-cms-layout')&&html.includes('cms-projection-proof'));
  const p=await browser.newPage(),writes=[];p.on('pageerror',e=>errors.push(e.message));
  await p.setRequestInterception(true);p.on('request',async r=>{const u=new URL(r.url());if(r.method()!=='GET')writes.push(r.url());
   if(u.pathname==='/cms/compose')return r.respond({status:200,contentType:'text/html',body:html});
   if(u.pathname==='/cms/compose/pages')return r.respond({status:200,contentType:'application/json',body:JSON.stringify({pages:[],has_more:false})});
   if(u.pathname==='/cms/compose/page')return r.respond({status:200,contentType:'application/json',body:JSON.stringify({schema:'bonfyre.page-snapshot.v1',id:5,digest:'a'.repeat(64),snapshot:{route:'/stitch',page:{title:'Native page',heading:'Native page',status:'draft'},fragments:[{coordinate:'section-one',heading:'Publication',body:'Material text',source_ref:'source:one',source_digest:'sha256:one',evidence_ref:'proof:one',slot:'body',status:'draft'}]}})});
   return r.abort();});
  await p.goto('https://aurekai.work/cms/compose?route=%2Fstitch',{waitUntil:'domcontentloaded'});await p.waitForSelector('#canvas section');
  assert.equal(await p.$eval('#canvas',n=>n.textContent.includes('Source: source:one · Digest: sha256:one · Evidence: proof:one')),true);
  for(const mode of ['cards','split','story']){await p.click('[data-cms-layout="'+mode+'"]');assert.equal(await p.$eval('#canvas',n=>n.dataset.cmsLayout),mode);}
  await p.click('#preview-phone');assert(await p.$eval('#desk',n=>n.classList.contains('cms-preview-width-phone')));
  assert.equal(await p.$eval('#dirty',n=>n.textContent),'Saved version');assert.deepEqual(writes,[]);await p.close();}
 // Governed review: exact source-bound status, five boolean gates, no approval action.
 {const source=read('integrations/browser/procurement-rfq-workbench.mjs'),match=source.match(/function renderAuthorityStatus\(status\)\{([\s\S]*?)\n\}\n\nfunction renderProtocolActorInputs/);assert(match);
  const code='function renderAuthorityStatus(status){'+match[1]+'\n}',p=await pageFor('<section data-ui="native-authority-status" hidden></section><section data-ui="native-order-section" hidden></section>');
  await p.evaluate(code=>{const ui=x=>document.querySelector('[data-ui="'+x+'"]'),el=(tag,text)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;return n;};window.renderProof=Function('ui','el','controls','approvalStatus',code+';return renderAuthorityStatus;')(ui,el,()=>{},null);},code);
  const base={state:'pending',record_key:'procurement-approval:one',revision:3,body_sha256:'b'.repeat(64),assent_present:false,approve_authority:false,spend_authority:false,amount_known:false,eligible_for_order_intent:false,cannot_infer:'No spending effect authorized.'};
  await p.evaluate(x=>window.renderProof(x),base);
  assert.equal(await p.$$eval('[data-governed-proof] li[data-verified=false]',x=>x.length),5);
  assert(await p.$eval('[data-ui=native-order-section]',n=>n.hidden));
  await p.evaluate(x=>window.renderProof({...x,state:'eligible',assent_present:true,approve_authority:true,spend_authority:true,amount_known:true,eligible_for_order_intent:true}),base);
  assert.equal(await p.$$eval('[data-governed-proof] li[data-verified=true]',x=>x.length),5);
  assert.equal(await p.$eval('[data-ui=native-order-section]',n=>n.hidden),false);await p.close();}
 assert.deepEqual(errors,[]);
 console.log('Stitch five families: PASS exact native literals, feed / cinema / spatial / Atelier / governance');
}finally{await browser.close();}
