import {test} from 'node:test';
import assert from 'node:assert/strict';
import {APEX_ORDER,inspectApex,inspectStylesheet,publicReadback} from '../native_apex_live_readback.mjs';
import {AUREKAI_VOICE} from '../../branding/voice.mjs';
const version='8d90117e7d4af5ad';
function html(t,{title=AUREKAI_VOICE.channels[t].title,css=version,active=t,header='' }={}){
 const url='https://aurekai.'+t+'/';
 return '<!doctype html><html lang="en"><head><title>'+title+'</title>'+
 '<meta property="og:title" content="'+title+'">'+
 '<link rel="canonical" href="'+url+'">'+
 '<link rel="stylesheet" href="/assets/front-brand.css?v='+css+'"></head>'+
 '<body><main>'+header+
 '<section data-brand-masthead data-brand-territory="'+t+'">'+
 'Built on software.<br>Driven by Aurekai.'+
 '<h1 class="af-brand-title" id="af-brand-title">'+title+'</h1>'+
 '<nav class="af-brand-nav">'+APEX_ORDER.map(x=>'<a href="https://aurekai.'+x+'/"'+(x===active?' aria-current="page"':'')+'>'+x+'</a>').join('')+
 '</nav></section></main></body></html>';
}
for(const t of APEX_ORDER)test('accept '+t+' only with its own title, H1, origin and CSS',()=>{
 const receipt=inspectApex(t,html(t));
 assert.equal(receipt.territory,t);
 assert.equal(receipt.version,version);
 assert.equal(receipt.page_sha256.length,64);
});
test('reject stale or swapped brand, duplicated hero, unowned territory',()=>{
 assert.throws(()=>inspectApex('work',html('work',{title:'Old Work'})),/stale native title/);
 assert.throws(()=>inspectApex('work',html('work',{active:'shop'})),/wrong active domain/);
 assert.throws(()=>inspectApex('work',html('work',{header:'<header class="hero"><h1>Old</h1></header>'})),/one H1/);
 assert.throws(()=>inspectApex('work',html('work',{css:'stale'})),/stylesheet absent/);
 assert.throws(()=>inspectApex('customer',html('work')),/unowned domain/);
});
test('public stylesheet is byte-exact, and a modified byte is refused',()=>{
 assert.equal(inspectStylesheet(Buffer.from('abc'),Buffer.from('abc')).length,64);
 assert.throws(()=>inspectStylesheet(Buffer.from('abcd'),Buffer.from('abc')),/stylesheet bytes differ/);
});
test('network refusal does not invent a live acceptance receipt',async()=>{
 const old=console.error;console.error=()=>{};
 try{
  await assert.rejects(()=>publicReadback(async()=>({status:503,headers:new Map([['content-type','text/html']])})),/HTTP HTML not 200/);
 }finally{console.error=old;}
});
