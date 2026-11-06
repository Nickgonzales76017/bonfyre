// Compile the Pack presentation enhancement into its existing native emitter.
// The JS/CSS sources are canonical; no deployed filesystem rewrite or extra runtime.
import {readFileSync,writeFileSync,renameSync,mkdirSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname,join} from 'node:path';
const root=join(dirname(fileURLToPath(import.meta.url)),'..');
const js=readFileSync(join(root,'src/pack_browser.js'),'utf8');
const css=readFileSync(join(root,'src/pack_browser.css'),'utf8');
if(Buffer.byteLength(js)>65536||Buffer.byteLength(css)>32768||/<\/script/i.test(js)||/<\/style/i.test(css))throw Error('Unsafe or oversized Pack browser source.');
const literal=(name,value)=>'static const char '+name+'[] =\n'+value.split('\n').map(line=>JSON.stringify(line+'\n')).join('\n')+';\n';
const controls='<div class="bf-pack-browser" data-pack-browser hidden role="search" aria-label="Tools in this view">'+
 '<label>Find a tool<input type="search" data-pack-filter placeholder="Search tools, tasks and workflows" autocomplete="off" maxlength="200"></label>'+
 '<label>Type<select data-pack-kind><option value="">All types</option></select></label>'+
 '<label>Order<select data-pack-sort><option value="published">Published order</option><option value="name">Name</option></select></label>'+
 '<div class="bf-pack-segments" role="group" aria-label="Tool display"><button type="button" data-pack-view="gallery" aria-pressed="true">Gallery</button><button type="button" data-pack-view="list" aria-pressed="false">List</button></div>'+
 '<span data-pack-count role="status" aria-live="polite"></span><button type="button" data-pack-reset hidden>Clear filters</button>'+
 '<p data-pack-empty hidden>No tools match these filters. Clear them to see the available tools.</p></div>';
const output='/* Generated from src/pack_browser.js and .css. Do not edit. */\n#ifndef BONFYRE_NATIVE_PACK_BROWSER_H\n#define BONFYRE_NATIVE_PACK_BROWSER_H\n'+
 literal('bf_pack_browser_style','<style>'+css+'</style>')+literal('bf_pack_browser_controls',controls)+literal('bf_pack_browser_script','<script>'+js+'</script>')+'#endif\n';
const path=join(root,'generated/native_pack_browser.h');
if(process.argv.includes('--check')){if(readFileSync(path,'utf8')!==output)throw Error('Pack browser projection is stale.');}
else{mkdirSync(dirname(path),{recursive:true});let same=false;try{same=readFileSync(path,'utf8')===output;}catch{}if(!same){writeFileSync(path+'.tmp',output);renameSync(path+'.tmp',path);}}
console.log('Pack browser projection verified: '+Buffer.byteLength(js)+' JavaScript bytes, '+Buffer.byteLength(css)+' CSS bytes.');
