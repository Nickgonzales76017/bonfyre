// Publish the reviewed ServiceSite browser projection under the existing showcase.
// This is a static application release. It does not claim native intake or merchant authority.
import {readFile,writeFile,readdir,cp,mkdir,rm} from 'node:fs/promises';
import {join,resolve,extname} from 'node:path';
import {createHash} from 'node:crypto';
import {gzipSync,gunzipSync} from 'node:zlib';
const input=resolve(process.argv[2]??'scene-run/preview'),output=resolve(process.argv[3]??'site/spaces');
const base='/bonfyre/spaces/',hash=b=>createHash('sha256').update(b).digest('hex');
const roots=['/assets/','/data/','/catalogue/','/red3-planner/','/product-room/'];
const local=s=>typeof s==='string'&&roots.some(p=>s.startsWith(p))?base+s.slice(1):s;
const map=x=>typeof x==='string'?local(x):Array.isArray(x)?x.map(map):x&&typeof x==='object'?Object.fromEntries(Object.entries(x).map(([k,v])=>[k,map(v)])):x;
const cfg=(html,name)=>JSON.parse(html.match(new RegExp('<script[^>]*'+name+'[^>]*>([\\s\\S]*?)</script>'))[1]);
const setCfg=(html,name,value)=>html.replace(new RegExp('(<script[^>]*'+name+'[^>]*>)[\\s\\S]*?(</script>)'),(_,a,b)=>a+JSON.stringify(value).replaceAll('<','\\u003c')+b);
await mkdir(output,{recursive:true});await cp(input,output,{recursive:true});
for(const p of ['scene-check.html','test-config.mjs','codec-fixtures'])await rm(join(output,p),{recursive:true,force:true});
let cat=await readFile(join(output,'catalogue/index.html'),'utf8'),catalogue=map(cfg(cat,'data-catalogue-config'));
// Route-local presentation paths participate in the fixture catalogue generation.
const generations=new Map();
for(const route of catalogue.roomRoutes??[]){const c=route.catalogue,old=c.digest;delete c.digest;c.digest=hash(JSON.stringify(c));generations.set(old,c.digest);}
for(const name of await readdir(join(output,'data/product-workspace'))){
 if(!name.endsWith('.json.gz'))continue;
 const p=join(output,'data/product-workspace',name),before=await readFile(p),rows=JSON.parse(gunzipSync(before)),next=map(rows),body=gzipSync(Buffer.from(JSON.stringify(next)));
 await writeFile(p,body);for(const entry of catalogue.entries)if(entry.chunk===name)entry.chunk_sha256=hash(body);
}
cat=setCfg(cat,'data-catalogue-config',catalogue);await writeFile(join(output,'catalogue/index.html'),cat);
for(const path of ['product-room/index.html','red3-planner/index.html']){
 const file=join(output,path);let html=await readFile(file,'utf8'),c=map(cfg(html,'data-planner-config'));
 const old=c.catalogue.digest;if(generations.has(old))c.catalogue.digest=generations.get(old);else{delete c.catalogue.digest;c.catalogue.digest=hash(JSON.stringify(c.catalogue));}
 c.page=base+path.replace('index.html','');html=setCfg(html,'data-planner-config',c);await writeFile(file,html);
}
async function walk(dir){const result=[];for(const e of await readdir(dir,{withFileTypes:true})){const p=join(dir,e.name);if(e.isDirectory())result.push(...await walk(p));else result.push(p);}return result;}
for(const file of await walk(output)){
 if(!['.html','.mjs','.css'].includes(extname(file)))continue;
 let text=await readFile(file,'utf8');
 // Rebase URL literals only. Absolute merchant URLs and source IDs are untouched.
 text=text.replace(/(["'`(=])\/(assets|data|catalogue|red3-planner|product-room)\//g,(_,q,p)=>q+base+p+'/');
 if(file.endsWith('.html')){
  text=text.replace(/href="\/#ask"/g,'href="https://aurekai-work-samples.pages.dev/#ask"');
  text=text.replace(/<link\b[^>]*href="[^"]*fonts\.googleapis[^>]*>/g,'');
  text=text.replace(/<link\b[^>]*rel="canonical"[^>]*>/g,'');
  text=text.replace('</head>','<meta name="robots" content="noindex"><link rel="stylesheet" href="'+base+'release.css"></head>');
  text=text.replace('<body>','<body><div class="spaces-release"><a href="/bonfyre/#experiences">Aurekai applications</a><a href="'+base+'catalogue/?store=wayfair-reference&workspace=inventory">Catalogue</a><a href="'+base+'product-room/">Room planner</a><span>Saved on this device · Review preparation</span></div>');
  text=text.replace(/<form data-ui="submit-form"[\s\S]*?<\/form>/g,'<section class="spaces-review-boundary"><h3>Keep this version for review</h3><p>Download the source-linked brief above. This public release does not submit contact details or send a supplier request.</p></section>');
  // Existing clients require their form nodes; hide and disable, rather than removing them.
  // The following hidden inert fields satisfy that contract without accepting personal data.
  if(pathHasPlanner(file))text=text.replace('</body>','<template id="spaces-unused"></template></body>');
 }
 // Refuse disconnected submission before any network or contact-data write.
 if(file.endsWith('planner-client.mjs'))text=text.replace("const response=await fetch('/api/lead/'+cfg.form", "throw Error('Native review is not connected. Download the brief for review.');const response=await fetch('/api/lead/'+cfg.form");
 await writeFile(file,text);
}
function pathHasPlanner(p){return p.endsWith('/product-room/index.html')||p.endsWith('/red3-planner/index.html');}
// Keep the production client's required nodes inside the planner section, inert and hidden.
for(const rel of ['product-room/index.html','red3-planner/index.html']){
 const p=join(output,rel);let html=await readFile(p,'utf8');html=html.replace('<section class="spaces-review-boundary">','<form data-ui="submit-form" hidden inert><input data-ui="name" disabled><input data-ui="email" disabled><button data-ui="submit" disabled></button><p data-ui="receipt"></p></form><section class="spaces-review-boundary">');await writeFile(p,html);
}
await writeFile(join(output,'release.css'),'.spaces-release{display:flex;flex-wrap:wrap;gap:18px;align-items:center;padding:12px 22px;background:#152b25;color:#f3f1e9;font:14px/1.5 system-ui}.spaces-release a{color:inherit}.spaces-release span{margin-left:auto;font-size:12px}.spaces-review-boundary{padding:18px;border:1px solid #adb8a4}.spaces-review-boundary h3{margin-top:0}');
await writeFile(join(output,'index.html'),'<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><meta http-equiv="refresh" content="0;url='+base+'catalogue/?store=wayfair-reference&workspace=inventory"><title>Aurekai product and room studio</title><a href="'+base+'catalogue/?store=wayfair-reference&workspace=inventory">Open the catalogue and room studio</a>');
const entries=[];for(const file of await walk(output)){const data=await readFile(file);entries.push([file.slice(output.length+1),hash(data),data.length].join('\t'));}
await writeFile(join(output,'RELEASE.tsv'),'path\tsha256\tbytes\n'+entries.sort().join('\n')+'\n');
console.log('Prepared '+entries.length+' files at '+base+'; native submission is not connected.');
