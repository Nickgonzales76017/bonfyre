#!/usr/bin/env node
// Post-activation readback: immutable public HTML/CSS witnesses, not a deploy.
// A compiled source or green unit test alone must never mark four-apex live.
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {AUREKAI_VOICE} from '../branding/voice.mjs';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'..');
export const APEX_ORDER=Object.freeze(['work','life','live','shop']);
export const hex=bytes=>createHash('sha256').update(bytes).digest('hex');
const refuse=(what)=>{throw Error(what)};
const htmlCount=(html,re)=>(html.match(re)||[]).length;

export function inspectApex(territory,html){
 const channel=AUREKAI_VOICE.channels[territory];
 if(!APEX_ORDER.includes(territory)||!channel)refuse('unowned domain');
 if(typeof html!=='string'||html.length>2_000_000)refuse('missing or oversized HTML');
 const end=html.indexOf('</head>');
 if(end<0||end>200_000)refuse('missing bounded document head');
 const head=html.slice(0,end);
 const url='https://aurekai.'+territory+'/';
 if(!head.includes('<title>'+channel.title+'</title>'))refuse('stale native title');
 if(!head.includes('property="og:title" content="'+channel.title+'"'))refuse('stale social preview');
 if(!head.includes('rel="canonical" href="'+url+'"'))refuse('wrong canonical address');
 const sheet=head.match(/<link\b[^>]*href="\/assets\/front-brand\.css\?v=([a-f0-9]{16})"[^>]*>/);
 if(!sheet)refuse('versioned stylesheet absent from head');
 if(htmlCount(html,/<h1\b/g)!==1)refuse('document must have one H1');
 if(!html.includes('<h1 class="af-brand-title" id="af-brand-title">'+channel.title+'</h1>'))
   refuse('homepage H1 not from canonical brand');
 if(htmlCount(html,/data-brand-masthead/g)!==1||
   !html.includes('data-brand-territory="'+territory+'"'))refuse('missing or duplicate native edition');
 if(html.includes('<header class="hero">'))refuse('generic hero still precedes native brand');
 if(!html.includes('Built on software.<br>Driven by Aurekai.'))refuse('corporate signature missing');
 const match=html.match(/<nav class="af-brand-nav"[^>]*>[\s\S]*?<\/nav>/);
 if(!match||htmlCount(match[0],/aria-current="page"/g)!==1)refuse('broken territory navigation');
 if(!match[0].includes('href="'+url+'" aria-current="page"'))refuse('wrong active domain');
 for(const domain of APEX_ORDER)
   if(!match[0].includes('href="https://aurekai.'+domain+'/"'))refuse('missing '+domain+' continuation');
 return Object.freeze({territory,version:sheet[1],page_sha256:hex(Buffer.from(html))});
}

export function inspectStylesheet(actual,expected){
 const a=Buffer.from(actual),e=Buffer.from(expected);
 if(!a.length||a.length>300_000||hex(a)!==hex(e))refuse('public stylesheet bytes differ from source');
 return hex(a);
}

export async function publicReadback(fetcher=fetch){
 const expected=readFileSync(join(root,'site/matrix/front-brand.css'));
 const receipts=[];
 for(const territory of APEX_ORDER){
  const url='https://aurekai.'+territory+'/';
  try{
   const options={redirect:'manual',headers:{Accept:'text/html','Cache-Control':'no-cache'},signal:AbortSignal.timeout(15000)};
   const response=await fetcher(url,options);
   if(response.status!==200||!String(response.headers.get('content-type')||'').includes('text/html'))
      refuse('HTTP HTML not 200 text/html');
   const html=await response.text();
   const receipt=inspectApex(territory,html);
   const cssUrl=url+'assets/front-brand.css?v='+receipt.version;
   const css=await fetcher(cssUrl,{redirect:'manual',headers:{Accept:'text/css','Cache-Control':'no-cache'},signal:AbortSignal.timeout(15000)});
   if(css.status!==200||!String(css.headers.get('content-type')||'').includes('text/css'))
      refuse('HTTP CSS not 200 text/css');
   const css_sha256=inspectStylesheet(Buffer.from(await css.arrayBuffer()),expected);
   receipts.push({...receipt,css_sha256});
   console.log('APEX_VERIFIED '+territory+' page_sha256='+receipt.page_sha256+' css_sha256='+css_sha256);
  }catch(e){
   console.error('APEX_READBACK_REFUSED '+territory+': '+(e?.message||String(e)));
   throw e;
  }
 }
 if(new Set(receipts.map(x=>x.version)).size!==1)refuse('four apices do not share the same immutable stylesheet version');
 console.log('FOUR_APEX_PUBLIC_READBACK_PASS source_evidence='+receipts.length+' exact_html_css_hosts');
 return receipts;
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)
 await publicReadback().catch(()=>{process.exitCode=1});
