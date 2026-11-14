// The ServiceSite CMS-to-publication boundary. No interactive UI logic.
import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join } from "node:path";

// Release guard for the actual gallery. Use its canonical CMS card links;
// never maintain a second registry of guides or claim a 404 is published.
const WORK_SAMPLES_ORIGIN='https://aurekai-work-samples.pages.dev';
const DOCS_ORIGIN='https://docs.aurekai.work';
// Collect authored URLs wherever the existing ServiceSite contract accepts
// links: homepage sections, subpages, nav, footers and nested buttons.
// Content remains canonical CMS material, not a second guide registry.
function documentationLinks(content) {
  const links=new Set();
  const visit=node=>{
    if(Array.isArray(node)){for(const item of node)visit(item);return;}
    if(!node||typeof node!=='object')return;
    for(const [key,value] of Object.entries(node)){
      if(key==='href'&&typeof value==='string'){
        try {const url=new URL(value);if(url.origin===DOCS_ORIGIN)links.add(url.href);}
        catch { /* Bad links are independently refused by the ServiceSite contract. */ }
      }else if(value&&typeof value==='object')visit(value);
    }
  };
  visit(content);
  return [...links].sort();
}
export async function verifyWorkSamplesGuides(content,{request=fetch}={}) {
  if(content.site_id!=='aurekai-work-samples')return {checked:0};
  if(!content.public_origin)throw Error('Work Samples publish requires public_origin for canonical URLs and sitemap.');
  if(content.public_origin.replace(/\/$/,'')!==WORK_SAMPLES_ORIGIN)
    throw Error('Work Samples public_origin does not match the declared Pages project.');
  const guides=documentationLinks(content);
  const failures=[];
  for(let i=0;i<guides.length;i+=4){
    await Promise.all(guides.slice(i,i+4).map(async url=>{
      try{
        const response=await request(url,{redirect:'follow',signal:AbortSignal.timeout(12000)});
        const destination=new URL(response.url||url),original=new URL(url);
        await response.body?.cancel();
        const path=value=>value.pathname.replace(/\/+$/,'')||'/';
        if(!response.ok||destination.origin!==original.origin||path(destination)!==path(original))
          failures.push(url+': HTTP '+response.status);
      }catch(error){failures.push(url+': '+error.message);}
    }));
  }
  if(failures.length)throw Error('Work Samples linked guide unavailable: '+failures.sort().join('; '));
  return {checked:guides.length};
}

// A deploy command succeeding is not a live readback. For the public gallery,
// compare served bytes against the exact built files, without another manifest.
export async function verifyWorkSamplesReadback(content,built,{request=fetch,attempts=3,pause=ms=>new Promise(done=>setTimeout(done,ms))}={}) {
  if(content.site_id!=='aurekai-work-samples')return {checked:0};
  const paths=['/',...(content.pages??[]).map(page=>'/'+page.path+'/'),'/assets/site.css','/robots.txt','/sitemap.xml'];
  const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
  const expected=paths.map(path=>{
    const local=path==='/'?'index.html':path.endsWith('/')?join(path.slice(1),'index.html'):path.slice(1);
    return {path,digest:sha(readFileSync(join(built.out,local)))};
  });
  let pending=expected,failures=[];
  for(let attempt=0;attempt<attempts&&pending.length;attempt++){
    if(attempt)await pause(800);
    failures=[];
    for(let i=0;i<pending.length;i+=6){
      const group=pending.slice(i,i+6);
      const checked=await Promise.all(group.map(async file=>{
        const url=WORK_SAMPLES_ORIGIN+file.path;
        try{
          const response=await request(url,{redirect:'follow',signal:AbortSignal.timeout(10000),cache:'no-store'});
          const final=new URL(response.url||url);
          if(!response.ok||final.origin!==WORK_SAMPLES_ORIGIN||
             (final.pathname.replace(/\/+$/,'')||'/')!==(file.path.replace(/\/+$/,'')||'/')){
            await response.body?.cancel();
            return {...file,reason:'HTTP '+response.status+' or unexpected redirect'};
          }
          const body=Buffer.from(await response.arrayBuffer());
          return sha(body)===file.digest?null:{...file,reason:'served bytes differ'};
        }catch(error){return {...file,reason:String(error.message)};}
      }));
      failures.push(...checked.filter(Boolean));
    }
    pending=failures;
  }
  if(pending.length)throw Error('Work Samples deploy accepted but live readback failed: '+
    pending.map(item=>item.path+' ('+item.reason+')').join('; '));
  return {state:'LIVE_VERIFIED',origin:WORK_SAMPLES_ORIGIN,files_checked:expected.length};
}

export async function runSitePublication({
  verb,file,dir,rows,cms,ns,type,local,validate,report=()=>{},
  buildSite=null,deploySite=null,guideRequest=fetch,publicReadback=verifyWorkSamplesReadback,
}) {
  if (!["push","pull","publish"].includes(verb)) throw Error("unknown publication operation");
  const peek=JSON.parse(readFileSync(file,"utf8")).site_id;
  const row=rows().find(entry=>entry.site_id===peek);
  if (verb==="push") {
    const content=await local();
    // The canonical CMS text field is bounded; local source is not authority.
    if (JSON.stringify(JSON.stringify(content)).length>30000)
      throw Error("the record is larger than the CMS holds in one field; shorten it or split the site");
    const body=JSON.stringify({
      site_id:content.site_id,business_name:content.business.name,
      pages_project:content.pages_project??"",
      status:content.preview?"preview":"public",
      content_json:JSON.stringify(content),
      note:"exported to the live page by site.mjs publish",
    });
    const answer=row?
      cms(["entry","update",type,String(row.id),"--ns",ns,body]):
      cms(["entry","create",type,"--ns",ns,body]);
    const event={pushed:content.site_id,created:!row,id:row?.id??answer?.data?.id??answer?.id??null};
    report(event);return event;
  }
  if (!row) throw Error("no record for "+peek+" in the CMS yet; run push first");
  const content=JSON.parse(row.content_json);
  await validate(content);
  if(content.site_id!==peek) throw Error("the record's site_id does not match this folder");
  if(verb==="publish")await verifyWorkSamplesGuides(content,{request:guideRequest});
  writeFileSync(file,JSON.stringify(content,null,2)+"\n");
  const event={pulled:content.site_id,id:row.id,preview:Boolean(content.preview)};
  report(event);
  if(verb==="publish") {
    if(!content.pages_project)throw Error("the record names no pages_project to publish to");
    const build=buildSite??(await import("./build.mjs")).build;
    const publish=deploySite??(await import("./cloud.mjs")).publish;
    const built=await build(dir);
    const receipt=await publish(built.out,content.pages_project,content.cloud_account);
    if(content.site_id==='aurekai-work-samples'){
      try{
        const live=await publicReadback(content,built);
        const verified={...receipt,live_readback:live};
        report(verified);return verified;
      }catch(error){
        report({deployment_accepted:receipt,live_readback:'UNVERIFIED',error:error.message});
        throw error;
      }
    }
    report(receipt);return receipt;
  }
  return event;
}
