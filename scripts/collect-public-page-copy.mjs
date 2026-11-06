#!/usr/bin/env bun
// Site-wide public copy census; CMS is read-only and no external publication occurs.
// Examples:
//   bun scripts/collect-public-page-copy.mjs --dry-run
//   bun scripts/collect-public-page-copy.mjs --canonical-only
//   bun scripts/collect-public-page-copy.mjs --live --max-pages 6 --refresh
// Output SQLite: pages, strings, sentences, links, canonical_copy.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, resolve, join } from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const ROOT = dirname(fileURLToPath(import.meta.url));
const APEX = ["aurekai.work","aurekai.life","aurekai.live","aurekai.shop","docs.aurekai.work"];
const COPY = ["title","heading","lede","eyebrow","note","label","description","summary","tagline","intro","body","text","markdown","content","caption","footnote"];
const hash = s => createHash("sha256").update(String(s)).digest("hex");
export const normalize = s => String(s ?? "").replace(/\s+/gu, " ").trim();
export function sentences(text, language="en") {
  const value = normalize(text);
  if (!value) return [];
  const lang = /^[a-z]{2,3}(?:-[a-zA-Z]{2,8})?$/.test(language) ? language : "en";
  return [...new Intl.Segmenter(lang,{granularity:"sentence"}).segment(value)].map((s,i)=>({
    index:i, text:normalize(s.segment), kind:/[.!?。！？]$/u.test(normalize(s.segment))?"sentence":"phrase"
  })).filter(s=>s.text);
}
export function publicUrl(href, host) {
  try {
    if (!/^(?:[a-z0-9-]+\.)*aurekai\.(?:work|life|live|shop)$/.test(host)) return null;
    const u=new URL(href,"https://"+host+"/");
    if (u.protocol!=="https:" || u.hostname!==host || u.username || u.password) return null;
    if (/^\/(?:api|auth|login|logout|sign-in|sign-out|register|signup|settings|account|admin|private|inbox|workspace|checkout|payments|internal)(?:\/|$)/i.test(u.pathname)) return null;
    if (/^\/apps\/[^/]+\/(?:draft|edit|settings|private)(?:\/|$)/i.test(u.pathname)) return null;
    const allow=new Set(["page","p","offset","cursor","after","before","tab"]);
    if ([...u.searchParams.keys()].some(k=>!allow.has(k)) || u.pathname.length>1024 || u.search.length>300) return null;
    u.hash="";u.searchParams.sort();
    return u.href;
  } catch { return null; }
}
const table = (db,name)=>!!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
const columns = (db,name)=>new Set(db.query("PRAGMA table_info("+name+")").all().map(r=>r.name));
export function openCanonicalCms(path, { forceCompat = false } = {}) {
  let readonly, primaryFailure;
  if (!forceCompat) {
    try {
      readonly = new Database(path, { readonly: true, create: false });
      // Bun may open the handle before finding a blocked WAL/-shm sidecar.
      readonly.query("SELECT name FROM sqlite_master LIMIT 1").get();
      return readonly;
    } catch (error) {
      primaryFailure = error;
      try { readonly?.close(); } catch {}
    }
  }
  // The native CMS reads this file with a normal SQLite handle.
  // Enable query_only BEFORE running any data statement; canonical rows are never mutated.
  let compatible;
  try {
    compatible = new Database(path, { create: false });
    compatible.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=3000;");
    if (Number(compatible.query("PRAGMA query_only").get()?.query_only) !== 1)
      throw Error("SQLite refused query_only protection");
    compatible.query("SELECT name FROM sqlite_master LIMIT 1").get();
    if (!forceCompat) console.warn("CMS readonly SQLite handle failed; guarded query_only compatibility mode enabled.");
    return compatible;
  } catch (error) {
    try { compatible?.close(); } catch {}
    throw new Error("Cannot query canonical CMS " + path +
      "; readonly: " + String(primaryFailure?.message || "not attempted") +
      "; query_only fallback: " + String(error.message || error) +
      ". Check disk space and WAL/-shm access. No CMS changes attempted.");
  }
}
export function targets(db) {
  const hosts=new Map(APEX.map(host=>[host,{host,site_id:"",subject:"",routes:new Set(["/"])}]));
  const bySite=new Map();
  if(table(db,"site_presentation")) for(const row of db.query("SELECT site_id,host,subject_address FROM site_presentation WHERE status='published' ORDER BY id DESC").all()){
    const host=String(row.host||"").toLowerCase();
    if(!publicUrl("/",host)) continue;
    if(!hosts.has(host)) hosts.set(host,{host,site_id:row.site_id,subject:row.subject_address||"",routes:new Set(["/"])});
    if(!bySite.has(row.site_id)) bySite.set(row.site_id,hosts.get(host));
  }
  if(table(db,"site_page")) for(const row of db.query("SELECT namespace,route FROM site_page WHERE status='published'").all()){
    const site=bySite.get(row.namespace);
    if(!site || !row.route) continue;
    const href=publicUrl(row.route,site.host);
    if(href) {const u=new URL(href);site.routes.add(u.pathname+u.search);}
  }
  return {hosts,bySite};
}
export function argsOf(argv=process.argv.slice(2)){
  const o={cms:join(homedir(),".bonfyre/estate-local/cms.db"),out:".tmp/public-page-copy.sqlite",live:false,dry:false,
    refresh:false,limitHosts:0,maxPages:0,maxPerHost:120,maxDepth:3,timeoutMs:18000};
  const named={"--cms-db":"cms","--out":"out","--limit-hosts":"limitHosts","--max-pages":"maxPages",
    "--max-per-host":"maxPerHost","--max-depth":"maxDepth","--timeout-ms":"timeoutMs"};
  for(let i=0;i<argv.length;i++){
    const arg=argv[i];
    if(arg==="--live")o.live=true;
    else if(arg==="--canonical-only")o.live=false;
    else if(arg==="--dry-run")o.dry=true;
    else if(arg==="--refresh")o.refresh=true;
    else if(named[arg] && argv[i+1])o[named[arg]]=argv[++i];
    else throw Error("Unknown or incomplete argument: "+arg);
  }
  for(const k of ["limitHosts","maxPages","maxPerHost","maxDepth","timeoutMs"]){
    o[k]=Number(o[k]);if(!Number.isSafeInteger(o[k])||o[k]<0)throw Error("Invalid "+k);
  }
  if(!o.maxPerHost||!o.timeoutMs)throw Error("max-per-host and timeout-ms must be positive");
  o.cms=resolve(o.cms);o.out=resolve(o.out);
  if(o.cms===o.out)throw Error("Output cannot overwrite canonical CMS");
  return o;
}
function openOutput(path){
  const db=new Database(path,{create:true});
  db.exec([
    "PRAGMA busy_timeout=3000; PRAGMA foreign_keys=ON;",
    "CREATE TABLE IF NOT EXISTS pages(id INTEGER PRIMARY KEY,url TEXT UNIQUE NOT NULL,host TEXT,route TEXT,site_id TEXT,subject TEXT,title TEXT,http_status INTEGER,observed_at TEXT,digest TEXT,error TEXT DEFAULT '');",
    "CREATE TABLE IF NOT EXISTS strings(id INTEGER PRIMARY KEY,page_id INTEGER REFERENCES pages(id) ON DELETE CASCADE,ordinal INTEGER,role TEXT,section TEXT,node_path TEXT,raw TEXT,normalized TEXT,digest TEXT,anchor_url TEXT,UNIQUE(page_id,ordinal));",
    "CREATE TABLE IF NOT EXISTS sentences(id INTEGER PRIMARY KEY,string_id INTEGER REFERENCES strings(id) ON DELETE CASCADE,ordinal INTEGER,kind TEXT,text TEXT,digest TEXT,UNIQUE(string_id,ordinal));",
    "CREATE TABLE IF NOT EXISTS links(page_id INTEGER REFERENCES pages(id) ON DELETE CASCADE,url TEXT,PRIMARY KEY(page_id,url));",
    "CREATE TABLE IF NOT EXISTS canonical_copy(type TEXT,entry_id INTEGER,namespace TEXT,field TEXT,host TEXT,route TEXT,subject TEXT,raw TEXT,normalized TEXT,digest TEXT,PRIMARY KEY(type,entry_id,namespace,field));",
    "CREATE INDEX IF NOT EXISTS strings_hash_idx ON strings(digest);",
    "CREATE INDEX IF NOT EXISTS sentences_hash_idx ON sentences(digest);"
  ].join("\n"));
  return db;
}
export function copyCanonical(input,output,census){
  const put=output.query("INSERT OR REPLACE INTO canonical_copy(type,entry_id,namespace,field,host,route,subject,raw,normalized,digest) VALUES(?,?,?,?,?,?,?,?,?,?)");
  let count=0;
  output.transaction(()=>{
    for(const type of ["site_page","site_fragment","site_nav","site_door","site_program"]){
      if(!table(input,type))continue;
      const c=columns(input,type);
      if(!c.has("id")||!c.has("namespace")||!c.has("status"))continue;
      for(const row of input.query("SELECT * FROM "+type+" WHERE status='published'").all()){
        const site=census.bySite.get(row.site_id||row.namespace);
        const host=site?.host||"", subject=site?.subject||"", route=row.route||"/";
        for(const field of COPY){
          if(!c.has(field)||typeof row[field]!=="string"||!normalize(row[field]))continue;
          const norm=normalize(row[field]);
          put.run(type,row.id,row.namespace,field,host,route,subject,row[field],norm,hash(norm));count++;
        }
        if(type==="site_program" && typeof row.actor_presentations==="string"){
          try {
            const presentations=JSON.parse(row.actor_presentations);
            for(const [key,value] of Object.entries(presentations)){
              if(typeof value!=="string"||!normalize(value))continue;
              const norm=normalize(value);
              put.run(type,row.id,row.namespace,"actor_presentations."+key,host,route,subject,value,norm,hash(norm));count++;
            }
          } catch {}
        }
      }
    }
  })();
  return count;
}
// Executes in a *public, unauthenticated* browser document. No private DOM snapshots.
export function readVisible(){
  const exclude="script,style,noscript,template,svg,canvas,[hidden],[inert],[aria-hidden='true']";
  const visible=el=>{
    if(!el||el.closest(exclude))return false;
    const style=getComputedStyle(el);
    return style.display!=="none"&&style.visibility!=="hidden"&&style.visibility!=="collapse"&&el.getClientRects().length>0;
  };
  const selector=el=>{
    const parts=[];
    for(let p=el;p&&p!==document.body&&parts.length<7;p=p.parentElement){
      let segment=p.tagName.toLowerCase();
      if(p.id){parts.unshift(segment+"#"+p.id.slice(0,64));break;}
      let index=1;for(let s=p.previousElementSibling;s;s=s.previousElementSibling)if(s.tagName===p.tagName)index++;
      parts.unshift(segment+":nth-of-type("+index+")");
    }
    return parts.join(">");
  };
  const items=[],append=(el,role,raw)=>{
    if(!raw||!raw.trim()||items.length>=15000)return;
    const scope=el.closest("section,article,main,nav,aside,header,footer");
    const href=el.closest("a[href]")?.href||"";
    items.push({role,raw:raw.slice(0,65536),section:scope?.getAttribute("aria-label")||scope?.id||scope?.tagName.toLowerCase()||"",
      node_path:selector(el),anchor_url:/^https?:/.test(href)?href:""});
  };
  const walker=document.createTreeWalker(document.body,NodeFilter.SHOW_TEXT);
  let node,scanned=0;
  while((node=walker.nextNode())&&++scanned<=100000){
    if(visible(node.parentElement))append(node.parentElement,node.parentElement.tagName.toLowerCase(),node.nodeValue);
  }
  for(const el of document.querySelectorAll("[aria-label],[title],[placeholder],img[alt],input[value]")){
    if(!visible(el))continue;
    for(const attr of ["aria-label","title","placeholder","alt"]){const s=el.getAttribute(attr);if(s)append(el,attr,s);}
    if(el.matches("input[type=button],input[type=submit]"))append(el,"value",el.value);
  }
  return {title:document.title,lang:document.documentElement.lang||"en",items,
    links:[...document.querySelectorAll("a[href]")].filter(visible).slice(0,5000).map(a=>a.href)};
}
async function visit(browser,url,timeout){
  const page=await browser.newPage();
  try{
    await page.setViewport({width:1440,height:900});
    await page.setRequestInterception(true);
    page.on("request",req=>{
      let good=false;
      try{good=["GET","HEAD"].includes(req.method())&&!!publicUrl("/",new URL(req.url()).hostname)&&
        !["image","font","media","websocket"].includes(req.resourceType());}catch{}
      void (good?req.continue():req.abort()).catch(()=>{});
    });
    const response=await page.goto(url,{waitUntil:"domcontentloaded",timeout});
    await new Promise(done=>setTimeout(done,175));
    const actual=new URL(page.url());
    if(actual.origin!==new URL(url).origin||!publicUrl(actual.href,actual.hostname))throw Error("Redirect left public surface");
    const content=await page.evaluate(readVisible);
    return {...content,status:response?.status()||0};
  }finally{await page.close();}
}
function retainPage(db,url,site,data,error){
  const u=new URL(url);
  db.transaction(()=>{
    db.query("INSERT INTO pages(url,host,route,site_id,subject,title,http_status,observed_at,digest,error) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(url) DO UPDATE SET title=excluded.title,http_status=excluded.http_status,observed_at=excluded.observed_at,digest=excluded.digest,error=excluded.error")
      .run(url,u.hostname,u.pathname+u.search,site.site_id,site.subject,data?.title||"",data?.status||0,new Date().toISOString(),
        data?hash(data.items.map(item=>item.raw).join("\n")):"",error);
    if(!data)return;
    const pageId=db.query("SELECT id FROM pages WHERE url=?").get(url).id;
    db.query("DELETE FROM sentences WHERE string_id IN (SELECT id FROM strings WHERE page_id=?)").run(pageId);
    db.query("DELETE FROM strings WHERE page_id=?").run(pageId);
    db.query("DELETE FROM links WHERE page_id=?").run(pageId);
    const putString=db.query("INSERT INTO strings(page_id,ordinal,role,section,node_path,raw,normalized,digest,anchor_url) VALUES(?,?,?,?,?,?,?,?,?)");
    const putSentence=db.query("INSERT INTO sentences(string_id,ordinal,kind,text,digest) VALUES(?,?,?,?,?)");
    let pos=0;
    for(const item of data.items){
      const norm=normalize(item.raw);if(!norm)continue;
      const receipt=putString.run(pageId,pos++,item.role,item.section,item.node_path,item.raw,norm,hash(norm),item.anchor_url);
      for(const s of sentences(item.raw,data.lang))putSentence.run(Number(receipt.lastInsertRowid),s.index,s.kind,s.text,hash(s.text));
    }
    const putLink=db.query("INSERT OR IGNORE INTO links(page_id,url) VALUES(?,?)");
    for(const href of data.links){const link=publicUrl(href,u.hostname);if(link)putLink.run(pageId,link);}
  })();
}
export async function run(o){
  if(!existsSync(o.cms))throw Error("Canonical CMS not found: "+o.cms);
  const input=openCanonicalCms(o.cms);
  let output;
  try{
    const census=targets(input);
    const hosts=[...census.hosts.values()].sort((a,b)=>a.host.localeCompare(b.host));
    const selected=o.limitHosts?hosts.slice(0,o.limitHosts):hosts;
    const seeds=selected.reduce((n,site)=>n+site.routes.size,0);
    console.log("Published host census: "+hosts.length+"; selected: "+selected.length+"; seeded routes: "+seeds);
    if(o.dry)return;
    mkdirSync(dirname(o.out),{recursive:true});
    output=openOutput(o.out);
    console.log("Canonical public authored fields indexed: "+copyCanonical(input,output,census));
    if(o.live){
      const require=createRequire(join(ROOT,"../tools/creator-ingest/package.json"));
      const puppeteer=require("puppeteer");
      const browser=await puppeteer.launch({headless:true,executablePath:process.env.CHROME_PATH||
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",args:["--no-sandbox","--disable-dev-shm-usage"]});
      let attempted=0,failed=0;
      try{
        for(const site of selected){
          const queue=[...site.routes].map(route=>({url:publicUrl(route,site.host),depth:0})).filter(item=>item.url);
          const seen=new Set(queue.map(item=>item.url));
          let count=0;
          while(queue.length&&count<o.maxPerHost&&(!o.maxPages||attempted<o.maxPages)){
            const {url,depth}=queue.shift();
            if(!o.refresh && output.query("SELECT 1 FROM pages WHERE url=? AND http_status BETWEEN 200 AND 299 AND error=''").get(url))continue;
            let data=null,error="";
            try{data=await visit(browser,url,o.timeoutMs);
              if(data.status<200||data.status>=300)error="HTTP "+data.status;
            }catch(e){error=String(e.message||e).slice(0,700);}
            retainPage(output,url,site,data,error);
            attempted++;count++;if(error)failed++;
            if(data&&depth<o.maxDepth)for(const href of data.links){
              const next=publicUrl(href,site.host);
              if(next&&!seen.has(next)){seen.add(next);queue.push({url:next,depth:depth+1});}
            }
          }
          if(o.maxPages&&attempted>=o.maxPages)break;
        }
      }finally{await browser.close();}
      console.log("Live pages attempted: "+attempted+"; failed: "+failed+". An attempt is not production verification.");
    }
    for(const [label,sql] of [
      ["Successful live pages","SELECT COUNT(*) AS n FROM pages WHERE http_status BETWEEN 200 AND 299 AND error=''"],
      ["Visible strings","SELECT COUNT(*) AS n FROM strings"],
      ["Sentence and phrase occurrences","SELECT COUNT(*) AS n FROM sentences"],
      ["Canonical copy fields","SELECT COUNT(*) AS n FROM canonical_copy"],
      ["Failed routes","SELECT COUNT(*) AS n FROM pages WHERE error<>''"]
    ])console.log(label+": "+output.query(sql).get().n);
    console.log("SQLite: "+o.out);
  }finally{output?.close();input.close();}
}
if(import.meta.main)run(argsOf()).catch(e=>{console.error(e.stack||e);process.exitCode=1;});
