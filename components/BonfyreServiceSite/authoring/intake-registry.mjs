// Multi-kind intake registry: WordPress, RSS/Atom, GeoJSON, Frappe AppPack
// and Ghost sources all normalize to aurekai.journal-intake.v1 journals,
// which ride the existing merge -> page-write -> promote -> /blog path.
// Every parser is bounded, deterministic and source-hashed; feeds digest
// to a declared top-N rather than refusing large but ordinary inputs.
import {createHash} from "node:crypto";
// Node-side registry (reads dump files), so node:crypto rather than the
// bounded portable hasher: same SHA-256, no 200 KB ceiling on sources.
const sha256Text = async text => createHash("sha256").update(text, "utf8").digest("hex");
const fail=s=>{throw Error("Intake registry: "+s);};
const safeSlug=t=>typeof t==="string"&&/^[a-z0-9][a-z0-9-]{0,70}$/.test(t);
const slugify=t=>String(t??"").toLowerCase().normalize("NFKD")
 .replace(/[\u0300-\u036f]/g,"").replace(/[^a-z0-9]+/g,"-").replace(/^-+|-+$/g,"").slice(0,60);
const clean=t=>String(t??"").replace(/\r\n?/g,"\n");
const decodeEntities=t=>String(t??"").replace(/&(#[0-9]+|#x[0-9a-fA-F]+|[a-zA-Z]+);/g,(m,e)=>{
 if(e[0]==="#"){const n=e[1]==="x"||e[1]==="X"?parseInt(e.slice(2),16):parseInt(e.slice(1),10);
  if(Number.isSafeInteger(n)&&n>0&&n<0x110000&&!(n>=0xd800&&n<=0xdfff))return String.fromCodePoint(n);return m;}
 return {amp:"&",lt:"<",gt:">",quot:'"',apos:"'",nbsp:"\u00a0"}[e]??m;});
const stripHtml=t=>decodeEntities(String(t??"").replace(/<script[\s\S]*?<\/script>/gi," ")
 .replace(/<style[\s\S]*?<\/style>/gi," ").replace(/<[^>]*>/g," "))
 .replace(/[ \t]+\n/g,"\n").replace(/\n{3,}/g,"\n\n").trim();
function sectionsOf(parts,title){
 if(!Array.isArray(parts)||!parts.length)fail("article has no content");
 if(parts.length>96)fail("split the article into shorter sections");
 return parts.map((p,i)=>{
  const heading=(p.heading||(i===0?title:"")).trim(),body=(p.body||"").trim();
  if(!heading||heading.length>220||!body||body.length>48000)
   fail("split the article into shorter sections");
  return {heading,body};
 });
}
async function journal({title,slug,description,category,published,sections,filename,source,source_kind}){
 if(typeof title!=="string"||!title.trim()||title.length>220||!safeSlug(slug))
  fail("article needs a readable title and safe slug");
 if((description||"").length>5000)fail("description too long");
 if(category&&!/^[A-Za-z0-9][A-Za-z0-9 .&'-]{0,59}$/.test(category))fail("invalid article category");
 if(published&&!/^\d{4}-\d{2}-\d{2}$/.test(published))fail("invalid publication date");
 return {schema:"aurekai.journal-intake.v1",title:title.trim(),slug,
  description:(description||"").trim(),published:published||"",category:category||"",icon:"",
  filename,source_digest:await sha256Text(source),sections,source_kind};
}
const FIELD_TYPES=["Unit","String","Int","Float","Currency","Date","Datetime","Time","Text","Long Text","Markdown","HTML","Select","Link","Dynamic Link","Table","Table MultiSelect","Attach","Attach Image","Check","Password","Data","Barcode","Button","Code","Color","Column Break","Duration","Geolocation","Heading","Icon","Image","JSON","Rating","Read Only","Section Break","Signature","Small Text","Tab Break","Text Editor"];
export async function parseWordPressNormalized(text,{filename="post.txt",slug}={}){
 if(typeof text!=="string"||text.length<20||text.length>180000)fail("article exceeds safe Markdown limits");
 const lines=clean(text).split("\n");
 const title=(lines.find(l=>l.trim())||"").replace(/^#\s+/,"").trim();
 const id=slug||(title?"wp-"+slugify(title):"");
 if(!safeSlug(id))fail("article needs a readable title and safe slug");
 const parts=[];let heading=title,bulb=[];
 const push=()=>{const body=bulb.join("\n").trim();if(body)parts.push({heading,body});bulb=[];};
 let first=true;
 for(const line of lines.slice(1)){
  const h=/^#{1,3}\s+(.+?)\s*$/.exec(line);
  if(h){if(first&&!bulb.join("").trim()){heading=h[1];first=false;continue;}push();heading=h[1];}
  else bulb.push(line);
  first=false;
 }
 push();
 return journal({title,slug:id,description:parts[0]?parts[0].body.split("\n\n")[0].slice(0,500):"",
  category:"WordPress",sections:sectionsOf(parts,title),filename,source:clean(text),source_kind:"wordpress-normalized"});
}
function rssItems(xml){
 const items=[];
 for(const m of clean(xml).matchAll(/<item\b[^>]*>([\s\S]*?)<\/item>/gi)){
  const g=(re)=>{const f=re.exec(m[1]);return f?stripHtml(f[1]).slice(0,8000):"";};
  const title=g(/<title\b[^>]*>([\s\S]*?)<\/title>/i);
  if(!title)continue;
  items.push({title:title.slice(0,220),
   link:(g(/<link\b[^>]*>([\s\S]*?)<\/link>/i)||"").split(/\s/)[0].slice(0,1400),
   date:g(/<pubDate\b[^>]*>([\s\S]*?)<\/pubDate>/i).slice(0,40),
   category:(g(/<category\b[^>]*>([\s\S]*?)<\/category>/i)||"").slice(0,60),
   body:(g(/<content:encoded\b[^>]*>([\s\S]*?)<\/content:encoded>/i)||g(/<description\b[^>]*>([\s\S]*?)<\/description>/i)).slice(0,48000)});
  if(items.length>=24)break;
 }
 return items;
}
const rssDate=s=>{const d=new Date(s);return Number.isNaN(d.getTime())?"":d.toISOString().slice(0,10);};
export async function parseRssFeed(xml,{filename="feed.xml",slug}={}){
 if(typeof xml!=="string"||xml.length<50||xml.length>2000000)fail("feed exceeds safe limits");
 if(!/<(rss|feed)\b/i.test(xml))fail("not a recognized feed");
 const text=clean(xml);
 const channel=(stripHtml((/<channel\b[^>]*>([\s\S]*?)<\/channel>/i.exec(text)?.[1]||"").match(/<title\b[^>]*>([\s\S]*?)<\/title>/i)?.[1]||"")||"Feed").slice(0,220) || "Feed";
 const items=rssItems(text);
 if(!items.length)fail("feed has no readable items");
 const total=(text.match(/<item\b/gi)||[]).length;
 const id=slug||("rss-"+slugify(channel));
 if(!safeSlug(id))fail("article needs a readable title and safe slug");
 const parts=items.map(it=>({heading:it.title,
  body:[it.link, it.date, "", it.body].filter((x,i)=>x||i===2).join("\n").trim()||"No summary retained."}));
 return journal({title:channel,slug:id,description:`${total} observed, digesting ${items.length}.`,
  category:"Feed",published:rssDate(items[0].date),
  sections:sectionsOf(parts,channel),filename,source:text,source_kind:"rss-feed"});
}
export async function parseGeoJsonFeed(json,{filename="feed.geojson",slug}={}){
 let doc;try{doc=JSON.parse(json);}catch{fail("not a recognized feed");}
 if(doc?.type!=="FeatureCollection"||!Array.isArray(doc.features)||!doc.features.length)
  fail("not a recognized feed");
 const feats=doc.features.filter(f=>f?.properties&&typeof f.properties.mag==="number"&&f.properties.place)
  .sort((a,b)=>b.properties.mag-a.properties.mag).slice(0,24);
 if(!feats.length)fail("feed has no readable items");
 const title=String(doc.metadata?.title||"Earthquake digest").slice(0,220)||"Earthquake digest";
 const id=slug||("usgs-"+slugify(title));
 if(!safeSlug(id))fail("article needs a readable title and safe slug");
 const parts=feats.map(f=>{const p=f.properties;
  const when=typeof p.time==="number"?new Date(p.time).toISOString():"";
  return {heading:`M${p.mag} \u00b7 ${String(p.place).slice(0,180)}`,
   body:[when?`Observed ${when.slice(0,10)} ${when.slice(11,16)} UTC`:"",p.url?String(p.url).slice(0,1400):"",
    typeof p.felt==="number"&&p.felt>0?`${p.felt} felt reports`:""].filter(Boolean).join("\n")||"No detail retained."};});
 const pub0=typeof feats[0].properties.time==="number"?new Date(feats[0].properties.time).toISOString().slice(0,10):"";
 return journal({title,slug:id,description:`${doc.features.length} observed, digesting the ${feats.length} largest.`,
  category:"Feed",published:/^\d{4}-\d{2}-\d{2}$/.test(pub0)?pub0:"",
  sections:sectionsOf(parts,title),filename,source:clean(json),source_kind:"geojson-feed"});
}
export async function parseFrappeAppPack(json,{filename="apppack.json",slug}={}){
 let doc;try{doc=JSON.parse(json);}catch{fail("not a recognized app pack");}
 const doctypes=doc?.schema?.doctypes;
 if(doc?.kind!=="AppPack"||!Array.isArray(doctypes)||!doctypes.length)fail("not a recognized app pack");
 const app=String(doc.app||"frappe").slice(0,60);
 const id=slug||("frappe-"+slugify(app)+"-surfaces");
 if(!safeSlug(id))fail("article needs a readable title and safe slug");
 const kept=doctypes.filter(d=>d?.name&&Array.isArray(d.fields)).slice(0,24);
 if(!kept.length)fail("app pack has no readable doctypes");
 const parts=kept.map(d=>{
  const rows=d.fields.filter(f=>f?.label).slice(0,60).map(f=>{
   const t=FIELD_TYPES[f.type]||`type ${f.type}`;
   return `- ${String(f.label).slice(0,80)} (${t}${f.required?", required":""}${f.options&&f.type===13?` \u2192 ${String(f.options).split("\n")[0].slice(0,60)}`:""})`;});
  return {heading:String(d.name).slice(0,220),body:rows.join("\n").slice(0,48000)||"No fields retained."};});
 return journal({title:`${app} surfaces`,slug:id,
  description:`${doctypes.length} doctypes observed, digesting ${kept.length}.`,
  category:"Frappe",sections:sectionsOf(parts,`${app} surfaces`),filename,source:clean(json),source_kind:"frappe-apppack"});
}
export async function parseGhostPost(json,{filename="post.json",slug}={}){
 let doc;try{doc=JSON.parse(json);}catch{fail("not a recognized Ghost post");}
 const post=Array.isArray(doc?.posts)?doc.posts[0]:doc;
 if(!post||typeof post.title!=="string"||typeof post.plaintext!=="string")fail("not a recognized Ghost post");
 const id=slug||(post.slug&&safeSlug(post.slug)?post.slug:"ghost-"+slugify(post.title));
 if(!safeSlug(id))fail("article needs a readable title and safe slug");
 const body=clean(post.plaintext).slice(0,48000).trim();
 if(!body)fail("article has no content");
 const tag=Array.isArray(post.tags)&&typeof post.tags[0]?.name==="string"?post.tags[0].name:"";
 const pub=typeof post.published_at==="string"?post.published_at.slice(0,10):"";
 return journal({title:post.title.slice(0,220),slug:id,description:String(post.excerpt||"").slice(0,5000),
  category:tag,icon:"",published:/^\d{4}-\d{2}-\d{2}$/.test(pub)?pub:"",
  sections:sectionsOf([{heading:post.title.slice(0,220),body}],post.title),filename,source:clean(json),source_kind:"ghost-post"});
}
export function detectIntakeKind(bytes,filename=""){
 const text=clean(bytes);
 if(/^---\n[\s\S]*?\n---(\n|$)/.test(text))return "local-markdown";
 const head=text.trimStart().slice(0,2000);
 if(head.startsWith("{")||head.startsWith("[")){
  let doc;try{doc=JSON.parse(text);}catch{return "unknown";}
  if(doc?.type==="FeatureCollection")return "geojson-feed";
  if(doc?.kind==="AppPack")return "frappe-apppack";
  if(Array.isArray(doc?.posts)||(doc?.title&&doc?.plaintext))return "ghost-post";
  return "unknown";
 }
 if(/<(rss|feed)\b/i.test(head))return "rss-feed";
 if(/wp-block-|wp-image-/i.test(text))return "wordpress-html";
 if(/^#\s+\S/m.test(text)&&!/^#{4,}/m.test(text))return "wordpress-normalized";
 void filename;return "unknown";
}
export async function parseIntake(kind,bytes,opts={}){
 switch(kind){
  case "wordpress-html":fail("parse the receipted normalized.txt, not raw block HTML");
  case "wordpress-normalized":return parseWordPressNormalized(bytes,opts);
  case "rss-feed":return parseRssFeed(bytes,opts);
  case "geojson-feed":return parseGeoJsonFeed(bytes,opts);
  case "frappe-apppack":return parseFrappeAppPack(bytes,opts);
  case "ghost-post":return parseGhostPost(bytes,opts);
  default:fail("unknown intake kind "+kind);
 }
}
export default {detectIntakeKind,parseIntake,parseWordPressNormalized,parseRssFeed,parseGeoJsonFeed,parseFrappeAppPack,parseGhostPost};
