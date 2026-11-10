// Source-bound Markdown intake for the existing account-owned page composer.
// Never fetches a public address, writes a CMS row, or publishes material.
import {newAuthoringTree,validateAuthoringTree} from "../../../cmd/BonfyreSurfaceScene/runtime/authoring-tree.mjs";
import {openNativeAuthoring,saveNativeAuthoring,toNativePageWrite} from "./native-cms.mjs";

import {sha256Text} from "./sha256-portable.mjs";
const fail=message=>{throw Error("Journal intake: "+message);};
const safeSlug=text=>typeof text==="string"&&/^[a-z0-9][a-z0-9-]{0,70}$/.test(text);
const dateOK=text=>{if(!/^\d{4}-\d{2}-\d{2}$/.test(text))return false;
 const [y,m,d]=text.split("-").map(Number),leap=y%4===0&&(y%100!==0||y%400===0);
 return m>=1&&m<=12&&d>=1&&d<=[31,leap?29:28,31,30,31,30,31,31,30,31,30,31][m-1];};
const normalize=text=>text.replace(/\r\n?/g,"\n");
const sourceName=name=>{
 if(typeof name!=="string"||!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,119}\.md$/i.test(name)||
    name.includes(".."))fail("choose a local Markdown file with a safe filename");
 return name;
};
function frontmatter(text){
 const match=/^---\n([\s\S]*?)\n---(?:\n|$)/.exec(text);
 if(!match)fail("journal Markdown needs a frontmatter title and slug");
 const values={};
 for(const line of match[1].split("\n")){
  if(!line.trim())continue;
  const field=/^([a-z][a-z0-9_-]*):\s*(.*)$/.exec(line);
  if(!field)fail("unsupported frontmatter syntax");
  const [,name,raw]=field;
  if(["slug","title","description","category","published","status","origin"].includes(name)){
   if(Object.hasOwn(values,name))fail("duplicate frontmatter "+name);
   values[name]=raw.replace(/^["']|["']$/g,"").trim();
  }
 }
 return {values,body:text.slice(match[0].length)};
}
function sectionsOf(body,title){
 const entries=[];let heading=title,parts=[];
 function push(){
  const text=parts.join("\n").trim();
  if(text)entries.push({heading,body:text});
  parts=[];
 }
 for(const line of body.split("\n")){
  const header=/^##\s+(.+?)\s*$/.exec(line);
  if(header){push();heading=header[1].replace(/\s+#+$/,"").trim();}
  else parts.push(line);
 }
 push();
 if(!entries.length)fail("article has no content");
 if(entries.length>96||entries.some(x=>!x.heading||x.heading.length>220||
    x.body.length>48000))fail("split the article into shorter sections");
 return entries;
}
export async function parseJournalMarkdown(text,{filename="journal.md"}={}){
 sourceName(filename);
 if(typeof text!=="string"||text.length<20||text.length>180000||
    /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(text))fail("article exceeds safe Markdown limits");
 const clean=normalize(text),{values,body}=frontmatter(clean);
 const title=values.title,slug=values.slug;
 if(typeof title!=="string"||!title.trim()||title.length>220||!safeSlug(slug))
  fail("article needs a readable title and safe slug");
 if(values.published&&!dateOK(values.published))fail("invalid publication date");
 if((values.description||"").length>5000)fail("description too long");
 const sections=sectionsOf(body,title);
 return {schema:"aurekai.journal-intake.v1",title,slug,description:values.description||"",
  published:values.published||"",status:values.status||"",
  filename,source_digest:await sha256Text(clean),sections,
  // The digest proves which locally selected bytes were used, not who authored them.
  source_kind:"local-markdown"};
}
export function mergeJournalIntoTree(tree,journal){
 validateAuthoringTree(tree);
 if(journal?.schema!=="aurekai.journal-intake.v1"||
    !safeSlug(journal.slug)||!/^[a-f0-9]{64}$/.test(journal.source_digest)||
    !Array.isArray(journal.sections)||!journal.sections.length)
  fail("source-bound journal required");
 if(tree.blocks.some(block=>block.props.source_digest===journal.source_digest))
  fail("this article is already present in the draft");
 if(tree.blocks.length+journal.sections.length>512)fail("page block limit exceeded");
 const draft=structuredClone(tree),prefix="journal-"+journal.source_digest.slice(0,20);
 for(const [i,section] of journal.sections.entries()){
  draft.blocks.push({id:prefix+"-"+String(i+1).padStart(3,"0"),kind:"profile-story",
   props:{heading:section.heading,body:section.body,variant:"editorial",
    source_ref:"local-markdown:"+journal.filename,source_digest:journal.source_digest},
   responsive:{},children:[]});
 }
 return validateAuthoringTree(draft);
}
export function journalPageMetadata(journal){
 if(journal?.schema!=="aurekai.journal-intake.v1"||!safeSlug(journal.slug))
  fail("journal metadata required");
 return {title:journal.title,heading:journal.title,lede:journal.description,
  editorial_kind:"article",media_subject_ref:"article:"+journal.slug,
  editorial_published_on:journal.published};
}
/* Bounded, explicit cohort intake. Every destination is checked before the
 * first write. Each native save is independently revision-checked and read back.
 * A partial commit is reported; it is never retried or called a publication. */
export async function saveJournalBatch(articles,{site="",fetcher=fetch,onProgress}={}){
 if(!Array.isArray(articles)||!articles.length||articles.length>32)
  fail("choose between one and 32 journal articles");
 if(typeof site!=="string"||site.length>127||
    (site&&!/^[a-z0-9][a-z0-9-]{0,126}$/.test(site)))
  fail("invalid destination namespace");
 const slugs=new Set(),digests=new Set(),plans=[];
 for(const article of articles){
  const metadata=journalPageMetadata(article);
  if(!/^[a-f0-9]{64}$/.test(article.source_digest||"")||
     !Array.isArray(article.sections)||!article.sections.length)
   fail("complete parsed Markdown source required");
  if(slugs.has(article.slug)||digests.has(article.source_digest))
   fail("duplicate article slug or source; choose each article once");
  slugs.add(article.slug);digests.add(article.source_digest);
  plans.push({article,metadata,route:"/journal/"+article.slug+"/"});
 }
 let namespace="";
 for(const plan of plans){
  const {base}=await openNativeAuthoring({route:plan.route,site,fetcher});
  if(base.id!==0||base.snapshot?.page!==null)
   fail("existing page "+plan.route+" must be edited, never replaced by batch intake");
  const current=base.snapshot.namespace;
  if(site&&current!==site||namespace&&namespace!==current)
   fail("batch namespace changed during preflight");
  namespace=current;
  plan.base=base;
  plan.tree=mergeJournalIntoTree(newAuthoringTree(current,plan.route),plan.article);
  toNativePageWrite(base,plan.tree,{pageDraft:plan.metadata});
 }
 const completed=[];
 for(const plan of plans){
  try{
   const saved=await saveNativeAuthoring({fetcher,base:plan.base,
    tree:plan.tree,pageDraft:plan.metadata,site});
   const entry={route:plan.route,slug:plan.article.slug,
    source_digest:plan.article.source_digest,revision:saved.revision};
   completed.push(entry);
   try{onProgress?.({saved:completed.length,total:plans.length,entry});}
   catch{/* Reporting cannot invalidate a completed owner write. */}
  }catch(cause){
   const error=new Error("Journal batch stopped at "+plan.route+" after "+
    completed.length+" confirmed saves. Reopen pages before retrying.");
   error.completed=completed.slice();error.route=plan.route;error.cause=cause;
   throw error;
  }
 }
 return {schema:"aurekai.journal-batch.v1",namespace,
  imported:completed.length,published:0,completed};
}
