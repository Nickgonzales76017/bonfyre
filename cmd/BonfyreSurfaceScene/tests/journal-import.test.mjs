import {test,expect} from "bun:test";
import {readFileSync,readdirSync} from "node:fs";
import {join} from "node:path";
import {createHash} from "node:crypto";
import {portableSha256,sha256Text} from "../../../components/BonfyreServiceSite/authoring/sha256-portable.mjs";
import {newAuthoringTree} from "../runtime/authoring-tree.mjs";
import {toNativePageWrite,treeFromNativeSnapshot} from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {parseJournalMarkdown,mergeJournalIntoTree,journalPageMetadata,saveJournalBatch}
 from "../../../components/BonfyreServiceSite/authoring/journal-import.mjs";

const posts=join(import.meta.dir,"../../../editorial/posts");
const base=(site,route)=>({schema:"bonfyre.page-snapshot.v1",digest:"b".repeat(64),id:0,
 snapshot:{namespace:site,route,page:null,fragments:[]}});

test("all sixteen real journal posts enter native, isolated, editable profile drafts",async()=>{
 const files=readdirSync(posts).filter(name=>name.endsWith(".md")).sort();
 expect(files.length).toBe(16);
 const digests=new Set();
 for(const filename of files){
  const raw=readFileSync(join(posts,filename),"utf8");
  const article=await parseJournalMarkdown(raw,{filename});
  const route="/blog/"+article.slug+"/",site="person-nick-life";
  expect(article.sections.length).toBeGreaterThan(0);
  expect(article.source_digest).toMatch(/^[a-f0-9]{64}$/);
  expect(digests.has(article.source_digest)).toBe(false);
  digests.add(article.source_digest);
  const draft=mergeJournalIntoTree(newAuthoringTree(site,route),article);
  const pageDraft=journalPageMetadata(article);
  expect(pageDraft.media_subject_ref).toBe("article:"+article.slug);
  expect(pageDraft.title).toBe(article.title);
  expect(pageDraft.eyebrow).toBe(article.category);
  expect(article.icon).toMatch(/^[a-z0-9-]+$/);
  const request=toNativePageWrite(base(site,route),draft,{pageDraft});
  expect(request.operation).toBe("create");
  expect(request.page.status).toBe("draft");
  expect(request.fragments.every(x=>x.source_digest===article.source_digest)).toBe(true);
  expect(request.fragments.map(x=>x.body).join("\n")).toContain(article.sections[0].body);
  const saved={...base(site,route),id:1,snapshot:{namespace:site,route,
   page:{...request.page},fragments:request.fragments}};
  const restored=treeFromNativeSnapshot(saved);
  expect(restored.blocks.map(x=>x.props.body)).toEqual(draft.blocks.map(x=>x.props.body));
  expect(restored.blocks.map(x=>x.props.source_digest)).toEqual(draft.blocks.map(x=>x.props.source_digest));
  expect(()=>mergeJournalIntoTree(draft,article)).toThrow(/already present/);
 }
 expect(digests.size).toBe(16);
});

test("an import cannot publish or erase another page's authored blocks",async()=>{
 const article=await parseJournalMarkdown("---\nslug: first-story\ntitle: First story\n---\n\nHello world.\n");
 const original=newAuthoringTree("person-nick-life","/blog/");
 original.blocks.push({id:"existing",kind:"profile-card",props:{heading:"Retained"},children:[]});
 const next=mergeJournalIntoTree(original,article);
 expect(original.blocks).toHaveLength(1);
 expect(next.blocks).toHaveLength(2);
 expect(next.blocks[0]).toEqual(original.blocks[0]);
 expect(toNativePageWrite(base(original.site_id,original.page),next,
  {pageDraft:journalPageMetadata(article)}).page.status).toBe("draft");
 expect(()=>mergeJournalIntoTree(newAuthoringTree("another-person","/blog/"),
  {...article,source_digest:"bad"})).toThrow();
});

test("unsafe filenames, false dates, duplicate metadata and oversized sections are rejected",async()=>{
 for(const filename of ["../bad.md","a/evil.md","other.html"])
  await expect(parseJournalMarkdown("---\nslug: story\ntitle: Story\n---\nHi there.",{filename})).rejects.toThrow();
 for(const content of [
  "---\nslug: story\ntitle: Story\npublished: 2026-02-29\n---\nContent",
  "---\nslug: ../bad\ntitle: Story\n---\nContent",
  "---\nslug: story\ntitle: Story\ntitle: Duplicate\n---\nContent",
  "---\nslug: story\ntitle: Story\ncategory: <script>\n---\nContent",
  "---\nslug: story\ntitle: Story\nicon: ../briefcase\n---\nContent",
  "---\nslug: story\ntitle: Story\n---\n"+("x".repeat(50000))
 ])await expect(parseJournalMarkdown(content)).rejects.toThrow();
});

test("batch intake opens all sixteen routes before committing account-owned journal drafts",async()=>{
 const namespace="person-journal-life",stored=new Map(),calls=[];
 const files=readdirSync(posts).filter(x=>x.endsWith(".md")).sort();
 const articles=await Promise.all(files.map(async filename=>
  parseJournalMarkdown(readFileSync(join(posts,filename),"utf8"),{filename})));
 const fetcher=async(url,options={})=>{
  if(options.method==="POST"){
   const req=JSON.parse(options.body);calls.push({kind:"save",route:req.route});
   expect(req.operation).toBe("create");
   expect(req.expected_digest).toBe("b".repeat(64));
   expect(req.page.status).toBe("draft");
   expect(req.fragments.every(x=>x.status==="draft")).toBe(true);
   if(stored.has(req.route))return {ok:false,status:409,json:async()=>({})};
   stored.set(req.route,{schema:"bonfyre.page-snapshot.v1",
    id:stored.size+1,digest:"c".repeat(64),snapshot:{namespace,route:req.route,
     page:{...req.page,id:stored.size+1},fragments:req.fragments}});
   return {ok:true,json:async()=>({committed:true})};
  }
  const parsed=new URL(url,"https://aurekai.work"),route=parsed.searchParams.get("route");
  expect(parsed.searchParams.get("site")).toBe(namespace);
  calls.push({kind:"read",route});
  return {ok:true,json:async()=>structuredClone(stored.get(route)||base(namespace,route))};
 };
 const progress=[];
 const done=await saveJournalBatch(articles,{site:namespace,fetcher,
  onProgress:p=>progress.push(p.saved)});
 expect(done.imported).toBe(16);
 expect(done.published).toBe(0);
 expect(stored.size).toBe(16);
 expect(progress).toEqual(Array.from({length:16},(_,i)=>i+1));
 expect(calls.slice(0,16).every(x=>x.kind==="read")).toBe(true);
 expect(calls.filter(x=>x.kind==="save")).toHaveLength(16);
 for(const article of articles){
  const page=stored.get("/blog/"+article.slug+"/").snapshot;
  expect(page.page.media_subject_ref).toBe("article:"+article.slug);
  expect(page.page.status).toBe("draft");
  expect(page.fragments.every(x=>x.source_digest===article.source_digest)).toBe(true);
 }
 const before=calls.length;
 await expect(saveJournalBatch(articles.slice(0,2),{site:namespace,fetcher}))
  .rejects.toThrow(/existing page/);
 expect(calls.slice(before).every(x=>x.kind==="read")).toBe(true);
});
test("batch refuses duplicates, existing routes, wrong account and partial uncertainty without retry",async()=>{
 const mk=async slug=>parseJournalMarkdown("---\nslug: "+slug+
  "\ntitle: Journal "+slug+"\n---\n\nA source-backed paragraph.\n");
 const first=await mk("first"),second=await mk("second");
 let writes=0,reads=0;
 const fetcher=async(url,options={})=>{
  if(options.method==="POST"){
   writes++;
   if(writes===2)return {ok:false,status:409,json:async()=>({error:"stale"})};
   const request=JSON.parse(options.body);
   return {ok:true,json:async()=>({committed:true})};
  }
  reads++;
  const route=new URL(url,"https://aurekai.work").searchParams.get("route");
  const empty=base("person-editor-life",route);
  if(route.includes("occupied")){empty.id=2;empty.snapshot.page={route,status:"draft"};}
  return {ok:true,json:async()=>empty};
 };
 await expect(saveJournalBatch([first,first],{fetcher})).rejects.toThrow(/duplicate/);
 expect(reads).toBe(0);
 await expect(saveJournalBatch([first,{...second,slug:"occupied"}],{fetcher}))
  .rejects.toThrow(/existing page/);
 expect(writes).toBe(0);
 await expect(saveJournalBatch([first],{site:"person-other-work",fetcher}))
  .rejects.toThrow(/namespace changed/);
 expect(writes).toBe(0);
 // A confirmed first write followed by a failed second readback is surfaced
 // as uncertain; the API does not issue a second write or claim atomic rollback.
 const commits=new Map();
 const partial=async(url,opts={})=>{
  if(opts.method==="POST"){
   const row=JSON.parse(opts.body);writes++;
   if(writes===2)return {ok:false,status:409,json:async()=>({error:"collision"})};
   commits.set(row.route,{schema:"bonfyre.page-snapshot.v1",id:1,
    digest:"c".repeat(64),snapshot:{namespace:"person-editor-life",route:row.route,
     page:{...row.page,id:1},fragments:row.fragments}});
   return {ok:true,json:async()=>({committed:true})};
  }
  const route=new URL(url,"https://aurekai.work").searchParams.get("route");
  return {ok:true,json:async()=>structuredClone(commits.get(route)||base("person-editor-life",route))};
 };
 try{
  await saveJournalBatch([first,second],{fetcher:partial});
  throw Error("Expected bounded partial failure");
 }catch(error){
  expect(error.completed).toHaveLength(1);
  expect(error.route).toBe("/blog/second/");
 }
 expect(writes).toBe(2);
 expect(commits.size).toBe(1);
});

test("portable SHA-256 equals native digest across Unicode, padding and long journal sources",async()=>{
 for(const text of ["","abc","é漢字🦊","a".repeat(55),"a".repeat(56),"a".repeat(64),"b".repeat(100005),
  readFileSync(join(posts,"a-council-agenda-can-be-useful.md"),"utf8")]){
  const bytes=new TextEncoder().encode(text);
  const expected=createHash("sha256").update(bytes).digest("hex");
  expect(portableSha256(bytes)).toBe(expected);
  expect(await sha256Text(text)).toBe(expected);
 }
});


test("a ninety-article cohort retains long slugs and preflights before writes",async()=>{
 const namespace="person-journal-life",stored=new Map(),calls=[];
 const articles=await Promise.all(Array.from({length:90},(_,i)=>{
  const slug=i===0?"a".repeat(90):"journal-"+i;
  return parseJournalMarkdown("---\nslug: "+slug+"\ntitle: Article "+i+"\n---\n\nPassage "+i+".\n");
 }));
 const fetcher=async(url,options={})=>{
  if(options.method==="POST"){
   const row=JSON.parse(options.body);calls.push("save");
   stored.set(row.route,{schema:"bonfyre.page-snapshot.v1",id:stored.size+1,digest:"c".repeat(64),
    snapshot:{namespace,route:row.route,page:{...row.page,id:stored.size+1},fragments:row.fragments}});
   return {ok:true,json:async()=>({committed:true})};
  }
  calls.push("read");
  const route=new URL(url,"https://aurekai.work").searchParams.get("route");
  return {ok:true,json:async()=>structuredClone(stored.get(route)||base(namespace,route))};
 };
 const done=await saveJournalBatch(articles,{site:namespace,fetcher});
 expect(done.imported).toBe(90);expect(done.published).toBe(0);
 expect(stored.size).toBe(90);expect(calls.slice(0,90).every(x=>x==="read")).toBe(true);
 expect(stored.has("/blog/"+"a".repeat(90)+"/")).toBe(true);
 await expect(parseJournalMarkdown("---\nslug: "+"a".repeat(97)+"\ntitle: Too long\n---\n\nPassage.\n")).rejects.toThrow();
 await expect(saveJournalBatch(Array(129).fill(articles[0]),{site:namespace,fetcher})).rejects.toThrow(/128/);
});
