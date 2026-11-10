/* Exact native CMS article revision -> destination-owned narration draft.
 * This is an explicit user action, never a publish, render or rights grant.
 */
import {openNativeAuthoring} from "./native-cms.mjs";
import {createProduction,productionDigest,validateProduction}
 from "../../../integrations/browser/narration-production.mjs";
import {openDestinationStore}
 from "../../../integrations/browser/workspace-store.mjs";
const hash=/^[a-f0-9]{64}$/;
const subject=/^article:[a-z0-9][a-z0-9._-]{0,95}$/;
const app=/^\/apps\/(ws-[a-f0-9]{40})\/audio\/$/;
const plain=text=>String(text||"")
 .replace(/\x60{3}[\s\S]*?\x60{3}/g," ")
 .replace(/<[^>]+>/g," ")
 .replace(/!?\[([^\]]+)\]\(https?:\/\/[^)]+\)/g,"$1")
 .replace(/(^|\n)\s{0,3}#{1,6}\s+/g,"$1")
 .replace(/(^|\n)\s{0,3}[-*+]\s+/g,"$1")
 .replace(/[\x60*_~]/g,"")
 .replace(/\r/g,"")
 .replace(/[ \t]{2,}/g," ")
 .trim();
const narrationKinds=new Set(["profile-story","service:prose","ui:editorial-story"]);
const editedNarration=row=>{
 let tags=row.semantic_tags;
 if(typeof tags==='string'){try{tags=JSON.parse(tags);}catch{throw Error('Invalid retained article editorial data.');}}
 const blocks=tags?.authoring_v1?.editorial;
 if(blocks==null||Array.isArray(blocks)&&!blocks.length)return row.body||'';
 if(!Array.isArray(blocks)||blocks.length>256)throw Error('The edited article is not in a supported form.');
 let inspected=0;
 const nested=(values,depth=0)=>{
  if(depth>8||++inspected>1024)throw Error('Editorial article text exceeds narration limits.');
  if(typeof values==='string')return values;
  if(!Array.isArray(values))return '';
  return values.map(v=>typeof v==='string'?v:
   v&&typeof v==='object'?[v.content||v.text||'',nested(v.items||[],depth+1)].filter(Boolean).join('. '):'')
   .filter(Boolean).join('. ');
 };
 return blocks.map(block=>{
  if(!block||!['paragraph','header','quote','list','checklist','table','code','delimiter'].includes(block.type))
   throw Error('The article includes an unsupported editorial block.');
  const data=block.data||{};
  if(block.type==='delimiter')return '';
  if(block.type==='list'||block.type==='checklist')return nested(data.items||[]);
  if(block.type==='table')return (data.content||[]).map(row=>Array.isArray(row)?row.join('. '):'').join('. ');
  return data.text||data.code||'';
 }).filter(Boolean).join('\n\n');
};
const pageURL=({origin,route,site})=>{
 const url=new URL("/cms/compose/advanced",origin);
 if(!["http:","https:"].includes(url.protocol)||url.username||url.password)
  throw Error("The authored article needs a current browser origin.");
 url.searchParams.set("route",route);
 if(site)url.searchParams.set("site",site);
 return url.href;
};
export async function planArticleNarration({base,origin,site="",nativeRevision=null}){
 const snap=base?.snapshot,page=snap?.page;
 if(!Number.isSafeInteger(base?.id)||base.id<1||!hash.test(base?.digest)||
    !snap||typeof snap.namespace!=="string"||!snap.namespace||
    !/^\/(?:[a-z0-9-]+\/?)*$/.test(snap.route||"")||!page||
    page.route!==snap.route||!subject.test(page.media_subject_ref||"")||
    !["article","essay","report","feature","longform"].includes(page.editorial_kind))
  throw Error("Save an identified article in native CMS before creating its audio edition.");
 if(!["draft","published"].includes(page.status))
  throw Error("The selected article revision is not available for narration.");
 if(!Array.isArray(snap.fragments)||snap.fragments.length>512)
  throw Error("The article needs a complete bounded native CMS snapshot.");
 const blocks=snap.fragments.filter(row=>row.page===snap.route&&
   row.slot==="showcase"&&row.audience==="public"&&narrationKinds.has(row.kind)&&
   row.status===page.status&&
   (typeof row.body==='string'||row.semantic_tags!=null))
  .sort((a,b)=>a.position-b.position||String(a.coordinate).localeCompare(String(b.coordinate)));
 const segments=[];
 const intro=plain([page.heading||page.title,page.lede||""].filter(Boolean).join("\n\n"));
 if(intro.length>=12)segments.push({id:"opening",text:intro});
 for(let i=0;i<blocks.length;i++){
  const text=plain([blocks[i].heading,editedNarration(blocks[i])].filter(Boolean).join("\n\n"));
  if(text.length<12)continue;
  if(text.length>12000)throw Error("An article passage exceeds 12,000 characters. Edit it into shorter sections.");
  segments.push({id:"passage-"+String(i+1).padStart(3,"0"),text});
 }
 if(segments.length<2||segments.length>128||
    segments.some(x=>x.text.length>12000)||
    segments.reduce((n,x)=>n+x.text.length,0)>100000)
  throw Error("The saved article needs readable story passages within narration limits.");
 if(nativeRevision!==null&&(!hash.test(nativeRevision.sha256||"")||
    nativeRevision.schema!=="bonfyre.article-revision.v2"||
    nativeRevision.namespace!==snap.namespace||nativeRevision.route!==snap.route||
    nativeRevision.subject!==page.media_subject_ref||nativeRevision.status!==page.status))
  throw Error("The native article revision does not match the saved page.");
 const revision=base.digest;
 const sourceHref=pageURL({origin,route:snap.route,site});
 const identity=await productionDigest({namespace:snap.namespace,route:snap.route,
  subject:page.media_subject_ref,revision});
 const key="draft:creator:narration:"+identity;
 const production=createProduction({id:"article-"+identity.slice(0,32),
  title:(page.title||page.heading||"Untitled article").slice(0,200),
  items:[{id:"article",title:(page.heading||page.title).slice(0,300),
   source:{sha256:revision,href:sourceHref},segments}],
  defaults:{rate:140,gapMs:450,gain:0.85}});
 production.provenance={schema:"bonfyre.authored-article-handoff.v1",
  sourceNamespace:snap.namespace,sourceRoute:snap.route,
  subjectRef:page.media_subject_ref,sourceDigest:revision,
  sourceKind:page.editorial_kind,sourceStatus:page.status,
  ...(nativeRevision?{nativeRevision:{schema:nativeRevision.schema,sha256:nativeRevision.sha256}}:{})};
 production.sourcePermission="unreviewed";
 validateProduction(production);
 return {key,identity,production};
}
const sameSourceItems=(a,b)=>JSON.stringify(a?.items)===JSON.stringify(b?.items);
export async function saveArticleNarration({base,site="",origin,editorHref,fetcher=fetch,
 storeFactory=openDestinationStore}={}){
 const match=app.exec(editorHref||"");
 if(!match)throw Error("Choose the currently installed Audio Studio for this account.");
 const route=base?.snapshot?.route;
 if(typeof route!=="string")throw Error("The saved article route is missing.");
 const current=await openNativeAuthoring({route,site,fetcher});
 if(current.base.digest!==base.digest||current.base.id!==base.id||
    current.base.snapshot?.namespace!==base.snapshot?.namespace)
  throw Error("The article changed since the last save. Reopen it before preparing narration.");
 const revisionURL=new URL("/cms/compose/narration-revision",origin);
 revisionURL.searchParams.set("route",route);
 if(site)revisionURL.searchParams.set("site",site);
 const response=await fetcher(revisionURL.href,{credentials:"same-origin",cache:"no-store",redirect:"error",headers:{Accept:"application/json"}});
 if(!response.ok)throw Error("The native article revision is unavailable. Reopen the saved article.");
 const nativeRevision=await response.json();
 if(!nativeRevision||typeof nativeRevision!=="object"||Array.isArray(nativeRevision))
  throw Error("The native article revision is unavailable. Reopen the saved article.");
 const plan=await planArticleNarration({base:current.base,site,origin,nativeRevision});
 const store=storeFactory("/apps/"+match[1]+"/_records");
 const existing=await store.readVersion(plan.key);
 const reread=await openNativeAuthoring({route,site,fetcher});
 if(reread.base.digest!==base.digest)
  throw Error("Article content changed while narration was prepared. No draft was created.");
 if(existing){
  if(existing.deleted||
     existing.value?.schema!=="aurekai.narration-production.v1"||
     existing.value.provenance?.schema!=="bonfyre.authored-article-handoff.v1"||
     existing.value.provenance.sourceDigest!==base.digest||
     existing.value.provenance.subjectRef!==plan.production.provenance.subjectRef||
     existing.value.provenance.nativeRevision?.sha256!==nativeRevision.sha256||
     !sameSourceItems(existing.value,plan.production))
   throw Error("The narration production was changed or withdrawn. Review it in Audio Studio.");
  return {key:plan.key,created:false,revision:existing.revision,
    href:editorHref+"?production="+plan.identity};
 }
 const saved=await store.saveLocal(plan.key,plan.production,0);
 const confirmed=await store.readVersion(plan.key);
 if(!confirmed||confirmed.deleted||confirmed.value?.schema!=="aurekai.narration-production.v1"||
    confirmed.value.provenance?.sourceDigest!==base.digest||
    confirmed.value.provenance?.nativeRevision?.sha256!==nativeRevision.sha256||
    !sameSourceItems(confirmed.value,plan.production))
  throw Error("The Audio Studio did not confirm its narration draft. Reconcile the destination before retrying.");
 return {key:plan.key,created:true,revision:saved?.revision||confirmed.revision,
  href:editorHref+"?production="+plan.identity};
}
