import {test,expect} from "bun:test";
import {planArticleNarration,saveArticleNarration}
 from "../../../components/BonfyreServiceSite/authoring/article-narration.mjs";
const digest=()=> "d".repeat(64);
const href="/apps/ws-"+"e".repeat(40)+"/audio/";
const article=()=>({
 id:101,digest:digest(),schema:"bonfyre.page-snapshot.v1",
 snapshot:{
  namespace:"person-test-work",route:"/blog/council-agenda/",
  page:{route:"/blog/council-agenda/",status:"draft",title:"A council agenda can be useful",
   heading:"A council agenda can be useful",lede:"A local council meeting can make public records easier to follow.",
   editorial_kind:"article",media_subject_ref:"article:council-agenda"},
  fragments:[
   {page:"/blog/council-agenda/",slot:"showcase",audience:"public",kind:"profile-story",coordinate:"narration-one",
    heading:"The agenda",body:"The agenda lists public decisions and the materials residents can review.",
    position:2,status:"draft",semantic_tags:{}},
   {page:"/blog/council-agenda/",slot:"showcase",audience:"public",kind:"profile-story",coordinate:"narration-two",
    heading:"What changed",body:"A transcript and a recording can explain how the meeting actually unfolded.",
    position:3,status:"draft",semantic_tags:{}},
   {page:"/blog/council-agenda/",slot:"showcase",audience:"public",kind:"profile-metric",coordinate:"not-article",
    body:"$28,000",position:1,status:"draft",semantic_tags:{}}
  ]
 }
});
const origin="https://aurekai.work";
const revisionFor=base=>({schema:"bonfyre.article-revision.v2",namespace:base.snapshot.namespace,
 route:base.snapshot.route,subject:base.snapshot.page.media_subject_ref,status:base.snapshot.page.status,sha256:"a".repeat(64)});
const fetchFor=(base,reads=[])=>async url=>{reads.push(url);return {ok:true,json:async()=>
 structuredClone(String(url).includes("/cms/compose/narration-revision")?revisionFor(base):base)};};
test("source-bound audio uses all saved article passages, stable identity, and a private edit address",async()=>{
 const base=article();
 const a=await planArticleNarration({base,origin});
 const b=await planArticleNarration({base,origin});
 expect(a.key).toBe(b.key);expect(a.key).toMatch(/^draft:creator:narration:[a-f0-9]{64}$/);
 expect(a.production.schema).toBe("aurekai.narration-production.v1");
 expect(a.production.items[0].segments.map(p=>p.id)).toEqual(["opening","passage-001","passage-002"]);
 expect(a.production.items[0].source.sha256).toBe(base.digest);
 expect(a.production.items[0].source.href).toBe("https://aurekai.work/cms/compose/advanced?route=%2Fblog%2Fcouncil-agenda%2F");
 expect(a.production.sourcePermission).toBe("unreviewed");
 expect(a.production.provenance.subjectRef).toBe("article:council-agenda");
 expect(a.production.cells.length).toBe(3);
 expect(a.production.items[0].segments.some(x=>x.text.includes("$28,000"))).toBe(false);
});
test("narration uses the actual Editor.js-edited article, never an older block body",async()=>{
 const base=article(),row=base.snapshot.fragments[0];
 row.body="This obsolete text must not enter the narration.";
 row.semantic_tags={authoring_v1:{editorial:[
  {id:"a",type:"header",data:{text:"An edited section"}},
  {id:"b",type:"paragraph",data:{text:"This is the <b>latest approved wording</b> from the editor."}},
  {id:"c",type:"list",data:{items:["First public action","Second public action"]}}
 ]}};
 const plan=await planArticleNarration({base,origin});
 const spoken=plan.production.items[0].segments[1].text;
 expect(spoken).toContain("latest approved wording");
 expect(spoken).toContain("First public action");
 expect(spoken).not.toContain("obsolete text");
 expect(plan.production.defaults.rate).toBe(140);
});
test("narration excludes private and other-revision fragments",async()=>{
 const base=article(), rows=base.snapshot.fragments;
 rows.push({...rows[0],coordinate:"private",audience:"private",body:"Private words must stay private."});
 rows.push({...rows[0],coordinate:"published",status:"published",body:"A different revision of this passage."});
 const plan=await planArticleNarration({base,origin});
 const text=plan.production.items[0].segments.map(x=>x.text).join(" ");
 expect(text).not.toContain("Private words");
 expect(text).not.toContain("different revision");
 expect(plan.production.items[0].segments).toHaveLength(3);
});
test("no unsaved page, identityless page, empty narration or unsafe origin can create a production",async()=>{
 const base=article();
 for(const mutate of [
  b=>{b.id=0;},
  b=>{b.snapshot.page.media_subject_ref="";},
  b=>{b.snapshot.fragments=[];},
  b=>{b.snapshot.fragments[0].body="x".repeat(12001);}
 ]){
  const draft=structuredClone(base);mutate(draft);
  await expect(planArticleNarration({base:draft,origin})).rejects.toThrow();
 }
 await expect(planArticleNarration({base,origin:"javascript:alert(1)"})).rejects.toThrow();
});
test("save opens the current CMS revision, retains the narration once, and can reopen it directly",async()=>{
 const base=article(),reads=[];let saved=null,writes=0;
 const fetcher=fetchFor(base,reads);
 const storeFactory=path=>{
  expect(path).toBe("/apps/ws-"+"e".repeat(40)+"/_records");
  return {readVersion:async()=>saved,saveLocal:async(key,value,revision)=>{
   expect(revision).toBe(0);writes++;saved={key,value,revision:1};return saved;
  }};
 };
 const opts={base,origin,editorHref:href,fetcher,storeFactory};
 const first=await saveArticleNarration(opts);
 expect(first.created).toBe(true);expect(writes).toBe(1);expect(reads.length).toBe(3);
 expect(first.href).toMatch(/\/audio\/\?production=[a-f0-9]{64}$/);
 const again=await saveArticleNarration(opts);
 expect(again.created).toBe(false);expect(writes).toBe(1);expect(saved.value.sourcePermission).toBe("unreviewed");expect(saved.value.provenance.nativeRevision.sha256).toBe("a".repeat(64));
});
test("reopening refuses a legacy production with private source passages",async()=>{
 const base=article(), plan=await planArticleNarration({base,origin});
 const legacy=structuredClone(plan.production);
 legacy.items[0].segments.push({id:"private",text:"Private passage from an older draft."});
 let writes=0;
 await expect(saveArticleNarration({base,origin,editorHref:href,
  fetcher:fetchFor(base),
  storeFactory:()=>({readVersion:async()=>({revision:1,deleted:false,value:legacy}),
   saveLocal:async()=>{writes++;}})})).rejects.toThrow(/changed or withdrawn/);
 expect(writes).toBe(0);
});
test("save refuses a readback with altered source passages",async()=>{
 const base=article();let calls=0,stored;
 await expect(saveArticleNarration({base,origin,editorHref:href,
  fetcher:fetchFor(base),
  storeFactory:()=>({readVersion:async()=>{
   if(++calls===1)return null;
   const altered=structuredClone(stored);altered.value.items[0].segments[0].text="A different retained source.";
   return altered;
  },saveLocal:async(key,value)=>{stored={revision:1,value};return stored;}})}))
  .rejects.toThrow(/did not confirm/);
});
test("native article revision must match the selected namespace and article",async()=>{
 const base=article();let writes=0;
 const fetcher=async url=>({ok:true,json:async()=>String(url).includes("/narration-revision")?
  {...revisionFor(base),namespace:"other-site"}:structuredClone(base)});
 await expect(saveArticleNarration({base,origin,editorHref:href,fetcher,
  storeFactory:()=>({readVersion:async()=>null,saveLocal:async()=>{writes++;}})}))
  .rejects.toThrow(/does not match/);
 expect(writes).toBe(0);
});
test("edited CMS, missing app, and conflicted destination all fail closed",async()=>{
 const base=article();
 const changed=structuredClone(base);changed.digest="a".repeat(64);
 const storeFactory=()=>({readVersion:async()=>null,saveLocal:async()=>{throw Error("should not write");}});
 await expect(saveArticleNarration({base,origin,editorHref:href,
  fetcher:async()=>({ok:true,json:async()=>changed}),storeFactory})).rejects.toThrow(/changed/);
 await expect(saveArticleNarration({base,origin,editorHref:"https://foreign.example/audio/",
  fetcher:async()=>({ok:true,json:async()=>base}),storeFactory})).rejects.toThrow(/installed/);
 const one=await planArticleNarration({base,origin});
 await expect(saveArticleNarration({base,origin,editorHref:href,
  fetcher:fetchFor(base),
  storeFactory:()=>({readVersion:async()=>({deleted:false,revision:1,value:{
   schema:"aurekai.narration-production.v1",provenance:{schema:"bonfyre.authored-article-handoff.v1",
    sourceDigest:"b".repeat(64),subjectRef:one.production.provenance.subjectRef}}})})
 })).rejects.toThrow(/changed or withdrawn/);
});


test("narration follows nested reading order and refuses broken composition",async()=>{
 const base=article(),[first,second]=base.snapshot.fragments;
 base.snapshot.fragments=[
  {...second,coordinate:"second",position:1,semantic_tags:{authoring_v1:{parent:null}}},
  {...first,coordinate:"nested",position:1,semantic_tags:{authoring_v1:{parent:"group"}}},
  {...first,coordinate:"group",kind:"ui:section",heading:"",body:"",position:0,semantic_tags:{authoring_v1:{parent:null}}},
  {...first,coordinate:"opening-story",heading:base.snapshot.page.heading,position:0,semantic_tags:{authoring_v1:{parent:"group"}}}
 ];
 const plan=await planArticleNarration({base,origin}),segments=plan.production.items[0].segments;
 expect(segments.map(s=>s.id)).toEqual(["opening","passage-001","passage-002","passage-003"]);
 expect(segments[1].text).not.toContain(base.snapshot.page.heading);
 expect(segments[2].text).toContain("The agenda");expect(segments[3].text).toContain("What changed");
 const orphan=structuredClone(base);orphan.snapshot.fragments[0].semantic_tags.authoring_v1.parent="missing";
 await expect(planArticleNarration({base:orphan,origin})).rejects.toThrow(/orphaned or cyclic/);
 const cyclic=structuredClone(base);cyclic.snapshot.fragments[2].semantic_tags.authoring_v1.parent="nested";
 await expect(planArticleNarration({base:cyclic,origin})).rejects.toThrow(/orphaned or cyclic/);
 const privateParent=structuredClone(base);privateParent.snapshot.fragments[2].audience="self";
 await expect(planArticleNarration({base:privateParent,origin})).rejects.toThrow(/orphaned or cyclic/);
});


test("equal-position passages follow native retained row order",async()=>{
 const base=article();base.snapshot.fragments=base.snapshot.fragments.slice(0,2);
 base.snapshot.fragments[0]={...base.snapshot.fragments[0],id:7,coordinate:"z-first",position:0};
 base.snapshot.fragments[1]={...base.snapshot.fragments[1],id:9,coordinate:"a-second",position:0};
 const text=(await planArticleNarration({base,origin})).production.items[0].segments;
 expect(text[1].text).toContain("The agenda");expect(text[2].text).toContain("What changed");
});


test("narration uses the rendered structured prose payload",async()=>{
 const base=article();base.snapshot.fragments[0]={...base.snapshot.fragments[0],kind:"service:prose",body:"Obsolete legacy summary.",semantic_tags:{authoring_v1:{payload:{title:"Research notes",items:[{title:"Architecture",text:"A source-managed site renderer."},{title:"Publication",text:"Revision checked and traceable."}]}}}};
 const text=(await planArticleNarration({base,origin})).production.items[0].segments[1].text;
 for(const phrase of ["Research notes","Architecture","A source-managed site renderer.","Publication","Revision checked and traceable."])expect(text).toContain(phrase);
 expect(text).not.toContain("Obsolete legacy summary");
 base.snapshot.fragments[0].semantic_tags.authoring_v1.payload.items=[{title:"Incomplete"}];
 await expect(planArticleNarration({base,origin})).rejects.toThrow(/saved prose/);
});
