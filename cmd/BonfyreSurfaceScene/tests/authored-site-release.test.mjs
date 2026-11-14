import {test,expect} from "bun:test";
import assert from "node:assert/strict";
import {existsSync,mkdtempSync,readFileSync,rmSync,writeFileSync,renameSync,symlinkSync,unlinkSync} from "node:fs";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {createHash} from "node:crypto";
import {spawnSync} from "node:child_process";
import {newAuthoringTree,validateAuthoringTree}
 from "../runtime/authoring-tree.mjs";
import {newAuthoredComponent}
 from "../../../components/BonfyreServiceSite/authoring/component-palette.mjs";
import {toNativePageWrite}
 from "../../../components/BonfyreServiceSite/authoring/native-cms.mjs";
import {compileAuthoredPublishedSite,writeAuthoredPublishedSite}
 from "../../../components/BonfyreServiceSite/authored-site-release.mjs";
import {writeAuthoredReleaseArchive}
 from "../../../components/BonfyreServiceSite/authored-release-archive.mjs";
import {verifyAndStage} from "../../../scripts/verify-authored-release.mjs";
const sha=s=>createHash("sha256").update(s).digest("hex");
const publication="b".repeat(64),revision="c".repeat(64);
function published(namespace,route,title,index=0){
 const doc=newAuthoringTree(namespace,route);
 const main=newAuthoredComponent("ui:documentation","root-"+index);
 main.props.payload.title=title;
 main.children=[newAuthoredComponent(index?"ui:modal":"ui:nav-tabs","control-"+index)];
 if(!index)main.children.push({id:"selected-work",kind:"service:cases",props:{
  heading:"Projects",payload:{title:"Original projects",items:[
   {title:"Source-backed research",text:"An editor-authored service section"}]}} ,
  responsive:{},children:[]});
 doc.blocks=[main];validateAuthoringTree(doc);
 const empty={schema:"bonfyre.page-snapshot.v1",id:0,digest:"a".repeat(64),
  snapshot:{namespace,route,page:null,fragments:[]}};
 const write=toNativePageWrite(empty,doc,{pageDraft:{title,heading:title}});
 return {schema:empty.schema,id:index+100,digest:revision,
  snapshot:{namespace,route,page:{...write.page,id:index+100,status:"published"},
   fragments:write.fragments.map(f=>({...f,status:"published"}))}};
}
function verified(x){return x.namespace==="person-original-work"&&
 x.publication_id===publication&&x.pages.length===2&&
 /^[a-f0-9]{64}$/.test(x.source_revision);}
const pages=()=>[
 published("person-original-work","/","Original Aurekai site"),
 published("person-original-work","/docs/","Documentation",1)
];
test("published site needs independent native owner acceptance, never a candidate downgrade",()=>{
 const sources=pages();
 expect(()=>compileAuthoredPublishedSite(sources,{publication_id:publication}))
  .toThrow(/native publication owner release proof/);
 expect(()=>compileAuthoredPublishedSite(sources,{publication_id:publication,
  authorize_native_release:()=>false})).toThrow(/did not verify/);
 const dirty=structuredClone(sources);dirty[0].snapshot.fragments[0].status="draft";
 expect(()=>compileAuthoredPublishedSite(dirty,{publication_id:publication,
  authorize_native_release:verified})).toThrow(/unpublished/);
 const unmanaged=structuredClone(sources);unmanaged[1].snapshot.fragments[0].semantic_tags.authoring_v1.version=0;
 expect(()=>compileAuthoredPublishedSite(unmanaged,{publication_id:publication,
  authorize_native_release:verified})).toThrow(/legacy/);
 const foreign=published("person-foreign-life","/docs/","Unauthorized",2);
 expect(()=>compileAuthoredPublishedSite([sources[0],foreign],{publication_id:publication,
  authorize_native_release:verified})).toThrow(/cross-account/);
});
test("original authored site becomes exact multi-route release with native CMS revisions",()=>{
 const temp=mkdtempSync(join(tmpdir(),"aurekai-authored-release-"));
 try{
  const output=join(temp,"artifact"),sources=pages();
  const result=writeAuthoredPublishedSite(sources,{output,publication_id:publication,
   authorize_native_release:verified});
  expect(result.pages).toBe(2);
  expect(result.release_id).toBe(publication);
  for(const path of ["site/index.html","site/docs/index.html","site/assets/site.css",
   "site/assets/pattern-runtime.js","RELEASE.tsv","FILES.tsv","SITE-PAGES.tsv"])
   expect(existsSync(join(output,path))).toBe(true);
  for(const path of ["site/index.html","site/docs/index.html"]){
   const html=readFileSync(join(output,path),"utf8");
   expect(html).toContain('data-authored-publication="'+publication+'"');
   expect(html).toContain('data-authored-profile="person-original-work"');
   expect(html).toContain('content="index,follow"');
   expect(html).not.toContain('content="noindex');
  }
  expect(readFileSync(join(output,"site/index.html"),"utf8"))
   .toContain("Source-backed research");
  // The same sealed tar is accepted by the exact GitHub Pages verifier.
  // This is a publication artifact, not a checkout copy or html importer.
  const archive=join(temp,"native-authored-published.tar.gz");
  const sealed=writeAuthoredReleaseArchive(output,archive);
  expect(sealed.sha256).toMatch(/^[a-f0-9]{64}$/);
  const repeated=writeAuthoredReleaseArchive(output,join(temp,"repeat.tar.gz"));
  expect(repeated.sha256).toBe(sealed.sha256);
  const checkout=join(temp,"github-pages-site");
  const admitted=verifyAndStage(archive,sealed.sha256,{output:checkout});
  expect(admitted.namespace).toBe("person-original-work");
  expect(admitted.pages).toBe(2);
  expect(readFileSync(join(checkout,"docs","index.html"),"utf8"))
   .toBe(readFileSync(join(output,"site","docs","index.html"),"utf8"));
  expect(()=>verifyAndStage(archive,"0".repeat(64),{output:join(temp,"rejected")}))
   .toThrow(/archive bytes differ/);
  // Serve the exact already-published HTML/assets using native C with no
  // JavaScript authoring runtime or per-section handwritten presentation.
  const native=join(temp,"native-release-reader");
  const cwd=join(import.meta.dir,"..");
  const compile=spawnSync("cc",["-std=c11","-Wall","-Wextra","-Werror",
   "-I../../lib/libbonfyre/include",
   "tests/native_authored_release_reader_test.c",
   "src/authored_release_reader.c",
   "../../lib/libbonfyre/libbonfyre.a","-lm","-o",native],
   {cwd,encoding:"utf8"});
  expect(compile.status).toBe(0);
  if(compile.status!==0)throw Error(compile.stderr);
  const run=spawnSync(native,[output,"person-original-work",publication,
   result.source_revision],{cwd,encoding:"utf8"});
  if(run.status!==0)throw Error(run.stderr||run.stdout);
  expect(run.stdout).toContain("native first-party release bytes PASS "+
   sha(readFileSync(join(output,"site","index.html"))));
  const assertDenial=mode=>{
   const denied=spawnSync(native,[output,"person-original-work",publication,
    result.source_revision,mode],{cwd,encoding:"utf8"});
   if(denied.status!==0)throw Error(denied.stderr||denied.stdout);
   expect(denied.stdout).toContain("native authored release refusal PASS");
  };
  const pagePath=join(output,"site","index.html"),pageBefore=readFileSync(pagePath);
  writeFileSync(pagePath,Buffer.concat([pageBefore,Buffer.from("<!-- changed after publication -->")]));
  assertDenial("deny-page");
  writeFileSync(pagePath,pageBefore);
  const stylesheet=join(output,"site","assets","site.css"),cssBefore=readFileSync(stylesheet);
  const savedStyle=join(output,"site","assets","published-style.backup");
  renameSync(stylesheet,savedStyle);
  symlinkSync(savedStyle,stylesheet);
  assertDenial("deny-asset");
  unlinkSync(stylesheet);
  renameSync(savedStyle,stylesheet);
  expect(readFileSync(stylesheet)).toEqual(cssBefore);
  const release=readFileSync(join(output,"RELEASE.tsv"),"utf8");
  expect(release).toContain("publication\tpublished");
  expect(release).toContain("release_id\t"+publication);
  const manifest=readFileSync(join(output,"FILES.tsv"),"utf8").trim().split("\n").slice(1);
  for(const line of manifest){
   const [path,digest]=line.split("\t");
   expect(digest).toBe(sha(readFileSync(join(output,path))));
  }
  const routes=readFileSync(join(output,"SITE-PAGES.tsv"),"utf8");
  expect(routes).toContain("/docs/\tDocumentation\t"+revision+"\t");
  expect(()=>writeAuthoredPublishedSite(sources,{output,publication_id:publication,
   authorize_native_release:verified})).toThrow(/existing release/);
 }finally{rmSync(temp,{recursive:true,force:true});}
},120000);
test("unresolved local destinations and unsealed media prevent publication",()=>{
 const broken=pages();const hero=newAuthoredComponent("ui:navbar","broken-nav");
 hero.props.payload.links=[{label:"Lost route",href:"/missing/"}];
 broken[0].snapshot.fragments.push({
  coordinate:"broken-nav",page:"/",slot:"showcase",audience:"public",
  kind:"ui:navbar",position:4,status:"published",
  semantic_tags:{authoring_v1:{version:1,parent:null,responsive:{},payload:hero.props.payload}},
  projection_contract:{html:true}
 });
 expect(()=>compileAuthoredPublishedSite(broken,{publication_id:publication,
  authorize_native_release:verified})).toThrow(/unbundled resource/);
 const evidence=pages();
 evidence[0].snapshot.fragments[0].source_ref="source:forged";
 expect(()=>compileAuthoredPublishedSite(evidence,{publication_id:publication,
  authorize_native_release:verified})).toThrow(/without verified source digest/);
});
