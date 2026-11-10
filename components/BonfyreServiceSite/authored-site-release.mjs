/* Published Aurekai site bundle — never an importer or a draft promotion.
 * Inputs are exact, published BonfyreCMS page aggregates. A separate native
 * publication owner MUST attest the complete source revision before HTML is
 * indexed or bundled for GitHub Pages. This module cannot authorize release.
 * Reuses the same account-owned renderer/CMS tree as the visual editor.
 */
import {createHash} from "node:crypto";
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,existsSync,renameSync,rmSync} from "node:fs";
import {dirname,join,resolve,sep} from "node:path";
import {tmpdir} from "node:os";
import {fileURLToPath} from "node:url";
import {treeFromNativeSnapshot} from "./authoring/native-cms.mjs";
import {renderAuthoredProfilePreview} from "./authored-page-renderer.mjs";
import {compileCss} from "./site-styles.mjs";
const sha=x=>createHash("sha256").update(x).digest("hex");
const hex=x=>typeof x==="string"&&/^[a-f0-9]{64}$/.test(x);
const fail=x=>{throw Error("authored published release: "+x);};
const repo=fileURLToPath(new URL("../../",import.meta.url));
const routeOK=s=>typeof s==="string"&&s.length<=127&&
 /^\/(?:[a-z0-9-]+\/)*$/.test(s)&&!s.includes("//");
const fileOf=route=>route==="/"?"index.html":route.slice(1)+"index.html";
const escape=x=>String(x??"").replace(/[&<>"']/g,c=>({
 "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
}[c]));
const titleOK=s=>typeof s==="string"&&!!s.trim()&&s.length<=220&&
 !/[\x00-\x1f\x7f]/.test(s);
function exactSource(snapshots){
 if(!Array.isArray(snapshots)||!snapshots.length||snapshots.length>512)
  fail("expected one to 512 exact published pages");
 const namespace=snapshots[0]?.snapshot?.namespace;
 if(!/^person-[a-z0-9-]{2,80}-(work|live|life|shop)$/.test(namespace||""))
  fail("invalid destination namespace");
 const pages=[],seen=new Set();
 for(const snap of snapshots){
  if(snap?.schema!=="bonfyre.page-snapshot.v1"||
     snap.snapshot?.namespace!==namespace||
     !hex(snap.digest)||!Number.isInteger(snap.id)||snap.id<1)
   fail("unverified or cross-account CMS aggregate");
  const route=snap.snapshot.route,page=snap.snapshot.page;
  if(!routeOK(route)||seen.has(route)||page?.route!==route||
     page?.status!=="published"||!titleOK(page.title))
   fail("missing exact published site page or duplicate route");
  seen.add(route);
  const fragments=snap.snapshot.fragments;
  if(!Array.isArray(fragments)||!fragments.length||fragments.length>512)
   fail("missing published authored composition");
  for(const f of fragments){
   if(f.page!==route||f.slot!=="showcase"||f.audience!=="public"||
      f.status!=="published"||f.projection_contract?.html!==true)
    fail("unpublished, nonpublic or unreviewed fragment");
   let tags=f.semantic_tags;
   if(typeof tags==="string")try{tags=JSON.parse(tags);}catch{fail("invalid retained authoring tag");}
   if(tags?.authoring_v1?.version!==1)
    fail("legacy fragments are not a native authored release");
   if(!/^ui:|^service:|^profile-/.test(f.kind||""))
    fail("unknown profile component kind");
   if(f.source_ref&&!hex(f.source_digest))
    fail("source reference without verified source digest");
  }
  pages.push({snap,route,title:page.title});
 }
 if(!seen.has("/"))fail("an authored homepage must exist");
 pages.sort((a,b)=>a.route.localeCompare(b.route));
 return {namespace,pages,source_revision:sha(pages.map(p=>
  [p.route,p.snap.id,p.snap.digest].join("\t")).join("\n")+"\n")};
}
function resolvedLinks(html,route,known){
 const misses=[];
 for(const [,attribute,url]of html.matchAll(/\b(href|src)="([^"]+)"/g)){
  if(!url||url.startsWith("#")||/^(?:https:\/\/|mailto:|tel:)/.test(url))continue;
  if(url.startsWith("javascript:")||url.startsWith("data:")||url.startsWith("//"))
   fail("unexpected active or remote link");
  const path=url.split(/[?#]/,1)[0],root=path.startsWith("/")?
   path: new URL(path,"https://authored.invalid"+route).pathname;
  if(!known.has(root)&&!known.has(root.replace(/\/?$/,"/")))misses.push(root);
 }
 return [...new Set(misses)];
}
function exportedHTML(html,namespace,publication){
 if(typeof html!=="string"||
    !html.includes('name="robots" content="noindex,nofollow"')||
    !html.includes('data-authored-profile="'+namespace+'"'))
  fail("authoring renderer lost its publication boundary");
 const marker='<html lang="en"';
 if(!html.includes(marker))fail("first-party full page required");
 const publicHTML=html.replace('name="robots" content="noindex,nofollow"',
  'name="robots" content="index,follow"')
  .replace(marker,marker+' data-authored-publication="'+publication+'"');
 if(publicHTML.includes("NOINDEX-CANDIDATE-ONLY")||
    publicHTML.includes('content="noindex'))
  fail("candidate marker survived publication");
 return publicHTML;
}
function checkedAssets(assets){
 if(!Array.isArray(assets)||assets.length>1000)fail("invalid asset closure");
 const results=new Map();
 for(const entry of assets){
  if(!entry||!/^assets\/[a-z0-9][a-z0-9_./-]*$|^data\/[a-z0-9][a-z0-9_./-]*$/.test(entry.path||"")||
     entry.path.split("/").some(x=>x===".."||x===".")||
     !hex(entry.sha256)||typeof entry.source!=="string"||!entry.source)
   fail("asset has no exact reviewed source receipt");
  const source=resolve(entry.source);
  // Asset providers may come from source packages; the owner supplies exact
  // paths AND SHA receipts. This is not a recursive directory copy.
  if(!source.startsWith(repo+sep)||results.has(entry.path))
   fail("untrusted, duplicate or foreign asset");
  const bytes=readFileSync(source);
  if(bytes.length>16*1024*1024||sha(bytes)!==entry.sha256)
   fail("asset bytes do not match declared source: "+entry.path);
  results.set(entry.path,bytes);
 }
 return results;
}
/* The full first-party section modules (not the generic C template names)
 * declare versioned material dependencies. Page links alone cannot close
 * library/search/catalogue/creator fetches encoded inside application data. */
function dependencyClosure(sourcePages,resources){
 const required=[];
 for(const {snap} of sourcePages){
  const tree=treeFromNativeSnapshot(snap);
  const traverse=nodes=>{
   for(const node of nodes){
    if(node.kind.startsWith("service:")){
     const payload=node.props.payload||{},name=node.kind.slice(8);
     if(payload.data!=null){
      if(typeof payload.data!=="string"||
         !/^\/data\/[a-z][a-z0-9-]*\/$/.test(payload.data))
       fail(name+" has an invalid published data package address");
      if(payload.asset_prefix!=null&&!/^v-[a-f0-9]{64}\/$/.test(payload.asset_prefix))
       fail(name+" has an unversioned client code package");
      const prefix=payload.data.slice(1)+(payload.asset_prefix||"");
      required.push({name,prefix});
     }else if(payload.asset_prefix!=null)
      fail(name+" has an asset prefix without a verified data package");
    }
    traverse(node.children);
   }
  };
  traverse(tree.blocks);
 }
 for(const {name,prefix} of required){
  if(![...resources.keys()].some(path=>path.startsWith(prefix)&&
       path.length>prefix.length))
   fail(name+" is missing its published material package "+prefix);
 }
 // The static producer must include transitive literal import dependencies.
 // Dynamic imports and fetch plans remain subject to independent browser
 // admission; this is a fail-closed baseline, not a claim of network parity.
 for(const [path,bytes] of resources){
  if(!path.endsWith(".mjs")||!bytes)continue;
  const code=bytes.toString("utf8");
  const refs=[
   ...code.matchAll(/\b(?:import|export)\s+(?:[^"']*?\s+from\s*)?["'](\.[^"']+)["']/g),
   ...code.matchAll(/\bimport\s*\(\s*["'](\.[^"']+)["']\s*\)/g)
  ];
  for(const match of refs){
   const address=new URL(match[1],"https://assets.invalid/"+path);
   if(address.origin!=="https://assets.invalid"||
      !resources.has(address.pathname.slice(1)))
    fail("unbundled first-party module import "+path+" → "+match[1]);
  }
 }
}

export function compileAuthoredPublishedSite(snapshots,{
 publication_id,authorize_native_release,name,contact,theme,assets=[]
}={}){
 const state=exactSource(snapshots);
 if(!hex(publication_id)||typeof authorize_native_release!=="function")
  fail("native publication owner release proof required");
 const proof=Object.freeze({
  namespace:state.namespace,source_revision:state.source_revision,
  publication_id,pages:Object.freeze(state.pages.map(({route,snap})=>
   Object.freeze({route,revision:snap.digest})))
 });
 if(authorize_native_release(proof)!==true)
  fail("native publication owner did not verify the complete CMS revision");
 const roots=state.pages.map(p=>p.route),navigation=state.pages.slice(0,35)
  .map(p=>({href:p.route,label:p.title}));
 const pages=state.pages.map(({snap,route,title})=>{
  const tree=treeFromNativeSnapshot(snap);
  const result=renderAuthoredProfilePreview(tree,{name:title||name,
   contact,theme,navigation});
  return {route,title,native_revision:snap.digest,
   file:fileOf(route),html:exportedHTML(result.html,state.namespace,publication_id),
   theme:result.theme};
 });
 const resources=new Map([["assets/site.css",null]]);
 if(pages.some(p=>p.html.includes("pattern-runtime.js"))){
  resources.set("assets/pattern-runtime.js",
   readFileSync(new URL("./authoring/pattern-runtime.js",import.meta.url)));
 }
 for(const [path,bytes]of checkedAssets(assets)){
  if(resources.has(path))fail("asset shadows native shared runtime");
  resources.set(path,bytes);
 }
 const known=new Set([...roots,...[...resources.keys()].map(x=>"/"+x)]);
 dependencyClosure(state.pages,resources);
 for(const page of pages){
  const unresolved=resolvedLinks(page.html,page.route,known);
  if(unresolved.length)fail("page "+page.route+" has unbundled resource(s): "+unresolved.join(", "));
 }
 return {...state,publication_id,pages,resources};
}
export function writeAuthoredPublishedSite(snapshots,{
 output,publication_id,authorize_native_release,name,contact,theme,assets=[]
}={}){
 if(typeof output!=="string"||!output)fail("distinct output directory required");
 const dest=resolve(output),publicSite=resolve(repo,"site");
 if(dest===publicSite||dest.startsWith(publicSite+sep)||existsSync(dest))
  fail("cannot overwrite a public checkout or an existing release");
 const result=compileAuthoredPublishedSite(snapshots,{
  publication_id,authorize_native_release,name,contact,theme,assets});
 const staging=mkdtempSync(join(tmpdir(),"bonfyre-published-site-"));
 const records=[];
 function put(path,bytes){
  const target=resolve(staging,"site",path);
  if(!target.startsWith(staging+sep))fail("bundle traversal");
  mkdirSync(dirname(target),{recursive:true});
  writeFileSync(target,bytes,{flag:"wx",mode:0o644});
  records.push(["site/"+path,sha(bytes)]);
 }
 try{
  for(const page of result.pages)put(page.file,page.html);
  const cssFile=join(staging,"site-css-generated.css");
  compileCss(result.pages.map(p=>({html:p.html})),result.pages[0].theme,cssFile);
  put("assets/site.css",readFileSync(cssFile));
  rmSync(cssFile);
  for(const [path,bytes]of result.resources)if(bytes)put(path,bytes);
  const pageManifest="route\ttitle\tnative_revision\thtml_sha256\n"+
   result.pages.map(p=>[p.route,p.title,p.native_revision,sha(p.html)].join("\t")).join("\n")+"\n";
  const meta=[
   ["schema","bonfyre.authored-site-release.v1"],
   ["publication","published"],
   ["account_namespace",result.namespace],
   ["release_id",publication_id],
   ["cms_revision",result.source_revision],
   ["pages",String(result.pages.length)]
  ].map(row=>row.join("\t")).join("\n")+"\n";
  writeFileSync(join(staging,"SITE-PAGES.tsv"),pageManifest,{flag:"wx"});
  writeFileSync(join(staging,"RELEASE.tsv"),meta,{flag:"wx"});
  writeFileSync(join(staging,"FILES.tsv"),"path\tsha256\n"+
   records.sort((a,b)=>a[0].localeCompare(b[0]))
    .map(row=>row.join("\t")).join("\n")+"\n",{flag:"wx"});
  mkdirSync(dirname(dest),{recursive:true});
  renameSync(staging,dest);
  return {directory:dest,namespace:result.namespace,release_id:publication_id,
   source_revision:result.source_revision,pages:result.pages.length,files:records.length};
 }finally{if(existsSync(staging))rmSync(staging,{recursive:true,force:true});}
}
