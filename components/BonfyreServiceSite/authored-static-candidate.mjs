// Data-only static candidate from an exact native CMS aggregate. It uses the
// SAME first-party section composition as preview, but never publishes.
// Public activation must be owned by WebAddress and the release verifier.
import {readFileSync,existsSync,writeFileSync,mkdirSync,renameSync,
 mkdtempSync,rmSync} from "node:fs";
import {dirname,join,resolve,sep} from "node:path";
import {tmpdir} from "node:os";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";
import {treeFromNativeSnapshot} from "./authoring/native-cms.mjs";
import {renderAuthoredProfilePreview} from "./authored-page-renderer.mjs";
import {compileCss} from "./site-styles.mjs";
const digest=body=>createHash("sha256").update(body).digest("hex");
const repo=fileURLToPath(new URL("../../",import.meta.url));
const fail=s=>{throw Error("authored static candidate: "+s);};
const legalRoot=path=>{
 const dest=resolve(path),live=join(repo,"site");
 if(dest===live||dest.startsWith(live+sep))fail("never overwrite the canonical public site");
 return dest;
};
export function compileAuthoredCandidate(base,{name,contact,theme,navigation}={}){
 if(!base||base.schema!=="bonfyre.page-snapshot.v1"||
    !/^[a-f0-9]{64}$/.test(base.digest||""))fail("exact native CMS snapshot required");
 const tree=treeFromNativeSnapshot(base);
 const result=renderAuthoredProfilePreview(tree,{name,contact,theme,navigation});
 if(!result.html.includes('name="robots" content="noindex,nofollow"'))
  fail("candidate must stay non-public");
 const page=tree.page.replace(/^\/|\/$/g,"");
 return {tree,result,file:page?page+"/index.html":"index.html",
  source_revision:base.digest,sha256:digest(result.html)};
}
export function writeAuthoredCandidate(base,options){
 const dest=legalRoot(options?.output||"");
 if(!options?.output||!options?.name)fail("explicit output directory and name required");
 if(existsSync(dest))fail("refusing to replace an existing candidate");
 const work=mkdtempSync(join(tmpdir(),"aurekai-authored-candidate-"));
 try{
  const built=compileAuthoredCandidate(base,options);
  const htmlFile=join(work,built.file);
  mkdirSync(dirname(htmlFile),{recursive:true});
  writeFileSync(htmlFile,built.result.html,{flag:"wx"});
  const style=join(work,"assets","site.css");mkdirSync(dirname(style),{recursive:true});
  compileCss([{html:built.result.html}],built.result.theme,style);
   const script=built.result.html.includes("assets/pattern-runtime.js")?
    readFileSync(fileURLToPath(new URL("./authoring/pattern-runtime.js",import.meta.url))):null;
   if(script)writeFileSync(join(work,"assets","pattern-runtime.js"),script,{flag:"wx"});
  const css=readFileSync(style),html=readFileSync(htmlFile);
  if(digest(html)!==built.sha256||!css.length)fail("candidate byte verification failed");
  // This receipt is NOT an approval, asset closure or release identity.
  const manifest=[
   ["schema","bonfyre.authored-candidate.v1"],
   ["publication","noindex-preview-only"],
   ["site_id",built.tree.site_id],
   ["page",built.tree.page],
   ["source_revision",built.source_revision],
   ["html_sha256",built.sha256],
   ["css_sha256",digest(css)],
    ...(script?[["pattern_runtime_sha256",digest(script)]]:[]),
    ["static_assets",script?"stylesheet-and-local-interactions; media still require closure":"stylesheet-only; media and app clients still require closure"]
  ].map(([key,value])=>key+"\t"+value).join("\n")+"\n";
  writeFileSync(join(work,"CANDIDATE.tsv"),manifest,{flag:"wx"});
  mkdirSync(dirname(dest),{recursive:true});
  if(existsSync(dest))fail("output became occupied during build");
  renameSync(work,dest);
  return {directory:dest,manifest,html_sha256:built.sha256,site_id:built.tree.site_id,
   page:built.tree.page,preview_only:true};
 }finally{if(existsSync(work))rmSync(work,{recursive:true,force:true});}
}
const argv=process.argv.slice(2);
if(import.meta.main){
 if(argv.length!==6||argv[0]!=="--candidate"||argv[2]!=="--out"||argv[4]!=="--name"){
  console.error("usage: bun authored-static-candidate.mjs --candidate SNAPSHOT --out DIR --name NAME");
  process.exit(2);
 }
 const base=JSON.parse(readFileSync(argv[1],"utf8"));
 const result=writeAuthoredCandidate(base,{output:argv[3],name:argv[5]});
 console.log("Candidate generated (not published): "+result.directory);
}
