// Verify declared upstream code against the actual authoring/runtime estate.
// Offline, read-only: source declarations alone are not installed capabilities.
import {readFileSync,existsSync} from "node:fs";
import {dirname,join,resolve} from "node:path";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";

const root=resolve(dirname(fileURLToPath(import.meta.url)),"..");
const at=path=>join(root,path);
const read=path=>readFileSync(at(path),"utf8");
const check=(condition,description)=>{if(!condition)throw Error("library candidate audit: "+description);};
const tsv="cmd/BonfyreSurfaceScene/UPSTREAM-CANDIDATES.tsv";
const rows=read(tsv).trimEnd().split(/\r?\n/).map(line=>line.split("\t"));
check(rows[0].join("\t")==="role\tupstream\trevision\tlicense\tadoption","candidate registry column grammar");
const names=new Set(),declared=new Map();
for(const [i,row] of rows.entries()){
 if(i===0)continue;
 check(row.length===5,"candidate row "+(i+1)+" has five fields");
 const [role,upstream,revision,license,adoption]=row;
 check(/^[a-z][a-z0-9-]{2,70}$/.test(role)&&!names.has(role),"unique candidate role "+role);
 check(/^https:\/\/(?:github\.com|gitlab\.com)\/[a-zA-Z0-9._-]+\/[a-zA-Z0-9._-]+\/?$/.test(upstream),"upstream HTTPS origin for "+role);
 check(/^[0-9a-f]{40}(?:-[a-zA-Z0-9-]+)?$/.test(revision),"pinned git revision for "+role);
 check(license.length>=3&&license.length<=70&&adoption.length>=4,"declared license and adoption for "+role);
 names.add(role);declared.set(role,{upstream,revision,license,adoption});
}
const required=["visual-layout","typed-components","editorial-blocks",
 "c-templates","c-markdown-compact","c-markdown-gfm"];
for(const role of required)check(declared.has(role),"missing candidate "+role);
check(declared.size>=6,"candidate catalogue unexpectedly contracted");

const manifest=JSON.parse(read("components/BonfyreServiceSite/authoring/package.json"));
const editor=read("components/BonfyreServiceSite/authoring/unified-editor.mjs");
const packages=[
 ["visual-layout","grapesjs",'import("grapesjs")'],
 ["typed-components","@puckeditor/core",'import("@puckeditor/core")'],
 ["editorial-blocks","@editorjs/editorjs",'import("@editorjs/editorjs")']
];
for(const [role,name,invocation]of packages){
 const version=manifest.dependencies?.[name];
 check(typeof version==="string"&&/^\d+\.\d+\.\d+$/.test(version),"unversioned installed authoring dependency "+name);
 check(editor.includes(invocation),"missing editor adapter "+name);
 check(!declared.get(role).adoption.includes("commercial-Studio-SDK")||name==="grapesjs",
  "unreviewed commercial substitute "+name);
}
check(editor.includes("runtime/authoring-tree.mjs"),"editors must share the native authoring tree");
const owners=[
 "cmd/BonfyreSurfaceScene/runtime/renderers/record-grid.mjs",
 "cmd/BonfyreSurfaceScene/runtime/renderers/review-sheet.mjs",
 "cmd/BonfyreSurfaceScene/runtime/renderers/edit-history.mjs",
 "cmd/BonfyreSurfaceScene/runtime/renderers/workspace-composition.mjs",
 "cmd/BonfyreSurfaceScene/runtime/renderers/collection-workbench.mjs",
 "cmd/BonfyreSurfaceScene/runtime/renderers/scene-workbench.mjs"
];
for(const file of owners)check(existsSync(at(file))&&read(file).length>100,
 "declared reusable native renderer missing "+file);

const vendors=[
 ["c-markdown-compact","cmd/BonfyreSurfaceScene/vendor/md4c/",
 "cmd/BonfyreSurfaceScene/vendor/md4c/SOURCE.md",[
 ["md4c.c","73a34be4d6e79167c304fce41c932c70fa4dd46c"],
 ["md4c.h","b6f28153a9cb6e3b746e0d691f4263177c4dfdd"],
 ["md4c-html.c","d1e1c7cbb1fbd10e1083e1071708ee55170db1b3"],
 ["md4c-html.h","9cd93afe38baf7e8d9d3294aa5f1c662488d4ac6"],
 ["entity.c","c72b81f8b06253f8cfebae4e36395c343de11a07"],
 ["entity.h","2ee99c8f1e54a0672cca0a9413c584892986d6cf"]]],
 ["c-templates","cmd/BonfyreSurfaceScene/vendor/mustach/",
 "cmd/BonfyreSurfaceScene/vendor/mustach/SOURCE.md",[
 ["mini-mustach.c","3e4726ef2f42576ce63174560e0ff76366c18941"],
 ["mini-mustach.h","348c20b4345c84a71d41885104167ff424a91574"],
 ["LICENSE.txt","13666423582b9e0301290f8a9df146e7cf97e747"]]]
];
// The SOURCE.md identities are upstream Git SHA-1 blob names. The local vendored
// copies also carry independently pinned full-byte SHA-256, including the two
// declared whitespace-only md4c changes. This audit checks the local bytes,
// while retaining the upstream identities as provenance, not confusing formats.
const localSHA256=Object.freeze({
 "cmd/BonfyreSurfaceScene/vendor/md4c/md4c.c": "5b45d3d1d036c71d37cf149b0a1311a9e4d29366ad5555c0b5187b9d2b19aa86",
 "cmd/BonfyreSurfaceScene/vendor/md4c/md4c.h": "1313c5dc17f058dc269a5c732e497705a14710f661df5fee1132c31c1135414a",
 "cmd/BonfyreSurfaceScene/vendor/md4c/md4c-html.c": "bf6f4f856e7352526c08b22916d1a840054b027a28f2b2a89482f50bf23e55b0",
 "cmd/BonfyreSurfaceScene/vendor/md4c/md4c-html.h": "6a045ecec5d8cf17a771654a31af06641df195d7ea8486a140a121b6c6e0191f",
 "cmd/BonfyreSurfaceScene/vendor/md4c/entity.c": "4c309bb8f696324205d61a6e0489ce2b58aa25ea1b067d085f6b834fdb3f2a92",
 "cmd/BonfyreSurfaceScene/vendor/md4c/entity.h": "99a8952be6629e4114947cb61a901bf4f73b10d84d1ac5e17ad8299dfbad708e",
 "cmd/BonfyreSurfaceScene/vendor/mustach/mini-mustach.c": "3e9ef5c142bfe0f0027b22ab11ee074386b8f998ad57d03f64db8c00e07e683d",
 "cmd/BonfyreSurfaceScene/vendor/mustach/mini-mustach.h": "785d7dac2c34efd870fbd473adf3931b784f061809af9e34ff30694a726b738f",
 "cmd/BonfyreSurfaceScene/vendor/mustach/LICENSE.txt": "72fa68bfbb9b5857f3ca1fcff10dc4ba4c1c27488f3f2c22a931bb2c4afca777"
});
let verified=0;
for(const [role,folder,sourcePath,blobs]of vendors){
 const source=read(sourcePath),pin=declared.get(role).revision.slice(0,40);
 check(source.includes(pin)&&source.includes(declared.get(role).upstream),
  "vendored lineage disagrees with catalogue "+role);
 for(const [name,expected]of blobs){
  const file=folder+name;
  const actual=createHash("sha256").update(readFileSync(at(file))).digest("hex");
  check(actual===localSHA256[file],role+"/"+name+" differs from pinned local source bytes");
  check(source.includes(expected),"missing upstream blob provenance for "+role+"/"+name);
  verified++;
 }
}
check(read("cmd/BonfyreSurfaceScene/src/authoring_markdown.c").includes("md_html("),
 "MD4C not connected to native markdown rendering");
check(read("cmd/BonfyreSurfaceScene/src/authoring_template.c").includes("mini-mustach.h"),
 "mustach not connected to native templates");
if(process.argv.slice(2).some(value=>value!=="--verify"))
 throw Error("read-only audit; supported invocation is --verify");
console.log("native library candidates: "+declared.size+" pinned upstream declarations, "+
 packages.length+" directly imported editor adapters, "+verified+
 " SHA-256 verified local vendor files, "+owners.length+" shared native renderers PASS");
