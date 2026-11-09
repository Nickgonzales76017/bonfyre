// One immutable source document -> one native C render -> one review artifact.
// The static preview is not a publication or permission grant.
import {spawnSync} from "node:child_process";
import {writeFileSync,mkdirSync,readFileSync} from "node:fs";
import {join,resolve} from "node:path";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";
import {deepStrictEqual} from "node:assert/strict";
import {journalScene,profileScene} from "../tests/fixtures/authoring-publication.mjs";
import {encodeNativeAuthoring,decodeNativeAuthoring} from "../runtime/authoring-carrier.mjs";
const root=resolve(fileURLToPath(new URL("..",import.meta.url)));
const dir=resolve(process.argv[2]||join(root,"build/authoring-previews"));
mkdirSync(dir,{recursive:true});
const exe=join(dir,"bf-authoring-preview-native");
function run(bin,args){
 const p=spawnSync(bin,args,{encoding:"utf8",timeout:25000});
 if(p.status!==0)throw Error("authoring preview failed "+bin+" "+(p.stderr||p.error));
 return p;
}
run(process.env.CC||"cc",["-std=c11","-Wall","-Wextra","-Werror",
 join(root,"tests/native_authoring_carrier_main.c"),"-o",exe]);
const sha=x=>createHash("sha256").update(x).digest("hex");
let receipts="site\troute\tcarrier_sha256\tpreview_sha256\n";
for(const [name,create] of [["journal",journalScene],["profile",profileScene]]){
 const doc=create(),wire=join(dir,name+".bfau"),html=join(dir,name+".html");
 const bytes=encodeNativeAuthoring(doc);
 deepStrictEqual(decodeNativeAuthoring(bytes),doc,
  "source document and native carrier diverged");
 writeFileSync(wire,bytes);
 run(exe,[wire,html]);
 receipts+=doc.site_id+"\t"+doc.page+"\t"+sha(bytes)+"\t"+sha(readFileSync(html))+"\n";
}
writeFileSync(join(dir,"SOURCE.tsv"),receipts);
console.log("AUTHORING_NATIVE_PREVIEWS_READY",dir,"2 isolated scenes / no publication");
