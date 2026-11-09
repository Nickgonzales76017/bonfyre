// Compile the approved site-fragment Mustach renderer to a native constant.
// This does not bypass account, source, or publication checks.
import {readFileSync,writeFileSync} from "node:fs";
import {fileURLToPath} from "node:url";
import {createHash} from "node:crypto";
const source=fileURLToPath(new URL("../templates/site_fragment.mustache",import.meta.url));
const output=fileURLToPath(new URL("../../BonfyreWebAddress/generated/native_site_fragment_template.h",import.meta.url));
export function compiledNativeFragment(){
 const body=readFileSync(source,"utf8");
 if(body.length>8192||/\{\{\{|\{\{&|\{\{=|<script|<iframe|onerror=/i.test(body))
  throw Error("unsafe approved fragment template");
 const digest=createHash("sha256").update(body).digest("hex");
 return "/* GENERATED; source: cmd/BonfyreSurfaceScene/templates/site_fragment.mustache\n"+
  " * SHA256: "+digest+"\n * Source owners and publication rules are separate. */\n"+
  "#ifndef BF_NATIVE_SITE_FRAGMENT_TEMPLATE_H\n#define BF_NATIVE_SITE_FRAGMENT_TEMPLATE_H\n"+
  "static const char bf_native_site_fragment_template[]="+JSON.stringify(body)+";\n"+
  "#endif\n";
}
if(process.argv[1]===fileURLToPath(import.meta.url)){
 const flag=process.argv[2],generated=compiledNativeFragment();
 if(flag==="--write"){writeFileSync(output,generated);console.log("native site-fragment template generated");}
 else if(flag==="--check"){
  if(readFileSync(output,"utf8")!==generated)throw Error("stale native fragment template");
  console.log("native site-fragment Mustach parity PASS");
 }else process.exit(2);
}
