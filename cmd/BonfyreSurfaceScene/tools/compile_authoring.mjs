// Compile the editor-neutral prototype's validated grammar into the native C
// lookup/size-checking surface. Review/publish/effects remain separate owners.
import {readFileSync,writeFileSync} from "node:fs";
import {fileURLToPath} from "node:url";
import {nativeAuthoringHeader,AUTHORING_BLOCKS} from "../runtime/authoring-prototype.mjs";
const target=fileURLToPath(new URL("../../BonfyreWebAddress/generated/native_authoring_contract.h",import.meta.url));
const args=process.argv.slice(2);
if(args.length!==1||!["--check","--write"].includes(args[0])){
 console.error("usage: node compile_authoring.mjs --check|--write");process.exit(2);
}
const generated=nativeAuthoringHeader();
if(args[0]==="--write"){writeFileSync(target,generated);console.log("native authoring grammar written",AUTHORING_BLOCKS.length);process.exit(0);}
let actual="";try{actual=readFileSync(target,"utf8");}catch{}
if(actual!==generated){console.error("native authoring grammar stale; regenerate and verify native parity");process.exit(1);}
console.log("native authoring grammar PASS",AUTHORING_BLOCKS.length);
