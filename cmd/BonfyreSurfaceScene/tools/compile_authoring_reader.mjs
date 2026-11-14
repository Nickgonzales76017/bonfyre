// Exactly one first-party visual language for C-rendered pages, browser
// previews and static exports. This copies the audited interaction controller;
// it does not bundle or execute editor dependencies in public readers.
import {readFileSync,writeFileSync,mkdirSync} from "node:fs";
import {dirname} from "node:path";
import {fileURLToPath} from "node:url";
import {authoredPatternStyles} from "../../../components/BonfyreServiceSite/authored-pattern-styles.mjs";
const base=new URL("../../../site/matrix/",import.meta.url);
const outputs=[
 {name:"authoring-reader.css",body:authoredPatternStyles+"\n"},
 {name:"authoring-reader.js",
  body:readFileSync(new URL("../../../components/BonfyreServiceSite/authoring/pattern-runtime.js",import.meta.url),"utf8")}
];
export const generatedReaderAssets=()=>outputs.map(x=>({...x}));
if(process.argv[1]&&fileURLToPath(import.meta.url)===process.argv[1]){
 const action=process.argv[2];
 for(const output of outputs){
  const target=fileURLToPath(new URL(output.name,base));
  if(action==="--write"){
   mkdirSync(dirname(target),{recursive:true});
   writeFileSync(target,output.body);
  }else if(action==="--check"){
   if(readFileSync(target,"utf8")!==output.body)
    throw Error("native authoring reader asset stale: "+output.name);
  }else throw Error("expected --check or --write");
 }
 console.log("shared authoring reader assets PASS");
}
