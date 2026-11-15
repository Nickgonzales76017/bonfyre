// Exactly one first-party visual language for C-rendered pages, browser
// previews and static exports. This copies the audited interaction controller;
// it does not bundle or execute editor dependencies in public readers.
import {readFileSync,writeFileSync,mkdirSync} from "node:fs";
import {dirname} from "node:path";
import {fileURLToPath} from "node:url";
import {spawnSync} from "node:child_process";
import {authoredPatternStyles} from "../../../components/BonfyreServiceSite/authored-pattern-styles.mjs";
// One pinned CSS transform, native: `bonfyre-surface-scene numeric lightning` (fixtures are test-only).
export function compileReaderCss({useLightning=false,fixture,engineLabel}={}){
 const source=authoredPatternStyles+"\n",args=["numeric","lightning",...(fixture?["--lightning-fixture",fixture]:[]),...(engineLabel?["--engine-label",engineLabel]:[])];
 if(!useLightning)return {css:source,engine:"postcss-compatibility"};
 const run=spawnSync(fileURLToPath(new URL("../bonfyre-surface-scene",import.meta.url)),args,{input:source,encoding:"utf8",maxBuffer:1<<20});
 if(run.status!==0)throw Error(String(run.stderr||run.error).trim());
 return JSON.parse(run.stdout);
}
if(process.env.BONFYRE_LIGHTNING_CSS && process.env.BONFYRE_LIGHTNING_CSS!=="1")
 throw Error("BONFYRE_LIGHTNING_CSS must be 1 or unset");
const stylesheet=compileReaderCss({useLightning:process.env.BONFYRE_LIGHTNING_CSS==="1"});
const base=new URL("../../../site/matrix/",import.meta.url);
const outputs=[
 {name:"authoring-reader.css",body:stylesheet.css},
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
