#!/usr/bin/env bun
// Install the typed Stitch-inspired dating composition through the SAME native
// BonfyreCMS owner as every Aurekai page. No sample members or private answers.
// Usage: bun cmd/BonfyreCMS/tools/install_dating_editorial.mjs --db DB --publish
//        bun cmd/BonfyreCMS/tools/install_dating_editorial.mjs --db DB --check
import {spawnSync} from "node:child_process";
import {readFileSync,existsSync} from "node:fs";
import {homedir} from "node:os";
import {join,resolve} from "node:path";
import {Database} from "bun:sqlite";
const root=resolve(import.meta.dir,"../../.."),args=process.argv.slice(2);
const value=(name)=>{const at=args.indexOf(name);return at<0?null:args[at+1]||null;};
const database=value("--db"),binary=value("--cms")||join(homedir(),".bonfyre/bin/bonfyre-cms");
const write=args.includes("--publish"),check=args.includes("--check");
if(!database||!database.startsWith("/")||!existsSync(binary)||write===check||
  args.some(x=>!["--db","--cms","--publish","--check",database,binary].includes(x))){
 console.error("Usage: --db ABSOLUTE_DB [--cms BIN] (--publish | --check)");
 process.exit(2);
}
const schemas=join(root,"cmd/BonfyreCMS/content-types");
const manifest=JSON.parse(readFileSync(join(root,"cmd/BonfyreCMS/data/dating-editorial-composition.json"),"utf8"));
if(manifest.schema!=="aurekai.dating.composition.v1"||
  manifest.template.template!=="aurekai-dating-editorial"||
  manifest.fragments.length!==4||
  manifest.fragments.map(x=>x.slot).join(",")!=="discover,matches,chat,studio"||
  manifest.fragments.some(x=>x.audience!=="self"||x.kind!=="dating-view"||
  x.page!=="/profile/dating"||x.status!=="published"))
 throw Error("The dating editorial manifest is outside its reviewed bounds");
function native(argv){
 const run=spawnSync(binary,[...argv,"--db",database,"--schemas",schemas],{encoding:"utf8",timeout:30000});
 if(run.status!==0)
  throw Error("Native CMS refused "+argv.slice(0,3).join(" ")+": "+
   String(run.stderr||run.stdout).split("\n").filter(s=>s&&!s.startsWith("[schema]")).join(" ").slice(0,1300));
 return run.stdout.trim();
}
if(write)native(["schema","migrate"]);
if(!existsSync(database))throw Error("Target CMS database does not exist; --check cannot create it");
const columns={
 presentation_template:{column:"template",value:manifest.template.template},
 site_fragment:{column:"coordinate",value:null}
};
function inspect(type,identity){
 const db=new Database(database,{readonly:true,strict:true});
 try{
  const column=columns[type].column;
  const query=db.query("SELECT * FROM "+type+" WHERE namespace='root' AND "+column+"=?1 LIMIT 2");
  const rows=query.all(identity);
  if(rows.length>1)throw Error("Ambiguous CMS coordinate: "+identity);
  return rows[0]||null;
 }finally{db.close();}
}
function exact(type,identity,attrs){
 const row=inspect(type,identity);
 if(!row)return false;
 for(const [field,target] of Object.entries(attrs)){
  let observed=row[field];
  if(target&&typeof target==="object"){
   try{observed=JSON.parse(observed);}catch{return false;}
   if(JSON.stringify(observed)!==JSON.stringify(target))return false;
  }else if(String(observed??"")!==String(target))return false;
 }
 return true;
}
const entries=[
 ["presentation_template",manifest.template.template,manifest.template],
 ...manifest.fragments.map(x=>["site_fragment",x.coordinate,x])
];
let created=0,updated=0,verified=0;
for(const [type,identity,attributes] of entries){
 const existing=inspect(type,identity);
 if(existing){
  if(type==="site_fragment"&&
     (existing.kind!=="dating-view"||existing.page!=="/profile/dating"||
      existing.audience!=="self"))
   throw Error("Refusing to replace an unrelated CMS fragment "+identity);
  if(type==="presentation_template"&&existing.reference_source!==manifest.template.reference_source)
   throw Error("Refusing to replace an unrelated editorial template");
 }
 if(exact(type,identity,attributes)){verified++;continue;}
 if(check)throw Error("CMS composition not installed or differs: "+type+" "+identity);
 if(existing){
  native(["entry","update",type,String(existing.id),JSON.stringify(attributes)]);
  updated++;
 }else{
  native(["entry","create",type,"--ns","root",JSON.stringify(attributes)]);
  created++;
 }
 if(!exact(type,identity,attributes))
  throw Error("Native CMS write did not read back exactly: "+type+" "+identity);
 verified++;
}
console.log(JSON.stringify({
 schema:manifest.schema,db:database,mode:write?"publish":"check",
 created,updated,verified,template:manifest.template.template,
 screens:manifest.fragments.map(x=>x.slot),
 media:"existing published Life site gallery; no synthetic assets",
 actions:"native BonfyreParticipation only",
 cannot_infer:"CMS composition publication is not a public WebAddress release or identity approval"
}));
