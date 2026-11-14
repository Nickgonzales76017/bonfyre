import {test,expect} from "bun:test";
import {mkdtempSync,rmSync,existsSync} from "node:fs";
import {tmpdir,homedir} from "node:os";
import {join,resolve} from "node:path";
import {spawnSync} from "node:child_process";
import {Database} from "bun:sqlite";
const root=resolve(import.meta.dir,"../../..");
const installer=join(root,"cmd/BonfyreCMS/tools/install_dating_editorial.mjs");
const local=join(root,"cmd/BonfyreCMS/bonfyre-cms");
const native=existsSync(local)?local:join(homedir(),".bonfyre/bin/bonfyre-cms");
const run=(db,...args)=>spawnSync("bun",[installer,"--db",db,"--cms",native,...args],{
 cwd:root,encoding:"utf8",timeout:90000
});
test("Stitch editorial CMS composition is exact, private and reproducible, not an identity",()=>{
 if(!existsSync(native))throw Error("Native BonfyreCMS binary is required");
 const dir=mkdtempSync(join(tmpdir(),"bonfyre-dating-cms-"));
 const path=join(dir,"site.db");
 try{
  expect(run(path).status).toBe(2); /* no implicit publication */
  const first=run(path,"--publish");
  expect(first.status).toBe(0);
  const one=JSON.parse(first.stdout.trim().split("\n").at(-1));
  expect(one.created).toBe(5);
  expect(one.screens).toEqual(["discover","matches","chat","studio"]);
  const db=new Database(path,{readonly:true,strict:true});
  try{
   expect(db.query("SELECT COUNT(*) AS n FROM presentation_template WHERE template='aurekai-dating-editorial' AND status='published'").get().n).toBe(1);
   expect(db.query("SELECT COUNT(*) AS n FROM site_fragment WHERE namespace='root' AND page='/profile/dating' AND audience='self' AND kind='dating-view' AND status='published'").get().n).toBe(4);
   expect(db.query("SELECT COUNT(*) AS n FROM site_fragment WHERE page='/profile/dating' AND audience='public'").get().n).toBe(0);
   const rows=db.query("SELECT slot,heading,body FROM site_fragment WHERE page='/profile/dating' ORDER BY position").all();
   expect(rows.map(x=>x.slot)).toEqual(["discover","matches","chat","studio"]);
   expect(rows.every(x=>x.heading&&x.body)).toBe(true);
   expect(db.query("SELECT COUNT(*) AS n FROM sqlite_master WHERE name LIKE 'dating_person' OR name LIKE 'dating_message'").get().n).toBe(0);
  }finally{db.close();}
  const again=run(path,"--publish");
  expect(again.status).toBe(0);
  expect(JSON.parse(again.stdout.trim().split("\n").at(-1)).created).toBe(0);
  const check=run(path,"--check");
  expect(check.status).toBe(0);
  expect(JSON.parse(check.stdout.trim().split("\n").at(-1)).verified).toBe(5);
 }finally{rmSync(dir,{recursive:true,force:true});}
},120000);
