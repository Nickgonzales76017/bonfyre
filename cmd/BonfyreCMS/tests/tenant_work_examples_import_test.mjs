#!/usr/bin/env bun
// Real SQLite and exact CLI argv boundary; no Python, foreign effects or deletes.
import { Database } from "bun:sqlite";
import { mkdtempSync, mkdirSync, writeFileSync, chmodSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import assert from "node:assert/strict";

const root=resolve(import.meta.dir,"../../..");
const importer=join(root,"cmd/BonfyreCMS/tools/install_work_examples.sh");
const sqliteCmd=[
 "#!/usr/bin/env bun",
 'import { Database } from "bun:sqlite";',
 'const a=process.argv.slice(2).filter(x=>x!=="-readonly");',
 'if(a.length!==2||!a[1].trim().toUpperCase().startsWith("SELECT "))process.exit(9);',
 'const db=new Database(a[0],{readonly:true});',
 'try{for(const row of db.query(a[1]).all())console.log(Object.values(row).map(v=>v==null?"":String(v)).join("|"))}',
 'finally{db.close()}',
].join("\n")+"\n";
const mockCms=[
 "#!/usr/bin/env bun",
 'import { Database } from "bun:sqlite";',
 'const a=process.argv.slice(2), dbpath=a[a.indexOf("--db")+1];',
 'if(!dbpath)process.exit(4);',
 'const db=new Database(dbpath);',
 'try {',
 ' if(a[0]==="schema"&&a[1]==="migrate"){',
 '  db.exec("CREATE TABLE IF NOT EXISTS program_template(id INTEGER PRIMARY KEY,namespace TEXT,template_id TEXT,status TEXT,description TEXT)");',
 '  db.exec("CREATE TABLE IF NOT EXISTS work_example(id INTEGER PRIMARY KEY,namespace TEXT,example_id TEXT,program_template TEXT,headline TEXT)");',
 ' } else if(a[0]==="entry"&&a[1]==="create"){',
 '  const type=a[2],rec=JSON.parse(a[3]),ns=a[a.indexOf("--ns")+1];',
 '  if(type==="program_template")db.query("INSERT INTO program_template(namespace,template_id,status,description) VALUES (?,?,?,?)").run(ns,rec.template_id,rec.status,rec.description??"");',
 '  else if(type==="work_example")db.query("INSERT INTO work_example(namespace,example_id,program_template,headline) VALUES (?,?,?,?)").run(ns,rec.example_id,rec.program_template,rec.headline??"");',
 '  else process.exit(5);',
 ' } else if(a[0]==="entry"&&a[1]==="update"){',
 '  if(a[2]!=="work_example")process.exit(6);',
 '  if(process.env.FAIL_CMS_UPDATE==="1")process.exit(7);',
 '  const rec=JSON.parse(a[4]);',
 '  const result=db.query("UPDATE work_example SET program_template=?,headline=? WHERE id=?").run(rec.program_template,rec.headline??"",Number(a[3]));',
 '  if(result.changes!==1)process.exit(8);',
 ' } else process.exit(9);',
 '} finally{db.close()}',
].join("\n")+"\n";
const temp=mkdtempSync(join(tmpdir(),"bf-tenant-work-cms-"));
const query=(db,ns,table,idfield,id)=>db.query("SELECT * FROM "+table+" WHERE namespace=? AND "+idfield+"=?").get(ns,id);
try{
 const bin=join(temp,"bin");mkdirSync(bin);
 const sqlite=join(bin,"sqlite3"),cms=join(bin,"mock-cms");
 writeFileSync(sqlite,sqliteCmd);chmodSync(sqlite,0o755);
 writeFileSync(cms,mockCms);chmodSync(cms,0o755);
 const path=join(temp,"cms.db"),db=new Database(path);
 db.exec("CREATE TABLE program_template(id INTEGER PRIMARY KEY,namespace TEXT,template_id TEXT,status TEXT,description TEXT)");
 db.exec("CREATE TABLE work_example(id INTEGER PRIMARY KEY,namespace TEXT,example_id TEXT,program_template TEXT,headline TEXT)");
 db.query("INSERT INTO program_template(namespace,template_id,status) VALUES ('root','shared','published')").run();
 db.query("INSERT INTO work_example(namespace,example_id,program_template,headline) VALUES ('root','shared-work','shared','root owns this')").run();
 const programs=join(temp,"programs.jsonl"),work=join(temp,"work.jsonl");
 writeFileSync(programs,'{"template_id": "shared", "status": "published", "description": "tenant program"}\n');
 writeFileSync(work,'{"example_id": "shared-work", "program_template": "shared", "headline": "tenant first"}\n');
 const env={...process.env,PATH:bin+":"+process.env.PATH,
  BONFYRE_CMS_BINARY:cms,BONFYRE_WORK_EXAMPLES:work,
  BONFYRE_WORK_EXAMPLE_PROGRAMS:programs,BONFYRE_CMS_NAMESPACE:"tenant_one"};
 const run=(e,p=path)=>spawnSync("sh",[importer,p],{env:e,encoding:"utf8"});
 const orig=query(db,"root","work_example","example_id","shared-work");
 let r=run(env);assert.equal(r.status,0,r.stderr);
 const first=query(db,"tenant_one","work_example","example_id","shared-work");
 assert.equal(first.headline,"tenant first");
 assert.deepEqual(query(db,"root","work_example","example_id","shared-work"),orig);
 assert.ok(query(db,"tenant_one","program_template","template_id","shared"));
 writeFileSync(work,'{"example_id": "shared-work", "program_template": "shared", "headline": "tenant updated"}\n');
 r=run({...env,FAIL_CMS_UPDATE:"1"});assert.notEqual(r.status,0,"native refusal must stop");
 assert.deepEqual(query(db,"tenant_one","work_example","example_id","shared-work"),first);
 r=run(env);assert.equal(r.status,0,r.stderr);
 const updated=query(db,"tenant_one","work_example","example_id","shared-work");
 assert.equal(updated.id,first.id,"reimport destroyed stable CMS row ID");
 assert.equal(updated.headline,"tenant updated");
 assert.deepEqual(query(db,"root","work_example","example_id","shared-work"),orig);
 r=run({...env,BONFYRE_CMS_NAMESPACE:"tenant_two"});
 assert.equal(r.status,0,r.stderr);
 assert.ok(query(db,"tenant_two","work_example","example_id","shared-work"));
 assert.deepEqual(query(db,"tenant_one","work_example","example_id","shared-work"),updated);
 r=run({...env,BONFYRE_CMS_NAMESPACE:""});
 assert.notEqual(r.status,0);assert.match(r.stderr,/requires BONFYRE_CMS_NAMESPACE/);
 r=run({...env,BONFYRE_CMS_NAMESPACE:"tenant' OR 1=1"});
 assert.notEqual(r.status,0);assert.match(r.stderr,/invalid CMS namespace/);
 r=run(env,join(temp,"nonexistent.db"));assert.notEqual(r.status,0);
 db.close();
 console.log("TENANT_WORK_EXAMPLES_PASS: namespace isolation, idempotent updates, failed-write preservation");
} finally{rmSync(temp,{recursive:true,force:true})}
