#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

bun -e '
import {Database} from "bun:sqlite";
import {readFileSync} from "node:fs";
import {createHash} from "node:crypto";
import {join} from "node:path";
const base=process.argv[1], script=process.argv[2];
const db=new Database(join(base,"cms.db"));
db.exec("CREATE TABLE program_template(template_id TEXT,name TEXT,description TEXT,semantic_slots TEXT,funding_candidates TEXT,status TEXT); CREATE TABLE program_instance(instance_id TEXT,template_id TEXT,owner_subject TEXT,site_id TEXT,archetype TEXT,status TEXT); CREATE TABLE funding_work_package(instance_id TEXT,funding_program TEXT,status TEXT)");
db.query("INSERT INTO program_template VALUES(?,?,?,?,?,?)").run("t","Library programme","A public reading programme","[]","[\"IMLS NLG-L\",\"NSF S&CC\"]","published");
db.query("INSERT INTO program_instance VALUES(?,?,?,?,?,?)").run("i","t","institution:library","site:library","library","published");
const add=db.query("INSERT INTO funding_work_package VALUES(?,?,?)");
add.run("i","IMLS NLG-L","published"); add.run("i","NSF S&CC","published");
db.close();
const body=JSON.stringify({data:{hitCount:2,oppHits:[
 {id:"generic",title:"General opportunity",agency:"Other",number:"O-1",oppStatus:"posted"},
 {id:"library",title:"National Leadership Grants Libraries",agency:"IMLS",number:"L-1",oppStatus:"posted"}
]}});
const digest="sha256:"+createHash("sha256").update(body).digest("hex");
let requests=0;
globalThis.fetch=async (_url,request)=>{
 requests++;
 const query=JSON.parse(request.body);
 if(query.keyword==="Smart Connected Communities") throw new Error("provider unavailable");
 if(query.keyword!=="National Leadership Grants Libraries"||query.rows!==25) throw new Error("wrong query");
 return new Response(body,{status:200,headers:{"Content-Type":"application/json"}});
};
const {main}=await import(script);
const rc=await main(["--cms",join(base,"cms.db"),"--out",join(base,"out"),"--rows","1"]);
if(rc!==0||requests!==2) throw new Error("observer did not run exactly two bounded requests");
const report=JSON.parse(readFileSync(join(base,"out/funding-frontier.json"),"utf8"));
const good=report.observations["IMLS NLG-L"],bad=report.observations["NSF S&CC"];
if(good.observation_state!=="observed"||good.provider_hit_count!==2||good.returned.length!==1||good.returned[0].opportunity_id!=="library"||good.response_digest!==digest) throw new Error("observed receipt mismatch");
if(bad.observation_state!=="unavailable"||bad.returned.length!==0||!bad.reason.includes("provider unavailable")) throw new Error("provider refusal was laundered");
const html=readFileSync(join(base,"out/funding.html"),"utf8");
if(!html.includes("National Leadership Grants Libraries")||!html.includes("not treated as zero opportunities")) throw new Error("frontier page missing truth boundary");
' "$tmp" "$root/scripts/bootstrap_funding_frontier.mjs" >/dev/null
echo 'bootstrap_funding_observation_test: PASS'
