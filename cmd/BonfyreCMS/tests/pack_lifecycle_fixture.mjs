import {mkdtempSync,mkdirSync,copyFileSync,rmSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {Database} from 'bun:sqlite';
export const repo=resolve(import.meta.dir,'../../..');
export function packFixture(){
 const home=mkdtempSync(join(tmpdir(),'pack-lifecycle-')),db=join(home,'cms.db'),schemas=join(home,'schemas'),binary=join(repo,'cmd/BonfyreCMS/bonfyre-cms');mkdirSync(schemas);
 const types=['pack_artifact','pack_version','pack_head','pack_dependency','pack_overlay','pack_demand','pack_installation','pack_activation'];
 for(const type of types)copyFileSync(join(repo,'cmd/BonfyreCMS/content-types',type+'.json'),join(schemas,type+'.json'));
 const env={...process.env,BONFYRE_FACT_DELTAS:''};
 function native(...args){return spawnSync(binary,[...args,'--db',db,'--schemas',schemas],{encoding:'utf8',env,timeout:15000,maxBuffer:1024*1024});}
 const migration=native('schema','migrate');if(migration.status!==0)throw Error(migration.stderr);
 const sql=new Database(db);
 function add(type,value,ns='root'){const r=native('entry','create',type,JSON.stringify(value),'--ns',ns);if(r.status!==0)throw Error(type+': '+r.stderr+' '+r.stdout);return r;}
 const hash=text=>createHash('sha256').update(text).digest('hex');
 function pack(id,{version='1',layer='official',kind='base',name=id,head=true}={}){
  add('pack_artifact',{pack:id,canonical_name:name,publisher:'fixture-publisher',layer,kind,purpose:'Explicitly fictional workflow configuration for native integration tests.',governed_by:'fixture-owner',cannot_infer:'Fixture configuration is not a provisioned real product.',status:'published'});
  add('pack_version',{pack:id,version,root:hash(id+':'+version),changed:'Initial fixture version',status:'published'});
  if(head)add('pack_head',{pack:id,channel:'stable',version,advanced_by:'fixture-owner',status:'published'});
 }
 function dependency(from,to,pin=''){add('pack_dependency',{pack:from,depends_on:to,pinned_version:pin,channel:'stable',why:'Required fixture behavior',status:'published'});}
 function parse(raw){const result={nodes:[]};for(const line of raw.trim().split('\n')){const [key,...values]=line.split('\t');if(key==='node')result.nodes.push(values);else if(values.length)result[key]=values.join('\t');}return result;}
 function call(operation,pack,options={}){
  const actor=options.actor??'user:101',subject=options.subject??actor,scope=options.scope??'self';
  const args=['pack-lifecycle','--operation',operation,'--pack',pack,'--subject',subject,'--scope',scope,'--actor',actor,'--version',options.version??'','--expected-revision',String(options.revision??0),'--expected-digest',options.digest??''];
  const run=native(...args);return {code:run.status,raw:run.stdout,stderr:run.stderr,...parse(run.stdout),args:[...args,'--db',db,'--schemas',schemas]};
 }
 function close(){sql.close();rmSync(home,{recursive:true,force:true});}
 return {home,db,schemas,binary,env,sql,add,pack,dependency,call,parse,hash,close};
}
