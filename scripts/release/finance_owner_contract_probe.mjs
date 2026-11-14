// Schema-only diagnostics for native Finance/CRM/Outreach readback.
// No row bodies, emails, names or secrets enter this receipt.
import {Database} from 'bun:sqlite';
import {existsSync,lstatSync,statSync,realpathSync,accessSync,constants,openSync,readSync,closeSync} from 'node:fs';
import {homedir} from 'node:os';
import {join} from 'node:path';
const root=homedir();
const sources=[
 ['finance',join(root,'.bonfyre/estate-local/finance.db'),{
  opportunities:['id','title','stage','compensation_cents','source_ref','actual_revenue_cents','expected_compute_cents','expected_external_cents'],
  opportunity_requirements:['opportunity_id','state','kind','requirement_key','evidence_ref']}],
 ['cms',join(root,'.bonfyre/estate-local/cms.db'),{
  frappe_crm_crm_deal:['id','namespace','source','organization_name','email','communication_status','next_step']}],
 ['outreach',join(root,'.local/share/bonfyre/outreach.db'),{
  sends:['id','target','offer_name','reply_type']}]
];
const results=[];
for(const [owner,file,tables] of sources){
 if(!existsSync(file)){results.push({owner,available:false});continue;}
 let db;
 try{
  db=new Database(file,{readonly:true,create:false});db.exec('PRAGMA query_only=ON');
  const checks=[];
  for(const [table,required] of Object.entries(tables)){
   const cols=db.query('PRAGMA table_info("'+table+'")').all().map(x=>x.name);
   checks.push({table,present:cols.length>0,missing:required.filter(x=>!cols.includes(x)),observedColumns:cols});
  }
  results.push({owner,available:true,checks});
 }catch(e){
  let meta=null;
  try{
   const l=lstatSync(file),s=statSync(file),canRead=(()=>{try{accessSync(file,constants.R_OK);return true;}catch{return false;}})();
   let signature='',fd;
   try{fd=openSync(file,'r');const bytes=Buffer.alloc(16);readSync(fd,bytes,0,16,0);signature=bytes.toString('ascii');}catch{}
   finally{if(fd!==undefined)closeSync(fd);}
   meta={regular:s.isFile(),symbolic:l.isSymbolicLink(),mode:(s.mode&0o777).toString(8),owner:s.uid,group:s.gid,
    bytes:s.size,canRead,signatureValid:signature==='SQLite format 3\0',realpath:realpathSync(file)===file};
  }catch(err){meta={error:String(err.message).slice(0,100)};}
  results.push({owner,available:false,error:String(e.message).slice(0,150),code:e.code,meta});
 }
 finally{db?.close();}
}
console.log(JSON.stringify({schema:'bonfyre.finance.owner-contract.probe.v1',results}));
