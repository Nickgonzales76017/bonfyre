#!/usr/bin/env bun
// Read-only estate schema crosswalk. Frappe is a donor grammar, never an
// authorization to migrate another institution's records or accounting effects.
// Only metadata and declared institutional contracts are read; no data rows.
import { Database } from 'bun:sqlite';
import { readdirSync, readFileSync, existsSync, mkdirSync, writeFileSync, statSync } from 'node:fs';
import { join, resolve, relative, basename } from 'node:path';
import { homedir } from 'node:os';
import { createHash } from 'node:crypto';

const APPS = ['frappe','erpnext','crm','hrms','lms','helpdesk','wiki','drive','insights'];
const skipDirs = new Set(['.git','node_modules','.venv','__pycache__','.github','.pi','frappe-ui','frontend','dist','build']);
const layouts = new Set(['Section Break','Column Break','Tab Break','HTML','Heading','Fold','Button']);
const links = new Set(['Link','Dynamic Link','Table','Table MultiSelect']);
const numberTypes = new Set(['Float','Int','Long Int','Currency','Percent','Rating','Duration']);
const dateTypes = new Set(['Date','Datetime','Time']);
const credentialPattern = /password|secret|token|api.?key|private.?key|credential|auth.?key/i;
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const norm = value => String(value??'').toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,'');
const quote = name => '"' + String(name).replaceAll('"','""') + '"';
function typeFamily(type) {
  const t=String(type??'');
  if (t==='Currency') return 'currency_decimal_semantics';
  if (numberTypes.has(t)) return t==='Int'||t==='Long Int'?'integer':'numeric';
  if (dateTypes.has(t)) return 'temporal';
  if (t==='Check') return 'boolean';
  if (t==='Password') return 'credential';
  if (t==='Link'||t==='Dynamic Link') return 'reference';
  if (t==='Table'||t==='Table MultiSelect') return 'child_records';
  if (layouts.has(t)) return 'presentation_only';
  if (t==='Select') return 'enumeration';
  if (/Attach|Image/.test(t)) return 'media_reference';
  if (t==='JSON'||t==='Code') return 'structured';
  return 'text_or_other';
}
function sqlFamily(type) {
  const t=String(type??'').toUpperCase();
  if (t.includes('INT')) return 'integer';
  if (t.includes('REAL')||t.includes('FLOAT')||t.includes('DOUBLE')) return 'floating_point';
  if (t.includes('DECIMAL')||t.includes('NUMERIC')) return 'numeric_declared';
  if (t.includes('BLOB')) return 'binary';
  if (t.includes('DATE')||t.includes('TIME')) return 'temporal_declared';
  if (t.includes('CHAR')||t.includes('TEXT')||t.includes('CLOB')) return 'text';
  return t?'other_declared':'untyped';
}
function parseArgs() {
  const args=process.argv.slice(2);
  const cfg={repo:resolve(import.meta.dir,'../../..'),estate:join(homedir(),'.bonfyre/estate-local'),
    out:'',ir:'',refresh:false};
  while(args.length){const a=args.shift();
    if(a==='--refresh-compiler'){cfg.refresh=true;continue;}
    if(!['--repo','--estate','--out','--ir'].includes(a)||!args[0])throw Error('unknown/incomplete argument '+a);
    cfg[a.slice(2)]=resolve(args.shift());
  }
  cfg.out ||=join(cfg.estate,'reports','estate-schema-crosswalk-20261009');
  cfg.ir ||=join(cfg.out,'native-frappe');
  return cfg;
}
function walk(root, pick) {
  const result=[],stack=[root];
  while(stack.length){const dir=stack.pop();if(!existsSync(dir))continue;
    for(const e of readdirSync(dir,{withFileTypes:true})){
      const p=join(dir,e.name);
      if(e.isDirectory()){if(!skipDirs.has(e.name))stack.push(p);}
      else if(e.isFile()&&pick(p))result.push(p);
    }
  }
  return result.sort();
}
function parseDocSources(repo,app){
  const base=join(repo,'integrations/frappe-bench/apps',app);
  const source=new Map();
  for(const file of walk(base,p=>p.endsWith('.json')&&p.includes('/doctype/'))){
    let parsed;try{parsed=JSON.parse(readFileSync(file,'utf8'));}catch{continue;}
    if(parsed.doctype!=='DocType'||!parsed.name||!Array.isArray(parsed.fields))continue;
    if(source.has(parsed.name))throw Error('ambiguous donor DocType '+app+'/'+parsed.name);
    const contents=readFileSync(file);
    source.set(parsed.name,{data:parsed,path:relative(repo,file),sha256:sha(contents)});
  }
  return source;
}
function emitSqliteSchema(cfg){
  const dirs=[
    [cfg.estate,'active_estate'],
    [join(cfg.repo,'integrations/frappe-bench/apps'),'donor_fixture'],
    [join(cfg.repo,'integrations/erpnext_app/private/files/bonfyre_erpnext/cms'),'erpnext_artifact'],
    [join(cfg.repo,'integrations/erpnext_app/private/files/bonfyre_erpnext/graph'),'erpnext_artifact'],
    [join(cfg.repo,'integrations/erpnext_app/private/files/bonfyre_erpnext/meter'),'erpnext_artifact'],
  ];
  const stores=[],tables=[],columns=[],foreignKeys=[];
  for(const [dir,source]of dirs){
    if(!existsSync(dir))continue;
    const files=readdirSync(dir).filter(n=>/\.(db|sqlite|sqlite3)$/i.test(n)
      &&!/before|legacy|replay|backup|\.bak|pre-|\.first/i.test(n)).sort();
    for(const name of files){
      const path=join(dir,name);
      if(!statSync(path).isFile())continue;
      const id=source+':'+name.replace(/\.db$/, '');
      let db;try{db=new Database(path,{readonly:true,create:false});
        const tableNames=db.query("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all();
        stores.push({store:id,source,path,size:statSync(path).size,tables:tableNames.length});
        for(const t of tableNames){
          const tab=id+':'+t.name;
          tables.push({table_id:tab,store:id,name:t.name,definition_sha256:t.sql?sha(t.sql):null});
          const pragma=db.query('PRAGMA table_xinfo('+quote(t.name)+')').all();
          for(const c of pragma)columns.push({column_id:tab+':'+c.name,store:id,table:t.name,column:c.name,
            sql_type:c.type||'',family:sqlFamily(c.type),not_null:!!c.notnull,primary_key_position:c.pk,
            hidden:!!c.hidden,source});
          for(const fk of db.query('PRAGMA foreign_key_list('+quote(t.name)+')').all())
            foreignKeys.push({store:id,from_table:t.name,from_column:fk.from,to_table:fk.table,to_column:fk.to,
              constraint_id:fk.id,seq:fk.seq});
        }
      }catch(e){stores.push({store:id,source,path,error:String(e.message).slice(0,180),tables:0});}
      finally{db?.close();}
    }
  }
  return{stores,tables,columns,foreignKeys};
}
function loadCmsContracts(cfg){
  const bases=[join(cfg.repo,'cmd/BonfyreCMS/content-types')];
  const packs=join(cfg.repo,'packs');
  if(existsSync(packs))for(const entry of readdirSync(packs,{withFileTypes:true}))
    if(entry.isDirectory()){const p=join(packs,entry.name,'schemas');if(existsSync(p))bases.push(p);}
  const result=[];
  for(const base of bases)for(const file of walk(base,p=>p.endsWith('.json'))){
    let x;try{x=JSON.parse(readFileSync(file,'utf8'));}catch{continue;}
    if(typeof x.name!=='string'||!x.attributes||typeof x.attributes!=='object')continue;
    const source=relative(cfg.repo,file);
    for(const [field,definition]of Object.entries(x.attributes)){
      result.push({type:x.name,field,declared_type:definition.type||'',required:!!definition.required,
        unique_in_namespace:!!definition.uniqueWithinNamespace,source,source_sha256:sha(readFileSync(file))});
    }
  }
  return result;
}
function loadDomainContracts(cfg){
  const dir=join(cfg.repo,'packs/upper90');const result=[];
  for(const name of ['product-contract-jrss.yaff','product-contract-ai-loss.yaff']){
    const path=join(dir,name);if(!existsSync(path))continue;
    let current='',product='';
    const lines=readFileSync(path,'utf8').split(/\r?\n/);
    lines.forEach((line,i)=>{
      const start=/^product_contract\s+(\S+)/.exec(line);
      if(start){current=start[1];product=current.includes('jrss')?'JRSS':'AI Loss';return;}
      const tag=/^\s{2}(subject|field|table|reconcile_entity|reconcile_entity_key|reconcile_entity_amount|reconcile_allocation|reconcile_allocation_key|reconcile_allocation_amount|reconcile_allocation_active|invariant|realized_by)\s+(.+)$/.exec(line);
      if(!current||!tag) return;
      result.push({institution:product,contract:current,kind:tag[1],declared_phrase:tag[2],source:relative(cfg.repo,path),
        line:i+1,physical_column_verified:false,authority:'domain_contract_not_live_database'});
    });
  }
  return result;
}
function outputLines(file,values){writeFileSync(file,values.map(x=>JSON.stringify(x)).join('\n')+(values.length?'\n':''),{mode:0o600});}
function build(){
  const cfg=parseArgs();mkdirSync(cfg.out,{recursive:true,mode:0o700});mkdirSync(cfg.ir,{recursive:true,mode:0o700});
  const donor=[],docs=[],edges=[],audit=[],recall=[];
  const bin=process.env.BONFYRE_FRAPPE_COMPILER||join(homedir(),'.bonfyre/bin/bonfyre-frappe-compiler');
  for(const app of APPS){
    const file=join(cfg.ir,app+'.schema-ir.json'),appPath=join(cfg.repo,'integrations/frappe-bench/apps',app);
    if(cfg.refresh||!existsSync(file)){
      const r=Bun.spawnSync([bin,'--root',cfg.repo,'--phase','1','--emit-schema-ir',file,appPath],
        {stdout:'pipe',stderr:'pipe',timeout:90000,maxBuffer:4e6});
      if(r.exitCode)throw Error('native compiler refused '+app+': '+r.stderr.toString().slice(0,500));
    }
    const src=JSON.parse(readFileSync(file,'utf8'));
    if(src.kind!=='BfSchemaGraph'||src.app!==app||!Array.isArray(src.schema?.doctypes))
      throw Error('native compiler IR contract mismatch: '+app);
    const originals=parseDocSources(cfg.repo,app);
    let skipped=0, mismatches=0;
    for(const dt of src.schema.doctypes){
      const o=originals.get(dt.name),originalFields=new Map((o?.data.fields||[]).map(f=>[f.fieldname,f]));
      if(o){
        const nativeNames=dt.fields.map(f=>f.name).sort().join('\0');
        const rawNames=(o.data.fields||[]).map(f=>f.fieldname).sort().join('\0');
        if(nativeNames!==rawNames){mismatches++;throw Error('native field loss/difference '+app+'/'+dt.name);}
      }else skipped++;
      docs.push({doc_id:app+':'+dt.name,app,name:dt.name,module:dt.module||'',source:o?.path||null,
        source_sha256:o?.sha256||null,compiler_only:!o,fields:dt.fields.length});
      const terms=[];
      for(const [ordinal,f] of dt.fields.entries()){
        const raw=originalFields.get(f.name),fieldtype=raw?.fieldtype||null;
        const family=fieldtype?typeFamily(fieldtype):'compiler_only_unresolved';
        const privacy=fieldtype==='Password'||credentialPattern.test(f.name)?'secret_name_guard':'no_secret_indicator';
        donor.push({doc_id:app+':'+dt.name,app,doctype:dt.name,field:f.name,ordinal,
          label:raw?.label||f.label||'',fieldtype,compiler_type:f.type,required:!!f.required,
          family,link_options:links.has(fieldtype)?raw?.options||'':null,
          conditional:!!(raw?.depends_on||raw?.mandatory_depends_on||raw?.read_only_depends_on),
          privacy,source:o?.path||null,source_sha256:o?.sha256||null});
        if(family!=='presentation_only'&&privacy!=='secret_name_guard')terms.push(f.name);
        if(links.has(fieldtype))edges.push({from:app+':'+dt.name,field:f.name,kind:fieldtype,
          target:raw?.options||'',target_is_dynamic:fieldtype==='Dynamic Link',source:o?.path||null});
      }
      recall.push({id:app+':'+dt.name,source:o?.path||'native-compiler:'+app,
        text:app+' '+dt.name+' module '+(dt.module||'')+' fields: '+terms.join(', ').slice(0,3600)});
    }
    audit.push({app,compiled_doctypes:src.schema.doctypes.length,
      compiled_fields:src.schema.doctypes.reduce((n,d)=>n+d.fields.length,0),
      raw_doctypes:originals.size,compiler_only:skipped,field_mismatch:mismatches,
      compiler_ir_sha256:sha(readFileSync(file))});
  }
  const physical=emitSqliteSchema(cfg),cms=loadCmsContracts(cfg),contracts=loadDomainContracts(cfg);
  const donorById=new Map(donor.map(f=>[f.doc_id+':'+f.field,f]));
  const sqliteCms=physical.columns.filter(f=>f.store==='active_estate:cms');
  const sqliteCmsNames=new Set(sqliteCms.map(f=>f.table+':'+f.column));
  const allLiveTables=new Set(physical.tables.filter(t=>t.store==='active_estate:cms').map(t=>t.name));
  const coverage=[],candidates=[];
  const nativeTables=new Map();
  for(const d of docs){
    const nativeName='frappe_'+d.app+'_'+norm(d.name);
    if(!allLiveTables.has(nativeName))continue;
    nativeTables.set(nativeName,d);
    let aligned=0, monetaryFloating=0;
    for(const f of donor.filter(f=>f.doc_id===d.doc_id&&f.family!=='presentation_only')){
      if(!sqliteCmsNames.has(nativeName+':'+f.field))continue;
      const physicalCol=sqliteCms.find(c=>c.table===nativeName&&c.column===f.field);
      const risk=f.family==='currency_decimal_semantics'&&physicalCol?.family==='floating_point'?'currency_precision_mismatch':null;
      if(risk)monetaryFloating++;
      candidates.push({source_kind:'native_cms_field',source_type:nativeName,source_field:f.field,
        donor_doc:d.doc_id,donor_field:f.field,classification:risk?'type_review_required':'exact_field_name_same_doc',
        relationship:'data_shape_only',conversion_allowed:false,warning:risk,
        proof:[f.source,f.source_sha256,'sqlite:active_estate:cms/'+nativeName+'/'+f.field]});
      aligned++;
    }
    coverage.push({doc:d.doc_id,table:nativeName,fields_compiled:d.fields,physical_exact_fields:aligned,
      currency_floating_fields:monetaryFloating,not_full_donor_runtime:true});
  }
  const contractAnchors=[
    {institution:'JRSS',contract:'pc:jrss-billing',concept:'Invoice',erpnext:'Sales Invoice',reason:'invoice/customer/amount identity, not a billing-cycle rule'},
    {institution:'JRSS',contract:'pc:jrss-billing',concept:'Payment',erpnext:'Payment Entry',reason:'recorded payment event, not a saved-card capture'},
    {institution:'JRSS',contract:'pc:jrss-billing',concept:'Payment allocation',erpnext:'Payment Entry Reference',reason:'per-invoice allocation; preserve residual, idempotency and transaction'},
    {institution:'JRSS',contract:'pc:jrss-billing',concept:'Statement / AR aging',erpnext:'Payment Ledger Entry',reason:'report projection over receivables; not a Journal Entry or invoice'},
    {institution:'JRSS',contract:'pc:jrss-daysheets',concept:'Billing cycle assignment',erpnext:'Sales Invoice',reason:'submission_date decides membership; posting_date is not equivalent'},
    {institution:'JRSS',contract:'pc:jrss-horses',concept:'Horse ownership shares',erpnext:'Customer',reason:'ownership percentages/effective transfers require separate domain records'},
    {institution:'AI Loss',contract:'pc:loss-payments',concept:'Incoming payment',erpnext:'Payment Entry',reason:'tblpayincoming.PayAmt referenced by contract; not a direct financial document equivalence'},
    {institution:'AI Loss',contract:'pc:loss-payments',concept:'Payment split',erpnext:'Payment Entry Reference',reason:'tblpayspltinc.PaySpltAIAmt allocation vs gross fee; preserve split tree'},
    {institution:'AI Loss',contract:'pc:loss-estimates-fees',concept:'Fee / estimate split tree',erpnext:'Journal Entry Account',reason:'tree/cascade semantics require original domain rules and reconciliation'},
  ].map((x,i)=>({...x,id:'anchor:'+String(i+1).padStart(3,'0'),
    state:'semantic_hypothesis_requires_physical_schema_and_witness',authorized_to_execute:false,
    evidence:x.institution==='JRSS'?'packs/upper90/product-contract-jrss.yaff':'packs/upper90/product-contract-ai-loss.yaff'}));
  for(const c of contractAnchors){
    if(!docs.some(x=>x.doc_id==='erpnext:'+c.erpnext))throw Error('unrecognized ERPNext anchor '+c.erpnext);
  }
  const declaredMissing=cms.filter(c=>allLiveTables.has(c.type)&&!sqliteCmsNames.has(c.type+':'+c.field));
  const declaredInstalled=cms.filter(c=>allLiveTables.has(c.type));
  const index=join(cfg.out,'schema-crosswalk.sqlite');
  // A disposable projection. Refuse to mistake this store for an owning finance/clinical database.
  if(existsSync(index))throw Error('derived index already exists: choose a fresh --out directory; never overwrite canonical stores');
  const db=new Database(index,{create:true});
  try{
    db.exec("CREATE TABLE donor_field(app TEXT, doctype TEXT, ordinal INTEGER, field TEXT, label TEXT, type TEXT, family TEXT, required INTEGER, privacy TEXT, source TEXT, PRIMARY KEY(app,doctype,ordinal));"+
      "CREATE TABLE physical_column(store TEXT, table_name TEXT, column_name TEXT, sql_type TEXT, family TEXT, not_null INTEGER, pk INTEGER, PRIMARY KEY(store,table_name,column_name));"+
      "CREATE TABLE proposed_crosswalk(source_type TEXT,source_field TEXT,donor_doc TEXT,donor_field TEXT,classification TEXT,warning TEXT, PRIMARY KEY(source_type,source_field,donor_doc,donor_field));"+
      "CREATE INDEX idx_donor_name ON donor_field(field);CREATE INDEX idx_phys_name ON physical_column(column_name);");
    const a=db.query('INSERT INTO donor_field VALUES(?,?,?,?,?,?,?,?,?,?)');
    const b=db.query('INSERT INTO physical_column VALUES(?,?,?,?,?,?,?)');
    const c=db.query('INSERT INTO proposed_crosswalk VALUES(?,?,?,?,?,?)');
    db.transaction(()=>{
      for(const f of donor)a.run(f.app,f.doctype,f.ordinal,f.field,f.label,f.fieldtype||'',f.family,+f.required,f.privacy,f.source||'');
      for(const x of physical.columns)b.run(x.store,x.table,x.column,x.sql_type,x.family,+x.not_null,x.primary_key_position);
      for(const x of candidates)c.run(x.source_type,x.source_field,x.donor_doc,x.donor_field,x.classification,x.warning||'');
    })();
  }finally{db.close();}
  const files={
    donor_fields:'frappe-fields.jsonl',donor_docs:'frappe-doctypes.jsonl',
    donor_links:'frappe-relationships.jsonl',native_columns:'native-columns.jsonl',
    native_tables:'native-tables.jsonl',native_stores:'native-stores.jsonl',
    physical_foreign_keys:'native-foreign-keys.jsonl',cms_declared:'cms-declared-fields.jsonl',
    crosswalk:'verified-shape-crosswalk.jsonl',contracts:'institutional-contract-evidence.jsonl',
    anchored:'semantic-crosswalk-hypotheses.jsonl',recall:'recall-schema-candidates.jsonl'
  };
  const values={donor_fields:donor,donor_docs:docs,donor_links:edges,native_columns:physical.columns,
    native_tables:physical.tables,native_stores:physical.stores,physical_foreign_keys:physical.foreignKeys,
    cms_declared:cms,crosswalk:candidates,contracts,anchored:contractAnchors,recall};
  for(const [key,name]of Object.entries(files))outputLines(join(cfg.out,name),values[key]);
  const report={
    schema:'bonfyre.estate.schema-crosswalk-index.v1',built_at:new Date().toISOString(),root:cfg.repo,
    donor:{apps:audit,doctypes:docs.length,fields:donor.length,declared_relationships:edges.length,
      missing_raw_doctypes:docs.filter(x=>x.compiler_only).map(x=>x.doc_id),
      fields_with_missing_names:donor.filter(x=>!x.field).length},
    stores:{store_count:physical.stores.length,tables:physical.tables.length,columns:physical.columns.length,
      foreign_keys:physical.foreignKeys.length,breakdown:physical.stores.map(s=>({store:s.store,source:s.source,tables:s.tables,error:s.error||null}))},
    cms:{declared_fields:cms.length,declared_installed:declaredInstalled.length,
      missing_declared_physical_fields:declaredMissing.length,
      missing_samples:declaredMissing.slice(0,80),compiler_native_projection_tables:coverage},
    crosswalk:{exact_field_name_candidates:candidates.length,
      safe_to_execute_count:0,semantic_hypotheses:contractAnchors.length,
      source_physical_schema_missing:['JRSSv1 PostgreSQL','AiLtd_Loss ai_carol MySQL'],
      boundary:'Exact aligned field names are shape evidence, not Frappe controllers, permission, compatible money precision or record equivalence'},
    outputs:{...files,index:'schema-crosswalk.sqlite'},
    forbidden_inferences:['A declared JRSS field phrase is not a physical SQL column',
      'An AI Loss code/table reference is not a live database inspection',
      'A schema-aligned field is not a valid migration or permission to write',
      'Currency in SQLite REAL is not decimal-preserving money semantics',
      'A Frappe Link/Table field is not necessarily an installed foreign key',
      'Frappe DocType exists does not mean Frappe app is deployed and running',
      'Recall semantic similarity is candidate discovery only, never canonical schema truth']
  };
  writeFileSync(join(cfg.out,'coverage-and-gaps.json'),JSON.stringify(report,null,2)+'\n',{mode:0o600});
  console.log(JSON.stringify({report:join(cfg.out,'coverage-and-gaps.json'),index,donor:report.donor,
    stores:{count:report.stores.store_count,tables:report.stores.tables,columns:report.stores.columns},
    cms:{declared:report.cms.declared_fields,missing:report.cms.missing_declared_physical_fields,
      projected_tables:coverage.length},crosswalk:report.crosswalk}));
}
try{build();}catch(e){console.error(e.stack||String(e));process.exitCode=1;}
