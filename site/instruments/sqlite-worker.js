/* Real SQLite in a disposable WASM database. This worker never opens a server DB. */
importScripts('./vendor/sql-wasm.js');
const ready=initSqlJs({locateFile:name=>new URL('./vendor/'+name,self.location).href});
function quoted(s){return '"'+s.replaceAll('"','""')+'"';}
function rows(db,sql){const r=db.exec(sql);return r.length?r[0].values.map(v=>Object.fromEntries(r[0].columns.map((c,i)=>[c,v[i]]))):[];}
function snapshot(db){
  const tables=rows(db,"SELECT name,sql FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 129");if(tables.length>128)throw Error('This editor supports at most 128 tables.');
  for(const t of tables){t.columns=rows(db,'PRAGMA table_info('+quoted(t.name)+')');t.foreignKeys=rows(db,'PRAGMA foreign_key_list('+quoted(t.name)+')');t.indexes=rows(db,'PRAGMA index_list('+quoted(t.name)+')');t.rows=rows(db,'SELECT count(*) AS n FROM '+quoted(t.name))[0].n;t.sample=rows(db,'SELECT * FROM '+quoted(t.name)+' LIMIT 30');}
  return {tables,indexNames:rows(db,"SELECT name FROM sqlite_schema WHERE type='index'").map(r=>r.name),foreignKeyErrors:rows(db,'PRAGMA foreign_key_check'),version:rows(db,'SELECT sqlite_version() AS version')[0].version};
}
self.onmessage=async({data})=>{const {id,action,input,operation}=data;let db;try{
  if(input&&(!(input instanceof Uint8Array)||input.byteLength>16*1024*1024))throw Error('Choose a SQLite database no larger than 16 MiB.');
  const SQL=await ready,core=await import('./schema.mjs');db=input?new SQL.Database(input):new SQL.Database();db.exec('PRAGMA foreign_keys=ON;');
  if(action==='example')db.exec(core.starterSQL);
  let before=null,sql='';
  if(action==='migrate'){before=snapshot(db);if(before.foreignKeyErrors.length)throw Error('The imported database already has foreign-key violations. Resolve these before this rehearsal.');sql=core.migrationSQL(before,operation);db.exec('BEGIN IMMEDIATE;');db.exec(sql);if(rows(db,'PRAGMA foreign_key_check').length)throw Error('The change creates a foreign-key violation.');db.exec('COMMIT;');}
  if(!['example','inspect','migrate'].includes(action))throw Error('Unknown SQLite operation.');
  const integrity=rows(db,'PRAGMA integrity_check');if(integrity.some(r=>Object.values(r)[0]!=='ok'))throw Error('Database integrity check failed.');
  const state=snapshot(db),bytes=db.export();if(bytes.byteLength>16*1024*1024)throw Error('Output exceeds the database bound.');
  self.postMessage({id,ok:true,state,bytes,sql,before},[bytes.buffer]);
}catch(error){self.postMessage({id,ok:false,error:error.message});}finally{db?.close();}};
