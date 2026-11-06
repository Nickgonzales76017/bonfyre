// Typed source cuts are owned by BonfyreHash; this file only binds its execution.
import {createHash} from 'node:crypto';
import {existsSync,lstatSync,realpathSync,statSync} from 'node:fs';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {hashFile} from './arrow-runtime-verify.mjs';
const here=dirname(fileURLToPath(import.meta.url));
const identity=s=>[s.dev,s.ino,s.size,s.mtimeMs,s.ctimeMs].join(':');
const run=argv=>{
  const r=Bun.spawnSync(argv,{stdout:'pipe',stderr:'pipe',timeout:30000,maxBuffer:16384});
  if(r.exitCode!==0)throw Error('native table-cut compiler unavailable');
  if(r.stdout.length>16384)throw Error('native table-cut response exceeds limit');
  return r.stdout.toString();
};
export async function tableCutRuntime(compilerSource,hashBinary) {
  const requested=hashBinary??process.env.BONFYRE_HASH_BINARY??join(here,'../cmd/BonfyreHash/bonfyre-hash');
  if(!existsSync(requested)||!lstatSync(requested).isFile())throw Error('native table-cut hash owner unavailable');
  const binary=realpathSync(requested),duck=Bun.which('duckdb');
  if(!duck)throw Error('native table-cut Parquet owner unavailable');
  const duckdb=realpathSync(duck);
  // A bounded metadata response at an external CLI boundary, never source rows.
  const rows=JSON.parse(run([duckdb,'-json','-c',"LOAD sqlite; SELECT extension_name,extension_version,install_path FROM duckdb_extensions() WHERE extension_name='sqlite_scanner';"]));
  if(!Array.isArray(rows)||rows.length!==1||rows[0].extension_name!=='sqlite_scanner'||typeof rows[0].install_path!=='string'||!existsSync(rows[0].install_path))throw Error('SQLite lowering extension is not pinned');
  const paths=[binary,duckdb,realpathSync(rows[0].install_path),compilerSource,fileURLToPath(import.meta.url)];
  const bindings=[];
  for(const path of paths){const before=identity(statSync(path)),digest=await hashFile(path);if(identity(statSync(path))!==before)throw Error('compiler changed during binding');bindings.push({path,digest,identity:before});}
  const compiler='bonfyre.sqlite-table-parquet.v1:'+createHash('sha256').update(bindings.map(x=>x.digest).join('\n')).digest('hex');
  const check=()=>{for(const bound of bindings)if(identity(statSync(bound.path))!==bound.identity)throw Error('compiler changed during materialization');};
  return {binary,duckdb,compiler,transform:'sqlite-table->parquet',check};
}
export function nativeTableCut(runtime,snapshot,table) {
  runtime.check();
  const wire=run([runtime.binary,'sqlite-table',snapshot,table]);
  const match=/^BFSQL1\t([a-f0-9]{64})\t(\d+)\t(\d+)\n$/.exec(wire);
  if(!match)throw Error('native table-cut contract mismatch');
  const rows=Number(match[2]),columns=Number(match[3]);
  if(!Number.isSafeInteger(rows)||!Number.isSafeInteger(columns)||columns<1)throw Error('native table-cut count invalid');
  runtime.check();return {digest:match[1],rows,columns};
}
