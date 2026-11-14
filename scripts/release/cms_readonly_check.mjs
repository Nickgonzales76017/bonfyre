// Read-only diagnosis of the exact CMS database carried by a staged public service.
import {Database} from 'bun:sqlite';
import {statSync,lstatSync,realpathSync,accessSync,constants,existsSync,openSync,readSync,closeSync} from 'node:fs';
import {dirname} from 'node:path';
const [plist]=process.argv.slice(2);
if(!plist)throw Error('Provide the frozen candidate-service.plist');
const p=Bun.spawnSync(['/usr/bin/plutil','-convert','json','-o','-',plist],{stdout:'pipe',stderr:'pipe',timeout:10000});
if(p.exitCode!==0)throw Error('Candidate plist cannot be read: '+p.stderr.toString().slice(0,160));
const config=JSON.parse(p.stdout.toString()),raw=config.EnvironmentVariables?.BONFYRE_CMS_DB;
if(typeof raw!=='string'||!raw.startsWith('/'))throw Error('No absolute canonical CMS path');
const state=path=>{try{const l=lstatSync(path),s=statSync(path);return {exists:true,regular:s.isFile(),link:l.isSymbolicLink(),bytes:s.size,mode:(s.mode&0o777).toString(8),uid:s.uid,gid:s.gid,real:realpathSync(path),readable:(()=>{try{accessSync(path,constants.R_OK);return true;}catch{return false}})()};}catch(e){return {exists:false,error:e.code||String(e.message).slice(0,100)}};};
let sqlite={ok:false};
try{const db=new Database(raw,{readonly:true,create:false});try{db.exec('PRAGMA busy_timeout=3000');sqlite={ok:true,total:db.query("SELECT COUNT(*) AS count FROM internet_source WHERE status='published' AND url<>''").get()?.count??null};}finally{db.close();}}catch(e){sqlite={ok:false,error:String(e.message||e).slice(0,200),code:e.code||null};}
const comparison=config.ProgramArguments?.[0]||null;
const run=args=>{try{const p=Bun.spawnSync(args,{stdout:'pipe',stderr:'pipe',timeout:8000});return {exit:p.exitCode,out:p.stdout.toString().slice(0,300),err:p.stderr.toString().slice(0,160)};}catch(e){return {exit:-1,error:String(e.message||e).slice(0,160)};}};
const header=(()=>{let fd;try{fd=openSync(raw,'r');const b=Buffer.alloc(100);readSync(fd,b,0,100,0);return {sqliteSignature:b.subarray(0,16).toString('latin1')==='SQLite format 3\\0',journalByte:b[18],readJournalByte:b[19]};}catch(e){return {error:e.code||String(e.message).slice(0,120)};}finally{if(fd!==undefined)closeSync(fd);}})();
const access=path=>{const o={};for(const [k,mask] of [['read',constants.R_OK],['write',constants.W_OK],['search',constants.X_OK]])try{accessSync(path,mask);o[k]=true;}catch{o[k]=false;}return o;};
const cli=run(['/usr/bin/sqlite3','-readonly',raw,"PRAGMA journal_mode; SELECT COUNT(*) FROM internet_source WHERE status='published' AND url<>'';"]);
let queryOnly={ok:false};
try{const db=new Database(raw,{readwrite:true,create:false});try{db.exec('PRAGMA query_only=ON');db.exec('PRAGMA busy_timeout=2500');queryOnly={ok:true,total:db.query("SELECT COUNT(*) AS count FROM internet_source WHERE status='published' AND url<>''").get()?.count??null,queryOnly:db.query('PRAGMA query_only').get()?.query_only};}finally{db.close();}}catch(e){queryOnly={ok:false,error:String(e.message||e).slice(0,140),code:e.code||null};}
console.log(JSON.stringify({schema:'bonfyre.cms-release-readonly-diagnostic.v3',uid:process.getuid?.(),config:{label:config.Label,program:comparison,canonicalCms:raw},database:state(raw),parent:state(dirname(raw)),parentAccess:access(dirname(raw)),header,sqlite,cli,queryOnly,shm:state(raw+'-shm'),wal:state(raw+'-wal')}));
