import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { chmodSync, closeSync, constants, existsSync, fstatSync, fsyncSync, lstatSync, mkdirSync, openSync, readFileSync,
  readSync, realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { canonicalJson } from './canonical_json.mjs';
import { projectRun } from './project_runtime.mjs';
import { encode, decode } from '../browser/workspace-msgpack.mjs';
import { createFabricPushStore } from './fabric_push_store.mjs';
import { hookEvidence } from './hook_bridge.mjs';

const MAX_BYTES = 8 * 1024 * 1024, MAX_READ = 64 * 1024 * 1024; // agent streams may exceed the store's write bound
const terminal = new Set(['completed', 'failed', 'cancelled', 'blocked', 'review_required', 'uncertain']);
const taskKinds = new Set(['claude','codex','duel','relay','review','verify','resume-claude','resume-codex','pr','batch','model']);
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const uid = () => randomUUID().replaceAll('-','').slice(0,24);
const json = value => canonicalJson(value);
const activityReceipts = Object.freeze({'model-output':'Model output','model-usage':'Model usage receipt',relay:'Implementation review receipt','relay-patch':'Implementation patch receipt',
  'relay-failed-tests':'Test outcome receipt',duel:'Agent review receipt','cross-review':'Cross-review receipt',
  'dual-verification':'Verification receipt',claude:'Claude work receipt',codex:'Codex work receipt',
  'resume-claude':'Claude continuation receipt','resume-codex':'Codex continuation receipt',pr:'PR review receipt',
  'project-apply-complete':'Application receipt','project-apply-refused':'Application refusal receipt','project-apply-uncertain':'Application reconciliation receipt'});
export class FabricActivityError extends Error { constructor(code,message) { super(message); this.code=code; } }
const activityFail = (condition,code,message) => { if (!condition) throw new FabricActivityError(code,message); };
export function fabricActivityBinding(value) {
  activityFail(value && typeof value==='object' && !Array.isArray(value) && Object.keys(value).length===3 &&
    Object.keys(value).every(key=>['actor','subject','scope'].includes(key)),'invalid','Resolve the current native destination.');
  const {actor,subject,scope}=value;
  activityFail(typeof actor==='string' && /^user:[1-9][0-9]{0,18}$/.test(actor) && typeof subject==='string' &&
    (scope==='self' ? subject===actor : ['institution','household'].includes(scope) && new RegExp(`^${scope}:[a-z0-9-]{1,63}$`).test(subject)),
    'invalid','The actor and destination do not match their native scope.');
  return {actor,subject,scope};
}
const activityRequest = value => { activityFail(typeof value==='string' && /^[A-Za-z0-9_.:-]{8,100}$/.test(value),'invalid','Retain one request identity for this action.'); return value; };
const activityTime = value => { const result=Math.round(value*1000); activityFail(Number.isSafeInteger(result) && result>=0 && result<=8640000000000000,'unavailable','The retained activity time is invalid.'); return result; };
function text(value, field, max = 200000, empty = false) {
  if (typeof value !== 'string' || (!empty && !value) || value.includes('\0') || Buffer.byteLength(value) > max) throw new Error(`invalid ${field}`);
  return value;
}
function identity(value) { text(value,'identity',128); if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value)) throw new Error('invalid identity'); return value; }
function integer(value, min, max) { if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error('integer out of bounds'); return value; }
function physical(path) {
  const rest = []; let parent = resolve(path);
  while (!existsSync(parent)) { rest.unshift(parent.slice(dirname(parent).length + 1)); parent = dirname(parent); }
  if (lstatSync(parent).isSymbolicLink()) throw new Error('symlink root refused');
  return join(realpathSync(parent), ...rest);
}
function confined(root,path) {
  path = resolve(path); const rel = relative(root,path);
  if (rel === '..' || rel.startsWith(`..${sep}`) || isAbsolute(rel)) throw new Error('path escapes owner');
  let current = root;
  if (existsSync(current) && lstatSync(current).isSymbolicLink()) throw new Error('symlink owner refused');
  for (const part of rel.split(sep).filter(Boolean)) {
    current = join(current,part);
    try { if (lstatSync(current).isSymbolicLink()) throw new Error('symlink path refused'); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  return path;
}
function bytes(path, maximum) {
  const fd = openSync(path,constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd,{bigint:true});
    if (!before.isFile() || before.size > BigInt(maximum)) throw new Error('file exceeds regular bounded input contract');
    const buffer = Buffer.alloc(Number(before.size)+1); let used = 0;
    while (used < buffer.length) { const n = readSync(fd,buffer,used,buffer.length-used,null); if (!n) break; used += n; }
    const after = fstatSync(fd,{bigint:true}), named = lstatSync(path,{bigint:true});
    const key = item => [item.dev,item.ino,item.size,item.mtimeNs,item.ctimeNs].join(':');
    if (used !== Number(before.size) || key(before) !== key(after) || key(after) !== key(named)) throw new Error('file changed during read');
    return buffer.subarray(0,used);
  } finally { closeSync(fd); }
}

// Git subprocesses must derive repository identity from the checked cwd, never
// ambient GIT_DIR/worktree/index/config injection inherited from a caller.
export function fabricGitEnvironment(env = process.env) {
  return {...Object.fromEntries(Object.entries(env).filter(([key,value])=>!key.startsWith('GIT_') && typeof value === 'string')),
    GIT_CONFIG_NOSYSTEM:'1',GIT_CONFIG_GLOBAL:'/dev/null',GIT_TERMINAL_PROMPT:'0'};
}

// Explicit file coverage, never an implicit scan or copy of the dirty checkout.
export function validateSourceFiles(files) {
  if (files === undefined) return undefined;
  if (!Array.isArray(files) || !files.length || files.length > 64) throw new Error('source_files requires 1 to 64 files');
  const seen = new Set();
  return files.map(item => {
    if (!item || typeof item !== 'object' || Array.isArray(item) || Object.keys(item).some(key => !['path','sha256'].includes(key)))
      throw new Error('invalid source file');
    const {path,sha256} = item;
    if (typeof path !== 'string' || !path || path.length > 2048 || /[\\\x00-\x1f\x7f]/.test(path) || // oxlint-disable-line no-control-regex -- refuses control characters
        isAbsolute(path) || path.split('/').some(part => !part || part === '.' || part === '..' || part.toLowerCase() === '.git'))
      throw new Error('invalid source path');
    if (sha256 !== null && (typeof sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(sha256))) throw new Error('source file requires a SHA-256 or null for absence');
    const key = path.toLowerCase();
    if (seen.has(key)) throw new Error('duplicate source path');
    seen.add(key); return {path,sha256};
  });
}

export function createFabricStore({ home, projectRoot, worktreeRoot, now = () => Date.now()/1000, run = projectRun, env = process.env } = {}) {
  text(home,'home'); text(projectRoot,'project root');
  if (typeof now !== 'function' || typeof run !== 'function') throw new Error('invalid store dependencies');
  home = physical(home); projectRoot = realpathSync(projectRoot); worktreeRoot = physical(worktreeRoot ?? join(home,'worktrees'));
  const dbPath = join(home,'fabric.sqlite3'), logDir = join(home,'logs'), taskDir = join(home,'tasks'), receiptDir = join(home,'receipts');
  // Scope read/write lanes to the canonical project, using the existing
  // persistent Fabric SQLite lock owner; never infer lock expiry as release.
  const repositoryLock = name => ['worktree-write','duel-read'].includes(name)
    ? `repo:${digest(Buffer.from(projectRoot))}:${name}` : name;
  let connection, closed = false;
  const clock = () => { const value = now(); if (!Number.isFinite(value) || value < 0) throw new Error('invalid clock'); return value; };
  function db() {
    if (closed) throw new Error('fabric store closed');
    if (!connection) {
      for (const path of [home,logDir,taskDir,receiptDir]) { confined(home,path); mkdirSync(path,{recursive:true,mode:0o700}); }
      confined(worktreeRoot,worktreeRoot); mkdirSync(worktreeRoot,{recursive:true,mode:0o700}); confined(home,dbPath);
      const opened = new Database(dbPath,{create:true,strict:true});
      try { opened.exec('PRAGMA busy_timeout=30000'); opened.exec(readFileSync(new URL('./fabric_schema.sql',import.meta.url),'utf8')); connection = opened; }
      catch (error) { opened.close(); throw error; }
    }
    return connection;
  }
  const get = (sql,...args) => db().query(sql).get(...args);
  const all = (sql,...args) => db().query(sql).all(...args);
  const write = (sql,...args) => db().query(sql).run(...args);
  const atomic = fn => db().transaction(fn).immediate();
  function recordEvent(source,kind,payload,session_id = null) {
    text(source,'source',256); text(kind,'kind',256); if (session_id !== null) text(session_id,'session',256);
    return Number(write('INSERT INTO events(ts,source,kind,session_id,payload_json) VALUES(?,?,?,?,?)',clock(),source,kind,session_id,json(payload)).lastInsertRowid);
  }
  function createTask({kind,title,prompt,apply_to_project = false,source_files}) {
    const sourceFiles = validateSourceFiles(source_files);
    if (sourceFiles && kind !== 'relay') throw new Error('source files require an implementation relay');
    if (!taskKinds.has(kind) || typeof apply_to_project !== 'boolean') throw new Error('invalid task kind or authority');
    text(title,'title',16384); text(prompt,'prompt',1000000,true);
    return atomic(() => {
      const id = uid(); write('INSERT INTO tasks(id,kind,title,prompt,state,apply_to_project,created_at) VALUES(?,?,?,?,?,?,?)',id,kind,title,prompt,'queued',Number(apply_to_project),clock());
      if (sourceFiles) saveReceipt(id,'source-request',{files:sourceFiles});
      recordEvent('broker','task.created',{task_id:id,kind,title}); return id;
    });
  }
  // Only a trusted native owner may call this after its own effect admission.
  // Scope is a separate argument: legacy MCP task arguments cannot self-tag work.
  function createScopedTask(input,binding) {
    binding=fabricActivityBinding(binding);
    activityFail(input && typeof input==='object' && !Array.isArray(input) && Object.keys(input).every(key=>['kind','title','prompt','apply_to_project','source_files','requestId'].includes(key)), 'invalid','Use the admitted native task fields.');
    const requestId=activityRequest(input.requestId),sourceFiles=validateSourceFiles(input.source_files);
    text(input.title,'title',500); text(input.prompt,'prompt',1000000,true);
    const hash=digest(encode([1,input.kind,input.title,input.prompt,input.apply_to_project??false,sourceFiles??null],{maxBytes:MAX_BYTES}));
    return atomic(()=>{
      const old=get('SELECT task_id,request_digest FROM task_scopes WHERE actor=? AND subject=? AND scope=? AND request_id=?',binding.actor,binding.subject,binding.scope,requestId);
      if(old){activityFail(old.request_digest===hash,'conflict','This request already names different work.');return {task_id:old.task_id,state:task(old.task_id).state,replayed:true};}
      activityFail(get('SELECT count(*) AS n FROM task_scopes WHERE subject=? AND scope=?',binding.subject,binding.scope).n<1000,'capacity','This destination has reached its retained Fabric task limit.');
      // Resumption needs original session and task authority, not a new disclosure tag.
      activityFail(!input.kind?.startsWith('resume-'),'admission','Scoped session resumption requires its original owner admission.');
      const id=createTask({...input,source_files:sourceFiles});
      write('INSERT INTO task_scopes(task_id,actor,subject,scope,request_id,request_digest,created_at) VALUES(?,?,?,?,?,?,?)',id,binding.actor,binding.subject,binding.scope,requestId,hash,clock());
      return {task_id:id,state:'queued',replayed:false};
    });
  }
  function notifyTask(id,eventKey,kind,title) {
    const admission=get('SELECT actor FROM task_scopes WHERE task_id=?',id);if(!admission)return;
    const at=clock();
    write("INSERT INTO task_notifications(id,task_id,recipient,event_key,kind,title,state,created_at,updated_at) VALUES(?,?,?,?,?,?,'unread',?,?) ON CONFLICT(task_id,recipient,event_key) DO NOTHING",uid(),id,admission.actor,eventKey,kind,title.slice(0,500),at,at);
  }
  function taskScope(id) { identity(id); const row=get('SELECT actor,subject,scope FROM task_scopes WHERE task_id=?',id); return row?fabricActivityBinding(row):null; }
  function findScopedTask(binding,requestId) {
    binding=fabricActivityBinding(binding);requestId=activityRequest(requestId);
    return get('SELECT s.task_id,t.state FROM task_scopes s JOIN tasks t ON t.id=s.task_id WHERE s.actor=? AND s.subject=? AND s.scope=? AND s.request_id=?',binding.actor,binding.subject,binding.scope,requestId)||null;
  }
  function task(id) { identity(id); const row = get('SELECT * FROM tasks WHERE id=?',id); if (!row) throw new Error('task not found'); return row; }
  function claimTask(id) {
    identity(id); return atomic(() => {
      const changed = write("UPDATE tasks SET state='running',started_at=? WHERE id=? AND state='queued'",clock(),id);
      return changed.changes ? task(id) : null;
    });
  }
  function updateTask(id,updates) {
    identity(id);
    if (!updates || typeof updates !== 'object' || Array.isArray(updates)) throw new Error('invalid task updates');
    const allowed = new Set(['title','prompt','state','worktree','branch','started_at','completed_at','result_json','error']);
    const entries = Object.entries(updates).map(([key,value]) => {
      if (!allowed.has(key)) throw new Error('invalid task update field');
      if (key === 'result_json') value = typeof value === 'string' ? json(JSON.parse(value)) : json(value);
      else if (key.endsWith('_at')) { if (value !== null && (!Number.isFinite(value) || value < 0)) throw new Error('invalid timestamp'); }
      else if (value !== null) text(value,key,1000000,true);
      if (key === 'state' && !['queued','running',...terminal].includes(value)) throw new Error('invalid task state');
      if (key === 'worktree' && value !== null) value = confined(worktreeRoot,value);
      return [key,value];
    });
    return atomic(() => {
      const current = task(id);
      if (current.state === 'cancelled') return current;
      if (terminal.has(current.state) && updates.state && updates.state !== current.state) return current;
      if (current.state === 'running' && updates.state === 'queued') throw new Error('cannot requeue running task');
      if (updates.state === 'completed' && get("SELECT id FROM runs WHERE task_id=? AND state='running' LIMIT 1",id)) throw new Error('cannot complete task with active runs');
      if (entries.length) write(`UPDATE tasks SET ${entries.map(([key])=>`${key}=?`).join(',')} WHERE id=?`,...entries.map(([,value])=>value),id);
      if(updates.state && updates.state!==current.state && terminal.has(updates.state))
        notifyTask(id,'state:'+updates.state,'task_'+updates.state,({completed:'Work completed',failed:'Work failed',cancelled:'Work cancelled',blocked:'Work is blocked',review_required:'Work needs review',uncertain:'Work needs reconciliation'})[updates.state]);
      return task(id);
    });
  }
  function cancelTask(id) {
    return atomic(() => {
      const current = task(id); if (terminal.has(current.state)) return {task_id:id,cancelled_runs:[],state:current.state};
      const running = all("SELECT id FROM runs WHERE task_id=? AND state='running' LIMIT 1001",id);
      if (running.length>1000) throw new Error('run budget exceeded');
      write("UPDATE tasks SET state='cancelled',completed_at=? WHERE id=?",clock(),id);
      write("UPDATE runs SET state='cancelled',completed_at=? WHERE task_id=? AND state='running'",clock(),id);
      notifyTask(id,'state:cancelled','task_cancelled','Work cancelled');
      recordEvent('broker','task.cancelled',{task_id:id,run_ids:running.map(row=>row.id)});
      // The process owner releases lane locks only after child termination is joined.
      return {task_id:id,cancelled_runs:running.map(row=>row.id),state:'cancelled'};
    });
  }
  const isTaskCancelled = id => task(id).state === 'cancelled';
  function acquireLock(name,id,ttl = 7200) {
    text(name,'lock',256); integer(ttl,1,86400);
    return atomic(() => {
      const owner = task(id); if (terminal.has(owner.state)) return false;
      const scoped = repositoryLock(name);
      // Let legacy global leases drain before new scoped lanes can proceed.
      const legacy = scoped === name ? null : get('SELECT * FROM locks WHERE name=?',name);
      if (legacy && legacy.task_id !== id) return false;
      name = scoped;
      const at = clock(), existing = get('SELECT * FROM locks WHERE name=?',name);
      if (existing) {
        if (existing.task_id === id) { write('UPDATE locks SET expires_at=? WHERE name=?',at+ttl,name); return true; }
        // Expiry is not proof a foreign executor stopped. Only explicit release transfers a held lane.
        return false;
      }
      write('INSERT INTO locks VALUES(?,?,?,?)',name,id,at,at+ttl); return true;
    });
  }
  function releaseTaskLocks(id) { identity(id); return write('DELETE FROM locks WHERE task_id=?',id).changes; }
  function releaseLock(name,id) { identity(id); return write('DELETE FROM locks WHERE name=? AND task_id=?',repositoryLock(text(name,'lock',256)),id).changes; }
  function writeText(path,value) {
    text(value,'file body',MAX_BYTES,true); return writeBytes(path,Buffer.from(value));
  }
  function writeBytes(path,value) {
    if (!(value instanceof Uint8Array) || value.byteLength>MAX_BYTES) throw new Error('invalid file bytes');
    db(); path = confined(home,path); mkdirSync(dirname(path),{recursive:true,mode:0o700});
    const temp = `${path}.${uid()}.tmp`; let fd;
    try { fd = openSync(temp,'wx',0o600); writeFileSync(fd,value); fsyncSync(fd); closeSync(fd); fd = undefined; renameSync(temp,path); }
    finally { if (fd !== undefined) closeSync(fd); if (existsSync(temp)) unlinkSync(temp); }
    return path;
  }
  function readText(path,{maxBytes = MAX_BYTES,missing = false} = {}) {
    integer(maxBytes,1,MAX_READ); path = confined(home,path);
    if (missing && !existsSync(path)) return '';
    return new TextDecoder('utf-8',{fatal:true}).decode(bytes(path,maxBytes));
  }
  function parseJsonLines(path) {
    let session_id = null; const parts = []; let length = 0;
    for (const line of readText(path,{missing:true}).split('\n')) {
      let item; try { item = JSON.parse(line); } catch { continue; }
      if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
      for (const key of ['session_id','sessionId','thread_id','threadId']) if (typeof item[key] === 'string' && item[key] && item[key].length <= 256) session_id = item[key];
      const add = value => { if (typeof value === 'string') { parts.push(value); length += value.length; while (parts.length>1 && length-parts[0].length >= 200000) length-=parts.shift().length; } };
      if (['result','assistant','message.completed','item.completed'].includes(item.type)) for (const key of ['result','text','message','output_text']) add(item[key]);
      if (item.type === 'item.completed' && item.item && typeof item.item === 'object') for (const key of ['text','content','message']) add(item.item[key]);
    }
    return {session_id,final:parts.join('\n').slice(-200000)};
  }
  function createRun({run_id = uid(),task_id,agent,role,command,cwd}) {
    if (typeof run_id !== 'string' || !/^[a-f0-9]{24}$/.test(run_id)) throw new Error('invalid run id');
    text(agent,'agent',256); text(role,'role',256); if (!Array.isArray(command) || !command.length || command.length>1000) throw new Error('invalid command');
    for (const arg of command) text(arg,'command argument',1000000,true);
    cwd = checkedCwd(cwd);
    return atomic(() => {
      if (task(task_id).state !== 'running') throw new Error('task is not running');
      const id = run_id, directory = confined(taskDir,join(taskDir,task_id,id));
      if (get('SELECT id FROM runs WHERE id=?',id) || existsSync(directory)) throw new Error('run identity already exists');
      mkdirSync(directory,{recursive:true,mode:0o700});
      const paths = {stdout_path:join(directory,'stream.jsonl'),stderr_path:join(directory,'stderr.log'),final_path:join(directory,'final.txt')}, at = clock();
      write('INSERT INTO runs(id,task_id,agent,role,state,command_json,cwd,stdout_path,stderr_path,final_path,started_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)',id,task_id,agent,role,'running',json(command),cwd,paths.stdout_path,paths.stderr_path,paths.final_path,at);
      recordEvent(agent,'run.started',{task_id,run_id:id,role}); return {run_id:id,...paths,started_at:at};
    });
  }
  function finishRun(id,result) {
    identity(id); result = JSON.parse(json(result));
    return atomic(() => {
      const row = get('SELECT * FROM runs WHERE id=?',id); if (!row) throw new Error('run not found');
      if (!['running','cancelled'].includes(row.state) || (row.state === 'cancelled' && row.result_json !== '{}')) return JSON.parse(row.result_json);
      const state = row.state === 'cancelled' || isTaskCancelled(row.task_id) ? 'cancelled' : result.state;
      if (!['passed','failed','timeout','cancelled','blocked','overflow'].includes(state)) throw new Error('invalid run outcome');
      const at = clock(), duration = Math.max(0,Math.floor((at-row.started_at)*1000));
      const session_id = result.session_id ?? null; if (session_id !== null) text(session_id,'session',256);
      const exit_code = result.exit_code ?? null; if (exit_code !== null) integer(exit_code,-255,65535);
      const saved = {...result,run_id:id,task_id:row.task_id,agent:row.agent,role:row.role,state,session_id,exit_code,duration_ms:duration,
        stdout_path:row.stdout_path,stderr_path:row.stderr_path,final_path:row.final_path};
      write('UPDATE runs SET state=?,session_id=?,exit_code=?,completed_at=?,duration_ms=?,result_json=? WHERE id=?',state,session_id,exit_code,at,duration,json(saved),id);
      if (session_id) {
        const prior = get('SELECT agent FROM sessions WHERE id=?',session_id);
        if (prior && prior.agent !== row.agent) throw new Error('session identity belongs to another agent');
        write(`INSERT INTO sessions VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET task_id=excluded.task_id,cwd=excluded.cwd,state=excluded.state,updated_at=excluded.updated_at,metadata_json=excluded.metadata_json`,session_id,row.agent,row.task_id,row.cwd,state,row.started_at,at,json({run_id:id,role:row.role}));
      }
      recordEvent(row.agent,'run.completed',saved,session_id); return saved;
    });
  }
  function session(id,agent) { text(id,'session',256); text(agent,'agent',256); return get('SELECT * FROM sessions WHERE id=? AND agent=?',id,agent); }
  function saveReceipt(task_id,kind,payload,suffix = 'json') {
    task(task_id); text(kind,'receipt kind',256); if (!['json','patch','txt','msgpack'].includes(suffix)) throw new Error('invalid receipt suffix');
    const body = suffix === 'msgpack' ? encode(payload,{maxBytes:MAX_BYTES}) : suffix === 'json' ? json(payload) : text(payload,'receipt body',MAX_BYTES,true), payloadJson = json(payload);
    return atomic(() => {
      const id = uid(), at = clock(), path = join(receiptDir,`${Math.floor(at)}-${id}.${suffix}`); writeBytes(path,typeof body==='string'?Buffer.from(body):body); const sha256 = digest(Buffer.from(body));
      write('INSERT INTO receipts VALUES(?,?,?,?,?,?,?)',id,task_id,kind,path,sha256,payloadJson,at);
      if(Object.hasOwn(activityReceipts,kind))notifyTask(task_id,'output-ready','output_ready','A work receipt is available');
      return {receipt_id:id,path,sha256,kind};
    });
  }
  function boundedRows(sql,args = []) {
    const rows = [], statement = db().prepare(sql); let size = 0;
    try { for (const row of statement.iterate(...args)) {
        size += Object.values(row).reduce((total,value)=>total+(typeof value === 'string' ? Buffer.byteLength(value) : 16),0);
        if (rows.length >= 1000 || size > MAX_BYTES) throw new Error('response row or byte budget exceeded'); rows.push(row);
      }
    } finally { statement.finalize(); }
    return rows;
  }
  function taskStatus(id) {
    const row = task(id); return {...row,result:JSON.parse(row.result_json),
      runs:boundedRows('SELECT * FROM runs WHERE task_id=? ORDER BY started_at LIMIT 1001',[id]),
      receipts:boundedRows('SELECT * FROM receipts WHERE task_id=? ORDER BY created_at LIMIT 1001',[id])};
  }
  function taskSummary(id) {
    identity(id);
    const row = get('SELECT id,kind,substr(title,1,500) AS title,state,worktree,branch,apply_to_project,created_at,started_at,completed_at,substr(error,1,2000) AS error,(length(title)>500 OR length(error)>2000) AS text_truncated FROM tasks WHERE id=?',id);
    if (!row) throw new Error('task not found');
    return {...row,detail:'summary',
      run_states:Object.fromEntries(all('SELECT state,COUNT(*) AS count FROM runs WHERE task_id=? GROUP BY state',id).map(item=>[item.state,item.count])),
      recent_runs:all('SELECT id,agent,role,state,exit_code,started_at,completed_at,duration_ms FROM runs WHERE task_id=? ORDER BY started_at DESC,rowid DESC LIMIT 8',id),
      receipt_count:get('SELECT COUNT(*) AS count FROM receipts WHERE task_id=?',id).count,
      recent_receipts:all('SELECT id,kind,path,digest,created_at FROM receipts WHERE task_id=? ORDER BY created_at DESC,rowid DESC LIMIT 8',id),
      next_read:'Use task_status with detail=full for review findings, patch and apply results.'};
  }
  function recentReceipts(limit = 50) { integer(limit,1,500); return {receipts:boundedRows('SELECT * FROM receipts ORDER BY created_at DESC LIMIT ?',[limit])}; }
  function events(limit = 500) { integer(limit,1,500); return {events:boundedRows('SELECT * FROM events ORDER BY seq DESC LIMIT ?',[limit]).map(({payload_json,...row})=>({...row,payload:JSON.parse(payload_json)}))}; }
  function safeEvents(limit = 500) {
    integer(limit,1,500);
    // SQL-level projection: large legacy hook bodies never cross into Bun.
    const ids=['session_id','thread_id','turn_id','run_id','request_id','message_id',
      'parent_session_id','agent_id','tool_use_id','tool_name','model'];
    const counts=['input_tokens','cached_input_tokens','cache_read_input_tokens',
      'cache_creation_input_tokens','output_tokens','reasoning_output_tokens'];
    const fields=[...ids.flatMap(key=>["'"+key+"'", "json_extract(payload_json,'$."+key+"')"]),
      "'usage'",'json_object('+counts.flatMap(key=>["'"+key+"'", "json_extract(payload_json,'$.usage."+key+"')"]).join(',')+')'].join(',');
    const sql="SELECT seq,ts,source,kind,session_id,CASE WHEN source IN ('claude','codex') "+
      "THEN CASE WHEN length(payload_json)<=32768 AND json_valid(payload_json) "+
      "THEN json_object("+fields+") ELSE '{}' END ELSE payload_json END AS payload_json "+
      "FROM events ORDER BY seq DESC LIMIT ?";
    return {events:boundedRows(sql,[limit]).map(({payload_json,...row})=>{
      if (!['claude','codex'].includes(row.source)) return {...row,payload:JSON.parse(payload_json)};
      const projected=JSON.parse(payload_json);
      const known=hookEvidence({source:row.source,kind:row.kind});
      const kind=known?.kind||'Unknown';
      const evidence=Object.keys(projected).length?hookEvidence({...projected,source:row.source,kind}):null;
      const id=typeof row.session_id==='string'&&/^[a-zA-Z0-9_.:-]{1,160}$/.test(row.session_id)
        ? row.session_id:null;
      return {...row,kind,session_id:id,payload:evidence||
        {source:row.source,kind,legacy_redacted:true}};
    })};
  }
  function summary() { return {tasks:Object.fromEntries(all('SELECT state,COUNT(*) AS count FROM tasks GROUP BY state').map(row=>[row.state,row.count])),
    runs:Object.fromEntries(all('SELECT state,COUNT(*) AS count FROM runs GROUP BY state').map(row=>[row.state,row.count])),locks:boundedRows('SELECT * FROM locks ORDER BY acquired_at LIMIT 1001'),
    historical_hook_audit:get("SELECT COUNT(*) AS observed_events, " +
      "COALESCE(SUM(CASE WHEN instr(payload_json,char(34)||'prompt'||char(34))>0 OR instr(payload_json,char(34)||'tool_input'||char(34))>0 " +
      "OR instr(payload_json,char(34)||'tool_response'||char(34))>0 OR instr(payload_json,char(34)||'cwd'||char(34))>0 " +
      "THEN 1 ELSE 0 END),0) AS potential_legacy_bodies FROM events WHERE source IN ('codex','claude')"),
    recent_tasks:all('SELECT id,kind,title,state,worktree,created_at,completed_at,error FROM tasks ORDER BY created_at DESC LIMIT 20')}; }
  function activityRead(authority,{taskId='',execution}={}) {
    const binding=fabricActivityBinding(authority),{actor,subject,scope}=binding;
    activityFail(typeof execution==='function','unavailable','The current Fabric process must witness execution.');
    if(taskId)identity(taskId);
    return atomic(()=>{
      const rows=all(`SELECT t.id,t.kind,substr(t.title,1,500) AS title,t.state,t.created_at,t.started_at,t.completed_at,s.revision
        FROM task_scopes s JOIN tasks t ON t.id=s.task_id WHERE s.subject=? AND s.scope=? ${taskId?'AND t.id=?':''} ORDER BY s.seq DESC LIMIT 21`,subject,scope,...(taskId?[taskId]:[]));
      activityFail(!taskId || rows.length===1,'not_found','This work is not available in the current destination.');
      let more=rows.length>20;const selected=rows.slice(0,20),ids=selected.map(row=>row.id),context=scope==='self'?'self':subject;
      const href=id=>'/apps?'+new URLSearchParams({context})+'#fabric-task-'+id;
      const phases={queued:'pending',running:'active',completed:'completed',passed:'completed',failed:'failed',timeout:'failed',overflow:'failed',cancelled:'cancelled',blocked:'blocked',review_required:'review',uncertain:'uncertain'};
      const status=row=>{const live=execution(row.id);activityFail(live && typeof live.owned==='boolean' && typeof live.queued==='boolean','unavailable','The live execution witness is incomplete.');return ['queued','running'].includes(row.state)&&!live.owned&&!live.queued;};
      const tasks=selected.map(row=>{const reconciliationRequired=status(row);return {id:'task:'+row.id,taskId:row.id,parentTaskId:null,kind:'task',workKind:row.kind==='model'?'model':'operator',title:row.title,state:row.state,
        phase:reconciliationRequired?'uncertain':phases[row.state],revision:row.revision,created:activityTime(row.created_at),updated:activityTime(Math.max(row.created_at,row.started_at||0,row.completed_at||0)),reconciliationRequired,href:href(row.id)};});
      const roots=new Map(tasks.map(row=>[row.taskId,row])),outputs=[];
      if(ids.length){
        const marks=ids.map(()=>'?').join(','),limit=100-tasks.length;
        const runs=all(`SELECT id,task_id,agent,role,state,started_at,completed_at FROM runs WHERE task_id IN (${marks}) ORDER BY started_at DESC,rowid DESC LIMIT ?`,...ids,limit+1);
        more ||= runs.length>limit;
        for(const run of runs.slice(0,limit)){const parent=roots.get(run.task_id),reconciliationRequired=run.state==='running'&&parent.reconciliationRequired;
          tasks.push({id:'run:'+run.id,taskId:run.task_id,parentTaskId:parent.id,kind:'subagent',title:(run.agent+' · '+run.role).slice(0,500),state:run.state,phase:reconciliationRequired?'uncertain':phases[run.state],revision:parent.revision,
            created:activityTime(run.started_at),updated:activityTime(run.completed_at||run.started_at),reconciliationRequired,href:href(run.task_id)});}
        const kinds=Object.keys(activityReceipts),receipts=all(`SELECT id,task_id,kind,digest,created_at FROM receipts WHERE task_id IN (${marks}) AND kind IN (${kinds.map(()=>'?').join(',')}) ORDER BY created_at DESC,rowid DESC LIMIT 41`,...ids,...kinds);
        more ||= receipts.length>40;
        for(const receipt of receipts.slice(0,40)){activityFail(/^[a-f0-9]{64}$/.test(receipt.digest),'unavailable','A retained receipt identity is incomplete.');outputs.push({id:receipt.id,taskId:receipt.task_id,title:activityReceipts[receipt.kind],kind:receipt.kind,digest:receipt.digest,created:activityTime(receipt.created_at),href:href(receipt.task_id)});}
      }
      const notices=all(`SELECT n.id,n.task_id,n.kind,n.title,n.state,n.revision,n.created_at,n.updated_at FROM task_notifications n JOIN task_scopes s ON s.task_id=n.task_id WHERE s.subject=? AND s.scope=? AND n.recipient=? AND n.state!='dismissed' ORDER BY n.seq DESC LIMIT 41`,subject,scope,actor);
      more ||= notices.length>40;
      const notifications=notices.slice(0,40).map(row=>({id:row.id,taskId:row.task_id,kind:row.kind,title:row.title,state:row.state,revision:row.revision,created:activityTime(row.created_at),updated:activityTime(row.updated_at),allowedActions:row.state==='unread'?['read','dismiss']:['dismiss'],href:href(row.task_id)}));
      const unread=get("SELECT count(*) AS n FROM task_notifications n JOIN task_scopes s ON s.task_id=n.task_id WHERE s.subject=? AND s.scope=? AND n.recipient=? AND n.state='unread'",subject,scope,actor).n;
      const value={source:'fabric',context,binding,tasks,outputs,notifications,unread,more};
      return {...value,revision:digest(encode(value))};
    });
  }
  function notificationAction(authority,input) {
    const {actor,subject,scope}=fabricActivityBinding(authority);
    activityFail(input && typeof input==='object' && !Array.isArray(input) && Object.keys(input).length===4 && Object.keys(input).every(key=>['id','revision','action','requestId'].includes(key)), 'invalid','Use the exact notification review coordinates.');
    activityFail(typeof input.id==='string'&&/^[a-f0-9]{24}$/.test(input.id)&&Number.isSafeInteger(input.revision)&&input.revision>0,'invalid','Use the retained notification identity and revision.');activityRequest(input.requestId);
    activityFail(['read','dismiss'].includes(input.action),'invalid','Choose read or dismiss for this notification.');
    const hash=digest(encode([1,input.id,input.revision,input.action]));
    return atomic(()=>{
      const row=get('SELECT n.* FROM task_notifications n JOIN task_scopes s ON s.task_id=n.task_id WHERE n.id=? AND n.recipient=? AND s.subject=? AND s.scope=?',input.id,actor,subject,scope);
      activityFail(row,'not_found','This notification is not available to the current recipient.');
      const old=get('SELECT request_digest,receipt FROM notification_actions WHERE recipient=? AND subject=? AND scope=? AND request_id=?',actor,subject,scope,input.requestId);
      if(old){activityFail(old.request_digest===hash,'conflict','This review identity already names a different action.');return {...decode(new Uint8Array(old.receipt)),replayed:true};}
      activityFail(row.revision===input.revision,'conflict','This notification changed. Read it again before acting.');
      activityFail(row.state!=='dismissed','conflict','This notification has already been dismissed.');
      activityFail(get('SELECT count(*) AS n FROM notification_actions WHERE recipient=? AND subject=? AND scope=?',actor,subject,scope).n<10000,'capacity','This recipient has reached its retained notification review limit.');
      const state=input.action==='dismiss'?'dismissed':'read',changed=state!==row.state,revision=row.revision+Number(changed),at=clock();
      if(changed)write('UPDATE task_notifications SET state=?,revision=?,updated_at=? WHERE id=? AND revision=?',state,revision,at,row.id,row.revision);
      const receipt={requestId:input.requestId,id:row.id,action:input.action,revision,state,replayed:false};
      write('INSERT INTO notification_actions VALUES(?,?,?,?,?,?,?,?)',actor,subject,scope,input.requestId,hash,row.id,encode(receipt),at);
      return receipt;
    });
  }
  function checkedCwd(path) {
    text(path,'cwd'); const actual = realpathSync(path); if (actual === projectRoot) return actual;
    confined(worktreeRoot,path); if (actual !== resolve(path)) throw new Error('worktree alias refused'); return actual;
  }
  function checkSignal(signal) {
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw new Error('invalid abort signal');
    if (signal?.aborted) throw new Error('git operation cancelled');
  }
  async function git(args,cwd = projectRoot,timeoutMs = 60000,signal,checkTask,input) {
    checkSignal(signal); checkTask?.();
    const receipt = await run(['git','-c','core.hooksPath=/dev/null','-c','core.fsmonitor=false',...args],
      {cwd,env:fabricGitEnvironment(env),timeoutMs,maxOutputBytes:MAX_BYTES,signal,input});
    checkSignal(signal); checkTask?.();
    if (!receipt || receipt.exit_code !== 0 || receipt.timeout || receipt.overflow || receipt.cancelled) {
      // Preserve the failure class and bounded Git diagnostic; without these,
      // checkout exhaustion, ref conflicts and output limits look identical.
      const why = !receipt ? 'no receipt' : [
        `exit=${receipt.exit_code}`, receipt.timeout ? 'timeout' : '',
        receipt.overflow ? 'output limit' : '', receipt.cancelled ? 'cancelled' : '',
        String(receipt.stderr || '').slice(-400).replace(/[^\\x20-\\x7e]/g, ' '),
      ].filter(Boolean).join('; ');
      throw new Error(`git operation failed or output exceeded bounds (${why})`);
    }
    return receipt.stdout;
  }
  async function registeredWorktree(path,signal,checkTask) {
    const listing = await git(['worktree','list','--porcelain','-z'],projectRoot,60000,signal,checkTask);
    return listing.split('\0').includes(`worktree ${path}`);
  }
  function readReceipt(id,kind) {
    identity(id); text(kind,'receipt kind',256);
    const row = get('SELECT id,path,digest FROM receipts WHERE task_id=? AND kind=? ORDER BY rowid DESC LIMIT 1',id,kind);
    if (!row) return null;
    const body = bytes(confined(home,row.path),MAX_BYTES);
    if (digest(body) !== row.digest) throw new Error('source receipt integrity failed');
    let value;
    if(row.path.endsWith('.msgpack'))value=decode(body,{maxBytes:MAX_BYTES});
    else{
      const decoded=new TextDecoder('utf-8',{fatal:true}).decode(body);
      if(row.path.endsWith('.json'))value=JSON.parse(decoded);
      else if(row.path.endsWith('.txt')||row.path.endsWith('.patch'))value=decoded;
      else throw new Error('source receipt encoding is not admitted');
    }
    return {value,receipt:{receipt_id:row.id,path:row.path,sha256:row.digest,kind}};
  }
  // A retained successful model output may outlive a lost Meter acknowledgement.
  // Only its exact physical attempt and native usage receipt can close that
  // uncertainty; this never reopens an execution lane or retries inference.
  function completeModelUsage(id,attemptRef) {
    identity(id);text(attemptRef,'model attempt',512);
    return atomic(()=>{
      const current=task(id),scope=taskScope(id),request=readReceipt(id,'model-request')?.value,
        attempt=readReceipt(id,'model-attempt')?.value,result=readReceipt(id,'model-result')?.value,
        usage=readReceipt(id,'model-usage')?.value,output=readReceipt(id,'model-output');
      activityFail(current.kind==='model'&&['uncertain','completed'].includes(current.state)&&scope&&
        request?.schema===1&&request.taskId===id&&request.calls===1&&request.model?.provider==='bonfyre-moe'&&
        attempt?.schema===1&&attempt.taskId===id&&typeof attempt.realizationRef==='string'&&
        /^bonfyre-moe:[a-f0-9]{64}$/.test(attempt.realizationRef)&&typeof attempt.at==='string'&&
        /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/.test(attempt.at)&&Number.isFinite(Date.parse(attempt.at))&&
        new Date(attempt.at).toISOString().replace('.000Z','Z')===attempt.at&&
        request?.binding?.actor===scope.actor&&request.binding.subject===scope.subject&&request.binding.scope===scope.scope&&
        attempt?.attemptRef===attemptRef&&result?.schema===1&&result.state==='completed'&&usage?.state==='recorded'&&
        Number.isSafeInteger(usage.eventId)&&usage.eventId>0&&
        usage.attemptRef===attemptRef&&usage.realizationRef===attempt.realizationRef&&usage.at===attempt.at&&
        usage.key===scope.subject&&usage.operation==='model-generation'&&usage.provider===request.model?.provider&&
        Number.isSafeInteger(result.bytes)&&result.bytes>=0&&result.bytes<=65536&&
        Number.isSafeInteger(result.durationMs)&&result.durationMs>=0&&result.durationMs<=125000&&
        usage.outputBytes===result.bytes&&usage.durationMs===result.durationMs&&output&&typeof output.value==='string'&&
        Buffer.byteLength(output.value)===result.bytes&&output.receipt.sha256===result.digest&&
        usage.receiptRef==='sha256:'+output.receipt.sha256&&!get("SELECT id FROM runs WHERE task_id=? AND state='running' LIMIT 1",id),
        'conflict','This model usage receipt does not resolve the retained physical attempt.');
      if(current.state==='uncertain'){
        write("UPDATE tasks SET state='completed',completed_at=?,error='' WHERE id=? AND state='uncertain'",clock(),id);
        notifyTask(id,'state:completed','task_completed','Work completed');
      }
      return task(id);
    });
  }
  const sourceReceipt=readReceipt;
  function captureSources(files,root = projectRoot) {
    let total = 0;
    return validateSourceFiles(files).map(item => {
      const path = confined(root,join(root,item.path));
      let mode;
      try { mode = lstatSync(path).mode; }
      catch (error) {
        if (error.code !== 'ENOENT' || item.sha256 !== null) throw error;
        return {...item,bytes:0,mode:null,body:null};
      }
      if (item.sha256 === null) throw new Error('source expected absent: '+item.path);
      const body = bytes(path,MAX_BYTES-total); total += body.length;
      if (digest(body) !== item.sha256) throw new Error('source hash changed: '+item.path);
      return {...item,bytes:body.length,mode:mode & 0o777,body};
    });
  }
  function verifySourceSnapshot(snapshot) {
    if (!snapshot) return {matches:true,scope:'Git base only; no current-source files selected'};
    const current = captureSources(snapshot.files.map(({path,sha256})=>({path,sha256})));
    if (current.some((item,i)=>item.mode !== snapshot.files[i].mode)) throw new Error('source mode changed since capture');
    return {matches:true,files:current.length,scope:'Selected files only; not an atomic repository snapshot'};
  }
  async function seedSources(id,path,commit,captured,signal,checkTask) {
    // Only caller-selected non-ignored regular files enter the baseline.
    const ignored = await git(['--literal-pathspecs','ls-files','--others','--ignored','--exclude-standard','-z','--',...captured.map(item=>item.path)],projectRoot,60000,signal,checkTask);
    if (ignored) throw new Error('ignored source files are not admitted');
    for (const item of captured) {
      checkSignal(signal); checkTask();
      const target = confined(path,join(path,item.path));
      if (existsSync(target) && !lstatSync(target).isFile()) throw new Error('source destination must be a regular file');
      if (item.body === null) {
        if (existsSync(target)) unlinkSync(target);
      } else {
        mkdirSync(dirname(target),{recursive:true});
        const temporary = `${target}.${uid()}.source`;
        try {
          writeFileSync(temporary,item.body,{flag:'wx',mode:0o600}); chmodSync(temporary,item.mode);
          confined(path,target); renameSync(temporary,target);
        } finally { if (existsSync(temporary)) unlinkSync(temporary); }
      }
    }
    // One object write and one index update for the selected set, not two
    // subprocesses per file. No Git clean filter rewrites the admitted bytes.
    const present = captured.filter(item=>item.body !== null);
    const blobs = present.length ? (await git(['hash-object','-w','--no-filters','--',...present.map(item=>item.path)],path,60000,signal,checkTask)).trim().split('\n') : [];
    if (blobs.length !== present.length) throw new Error('incomplete source blob identities');
    const identities = new Map(present.map((item,i)=>{
      const blob = blobs[i];
      if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(blob)) throw new Error('invalid source blob identity');
      const expectedBlob = createHash(blob.length === 40 ? 'sha1' : 'sha256').update(`blob ${item.body.length}\0`).update(item.body).digest('hex');
      if (blob !== expectedBlob) throw new Error('isolated source changed during blob creation');
      return [item.path,blob];
    }));
    const entries = captured.map(item=>`${item.body === null ? '0' : item.mode & 0o111 ? '100755' : '100644'} ${identities.get(item.path) ?? '0'.repeat(commit.length)}\t${item.path}\0`).join('');
    await git(['update-index','-z','--index-info'],path,60000,signal,checkTask,entries);
    const tree = (await git(['write-tree'],path,60000,signal,checkTask)).trim();
    if (!/^[a-f0-9]{40,64}$/.test(tree)) throw new Error('invalid source tree identity');
    const snapshotCommit = (await git(['-c','user.name=Bonfyre source snapshot','-c','user.email=source-snapshot@localhost','-c','commit.gpgsign=false',
      'commit-tree',tree,'-p',commit,'-m','Isolated relay source baseline'],path,60000,signal,checkTask)).trim();
    if (!/^[a-f0-9]{40,64}$/.test(snapshotCommit)) throw new Error('invalid source baseline identity');
    const value = {schema:'bonfyre.relay-source-snapshot.v1',base_commit:commit,commit:snapshotCommit,
      files:captured.map(({body:_body,...item})=>item),scope:'Explicit current-source files over Git base; unselected paths remain at that base'};
    verifySourceSnapshot(value);
    const seeded = captureSources(value.files.map(({path,sha256})=>({path,sha256})),path);
    if (seeded.some((item,i)=>item.mode !== value.files[i].mode)) throw new Error('isolated source mode changed');
    await git(['diff','--quiet','--no-ext-diff','--no-textconv',snapshotCommit,'--'],path,60000,signal,checkTask);
    await git(['update-ref','HEAD',snapshotCommit,commit],path,60000,signal,checkTask);
    return {...value,receipt:saveReceipt(id,'source-snapshot',value)};
  }
  async function createWorktree(id,{base = 'HEAD',signal} = {}) {
    checkSignal(signal); identity(id); text(base,'base',1024);
    const checkTask = () => { if (task(id).state !== 'running') throw new Error('task is not running'); };
    checkTask();
    const request = sourceReceipt(id,'source-request');
    const captured = request ? captureSources(request.value.files) : null;
    const path = confined(worktreeRoot,join(worktreeRoot,id)), branch = `bonfyre/fabric-${id.slice(0,12)}`;
    if (existsSync(path)) {
      if (!await registeredWorktree(path,signal,checkTask)) throw new Error('existing path is not a registered project worktree');
      if (isTaskCancelled(id)) throw new Error('task cancelled');
      const currentBranch = (await git(['symbolic-ref','--short','-q','HEAD'],path,60000,signal,checkTask).catch(()=>{
        checkSignal(signal);checkTask();return '(detached)';
      })).trim();
      const snapshot = sourceReceipt(id,'source-snapshot');
      if (request && !snapshot) throw new Error('incomplete source baseline requires reconciliation');
      const source_snapshot = snapshot ? {...snapshot.value,receipt:snapshot.receipt} : null;
      verifySourceSnapshot(source_snapshot);
      const base_commit = source_snapshot?.commit ?? (await git(['rev-parse','HEAD'],path,60000,signal,checkTask)).trim();
      updateTask(id,{worktree:path,branch:currentBranch}); return {worktree:path,branch:currentBranch,base_commit,source_snapshot};
    }
    const commit = (await git(['rev-parse','--verify','--end-of-options',`${base}^{commit}`],projectRoot,60000,signal,checkTask)).trim();
    if (!/^[0-9a-f]{40,64}$/.test(commit)) throw new Error('invalid base commit');
    if (isTaskCancelled(id)) throw new Error('task cancelled');
    let selected = branch;
    try { await git(['worktree','add','--quiet','-b',branch,'--',path,commit],projectRoot,180000,signal,checkTask); }
    catch (error) {
      if (existsSync(path) || isTaskCancelled(id)) throw error;
      await git(['worktree','add','--quiet','--detach','--',path,commit],projectRoot,180000,signal,checkTask); selected = '(detached)';
    }
    if (!await registeredWorktree(path,signal,checkTask)) throw new Error('new worktree registration missing');
    if (isTaskCancelled(id)) throw new Error('task cancelled after worktree creation; worktree retained');
    const source_snapshot = captured ? await seedSources(id,path,commit,captured,signal,checkTask) : null;
    updateTask(id,{worktree:path,branch:selected}); return {worktree:path,branch:selected,base_commit:source_snapshot?.commit ?? commit,source_snapshot};
  }
  async function diffPayload(path,{signal,base = 'HEAD'} = {}) {
    if (base !== 'HEAD' && !/^[a-f0-9]{40,64}$/.test(base)) throw new Error('invalid diff baseline');
    checkSignal(signal); path = checkedCwd(path); if (path !== projectRoot && !await registeredWorktree(path,signal)) throw new Error('unregistered worktree');
    const args = ['diff','--no-ext-diff','--no-textconv'];
    const stat = await git([...args,'--stat',base,'--'],path,60000,signal), names = await git([...args,'--name-status',base,'--'],path,60000,signal);
    const patch = await git([...args,'--binary',base,'--'],path,120000,signal);
    // Existing protocol is a tracked-file patch, not an assertion about untracked files.
    const untracked = await git(['ls-files','--others','--exclude-standard','-z'],path,60000,signal);
    return {stat,names,patch,patch_sha256:digest(Buffer.from(patch)),untracked:untracked.split('\0').filter(Boolean)};
  }
  const push=createFabricPushStore({get,all,write,atomic,clock,binding:fabricActivityBinding,fail:activityFail});
  function close() { if (connection) connection.close(); connection = null; closed = true; }
  return Object.freeze({home,projectRoot,worktreeRoot,dbPath,logDir,taskDir,receiptDir,createTask,task,claimTask,updateTask,cancelTask,isTaskCancelled,
    recordEvent,events,safeEvents,acquireLock,releaseLock,releaseTaskLocks,writeText,readText,parseJsonLines,createRun,finishRun,session,saveReceipt,taskStatus,taskSummary,recentReceipts,summary,createWorktree,verifySourceSnapshot,diffPayload,close,
    createScopedTask,taskScope,findScopedTask,readReceipt,completeModelUsage,activityRead,notificationAction,push});
}
