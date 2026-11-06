import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createFabricStore, fabricGitEnvironment, validateSourceFiles } from '../fabric_store.mjs';
import { projectRun } from '../project_runtime.mjs';
const roots = [], stores = [];
function fixture(options = {}) {
  const root = realpathSync(mkdtempSync(join(tmpdir(),'fabric-store-test-'))); roots.push(root);
  const projectRoot = join(root,'project'); mkdirSync(projectRoot);
  const store = createFabricStore({home:join(root,'owner'),projectRoot,now:()=>100,...options}); stores.push(store);
  return {root,projectRoot,store};
}
afterEach(()=>{for(const store of stores.splice(0)) store.close(); for(const root of roots.splice(0)) rmSync(root,{recursive:true,force:true});});
const create = store => store.createTask({kind:'claude',title:'fixture',prompt:'inspect'});

test('lazy store preserves six legacy tables and adds scoped recipient tables without seeding work',()=>{
  const {store} = fixture(); expect(existsSync(store.home)).toBe(false);
  expect(store.summary().tasks).toEqual({});
  const db = new Database(store.dbPath,{readonly:true});
  expect(db.query("SELECT name FROM sqlite_master WHERE type='table' AND name!='sqlite_sequence' ORDER BY name").all().map(x=>x.name)).toEqual(['events','locks','notification_actions','push_deliveries','push_subscriptions','receipts','runs','sessions','task_notifications','task_scopes','tasks']);
  for(const table of ['task_scopes','task_notifications','notification_actions','push_subscriptions','push_deliveries'])expect(db.query(`SELECT count(*) AS n FROM ${table}`).get().n).toBe(0);db.close();
});

test('retained request recovery finds only the exact actor and destination after restart',()=>{
  const {store,projectRoot}=fixture(),binding={actor:'user:1',subject:'institution:studio',scope:'institution'},requestId='review-request-001';
  const task=store.createScopedTask({kind:'model',title:'Reviewed model request',prompt:'',requestId},binding);
  store.updateTask(task.task_id,{state:'completed'});
  const reopened=createFabricStore({home:store.home,projectRoot});stores.push(reopened);
  expect(reopened.findScopedTask(binding,requestId)).toEqual({task_id:task.task_id,state:'completed'});
  for(const other of [{...binding,actor:'user:2'},{...binding,subject:'institution:other'},{actor:'user:1',subject:'user:1',scope:'self'}])
    expect(reopened.findScopedTask(other,requestId)).toBeNull();
  expect(reopened.findScopedTask(binding,'unknown-request')).toBeNull();
  expect(()=>reopened.findScopedTask(binding,'' )).toThrow();
  expect(()=>reopened.findScopedTask({...binding,subject:'user:1'},requestId)).toThrow();
});

test('compact task polling omits large prompts, agent text and receipt payloads while retaining evidence coordinates',()=>{
  const {store,projectRoot}=fixture();
  const id=store.createTask({kind:'relay',title:'compact fixture',prompt:'private prompt '.repeat(10000)});store.claimTask(id);
  for(let i=0;i<10;i++){
    const run=store.createRun({task_id:id,agent:'codex',role:`stage-${i}`,command:['codex','large prompt'],cwd:projectRoot});
    store.finishRun(run.run_id,{state:'passed',exit_code:0,final:'large result '.repeat(10000)});
    store.saveReceipt(id,'test',{body:'large receipt '.repeat(10000)});
  }
  const summary=store.taskSummary(id),encoded=JSON.stringify(summary);
  expect(summary.run_states.passed).toBe(10);
  expect(summary.receipt_count).toBe(10);
  expect(summary.recent_runs).toHaveLength(8);
  expect(summary.recent_receipts).toHaveLength(8);
  expect(encoded.length).toBeLessThan(8000);
  expect(encoded).not.toContain('private prompt');expect(encoded).not.toContain('large result');expect(encoded).not.toContain('large receipt');
  expect(store.taskStatus(id).runs).toHaveLength(10);
  store.updateTask(id,{error:'e'.repeat(10000),title:'t'.repeat(1000)});
  expect(store.taskSummary(id)).toMatchObject({error:'e'.repeat(2000),title:'t'.repeat(500),text_truncated:1});
  expect(()=>store.taskSummary('bad')).toThrow();
});

test('claim is compare-and-swap and cancelled state survives late success',()=>{
  const {store,projectRoot} = fixture(); const id = create(store);
  expect(store.claimTask(id).state).toBe('running'); expect(store.claimTask(id)).toBeNull();
  const run = store.createRun({task_id:id,agent:'claude',role:'analysis',command:['claude','--version'],cwd:projectRoot});
  expect(store.cancelTask(id).cancelled_runs).toEqual([run.run_id]);
  const result = store.finishRun(run.run_id,{state:'passed',exit_code:0,final:'too late',session_id:'session-one'});
  expect(result.state).toBe('cancelled'); expect(store.updateTask(id,{state:'completed',result_json:{late:true}}).state).toBe('cancelled');
  expect(store.session('session-one','claude').state).toBe('cancelled');
  expect(()=>store.createRun({task_id:id,agent:'claude',role:'late',command:['true'],cwd:projectRoot})).toThrow('not running');
});

test('completed and failed runs are immutable; task cannot complete while a child remains active',()=>{
  const {store,projectRoot} = fixture(); const id = create(store); store.claimTask(id);
  const run = store.createRun({task_id:id,agent:'codex',role:'analysis',command:['true'],cwd:projectRoot});
  expect(()=>store.updateTask(id,{state:'completed'})).toThrow('active runs');
  expect(store.finishRun(run.run_id,{state:'failed',exit_code:1}).state).toBe('failed');
  expect(store.finishRun(run.run_id,{state:'passed',exit_code:0}).state).toBe('failed');
  store.updateTask(id,{state:'failed'}); expect(store.updateTask(id,{state:'running'}).state).toBe('failed');
});

test('cancel preserves lane lock until joined owner releases; expired foreign lock is not proof of death',()=>{
  let at = 100; const {store} = fixture({now:()=>at}); const a=create(store),b=create(store); store.claimTask(a);store.claimTask(b);
  expect(store.acquireLock('writer',a,1)).toBe(true); at=200;
  expect(store.acquireLock('writer',b)).toBe(false); store.cancelTask(a);
  expect(store.acquireLock('writer',b)).toBe(false); expect(store.releaseTaskLocks(a)).toBe(1);
  expect(store.acquireLock('writer',b)).toBe(true);
});

test('canonical project lanes allow independent repositories while legacy global leases must drain',()=>{
  const {store,root,projectRoot}=fixture(),otherRoot=join(root,'other-project');mkdirSync(otherRoot);
  const same=createFabricStore({home:store.home,projectRoot}),other=createFabricStore({home:store.home,projectRoot:otherRoot});
  stores.push(same,other);
  const first=create(store),second=create(same),third=create(other);
  store.claimTask(first);same.claimTask(second);other.claimTask(third);
  expect(store.acquireLock('worktree-write',first)).toBe(true);
  expect(same.acquireLock('worktree-write',second)).toBe(false);
  expect(other.acquireLock('worktree-write',third)).toBe(true);
  const locks=store.summary().locks.map(row=>row.name);
  expect(locks).toHaveLength(2);
  expect(locks.every(name=>/^repo:[a-f0-9]{64}:worktree-write$/.test(name))).toBe(true);
  store.releaseTaskLocks(first); expect(same.acquireLock('worktree-write',second)).toBe(true);
  same.releaseTaskLocks(second);other.releaseTaskLocks(third);
  const legacy=create(store);store.claimTask(legacy);
  const sql=new Database(store.dbPath);
  sql.query('INSERT INTO locks(name,task_id,acquired_at,expires_at) VALUES(?,?,?,?)').run('duel-read',legacy,1,2);
  sql.close();
  expect(same.acquireLock('duel-read',second)).toBe(false);
  expect(other.acquireLock('duel-read',third)).toBe(false);
  store.releaseTaskLocks(legacy);
  expect(same.acquireLock('duel-read',second)).toBe(true);
  expect(other.acquireLock('duel-read',third)).toBe(true);
});

test('confined file IO refuses symlinks, traversal, oversized files and invalid JSON-lines safely',()=>{
  const {store,root} = fixture(); create(store); const outside=join(root,'outside'); writeFileSync(outside,'original');
  const link=join(store.home,'link');symlinkSync(outside,link);
  expect(()=>store.writeText(link,'replace')).toThrow('symlink'); expect(()=>store.readText(outside)).toThrow('escapes');
  expect(readFileSync(outside,'utf8')).toBe('original');
  const log=join(store.logDir,'events');store.writeText(log,'null\n[]\nnot json\n{"threadId":"thread","type":"item.completed","item":{"text":"final"}}\n');
  expect(store.parseJsonLines(log)).toEqual({session_id:'thread',final:'final'});
  expect(()=>store.readText(log,{maxBytes:2})).toThrow('bounded');
  expect(()=>store.writeText(join(store.home,'..','escape'),'x')).toThrow('escapes');
});

test('receipt bytes match digest and task status/event/recent projections retain legacy shape',()=>{
  const {store} = fixture(); const id=create(store); const receipt=store.saveReceipt(id,'proof',{unicode:'ÿ'});
  expect(createHash('sha256').update(readFileSync(receipt.path)).digest('hex')).toBe(receipt.sha256);
  expect(store.taskStatus(id).receipts[0].digest).toBe(receipt.sha256);
  expect(store.recentReceipts(1).receipts[0].id).toBe(receipt.receipt_id);
  expect(store.events(1).events[0].payload.task_id).toBe(id);
  expect(()=>store.saveReceipt(id,'proof',{},'../../escape')).toThrow('suffix');
  expect(()=>store.updateTask(id,{'state=1 --':'completed'})).toThrow('field');
  expect(()=>store.events(501)).toThrow();
});

test('historical hook audit counts suspicious legacy bodies without exposing them', () => {
  const {store}=fixture();
  store.recordEvent('claude','PostToolUse',{source:'claude',kind:'PostToolUse',
    prompt:'PRIVATE_BODY',tool_input:{command:'do not disclose'}},'s1');
  store.recordEvent('codex','PostToolUse',{source:'codex',kind:'PostToolUse',
    session_id:'s2',usage:{input_tokens:1}},'s2');
  const audit=store.summary().historical_hook_audit;
  expect(audit).toEqual({observed_events:2,potential_legacy_bodies:1});
  expect(JSON.stringify(audit)).not.toContain('PRIVATE_BODY');
});

test('safe event projection never loads legacy hook bodies or untrusted session identities', () => {
  const {store}=fixture();
  for(let i=0;i<9;i++)store.recordEvent('claude','PostToolUse',{
    source:'claude',kind:'PostToolUse',session_id:'/Users/private/session',
    prompt:'PRIVATE_BULK_BODY_'.repeat(65000),
    tool_input:{command:'PRIVATE_COMMAND'}},'/Users/private/session');
  store.recordEvent('codex','PostToolUse',{source:'codex',kind:'PostToolUse',
    session_id:'safe-19',usage:{input_tokens:17,output_tokens:3,cache_read_input_tokens:14,
      extra:'PRIVATE_USAGE'}},'safe-19');
  expect(()=>store.events(500)).toThrow('byte budget');
  const safe=store.safeEvents(500).events;
  expect(safe).toHaveLength(10);
  expect(safe[0].payload).toEqual({source:'codex',kind:'PostToolUse',
    session_id:'safe-19',usage:{input_tokens:17,cache_read_input_tokens:14,output_tokens:3}});
  expect(safe[1].session_id).toBeNull();
  expect(safe[1].payload).toEqual({source:'claude',kind:'PostToolUse',legacy_redacted:true});
  expect(JSON.stringify(safe)).not.toContain('PRIVATE_');
  expect(store.summary().historical_hook_audit.observed_events).toBe(10);
});

test('unknown historical hook kind cannot disclose a private event label', () => {
  const {store}=fixture();
  store.recordEvent('codex','PRIVATE_KIND',{session_id:'safe-id',
    prompt:'PRIVATE_TEXT',usage:{input_tokens:9}},'/Users/private');
  const row=store.safeEvents(1).events[0];
  expect(row.kind).toBe('Unknown');
  expect(row.session_id).toBeNull();
  expect(row.payload).toEqual({source:'codex',kind:'Unknown',legacy_redacted:true});
  expect(JSON.stringify(row)).not.toContain('PRIVATE_');
});

test('reopening existing rows neither replays nor rewrites legacy queued/running work',()=>{
  const {store,projectRoot} = fixture(); const queued=create(store),running=create(store);store.claimTask(running);
  const owner=createFabricStore({home:store.home,projectRoot});stores.push(owner);
  expect(owner.summary().tasks).toEqual({queued:1,running:1});
  expect(owner.task(queued)).toEqual(store.task(queued));expect(owner.task(running)).toEqual(store.task(running));
});

async function repository(path) {
  for(const args of [['init','-q'],['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--allow-empty','-m','initial']]) {
    const r=await projectRun(['git',...args],{cwd:path,timeoutMs:10000}); expect(r.exit_code).toBe(0);
  }
}

test('real isolated worktree creation and reuse validate Git registration; diff reports untracked files',async()=>{
  const {store,projectRoot}=fixture();await repository(projectRoot);
  const id=create(store);store.claimTask(id);const first=await store.createWorktree(id);const again=await store.createWorktree(id);
  expect(again.worktree).toBe(first.worktree);expect(store.task(id).worktree).toBe(first.worktree);
  writeFileSync(join(first.worktree,'new.txt'),'candidate');
  const diff=await store.diffPayload(first.worktree);expect(diff.patch).toBe('');expect(diff.untracked).toEqual(['new.txt']);
  expect(diff.patch_sha256).toBe(createHash('sha256').update('').digest('hex'));
},10000);

test('preexisting arbitrary directory is not trusted as a worktree; option-shaped base cannot inject commands',async()=>{
  const {store,projectRoot}=fixture();await repository(projectRoot);const id=create(store);store.claimTask(id);
  mkdirSync(join(store.worktreeRoot,id));await expect(store.createWorktree(id)).rejects.toThrow('registered');
  const other=create(store);store.claimTask(other);await expect(store.createWorktree(other,{base:'--help'})).rejects.toThrow('git operation failed');
},10000);

test('overflowed git diff never yields a complete patch receipt',async()=>{
  const {store,projectRoot}=fixture({run:async argv=>({exit_code:0,stdout:'partial',stderr:'',overflow:argv.includes('--binary')})});
  await expect(store.diffPayload(projectRoot)).rejects.toThrow('output exceeded');
});

test('explicit run identity pins Codex output path and terminal review/uncertain cannot replay',()=>{
  const {store,projectRoot}=fixture();const id=create(store);store.claimTask(id);const run_id='a'.repeat(24);
  const final=join(store.taskDir,id,run_id,'final.txt');
  const spec={run_id,task_id:id,agent:'codex',role:'write',command:['codex','-o',final],cwd:projectRoot};
  expect(store.createRun(spec).final_path).toBe(final);expect(()=>store.createRun(spec)).toThrow('already exists');
  store.finishRun(run_id,{state:'overflow',exit_code:1,overflow:true});
  expect(store.updateTask(id,{state:'review_required'}).state).toBe('review_required');
  expect(store.claimTask(id)).toBeNull();expect(store.updateTask(id,{state:'completed'}).state).toBe('review_required');
  const other=create(store);store.claimTask(other);store.updateTask(other,{state:'uncertain'});expect(store.claimTask(other)).toBeNull();
});

test('event projections enforce byte budget before accumulating unbounded result sets',()=>{
  const {store}=fixture();for(let i=0;i<9;i++)store.recordEvent('fixture','large',{text:'a'.repeat(1000000)});
  expect(()=>store.events()).toThrow('byte budget');expect(store.events(1).events).toHaveLength(1);
});

test('Git cannot inherit repository, index, executable or config injection',async()=>{
  const poison={PATH:process.env.PATH,GIT_DIR:'/untrusted/repo',GIT_WORK_TREE:'/untrusted',GIT_INDEX_FILE:'/untrusted/index',
    GIT_CONFIG_COUNT:'1',GIT_CONFIG_KEY_0:'core.sshCommand',GIT_CONFIG_VALUE_0:'unexpected',GIT_EXEC_PATH:'/untrusted/bin',GIT_CONFIG_PARAMETERS:'poison'};
  const clean=fabricGitEnvironment(poison);expect(clean.PATH).toBe(process.env.PATH);
  expect(Object.keys(clean).filter(key=>key.startsWith('GIT_')).sort()).toEqual(['GIT_CONFIG_GLOBAL','GIT_CONFIG_NOSYSTEM','GIT_TERMINAL_PROMPT']);
  const calls=[];const {store,projectRoot}=fixture({env:poison,run:async(argv,options)=>{calls.push({argv,options});return{exit_code:0,stdout:'',stderr:''};}});
  await store.diffPayload(projectRoot);
  expect(calls).toHaveLength(4);
  for(const {argv,options}of calls){expect(options.env).toEqual(clean);expect(argv).toContain('core.fsmonitor=false');expect(options.cwd).toBe(projectRoot);}
});

test('pre-aborted worktree/diff performs zero subprocesses and zero filesystem effects',async()=>{
  let calls=0;const {store,projectRoot}=fixture({run:async()=>{calls++;throw new Error('must not run');}});
  const id=create(store);store.claimTask(id);const controller=new AbortController();controller.abort();
  await expect(store.createWorktree(id,{signal:controller.signal})).rejects.toThrow('cancelled');
  await expect(store.diffPayload(projectRoot,{signal:controller.signal})).rejects.toThrow('cancelled');
  expect(calls).toBe(0);expect(existsSync(join(store.worktreeRoot,id))).toBe(false);expect(store.task(id).worktree).toBeNull();
});

test('abort after actual Git read prevents subsequent diff or worktree mutation',async()=>{
  const controller=new AbortController();let calls=0;
  const {store,projectRoot}=fixture({run:async(argv,options)=>{
    expect(options.signal).toBe(controller.signal);const result=await projectRun(argv,options);calls++;controller.abort();return result;
  }});
  await repository(projectRoot);const id=create(store);store.claimTask(id);
  await expect(store.createWorktree(id,{signal:controller.signal})).rejects.toThrow('cancelled');
  expect(calls).toBe(1);expect(existsSync(join(store.worktreeRoot,id))).toBe(false);
});

test('abort on registered existing worktree lookup cannot be swallowed as detached success',async()=>{
  const {store,projectRoot}=fixture();await repository(projectRoot);const id=create(store);store.claimTask(id);const first=await store.createWorktree(id);
  const controller=new AbortController();let calls=0;
  const other=createFabricStore({home:store.home,projectRoot,run:async(argv,options)=>{
    const result=await projectRun(argv,options);calls++;if(calls===2)controller.abort();return result;
  }});stores.push(other);
  await expect(other.createWorktree(id,{signal:controller.signal})).rejects.toThrow('cancelled');
  expect(calls).toBe(2);expect(store.task(id).branch).toBe(first.branch);
});

const sourceHash = value => createHash('sha256').update(value).digest('hex');
const selectSource = (projectRoot,path) => ({path,sha256:existsSync(join(projectRoot,path))?sourceHash(readFileSync(join(projectRoot,path))):null});
async function sourceGit(root,args) {
  const result=await projectRun(['git',...args],{cwd:root,timeoutMs:10000});
  if(result.exit_code)throw Error(result.stderr);return result.stdout.trim();
}
test('explicit source baseline captures staged, unstaged, new, binary and deleted inputs without touching the project index',async()=>{
  const {store,projectRoot}=fixture();await repository(projectRoot);
  writeFileSync(join(projectRoot,'tracked.txt'),'committed\n');writeFileSync(join(projectRoot,'deleted.txt'),'committed deletion\n');
  await sourceGit(projectRoot,['add','.']);await sourceGit(projectRoot,['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','source']);
  writeFileSync(join(projectRoot,'tracked.txt'),'staged\n');await sourceGit(projectRoot,['add','tracked.txt']);
  writeFileSync(join(projectRoot,'tracked.txt'),'current uncommitted\n');writeFileSync(join(projectRoot,'new.bin'),Buffer.from([0,255,1]));
  writeFileSync(join(projectRoot,'empty.txt'),'');rmSync(join(projectRoot,'deleted.txt'));
  const paths=['tracked.txt','new.bin','empty.txt','deleted.txt'],files=paths.map(path=>selectSource(projectRoot,path));
  const index=readFileSync(join(projectRoot,'.git/index')),head=await sourceGit(projectRoot,['rev-parse','HEAD']);
  const id=store.createTask({kind:'relay',title:'snapshot',prompt:'fixture',source_files:files});store.claimTask(id);
  const start=await store.createWorktree(id);expect(start.source_snapshot.files).toHaveLength(4);expect(start.source_snapshot.base_commit).toBe(head);
  expect(start.base_commit).not.toBe(head);expect(readFileSync(join(projectRoot,'.git/index'))).toEqual(index);expect(await sourceGit(projectRoot,['rev-parse','HEAD'])).toBe(head);
  expect(readFileSync(join(start.worktree,'tracked.txt'),'utf8')).toBe('current uncommitted\n');expect(readFileSync(join(start.worktree,'new.bin'))).toEqual(Buffer.from([0,255,1]));
  expect(existsSync(join(start.worktree,'deleted.txt'))).toBe(false);expect((await store.diffPayload(start.worktree,{base:start.base_commit})).patch).toBe('');
  expect((await store.createWorktree(id)).source_snapshot.commit).toBe(start.base_commit);
  writeFileSync(join(start.worktree,'tracked.txt'),'agent candidate\n');
  await sourceGit(start.worktree,['add','tracked.txt']);await sourceGit(start.worktree,['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','agent commit']);
  const delta=await store.diffPayload(start.worktree,{base:start.base_commit});expect(delta.patch).toContain('-current uncommitted');expect(delta.patch).toContain('+agent candidate');expect(delta.names).toBe('M\ttracked.txt\n');
  expect(store.verifySourceSnapshot(start.source_snapshot).matches).toBe(true);
  writeFileSync(join(projectRoot,'unrelated.txt'),'outside explicit coverage');expect(store.verifySourceSnapshot(start.source_snapshot).matches).toBe(true);
  writeFileSync(join(projectRoot,'tracked.txt'),'newer user edit\n');expect(()=>store.verifySourceSnapshot(start.source_snapshot)).toThrow('source hash changed');
},15000);

test('source selectors refuse ambiguous names, metadata paths and missing hashes before creating a task',()=>{
  const {store}=fixture();
  for(const files of [[],Array(65).fill({path:'x',sha256:'a'.repeat(64)}),[{path:'.git/config',sha256:null}],[{path:'a/../b',sha256:null}],[{path:'a\\b',sha256:null}],[{path:'x\ny',sha256:null}],[{path:'/absolute',sha256:null}],[{path:'x'}],[{path:'x',sha256:'z'.repeat(64)}],[{path:'x',sha256:null},{path:'X',sha256:null}],[{path:'x',sha256:null,extra:true}]])
    expect(()=>store.createTask({kind:'relay',title:'bad',prompt:'fixture',source_files:files})).toThrow();
  expect(store.summary().tasks).toEqual({});expect(validateSourceFiles([{path:'file with spaces',sha256:null}])).toHaveLength(1);
});

test('wrong or stale source hashes stop before creating a worktree, including absent-file preconditions',async()=>{
  const {store,projectRoot}=fixture();await repository(projectRoot);writeFileSync(join(projectRoot,'current'),'present');
  for(const files of [[{path:'current',sha256:'0'.repeat(64)}],[{path:'current',sha256:null}],[{path:'missing',sha256:'0'.repeat(64)}]]){
    const id=store.createTask({kind:'relay',title:'stale',prompt:'fixture',source_files:files});store.claimTask(id);
    await expect(store.createWorktree(id)).rejects.toThrow();expect(existsSync(join(store.worktreeRoot,id))).toBe(false);
  }
});

test('source admission refuses symlinks, ignored files and aggregate byte overflow',async()=>{
  const {store,projectRoot,root}=fixture();await repository(projectRoot);
  writeFileSync(join(root,'private'),'outside');symlinkSync(join(root,'private'),join(projectRoot,'link'));
  writeFileSync(join(projectRoot,'.gitignore'),'ignored\n');writeFileSync(join(projectRoot,'ignored'),'hidden');
  writeFileSync(join(projectRoot,'big-a'),Buffer.alloc(5*1024*1024));writeFileSync(join(projectRoot,'big-b'),Buffer.alloc(4*1024*1024));
  for(const [files,error] of [[[selectSource(projectRoot,'link')],'symlink'],[[selectSource(projectRoot,'ignored')],'ignored'],[['big-a','big-b'].map(path=>selectSource(projectRoot,path)),'bounded']]){
    const id=store.createTask({kind:'relay',title:'refused',prompt:'fixture',source_files:files});store.claimTask(id);await expect(store.createWorktree(id)).rejects.toThrow(error);
  }
});

test('source mode changes invalidate review context and snapshot receipt corruption refuses reuse',async()=>{
  const {store,projectRoot}=fixture();await repository(projectRoot);writeFileSync(join(projectRoot,'run.sh'),'exit 0\n');
  const id=store.createTask({kind:'relay',title:'mode',prompt:'fixture',source_files:[selectSource(projectRoot,'run.sh')]});store.claimTask(id);
  const start=await store.createWorktree(id);chmodSync(join(projectRoot,'run.sh'),0o755);expect(()=>store.verifySourceSnapshot(start.source_snapshot)).toThrow('mode changed');
  writeFileSync(start.source_snapshot.receipt.path,'{}');await expect(store.createWorktree(id)).rejects.toThrow('integrity');
});

test('a source edit during baseline preparation refuses the mixed snapshot and preserves project content',async()=>{
  let rootToChange;
  const {store,projectRoot}=fixture({run:async(argv,options)=>{
    const r=await projectRun(argv,options);
    if(rootToChange&&argv.includes('hash-object')){writeFileSync(join(rootToChange,'source.txt'),'concurrent edit');rootToChange=null;}
    return r;
  }});await repository(projectRoot);writeFileSync(join(projectRoot,'source.txt'),'selected');rootToChange=projectRoot;
  const id=store.createTask({kind:'relay',title:'race',prompt:'fixture',source_files:[selectSource(projectRoot,'source.txt')]});store.claimTask(id);
  await expect(store.createWorktree(id)).rejects.toThrow('source hash changed');expect(readFileSync(join(projectRoot,'source.txt'),'utf8')).toBe('concurrent edit');
  expect(store.taskStatus(id).receipts.some(row=>row.kind==='source-snapshot')).toBe(false);
});

test('cancel during source seeding cannot activate a baseline or resume partial preparation',async()=>{
  const controller=new AbortController();let abortHash=false;
  const {store,projectRoot}=fixture({run:async(argv,options)=>{
    const r=await projectRun(argv,options);if(abortHash&&argv.includes('hash-object'))controller.abort();return r;
  }});await repository(projectRoot);writeFileSync(join(projectRoot,'input'),'source');
  const id=store.createTask({kind:'relay',title:'abort baseline',prompt:'fixture',source_files:[selectSource(projectRoot,'input')]});store.claimTask(id);abortHash=true;
  await expect(store.createWorktree(id,{signal:controller.signal})).rejects.toThrow('cancelled');
  expect(store.taskStatus(id).receipts.some(row=>row.kind==='source-snapshot')).toBe(false);
  await expect(store.createWorktree(id)).rejects.toThrow('requires reconciliation');
  expect(readFileSync(join(projectRoot,'input'),'utf8')).toBe('source');
});

test('an unsafe destination symlink in the configured Git base cannot overwrite another owner',async()=>{
  const {store,projectRoot,root}=fixture();await repository(projectRoot);const outside=join(root,'outside');writeFileSync(outside,'outside owner');
  symlinkSync(outside,join(projectRoot,'target'));await sourceGit(projectRoot,['add','target']);await sourceGit(projectRoot,['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','link base']);
  rmSync(join(projectRoot,'target'));writeFileSync(join(projectRoot,'target'),'current regular input');
  const id=store.createTask({kind:'relay',title:'link base',prompt:'fixture',source_files:[selectSource(projectRoot,'target')]});store.claimTask(id);
  await expect(store.createWorktree(id)).rejects.toThrow('symlink');expect(readFileSync(outside,'utf8')).toBe('outside owner');
});

test('sixty-four source files use one batched blob write and one index update',async()=>{
  const calls=[];const {store,projectRoot}=fixture({run:async(argv,options)=>{calls.push(argv);return projectRun(argv,options);}});await repository(projectRoot);
  const files=Array.from({length:64},(_,i)=>{const path=`input-${i}.txt`;writeFileSync(join(projectRoot,path),`source ${i}\n`);return selectSource(projectRoot,path);});
  const id=store.createTask({kind:'relay',title:'bounded batch',prompt:'fixture',source_files:files});store.claimTask(id);
  const start=await store.createWorktree(id);expect(start.source_snapshot.files).toHaveLength(64);expect(calls.filter(argv=>argv.includes('hash-object'))).toHaveLength(1);expect(calls.filter(argv=>argv.includes('update-index'))).toHaveLength(1);
  expect((await store.diffPayload(start.worktree,{base:start.base_commit})).patch).toBe('');
});

test('Git attributes that would reinterpret captured bytes refuse the baseline before agents',async()=>{
  const {store,projectRoot}=fixture();await repository(projectRoot);
  writeFileSync(join(projectRoot,'.gitattributes'),'*.txt text eol=lf\n');await sourceGit(projectRoot,['add','.gitattributes']);await sourceGit(projectRoot,['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','attributes']);
  writeFileSync(join(projectRoot,'input.txt'),'exact\r\n');const id=store.createTask({kind:'relay',title:'transformed input',prompt:'fixture',source_files:[selectSource(projectRoot,'input.txt')]});store.claimTask(id);
  await expect(store.createWorktree(id)).rejects.toThrow('git operation failed');expect(store.taskStatus(id).receipts.some(row=>row.kind==='source-snapshot')).toBe(false);
});
