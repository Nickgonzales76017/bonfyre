import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createOperatorStore } from '../operator_store.mjs';

async function fixture(check) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-operator-store-'));
  let time = 1800000000;
  const stores = [];
  const open = () => {
    const store = createOperatorStore({ home, now: () => time });
    stores.push(store);
    return store;
  };
  try { return await check({ home, open, now: () => time, advance: seconds => { time += seconds; } }); }
  finally { for (const store of stores) store.close(); rmSync(home, { recursive: true, force: true }); }
}
const add = (store, key = 'a', extra = {}) => store.addGoal({ title: `Goal ${key}`, details: 'Fixture only',
  priority: 50, source: 'test', dedupe_key: key, ...extra });
const claim = store => store.claimGoal();

test('construction and close do not create a database or seed goals', () => fixture(({ home, open }) => {
  const store = open();
  expect(existsSync(join(home, 'operator.sqlite3'))).toBe(false);
  store.close();
  expect(existsSync(join(home, 'operator.sqlite3'))).toBe(false);
  const next = open();
  expect(next.selectGoal()).toBeNull();
  expect(next.seedGoals()).toHaveLength(9);
  expect(next.seedGoals()).toHaveLength(9);
  const db = new Database(next.path, { readonly: true });
  try { expect(db.query('SELECT COUNT(*) AS n FROM goals').get().n).toBe(9); }
  finally { db.close(); }
}));

test('legacy schema opens additively and preserves rows and project-facing columns', () => fixture(({ home, open }) => {
  const db = new Database(join(home, 'operator.sqlite3'));
  db.exec(`CREATE TABLE goals (id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,
    priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,
    interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE runs (id TEXT PRIMARY KEY,goal_id TEXT NOT NULL,started_at REAL NOT NULL,completed_at REAL,
    state TEXT NOT NULL,response_id TEXT,summary TEXT NOT NULL DEFAULT '',usage_json TEXT NOT NULL DEFAULT '{}');
    CREATE TABLE actions (seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,tool TEXT NOT NULL,args_json TEXT NOT NULL,
    state TEXT NOT NULL,result_json TEXT NOT NULL,started_at REAL NOT NULL,duration_ms INTEGER NOT NULL);
    CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE budgets (day TEXT PRIMARY KEY,cycles INTEGER NOT NULL DEFAULT 0,tool_calls INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,output_tokens INTEGER NOT NULL DEFAULT 0);
    INSERT INTO goals VALUES('legacy','legacy','Keep me','old detail',90,'resolved','python',0,0,0,4,'',1,2);
    INSERT INTO meta VALUES('event_cursor','47');`);
  const original = db.query('PRAGMA table_info(goals)').all();
  db.close();
  const store = open();
  expect(store.getGoal('legacy').attempts).toBe(4);
  expect(store.metaGet('event_cursor')).toBe('47');
  add(store, 'legacy', { title: 'Still resolved', recurring: true });
  expect(store.getGoal('legacy').state).toBe('resolved');
  expect(store.getGoal('legacy').recurring).toBe(0);
  const verify = new Database(store.path, { readonly: true });
  try {
    expect(verify.query('PRAGMA table_info(goals)').all()).toEqual(original);
    expect(verify.query("SELECT name FROM sqlite_master WHERE name='operator_response_usage'").get()).not.toBeNull();
  } finally { verify.close(); }
}));

test('claim is exclusive across connections and duplicate goal ingestion cannot requeue active work', () => fixture(({ open }) => {
  const first = open(), second = open();
  const goal = add(first);
  expect(second.selectGoal().id).toBe(goal.id); // stale selection cannot authorize a later claim.
  const active = first.claimGoal({ goalId: goal.id });
  expect(active.goal.state).toBe('in_progress');
  expect(second.claimGoal({ goalId: goal.id })).toBeNull();
  expect(add(second, 'a', { priority: 70 }).state).toBe('in_progress');
  expect(second.claimGoal()).toBeNull();
  // A legacy producer can still requeue; the open run also prevents duplication.
  const legacy = new Database(first.path);
  legacy.query("UPDATE goals SET state='queued' WHERE id=?").run(goal.id);
  legacy.close();
  expect(second.claimGoal()).toBeNull();
  expect(first.cyclesLastHour()).toBe(1);
}));

test('dedupe replay cannot revive or rewrite a migrated source shard', () => fixture(({ open, advance }) => {
  const store = open();
  const original = add(store, 'catalogue:shard:1', { title: 'Source catalogue slice', details: 'Verbatim original source URLs', priority: 92 });
  const db = new Database(store.path);
  db.query("UPDATE goals SET state='superseded',last_error='Moved to canonical source collection' WHERE id=?").run(original.id);
  const before = db.query('SELECT * FROM goals WHERE id=?').get(original.id);
  db.close();
  advance(1000);
  const replay = add(store, 'catalogue:shard:1', { title: 'Try to requeue source text', details: 'Overwrite attempt', priority: 100 });
  expect(replay).toEqual(before);
  expect(store.selectGoal()).toBeNull();
}));


test('claim enforces daily and hourly budgets in its write transaction', () => fixture(({ open, advance }) => {
  const store = open();
  add(store, 'a', { priority: 90 }); add(store, 'b');
  expect(store.claimGoal({ policy: { daily_input_token_budget: 0 } })).toBeNull();
  const active = store.claimGoal({ policy: { max_cycles_per_hour: 1 } });
  expect(active.goal.title).toBe('Goal a');
  expect(store.claimGoal({ policy: { max_cycles_per_hour: 1 } })).toBeNull();
  expect(store.budgetOk().current.cycles).toBe(1);
  store.recordResponse({ run_id: active.run.id, response_id: 'r1', usage: { input_tokens: 10, output_tokens: 5 } });
  advance(3601);
  expect(store.claimGoal({ policy: { daily_input_token_budget: 10 } })).toBeNull();
  expect(store.claimGoal({ policy: { daily_input_token_budget: 11 } }).goal.title).toBe('Goal b');
}));

test('a charged cycle continues only for its exact live run and still enforces resource budgets', () => fixture(({ open, now }) => {
  const store = open();
  add(store, 'first'); add(store, 'second');
  const policy = { daily_cycle_budget: 1, daily_tool_call_budget: 2,
    daily_input_token_budget: 6, daily_output_token_budget: 8 };
  const { run } = store.claimGoal({ policy });
  expect(store.budgetOk(policy).okay).toBe(false);
  expect(store.budgetOk(policy, now(), { continuingRunId: run.id }).okay).toBe(true);
  expect(store.budgetOk(policy, now(), { continuingRunId: 'missing-run' }).okay).toBe(false);
  expect(store.claimGoal({ policy })).toBeNull();
  store.recordAction({ run_id: run.id, tool: 'read', args: {}, state: 'passed',
    result: {}, started_at: now(), policy });
  expect(store.budgetOk(policy, now(), { continuingRunId: run.id }).okay).toBe(true);
  store.recordAction({ run_id: run.id, tool: 'read', args: {}, state: 'passed',
    result: {}, started_at: now(), policy });
  expect(store.budgetOk(policy, now(), { continuingRunId: run.id }).okay).toBe(false);
  expect(store.budgetOk(policy, now(), { continuingRunId: run.id, forModelResponse: true }).okay).toBe(true);
  // A revoked or tightened budget cannot authorize another model response.
  expect(store.budgetOk({ ...policy, daily_tool_call_budget: 0 }, now(),
    { continuingRunId: run.id, forModelResponse: true }).okay).toBe(false);
  expect(store.budgetOk({ ...policy, daily_tool_call_budget: 1 }, now(),
    { continuingRunId: run.id, forModelResponse: true }).okay).toBe(false);
  expect(store.budgetOk(policy, now(), { continuingRunId: 'unknown', forModelResponse: true }).okay).toBe(false);
  expect(store.budgetOk(policy, now(), { forModelResponse: true }).okay).toBe(false);
  expect(() => store.recordAction({ run_id: run.id, tool: 'read', args: {}, state: 'passed',
    result: {}, started_at: now(), policy })).toThrow('budget exhausted');
  store.recordResponse({ run_id: run.id, response_id: 'r1', usage: { input_tokens: 6, output_tokens: 2 } });
  expect(store.budgetOk(policy, now(), { continuingRunId: run.id }).okay).toBe(false);
  expect(store.budgetOk({ ...policy, daily_cycle_budget: 0 }, now(), { continuingRunId: run.id }).okay).toBe(false);
  store.finishGoal({ run_id: run.id, disposition: 'resolved', summary: 'safe fixture' });
  expect(store.budgetOk(policy, now(), { continuingRunId: run.id }).okay).toBe(false);
}));

test('every response is durably counted once, including responses before failed runs', () => fixture(({ open }) => {
  const store = open(); add(store);
  const { run } = claim(store);
  const one = { run_id: run.id, response_id: 'one', usage: { input_tokens: 100, output_tokens: 10 } };
  expect(store.recordResponse(one).recorded).toBe(true);
  expect(store.recordResponse(one).recorded).toBe(false);
  expect(() => store.recordResponse({ ...one, usage: { input_tokens: 999 } })).toThrow('conflicting');
  store.recordResponse({ run_id: run.id, response_id: 'two', usage: { input_tokens: 200, output_tokens: 20 } });
  const failed = store.failRun({ run_id: run.id, error: 'transient connection reset' });
  expect(failed.run.state).toBe('failed');
  expect(JSON.parse(failed.run.usage_json)).toEqual({ input_tokens: 300, output_tokens: 30, total_tokens: 330, response_count: 2 });
  expect(store.budgetOk().current).toMatchObject({ cycles: 1, input_tokens: 300, output_tokens: 30 });
  expect(() => store.recordResponse({ run_id: run.id, response_id: 'three', usage: {} })).toThrow('not active');
  expect(store.recordResponse(one).recorded).toBe(false);
}));

test('actions account successful and failed tool calls atomically with bounded JSON', () => fixture(({ open, now, advance }) => {
  const store = open(); add(store); const { run } = claim(store);
  const started = now(); advance(2);
  const action = { run_id: run.id, tool: 'fixture', args: { text: 'safe' }, state: 'failed', result: { error: 'fixture' }, started_at: started };
  expect(store.recordAction(action)).toBe(1);
  store.recordAction({ ...action, state: 'completed' });
  expect(store.budgetOk().current.tool_calls).toBe(2);
  expect(() => store.recordAction({ ...action, result: 'x'.repeat(1048577) })).toThrow('oversized');
  expect(() => store.recordAction({ ...action, run_id: 'missing' })).toThrow('not active');
  expect(store.budgetOk().current.tool_calls).toBe(2);
  const db = new Database(store.path, { readonly: true });
  try { expect(db.query('SELECT duration_ms FROM actions WHERE seq=1').get().duration_ms).toBe(2000); }
  finally { db.close(); }
}));

test('action intent is counted before dispatch and completion does not double count or replay', () => fixture(({ open, now, advance }) => {
  const store = open(); add(store); const { run } = claim(store);
  const seq = store.recordAction({ run_id: run.id, tool: 'fixture', args: {}, state: 'started', result: null, started_at: now() });
  expect(store.budgetOk().current.tool_calls).toBe(1);
  advance(3);
  expect(store.completeAction({ seq, state: 'passed', result: { okay: true } })).toBe(seq);
  expect(store.budgetOk().current.tool_calls).toBe(1);
  expect(() => store.completeAction({ seq, state: 'passed', result: {} })).toThrow('not awaiting');
  const db = new Database(store.path, { readonly: true });
  try { expect(db.query('SELECT state,duration_ms FROM actions WHERE seq=?').get(seq))
    .toEqual({ state: 'passed', duration_ms: 3000 }); }
  finally { db.close(); }
}));

test('unresolved action outcomes override retry and claimed resolution even without process death', () => fixture(({ open, now }) => {
  const store = open();
  for (const state of ['started', 'outcome_uncertain']) {
    for (const disposition of ['retry', 'resolved', 'failed']) {
      add(store, `${state}:${disposition}`); const { run } = claim(store);
      store.recordAction({ run_id: run.id, tool: 'fixture', args: {}, state, result: null, started_at: now() });
      const result = disposition === 'failed' ? store.failRun({ run_id: run.id, error: 'request timeout' }) :
        store.finishGoal({ run_id: run.id, disposition, summary: 'model claim' });
      expect(result.goal).toMatchObject({ state: 'blocked', retry_at: 0 });
      expect(result.goal.last_error).toContain('reconciliation');
      expect(result.run.summary).toContain('reconciliation');
      expect(store.claimGoal()).toBeNull();
    }
  }
}));

test('event ingestion and cursor commit together and cursor never regresses', () => fixture(({ open }) => {
  const store = open();
  const events = [{ kind: 'project.delta', payload: { status_hash: 'delta' } },
    { kind: 'mcp.call', payload: { state: 'failed', tool: 'test', call_id: 'call' } },
    { kind: 'task.started', payload: { state: 'failed_to_start', task_id: 'task' } },
    { kind: 'ignored', payload: {} }];
  expect(store.ingestEvents(events, { nextAfter: 9 })).toBe(3);
  expect(store.metaGet('event_cursor')).toBe('9');
  expect(store.ingestEvents(events, { nextAfter: 2 })).toBe(3);
  expect(store.metaGet('event_cursor')).toBe('9');
  expect(() => store.ingestEvents([{ kind: 'project.delta', payload: { status_hash: 'rollback' } },
    { kind: 'mcp.call', payload: [] }], { nextAfter: 10 })).toThrow('payload');
  expect(store.metaGet('event_cursor')).toBe('9');
  const db = new Database(store.path, { readonly: true });
  try { expect(db.query('SELECT COUNT(*) AS n FROM goals').get().n).toBe(3); }
  finally { db.close(); }
}));

test('finish recurrence, retry caps and terminal causes share atomic run/goal transitions', () => fixture(({ open, now, advance }) => {
  const store = open(); add(store, 'repeat', { recurring: true, interval_seconds: 30 });
  let { run } = claim(store);
  const result = store.finishGoal({ run_id: run.id, disposition: 'resolved', summary: 'Verified' });
  expect(result.goal.state).toBe('retry');
  expect(result.goal.retry_at).toBe(now() + 60);
  expect(result.goal.attempts).toBe(1);
  expect(store.claimGoal()).toBeNull();
  expect(() => store.finishGoal({ run_id: run.id, disposition: 'resolved' })).toThrow('not active');
  advance(60); ({ run } = claim(store));
  const capped = store.finishGoal({ run_id: run.id, disposition: 'deferred', summary: 'Try later', policy: { max_goal_attempts: 2 } });
  expect(capped.goal.state).toBe('blocked');
  add(store, 'terminal'); ({ run } = claim(store));
  expect(store.failRun({ run_id: run.id, error: 'API: insufficient_quota' }).goal.state).toBe('blocked');
  add(store, 'resolved'); ({ run } = claim(store));
  expect(store.finishGoal({ run_id: run.id, disposition: 'resolved' }).goal.state).toBe('resolved');
  add(store, 'blocked'); ({ run } = claim(store));
  expect(store.finishGoal({ run_id: run.id, disposition: 'blocked', summary: 'Requires approval' }).goal)
    .toMatchObject({ state: 'blocked', retry_at: 0, attempts: 1 });
}));

test('orphan reconciliation never mistakes old timestamps for dead executors', () => fixture(({ open, advance }) => {
  const store = open(); const old = add(store, 'old', { priority: 90 }); const { run: oldRun } = claim(store);
  advance(7201);
  const recent = add(store, 'recent'); const { run: recentRun } = claim(store);
  expect(store.reconcileOrphans({ cycle_timeout_seconds: 100 }))
    .toMatchObject({ reclaimed: 0, held_live: 1, held_unknown: 0 });
  expect(store.getGoal(old.id).state).toBe('in_progress');
  const legacy = new Database(store.path);
  legacy.query('DELETE FROM operator_run_owners WHERE run_id=?').run(oldRun.id);
  legacy.close();
  expect(store.reconcileOrphans({ cycle_timeout_seconds: 100 }))
    .toMatchObject({ reclaimed: 0, held_live: 0, held_unknown: 1 });
  expect(store.getGoal(recent.id).state).toBe('in_progress');
  expect(store.getRun(recentRun.id).completed_at).toBeNull();
}));

async function childClaim(home, now, action = false) {
  const source = `import {createOperatorStore} from ${JSON.stringify(new URL('../operator_store.mjs', import.meta.url).pathname)};
    const store=createOperatorStore({home:${JSON.stringify(home)},now:()=>${now}});
    const claim=store.claimGoal();
    if(claim && ${action}) store.recordAction({run_id:claim.run.id,tool:'fixture',args:{},state:'started',result:null,started_at:${now}});
    console.log(JSON.stringify(claim));store.close();`;
  const result = await promisify(execFile)(process.execPath, ['--eval', source], { timeout: 10000, maxBuffer: 65536 });
  return JSON.parse(result.stdout);
}

test('concurrent processes obtain exactly one claim; proven dead local executor can be reclaimed', () => fixture(async ({ home, open, now, advance }) => {
  const store = open(), goal = add(store);
  const results = await Promise.all([childClaim(home, now()), childClaim(home, now())]);
  expect(results.filter(Boolean)).toHaveLength(1);
  const old = results.find(Boolean);
  expect(store.cyclesLastHour()).toBe(1);
  expect(store.reconcileOrphans({ cycle_timeout_seconds: 100 }).reclaimed).toBe(0);
  advance(7201);
  expect(store.reconcileOrphans({ cycle_timeout_seconds: 100 }).reclaimed).toBe(1);
  expect(store.getGoal(goal.id)).toMatchObject({ state: 'retry', attempts: 1, retry_at: now() });
  expect(store.getRun(old.run.id).state).toBe('failed');
  expect(store.claimGoal({ goalId: goal.id })).not.toBeNull();
  expect(() => store.finishGoal({ run_id: old.run.id, disposition: 'resolved' })).toThrow('not active');
}));

test('a dead executor with attempted effects is blocked instead of automatically replayed', () => fixture(async ({ home, open, now, advance }) => {
  const store = open(), goal = add(store);
  const active = await childClaim(home, now(), true);
  advance(7201);
  expect(store.reconcileOrphans({ cycle_timeout_seconds: 100 }))
    .toMatchObject({ reclaimed: 0, blocked: 1, held_unknown: 0, held_live: 0 });
  expect(store.getGoal(goal.id)).toMatchObject({ state: 'blocked', retry_at: 0 });
  expect(store.getRun(active.run.id).summary).toContain('reconciliation');
  expect(store.claimGoal()).toBeNull();
  expect(store.budgetOk().current.tool_calls).toBe(1);
}));

test('local calendar rollover accounts each response and action on its recorded day', () => fixture(({ open, now, advance }) => {
  const store = open(); add(store); const { run } = claim(store);
  store.recordResponse({ run_id: run.id, response_id: 'yesterday', usage: { input_tokens: 10 } });
  const first = store.budgetOk().current;
  const midnight = new Date(now() * 1000); midnight.setHours(24, 0, 0, 0);
  advance(midnight.getTime() / 1000 - now());
  store.recordResponse({ run_id: run.id, response_id: 'today', usage: { input_tokens: 7 } });
  const second = store.budgetOk().current;
  expect(second.day).not.toBe(first.day);
  expect(first.input_tokens).toBe(10);
  expect(second).toMatchObject({ cycles: 0, input_tokens: 7 });
  expect(JSON.parse(store.getRun(run.id).usage_json).input_tokens).toBe(17);
}));

test('invalid amounts cannot corrupt accounting and reopening preserves durable totals', () => fixture(({ open }) => {
  const first = open(); add(first); const { run } = claim(first);
  for (const input of [-1, 1.5, '7', Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => first.recordResponse({ run_id: run.id, response_id: 'bad', usage: { input_tokens: input } })).toThrow();
  }
  expect(first.budgetOk().current.input_tokens).toBe(0);
  first.recordResponse({ run_id: run.id, response_id: 'good', usage: { input_tokens: 7, output_tokens: 3 } });
  first.close();
  const second = open();
  expect(second.budgetOk().current).toMatchObject({ cycles: 1, input_tokens: 7, output_tokens: 3 });
  expect(JSON.parse(second.getRun(run.id).usage_json).response_count).toBe(1);
}));

test('budget arithmetic overflow rolls back response accounting and run aggregates together', () => fixture(({ open }) => {
  const store = open(); add(store, 'large', { priority: 90 }); add(store, 'small');
  const large = claim(store), small = claim(store);
  store.recordResponse({ run_id: large.run.id, response_id: 'large', usage: { input_tokens: Number.MAX_SAFE_INTEGER } });
  expect(() => store.recordResponse({ run_id: small.run.id, response_id: 'overflow', usage: { input_tokens: 1 } })).toThrow('total');
  expect(store.budgetOk().current.input_tokens).toBe(Number.MAX_SAFE_INTEGER);
  expect(JSON.parse(store.getRun(small.run.id).usage_json)).toEqual({});
  const db = new Database(store.path, { readonly: true });
  try { expect(db.query("SELECT response_id FROM operator_response_usage WHERE response_id='overflow'").get()).toBeNull(); }
  finally { db.close(); }
}));
test('follow-up batch admission is atomic, replay-safe, and accepts empty optional details', () => fixture(({ open }) => {
  const store = open();
  const base = { priority: 50, source: 'run:fixture', recurring: false, interval_seconds: 0 };
  const batch = [{ ...base, title: 'Empty details', details: '', dedupe_key: 'run:fixture:a' },
    { ...base, title: 'Another goal', details: 'full details', dedupe_key: 'run:fixture:b' }];
  expect(store.importGoals(batch, { apply: false })).toMatchObject({ created: 0, missing: 2 });
  const accepted = store.importGoals(batch, { apply: true });
  expect(accepted).toMatchObject({ created: 2, unchanged: 0 });
  const firstId = accepted.goals[0].id;
  const active = store.claimGoal({ goalId: firstId });
  expect(active.goal.id).toBe(firstId);
  store.finishGoal({ run_id: active.run.id, disposition: 'resolved', summary: 'already completed' });
  const replay = store.importGoals(batch, { apply: true });
  expect(replay).toMatchObject({ created: 0, unchanged: 2 });
  expect(replay.goals[0].state).toBe('resolved');
  expect(() => store.importGoals([batch[0], { ...batch[1], title: 'Changed without review' }],
    { apply: true })).toThrow('Delivery definitions changed');
  expect(store.getGoal(firstId).state).toBe('resolved');
  const db = new Database(store.path, { readonly: true });
  try { expect(db.query('SELECT count(*) AS n FROM goals').get().n).toBe(2); }
  finally { db.close(); }
}));
