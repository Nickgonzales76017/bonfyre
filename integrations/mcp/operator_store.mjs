import { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';
import { existsSync, lstatSync, mkdirSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { hostname } from 'node:os';

const TERMINAL = ['you have no credit', 'insufficient_quota', 'credit_balance_exhausted',
  'billing_hard_limit', 'exceeded your current quota', 'invalid_api_key', 'incorrect api key',
  'account is not active', 'account_deactivated'];
export const PROVIDERS = ['openai'];
export const terminalCause = message => TERMINAL.some(marker => String(message).toLowerCase().includes(marker));
const uuid = () => randomUUID().replaceAll('-', '');
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function string(value, name, max = 4096, empty = false) {
  if (typeof value !== 'string' || (!empty && !value.trim()) || Buffer.byteLength(value) > max || value.includes('\0'))
    throw new Error(`invalid ${name}`);
  return value;
}
function integer(value, name, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`invalid ${name}`);
  return value;
}
function encoded(value, name, max = 1024 * 1024) {
  const json = JSON.stringify(value);
  if (typeof json !== 'string' || Buffer.byteLength(json) > max) throw new Error(`invalid or oversized ${name}`);
  return json;
}
function dayAt(seconds) {
  const date = new Date(seconds * 1000);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}
const SCHEMA = `
CREATE TABLE IF NOT EXISTS goals (
 id TEXT PRIMARY KEY,dedupe_key TEXT UNIQUE,title TEXT NOT NULL,details TEXT NOT NULL,
 priority INTEGER NOT NULL,state TEXT NOT NULL,source TEXT NOT NULL,recurring INTEGER NOT NULL DEFAULT 0,
 interval_seconds INTEGER NOT NULL DEFAULT 0,retry_at REAL NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
 last_error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS runs (
 id TEXT PRIMARY KEY,goal_id TEXT NOT NULL,started_at REAL NOT NULL,completed_at REAL,state TEXT NOT NULL,
 response_id TEXT,summary TEXT NOT NULL DEFAULT '',usage_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE IF NOT EXISTS actions (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,tool TEXT NOT NULL,args_json TEXT NOT NULL,
 state TEXT NOT NULL,result_json TEXT NOT NULL,started_at REAL NOT NULL,duration_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS budgets (day TEXT PRIMARY KEY,cycles INTEGER NOT NULL DEFAULT 0,
 tool_calls INTEGER NOT NULL DEFAULT 0,input_tokens INTEGER NOT NULL DEFAULT 0,output_tokens INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS operator_response_usage (
 response_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,usage_json TEXT NOT NULL,recorded_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS operator_run_owners (
 run_id TEXT PRIMARY KEY,pid INTEGER NOT NULL,owner_host TEXT NOT NULL,owner_nonce TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS operator_provider_health (
 provider TEXT PRIMARY KEY,state TEXT NOT NULL,cause TEXT NOT NULL,
 observed_at REAL NOT NULL,retry_at REAL NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS operator_goals_due ON goals(state,priority DESC,updated_at,retry_at);
CREATE INDEX IF NOT EXISTS operator_runs_goal_open ON runs(goal_id,completed_at,started_at);
CREATE INDEX IF NOT EXISTS operator_runs_started ON runs(started_at);
CREATE INDEX IF NOT EXISTS operator_actions_run ON actions(run_id,seq);
`;

// Deliberately explicit: construction/import/normal daemon startup never seeds work.
const SEEDS = [
  ['Repair native model runtime 500', 'Diagnose /api/native-build/model-runtime using live Oracle evidence and local source. Patch, test, and verify the route if the fix is low-risk.', 98, 'seed:native-model-runtime-500', false, 0],
  ['Close provider-credit failures', 'Read failed provider-credit receipts and logs, identify exact blockers, repair low-risk code/config defects, and rerun the profile.', 95, 'seed:provider-credit', false, 0],
  ['Close cloud-placement failures', 'Read all cloud-placement failed receipts, distinguish missing credentials from code defects, repair low-risk defects, and rerun only affected tools.', 94, 'seed:cloud-placement', false, 0],
  ['Promote MCP deep flows into normal operations', 'Run MCP execution/parity/recursive interaction flows, repair failed edges, and convert useful outputs into operator goals and receipts.', 90, 'seed:mcp-deep-flows', true, 21600],
  ['Behaviorally certify the 93-command plane', 'Move beyond --help probes. Select representative real fixtures per command family, execute safe behavior tests, and record pass/fail/unsupported evidence.', 88, 'seed:command-behavior', true, 43200],
  ['Resolve route contract gaps', 'Investigate proof manifest and measured finance 404s, route aliases, and runtime contract mismatches. Repair only when intended routes are confirmed by source.', 86, 'seed:route-contract-gaps', false, 0],
  ['Resolve BonfyreWatch probe timeout', 'Determine whether BonfyreWatch is a daemon-style command or has a broken help/version contract. Add a bounded diagnostic contract if appropriate.', 80, 'seed:watch-timeout', false, 0],
  ['Activate a real Linux HVM4 lane', 'Locate source/build contract, build a Linux HVM4 runtime, run structural smoke tests, and remove the blocked classification only with executable proof.', 75, 'seed:hvm4-linux', false, 0],
  ['Continuous autonomous power-user sweep', 'Inspect recent project deltas, MCP failures, runtime 5xx responses, failed receipts, stale tasks, release drift, and high-leverage closure opportunities. Resolve what is safely actionable.', 60, 'seed:continuous-sweep', true, 900],
];

export function createOperatorStore({ home, now = () => Date.now() / 1000 } = {}) {
  string(home, 'operator home', 16384);
  if (typeof now !== 'function') throw new Error('invalid clock');
  const directory = resolve(home), path = join(directory, 'operator.sqlite3');
  const ownerHost = hostname(), ownerNonce = uuid();
  let connection = null, closed = false;
  function clock() {
    const value = now();
    if (!Number.isFinite(value) || value < 0 || value > 8640000000000) throw new Error('invalid clock value');
    return value;
  }
  function db() {
    if (closed) throw new Error('operator store is closed');
    if (!connection) {
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      if (!lstatSync(directory).isDirectory() || lstatSync(directory).isSymbolicLink() ||
          (existsSync(path) && (!lstatSync(path).isFile() || lstatSync(path).isSymbolicLink())))
        throw new Error('operator store requires regular local paths');
      const opened = new Database(path, { create: true, strict: true });
      try {
        opened.exec('PRAGMA busy_timeout=30000; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;');
        opened.transaction(() => opened.exec(SCHEMA)).immediate();
        connection = opened;
      } catch (error) { opened.close(); throw error; }
    }
    return connection;
  }
  const query = (sql, ...args) => db().query(sql).get(...args);
  const write = (sql, ...args) => db().query(sql).run(...args);
  const atomic = fn => db().transaction(fn).immediate();
  function metaGet(key, fallback = '') {
    string(key, 'meta key');
    return query('SELECT value FROM meta WHERE key=?', key)?.value ?? fallback;
  }
  function metaSet(key, value) {
    string(key, 'meta key');
    if (!['string', 'number', 'boolean'].includes(typeof value)) throw new Error('invalid meta value');
    string(String(value), 'meta value', 65536, true);
    write('INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', key, String(value));
  }
  function addGoal({ title, details = '', priority = 50, source = 'operator', dedupe_key,
    recurring = false, interval_seconds = 0 }) {
    string(title, 'goal title'); string(details, 'goal details', 262144, true);
    string(source, 'goal source'); string(dedupe_key, 'goal dedupe key');
    integer(priority, 'priority', -1000000, 1000000);
    integer(interval_seconds, 'interval_seconds', 0, 315360000);
    if (typeof recurring !== 'boolean') throw new Error('invalid recurring flag');
    const at = clock();
    return atomic(() => {
      write(`INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,
        interval_seconds,retry_at,attempts,last_error,created_at,updated_at)
        VALUES(?,?,?,?,?,'queued',?,?,?,0,0,'',?,?)
        ON CONFLICT(dedupe_key) DO UPDATE SET title=excluded.title,details=excluded.details,
        priority=MAX(goals.priority,excluded.priority),
        state=CASE WHEN goals.state IN ('in_progress','blocked','retry') OR (goals.state='resolved' AND goals.recurring=0)
          THEN goals.state ELSE 'queued' END,
        retry_at=CASE WHEN goals.state IN ('in_progress','blocked','retry') THEN goals.retry_at ELSE 0 END,
        updated_at=excluded.updated_at WHERE goals.state <> 'superseded'`, uuid(), dedupe_key, title, details, Math.max(1, Math.min(priority, 100)),
      source, recurring ? 1 : 0, interval_seconds, at, at);
      return query('SELECT * FROM goals WHERE dedupe_key=?', dedupe_key);
    });
  }
  // Import an already reviewed delivery batch into this owner. Unlike addGoal,
  // replay never rewrites a goal, raises its priority or changes its disposition.
  // A changed existing definition refuses the entire batch before any insertion.
  function importGoals(goals, { apply = false } = {}) {
    if (!Array.isArray(goals) || !goals.length || goals.length > 2048 || typeof apply !== 'boolean')
      throw new Error('invalid delivery batch');
    const batch = JSON.parse(encoded(goals, 'delivery batch', 16 * 1024 * 1024));
    const keys = new Set(), fields = ['title', 'details', 'priority', 'source', 'dedupe_key', 'recurring', 'interval_seconds'];
    for (const item of batch) {
      if (!object(item) || Object.keys(item).some(key => !fields.includes(key)))
        throw new Error('invalid delivery goal fields');
      string(item.title, 'goal title'); string(item.details, 'goal details', 262144, true);
      string(item.source, 'goal source'); string(item.dedupe_key, 'goal dedupe key');
      integer(item.priority, 'goal priority', 1, 100);
      if (item.recurring !== false || item.interval_seconds !== 0)
        throw new Error('delivery import does not schedule recurring work');
      if (keys.has(item.dedupe_key)) throw new Error('duplicate delivery goal identity');
      keys.add(item.dedupe_key);
    }
    return atomic(() => {
      const prior = batch.map(item => query('SELECT * FROM goals WHERE dedupe_key=?', item.dedupe_key));
      const conflicts = [];
      for (let i = 0; i < batch.length; i++) {
        const old = prior[i], item = batch[i];
        if (old && fields.some(key => old[key] !== (key === 'recurring' ? 0 : item[key])))
          conflicts.push(item.dedupe_key);
      }
      if (conflicts.length) throw new Error('Delivery definitions changed; review existing work before replacing it: ' + conflicts.join(', '));
      const at = clock(), rows = [];
      for (let i = 0; i < batch.length; i++) {
        const item = batch[i]; let old = prior[i];
        if (!old && apply) {
          write(`INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,
            interval_seconds,retry_at,attempts,last_error,created_at,updated_at)
            VALUES(?,?,?,?,?,'queued',?,0,0,0,0,'',?,?)`, uuid(), item.dedupe_key,
            item.title, item.details, item.priority, item.source, at, at);
          old = query('SELECT * FROM goals WHERE dedupe_key=?', item.dedupe_key);
        }
        rows.push({ dedupe_key: item.dedupe_key, id: old?.id ?? null, state: old?.state ?? 'not_imported',
          outcome: prior[i] ? 'unchanged' : apply ? 'created' : 'missing' });
      }
      return { applied: apply, created: rows.filter(row => row.outcome === 'created').length,
        unchanged: rows.filter(row => row.outcome === 'unchanged').length,
        missing: rows.filter(row => row.outcome === 'missing').length, goals: rows,
        cannot_infer: 'Queued delivery requirements are not executed operations, verified products, dispatch or effect authority.' };
    });
  }

  function seedGoals() {
    return atomic(() => SEEDS.map(([title, details, priority, dedupe_key, recurring, interval_seconds]) =>
      addGoal({ title, details, priority, dedupe_key, recurring, interval_seconds, source: 'seed' })));
  }
  function ingestEvents(events, { nextAfter } = {}) {
    if (!Array.isArray(events) || events.length > 1000) throw new Error('invalid event batch');
    // Bound and snapshot before opening the write transaction; no caller mutation survives.
    const batch = JSON.parse(encoded(events, 'event batch', 8 * 1024 * 1024));
    if (nextAfter !== undefined) integer(nextAfter, 'event cursor');
    return atomic(() => {
      let added = 0;
      for (const event of batch) {
        if (!object(event) || typeof event.kind !== 'string') throw new Error('invalid event');
        const payload = event.payload ?? {};
        if (!object(payload)) throw new Error('invalid event payload');
        let goal;
        if (event.kind === 'project.delta') {
          const hash = string(payload.status_hash ?? '', 'delta status hash', 4096, true);
          goal = { title: 'Review live project delta', details: `A live project delta arrived: ${encoded(payload, 'delta payload', 240000)}. Determine whether it affects active failures or operator work.`, priority: 45, dedupe_key: `delta:${hash}` };
        } else if (event.kind === 'mcp.call' && payload.state === 'failed') {
          const tool = string(payload.tool ?? 'unknown', 'event tool', 2048), id = string(payload.call_id ?? '', 'call id', 2048, true);
          goal = { title: `Resolve failed MCP tool: ${tool}`, details: `MCP call ${id} failed. Inspect call history, exact result, source, and runtime dependencies; repair if safely actionable.`, priority: 82, dedupe_key: `mcp-failure:${id || tool}` };
        } else if (event.kind === 'task.started' && payload.state === 'failed_to_start') {
          const id = string(payload.task_id ?? '', 'task id', 2048, true);
          goal = { title: 'Repair failed MCP task launch', details: `Task launch failed: ${encoded(payload, 'task payload', 240000)}`, priority: 80, dedupe_key: `task-launch:${id}` };
        }
        if (goal) { addGoal({ ...goal, source: 'event' }); added++; }
      }
      if (nextAfter !== undefined) {
        const previous = Number(metaGet('event_cursor', '0'));
        integer(previous, 'stored event cursor');
        metaSet('event_cursor', Math.max(previous, nextAfter));
      }
      return added;
    });
  }
  function budgetRow(at) {
    const day = dayAt(at);
    write('INSERT OR IGNORE INTO budgets(day) VALUES(?)', day);
    return query('SELECT * FROM budgets WHERE day=?', day);
  }
  function budgetOk(policy = {}, at = clock(), { continuingRunId = null, forModelResponse = false } = {}) {
    if (typeof forModelResponse !== 'boolean') throw new Error('invalid budget admission purpose');
    const limits = {
      input_tokens: integer(policy.daily_input_token_budget ?? 2000000, 'input token budget'),
      output_tokens: integer(policy.daily_output_token_budget ?? 500000, 'output token budget'),
      cycles: integer(policy.daily_cycle_budget ?? 48, 'daily cycle budget'),
      tool_calls: integer(policy.daily_tool_call_budget ?? 1000, 'daily tool call budget'),
    };
    const current = budgetRow(at);
    // An exact live run already spent its admission slot. Continuing it
    // consumes the remaining token/tool budgets without needing another slot.
    // Unknown or completed run IDs cannot relax new-cycle admission.
    const continued = continuingRunId === null ? false :
      (() => { const run = getRun(continuingRunId); return run?.state === 'running' && run.completed_at === null; })();
    // An exact running cycle may request its terminal model response when
    // earlier tools consumed the last tool slot. Model responses still need
    // token budget; tool dispatch and fresh-cycle claims never get this relief.
    const within = key => key === 'tool_calls' && continued && forModelResponse ?
      limits.tool_calls > 0 && current.tool_calls <= limits.tool_calls :
      key === 'cycles' && continued ? current.cycles <= limits.cycles : current[key] < limits[key];
    return { okay: Object.keys(limits).every(within), current, limits };
  }
  function incrementBudget(at, changes) {
    const current = budgetRow(at);
    for (const [key, value] of Object.entries(changes)) {
      integer(value, key); integer(current[key] + value, `${key} total`);
    }
    write(`UPDATE budgets SET cycles=cycles+?,tool_calls=tool_calls+?,input_tokens=input_tokens+?,
      output_tokens=output_tokens+? WHERE day=?`, changes.cycles ?? 0, changes.tool_calls ?? 0,
    changes.input_tokens ?? 0, changes.output_tokens ?? 0, current.day);
  }
  function historicalTerminalFailure(provider) {
    if (provider !== 'openai') return null;
    // Compaction may supersede the failed goal. Its terminal credit evidence
    // still refuses admission even if an unverified health row was inserted.
    return query(`SELECT updated_at FROM goals WHERE
      (last_error LIKE '%credit_balance_exhausted%' OR last_error LIKE '%insufficient_quota%'
       OR last_error LIKE '%billing_hard_limit%' OR last_error LIKE '%invalid_api_key%')
      ORDER BY updated_at DESC LIMIT 1`);
  }
  // A provider failure is not an authorization to spend another model call.
  // A cooldown expiry calls for a human-verified probe, never silent reenrollment.
  function providerStatus(provider = 'openai') {
    string(provider, 'provider', 80);
    const record = query('SELECT * FROM operator_provider_health WHERE provider=?', provider);
    // Neither a legacy unverified row nor a transient outage may hide
    // terminal quota evidence retained in a superseded goal.
    const prior = (!record || record.state === 'unverified' || record.retry_at > 0)
      ? historicalTerminalFailure(provider) : null;
    if (prior) return {provider,state:'quarantined',admitted:false,
      cause:'historical_terminal_quota',observed_at:prior.updated_at,retry_at:0};
    if (!record) return { provider, state: 'unverified', admitted: false };
    return { provider, state: record.retry_at > 0 && record.retry_at <= clock() ? 'recheck_required' : record.state,
      admitted: false, cause: record.cause, observed_at: record.observed_at, retry_at: record.retry_at };
  }
  function quarantineProvider(provider, cause, cooldownSeconds = 0) {
    string(provider, 'provider', 80); string(cause, 'provider cause', 80);
    integer(cooldownSeconds, 'provider cooldown', 0, 86400);
    if (!['credit_balance_exhausted','insufficient_quota','invalid_api_key','billing_hard_limit','rate_limited','connection_unavailable'].includes(cause))
      throw new Error('unrecognized provider failure classification');
    const at = clock();
    return atomic(() => {
      // A transient failure cannot replace the historical terminal refusal.
      if (['rate_limited', 'connection_unavailable'].includes(cause) &&
          historicalTerminalFailure(provider)) return providerStatus(provider);
      const previous = query('SELECT * FROM operator_provider_health WHERE provider=?', provider);
      // A transient outage never downgrades an already terminal quarantine.
      if (previous && previous.retry_at === 0 && previous.state === 'quarantined' &&
          ['rate_limited', 'connection_unavailable'].includes(cause))
        return providerStatus(provider);
      write(`INSERT INTO operator_provider_health(provider,state,cause,observed_at,retry_at) VALUES(?,'quarantined',?,?,?)
        ON CONFLICT(provider) DO UPDATE SET state='quarantined',cause=excluded.cause,
        observed_at=excluded.observed_at,retry_at=excluded.retry_at`, provider, cause, at,
      cause === 'rate_limited' || cause === 'connection_unavailable' ? at + Math.max(300,cooldownSeconds) : 0);
      return providerStatus(provider);
    });
  }
  function cyclesLastHour(at = clock()) {
    return query('SELECT COUNT(*) AS count FROM runs WHERE started_at>=?', at - 3600).count;
  }
  // The legacy loop retried seed goals 540 times past a cap of 6. A goal that
  // has used its attempts is not claimable again until a person requeues it.
  function dueGoal(at, goalId, cap = null) {
    return query(`SELECT g.* FROM goals g WHERE g.state IN ('queued','retry') AND g.retry_at<=?
      AND (? IS NULL OR g.id=?) AND (? IS NULL OR g.attempts<?)
      AND NOT EXISTS(SELECT 1 FROM runs r WHERE r.goal_id=g.id AND r.completed_at IS NULL)
      ORDER BY g.priority DESC,g.updated_at ASC,g.id ASC LIMIT 1`, at, goalId ?? null, goalId ?? null, cap, cap) ?? null;
  }
  const attemptCap = policy => policy ? integer(policy.max_goal_attempts ?? 5, 'max goal attempts', 1) : null;
  function selectGoal(policy) { return dueGoal(clock(), undefined, attemptCap(policy)); }
  function claimGoal({ goalId, policy } = {}) {
    if (goalId !== undefined) string(goalId, 'goal id');
    const at = clock();
    return atomic(() => {
      if (policy && (!budgetOk(policy, at).okay ||
          cyclesLastHour(at) >= integer(policy.max_cycles_per_hour ?? 6, 'hourly cycle budget'))) return null;
      const goal = dueGoal(at, goalId, attemptCap(policy));
      if (!goal) return null;
      const id = uuid();
      write("UPDATE goals SET state='in_progress',updated_at=? WHERE id=?", at, goal.id);
      write("INSERT INTO runs(id,goal_id,started_at,state) VALUES(?,?,?,'running')", id, goal.id, at);
      write('INSERT INTO operator_run_owners(run_id,pid,owner_host,owner_nonce) VALUES(?,?,?,?)',
        id, process.pid, ownerHost, ownerNonce);
      incrementBudget(at, { cycles: 1 });
      return { goal: getGoal(goal.id), run: getRun(id) };
    });
  }
  function getGoal(id) { string(id, 'goal id'); return query('SELECT * FROM goals WHERE id=?', id) ?? null; }
  function getRun(id) { string(id, 'run id'); return query('SELECT * FROM runs WHERE id=?', id) ?? null; }
  function liveRun(id) {
    const run = getRun(id);
    if (!run || run.completed_at !== null || run.state !== 'running') throw new Error('run is not active');
    return run;
  }
  function recordAction({ run_id, tool, args, state, result, started_at, policy = {} }) {
    string(tool, 'action tool'); string(state, 'action state', 128);
    const argsJson = encoded(args, 'action arguments'), resultJson = encoded(result, 'action result');
    const at = clock();
    if (!Number.isFinite(started_at) || started_at < 0 || started_at > at) throw new Error('invalid action start time');
    const duration = integer(Math.floor((at - started_at) * 1000), 'action duration');
    return atomic(() => {
      liveRun(run_id);
      // The budget check and durable intent share a single SQLite transaction.
      const limit = integer(policy.daily_tool_call_budget ?? 1000, 'daily tool call budget');
      if (budgetRow(at).tool_calls >= limit) throw new Error('daily operator tool-call budget exhausted');
      const inserted = write(`INSERT INTO actions(run_id,tool,args_json,state,result_json,started_at,duration_ms)
        VALUES(?,?,?,?,?,?,?)`, run_id, tool, argsJson, state, resultJson, started_at, duration);
      incrementBudget(at, { tool_calls: 1 });
      return integer(Number(inserted.lastInsertRowid), 'action sequence', 1);
    });
  }
  function completeAction({ seq, state, result }) {
    integer(seq, 'action sequence', 1); string(state, 'action state', 128);
    if (state === 'started') throw new Error('action completion needs a terminal state');
    const resultJson = encoded(result, 'action result'), at = clock();
    return atomic(() => {
      const action = query('SELECT run_id,state,started_at FROM actions WHERE seq=?', seq);
      if (!action || action.state !== 'started') throw new Error('action is not awaiting completion');
      liveRun(action.run_id);
      const duration = integer(Math.floor((at - action.started_at) * 1000), 'action duration');
      write('UPDATE actions SET state=?,result_json=?,duration_ms=? WHERE seq=? AND state=\'started\'',
        state, resultJson, duration, seq);
      return seq;
    });
  }
  function recordResponse({ run_id, response_id, usage = {} }) {
    string(response_id, 'response id');
    if (!object(usage)) throw new Error('invalid response usage');
    const raw = encoded(usage, 'response usage', 65536), at = clock();
    const input = integer(usage.input_tokens ?? 0, 'input tokens'), output = integer(usage.output_tokens ?? 0, 'output tokens');
    integer(input + output, 'total tokens');
    return atomic(() => {
      const existing = query('SELECT * FROM operator_response_usage WHERE response_id=?', response_id);
      if (existing) {
        if (existing.run_id !== run_id || existing.usage_json !== raw) throw new Error('response id has conflicting accounting');
        return { recorded: false, usage: JSON.parse(getRun(run_id).usage_json) };
      }
      const run = liveRun(run_id), previous = JSON.parse(run.usage_json);
      const total = {
        input_tokens: integer((previous.input_tokens ?? 0) + input, 'run input tokens'),
        output_tokens: integer((previous.output_tokens ?? 0) + output, 'run output tokens'),
        total_tokens: integer((previous.input_tokens ?? 0) + input + (previous.output_tokens ?? 0) + output, 'run total tokens'),
        response_count: integer((previous.response_count ?? 0) + 1, 'response count'),
      };
      write('INSERT INTO operator_response_usage(response_id,run_id,usage_json,recorded_at) VALUES(?,?,?,?)', response_id, run_id, raw, at);
      incrementBudget(at, { input_tokens: input, output_tokens: output });
      write('UPDATE runs SET response_id=?,usage_json=? WHERE id=?', response_id, JSON.stringify(total), run_id);
      return { recorded: true, usage: total };
    });
  }
  function finishGoal({ run_id, disposition = 'retry', summary = '', retry_after_seconds = 900, policy = {} }) {
    if (!['resolved', 'retry', 'deferred', 'blocked', 'failed'].includes(disposition)) throw new Error('invalid disposition');
    string(summary, 'run summary', 400000, true); summary = summary.slice(0, 100000);
    integer(retry_after_seconds, 'retry delay', 0, 315360000);
    const maximum = integer(policy.max_goal_attempts ?? 5, 'max goal attempts', 1), at = clock();
    return atomic(() => {
      const run = liveRun(run_id), goal = getGoal(run.goal_id);
      if (!goal || goal.state !== 'in_progress') throw new Error('goal is no longer owned by this run');
      const uncertain = query("SELECT 1 AS present FROM actions WHERE run_id=? AND state IN ('started','outcome_uncertain') LIMIT 1", run_id);
      const finalSummary = uncertain ? `Action outcome requires reconciliation; automatic retry/resolution refused.\n${summary}`.slice(0, 100000) : summary;
      let state = 'retry', retryAt = 0;
      if (uncertain) state = 'blocked';
      else if (disposition === 'resolved') {
        if (goal.recurring) retryAt = at + Math.max(goal.interval_seconds, 60);
        else state = 'resolved';
      } else if (terminalCause(summary) || goal.attempts + 1 >= maximum || disposition === 'blocked') state = 'blocked';
      else retryAt = at + Math.max(retry_after_seconds, ['retry', 'deferred'].includes(disposition) ? 300 : 1800);
      write('UPDATE goals SET state=?,retry_at=?,attempts=attempts+1,last_error=?,updated_at=? WHERE id=?',
        state, retryAt, uncertain ? finalSummary.slice(0, 4000) : disposition === 'resolved' ? '' : summary.slice(-4000), at, goal.id);
      write('UPDATE runs SET completed_at=?,state=?,summary=? WHERE id=?', at, uncertain ? 'blocked' : disposition, finalSummary, run_id);
      return { goal: getGoal(goal.id), run: getRun(run_id) };
    });
  }
  function failRun({ run_id, error, policy = {}, provider_failure }) {
    return atomic(() => {
      if (provider_failure !== undefined &&
          (!object(provider_failure) || !PROVIDERS.includes(provider_failure.provider)))
        throw new Error('provider quarantine requires a known transport origin');
      const prior = providerStatus('openai');
      // Untrusted tool or goal text must not manufacture a historical 429.
      // Existing legacy terminal failures retain their existing quarantine.
      if (provider_failure === undefined && prior.state === 'unverified')
        write("INSERT OR IGNORE INTO operator_provider_health(provider,state,cause,observed_at,retry_at) VALUES('openai','unverified','no_carrier_failure',?,0)", clock());
      const result = finishGoal({ run_id, disposition: 'retry', summary: String(error), retry_after_seconds: 900, policy });
      if (provider_failure !== undefined)
        quarantineProvider(provider_failure.provider, provider_failure.cause, provider_failure.cooldown_seconds ?? 0);
      write("UPDATE runs SET state='failed' WHERE id=?", run_id);
      return { goal: result.goal, run: getRun(run_id) };
    });
  }
  function reconcileOrphans(policy = {}) {
    const cycle = integer(policy.cycle_timeout_seconds ?? 2700, 'cycle timeout', 1, 31536000);
    const horizon = Math.max(4 * cycle, 7200), at = clock();
    return atomic(() => {
      const goals = db().query(`SELECT g.id FROM goals g WHERE g.state='in_progress' AND NOT EXISTS (
        SELECT 1 FROM runs r WHERE r.goal_id=g.id AND r.completed_at IS NULL AND r.started_at>?)
        ORDER BY g.updated_at ASC,g.id ASC LIMIT 1001`).all(at - horizon);
      const report = { reclaimed: 0, blocked: 0, held_unknown: 0, held_live: 0, has_more: goals.length > 1000 };
      for (const goal of goals.slice(0, 1000)) {
        const owners = db().query(`SELECT o.pid,o.owner_host,o.owner_nonce FROM runs r
          LEFT JOIN operator_run_owners o ON o.run_id=r.id
          WHERE r.goal_id=? AND r.completed_at IS NULL LIMIT 101`).all(goal.id);
        if (!owners.length || owners.length > 100 || owners.some(owner => owner.owner_host !== ownerHost ||
            !Number.isSafeInteger(owner.pid) || owner.pid <= 0 || typeof owner.owner_nonce !== 'string')) {
          report.held_unknown++; continue;
        }
        let unknown = false, live = false;
        for (const owner of owners) {
          try { process.kill(owner.pid, 0); live = true; }
          catch (error) { if (error.code !== 'ESRCH') unknown = true; }
        }
        if (unknown) { report.held_unknown++; continue; }
        if (live) { report.held_live++; continue; }
        const attempted = query(`SELECT 1 AS present FROM actions a JOIN runs r ON r.id=a.run_id
          WHERE r.goal_id=? AND r.completed_at IS NULL LIMIT 1`, goal.id);
        const message = attempted ? 'Local executor absent; attempted actions require reconciliation, not automatic replay.' :
          `Local executor absent (ESRCH); stale run reclaimed after ${horizon}s.`;
        write("UPDATE goals SET state=?,retry_at=?,attempts=attempts+1,last_error=?,updated_at=? WHERE id=? AND state='in_progress'",
          attempted ? 'blocked' : 'retry', attempted ? 0 : at, message, at, goal.id);
        write("UPDATE runs SET completed_at=?,state='failed',summary=? WHERE goal_id=? AND completed_at IS NULL", at, message, goal.id);
        if (attempted) report.blocked++; else report.reclaimed++;
      }
      return report;
    });
  }
  function close() { if (connection) connection.close(); connection = null; closed = true; }
  return Object.freeze({ path, metaGet, metaSet, addGoal, importGoals, seedGoals, ingestEvents, budgetOk, cyclesLastHour,
    selectGoal, claimGoal, getGoal, getRun, recordAction, completeAction, recordResponse, finishGoal, failRun, reconcileOrphans,
    providerStatus, quarantineProvider, close });
}
