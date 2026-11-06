// The broker's narrow estate owner: legacy status and mission admission only.
// Pause/resume/cycle/census/compile remain outside this module and are not
// silently routed through the retired Python launcher.
import { Database } from 'bun:sqlite';
import { randomUUID } from 'node:crypto';
import { existsSync, lstatSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';

function existingDatabase(path) {
  path = resolve(path);
  if (!existsSync(path) || !lstatSync(path).isFile() || lstatSync(path).isSymbolicLink())
    throw new Error('estate database is missing or not a regular file');
  return path;
}
function value(input, name, { empty = false, maxBytes = 16_384 } = {}) {
  if (typeof input !== 'string' || !empty && !input || input.includes('\0') || Buffer.byteLength(input) > maxBytes)
    throw new Error(`invalid ${name}`);
  return input;
}
function locksJson(text) {
  // Python's json.dumps(list) uses ", " separators; retain the on-disk shape.
  return `[${text.split(',').map(item => JSON.stringify(item)).join(', ')}]`;
}

export function createFabricEstate({ home = homedir(), worldDbPath = join(home, '.bonfyre/estate-compiler/estate.sqlite3'),
  operatorDbPath = join(home, '.bonfyre/operator/operator.sqlite3'), now = () => Date.now() / 1000,
  randomId = () => randomUUID().replaceAll('-', '') } = {}) {
  if (typeof now !== 'function' || typeof randomId !== 'function') throw new Error('invalid estate dependencies');
  function status() {
    let world, operator;
    try {
      world = new Database(existingDatabase(worldDbPath), { readonly: true, strict: true });
      operator = new Database(existingDatabase(operatorDbPath), { readonly: true, strict: true });
      return {
        paused: existsSync(join(home, '.bonfyre/estate-compiler/STOP')),
        world: { entities: Number(world.query('SELECT COUNT(*) AS n FROM entities').get().n),
          signals: world.query('SELECT state,COUNT(*) AS count FROM signals GROUP BY state').all(),
          missions: world.query('SELECT state,COUNT(*) AS count FROM missions GROUP BY state').all(),
          locks: world.query('SELECT * FROM locks ORDER BY acquired_at').all() },
        top_missions: world.query('SELECT id,family,title,priority,state,operator_goal_id,last_error FROM missions ORDER BY priority DESC,updated_at DESC LIMIT 30').all(),
        operator: { goals: operator.query('SELECT state,COUNT(*) AS count FROM goals GROUP BY state').all(),
          running: operator.query("SELECT id,title,priority,state,source,updated_at FROM goals WHERE state='in_progress' ORDER BY priority DESC").all() },
      };
    } finally { world?.close(); operator?.close(); }
  }
  function add({ title, family = 'manual', details = '', priority = 90, locks = 'general', dedupe = '' } = {}) {
    value(title, 'mission title'); value(family, 'mission family'); value(details, 'mission details', { empty: true, maxBytes: 1_000_000 });
    value(locks, 'mission locks', { empty: true }); value(dedupe, 'mission dedupe', { empty: true });
    if (!Number.isSafeInteger(priority) || priority < 1 || priority > 100) throw new Error('invalid mission priority');
    const id = randomId(), at = now();
    if (!/^[a-f0-9]{32}$/.test(id) || !Number.isFinite(at) || at < 0) throw new Error('invalid mission identity or time');
    const key = dedupe || `manual:${randomId()}`;
    if (Buffer.byteLength(key) > 16_384) throw new Error('invalid mission dedupe');
    let world;
    try {
      world = new Database(existingDatabase(worldDbPath), { create: false, strict: true });
      world.exec('PRAGMA busy_timeout=30000');
      const transact = world.transaction(() => {
        world.query(`INSERT INTO missions(
          id,dedupe_key,family,title,details,priority,state,lock_set_json,
          dependency_ids_json,source_signal_ids_json,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,'compiled',?,?,?,?,?)
        ON CONFLICT(dedupe_key) DO UPDATE SET
          title=excluded.title,details=excluded.details,
          priority=MAX(missions.priority,excluded.priority),state='compiled',updated_at=excluded.updated_at`)
          .run(id, key, family, title, details, priority, locksJson(locks), '[]', '[]', at, at);
        const saved = world.query('SELECT id,dedupe_key FROM missions WHERE dedupe_key=?').get(key);
        if (!saved) throw new Error('mission persistence unverified');
        return { mission_id: saved.id, dedupe_key: saved.dedupe_key };
      });
      return transact.immediate();
    } finally { world?.close(); }
  }
  return { status, add };
}
