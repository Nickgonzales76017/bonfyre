#!/usr/bin/env bun
// The membrane stores publisher-carrier observations and claimed crawler demand.
import { readFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Database } from 'bun:sqlite';
import { discoverForeign } from './foreign-discover.mjs';
import { analyzeReadership } from './machine-readership.mjs';

let cachedPolicy;
export function membranePolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('./machine-membrane.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'machineMembrane']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const defaultDb = () => process.env.BONFYRE_MEMBRANE_DB ?? '/var/lib/bonfyre/state/membrane.db';
export function membraneDatabase(path = defaultDb()) {
  mkdirSync(dirname(path), { recursive: true });
  const db = new Database(path, { create: true });
  db.exec(`PRAGMA busy_timeout=15000;
    CREATE TABLE IF NOT EXISTS source_passport(
      root TEXT PRIMARY KEY,best_carrier TEXT,why TEXT,rung INTEGER,
      robots_status INTEGER,robots_allows INTEGER,sitemaps INTEGER DEFAULT 0,
      feeds INTEGER DEFAULT 0,hubs INTEGER DEFAULT 0,structured INTEGER DEFAULT 0,
      conditional INTEGER DEFAULT 0,descriptors INTEGER DEFAULT 0,declined_reason TEXT,
      first_seen TEXT,last_probed TEXT,probes INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS source_protocol_descriptor(
      root TEXT NOT NULL,family TEXT NOT NULL,descriptor TEXT NOT NULL,
      truth_level TEXT NOT NULL CHECK(truth_level='declared_descriptor'),
      active INTEGER NOT NULL DEFAULT 1,first_seen TEXT NOT NULL,last_probed TEXT NOT NULL,
      PRIMARY KEY(root,family,descriptor));
    CREATE INDEX IF NOT EXISTS source_protocol_descriptor_active
      ON source_protocol_descriptor(active,family,root);
    CREATE TABLE IF NOT EXISTS machine_reader(
      agent TEXT PRIMARY KEY,family TEXT,requests INTEGER DEFAULT 0,
      answered_200 INTEGER DEFAULT 0,answered_404 INTEGER DEFAULT 0,
      top_path TEXT,last_seen TEXT);`);
  const columns = db.query('PRAGMA table_info(source_passport)').all().map(row => row.name);
  if (!columns.includes('descriptors')) db.exec('ALTER TABLE source_passport ADD COLUMN descriptors INTEGER DEFAULT 0');
  return db;
}

const descriptorRows = (passport, policy) => (passport.protocol_descriptors ?? []).filter(row =>
  row.truth_level === policy.descriptor_truth && row.family && row.descriptor);

export function retainPassport(db, passport, policy = membranePolicy()) {
  if (!passport.root) throw new Error('passport root required');
  const carrier = passport.best_carrier ?? 'html-extraction';
  const position = policy.carrier_ladder.indexOf(carrier);
  const rung = position >= 0 ? position : policy.carrier_ladder.length - 1;
  const declined = !passport.robots_allows_us ? policy.robots_decline
    : carrier === 'html-extraction' ? policy.expensive_decline : null;
  const descriptors = descriptorRows(passport, policy);
  db.transaction(() => {
    db.query(`INSERT INTO source_passport(root,best_carrier,why,rung,robots_status,
      robots_allows,sitemaps,feeds,hubs,structured,conditional,descriptors,
      declined_reason,first_seen,last_probed,probes)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'),1)
      ON CONFLICT(root) DO UPDATE SET
      best_carrier=excluded.best_carrier,why=excluded.why,rung=excluded.rung,
      robots_status=excluded.robots_status,robots_allows=excluded.robots_allows,
      sitemaps=excluded.sitemaps,feeds=excluded.feeds,hubs=excluded.hubs,
      structured=excluded.structured,conditional=excluded.conditional,
      descriptors=excluded.descriptors,declined_reason=excluded.declined_reason,
      last_probed=datetime('now'),probes=source_passport.probes+1`).run(
      passport.root, carrier, passport.why ?? '', rung, passport.robots_status ?? 0,
      Number(Boolean(passport.robots_allows_us)), passport.sitemaps?.length ?? 0,
      passport.feeds?.length ?? 0, passport.websub_hubs?.length ?? 0,
      passport.structured_types?.length ?? 0, Number(Boolean(Object.keys(passport.conditional ?? {}).length)),
      descriptors.length, declined);
    db.query('UPDATE source_protocol_descriptor SET active=0 WHERE root=?').run(passport.root);
    const put = db.query(`INSERT INTO source_protocol_descriptor(
      root,family,descriptor,truth_level,active,first_seen,last_probed)
      VALUES(?,?,?,'declared_descriptor',1,datetime('now'),datetime('now'))
      ON CONFLICT(root,family,descriptor) DO UPDATE SET active=1,last_probed=datetime('now')`);
    for (const descriptor of descriptors) put.run(passport.root, descriptor.family, descriptor.descriptor);
  })();
  return { root: passport.root, carrier, rung, declined };
}

export async function refreshMembrane(hosts, options = {}) {
  const db = membraneDatabase(options.dbPath);
  const discover = options.discover ?? discoverForeign;
  const rows = [];
  try {
    for (const host of hosts) {
      try { rows.push(retainPassport(db, await discover(host), options.policy)); }
      catch (error) { rows.push({ root: host, error: `probe produced no passport: ${error.message}` }); }
    }
  } finally { db.close(); }
  return rows;
}

export function retainReadership(db, content) {
  const result = analyzeReadership([content]);
  const put = db.query(`INSERT INTO machine_reader(agent,family,requests,answered_200,
    answered_404,top_path,last_seen) VALUES(?,?,?,?,?,?,datetime('now'))
    ON CONFLICT(agent) DO UPDATE SET family=excluded.family,requests=excluded.requests,
    answered_200=excluded.answered_200,answered_404=excluded.answered_404,
    top_path=excluded.top_path,last_seen=datetime('now')`);
  db.transaction(() => {
    for (const row of result.by_agent) put.run(row.agent, row.family, row.count,
      row.statuses.find(status => status.status === '200')?.count ?? 0,
      row.statuses.find(status => status.status === '404')?.count ?? 0, row.top_path);
  })();
  return { agents: result.by_agent.length, machine_requests: result.declared_machine_requests };
}

export function membraneReadership(path, options = {}) {
  const db = membraneDatabase(options.dbPath);
  try { return retainReadership(db, readFileSync(path, 'utf8')); }
  finally { db.close(); }
}

export function membraneSources(path = defaultDb()) {
  const db = membraneDatabase(path);
  try { return db.query(`SELECT root,best_carrier,rung,declined_reason,last_probed
    FROM source_passport ORDER BY rung DESC`).all(); }
  finally { db.close(); }
}

const descriptorQuery = `SELECT root,family,descriptor,truth_level,active,first_seen,last_probed
  FROM source_protocol_descriptor ORDER BY active DESC,family,root`;
export function membraneDescriptors(path = defaultDb(), policy = membranePolicy()) {
  const db = membraneDatabase(path);
  try { return { descriptors: db.query(descriptorQuery).all(),
    forbidden_inference: policy.descriptor_forbidden_inference }; }
  finally { db.close(); }
}

export function membraneSummary(path = defaultDb(), policy = membranePolicy()) {
  const db = membraneDatabase(path);
  try {
    const sources = db.query('SELECT * FROM source_passport ORDER BY rung DESC,root').all();
    const readers = db.query('SELECT * FROM machine_reader ORDER BY requests DESC LIMIT 20').all();
    return { inbound: { sources: sources.length,
      readable_cheaply: sources.filter(row => row.rung <= policy.carrier_ladder.indexOf('sitemap')).length,
      declining_or_expensive: sources.filter(row => row.declined_reason).length,
      passports: sources, declared_descriptor_candidates: db.query(descriptorQuery).all() },
    outbound: { declared_agents: readers.length,
      answered: readers.reduce((sum, row) => sum + row.answered_200, 0),
      missed: readers.reduce((sum, row) => sum + row.answered_404, 0), readers },
    forbidden_inference: policy.source_forbidden_inference };
  } finally { db.close(); }
}

if (import.meta.main) {
  const [command, ...args] = process.argv.slice(2);
  try {
    let result;
    if (command === 'refresh' && args.length) result = await refreshMembrane(args);
    else if (command === 'readership') result = membraneReadership(args[0] ?? '/var/log/nginx/access.log');
    else if (command === 'sources') result = membraneSources();
    else if (command === 'descriptors') result = membraneDescriptors();
    else if (command === 'summary') result = membraneSummary();
    else throw new Error('usage: membrane-store.mjs refresh <host>... | readership [log] | sources | descriptors | summary');
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } catch (error) { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }
}
