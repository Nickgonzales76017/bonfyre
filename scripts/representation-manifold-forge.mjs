#!/usr/bin/env bun
// Demand-driven physical projection; representation records never own semantics.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readFileSync, renameSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { bodyDigest } from "./columnar_body.mjs";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const hash = value => createHash("sha256").update(value).digest("hex");
const sqlLiteral = value => `'${String(value).replaceAll("'", "''")}'`;
const sqlIdentifier = value => `"${String(value).replaceAll('"', '""')}"`;
const timestamp = () => new Date().toISOString();

export function loadPolicy(cuePath = join(scriptDir, "representation-forge.cue")) {
  const result = Bun.spawnSync(["cue", "export", cuePath, "-e", "graphProjection", "--out", "json"], { stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(`graph policy unavailable: ${result.stderr.toString().trim()}`);
  return JSON.parse(result.stdout.toString());
}

const graphPolicy = value => value && typeof value === "object" ? value : loadPolicy(value);

function activeKinds(estate) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing representation manifold: ${path}`);
  const db = new Database(path, { readonly: true, create: false });
  try {
    return db.query(`SELECT DISTINCT ri.schema_ref kind FROM representation_instance ri
      JOIN physical_realization pr ON pr.representation_id=ri.id
      WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1
      ORDER BY ri.schema_ref`).all().map(row => row.kind);
  } finally { db.close(); }
}

function duck(sql) {
  const result = Bun.spawnSync(["duckdb", "-json", ":memory:", sql], { stdout: "pipe", stderr: "pipe", maxBuffer: 64 * 1024 * 1024 });
  if (result.exitCode !== 0) throw new Error(`DuckDB: ${result.stderr.toString().trim() || result.stdout.toString().trim()}`);
  try { return JSON.parse(result.stdout.toString()); }
  catch { throw new Error(`DuckDB returned invalid JSON: ${result.stdout.toString().slice(0, 300)}`); }
}

function duckExec(sql) {
  const result = Bun.spawnSync(["duckdb", "-batch", ":memory:", sql], { stdout: "pipe", stderr: "pipe", maxBuffer: 64 * 1024 * 1024 });
  if (result.exitCode !== 0) throw new Error(`DuckDB: ${result.stderr.toString().trim() || result.stdout.toString().trim()}`);
}

function ensureSchema(db) {
  db.exec(readFileSync(join(scriptDir, "..", "schemas", "storage", "representation-manifold.sql"), "utf8"));
}

function rootRows(db, kind) {
  return db.query(`SELECT ri.id parent, ri.schema_ref kind, ri.digest digest, ri.frontier frontier,
    pr.coordinate coordinate, pr.digest physical_digest
    FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
    WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1 AND ri.schema_ref=?
    ORDER BY ri.created_at DESC, ri.id DESC`).all(kind);
}

async function verifiedRoot(db, kind) {
  const roots = rootRows(db, kind);
  if (!roots.length) throw new Error(`no available physical Parquet root for ${kind}`);
  if (roots.length !== 1) throw new Error(`ambiguous active Parquet roots for ${kind}`);
  const row = roots[0];
  try {
    const stat = lstatSync(row.coordinate);
    if (stat.isFile() && !stat.isSymbolicLink()) {
      const actual = await bodyDigest(row.coordinate);
      if (actual === row.digest && actual === row.physical_digest) return row;
    }
  } catch { /* Refuse a missing or unreadable canonical body. */ }
  throw new Error(`no digest-valid Parquet root for ${kind}`);
}

function candidatePairs(schema, policy) {
  const names = new Map(schema.map(row => [row.column_name.toLowerCase(), row.column_name]));
  return policy.directedPairs
    .map(pair => [names.get(pair.source), names.get(pair.target)])
    .filter(([source, target]) => source && target && source !== target);
}

function schemaFor(path) {
  return duck(`SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM read_parquet(${sqlLiteral(path)}))`);
}

function virtualSql(root, kind, source, target) {
  const src = sqlIdentifier(source), dst = sqlIdentifier(target);
  return `SELECT ${sqlLiteral(kind)}::VARCHAR semantic_kind, ${sqlLiteral(source)}::VARCHAR source_column,
    CAST(${src} AS VARCHAR) src, ${sqlLiteral(target)}::VARCHAR target_column,
    CAST(${dst} AS VARCHAR) dst, ${sqlLiteral(`${source}->${target}`)}::VARCHAR relation
    FROM read_parquet(${sqlLiteral(root.coordinate)})
    WHERE ${src} IS NOT NULL AND ${dst} IS NOT NULL`;
}

export async function registerVirtualGraph(estate, kind, cuePath) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing representation manifold: ${path}`);
  const db = new Database(path);
  try {
    const root = await verifiedRoot(db, kind);
    const policy = graphPolicy(cuePath);
    const pairs = candidatePairs(schemaFor(root.coordinate), policy);
    if (await bodyDigest(root.coordinate) !== root.digest) throw new Error(`Parquet root changed during virtual graph planning: ${kind}`);
    ensureSchema(db);
    const compiler = `bonfyre-virtual-graph.v1+duckdb+policy:${hash(JSON.stringify(policy))}`;
    const programs = pairs.map(([source, target]) => {
      const sql = virtualSql(root, kind, source, target);
      const proof = hash(`${compiler}|${root.parent}|${root.digest}|${root.frontier}|${sql}`);
      return { source, target, sql, proof, representation_id: hash(`graph-virtual|${proof}`) };
    });
    const alreadyRegistered = programs.every(program => {
      const row = db.query(`SELECT proof_digest,state FROM virtual_graph_projection_v2
        WHERE semantic_kind=? AND source_column=? AND target_column=?`).get(kind, program.source, program.target);
      const receipt = db.query(`SELECT proof_state FROM transformation_occurrence WHERE id=?`).get(program.proof);
      const instance = db.query("SELECT state FROM representation_instance WHERE id=?").get(program.representation_id);
      return row?.proof_digest === program.proof && row.state === "executable" &&
        receipt?.proof_state === "planned" && instance?.state === "executable";
    });
    if (alreadyRegistered) return { kind, input_digest: root.digest, programs: programs.map(({ source, target, proof, representation_id }) =>
      ({ source, target, proof_digest: proof, representation_id, state: "verified_existing" })) };
    const now = timestamp();
    db.transaction(() => {
      for (const program of programs) {
        const prior = db.query(`SELECT source_representation_id,source_digest,proof_digest FROM virtual_graph_projection_v2
          WHERE semantic_kind=? AND source_column=? AND target_column=?`).get(kind, program.source, program.target);
        if (prior && prior.proof_digest !== program.proof) {
          db.query("UPDATE representation_instance SET state='superseded' WHERE id=?").run(hash(`graph-virtual|${prior.proof_digest}`));
        }
        db.query(`INSERT OR REPLACE INTO virtual_graph_projection_v2 VALUES(?,?,?,?,?,?,?,?,?,?,?)`).run(
          kind, program.source, program.target, root.parent, root.coordinate, root.digest,
          root.frontier, program.sql, program.proof, "executable", now);
        db.query(`INSERT OR REPLACE INTO representation_instance
          (id,subject_id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state)
          VALUES(?,?,?,?,?,?,?,?,?,?)`).run(program.representation_id, null, "graph-virtual", "derived",
          root.frontier, program.proof, kind, now, 1, "executable");
        db.query(`INSERT OR IGNORE INTO representation_edge
          (parent_representation_id,child_representation_id,transform,proof_state,created_at)
          VALUES(?,?,?,?,?)`).run(root.parent, program.representation_id,
          `parquet->graph-virtual:${program.source}->${program.target}`, "planned", now);
        db.query(`INSERT OR REPLACE INTO transformation_occurrence
          (id,transform,compiler,input_ref,output_ref,input_digest,output_digest,occurred_at,proof_state)
          VALUES(?,?,?,?,?,?,?,?,?)`).run(program.proof,
          `parquet->graph-virtual:${program.source}->${program.target}`, compiler,
          root.coordinate, `duckdb-sql:${program.representation_id}`, root.digest, program.proof, now, "planned");
      }
    })();
    return { kind, input_digest: root.digest, programs: programs.map(({ source, target, proof, representation_id }) =>
      ({ source, target, proof_digest: proof, representation_id, state: "executable_candidate_only" })) };
  } finally { db.close(); }
}

export async function queryVirtualGraph(estate, kind, source, target, limit = 100) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10000) throw new Error("limit must be 1..10000");
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing representation manifold: ${path}`);
  const db = new Database(path, { readonly: true, create: false });
  try {
    const row = db.query(`SELECT * FROM virtual_graph_projection_v2
      WHERE semantic_kind=? AND source_column=? AND target_column=? AND state='executable'`).get(kind, source, target);
    if (!row) throw new Error(`no virtual graph program for ${kind}:${source}->${target}`);
    const root = await verifiedRoot(db, kind);
    if (root.parent !== row.source_representation_id || root.digest !== row.source_digest ||
        root.frontier !== row.frontier || root.coordinate !== row.source_coordinate)
      throw new Error(`virtual graph source frontier changed: ${kind}:${source}->${target}`);
    const sql = virtualSql(root, kind, source, target);
    if (sql !== row.sql_text || !/^[0-9a-f]{64}$/.test(row.proof_digest))
      throw new Error(`virtual graph program differs from registered proof: ${kind}:${source}->${target}`);
    const receipt = db.query(`SELECT compiler FROM transformation_occurrence WHERE id=? AND input_digest=? AND proof_state='planned'`).get(
      row.proof_digest, root.digest);
    if (!receipt || hash(`${receipt.compiler}|${root.parent}|${root.digest}|${root.frontier}|${sql}`) !== row.proof_digest)
      throw new Error(`virtual graph proof mismatch: ${kind}:${source}->${target}`);
    const rows = duck(`SELECT * FROM (${sql}) ORDER BY src,dst LIMIT ${limit}`);
    if (await bodyDigest(root.coordinate) !== root.digest)
      throw new Error(`Parquet root changed during virtual graph query: ${kind}`);
    return { kind, source, target, candidate_only: true, input_digest: root.digest, rows };
  } finally { db.close(); }
}

export async function plan(estate, kind, cuePath) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing representation manifold: ${path}`);
  const db = new Database(path, { readonly: true, create: false });
  try {
    const root = await verifiedRoot(db, kind);
    const policy = graphPolicy(cuePath);
    const pairs = candidatePairs(schemaFor(root.coordinate), policy);
    return { kind, source: root.coordinate, input_digest: root.digest, frontier: root.frontier,
      pairs: pairs.map(([source, target]) => ({ source, target, state: "candidate_only" })), policy: policy.schema };
  } finally { db.close(); }
}

// The historical Python worker eagerly materialized guessed graph edges for
// every source. This pass preflights all roots, then registers only CUE-authored
// directed candidate pairs without multiplying graph bytes.
export async function planAllGraphs(estate, cuePath) {
  const policy = graphPolicy(cuePath);
  const kinds = activeKinds(estate);
  const plans = [];
  for (const kind of kinds) plans.push(await plan(estate, kind, policy));
  return { roots: kinds.length,
    candidates: plans.filter(item => item.pairs.length),
    without_authored_pair: plans.filter(item => !item.pairs.length).map(item => item.kind) };
}

export async function registerAllVirtualGraphs(estate, cuePath) {
  const policy = loadPolicy(cuePath);
  const planned = await planAllGraphs(estate, policy);
  const registered = [];
  for (const item of planned.candidates) registered.push(await registerVirtualGraph(estate, item.kind, policy));
  return { roots: planned.roots, candidate_roots: planned.candidates.length,
    programs: registered.reduce((count, item) => count + item.programs.length, 0),
    registered, without_authored_pair: planned.without_authored_pair };
}

function record(db, root, source, target, output, digest, rowCount, started, compiler) {
  const kind = "graph-edge-parquet", now = timestamp();
  const id = hash(`${kind}|${root.kind}|${root.frontier}|${digest}|${output}`);
  const transform = `parquet->${source}->${target}`;
  db.transaction(() => {
    db.query(`INSERT OR REPLACE INTO representation_instance
      (id,subject_id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state)
      VALUES(?,?,?,?,?,?,?,?,?,?)`).run(id, null, kind, "derived", root.frontier, digest, root.kind, now, 1, "physical");
    db.query(`INSERT OR REPLACE INTO physical_realization
      (id,representation_id,carrier,coordinate,bytes,digest,temperature,available,checked_at)
      VALUES(?,?,?,?,?,?,?,?,?)`).run(hash(`local|${output}`), id, "local", output, lstatSync(output).size, digest, "warm", 1, now);
    db.query(`INSERT OR IGNORE INTO representation_edge
      (parent_representation_id,child_representation_id,transform,proof_state,created_at)
      VALUES(?,?,?,?,?)`).run(root.parent, id, transform, "materialized", now);
    db.query(`INSERT OR REPLACE INTO graph_projection VALUES(?,?,?,?,?,?,?)`).run(root.kind, source, target, output, rowCount, digest, now);
    db.query(`INSERT OR REPLACE INTO transformation_occurrence
      (id,transform,compiler,input_ref,output_ref,input_digest,output_digest,occurred_at,proof_state)
      VALUES(?,?,?,?,?,?,?,?,?)`).run(hash(`forge|${compiler}|${transform}|${root.digest}|${digest}`), transform,
      compiler, root.coordinate, output, root.digest, digest, now, "materialized");
    db.query(`INSERT OR REPLACE INTO forge_event VALUES(?,?,?,?,?,?,?)`).run(hash(`graph|${root.kind}|${source}|${target}|${started}`),
      "graph", `${root.kind}:${source}->${target}`, "physical", `${rowCount} candidate rows; not a semantic relation`, started, now);
  })();
  return { kind: root.kind, source, target, coordinate: output, digest, rows: rowCount, representation_id: id };
}

export async function materializeGraph(estate, kind, cuePath) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing representation manifold: ${path}`);
  const db = new Database(path);
  const created = [];
  try {
    const root = await verifiedRoot(db, kind);
    const policy = graphPolicy(cuePath);
    const pairs = candidatePairs(schemaFor(root.coordinate), policy);
    if (!new Set(["ZSTD", "SNAPPY", "GZIP", "UNCOMPRESSED"]).has(policy.batchCompression))
      throw new Error(`unsupported graph compression: ${policy.batchCompression}`);
    const compiler = `bonfyre-representation-forge.v1+duckdb+policy:${hash(JSON.stringify(policy))}`;
    ensureSchema(db);
    const directory = join(resolve(estate), "representations", "graph", "edges");
    for (const [source, target] of pairs) {
      const started = timestamp();
      const stem = hash(`${kind}|${source}|${target}|${root.digest}`);
      const prior = db.query(`SELECT coordinate,digest FROM graph_projection WHERE semantic_kind=? AND source_column=? AND target_column=?`).get(kind, source, target);
      const priorReceipt = prior && db.query(`SELECT 1 present FROM transformation_occurrence
        WHERE output_ref=? AND output_digest=? AND input_digest=? AND compiler=? AND proof_state='materialized' LIMIT 1`).get(
        prior.coordinate, prior.digest, root.digest, compiler);
      if (priorReceipt && existsSync(prior.coordinate) && prior.digest === await bodyDigest(prior.coordinate)) {
        created.push({ kind, source, target, coordinate: prior.coordinate, digest: prior.digest, state: "verified_existing" });
        continue;
      }
      mkdirSync(directory, { recursive: true });
      const staging = join(directory, `.${stem}.${process.pid}.partial`);
      try {
        const from = `read_parquet(${sqlLiteral(root.coordinate)})`, sourceName = sqlIdentifier(source), targetName = sqlIdentifier(target);
        const projection = `SELECT ${sqlLiteral(kind)}::VARCHAR semantic_kind, ${sqlLiteral(source)}::VARCHAR source_column,
          CAST(${sourceName} AS VARCHAR) src, ${sqlLiteral(target)}::VARCHAR target_column,
          CAST(${targetName} AS VARCHAR) dst, ${sqlLiteral(`${source}->${target}`)}::VARCHAR relation,
          ${sqlLiteral(root.coordinate)}::VARCHAR source_parquet FROM ${from}
          WHERE ${sourceName} IS NOT NULL AND ${targetName} IS NOT NULL`;
        duckExec(`COPY (${projection}) TO ${sqlLiteral(staging)} (FORMAT PARQUET, COMPRESSION ${policy.batchCompression})`);
        const rows = Number(duck(`SELECT count(*) n FROM read_parquet(${sqlLiteral(staging)})`)[0].n);
        if (!Number.isSafeInteger(rows) || rows < 0) throw new Error(`invalid graph row count for ${kind}`);
        if (await bodyDigest(root.coordinate) !== root.digest) throw new Error(`Parquet root changed during graph projection: ${kind}`);
        const digest = await bodyDigest(staging);
        const output = join(directory, `${stem}-${digest}.parquet`);
        if (existsSync(output)) {
          if (await bodyDigest(output) !== digest) throw new Error(`content-addressed graph output corrupted: ${output}`);
        } else renameSync(staging, output);
        created.push({ ...record(db, root, source, target, output, digest, rows, started, compiler), state: "physical" });
      } finally { if (existsSync(staging)) rmSync(staging); }
    }
    return { kind, pairs: created, input_digest: root.digest };
  } finally { db.close(); }
}

export function status(estate) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing representation manifold: ${path}`);
  const db = new Database(path, { readonly: true, create: false });
  try {
    return { estate: resolve(estate), representations: db.query(`SELECT kind,state,count(*) n FROM representation_instance GROUP BY kind,state ORDER BY kind,state`).all(),
      targets: db.query(`SELECT status,count(*) n FROM representation_target GROUP BY status ORDER BY status`).all() };
  } finally { db.close(); }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  const args = process.argv.slice(2), operation = args[0] ?? "status";
  const option = (key, fallback) => { const index = args.indexOf(key); return index < 0 ? fallback : args[index + 1]; };
  try {
    const estate = option("--estate", join(homedir(), ".bonfyre/estate-local"));
    const kind = option("--kind", null);
    const cue = option("--policy", undefined);
    if (!estate || !["status", "plan-graph", "graph", "virtual-graph", "query-virtual-graph", "plan-all-graphs", "virtual-all-graphs"].includes(operation) ||
        (!["status", "plan-all-graphs", "virtual-all-graphs"].includes(operation) && !kind))
      throw new Error("usage: bun scripts/representation-manifold-forge.mjs status|plan-graph|graph|virtual-graph|query-virtual-graph|plan-all-graphs|virtual-all-graphs [--estate path] [--kind exact-semantic-kind]");
    const source = option("--source", null), target = option("--target", null);
    if (operation === "query-virtual-graph" && (!source || !target)) throw new Error("query-virtual-graph needs --source and --target");
    const result = operation === "status" ? status(estate) :
      operation === "plan-all-graphs" ? await planAllGraphs(estate, cue) :
      operation === "virtual-all-graphs" ? await registerAllVirtualGraphs(estate, cue) :
      operation === "plan-graph" ? await plan(estate, kind, cue) :
      operation === "graph" ? await materializeGraph(estate, kind, cue) :
      operation === "virtual-graph" ? await registerVirtualGraph(estate, kind, cue) :
      await queryVirtualGraph(estate, kind, source, target, Number(option("--limit", 100)));
    console.log(JSON.stringify(result, null, 2));
  } catch (error) { console.error(`representation-forge: ${error.message}`); process.exitCode = 1; }
}
