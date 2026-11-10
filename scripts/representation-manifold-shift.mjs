#!/usr/bin/env bun
// Physical estate compilation. Semantic owners remain the SQLite source stores.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, renameSync, rmSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { homedir, tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { hashFile, verifyArrowRuntime } from "./arrow-runtime-verify.mjs";
export { verifyArrowRuntime } from "./arrow-runtime-verify.mjs";
import { tableCutRuntime, nativeTableCut } from "./representation-manifold-table-cut.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "..");
const cue = Bun.spawnSync(["cue", "export", join(repo, "schemas/storage/representation-portfolio.cue"), "-e", "representationPortfolio", "--out", "json"]);
if (cue.exitCode !== 0) throw new Error(cue.stderr.toString());
export const portfolio = JSON.parse(cue.stdout.toString());
const now = () => new Date().toISOString();
const sha = value => createHash("sha256").update(value).digest("hex");
const literal = value => `'${String(value).replaceAll("'", "''")}'`;
const ident = value => `"${String(value).replaceAll('"', '""')}"`;

function openSource(path) {
  return new Database(path, { readonly: true, create: false });
}

async function snapshotSources(estate, requestedKinds = null, manifold = null, runtime = null, allowSnapshots = true) {
  let directory = null, created = 0, bytes = 0, elided = 0;
  const sources = [], errors = [];
  const requestedSources = requestedKinds && new Set([...requestedKinds].map(kind => kind.slice(0, kind.indexOf("."))));
  try {
    const files = requestedSources ? [...requestedSources].map(stem => stem + ".db").sort()
      : readdirSync(estate).filter(name => name.endsWith(".db") && name !== "representation-manifold.db").sort();
    for (const file of files) {
      if (requestedSources && !requestedSources.has(file.slice(0, -3))) continue;
      const original = join(estate, file);
      let connection;
      try {
        const identity = requestedKinds ? lstatSync(original) : null;
        if (identity && !identity.isFile()) throw Error("demanded source is not a regular file");
        connection = openSource(original);
        const version = connection.query("PRAGMA data_version").get().data_version;
        const source = { file, original, snapshot: null, connection, version, identity };
        // One retained connection observes the entire no-copy verification window.
        // Reopened-connection data_version values are never compared or persisted.
        if (runtime && await reuseSource(manifold, estate, runtime, source, requestedKinds)) {
          source.snapshot = realpathSync(original); // read-only seed path, not copied bytes
          sources.push(source); elided++; continue;
        }
        if (runtime && !sourceUnchanged(source)) throw Error("source changed during reuse preflight");
        if (!allowSnapshots) {
          for (const kind of requestedKinds) if (kind.startsWith(file.slice(0, -3) + "."))
            errors.push({ source: original, kind, target: "parquet", state: "snapshot_required" });
          connection.close(); connection = null; continue;
        }
        if (!directory) directory = realpathSync(mkdtempSync(join(tmpdir(), "bonfyre-manifold-snapshot-")));
        source.snapshot = join(directory, file);
        connection.exec('VACUUM INTO ' + literal(source.snapshot));
        created++; bytes += statSync(source.snapshot).size;
        sources.push(source);
      } catch (error) {
        connection?.close();
        const selected = requestedKinds && [...requestedKinds].filter(kind => kind.startsWith(file.slice(0, -3) + '.'));
        if (selected) for (const kind of selected) errors.push({ source: original + '#' + kind.slice(kind.indexOf('.') + 1), kind, target: "parquet", state: "pending", error: error.message });
        else errors.push({ source: original, target: "parquet", state: "source_unreadable", error: error.message });
      }
    }
    return { directory, sources, errors, created, bytes, elided };
  } catch (error) {
    for (const source of sources) source.connection.close();
    if (directory) rmSync(directory, { recursive: true, force: true });
    throw error;
  }
}

function sourceUnchanged(source) {
  try {
    if (source.identity) {
      const current = lstatSync(source.original);
      if (!current.isFile() || current.dev !== source.identity.dev || current.ino !== source.identity.ino) return false;
    }
    return source.connection.query("PRAGMA data_version").get().data_version === source.version;
  } catch { return false; }
}

function tables(db) {
  return db.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map(row => row.name);
}

function columns(db, table) {
  return db.query(`PRAGMA table_info(${ident(table)})`).all().map(row => row.name);
}

function upsert(db, sql, params) { db.query(sql).run(...params); }

function seed(manifold, snapshots, requestedKinds = null) {
  const cases = [
    { file: "cms.db", table: "internet_source", fields: ["source"], kind: "InternetSource", owner: "BonfyreCMS", prefix: "internet_source:" },
    { file: "items.db", table: "item", fields: ["source", "key", "kind"], kind: "RetainedMaterial", owner: "BonfyreItemArchive", prefix: "retained:" },
    { file: "frontier.db", table: "channel", fields: ["id", "channel", "url", "href", "census_source"], kind: "SourceChannel", owner: "BonfyreSourceFrontier", prefix: "channel:" },
  ];
  const insertSubject = manifold.query("INSERT OR IGNORE INTO semantic_object VALUES(?,?,?,?,?)");
  const insertOccurrence = manifold.query("INSERT OR IGNORE INTO occurrence(id,kind,subject_id,observed_at,source_coordinate) VALUES(?,?,?,?,?)");
  for (const spec of cases) {
    if (requestedKinds && !requestedKinds.has(`${spec.file.slice(0, -3)}.${spec.table}`)) continue;
    const sourceFile = snapshots.sources.find(source => source.file === spec.file);
    if (!sourceFile) continue;
    const path = sourceFile.original;
    let source;
    try {
      source = openSource(sourceFile.snapshot);
      if (!tables(source).includes(spec.table)) continue;
      const fields = columns(source, spec.table);
      const selected = spec.table === "channel" ? spec.fields.find(field => fields.includes(field)) : spec.fields.every(field => fields.includes(field)) ? spec.fields : null;
      if (!selected) continue;
      const select = (Array.isArray(selected) ? selected : [selected]).map(ident).join(",");
      for (const row of source.query(`SELECT ${select} FROM ${ident(spec.table)}`).iterate()) {
        const values = (Array.isArray(selected) ? selected : [selected]).map(field => row[field]);
        if (values.some(value => value == null)) continue;
        const key = spec.table === "item" ? `${values[0]}:${values[1]}` : String(values[0]);
        const id = spec.prefix + key, coordinate = `${path}#${spec.table}:${key}`, ts = now();
        insertSubject.run(id, spec.kind, spec.owner, coordinate, ts);
        if (spec.table === "item") insertOccurrence.run(sha(`occ|${id}`), String(values[2]), id, ts, coordinate);
      }
    } finally { source?.close(); }
  }
}

function duck(query, binary = "duckdb") {
  try {
    const run = Bun.spawnSync([binary, "-csv", "-noheader", "-c", query], { env: { ...process.env } });
    return { ok: run.exitCode === 0, out: run.stdout.toString().trim(), error: (run.stderr.toString() || run.stdout.toString()).trim().slice(-1600) };
  } catch (error) { return { ok: false, out: "", error: error.message }; }
}

function parquetRows(path, binary = "duckdb") {
  const result = duck(`SELECT count(*) FROM read_parquet(${literal(path)});`, binary);
  return result.ok && /^\d+$/.test(result.out) ? Number(result.out) : null;
}

// The same exact receipt check serves snapshot lowering and no-copy reuse.
function tableReceipt(manifold, estate, runtime, source, table, cut) {
  const kind = source.file.slice(0, -3) + '.' + table;
  const input = source.original + '#' + table;
  const output = join(estate, 'representations', 'parquet', source.file.slice(0, -3), table + '.parquet');
  const frontier = 'sqlite-table-sha256:' + cut.digest + ';rows:' + cut.rows;
  const binding = manifold.query("SELECT 1 FROM source_binding WHERE source_db=? AND source_table=? AND semantic_kind=? AND representation_kind='sqlite' AND primary_coordinate=?").get(source.original, table, kind, input);
  if (!binding) return null;
  const prior = manifold.query("SELECT output_digest FROM transformation_occurrence WHERE transform=? AND compiler=? AND input_ref=? AND output_ref=? AND input_digest=? AND proof_state='materialized' ORDER BY occurred_at DESC LIMIT 1")
    .get(runtime.transform, runtime.compiler, input, output, cut.digest);
  if (!prior) return null;
  const bound = manifold.query("SELECT ri.id FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id WHERE ri.id=? AND ri.kind='parquet' AND ri.schema_ref=? AND ri.frontier=? AND ri.digest=? AND ri.state='physical' AND pr.coordinate=? AND pr.digest=? AND pr.available=1")
    .get(sha('parquet|' + kind + '|' + prior.output_digest), kind, frontier, prior.output_digest, output, prior.output_digest);
  return bound ? { source: input, kind, target: output, state: 'physical', digest: prior.output_digest, frontier, compiler: runtime.compiler, reused: true } : null;
}
const fileStamp = path => {
  const s = lstatSync(path);
  if (!s.isFile() || s.size < 1) return null;
  return [s.dev, s.ino, s.size, s.mtimeMs, s.ctimeMs].join(':');
};
async function verifiedReuse(manifold, estate, runtime, source, table, cut) {
  const receipt = tableReceipt(manifold, estate, runtime, source, table, cut);
  if (!receipt || !sourceUnchanged(source)) return null;
  try {
    const stamp = fileStamp(receipt.target);
    if (!stamp || await hashFile(receipt.target) !== receipt.digest ||
        parquetRows(receipt.target, runtime.duckdb) !== cut.rows || fileStamp(receipt.target) !== stamp) return null;
    runtime.check();
    const current = tableReceipt(manifold, estate, runtime, source, table, cut);
    if (!sourceUnchanged(source) || current?.digest !== receipt.digest) return null;
    return { manifest: receipt, cut, stamp };
  } catch { return null; } // A cache miss still owes the ordinary checked lowering.
}
async function reuseSource(manifold, estate, runtime, source, requestedKinds) {
  const prefix = source.file.slice(0, -3) + '.';
  const selected = [...requestedKinds].filter(kind => kind.startsWith(prefix)).map(kind => kind.slice(prefix.length)).sort();
  // Cold sources and compiler revisions go directly to one consistent snapshot.
  const prior = manifold.query("SELECT 1 FROM transformation_occurrence WHERE transform=? AND compiler=? AND input_ref=? AND proof_state='materialized' LIMIT 1");
  if (selected.some(table => !prior.get(runtime.transform, runtime.compiler, source.original + '#' + table))) return false;
  const reused = new Map();
  const path = realpathSync(source.original);
  for (const table of selected) {
    const cut = nativeTableCut(runtime, path, table);
    const result = await verifiedReuse(manifold, estate, runtime, source, table, cut);
    if (!sourceUnchanged(source)) throw Error('source changed during reuse preflight');
    if (!result) return false;
    reused.set(table, result);
  }
  runtime.check();
  if (!sourceUnchanged(source)) throw Error('source changed during reuse preflight');
  source.reused = reused;
  return true;
}
function refreshWarmReceipts(manifold, estate, snapshots, runtime, manifest) {
  for (const source of snapshots.sources.filter(item => item.reused)) {
    for (const [table, saved] of source.reused) {
      let valid = false;
      try {
        runtime.check();
        valid = sourceUnchanged(source) && fileStamp(saved.manifest.target) === saved.stamp &&
          tableReceipt(manifold, estate, runtime, source, table, saved.cut)?.digest === saved.manifest.digest;
      } catch { /* Changed files, bindings or source standing hold the cut. */ }
      if (!valid) {
        const i = manifest.findIndex(row => row.kind === saved.manifest.kind);
        if (i >= 0) manifest[i] = { source: saved.manifest.source, kind: saved.manifest.kind, target: 'parquet', state: 'pending', error: 'source or representation changed during no-copy reuse' };
      }
    }
  }
}

async function materialize(manifold, estate, snapshots, requestedKinds = null, runtime = null) {
  const root = join(estate, "representations"), manifest = [...snapshots.errors];
  for (const sourceFile of snapshots.sources) {
    const { file, original: path, snapshot } = sourceFile, stem = file.slice(0, -3);
    if (sourceFile.reused) {
      for (const saved of sourceFile.reused.values()) manifest.push(saved.manifest);
      continue;
    }
    let source, names;
    try { source = openSource(snapshot); names = tables(source); }
    catch (error) { manifest.push({ source: path, target: "parquet", state: "source_unreadable", error: error.message }); continue; }
    finally { source?.close(); }
    // Full compilation retains its whole-snapshot identity. Demand cuts hash
    // typed values inside this same private snapshot using the native owner.
    // PRAGMA data_version still rejects changes during either compilation.
    const snapshotDigest = !runtime && names.length ? await hashFile(snapshot) : null;
    for (const table of names) {
      const kind = `${stem}.${table}`, input = `${path}#${table}`;
      if (requestedKinds && !requestedKinds.has(kind)) continue;
      const output = join(root, "parquet", stem, `${table}.parquet`);
      upsert(manifold, "INSERT OR REPLACE INTO source_binding VALUES(?,?,?,?,?)", [path, table, kind, "sqlite", input]);
      mkdirSync(dirname(output), { recursive: true });
      let cut = null;
      try { if (runtime) cut = nativeTableCut(runtime, snapshot, table); }
      catch (error) { manifest.push({ source: input, kind, target: "parquet", state: "pending", error: error.message }); continue; }
      const before = cut?.digest ?? snapshotDigest;
      const compiler = runtime?.compiler ?? "duckdb", transform = runtime?.transform ?? "sqlite->parquet";
      const cutFrontier = cut && `sqlite-table-sha256:${before};rows:${cut.rows}`;
      let digest = null, rows = null;
      if (runtime && existsSync(output) && !lstatSync(output).isFile()) {
        manifest.push({ source: input, kind, target: "parquet", state: "pending", error: "derived output is not a regular file" }); continue;
      }
      if (runtime) {
        const reused = await verifiedReuse(manifold, estate, runtime, sourceFile, table, cut);
        if (reused) { manifest.push(reused.manifest); continue; }
      } else {
        const prior = manifold.query("SELECT output_digest FROM transformation_occurrence WHERE input_ref=? AND input_digest=? AND proof_state='materialized' ORDER BY occurred_at DESC LIMIT 1").get(input, before);
        if (prior && sourceUnchanged(sourceFile) && existsSync(output) && statSync(output).size > 0 && await hashFile(output) === prior.output_digest) {
          digest = prior.output_digest; rows = parquetRows(output);
        }
      }
      if (!digest) {
        const temporary = `${output}.partial-${process.pid}-${crypto.randomUUID()}`;
        const result = duck(`LOAD sqlite; ATTACH ${literal(snapshot)} AS s (TYPE SQLITE); COPY (SELECT * FROM s.${ident(table)}) TO ${literal(temporary)} (FORMAT PARQUET, COMPRESSION ZSTD);`, runtime?.duckdb);
        const drifted = !sourceUnchanged(sourceFile);
        if (!result.ok || drifted || !existsSync(temporary) || statSync(temporary).size === 0) {
          if (existsSync(temporary)) rmSync(temporary);
          manifest.push({ source: input, kind, target: "parquet", state: "pending", error: drifted ? "source changed after snapshot" : result.error || "no physical output" });
          continue;
        }
        digest = await hashFile(temporary);
        rows = parquetRows(temporary, runtime?.duckdb);
        if (runtime) {
          try {
            runtime.check();
            if (rows !== cut.rows || !sourceUnchanged(sourceFile)) throw Error("source cut or lowered row count changed");
          } catch (error) {
            rmSync(temporary);manifest.push({ source: input, kind, target: "parquet", state: "pending", error: error.message });continue;
          }
        }
        renameSync(temporary, output);
      }
      const frontier = cutFrontier ?? `sqlite-sha256:${before};${rows == null ? `parquet-sha256:${digest}` : `rows:${rows}`}`;
      const id = sha(`parquet|${kind}|${digest}`), ts = now();
      upsert(manifold, "INSERT OR REPLACE INTO representation_instance VALUES(?,?,?,?,?,?,?,?,?,?)", [id, null, "parquet", "derived", frontier, digest, kind, ts, 1, "physical"]);
      upsert(manifold, "INSERT OR REPLACE INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)", [sha(`file|${output}`), id, "local", output, statSync(output).size, digest, "warm", 1, ts]);
      const transformation = runtime ? sha(`${transform}|${kind}|${before}|${digest}|${compiler}`) : sha(`sqlite->parquet|${kind}|${before}|${digest}`);
      upsert(manifold, "INSERT OR REPLACE INTO transformation_occurrence VALUES(?,?,?,?,?,?,?,?,?)", [transformation, transform, compiler, input, output, before, digest, ts, "materialized"]);
      manifest.push({ source: input, kind, target: output, state: "physical", digest, frontier, ...(runtime ? { compiler, reused: false } : {}) });
    }
  }
  if (runtime) refreshWarmReceipts(manifold, estate, snapshots, runtime, manifest);
  // Scoped native receipts remain in the manifold database. Never replace
  // the global manifest with a partial JSON inventory.
  if (!requestedKinds) {
    mkdirSync(root, { recursive: true });
    await Bun.write(join(root, "manifest.json"), JSON.stringify(manifest, null, 2));
  }
  return manifest;
}

function substrate(root, name) {
  for (const relative of portfolio.substrate_coordinates[name] ?? [name]) {
    const path = join(root, relative);
    if (existsSync(path)) return path;
  }
  return null;
}

async function installPortfolio(manifold, estate, substrateRoot, manifest, requestedKinds = null) {
  for (const [kind, capabilities] of Object.entries(portfolio.capability_unlocks)) {
    for (const capability of capabilities) upsert(manifold, "INSERT OR IGNORE INTO capability_unlock VALUES(?,?)", [kind, capability]);
  }
  const kinds = manifold.query("SELECT DISTINCT semantic_kind FROM source_binding").all().map(row => row.semantic_kind)
    .filter(kind => !requestedKinds || requestedKinds.has(kind));
  const current = new Map(manifest.filter(row => row.kind).map(row => [row.kind, row.state]));
  const targets = Object.values(portfolio.classes).flat();
  // Substrate availability is independent of the semantic kind. Observe it
  // once per target during this compilation, not 23,836 times per run.
  const substrateByTarget = new Map(targets.map(target => [target, substrate(substrateRoot, target)]));
  // Avoid DELETE+INSERT replacement for identical target rows. SQLite IS
  // comparison preserves NULL vs non-NULL and every status/reason exactly.
  const insertTarget = manifold.query("INSERT INTO representation_target(semantic_kind,representation_kind,status,substrate_path,reason) VALUES(?,?,?,?,?) "
    + "ON CONFLICT(semantic_kind,representation_kind) DO UPDATE SET status=excluded.status,substrate_path=excluded.substrate_path,reason=excluded.reason "
    + "WHERE representation_target.status IS NOT excluded.status OR representation_target.substrate_path IS NOT excluded.substrate_path OR representation_target.reason IS NOT excluded.reason");
  const seedUnresolved = requestedKinds && manifold.query(
    "INSERT OR IGNORE INTO representation_target(semantic_kind,representation_kind,status,substrate_path,reason) VALUES(?,?,?,?,?)");
  for (const kind of kinds) {
    for (const target of targets) {
      const path = substrateByTarget.get(target);
      const physical = target === "parquet" && current.get(kind) === "physical";
      const state = physical ? "physical" : target === "parquet" && current.get(kind) === "pending" ? "compilation_pending" : path ? "substrate_present" : "declared_target";
      // Other owners publish real Zarr/Delta/Substrait states. Scoped Parquet
      // lowering can seed unknown targets but may not reset their evidence.
      const writer = seedUnresolved && target !== "parquet" ? seedUnresolved : insertTarget;
      writer.run(kind, target, state, path, "authored representation portfolio");
    }
  }
  // Full compilation retains the historical contract. A demand cut reads
  // canonical CUE policy instead of maintaining a duplicate JSON catalog.
  if (!requestedKinds) await Bun.write(join(estate, "representation-portfolio.json"), JSON.stringify({ schema: portfolio.schema, generated_at: now(), law: portfolio.law,
    classes: portfolio.classes, capability_unlocks: portfolio.capability_unlocks }, null, 2));
}

function frontierProofs(manifold, requestedKinds = null) {
  for (const row of manifold.query("SELECT semantic_kind, COUNT(*) n FROM source_binding GROUP BY semantic_kind").all()) {
    if (requestedKinds && !requestedKinds.has(row.semantic_kind)) continue;
    const digest = sha(`${row.semantic_kind}|${row.n}`);
    upsert(manifold, "INSERT OR REPLACE INTO frontier_proof VALUES(?,?,?,?,?,?)", [sha(`frontier|${digest}`), row.semantic_kind, `bindings:${row.n}`, row.n, digest, now()]);
  }
}

// Demand names an exact source-owned SQLite table, never a foreign URL or
// user-supplied path. Matrix/AuthorityGraph still decides what may be read.
function validateDemand(estate, kinds) {
  if (kinds == null) return null;
  if (!Array.isArray(kinds) || !kinds.length) throw new Error("nonempty kinds array required");
  const selected = new Set(), bySource = new Map();
  for (const kind of kinds) {
    if (typeof kind !== "string") throw new Error("demand kind must be a string");
    const dot = kind.indexOf("."), stem = kind.slice(0, dot), table = kind.slice(dot + 1);
    if (dot < 1 || !/^[A-Za-z0-9_-]+$/.test(stem) || !/^[A-Za-z0-9_-]+$/.test(table))
      throw new Error(`invalid demanded semantic kind: ${kind}`);
    if (stem === "representation-manifold") throw Error("the derived manifold cannot snapshot itself");
    selected.add(kind);
    if (!bySource.has(stem)) bySource.set(stem, new Set());
    bySource.get(stem).add(table);
  }
  for (const [stem, requestedTables] of bySource) {
    const path = join(estate, `${stem}.db`);
    if (!existsSync(path) || !lstatSync(path).isFile())
      throw new Error(`demand source unavailable: ${stem}`);
    const db = openSource(path);
    try {
      const present = new Set(tables(db));
      for (const table of requestedTables)
        if (!present.has(table)) throw new Error(`demanded table unavailable: ${stem}.${table}`);
    } finally { db.close(); }
  }
  return selected;
}

// Reuse eligibility is a read result. It never saves a grant, copied source,
// semantic seed, target state, frontier record, manifest or materialization.
export async function verifyDemand(estate, kinds, options = {}) {
  const requested = validateDemand(estate, kinds);
  if (!requested) throw Error('verification requires exact demanded kinds');
  const path = join(estate, 'representation-manifold.db');
  if (!existsSync(path) || !lstatSync(path).isFile()) throw Error('missing or unsafe manifold');
  const runtime = await tableCutRuntime(fileURLToPath(import.meta.url), options.hashBinary);
  const db = new Database(path, { readonly: true, create: false });
  let sources;
  try {
    sources = await snapshotSources(estate, requested, db, runtime, false);
    const manifest = [...sources.errors, ...sources.sources.flatMap(source => [...source.reused.values()].map(row => row.manifest))];
    refreshWarmReceipts(db, estate, sources, runtime, manifest);
    const held = manifest.filter(row => row.state === 'pending').length;
    const required = manifest.filter(row => row.state === 'snapshot_required').length;
    return { state: held ? 'HELD' : required ? 'SNAPSHOT_REQUIRED' : 'REUSABLE', read_only: true, manifest,
      demand: { kinds: [...requested].sort(), reused: manifest.filter(row => row.reused).length,
        lowerings: 0, held, snapshot_required: required,
        source_snapshots: 0, source_snapshot_bytes: 0, source_snapshots_elided: sources.elided } };
  } finally { for (const source of sources?.sources ?? []) source.connection.close(); db.close(); }
}

export async function apply(estate, substrateRoot = join(homedir(), ".bonfyre/substrates/v6.1"), options = {}) {
  if (!existsSync(estate) || !statSync(estate).isDirectory()) throw new Error(`estate directory missing: ${estate}`);
  const requestedKinds = validateDemand(estate, options.kinds ?? null);
  const runtime = requestedKinds ? await tableCutRuntime(fileURLToPath(import.meta.url), options.hashBinary) : null;
  const dbPath = join(estate, "representation-manifold.db");
  const manifold = new Database(dbPath, { create: true });
  let snapshots;
  try {
    manifold.exec(readFileSync(join(repo, "schemas/storage/representation-manifold.sql"), "utf8"));
    snapshots = await snapshotSources(estate, requestedKinds, manifold, runtime);
    upsert(manifold, "INSERT OR REPLACE INTO manifold_meta VALUES(?,?)", ["shift", "representation-manifold-v2-native"]);
    upsert(manifold, "INSERT OR REPLACE INTO manifold_meta VALUES(?,?)", ["applied_at", now()]);
    manifold.transaction(() => {
      seed(manifold, snapshots, requestedKinds);
      for (const source of snapshots.sources) if (source.reused && !sourceUnchanged(source))
        throw Error('source changed during no-copy seed read');
    })();
    const manifest = await materialize(manifold, estate, snapshots, requestedKinds, runtime);
    await installPortfolio(manifold, estate, substrateRoot, manifest, requestedKinds);
    frontierProofs(manifold, requestedKinds);
    return { manifest, status: status(estate, manifold),
      ...(requestedKinds ? { demand: { kinds: [...requestedKinds].sort(),
        materialized: manifest.filter(row => row.state === "physical").length,
        held: manifest.filter(row => row.state !== "physical").length,
        reused: manifest.filter(row => row.reused === true).length,
        lowerings: manifest.filter(row => row.state === "physical" && row.reused === false).length,
        source_snapshots: snapshots.created, source_snapshot_bytes: snapshots.bytes,
        source_snapshots_elided: snapshots.elided } } : {}) };
  } finally {
    for (const source of snapshots?.sources ?? []) source.connection.close();
    if (snapshots?.directory) rmSync(snapshots.directory, { recursive: true, force: true });
    manifold.close();
  }
}

export function status(estate, opened = null) {
  const path = join(estate, "representation-manifold.db");
  if (!opened && !existsSync(path)) return { estate, state: "NOT MATERIALIZED" };
  const db = opened ?? new Database(path, { readonly: true, create: false });
  try {
    const counts = {};
    for (const table of ["semantic_object", "occurrence", "representation_instance", "physical_realization", "transformation_occurrence", "capability_unlock", "representation_target", "frontier_proof"])
      counts[table] = db.query(`SELECT COUNT(*) n FROM ${ident(table)}`).get().n;
    return { estate, state: "MATERIALIZED", counts, targets: db.query("SELECT status,COUNT(*) n FROM representation_target GROUP BY status ORDER BY status").all() };
  } finally { if (!opened) db.close(); }
}

const flightName = kind => {
  let name = String(kind).replaceAll(/[^A-Za-z0-9_]+/g, "_");
  if (!name || /^\d/.test(name)) name = `_${name}`;
  return name.slice(0, 180);
};

// Registration is a build-time capability receipt, not a claim that a Flight
// service is running or that a derived Parquet body owns semantic truth.
export async function bindArrowRuntime(estate, runtime) {
  const dbPath = join(estate, "representation-manifold.db");
  if (!existsSync(dbPath)) throw new Error(`missing manifold: ${dbPath}`);
  const manifest = await verifyArrowRuntime(runtime);
  const db = new Database(dbPath);
  try {
    db.exec(readFileSync(join(repo, "schemas/storage/representation-manifold.sql"), "utf8"));
    const roots = db.query(`SELECT ri.id,ri.schema_ref,pr.coordinate,ri.digest,ri.frontier
      FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
      WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1
      ORDER BY ri.schema_ref`).all();
    const names = new Set(), tables = [];
    for (const row of roots) {
      const name = flightName(row.schema_ref);
      if (names.has(name)) throw new Error(`Flight name collision: ${name}`);
      names.add(name);
      if (!existsSync(row.coordinate) || await hashFile(row.coordinate) !== row.digest)
        throw new Error(`canonical Parquet drift: ${row.schema_ref}`);
      tables.push({ name, semantic_kind: row.schema_ref, parquet: row.coordinate,
        digest: row.digest, frontier: row.frontier });
    }
    const body = JSON.stringify({ tables });
    const digest = sha(body);
    const directory = join(estate, "representations", "flight");
    mkdirSync(directory, { recursive: true });
    const catalog = join(directory, `catalog-${digest}.json`);
    if (existsSync(catalog)) {
      if (await hashFile(catalog) !== digest) throw new Error(`Flight catalog drift: ${catalog}`);
    } else {
      const temporary = `${catalog}.partial-${process.pid}`;
      try { await Bun.write(temporary, body); renameSync(temporary, catalog); }
      finally { if (existsSync(temporary)) rmSync(temporary); }
    }
    db.exec("BEGIN IMMEDIATE");
    try {
      const ts = now(), flight = join(runtime, "bonfyre-arrow-flight"),
        stream = join(runtime, "bonfyre-arrow-stream"),
        cAbi = join(runtime, "libbonfyre_arrow_c.dylib");
      db.query("UPDATE executable_projection SET state='superseded' WHERE kind IN ('arrow-flight','arrow-ipc-on-demand')").run();
      db.query("UPDATE transport_binding SET state='superseded',checked_at=? WHERE transport IN ('arrow-flight','arrow-c-stream')").run(ts);
      db.query("UPDATE execution_binding SET state='superseded',checked_at=? WHERE engine='arrow-ipc-on-demand'").run(ts);
      db.query("INSERT OR REPLACE INTO executable_projection VALUES(?,?,?,?,?,?)")
        .run(sha(`native-flight|${digest}`), "arrow-flight", flight,
          manifest.binaries["bonfyre-arrow-flight"].sha256, "installed_verified", ts);
      db.query("INSERT OR REPLACE INTO executable_projection VALUES(?,?,?,?,?,?)")
        .run(sha(`native-ipc|${digest}`), "arrow-ipc-on-demand", stream,
          manifest.binaries["bonfyre-arrow-stream"].sha256, "installed_verified", ts);
      for (const row of tables) {
        db.query("INSERT OR REPLACE INTO transport_binding VALUES(?,?,?,?,?,?)")
          .run(row.semantic_kind, "arrow-flight", `flight://estate/${row.name}`,
            "installed_unserved", sha(`flight|${digest}|${row.digest}|${row.frontier}`), ts);
        db.query("INSERT OR REPLACE INTO transport_binding VALUES(?,?,?,?,?,?)")
          .run(row.semantic_kind, "arrow-c-stream",
            `${cAbi}#bf_parquet_export_verified_stream?sha256=${row.digest}`,
            "installed_verified", sha(`c-data|${row.digest}|${row.frontier}`), ts);
        db.query("INSERT OR REPLACE INTO execution_binding VALUES(?,?,?,?,?,?)")
          .run(row.semantic_kind, "arrow-ipc-on-demand", stream, "installed_verified",
            sha(`ipc|${row.digest}|${row.frontier}`), ts);
      }
      db.exec("COMMIT");
    } catch (error) { db.exec("ROLLBACK"); throw error; }
    return { estate, runtime, catalog, digest, tables: tables.length,
      state: "INSTALLED_UNSERVED" };
  } finally { db.close(); }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    const argv = process.argv.slice(2);
    const argument = (flag, fallback) => argv.includes(flag) ? argv[argv.indexOf(flag) + 1] : fallback;
    const estate = resolve(argument("--estate", join(homedir(), ".bonfyre/estate-local")));
    const demanded = argv.flatMap((part, index) => part === "--kind" ? [argv[index + 1]] : []);
    const verifying = argv.includes("--verify");
    if (verifying && (argv.includes("--apply") || argv.includes("--bind-arrow-runtime"))) throw Error("--verify cannot mutate or bind");
    if (verifying && !demanded.length) throw Error("--verify requires --kind");
    if (demanded.length && !argv.includes("--apply") && !verifying) throw new Error("--kind requires --apply or --verify");
    const answer = verifying ? await verifyDemand(estate, demanded) : argv.includes("--bind-arrow-runtime")
      ? await bindArrowRuntime(estate, resolve(argument("--bind-arrow-runtime")))
      : argv.includes("--apply")
        ? await apply(estate, argument("--substrate-root", join(homedir(), ".bonfyre/substrates/v6.1")),
            demanded.length ? { kinds: demanded } : {})
        : status(estate);
    if (argv.includes("--json")) console.log(JSON.stringify(answer, null, 2));
    else {
      const state = answer.status ?? answer;
      console.log(`BONFŸRE REPRESENTATION MANIFOLD\nestate ${estate}\nstate ${state.state}`);
      if (state.catalog) console.log(`catalog ${state.catalog}\ntables ${state.tables}`);
      for (const [name, count] of Object.entries(state.counts ?? {})) console.log(`${name.padEnd(32)} ${count}`);
      for (const row of state.targets ?? []) console.log(`  ${row.status}: ${row.n}`);
      if (answer.demand) {
        const d = answer.demand;
        console.log('demand ' + d.kinds.join(', '));
        console.log('reuse ' + d.reused + ' reused; ' + d.lowerings + ' lowered; ' + d.held + ' held');
        console.log('snapshots ' + d.source_snapshots + ' created; ' + d.source_snapshot_bytes + ' bytes; ' + d.source_snapshots_elided + ' avoided');
        if (d.snapshot_required) console.log('snapshot required for ' + d.snapshot_required + ' selected table(s)');
      }
    }
    if (verifying && answer.state !== "REUSABLE") process.exitCode = 3;
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
