#!/usr/bin/env bun
// A demand-driven Delta log over a copy-on-write Parquet clone. The view is
// reconstructable; neither it nor Delta owns the semantic source.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { constants, copyFileSync, existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { bodyDigest, bodyIdentity } from "./columnar_body.mjs";

const sha = value => createHash("sha256").update(value).digest("hex");
const quote = value => `'${String(value).replaceAll("'", "''")}'`;
const instant = () => new Date().toISOString();
const digestShape = /^[0-9a-f]{64}$/;

function duck(sql) {
  const child = Bun.spawnSync(["duckdb", "-json", ":memory:", sql], { stdout: "pipe", stderr: "pipe", maxBuffer: 64 * 1024 * 1024 });
  if (child.exitCode) throw new Error(`DuckDB schema read failed: ${child.stderr.toString().trim()}`);
  return JSON.parse(child.stdout.toString());
}

// The v1 log uses the basic Delta protocol. Refuse types that need a newer
// feature or a representation-changing conversion rather than guessing.
export function deltaType(input) {
  const type = input.trim().toUpperCase();
  if (type.endsWith("[]")) return { type: "array", elementType: deltaType(type.slice(0, -2)), containsNull: true };
  const decimal = /^DECIMAL\((\d+),(\d+)\)$/.exec(type);
  if (decimal) {
    const precision = Number(decimal[1]), scale = Number(decimal[2]);
    if (precision >= 1 && precision <= 38 && scale <= precision) return `decimal(${precision},${scale})`;
  }
  const types = new Map([
    ["VARCHAR", "string"], ["BOOLEAN", "boolean"], ["TINYINT", "byte"], ["SMALLINT", "short"],
    ["INTEGER", "integer"], ["BIGINT", "long"], ["FLOAT", "float"], ["DOUBLE", "double"],
    ["BLOB", "binary"], ["DATE", "date"],
  ]);
  if (types.has(type)) return types.get(type);
  throw new Error(`Delta v1 cannot represent Parquet type without conversion: ${input}`);
}

function tableSchema(columns) {
  if (!columns.length) throw new Error("Parquet body has no columns");
  const seen = new Set();
  return { type: "struct", fields: columns.map(column => {
    const folded = column.column_name.toLowerCase();
    if (seen.has(folded)) throw new Error(`Delta column names collide without case: ${column.column_name}`);
    seen.add(folded);
    return { name: column.column_name, type: deltaType(column.column_type), nullable: true, metadata: {} };
  }) };
}

async function canonicalRoot(db, kind) {
  const roots = db.query(`SELECT ri.id,ri.schema_ref,ri.frontier,ri.digest,pr.coordinate,pr.digest physical_digest
    FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
    WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1 AND ri.schema_ref=?`).all(kind);
  if (roots.length !== 1) throw new Error(`expected one active Parquet root for ${kind}, found ${roots.length}`);
  const root = roots[0];
  const stat = lstatSync(root.coordinate);
  if (!stat.isFile() || stat.isSymbolicLink() || !digestShape.test(root.digest) ||
      root.physical_digest !== root.digest || await bodyDigest(root.coordinate) !== root.digest)
    throw new Error(`canonical Parquet digest drift for ${kind}`);
  const target = db.query(`SELECT status FROM representation_target
    WHERE semantic_kind=? AND representation_kind='delta'`).get(kind);
  if (!target) throw new Error(`Delta is not an authored representation target for ${kind}`);
  return root;
}

function tableName(kind) {
  const slug = kind.replace(/[^A-Za-z0-9_-]+/g, "_").slice(0, 96);
  return `${slug || "body"}-${sha(kind).slice(0, 12)}`;
}

function uuidFromDigest(digest) {
  const bytes = digest.slice(0, 32).split("");
  bytes[12] = "4";
  bytes[16] = "8";
  const s = bytes.join("");
  return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
}

const localDataPath = root => `part-${root.digest}.parquet`;

function logBody(root, schema, rows) {
  const source = localDataPath(root);
  const timestamp = Date.now();
  const tableId = uuidFromDigest(sha(`${root.id}|${root.digest}|${JSON.stringify(schema)}`));
  const actions = [
    { protocol: { minReaderVersion: 1, minWriterVersion: 2 } },
    { metaData: { id: tableId, format: { provider: "parquet", options: {} },
      schemaString: JSON.stringify(schema), partitionColumns: [], configuration: {}, createdTime: timestamp } },
    { add: { path: source, partitionValues: {}, size: statSync(root.coordinate).size,
      modificationTime: timestamp, dataChange: true, stats: JSON.stringify({ numRecords: rows }),
      tags: { "bonfyre.sourceSha256": root.digest, "bonfyre.frontier": root.frontier ?? "" } } },
  ];
  return actions.map(action => JSON.stringify(action)).join("\n") + "\n";
}

async function verifiedLog(directory, root, schema, rows) {
  const directoryStat = lstatSync(directory);
  if (!directoryStat.isDirectory() || directoryStat.isSymbolicLink() ||
      readdirSync(directory).sort().join("\n") !== `_delta_log\n${localDataPath(root)}` ||
      readdirSync(join(directory, "_delta_log")).join("\n") !== "00000000000000000000.json")
    throw new Error(`Delta view contains an unowned member: ${directory}`);
  const log = join(directory, "_delta_log", "00000000000000000000.json");
  const entries = readFileSync(log, "utf8").trimEnd().split("\n").map(line => JSON.parse(line));
  if (entries.length !== 3 || entries[0].protocol?.minReaderVersion !== 1 ||
      entries[0].protocol?.minWriterVersion !== 2 || entries[1].metaData?.format?.provider !== "parquet" ||
      entries[2].add?.path !== localDataPath(root) ||
      entries[2].add?.tags?.["bonfyre.sourceSha256"] !== root.digest ||
      entries[2].add?.tags?.["bonfyre.frontier"] !== (root.frontier ?? "") ||
      entries[2].add?.size !== statSync(root.coordinate).size ||
      JSON.stringify(JSON.parse(entries[1].metaData?.schemaString ?? "null")) !== JSON.stringify(schema) ||
      JSON.parse(entries[2].add?.stats ?? "null")?.numRecords !== rows)
    throw new Error(`Delta log does not match its canonical source: ${directory}`);
  if (await bodyDigest(join(directory, localDataPath(root))) !== root.digest)
    throw new Error(`Delta data file differs from canonical digest: ${directory}`);
  return bodyIdentity(directory);
}

export async function planDelta(estate, kind) {
  const db = new Database(join(resolve(estate), "representation-manifold.db"), { readonly: true, create: false });
  try {
    const root = await canonicalRoot(db, kind);
    const columns = duck(`SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM read_parquet(${quote(root.coordinate)}))`);
    const schema = tableSchema(columns);
    const rows = Number(duck(`SELECT count(*) n FROM read_parquet(${quote(root.coordinate)})`)[0].n);
    if (!Number.isSafeInteger(rows) || rows < 0 || await bodyDigest(root.coordinate) !== root.digest)
      throw new Error(`canonical Parquet changed during Delta planning for ${kind}`);
    return { kind, root, schema, rows, directory: join(resolve(estate), "representations", "delta",
      `${tableName(kind)}-${root.digest.slice(0, 20)}`) };
  } finally { db.close(); }
}

export async function materializeDelta(estate, kind) {
  const planned = await planDelta(estate, kind);
  const { root, schema, rows, directory } = planned;
  const db = new Database(join(resolve(estate), "representation-manifold.db"));
  const started = instant();
  let staging = null;
  try {
    const current = await canonicalRoot(db, kind);
    if (current.id !== root.id || current.digest !== root.digest || current.frontier !== root.frontier ||
        current.coordinate !== root.coordinate)
      throw new Error(`canonical Parquet root changed after Delta planning for ${kind}`);
    if (!existsSync(directory)) {
      mkdirSync(resolve(directory, ".."), { recursive: true });
      staging = `${directory}.partial-${process.pid}-${Date.now()}`;
      mkdirSync(join(staging, "_delta_log"), { recursive: true });
      try { copyFileSync(root.coordinate, join(staging, localDataPath(root)), constants.COPYFILE_FICLONE_FORCE); }
      catch (error) { throw new Error(`Delta needs a copy-on-write Parquet clone on this volume: ${error.message}`); }
      writeFileSync(join(staging, "_delta_log", "00000000000000000000.json"), logBody(root, schema, rows), { flag: "wx" });
      if (await bodyDigest(root.coordinate) !== root.digest) throw new Error(`canonical Parquet changed during Delta publication: ${kind}`);
      renameSync(staging, directory);
      staging = null;
    }
    const body = await verifiedLog(directory, root, schema, rows);
    if (await bodyDigest(root.coordinate) !== root.digest) throw new Error(`canonical Parquet changed after Delta publication: ${kind}`);
    const id = sha(`delta|${kind}|${root.id}|${root.digest}|${body.digest}`);
    const compiler = "bonfyre-delta-log.v2:cow-parquet";
    const eventId = sha(`delta|${kind}|${started}`), at = instant();
    const prior = db.query(`SELECT ri.id,ri.digest,ri.frontier,pr.coordinate,pr.digest physical_digest,
      t.input_digest,t.output_digest FROM representation_instance ri
      JOIN physical_realization pr ON pr.representation_id=ri.id
      JOIN transformation_occurrence t ON t.output_ref=pr.coordinate
      WHERE ri.id=? AND ri.state='physical' AND pr.available=1 AND t.compiler=? AND t.proof_state='materialized'`).get(id, compiler);
    if (prior?.digest === body.digest && prior?.physical_digest === body.digest &&
        prior?.frontier === root.frontier && prior?.coordinate === directory &&
        prior?.input_digest === root.digest && prior?.output_digest === body.digest)
      return { kind, state: "verified_existing", coordinate: directory, digest: body.digest,
        input_digest: root.digest, representation_id: id, rows, clone: "copy_on_write" };
    db.transaction(() => {
      const active = db.query(`SELECT ri.id,ri.digest,ri.frontier,pr.coordinate FROM representation_instance ri
        JOIN physical_realization pr ON pr.representation_id=ri.id
        WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1 AND ri.schema_ref=?`).all(kind);
      if (active.length !== 1 || active[0].id !== root.id || active[0].digest !== root.digest ||
          active[0].frontier !== root.frontier || active[0].coordinate !== root.coordinate)
        throw new Error(`canonical Parquet receipt changed during Delta commit for ${kind}`);
      db.query(`UPDATE representation_instance SET state='superseded'
        WHERE kind='delta' AND schema_ref=? AND state='physical' AND id<>?`).run(kind, id);
      db.query(`INSERT OR REPLACE INTO representation_instance
        (id,subject_id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state)
        VALUES(?,?,?,?,?,?,?,?,?,?)`).run(id, null, "delta", "derived", root.frontier, body.digest, kind, at, 1, "physical");
      db.query(`INSERT OR REPLACE INTO physical_realization
        (id,representation_id,carrier,coordinate,bytes,digest,temperature,available,checked_at)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(sha(`local|${directory}`), id, "local", directory, body.bytes, body.digest, "cold", 1, at);
      db.query(`INSERT OR IGNORE INTO representation_edge
        (parent_representation_id,child_representation_id,transform,proof_state,created_at)
        VALUES(?,?,?,?,?)`).run(root.id, id, "parquet->delta-log", "materialized", at);
      db.query(`INSERT OR REPLACE INTO transformation_occurrence
        (id,transform,compiler,input_ref,output_ref,input_digest,output_digest,occurred_at,proof_state)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(sha(`delta|${compiler}|${root.digest}|${body.digest}`), "parquet->delta-log",
        compiler, root.coordinate, directory, root.digest, body.digest, at, "materialized");
      db.query(`UPDATE representation_target SET status='physical',reason='digest-bound local Delta view; Parquet remains canonical'
        WHERE semantic_kind=? AND representation_kind='delta'`).run(kind);
      db.query(`INSERT OR REPLACE INTO forge_event VALUES(?,?,?,?,?,?,?)`).run(eventId, "lower", `delta:${kind}`,
        "physical", `${rows} rows; copy-on-write Parquet clone; canonical body unchanged`, started, at);
    })();
    return { kind, state: "physical", coordinate: directory, digest: body.digest,
      input_digest: root.digest, representation_id: id, rows, clone: "copy_on_write" };
  } finally {
    if (staging && existsSync(staging)) rmSync(staging, { recursive: true });
    db.close();
  }
}

if (import.meta.main) {
  try {
    const [verb, kind, ...rest] = process.argv.slice(2);
    if (!["plan", "materialize"].includes(verb) || !kind || rest.length > 2 ||
        (rest.length && (rest[0] !== "--estate" || !rest[1])))
      throw new Error("usage: representation-manifold-delta.mjs plan|materialize KIND [--estate ESTATE]");
    const estate = rest[1] ?? join(homedir(), ".bonfyre", "estate-local");
    const value = verb === "plan" ? await planDelta(estate, kind) : await materializeDelta(estate, kind);
    console.log(JSON.stringify(value, null, 2));
  } catch (error) { console.error(`delta forge: ${error.message}`); process.exitCode = 1; }
}
