#!/usr/bin/env bun
// A demand-driven Substrait named-table scan over one verified Parquet root.
// This encodes the small, stable protobuf scan subset; it does not claim to be
// a general SQL-to-Substrait compiler.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readFileSync, realpathSync, renameSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { bodyDigest } from "./columnar_body.mjs";

const sha = value => createHash("sha256").update(value).digest("hex");
const now = () => new Date().toISOString();
const schemaPath = join(import.meta.dir, "..", "schemas/storage/representation-manifold.sql");
const primitiveFields = new Map([
  ["BOOLEAN", 1], ["TINYINT", 2], ["SMALLINT", 3], ["INTEGER", 5],
  ["BIGINT", 7], ["FLOAT", 10], ["DOUBLE", 11], ["DATE", 16],
  ["VARCHAR", 12], ["BLOB", 13],
]);

function varint(value) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`invalid protobuf unsigned value: ${value}`);
  const bytes = [];
  do {
    let byte = value % 128;
    value = Math.floor(value / 128);
    if (value) byte |= 128;
    bytes.push(byte);
  } while (value);
  return Buffer.from(bytes);
}
const bytes = (field, value) => Buffer.concat([varint(field * 8 + 2), varint(value.length), value]);
const string = (field, value) => bytes(field, Buffer.from(value, "utf8"));
const integer = (field, value) => Buffer.concat([varint(field * 8), varint(value)]);
const message = (field, values) => bytes(field, Buffer.concat(values));

export function substraitType(sourceType) {
  const type = String(sourceType).trim().toUpperCase();
  if (type.endsWith("[]")) return message(27, [message(1, [substraitType(type.slice(0, -2))]), integer(3, 1)]);
  const decimal = /^DECIMAL\((\d+),(\d+)\)$/.exec(type);
  if (decimal) {
    const precision = Number(decimal[1]), scale = Number(decimal[2]);
    if (precision >= 1 && precision <= 38 && scale <= precision)
      return message(24, [integer(1, scale), integer(2, precision), integer(4, 1)]);
  }
  const field = primitiveFields.get(type);
  if (!field) throw new Error(`Substrait scan type requires explicit lowering: ${sourceType}`);
  const body = type === "VARCHAR" || type === "BLOB"
    ? [integer(1, 2), integer(2, 1)] : [integer(2, 1)];
  return message(field, body);
}

export function encodeSubstraitScan(table, columns) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table) || !columns.length ||
      columns.some(column => !column.column_name || !column.column_type))
    throw new Error("Substrait scan requires one named table with typed columns");
  const names = columns.map(column => String(column.column_name));
  if (new Set(names).size !== names.length) throw new Error("Substrait scan has duplicate column names");
  const schema = message(2, [
    ...names.map(name => string(1, name)),
    message(2, [...columns.map(column => message(1, [substraitType(column.column_type)])), integer(3, 2)]),
  ]);
  const projection = message(4, [message(1, names.map((_, index) =>
    message(1, index ? [integer(1, index)] : [])))]);
  const read = message(1, [schema, projection, message(7, [string(1, table)])]);
  const root = message(2, [message(1, [read]), ...names.map(name => string(2, name))]);
  return Buffer.concat([message(3, [root]), message(6, [integer(2, 85), string(5, "bonfyre")])]);
}

function manifold(estate, readonly) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing manifold: ${path}`);
  return readonly ? new Database(path, { readonly: true, create: false }) : new Database(path);
}

function canonicalPath(estate, coordinate) {
  if (!isAbsolute(coordinate) || !existsSync(coordinate) || lstatSync(coordinate).isSymbolicLink())
    throw new Error(`unsafe canonical Parquet coordinate: ${coordinate}`);
  const root = realpathSync(join(resolve(estate), "representations"));
  const path = realpathSync(coordinate), rel = relative(root, path);
  if (!rel || rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel) || !lstatSync(path).isFile())
    throw new Error(`canonical Parquet escapes estate: ${coordinate}`);
  return path;
}

function duckdbSchema(path) {
  const sql = `SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM read_parquet('${path.replaceAll("'", "''")}'))`;
  const run = Bun.spawnSync(["duckdb", "-json", "-c", sql],
    { stdout: "pipe", stderr: "pipe", maxBuffer: 8 * 1024 * 1024 });
  if (run.exitCode !== 0) throw new Error(`DuckDB Parquet schema read failed: ${run.stderr.toString().trim()}`);
  return JSON.parse(run.stdout.toString());
}

export async function planSubstrait(estate, kind) {
  if (!kind) throw new Error("Substrait plan requires --kind");
  const db = manifold(estate, true);
  try {
    const target = db.query("SELECT status FROM representation_target WHERE semantic_kind=? AND representation_kind='substrait'").get(kind);
    if (!target) throw new Error(`Substrait is not an authored target for ${kind}`);
    const roots = db.query(`SELECT ri.id,ri.digest,ri.frontier,pr.coordinate,pr.digest physical_digest
      FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
      WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1 AND ri.schema_ref=?`).all(kind);
    if (roots.length !== 1) throw new Error(`expected one canonical Parquet root for ${kind}, found ${roots.length}`);
    const root = roots[0], path = canonicalPath(estate, root.coordinate);
    if (!/^[0-9a-f]{64}$/.test(root.digest) || root.digest !== root.physical_digest ||
        await bodyDigest(path) !== root.digest) throw new Error(`canonical Parquet drift: ${kind}`);
    const columns = duckdbSchema(path), table = `bf_${sha(kind).slice(0, 24)}`;
    const body = encodeSubstraitScan(table, columns), digest = sha(body);
    const coordinate = join(resolve(estate), "representations", "substrait", "sha256",
      digest.slice(0, 2), `${digest}.substrait`);
    return { semantic_kind: kind, source_id: root.id, source_digest: root.digest,
      frontier: root.frontier, table, columns, digest, coordinate, body };
  } finally { db.close(); }
}

async function commitSubstrait(estate, plan) {
  const kind = plan.semantic_kind;
  const { body, ...receipt } = plan;
  const path = plan.coordinate;
  if (existsSync(path)) {
    if (lstatSync(path).isSymbolicLink() || !lstatSync(path).isFile() || await bodyDigest(path) !== plan.digest)
      throw new Error(`existing Substrait plan drift: ${kind}`);
  } else {
    mkdirSync(resolve(path, ".."), { recursive: true });
    const temporary = `${path}.partial-${process.pid}`;
    try { await Bun.write(temporary, body); renameSync(temporary, path); }
    finally { if (existsSync(temporary)) rmSync(temporary); }
  }
  const db = manifold(estate, false);
  try {
    db.exec(readFileSync(schemaPath, "utf8"));
    const root = db.query(`SELECT ri.digest,ri.frontier,pr.coordinate
      FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
      WHERE ri.id=? AND ri.kind='parquet' AND ri.state='physical' AND pr.available=1`).get(plan.source_id);
    if (!root || root.digest !== plan.source_digest || root.frontier !== plan.frontier ||
        await bodyDigest(root.coordinate) !== root.digest || await bodyDigest(path) !== plan.digest)
      throw new Error(`Substrait input or output changed before commit: ${kind}`);
    const id = sha(`substrait|${kind}|${plan.source_id}|${plan.digest}`), ts = now();
    db.transaction(() => {
      db.query("UPDATE representation_instance SET state='superseded' WHERE kind='substrait' AND schema_ref=? AND id<>?").run(kind, id);
      db.query("INSERT OR IGNORE INTO representation_instance VALUES(?,?,?,?,?,?,?,?,?,?)")
        .run(id, null, "substrait", "derived", plan.frontier, plan.digest, kind, ts, 1, "physical");
      db.query("INSERT OR IGNORE INTO physical_realization VALUES(?,?,?,?,?,?,?,?,?)")
        .run(sha(`substrait-file|${id}`), id, "local", path, body.length, plan.digest, "cold", 1, ts);
      db.query("INSERT OR IGNORE INTO representation_edge VALUES(?,?,?,?,?)")
        .run(plan.source_id, id, "parquet->substrait-scan", "materialized", ts);
      db.query("INSERT OR IGNORE INTO transformation_occurrence VALUES(?,?,?,?,?,?,?,?,?)")
        .run(sha(`substrait-transform|${id}`), "parquet->substrait-scan", "bonfyre-substrait-scan-v1",
          plan.source_id, id, plan.source_digest, plan.digest, ts, "materialized");
      db.query("INSERT OR REPLACE INTO plan_representation VALUES(?,?,?,?,?,?)")
        .run(kind, "substrait-scan", path, plan.digest, "physical", ts);
      db.query("UPDATE representation_target SET status='physical',reason='verified native Substrait scan over canonical Parquet' WHERE semantic_kind=? AND representation_kind='substrait'").run(kind);
    })();
    return { ...receipt, representation_id: id, state: "physical" };
  } finally { db.close(); }
}

export async function materializeSubstrait(estate, kind) {
  return commitSubstrait(estate, await planSubstrait(estate, kind));
}

export async function materializeAllSubstrait(estate) {
  const db = manifold(estate, true);
  let kinds;
  try {
    kinds = db.query("SELECT semantic_kind FROM representation_target WHERE representation_kind='substrait' ORDER BY semantic_kind")
      .all().map(row => row.semantic_kind);
  } finally { db.close(); }
  // Preflight every authored root before writing any plan or receipt. This is
  // demand-driven, not an automatic plan for every available Parquet file.
  const plans = [];
  for (const kind of kinds) plans.push(await planSubstrait(estate, kind));
  for (const plan of plans) await commitSubstrait(estate, plan);
  return { state: "MATERIALIZED", plans: plans.length };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    const [command, ...args] = process.argv.slice(2), options = new Map();
    for (let i = 0; i < args.length; i += 2) {
      if (!args[i]?.startsWith("--") || !args[i + 1] || options.has(args[i]))
        throw new Error(`invalid Substrait option: ${args[i]}`);
      options.set(args[i], args[i + 1]);
    }
    for (const key of options.keys()) if (!["--estate", "--kind"].includes(key)) throw new Error(`unknown Substrait option: ${key}`);
    const estate = resolve(options.get("--estate") ?? join(homedir(), ".bonfyre/estate-local"));
    const kind = options.get("--kind");
    if (command === "materialize-all" && kind) throw new Error("materialize-all does not accept --kind");
    const answer = command === "plan" ? await planSubstrait(estate, kind)
      : command === "materialize" ? await materializeSubstrait(estate, kind)
        : command === "materialize-all" ? await materializeAllSubstrait(estate)
        : (() => { throw new Error("usage: substrait plan|materialize --estate PATH --kind KIND; or materialize-all --estate PATH"); })();
    const { body, ...report } = answer;
    console.log(JSON.stringify(report));
  } catch (error) { console.error(`representation-substrait: ${error.message}`); process.exitCode = 1; }
}
