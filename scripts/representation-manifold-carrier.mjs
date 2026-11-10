#!/usr/bin/env bun
// Local filesystem carrier for canonical Parquet; no general OpenDAL claim.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { closeSync, constants, existsSync, fstatSync, lstatSync, openSync, readFileSync,
  readSync, realpathSync } from "node:fs";
import { homedir } from "node:os";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { bodyDigest } from "./columnar_body.mjs";

const MAX_READ_BYTES = 8 * 1024 * 1024;
const now = () => new Date().toISOString();
const inside = (root, path) => {
  const rel = relative(root, path);
  return rel && rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel) ? rel : null;
};
const sqlPath = join(import.meta.dir, "..", "schemas", "storage", "representation-manifold.sql");

function manifoldAt(estate, readonly) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing manifold: ${path}`);
  return readonly ? new Database(path, { readonly: true, create: false }) : new Database(path);
}

function physicalPath(estate, coordinate) {
  if (typeof coordinate !== "string" || !isAbsolute(coordinate) ||
      !existsSync(coordinate) || lstatSync(coordinate).isSymbolicLink())
    throw new Error(`unsafe carrier coordinate: ${coordinate}`);
  const root = realpathSync(join(resolve(estate), "representations"));
  const path = realpathSync(coordinate);
  const logical = inside(root, path);
  if (!logical || !lstatSync(path).isFile()) throw new Error(`carrier escapes representation estate: ${coordinate}`);
  return { root, path, logical };
}

function parquetMagicFd(fd, size) {
  if (size < 8) return false;
  const magic = Buffer.allocUnsafe(4), tail = Buffer.allocUnsafe(4);
  return readSync(fd, magic, 0, 4, 0) === 4 && readSync(fd, tail, 0, 4, size - 4) === 4 &&
    magic.toString() === "PAR1" && tail.toString() === "PAR1";
}

function parquetMagic(path, size) {
  const fd = openSync(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try { return parquetMagicFd(fd, size); }
  finally { closeSync(fd); }
}

export async function planNativeFsCarrier(estate) {
  const db = manifoldAt(estate, true);
  try {
    const roots = db.query(`SELECT ri.id,ri.schema_ref,ri.digest,ri.frontier,
      pr.coordinate,pr.digest physical_digest,pr.bytes
      FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
      WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1
      ORDER BY ri.schema_ref,ri.id`).all();
    const planned = [];
    for (const row of roots) {
      const { root, path, logical } = physicalPath(estate, row.coordinate);
      const stat = lstatSync(path);
      if (row.digest !== row.physical_digest || row.bytes !== stat.size ||
          !parquetMagic(path, stat.size) || await bodyDigest(path) !== row.digest)
        throw new Error(`canonical Parquet carrier drift: ${row.schema_ref}`);
      planned.push({ id: row.id, semantic_kind: row.schema_ref, frontier: row.frontier,
        digest: row.digest, bytes: stat.size, coordinate: path, logical_path: logical,
        physical_root: root, capability: { stat: true, read: true, seek: true, format: "parquet", digest: row.digest } });
    }
    return planned;
  } finally { db.close(); }
}

export async function bindNativeFsCarrier(estate) {
  const planned = await planNativeFsCarrier(estate);
  const db = manifoldAt(estate, false);
  try {
    db.exec(readFileSync(sqlPath, "utf8"));
    db.transaction(() => {
      const ts = now();
      db.query("UPDATE carrier_binding SET state='superseded',checked_at=? WHERE carrier='native-fs'").run(ts);
      const insert = db.query(`INSERT OR REPLACE INTO carrier_binding
        (representation_id,carrier,logical_path,physical_root,state,capability_json,checked_at)
        VALUES(?,?,?,?,?,?,?)`);
      for (const row of planned) insert.run(row.id, "native-fs", row.logical_path,
        row.physical_root, "ready_verified", JSON.stringify(row.capability), ts);
    })();
    return { state: "BOUND_VERIFIED", representations: planned.length };
  } finally { db.close(); }
}

export function readNativeFsCarrier(estate, representationId, offset, length) {
  if (!representationId || !Number.isSafeInteger(offset) || offset < 0 ||
      !Number.isSafeInteger(length) || length < 1 || length > MAX_READ_BYTES)
    throw new Error(`read requires representation id, nonnegative offset, and length 1..${MAX_READ_BYTES}`);
  const db = manifoldAt(estate, true);
  try {
    const row = db.query(`SELECT cb.logical_path,cb.physical_root,cb.state,cb.capability_json,
      ri.digest,ri.schema_ref,pr.coordinate,pr.digest physical_digest
      FROM carrier_binding cb JOIN representation_instance ri ON ri.id=cb.representation_id
      JOIN physical_realization pr ON pr.representation_id=ri.id
      WHERE cb.representation_id=? AND cb.carrier='native-fs' AND ri.kind='parquet'
        AND ri.state='physical' AND pr.available=1`).get(representationId);
    if (!row || row.state !== "ready_verified") throw new Error(`no active native filesystem carrier: ${representationId}`);
    const { root, path, logical } = physicalPath(estate, row.coordinate);
    const capability = JSON.parse(row.capability_json);
    if (root !== row.physical_root || logical !== row.logical_path ||
        row.digest !== row.physical_digest || capability.digest !== row.digest)
      throw new Error(`native filesystem carrier receipt drift: ${row.schema_ref}`);
    const fd = openSync(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    try {
      const size = fstatSync(fd).size;
      if (offset + length > size || !parquetMagicFd(fd, size)) throw new Error("carrier read exceeds verified Parquet body");
      const output = Buffer.allocUnsafe(length);
      if (readSync(fd, output, 0, length, offset) !== length) throw new Error("short carrier read");
      const hash = createHash("sha256"), chunk = Buffer.allocUnsafe(1024 * 1024);
      for (let position = 0; position < size;) {
        const n = readSync(fd, chunk, 0, Math.min(chunk.length, size - position), position);
        if (!n) throw new Error("short carrier digest read");
        hash.update(chunk.subarray(0, n));
        position += n;
      }
      if (hash.digest("hex") !== row.digest) throw new Error(`native filesystem carrier body drift: ${row.schema_ref}`);
      return output;
    } finally { closeSync(fd); }
  } finally { db.close(); }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    const args = process.argv.slice(2), command = args[0];
    if (!["plan", "bind", "read"].includes(command))
      throw new Error("usage: carrier plan|bind|read --estate PATH [--representation-id ID --offset N --length N]");
    const values = new Map();
    for (let i = 1; i < args.length; i += 2) {
      if (!args[i]?.startsWith("--") || i + 1 >= args.length || values.has(args[i]) || args[i + 1].startsWith("--"))
        throw new Error(`invalid carrier option: ${args[i]}`);
      values.set(args[i], args[i + 1]);
    }
    const allowed = new Set(["--estate", "--representation-id", "--offset", "--length"]);
    for (const key of values.keys()) if (!allowed.has(key)) throw new Error(`unknown carrier option: ${key}`);
    if (command === "read" && ["--representation-id", "--offset", "--length"].some(key => !values.has(key)))
      throw new Error("read requires --representation-id, --offset, and --length");
    if (command !== "read" && ["--representation-id", "--offset", "--length"].some(key => values.has(key)))
      throw new Error(`${command} does not accept read options`);
    const estate = resolve(values.get("--estate") ?? join(homedir(), ".bonfyre/estate-local"));
    if (command === "plan") console.log(JSON.stringify(await planNativeFsCarrier(estate), null, 2));
    else if (command === "bind") console.log(JSON.stringify(await bindNativeFsCarrier(estate), null, 2));
    else if (command === "read") process.stdout.write(readNativeFsCarrier(estate,
      values.get("--representation-id"), Number(values.get("--offset")), Number(values.get("--length"))));
  } catch (error) { console.error(`representation-carrier: ${error.message}`); process.exitCode = 1; }
}
