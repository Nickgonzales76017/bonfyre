#!/usr/bin/env bun
// Demand-driven Zarr v2 realization from a digest-bound Parquet root. Native
// Arrow owns batch conversion; this layer owns receipts and publication.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { bodyIdentity, bodyDigest } from "./columnar_body.mjs";
import { verifyArrowRuntime } from "./arrow-runtime-verify.mjs";

const sha = value => createHash("sha256").update(value).digest("hex");
const at = () => new Date().toISOString();

async function rootFor(db, kind) {
  const roots = db.query(`SELECT ri.id,ri.digest,ri.frontier,pr.coordinate,pr.digest physical_digest
    FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
    WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1 AND ri.schema_ref=?`).all(kind);
  if (roots.length !== 1) throw new Error(`expected one active canonical Parquet root for ${kind}, found ${roots.length}`);
  const root = roots[0], body = lstatSync(root.coordinate);
  if (!body.isFile() || body.isSymbolicLink() || !/^[0-9a-f]{64}$/.test(root.digest) ||
      root.digest !== root.physical_digest || await bodyDigest(root.coordinate) !== root.digest)
    throw new Error(`canonical Parquet digest drift for ${kind}`);
  if (!db.query(`SELECT 1 FROM representation_target WHERE semantic_kind=? AND representation_kind='zarr'`).get(kind))
    throw new Error(`Zarr is not an authored representation target for ${kind}`);
  return root;
}

export async function planZarr(estate, kind) {
  const db = new Database(join(resolve(estate), "representation-manifold.db"), { readonly: true, create: false });
  try {
    const root = await rootFor(db, kind);
    const directory = join(resolve(estate), "representations", "zarr",
      `${kind.replace(/[^A-Za-z0-9_-]+/g, "_").slice(0, 96) || "body"}-${sha(kind).slice(0, 12)}-${root.digest.slice(0, 20)}.zarr`);
    return { kind, root, directory, state: existsSync(directory) ? "verify_existing" : "demanded_unbuilt" };
  } finally { db.close(); }
}

async function verifyOutput(directory, root) {
  const attributes = JSON.parse(readFileSync(join(directory, ".zattrs"), "utf8"));
  if (JSON.parse(readFileSync(join(directory, ".zgroup"), "utf8")).zarr_format !== 2 ||
      attributes["bonfyre:sourceParquetSha256"] !== root.digest ||
      !Array.isArray(attributes["bonfyre:columns"]) || !attributes["bonfyre:columns"].length)
    throw new Error(`Zarr metadata does not match canonical Parquet: ${directory}`);
  const allowed = new Set([".zattrs", ".zgroup"]);
  for (const column of attributes["bonfyre:columns"]) {
    if (!/^c\d+$/.test(column.values) || !/^m\d+$/.test(column.validity))
      throw new Error(`invalid Zarr array coordinate: ${directory}`);
    allowed.add(column.values);
    allowed.add(column.validity);
    const value = JSON.parse(readFileSync(join(directory, column.values, ".zarray"), "utf8"));
    const mask = JSON.parse(readFileSync(join(directory, column.validity, ".zarray"), "utf8"));
    if (value.zarr_format !== 2 || mask.zarr_format !== 2 || value.dtype !== column.dtype ||
        mask.dtype !== "|u1" || value.compressor !== null || mask.compressor !== null ||
        !Array.isArray(value.shape) || value.shape.length !== 1 ||
        !Array.isArray(value.chunks) || value.chunks.length !== 1 ||
        JSON.stringify(value.shape) !== JSON.stringify(mask.shape) ||
        JSON.stringify(value.chunks) !== JSON.stringify(mask.chunks) ||
        !Number.isSafeInteger(value.shape[0]) || value.shape[0] < 0 ||
        !Number.isSafeInteger(value.chunks[0]) || value.chunks[0] < 1)
      throw new Error(`invalid Zarr array metadata: ${directory}`);
    const chunks = Math.ceil(value.shape[0] / value.chunks[0]);
    const expected = [".zarray", ...Array.from({ length: chunks }, (_, i) => String(i))].sort();
    for (const coordinate of [column.values, column.validity]) {
      if (readdirSync(join(directory, coordinate)).sort().join("\n") !== expected.join("\n"))
        throw new Error(`Zarr array has missing or unowned chunks: ${coordinate}`);
    }
  }
  if (readdirSync(directory).sort().join("\n") !== [...allowed].sort().join("\n"))
    throw new Error(`Zarr view contains unowned members: ${directory}`);
  return { ...await bodyIdentity(directory), numeric_columns: attributes["bonfyre:columns"].length };
}

function verifyNativeLinkage(binary, bundle) {
  const linkage = Bun.spawnSync(["otool", "-L", binary], { stdout: "pipe", stderr: "pipe" });
  const commands = Bun.spawnSync(["otool", "-l", binary], { stdout: "pipe", stderr: "pipe" });
  if (linkage.exitCode || commands.exitCode) throw new Error("native Zarr linkage inspection failed");
  const libraries = linkage.stdout.toString(), loads = commands.stdout.toString();
  if (!libraries.includes("@rpath/libarrow.2500.dylib") ||
      !libraries.includes("@rpath/libparquet.2500.dylib") ||
      !loads.includes(`path ${join(resolve(bundle), "lib")} (offset 12)`) ||
      /site-packages|Python\.framework|libpython/i.test(libraries + loads))
    throw new Error("native Zarr lowerer is not linked to this Python-free Arrow bundle");
}

export function assertExistingZarrReceipt(db, directory, inputDigest, outputDigest, compiler) {
  const receipts = db.query(`SELECT input_digest,output_digest,compiler FROM transformation_occurrence
    WHERE transform='parquet->zarr-numeric' AND output_ref=? AND proof_state='materialized'`).all(directory);
  if (receipts.length !== 1 || receipts[0].input_digest !== inputDigest ||
      receipts[0].output_digest !== outputDigest || receipts[0].compiler !== compiler)
    throw new Error(`existing Zarr view differs from its exact materialization receipt: ${directory}`);
}

export async function materializeZarr(estate, kind, { binary, bundle, chunkSize = 8192 }) {
  if (!binary || !bundle) throw new Error("Zarr lowering requires explicit native binary and Arrow bundle");
  if (!Number.isSafeInteger(chunkSize) || chunkSize < 1 || chunkSize > 1_000_000)
    throw new Error("chunk size must be 1..1000000");
  const plan = await planZarr(estate, kind), { root, directory } = plan;
  const binaryPath = resolve(binary), binaryStat = lstatSync(binaryPath);
  if (!binaryStat.isFile() || binaryStat.isSymbolicLink()) throw new Error("Zarr native binary is not a regular file");
  const binaryDigest = await bodyDigest(binaryPath);
  const manifest = await verifyArrowRuntime(resolve(bundle));
  verifyNativeLinkage(binaryPath, bundle);
  const compiler = `bonfyre-zarr-forge.v1:${binaryDigest}:${sha(JSON.stringify(manifest))}`;
  const db = new Database(join(resolve(estate), "representation-manifold.db"));
  let stage = null;
  try {
    const current = await rootFor(db, kind);
    if (current.id !== root.id || current.digest !== root.digest || current.frontier !== root.frontier ||
        current.coordinate !== root.coordinate) throw new Error(`Parquet root changed during Zarr planning for ${kind}`);
    const existingDirectory = existsSync(directory);
    if (!existingDirectory) {
      mkdirSync(resolve(directory, ".."), { recursive: true });
      stage = `${directory}.partial-${process.pid}-${Date.now()}`;
      const child = Bun.spawnSync([binaryPath, root.coordinate, stage, root.digest, String(chunkSize)],
        { stdout: "pipe", stderr: "pipe", maxBuffer: 1024 * 1024 });
      if (child.exitCode) throw new Error(`native Zarr lowering failed: ${child.stderr.toString().trim()}`);
      if (await bodyDigest(root.coordinate) !== root.digest) throw new Error(`Parquet changed during Zarr lowering for ${kind}`);
      await verifyOutput(stage, root);
      renameSync(stage, directory);
      stage = null;
    }
    const body = await verifyOutput(directory, root);
    if (existingDirectory) assertExistingZarrReceipt(db, directory, root.digest, body.digest, compiler);
    if (await bodyDigest(root.coordinate) !== root.digest) throw new Error(`Parquet changed after Zarr lowering for ${kind}`);
    const id = sha(`zarr|${kind}|${root.id}|${root.digest}|${body.digest}`), started = at();
    const existing = db.query(`SELECT ri.id FROM representation_instance ri
      JOIN physical_realization pr ON pr.representation_id=ri.id
      JOIN transformation_occurrence t ON t.output_ref=pr.coordinate
      WHERE ri.id=? AND ri.state='physical' AND pr.available=1 AND pr.digest=? AND t.input_digest=?
        AND t.output_digest=? AND t.compiler=? AND t.proof_state='materialized'`).get(
      id, body.digest, root.digest, body.digest, compiler);
    if (existing) return { kind, state: "verified_existing", coordinate: directory, digest: body.digest,
      input_digest: root.digest, representation_id: id, numeric_columns: body.numeric_columns };
    db.transaction(() => {
      const active = db.query(`SELECT ri.id,ri.digest,ri.frontier,pr.coordinate FROM representation_instance ri
        JOIN physical_realization pr ON pr.representation_id=ri.id
        WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1 AND ri.schema_ref=?`).all(kind);
      if (active.length !== 1 || active[0].id !== root.id || active[0].digest !== root.digest ||
          active[0].frontier !== root.frontier || active[0].coordinate !== root.coordinate)
        throw new Error(`Parquet receipt changed during Zarr commit for ${kind}`);
      db.query(`UPDATE representation_instance SET state='superseded'
        WHERE kind='zarr' AND schema_ref=? AND state='physical' AND id<>?`).run(kind, id);
      db.query(`INSERT OR REPLACE INTO representation_instance
        (id,subject_id,kind,authority,frontier,digest,schema_ref,created_at,reconstructable,state)
        VALUES(?,?,?,?,?,?,?,?,?,?)`).run(id, null, "zarr", "derived", root.frontier, body.digest, kind, started, 1, "physical");
      db.query(`INSERT OR REPLACE INTO physical_realization
        (id,representation_id,carrier,coordinate,bytes,digest,temperature,available,checked_at)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(sha(`local|${directory}`), id, "local", directory, body.bytes, body.digest, "cold", 1, started);
      db.query(`INSERT OR IGNORE INTO representation_edge
        (parent_representation_id,child_representation_id,transform,proof_state,created_at)
        VALUES(?,?,?,?,?)`).run(root.id, id, "parquet->zarr-numeric", "materialized", started);
      db.query(`INSERT OR REPLACE INTO transformation_occurrence
        (id,transform,compiler,input_ref,output_ref,input_digest,output_digest,occurred_at,proof_state)
        VALUES(?,?,?,?,?,?,?,?,?)`).run(sha(`${compiler}|${root.digest}|${body.digest}`), "parquet->zarr-numeric",
        compiler, root.coordinate, directory, root.digest, body.digest, started, "materialized");
      db.query(`INSERT OR REPLACE INTO executable_projection VALUES(?,?,?,?,?,?)`).run(
        sha(`zarr-native|${binaryDigest}`), "arrow-zarr-lowerer", binaryPath, binaryDigest, "installed_verified", started);
      db.query(`UPDATE representation_target SET status='physical',reason='native Arrow numeric Zarr with explicit validity masks'
        WHERE semantic_kind=? AND representation_kind='zarr'`).run(kind);
      db.query(`INSERT OR REPLACE INTO forge_event VALUES(?,?,?,?,?,?,?)`).run(
        sha(`zarr|${kind}|${started}`), "lower", `zarr:${kind}`, "physical",
        `${body.numeric_columns} numeric arrays plus validity; canonical Parquet unchanged`, started, at());
    })();
    return { kind, state: "physical", coordinate: directory, digest: body.digest,
      input_digest: root.digest, representation_id: id, numeric_columns: body.numeric_columns };
  } finally {
    if (stage && existsSync(stage)) rmSync(stage, { recursive: true });
    db.close();
  }
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2), verb = args.shift(), kind = args.shift();
    const values = new Map();
    for (let i = 0; i < args.length; i += 2) {
      if (!args[i]?.startsWith("--") || !args[i + 1] || values.has(args[i])) throw new Error("invalid Zarr option");
      values.set(args[i], args[i + 1]);
    }
    for (const option of values.keys()) if (!["--estate", "--binary", "--bundle", "--chunk-size"].includes(option))
      throw new Error(`unknown Zarr option: ${option}`);
    if (!["plan", "materialize"].includes(verb) || !kind ||
        (verb === "plan" && (values.has("--binary") || values.has("--bundle") || values.has("--chunk-size"))))
      throw new Error("usage: representation-manifold-zarr.mjs plan|materialize KIND [--estate ESTATE] [--binary NATIVE] [--bundle ARROW_BUNDLE] [--chunk-size N]");
    const estate = values.get("--estate") ?? join(homedir(), ".bonfyre", "estate-local");
    const result = verb === "plan" ? await planZarr(estate, kind) : await materializeZarr(estate, kind, {
      binary: values.get("--binary"), bundle: values.get("--bundle"),
      chunkSize: Number(values.get("--chunk-size") ?? 8192) });
    console.log(JSON.stringify(result, null, 2));
  } catch (error) { console.error(`zarr forge: ${error.message}`); process.exitCode = 1; }
}
