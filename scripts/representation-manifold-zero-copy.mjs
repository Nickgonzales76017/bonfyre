#!/usr/bin/env bun
// Native coordinator for the parts of the zero-copy pivot with local evidence.
// Iceberg add-files has no native writer in this repository and is never forged.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { createReadStream, existsSync, linkSync, lstatSync, mkdirSync, readFileSync, realpathSync, renameSync, statfsSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { pathToFileURL } from "node:url";
import { fileURLToPath } from "node:url";
import { bodyDigest } from "./columnar_body.mjs";
import { registerVirtualGraph } from "./representation-manifold-forge.mjs";
import { materializeDelta } from "./representation-manifold-delta.mjs";
import { runNativeFabric } from "./representation-manifold-fabric.mjs";
import { reclaimDerived } from "./representation-manifold-gc.mjs";

const sha = value => createHash("sha256").update(value).digest("hex");
const now = () => new Date().toISOString();
const freeBytes = path => { const s = statfsSync(path); return s.bavail * s.bsize; };
const inside = (root, path) => { const r = relative(root, path); return r && r !== ".." && !r.startsWith(`..${sep}`) && !isAbsolute(r); };
const sqlQuote = value => `'${String(value).replaceAll("'", "''")}'`;
const nativeGaps = Object.freeze([
  "Iceberg add-files: no spec-valid native writer/manifest reader is installed",
  "DataFusion, Polars and ADBC: no native executable probe is registered by this coordinator",
]);

let cachedBytePolicy;
function bytePolicy() {
  if (cachedBytePolicy) return cachedBytePolicy;
  const cue = join(dirname(fileURLToPath(import.meta.url)), "..", "schemas", "storage", "representation-portfolio.cue");
  const child = Bun.spawnSync(["cue", "export", cue, "-e", "representationPortfolio.byte_economics", "--out", "json"],
    { stdout: "pipe", stderr: "pipe" });
  if (child.exitCode !== 0) throw new Error(`authored byte policy unavailable: ${child.stderr.toString().trim()}`);
  const rows = JSON.parse(child.stdout.toString());
  if (!rows || typeof rows !== "object" || Array.isArray(rows) || !Object.keys(rows).length)
    throw new Error("authored byte policy is empty");
  for (const [kind, row] of Object.entries(rows)) {
    if (!kind || !row || typeof row !== "object" ||
        ["byte_policy", "preferred_temperature", "canonical_body_kind", "rationale"]
          .some(field => typeof row[field] !== "string" || !row[field]))
      throw new Error(`invalid authored byte policy: ${kind}`);
  }
  cachedBytePolicy = rows;
  return rows;
}

function ensureTables(db) {
  db.exec(`PRAGMA busy_timeout=30000;
    CREATE TABLE IF NOT EXISTS metadata_overlay(
      semantic_kind TEXT NOT NULL,format TEXT NOT NULL,source_representation_id TEXT NOT NULL,
      source_coordinate TEXT NOT NULL,coordinate TEXT,state TEXT NOT NULL,detail TEXT,
      created_at TEXT NOT NULL,PRIMARY KEY(semantic_kind,format));
    CREATE TABLE IF NOT EXISTS zero_copy_event(
      id TEXT PRIMARY KEY,kind TEXT NOT NULL,semantic_kind TEXT,state TEXT NOT NULL,
      detail TEXT,occurred_at TEXT NOT NULL);`);
  const at = now();
  for (const [kind, row] of Object.entries(bytePolicy()))
    db.query("INSERT OR IGNORE INTO byte_economics VALUES(?,?,?,?,?,?)")
      .run(kind, row.byte_policy, row.preferred_temperature, row.canonical_body_kind, row.rationale, at);
}

async function roots(db) {
  const rows = db.query(`SELECT ri.id,ri.schema_ref kind,ri.digest,ri.frontier,pr.coordinate,pr.digest physical_digest
    FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
    WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1 ORDER BY ri.schema_ref,ri.id`).all();
  const seen = new Set();
  for (const row of rows) {
    if (seen.has(row.kind)) throw new Error(`ambiguous active Parquet roots for ${row.kind}`);
    seen.add(row.kind);
    const s = lstatSync(row.coordinate);
    if (!s.isFile() || s.isSymbolicLink() || !/^[0-9a-f]{64}$/.test(row.digest) ||
        row.physical_digest !== row.digest || await bodyDigest(row.coordinate) !== row.digest)
      throw new Error(`canonical Parquet digest drift: ${row.kind}`);
  }
  return rows;
}

export async function planZeroCopy(estate) {
  estate = resolve(estate);
  const path = join(estate, "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing manifold: ${path}`);
  const db = new Database(path, { readonly: true, create: false });
  try {
    const canonical = await roots(db);
    const targets = db.query("SELECT semantic_kind,representation_kind,status FROM representation_target ORDER BY semantic_kind,representation_kind").all();
    return { estate, canonical, targets, native_gaps: nativeGaps };
  } finally { db.close(); }
}

async function hashFile(path) {
  const h = createHash("sha256");
  for await (const chunk of createReadStream(path)) h.update(chunk);
  return h.digest("hex");
}

// Only digest-registered canonical Parquet bodies are eligible. Delta's clone
// must keep its own inode; metadata/log files may be mutable and are excluded.
export async function hardlinkExactParquet(estate, { minBytes = 1 << 20 } = {}) {
  if (!Number.isSafeInteger(minBytes) || minBytes < 0) throw new Error("invalid dedup minimum");
  // Observed verification work, not a claim of physical bytes fetched from
  // disk (OS page cache is allowed) or of reimbursable cloud costs.
  let completedDigests = 0, logicalHashed = 0n;
  const measuredHash = async (file, logicalBytes) => {
    const digest = await hashFile(file);
    completedDigests++;
    logicalHashed += BigInt(logicalBytes);
    return digest;
  };
  const verification = () => ({
    sha256_scans: completedDigests,
    logical_bytes_hashed: logicalHashed.toString(),
    physical_io_bytes: null,
    physical_space_reclaimed_bytes: null,
    boundary: "logical SHA input work; page-cache I/O and actual reclaim are unmeasured"
  });
  const roots = ["bodies/parquet", "parquet"].map(part => resolve(estate, "representations", part))
    .filter(path => existsSync(path) && lstatSync(path).isDirectory() && !lstatSync(path).isSymbolicLink())
    .map(realpathSync);
  if (!roots.length) return { eligible: 0, links: 0, logical_saved_bytes: 0, verification: verification() };
  const db = new Database(join(resolve(estate), "representation-manifold.db"), { readonly: true, create: false });
  try {
    const records = db.query(`SELECT ri.digest,pr.coordinate,pr.digest physical_digest FROM representation_instance ri
      JOIN physical_realization pr ON pr.representation_id=ri.id WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1`).all();
    const buckets = new Map();
    for (const row of records) {
      const path = resolve(row.coordinate);
      if (!existsSync(path) || !roots.some(root => inside(root, realpathSync(path))) ||
          !path.endsWith(".parquet") ||
          row.digest !== row.physical_digest || !/^[0-9a-f]{64}$/.test(row.digest)) continue;
      const s = lstatSync(path);
      if (!s.isFile() || s.isSymbolicLink() || s.size < minBytes) continue;
      if (await measuredHash(path, s.size) !== row.digest) throw new Error(`registered Parquet digest drift: ${path}`);
      const key = `${s.dev}|${s.size}|${row.digest}`;
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key).push({ path, stat: s, digest: row.digest });
    }
    let links = 0, saved = 0, eligible = 0;
    for (const entries of buckets.values()) {
      const distinct = [...new Map(entries.map(row => [row.path, row])).values()].sort((a, b) => a.path.localeCompare(b.path));
      if (distinct.length < 2) continue;
      eligible += distinct.length;
      const source = distinct[0];
      for (const duplicate of distinct.slice(1)) {
        const left = lstatSync(source.path), right = lstatSync(duplicate.path);
        if (left.ino === right.ino) continue;
        if (left.dev !== right.dev || left.size !== right.size || left.uid !== right.uid ||
            left.gid !== right.gid || left.mode !== right.mode ||
            left.ino !== source.stat.ino || right.ino !== duplicate.stat.ino ||
            await measuredHash(source.path, left.size) !== source.digest ||
            await measuredHash(duplicate.path, right.size) !== source.digest)
          throw new Error(`Parquet changed during exact dedup: ${duplicate.path}`);
        const temporary = `${duplicate.path}.bonfyre-dedup-${process.pid}`;
        if (existsSync(temporary)) throw new Error(`dedup staging path occupied: ${temporary}`);
        linkSync(source.path, temporary);
        try {
          if (lstatSync(duplicate.path).ino !== duplicate.stat.ino || lstatSync(source.path).ino !== source.stat.ino)
            throw new Error(`Parquet changed before exact dedup replacement: ${duplicate.path}`);
          renameSync(temporary, duplicate.path);
        } finally { if (existsSync(temporary)) unlinkSync(temporary); }
        links++;
        saved += right.size;
      }
    }
    return { eligible, links, logical_saved_bytes: saved, verification: verification() };
  } finally { db.close(); }
}

function safeName(value) {
  let name = String(value).replace(/[^A-Za-z0-9_]+/g, "_");
  if (!name || /^[0-9]/.test(name)) name = `_${name}`;
  return name.slice(0, 180);
}
const yaml = value => JSON.stringify(String(value));

export async function stageXTableConfigs(estate) {
  estate = resolve(estate);
  const db = new Database(join(estate, "representation-manifold.db"));
  try {
    ensureTables(db);
    const canonical = await roots(db);
    const ready = [], targetRoot = join(estate, "representations", "xtable-configs");
    const names = new Set();
    for (const row of canonical) {
      const overlay = db.query("SELECT * FROM metadata_overlay WHERE semantic_kind=? AND format='iceberg' AND state='physical_zero_copy'").get(row.kind);
      if (!overlay) continue;
      if (overlay.source_representation_id !== row.id || overlay.source_coordinate !== row.coordinate)
        throw new Error(`stale Iceberg overlay source: ${row.kind}`);
      const metadata = overlay.coordinate?.startsWith("file://") ? fileURLToPath(overlay.coordinate) : overlay.coordinate;
      if (!metadata || !existsSync(metadata) || !lstatSync(metadata).isFile() || lstatSync(metadata).isSymbolicLink())
        throw new Error(`Iceberg metadata unavailable: ${row.kind}`);
      const metadataRoot = join(estate, "representations", "iceberg-zero-copy");
      if (!existsSync(metadataRoot) || lstatSync(metadataRoot).isSymbolicLink() ||
          !inside(realpathSync(metadataRoot), realpathSync(metadata)))
        throw new Error(`Iceberg metadata escaped its estate root: ${row.kind}`);
      let header;
      try { header = JSON.parse(readFileSync(metadata, "utf8")); }
      catch { throw new Error(`Iceberg metadata is not JSON: ${row.kind}`); }
      if (![1, 2].includes(header["format-version"]) || typeof header["table-uuid"] !== "string" ||
          !Array.isArray(header.schemas) || !header.schemas.length)
        throw new Error(`Iceberg metadata header is incomplete: ${row.kind}`);
      // Existing PyIceberg metadata is admitted as an upstream receipt. This
      // coordinator does not claim to verify its Avro manifest or create it.
      if (await bodyDigest(row.coordinate) !== row.digest) throw new Error(`canonical Parquet drift: ${row.kind}`);
      const table = safeName(row.kind).toLowerCase();
      if (names.has(table)) throw new Error(`XTable table-name collision: ${table}`);
      names.add(table);
      const base = basename(metadata).endsWith(".json") ? dirname(dirname(metadata)) : dirname(metadata);
      const body = `sourceFormat: ICEBERG\ntargetFormats:\n  - DELTA\n  - HUDI\ndatasets:\n  - tableBasePath: ${yaml(pathToFileURL(base).href)}\n    tableDataPath: ${yaml(pathToFileURL(dirname(row.coordinate)).href)}\n    tableName: ${yaml(table)}\n`;
      const config = join(targetRoot, `${table}.yaml`);
      mkdirSync(targetRoot, { recursive: true });
      if (existsSync(config) && readFileSync(config, "utf8") !== body)
        throw new Error(`XTable config differs from existing file: ${config}`);
      if (!existsSync(config)) writeFileSync(config, body, { flag: "wx" });
      db.query("INSERT OR REPLACE INTO metadata_overlay VALUES(?,?,?,?,?,?,?,?)").run(
        row.kind, "xtable-delta-hudi", row.id, row.coordinate, config, "config_ready",
        "XTable config over existing Iceberg metadata; no Delta/Hudi result asserted", now());
      for (const kind of ["delta", "hudi"])
        db.query(`UPDATE representation_target SET status='metadata_overlay_ready',reason='XTable config ready; target metadata not verified'
          WHERE semantic_kind=? AND representation_kind=? AND status NOT IN ('physical','physical_zero_copy')`).run(row.kind, kind);
      ready.push({ kind: row.kind, config });
    }
    return ready;
  } finally { db.close(); }
}

export async function runXTable(estate, configs, jar) {
  if (!jar || !existsSync(jar) || !lstatSync(jar).isFile()) throw new Error("--xtable-jar must name an installed XTable jar");
  const spark = Bun.which("spark-submit");
  if (!spark) throw new Error("spark-submit is required to run XTableSparkSync");
  const db = new Database(join(resolve(estate), "representation-manifold.db"));
  try {
    ensureTables(db);
    const results = [];
    for (const { kind, config } of configs) {
      const child = Bun.spawn([spark, "--class", "org.apache.xtable.spark.XTableSparkSync",
        "--master", "local[*]", resolve(jar), "--datasetconfig", config],
        { stdin: "ignore", stdout: "pipe", stderr: "pipe" });
      let timedOut = false;
      const timer = setTimeout(() => { timedOut = true; child.kill(); }, 7_200_000);
      const output = new Response(child.stdout).text(), diagnostic = new Response(child.stderr).text();
      let exitCode;
      try { exitCode = await child.exited; }
      finally { clearTimeout(timer); }
      const state = !timedOut && exitCode === 0 ? "conversion_exited_zero_unverified" : "failed";
      const detail = `${timedOut ? "XTable timed out after 7200 seconds\n" : ""}${(await diagnostic) || (await output)}`.slice(-12000);
      const at = now();
      db.query("INSERT INTO zero_copy_event VALUES(?,?,?,?,?,?)").run(sha(`xtable|${kind}|${at}|${Math.random()}`),
        "xtable", kind, state, detail, at);
      results.push({ kind, state, exit_code: exitCode, detail });
    }
    return results;
  } finally { db.close(); }
}

export async function probeDuckDB(estate) {
  estate = resolve(estate);
  const db = new Database(join(estate, "representation-manifold.db"));
  try {
    const canonical = await roots(db), result = [];
    for (const row of canonical) {
      const query = `SELECT count(*) n FROM (SELECT * FROM read_parquet(${sqlQuote(row.coordinate)}) LIMIT 1)`;
      const child = Bun.spawnSync(["duckdb", "-json", ":memory:", query], { stdout: "pipe", stderr: "pipe" });
      if (child.exitCode !== 0) { result.push({ kind: row.kind, state: "failed", detail: child.stderr.toString().trim() }); continue; }
      const parsed = JSON.parse(child.stdout.toString());
      if (!Array.isArray(parsed) || parsed.length !== 1 || ![0, 1].includes(Number(parsed[0].n)) ||
          await bodyDigest(row.coordinate) !== row.digest)
        throw new Error(`DuckDB probe or canonical Parquet drift: ${row.kind}`);
      const proof = sha(`duckdb-probe.v1|${row.id}|${row.digest}|${query}|${parsed[0].n}`);
      db.query("INSERT OR REPLACE INTO execution_binding VALUES(?,?,?,?,?,?)")
        .run(row.kind, "duckdb", row.coordinate, "executable", proof, now());
      result.push({ kind: row.kind, state: "executable", proof_digest: proof });
    }
    return result;
  } finally { db.close(); }
}

const human = n => { let x = Number(n) || 0; for (const u of ["B", "KiB", "MiB", "GiB", "TiB"]) {
  if (x < 1024 || u === "TiB") return `${x.toFixed(1)}${u}`; x /= 1024;
} };

export function writeZeroCopyReport(estate, before, after, extra = {}) {
  estate = resolve(estate);
  const db = new Database(join(estate, "representation-manifold.db"), { readonly: true, create: false });
  try {
    const object = { schema: "bonfyre-zero-copy-pivot.v1", generated_at: now(), free_before: before, free_after: after,
      physical_kinds: db.query("SELECT kind,count(*) n FROM representation_instance WHERE state='physical' GROUP BY kind ORDER BY n DESC").all().map(r => [r.kind,r.n]),
      executable_kinds: db.query("SELECT kind,count(*) n FROM representation_instance WHERE state='executable' GROUP BY kind ORDER BY n DESC").all().map(r => [r.kind,r.n]),
      evictions: db.query("SELECT kind,count(*) n,sum(bytes) b FROM eviction_receipt GROUP BY kind ORDER BY b DESC").all().map(r => [r.kind,r.n,r.b]),
      overlays: db.query("SELECT format,state,count(*) n FROM metadata_overlay GROUP BY format,state ORDER BY format,state").all().map(r => [r.format,r.state,r.n]),
      target_states: db.query("SELECT status,count(*) n FROM representation_target GROUP BY status ORDER BY status").all().map(r => [r.status,r.n]),
      virtual_graphs: db.query("SELECT count(*) n FROM virtual_graph_projection_v2").get().n,
      native_gaps: nativeGaps, ...extra };
    const root = join(estate, "representations"); mkdirSync(root, { recursive: true });
    writeFileSync(join(root, "zero-copy-report.json"), `${JSON.stringify(object, null, 2)}\n`);
    const lines = ["BONFŸRE SHIFT 1.5 — ZERO-COPY PIVOT", "=".repeat(84), `free before: ${human(before)}`, `free after:  ${human(after)}`,
      "", "EVICTIONS", ...object.evictions.map(([k,n,b]) => `  ${k.padEnd(22)} ${String(n).padStart(5)} ${human(b)}`),
      "", "METADATA OVERLAYS", ...object.overlays.map(([f,s,n]) => `  ${f.padEnd(24)} ${s.padEnd(24)} ${n}`),
      "", "PHYSICAL", ...object.physical_kinds.map(([k,n]) => `  ${k.padEnd(24)} ${n}`),
      "", "EXECUTABLE", ...object.executable_kinds.map(([k,n]) => `  ${k.padEnd(24)} ${n}`),
      "", `virtual graph projections: ${object.virtual_graphs}`, "", "TARGET STATES",
      ...object.target_states.map(([s,n]) => `  ${s.padEnd(30)} ${n}`), "", "NATIVE GAPS", ...nativeGaps.map(g => `  ${g}`)];
    writeFileSync(join(root, "zero-copy-report.txt"), `${lines.join("\n")}\n`);
    return object;
  } finally { db.close(); }
}

export async function applyZeroCopy(options) {
  const estate = resolve(options.estate);
  if (!options.bundle || !existsSync(join(resolve(options.bundle), "manifest.json")))
    throw new Error("--native-arrow-bundle with manifest.json is required before mutation");
  if (options.runXTable && (!options.xtableJar || !existsSync(options.xtableJar) ||
      !lstatSync(options.xtableJar).isFile()))
    throw new Error("--run-xtable requires --xtable-jar pointing to an installed jar");
  if (options.runXTable && !Bun.which("spark-submit"))
    throw new Error("--run-xtable requires spark-submit for XTableSparkSync");
  const plan = await planZeroCopy(estate);
  const iceberg = plan.targets.filter(row => row.representation_kind === "iceberg" && row.status !== "physical_zero_copy");
  if (iceberg.length && !options.allowPartial)
    throw new Error(`${iceberg.length} Iceberg targets require add-files; native writer unavailable (use --allow-partial for supported stages)`);
  const before = freeBytes(estate);
  const binding = await runNativeFabric(["bind", "--bundle", resolve(options.bundle), "--estate", estate]);
  if (binding.state !== "INSTALLED_UNSERVED" || binding.tables !== plan.canonical.length)
    throw new Error(`Arrow binding did not cover ${plan.canonical.length} roots`);
  const db = new Database(join(estate, "representation-manifold.db"));
  try { ensureTables(db); } finally { db.close(); }
  const gc = options.reclaimDerived ? await reclaimDerived(estate, { kinds: ["arrow", "delta",
    ...(options.reclaimLance ? ["lance"] : []), ...(options.reclaimZarr ? ["zarr"] : [])],
    floorBytes: options.freeFloorGb * 1024 ** 3 }) : null;
  const dedup = await hardlinkExactParquet(estate);
  const graphs = [];
  for (const row of plan.canonical) {
    try { graphs.push(await registerVirtualGraph(estate, row.kind)); }
    catch (error) {
      if (!options.allowPartial) throw error;
      graphs.push({ kind: row.kind, state: "unavailable", reason: error.message });
    }
  }
  const duckdb = await probeDuckDB(estate);
  if (!options.allowPartial && duckdb.some(row => row.state !== "executable"))
    throw new Error("DuckDB probe failed for one or more canonical roots");
  const xtableConfigs = await stageXTableConfigs(estate);
  const xtable = options.runXTable ? await runXTable(estate, xtableConfigs, options.xtableJar) : [];
  if (!options.allowPartial && xtable.some(row => row.state === "failed"))
    throw new Error("XTable conversion failed for one or more configs");
  const delta = [];
  if (options.materializeDelta) for (const row of plan.targets.filter(row => row.representation_kind === "delta")) {
    delta.push(await materializeDelta(estate, row.semantic_kind));
  }
  const after = freeBytes(estate);
  const report = writeZeroCopyReport(estate, before, after, { binding, gc, dedup, graphs, duckdb, xtable_configs: xtableConfigs,
    xtable, delta, iceberg_unavailable: iceberg.map(row => row.semantic_kind) });
  return report;
}

function argumentsFor(argv) {
  const known = new Set(["--estate", "--native-arrow-bundle", "--free-floor-gb", "--xtable-jar"]);
  const flags = new Set(["--apply", "--worker", "--allow-partial", "--reclaim-derived", "--reclaim-lance", "--reclaim-zarr", "--run-xtable", "--materialize-delta"]);
  const values = new Map(), selected = new Set();
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (flags.has(key)) selected.add(key);
    else if (known.has(key) && i + 1 < argv.length && !argv[i + 1].startsWith("--")) values.set(key, argv[++i]);
    else throw new Error(`invalid option: ${key}`);
  }
  const freeFloorGb = Number(values.get("--free-floor-gb") ?? 4);
  if (!Number.isFinite(freeFloorGb) || freeFloorGb < 0) throw new Error("invalid --free-floor-gb");
  return { estate: values.get("--estate") ?? join(homedir(), ".bonfyre", "estate-local"),
    bundle: values.get("--native-arrow-bundle"), freeFloorGb,
    xtableJar: values.get("--xtable-jar"), apply: selected.has("--apply") || selected.has("--worker"),
    allowPartial: selected.has("--allow-partial"), reclaimDerived: selected.has("--reclaim-derived"),
    reclaimLance: selected.has("--reclaim-lance"), reclaimZarr: selected.has("--reclaim-zarr"),
    runXTable: selected.has("--run-xtable"), materializeDelta: selected.has("--materialize-delta") };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    const options = argumentsFor(process.argv.slice(2));
    console.log(JSON.stringify(options.apply ? await applyZeroCopy(options) : await planZeroCopy(options.estate), null, 2));
  } catch (error) { console.error(`representation-zero-copy: ${error.message}`); process.exitCode = 1; }
}
