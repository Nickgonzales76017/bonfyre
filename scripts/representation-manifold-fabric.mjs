#!/usr/bin/env bun
// Native Arrow process/transport boundary. Semantics remain in the manifold.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readFileSync, renameSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { verifyArrowRuntime, verifyArrowRuntimeFast } from "./arrow-runtime-verify.mjs";
import { bodyDigest } from "./columnar_body.mjs";

const sha = value => createHash("sha256").update(value).digest("hex");
const now = () => new Date().toISOString();
const legacyXTableJar = "xtable-spark-runtime_2.12-0.4.0-incubating.jar";

function applyOptions(args) {
  const values = new Map(), flags = new Set();
  const accepted = new Set(["--estate", "--runtime", "--bundle", "--xtable-jar",
    "--xtable-url", "--xtable-sha256", "--adbc-driver", "--adbc-probe"]);
  for (let i = 0; i < args.length; i++) {
    const key = args[i];
    if (["--run-xtable", "--allow-network"].includes(key) && !flags.has(key)) flags.add(key);
    else if (accepted.has(key) && !values.has(key) && args[i + 1] && !args[i + 1].startsWith("--"))
      values.set(key, args[++i]);
    else throw new Error(`invalid fabric apply option: ${key}`);
  }
  if (Boolean(values.has("--adbc-driver")) !== Boolean(values.has("--adbc-probe")))
    throw new Error("ADBC requires both --adbc-driver and --adbc-probe");
  if (values.has("--xtable-url") &&
      (!values.has("--xtable-sha256") || !flags.has("--allow-network")))
    throw new Error("XTable download requires --allow-network, --xtable-url and --xtable-sha256");
  if (flags.has("--allow-network") && (!values.has("--xtable-url") || values.has("--xtable-jar")))
    throw new Error("network staging requires a pinned URL and no local --xtable-jar");
  if (values.has("--xtable-sha256") && !/^[0-9a-f]{64}$/.test(values.get("--xtable-sha256")))
    throw new Error("--xtable-sha256 must be a lowercase SHA-256 digest");
  if (values.has("--xtable-url") && !/^https:\/\//.test(values.get("--xtable-url")))
    throw new Error("--xtable-url must use HTTPS");
  return { estate: resolve(values.get("--estate") ?? join(homedir(), ".bonfyre/estate-local")),
    runtime: resolve(values.get("--runtime") ?? join(homedir(), ".bonfyre/representation-runtime")),
    bundle: values.get("--bundle") && resolve(values.get("--bundle")),
    xtableJar: values.get("--xtable-jar") && resolve(values.get("--xtable-jar")),
    xtableUrl: values.get("--xtable-url"), xtableSha256: values.get("--xtable-sha256"),
    adbcDriver: values.get("--adbc-driver"), adbcProbe: values.get("--adbc-probe"),
    runXTable: flags.has("--run-xtable") };
}

export async function stageXTableRuntime(options, fetcher = fetch) {
  const root = join(options.runtime, "xtable");
  let jar = options.xtableJar ?? join(root, legacyXTableJar);
  if (options.xtableUrl) {
    if (existsSync(root) && (lstatSync(root).isSymbolicLink() || !lstatSync(root).isDirectory()))
      throw new Error(`unsafe XTable runtime directory: ${root}`);
    mkdirSync(root, { recursive: true });
    jar = join(root, `${options.xtableSha256}.jar`);
    if (!existsSync(jar)) {
      const response = await fetcher(options.xtableUrl);
      if (!response.ok) throw new Error(`XTable download failed: HTTP ${response.status}`);
      const temporary = join(root, `.download-${process.pid}-${crypto.randomUUID()}`);
      try {
        await Bun.write(temporary, response);
        const { bodyDigest } = await import("./columnar_body.mjs");
        if (await bodyDigest(temporary) !== options.xtableSha256)
          throw new Error("XTable downloaded jar digest mismatch");
        renameSync(temporary, jar);
      } finally { if (existsSync(temporary)) rmSync(temporary); }
    }
  }
  if (existsSync(jar) && (!lstatSync(jar).isFile() || lstatSync(jar).isSymbolicLink()))
    throw new Error(`XTable jar must be a regular non-symlink file: ${jar}`);
  const present = existsSync(jar), spark = Bun.which("spark-submit");
  if (options.xtableSha256 && !present) throw new Error(`pinned XTable jar missing: ${jar}`);
  if (options.runXTable && (!present || !spark))
    throw new Error("--run-xtable requires an installed jar and spark-submit");
  let digest = null;
  if (present) {
    const { bodyDigest } = await import("./columnar_body.mjs");
    digest = await bodyDigest(jar);
    if (options.xtableSha256 && digest !== options.xtableSha256)
      throw new Error("XTable jar digest mismatch");
  }
  return { jar, digest, spark, state: `${present ? "runtime_staged" : "jar_missing"}+${spark ? "spark_ready" : "spark_missing"}` };
}

export async function applyFabric(options) {
  const estate = resolve(options.estate), dbPath = join(estate, "representation-manifold.db");
  if (!existsSync(dbPath) || !lstatSync(dbPath).isFile() || lstatSync(dbPath).isSymbolicLink())
    throw new Error(`missing or unsafe manifold: ${dbPath}`);
  const db = new Database(dbPath, { readonly: true, create: false });
  let expected, canonical;
  try {
    expected = db.query("SELECT count(*) n FROM representation_target WHERE representation_kind='substrait'").get().n;
    canonical = db.query(`SELECT count(*) n FROM representation_instance ri
      JOIN physical_realization pr ON pr.representation_id=ri.id
      WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1`).get().n;
  } finally { db.close(); }
  // Preflight explicit binaries and downloaded material before writing any
  // manifold receipts. The existing owners verify canonical bodies themselves.
  const runtime = await stageXTableRuntime(options);
  if (options.adbcDriver) {
    const { probeAdbcDriver } = await import("./representation-manifold-adbc.mjs");
    await probeAdbcDriver({ driver: options.adbcDriver, probe: options.adbcProbe });
  }
  if (options.bundle) await verifyArrowRuntime(options.bundle);
  const { materializeAllSubstrait } = await import("./representation-manifold-substrait.mjs");
  const { bindNativeFsCarrier } = await import("./representation-manifold-carrier.mjs");
  const { stageXTableConfigs, runXTable } = await import("./representation-manifold-zero-copy.mjs");
  const { writeFabricReport } = await import("./representation-manifold-report.mjs");
  const substrait = await materializeAllSubstrait(estate);
  if (substrait.state !== "MATERIALIZED" || substrait.plans !== expected)
    throw new Error("native Substrait compilation returned an unexpected target frontier");
  const flight = options.bundle ? await runNativeFabric(["bind", "--bundle", options.bundle, "--estate", estate])
    : { state: "native_bundle_required", tables: 0 };
  if (options.bundle && (flight.state !== "INSTALLED_UNSERVED" || flight.tables !== canonical))
    throw new Error("native Flight registration returned an unexpected frontier");
  const carrier = await bindNativeFsCarrier(estate);
  if (carrier.state !== "BOUND_VERIFIED" || carrier.representations !== canonical)
    throw new Error("native filesystem carrier returned an unexpected frontier");
  const xtableConfigs = await stageXTableConfigs(estate);
  const xtable = options.runXTable ? await runXTable(estate, xtableConfigs, runtime.jar) : [];
  if (runtime.digest) {
    const { bodyDigest } = await import("./columnar_body.mjs");
    if (await bodyDigest(runtime.jar) !== runtime.digest) throw new Error("XTable jar changed during Fabric apply");
  }
  const receipt = new Database(dbPath);
  try {
    receipt.query("INSERT OR REPLACE INTO metadata_fabric VALUES(?,?,?,?,?)")
      .run("xtable-runtime", runtime.jar, runtime.state, JSON.stringify({ spark: runtime.spark,
        sha256: runtime.digest,
        config_count: xtableConfigs.length, limitation: "conversion output unverified" }), now());
  } finally { receipt.close(); }
  const adbc = options.adbcDriver ? await (await import("./representation-manifold-adbc.mjs"))
    .bindAdbcDriver(estate, { driver: options.adbcDriver, probe: options.adbcProbe })
    : { state: "explicit_native_driver_and_probe_required" };
  const report = writeFabricReport(estate);
  return { state: xtable.some(row => row.state === "failed") ? "PARTIAL_XTABLE_FAILURE" : "APPLIED",
    canonical_roots: canonical, substrait, flight, carrier,
    xtable: { runtime, configs: xtableConfigs, runs: xtable }, adbc, report: { state: report.state,
      json: report.json, text: report.text } };
}

function options(args) {
  if (!args.length || !["bind", "stream", "serve"].includes(args[0]))
    throw new Error("usage: fabric bind|stream|serve --bundle NATIVE_BUNDLE [--estate ESTATE] [--catalog BOUND_CATALOG] [--kind SEMANTIC_KIND] [--batch-size N] [--strict-runtime true] [--host LOOPBACK] [--port N]");
  const values = new Map();
  for (let i = 1; i < args.length; i += 2) {
    if (!args[i].startsWith("--") || i + 1 >= args.length || values.has(args[i]))
      throw new Error(`invalid fabric option: ${args[i]}`);
    values.set(args[i], args[i + 1]);
  }
  const accepted = new Set(["--bundle", "--estate", "--catalog", "--kind", "--batch-size", "--strict-runtime", "--host", "--port"]);
  for (const key of values.keys()) if (!accepted.has(key)) throw new Error(`unknown fabric option: ${key}`);
  if (!values.get("--bundle")) throw new Error("--bundle is required; the native runtime is never implicit");
  const command = args[0], estate = resolve(values.get("--estate") ?? join(homedir(), ".bonfyre/estate-local"));
  const bundle = resolve(values.get("--bundle"));
  const catalog = values.get("--catalog") ? resolve(values.get("--catalog")) : null;
  const kind = values.get("--kind");
  const batchSize = Number(values.get("--batch-size") ?? 65536);
  const host = values.get("--host") ?? "127.0.0.1";
  const port = Number(values.get("--port") ?? 8815);
  const strictRuntime = values.get("--strict-runtime") === "true";
  if (values.has("--strict-runtime") && (command !== "stream" || values.get("--strict-runtime") !== "true"))
    throw new Error("--strict-runtime true is supported only for stream");
  if (command === "stream" && !kind) throw new Error("stream requires --kind");
  if (command !== "bind" && !catalog) throw new Error(`${command} requires --catalog from an explicit bind`);
  if ((command !== "stream" && (kind || values.has("--batch-size"))) ||
      (command !== "serve" && (values.has("--host") || values.has("--port"))) ||
      (command === "bind" && catalog))
    throw new Error(`unsupported option for ${command}`);
  if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 1_000_000)
    throw new Error("batch size must be 1..1000000");
  if (!["127.0.0.1", "localhost", "::1"].includes(host))
    throw new Error("Arrow Flight has no remote authentication; host must be loopback");
  if (!Number.isSafeInteger(port) || port < 0 || port > 65535) throw new Error("port must be 0..65535");
  return { command, estate, bundle, catalog, kind, batchSize, host, port, strictRuntime };
}

function catalogAt(path, estate) {
  const root = join(estate, "representations", "flight");
  if (resolve(path, "..") !== root) throw new Error("Flight catalog must be inside this estate");
  const match = /^catalog-([0-9a-f]{64})\.json$/.exec(basename(path));
  if (!match || !existsSync(path) || !lstatSync(path).isFile() || lstatSync(path).isSymbolicLink())
    throw new Error("Flight catalog is not a registered immutable file");
  const body = readFileSync(path, "utf8");
  if (sha(body) !== match[1]) throw new Error("Flight catalog digest drift");
  const catalog = JSON.parse(body);
  if (!Array.isArray(catalog.tables)) throw new Error("native Flight catalog has no tables");
  return { digest: match[1], tables: catalog.tables };
}

export async function verifyRegisteredFabric(estate, bundle, catalogPath, selectedKind = null,
  verifySelectedBody = true, strictRuntime = true) {
  // Fast stream mode trusts the bundle immutability attested at explicit bind:
  // all files retain shape/size, and the selected IPC binary is rehashed. A
  // same-size hostile library rewrite is detected only by strict verification.
  const manifest = strictRuntime ? await verifyArrowRuntime(bundle)
    : await verifyArrowRuntimeFast(bundle, "bonfyre-arrow-stream");
  const { digest, tables } = catalogAt(catalogPath, estate);
  const selected = selectedKind ? tables.filter(row => row.semantic_kind === selectedKind) : tables;
  if (selectedKind && selected.length !== 1) throw new Error(`unknown or ambiguous semantic kind: ${selectedKind}`);
  const dbPath = join(estate, "representation-manifold.db");
  if (!existsSync(dbPath)) throw new Error(`missing manifold: ${dbPath}`);
  const db = new Database(dbPath, { readonly: true, create: false });
  try {
    const flight = db.query("SELECT coordinate,digest,state FROM executable_projection WHERE id=?").get(sha(`native-flight|${digest}`));
    if (!flight || flight.state !== "installed_verified" ||
        flight.coordinate !== join(bundle, "bonfyre-arrow-flight") ||
        flight.digest !== manifest.binaries["bonfyre-arrow-flight"].sha256)
      throw new Error("native Flight executable receipt is missing or stale");
    if (selectedKind) {
      const ipc = db.query("SELECT coordinate,digest,state FROM executable_projection WHERE id=?").get(sha(`native-ipc|${digest}`));
      if (!ipc || ipc.state !== "installed_verified" ||
          ipc.coordinate !== join(bundle, "bonfyre-arrow-stream") ||
          ipc.digest !== manifest.binaries["bonfyre-arrow-stream"].sha256)
        throw new Error("native IPC executable receipt is missing or stale");
    }
    for (const table of selected) {
      const roots = db.query(`SELECT ri.digest,ri.frontier,pr.coordinate,pr.digest physical_digest
        FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
        WHERE ri.kind='parquet' AND ri.state='physical' AND pr.available=1 AND ri.schema_ref=?`).all(table.semantic_kind);
      if (roots.length !== 1 || roots[0].digest !== table.digest ||
          roots[0].physical_digest !== table.digest ||
          roots[0].frontier !== table.frontier || roots[0].coordinate !== table.parquet)
        throw new Error(`registered canonical Parquet frontier changed: ${table.semantic_kind}`);
      const transport = db.query("SELECT coordinate,state,proof_digest FROM transport_binding WHERE semantic_kind=? AND transport='arrow-flight'").get(table.semantic_kind);
      if (!transport || transport.state !== "installed_unserved" ||
          transport.coordinate !== `flight://estate/${table.name}` ||
          transport.proof_digest !== sha(`flight|${digest}|${table.digest}|${table.frontier}`))
        throw new Error(`native Flight binding is missing or stale: ${table.semantic_kind}`);
      if (selectedKind) {
        const ipc = db.query("SELECT coordinate,state,proof_digest FROM execution_binding WHERE semantic_kind=? AND engine='arrow-ipc-on-demand'").get(table.semantic_kind);
        if (!ipc || ipc.state !== "installed_verified" ||
            ipc.coordinate !== join(bundle, "bonfyre-arrow-stream") ||
            ipc.proof_digest !== sha(`ipc|${table.digest}|${table.frontier}`))
          throw new Error(`native IPC binding is missing or stale: ${table.semantic_kind}`);
        if (verifySelectedBody && await bodyDigest(table.parquet) !== table.digest)
          throw new Error(`canonical Parquet drift: ${table.semantic_kind}`);
      }
    }
    return { digest, tables, selected: selected[0] ?? null };
  } finally { db.close(); }
}

export async function runNativeFabric(args) {
  const opts = options(args);
  if (opts.command === "bind") {
    const { bindArrowRuntime } = await import("./representation-manifold-shift.mjs");
    const binding = await bindArrowRuntime(opts.estate, opts.bundle);
    return { state: binding.state, catalog: binding.catalog, tables: binding.tables };
  }
  const binding = await verifyRegisteredFabric(opts.estate, opts.bundle, opts.catalog,
    opts.command === "stream" ? opts.kind : null, opts.command !== "stream",
    opts.command !== "stream" || opts.strictRuntime);
  const argv = opts.command === "stream"
    ? [join(opts.bundle, "bonfyre-arrow-stream"), binding.selected.parquet,
      "--batch-size", String(opts.batchSize), "--expected-sha256", binding.selected.digest]
    : [join(opts.bundle, "bonfyre-arrow-flight"), "--catalog", opts.catalog,
      "--host", opts.host, "--port", String(opts.port)];
  // The IPC writer hashes the same opened file descriptor before emitting
  // bytes. Do not hash in Bun too: that only adds a TOCTOU window and I/O.
  const child = Bun.spawn(argv, { stdin: "ignore", stdout: "inherit",
    stderr: opts.command === "stream" ? "pipe" : "inherit" });
  const diagnostic = opts.command === "stream" ? new Response(child.stderr).text() : Promise.resolve("");
  const terminate = () => child.kill();
  process.on("SIGINT", terminate);
  process.on("SIGTERM", terminate);
  try {
    const exitCode = await child.exited;
    if (exitCode !== 0) throw new Error(`${opts.command} native process exited ${exitCode}: ${(await diagnostic).trim()}`);
    return { state: "process_exited", exitCode };
  } finally {
    process.off("SIGINT", terminate);
    process.off("SIGTERM", terminate);
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    const answer = process.argv[2] === "apply"
      ? await applyFabric(applyOptions(process.argv.slice(3)))
      : await runNativeFabric(process.argv.slice(2));
    if (["bind", "apply"].includes(process.argv[2])) console.log(JSON.stringify(answer, null, 2));
  } catch (error) {
    console.error(`representation-fabric: ${error.message}`);
    process.exitCode = 1;
  }
}
