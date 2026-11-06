#!/usr/bin/env bun
// Local ADBC Flight SQL driver ABI admission. This does not open a database,
// execute SQL, or prove that any Flight SQL endpoint is serving.
import { Database } from "bun:sqlite";
import { existsSync, lstatSync, readFileSync } from "node:fs";
import { resolve, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { bodyDigest } from "./columnar_body.mjs";

const schemaPath = join(import.meta.dir, "..", "schemas/storage/representation-manifold.sql");

function regular(path, label) {
  if (!existsSync(path)) throw new Error(`${label} missing: ${path}`);
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error(`${label} must be a regular non-symlink file: ${path}`);
}

export async function probeAdbcDriver({ driver, probe }) {
  if (!driver || !probe) throw new Error("explicit --driver and --probe are required");
  driver = resolve(driver); probe = resolve(probe);
  regular(driver, "ADBC driver"); regular(probe, "ADBC probe");
  const [driverDigest, probeDigest] = await Promise.all([bodyDigest(driver), bodyDigest(probe)]);
  const run = Bun.spawnSync([probe, driver], { stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  if (run.exitCode !== 0) throw new Error(`native ADBC ABI probe failed: ${run.stderr.toString().trim()}`);
  const evidence = JSON.parse(run.stdout.toString());
  if (evidence.state !== "driver_initialized_unserved" ||
      ![1000000, 1001000].includes(evidence.abi_version))
    throw new Error("native ADBC probe returned an invalid ABI receipt");
  if (await bodyDigest(driver) !== driverDigest || await bodyDigest(probe) !== probeDigest)
    throw new Error("ADBC driver or probe changed during verification");
  return { state: evidence.state, abi_version: evidence.abi_version,
    driver, driver_digest: driverDigest, probe, probe_digest: probeDigest };
}

export async function bindAdbcDriver(estate, options) {
  const receipt = await probeAdbcDriver(options), path = join(resolve(estate), "representation-manifold.db");
  regular(path, "manifold");
  const db = new Database(path);
  try {
    db.exec(readFileSync(schemaPath, "utf8"));
    db.transaction(() => {
      db.query("DELETE FROM metadata_fabric WHERE kind=? AND coordinate=?")
        .run("adbc-flight-sql-client", "python://adbc_driver_flightsql");
      db.query("INSERT OR REPLACE INTO metadata_fabric VALUES(?,?,?,?,?)")
        .run("adbc-flight-sql-client", pathToFileURL(receipt.driver).href, receipt.state,
          JSON.stringify({ abi_version: receipt.abi_version, driver_digest: receipt.driver_digest,
            probe: receipt.probe, probe_digest: receipt.probe_digest,
            limitation: "driver ABI only; no SQL execution or Flight SQL service" }),
          new Date().toISOString());
    })();
    return receipt;
  } finally { db.close(); }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    const [command, ...args] = process.argv.slice(2), values = new Map();
    if (!["plan", "bind"].includes(command)) throw new Error("usage: adbc plan|bind --driver LIB --probe BIN [--estate ESTATE]");
    for (let i = 0; i < args.length; i += 2) {
      if (!args[i]?.startsWith("--") || !args[i + 1] || values.has(args[i]))
        throw new Error(`invalid ADBC option: ${args[i]}`);
      values.set(args[i], args[i + 1]);
    }
    for (const key of values.keys()) if (!["--driver", "--probe", "--estate"].includes(key))
      throw new Error(`unknown ADBC option: ${key}`);
    if (command === "plan" && values.has("--estate")) throw new Error("plan does not accept --estate");
    if (command === "bind" && !values.has("--estate")) throw new Error("bind requires --estate");
    const options = { driver: values.get("--driver"), probe: values.get("--probe") };
    const receipt = command === "plan" ? await probeAdbcDriver(options)
      : await bindAdbcDriver(values.get("--estate"), options);
    console.log(JSON.stringify(receipt));
  } catch (error) { console.error(`representation-adbc: ${error.message}`); process.exitCode = 1; }
}
