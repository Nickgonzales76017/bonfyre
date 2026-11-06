import { afterAll, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { appendFileSync, mkdtempSync, mkdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { createServer } from "node:net";
import { join, resolve } from "node:path";
import { bindArrowRuntime } from "../../../scripts/representation-manifold-shift.mjs";

const root = resolve(import.meta.dir, "../../..");
const include = process.env.ARROW_CPP_INCLUDE;
const arrow = process.env.ARROW_CPP_LIBARROW;
const parquet = process.env.ARROW_CPP_LIBPARQUET;
const flight = process.env.ARROW_CPP_LIBFLIGHT;
const simdjson = process.env.SIMDJSON_DYLIB;
const enabled = Boolean(include && arrow && parquet && flight && simdjson &&
  process.env.ARROW_CPP_LICENSE && process.env.ARROW_CPP_NOTICE &&
  process.env.SIMDJSON_LICENSE);
const scratch = enabled ? mkdtempSync(join(tmpdir(), "bonfyre-flight-")) : null;
afterAll(() => { if (scratch) rmSync(scratch, { recursive: true, force: true }); });

function run(argv) {
  const result = Bun.spawnSync(argv, { cwd: root, stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) {
    throw new Error(`${argv[0]} failed: ${result.stderr.toString()}`);
  }
  return result.stdout.toString();
}

test.skipIf(!enabled)("native Flight reads the canonical Parquet catalog", async () => {
  const file = join(scratch, "sample.parquet");
  const estate = join(scratch, "estate");
  mkdirSync(estate);
  const bundle = join(scratch, "bundle");
  const server = join(bundle, "bonfyre-arrow-flight");
  const writer = join(bundle, "bonfyre-arrow-stream");
  const client = join(scratch, "verify");
  const ipcVerify = join(scratch, "verify-ipc");
  const cDataVerify = join(scratch, "verify-c-data");
  const libs = [flight, parquet, arrow];
  const common = ["clang++", "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror",
    "-isystem", include, `-Wl,-rpath,${resolve(arrow, "..")}`];
  run(["duckdb", "-c", `COPY (SELECT 1::BIGINT AS id, 'alpha'::VARCHAR AS name,
    [1,2,3]::INTEGER[] AS tags, 12.34::DECIMAL(8,2) AS amount,
    DATE '2024-01-02' AS d UNION ALL SELECT 2, NULL, []::INTEGER[],
    -5.67::DECIMAL(8,2), DATE '2024-01-03') TO '${file}' (FORMAT PARQUET)`]);
  const canonical = Buffer.from(await Bun.file(file).arrayBuffer());
  const digest = createHash("sha256").update(canonical).digest("hex");
  const db = new Database(join(estate, "representation-manifold.db"), { create: true });
  db.exec(`CREATE TABLE representation_instance(id TEXT,schema_ref TEXT,kind TEXT,state TEXT,digest TEXT,frontier TEXT);
    CREATE TABLE physical_realization(representation_id TEXT,coordinate TEXT,available INTEGER);`);
  db.query("INSERT INTO representation_instance VALUES(?,?,?,?,?,?)")
    .run("fixture", "sample", "parquet", "physical", digest, "fixture-frontier");
  db.query("INSERT INTO physical_realization VALUES(?,?,?)").run("fixture", file, 1);
  db.close();
  run(["bun", join(root, "cmd/BonfyreArrowFlight/build.mjs"), "--out", bundle]);
  const binding = await bindArrowRuntime(estate, bundle);
  expect(binding.state).toBe("INSTALLED_UNSERVED");
  expect(binding.tables).toBe(1);
  const catalog = binding.catalog;
  const receipts = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(receipts.query("SELECT state FROM transport_binding WHERE transport='arrow-flight'").get().state)
    .toBe("installed_unserved");
  expect(receipts.query("SELECT state FROM transport_binding WHERE transport='arrow-c-stream'").get().state)
    .toBe("installed_verified");
  expect(receipts.query("SELECT coordinate FROM transport_binding WHERE transport='arrow-c-stream'").get().coordinate)
    .toContain(`bf_parquet_export_verified_stream?sha256=${digest}`);
  receipts.close();
  run([...common, join(root, "cmd/BonfyreArrowFlight/tests/verify.cpp"),
    ...libs, "-o", client]);
  run([...common, join(root, "cmd/BonfyreArrowStream/tests/verify.cpp"),
    arrow, parquet, "-o", ipcVerify]);
  run([...common, "-I", join(bundle, "include"), `-Wl,-rpath,${bundle}`,
    join(root, "cmd/BonfyreArrowFlight/tests/verify_c_data.cpp"),
    join(bundle, "libbonfyre_arrow_c.dylib"), arrow, parquet,
    "-o", cDataVerify]);
  const linked = run(["otool", "-L", server]);
  expect(linked).not.toContain("site-packages");
  const stream = Bun.spawnSync([writer, file, "--batch-size", "1",
    "--expected-sha256", digest], {
    cwd: root, stdout: "pipe", stderr: "pipe",
  });
  if (stream.exitCode !== 0) throw new Error(`native IPC writer: ${stream.stderr.toString()}`);
  const ipc = join(scratch, "sample.ipc");
  await Bun.write(ipc, stream.stdout);
  expect(run([ipcVerify, file, ipc])).toContain("match Parquet");
  expect(run([cDataVerify, file, digest])).toContain("matches Parquet");
  const remote = Bun.spawnSync([server, "--catalog", catalog, "--host", "0.0.0.0"],
    { stdout: "pipe", stderr: "pipe" });
  expect(remote.exitCode).toBe(2);
  expect(remote.stderr.toString()).toContain("loopback only");
  const probe = createServer();
  await new Promise((ok) => probe.listen(0, "127.0.0.1", ok));
  const port = probe.address().port;
  await new Promise((ok) => probe.close(ok));
  const proc = Bun.spawn([server, "--catalog", catalog, "--port", String(port)], {
    stdout: "pipe", stderr: "pipe",
  });
  try {
    const reader = proc.stdout.getReader();
    let output = "";
    while (!output.includes("\n")) {
      const { value, done } = await reader.read();
      if (done) throw new Error(`Flight server exited before listening: ${await new Response(proc.stderr).text()}`);
      output += new TextDecoder().decode(value);
    }
    reader.releaseLock();
    expect(output).toContain(`:${port}`);
    try {
      expect(run([client, file, String(port)])).toContain("match Parquet");
      appendFileSync(file, "drift");
      const changed = Bun.spawnSync([client, file, String(port)],
        { stdout: "pipe", stderr: "pipe" });
      expect(changed.exitCode).toBe(1);
      expect(changed.stderr.toString()).toContain("changed after verification");
    } catch (error) {
      proc.kill();
      await proc.exited;
      throw new Error(`${error.message}; server exit=${proc.exitCode} signal=${proc.signalCode} stderr: ${await new Response(proc.stderr).text()}`);
    }
  } finally {
    proc.kill();
    await proc.exited;
  }
  const staleServer = Bun.spawnSync([server, "--catalog", catalog, "--port", "0"],
    { stdout: "pipe", stderr: "pipe" });
  expect(staleServer.exitCode).toBe(1);
  expect(staleServer.stderr.toString()).toContain("canonical Parquet drift");
  const staleStream = Bun.spawnSync([writer, file, "--expected-sha256", digest],
    { stdout: "pipe", stderr: "pipe" });
  expect(staleStream.exitCode).toBe(1);
  expect(staleStream.stderr.toString()).toContain("canonical Parquet drift");
  await expect(bindArrowRuntime(estate, bundle)).rejects.toThrow("canonical Parquet drift");
  const retained = new Database(join(estate, "representation-manifold.db"));
  expect(retained.query("SELECT state FROM transport_binding WHERE transport='arrow-flight'").get().state)
    .toBe("installed_unserved");
  await Bun.write(file, canonical);
  retained.query("UPDATE representation_instance SET state='reconstructable' WHERE id='fixture'").run();
  retained.close();
  const released = await bindArrowRuntime(estate, bundle);
  expect(released.tables).toBe(0);
  const stale = new Database(join(estate, "representation-manifold.db"), { readonly: true });
  expect(stale.query("SELECT state FROM transport_binding WHERE transport='arrow-flight'").get().state)
    .toBe("superseded");
  stale.close();
  const bundled = await Bun.file(join(bundle, "manifest.json")).json();
  appendFileSync(join(bundle, "lib", bundled.libraries.arrow.file), "drift");
  await expect(bindArrowRuntime(estate, bundle)).rejects.toThrow("native library drift");
}, 180000);   // the bundle now carries and re-signs its whole library closure, which takes longer than the old limit
