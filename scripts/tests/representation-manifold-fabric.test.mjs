import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { appendFileSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { apply } from "../representation-manifold-shift.mjs";
import { runNativeFabric } from "../representation-manifold-fabric.mjs";

const inputs = ["ARROW_CPP_INCLUDE", "ARROW_CPP_LIBARROW", "ARROW_CPP_LIBPARQUET",
  "ARROW_CPP_LIBFLIGHT", "SIMDJSON_DYLIB", "ARROW_CPP_LICENSE", "ARROW_CPP_NOTICE",
  "SIMDJSON_LICENSE"];
const nativeAvailable = (Boolean(process.env.ARROW_RUNTIME_BUNDLE) && Boolean(process.env.ARROW_CPP_INCLUDE)) ||
  inputs.every(name => process.env[name]);

test("fabric refuses implicit runtime and unauthenticated non-loopback exposure", async () => {
  expect(runNativeFabric(["bind"])).rejects.toThrow("--bundle is required");
  expect(runNativeFabric(["serve", "--bundle", "/missing", "--catalog", "/missing/catalog.json", "--host", "0.0.0.0"]))
    .rejects.toThrow("host must be loopback");
  expect(runNativeFabric(["stream", "--bundle", "/missing"]))
    .rejects.toThrow("stream requires --kind");
});

(nativeAvailable ? test : test.skip)("fabric binds a relocatable native bundle and streams only digest-valid Parquet", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-native-fabric-"));
  try {
    const source = new Database(join(dir, "cms.db"));
    source.exec("CREATE TABLE body(id INTEGER,name TEXT); INSERT INTO body VALUES (1,'alpha'),(2,NULL)");
    source.close();
    const result = await apply(dir, join(dir, "no-substrates"));
    const bundle = process.env.ARROW_RUNTIME_BUNDLE ? resolve(process.env.ARROW_RUNTIME_BUNDLE)
      : join(dir, "native-bundle");
    if (!process.env.ARROW_RUNTIME_BUNDLE) {
      const build = Bun.spawnSync(["bun", resolve(import.meta.dir, "../../cmd/BonfyreArrowFlight/build.mjs"), "--out", bundle],
        { stdout: "pipe", stderr: "pipe" });
      expect(build.exitCode).toBe(0);
    }
    const binding = await runNativeFabric(["bind", "--bundle", bundle, "--estate", dir]);
    expect(binding.state).toBe("INSTALLED_UNSERVED");
    expect(binding.tables).toBeGreaterThan(0);
    const bound = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    const before = bound.query("SELECT checked_at FROM transport_binding WHERE semantic_kind='cms.body' AND transport='arrow-flight'").get().checked_at;
    bound.close();
    const stream = Bun.spawnSync(["bun", resolve(import.meta.dir, "../representation-manifold-fabric.mjs"),
      "stream", "--bundle", bundle, "--estate", dir, "--catalog", binding.catalog, "--kind", "cms.body"],
      { stdout: "pipe", stderr: "pipe", maxBuffer: 2 * 1024 * 1024 });
    expect(stream.exitCode).toBe(0);
    expect(stream.stdout.length).toBeGreaterThan(100);
    expect(stream.stdout[0]).toBe(255); // Arrow IPC continuation marker
    const stage = join(dir, "stage");
    const install = Bun.spawnSync(["make", "-C", resolve(import.meta.dir, ".."),
      "install-representation-fabric", `MEMBRANE_ROOT=${stage}`], { stdout: "pipe", stderr: "pipe" });
    expect(install.exitCode).toBe(0);
    const stagedStream = Bun.spawnSync(["bun", join(stage, "scripts", "representation-manifold-fabric.mjs"),
      "stream", "--bundle", bundle, "--estate", dir, "--catalog", binding.catalog, "--kind", "cms.body"],
      { stdout: "pipe", stderr: "pipe", maxBuffer: 2 * 1024 * 1024 });
    expect(stagedStream.exitCode).toBe(0);
    expect(Buffer.from(stagedStream.stdout).equals(Buffer.from(stream.stdout))).toBe(true);
    const db = new Database(join(dir, "representation-manifold.db"), { readonly: true });
    expect(db.query("SELECT state FROM transport_binding WHERE semantic_kind='cms.body' AND transport='arrow-flight'").get().state)
      .toBe("installed_unserved");
    expect(db.query("SELECT checked_at FROM transport_binding WHERE semantic_kind='cms.body' AND transport='arrow-flight'").get().checked_at)
      .toBe(before);
    db.close();
    const parquet = result.manifest.find(row => row.kind === "cms.body").target;
    const client = join(dir, "flight-verify");
    const prebuilt = Boolean(process.env.ARROW_RUNTIME_BUNDLE);
    const manifest = prebuilt ? JSON.parse(readFileSync(join(bundle, "manifest.json"), "utf8")) : null;
    const library = name => prebuilt ? join(bundle, "lib", manifest.libraries[name].file)
      : process.env[`ARROW_CPP_LIB${name.toUpperCase()}`];
    const compile = Bun.spawnSync(["clang++", "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror",
      "-isystem", process.env.ARROW_CPP_INCLUDE ?? join(bundle, "include"),
      `-Wl,-rpath,${prebuilt ? join(bundle, "lib") : resolve(process.env.ARROW_CPP_LIBARROW, "..")}`,
      resolve(import.meta.dir, "../../cmd/BonfyreArrowFlight/tests/verify.cpp"),
      library("flight"), library("parquet"), library("arrow"), "-o", client],
      { stdout: "pipe", stderr: "pipe" });
    if (compile.exitCode !== 0) throw new Error(`Flight client compile failed: ${compile.stderr.toString()}`);
    expect(compile.exitCode).toBe(0);
    const server = Bun.spawn(["bun", resolve(import.meta.dir, "../representation-manifold-fabric.mjs"),
      "serve", "--bundle", bundle, "--estate", dir, "--catalog", binding.catalog, "--port", "0"],
      { stdout: "pipe", stderr: "pipe" });
    try {
      const reader = server.stdout.getReader();
      let line = "";
      while (!line.includes("\n")) {
        const { done, value } = await reader.read();
        if (done) throw new Error(`fabric server failed: ${await new Response(server.stderr).text()}`);
        line += new TextDecoder().decode(value);
      }
      reader.releaseLock();
      const port = Number(line.match(/grpc:\/\/[^:]+:(\d+)/)?.[1]);
      expect(Number.isSafeInteger(port) && port > 0).toBe(true);
      const reply = Bun.spawnSync([client, parquet, String(port), "cms_body"], { stdout: "pipe", stderr: "pipe" });
      expect(reply.exitCode).toBe(0);
      expect(reply.stdout.toString()).toContain("match Parquet");
      const live = new Database(join(dir, "representation-manifold.db"), { readonly: true });
      expect(live.query("SELECT state FROM transport_binding WHERE semantic_kind='cms.body' AND transport='arrow-flight'").get().state)
        .toBe("installed_unserved");
      live.close();
    } finally { server.kill(); await server.exited; }
    appendFileSync(parquet, "drift");
    expect(runNativeFabric(["stream", "--bundle", bundle, "--estate", dir,
      "--catalog", binding.catalog, "--kind", "cms.body"]))
      .rejects.toThrow("canonical Parquet drift");
    expect(runNativeFabric(["bind", "--bundle", bundle, "--estate", dir]))
      .rejects.toThrow("canonical Parquet drift");
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 240000);   // the bundle build now carries and re-signs its whole library closure
