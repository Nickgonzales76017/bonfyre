import { afterAll, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, renameSync, rmSync, statSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";

const root = resolve(import.meta.dir, "../../..");
const include = process.env.ARROW_CPP_INCLUDE;
const arrow = process.env.ARROW_CPP_LIBARROW;
const parquet = process.env.ARROW_CPP_LIBPARQUET;
const flight = process.env.ARROW_CPP_LIBFLIGHT;
const simdjson = process.env.SIMDJSON_DYLIB;
const enabled = Boolean(include && arrow && parquet && flight && simdjson);
const scratch = enabled ? mkdtempSync(join(tmpdir(), "bonfyre-flight-integrity-")) : null;
afterAll(() => { if (scratch) rmSync(scratch, { recursive: true, force: true }); });

function run(argv) {
  const result = Bun.spawnSync(argv, { cwd: root, stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(`${argv[0]} failed: ${result.stderr}`);
  return result.stdout.toString();
}

test.skipIf(!enabled)("Flight refuses equal-size Parquet replacement with restored mtime", async () => {
  const original = join(scratch, "sample.parquet");
  const replacement = join(scratch, "replacement.parquet");
  const catalog = join(scratch, "catalog.json");
  const server = join(scratch, "server");
  const client = join(scratch, "client");
  const common = ["clang++", "-std=c++20", "-O2", "-Wall", "-Wextra", "-Werror",
    "-isystem", include, `-Wl,-rpath,${dirname(arrow)}`];
  for (const [path, name] of [[original, "alpha"], [replacement, "omega"]]) {
    run(["duckdb", "-c", `COPY (SELECT 1::BIGINT AS id, '${name}'::VARCHAR AS name,
      [1,2,3]::INTEGER[] AS tags, 12.34::DECIMAL(8,2) AS amount,
      DATE '2024-01-02' AS d UNION ALL SELECT 2, NULL, []::INTEGER[],
      -5.67::DECIMAL(8,2), DATE '2024-01-03') TO '${path}' (FORMAT PARQUET)`]);
  }
  const canonicalBytes = statSync(original).size;
  expect(canonicalBytes).toBe(statSync(replacement).size);
  const before = createHash("sha256").update(Buffer.from(await Bun.file(original).arrayBuffer())).digest("hex");
  const after = createHash("sha256").update(Buffer.from(await Bun.file(replacement).arrayBuffer())).digest("hex");
  expect(after).not.toBe(before);
  await Bun.write(catalog, JSON.stringify({ tables: [{ name: "sample", parquet: original,
    digest: before, frontier: "fixture" }] }));
  run([...common, "-isystem", process.env.SIMDJSON_INCLUDE ?? join(dirname(dirname(simdjson)), "include"),
    join(root, "cmd/BonfyreArrowFlight/src/main.cpp"), flight, parquet, arrow, simdjson,
    "-o", server]);
  run([...common, join(root, "cmd/BonfyreArrowFlight/tests/verify.cpp"),
    flight, parquet, arrow, "-o", client]);
  const probe = createServer();
  await new Promise(ok => probe.listen(0, "127.0.0.1", ok));
  const port = probe.address().port;
  await new Promise(ok => probe.close(ok));
  const proc = Bun.spawn([server, "--catalog", catalog, "--port", String(port)], {
    stdout: "pipe", stderr: "pipe",
  });
  try {
    const reader = proc.stdout.getReader();
    let output = "";
    while (!output.includes("\n")) {
      const { value, done } = await reader.read();
      if (done) throw new Error(`Flight exited before listening: ${await new Response(proc.stderr).text()}`);
      output += new TextDecoder().decode(value);
    }
    reader.releaseLock();
    expect(run([client, original, String(port)])).toContain("match Parquet");

    // An inode/path replacement can preserve the two metadata fields used by
    // CheckDataset while changing the catalog's cryptographic identity.
    run(["touch", "-r", original, replacement]);
    const originalTime = statSync(original, { bigint: true }).mtimeNs;
    renameSync(replacement, original);
    expect(statSync(original, { bigint: true }).mtimeNs).toBe(originalTime);
    expect(statSync(original).size).toBe(canonicalBytes);
    const changed = Bun.spawnSync([client, original, String(port)], {
      cwd: root, stdout: "pipe", stderr: "pipe",
    });
    expect(changed.exitCode).toBe(1);
    const error = changed.stderr.toString();
    expect(error).toContain("info; ");
    expect(error).not.toContain("stream; ");
    expect(error).toContain("canonical Parquet drift");
  } finally {
    proc.kill();
    await proc.exited;
  }
}, 30000);
