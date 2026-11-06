import { afterAll, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { inspect } from "../inspect_foreign_field.mjs";
import { bodyDigest } from "../lower_parquet_to_zarr.mjs";

const root = mkdtempSync(join(tmpdir(), "zarr-lowering-"));
const script = fileURLToPath(new URL("../lower_parquet_to_zarr.mjs", import.meta.url));
const parquet = join(root, "body'one.parquet"), out = join(root, "field.zarr"), query = join(root, "query"), sqlTrace = join(root, "sql.txt");
afterAll(() => rmSync(root, { recursive: true, force: true }));
writeFileSync(parquet, "retained parquet fixture");
writeFileSync(query, `#!/usr/bin/env bun
import { writeFileSync } from "node:fs";
writeFileSync(process.env.SQL_TRACE, process.argv[4]);
if (process.env.QUERY_FAIL === "1") process.exit(2);
console.log("[query] fixture");
console.log(process.env.QUERY_ROWS ?? JSON.stringify([
  {src_id:"src:b",bucket:"2026-09-30 10:05:00",observations:3},
  {src_id:"src:a",bucket:"2026-09-30 10:00:00",observations:2},
  {src_id:"src:a",bucket:"2026-09-30 10:05:00",observations:4}
]));
`);
chmodSync(query, 0o755);
const invoke = (...args) => {
  const child = spawnSync("bun", [script, "--parquet", parquet, "--out", out, "--query", query,
    "--scratch", join(root, "scratch.duckdb"), ...args], { encoding: "utf8",
    env: { ...process.env, SQL_TRACE: sqlTrace } });
  return { status: child.status, report: child.stdout ? JSON.parse(child.stdout) : undefined, stderr: child.stderr };
};

test("directory body identity is a sorted manifest of member digests", async () => {
  const parts = join(root, "parts");
  mkdirSync(parts);
  writeFileSync(join(parts, "b"), "B");
  writeFileSync(join(parts, "a"), "A");
  const sha = value => createHash("sha256").update(value).digest("hex");
  expect(await bodyDigest(parts)).toBe(sha(`a:${sha("A")}\nb:${sha("B")}`));
});

test("dry run plans a field without writing it and escapes the query path", () => {
  const { status, report } = invoke("--dry-run");
  expect(status).toBe(0);
  expect(report).toMatchObject({ sources: 2, buckets: 2, cells: 4, dry_run: true });
  expect(existsSync(out)).toBe(false);
  expect(readFileSync(sqlTrace, "utf8")).toContain("read_parquet('");
  expect(readFileSync(sqlTrace, "utf8")).toContain("body''one.parquet");
});

test("lowering emits a complete field with lineage and exact cell sum", () => {
  const { status, report } = invoke();
  expect(status).toBe(0);
  expect(report).toMatchObject({ shape: [2, 2], cells: 4, nonzero: 3, total_observations: 9 });
  expect(inspect(out)).toMatchObject({ shape: [2, 2], total: 9, has_derivation_frontier: true });
  expect(report.derived_from_digest).toBe(createHash("sha256").update("retained parquet fixture").digest("hex"));
});

test("query failure and an unowned output file do not replace the existing field", () => {
  const before = readFileSync(join(out, "0.0"));
  writeFileSync(join(out, "unowned.txt"), "keep");
  const unowned = invoke();
  expect(unowned.status).toBe(1);
  expect(unowned.stderr).toContain("unowned files");
  expect(readFileSync(join(out, "0.0"))).toEqual(before);
  const child = spawnSync("bun", [script, "--parquet", parquet, "--out", out, "--query", query], {
    encoding: "utf8", env: { ...process.env, SQL_TRACE: sqlTrace, QUERY_FAIL: "1" } });
  expect(child.status).toBe(1);
  expect(child.stderr).toContain("field query failed");
  expect(readFileSync(join(out, "0.0"))).toEqual(before);
});

test("missing or duplicate field coordinates fail before publication", () => {
  const target = join(root, "invalid.zarr");
  for (const rows of [[{ src_id: null, bucket: "t", observations: 1 }],
    [{ src_id: "a", bucket: "t", observations: 1 }, { src_id: "a", bucket: "t", observations: 2 }]]) {
    const child = spawnSync("bun", [script, "--parquet", parquet, "--out", target, "--query", query], {
      encoding: "utf8", env: { ...process.env, SQL_TRACE: sqlTrace, QUERY_ROWS: JSON.stringify(rows) } });
    expect(child.status).toBe(1);
    expect(existsSync(target)).toBe(false);
  }
});
