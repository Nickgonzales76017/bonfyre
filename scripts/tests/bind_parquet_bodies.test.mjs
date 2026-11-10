import { afterAll, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { bodyIdentity } from "../columnar_body.mjs";

const root = mkdtempSync(join(tmpdir(), "parquet-binding-"));
const script = fileURLToPath(new URL("../bind_parquet_bodies.mjs", import.meta.url));
const parquet = join(root, "observations.parquet"), field = join(root, "observations.zarr");
const query = join(root, "query"), organ = join(root, "organ"), trace = join(root, "contributions.jsonl");
afterAll(() => rmSync(root, { recursive: true, force: true }));
writeFileSync(parquet, "observations are source-bound");
mkdirSync(field);
writeFileSync(query, `#!/usr/bin/env bun
if (process.env.QUERY_FAIL === "1") process.exit(2);
console.log("[query] fixture");
console.log('[{"src_id":"src:a"},{"src_id":"src:b"}]');
`);
writeFileSync(organ, `#!/usr/bin/env bun
import { appendFileSync } from "node:fs";
appendFileSync(process.env.CONTRIBUTION_TRACE, JSON.stringify(process.argv.slice(2)) + "\\n");
if (process.env.BIND_FAIL === "1" && process.argv.includes("Parquet")) process.exit(3);
`);
chmodSync(query, 0o755);
chmodSync(organ, 0o755);

const invoke = (extra = [], env = {}) => spawnSync("bun", [script, "--parquet", parquet,
  "--field", field, "--matrix", join(root, "matrix.db"), "--query", query, "--organ", organ, ...extra], {
  encoding: "utf8", env: { ...process.env, CONTRIBUTION_TRACE: trace, ...env } });

test("one columnar body identity is reused for every source slice", async () => {
  const identity = await bodyIdentity(parquet);
  expect(identity).toMatchObject({ bytes: 29, members: 1 });
  writeFileSync(join(field, ".zattrs"), JSON.stringify({ "bonfyre:derivedFrom": { digest: identity.digest },
    "bonfyre:sources": ["src:a", "src:b"], "bonfyre:derivation": "fixture",
    "bonfyre:frontier": "2026-09-30" }));
  writeFileSync(join(field, ".zarray"), JSON.stringify({ shape: [2, 1], dtype: "<i4", compressor: null }));
  writeFileSync(join(field, "0.0"), Buffer.alloc(8));
  const child = invoke(["--dry-run"]);
  expect(child.status).toBe(0);
  expect(JSON.parse(child.stdout)).toMatchObject({ slices: 2, bound: 2,
    body_digest: identity.digest, field_rows_bound: 0, dry_run: true });
});

test("stale field lineage refuses all binding before any effect", async () => {
  writeFileSync(join(field, ".zattrs"), JSON.stringify({ "bonfyre:derivedFrom": { digest: "stale" },
    "bonfyre:sources": ["src:a", "src:b"] }));
  const child = invoke();
  expect(child.status).toBe(1);
  expect(child.stderr).toContain("does not match");
});

test("a truncated field chunk cannot be bound as a complete material representation", async () => {
  const body = await bodyIdentity(parquet);
  writeFileSync(join(field, ".zattrs"), JSON.stringify({ "bonfyre:derivedFrom": { digest: body.digest },
    "bonfyre:sources": ["src:a", "src:b"], "bonfyre:derivation": "fixture",
    "bonfyre:frontier": "2026-09-30" }));
  writeFileSync(join(field, "0.0"), Buffer.alloc(4));
  const child = invoke();
  expect(child.status).toBe(1);
  expect(child.stderr).toContain("chunk length does not match shape");
  writeFileSync(join(field, "0.0"), Buffer.alloc(8));
});

test("material closure binds body and matching field with source row selectors", async () => {
  const body = await bodyIdentity(parquet);
  writeFileSync(join(field, ".zattrs"), JSON.stringify({ "bonfyre:derivedFrom": { digest: body.digest },
    "bonfyre:sources": ["src:b", "src:a"], "bonfyre:derivation": "fixture",
    "bonfyre:frontier": "2026-09-30" }));
  const child = invoke();
  expect(child.status).toBe(0);
  expect(JSON.parse(child.stdout)).toMatchObject({ slices: 2, bound: 2, skipped: 0, field_rows_bound: 2 });
  const calls = readFileSync(trace, "utf8").trim().split("\n").map(line => JSON.parse(line));
  expect(calls).toHaveLength(4);
  expect(calls.filter(args => args.includes("Parquet")).every(args => args.includes(body.digest))).toBe(true);
  expect(calls.filter(args => args.includes("Zarr")).map(args => args[args.indexOf("--resource") + 1]))
    .toEqual(["bonfyre://observation-field/observations.zarr#row=1", "bonfyre://observation-field/observations.zarr#row=0"]);
  expect(calls.every(args => args.includes("material"))).toBe(true);
});

test("query and owner failures are visible, not successful empty or complete bindings", () => {
  const queryFailure = invoke([], { QUERY_FAIL: "1" });
  expect(queryFailure.status).toBe(1);
  expect(queryFailure.stderr).toContain("columnar source query failed");
  const bindFailure = invoke([], { BIND_FAIL: "1" });
  expect(bindFailure.status).toBe(1);
  expect(JSON.parse(bindFailure.stdout)).toMatchObject({ bound: 0, skipped: 2, field_rows_bound: 2 });
});
