import { afterAll, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { sourceIdentity } from "../promote_observation_sources.mjs";

const root = mkdtempSync(join(tmpdir(), "source-promotion-"));
const script = fileURLToPath(new URL("../promote_observation_sources.mjs", import.meta.url));
const query = join(root, "query"), organ = join(root, "organ"), trace = join(root, "calls.jsonl"), sqlTrace = join(root, "sql.txt");
afterAll(() => rmSync(root, { recursive: true, force: true }));
writeFileSync(query, `#!/usr/bin/env bun
import { writeFileSync } from "node:fs";
writeFileSync(process.env.SQL_TRACE, process.argv[4]);
if (process.env.QUERY_FAIL === "1") process.exit(2);
console.log("[query] local fixture");
console.log(JSON.stringify([
  {src_id:"src:good", observations:3, pages:2, page_set:"a,b", last_seen:"2026-09-30"},
  {src_id:"src:bad", observations:1, pages:1, page_set:"c", last_seen:"2026-09-29"}
]));
`);
writeFileSync(organ, `#!/usr/bin/env bun
import { appendFileSync } from "node:fs";
appendFileSync(process.env.PROMOTE_TRACE, JSON.stringify(process.argv.slice(2)) + "\\n");
if (process.argv.includes("foreign_twin:src-bad")) process.exit(3);
`);
chmodSync(query, 0o755);
chmodSync(organ, 0o755);
const invoke = (args = [], env = {}) => {
  const child = spawnSync("bun", [script, "--query", query, "--organ", organ, "--parquet", "with'quote.parquet",
    "--matrix", join(root, "matrix.db"), ...args], { encoding: "utf8", env: { ...process.env, SQL_TRACE: sqlTrace,
      PROMOTE_TRACE: trace, ...env } });
  return { status: child.status, stdout: child.stdout, stderr: child.stderr,
    report: child.stdout ? JSON.parse(child.stdout) : undefined };
};

test("source digest is stable over the sorted page set and carries no route", () => {
  const identity = sourceIdentity({ src_id: "src:good", page_set: "a,b", observations: 3, pages: 2, last_seen: "2026-09-30" });
  expect(identity).toEqual({ sid: "foreign_twin:src-good",
    digest: createHash("sha256").update("a,b").digest("hex"),
    evidence: "observation-corpus:3:2pages:2026-09-30" });
});

test("dry run reads the query but does not register or bind presence", () => {
  const { status, report } = invoke(["--dry-run", "--limit", "1"]);
  expect(status).toBe(0);
  expect(report).toMatchObject({ sources: 1, promoted: 1, failed: 0, dry_run: true });
  expect(readFileSync(sqlTrace, "utf8")).toContain("read_parquet('with''quote.parquet')");
});

test("registration uses the exact owner CLI and reports partial failure", () => {
  const { status, report } = invoke();
  expect(status).toBe(1);
  expect(report).toMatchObject({ sources: 2, promoted: 1, failed: 1, failures: ["foreign_twin:src-bad"] });
  const calls = readFileSync(trace, "utf8").trim().split("\n").map(JSON.parse);
  expect(calls).toHaveLength(2);
  expect(calls[0]).toContain("--digest");
  expect(calls[0]).not.toContain("presence");
  expect(calls[0]).not.toContain("--url");
});

test("query failure is not misreported as an empty corpus", () => {
  const { status, stderr } = invoke([], { QUERY_FAIL: "1" });
  expect(status).toBe(1);
  expect(stderr).toContain("observation query failed");
});
