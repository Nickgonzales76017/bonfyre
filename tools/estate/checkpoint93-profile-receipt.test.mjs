import { afterAll, expect, test } from "bun:test";
import { appendFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendProfileReceipt, summarizeProfileFailures } from "./checkpoint93-profile-receipt.mjs";

const directory = mkdtempSync(join(tmpdir(), "checkpoint93-receipt-"));
const receipts = join(directory, "profile-run-receipts.jsonl");
const script = join(import.meta.dir, "checkpoint93-profile-receipt.mjs");
afterAll(() => rmSync(directory, { recursive: true, force: true }));

test("the production runner calls Bun for receipt append and summary", () => {
  const source = readFileSync(join(import.meta.dir, "checkpoint93-profile-runner.sh"), "utf8");
  expect(source).toContain('"$BUN" "$RECEIPT_HELPER" append');
  expect(source).toContain('"$BUN" "$RECEIPT_HELPER" summary');
  expect(source).not.toContain("python3");
});

test("append keeps compact receipt shape and concurrent records remain whole", async () => {
  const stamp = "2026-10-02T12:00:00Z";
  appendProfileReceipt(receipts, "provider-credit", "first.ts", 1, stamp, stamp, 4, "failed", join(directory, "first.log"));
  const first = readFileSync(receipts, "utf8").trimEnd();
  expect(first).toBe('{"schema":"bonfyre.phase94.profile-receipt.v1","profile":"provider-credit","tool":"first.ts","exit_code":1,"started_at":"2026-10-02T12:00:00Z","completed_at":"2026-10-02T12:00:00Z","duration_seconds":4,"state":"failed","log_path":"' + join(directory, "first.log") + '"}');
  const children = Array.from({ length: 8 }, (_, i) => Bun.spawn(["bun", script, "append", receipts,
    "provider-credit", `concurrent-${i}.ts`, "0", stamp, stamp, "1", "passed", join(directory, `${i}.log`)],
    { stdin: "ignore", stdout: "pipe", stderr: "pipe" }));
  expect(await Promise.all(children.map(child => child.exited))).toEqual(Array(8).fill(0));
  const lines = readFileSync(receipts, "utf8").trimEnd().split("\n");
  expect(lines.length).toBe(9);
  expect(lines.map(JSON.parse).filter(row => row.tool.startsWith("concurrent-")).length).toBe(8);
});

test("summary skips broken lines, filters prior runs, and selects the last error line", () => {
  const log = join(directory, "first.log");
  writeFileSync(log, "opening\nerror: first problem\nnoise\nerror: final problem\ntrailer\n");
  appendFileSync(receipts, "{bad json\n");
  appendProfileReceipt(receipts, "provider-credit", "old.ts", 9, "2026-10-01T12:00:00Z",
    "2026-10-01T12:00:00Z", 2, "failed", join(directory, "old.log"));
  appendProfileReceipt(receipts, "other", "other.ts", 3, "2026-10-02T12:00:00Z",
    "2026-10-02T12:00:00Z", 2, "failed", join(directory, "other.log"));
  const output = summarizeProfileFailures(receipts, "provider-credit", "2026-10-02T12:00:00Z");
  expect(output).toContain("1 unreadable receipt line(s) skipped");
  expect(output).toContain("first.ts rc=1 failed");
  expect(output).toContain("error: final problem");
  expect(output).toContain(log);
  expect(output).not.toContain("old.ts");
  expect(output).not.toContain("other.ts");
  expect(output).not.toContain("concurrent-");
  const cli = Bun.spawnSync(["bun", script, "summary", receipts, "provider-credit", "2026-10-02T12:00:00Z"],
    { stdout: "pipe", stderr: "pipe" });
  expect(cli.exitCode).toBe(0);
  expect(cli.stdout.toString()).toBe(output);
});
