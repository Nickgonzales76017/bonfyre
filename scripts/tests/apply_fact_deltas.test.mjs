// What this does not prove:
// - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
import { afterAll, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { affectedTypes } from "../apply_fact_deltas.mjs";

const root = mkdtempSync(join(tmpdir(), "fact-delta-consumer-"));
const script = fileURLToPath(new URL("../apply_fact_deltas.mjs", import.meta.url));
const cms = join(root, "cms");
afterAll(() => rmSync(root, { recursive: true, force: true }));

writeFileSync(cms, `#!/usr/bin/env bun
const type = process.argv[4];
if (type === "broken") process.exit(1);
console.log(JSON.stringify({indexed: type === "empty" ? 0 : 7}));
`);
chmodSync(cms, 0o755);

const delta = (name, writes) => {
  const path = join(root, `${name}.json`);
  writeFileSync(path, `${JSON.stringify({ schema: "bonfyre-fact-delta.v1", delta_id: name, writes })}\n`);
  return path;
};
const invoke = (...args) => {
  const child = spawnSync("bun", [script, "--deltas", root, "--cms", cms, "--db", join(root, "cms.db"), "--json", ...args], { encoding: "utf8" });
  return { status: child.status, report: JSON.parse(child.stdout), stderr: child.stderr };
};

test("only exact CMS type writes invalidate indexes", () => {
  expect([...affectedTypes({ writes: ["cms:story", "cms:story:identity", "cms:publication_source:a", "matrix_subjects", "cms:bad-name"] })]).toEqual(["story"]);
});

test("dry run does not rebuild or mark applied", () => {
  const path = delta("dry", ["cms:empty"]);
  const { status, report } = invoke("--dry-run");
  expect(status).toBe(0);
  expect(report.invalidated_types).toEqual(["empty"]);
  expect(report.rebuilt).toEqual({});
  expect(JSON.parse(readFileSync(path, "utf8")).applied).toBeUndefined();
});

test("failed rebuild stays pending while independent deltas apply", () => {
  const failed = delta("failed", ["cms:broken", "cms:empty"]);
  const independent = delta("independent", ["cms:story"]);
  const { status, report } = invoke();
  expect(status).toBe(1);
  expect(report.rebuilt).toEqual({ broken: -1, empty: 0, story: 7 });
  expect(report.failed_types).toEqual(["broken"]);
  expect(report.applied).toBe(2); // dry + independent
  expect(JSON.parse(readFileSync(failed, "utf8")).applied).toBeUndefined();
  expect(JSON.parse(readFileSync(independent, "utf8")).applied).toBeUndefined();
  expect(readdirSync(join(root, ".applied")).filter(name => name.endsWith(".json"))).toHaveLength(2);
  expect(invoke("--dry-run").report.pending_deltas).toBe(1);
});

function isolated(mock) {
  const dir = mkdtempSync(join(tmpdir(), "fact-delta-adversarial-"));
  const owner = join(dir, "cms"), path = join(dir, "delta.json");
  const original = { schema: "bonfyre-fact-delta.v1", delta_id: "old", writes: ["cms:old_type"] };
  const replacement = { schema: "bonfyre-fact-delta.v1", delta_id: "new", writes: ["cms:new_type"] };
  writeFileSync(path, JSON.stringify(original));
  const install = body => { writeFileSync(owner, `#!/usr/bin/env bun\n${body}\n`); chmodSync(owner, 0o755); };
  const args = [script, "--deltas", dir, "--cms", owner, "--db", join(dir, "unused.db"), "--json"];
  const invoke = (...extra) => {
    const child = spawnSync("bun", [...args, ...extra], { encoding: "utf8" });
    return { status: child.status, report: JSON.parse(child.stdout), stderr: child.stderr };
  };
  const replace = `import {writeFileSync,renameSync} from 'node:fs';
writeFileSync(${JSON.stringify(path + ".tmp")}, ${JSON.stringify(JSON.stringify(replacement))});
renameSync(${JSON.stringify(path + ".tmp")}, ${JSON.stringify(path)});`;
  try { mock({ dir, owner, path, original, replacement, args, install, invoke, replace }); }
  finally { rmSync(dir, { recursive: true, force: true }); }
}

test("replacement during successful rebuild remains pending without overwriting producer bytes", () => isolated(f => {
  f.install(`${f.replace}\nconsole.log('{"indexed":3}');`);
  const first = f.invoke();
  expect(first.status).toBe(1);
  expect(first.report.applied).toBe(1);
  expect(first.report.changed_deltas).toEqual(["delta.json"]);
  expect(JSON.parse(readFileSync(f.path, "utf8"))).toEqual(f.replacement);
  expect(f.invoke("--dry-run").report.invalidated_types).toEqual(["new_type"]);
  f.install(`console.log('{"indexed":4}');`);
  expect(f.invoke().report.rebuilt).toEqual({ new_type: 4 });
  expect(f.invoke("--dry-run").report.pending_deltas).toBe(0);
  expect(readdirSync(join(f.dir, ".applied")).filter(name => name.endsWith(".json"))).toHaveLength(2);
}));

test("replacement during failed rebuild retains both old captured work and newly published work", () => isolated(f => {
  f.install(`${f.replace}\nprocess.exit(1);`);
  const first = f.invoke();
  expect(first.report.failed_types).toEqual(["old_type"]);
  expect(first.report.applied).toBe(0);
  expect(JSON.parse(readFileSync(f.path, "utf8"))).toEqual(f.replacement);
  const pending = f.invoke("--dry-run").report;
  expect(pending.pending_deltas).toBe(2);
  expect(pending.invalidated_types).toEqual(["new_type", "old_type"]);
  f.install(`console.log('{"indexed":4}');`);
  const retried = f.invoke().report;
  expect(retried.rebuilt).toEqual({ new_type: 4, old_type: 4 });
  expect(retried.applied).toBe(2);
  expect(retried.changed_deltas).toEqual([]);
  expect(f.invoke("--dry-run").report.pending_deltas).toBe(0);
}));

test("competing consumer cannot rebuild a claimed batch", () => isolated(f => {
  const nested = join(f.dir, "nested-result");
  f.install(`import {writeFileSync} from 'node:fs';
const child = Bun.spawnSync(['bun', ...${JSON.stringify(f.args)}]);
writeFileSync(${JSON.stringify(nested)}, child.stdout);
console.log('{"indexed":1}');`);
  expect(f.invoke().status).toBe(0);
  const second = JSON.parse(readFileSync(nested, "utf8"));
  expect(second.held).toBe(true);
  expect(second.rebuilt).toEqual({});
  expect(second.applied).toBe(0);
  expect(f.invoke("--dry-run").report.pending_deltas).toBe(0);
}));

test("surviving claim refuses replay and legacy applied records remain readable", () => isolated(f => {
  mkdirSync(join(f.dir, ".apply-fact-deltas.claim"));
  f.install(`throw new Error('must not run');`);
  expect(f.invoke().report.held).toBe(true);
  expect(f.invoke("--dry-run").report.pending_deltas).toBe(1);
  writeFileSync(f.path, JSON.stringify({ ...f.original, applied: true }));
  expect(f.invoke("--dry-run").report.pending_deltas).toBe(0);
}));

test("oversized input refuses without replacing bytes or acknowledging work", () => isolated(f => {
  const bytes = " ".repeat(1024 * 1024 + 1);
  writeFileSync(f.path, bytes);
  f.install(`throw new Error('must not run');`);
  const answer = f.invoke();
  expect(answer.status).toBe(1);
  expect(answer.report.invalid_deltas).toEqual(["delta.json"]);
  expect(answer.report.applied).toBe(0);
  expect(readFileSync(f.path, "utf8")).toBe(bytes);
}));

test("captured journal tampering refuses without consuming its alleged work", () => isolated(f => {
  f.install(`process.exit(1);`);
  expect(f.invoke().report.applied).toBe(0);
  const captured = join(f.dir, ".inputs", readdirSync(join(f.dir, ".inputs"))[0]);
  const record = JSON.parse(readFileSync(captured, "utf8"));
  record.source_bytes = Buffer.from(JSON.stringify(f.replacement)).toString("base64");
  writeFileSync(captured, JSON.stringify(record));
  const result = f.invoke("--dry-run");
  expect(result.status).toBe(1);
  expect(result.report.invalid_deltas).toEqual([`.inputs/${captured.split("/").at(-1)}`]);
  expect(result.report.invalidated_types).toEqual(["old_type"]);
}));

test("valid prototype-like CMS names are data, not object properties", () => isolated(f => {
  writeFileSync(f.path, JSON.stringify({ ...f.original, writes: ["cms:__proto__", "cms:constructor"] }));
  f.install(`console.log('{"indexed":1}');`);
  const answer = f.invoke();
  expect(answer.status).toBe(0);
  expect(Object.keys(answer.report.rebuilt)).toEqual(["__proto__", "constructor"]);
  expect(answer.report.rebuilt.__proto__).toBe(1);
  expect(answer.report.rebuilt.constructor).toBe(1);
  expect(f.invoke("--dry-run").report.pending_deltas).toBe(0);
}));

test("malformed deltas are visible and never marked applied", () => {
  const path = join(root, "malformed.json");
  writeFileSync(path, "{not json\n");
  const { status, report } = invoke("--dry-run");
  expect(status).toBe(1);
  expect(report.invalid_deltas).toEqual(["malformed.json"]);
  expect(readFileSync(path, "utf8")).toBe("{not json\n");
});
