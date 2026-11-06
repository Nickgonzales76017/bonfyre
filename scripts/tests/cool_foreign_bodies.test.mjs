import { afterAll, expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, symlinkSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { subjectId } from "../cool_foreign_bodies.mjs";

const root = mkdtempSync(join(tmpdir(), "foreign-cooling-"));
const receipts = join(root, "receipts"), bodies = join(root, "bodies"), dbPath = join(root, "cms.db");
const binary = join(root, "project"), script = fileURLToPath(new URL("../cool_foreign_bodies.mjs", import.meta.url));
mkdirSync(receipts);
mkdirSync(bodies);
afterAll(() => rmSync(root, { recursive: true, force: true }));

const cooledUrl = "https://example.test/cool", retainedUrl = "https://example.test/keep";
const cooledBody = join(bodies, "cool.body"), retainedBody = join(bodies, "keep.body"), outside = join(root, "outside.body");
const receipt = (name, url, body) => writeFileSync(join(receipts, name), JSON.stringify({ source_url: url, body_path: body }));
writeFileSync(cooledBody, "cold bytes");
writeFileSync(retainedBody, "retained bytes");
writeFileSync(outside, "unrelated bytes");
receipt("cool.json", cooledUrl, cooledBody);
receipt("keep.json", retainedUrl, retainedBody);
const db = new Database(dbPath);
db.exec("CREATE TABLE twin_policy(twin_subject TEXT,regime TEXT,retain_body INTEGER,status TEXT)");
db.query("INSERT INTO twin_policy VALUES(?,?,?,?)").run(subjectId(cooledUrl), "REMOTE", 0, "published");
db.query("INSERT INTO twin_policy VALUES(?,?,?,?)").run(subjectId(retainedUrl), "MIRROR-LIKE", 1, "published");
db.close();
writeFileSync(binary, `#!/usr/bin/env bun
console.log(JSON.stringify({twins:[{url:"${cooledUrl}"},{url:"${retainedUrl}"}]}));
`);
chmodSync(binary, 0o755);
const invoke = (...args) => {
  const child = spawnSync("bun", [script, "--receipts", receipts, "--bodies", bodies, "--cms", dbPath,
    "--binary", binary, ...args], { encoding: "utf8" });
  return { status: child.status, report: child.stdout ? JSON.parse(child.stdout) : undefined, stderr: child.stderr };
};

test("subject policy key is address-normalized, not a claimed owner", () => {
  expect(subjectId(cooledUrl)).toBe("foreign_twin:example-test-cool");
});

test("dry run reports only authored cooling and retains every byte", () => {
  const { status, report } = invoke();
  expect(status).toBe(0);
  expect(report).toMatchObject({ applied: false, bodies_cooled: 1, bytes_freed: 10, retained_by_regime: 1 });
  expect(existsSync(cooledBody)).toBe(true);
  expect(existsSync(retainedBody)).toBe(true);
});

test("one out-of-root receipt refuses the whole apply before deletion", () => {
  receipt("malicious.json", cooledUrl, outside);
  const { status, report } = invoke("--apply");
  expect(status).toBe(1);
  expect(report.applied).toBe(false);
  expect(report.failures[0]).toContain("outside public body root");
  expect(existsSync(cooledBody)).toBe(true);
  expect(readFileSync(outside, "utf8")).toBe("unrelated bytes");
  unlinkSync(join(receipts, "malicious.json"));
});

test("a symlink or body shared by two sources also refuses the whole apply", () => {
  const link = join(bodies, "linked.body");
  symlinkSync(outside, link);
  receipt("link.json", cooledUrl, link);
  const linked = invoke("--apply");
  expect(linked.status).toBe(1);
  expect(linked.report.failures[0]).toContain("not a direct regular file");
  expect(existsSync(cooledBody)).toBe(true);
  unlinkSync(join(receipts, "link.json"));
  unlinkSync(link);

  receipt("shared.json", retainedUrl, cooledBody);
  const shared = invoke("--apply");
  expect(shared.status).toBe(1);
  expect(shared.report.failures[0]).toContain("shared by multiple sources");
  expect(existsSync(cooledBody)).toBe(true);
  unlinkSync(join(receipts, "shared.json"));

  receipt("aliased.json", retainedUrl, `${bodies}/./cool.body`);
  const aliased = invoke("--apply");
  expect(aliased.status).toBe(1);
  expect(aliased.report.failures[0]).toContain("shared by multiple sources");
  expect(existsSync(cooledBody)).toBe(true);
  unlinkSync(join(receipts, "aliased.json"));
});

test("apply removes only the bounded body and keeps receipts and retained bodies", () => {
  const { status, report } = invoke("--apply");
  expect(status).toBe(0);
  expect(report).toMatchObject({ applied: true, bodies_cooled: 1, bytes_freed: 10, retained_by_regime: 1 });
  expect(existsSync(cooledBody)).toBe(false);
  expect(existsSync(retainedBody)).toBe(true);
  expect(readdirSync(receipts)).toEqual(["cool.json", "keep.json"]);
  expect(invoke().report.bodies_cooled).toBe(0);
});
