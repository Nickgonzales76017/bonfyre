import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";
import assert from "node:assert/strict";

const dir = mkdtempSync(join(tmpdir(), "bonfyre-tenant-houses-"));
const importer = resolve(import.meta.dir, "install_territory_houses.mjs");
const run = (...args) => spawnSync(process.execPath, [importer, ...args], { encoding: "utf8" });
try {
  const good = join(dir, "tenant-houses.json");
  const records = { houses: [
    { template: "house-journal", territory_affinity: "journal" },
    { template: "house-research", territory_affinity: "research" },
  ] };
  writeFileSync(good, JSON.stringify(records));
  const valid = run("--data", good, "--check");
  assert.equal(valid.status, 0, valid.stderr || valid.stdout);
  assert.match(valid.stdout, /valid templates=2/);
  assert.match(valid.stdout, /tenant-houses\.json/);

  const missing = run("--data", "--check");
  assert.notEqual(missing.status, 0, "missing --data argument must refuse");
  assert.match(missing.stderr, /--data needs an exact argument/);

  const absent = run("--data", join(dir, "missing.json"), "--check");
  assert.notEqual(absent.status, 0, "missing tenant source cannot use bundled default");

  const duplicated = join(dir, "duplicate.json");
  writeFileSync(duplicated, JSON.stringify({ houses: [records.houses[0], records.houses[0]] }));
  const refused = run("--data", duplicated, "--check");
  assert.notEqual(refused.status, 0, "duplicate house template cannot be imported");
  assert.match(refused.stderr, /invalid or duplicate template/);

  const invalid = join(dir, "invalid.json");
  writeFileSync(invalid, JSON.stringify({ houses: [{ template: "bad/name", territory_affinity: "journal" }] }));
  assert.notEqual(run("--data", invalid, "--check").status, 0, "unsafe template rejected");

  const environment = spawnSync(process.execPath, [importer, "--check"],
    { encoding: "utf8", env: { ...process.env, BONFYRE_TENANT_HOUSES_FILE: good } });
  assert.equal(environment.status, 0, environment.stderr);
  assert.match(environment.stdout, /valid templates=2/);
  console.log("TENANT_HOUSES_SOURCE_PASS: explicit generic business source without CMS writes");
} finally {
  rmSync(dir, { recursive: true, force: true });
}
