// What this does not prove: the .bqfp and .vecf here are bytes written by a
// script. The readers accepting them shows the formats are parsed, and
// nothing about a model or an embedding.
import { afterAll, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { bqfpFixture, vecfFixture } from "../command-workload-binary-fixtures.mjs";

const root = resolve(import.meta.dir, "../..");
const temporary = mkdtempSync(join(tmpdir(), "bonfyre-command-binary-fixtures-"));
const fixtures = join(temporary, "state", "command-workloads");
const script = join(root, "scripts", "command-workload-binary-fixtures.mjs");
afterAll(() => rmSync(temporary, { recursive: true, force: true }));
const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");

test("workload invokes Bun once and writes exact legacy BQFP/VECF bytes in its state layout", () => {
  const workload = readFileSync(join(root, "scripts", "bonfyre-command-workloads"), "utf8");
  expect(workload).toContain('bun "$root/scripts/command-workload-binary-fixtures.mjs" --out-dir "$fixtures"');
  // The .bqfp is a format fixture for the readers below; no operator is certified on it.
  expect(workload).not.toContain('ingest_artifact "$fixtures/fixture.bqfp"');
  expect(workload).toContain('vecf_uri=$(ingest_artifact "$fixtures/fixture.vecf" application/octet-stream)');
  expect(workload).not.toContain("python3 -");

  const made = Bun.spawnSync(["bun", script, "--out-dir", fixtures], { stdout: "pipe", stderr: "pipe" });
  expect(made.exitCode).toBe(0);
  const bqfp = readFileSync(join(fixtures, "fixture.bqfp"));
  const vecf = readFileSync(join(fixtures, "fixture.vecf"));
  expect(bqfp.equals(bqfpFixture())).toBe(true);
  expect(vecf.equals(vecfFixture())).toBe(true);
  expect(bqfp.length).toBe(552);
  expect(vecf.length).toBe(24);
  // Captured from the former Python struct/random fixture generator once.
  expect(sha256(bqfp)).toBe("3c48c47b12844921c1a7f35d56cfcdf643b8043402452c58965648c7779c048d");
  expect(sha256(vecf)).toBe("a19384a131637b75994f9c330f58e0767ed4e9dbcfce98be1ac1913fd86a080d");
  expect(bqfp.readUInt32LE(0)).toBe(0x50464251);
  expect(bqfp.readUInt32LE(288)).toBe(4);
  expect(vecf.readUInt32LE(0)).toBe(0x46434556);
  expect(vecf.readUInt32LE(4)).toBe(4);
  expect([...Array(4)].map((_, i) => vecf.readFloatLE(8 + 4 * i))).toEqual([3, 4, 0, 0]);
});

const sli = join(root, "cmd", "BonfyreSLI", "bonfyre-sli");
const fpqx = join(root, "cmd", "BonfyreFPQx", "bonfyre-fpqx");
const physics = join(root, "cmd", "BonfyrePhysics", "bonfyre-physics");
( [sli, fpqx, physics].every(existsSync) ? test : test.skip )
  ("existing native readers accept both generated formats", () => {
    const bqfp = join(fixtures, "fixture.bqfp"), vecf = join(fixtures, "fixture.vecf");
    const inspected = Bun.spawnSync([sli, "inspect", "--model", bqfp], { stdout: "pipe", stderr: "pipe" });
    expect(inspected.exitCode).toBe(0);
    expect(inspected.stdout.toString()).toContain("eff_k=4");

    const alignDir = join(temporary, "alignment");
    const aligned = Bun.spawnSync([fpqx, "align", bqfp, bqfp, "--out", alignDir],
      { stdout: "pipe", stderr: "pipe" });
    expect(aligned.exitCode).toBe(0);
    expect(readFileSync(join(alignDir, "fpqx_alignment.bin")).readUInt32LE(0)).toBe(0x58515046);

    const state = join(temporary, "physics.bfps");
    const initialized = Bun.spawnSync([physics, "--state", state, "--pack", join(temporary, "absent.pack"), "init", vecf],
      { stdout: "pipe", stderr: "pipe", env: { ...process.env, BONFYRE_STATE_DIR: temporary } });
    expect(initialized.exitCode).toBe(0);
    expect(initialized.stdout.toString()).toContain("dim    = 4");
    expect(existsSync(state)).toBe(true);
  });
