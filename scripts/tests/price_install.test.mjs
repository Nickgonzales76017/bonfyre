import { test, expect } from "bun:test";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("price and 402 collectors stage CUE and the shared robots gate without Python", () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-price-install-"));
  try {
    const root = join(base, "native-src");
    const installed = Bun.spawnSync(["make", "-C", new URL("..", import.meta.url).pathname,
      `MEMBRANE_ROOT=${root}`, "install-price-observation", "install-rate-discovery"]);
    expect(installed.exitCode).toBe(0);
    for (const script of ["observe-prices.mjs", "sweep-rates.mjs"])
      expect(statSync(join(root, "scripts", script)).mode & 0o111).not.toBe(0);
    const observation = Bun.spawnSync(["bun", join(root, "scripts", "observe-prices.mjs"), "--source", "missing"]);
    expect(observation.exitCode).toBe(0);
    expect(JSON.parse(observation.stdout.toString()).sources_that_refused.missing).toContain("unknown price source");
    const sweep = Bun.spawnSync(["bun", join(root, "scripts", "sweep-rates.mjs"), "--limit", "0"]);
    expect(sweep.exitCode).toBe(2);
    expect(sweep.stderr.toString()).toContain("limit must be a positive safe integer");
  } finally { rmSync(base, { recursive: true, force: true }); }
});
