import { test, expect } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

test("the scheduled native screen is staged with its CUE policy and reports missing private terms", () => {
  const root = resolve(import.meta.dir, "../..");
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-screen-install-"));
  try {
    const install = Bun.spawnSync(["make", "-C", join(root, "scripts"), "install-content-screen", `MEMBRANE_ROOT=${dir}`]);
    expect(install.exitCode).toBe(0);
    const cli = join(dir, "scripts", "content-screen.mjs");
    expect(existsSync(cli)).toBe(true);
    expect(existsSync(join(dir, "scripts", "content-screen.cue"))).toBe(true);
    const run = Bun.spawnSync(["bun", cli], { env: { ...process.env, BONFYRE_SCREEN_TERMS: join(dir, "missing.txt") } });
    expect(run.exitCode).toBe(2);
    expect(JSON.parse(run.stdout.toString()).state).toBe("installed_without_terms");
    writeFileSync(join(dir, "empty.txt"), "");
    const failed = Bun.spawnSync(["bun", cli], { env: { ...process.env, BONFYRE_SCREEN_TERMS: join(dir, "empty.txt") } });
    // Only an absent list is the declared unconfigured state. A malformed
    // installation is a service failure, never silently accepted by systemd.
    expect(failed.exitCode).toBe(1);
    const unit = readFileSync(join(root, "deploy/systemd/bonfyre-content-screen.service"), "utf8");
    expect(unit).toContain("/opt/bonfyre/native-src/scripts/content-screen.mjs");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
