// Every command a person is shown must answer --help, -h and help the same way:
// usage on stdout, nothing on stderr, exit 0, and nothing created or opened.
// Built organs are checked from their source tree; an unbuilt organ is reported, not passed.
import { test, expect } from "bun:test";
import { existsSync, mkdtempSync, readdirSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const front = join(root, "cmd/BonfyreCLI/bonfyre");
const surface = JSON.parse(Bun.spawnSync([front, "list", "--json"], { stdout: "pipe" }).stdout.toString());
const shown = surface.sections.flatMap(section => section.commands).filter(row => row.tier && row.tier !== "lab");

function built(row) {
  for (const path of [`cmd/${row.module}/${row.binary}`, `cmd/${row.module}/build/${row.binary}`])
    if (existsSync(join(root, path))) return realpathSync(join(root, path));
  return null;
}

test("every command on the start screen and in 'bonfyre list' keeps the help contract", () => {
  const home = mkdtempSync(join(tmpdir(), "bonfyre-help-")), failures = [], unbuilt = [];
  try {
    for (const row of shown) {
      const binary = built(row);
      if (!binary) { unbuilt.push(row.command); continue; }
      // The runtime gateway answers 'doctor' as a verb, exactly as the front door calls it.
      const prefix = row.binary === "bonfyre-runtime" && row.command !== "runtime" ? [row.command] : [];
      for (const flag of ["--help", "-h", "help"]) {
        const out = Bun.spawnSync([binary, ...prefix, flag], { cwd: home, stdout: "pipe", stderr: "pipe", timeout: 5000,
          env: { ...process.env, HOME: home, XDG_DATA_HOME: join(home, "data"), XDG_STATE_HOME: join(home, "state") } });
        const why = [];
        if (out.exitCode !== 0) why.push(`exit ${out.exitCode}`);
        if (!out.stdout.length) why.push("no usage on stdout");
        if (out.stderr.length) why.push("wrote to stderr");
        if (why.length) failures.push(`${row.command} ${flag}: ${why.join(", ")}`);
      }
      const left = readdirSync(home);
      if (left.length) { failures.push(`${row.command}: help created ${left.join(", ")}`); for (const name of left) rmSync(join(home, name), { recursive: true, force: true }); }
    }
  } finally { rmSync(home, { recursive: true, force: true }); }
  if (unbuilt.length) console.log(`not built here, so not checked: ${unbuilt.join(", ")}`);
  expect(failures).toEqual([]);
  expect(shown.length - unbuilt.length).toBeGreaterThan(60);
}, 120000);
