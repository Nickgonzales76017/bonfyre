// A person who has never used bonfyre must be able to start from the bare command:
// a short start screen, commands grouped by what they want to do, plain errors that
// point somewhere, and none of the lab kernels or backup binaries in their way.
import { test, expect } from "bun:test";
import { chmodSync, mkdtempSync, mkdirSync, rmSync, writeFileSync, copyFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const binary = resolve(here, "../bonfyre");
const run = (args, opts = {}) => {
  const out = Bun.spawnSync([opts.bin ?? binary, ...args], { stdout: "pipe", stderr: "pipe", env: { ...process.env, ...opts.env } });
  return { code: out.exitCode, out: out.stdout.toString(), err: out.stderr.toString() };
};
const surface = JSON.parse(run(["list", "--json", "--all"]).out);
const rows = surface.sections.flatMap(section => section.commands.map(row => ({ ...row, section: section.name })));
const routed = rows.filter(row => row.tier);

test("bare bonfyre is a start screen, not the whole command table", () => {
  const bare = run([]);
  expect(bare.code).toBe(0);
  expect(bare.out).toContain("Start here");
  expect(bare.out).toContain("bonfyre site new");
  expect(bare.out.split("\n").length).toBeLessThan(45);
  expect(run(["help"]).out).toBe(bare.out);
});

test("every routed command carries a tier and every everyday command is reachable from the start screen", () => {
  expect(routed.length).toBeGreaterThan(90);
  for (const row of routed) expect(["start", "everyday", "lab"]).toContain(row.tier);
  const help = run(["help"]).out;
  for (const row of routed.filter(row => row.tier !== "lab")) expect(help).toMatch(new RegExp(`(^|\\s)${row.command}(\\s|$)`, "m"));
  for (const row of routed.filter(row => row.tier === "start")) expect(help.split("Start here")[1]).toContain(`bonfyre ${row.command}`);
});
test("RepositoryFabric, PackGraph and Parquet are first-class read-only discoverable surfaces", () => {
  const publicNames = rows.filter(row => row.tier !== "lab").map(row => row.command);
  for (const command of ["repository", "pack-graph", "parquet"]) expect(publicNames).toContain(command);
  const repository = run(["repository", "--help"], { env: { HOME: join(tmpdir(), "bonfyre-unconfigured-help") } });
  expect(repository.code).toBe(0);
  expect(repository.out).toContain("checkout");
  expect(repository.out).toContain("--receipt");
  const graph = run(["pack-graph", "--help"]);
  expect(graph.code).toBe(0);
  expect(graph.out).toContain("compile|verify|plan");
  expect(graph.out).toContain("install");
  const parquet = run(["parquet", "index", "--help"]);
  expect(parquet.code).toBe(0);
  expect(parquet.out).toContain("read-only derived index");
  const invalid = run(["pack-graph", "unknown"]);
  expect(invalid.code).toBe(2);
  expect(invalid.err).toContain("unknown operation");
  expect(rows.find(row => row.command === "pack")?.description).not.toContain("PackGraph");
});


test("JSON discovery has the same public scope as the human list, with lab opt-in", () => {
  const publicJson = JSON.parse(run(["list", "--json"]).out);
  const allJson = JSON.parse(run(["list", "--json", "--all"]).out);
  const publicRows = publicJson.sections.flatMap(section => section.commands);
  const allRows = allJson.sections.flatMap(section => section.commands);
  expect(publicRows.every(row => row.tier !== "lab")).toBe(true);
  expect(allRows.some(row => row.tier === "lab")).toBe(true);
  expect(publicJson.summary.total).toBeLessThan(allJson.summary.total);
  expect(publicRows.map(row => row.command)).toEqual(allRows.filter(row => row.tier && row.tier !== "lab").map(row => row.command));
});

test("lab kernels stay out of the everyday list and search, and come back with --all", () => {
  const lab = routed.filter(row => row.tier === "lab").map(row => row.command);
  expect(lab).toContain("violence");
  const everyday = run(["list"]).out, all = run(["list", "--all"]).out;
  for (const name of lab) {
    const listed = new RegExp(`^\\s+\\w+\\s+${name}\\s`, "m");
    expect(everyday).not.toMatch(listed);
    expect(all).toMatch(listed);
  }
  expect(run(["list", "violence"]).out).toContain("add --all");
});

test("a mistyped or unknown command fails and points at the right one", () => {
  const typo = run(["transcrib"]);
  expect(typo.code).toBe(1);
  expect(typo.err).toContain("did you mean: transcribe");
  expect(run(["lisst"]).err).toContain("did you mean: list");
  const goal = run(["invoice"]);
  expect(goal.code).toBe(1);
  expect(goal.err).toMatch(/try:\s+pay/);
  expect(goal.err.match(/\bpay\b/g)).toHaveLength(1);
});

test("subcommand families print usage instead of exiting silently", () => {
  for (const family of ["self", "precision"]) {
    const bare = run([family]);
    expect(bare.code).toBe(2);
    expect(bare.err).toContain(`usage: bonfyre ${family}`);
  }
});

test("backup and patched binaries beside the front door are never commands", () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-front-door-")), bin = join(root, "BonfyreCLI");
  try {
    mkdirSync(bin);
    copyFileSync(binary, join(bin, "bonfyre"));
    chmodSync(join(bin, "bonfyre"), 0o755);
    for (const name of ["bonfyre-moe.patched2", "bonfyre-api-selftest.archive", "bonfyre-widget"]) {
      writeFileSync(join(bin, name), "#!/bin/sh\necho ran-$0\n");
      chmodSync(join(bin, name), 0o755);
    }
    const all = run(["list", "--all"], { bin: join(bin, "bonfyre") }).out;
    expect(all).toContain("widget");
    expect(all).not.toContain("patched2");
    expect(all).not.toContain("selftest.archive");
    expect(run(["list"], { bin: join(bin, "bonfyre") }).out).not.toContain("widget");
    const backup = run(["moe.patched2"], { bin: join(bin, "bonfyre") });
    expect(backup.code).toBe(1);
    expect(backup.out).not.toContain("ran-");
    expect(run(["widget"], { bin: join(bin, "bonfyre") }).out).toContain("ran-");
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("self repair honors an explicit repo root even when launched elsewhere", () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-repair-root-"));
  const home = mkdtempSync(join(tmpdir(), "bonfyre-repair-home-"));
  try {
    mkdirSync(join(root, "cmd", "BonfyreCLI", "src"), { recursive: true });
    mkdirSync(join(root, "cmd", "BonfyreModel"), { recursive: true });
    mkdirSync(join(home, ".bonfyre", "bin"), { recursive: true });
    writeFileSync(join(root, "Makefile"), "all:\n\t@true\n");
    writeFileSync(join(root, "cmd", "BonfyreCLI", "src", "main.c"), "/* fixture */\n");
    writeFileSync(join(root, "cmd", "BonfyreModel", "bonfyre-model"), "#!/bin/sh\necho repo-model\n");
    chmodSync(join(root, "cmd", "BonfyreModel", "bonfyre-model"), 0o755);
    writeFileSync(join(home, ".bonfyre", "bin", "bonfyre-model"), "#!/bin/sh\necho installed-model\n");
    chmodSync(join(home, ".bonfyre", "bin", "bonfyre-model"), 0o755);

    const result = run(["self", "optimize", "--root", root, "--dry-run"], {
      env: {
        HOME: home,
        BONFYRE_REPO_ROOT: "",
        BONFYRE_ROOT: "",
        PATH: process.env.PATH,
      },
    });
    expect(result.code).toBe(0);
    const body = JSON.parse(result.out);
    const modelRepair = body.plans.find(plan =>
      plan.kind === "command_repair" && plan.title.includes("model"));
    expect(modelRepair).toBeTruthy();
  } finally {
    rmSync(root, { recursive: true, force: true });
    rmSync(home, { recursive: true, force: true });
  }
}, 15000);
