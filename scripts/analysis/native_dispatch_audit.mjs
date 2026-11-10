import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dir, "../..");
const rule = resolve(import.meta.dir, "native-command-string-dispatch.yml");
const scan = spawnSync("ast-grep", ["scan", "--rule", rule, "--json=stream", "cmd"], {
  cwd: root,
  encoding: "utf8",
  maxBuffer: 16 * 1024 * 1024,
});
if (scan.error || (scan.status !== 0 && scan.status !== 1)) {
  throw new Error(scan.error?.message ?? scan.stderr.trim() ?? `ast-grep exited ${scan.status}`);
}

const byFile = new Map();
for (const line of scan.stdout.split("\n")) {
  if (!line) continue;
  const match = JSON.parse(line);
  const sites = byFile.get(match.file) ?? [];
  sites.push({ line: match.range.start.line + 1, expression: match.text });
  byFile.set(match.file, sites);
}
const files = [...byFile].map(([file, sites]) => ({ file, count: sites.length, sites }));
files.sort((a, b) => b.count - a.count || a.file.localeCompare(b.file));
const registeredOwners = new Set([...readFileSync(resolve(root, "architecture/commands/native-dispatch.yaff"), "utf8")
  .matchAll(/^  source_path (cmd\/[^\s]+\/src\/main\.c)$/gm)]
  .map((match) => match[1]));
const regressedOwners = files.filter(({ file }) => registeredOwners.has(file)).map(({ file }) => file);
const full = process.argv.includes("--all");
console.log(JSON.stringify({
  source: "ast-grep:C",
  owners: files.length,
  comparisons: files.reduce((count, file) => count + file.count, 0),
  regressed_owners: regressedOwners,
  files: full ? files : files.slice(0, 12).map(({ file, count }) => ({ file, count })),
  omitted_owners: full ? 0 : Math.max(0, files.length - 12),
}));
if (regressedOwners.length) process.exitCode = 1;
