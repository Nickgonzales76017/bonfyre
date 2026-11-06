#!/usr/bin/env bun
// The public command contract is not the implementation or executable census.
import { readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const cmd = join(root, "cmd");
const contract = JSON.parse(readFileSync(join(cmd, "public-command-contract.json"), "utf8"));
const smoke = JSON.parse(readFileSync(join(cmd, "command_smoke.json"), "utf8"));
const names = contract.commands ?? [];
const expected = 91; // The declared stable external ABI, not a count of directories.
if (contract.schema !== "bonfyre.public-command-contract.v1") throw new Error("public command contract: unknown schema");
if (contract.count !== expected || names.length !== expected)
  throw new Error(`public command contract: expected immutable count ${expected}; header=${contract.count} rows=${names.length}`);
if (new Set(names).size !== expected) throw new Error("public command contract: duplicate identity");
const orderedNames = [...names].sort();
if (names.some((name, index) => name !== orderedNames[index]))
  throw new Error("public command contract: identities are not canonical/sorted");
const publicNames = new Set(names);
const missingSource = names.filter(name => {
  try { return !statSync(join(cmd, name)).isDirectory(); } catch { return true; }
});
if (missingSource.length) throw new Error(`public command contract: source family missing: ${missingSource.join(", ")}`);
const rows = smoke.results ?? [];
const seen = new Set(rows.map(row => row.command).filter(Boolean));
const missingCensus = names.filter(name => !seen.has(name));
if (missingCensus.length) throw new Error(`public command contract: absent from smoke census: ${missingCensus.join(", ")}`);
const publicLive = rows.filter(row => publicNames.has(row.command) && row.responds).length;
const auxiliary = [...seen].filter(name => !publicNames.has(name)).sort();
const implementationOnly = readdirSync(cmd).filter(name => name.startsWith("Bonfyre") && !publicNames.has(name)
  && statSync(join(cmd, name)).isDirectory()).sort();
console.log(`public command contract: PASS public=${expected} live=${publicLive} auxiliary_census=${auxiliary.length} implementation_only_dirs=${implementationOnly.length}`);
if (auxiliary.length) console.log(`  auxiliary smoke rows: ${auxiliary.join(", ")}`);
if (implementationOnly.length) console.log(`  implementation-only dirs: ${implementationOnly.join(", ")}`);
