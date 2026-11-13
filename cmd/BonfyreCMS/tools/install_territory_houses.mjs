#!/usr/bin/env bun
// Publish *the selected tenant's* presentation houses using the CMS's
// named entry APIs, not a hardcoded business identity or domain suffix.
// --data PATH or BONFYRE_TENANT_HOUSES_FILE overrides the historical fixture.
// --check validates the selected source without opening the CMS or writing.
//
//   bun cmd/BonfyreCMS/tools/install_territory_houses.mjs --data TENANT.json --check
//   bun cmd/BonfyreCMS/tools/install_territory_houses.mjs --data TENANT.json --db CMS.db --cms BIN
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "../../..");
const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const at = argv.indexOf(name);
  if (at < 0) return fallback;
  if (!argv[at + 1] || argv[at + 1].startsWith("--")) throw new Error(`${name} needs an exact argument`);
  return argv[at + 1];
};
const db = opt("--db", join(homedir(), ".bonfyre/estate-local/cms.db"));
const cms = opt("--cms", join(homedir(), ".bonfyre/bin/bonfyre-cms"));
const sourcePath = resolve(opt("--data", process.env.BONFYRE_TENANT_HOUSES_FILE ||
  join(root, "cmd/BonfyreCMS/data/territory-houses.json")));
const data = JSON.parse(readFileSync(sourcePath, "utf8"));
if (!data || !Array.isArray(data.houses) || !data.houses.length) {
  throw new Error("territory houses: the selected source needs a nonempty houses array");
}
const seen = new Set();
for (const house of data.houses) {
  if (!house || typeof house.template !== "string" ||
      !/^[a-z0-9][a-z0-9_-]{0,127}$/.test(house.template) ||
      typeof house.territory_affinity !== "string" ||
      !/^[a-z][a-z0-9-]{0,31}$/.test(house.territory_affinity) ||
      seen.has(house.template)) {
    throw new Error("territory houses: invalid or duplicate template/territory contract");
  }
  seen.add(house.template);
}
if (argv.includes("--check")) {
  console.log(`territory houses source: valid templates=${data.houses.length} path=${sourcePath}`);
  process.exit(0);
}

function run(args) {
  const r = spawnSync(cms, [...args, "--db", db, "--schemas", join(root, "cmd/BonfyreCMS/content-types")], { encoding: "utf8" });
  // The CMS reports every schema it loads on stderr; only a failure is worth showing.
  if (r.status !== 0) throw new Error(`${args.slice(0, 3).join(" ")}: ${(r.stderr || r.stdout).trim().split("\n").filter(l => !l.startsWith("[schema]")).join(" ")}`);
  return r.stdout.trim();
}
// Which houses are already there, read straight from the table (read-only).
const ids = new Map(spawnSync("sqlite3", ["-readonly", db, "SELECT template, id FROM presentation_template WHERE namespace='root' AND template LIKE 'house-%'"],
  { encoding: "utf8" }).stdout.trim().split("\n").filter(Boolean).map(line => line.split("|")));
for (const house of data.houses) {
  const { says, layout_tokens, ...fields } = house;
  const entry = { ...fields, status: "published", family: "house", reference_categories: "publication house", reference_source: `measured:${data.distilled_from}`,
    semantic_roles: { says }, layout_tokens,
    acceptance_contract: "one house per territory; bands rotate field, paper, paper, night",
    cannot_infer: "a house is presentation only: it borrows no publication's marks, code or words, implies no affiliation with the publications it was distilled from, and cannot select material or change a word on the page" };
  const found = ids.get(house.template);
  const out = found ? run(["entry", "update", "presentation_template", found, JSON.stringify(entry)])
                    : run(["entry", "create", "presentation_template", "--ns", "root", JSON.stringify(entry)]);
  console.log(JSON.stringify({ template: house.template, territory: house.territory_affinity, written: found ? "updated" : "created", out: out.slice(0, 120) }));
}
