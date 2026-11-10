#!/usr/bin/env bun
// Publish each territory's house -- the studied presentation its pages wear on
// every band -- from cmd/BonfyreCMS/data/territory-houses.json, through the
// CMS's own entry commands.  A house already present is updated in place.
//
//   bun cmd/BonfyreCMS/tools/install_territory_houses.mjs --db CMS.db [--cms BIN]
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "../../..");
const argv = process.argv.slice(2);
const opt = (name, fallback) => argv.includes(name) ? argv[argv.indexOf(name) + 1] : fallback;
const db = opt("--db", join(homedir(), ".bonfyre/estate-local/cms.db"));
const cms = opt("--cms", join(homedir(), ".bonfyre/bin/bonfyre-cms"));
const data = JSON.parse(readFileSync(join(root, "cmd/BonfyreCMS/data/territory-houses.json"), "utf8"));

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
