#!/usr/bin/env bun
import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

const systemColumns = new Set(["id", "namespace", "created_at", "updated_at", "published_at"]);

function options(argv) {
  const out = {
    estate: join(homedir(), ".bonfyre/estate-local"),
    repo: join(homedir(), "Developer/Bonfyre"),
    apply: false,
  };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--apply") out.apply = true;
    else if ((argv[i] === "--estate" || argv[i] === "--repo") && i + 1 < argv.length) {
      out[argv[i].slice(2)] = argv[++i];
    } else throw new Error(`unknown or incomplete argument: ${argv[i]}`);
  }
  return out;
}

function main(args) {
  const cmsBin = join(args.repo, "cmd/BonfyreCMS/bonfyre-cms");
  const schemas = join(args.repo, "cmd/BonfyreCMS/content-types");
  const dbPath = join(args.estate, "cms.db");
  if (!existsSync(cmsBin) || !existsSync(dbPath)) {
    console.log(JSON.stringify({ error: "CMS owner or estate is unavailable" }));
    return 2;
  }
  const db = new Database(dbPath);
  let rows;
  try {
    rows = db.query(
      "SELECT * FROM internet_source WHERE status='published' AND kind='event' " +
      "AND COALESCE(family,'')<>'events' ORDER BY id").all();
  } finally {
    db.close();
  }
  const sources = rows.map(row => row.source);
  if (!args.apply) {
    console.log(JSON.stringify({ event_sources_to_refile: rows.length, sources }, null, 1));
    return 0;
  }
  let written = 0;
  let refused = 0;
  for (const row of rows) {
    const payload = Object.fromEntries(Object.entries(row).filter(([key]) => !systemColumns.has(key)));
    payload.family = "events";
    const result = Bun.spawnSync([
      cmsBin, "entry", "update", "internet_source", String(row.id),
      JSON.stringify(payload), "--ns", row.namespace, "--db", dbPath,
      "--schemas", schemas,
    ], { stdout: "pipe", stderr: "pipe" });
    if (result.exitCode) refused++;
    else written++;
  }
  console.log(JSON.stringify({ event_sources_refiled: written, refused, sources }, null, 1));
  return refused ? 1 : 0;
}

try {
  process.exitCode = main(options(Bun.argv.slice(2)));
} catch (error) {
  console.error(`project event sources failed: ${error.message}`);
  process.exitCode = 1;
}
