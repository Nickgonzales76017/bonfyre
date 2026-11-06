#!/usr/bin/env bun
// WorkGraph leases execution; SourceFrontier still owns channel semantics.
import { readFileSync, writeFileSync } from "node:fs";

export function projectClaims(lines) {
  const ids = [];
  const finish = [];
  for (const line of lines.split(/\r?\n/)) {
    let row;
    try { row = JSON.parse(line); } catch { continue; }
    const mission = String(row.mission_id ?? "");
    const token = String(row.claim_token ?? "");
    const node = String(row.node_id ?? "job");
    const worker = String(row.worker_id ?? "");
    if (!mission.startsWith("source-qualify:") || !token || !worker) continue;
    const channel = mission.slice("source-qualify:".length);
    if (!channel || [mission, token, node, worker].some(value => /[\t\r\n]/.test(value))) continue;
    ids.push(channel);
    finish.push([mission, node, worker, token, `bonfyre://source-frontier/channel/${channel}`].join("\t"));
  }
  return { ids, finish };
}

if (import.meta.main) {
  try {
    const argv = process.argv.slice(2);
    const args = {};
    for (let i = 0; i < argv.length; i += 2) args[argv[i]] = argv[i + 1];
    for (const flag of ["--claims", "--ids", "--finish"]) if (!args[flag]) throw new Error(`missing ${flag}`);
    const projected = projectClaims(readFileSync(args["--claims"], "utf8"));
    writeFileSync(args["--ids"], projected.ids.join("\n") + (projected.ids.length ? "\n" : ""));
    writeFileSync(args["--finish"], projected.finish.join("\n") + (projected.finish.length ? "\n" : ""));
    console.log(JSON.stringify({ claimed: projected.ids.length }));
  } catch (error) { console.error(error); process.exitCode = 1; }
}
