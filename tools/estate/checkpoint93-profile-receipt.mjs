#!/usr/bin/env bun
// Shared receipt append and failure summary for checkpoint93 profile units.
import { constants, closeSync, existsSync, openSync, readFileSync, statSync, writeSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function appendProfileReceipt(file, profile, tool, exitCode, started, completed, duration, state, logPath) {
  const rc = Number(exitCode), seconds = Number(duration);
  if (!Number.isSafeInteger(rc) || !Number.isSafeInteger(seconds))
    throw new Error("receipt exit code and duration must be integers");
  const record = {
    schema: "bonfyre.phase94.profile-receipt.v1",
    profile, tool, exit_code: rc, started_at: started, completed_at: completed,
    duration_seconds: seconds, state, log_path: logPath,
  };
  const line = Buffer.from(`${JSON.stringify(record)}\n`, "utf8");
  // One O_APPEND write keeps concurrent profile records from interleaving.
  const fd = openSync(file, constants.O_WRONLY | constants.O_APPEND | constants.O_CREAT, 0o644);
  try {
    if (writeSync(fd, line) !== line.length) throw new Error("short receipt append");
  } finally { closeSync(fd); }
  return record;
}

export function summarizeProfileFailures(file, profile, runStamp) {
  const lines = readFileSync(file, "utf8").split(/\r?\n/);
  const rows = [];
  let unreadable = 0;
  for (const line of lines) {
    if (!line.trim()) continue;
    try { rows.push(JSON.parse(line)); }
    catch { unreadable++; }
  }
  const output = [];
  if (unreadable) output.push(`  (${unreadable} unreadable receipt line(s) skipped)`);
  for (const row of rows) {
    if (row?.profile !== profile || row?.state === "passed" || (row?.started_at ?? "") < runStamp) continue;
    output.push(`  ${row.tool} rc=${row.exit_code} ${row.state}`);
    const log = row.log_path;
    if (typeof log === "string" && existsSync(log) && statSync(log).isFile()) {
      const content = readFileSync(log, "utf8").split(/\r?\n/).map(line => line.trimEnd()).filter(line => line.trim());
      const error = content.findLast(line => line.trimStart().startsWith("error:"));
      const diagnostic = (error ?? content.at(-1) ?? "no output").trim();
      output.push(`      ${Array.from(diagnostic).slice(0, 200).join("")}`);
      output.push(`      ${log}`);
    }
  }
  return output.join("\n") + (output.length ? "\n" : "");
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    const [action, ...args] = process.argv.slice(2);
    if (action === "append" && args.length === 9) appendProfileReceipt(...args);
    else if (action === "summary" && args.length === 3) process.stdout.write(summarizeProfileFailures(...args));
    else throw new Error("usage: checkpoint93-profile-receipt.mjs append FILE PROFILE TOOL RC STARTED COMPLETED DURATION STATE LOG | summary FILE PROFILE RUN_STAMP");
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
