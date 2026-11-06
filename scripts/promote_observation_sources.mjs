#!/usr/bin/env bun
// Register observed source identities without binding presence or inventing a URL.
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";

const defaults = {
  parquet: "/var/lib/bonfyre/state/fleming/estate.parquet",
  scratch: "/tmp/source-promote.duckdb",
  matrix: "/var/lib/bonfyre/matrix.db",
  organ: "/usr/local/bin/bonfyre-web-address",
  query: "/usr/local/bin/bonfyre-query",
  limit: 0,
  dryRun: false,
};

export function options(args) {
  const result = { ...defaults };
  const values = new Map([["--parquet", "parquet"], ["--scratch", "scratch"], ["--matrix", "matrix"],
    ["--organ", "organ"], ["--query", "query"], ["--limit", "limit"]]);
  for (let i = 0; i < args.length; i++) {
    const key = values.get(args[i]);
    if (key) {
      if (!args[i + 1]) throw new Error(`${args[i]} requires a value`);
      result[key] = args[++i];
    } else if (args[i] === "--dry-run") result.dryRun = true;
    else throw new Error(`unknown option: ${args[i]}`);
  }
  result.limit = Number(result.limit);
  if (!Number.isSafeInteger(result.limit) || result.limit < 0) throw new Error("--limit must be a nonnegative integer");
  return result;
}

export function sourceIdentity(row) {
  if (typeof row?.src_id !== "string" || !row.src_id) throw new Error("observation source has no src_id");
  const pageSet = row.page_set ?? "";
  if (typeof pageSet !== "string") throw new Error(`source ${row.src_id} has an invalid page set`);
  return {
    sid: `foreign_twin:${row.src_id.replaceAll("src:", "src-").replaceAll(":", "-")}`,
    digest: createHash("sha256").update(pageSet, "utf8").digest("hex"),
    evidence: `observation-corpus:${row.observations}:${row.pages}pages:${String(row.last_seen ?? "None").slice(0, 32)}`,
  };
}

function sourceRows(config) {
  const path = config.parquet.replaceAll("'", "''");
  const sql = `SELECT src_id, dialect, COUNT(*) AS observations,
    COUNT(DISTINCT page_sha256) AS pages,
    string_agg(DISTINCT page_sha256, ',' ORDER BY page_sha256) AS page_set,
    MAX(observed_at) AS last_seen
    FROM read_parquet('${path}')
    GROUP BY src_id, dialect ORDER BY observations DESC`;
  const child = spawnSync(config.query, ["sql", config.scratch, sql], {
    encoding: "utf8", timeout: 600_000, maxBuffer: 128 * 1024 * 1024,
  });
  if (child.error || child.status !== 0) throw new Error(`observation query failed: ${child.error?.message ?? child.stderr.trim()}`);
  const body = child.stdout.split(/\r?\n/).filter(line => !line.startsWith("[query]")).join("\n").trim();
  let rows;
  try { rows = JSON.parse(body); } catch { throw new Error("observation query returned invalid JSON"); }
  if (!Array.isArray(rows)) throw new Error("observation query did not return rows");
  return rows;
}

export function run(config) {
  const observed = sourceRows(config);
  if (!observed.length) throw new Error(`no observation sources found in ${config.parquet}`);
  const rows = config.limit ? observed.slice(0, config.limit) : observed;
  let promoted = 0, failed = 0;
  const failures = [];
  for (const row of rows) {
    let identity;
    try { identity = sourceIdentity(row); }
    catch (error) { failed++; failures.push(error.message); continue; }
    if (config.dryRun) { promoted++; continue; }
    const subject = spawnSync(config.organ, ["subject", "--id", identity.sid, "--kind", "ForeignTwin",
      "--digest", identity.digest, "--evidence", identity.evidence, "--db", config.matrix], {
      encoding: "utf8", timeout: 60_000, maxBuffer: 8 * 1024 * 1024,
    });
    if (subject.error || subject.status !== 0) {
      failed++;
      failures.push(identity.sid);
    } else promoted++;
  }
  return { schema: "bonfyre.observation-source.promote.v1", sources: rows.length, promoted, failed,
    dry_run: config.dryRun, identity_rule: "sha256 over the source's sorted distinct page digests",
    forbidden_inference: "a promoted source is not a claimed, licensed or authorized one; it is a thing observed at scale and chosen to be nameable, and it carries no web address because this estate holds no route to it",
    failures };
}

if (import.meta.main) {
  try {
    const result = run(options(process.argv.slice(2)));
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    if (result.failed) process.exitCode = 1;
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}
