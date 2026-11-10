#!/usr/bin/env bun
// Rebuild only ANN indexes named by published CMS FactDeltas. A failed rebuild
// leaves its deltas pending; the event record is never deleted.
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { closeSync, constants, existsSync, fstatSync, fsyncSync, linkSync, lstatSync, mkdirSync, openSync, readSync, readdirSync, rmdirSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";

const defaults = {
  deltas: "/var/lib/bonfyre/births/deltas",
  cms: "/usr/local/bin/bonfyre-cms",
  db: "/var/lib/bonfyre/cms/bonfyre_cms.db",
  dryRun: false,
  json: false,
};
const forbiddenInference = "an applied delta is not a correct index; it records that recomputation ran over the named write set";

export function options(args) {
  const result = { ...defaults };
  const values = new Map([["--deltas", "deltas"], ["--cms", "cms"], ["--db", "db"]]);
  for (let i = 0; i < args.length; i++) {
    const key = values.get(args[i]);
    if (key) {
      if (!args[i + 1]) throw new Error(`${args[i]} requires a value`);
      result[key] = args[++i];
    } else if (args[i] === "--dry-run") result.dryRun = true;
    else if (args[i] === "--json") result.json = true;
    else throw new Error(`unknown option: ${args[i]}`);
  }
  return result;
}

export function affectedTypes(delta) {
  const result = new Set();
  for (const write of Array.isArray(delta?.writes) ? delta.writes : []) {
    if (typeof write !== "string") continue;
    const match = /^cms:([A-Za-z0-9_]+)$/.exec(write);
    if (match) result.add(match[1]);
  }
  return result;
}

function rebuild(cms, db, type) {
  const child = spawnSync(cms, ["ann", "build", type, "--db", db], {
    encoding: "utf8", timeout: 120_000, maxBuffer: 8 * 1024 * 1024,
  });
  if (child.error || child.status !== 0) return -1;
  const last = child.stdout.trim().split(/\r?\n/).at(-1);
  try {
    const indexed = JSON.parse(last).indexed;
    return Number.isSafeInteger(indexed) && indexed >= 0 ? indexed : -1;
  } catch {
    return -1;
  }
}

const MAX_DELTA_BYTES = 1024 * 1024;
const sameFile = (a, b) => ["dev", "ino", "size", "mtimeNs", "ctimeNs"].every(key => a[key] === b[key]);
const digest = (name, bytes) => createHash("sha256").update(JSON.stringify(name)).update("\0").update(bytes).digest("hex");

function syncDirectory(path) {
  const fd = openSync(path, "r");
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

function boundedRead(path, maximum) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd, { bigint: true });
    if (!before.isFile() || before.size > BigInt(maximum)) throw new Error("input must be a bounded regular file");
    const buffer = Buffer.alloc(Number(before.size) + 1);
    let used = 0, n;
    while (used < buffer.length && (n = readSync(fd, buffer, used, buffer.length - used, null)) > 0) used += n;
    if (used !== Number(before.size) || !sameFile(before, fstatSync(fd, { bigint: true }))) throw new Error("input changed during read");
    return buffer.subarray(0, used);
  } finally { closeSync(fd); }
}

function parseDelta(path, bytes) {
  const delta = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  if (!delta || typeof delta !== "object" || Array.isArray(delta) || !Array.isArray(delta.writes)) throw new Error("invalid delta");
  return { path, bytes, delta, digest: digest(basename(path), bytes), types: affectedTypes(delta) };
}
const readDelta = path => parseDelta(path, boundedRead(path, MAX_DELTA_BYTES));

function acknowledged(directory, entry) {
  try {
    const receipt = JSON.parse(boundedRead(join(directory, `${entry.digest}.json`), MAX_DELTA_BYTES * 2));
    if (receipt.schema !== "bonfyre.fact-delta.applied.v1" || receipt.source_name !== basename(entry.path) || receipt.source_digest !== entry.digest)
      throw new Error("applied receipt identity mismatch");
    return true;
  } catch (error) {
    if (error.code === "ENOENT") return false;
    throw error;
  }
}

function publishRecord(directory, key, value) {
  const temporary = join(directory, `.${randomUUID()}.tmp`);
  let fd;
  try {
    fd = openSync(temporary, "wx", 0o600);
    const bytes = `${JSON.stringify(value)}\n`;
    if (Buffer.byteLength(bytes) > MAX_DELTA_BYTES * 2) throw new Error("record exceeds bound");
    writeFileSync(fd, bytes);
    fsyncSync(fd);
    closeSync(fd);
    fd = undefined;
    // Publish without replacing any existing receipt, and NEVER rewrite the
    // producer's path. A concurrently published new version has a new digest
    // and remains pending even if it reuses exactly the same filename.
    try { linkSync(temporary, join(directory, `${key}.json`)); }
    catch (error) {
      if (error.code !== "EEXIST" || boundedRead(join(directory, `${key}.json`), MAX_DELTA_BYTES * 2).toString() !== bytes) throw error;
    }
    syncDirectory(directory);
  } finally {
    if (fd !== undefined) closeSync(fd);
    if (existsSync(temporary)) unlinkSync(temporary);
  }
}

function markApplied(directory, entry, rebuilt) {
  publishRecord(directory, entry.digest, { schema: "bonfyre.fact-delta.applied.v1", source_name: basename(entry.path),
    source_digest: entry.digest, delta: entry.delta,
    rebuilt: Object.fromEntries([...entry.types].map(type => [type, rebuilt[type]])), forbidden_inference: forbiddenInference });
}

function readCaptured(directory, name, root) {
  const record = JSON.parse(boundedRead(join(directory, name), MAX_DELTA_BYTES * 2));
  if (record.schema !== "bonfyre.fact-delta.input.v1" || typeof record.source_name !== "string" ||
      basename(record.source_name) !== record.source_name || !record.source_name.endsWith(".json") ||
      typeof record.source_bytes !== "string") throw new Error("invalid captured delta");
  const bytes = Buffer.from(record.source_bytes, "base64");
  if (bytes.length > MAX_DELTA_BYTES || bytes.toString("base64") !== record.source_bytes) throw new Error("invalid captured bytes");
  const entry = parseDelta(join(root, record.source_name), bytes);
  if (record.source_digest !== entry.digest || name !== `${entry.digest}.json`) throw new Error("captured delta digest mismatch");
  return entry;
}

export function run(config) {
  if (!existsSync(config.deltas) || !statSync(config.deltas).isDirectory()) {
    throw new Error(`no delta directory at ${config.deltas}`);
  }
  const receipts = join(config.deltas, ".applied"), inputs = join(config.deltas, ".inputs"), claim = join(config.deltas, ".apply-fact-deltas.claim");
  for (const directory of [receipts, inputs]) {
    if (existsSync(directory) && !lstatSync(directory).isDirectory()) throw new Error("consumer state path must be a real directory");
  }
  let claimed = false;
  if (!config.dryRun) {
    // Serialise consumers, not publishers. A crashed owner leaves the claim
    // held for explicit reconciliation; PID/timeout guesses cannot steal it.
    try { mkdirSync(claim, { mode: 0o700 }); claimed = true; }
    catch (error) {
      if (error.code !== "EEXIST") throw error;
      return { schema: "bonfyre.fact-delta.apply.v1", pending_deltas: null, invalidated_types: [], rebuilt: {},
        applied: 0, failed_types: [], invalid_deltas: [], changed_deltas: [], held: true, dry_run: false, forbidden_inference: forbiddenInference };
    }
  }
  try { return applyBatch(config, receipts, inputs); }
  finally {
    if (claimed) { rmdirSync(claim); syncDirectory(config.deltas); }
  }
}

function applyBatch(config, receipts, inputs) {
  if (!config.dryRun) {
    for (const directory of [receipts, inputs]) {
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      if (!lstatSync(directory).isDirectory()) throw new Error("consumer state path must be a real directory");
    }
    syncDirectory(config.deltas);
  }
  const pending = [], invalid = [], seen = new Set();
  const include = entry => {
    if (entry.delta.applied !== true && !seen.has(entry.digest) && !acknowledged(receipts, entry)) {
      pending.push(entry); seen.add(entry.digest);
    }
  };
  if (existsSync(inputs)) {
    if (!lstatSync(inputs).isDirectory()) throw new Error("input journal must be a real directory");
    for (const name of readdirSync(inputs).filter(name => name.endsWith(".json")).sort()) {
      try { include(readCaptured(inputs, name, config.deltas)); }
      catch { invalid.push(`.inputs/${name}`); }
    }
  }
  for (const name of readdirSync(config.deltas).filter(name => name.endsWith(".json")).sort()) {
    const path = join(config.deltas, name);
    try {
      const entry = readDelta(path);
      include(entry);
    } catch { invalid.push(name); }
  }
  const types = [...new Set(pending.flatMap(entry => [...entry.types]))].sort();
  const rebuilt = Object.create(null), applied = [], failedTypes = [], changed = [];
  if (!config.dryRun) {
    // Capture before launching work: if the publisher replaces a filename
    // during a FAILED build, both its new event and our old version survive.
    for (const entry of pending) publishRecord(inputs, entry.digest, { schema: "bonfyre.fact-delta.input.v1",
      source_name: basename(entry.path), source_digest: entry.digest, source_bytes: entry.bytes.toString("base64") });
    for (const type of types) {
      rebuilt[type] = rebuild(config.cms, config.db, type);
      if (rebuilt[type] < 0) failedTypes.push(type);
    }
    for (const entry of pending) {
      if ([...entry.types].some(type => rebuilt[type] < 0)) continue;
      try {
        markApplied(receipts, entry, rebuilt); applied.push(basename(entry.path));
      }
      catch { invalid.push(basename(entry.path)); }
    }
    for (const name of new Set(applied)) {
      try {
        const current = readDelta(join(config.deltas, name));
        if (current.delta.applied !== true && !acknowledged(receipts, current)) changed.push(name);
      } catch { changed.push(name); }
    }
  }
  return {
    schema: "bonfyre.fact-delta.apply.v1",
    pending_deltas: pending.length,
    invalidated_types: types,
    rebuilt,
    applied: applied.length,
    failed_types: failedTypes,
    invalid_deltas: [...new Set(invalid)].sort(),
    changed_deltas: [...new Set(changed)].sort(),
    held: false,
    dry_run: config.dryRun,
    forbidden_inference: forbiddenInference,
  };
}

if (import.meta.main) {
  try {
    const config = options(process.argv.slice(2));
    const report = run(config);
    if (config.json) process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    else {
      process.stdout.write(`pending deltas: ${report.pending_deltas}\n`);
      process.stdout.write(`invalidated:    ${report.invalidated_types.join(", ") || "(nothing)"}\n`);
      for (const [type, indexed] of Object.entries(report.rebuilt)) process.stdout.write(`  rebuilt ${type}: ${indexed} indexed\n`);
      if (!config.dryRun) process.stdout.write(`acknowledged:   ${report.applied}\n`);
      if (report.held) process.stdout.write("held: consumer claim exists; verify the previous owner stopped before explicit reconciliation\n");
      if (report.changed_deltas.length) process.stdout.write(`new versions:   ${report.changed_deltas.join(", ")} (remain pending)\n`);
    }
    if (report.held || report.changed_deltas.length || report.failed_types.length || report.invalid_deltas.length) process.exitCode = 1;
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}
