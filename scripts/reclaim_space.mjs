#!/usr/bin/env bun
// Reclaim rebuildable caches and compress retained bodies without losing bytes.
//
// This is for odd cases and for trees that are new, not a schedule. Anything
// that keeps producing waste is fixed where it writes, and then left out of
// here, so a reclaim never has to undo the same thing twice:
//   foreign bodies     settled compressed, and left alone when unchanged, by
//                      bonfyre-project's observe_foreign (settle_retained_body)
//   publish.log        one pass per file, seven compressed (publish_estate.sh)
//   watcher log storm  backs off instead of logging every 750 ms
//   operator.err.log   a reused pid in a stale lock no longer crash-loops it
//   __pycache__ and crate sources are not pruned: they return on next use.
import { statfsSync, chmodSync, closeSync, createReadStream, createWriteStream, existsSync, lstatSync, openSync, readFileSync, readSync, readdirSync, realpathSync, renameSync, rmSync, statSync, unlinkSync, utimesSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { createGzip } from "node:zlib";
import { pipeline } from "node:stream/promises";
import { isAbsolute, join, relative, resolve } from "node:path";
import { tmpdir } from "node:os";

const home = process.env.HOME ?? "";
const prunable = [
  ["~/Library/Caches/com.microsoft.VSCode.ShipIt", "VS Code update staging"],
  ["~/Library/Caches/Google", "browser cache"],
  ["~/Library/Caches/node-gyp", "node build headers"],
  ["~/Library/Caches/pnpm", "package cache"],
  ["~/.cache/puppeteer", "downloaded browsers"],
  ["~/.cache/zig", "compiler cache"],
  ["~/.npm/_cacache", "npm cache"],
  ["~/Library/Caches/pip", "pip cache"],
  ["~/.cache/pip", "pip cache"],
  ["~/.bun/install/cache", "bun package cache"],
];
// Cold, read-mostly trees whose paths and bytes are named by receipts. APFS transparent
// compression keeps both exact for every reader; only the allocation shrinks.
const transparent = [
  "~/.bonfyre/estate/chain", "~/.bonfyre/estate/publications",
  "~/.bonfyre/operator/patch-receipts", "~/.bonfyre/rollback", "~/.bonfyre/agent-hypervisor/backups",
  "~/.bonfyre/agent-hypervisor/recursive-hypervisor-v6", "~/.bonfyre/share/bonfyre/experience-map/releases",
  "~/.bonfyre/workspace-packages", "~/.bonfyre/estate-local/recipe-sources", "~/.bonfyre/estate-local/reports",
];
const dedupeRoots = ["~/.bonfyre"];
const darwin = process.platform === "darwin";
const compressible = [
  ["~/.bonfyre/foreign-sweep/bodies", ".body"],
  ["~/.bonfyre/estate/served", "*"],
  ["/var/lib/bonfyre/foreign/bodies", ".body"],
];
// Never rewritten in place even when bytes would be preserved: live databases, keys, locks.
const liveSuffixes = [".db", ".sqlite3", ".sqlite", ".key", "-wal", "-shm", ".lock", ".pid", ".sock"];
const retainedSuffixes = [".db", ".sqlite3", ".sqlite", ".key", "-wal", "-shm", ".jsonl", ".json"];

function argsOf(argv) {
  const args = { apply: false, keepRawDays: 0.5, scratch: "/private/tmp/claude-503",
    keepScratch: process.env.CLAUDE_SESSION_ID ?? "", keepScratchHours: 24,
    keepPaths: process.env.CLAUDE_SCRATCHPAD ?? "", compressDirs: [], transparentDirs: [], dedupeDirs: [],
    json: false, pruneCaches: true, transparent: true, dedupe: true, coldDays: 2 };
  const values = new Map([["--keep-raw-days", "keepRawDays"], ["--scratch", "scratch"],
    ["--keep-scratch", "keepScratch"], ["--keep-scratch-hours", "keepScratchHours"], ["--keep-paths", "keepPaths"]]);
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i];
    if (flag === "--apply") args.apply = true;
    else if (flag === "--json") args.json = true;
    else if (flag === "--no-prune-caches") args.pruneCaches = false;
    else if (flag === "--no-transparent") args.transparent = false;
    else if (flag === "--no-dedupe") args.dedupe = false;
    else if (flag === "--compress-dir") args.compressDirs.push(argv[++i]);
    else if (flag === "--transparent-dir") args.transparentDirs.push(argv[++i]);
    else if (flag === "--dedupe-dir") args.dedupeDirs.push(argv[++i]);
    else if (flag === "--cold-days") args.coldDays = argv[++i];
    else if (values.has(flag)) args[values.get(flag)] = argv[++i];
    else throw new Error(`unknown argument: ${flag}`);
  }
  for (const key of ["keepRawDays", "keepScratchHours", "coldDays"]) {
    args[key] = Number(args[key]);
    if (!Number.isFinite(args[key]) || args[key] < 0) throw new Error(`invalid ${key}`);
  }
  return args;
}

function expand(path) { return path.startsWith("~/") ? join(home, path.slice(2)) : path; }
function isDirectory(path) { try { return lstatSync(path).isDirectory(); } catch { return false; } }
function sizeOf(path) {
  let total = 0;
  const stack = [path];
  while (stack.length) {
    const next = stack.pop();
    let entries;
    try { entries = readdirSync(next, { withFileTypes: true }); } catch { continue; }
    for (const entry of entries) {
      const full = join(next, entry.name);
      if (entry.isDirectory()) stack.push(full);
      else { try { total += lstatSync(full).size; } catch {} }
    }
  }
  return total;
}
function newestFile(path) {
  let newest = 0;
  const stack = [path];
  while (stack.length) {
    const next = stack.pop();
    try { newest = Math.max(newest, lstatSync(next).mtimeMs); } catch { continue; }
    let entries;
    try { entries = readdirSync(next, { withFileTypes: true }); } catch { continue; }
    for (const entry of entries) {
      const full = join(next, entry.name);
      if (entry.isDirectory()) stack.push(full);
      else { try { newest = Math.max(newest, lstatSync(full).mtimeMs); } catch {} }
    }
  }
  return newest;
}
function human(value) {
  let n = value;
  for (const unit of ["B", "K", "M", "G", "T"]) {
    if (n < 1024 || unit === "T") return `${Math.round(n)}${unit}`;
    n /= 1024;
  }
}
function protectedArtifact(path) { return retainedSuffixes.some(suffix => path.endsWith(suffix)); }
function within(a, b) { const sub = relative(a, b); return sub === "" || (!sub.startsWith("..") && !isAbsolute(sub)); }

async function compressDir(path, suffix, keepRawDays, apply) {
  const cutoff = Date.now() - keepRawDays * 86400000;
  const result = { path, files: 0, before: 0, after: 0 };
  const stack = [path];
  while (stack.length) {
    const next = stack.pop();
    let entries;
    try { entries = readdirSync(next, { withFileTypes: true }); } catch { continue; }
    for (const entry of entries) {
      const full = join(next, entry.name);
      if (entry.isDirectory()) { stack.push(full); continue; }
      if (!entry.isFile() || entry.name.endsWith(".gz") || protectedArtifact(entry.name)
        || (suffix !== "*" && !entry.name.endsWith(suffix))) continue;
      let metadata;
      try { metadata = statSync(full); } catch { continue; }
      if (!metadata.size || metadata.mtimeMs > cutoff) continue;
      result.files++;
      result.before += metadata.size;
      if (!apply) { result.after += Math.floor(metadata.size / 7); continue; }
      const packed = `${full}.gz`;
      const temporary = `${packed}.${process.pid}.tmp`;
      if (existsSync(packed)) { result.after += metadata.size; continue; }
      try {
        await pipeline(createReadStream(full), createGzip({ level: 9 }), createWriteStream(temporary, { flags: "wx" }));
        utimesSync(temporary, metadata.atime, metadata.mtime);
        renameSync(temporary, packed);
        result.after += statSync(packed).size;
        unlinkSync(full);
      } catch (error) {
        rmSync(temporary, { force: true });
        result.after += metadata.size;
        console.error(`keeping ${full}: ${error.message}`);
      }
    }
  }
  return result;
}

function walkFiles(root, visit) {
  const stack = [root];
  while (stack.length) {
    const next = stack.pop();
    let entries;
    try { entries = readdirSync(next, { withFileTypes: true }); } catch { continue; }
    for (const entry of entries) {
      const full = join(next, entry.name);
      if (entry.isDirectory()) stack.push(full);
      else if (entry.isFile()) visit(full, entry.name);
    }
  }
}
function allocated(path) { try { return statSync(path).blocks * 512; } catch { return 0; } }
function digest(path) {
  const hash = createHash("sha256"), buffer = Buffer.allocUnsafe(1 << 20), fd = openSync(path, "r");
  try { for (let n; (n = readSync(fd, buffer, 0, buffer.length, null)) > 0;) hash.update(buffer.subarray(0, n)); }
  finally { closeSync(fd); }
  return hash.digest("hex");
}
function openFiles(root) {
  const out = spawnSync("lsof", ["-Fn", "+D", root], { encoding: "utf8", maxBuffer: 1 << 28 }).stdout ?? "";
  return new Set(out.split("\n").filter(line => line.startsWith("n")).map(line => line.slice(1)));
}
// A file another process holds, a database, or a hard link is never rewritten.
function rewritable(path, name, open) {
  if (liveSuffixes.some(suffix => name.endsWith(suffix)) || open.has(path)) return null;
  try { const metadata = lstatSync(path); return metadata.isFile() && metadata.nlink === 1 ? metadata : null; }
  catch { return null; }
}
// Swap in a replacement only when its bytes are identical, keeping mode and times.
function swapIn(path, temporary, metadata, expected) {
  if (digest(temporary) !== expected) throw new Error("replacement bytes differ");
  chmodSync(temporary, metadata.mode & 0o7777);
  utimesSync(temporary, metadata.atime, metadata.mtime);
  renameSync(temporary, path);
}

function transparentDir(path, coldDays, apply) {
  const result = { path, files: 0, before: 0, after: 0 };
  if (!darwin) return result;
  const cutoff = Date.now() - coldDays * 86400000, open = openFiles(path);
  walkFiles(path, (full, name) => {
    if (name.endsWith(".gz") || name.endsWith(".zst") || name.includes(".afsc.")) return;
    const metadata = rewritable(full, name, open);
    if (!metadata || metadata.size < 8192 || metadata.mtimeMs > cutoff) return;
    // A clone's blocks are already shared; compressing it gives it private ones.
    if (sharesAllBlocks(full)) return;
    const before = allocated(full), temporary = join(full, "..", `.${name}.afsc.${process.pid}`);
    try {
      if (spawnSync("/usr/bin/ditto", ["--hfsCompression", full, temporary]).status !== 0) throw new Error("ditto failed");
      const after = allocated(temporary);
      if (after >= before) { rmSync(temporary, { force: true }); return; }
      result.files++; result.before += before; result.after += after;
      if (apply) swapIn(full, temporary, metadata, digest(full)); else rmSync(temporary, { force: true });
    } catch (error) { rmSync(temporary, { force: true }); console.error(`keeping ${full}: ${error.message}`); }
  });
  return result;
}

// Byte-identical files become APFS clones of one copy: same paths and bytes, shared blocks
// until one is written. Run after transparent compression so clones share compressed blocks.
// APFS marks a file whose every block is shared with another file (a clone).
// Without this a second run "found" every file the first had already cloned,
// re-cloned it and reported the same 394 MB as savings again.
let extFlags;
function sharesAllBlocks(path) {
  if (!darwin) return false;
  if (extFlags === undefined) {
    try {
      const { dlopen, FFIType, ptr } = require("bun:ffi");
      const libc = dlopen("/usr/lib/libSystem.B.dylib", { getattrlist: { args: [FFIType.cstring, FFIType.ptr, FFIType.ptr, FFIType.u64, FFIType.u32], returns: FFIType.i32 } });
      const request = new Uint8Array(24), view = new DataView(request.buffer);
      view.setUint16(0, 5, true);            // ATTR_BIT_MAP_COUNT
      view.setUint32(20, 0x00000200, true);  // forkattr: ATTR_CMNEXT_EXT_FLAGS
      extFlags = file => {
        const answer = new Uint8Array(16);
        const name = Buffer.from(file + "\0");
        // FSOPT_NOFOLLOW | FSOPT_ATTR_CMN_EXTENDED
        if (libc.symbols.getattrlist(ptr(name), ptr(request), ptr(answer), 16, 0x21) !== 0) return 0n;
        return new DataView(answer.buffer).getBigUint64(4, true);
      };
    } catch { extFlags = null; }
  }
  return extFlags ? (extFlags(path) & 0x40n) !== 0n : false; // EF_SHARES_ALL_BLOCKS
}

function dedupeDir(path, apply) {
  const result = { path, files: 0, bytes: 0 };
  if (!darwin) return result;
  const open = openFiles(path), bySize = new Map();
  walkFiles(path, (full, name) => {
    const metadata = rewritable(full, name, open);
    if (!metadata || metadata.size < 65536) return;
    (bySize.get(metadata.size) ?? bySize.set(metadata.size, []).get(metadata.size)).push([full, metadata]);
  });
  for (const group of bySize.values()) {
    if (group.length < 2) continue;
    const byHash = new Map();
    for (const [full, metadata] of group) {
      try { const hash = digest(full); (byHash.get(hash) ?? byHash.set(hash, []).get(hash)).push([full, metadata]); } catch {}
    }
    for (const [hash, same] of byHash) {
      const [[canonical], ...rest] = same;
      for (const [full, metadata] of rest) {
        if (sharesAllBlocks(full)) continue;
        result.files++; result.bytes += allocated(full);
        if (!apply) continue;
        const temporary = join(full, "..", `.${full.split("/").pop()}.clone.${process.pid}`);
        try {
          if (spawnSync("/bin/cp", ["-c", canonical, temporary]).status !== 0) throw new Error("clone failed");
          swapIn(full, temporary, metadata, hash);
        } catch (error) { rmSync(temporary, { force: true }); console.error(`keeping ${full}: ${error.message}`); }
      }
    }
  }
  return result;
}

function obsoleteExtensions() {
  const root = expand("~/.vscode/extensions"), marker = join(root, ".obsolete");
  let names;
  try { names = Object.keys(JSON.parse(readFileSync(marker, "utf8"))); } catch { return { root, marker, names: [] }; }
  return { root, marker, names: names.filter(name => /^[\w.-]+$/.test(name) && isDirectory(join(root, name))) };
}

async function main() {
  const args = argsOf(process.argv.slice(2));
  const free = () => { try { const fs = statfsSync(home || "/"); return fs.bavail * fs.bsize; } catch { return 0; } };
  const freeBefore = free();
  const report = { schema: "bonfyre-reclaim.v1", applied: args.apply, compressed: [], pruned: [], kept: [] };
  for (const [path, suffix] of [...compressible, ...args.compressDirs.map(path => [path, "*"])]) {
    const full = expand(path);
    if (isDirectory(full)) report.compressed.push(await compressDir(full, suffix, args.keepRawDays, args.apply));
  }
  if (args.pruneCaches) for (const [path, why] of prunable) {
    const full = expand(path);
    if (!isDirectory(full)) continue;
    const bytes = sizeOf(full);
    if (!bytes) continue;
    if (args.apply) rmSync(full, { recursive: true, force: true });
    report.pruned.push({ path: full, why, bytes });
  }
  if (args.pruneCaches) {
    const extensions = obsoleteExtensions();
    for (const name of extensions.names) {
      const full = join(extensions.root, name), bytes = sizeOf(full);
      if (args.apply) rmSync(full, { recursive: true, force: true });
      report.pruned.push({ path: full, why: "editor extension version VS Code marked replaced", bytes });
    }
    if (args.apply && extensions.names.length) writeFileSync(extensions.marker, "{}");
  }
  report.transparent = [];
  if (args.transparent) for (const path of args.transparentDirs.length ? args.transparentDirs : transparent) {
    const full = expand(path);
    if (isDirectory(full)) report.transparent.push(transparentDir(full, args.coldDays, args.apply));
  }
  report.deduped = [];
  if (args.dedupe) for (const path of args.dedupeDirs.length ? args.dedupeDirs : dedupeRoots) {
    const full = expand(path);
    if (isDirectory(full)) report.deduped.push(dedupeDir(full, args.apply));
  }
  if (isDirectory(args.scratch)) {
    const scratch = realpathSync(args.scratch);
    const temporaryRoots = [realpathSync(tmpdir()), "/private/tmp", "/tmp"];
    if (!temporaryRoots.some(root => within(root, scratch) && root !== scratch)) throw new Error(`scratch must be below a temporary directory: ${scratch}`);
    const keep = args.keepPaths.split(",").map(path => path.trim()).filter(Boolean).map(path => {
      try { return realpathSync(path); } catch { return resolve(path); }
    });
    for (const group of readdirSync(args.scratch, { withFileTypes: true })) {
      if (!group.isDirectory() || group.name === "bundled-skills") continue;
      const parent = join(args.scratch, group.name);
      for (const session of readdirSync(parent, { withFileTypes: true })) {
        if (!session.isDirectory() || (args.keepScratch && session.name === args.keepScratch)) continue;
        const full = join(parent, session.name);
        if (newestFile(full) > Date.now() - args.keepScratchHours * 3600000) {
          report.kept.push({ path: full, why: "holds work written inside the keep window" });
          continue;
        }
        const canonical = realpathSync(full);
        if (keep.some(path => within(canonical, path) || within(path, canonical))) {
          report.kept.push({ path: full, why: "named as kept" });
          continue;
        }
        const bytes = sizeOf(full);
        if (bytes < 1_000_000) continue;
        if (args.apply) rmSync(full, { recursive: true, force: true });
        report.pruned.push({ path: full, why: "another session's scratch", bytes });
      }
    }
  }
  report.compressed_bytes = report.compressed.reduce((sum, item) => sum + item.before - item.after, 0);
  report.pruned_bytes = report.pruned.reduce((sum, item) => sum + item.bytes, 0);
  report.transparent_bytes = report.transparent.reduce((sum, item) => sum + item.before - item.after, 0);
  report.deduped_bytes = report.deduped.reduce((sum, item) => sum + item.bytes, 0);
  report.reclaimed_bytes = report.compressed_bytes + report.pruned_bytes + report.transparent_bytes + report.deduped_bytes;
  // Identical files may already share blocks, so an applied run reports what the volume actually gained.
  if (args.apply) report.volume_gained_bytes = free() - freeBefore;
  if (args.json) console.log(JSON.stringify(report));
  else {
    for (const item of report.compressed) if (item.files) console.log(`compress ${item.path}: ${item.files} files ${human(item.before)} -> ${human(item.after)}`);
    for (const item of report.pruned) console.log(`prune    ${item.path} (${item.why}) ${human(item.bytes)}`);
    for (const item of report.transparent) if (item.files) console.log(`shrink   ${item.path}: ${item.files} files ${human(item.before)} -> ${human(item.after)} on disk, same bytes`);
    for (const item of report.deduped) if (item.files) console.log(`share    ${item.path}: ${item.files} identical files ${human(item.bytes)}`);
    console.log(`${args.apply ? "reclaimed" : "would reclaim"} ${human(report.reclaimed_bytes)} (compressed ${human(report.compressed_bytes)}, pruned ${human(report.pruned_bytes)}, shrunk ${human(report.transparent_bytes)}, shared ${human(report.deduped_bytes)})`);
    if (args.apply) console.log(`volume gained ${human(Math.max(0, report.volume_gained_bytes))}`);
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
