#!/usr/bin/env bun
// Evict derived file bytes without evicting semantic identity or its lineage.
// Deliberately file-only: directory formats need their own atomic owner.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { createReadStream, existsSync, lstatSync, readdirSync, realpathSync, rmSync, statfsSync, statSync, unlinkSync } from "node:fs";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const now = () => new Date().toISOString();
const digest = value => createHash("sha256").update(value).digest("hex");
async function hashFile(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}
const beneath = (root, path) => {
  const rel = relative(root, path);
  return rel !== "" && rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel);
};
const freeBytes = path => {
  const fs = statfsSync(path);
  return fs.bavail * fs.bsize;
};

function coordinatePath(coordinate) {
  if (typeof coordinate !== "string" || coordinate.includes("#") || coordinate.startsWith("file://")) return null;
  return isAbsolute(coordinate) ? coordinate : null;
}

// The directory digest is the historical Zarr forge's sorted
// `relative-path:file-sha256` manifest, not an invented new identity.
async function bodyFacts(path) {
  const stat = lstatSync(path);
  if (stat.isSymbolicLink()) return null;
  if (stat.isFile()) return { digest: await hashFile(path), bytes: stat.size, directory: false };
  if (!stat.isDirectory()) return null;
  const files = [];
  const walk = directory => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const child = join(directory, entry.name);
      if (entry.isDirectory()) walk(child);
      else if (entry.isFile()) files.push(child);
      else throw new Error(`unsafe directory member: ${child}`);
    }
  };
  walk(path);
  if (!files.length) return null;
  files.sort((a, b) => relative(path, a) < relative(path, b) ? -1 :
    relative(path, a) > relative(path, b) ? 1 : 0);
  const manifest = [];
  let bytes = 0;
  for (const file of files) {
    manifest.push(`${relative(path, file)}:${await hashFile(file)}`);
    bytes += statSync(file).size;
  }
  return { digest: digest(manifest.join("|")), bytes, directory: true };
}

function candidateRows(db, kinds) {
  if (!kinds.length) return [];
  const slots = kinds.map(() => "?").join(",");
  return db.query(`SELECT ri.id,ri.schema_ref,ri.kind,ri.digest,ri.frontier,
      pr.coordinate,pr.bytes,be.canonical_body_kind,be.byte_policy
    FROM representation_instance ri
    JOIN physical_realization pr ON pr.representation_id=ri.id
    JOIN byte_economics be ON be.representation_kind=ri.kind
    WHERE ri.kind IN (${slots}) AND ri.kind<>be.canonical_body_kind AND ri.authority='derived'
      AND ri.reconstructable=1 AND ri.state='physical' AND pr.available=1
      AND be.byte_policy<>'canonical' AND be.canonical_body_kind IS NOT NULL
      AND (SELECT COUNT(*) FROM physical_realization same
        WHERE same.representation_id=ri.id AND same.available=1)=1
    ORDER BY COALESCE(pr.bytes,0) DESC,ri.id`).all(...kinds);
}

function parentRows(db, id, kind) {
  return db.query(`SELECT ri.id,ri.digest,pr.coordinate FROM representation_edge e
    JOIN representation_instance ri ON ri.id=e.parent_representation_id
    JOIN physical_realization pr ON pr.representation_id=ri.id
    WHERE e.child_representation_id=? AND ri.kind=? AND ri.state='physical'
      AND pr.available=1 ORDER BY ri.id`).all(id, kind);
}

async function inspectCandidate(db, estate, row) {
  const root = realpathSync(join(estate, "representations"));
  const path = coordinatePath(row.coordinate);
  if (!path || !existsSync(path) || lstatSync(path).isSymbolicLink()) return null;
  const actual = realpathSync(path);
  if (!beneath(root, actual)) return null;
  const siblings = db.query(`SELECT coordinate FROM physical_realization
    WHERE available=1 AND representation_id<>?`).all(row.id);
  for (const sibling of siblings) {
    const other = coordinatePath(sibling.coordinate);
    if (other && existsSync(other)) {
      const occupied = realpathSync(other);
      if (occupied === actual || beneath(actual, occupied) || beneath(occupied, actual)) return null;
    }
  }
  const stat = statSync(actual);
  const body = await bodyFacts(actual);
  if (!body || !row.digest || body.digest !== row.digest) return null;
  const parents = parentRows(db, row.id, row.canonical_body_kind);
  if (parents.length !== 1) return null;
  const parent = parents[0], parentPath = coordinatePath(parent.coordinate);
  if (!parentPath || !existsSync(parentPath) || !statSync(parentPath).isFile() ||
      !parent.digest || await hashFile(parentPath) !== parent.digest) return null;
  return { id: row.id, semantic_kind: row.schema_ref, kind: row.kind,
    coordinate: actual, registered_coordinate: row.coordinate, digest: row.digest,
    bytes: body.bytes, directory: body.directory,
    parent_coordinate: parentPath, parent_digest: parent.digest,
    policy: row.byte_policy, canonical_body_kind: row.canonical_body_kind,
    inode: stat.ino, device: stat.dev };
}

export async function planDerivedGc(estate, kinds) {
  const path = join(estate, "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing manifold: ${path}`);
  if (!Array.isArray(kinds) || !kinds.length || kinds.some(kind => !kind || typeof kind !== "string"))
    throw new Error("explicit representation kinds required");
  const db = new Database(path, { readonly: true });
  try {
    const eligible = [];
    for (const row of candidateRows(db, kinds)) {
      const inspected = await inspectCandidate(db, estate, row);
      if (inspected) eligible.push(inspected);
    }
    return eligible;
  } finally { db.close(); }
}

// First make a derived body unavailable in SQLite; only then unlink it.
// A crash between those steps leaves an unadvertised orphan, never a live
// catalog pointer to missing bytes. A receipt is written only after unlink.
export async function reclaimDerived(estate, { kinds, floorBytes, limit = Number.MAX_SAFE_INTEGER, reason = "resource pressure" }) {
  if (!Number.isFinite(floorBytes) || floorBytes < 0) throw new Error("invalid free-byte floor");
  if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("invalid eviction limit");
  const planned = await planDerivedGc(estate, kinds);
  const db = new Database(join(estate, "representation-manifold.db"));
  const evicted = [];
  try {
    db.exec("PRAGMA busy_timeout=30000");
    for (const candidate of planned.slice(0, limit)) {
      if (freeBytes(estate) >= floorBytes) break;
      const row = db.query(`SELECT ri.id,ri.schema_ref,ri.kind,ri.digest,pr.coordinate
        FROM representation_instance ri JOIN physical_realization pr ON pr.representation_id=ri.id
        WHERE ri.id=? AND ri.state='physical' AND pr.available=1`).get(candidate.id);
      if (!row || row.coordinate !== candidate.registered_coordinate || row.digest !== candidate.digest) continue;
      const checked = await inspectCandidate(db, estate, {
        ...row, canonical_body_kind: candidate.canonical_body_kind, byte_policy: candidate.policy
      });
      if (!checked || checked.parent_digest !== candidate.parent_digest) continue;
      const stat = statSync(candidate.coordinate);
      if (stat.ino !== candidate.inode || stat.dev !== candidate.device) continue;
      const ts = now();
      db.exec("BEGIN IMMEDIATE");
      try {
        const changed = db.query(`UPDATE physical_realization SET available=0,temperature='evicted',checked_at=?
          WHERE representation_id=? AND available=1 AND coordinate=? AND digest=?`)
          .run(ts, candidate.id, candidate.registered_coordinate, candidate.digest);
        if (changed.changes !== 1) throw new Error(`concurrent realization change: ${candidate.id}`);
        const instance = db.query(`UPDATE representation_instance SET state='reconstructable'
          WHERE id=? AND digest=? AND state='physical' AND NOT EXISTS(
            SELECT 1 FROM physical_realization WHERE representation_id=? AND available=1)`)
          .run(candidate.id, candidate.digest, candidate.id);
        if (instance.changes !== 1) throw new Error(`concurrent representation change: ${candidate.id}`);
        db.query(`UPDATE representation_target SET status='reconstructable',
          reason=? WHERE semantic_kind=? AND representation_kind=?
          AND NOT EXISTS(SELECT 1 FROM representation_instance other
            JOIN physical_realization live ON live.representation_id=other.id
            WHERE other.schema_ref=? AND other.kind=? AND live.available=1)`)
          .run(`derived bytes evicted; reconstruct from ${candidate.parent_coordinate}`,
            candidate.semantic_kind, candidate.kind, candidate.semantic_kind, candidate.kind);
        db.exec("COMMIT");
      } catch (error) { db.exec("ROLLBACK"); throw error; }
      const beforeUnlink = lstatSync(candidate.coordinate);
      const finalBody = await bodyFacts(candidate.coordinate);
      if (beforeUnlink.ino !== candidate.inode || beforeUnlink.dev !== candidate.device ||
          !finalBody || finalBody.bytes !== candidate.bytes ||
          finalBody.digest !== candidate.digest ||
          await hashFile(candidate.parent_coordinate) !== candidate.parent_digest) continue;
      if (candidate.directory) rmSync(candidate.coordinate, { recursive: true });
      else unlinkSync(candidate.coordinate);
      const completed = now();
      const receipt = digest(`evict|${candidate.id}|${candidate.coordinate}|${candidate.digest}|${completed}`);
      db.query("INSERT OR REPLACE INTO eviction_receipt VALUES(?,?,?,?,?,?,?,?,?)")
        .run(receipt, candidate.id, candidate.kind, candidate.coordinate,
          candidate.digest, candidate.bytes, reason, candidate.parent_coordinate, completed);
      evicted.push({ ...candidate, receipt });
    }
  } finally { db.close(); }
  return { planned: planned.length, evicted, free_bytes: freeBytes(estate) };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  try {
    const args = process.argv.slice(2);
    const value = flag => args.includes(flag) ? args[args.indexOf(flag) + 1] : undefined;
    const estate = resolve(value("--estate") ?? "");
    const kinds = args.flatMap((arg, index) => arg === "--kind" ? [args[index + 1]] : []);
    const floorGb = Number(value("--free-floor-gb") ?? "4");
    if (!value("--estate") || !Number.isFinite(floorGb) || floorGb < 0)
      throw new Error("usage: --estate PATH --kind KIND [--kind KIND...] [--free-floor-gb N] [--apply]");
    const result = args.includes("--apply")
      ? await reclaimDerived(estate, { kinds, floorBytes: floorGb * 1024 ** 3 })
      : { planned: await planDerivedGc(estate, kinds), free_bytes: freeBytes(estate), state: "DRY_RUN" };
    console.log(JSON.stringify(result, null, 2));
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
