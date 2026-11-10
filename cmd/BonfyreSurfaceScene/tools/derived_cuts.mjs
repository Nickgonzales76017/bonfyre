// Readback-checked materialization of native SurfaceScene's derived indexes.
// An input generation is not owner evidence; it may skip compute only while
// the named derived output still matches the last successfully committed cut.
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

export const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
export const digest = value => sha256(JSON.stringify(value));
export const codeDigest = (...paths) => digest(paths.map(path => sha256(readFileSync(path))));

const order = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const compare = (a, b) => order(a.key, b.key);
function canonical(rows, identity) {
  const seen = new Set();
  return rows.map(row => {
    const key = identity(row);
    if (typeof key !== "string" || !key || seen.has(key))
      throw Error("duplicate or missing derived row identity: " + key);
    seen.add(key);
    return { key, data: row };
  }).sort(compare);
}
function fingerprint(rows, identity) {
  return digest(canonical(rows, identity));
}
export function ensureDerivedLedger(db) {
  db.exec("CREATE TABLE IF NOT EXISTS _scene_derived_generation (" +
    "name TEXT PRIMARY KEY, input_digest TEXT NOT NULL, " +
    "output_digest TEXT NOT NULL, rows INTEGER NOT NULL, " +
    "computed_at TEXT NOT NULL)");
}
export function inspectDerived(db, { name, input, read, identity }) {
  ensureDerivedLedger(db);
  const prior = db.query("SELECT input_digest,output_digest,rows FROM _scene_derived_generation WHERE name=?").get(name);
  const rows = read();
  const actual = fingerprint(rows, identity);
  return { prior, rows, actual, hit: !!prior && prior.input_digest === input &&
    prior.output_digest === actual && prior.rows === rows.length };
}
export function commitDerived(db, { name, input, identity, read, remove, upsert, desired, observed }) {
  const proposed = canonical(desired, identity), wanted = digest(proposed);
  const transaction = db.transaction(() => {
    // A concurrent canonical write is neither this producer's checkpoint nor
    // permission to overwrite it. Refuse and allow a fresh tick to reconcile.
    const latest = db.query("SELECT input_digest,output_digest,rows FROM _scene_derived_generation WHERE name=?").get(name);
    if (digest(latest || null) !== digest(observed.prior || null))
      throw Error("another derivation advanced the generation; refusing stale replacement");
    const before = read();
    if (fingerprint(before, identity) !== observed.actual)
      throw Error("derived owner changed during computation; refusing stale replacement");
    const old = new Map(canonical(before, identity).map(row => [row.key, row.data]));
    const now = new Map(proposed.map(row => [row.key, row.data]));
    let deleted = 0, updated = 0, retained = 0;
    for (const [key, row] of old) {
      if (!now.has(key)) { remove(row); deleted++; }
    }
    for (const [key, row] of now) {
      if (old.has(key) && digest(old.get(key)) === digest(row)) { retained++; continue; }
      upsert(row);
      updated++;
    }
    const actual = fingerprint(read(), identity);
    if (actual !== wanted) throw Error("derived owner readback failed; rolling back output and generation");
    db.query("INSERT INTO _scene_derived_generation(name,input_digest,output_digest,rows,computed_at) " +
      "VALUES(?,?,?,?,datetime('now')) ON CONFLICT(name) DO UPDATE SET " +
      "input_digest=excluded.input_digest,output_digest=excluded.output_digest, " +
      "rows=excluded.rows,computed_at=excluded.computed_at").run(name, input, wanted, desired.length);
    return { deleted, updated, retained };
  });
  return transaction();
}
