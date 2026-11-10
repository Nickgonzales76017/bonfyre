#!/usr/bin/env bun
// Derived operational summary; never a semantic or transport authority.
import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const sections = Object.freeze([
  { name: "plans", table: "plan_representation", fields: "plan_kind,state", title: "PLANS", width: 28 },
  { name: "carriers", table: "carrier_binding", fields: "carrier,state", title: "CARRIERS", width: 28 },
  { name: "transports", table: "transport_binding", fields: "transport,state", title: "TRANSPORTS", width: 28 },
  { name: "metadata", table: "metadata_fabric", fields: "kind,state", title: "METADATA", width: 28 },
  { name: "targets", table: "representation_target", fields: "status", title: "TARGET STATES", width: 30 },
]);

export function inspectFabric(estate) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error(`missing manifold: ${path}`);
  const db = new Database(path, { readonly: true, create: false });
  try {
    const result = { schema: "bonfyre-shift-1.6-fabric.v1", generated_at: new Date().toISOString() };
    for (const section of sections) {
      const groups = db.query(`SELECT ${section.fields},count(*) count FROM ${section.table}
        GROUP BY ${section.fields} ORDER BY ${section.fields}`).all();
      result[section.name] = groups.map(row => [...section.fields.split(",").map(field => row[field]), row.count]);
    }
    return result;
  } finally { db.close(); }
}


/** Exact, read-only law-plus-exception proof for the current mutable target
 * owner. Nothing is dropped, installed, or rewritten. All five old columns
 * must reconstruct from the existing source_binding semantic dimension.
 * This is a prerequisite for a later storage migration, not that migration.
 */
export function inspectTargetLaws(estate) {
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw new Error("missing manifold: " + path);
  const db = new Database(path, { readonly: true, create: false });
  try {
    db.exec("BEGIN");
    try {
      const semanticKinds = db.query("SELECT DISTINCT semantic_kind FROM representation_target ORDER BY semantic_kind COLLATE BINARY").all().map(r => r.semantic_kind);
      const ownedKinds = new Set(db.query("SELECT DISTINCT semantic_kind FROM source_binding").all().map(r => r.semantic_kind));
      const profiles = db.query(
        "SELECT representation_kind,status,substrate_path,reason,COUNT(*) members FROM representation_target "
        + "GROUP BY representation_kind,status,substrate_path,reason "
        + "ORDER BY representation_kind COLLATE BINARY,members DESC,status COLLATE BINARY,substrate_path COLLATE BINARY,reason COLLATE BINARY"
      ).all();
      const laws = new Map();
      for (const row of profiles) if (!laws.has(row.representation_kind)) laws.set(row.representation_kind, row);
      const representationKinds = [...laws.keys()].sort((a, b) => {
        // SQLite's default COLLATE BINARY sorts UTF-8 bytes; the schema uses
        // ASCII representation identifiers. Never silently normalize names.
        return Buffer.compare(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"));
      });
      const source = createHash("sha256");
      const exceptions = new Map();
      let originalRows = 0;
      for (const row of db.query(
        "SELECT semantic_kind,representation_kind,status,substrate_path,reason FROM representation_target "
        + "ORDER BY semantic_kind COLLATE BINARY,representation_kind COLLATE BINARY"
      ).iterate()) {
        source.update(JSON.stringify([row.semantic_kind,row.representation_kind,row.status,row.substrate_path,row.reason]) + "\n");
        const base = laws.get(row.representation_kind);
        if (!base) throw new Error("missing representation law");
        if (row.status !== base.status || row.substrate_path !== base.substrate_path || row.reason !== base.reason)
          exceptions.set(JSON.stringify([row.semantic_kind,row.representation_kind]), row);
        originalRows++;
      }
      const expected = semanticKinds.length * representationKinds.length;
      const owned = semanticKinds.every(kind => ownedKinds.has(kind));
      const status = {
        schema: "bonfyre.representation-target-law-proof.v1",
        state: "HELD_SPARSE_OR_UNOWNED",
        source_rows: originalRows, semantic_kinds: semanticKinds.length,
        representation_kinds: representationKinds.length, law_rows: laws.size,
        exception_rows: exceptions.size, expected_cartesian_rows: expected,
        semantic_dimension_owner_complete: owned,
        original_digest_sha256: source.digest("hex"),
        reconstructed_digest_sha256: null,
        no_effect: true, storage_replaced: false,
        cannot_infer: [
          "a smaller logical rule population proves physical SQLite bytes saved",
          "read parity grants permission to drop the mutable representation_target table",
          "historical equality proves concurrent writers preserve all future exceptions"
        ]
      };
      if (!originalRows || !representationKinds.length || !owned || originalRows !== expected)
        return status;
      const reconstruction = createHash("sha256");
      for (const semanticKind of semanticKinds) for (const representationKind of representationKinds) {
        const row = exceptions.get(JSON.stringify([semanticKind,representationKind])) || laws.get(representationKind);
        reconstruction.update(JSON.stringify([semanticKind,representationKind,row.status,row.substrate_path,row.reason]) + "\n");
      }
      status.reconstructed_digest_sha256 = reconstruction.digest("hex");
      if (status.original_digest_sha256 !== status.reconstructed_digest_sha256)
        throw new Error("lossless law projection differs from representation_target source");
      return {...status, state: "EXACT_READ_PROVEN",
        factorable_records: originalRows - laws.size - exceptions.size,
        semantic_kinds_reused_from_source_binding: semanticKinds.length };
    } finally { db.exec("ROLLBACK"); }
  } finally { db.close(); }
}

export function formatFabricReport(result) {
  const lines = ["BONFŸRE SHIFT 1.6 — PLAN / TRANSPORT / CARRIER FABRIC", "=".repeat(86), ""];
  for (const section of sections) {
    lines.push(section.title);
    for (const row of result[section.name]) {
      const count = row.at(-1).toLocaleString("en-US");
      lines.push(section.name === "targets"
        ? `  ${String(row[0]).padEnd(section.width)} ${count}`
        : `  ${String(row[0]).padEnd(section.width)} ${String(row[1]).padEnd(22)} ${count}`);
    }
    lines.push("");
  }
  return lines.join("\n");
}

function atomicWrite(path, body) {
  if (existsSync(path) && (lstatSync(path).isSymbolicLink() || !lstatSync(path).isFile()))
    throw new Error(`unsafe existing report: ${path}`);
  const temporary = `${path}.partial-${process.pid}`;
  try { writeFileSync(temporary, body); renameSync(temporary, path); }
  finally { if (existsSync(temporary)) rmSync(temporary); }
}

export function writeFabricReport(estate) {
  const result = inspectFabric(estate), root = join(resolve(estate), "representations");
  if (existsSync(root) && lstatSync(root).isSymbolicLink())
    throw new Error(`unsafe representation root: ${root}`);
  mkdirSync(root, { recursive: true });
  const json = join(root, "fabric-report.json"), text = join(root, "fabric-report.txt");
  atomicWrite(json, JSON.stringify(result, null, 2));
  atomicWrite(text, formatFabricReport(result));
  return { state: "DERIVED_REPORT", json, text, ...result };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  try {
    const [command, ...args] = process.argv.slice(2);
    if (!["inspect", "write", "laws"].includes(command) || args.length > 2 ||
        (args.length && (args[0] !== "--estate" || !args[1])))
      throw new Error("usage: report inspect|write|laws [--estate PATH]");
    const estate = resolve(args[1] ?? join(homedir(), ".bonfyre/estate-local"));
    console.log(JSON.stringify(command === "inspect" ? inspectFabric(estate)
      : command === "laws" ? inspectTargetLaws(estate) : writeFabricReport(estate)));
  } catch (error) { console.error(`representation-report: ${error.message}`); process.exitCode = 1; }
}
