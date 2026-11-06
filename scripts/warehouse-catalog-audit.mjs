#!/usr/bin/env bun
// Read-only warehouse metadata witness, not a migration or authority grant.
import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function inspectWarehouseCatalog(estate, limit = 8) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw Error("limit must be 1..100");
  const path = join(resolve(estate), "representation-manifold.db");
  if (!existsSync(path)) throw Error("missing manifold: " + path);
  const db = new Database(path, { readonly: true, create: false });
  try {
    db.exec("BEGIN");
    try {
      const tables = new Set(db.query("SELECT name FROM sqlite_master WHERE type='table'").all().map(r => r.name));
      for (const table of ["representation_instance","physical_realization","eviction_receipt","representation_target"])
        if (!tables.has(table)) throw Error("missing catalog table: " + table);
      const instances = db.query("SELECT COUNT(*) total, COALESCE(SUM(state='physical'),0) physical, "
        + "COALESCE(SUM(state='reconstructable'),0) reconstructable FROM representation_instance").get();
      const realizations = db.query("SELECT COUNT(*) total, COALESCE(SUM(available=1),0) available, "
        + "COALESCE(SUM(available=0),0) unavailable FROM physical_realization").get();
      const evictions = db.query("SELECT COUNT(*) count FROM eviction_receipt").get().count;
      const targets = db.query("SELECT COUNT(*) count FROM representation_target").get().count;
      const cohorts = db.query("SELECT COALESCE(schema_ref,'') semantic_kind,kind,COUNT(*) generations "
        + "FROM representation_instance GROUP BY schema_ref,kind "
        + "ORDER BY generations DESC,semantic_kind,kind LIMIT ?").all(limit);
      const query = "EXPLAIN QUERY PLAN SELECT id FROM representation_instance "
        + "WHERE schema_ref=? AND kind=? ORDER BY created_at DESC,id DESC LIMIT 1";
      const plan = db.query(query).all("_witness_", "parquet").map(r=>r.detail).join(" | ");
      const indexed = /USING (?:COVERING )?INDEX/i.test(plan);
      const indexes = db.query("PRAGMA index_list('representation_instance')").all().map(r=>r.name).sort();
      const freelist = db.query("PRAGMA freelist_count").get().freelist_count;
      const pageSize = db.query("PRAGMA page_size").get().page_size;
      return {
        schema:"bonfyre.warehouse-catalog-witness.v1",
        state: indexed ? "INDEXED_LOOKUP" : "UNINDEXED_LOOKUP",
        readonly:true, instances, realizations, evictions, targets, cohorts,
        latest_lookup:{indexed,plan,indexes},
        freelist_bytes:freelist * pageSize,
        cannot_infer:["unavailable data are safe to delete",
          "freelist pages are object-store bytes",
          "an indexed plan proves distributed execution or concurrency",
          "historical row counts alone prove a metadata leak"]
      };
    } finally { db.exec("ROLLBACK"); }
  } finally { db.close(); }
}

export function formatWarehouseCatalog(a) {
  const n = x => Number(x).toLocaleString("en-US");
  return [
    "BONFŸRE WAREHOUSE CATALOG WITNESS (READ ONLY)",
    "Lookup: " + a.state,
    "Representations: " + n(a.instances.total) + " | physical: " + n(a.instances.physical)
      + " | reconstructable: " + n(a.instances.reconstructable),
    "Realizations: " + n(a.realizations.total) + " | available: " + n(a.realizations.available),
    "Eviction receipts: " + n(a.evictions) + " | targets: " + n(a.targets),
    "Newest-generation query plan: " + a.latest_lookup.plan,
    "SQLite freelist bytes (not deletion authority): " + n(a.freelist_bytes),
    ...a.cohorts.map(g => "  " + g.semantic_kind + "/" + g.kind + ": " + n(g.generations)),
    "No schema, physical data, receipts or authority were changed."
  ].join("\n");
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const args = process.argv.slice(2);
    if (args.length && (args.length !== 2 || args[0] !== "--estate"))
      throw Error("usage: warehouse-catalog-audit [--estate PATH]");
    console.log(formatWarehouseCatalog(inspectWarehouseCatalog(
      args[1] ?? join(homedir(), ".bonfyre/estate-local"))));
  } catch (error) { console.error("warehouse-catalog-audit: " + error.message); process.exitCode = 1; }
}
