#!/usr/bin/env bun
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const law = JSON.parse(readFileSync(join(here, "../../BonfyreExperienceMap/tools/participation-law.generated.json"), "utf8"));
const [mode, path] = process.argv.slice(2);
if (!mode || !path) throw new Error("usage: participation_contract.mjs --plane FILE | --reject REPORT");
if (mode === "--plane") {
  const rows = readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean).map(JSON.parse);
  const expectedAxes = ["discovery", "capability", "round_trip"];
  assert.ok(rows.length > 0, "no participation emitted");
  const seen = new Set();
  for (const row of rows) {
    assert.equal(row.schema, "bonfyre-owner-participation.v1");
    assert.ok(row.occurrence_id.startsWith("external:"));
    assert.ok(law.axis_ladders[row.axis]?.[row.state], "unrecognized axis/rung");
    assert.match(row.basis_sha256, /^[0-9a-f]{64}$/);
    assert.match(row.evidence_sha256, /^[0-9a-f]{64}$/);
    assert.notEqual(row.from_owner, row.to_owner);
    assert.ok(row.produced_by);
    assert.ok(["routable", "qualified", "projected"].includes(row.state), "plane claimed a deeper rung");
    assert.notEqual(row.axis, "economic");
    seen.add(row.axis);
  }
  assert.deepEqual([...seen].sort(), expectedAxes.sort());
  console.log(`participation rows: ${rows.length}`);
} else if (mode === "--reject") {
  const report = JSON.parse(readFileSync(path, "utf8"));
  assert.equal((report.measured_axis_counts.economic ?? 0) + (report.measured_axis_counts.capability ?? 0), 0,
    "unverifiable owner row was counted as measured");
} else throw new Error(`unknown mode ${mode}`);
