#!/usr/bin/env bun
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const [densityPath, reportPath] = process.argv.slice(2);
if (!densityPath || !reportPath) throw new Error("usage: reader_agreement.mjs DENSITY REPORT");
const density = JSON.parse(readFileSync(densityPath, "utf8"));
const report = JSON.parse(readFileSync(reportPath, "utf8"));
assert.equal(density.owner_transition_count, 2);
assert.equal(density.longest_owner_path, 2);
assert.equal(density.distinct_owners, 3);
assert.equal(report.measured_axis_counts.observation, 1);
assert.equal(report.measured_axis_counts.semantic, 1);
assert.equal(report.braid_depth_distribution["2"], 1);
console.log("reader agreement: PASS (observation edge seen by both readers)");
