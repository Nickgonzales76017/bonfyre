#!/usr/bin/env bun
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const law = JSON.parse(readFileSync(join(here, "../../BonfyreExperienceMap/tools/participation-law.generated.json"), "utf8"));
const [reportPath, densityPath] = process.argv.slice(2);
if (!reportPath || !densityPath) throw new Error("usage: estate-refresh-summary.mjs REPORT DENSITY");
const report = JSON.parse(readFileSync(reportPath, "utf8"));
const density = JSON.parse(readFileSync(densityPath, "utf8"));
const axes = report.measured_axis_counts;
const total = report.total_occurrences;
console.log(`estate: ${total} occurrences, ${report.owner_evidence_rows} owner evidence rows`);
console.log(`  braid depth      ${JSON.stringify(report.braid_depth_distribution)}`);
console.log(`  owners           ${density.distinct_owners} distinct, ${density.owner_transition_count} transitions, longest path ${density.longest_owner_path}`);
console.log(`  branch / rejoin  ${JSON.stringify(density.branch_points)} / ${JSON.stringify(density.rejoin_points)} (braid_excess ${density.braid_excess})`);
for (const axis of law.axes) console.log(`  ${axis.padEnd(12)} ${String(axes[axis] ?? 0).padStart(6)} / ${total}`);
console.log("  refresh fetched nothing; observation rows are receipts a sweep already earned");
console.log("  every other axis is at the lowest rung its owner can defend locally");
