import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { listIntegrationTargets } from "../src/integration-engine.mjs";
import { ingestIntegrationBatch, ingestIntegrationTarget } from "../src/integration-connectors.mjs";

const root = resolve(fileURLToPath(new URL("../", import.meta.url)));
const event = { event_id: "synthetic-ingest", run_id: "synthetic-ingest-cut", action: "observe",
  source: "public-parity-test", risk_score: 0.13, deformation_score: 0.12, witness_skew: 0.11 };

test("connector fan-out shares exact owner inputs without changing individual evidence or parser results", async () => {
  const listed = listIntegrationTargets(root);
  const tally = new Map();
  for (const row of listed) tally.set(row.surface, (tally.get(row.surface) ?? 0) + 1);
  const surface = [...tally].sort((a, b) => b[1] - a[1])[0]?.[0];
  const targets = listIntegrationTargets(root, { surface });
  assert.ok(targets.length >= 4, "need a real multi-target surface");

  const OriginalDate = globalThis.Date;
  globalThis.Date = class extends OriginalDate {
    constructor(...args) { super(...(args.length ? args : ["2026-10-13T00:00:00.000Z"])); }
    static now() { return OriginalDate.parse("2026-10-13T00:00:00.000Z"); }
  };
  const oldRead = fs.readFileSync;
  let reads = 0;
  fs.readFileSync = function (path, ...args) {
    if (String(path).startsWith(join(root, "registry") + "/")) reads++;
    return oldRead.call(this, path, ...args);
  };
  syncBuiltinESMExports();
  try {
    reads = 0;
    const batch = await ingestIntegrationBatch(root, { surface, event, persist: false });
    const batchReads = reads;
    reads = 0;
    const individual = [];
    for (const item of targets) {
      const result = await ingestIntegrationTarget(root, {
        target: item.target, event, persist: false,
      });
      individual.push({
        target: result.target,
        surface: result.surface,
        category: result.category,
        run_id: result.execution.run_id,
        committed_state_hash: result.execution.committed_state.state_hash,
        policy_decision: result.execution.policies[0].decision,
        persisted_to: result.persisted_to,
        source: result.source,
        endpoint_template: result.endpoint_template,
        validation: result.validation,
        preset: result.preset,
      });
    }
    const individualReads = reads;
    assert.equal(batch.count, targets.length);
    assert.deepEqual(batch.executions, individual);
    assert.ok(batchReads <= 11, `expected one ingestion cut, saw ${batchReads} registry reads`);
    assert.ok(individualReads >= targets.length * 9,
      `baseline no longer repeats registry inputs: ${individualReads}`);
    assert.ok(individualReads > batchReads * 4);
    console.log(JSON.stringify({ surface, targets: targets.length,
      batch_registry_reads: batchReads, independent_registry_reads: individualReads }));
  } finally {
    fs.readFileSync = oldRead;
    syncBuiltinESMExports();
    globalThis.Date = OriginalDate;
  }
});
