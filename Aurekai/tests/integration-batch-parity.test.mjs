import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  executeIntegration, executeSurfaceBatch, listIntegrationTargets,
} from "../src/integration-engine.mjs";

const root = resolve(fileURLToPath(new URL("../", import.meta.url)));
const event = { run_id: "synthetic-batch-parity", event_id: "synthetic", risk_score: 0.13, deformation_score: 0.12 };

test("a shared execution cut preserves exact per-target outputs while avoiding repeated registry reads", () => {
  const all = listIntegrationTargets(root);
  const frequencies = new Map();
  for (const row of all) frequencies.set(row.surface, (frequencies.get(row.surface) ?? 0) + 1);
  const surface = [...frequencies].sort((a, b) => b[1] - a[1])[0]?.[0];
  assert.ok(surface && frequencies.get(surface) >= 4, "need a real multi-target surface");
  const members = listIntegrationTargets(root, { surface });

  // Deterministic clocks make the *whole response* comparable rather than
  // checking only decisions while allowing a changed trace to slip through.
  const OriginalDate = globalThis.Date;
  globalThis.Date = class extends OriginalDate {
    constructor(...args) { super(...(args.length ? args : ["2026-10-13T00:00:00.000Z"])); }
    static now() { return OriginalDate.parse("2026-10-13T00:00:00.000Z"); }
  };
  const originalRead = fs.readFileSync;
  let reads = 0;
  fs.readFileSync = function (path, ...args) {
    if (String(path).startsWith(join(root, "registry") + "/")) reads++;
    return originalRead.call(this, path, ...args);
  };
  syncBuiltinESMExports();
  try {
    reads = 0;
    const batch = executeSurfaceBatch(root, { surface, event });
    const batchReads = reads;
    reads = 0;
    const independently = members.map(item => {
      const result = executeIntegration(root, { target: item.target, event });
      return {
        target: item.target,
        surface: item.surface,
        category: item.category,
        run_id: result.envelope.run_id,
        committed_state_hash: result.envelope.committed_state.state_hash,
        policy_decision: result.envelope.policies[0].decision,
        persisted_to: result.persistedTo,
      };
    });
    const separateReads = reads;
    assert.equal(batch.count, members.length);
    assert.deepEqual(batch.executions, independently);
    assert.ok(batchReads <= 8, `unexpected batch registry loads: ${batchReads}`);
    assert.ok(separateReads >= members.length * 5, `independent loads too low: ${separateReads}`);
    assert.ok(separateReads > batchReads * 2, "batch did not avoid repeated registry loads");
    console.log(JSON.stringify({ surface, targets: members.length, batch_registry_reads: batchReads,
      independent_registry_reads: separateReads, materialized_policy_copies: 0 }));
  } finally {
    fs.readFileSync = originalRead;
    syncBuiltinESMExports();
    globalThis.Date = OriginalDate;
  }
});

test("a new batch observes revised policy rather than reusing stale cached closure", () => {
  const tmp = mkdtempSync(join(tmpdir(), "aurekai-batch-policy-"));
  try {
    cpSync(join(root, "registry"), join(tmp, "registry"), { recursive: true });
    const policyPath = join(tmp, "registry", "continuity-policy-families.json");
    const policy = JSON.parse(readFileSync(policyPath, "utf8"));
    const familyIDs = new Set(policy.families.map(f => f.id));
    const candidate = listIntegrationTargets(tmp).find(row => familyIDs.has(row.policy_family));
    assert.ok(candidate, "missing known policy-family target");
    const before = executeSurfaceBatch(tmp, { surface: candidate.surface, event }).executions
      .find(row => row.target === candidate.target);
    assert.ok(before);
    const family = policy.families.find(row => row.id === candidate.policy_family);
    family.residual_thresholds = { gate: -1, halt: -1 };
    writeFileSync(policyPath, JSON.stringify(policy));
    const after = executeSurfaceBatch(tmp, { surface: candidate.surface, event }).executions
      .find(row => row.target === candidate.target);
    assert.equal(after.policy_decision, "halt");
    // This must only be a no-cache assertion if the original decision was different.
    assert.notEqual(before.policy_decision, after.policy_decision);
  } finally { rmSync(tmp, { recursive: true, force: true }); }
});
