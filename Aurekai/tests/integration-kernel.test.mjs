import test from "node:test";
import assert from "node:assert/strict";
import { dirname, join } from "node:path";
import { existsSync, mkdtempSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import {
  runKernelIntegration,
  listKernelTargets,
} from "@aurekai/continuity-core";
import { schemas, validate } from "./schema-check.mjs";
import { scaffoldIntegrationRepos } from "../src/integration-repo-scaffold.mjs";

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const SURFACES = ["hard-fork", "soft-fork", "adapter", "observer", "state-machine"];

const event = {
  event_id: "kernel-test-001",
  action: "commit",
  source: "aurekai-tests",
  risk_score: 0.42,
  deformation_score: 0.31,
  witness_skew: 0.17,
  continuity_gap: 0.23,
};

function allTargets() {
  return SURFACES.flatMap((surface) => listKernelTargets(repoRoot, { surface }));
}

function run(target, overrides = {}) {
  return runKernelIntegration(repoRoot, { target, event: { ...event, ...overrides } }).envelope;
}

test("committed state is content-addressed, not run-addressed", () => {
  for (const target of ["IBC/Cosmos", "Uniswap", "Chainlink/Pyth oracle rounds"]) {
    const first = run(target);
    const second = run(target);
    assert.equal(
      first.committed_state.state_hash,
      second.committed_state.state_hash,
      `${target}: identical state must commit to one hash across runs`,
    );
    assert.notEqual(first.run_id, second.run_id, `${target}: each run keeps its own id`);
    assert.notEqual(
      first.committed_state.run_hash,
      second.committed_state.run_hash,
      `${target}: run_hash carries the run context that state_hash must not`,
    );
    assert.equal(first.committed_state.commitment_basis, "semantic");
  }
});

test("a changed host event changes the committed state hash", () => {
  const base = run("IBC/Cosmos");
  const moved = run("IBC/Cosmos", { risk_score: 0.99 });
  assert.notEqual(base.committed_state.state_hash, moved.committed_state.state_hash);
});

test("state hashes separate every distinct target", () => {
  const names = [...new Set(allTargets().map((entry) => entry.target))];
  const hashes = names.map((name) => run(name).committed_state.state_hash);
  assert.equal(new Set(hashes).size, names.length, "two targets must not share a commitment");
});

test("listing is indexed by surface, resolution is indexed by name", () => {
  // A target may be listed under more than one surface, but integrate:run
  // resolves a single descriptor by name. Exercising a target *as* a state
  // machine goes through runKernelStateMachineBatch, not through the name.
  const listed = allTargets();
  const names = new Set(listed.map((entry) => entry.target));
  assert.ok(listed.length > names.size, "expected multi-surface listings in the registry");
  const shared = listed.filter((entry) => entry.target === "Aave").map((entry) => entry.surface);
  assert.ok(shared.length > 1, "Aave is listed under more than one surface");
  assert.equal(new Set([run("Aave").surface]).size, 1, "name resolution yields one surface");
});

test("hashing does not depend on host key order", () => {
  const forward = runKernelIntegration(repoRoot, {
    target: "IBC/Cosmos",
    event: { alpha: 1, beta: 2, ...event },
  }).envelope;
  const reversed = runKernelIntegration(repoRoot, {
    target: "IBC/Cosmos",
    event: { ...event, beta: 2, alpha: 1 },
  }).envelope;
  assert.equal(forward.committed_state.state_hash, reversed.committed_state.state_hash);
});

const EMITTED = [
  ["aurekai.integration_execution.v1.json", (envelope) => [envelope]],
  ["aurekai.committed_state.v1.json", (envelope) => [envelope.committed_state]],
  ["aurekai.transition.v1.json", (envelope) => envelope.transitions ?? []],
  ["aurekai.lineage_edge.v1.json", (envelope) => envelope.lineage_edges ?? []],
  ["aurekai.export_projection.v1.json", (envelope) => envelope.export_projections ?? []],
  ["aurekai.functional_claim.v1.json", (envelope) => envelope.functional_claims ?? []],
  ["aurekai.trajectory.v1.json", (envelope) => [envelope.trajectory]],
  ["aurekai.policy_decision.v1.json", (envelope) => envelope.policies ?? []],
  ["aurekai.fulfillment_transition.v1.json", (e) => [e.commerce_continuity?.fulfillment_transition]],
  ["aurekai.delivery_witness.v1.json", (e) => [e.commerce_continuity?.delivery_witness]],
  ["aurekai.sla.continuity.v1.json", (e) => [e.commerce_continuity?.sla_continuity]],
  ["aurekai.invoice.proof.v1.json", (e) => [e.commerce_continuity?.invoice_proof]],
  ["aurekai.chargeability_claim.v1.json", (e) => [e.commerce_continuity?.chargeability_claim]],
  ["aurekai.treasury_continuity.v1.json", (e) => [e.commerce_continuity?.treasury_continuity]],
];

test("every emitted object satisfies its published schema", () => {
  const failures = [];
  let checked = 0;
  for (const entry of allTargets()) {
    const envelope = run(entry.target);
    for (const [file, extract] of EMITTED) {
      for (const object of extract(envelope)) {
        if (object === undefined || object === null) continue;
        checked += 1;
        const errors = validate(object, schemas.get(file));
        if (errors.length) failures.push(`${entry.target} / ${file}: ${errors.slice(0, 3).join("; ")}`);
      }
    }
  }
  assert.ok(checked > 0, "no objects were checked");
  assert.deepEqual(failures.slice(0, 5), [], `${failures.length} of ${checked} objects broke their schema`);
});

test("doctrine refuses a protocol-critical surface with no mutation boundary", () => {
  const unbounded = allTargets().find(
    (entry) => entry.trust_level === "protocol-critical" && entry.target === "Blast",
  );
  assert.ok(unbounded, "expected an unbounded protocol-critical target in the registry");
  assert.throws(
    () => runKernelIntegration(repoRoot, { target: unbounded.target, event, enforceDoctrine: true }),
    /protocol_boundary_required/,
  );
});

test("doctrine admits a protocol-critical surface that has a boundary", () => {
  const envelope = runKernelIntegration(repoRoot, {
    target: "IBC/Cosmos",
    event,
    enforceDoctrine: true,
  }).envelope;
  assert.ok(envelope.mutation_boundary, "a bounded target keeps its mutation boundary");
});

test("the residual calibrator separates all four taxonomy bands", () => {
  const bands = new Set();
  for (let step = 0; step <= 10; step += 1) {
    const x = step / 10;
    const envelope = run("IBC/Cosmos", {
      risk_score: x,
      deformation_score: x,
      witness_skew: x,
      continuity_gap: x,
    });
    bands.add(envelope.committed_state.residual_assessment.taxonomy);
  }
  assert.deepEqual(
    [...bands].sort(),
    ["drifting", "rupture", "stable", "strained"],
    "a calibrator that cannot separate bands is decorative",
  );
});

test("every declared surface is reachable, including adapter", () => {
  const counts = Object.fromEntries(
    SURFACES.map((surface) => [surface, listKernelTargets(repoRoot, { surface }).length]),
  );
  for (const [surface, count] of Object.entries(counts)) {
    assert.ok(count > 0, `surface '${surface}' lists no targets`);
  }
});

test("a name declared on two surfaces resolves to each of them", () => {
  // LangGraph is both a soft-fork runtime shim and a thin-kernel projector.
  // Before the index was surface-addressable the adapter reading was unreachable.
  const asAdapter = runKernelIntegration(repoRoot, {
    target: "LangGraph", surface: "adapter", event,
  }).envelope;
  const asSoftFork = runKernelIntegration(repoRoot, {
    target: "LangGraph", surface: "soft-fork", event,
  }).envelope;

  assert.equal(asAdapter.surface, "adapter");
  assert.equal(asSoftFork.surface, "soft-fork");
  assert.notDeepEqual(asAdapter.required_kernel_layers, asSoftFork.required_kernel_layers);
  assert.notEqual(
    asAdapter.committed_state.state_hash,
    asSoftFork.committed_state.state_hash,
    "two surfaces are two states and must not share a commitment",
  );
});

test("virtual crate plan covers the existing integration forest without creating directories", () => {
  const dir = mkdtempSync(join(tmpdir(), "aurekai-virtual-crate-"));
  try {
    const outDir = join(dir, "not-materialized");
    const first = scaffoldIntegrationRepos(repoRoot, { outDir, mode: "virtual" });
    const again = scaffoldIntegrationRepos(repoRoot, { outDir, mode: "virtual" });
    assert.ok(first.count >= 100, "retain existing registry coverage");
    assert.equal(first.mode, "virtual");
    assert.equal(first.recipe_sha256, again.recipe_sha256);
    assert.equal(first.repos.length, first.count);
    assert.ok(first.repos.every(entry => entry.artifacts.length === 6 &&
      entry.artifacts.every(a => /^[0-9a-f]{64}$/.test(a.sha256))));
    assert.equal(existsSync(outDir), false, "virtual closure must not emit the six-file forests");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("demanded crate materializes exact bytes; verify refuses changes without rewriting extracted source", () => {
  const dir = mkdtempSync(join(tmpdir(), "aurekai-demand-crate-"));
  try {
    const outDir = join(dir, "integrations");
    const plan = scaffoldIntegrationRepos(repoRoot, { outDir, target: "Aave", mode: "virtual" });
    assert.equal(existsSync(outDir), false);
    const built = scaffoldIntegrationRepos(repoRoot, { outDir, target: "Aave" });
    assert.equal(built.count, 1);
    const verified = scaffoldIntegrationRepos(repoRoot, { outDir, target: "Aave", mode: "verify" });
    assert.equal(verified.recipe_sha256, plan.recipe_sha256);
    assert.equal(verified.repos[0].artifacts.length, 6);
    const readme = join(outDir, verified.repos[0].slug, "README.md");
    const before = readFileSync(readme);
    writeFileSync(readme, Buffer.concat([before, Buffer.from("\n# Edited independently\n")]));
    assert.throws(
      () => scaffoldIntegrationRepos(repoRoot, { outDir, target: "Aave", mode: "verify" }),
      /scaffold differs or is missing/
    );
    assert.ok(readFileSync(readme).includes(Buffer.from("Edited independently")),
      "read-only verification must not overwrite edits");
    scaffoldIntegrationRepos(repoRoot, { outDir, target: "Aave" });
    assert.equal(
      scaffoldIntegrationRepos(repoRoot, { outDir, target: "Aave", mode: "verify" }).recipe_sha256,
      plan.recipe_sha256
    );
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("an unknown target names the surface it was asked for", () => {
  assert.throws(
    () => runKernelIntegration(repoRoot, { target: "NotAProtocol", surface: "adapter", event }),
    /on surface adapter/,
  );
});
