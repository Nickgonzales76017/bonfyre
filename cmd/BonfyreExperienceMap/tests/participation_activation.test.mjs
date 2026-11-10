import { test, expect } from "bun:test";
import { mkdtempSync, writeFileSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  law, leverageModes, nextActivation, protocolDerivationWitness,
  disturbanceAcceptance, build, main,
} from "../tools/participation_activation.mjs";

const unknown = () => ({ status: "unknown", depth: 0, depth_name: "unknown" });
const profile = observation => ({
  id: "external:fixture", name: "Fixture", source_kind: "public_api", url: "https://example.test",
  investigation_priority: 0,
  candidate: {
    candidate_planes: Object.fromEntries(["discovery", "observation", "semantic", "protocol", "capability", "economic", "round_trip"].map(key => [key, true])),
    role_classes: ["service", "action"], protocol_carriers: [], join_power: 3, chain_span: 4, candidate_plane_count: 7,
  },
  realized: {
    braid_depth: 0,
    axes: { discovery: { status: "measured", depth: 1 }, observation,
      semantic: unknown(), protocol: unknown(), capability: unknown(), economic: unknown(), round_trip: unknown() },
  },
});

test("CUE-routed modes and next owner preserve the observation/semantic boundary", () => {
  expect(leverageModes(profile(unknown()))).toEqual(["cross_world_service", "effect_adapter"]);
  const expected = [
    [unknown(), "observe_exact_candidate"],
    [{ status: "measured", depth: 1 }, "resolve_observation_boundary"],
    [{ status: "measured", depth: 2 }, "resolve_observation_boundary"],
    [{ status: "measured", depth: 3 }, "retain_exact_observation"],
    [{ status: "measured", depth: 4 }, "bind_semantic_profile"],
    [{ status: "measured", depth: 5 }, "bind_semantic_profile"],
  ];
  for (const [observation, action] of expected) expect(nextActivation(profile(observation)).action).toBe(action);
  expect(law.cannot_infer).toContain("activation rank -> authority to observe or activate");
});

test("native protocol witness requires positive scoped atomic registration", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-activation-witness-"));
  const receipt = join(dir, "receipt.json");
  const bridge = join(dir, "bridge.c");
  writeFileSync(bridge, "/* witness target */\n");
  writeFileSync(receipt, JSON.stringify({ schema: "bonfyre-protocol-derivation-bridge.v1",
    registered: true, contract: "fixture", enactments: 2, programs: 8,
    scoped: true, targeted_invalidation: true, atomic: true }));
  expect(protocolDerivationWitness(receipt, bridge)).toBe(true);
  writeFileSync(receipt, JSON.stringify({ schema: "bonfyre-protocol-derivation-bridge.v1", registered: true }));
  expect(protocolDerivationWitness(receipt, bridge)).toBe(false);
});

const event = (number, owner, kind, parent = null, verdict = null) => ({
  schema: "bonfyre.disturbance-event.v1", disturbance_id: "storm:fixture", event_id: `e${String(number).padStart(2, "0")}`,
  owner, kind, subject: "festival:fixture", causal_parents: parent === null ? [] : [`e${String(parent).padStart(2, "0")}`],
  evidence_ref: `receipt:fixture:${number}`, ...(verdict ? { verdict } : {}),
});

test("causal owner braid is accepted; scripted step counts are refused", () => {
  const emergent = [
    event(1, "ForeignTwinGraph", "disturbance_observed"),
    event(2, "DomainProfile", "interpreted", 1),
    event(3, "SemanticDerivationGraph", "assumption_retracted", 2),
    event(4, "EvidenceGraph", "evidence_changed", 3),
    event(5, "BlockerGraph", "blocker_opened", 4),
    event(6, "UnlockGraph", "join_admitted", 5),
    event(7, "ActorGraph", "human_decision", 6),
    event(8, "WorkGraph", "work_opened", 7),
    event(9, "CommitmentGraph", "commitment_opened", 8),
    event(10, "MoneyGraph", "quoted", 9),
    event(11, "PublicationProgram", "publication_projected", 10),
    event(12, "RoundTripTwin", "roundtrip_checked", 11, "drifted"),
    event(13, "WorkGraph", "correction_opened", 12),
    event(14, "CommitmentGraph", "commitment_compensated", 13),
    event(15, "MoneyGraph", "settled", 14),
    event(16, "BlockerGraph", "blocker_resolved", 15),
  ];
  const accepted = disturbanceAcceptance(emergent);
  expect(accepted.accepted).toBe(true);
  expect(accepted.metrics.max_owner_braid_depth).toBeGreaterThanOrEqual(8);
  expect(accepted.metrics.round_trip).toEqual({ drifted: 1 });
  const scripted = Array.from({ length: 47 }, (_, index) => ({
    schema: "bonfyre.disturbance-event.v1", disturbance_id: "storm:scripted",
    event_id: `step-${index + 1}`, owner: "WorkGraph", kind: "scripted_step", subject: "festival:fixture",
    causal_parents: [], step: index + 1,
  }));
  const rejected = disturbanceAcceptance(scripted);
  expect(rejected.accepted).toBe(false);
  expect(rejected.failures.some(failure => failure.includes("owner evidence"))).toBe(true);
  expect(rejected.failures.some(failure => failure.includes("one root disturbance"))).toBe(true);
});

test("native projection writes its exact current manifest and frontier", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-activation-"));
  writeFileSync(join(dir, "participation_profiles.jsonl"), `${JSON.stringify(profile(unknown()))}\n`);
  const projected = build({ generated: dir, limit: 1 });
  expect(projected.manifest.total_occurrences).toBe(1);
  expect(projected.selected[0].next_activation.owner).toBe("ForeignTwin");
  expect(projected.disturbance.owner_bridge_evidence["ForeignTwinGraph->DomainProfile"].receipt_ingress_observed).toBe(false);
  expect(projected.disturbance.owner_bridge_evidence["ProtocolContract->SemanticDerivationGraph"].native_graph_registration).toBe(false);
  expect(projected.disturbance.owner_bridge_readiness).toBeUndefined();
  const stdout = process.stdout.write;
  process.stdout.write = () => true;
  try { main(["--generated", dir, "--limit", "1", "--write"]); }
  finally { process.stdout.write = stdout; }
  const saved = JSON.parse(readFileSync(join(dir, "participation_activation_manifest.json"), "utf8"));
  expect(saved).toEqual(projected.manifest);
  expect(readFileSync(join(dir, "participation_activation_frontier.jsonl"), "utf8")).toContain("external:fixture");
});
