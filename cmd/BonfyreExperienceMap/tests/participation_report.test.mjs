import { test, expect } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildProfile, evidenceIndex, normalizeFact } from "../tools/participation_report.mjs";

const row = (id, name, { human, protocols, roles, support = [], direct = [] }) => ({
  id, name, source_kind: "fixture", url: "https://example.test/", human_chains: human,
  protocol_chains: protocols, reagent_roles: roles, support_chains: support, direct_domains: direct,
});
const crossworld = row("external:x402", "Cross-world paid data mesh", {
  human: ["discover", "emergency", "publication", "logistics", "research"],
  protocols: ["mcp", "x402"],
  roles: ["material_source", "action_adapter", "publication_carrier", "external_service_candidate"],
  support: ["commerce", "finance"], direct: ["weather", "medicine", "finance", "media"],
});
const joke = row("external:joke", "JokeAPI", {
  human: ["discover"], protocols: ["http-api"], roles: ["external_service_candidate"], support: ["software-work"],
});
const projected = (candidate, receipts) => {
  const [byOccurrence, transitions] = evidenceIndex(receipts);
  return buildProfile(candidate, byOccurrence.get(candidate.id) ?? [], transitions.get(candidate.id) ?? new Set());
};
const foreignReceipt = {
  schema: "bonfyre-foreign-observation.v1", twin_id: "external:x402", source_url: "https://example.test/",
  observation_kind: "source_observed", content_sha256: "a".repeat(64),
  _source_path: "/receipts/external-x402.json",
};

test("candidate join power remains separate from measured owner depth", () => {
  const deep = projected(crossworld, []), shallow = projected(joke, []);
  expect(deep.candidate.join_power).toBeGreaterThan(shallow.candidate.join_power);
  expect(deep.candidate.chain_span).toBeGreaterThan(shallow.candidate.chain_span);
  expect(deep.candidate.candidate_planes.economic).toBe(true);
  expect(shallow.candidate.candidate_planes.economic).toBe(false);
  expect(deep.realized.axes.discovery.depth).toBe(1);
  expect(deep.realized.axes.observation.depth).toBe(0);
  expect(deep.realized.braid_depth).toBe(0);
  expect(deep.cannot_infer).toContain("missing owner evidence -> non-participation");
});

test("observed and retained bodies require distinct evidence; repeat observation is deeper", () => {
  const observed = projected(crossworld, [foreignReceipt]);
  expect(observed.realized.axes.observation.depth).toBe(3);
  expect(observed.realized.braid_path).toEqual(["ExternalEstate", "ForeignTwin"]);
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-retained-body-"));
  const body = join(dir, "body.html");
  writeFileSync(body, "retained bytes\n");
  const retained = { ...foreignReceipt, body_path: body, _source_path: "/receipts/retained.json" };
  const held = projected(crossworld, [retained]);
  expect(held.realized.axes.observation.depth).toBe(4);
  expect(held.realized.axes.observation.depth_name).toBe("body-retained");
  const absent = { ...retained, body_path: join(dir, "missing.body") };
  expect(projected(crossworld, [absent]).realized.axes.observation.depth).toBe(3);
  expect(projected(crossworld, [foreignReceipt, retained]).realized.axes.observation.depth).toBe(5);
});

test("unreceipted owner claims and digest-free enactments are refused", () => {
  const bare = { occurrence_id: "external:x402", axis: "semantic", owner: "DomainProfile", state: "profile_bound",
    from_owner: "ForeignTwin", to_owner: "DomainProfile" };
  expect(normalizeFact(bare)).toBeNull();
  expect(normalizeFact({ ...bare, schema: "bonfyre-owner-participation.v1" })).toBeNull();
  expect(normalizeFact({ schema: "bonfyre-protocol-enactment.v1", occurrence_id: "external:x402",
    axis: "capability", state: "authorized", contract: "fixture" })).toBeNull();
  const facts = [foreignReceipt,
    { schema: "bonfyre-domain-binding.v1", occurrence_id: "external:x402", owner: "DomainProfile",
      state: "profile_bound", binding_validated: true, semantic_record_sha256: "b".repeat(64) },
    { schema: "bonfyre-protocol-binding.v1", occurrence_id: "external:x402", axis: "protocol",
      owner: "ProtocolContract", state: "compiled", from_owner: "DomainProfile", to_owner: "ProtocolContract",
      binding_validated: true, contract: "fixture", programs: 3, semantic_record_sha256: "c".repeat(64),
      domain_binding_sha256: "d".repeat(64), native_registration_receipt_sha256: "e".repeat(64) },
  ];
  const braided = projected(crossworld, facts);
  expect(braided.realized.axes.semantic.depth).toBe(2);
  expect(braided.realized.axes.protocol.depth).toBe(2);
  expect(braided.realized.braid_path).toEqual(["ExternalEstate", "ForeignTwin", "DomainProfile", "ProtocolContract"]);
});
