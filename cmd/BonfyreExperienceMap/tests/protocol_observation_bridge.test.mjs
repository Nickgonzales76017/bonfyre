import { test, expect } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendOnce, bindObservation } from "../tools/protocol_observation_bridge.mjs";

const sha = value => createHash("sha256").update(value).digest("hex");
test("a scoped native registration binds one validated domain receipt without enactment", () => {
  const base = mkdtempSync(join(tmpdir(), "bonfyre-protocol-native-"));
  try {
    const evidence = join(base, "evidence.jsonl"), native = join(base, "native.json"), output = join(base, "out.jsonl");
    const domain = { schema: "bonfyre-domain-binding.v1", occurrence_id: "external:protocol-0304",
      owner: "DomainProfile", state: "profile_bound", binding_validated: true, semantic_record_sha256: "a".repeat(64) };
    const compiled = { schema: "bonfyre-protocol-observation-bridge.v1", registered: true,
      occurrence_id: domain.occurrence_id, contract: "ogc-api-common-observation", programs: 3, scoped: true, state: "compiled" };
    writeFileSync(evidence, `${JSON.stringify(domain)}\n`);
    writeFileSync(native, JSON.stringify(compiled));
    const bound = bindObservation(domain.occurrence_id, evidence, native);
    expect(bound.owner).toBe("ProtocolContract");
    expect(bound.state).toBe("compiled");
    expect(bound).not.toHaveProperty("enacted");
    expect(bound.domain_binding_sha256).toBe(sha(JSON.stringify(Object.fromEntries(Object.entries(domain).sort(([a], [b]) => a.localeCompare(b))))));
    expect(bound.native_registration_receipt_sha256).toBe(sha(readFileSync(native)));
    expect(appendOnce(output, bound)).toBe(true);
    expect(appendOnce(output, bound)).toBe(false);
    expect(readFileSync(output, "utf8").trim().split("\n")).toHaveLength(1);
    writeFileSync(native, JSON.stringify({ ...compiled, occurrence_id: "external:other" }));
    expect(() => bindObservation(domain.occurrence_id, evidence, native)).toThrow("another occurrence");
    writeFileSync(native, JSON.stringify({ ...compiled, scoped: false }));
    expect(() => bindObservation(domain.occurrence_id, evidence, native)).toThrow("lost occurrence scope");
    writeFileSync(evidence, `${JSON.stringify({ ...domain, binding_validated: false })}\n`);
    expect(() => bindObservation(domain.occurrence_id, evidence, native)).toThrow("no validated DomainProfile receipt");
  } finally { rmSync(base, { recursive: true, force: true }); }
});
