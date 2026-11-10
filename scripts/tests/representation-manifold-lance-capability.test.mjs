import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { assessLanceActivation, assessLanceFile } from "../representation-manifold-lance-capability.mjs";

test("Lance roundtrip activation is probe-only, not a general writer", async () => {
  const root = mkdtempSync(join(tmpdir(), "lance-capability-"));
  try {
    const binary = Buffer.from("fixture-probe");
    writeFileSync(join(root, "probe"), binary);
    const manifest = { schema: "bonfyre.substrate-activation.v1", id: "lance", state: "active",
      semantic_probe: { state: "passed", kind: "lance-dataset-write-read-roundtrip" },
      artifacts: [{ role: "lance-dataset-write-read-probe-binary", path: "probe", bytes: binary.length,
        sha256: createHash("sha256").update(binary).digest("hex") }] };
    const activation = join(root, "activation.json");
    writeFileSync(activation, JSON.stringify(manifest));
    expect((await assessLanceFile(activation)).state).toBe("probe_only");
    expect((await assessLanceActivation({ ...manifest, semantic_probe: { state: "failed" } }, activation)).state).toBe("unverified");
    writeFileSync(join(root, "probe"), "tampered");
    expect((await assessLanceFile(activation)).state).toBe("unverified");
    expect((await assessLanceActivation({ ...manifest, artifacts: [{ ...manifest.artifacts[0], path: "../probe" }] }, activation)).state).toBe("unverified");
  } finally { rmSync(root, { recursive: true }); }
});
