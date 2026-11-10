#!/usr/bin/env bun
// An upstream roundtrip probe is evidence of library viability, not a general
// Parquet-to-Lance lowering interface. This audit never materializes data.
import { createHash } from "node:crypto";
import { createReadStream, lstatSync, readFileSync } from "node:fs";
import { dirname, isAbsolute, join, resolve, sep } from "node:path";

export async function assessLanceActivation(manifest, activationPath) {
  if (manifest?.schema !== "bonfyre.substrate-activation.v1" || manifest.id !== "lance" ||
      manifest.state !== "active") return { state: "unverified", reason: "Lance activation is absent or invalid" };
  const artifacts = manifest.artifacts;
  if (!Array.isArray(artifacts) || artifacts.length === 0)
    return { state: "unverified", reason: "Lance activation has no verified artifacts" };
  const probe = artifacts.find(item => item.role === "lance-dataset-write-read-probe-binary");
  if (!probe || manifest.semantic_probe?.state !== "passed" ||
      manifest.semantic_probe?.kind !== "lance-dataset-write-read-roundtrip")
    return { state: "unverified", reason: "Lance dataset roundtrip is not proven" };
  if (!/^[0-9a-f]{64}$/.test(probe.sha256) || typeof probe.path !== "string" ||
      !probe.path || isAbsolute(probe.path) || probe.path.split(/[\\/]/).includes(".."))
    return { state: "unverified", reason: "Lance probe artifact coordinate is invalid" };
  try {
    const base = resolve(dirname(activationPath));
    const artifact = resolve(join(base, probe.path));
    if (!artifact.startsWith(base + sep)) throw new Error("artifact outside activation");
    const stat = lstatSync(artifact);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size !== probe.bytes)
      throw new Error("artifact identity mismatch");
    const digest = createHash("sha256");
    for await (const block of createReadStream(artifact)) digest.update(block);
    if (digest.digest("hex") !== probe.sha256) throw new Error("artifact digest mismatch");
  } catch {
    return { state: "unverified", reason: "Lance probe artifact identity does not match activation" };
  }
  return { state: "probe_only", reason: "Roundtrip probe has no verified arbitrary Parquet input, Lance output, or schema interface; Python Lance writer remains active" };
}

export async function assessLanceFile(path) {
  try { return await assessLanceActivation(JSON.parse(readFileSync(path, "utf8")), path); }
  catch { return { state: "unverified", reason: "Lance activation cannot be read" }; }
}

if (import.meta.main) {
  const [, , activationPath] = process.argv;
  if (!activationPath || process.argv.length !== 3) {
    console.error("usage: representation-manifold-lance-capability.mjs ACTIVATION_JSON");
    process.exitCode = 2;
  } else {
    const result = await assessLanceFile(activationPath);
    console.log(JSON.stringify(result));
    if (result.state !== "ready") process.exitCode = 1;
  }
}
