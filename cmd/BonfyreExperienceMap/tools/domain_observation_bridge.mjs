#!/usr/bin/env bun
// Bind a retained observation to the declared DomainProfile evidence boundary.
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import policy from "./domain-observation.generated.json" with { type: "json" };
import { canonicalJson } from "./json_contract.mjs";

const here = dirname(fileURLToPath(import.meta.url));
export { policy };
const sha = bytes => createHash("sha256").update(bytes).digest("hex");
const canonicalSha = value => sha(canonicalJson(value));
const lines = path => readFileSync(path, "utf8").split(/\r?\n/).filter(line => line.trim()).map(line => JSON.parse(line));

async function metadataRecord(bytes, fallbackUrl) {
  const meta = new Map();
  const titleParts = [];
  await new HTMLRewriter()
    .on("title", { text(chunk) { if (chunk.text.trim()) titleParts.push(chunk.text.trim()); } })
    .on("meta", { element(element) {
      const key = element.getAttribute("property") || element.getAttribute("name");
      const value = element.getAttribute("content");
      if (key && value) meta.set(key.toLowerCase(), value.trim());
    } })
    .transform(new Response(bytes, { headers: { "content-type": "text/html; charset=utf-8" } })).arrayBuffer();
  const record = {
    name: meta.get("og:title") || titleParts.join(" ").trim(),
    authority: meta.get("og:site_name"),
    description: meta.get("description") || meta.get("og:description"),
    source_url: meta.get("og:url") || fallbackUrl,
  };
  const updated = meta.get("og:updated_time") || meta.get("article:modified_time");
  if (updated) record.updated_at = updated;
  const missing = Object.entries(record).filter(([, value]) => !value).map(([key]) => key).sort();
  if (missing.length) throw new Error(`parser ${policy.parser} did not produce required field(s): ${JSON.stringify(missing)}`);
  return record;
}

export async function bindObservation(occurrenceId, receiptPath, ingestDir) {
  const observation = lines(receiptPath).filter(row => row.schema === policy.observation_schema &&
    row.twin_id === occurrenceId && row.observation_kind === "source_observed" && row.body_path).at(-1);
  if (!observation) throw new Error(`${occurrenceId} has no retained ForeignTwin observation`);
  const observedHash = observation.content_sha256;
  if (!observedHash || sha(readFileSync(observation.body_path)) !== observedHash)
    throw new Error("retained body no longer matches its ForeignTwin receipt");
  const manifestPath = join(ingestDir, "intake-manifest.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  if (manifest.status !== "ready") throw new Error("ingest artifact is not ready");
  if (manifest.source_content_hash !== observedHash)
    throw new Error("ingest source hash does not match ForeignTwin content hash");
  if (typeof manifest.normalized_path !== "string") throw new Error("ingest manifest has no normalized path");
  const normalized = readFileSync(join(ingestDir, manifest.normalized_path));
  const normalizedHash = manifest.normalized_content_hash;
  if (!normalizedHash || sha(normalized) !== normalizedHash)
    throw new Error("normalized artifact no longer matches its ingest manifest");
  const record = await metadataRecord(normalized, observation.source_url);
  const undeclared = Object.keys(record).filter(field => !policy.fields.includes(field)).sort();
  if (undeclared.length) throw new Error(`parser produced undeclared semantic field(s): ${JSON.stringify(undeclared)}`);
  return {
    schema: policy.schema, occurrence_id: occurrenceId, axis: "semantic", owner: policy.owner,
    state: "profile_bound", from_owner: "ForeignTwin", to_owner: policy.owner,
    profile: policy.profile, parser: policy.parser, binding_validated: true,
    source_url: observation.source_url, source_content_sha256: observedHash,
    normalized_content_sha256: normalizedHash, foreign_receipt_sha256: canonicalSha(observation),
    ingest_manifest_sha256: sha(readFileSync(manifestPath)), semantic_record_sha256: canonicalSha(record),
    semantic_fields: Object.keys(record).sort(), evidence_requirements: policy.evidence_requirements,
    cannot_infer: policy.cannot_infer,
  };
}

export function appendOnce(path, receipt) {
  if (existsSync(path) && lines(path).some(row => row.schema === policy.schema &&
    row.occurrence_id === receipt.occurrence_id && row.semantic_record_sha256 === receipt.semantic_record_sha256)) return false;
  mkdirSync(dirname(path), { recursive: true });
  appendFileSync(path, `${canonicalJson(receipt)}\n`);
  return true;
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2), occurrenceId = args.shift();
    const read = key => { const i = args.indexOf(key); return i >= 0 ? args[i + 1] : null; };
    const evidence = read("--observation-receipts"), ingest = read("--ingest-dir"), output = read("--output");
    if (!occurrenceId || !evidence || !ingest) throw new Error("usage: domain_observation_bridge.mjs OCCURRENCE --observation-receipts PATH --ingest-dir PATH [--output PATH]");
    const receipt = await bindObservation(occurrenceId, evidence, ingest);
    if (output) appendOnce(output, receipt);
    console.log(canonicalJson(receipt));
  } catch (error) { console.log(JSON.stringify({ bound: false, error: error.message })); process.exitCode = 2; }
}
