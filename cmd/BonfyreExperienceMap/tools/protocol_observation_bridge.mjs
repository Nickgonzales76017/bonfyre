#!/usr/bin/env bun
// Receipted handoff from DomainProfile evidence to a native compiled contract.
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import policy from "./protocol-observation.generated.json" with { type: "json" };
import { canonicalJson } from "./json_contract.mjs";
export { policy };
const sha = bytes => createHash("sha256").update(bytes).digest("hex");
const canonicalSha = value => sha(canonicalJson(value));
const lines = path => readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));

export function bindObservation(occurrenceId, evidencePath, nativeReceiptPath) {
  const domain = lines(evidencePath).filter(item => item.schema === policy.domain_schema &&
    item.occurrence_id === occurrenceId && item.binding_validated === true &&
    item.owner === policy.domain_owner && item.state === "profile_bound").at(-1);
  if (!domain) throw new Error(`${occurrenceId} has no validated DomainProfile receipt`);
  if (!/^[a-fA-F0-9]{64}$/.test(domain.semantic_record_sha256 ?? ""))
    throw new Error("DomainProfile receipt has no semantic record hash");
  const nativeBytes = readFileSync(nativeReceiptPath);
  const native = JSON.parse(nativeBytes.toString());
  if (native.schema !== policy.native_schema) throw new Error("wrong native protocol bridge receipt schema");
  if (native.registered !== true || native.state !== "compiled") throw new Error("native protocol bridge did not compile a contract");
  if (native.occurrence_id !== occurrenceId) throw new Error("native protocol bridge receipt is for another occurrence");
  if (native.contract !== policy.contract || native.programs !== policy.programs)
    throw new Error("native protocol bridge did not compile the declared OGC contract");
  if (native.scoped !== true) throw new Error("native protocol bridge lost occurrence scope");
  return { schema: policy.schema, occurrence_id: occurrenceId, axis: "protocol", owner: policy.protocol_owner,
    state: "compiled", from_owner: policy.domain_owner, to_owner: policy.protocol_owner, binding_validated: true,
    contract: native.contract, semantic_record_sha256: domain.semantic_record_sha256,
    domain_binding_sha256: canonicalSha(domain), native_registration_receipt_sha256: sha(nativeBytes),
    programs: native.programs, cannot_infer: policy.cannot_infer };
}

export function appendOnce(path, receipt) {
  if (existsSync(path) && lines(path).some(item => item.schema === policy.schema &&
    item.occurrence_id === receipt.occurrence_id && item.domain_binding_sha256 === receipt.domain_binding_sha256)) return false;
  mkdirSync(dirname(path), { recursive: true });
  appendFileSync(path, `${canonicalJson(receipt)}\n`);
  return true;
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2), occurrenceId = args.shift();
    const read = key => { const i = args.indexOf(key); return i >= 0 ? args[i + 1] : null; };
    const evidence = read("--evidence"), native = read("--native-receipt"), output = read("--output");
    if (!occurrenceId || !evidence || !native) throw new Error("usage: protocol_observation_bridge.mjs OCCURRENCE --evidence PATH --native-receipt PATH [--output PATH]");
    const receipt = bindObservation(occurrenceId, evidence, native);
    if (output) appendOnce(output, receipt);
    console.log(canonicalJson(receipt));
  } catch (error) { console.log(JSON.stringify({ bound: false, error: error.message })); process.exitCode = 2; }
}
