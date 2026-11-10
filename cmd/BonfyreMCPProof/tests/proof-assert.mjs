const chunks = [];
for await (const chunk of Bun.stdin.stream()) chunks.push(chunk);
const proof = JSON.parse(Buffer.concat(chunks).toString("utf8"));
if (proof.passed !== true || proof.verified_owner_count !== 5) {
  throw new Error(`owner proof did not pass: ${JSON.stringify(proof)}`);
}
for (const name of [
  "control_plane_manifest_persisted",
  "cms_output_content_hash",
  "artifact_output_hash",
  "flow_definition",
  "discipl_contract",
]) {
  if (proof.checks?.[name]?.passed !== true) {
    throw new Error(`${name} did not pass: ${JSON.stringify(proof.checks?.[name])}`);
  }
}
