import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageRoot = join(__dirname, "..");
const schemasDir = join(packageRoot, "..", "schemas");

const SCHEMA_FILES = [
  "aurekai.adapter.contract.v1.json",
  "aurekai.committed_state.v1.json",
  "aurekai.transition.v1.json",
  "aurekai.trajectory.v1.json",
  "aurekai.policy_decision.v1.json",
  "aurekai.lineage_edge.v1.json",
  "aurekai.functional_claim.v1.json",
  "aurekai.state_object.v1.json",
  "aurekai.export_projection.v1.json",
  "aurekai.integration_execution.v1.json",
  "aurekai.fulfillment_transition.v1.json",
  "aurekai.invoice.proof.v1.json",
  "aurekai.sla.continuity.v1.json",
  "aurekai.delivery_witness.v1.json",
    "aurekai.chargeability_claim.v1.json",
    "aurekai.treasury_continuity.v1.json"
];

function parseJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

export function listCanonicalSchemas() {
  return SCHEMA_FILES.map((name) => ({
    name,
    id: parseJson(join(schemasDir, name)).$id || null,
  }));
}

export function getCanonicalSchema(name) {
  return parseJson(join(schemasDir, name));
}

export function getCanonicalSchemaRegistry() {
  const registry = {};
  for (const file of SCHEMA_FILES) {
    const schema = parseJson(join(schemasDir, file));
    registry[file] = schema;
  }
  return registry;
}
