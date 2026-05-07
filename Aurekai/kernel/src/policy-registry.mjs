import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageRoot = join(__dirname, "..");
const policyRegistryPath = join(packageRoot, "..", "registry", "continuity-policy-families.json");

export function listPolicyFamilies() {
  const parsed = JSON.parse(readFileSync(policyRegistryPath, "utf8"));
  return Array.isArray(parsed?.families) ? parsed.families : [];
}

export function getPolicyFamily(id) {
  return listPolicyFamilies().find((item) => item.id === id) || null;
}

export function getPolicyRegistry() {
  return {
    schema_version: "aurekai.kernel_policy_registry.v1",
    families: listPolicyFamilies(),
  };
}
