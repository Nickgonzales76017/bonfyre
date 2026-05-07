import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageRoot = join(__dirname, "..");
const chartRegistryPath = join(packageRoot, "..", "registry", "chart-families.json");

export function listChartFamilies() {
  const parsed = JSON.parse(readFileSync(chartRegistryPath, "utf8"));
  return Array.isArray(parsed?.families) ? parsed.families : [];
}

export function getChartFamily(id) {
  return listChartFamilies().find((item) => item.id === id) || null;
}

export function getChartRegistry() {
  return {
    schema_version: "aurekai.kernel_chart_registry.v1",
    families: listChartFamilies(),
  };
}
