// Canonical section registry. Adding a kind requires one sections/<kind>.mjs owner.
import { readdirSync } from "node:fs";
import { join } from "node:path";
const root=import.meta.dir;
export const kinds = Object.fromEntries(await Promise.all(readdirSync(join(root, "sections")).filter(f => f.endsWith(".mjs"))
  .map(async f => [f.replace(/\.mjs$/, ""), await import(join(root, "sections", f))])));
