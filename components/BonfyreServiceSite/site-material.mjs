// Admitted material resolution: only explicitly named material functions run.
import { existsSync } from "node:fs";
import { join } from "node:path";
import { pagesOf } from "./site-contract.mjs";

// Material is what a section shows that the record does not state: rows read
// from records this estate holds. The site folder provides it by name; a
// record can only ask for a name the folder has, never run anything itself.
export function materialFilePath(sectionId, relativePath) {
  // Material is a data projection, not a destination writer. No absolute
  // pathname, parent segment, backslash or empty segment may escape its owner.
  if (!/^[a-z][a-z0-9-]*$/.test(sectionId) || typeof relativePath !== "string" ||
      !relativePath || relativePath.includes("\\") || relativePath.includes("\0") ||
      relativePath.split("/").some(segment => !segment || segment === "." || segment === ".."))
    throw new Error("material file path must stay inside the named section");
  return "data/" + sectionId + "/" + relativePath;
}

export async function withMaterial(content, siteDir) {
  const wanted = pagesOf(content).flatMap(p => p.sections).filter(s => s.material);
  if (!wanted.length) return content;
  const file = siteDir && join(siteDir, "material.mjs");
  if (!file || !existsSync(file)) throw new Error(`site record asks for material "${wanted[0].material}" but the site folder has no material.mjs`);
  const provided = await import(file);
  const filled = structuredClone(content);
  for (const s of pagesOf(filled).flatMap(p => p.sections).filter(s => s.material)) {
    if (typeof provided[s.material] !== "function") throw new Error(`material "${s.material}" is not provided by ${file}`);
    if (!/^[a-z][a-z0-9-]*$/.test(s.id ?? "")) throw new Error(`a section that takes material needs an id (${s.kind})`);
    const given = await provided[s.material]();
    // Material may also hand over files (pieces of a large collection). They are published under the section's own name.
    if (given.files) {
      if (typeof given.files !== "object" || Array.isArray(given.files)) throw new Error("material files must be a filename-to-bytes map");
      const entries = Object.entries(given.files).map(([path, bytes]) => {
        if (typeof bytes !== "string" && !ArrayBuffer.isView(bytes)) throw new Error("material file must contain bytes or text");
        return [materialFilePath(s.id, path), bytes];
      });
      filled._files = { ...(filled._files ?? {}), ...Object.fromEntries(entries) };
      given.data = "/data/" + s.id + "/";
    }
    Object.assign(s, { ...given, files: undefined });
  }
  return filled;
}
