// What the recipe site shows that its record does not state: the catalogue.
// It is read from the families the recipe organ's own transformation derived
// (cmd/BonfyreRecipe/tools/derive_families.mjs): recipes that are one base told
// several ways are one entry with its variations; every other recipe stands alone.
import { existsSync, readdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { libraryFiles } from "../../library.mjs";

const sources = join(homedir(), ".bonfyre/estate-local/recipe-sources");
export const families = process.env.RECIPE_SITE_FAMILIES ?? (existsSync(sources) ? readdirSync(sources).filter(f => /^ffts-families-.*\.jsonl\.zst$/.test(f)).sort().map(f => join(sources, f)).pop() : null);

const sentence = text => { const t = String(text ?? "").trim(); return t && t === t.toUpperCase() ? t.toLowerCase().replace(/(^|[.!?]\s+)([a-z])/g, (m, a, b) => a + b.toUpperCase()) : t; };
const titled = text => (text === text.toUpperCase() ? text.toLowerCase().replace(/\b[a-z]/g, c => c.toUpperCase()) : text);
const plural = { cup: "cups", package: "packages", can: "cans", clove: "cloves", slice: "slices", quart: "quarts", pint: "pints", bunch: "bunches", pinch: "pinches", dash: "dashes", drop: "drops" };
// 9/4 cup -> "2 1/4 cups"; a counted thing ("each") is just its number.
const amount = i => { const whole = Math.floor(i.num / i.den), left = i.num - whole * i.den, n = [whole || "", left ? `${left}/${i.den}` : ""].filter(Boolean).join(" ");
  const unit = i.unit === "each" ? "" : i.num > i.den ? plural[i.unit] ?? i.unit : i.unit; return `${n}${unit ? " " + unit : ""} ${i.name.toLowerCase()}`; };

async function* records() {
  if (!families) throw new Error("no derived families are on file; run cmd/BonfyreRecipe/tools/derive_families.mjs first");
  const proc = Bun.spawn(["zstd", "-dc", families], { stdout: "pipe", stderr: "ignore" });
  let rest = ""; const decoder = new TextDecoder();
  for await (const chunk of proc.stdout) { rest += decoder.decode(chunk, { stream: true }); let at; while ((at = rest.indexOf("\n")) >= 0) { const line = rest.slice(0, at); rest = rest.slice(at + 1); if (line) yield JSON.parse(line); } }
  if (rest.trim()) yield JSON.parse(rest);
}
const kindsOf = new Map();
async function* entries(tally) {
  for await (const r of records()) {
    if (r.schema === "aurekai.preparation-family.v1") {
      const p = r.preparation; tally.families++;
      yield { id: r.recipe_id, title: titled(r.title), kinds: ["one base, many ways"], serves: "", parts: p.shared_batch.inputs.map(amount), method: "",
        ways: p.variants.map(v => [titled(v.name), [...v.additions.map(amount), ...(v.also ?? [])], sentence(v.instructions), v.serves ?? ""]),
        notes: [], from: `${p.variants.length} variations drawn together from ${r.source.tellings} tellings in the ${r.source.publisher} collections. Grouped by what the recipes share; not cooked as a set, not tested.` };
    } else {
      tally.single++;
      const archive = (r.source?.archive_url ?? "").split("/").pop().replace(/\.zip$/i, "");
      yield { id: r.recipe_id, title: titled(r.title), kinds: r.categories ?? [], serves: r.servings?.raw ?? "", parts: (r.ingredients ?? []).map(i => i.raw), method: sentence(r.instructions),
        notes: (r.substitution_candidates ?? []).map(n => n.source_note), from: `From the ${r.source?.publisher ?? "source"} collection “${archive}”${r.told > 1 ? `, where it appears ${r.told} times` : ""}. Kept as written; not tested here.` };
    }
  }
}

export async function catalogue() {
  const tally = { families: 0, single: 0 }, lib = await libraryFiles(entries(tally));
  return { count: lib.count, files: lib.files, noun: "recipes and families" };
}
