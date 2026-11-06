#!/usr/bin/env bun
// Recipes that are one preparation told many times -> one preparation family.
//
// The authored oat-bite family showed the shape: one base stated once, and each
// variation stated only as what it adds. This finds that shape in a recipe
// collection. Recipes for the same dish that share the same ingredients in the
// same amounts are one base; each recipe becomes a variation carrying only its
// own additions and its own method. The result is the organ's existing
// aurekai.preparation-family.v1 record, so the native quantity code scales it.
//
// A family is only what the sources already agree on: an ingredient is in the
// base when every member lists it with the same amount and unit. Nothing is
// averaged, converted or assumed, and the family says it was grouped by a
// machine and never cooked as a set.
//
//   bun derive_families.mjs <records.jsonl[.zst]> <out.jsonl>     families first, then every recipe left on its own
import { createWriteStream } from "node:fs";
import { pythonJson } from "./ffts_mealmaster.mjs";

const gcd = (a, b) => (b ? gcd(b, a % b) : a);
// "1 1/2" -> [3, 2]; anything that is not a plain amount has no exact value and cannot be part of a base.
export function exact(quantity) {
  const m = /^(?:(\d+)\s+)?(\d+)\/(\d+)$/.exec(quantity ?? "") ?? /^(\d+)()()$/.exec(quantity ?? "");
  if (!m) return null;
  const whole = m[3] ? Number(m[1] ?? 0) : Number(m[1]), num = m[3] ? whole * Number(m[3]) + Number(m[2]) : whole, den = m[3] ? Number(m[3]) : 1;
  if (!den || num <= 0 || num > 1e9 || den > 1e6) return null;
  const g = gcd(num, den); return [num / g, den / g];
}
const UNIT = { c: "cup", cup: "cup", cups: "cup", t: "tsp", ts: "tsp", tsp: "tsp", teaspoon: "tsp", teaspoons: "tsp", tb: "tbsp", tbs: "tbsp", tbsp: "tbsp", tablespoon: "tbsp", tablespoons: "tbsp", lb: "lb", lbs: "lb", pound: "lb", pounds: "lb", oz: "oz", ounce: "oz", ounces: "oz",
  pk: "package", pkg: "package", cn: "can", can: "can", cans: "can", ea: "each", x: "each", sm: "small", md: "medium", lg: "large", cl: "clove", pn: "pinch", ds: "dash", qt: "quart", pt: "pint", ga: "gallon", g: "g", kg: "kg", ml: "ml", l: "l", sl: "slice", bn: "bunch", ct: "carton", dr: "drop", drops: "drop" };
// The ingredient's own name: what is before the first comma, bracket, dash or semicolon, lower-cased, plurals folded.
export const nameOf = text => String(text ?? "").toLowerCase().split(/[,;(]|--| - /)[0].replace(/[^a-z0-9 ]+/g, " ").replace(/\s+/g, " ").trim().replace(/(?<=[a-z]{3})(ies)$/, "y").replace(/(?<=[a-z]{3}[^s])s$/, "");
const CREDIT = /\b(formatted by|posted by|typed by|recipe by|submitted by|from the kitchen|mm by|converted by|source:)/i;
export function partsOf(recipe) {
  const seen = new Map();
  for (const i of recipe.ingredients ?? []) {
    const amount = exact(i.quantity), name = nameOf(i.name);
    if (!amount || !name || CREDIT.test(i.name)) continue;       // a typist's credit line is not an ingredient
    const unit = UNIT[(i.unit ?? "").toLowerCase()] ?? (i.unit ?? "each").toLowerCase();
    const key = `${name}|${amount[0]}/${amount[1]}|${unit}`;
    if (!seen.has(key)) seen.set(key, { key, id: name.replace(/ /g, "-").slice(0, 60), name: i.name.split(/[,;(]|--/)[0].trim().replace(/\s+/g, " "), num: amount[0], den: amount[1], unit });
  }
  return seen;
}
// The dish a title names: its last two words of substance, without numbers, credits or brackets.
const NOISE = new Set(["the", "and", "with", "for", "from", "recipe", "style", "easy", "quick", "best", "old", "fashioned", "homemade", "low", "fat", "free", "favorite", "new", "my", "moms", "mom", "grandmas", "grandma", "aunt", "s"]);
export const dishOf = title => { const words = String(title ?? "").toLowerCase().replace(/\([^)]*\)|#\s*\d+|\bno\.?\s*\d+/g, " ").replace(/[^a-z ]+/g, " ").split(/\s+/).filter(w => w.length > 1 && !NOISE.has(w)); return words.slice(-2).join(" "); };

export function deriveFamilies(recipes, { least = 3, share = 0.6, most = 128 } = {}) {
  const dishes = new Map();
  recipes.forEach((r, n) => { const dish = dishOf(r.title); if (!dish || r.parts.size < least) return; if (!dishes.has(dish)) dishes.set(dish, []); dishes.get(dish).push(n); });
  const families = [], taken = new Set();
  for (const [dish, members] of dishes) {
    if (members.length < 2) continue;
    // Largest ingredient lists seed first, so the fullest telling of a dish defines its family.
    members.sort((a, b) => recipes[b].parts.size - recipes[a].parts.size || a - b);
    // An index from each exact ingredient to the members listing it keeps big dishes from being compared pair by pair.
    const holds = new Map();
    for (const n of members) for (const key of recipes[n].parts.keys()) { if (!holds.has(key)) holds.set(key, []); holds.get(key).push(n); }
    for (const seed of members) {
      if (taken.has(seed)) continue;
      const mine = recipes[seed].parts, tally = new Map();
      for (const key of mine.keys()) for (const n of holds.get(key)) if (n !== seed && !taken.has(n)) tally.set(n, (tally.get(n) ?? 0) + 1);
      const close = [...tally].filter(([n, c]) => c >= least && c >= share * Math.min(mine.size, recipes[n].parts.size)).sort((a, b) => b[1] - a[1] || a[0] - b[0]);
      if (!close.length) continue;
      let base = new Set(mine.keys()); const group = [seed];
      for (const [n] of close) {
        if (group.length === most) break;
        const next = new Set([...base].filter(key => recipes[n].parts.has(key)));
        if (next.size < least || next.size < share * Math.min(mine.size, recipes[n].parts.size)) continue;
        base = next; group.push(n);
      }
      if (group.length < 2) continue;
      for (const n of group) taken.add(n);
      families.push({ dish, base: [...base], members: group });
    }
  }
  return { families, taken };
}

const slug = text => text.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 48) || "x";
const titled = text => text.toLowerCase().replace(/\b[a-z]/g, c => c.toUpperCase());
const listed = names => names.length < 2 ? names.join("") : names.slice(0, -1).join(", ") + " and " + names.at(-1);

// A family's members, with retellings folded together. Two members that add the same things to the base are
// one variation told twice; the first telling is kept and the rest are counted.
export function variationsOf(family, recipes) {
  const base = new Set(family.base), seen = new Map();
  for (const n of family.members) {
    const r = recipes[n], additions = [...r.parts.values()].filter(p => !base.has(p.key));
    const sign = additions.map(p => p.key).sort().join(";") + "#" + r.loose.map(l => nameOf(l.replace(/^[\d\s\/.]+/, ""))).filter(Boolean).sort().join(";");
    if (seen.has(sign)) seen.get(sign).told.push(r.id); else seen.set(sign, { recipe: r, additions, told: [r.id] });
  }
  return [...seen.values()];
}

export function familyRecord(family, recipes, variations = variationsOf(family, recipes)) {
  const first = recipes[family.members[0]], print = new Bun.CryptoHasher("sha256").update(family.members.map(n => recipes[n].id).join("\n")).digest("hex").slice(0, 12);
  const ids = new Set(), unique = base => { let id = base, n = 2; while (ids.has(id)) id = `${base}-${n++}`; ids.add(id); return id; };
  // A variation is named by its source's own title when that tells it apart, and otherwise by what it adds.
  const titles = new Map(); for (const v of variations) { const t = titled(v.recipe.title.replace(/\s*#\s*\d+\s*$/, "")); titles.set(t, (titles.get(t) ?? 0) + 1); }
  const nameFor = v => { const t = titled(v.recipe.title.replace(/\s*#\s*\d+\s*$/, "")); if (titles.get(t) === 1) return t;
    const adds = [...v.additions.map(p => p.name.toLowerCase()), ...v.recipe.loose.map(l => l.replace(/^[\d\s\/.]+[a-z]{0,3}\s+/i, "").toLowerCase())].slice(0, 3);
    return adds.length ? `With ${listed(adds)}` : "The plain base"; };
  return { schema: "aurekai.preparation-family.v1", recipe_id: `family:${slug(family.dish)}-${print}`, title: `${titled(family.dish)}, ${variations.length} ways`,
    source: { publisher: first.publisher, archive_url: "", status: "grouped_by_machine_not_cooked_as_a_set", derived_from: variations.flatMap(v => v.told), tellings: family.members.length,
      rule: "Every member lists each base ingredient with the same amount and unit; a variation is a member's remaining ingredients and its own method. Members adding the same things are one variation." },
    preparation: {
      shared_batch: { id: "base", yield: { num: 1, den: 1, unit: "batch" }, portion: { num: 1, den: 1, unit: "batch" },
        inputs: family.base.map(key => { const p = first.parts.get(key); return { id: p.id, name: p.name, num: p.num, den: p.den, unit: p.unit }; }),
        instructions: "Each variation is its own batch of this base. The base is what every source agrees on; it has not been cooked as one shared batch." },
      per_variant: [],
      variants: variations.map(v => ({ id: unique(slug(nameFor(v))), name: nameFor(v), additions: v.additions.map(p => ({ id: p.id, name: p.name, num: p.num, den: p.den, unit: p.unit })), also: v.recipe.loose,
        instructions: v.recipe.method ?? "", serves: v.recipe.serves ?? "", from: v.recipe.id, told: v.told.length })) } };
}

if (import.meta.main) {
  const [input, output] = process.argv.slice(2);
  if (!output) { console.error("usage: bun derive_families.mjs <records.jsonl[.zst]> <out.jsonl>"); process.exit(2); }
  const proc = Bun.spawn(input.endsWith(".zst") ? ["zstd", "-dc", input] : ["cat", input], { stdout: "pipe" });
  const recipes = [], decoder = new TextDecoder(); let rest = "";
  const take = line => { const r = JSON.parse(line); if (r.schema !== "bonfyre-household-recipe.v1") return; const parts = partsOf(r);
    const keys = new Set([...parts.values()].map(p => p.name.toLowerCase()));
    recipes.push({ id: r.recipe_id, title: r.title.trim(), parts, method: r.instructions, serves: r.servings?.raw ?? "", publisher: r.source?.publisher ?? "", kinds: r.categories ?? [],
      // Ingredients with no exact amount stay as the source wrote them and travel with their variation.
      loose: (r.ingredients ?? []).filter(i => !exact(i.quantity)).map(i => i.raw), line }); };
  for await (const chunk of proc.stdout) { rest += decoder.decode(chunk, { stream: true }); let at; while ((at = rest.indexOf("\n")) >= 0) { const line = rest.slice(0, at); rest = rest.slice(at + 1); if (line) take(line); } }
  if (rest.trim()) take(rest);
  const { families, taken } = deriveFamilies(recipes);
  const out = createWriteStream(output); let bytes = 0;
  const write = text => { bytes += Buffer.byteLength(text); return out.write(text) || new Promise(done => out.once("drain", done)); };
  // A group that turns out to be one recipe retold is not a family: its first telling stands for all of them.
  let real = 0, retold = 0, folded = 0, variationsOut = 0; const standsFor = new Map();
  for (const family of families) { const variations = variationsOf(family, recipes);
    if (variations.length < 2) { retold++; folded += family.members.length - 1; standsFor.set(family.members[0], family.members.length); for (const n of family.members.slice(1)) taken.add(n); taken.delete(family.members[0]); continue; }
    real++; variationsOut += variations.length; folded += family.members.length - variations.length;
    await write(pythonJson(familyRecord(family, recipes, variations)) + "\n"); }
  let alone = 0; for (let n = 0; n < recipes.length; n++) if (!taken.has(n)) { alone++;
    await write((standsFor.has(n) ? pythonJson({ ...JSON.parse(recipes[n].line), told: standsFor.get(n) }) : recipes[n].line) + "\n"); }
  await new Promise(done => out.end(done));
  const sizes = families.map(f => f.members.length).sort((a, b) => b - a);
  console.log(JSON.stringify({ recipes: recipes.length, families: real, variations_in_families: variationsOut, one_recipe_retold: retold, retellings_folded: folded, recipes_on_their_own: alone, catalogue_entries: real + alone, in_bytes: recipes.reduce((n, r) => n + Buffer.byteLength(r.line) + 1, 0), out_bytes: bytes }));
}
