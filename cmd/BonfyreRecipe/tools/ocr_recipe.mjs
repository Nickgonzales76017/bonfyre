#!/usr/bin/env bun
// A photographed or scanned recipe -> one household recipe record.
// The image is read by the estate's own operator (cmd/BonfyreOCR); this turns
// the lines it found, and where they sit on the page, into the same record the
// recipe organ keeps for every other recipe, so the photographed one can be
// searched, planned and scaled by the same native code.
//
// The record says it was read by a machine and not reviewed, carries the
// recogniser's confidence, and names the lines it was least sure of. Nothing
// is corrected, guessed or filled in.
//
//   bun ocr_recipe.mjs <image-or-pdf> [--from "whose recipe / where from"]      one JSON record on stdout
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ingredient, pythonJson } from "./ffts_mealmaster.mjs";

const here = import.meta.dir, built = join(here, "../../BonfyreOCR/bonfyre-ocr");
const FRACTIONS = { "½": "1/2", "⅓": "1/3", "⅔": "2/3", "¼": "1/4", "¾": "3/4", "⅛": "1/8", "⅜": "3/8", "⅝": "5/8", "⅞": "7/8" };
const plain = text => text.replace(/[½⅓⅔¼¾⅛⅜⅝⅞]/g, f => " " + FRACTIONS[f]).replace(/(\d)\s+(\d\/\d)/, "$1 $2").replace(/^[•·▪◦*–—-]\s*/, "").replace(/\s+/g, " ").trim();
const AMOUNT = /^(?:\d+(?:\s+\d+\/\d+)?|\d+\/\d+|\d*\.\d+|a|an|one|two|three|four|half)\b/i;
const HEAD = { parts: /^(ingredients?|what you need|you will need)\b:?$/i, method: /^(method|directions?|instructions?|preparation|steps|how to make it|to make)\b:?$/i, notes: /^(notes?|tips?|variations?)\b:?$/i };

export function readLines(tsv) {
  return tsv.split("\n").filter(Boolean).map(row => { const [page, x, y, w, h, confidence, ...text] = row.split("\t"); return { page: Number(page), x: Number(x), y: Number(y), w: Number(w), h: Number(h), confidence: Number(confidence), text: text.join(" ") }; });
}

export function recipeFromLines(lines, { imageSha, from = null } = {}) {
  if (!lines.length) throw new Error("no text was read from the image");
  // The title is the tallest line in the top third of the first page; failing that, the first line.
  const first = lines.filter(l => l.page === lines[0].page), top = first.filter(l => l.y < 0.34 && !Object.values(HEAD).some(h => h.test(l.text.trim())));
  const title = (top.length ? top.reduce((a, b) => (b.h > a.h * 1.15 ? b : a)) : first[0]);
  const rest = lines.filter(l => l !== title);
  let serves = null; const parts = [], method = [], notes = [], unsure = [];
  let zone = null, sawHead = false;
  // Where the page sets ingredients in two columns, each column is read down before the next.
  const region = rest.filter(l => AMOUNT.test(plain(l.text)) || /^[•·▪◦*–—-]\s/.test(l.text));
  const rightStart = (() => { const xs = region.map(l => l.x).sort((a, b) => a - b); let gap = 0, at = null; for (let i = 1; i < xs.length; i++) if (xs[i] - xs[i - 1] > gap) { gap = xs[i] - xs[i - 1]; at = (xs[i] + xs[i - 1]) / 2; } return gap > 0.22 ? at : null; })();
  const ordered = [];
  { let run = [];
    const flush = () => { if (!run.length) return; if (rightStart != null && run.some(l => l.x >= rightStart) && run.some(l => l.x < rightStart)) ordered.push(...run.filter(l => l.x < rightStart), ...run.filter(l => l.x >= rightStart)); else ordered.push(...run); run = []; };
    for (const l of rest) { if (region.includes(l)) run.push(l); else { flush(); ordered.push(l); } }
    flush(); }
  for (const l of ordered) {
    const text = l.text.trim(), flat = plain(text);
    if (!flat) continue;
    if (l.confidence < 0.5) unsure.push(text);
    const servesLine = /^(serves|servings?|yield|makes)\b[:\s]*(.+)$/i.exec(flat);
    if (servesLine && !serves) { serves = flat; continue; }
    if (HEAD.parts.test(text)) { zone = "parts"; sawHead = true; continue; }
    if (HEAD.method.test(text)) { zone = "method"; sawHead = true; continue; }
    if (HEAD.notes.test(text)) { zone = "notes"; continue; }
    if (zone === "notes") { notes.push("* " + flat); continue; }
    const looksListed = AMOUNT.test(flat) && flat.length <= 70 && !/[.!?]$/.test(flat) || /^[•·▪◦*–—-]\s/.test(text);
    if (zone === "parts" || (zone === null && looksListed && !method.length)) {
      if (zone === null && !looksListed) { method.push(flat); continue; }
      if (zone === "parts" && !looksListed && parts.length && flat.length < 40 && /^[a-z(]/.test(flat)) { parts.at(-1).raw += " " + flat; parts.at(-1).name += " " + flat.toLowerCase(); continue; }   // a wrapped ingredient line
      if (zone === "parts" && !looksListed && !sawHead) { zone = "method"; method.push(flat); continue; }
      parts.push(ingredient(flat) ?? { raw: flat, quantity: null, unit: null, name: flat.toLowerCase() });
      continue;
    }
    if (zone === null && parts.length) zone = "method";
    method.push(flat);
  }
  const count = serves && /(\d+(?:\.\d+)?)/.exec(serves);
  const mean = lines.reduce((sum, l) => sum + l.confidence, 0) / lines.length;
  return { schema: "bonfyre-household-recipe.v1", recipe_id: `photo-${imageSha.slice(0, 16)}`, title: plain(title.text),
    categories: ["photographed"], servings: { raw: serves, count: count ? Number(count[1]) : null, unit: count ? "servings" : null },
    ingredients: parts, substitution_candidates: notes.filter(n => /\b(or|optional|try)\b/i.test(n)).map(n => ({ source_note: n, status: "unverified_candidate" })),
    instructions: method.join(" ") || null,
    source: { publisher: from ?? "a photograph supplied to this estate", archive_url: "", image_sha256: imageSha, format: "photograph", read_by: "bonfyre-ocr", status: "machine_read_unreviewed",
      reading: { lines: lines.length, mean_confidence: Math.round(mean * 100) / 100, least_sure: unsure.slice(0, 12) } } };
}

export function recipeFromImage(path, { from = null, ocr = process.env.BONFYRE_OCR ?? built } = {}) {
  const out = Bun.spawnSync([ocr, "lines", path], { stdout: "pipe", stderr: "pipe" });
  if (out.exitCode !== 0) throw new Error(out.stderr.toString().trim() || "the image could not be read");
  return recipeFromLines(readLines(out.stdout.toString()), { imageSha: new Bun.CryptoHasher("sha256").update(readFileSync(path)).digest("hex"), from });
}

if (import.meta.main) {
  const [path, ...rest] = process.argv.slice(2), at = rest.indexOf("--from");
  if (!path) { console.error('usage: bun ocr_recipe.mjs <image-or-pdf> [--from "whose recipe / where from"]'); process.exit(2); }
  console.log(pythonJson(recipeFromImage(path, { from: at >= 0 ? rest[at + 1] : null })));
}
