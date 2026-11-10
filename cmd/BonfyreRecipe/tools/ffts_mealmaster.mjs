#!/usr/bin/env bun
// Meal-Master archives from the FFTS catalogue -> household recipe records.
// The native successor of tools/normalize_ffts_mealmaster.py.archive: the same
// reading of the same format, the same record, byte for byte, so a recipe
// already retained keeps its identity and digest. It differs in scope only: it
// reads every archive the catalogue lists, and archives with several members.
//
// The source's units and ingredient language are preserved. A note mentioning
// an alternative stays an unverified candidate; nothing is promoted to a
// substitution, a price, a nutrition fact or a tested method.
//
//   bun ffts_mealmaster.mjs catalogue <recipes.htm>                 list archive addresses
//   bun ffts_mealmaster.mjs archive <file.zip> <archive-url> [--first-form]   records for one archive, to stdout
//   bun ffts_mealmaster.mjs gather <recipes.htm> <work-dir> <out.jsonl> <receipts.jsonl> [--only a,b] [--wait ms] [--retry-empty] [--keep-archives]
import { appendFileSync, existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";

const HEADER = "---------- Recipe via Meal-Master";
const INGREDIENT = /^\s*((?:\d+(?:\s+\d+\/\d+)?|\d+\/\d+|[A-Z]))\s+(?:([A-Za-z]+)\s+)?(.+?)\s*$/;
const YIELD = /\b(\d+(?:\.\d+)?)\s+servings?\b/i;
const sha256 = bytes => new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
// Python's str.strip()/rstrip()/splitlines() treat a wider set of characters as space and line ends than JS trim does.
const U = (...codes) => String.fromCharCode(...codes);
const SPACE = `\\t\\n\\v\\f\\r\\x1c-\\x1f \\x85\\xa0${U(0x1680)}${U(0x2000)}-${U(0x200a)}${U(0x2028)}${U(0x2029)}${U(0x202f)}${U(0x205f)}${U(0x3000)}`;
const strip = text => text.replace(new RegExp(`^[${SPACE}]+|[${SPACE}]+$`, "g"), "");
const rstrip = text => text.replace(new RegExp(`[${SPACE}]+$`), "");
const LINE_END = new RegExp(`\\r\\n|[\\n\\r\\v\\f\\x1c\\x1d\\x1e\\x85${U(0x2028)}${U(0x2029)}]`);
const lines = text => { const out = text.split(LINE_END); if (out.at(-1) === "") out.pop(); return out; };

// json.dumps(record, sort_keys=True): sorted keys, ", " and ": ", ASCII only, floats keep their ".0".
export function pythonJson(value, float = false) {
  if (value === null || value === undefined) return "null";
  if (typeof value === "number") return float && Number.isInteger(value) ? value.toFixed(1) : String(value);
  if (typeof value === "string") return '"' + [...value].map(ch => { const c = ch.codePointAt(0);
    if (ch === '"') return '\\"'; if (ch === "\\") return "\\\\"; if (ch === "\n") return "\\n"; if (ch === "\r") return "\\r"; if (ch === "\t") return "\\t"; if (ch === "\b") return "\\b"; if (ch === "\f") return "\\f";
    if (c < 0x20 || c > 0x7e) return c > 0xffff ? [...ch].flatMap(() => [0xd800 + ((c - 0x10000) >> 10), 0xdc00 + ((c - 0x10000) & 0x3ff)]).slice(0, 2).map(u => "\\u" + u.toString(16).padStart(4, "0")).join("") : "\\u" + c.toString(16).padStart(4, "0");
    return ch; }).join("") + '"';
  if (Array.isArray(value)) return "[" + value.map(v => pythonJson(v)).join(", ") + "]";
  return "{" + Object.keys(value).sort().map(key => `${pythonJson(key)}: ${pythonJson(value[key], key === "count")}`).join(", ") + "}";
}

export function ingredient(part) {
  const m = INGREDIENT.exec(part);
  if (!m) return null;
  const name = strip(m[3]);
  if (!name || [...name].every(ch => ch === "*" || ch === "-")) return null;
  return { raw: strip(part), quantity: m[1], unit: (m[2] ?? "").toLowerCase() || null, name: name.toLowerCase() };
}

// `collection` names the archive in each identity; the first retained archive was "allrecip".
export function* recipeRecords(text, sourceUrl, archiveSha, member, collection) {
  let ordinal = 0;
  for (const block of text.split(HEADER)) {
    if (!strip(block)) continue;
    const meta = {}, body = [];
    for (const line of lines(block)) { const m = /^\s*(Title|Categories|Yield):\s*(.*?)\s*$/i.exec(line); if (m) meta[m[1].toLowerCase()] = m[2]; else body.push(rstrip(line)); }
    if (!meta.title) continue;
    ordinal++;
    const ingredients = [], notes = [], instructions = [];
    let reading = true, found = false;
    for (const line of body) {
      const stripped = strip(line);
      if (!stripped || stripped.startsWith("-----")) continue;
      if (stripped.startsWith("*")) { notes.push(stripped); continue; }
      if (reading) {
        // Meal-Master renders two ingredients in fixed-width columns.
        const chars = [...line], here = [chars.slice(0, 40).join(""), chars.slice(40).join("")].map(ingredient).filter(Boolean);
        if (here.length) { ingredients.push(...here); found = true; continue; }
        if (found) reading = false;
      }
      if (!reading) instructions.push(stripped);
    }
    const rawYield = meta.yield ?? "", servings = YIELD.exec(rawYield);
    const fingerprint = sha256(Buffer.from(meta.title + "\n" + block, "utf8")).slice(0, 16);
    yield { schema: "bonfyre-household-recipe.v1", recipe_id: `ffts-${collection}-${String(ordinal).padStart(4, "0")}-${fingerprint}`, title: meta.title,
      categories: (meta.categories ?? "").split(",").map(item => strip(item).toLowerCase()).filter(Boolean),
      servings: { raw: rawYield || null, count: servings ? Number(servings[1]) : null, unit: servings ? "servings" : null },
      ingredients, substitution_candidates: notes.filter(note => /\b(or|optional|try)\b/i.test(note)).map(note => ({ source_note: note, status: "unverified_candidate" })),
      instructions: instructions.join(" ") || null,
      source: { publisher: "FFTS / Now You're Cooking!", catalogue_url: "https://www.ffts.com/recipes.htm", archive_url: sourceUrl, archive_sha256: archiveSha, archive_member: member, format: "Meal-Master" } };
  }
}

// Meal-Master wrote the same recipe in more than one dress. The first retained
// archive uses "---------- Recipe via Meal-Master"; that reading above is kept
// exactly, so its records keep their identity. Other archives open a recipe
// with "MMMMM----- Recipe via Meal-Master" or "--- Recipe Extracted from
// Meal-Master (tm) Database ---", give "Servings:" where the first gives
// "Yield:", and may list kinds with spaces where the first uses commas.
const OPENING = /^(?:M{5}|-{5})[-M]*\s*Recipe (?:via|Extracted from)\b.*Meal-Master.*$/;
export function* recipeRecordsOtherForms(text, sourceUrl, archiveSha, member, collection) {
  const blocks = []; let current = null;
  for (const line of lines(text)) { if (OPENING.test(strip(line))) { current = []; blocks.push(current); } else if (current) current.push(line); }
  let ordinal = 0;
  for (const block of blocks) {
    const meta = {}, body = [];
    for (const line of block) { const m = /^\s*(Title|Categories|Yield|Servings):\s*(.*?)\s*$/i.exec(line); if (m && !(m[1].toLowerCase() in meta)) meta[m[1].toLowerCase()] = m[2]; else body.push(rstrip(line)); }
    if (!meta.title) continue;
    ordinal++;
    // Meal-Master lays ingredients out in two fixed columns, read down the left and then down the right.
    // A line beginning with a dash carries on the ingredient above it in its own column. The ingredient
    // block ends at the first empty line that is not followed by a part heading ("-----SAUCE-----").
    const notes = [], instructions = [], left = [], right = [];
    const heading = text => /^(?:-{5}|M{5})/.test(text);
    const open = text => /(?:--|\bor|,)\s*$/.test(text);                 // an ingredient that says more is coming
    const put = (column, part) => {
      let text = strip(part);
      if (!text) return;
      // A leading dash carries on the ingredient above. A leading colon stands where an amount would be: it
      // carries on an ingredient left open, names a part ("*For the sauce:"), or is an ingredient with no amount.
      const carried = /^-+\s*(.*)$/.exec(text), bare = /^:\s*(.*)$/.exec(text);
      if (bare) { text = strip(bare[1]); if (!text) return; if (text.startsWith("*")) { notes.push(text); return; } }
      if ((carried || (bare && column.length && open(column.at(-1).raw))) && column.length) { const last = column.at(-1), more = strip(carried ? carried[1] : text); if (more) { last.raw += " " + more; last.name += " " + more.toLowerCase(); } return; }
      column.push((bare ? null : ingredient(part)) ?? { raw: text, quantity: null, unit: null, name: text.toLowerCase() });
    };
    let at = 0;
    while (at < body.length && !strip(body[at])) at++;
    // An ingredient line starts with an amount (wherever the export put the amount column), with the colon
    // that stands for no amount, or, once the list has begun, is indented to the name column. Prose starts at the margin.
    const amount = /^\s{0,12}(?:\d+(?:\s+\d+\/\d+)?|\d+\/\d+|\.\d+|\d+\.\d+)\s/, begun = () => left.length > 0;
    const listed = line => amount.test(line) && [...line].length <= 84 || /^\s{0,4}:\s/.test(line) || /^\s{8,}\S/.test(line) && [...line].length <= 84 && begun();
    for (; at < body.length; at++) {
      const line = body[at], stripped = strip(line);
      if (!stripped) { let next = at + 1; while (next < body.length && !strip(body[next])) next++; if (next < body.length && heading(strip(body[next])) && /[A-Za-z]/.test(body[next].replace(/^M{5}/, ""))) continue; if (begun()) break; continue; }
      if (heading(stripped)) continue;
      if (stripped.startsWith("*")) { notes.push(stripped); continue; }
      if (!listed(line)) { if (begun()) break; continue; }
      // Two columns only where the right half itself starts an ingredient or carries one on; otherwise the line is one long ingredient.
      const chars = [...line], second = chars.slice(41).join("");
      if (chars[40] === " " && (amount.test(second) && /^\s{0,7}\S/.test(second) || /^\s*-\S?/.test(second) && right.length)) { put(left, chars.slice(0, 41).join("")); put(right, second); }
      else put(left, line);
    }
    for (; at < body.length; at++) { const stripped = strip(body[at]); if (!stripped || heading(stripped)) continue; if (stripped.startsWith("*")) notes.push(stripped); else instructions.push(stripped); }
    const ingredients = [...left, ...right];
    const rawYield = meta.yield ?? (meta.servings ? `${meta.servings} servings` : ""), servings = YIELD.exec(rawYield);
    const kinds = meta.categories ?? "";
    const fingerprint = sha256(Buffer.from(meta.title + "\n" + block.join("\n"), "utf8")).slice(0, 16);
    yield { schema: "bonfyre-household-recipe.v1", recipe_id: `ffts-${collection}-${String(ordinal).padStart(4, "0")}-${fingerprint}`, title: meta.title,
      categories: (kinds.includes(",") ? kinds.split(",") : kinds.split(/\s+/)).map(item => strip(item).toLowerCase()).filter(Boolean),
      servings: { raw: rawYield || null, count: servings ? Number(servings[1]) : null, unit: servings ? "servings" : null },
      ingredients, substitution_candidates: notes.filter(note => /\b(or|optional|try)\b/i.test(note)).map(note => ({ source_note: note, status: "unverified_candidate" })),
      instructions: instructions.join(" ") || null,
      source: { publisher: "FFTS / Now You're Cooking!", catalogue_url: "https://www.ffts.com/recipes.htm", archive_url: sourceUrl, archive_sha256: archiveSha, archive_member: member, format: "Meal-Master" } };
  }
}

const decode = bytes => new TextDecoder("windows-1252").decode(bytes);
// The first form's opening is also read by the fuller reading below, which keeps continued lines with their
// ingredient and reads the two columns in their own order. `firstFormOnly` asks for the retired tool's exact
// reading instead, which is what the organ's first retained pack was made with.
export function archiveRecords(zipPath, archiveUrl, { firstFormOnly = false } = {}) {
  const bytes = readFileSync(zipPath);
  // A catalogue link that no longer leads to an archive answers with a web page.
  if (bytes.subarray(0, 2).toString("latin1") !== "PK") throw new Error(/^\s*<(!doctype|html)/i.test(bytes.subarray(0, 200).toString("latin1")) ? "the catalogue's link answers with a web page, not an archive" : "not a zip archive");
  const digest = sha256(bytes), collection = basename(new URL(archiveUrl).pathname).replace(/\.zip$/i, "").toLowerCase();
  const listed = Bun.spawnSync(["unzip", "-Z1", zipPath], { stdout: "pipe", stderr: "pipe" });
  if (listed.exitCode !== 0) throw new Error(`not a readable archive: ${basename(zipPath)}`);
  const members = listed.stdout.toString("latin1").split("\n").filter(name => name && !name.endsWith("/"));
  const records = [];
  for (const member of members) {
    // A member's name can hold characters the shell-level tool treats as a pattern; they are escaped, never trusted.
    const body = Bun.spawnSync(["unzip", "-p", zipPath, member.replace(/[\[\]*?\\]/g, "\\$&")], { stdout: "pipe", stderr: "pipe", maxBuffer: 1 << 30 });
    if (body.exitCode !== 0 || !body.stdout.length) continue;
    const text = decode(body.stdout);
    if (!/Meal-Master/.test(text)) continue;
    // One member keeps the first archive's naming; several are told apart by their own names.
    const name = members.length === 1 ? collection : `${collection}.${member.replace(/\.[^.]*$/, "").replace(/[^A-Za-z0-9]+/g, "-").toLowerCase()}`;
    // Where the first form's opening is the only one present, the first reading applies unchanged.
    const first = text.includes(HEADER), others = lines(text).some(line => OPENING.test(strip(line)) && !strip(line).startsWith(HEADER));
    records.push(...(firstFormOnly && first && !others ? recipeRecords : recipeRecordsOtherForms)(text, archiveUrl, digest, member, name));
  }
  return { digest, bytes: bytes.length, members, records };
}

export const catalogueArchives = html => [...new Set([...html.matchAll(/href="([^"]+\.zip)"/gi)].map(m => new URL(m[1], "https://www.ffts.com/recipes.htm").href))].filter(url => new URL(url).host === "www.ffts.com");

if (import.meta.main) {
  const [verb, ...args] = process.argv.slice(2);
  if (verb === "catalogue") console.log(catalogueArchives(readFileSync(args[0], "latin1")).join("\n"));
  else if (verb === "archive") { for (const record of archiveRecords(args[0], args[1], { firstFormOnly: args.includes("--first-form") }).records) process.stdout.write(pythonJson(record) + "\n"); }
  else if (verb === "gather") {
    const [cataloguePath, work, out, receipts] = args, flag = name => { const at = args.indexOf(name); return at > 0 ? args[at + 1] : null; };
    const only = flag("--only")?.split(","), wait = Number(flag("--wait") ?? 700);
    const catalogue = readFileSync(cataloguePath), urls = catalogueArchives(catalogue.toString("latin1")).filter(url => !only || only.includes(basename(url).replace(/\.zip$/i, "")));
    mkdirSync(work, { recursive: true });
    // A later line for the same archive replaces an earlier one. --retry-empty reads again those that yielded nothing.
    const last = new Map((existsSync(receipts) ? readFileSync(receipts, "utf8").split("\n").filter(Boolean) : []).map(line => JSON.parse(line)).map(r => [r.archive_url, r]));
    const done = new Set([...last.values()].filter(r => !(args.includes("--retry-empty") && !r.recipe_count)).map(r => r.archive_url));
    const seen = new Set(existsSync(out) ? readFileSync(out, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line).recipe_id) : []);
    const prints = new Set([...seen].map(id => id.slice(id.lastIndexOf("-") + 1)));
    let total = 0;
    for (const url of urls) {
      if (done.has(url)) continue;                                   // a run that stopped picks up where it was
      const file = join(work, decodeURIComponent(new URL(url).pathname).replace(/^\/recipes\//, "").replace(/[^A-Za-z0-9._-]+/g, "_"));
      // An archive already kept from an earlier pass is read from disk; the source is not asked again.
      if (existsSync(file)) {
        try { const { digest, bytes, members, records } = archiveRecords(file, url); const sound = records.filter(record => pythonJson(record).length <= 262144), fp = record => record.recipe_id.slice(record.recipe_id.lastIndexOf("-") + 1), fresh = [];
          for (const record of sound) { if (seen.has(record.recipe_id) || prints.has(fp(record))) continue; prints.add(fp(record)); seen.add(record.recipe_id); fresh.push(record); appendFileSync(out, pythonJson(record) + "\n"); } total += fresh.length;
          appendFileSync(receipts, JSON.stringify({ schema: "bonfyre-household-food-source.v1", publisher: "FFTS / Now You're Cooking!", catalogue_url: "https://www.ffts.com/recipes.htm", catalogue_body_sha256: sha256(catalogue), archive_url: url, retrieved_at: new Date(statSync(file).mtimeMs).toISOString(), format: "Meal-Master", read_from_kept_archive: true,
            outcome: sound.length ? "read" : "no Meal-Master recipes found", archive_sha256: digest, archive_bytes: bytes, archive_members: members.length, recipe_count: sound.length, new_records: fresh.length, repeats_of_earlier: sound.length - fresh.length }) + "\n");
          continue; } catch (error) { console.error(`${basename(file)} (kept): ${error.message}`); }
      }
      const fetched = await fetch(url, { headers: { "user-agent": "aurekai-recipe-material/1 (one polite pass; contact via aurekai.life)" } }).catch(error => ({ ok: false, status: String(error.message) }));
      const receipt = { schema: "bonfyre-household-food-source.v1", publisher: "FFTS / Now You're Cooking!", catalogue_url: "https://www.ffts.com/recipes.htm", catalogue_body_sha256: sha256(catalogue), archive_url: url, retrieved_at: new Date().toISOString(), format: "Meal-Master",
        fact_boundary: "Recipe text is a source observation; no current price, store availability, nutrition, or verified substitution fact is asserted." };
      if (!fetched.ok) { appendFileSync(receipts, JSON.stringify({ ...receipt, outcome: `not retrieved: ${fetched.status}`, recipe_count: 0 }) + "\n"); console.error(`${basename(url)}: not retrieved (${fetched.status})`); await Bun.sleep(wait); continue; }
      writeFileSync(file, Buffer.from(await fetched.arrayBuffer()));
      try {
        const { digest, bytes, members, records } = archiveRecords(file, url);
        // A "recipe" of a quarter megabyte is recipes run together by an opening this reader does not know; it is counted, not kept.
        const sound = records.filter(record => pythonJson(record).length <= 262144), ranTogether = records.length - sound.length;
        // The same recipe text often sits in several archives. Its fingerprint ends its identity; the first copy is kept and later ones counted.
        const print = record => record.recipe_id.slice(record.recipe_id.lastIndexOf("-") + 1);
        const fresh = sound.filter(record => !seen.has(record.recipe_id) && !prints.has(print(record)));
        for (const record of fresh) prints.add(print(record));
        for (const record of fresh) { seen.add(record.recipe_id); appendFileSync(out, pythonJson(record) + "\n"); }
        total += fresh.length;
        appendFileSync(receipts, JSON.stringify({ ...receipt, outcome: sound.length ? "read" : "no Meal-Master recipes found", archive_sha256: digest, archive_bytes: bytes, archive_members: members.length, recipe_count: sound.length, new_records: fresh.length, ...(ranTogether ? { ran_together_and_dropped: ranTogether } : {}) }) + "\n");
        console.error(`${basename(url)}: ${records.length} recipes (${fresh.length} new), ${bytes} bytes`);
      } catch (error) { appendFileSync(receipts, JSON.stringify({ ...receipt, outcome: `unreadable: ${error.message}`, recipe_count: 0 }) + "\n"); console.error(`${basename(url)}: ${error.message}`); }
      if (!args.includes("--keep-archives")) rmSync(file, { force: true });   // the digest is kept; the archive can be fetched again
      await Bun.sleep(wait);
    }
    console.log(JSON.stringify({ archives: urls.length, new_records: total, out, bytes: existsSync(out) ? statSync(out).size : 0 }));
  } else { console.error("usage: bun ffts_mealmaster.mjs catalogue|archive|gather ..."); process.exit(2); }
}
