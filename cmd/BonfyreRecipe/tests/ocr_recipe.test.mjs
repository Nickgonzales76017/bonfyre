// A recipe card is drawn, photographed (rendered to a picture by a real browser),
// read by the estate's own OCR operator, and turned into a recipe record that
// the native recipe organ then scales. Nothing is read from the card's markup.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pythonJson } from "../tools/ffts_mealmaster.mjs";
import { readLines, recipeFromImage, recipeFromLines } from "../tools/ocr_recipe.mjs";
import { withChrome } from "../../../integrations/browser/capture/chrome-session.mjs";

const root = join(import.meta.dir, ".."), ocr = join(root, "../BonfyreOCR/bonfyre-ocr"), cli = join(root, "build/bonfyre-recipe");
const ready = existsSync(ocr) && existsSync(cli), when = ready ? test : test.skip;      // the operator is built on a Mac; elsewhere these are skipped, not faked
const dir = mkdtempSync(join(tmpdir(), "ocr-recipe-"));
const card = body => `<!doctype html><meta charset="utf-8"><body style="margin:0;background:#fbf7ee;font:22px Georgia,serif;color:#222;width:900px;padding:48px;box-sizing:border-box">${body}</body>`;
const printed = card(`<h1 style="font-size:46px;margin:0 0 6px">Nana's Lemon Barley Soup</h1><p style="margin:0 0 24px">Serves 6</p>
  <h2 style="font-size:26px;margin:0 0 8px">Ingredients</h2>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px 60px"><div>1 ½ cups pearl barley</div><div>2 tbsp olive oil</div><div>3 carrots, diced</div><div>¼ tsp black pepper</div><div>8 cups chicken stock</div><div>Juice of 2 lemons</div></div>
  <h2 style="font-size:26px;margin:26px 0 8px">Method</h2>
  <p style="margin:0;line-height:1.5">Warm the oil in a large pot and soften the carrots. Add the barley and stock, then simmer 40 minutes until tender. Stir in the lemon juice and pepper before serving.</p>
  <h2 style="font-size:26px;margin:26px 0 8px">Notes</h2><p style="margin:0">Try rice or use orzo if you have no barley.</p>`);
const typed = card(`<div style="font:24px 'Courier New',monospace;line-height:1.6"><div style="font-size:34px;font-weight:bold">SKILLET CORNBREAD</div><br>
  <div>2 cups cornmeal</div><div>1 cup buttermilk</div><div>2 eggs</div><div>1 tsp salt</div><br>
  <div>Heat a greased skillet in a hot oven. Mix everything and pour it in.</div><div>Bake 25 minutes until golden.</div></div>`);

beforeAll(async () => {
  if (!ready) return;
  await withChrome(async ({ open }) => {
    for (const [name, html] of [["printed", printed], ["typed", typed]]) {
      const { page, go } = await open();
      await page("Emulation.setDeviceMetricsOverride", { width: 900, height: 800, deviceScaleFactor: 2, mobile: false });
      await go("data:text/html;charset=utf-8;base64," + Buffer.from(html).toString("base64"));
      const { result } = await page("Runtime.evaluate", { returnByValue: true, expression: "document.body.scrollHeight" });
      const { data } = await page("Page.captureScreenshot", { format: "png", captureBeyondViewport: true, clip: { x: 0, y: 0, width: 900, height: result.value, scale: 1 } });
      writeFileSync(join(dir, name + ".png"), Buffer.from(data, "base64"));
    }
  }, { timeoutMs: 60_000 });
}, 90_000);
afterAll(() => rmSync(dir, { recursive: true, force: true }));

when("a printed card with headings and two columns of ingredients becomes a whole recipe record", () => {
  const r = recipeFromImage(join(dir, "printed.png"), { from: "Nana's card box" });
  expect(r.title).toBe("Nana's Lemon Barley Soup");
  expect(r.servings).toEqual({ raw: "Serves 6", count: 6, unit: "servings" });
  expect(r.ingredients.map(i => i.raw)).toEqual(["1 1/2 cups pearl barley", "3 carrots, diced", "8 cups chicken stock", "2 tbsp olive oil", "1/4 tsp black pepper", "Juice of 2 lemons"]);   // down the left column, then the right
  expect(r.ingredients[0]).toMatchObject({ quantity: "1 1/2", unit: "cups", name: "pearl barley" });
  expect(r.ingredients[5]).toMatchObject({ quantity: null, name: "juice of 2 lemons" });                // no amount stated first: none invented
  expect(r.instructions).toBe("Warm the oil in a large pot and soften the carrots. Add the barley and stock, then simmer 40 minutes until tender. Stir in the lemon juice and pepper before serving.");
  expect(r.substitution_candidates).toEqual([{ source_note: "* Try rice or use orzo if you have no barley.", status: "unverified_candidate" }]);
  expect(r.source).toMatchObject({ publisher: "Nana's card box", format: "photograph", read_by: "bonfyre-ocr", status: "machine_read_unreviewed" });
  expect(r.source.reading.mean_confidence).toBeGreaterThan(0.5); expect(r.recipe_id).toMatch(/^photo-[0-9a-f]{16}$/);
}, 60_000);

when("a typed card with no headings is still split into what goes in and what to do", () => {
  const r = recipeFromImage(join(dir, "typed.png"));
  expect(r.title).toBe("SKILLET CORNBREAD");
  expect(r.ingredients.map(i => i.raw)).toEqual(["2 cups cornmeal", "1 cup buttermilk", "2 eggs", "1 tsp salt"]);
  expect(r.instructions).toBe("Heat a greased skillet in a hot oven. Mix everything and pour it in. Bake 25 minutes until golden.");
  expect(r.servings.count).toBe(null); expect(r.source.publisher).toBe("a photograph supplied to this estate");
}, 60_000);

when("the photographed recipe is kept by the native organ and written back out exactly", () => {
  const r = recipeFromImage(join(dir, "printed.png")), source = join(dir, "photos.jsonl"), pack = join(dir, "photos.db");
  writeFileSync(source, pythonJson(r) + "\n");
  const run = args => Bun.spawnSync([cli, ...args], { stdout: "pipe", stderr: "pipe" });
  expect(run(["corpus", "factor", source, pack]).exitCode).toBe(0);
  expect(run(["corpus", "verify", pack, source]).exitCode).toBe(0);
  expect(run(["corpus", "export", pack]).stdout.toString()).toBe(pythonJson(r) + "\n");
}, 60_000);

test("lines the recogniser was unsure of are named, and an empty reading is refused", () => {
  const lines = readLines(["1\t0.1\t0.05\t0.5\t0.06\t0.98\tPlum Jam", "1\t0.1\t0.2\t0.4\t0.03\t0.31\t2 cups p1ums", "1\t0.1\t0.25\t0.4\t0.03\t0.95\t1 cup sugar", "1\t0.1\t0.4\t0.7\t0.03\t0.9\tBoil until it sets."].join("\n"));
  const r = recipeFromLines(lines, { imageSha: "ab".repeat(32) });
  expect(r.title).toBe("Plum Jam"); expect(r.ingredients.map(i => i.raw)).toEqual(["2 cups p1ums", "1 cup sugar"]);      // the misread stays as read
  expect(r.source.reading.least_sure).toEqual(["2 cups p1ums"]); expect(r.instructions).toBe("Boil until it sets.");
  expect(() => recipeFromLines([], { imageSha: "00".repeat(32) })).toThrow(/no text was read/);
});
