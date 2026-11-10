// The Meal-Master reader, on the forms the FFTS archives actually use.
import { expect, test } from "bun:test";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { archiveRecords, catalogueArchives, pythonJson, recipeRecords, recipeRecordsOtherForms } from "../tools/ffts_mealmaster.mjs";

const first = `---------- Recipe via Meal-Master (tm) v8.01\r\n \r\n      Title: Carrot Cake\r\n Categories: Cakes, Main dish\r\n      Yield: 12 servings\r\n \r\n      1 c  Oil                                 1 ts Baking soda\r\n  1 1/2 c  Sugar\r\n \r\n  Mix and bake.\r\n  * or use butter\r\n \r\n-----\r\n`;
const mmmmm = `MMMMM----- Recipe via Meal-Master (tm) v8.05\r\n \r\n      Title: London Broil\r\n Categories: Beef, Grill\r\n      Yield: 4 Servings\r\n \r\n    1/3 c  Soy sauce\r\n      2 tb Mustard\r\nMMMMM--------------------------SAUCE------\r\n      1 c  Stock\r\n \r\n  Marinate, then grill 2 hours before serving.\r\n \r\nMMMMM\r\n \r\nMMMMM----- Recipe via Meal-Master (tm) v8.05\r\n \r\n      Title: Second One\r\n Categories: Sides\r\n      Yield: 2 servings\r\n \r\n      1 c  Rice\r\n \r\n  Boil.\r\n \r\nMMMMM\r\n`;
const extracted = `------------- Recipe Extracted from Meal-Master (tm) Database --------------\r\n \r\n     Title: Cold Clam Dip\r\nCategories: Dips Appetizers    \r\n  Servings:  4\r\n\r\n     10 oz Clams                               6 oz Cream Cheese \r\n\r\n     Blend all ingredients together.  Let set for 2 hours before serving.\r\n \r\n-----------------------------------------------------------------------------\r\n`;
// Two columns read down then across, a continued line in each, an ingredient with no amount, and a part heading after a gap.
const columns = `MMMMM----- Recipe via Meal-Master (tm) v8.02\r\n \r\n      Title: Chicken With Saffron Rice\r\n Categories: Main dish, Poultry\r\n      Yield: 4 servings\r\n \r\n      4 oz Soft-spreadable cheese               2    Whole chicken breasts,\r\n           -with garlic and herbs                   -boned, halved\r\n      5 tb Butter, cut into pieces            12 md Mushrooms\r\n           Salt and pepper\r\n \r\nMMMMM---------------------------SAUCE---------------------------\r\n    1/4 c  White wine\r\n \r\n  Spread the cheese under the skin.\r\n  2 hours later, bake until done.\r\n \r\nMMMMM\r\n`;
// A third layout: one wide column, the amount column further in, and a colon standing where an amount would be.
const colons = `MMMMM----- Recipe via Meal-Master (tm) v8.05\r\n \r\n      Title: Saffron Rice With Wild Mushroom Sauce\r\n Categories: Meatless, Entree\r\n      Yield: 6 Servings\r\n \r\n \r\n  :          *For the saffron rice:\r\n        1 TB extra virgin olive oil\r\n    1 1/4 c  Arborio rice\r\n        1 TB grated Grana Padano -- or\r\n  :          Parmigiano Reggiano\r\n  :          salt\r\n        1    leek (white part only) --\r\n  :          thinly sliced\r\n        3 oz Fontina cheese, cubed small enough to melt quickly\r\n \r\n  Heat the oil.  Add 2 cups of the broth\r\n  and stir.\r\n \r\nMMMMM\r\n`;
const unregistered = `---------- Recipe via UNREGISTERED Meal-Master (tm) v8.02\r\n \r\n      Title: A\r\n Categories: X\r\n      Yield: 1 servings\r\n \r\n      1 c  Water\r\n \r\n  Drink.\r\n \r\n-----\r\n---------- Recipe via UNREGISTERED Meal-Master (tm) v8.02\r\n \r\n      Title: B\r\n Categories: X\r\n      Yield: 1 servings\r\n \r\n      1 c  Tea\r\n \r\n  Sip.\r\n \r\n-----\r\n`;

test("the first form reads as it always did, and is written exactly as the retired tool wrote it", () => {
  const [r] = [...recipeRecords(first, "https://www.ffts.com/recipes/x.zip", "abc", "X.MMF", "x")];
  expect(r).toMatchObject({ title: "Carrot Cake", categories: ["cakes", "main dish"], servings: { raw: "12 servings", count: 12, unit: "servings" }, instructions: "Mix and bake." });
  expect(r.ingredients.map(i => i.raw)).toEqual(["1 c  Oil", "1 ts Baking soda", "1 1/2 c  Sugar"]);
  expect(r.substitution_candidates).toEqual([{ source_note: "* or use butter", status: "unverified_candidate" }]);
  expect(r.recipe_id).toMatch(/^ffts-x-0001-[0-9a-f]{16}$/);
  const line = pythonJson(r);
  expect(line.startsWith('{"categories": ["cakes", "main dish"], "ingredients": [{"name": "oil", "quantity": "1", "raw": "1 c  Oil", "unit": "c"}')).toBe(true);
  expect(line).toContain('"servings": {"count": 12.0, "raw": "12 servings", "unit": "servings"}');
  expect(pythonJson({ a: "café   🍰", count: null })).toBe('{"a": "caf\\u00e9 \\u2028 \\ud83c\\udf70", "count": null}');
});

test("the other forms open, end and label recipes differently and are still read one recipe at a time", () => {
  const m = [...recipeRecordsOtherForms(mmmmm, "u", "d", "M.MMF", "m")];
  expect(m.map(r => r.title)).toEqual(["London Broil", "Second One"]);
  expect(m[0].ingredients.map(i => i.raw)).toEqual(["1/3 c  Soy sauce", "2 tb Mustard", "1 c  Stock"]);     // the part heading is not an ingredient, and the part's own are kept
  expect(m[0].instructions).toBe("Marinate, then grill 2 hours before serving.");                              // a sentence starting with a number is not an ingredient
  expect(m[0].categories).toEqual(["beef", "grill"]); expect(m[0].servings.count).toBe(4);
  const [e] = [...recipeRecordsOtherForms(extracted, "u", "d", "E.MMF", "e")];
  expect(e).toMatchObject({ title: "Cold Clam Dip", categories: ["dips", "appetizers"], servings: { raw: "4 servings", count: 4, unit: "servings" } });
  expect(e.ingredients.map(i => i.name)).toEqual(["clams", "cream cheese"]);
  expect(e.instructions).toBe("Blend all ingredients together.  Let set for 2 hours before serving.");
  expect([...recipeRecordsOtherForms(unregistered, "u", "d", "U.MMF", "u")].map(r => r.title)).toEqual(["A", "B"]);
});

test("ingredients are read down each column, with continued lines kept on their ingredient", () => {
  const [r] = [...recipeRecordsOtherForms(columns, "u", "d", "C.MMF", "c")];
  expect(r.ingredients.map(i => i.raw)).toEqual(["4 oz Soft-spreadable cheese with garlic and herbs", "5 tb Butter, cut into pieces", "Salt and pepper", "1/4 c  White wine", "2    Whole chicken breasts, boned, halved", "12 md Mushrooms"]);
  expect(r.ingredients[0]).toMatchObject({ quantity: "4", unit: "oz", name: "soft-spreadable cheese with garlic and herbs" });
  expect(r.ingredients[2]).toEqual({ raw: "Salt and pepper", quantity: null, unit: null, name: "salt and pepper" });
  expect(r.instructions).toBe("Spread the cheese under the skin. 2 hours later, bake until done.");
});

test("a wide single column with colon lines is read as one list, and a part's name is a note", () => {
  const [r] = [...recipeRecordsOtherForms(colons, "u", "d", "K.MMF", "k")];
  expect(r.ingredients.map(i => i.raw)).toEqual(["1 TB extra virgin olive oil", "1 1/4 c  Arborio rice", "1 TB grated Grana Padano -- or Parmigiano Reggiano", "salt", "1    leek (white part only) -- thinly sliced", "3 oz Fontina cheese, cubed small enough to melt quickly"]);
  expect(r.ingredients[3]).toEqual({ raw: "salt", quantity: null, unit: null, name: "salt" });
  expect(r.instructions).toBe("Heat the oil.  Add 2 cups of the broth and stir.");
  expect(r.substitution_candidates).toEqual([]); expect(r.categories).toEqual(["meatless", "entree"]);
});

test("an archive is read whatever its members and forms, and a dead link is said to be one", () => {
  const dir = mkdtempSync(join(tmpdir(), "ffts-reader-"));
  try {
    writeFileSync(join(dir, "ONE.MMF"), Buffer.from(mmmmm, "latin1")); writeFileSync(join(dir, "FILE_ID.DIZ"), "notes, not recipes"); writeFileSync(join(dir, "TWO.TXT"), Buffer.from(extracted, "latin1"));
    expect(Bun.spawnSync(["zip", "-jq", join(dir, "both.zip"), join(dir, "ONE.MMF"), join(dir, "FILE_ID.DIZ"), join(dir, "TWO.TXT")]).exitCode).toBe(0);
    const read = archiveRecords(join(dir, "both.zip"), "https://www.ffts.com/recipes/ac/Both.zip");
    expect(read.records.map(r => r.title)).toEqual(["London Broil", "Second One", "Cold Clam Dip"]);
    expect(new Set(read.records.map(r => r.recipe_id.replace(/-\d{4}-[0-9a-f]{16}$/, "")))).toEqual(new Set(["ffts-both.one", "ffts-both.two"]));
    expect(read.records[0].source).toMatchObject({ archive_url: "https://www.ffts.com/recipes/ac/Both.zip", archive_member: "ONE.MMF", archive_sha256: read.digest });
    writeFileSync(join(dir, "dead.zip"), "<!DOCTYPE HTML><html>404</html>");
    expect(() => archiveRecords(join(dir, "dead.zip"), "https://www.ffts.com/recipes/dead.zip")).toThrow(/answers with a web page/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
  expect(catalogueArchives('<a href="recipes/a.zip">a</a> <a href="https://www.ffts.com/recipes/lg/b.zip">b</a> <a href="https://elsewhere.example/c.zip">c</a> <a href="recipes/a.zip">again</a>'))
    .toEqual(["https://www.ffts.com/recipes/a.zip", "https://www.ffts.com/recipes/lg/b.zip"]);
});
