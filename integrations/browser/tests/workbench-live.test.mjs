// The commerce workbench as an installed extension in a real browser: a page
// is read by the workbench's own capture function, becomes an observation in
// the browser's local store, is shown by the workbench page, feeds a compiled
// draft, and a requested comparison is handed to the real native host and held
// in the real queue. The one step no automation can take is the reader's click
// that grants the extension the tab; the capture function is therefore run in
// the page through the debugger, which is the same function the extension runs.
import { test, expect } from "bun:test";
import {loopbackServer} from './browser-fixture.mjs';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { withChrome } from "../capture/chrome-session.mjs";
import {openCollection,importExport} from "../../../cmd/BonfyreSourceFrontier/tools/traktok_collect.mjs";

const repo = join(import.meta.dir, "../../..");
const extension = join(repo, "integrations/browser/extension");
const project = join(repo, "components/BonfyreProject/bonfyre-project"), queue = join(repo, "cmd/BonfyreQueue/bonfyre-queue");
function testingBrowser() {
  if (process.env.BONFYRE_TEST_BROWSER) return process.env.BONFYRE_TEST_BROWSER;
  const cache = join(homedir(), ".cache/puppeteer/chrome");
  for (const version of existsSync(cache) ? readdirSync(cache).sort().reverse() : []) {
    const path = join(cache, version, "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing");
    if (existsSync(path)) return path;
  }
  return "";
}
const browser = testingBrowser();
const id = [...createHash("sha256").update(extension).digest("hex").slice(0, 32)].map(c => String.fromCharCode(97 + parseInt(c, 16))).join("");

// A shop page: a product with a listed price, visible terms, and a form whose
// contents must never be read.
const shop = `<!doctype html><html><head><title>Tin Club monthly box</title>
<script type="application/ld+json">{"@context":"https://schema.org","@type":"Product","name":"Tin Club monthly box","sku":"TC-1","offers":{"@type":"Offer","price":"45.00","priceCurrency":"USD"}}</script></head>
<body><h1>Tin Club</h1><p>Four tins each month. Cancel any time before the 20th; shipping is calculated at checkout.</p>
<p>Returns accepted on damaged tins only.</p><p>Our founders love sardines.</p>
<form><p>Renewal reminder email</p><input id="secret" value="card 4111 1111 1111 1111"></form>
<p style="visibility:hidden">Hidden subscription upsell: renew at double price.</p></body></html>`;

test.skipIf(!browser || !existsSync(project) || !existsSync(queue))("the workbench, installed, carries a page into an observation, a draft and held work", async () => {
  const root = mkdtempSync(join(tmpdir(), "bonfyre-workbench-live-"));
  const server = loopbackServer({ port: 0, hostname: "127.0.0.1", fetch: () => new Response(shop, { headers: { "content-type": "text/html; charset=utf-8" } }) });
  const before = process.env.BONFYRE_CHROME;
  process.env.BONFYRE_CHROME = browser;
  try {
    const frontier=join(repo,"cmd/BonfyreSourceFrontier/bonfyre-source-frontier"),frontierDb=join(root,"frontier.db"),bodies=join(root,"bodies");
    const init=Bun.spawnSync([frontier,"init","--db",frontierDb]);expect(init.exitCode).toBe(0);
    const recordFile=join(root,"retained.jsonl");
    writeFileSync(recordFile,JSON.stringify({id:"7350123456789012345",username:"fixture",video_description:"Previously collected preparation demonstration",voice_to_text:"Retained source transcript, not an inferred recipe",hashtag_names:["mealprep"]})+'\n');
    const collectionDb=openCollection(frontierDb);let collection;
    try{collection=(await importExport(collectionDb,{path:recordFile,bodyDir:bodies,sourceURI:"fixture:installed-browser-source"})).collection;}finally{collectionDb.close();}
    const launcher = join(root, "host.sh");
    writeFileSync(launcher, `#!/bin/sh\nexport PATH="${process.env.PATH}" HOME="${homedir()}" BONFYRE_FOREIGN_ROOT="${root}" BONFYRE_FOREIGN_RECEIPTS="${root}/receipts" BONFYRE_BROWSER_QUEUE_DB="${root}/held.db" BONFYRE_PROJECT_BINARY="${project}" BONFYRE_QUEUE_BINARY="${queue}" BONFYRE_SOURCE_FRONTIER_DB="${frontierDb}" BONFYRE_SOURCE_FRONTIER_BINARY="${frontier}" BONFYRE_TRAKTOK_BODIES="${bodies}" BONFYRE_TRAKTOK_COLLECTOR="${join(repo,"cmd/BonfyreSourceFrontier/tools/traktok_collect.mjs")}" BONFYRE_BUN_BINARY="${process.execPath}"\nexec "${process.execPath}" "${join(repo, "integrations/browser/host/bonfyre-browser-host.mjs")}"\n`);
    chmodSync(launcher, 0o755);
    const prepare = profile => {
      mkdirSync(join(profile, "NativeMessagingHosts"), { recursive: true });
      writeFileSync(join(profile, "NativeMessagingHosts/com.bonfyre.browser_host.json"), JSON.stringify({ name: "com.bonfyre.browser_host", description: "test", path: launcher, type: "stdio", allowed_origins: [`chrome-extension://${id}/`] }));
    };
    await withChrome(async ({ open }) => {
      const bench = await open();
      await bench.go(`chrome-extension://${id}/workbench.html`);
      const inBench = async expression => {
        const answer = await bench.page("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
        if (answer.exceptionDetails) throw new Error(answer.exceptionDetails.exception?.description ?? answer.exceptionDetails.text);
        return answer.result.value;
      };
      const until = async (expression, test, what) => { for (let i = 0; i < 200; i++) { const value = await inBench(expression).catch(() => null); if (test(value)) return value; await Bun.sleep(100); } throw new Error("the workbench never showed " + what + ": " + await inBench(expression).catch(String)); };
      // The catalogue the workbench offers is the one the native estate holds.
      expect(await until(`document.querySelector("#template").options.length`, n => n === 36, "the templates")).toBe(36);
      const captureSource = await inBench(`import("./capture.mjs").then(module => String(module.capturePage))`);

      // The reader's page, read by the extension's own capture function.
      const page = await open();
      await page.go(`http://127.0.0.1:${server.port}/tin-club`);
      const raw = (await page.page("Runtime.evaluate", { expression: `(${captureSource})()`, returnByValue: true })).result.value;
      expect(raw.title).toBe("Tin Club monthly box");
      expect(raw.structured).toHaveLength(1);
      expect(raw.term_snippets).toEqual(["Four tins each month. Cancel any time before the 20th; shipping is calculated at checkout.", "Returns accepted on damaged tins only."]);
      // Nothing from the form, nothing hidden, nothing that is not a term.
      expect(JSON.stringify(raw)).not.toContain("4111");
      expect(JSON.stringify(raw)).not.toContain("Renewal reminder");
      expect(JSON.stringify(raw)).not.toContain("double price");
      expect(JSON.stringify(raw)).not.toContain("founders");

      // Carried the way the panel carries it: observed, kept in the browser's own store, opened in the workbench.
      const carry = raw => `(async () => { const { observe } = await import("./observation.mjs"); const { saveLocal, readLocal } = await import("./store.mjs");
        const catalog = await (await fetch("./catalog.json")).json(); const observation = await observe(${JSON.stringify(raw)}, catalog);
        if (!(await readLocal("observation:")).find(row => row.key === observation.id)) await saveLocal(observation.id, observation, 0);
        return { id: observation.id, offers: observation.offers, authority: observation.authority }; })()`;
      const kept = await inBench(carry(raw));
      expect(kept.offers).toHaveLength(1);
      expect(kept.offers[0]).toMatchObject({ product_name: "Tin Club monthly box", price: { usd_cents: 4500, currency: "USD" }, shipping: "not_quoted", tax: "not_quoted", origin: "page_jsonld_unverified" });
      expect(kept.authority).toBe("observed_not_merchant_verified");
      await bench.go(`chrome-extension://${id}/workbench.html?capture=${encodeURIComponent(kept.id)}`);
      const shown = await until(`document.getElementById("capture-result").textContent`, text => text && text.includes("Tin Club"), "the captured page");
      expect(shown).toContain("Cancel any time before the 20th");

      // A draft compiled against that observation, by the same compiler the estate tests.
      await inBench(`document.getElementById("compile").click()`);
      const summary = await until(`document.getElementById("draft-output").textContent + document.getElementById("draft-summary").textContent + document.getElementById("error").textContent`, text => text && text.length > 20, "a compiled draft");
      expect(await inBench(`document.getElementById("error").textContent`)).toBe("");
      expect(summary).toContain("draft");

      // Work on a loopback page is refused before it reaches the host: the host carries public https only.
      await inBench(`document.getElementById("work-purpose").value = "compare with my current box"; document.getElementById("hold-work").click()`);
      expect(await until(`document.getElementById("error").textContent`, text => Boolean(text), "why it was not held")).toContain("HTTPS");
      expect(existsSync(join(root, "held.db"))).toBe(false);

      // A public page: captured, carried, and held in the real queue through the real host.
      await page.go("https://example.com/");
      const publicRaw = (await page.page("Runtime.evaluate", { expression: `(${captureSource})()`, returnByValue: true })).result.value;
      const publicKept = await inBench(carry(publicRaw));
      await bench.go(`chrome-extension://${id}/workbench.html?capture=${encodeURIComponent(publicKept.id)}`);
      await until(`document.getElementById("capture-result").textContent`, text => text && text.includes("Example Domain"), "the public page");
      expect(await inBench(`document.getElementById("hold-work").disabled`)).toBe(false);
      await inBench(`document.getElementById("work-purpose").value = "live test: compare this page"; document.getElementById("hold-work").click()`);
      const result = JSON.parse(await until(`document.getElementById("work-result").textContent`, text => text && text.includes("held_for_work"), "held work"));
      expect(result).toMatchObject({ state: "held_for_work", owner: "BonfyreQueue" });
      const node = JSON.parse(Bun.spawnSync([queue, "mission", result.mission, "--db", join(root, "held.db")]).stdout.toString());
      expect(node).toMatchObject({ family: "held-selection", status: "ready" });
      expect(JSON.parse(node.input_uri).url).toBe("https://example.com/");
      // Asked twice, held once.
      await inBench(`document.getElementById("hold-work").click()`);
      await Bun.sleep(1500);
      expect(Bun.spawnSync([queue, "depth", "--family", "held-selection", "--db", join(root, "held.db")]).stdout.toString()).toMatch(/\b1\b/);
      // The new source shapes render in the installed workbench; no second UI.
      const sourceRaw={url:'https://www.tiktok.com/@fixture/video/7350123456789012345',title:'Source material fixture',captured_at:'2026-10-04T00:00:00Z',fixture:true,
        platform_post:{status:'observed',id:'7350123456789012345',author:'fixture',description:'Ten variations from one base',duration_seconds:'42'},
        structured:[{'@type':'Recipe',name:'Source oat bites',recipeIngredient:['1 1/2 cups oats'],nutrition:{'@type':'NutritionInformation',servingSize:'one portion',proteinContent:'7 g'}}],term_snippets:[]};
      const sourceKept=await inBench(carry(sourceRaw));expect(sourceKept.offers).toHaveLength(0);
      await bench.go(`chrome-extension://${id}/workbench.html?capture=${encodeURIComponent(sourceKept.id)}`);
      const sourceShown=await until('document.getElementById("capture-result").textContent',text=>text?.includes('Source oat bites'),'source recipe and media');
      expect(sourceShown).toContain('proteinContent: 7 g');expect(sourceShown).toContain('serving basis: one portion');expect(sourceShown).toContain('fixture');
      const projection=await inBench(`(async()=>{const m=await import("./observation.mjs"),c=await(await fetch("./catalog.json")).json();return m.cmsProjection(await m.observe(${JSON.stringify(sourceRaw)},c)).content_type})()`);
      expect(projection).toBe('wiki_page');
      await inBench(`document.getElementById("collection-id").value=${JSON.stringify(collection)};document.getElementById("collected-url").value="https://www.tiktok.com/@fixture/video/7350123456789012345";document.getElementById("open-collected").click();`);
      const collected=await until('document.getElementById("capture-result").textContent',text=>text?.includes('Previously collected preparation demonstration'),'the retained native observation');
      expect(collected).toContain('Retained source transcript, not an inferred recipe');
      expect(collected).toContain('not a live-page read');
      expect(await inBench('document.getElementById("error").textContent')).toBe('');
      await inBench('document.getElementById("save-observation").click()');
      const retained=await until('document.getElementById("observations").textContent',text=>text?.includes('Previously collected preparation demonstration'),'saved collected observation');
      expect(retained).toContain('Previously collected preparation demonstration');
    }, { flags: [`--load-extension=${extension}`, `--disable-extensions-except=${extension}`], prepare, timeoutMs: 150_000 });
  } finally {
    if (before === undefined) delete process.env.BONFYRE_CHROME; else process.env.BONFYRE_CHROME = before;
    server.stop(true);
    rmSync(root, { recursive: true, force: true });
  }
}, 180_000);
