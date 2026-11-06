// The kit is tested by building different businesses from their records,
// serving each the way it is published (static files plus the site's own
// lead-keeping code), and using both in a real browser at desk and phone width.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { build, renderPage, renderSite, validate } from "../build.mjs";
import { contrast, resolveTheme } from "../theme.mjs";
import { withChrome } from "../../../integrations/browser/capture/chrome-session.mjs";

const repo = join(import.meta.dir, "../../..");
// The Fleming checks were written against Fleming's record as it stood on 2026-10-04 (three-step seller form,
// a buyers page with a sum). That record has since been rewritten from the client's own brief and is checked by
// completion.test.mjs. These run only when SERVICE_SITE_EARLIER_FLEMING names a folder holding that earlier record.
const fleming = process.env.SERVICE_SITE_EARLIER_FLEMING ?? join(repo, "packs/fleming-equity-group/site/fixtures/none");
const sample = join(import.meta.dir, "fixtures/sample-lawn");
const record = dir => JSON.parse(readFileSync(join(dir, "content.json"), "utf8"));
const work = mkdtempSync(join(tmpdir(), "service-site-test-"));
// The Fleming pack is private and absent from a public checkout; its tests run where it is.
const here = existsSync(join(fleming, "content.json")), withFleming = here ? test : test.skip;
const served = {};

// One site at the root of its own port: files from its build, /api/lead from its own _worker.js.
async function serve(name, dir) {
  const out = join(work, name), built = await build(dir, out), kept = new Map();
  const counts = new Map();
  const env = { LEADS: { get: async key => counts.get(key) ?? null, put: async (key, value, options) => { if (key.startsWith("count:")) counts.set(key, value); else kept.set(key, { value: JSON.parse(value), options }); } },
    ASSETS: { fetch: async request => { const path = decodeURIComponent(new URL(request.url).pathname); const file = Bun.file(join(out, path.endsWith("/") ? path + "index.html" : path));
      return await file.exists() ? new Response(file) : new Response(Bun.file(join(out, "404.html")), { status: 404, headers: { "content-type": "text/html" } }); } } };
  const worker = existsSync(join(out, "_worker.js")) ? (await import(join(out, "_worker.js"))).default : { fetch: (request, e) => e.ASSETS.fetch(request) };
  const server = Bun.serve({ port: 0, fetch: request => worker.fetch(request, env) });
  served[name] = { server, kept, counts, env, built, origin: `http://127.0.0.1:${server.port}`, worker };
}
beforeAll(async () => { if (here) await serve("fleming", fleming); await serve("sample", sample); }, 240_000);
afterAll(() => { for (const s of Object.values(served)) s.server.stop(true); rmSync(work, { recursive: true, force: true }); });

// Open a page and report what a visitor's browser ended up with; `then` runs in the page afterwards.
const visit = (name, path = "/", { width = 1280, then = "null" } = {}) => withChrome(async ({ open, events }) => {
  const { page, go, sessionId } = await open();
  await page("Emulation.setDeviceMetricsOverride", { width, height: 800, deviceScaleFactor: 1, mobile: width < 600 });
  if (width < 600) await page("Emulation.setTouchEmulationEnabled", { enabled: true });
  await go(served[name].origin + path);
  await page("Runtime.evaluate", { awaitPromise: true, expression: "document.fonts.ready.then(() => 1)" });
  const { result } = await page("Runtime.evaluate", { returnByValue: true, expression: `(() => {
    const style = (sel, prop) => { const e = document.querySelector(sel); return e ? getComputedStyle(e)[prop] : null; };
    const vw = document.documentElement.clientWidth, visible = e => { const r = e.getBoundingClientRect(); return r.width > 1 && r.height > 1 && getComputedStyle(e).visibility !== "hidden"; };
    const hrefs = [...document.querySelectorAll("a[href]")].map(a => a.getAttribute("href"));
    return {
      sections: [...document.querySelectorAll("main > section")].map(s => s.id || "-"),
      menu: [...document.querySelectorAll("header > div > nav a")].map(a => a.textContent.trim()),
      menuShown: visible(document.querySelector("header > div > nav")), buttonShown: !!document.querySelector("[data-menu] summary") && visible(document.querySelector("[data-menu] summary")),
      dangling: hrefs.filter(href => href === "#" || (href.startsWith("#") && !document.getElementById(href.slice(1)))),
      local: [...new Set(hrefs.filter(href => href.startsWith("/")))],
      h1: document.querySelector("h1").innerText, h1Colour: style("h1", "color"), ground: style("body", "backgroundColor"), button: style("form.ask-form [type=submit]", "backgroundColor"), face: style("h1", "fontFamily"),
      overflow: document.documentElement.scrollWidth > vw,
      offScreen: [...document.querySelectorAll("main *, footer *, header *")].filter(e => visible(e) && !e.closest(".sr-only") && e.getBoundingClientRect().right > vw + 1).map(e => e.tagName + " " + (e.innerText || "").slice(0, 30)),
      smallTargets: [...document.querySelectorAll("a, button, summary, input:not([type=radio]), label:has(input[type=radio])")].filter(e => visible(e) && !e.closest(".sr-only, aside, p") && (e.getBoundingClientRect().height < 44)).map(e => (e.innerText || e.placeholder || e.tagName).trim().slice(0, 30) + " " + Math.round(e.getBoundingClientRect().height)),
      tinyText: [...document.querySelectorAll("body *")].filter(e => visible(e) && e.children.length === 0 && e.innerText?.trim() && parseFloat(getComputedStyle(e).fontSize) < 12).map(e => e.innerText.trim().slice(0, 30)),
      zoomingInputs: [...document.querySelectorAll("input:not([type=radio])")].filter(e => visible(e) && parseFloat(getComputedStyle(e).fontSize) < 16).length,
      images: [...document.images].map(i => [i.getAttribute("src"), i.naturalWidth]), scripts: [...document.scripts].filter(s => s.src).map(s => s.src),
      text: document.querySelector("main").innerText, note: document.querySelector("aside")?.innerText ?? "", title: document.title,
      template: document.documentElement.dataset.template ?? null, robots: document.querySelector('meta[name=robots]').content,
      labelled: [...document.querySelectorAll("form.ask-form input[data-label]")].every(i => i.labels.length === 1),
    };
  })()` });
  const after = await page("Runtime.evaluate", { returnByValue: true, awaitPromise: true, expression: `(async () => { ${then} })()` });
  if (after.exceptionDetails) throw new Error(after.exceptionDetails.exception?.description ?? after.exceptionDetails.text);
  const mine = events.filter(event => event.sessionId === sessionId);
  return { ...result.value, after: after.result.value,
    failed: mine.filter(event => event.method === "Network.loadingFailed").map(event => event.params.errorText),
    missing: mine.filter(event => event.method === "Network.responseReceived" && event.params.response.status >= 400).map(event => event.params.response.url),
    errors: mine.filter(event => event.method === "Runtime.exceptionThrown").map(event => event.params.exceptionDetails?.exception?.description ?? event.params.exceptionDetails?.text) };
}, { timeoutMs: 60_000 });
const clean = seen => { expect(seen.errors).toEqual([]); expect(seen.failed).toEqual([]); expect(seen.missing).toEqual([]); expect(seen.dangling).toEqual([]); expect(seen.scripts).toEqual([]); expect(seen.images.every(([, w]) => w > 0)).toBe(true); };
const phone = seen => { expect(seen.overflow).toBe(false); expect(seen.offScreen).toEqual([]); expect(seen.smallTargets).toEqual([]); expect(seen.tinyText).toEqual([]); expect(seen.zoomingInputs).toBe(0); expect(seen.menuShown).toBe(false); expect(seen.buttonShown).toBe(true); };

// In-page helpers used by the scripted visits below.
const helpers = `
  const wait = ms => new Promise(done => setTimeout(done, ms));
  let mailed = null; document.addEventListener("click", event => { const a = event.target.closest?.("a[href^='mailto:']"); if (a) { mailed = a.href; event.preventDefault(); } }, true);
  const form = document.querySelector("form.ask-form"), box = form.parentNode;
  const type = (label, value) => { form.querySelector('input[data-label="' + label + '"]').value = value; };
  const pick = (label, value) => { form.querySelector('fieldset[data-label="' + label + '"] input[value="' + value + '"]').click(); };
  const shown = sel => !!box.querySelector(sel) && !box.querySelector(sel).classList.contains("hidden");
  const step = () => [...form.querySelectorAll("fieldset[data-step]")].findIndex(s => !s.classList.contains("hidden"));
`;

withFleming("Fleming's home page loads whole at desk width and says what the record says", async () => {
  const seen = await visit("fleming");
  clean(seen);
  expect(seen.sections).toEqual(["-", "-", "how-it-works", "compare", "situations", "about", "-"]);
  expect(seen.menu).toEqual(["How It Works", "Compare Options", "Situations We Help", "About Bryston", "For buyers"]);
  expect(seen.menuShown).toBe(true); expect(seen.buttonShown).toBe(false);
  expect(seen.local.sort()).toEqual(["/", "/buyers/"]);
  expect(seen.text + seen.note).not.toMatch(/Tarrant|Fort Worth|DFW|Collin|Denton/);
  expect(seen.h1Colour).toBe("rgb(255, 255, 255)"); expect(seen.ground).toBe("rgb(10, 14, 22)"); expect(seen.button).toBe("rgb(16, 185, 129)");
  expect(seen.face).toContain("Plus Jakarta Sans");
  expect(seen.text).toContain("(214) 900-5577"); expect(seen.text).not.toContain("What sellers say");
  expect(seen.robots).toBe("noindex, nofollow"); expect(seen.note).toContain("Not yet public"); expect(seen.labelled).toBe(true);
}, 90_000);

withFleming("a seller walks the three steps on a phone and the request is kept by the site's own server code", async () => {
  served.fleming.kept.clear();
  const seen = await visit("fleming", "/", { width: 390, then: helpers + `
    const trail = [step()];
    form.querySelector("[data-next]").click(); trail.push(step());            // nothing typed: stays on step one
    type("Property Address", "1 Test Street, Dallas"); form.querySelector("[data-next]").click(); trail.push(step());
    form.querySelector("[data-next]").click(); trail.push(step());            // no condition picked: stays on step two
    pick("Condition", "Needs updating"); form.querySelector("[data-next]").click(); trail.push(step());
    form.querySelector("[data-back]").click(); trail.push(step()); form.querySelector("[data-next]").click();
    pick("How soon", "Within a month"); type("Phone Number", "555 010 0199");
    form.requestSubmit(); await wait(600);
    return { trail, sent: shown(".ask-sent"), mailedNote: shown(".ask-mailed"), mailed, formGone: form.classList.contains("hidden"), words: box.querySelector(".ask-sent").innerText.trim() };` });
  clean(seen); phone(seen);
  expect(seen.after.trail).toEqual([0, 0, 1, 1, 2, 1]);
  expect(seen.after).toMatchObject({ sent: true, mailedNote: false, mailed: null, formGone: true, words: "check_circle\nWe have your details. Bryston will call you about the property." });
  const kept = [...served.fleming.kept.values()];
  expect(kept.length).toBe(1);
  expect(kept[0].value).toMatchObject({ site_id: "fleming-equity-group", form: "seller", page: "/", fields: [
    { label: "Property Address", value: "1 Test Street, Dallas" }, { label: "Condition", value: "Needs updating" }, { label: "How soon", value: "Within a month" }, { label: "Phone Number", value: "555 010 0199" }] });
  expect(Object.keys(kept[0].value).sort()).toEqual(["fields", "form", "page", "received_at", "site_id"]);   // nothing about the visitor beyond what they typed
  expect(kept[0].options.expirationTtl).toBeGreaterThan(0);
}, 90_000);

withFleming("if the server cannot keep a request the visitor's mail app gets it instead, and the page says which happened", async () => {
  const leads = served.fleming.env.LEADS; delete served.fleming.env.LEADS;
  try {
    const seen = await visit("fleming", "/buyers/", { width: 390, then: helpers + `
      type("Your Name", "Pat Buyer"); type("Phone Number", "555 010 0142"); type("Email Address", "pat@example.org"); pick("What you buy", "Land and lots");
      form.requestSubmit(); await wait(600);
      return { sent: shown(".ask-sent"), mailedNote: shown(".ask-mailed"), mailed };` });
    clean({ ...seen, missing: seen.missing.filter(url => !url.endsWith("/api/lead")) }); phone(seen);
    expect(seen.after.sent).toBe(false); expect(seen.after.mailedNote).toBe(true);
    const mail = new URL(seen.after.mailed);
    expect(mail.pathname).toBe("bryston@flemingequitygroup.com"); expect(mail.searchParams.get("subject")).toBe("Buyers list: Pat Buyer");
    expect(mail.searchParams.get("body")).toBe("Your Name: Pat Buyer\nPhone Number: 555 010 0142\nEmail Address: pat@example.org\nWhat you buy: Land and lots\n\nPlease add me to the buyers list.");
  } finally { served.fleming.env.LEADS = leads; }
}, 90_000);

withFleming("the buyers page shows no property that is not on file, and its sum is the visitor's own arithmetic", async () => {
  const seen = await visit("fleming", "/buyers/", { then: `
    const set = (k, v) => { const e = document.querySelector("[data-sum=" + k + "]"); e.value = v; e.dispatchEvent(new Event("input", { bubbles: true })); };
    const out = () => document.querySelector("[data-sum-out]").textContent;
    const before = out(); set("value", "300000"); set("repairs", "40000"); const seventy = out(); set("share", "65"); const sixtyFive = out(); set("repairs", "900000");
    return { before, seventy, sixtyFive, never_negative: out() };` });
  clean(seen);
  expect(seen.sections).toEqual(["-", "sum", "-"]);                 // the listings section is absent: none on file
  expect(seen.menu).toEqual(["Sell a house", "Work a deal"]);
  expect(seen.text).not.toMatch(/Available now|ZIP|Lockbox|Spread/);
  expect(seen.after).toEqual({ before: "—", seventy: "$170,000", sixtyFive: "$155,000", never_negative: "$0" });
  expect(seen.title).toBe("For cash buyers | Fleming Equity Group LLC");
  // With a property on file the section appears, and one without a price does not.
  const withOne = structuredClone(record(fleming));   withOne.pages[0].sections[1].items = [{ name: "12 Example St", place: "Example · Dallas", price: 180000, after_repair: 300000, repairs: 40000 }, { name: "No price given", place: "Nowhere" }];
  const html = renderSite(withOne).pages.find(p => p.path === "buyers").html;
  expect(html).toContain("12 Example St"); expect(html).toContain("$80,000"); expect(html).not.toContain("No price given");
}, 90_000);

test("figures are shown only with their source and the day read, and read well on a phone", async () => {
  const dir = mkdtempSync(join(tmpdir(), "service-site-figures-"));
  try {
    const c = record(sample);
    c.pages = [{ path: "numbers", title: "Numbers", menu: "Numbers", sections: [{ kind: "hero", headline: "Numbers", button: "Ask" },
      { kind: "figures", id: "roll", menu: "The roll", title: "From the roll", material: "roll" }, { kind: "call", title: "Ask" }] }];
    writeFileSync(join(dir, "content.json"), JSON.stringify(c));
    writeFileSync(join(dir, "material.mjs"), `export const roll = () => ({ columns: ["Place", "Median value", "Counted"], rows: [["Exampleville", "$123,456", "1,000"], ["Sampleton", "$234,567", "2,000"]], source: { name: "Example County roll", observed: "2026-01-02" }, caveat: "Appraised values, not sale prices." });`);
    await serve("figures", dir);
    for (const width of [1280, 390]) {
      const seen = await visit("figures", "/numbers/", { width });
      clean(seen); if (width < 600) phone(seen);
      expect(seen.sections).toEqual(["-", "roll", "-"]);
      expect(seen.text).toContain("Exampleville"); expect(seen.text).toContain("$234,567");
      expect(seen.text).toContain("Source: Example County roll, read 2026-01-02. Appraised values, not sale prices.");
      expect(seen.local).toContain("/#ask");                                         // a page with no form sends the ask home
    }
    const noSource = structuredClone(c), figures = noSource.pages[0].sections[1]; delete figures.material; figures.columns = ["City", "Value"]; figures.rows = [["Somewhere", "$1"]];
    expect(renderSite(noSource).pages.some(p => p.path === "numbers")).toBe(false);   // numbers without a source are not published, nor a page left empty by it
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 240_000);

withFleming("every statement on Fleming's home page comes from the record", async () => {
  const content = record(fleming), words = [];
  (function walk(v) { if (typeof v === "string" && v.length > 1) words.push(v.replace(/\*/g, "")); else if (v && typeof v === "object") Object.values(v).forEach(walk); })(content);
  let left = (await visit("fleming")).text;
  for (const w of words.sort((a, b) => b.length - a.length)) left = left.split(w).join(" ").split(w.toUpperCase()).join(" ");
  left = left.replace(/\(214\) 900-5577|Call|Direct Line:|\bBF\b|Step \d of \d|\b\d\.|Next|NEXT|Back|BACK|Leave this empty|\(optional\)|Your mail app has opened with your details\. Press send and it comes straight to us\.|[→•“”]|\b0[1-9]\b|\b[a-z_]+\b/g, " ").replace(/\s+/g, " ").trim();
  expect(left).toBe("");
}, 90_000);

test("a second business in another look is another record and nothing else, and reads well on a phone", async () => {
  const seen = await visit("sample");
  clean(seen);
  expect(seen.sections).toEqual(["-", "work", "how", "reviews", "about", "-"]);
  expect(seen.menu).toEqual(["What we do", "How it goes", "Reviews", "About Sam"]);
  expect(seen.template).toBe("ghost-ease");
  expect(seen.ground).toBe("rgb(252, 252, 253)"); expect(seen.h1Colour).toBe("rgb(13, 17, 23)"); expect(seen.button).toBe("rgb(7, 87, 186)");
  expect(seen.face).toContain("IBM Plex Sans");
  expect(seen.text).toContain("Showed up when they said."); expect(seen.text).not.toContain("unsigned praise");
  expect(seen.text).not.toMatch(/Fleming|Dallas|DFW|cash offer/i);
  expect(existsSync(join(work, "sample/_worker.js"))).toBe(false);          // a site that sends by mail ships no server code
  const small = await visit("sample", "/", { width: 390, then: helpers + `
    const menu = document.querySelector("[data-menu]"); menu.querySelector("summary").click(); const opened = menu.open, items = [...menu.querySelectorAll("a")].map(a => a.textContent.trim());
    menu.querySelector('a[href="#how"]').click(); await wait(50); const closedAfter = !menu.open;
    type("Street address", "1 Test Street"); type("Phone", "555 010 0199"); type("Anything we should know", "side gate"); form.requestSubmit(); await wait(200);
    return { opened, items, closedAfter, mailed, mailedNote: shown(".ask-mailed"), sent: shown(".ask-sent") };` });
  clean(small); phone(small);
  expect(small.after).toMatchObject({ opened: true, items: ["What we do", "How it goes", "Reviews", "About Sam"], closedAfter: true, mailedNote: true, sent: false });
  expect(new URL(small.after.mailed).searchParams.get("body")).toBe("Street address: 1 Test Street\nPhone: 555 010 0199\nAnything we should know: side gate");
  for (const width of [320, 768]) { const at = await visit("sample", "/", { width }); expect(at.overflow).toBe(false); expect(at.offScreen).toEqual([]); }
}, 180_000);

withFleming("Fleming's home page fits a small phone and a tablet", async () => {
  for (const width of [320, 768, 1024]) { const at = await visit("fleming", "/", { width }); clean(at); expect(at.overflow).toBe(false); expect(at.offScreen).toEqual([]); if (width < 600) phone(at); }
}, 180_000);

withFleming("the site's server code keeps only what one of its own forms asks for", async () => {
  const { worker, env, kept, origin } = served.fleming, good = { form: "seller", page: "/", fields: [{ label: "Property Address", value: "9 Elm" }, { label: "Condition", value: "Ready to live in" }, { label: "How soon", value: "No rush" }, { label: "Phone Number", value: "5550100" }] };
  served.fleming.counts.clear();
  const post = (body, headers = {}) => worker.fetch(new Request(origin + "/api/lead", { method: "POST", headers: { origin, "content-type": "application/json", ...headers }, body: typeof body === "string" ? body : JSON.stringify(body) }), env).then(r => r.status);
  const change = edit => { const b = structuredClone(good); edit(b); return b; };
  kept.clear();
  expect(await post(good)).toBe(201); expect(kept.size).toBe(1);
  expect(await post(good, { origin: "https://elsewhere.example" })).toBe(403);
  expect(await post(good, { "content-type": "text/plain" })).toBe(415);
  expect(await post("{not json")).toBe(400);
  expect(await post(change(b => { b.form = "newsletter"; }))).toBe(400);
  expect(await post(change(b => { b.fields.push({ label: "Notes", value: "x" }); }))).toBe(400);          // not a field of this form
  expect(await post(change(b => { b.fields[1].value = "Free money"; }))).toBe(400);                        // not one of the stated choices
  expect(await post(change(b => { b.fields.pop(); }))).toBe(400);                                          // no phone: nobody could answer
  expect(await post(change(b => { b.fields.push(b.fields[0]); }))).toBe(400);
  expect(await post(change(b => { b.fields[0].value = "x".repeat(9000); }))).toBe(413);
  expect(kept.size).toBe(1);
  expect(await post(change(b => { b.company_website = "http://spam.example"; }))).toBe(201); expect(kept.size).toBe(1);   // the trap: told yes, nothing kept
  // One sender is answered five times an hour and then told to stop; another sender is still answered; the site stops at its daily limit.
  const { counts } = served.fleming; counts.clear(); kept.clear();
  const from = ip => post(good, { "cf-connecting-ip": ip });
  expect([await from("203.0.113.9"), await from("203.0.113.9"), await from("203.0.113.9"), await from("203.0.113.9"), await from("203.0.113.9"), await from("203.0.113.9")]).toEqual([201, 201, 201, 201, 201, 429]);
  expect(await from("203.0.113.10")).toBe(201); expect(kept.size).toBe(6);
  expect([...counts.keys()].join(" ")).not.toContain("203.0.113");                       // the address itself is never a key
  expect(JSON.stringify([...kept.values()])).not.toContain("203.0.113");                  // nor anywhere in a kept request
  counts.set([...counts.keys()].find(k => k.startsWith("count:day:")), "300");
  expect(await from("203.0.113.11")).toBe(429); expect(kept.size).toBe(6);
  counts.clear();
  expect((await worker.fetch(new Request(origin + "/api/lead"), env)).status).toBe(405);
  expect((await worker.fetch(new Request(origin + "/buyers/"), env)).status).toBe(200);
});

test("a large collection is searched, browsed and read on a phone while fetching only small pieces of it", async () => {
  const dir = mkdtempSync(join(tmpdir(), "service-site-library-"));
  try {
    const c = record(sample); c.contact = {}; c.leads = "server"; delete c.footer;
    c.sections = [c.sections[0], { kind: "library", id: "shelf", menu: "Search", title: "Find one", material: "shelf", meter: true },
      { kind: "amounts", id: "gather", menu: "Amounts", title: "What to gather", material: "lists" }, { kind: "call", title: "Ask" }];
    c.sections[0].form.fields = [{ label: "Email", type: "email", wide: true }];
    writeFileSync(join(dir, "content.json"), JSON.stringify(c));
    // 1,000 made-up entries: 700 soups (so the kind runs past two pages) and 300 pies; one with markup in its name.
    writeFileSync(join(dir, "material.mjs"), `import { libraryFiles } from ${JSON.stringify(join(import.meta.dir, "../library.mjs"))};
      const entries = Array.from({ length: 1000 }, (_, n) => ({ id: "x-" + n, title: n === 7 ? "<b>Bold</b> Barley Soup 7" : (n < 700 ? "Barley Soup " : "Quince Pie ") + n, kinds: [n < 700 ? "soups" : "pies", ...(n === 3 ? ["stray"] : [])], serves: "4 servings",
        parts: ["1 c barley " + n, "2 c water"], method: n === 999 ? "" : "Simmer " + n + ".", notes: n === 705 ? ["* or use pears"] : [], from: "From the made-up shelf.",
        ways: n === 999 ? [["With <i>plums</i>", ["1 cup plums", "2 tsp spice"], "Fold in and bake.", "Serves 8"], ["The plain base", [], "Bake.", ""]] : [] }));
      export const shelf = async () => { const lib = await libraryFiles(entries); return { count: lib.count, files: lib.files, noun: "recipes" }; };
      export const lists = () => ({ choices: [{ label: "One", rows: [["Oats", "1/3 cup"], ["Syrup", "3/4 cup"]] }, { label: "Two", rows: [["Oats", "2/3 cup"], ["Syrup", "1 1/2 cups"]] }] });`);
    await serve("library", dir);
    const built = served.library.built;
    expect(built.data_files).toBeGreaterThan(20); expect(built.phone).toBe(null); expect(built.email).toBe(null);
    const seen = await visit("library", "/", { width: 390, then: `
      const wait = ms => new Promise(d => setTimeout(d, ms)), box = document.querySelector("[data-library]"), q = s => box.querySelector(s), rows = () => [...q("[data-lib-list]").children].map(li => li.textContent);
      const until = async test => { for (let i = 0; i < 80 && !test(); i++) await wait(50); return test(); };
      const sizes = () => performance.getEntriesByType("resource").filter(r => r.name.includes("/data/shelf/")).map(r => [r.name.split("/data/shelf/")[1], r.encodedBodySize || r.transferSize]);
      await until(() => q("[data-lib-chips]").children.length > 0);
      const out = { chips: [...q("[data-lib-chips]").children].map(b => b.textContent), atStart: sizes().map(s => s[0]), fetchedAtStart: q("[data-lib-fetched]").textContent };
      const input = q("input[type=search]"); const type = async v => { input.value = v; input.dispatchEvent(new Event("input", { bubbles: true })); await wait(450); };
      await type("qu"); out.short = q("[data-lib-status]").textContent;
      await type("quince 70"); await until(() => rows().length > 0); out.search = { status: q("[data-lib-status]").textContent, first: rows()[0], count: rows().length, all: rows().every(t => /^Quince Pie 70/.test(t)) };
      await type("zzzzz"); out.none = q("[data-lib-status]").textContent;
      await type("bold barley"); await until(() => rows().length === 1); out.markup = { text: rows()[0], injected: !!q("[data-lib-list] b") };
      [...q("[data-lib-chips]").children].find(b => b.textContent.startsWith("soups")).click(); await until(() => rows().length === 40);
      out.kind = { status: q("[data-lib-status]").textContent, shown: rows().length };
      const more = q("[data-lib-more]"); let clicks = 0; while (!more.classList.contains("hidden") && clicks < 40) { more.click(); clicks++; await wait(120); }
      out.kindAll = { shown: rows().length, clicks, last: rows().at(-1), status: q("[data-lib-status]").textContent };
      [...q("[data-lib-chips]").children].find(b => b.textContent.startsWith("pies")).click(); await until(() => rows().length === 40 && rows()[0].startsWith("Quince"));
      [...box.querySelectorAll("[data-lib-list] a")].find(a => a.textContent === "Quince Pie 705").click(); await until(() => !q("[data-lib-entry]").classList.contains("hidden"));
      out.entry = { text: q("[data-lib-entry]").innerText, hash: location.hash, listHidden: q("[data-lib-list]").classList.contains("hidden") };
      q("[data-lib-entry] button").click(); out.back = { entryHidden: q("[data-lib-entry]").classList.contains("hidden"), rows: rows().length, hash: location.hash };
      location.hash = "#shelf=999"; await until(() => !q("[data-lib-entry]").classList.contains("hidden") && q("[data-lib-entry] h3").textContent.endsWith("999")); out.deep = q("[data-lib-entry] h3").textContent;
      out.family = { text: q("[data-lib-entry]").innerText.replace(/\\s+/g, " "), blocks: q("[data-lib-entry]").querySelectorAll("details").length, firstOpen: q("[data-lib-entry] details").open, injected: !!q("[data-lib-entry] i") };
      out.pieces = sizes(); out.fetched = q("[data-lib-fetched]").textContent;
      const lists = [...document.querySelectorAll("[data-amounts-list]")], shownList = () => lists.findIndex(l => !l.classList.contains("hidden"));
      out.amounts = [shownList(), lists[0].innerText.replace(/\\s+/g, " ").trim()]; document.querySelector('[data-amounts] input[value="1"]').click(); out.amounts.push(shownList(), lists[1].innerText.replace(/\\s+/g, " ").trim());
      return out;` });
    expect(seen.errors).toEqual([]); expect(seen.failed).toEqual([]); phone(seen);
    const a = seen.after;
    expect(a.chips).toEqual(["soups 700", "pies 300"]);                               // a kind held by one entry is not offered
    expect(a.atStart).toEqual(["about.json"]);                                        // nothing of the collection is fetched until asked
    expect(a.short).toBe("Type at least three letters.");
    expect(a.search).toMatchObject({ first: "Quince Pie 700", count: 10, all: true }); expect(a.search.status).toBe("10 matching “quince 70”");
    expect(a.none).toBe("Nothing matches. Try another word.");
    expect(a.markup).toEqual({ text: "<b>Bold</b> Barley Soup 7", injected: false });  // a title is text, never markup
    expect(a.kind).toEqual({ status: "300 of 700 in “soups”", shown: 40 });
    expect(a.kindAll.shown).toBe(700); expect(a.kindAll.status).toBe("700 of 700 in “soups”");
    expect(a.entry.text).toContain("Quince Pie 705"); expect(a.entry.text).toContain("1 c barley 705"); expect(a.entry.text).toContain("Simmer 705."); expect(a.entry.text).toContain("* or use pears"); expect(a.entry.text).toContain("From the made-up shelf.");
    expect(a.entry).toMatchObject({ hash: "#shelf=705", listHidden: true }); expect(a.back).toEqual({ entryHidden: true, rows: 40, hash: "" });
    expect(a.deep).toBe("Quince Pie 999");
    // A family shows its base once and each way as what that way adds.
    expect(a.family).toMatchObject({ blocks: 2, firstOpen: true, injected: false });
    expect(a.family.text).toContain("THE BASE, IN EVERY ONE 1 c barley 999 2 c water"); expect(a.family.text).toContain("2 WAYS TO FINISH IT With <i>plums</i> adds 2 + 1 cup plums + 2 tsp spice Serves 8 Fold in and bake."); expect(a.family.text).toContain("The plain base nothing added");
    // Reading all that took a handful of pieces, each small, and the page says how much.
    expect(a.pieces.length).toBeLessThan(14); expect(Math.max(...a.pieces.map(p => p[1]))).toBeLessThan(12000);
    expect(a.fetched).toMatch(/^All 1,000 recipes are here to search\. To show you this, your device has fetched \d+ KB in \d+ pieces\.$/);
    expect(a.amounts).toEqual([0, "What to gather: One Ingredient Amount Oats 1/3 cup Syrup 3/4 cup", 1, "What to gather: Two Ingredient Amount Oats 2/3 cup Syrup 1 1/2 cups"]);
    // No phone or email on record: none is shown, and a request the server cannot keep is said to have failed rather than sent anywhere.
    expect(seen.text + seen.note).not.toMatch(/Call|\(\d{3}\) \d{3}/); expect(seen.local.some(href => href.startsWith("tel:"))).toBe(false);
    const leads = served.library.env.LEADS; delete served.library.env.LEADS;
    const failed = await visit("library", "/", { width: 390, then: helpers + `type("Email", "a@example.org"); form.requestSubmit(); await wait(600); return { sent: shown(".ask-sent"), mailed: shown(".ask-mailed"), failed: shown(".ask-failed"), link: mailed, formStill: !form.classList.contains("hidden") };` });
    served.library.env.LEADS = leads;
    expect(failed.after).toEqual({ sent: false, mailed: false, failed: true, link: null, formStill: true });
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 300_000);

test("an address the site does not have says so in the site's own look and offers the way back", async () => {
  const seen = await visit("sample", "/no-such-page/", { width: 390 });
  expect(seen.missing.length).toBe(1); expect(seen.errors).toEqual([]); expect(seen.dangling).toEqual([]);
  expect(seen.h1).toBe("That page is not here"); expect(seen.local).toContain("/"); expect(seen.overflow).toBe(false);
  expect(seen.ground).toBe("rgb(252, 252, 253)");
}, 90_000);

test("a record that would make a broken or unreadable page is refused by name", () => {
  const good = record(sample), broken = change => { const c = structuredClone(good); change(c); return () => renderPage(c); };
  expect(broken(c => { c.sections[1].kind = "carousel"; })).toThrow(/kind "carousel"/);
  expect(broken(c => { delete c.sections[2].items; })).toThrow(/steps\) needs items/);
  expect(broken(c => { c.sections.reverse(); })).toThrow(/first section must be the hero/);
  expect(broken(c => { c.sections[2].id = "work"; })).toThrow(/repeated/);
  expect(broken(c => { c.contact.phone = "555-0100"; })).toThrow(/ten-digit/);
  expect(broken(c => { c.contact.email = 'a@b.co"+alert(1)+"'; })).toThrow(/not an email/);
  expect(broken(c => { delete c.contact.email; })).toThrow(/email is required when requests are sent by mail/);
  expect(broken(c => { delete c.contact.name; })).toThrow(/person section needs contact.name/);
  expect(broken(c => { c.theme = { template: "ghost-nowhere" }; })).toThrow(/not an authored presentation template/);
  expect(broken(c => { c.theme = { ground: "#ffffff", ink: "#eeeeee", display: "#f4f4f4", accent: "#fafafa", muted: "#dddddd" }; })).toThrow(/not readable/);
  expect(broken(c => { delete c.sections[0].form; })).toThrow(/home page needs the request form/);
  expect(broken(c => { c.sections.push({ kind: "ask", form: { ...c.sections[0].form, id: "second" } }); })).toThrow(/a page has one request form/);
  expect(broken(c => { const form = c.sections[0].form; delete c.sections[0].form; c.sections.push({ kind: "ask", title: "Ask", form }); })().html).toContain('id="ask"');
  expect(broken(c => { c.sections[0].form.fields = [{ label: "Name" }]; })).toThrow(/must ask for a phone or an email/);
  expect(broken(c => { c.sections[0].form.fields[0].type = "choice"; })).toThrow(/at least two options/);
  expect(broken(c => { c.pages = [{ path: "api", title: "x", menu: "x", sections: [{ kind: "hero", headline: "x" }] }]; })).toThrow(/path must be/);
  expect(broken(c => { c.pages = [{ path: "more", title: "x", menu: "x", sections: [{ kind: "hero", headline: "x", form: { ...c.sections[0].form } }] }]; })).toThrow(/used on another page/);
  expect(broken(c => { c.leads = "fax"; })).toThrow(/"mail" or "server"/);
  expect(broken(c => { c.sections[0].headline = "<img src=x onerror=alert(1)>"; })().html).not.toContain("<img src=x");
  // A door sends a search to a room elsewhere over https and keeps nothing itself; any other address is refused.
  const door = broken(c => { c.sections.push({ kind: "door", id: "find", title: "Find", action: "https://rooms.example/recipes", placeholder: "soup", links: [{ label: "Plan", href: "https://rooms.example/plan" }] }); })().html;
  expect(door).toContain('<form role="search" action="https://rooms.example/recipes" method="get"'); expect(door).toContain('name="q"'); expect(door).toContain('href="https://rooms.example/plan"');
  expect(broken(c => { c.sections.push({ kind: "door", id: "find", title: "Find", action: "javascript:alert(1)" }); })).toThrow(/must be an https address/);
  expect(() => validate(good)).not.toThrow();
});

test("every authored estate look is either readable on a service site or refused", () => {
  const rows = readFileSync(join(repo, "site/matrix/surfaces/build/presentation-templates.jsonl"), "utf8").trim().split("\n").map(l => JSON.parse(l).template);
  const outcome = Object.fromEntries(rows.map(name => { try { const t = resolveTheme({ template: name }); return [name, contrast(t.colors.display, t.colors.surface) >= 4.5]; } catch (e) { return [name, String(e.message)]; } }));
  expect(Object.keys(outcome).length).toBe(6);
  for (const [name, result] of Object.entries(outcome)) expect(result === true || /not readable|must be a hex/.test(result), `${name}: ${result}`).toBe(true);
});

test("a picture the record names but the folder lacks, or material the folder does not provide, stops the build", async () => {
  const dir = mkdtempSync(join(tmpdir(), "service-site-missing-"));
  try {
    const c = record(sample); c.business.logo = "assets/logo.png";
    writeFileSync(join(dir, "content.json"), JSON.stringify(c));
    await expect(build(dir)).rejects.toThrow(/assets\/logo.png, which is not in/);
    delete c.business.logo; c.sections.push({ kind: "figures", id: "n", title: "Numbers", material: "anything" });
    writeFileSync(join(dir, "content.json"), JSON.stringify(c));
    await expect(build(dir)).rejects.toThrow(/has no material.mjs/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
