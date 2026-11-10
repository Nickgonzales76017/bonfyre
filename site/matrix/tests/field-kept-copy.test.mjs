// The field, in a real browser, across real restarts: first visit keeps the
// catalogue, a second visit downloads nothing, a republished file alone is
// fetched again, and with the estate out of reach the page still opens, reads
// its kept copy, draws, and says so. The edge is stood in for by a local
// server that serves this tree's own page and assets and answers conditional
// requests the way deploy/worker/estate-worker.mjs does.
import { test, expect } from "bun:test";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
import { findChrome, withChrome } from "../../../integrations/browser/capture/chrome-session.mjs";

const site = join(import.meta.dir, "..");
const live = join(homedir(), ".bonfyre/estate-local/catalog");
const ready = Boolean(findChrome()) && Bun.which("duckdb") && existsSync(join(live, "source_channel.parquet")) && existsSync(join(site, "duckdb-eh.wasm"));

test.skipIf(!ready)("the field keeps its catalogue, refreshes only what changed, and opens with the estate out of reach", async () => {
  const work = mkdtempSync(join(tmpdir(), "bonfyre-field-"));
  const catalog = join(work, "catalog"), profile = join(work, "profile");
  mkdirSync(catalog); mkdirSync(profile);
  const cut = (name, rows) => { const out = Bun.spawnSync(["duckdb", "-c", `COPY (SELECT * FROM read_parquet('${join(live, name)}') LIMIT ${rows}) TO '${join(catalog, name)}' (FORMAT PARQUET)`]); if (out.exitCode !== 0) throw new Error(out.stderr.toString()); };
  cut("source_channel.parquet", 4000); cut("source_root.parquet", 1000000); cut("material.parquet", 500);
  const log = [];
  const type = path => path.endsWith(".html") ? "text/html; charset=utf-8" : path.endsWith(".wasm") ? "application/wasm" : /\.m?js$/.test(path) ? "application/javascript; charset=utf-8" : path.endsWith(".css") ? "text/css" : "application/octet-stream";
  const serve = port => Bun.serve({ port, hostname: "127.0.0.1", fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname === "/field" ? join(site, "field.html") : url.pathname === "/field-keeper.js" ? join(site, "field-keeper.js")
      : url.pathname.startsWith("/assets/") ? join(site, url.pathname.slice(8)) : url.pathname.startsWith("/catalog/") ? join(catalog, url.pathname.slice(9)) : "";
    if (!path || path.includes("..") || !existsSync(path)) return new Response("no page", { status: 404 });
    const body = readFileSync(path), etag = `"${createHash("sha256").update(body).digest("hex").slice(0, 32)}"`;
    const headers = { "content-type": type(path), etag, "cache-control": "no-cache" };
    if (url.pathname.startsWith("/catalog/")) {
      if (request.headers.get("if-none-match") === etag) { log.push([url.pathname, 304]); return new Response(null, { status: 304, headers }); }
      log.push([url.pathname, 200]);
    }
    return new Response(body, { headers });
  } });
  let server = serve(0);
  const port = server.port;
  // One visit in a browser that is started, used and stopped; the profile outlives it.
  const visit = () => withChrome(async ({ open, events }) => {
    const tab = await open();
    visit.events = events;
    await tab.go(`http://127.0.0.1:${port}/field`).catch(() => {});
    const read = async expression => (await tab.page("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true })).result.value;
    for (let i = 0; i < 300; i++) {
      const state = await read(`JSON.stringify({ catalogue: document.documentElement.dataset.catalogue ?? "", material: document.documentElement.dataset.material ?? "", note: document.getElementById("state-note")?.textContent ?? "", reading: document.getElementById("reading")?.textContent ?? "" })`).catch(() => null);
      const seen = state ? JSON.parse(state) : null;
      if (seen && seen.catalogue && seen.material && (seen.note || seen.reading.startsWith("The field could not"))) {
        seen.kept = await read(`(async () => { const reg = await navigator.serviceWorker.getRegistration("/field"); const keys = await (await caches.open("field-keeper-1")).keys(); return (reg && reg.active ? "active" : "no worker") + " controller:" + Boolean(navigator.serviceWorker.controller) + " kept:" + keys.map(r => new URL(r.url).pathname.split("/").pop()).filter(n => /wasm|worker|field/.test(n)).join(","); })()`).catch(why => String(why));
        if (process.env.FIELD_DEBUG) console.log(seen.catalogue, "|", seen.kept);
        return seen;
      }
      await Bun.sleep(100);
    }
    // What the browser could not load is part of why it never finished.
    const urls = new Map(visit.events.filter(event => event.method === "Network.requestWillBeSent").map(event => [event.params.requestId, event.params.request.url]));
    const failed = visit.events.filter(event => event.method === "Network.loadingFailed").map(event => `${urls.get(event.params.requestId) ?? "?"} (${event.params.errorText})`);
    throw new Error("the field never finished; failed loads: " + JSON.stringify(failed.slice(0, 6)) + " " + await read(`JSON.stringify({ c: document.documentElement.dataset.catalogue, m: document.documentElement.dataset.material, r: document.getElementById("reading")?.textContent, n: document.getElementById("state-note")?.textContent })`).catch(why => String(why)));
  }, { profile, timeoutMs: 90_000 });
  const since = mark => log.slice(mark).map(([path, status]) => `${path.split("/").pop()} ${status}`).sort();
  try {
    let mark = log.length;
    const first = await visit();
    expect(first.catalogue).toBe("first");
    expect(first.reading).toContain("4,000 records");
    expect(first.note).toContain("A copy of the catalogue is now kept on this device.");
    expect(since(mark)).toEqual(["material.parquet 200", "source_channel.parquet 200", "source_root.parquet 200"]);

    // A new browser process on the same profile: nothing is downloaded again.
    mark = log.length;
    const second = await visit();
    expect(second.catalogue).toBe("unchanged");
    expect(second.note).toContain("nothing was downloaded again");
    expect(since(mark)).toEqual(["material.parquet 304", "source_channel.parquet 304", "source_root.parquet 304"]);

    // One file is republished: it alone is fetched, and the field draws the new one.
    cut("source_channel.parquet", 1500);
    mark = log.length;
    const third = await visit();
    expect(third.catalogue).toBe("refreshed");
    expect(third.reading).toContain("1,500 records");
    expect(since(mark)).toEqual(["material.parquet 304", "source_channel.parquet 200", "source_root.parquet 304"]);

    // The estate goes away. The page, the engine and the catalogue are all the device's own.
    server.stop(true);
    const fourth = await visit();
    expect(fourth.catalogue).toBe("offline");
    expect(fourth.material).toBe("offline");
    expect(fourth.reading).toContain("1,500 records");
    expect(fourth.note).toContain("The estate could not be reached; this is the copy kept on this device from the last visit.");

    // It comes back: the kept copy is confirmed current rather than fetched again.
    server = serve(port);
    mark = log.length;
    const fifth = await visit();
    expect(fifth.catalogue).toBe("unchanged");
    expect(since(mark)).toEqual(["material.parquet 304", "source_channel.parquet 304", "source_root.parquet 304"]);
  } finally { server.stop(true); rmSync(work, { recursive: true, force: true }); }
}, 300_000);
