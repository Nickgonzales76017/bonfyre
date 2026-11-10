import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const file = new URL("../../deploy/worker/estate-worker.mjs", import.meta.url);
let source = await readFile(file, "utf8");
source = source.replace("export default {", "globalThis.__estateWorker = {");
await import("data:text/javascript," + encodeURIComponent(source));

const seen = [];
globalThis.caches = { default: { match: async () => null, put: async () => {} } };
const env = { ESTATE: { get: async (key) => {
  // These routes are shared-estate material. Model an absent subject override
  // so the witness asserts the fallback key rather than a host probe.
  if (key.startsWith("hosts/")) return null;
  seen.push(key);
  return { body: "<main>ledger</main>", httpEtag: '"test"', writeHttpMetadata() {} };
} } };
const ctx = { waitUntil() {} };

let response = await globalThis.__estateWorker.fetch(
  new Request("https://aurekai.live/channels?from=40&standing=admitted"), env, ctx);
assert.equal(response.status, 200);
assert.equal(seen.pop(), "channels/__q/standing=admitted&from=40.html");

response = await globalThis.__estateWorker.fetch(
  new Request("https://aurekai.live/channels?channel=https%3A%2F%2Ftwo.example%2Fspace%20here"), env, ctx);
assert.equal(response.status, 200);
assert.equal(seen.pop(), "channels/__q/channel=https%3A%2F%2Ftwo.example%2Fspace%20here.html");

const longChannel = "https://long.example/" + "x".repeat(300);
response = await globalThis.__estateWorker.fetch(
  new Request("https://aurekai.live/channels?channel=" + encodeURIComponent(longChannel)), env, ctx);
assert.equal(response.status, 200);
const longDigest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode("channel=" + encodeURIComponent(longChannel)));
const expectedLong = "channels/__q/h/" + Array.from(new Uint8Array(longDigest), byte => byte.toString(16).padStart(2, "0")).join("") + ".html";
assert.equal(seen.pop(), expectedLong);

response = await globalThis.__estateWorker.fetch(
  new Request("https://aurekai.live/channels?not-a-coordinate=1"), env, ctx);
assert.equal(response.status, 200);
assert.equal(seen.pop(), "channels.html");

const creditedName = "Alex & Co";
const creditedSource = "publisher-a";
const creditedQuery = "who=" + encodeURIComponent(creditedName) + "&source=" + encodeURIComponent(creditedSource);
response = await globalThis.__estateWorker.fetch(
  new Request("https://aurekai.live/people?" + creditedQuery), env, ctx);
assert.equal(response.status, 200);
const creditedDigest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(creditedQuery));
const expectedCredited = "people/__q/h/" + Array.from(new Uint8Array(creditedDigest), byte => byte.toString(16).padStart(2, "0")).join("") + ".html";
assert.equal(seen.pop(), expectedCredited);

const chooserQuery = "who=" + encodeURIComponent(creditedName);
response = await globalThis.__estateWorker.fetch(
  new Request("https://aurekai.live/people?" + chooserQuery), env, ctx);
assert.equal(response.status, 200);
const chooserDigest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(chooserQuery));
const expectedChooser = "people/__q/h/" + Array.from(new Uint8Array(chooserDigest), byte => byte.toString(16).padStart(2, "0")).join("") + ".html";
assert.equal(seen.pop(), expectedChooser);

response = await globalThis.__estateWorker.fetch(
  new Request("https://aurekai.live/people?source=publisher-a"), env, ctx);
assert.equal(response.status, 200);
assert.equal(seen.pop(), "people.html");
console.log("estate worker source ledger: PASS");
