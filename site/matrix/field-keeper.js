/* Keeps what the field needs to open with the estate out of reach: its own
 * page, the query engine, and the scripts it draws with. Registered with the
 * scope /field, so it answers for that page and for nothing else on the site.
 *
 * The catalogue is not kept here. The page keeps it itself, beside the stamp
 * the estate gave it, and asks whether it has changed on every visit.
 */
const KEPT = "field-keeper-1";

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", event => event.waitUntil((async () => {
  for (const name of await caches.keys()) if (name !== KEPT && name.startsWith("field-keeper-")) await caches.delete(name);
  await self.clients.claim();
})()));

self.addEventListener("fetch", event => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== "GET" || url.origin !== self.location.origin) return;
  if (url.pathname.startsWith("/catalog/") || url.pathname === "/mcp") return;
  const page = request.mode === "navigate";
  const asset = url.pathname.startsWith("/assets/");
  if (!page && !asset) return;
  event.respondWith((async () => {
    const cache = await caches.open(KEPT);
    const key = page ? "/field" : request.url;
    /* A stamped asset cannot change without its address changing. */
    if (asset && url.searchParams.has("b")) {
      const kept = await cache.match(key);
      if (kept) return kept;
    }
    try {
      const answer = await fetch(request);
      // Keeping a copy is a convenience and must never cost the reader the
      // page. The body is read whole and stored from memory: storing it as it
      // streams from the network was seen to fail on a 34 MB engine ("put
      // encountered a network error") while the same bytes stored from a
      // buffer went in. The write is waited for, so a visit that ends right
      // after drawing has still kept what it fetched.
      if (answer.ok && answer.status === 200) {
        try {
          const bytes = await answer.clone().arrayBuffer();
          await cache.put(key, new Response(bytes, { status: 200, headers: answer.headers }));
        } catch (_) {}
      }
      return answer;
    } catch (why) {
      const kept = await cache.match(key);
      if (kept) return kept;
      throw why;
    }
  })());
});
