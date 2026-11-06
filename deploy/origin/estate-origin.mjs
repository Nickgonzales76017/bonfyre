/**
 * The estate's published pages, served from this machine.
 *
 * The edge Worker in deploy/worker answers every request for a published page,
 * stylesheet or script, and a free Worker stops at one hundred thousand
 * requests a day.  The estate passed that: every afternoon each address it
 * gives a publisher or a member answered "rate limited" until midnight UTC.
 * Nothing about handing out a stored page needs a Worker.
 *
 * This runs the Worker's own code, unchanged, as an ordinary origin behind the
 * tunnel that already carries the territory apexes.  It reads the same object
 * store, applies the same address rules and refusals, and has no daily limit.
 * Search and the machine aperture stay on the Worker, which keeps the keys
 * they need and now sees only those few requests.
 *
 * What it does not do: compose anything.  A page served here is what the
 * estate wrote to the object store when it last published, exactly as at the
 * edge.
 */
import worker from "../worker/estate-worker.mjs";

const need = name => { const v = process.env[name]; if (!v) throw new Error(`estate origin: ${name} is not set`); return v; };
const store = new Bun.S3Client({
  accessKeyId: need("CLOUDFLARE_ACCESS_KEY_ID"),
  secretAccessKey: need("CLOUDFLARE_SECRET_ACCESS_KEY"),
  endpoint: need("CLOUDFLARE_R2_ENDPOINT"),
  bucket: process.env.BONFYRE_R2_BUCKET ?? "aurekai-estate",
  region: "auto",
});
const port = Number(process.env.BONFYRE_ESTATE_ORIGIN_PORT ?? 28931);

/* What was just handed out is kept for a few minutes, so a page many readers
 * ask for costs the object store one read, not one each.  Bounded twice: no
 * single object over half a megabyte, and no more than the budget in total. */
const KEEP_MS = 5 * 60 * 1000, KEEP_EACH = 512 * 1024, KEEP_TOTAL = 160 * 1024 * 1024;
const kept = new Map();
let keptBytes = 0;
function remember(key, entry) {
  const old = kept.get(key);
  if (old) { keptBytes -= old.bytes?.byteLength ?? 0; kept.delete(key); }
  kept.set(key, entry);
  keptBytes += entry.bytes?.byteLength ?? 0;
  for (const [k, v] of kept) { if (keptBytes <= KEEP_TOTAL && kept.size <= 60000) break; keptBytes -= v.bytes?.byteLength ?? 0; kept.delete(k); }
}
function recall(key) {
  const entry = kept.get(key);
  if (!entry) return undefined;
  if (Date.now() - entry.at > KEEP_MS) { keptBytes -= entry.bytes?.byteLength ?? 0; kept.delete(key); return undefined; }
  return entry;
}

function asObject(meta, body, range) {
  return {
    size: meta.size,
    httpEtag: meta.etag?.startsWith('"') ? meta.etag : `"${meta.etag}"`,
    range,
    body,
    writeHttpMetadata(headers) {
      // The store records "text/html" bare; these pages are UTF-8 and say so.
      if (meta.type) headers.set("content-type", /^(text\/|application\/(javascript|json))/.test(meta.type) && !/charset=/i.test(meta.type) ? `${meta.type}; charset=utf-8` : meta.type);
    },
    async text() { return await new Response(body).text(); },
  };
}

/* The one binding the Worker uses, answered from the same bucket. */
const ESTATE = {
  async get(key, options) {
    const hit = recall(key);
    if (hit && hit.missing) return null;
    if (hit && !options?.range) return asObject(hit, hit.bytes, undefined);
    const file = store.file(key);
    let meta = hit;
    if (!meta) {
      try { meta = await file.stat(); }
      catch (error) {
        if (error?.code === "NoSuchKey" || error?.name === "S3Error" && /NoSuchKey|NotFound|404/.test(String(error?.code ?? error?.message))) {
          remember(key, { missing: true, at: Date.now() });
          return null;
        }
        throw error;
      }
    }
    if (options?.range) {
      const r = options.range;
      const offset = r.suffix !== undefined ? Math.max(0, meta.size - r.suffix) : (r.offset ?? 0);
      const length = r.suffix !== undefined ? meta.size - offset : (r.length ?? meta.size - offset);
      return asObject(meta, file.slice(offset, offset + length).stream(), { offset, length });
    }
    if (meta.size <= KEEP_EACH) {
      const bytes = new Uint8Array(await file.arrayBuffer());
      remember(key, { size: meta.size, etag: meta.etag, type: meta.type, bytes, at: Date.now() });
      return asObject(meta, bytes, undefined);
    }
    return asObject(meta, file.stream(), undefined);
  },
};

/* The Worker asks the edge's own cache first.  Here the cache in front of this
 * origin is Cloudflare's, so the Worker's is simply empty. */
globalThis.caches ??= { default: { async match() { return undefined; }, async put() {} } };
const ctx = { waitUntil(promise) { Promise.resolve(promise).catch(() => {}); }, passThroughOnException() {} };
const env = { ESTATE, SUPABASE_URL: process.env.SUPABASE_URL ?? "", SUPABASE_ANON_KEY: process.env.SUPABASE_ANON_KEY ?? "" };

const server = Bun.serve({
  hostname: "127.0.0.1",
  port,
  async fetch(request) {
    let seen;
    try { seen = new URL(request.url); } catch { return new Response("Not a readable address.", { status: 400 }); }
    const host = (request.headers.get("host") ?? seen.host).split(":")[0].toLowerCase();
    // Only names this estate gives out; anything else is not ours to answer.
    if (!/^([a-z0-9-]+\.)+aurekai\.(life|live|shop|work)$/.test(host)) return new Response("Not an address of this estate.", { status: 421 });
    // Readers arrive over https at the tunnel's far end; the Worker's own
    // links and descriptors should say so.
    const asked = new Request(`https://${host}${seen.pathname}${seen.search}`, { method: request.method, headers: request.headers, body: request.body });
    try { return await worker.fetch(asked, env, ctx); }
    catch (error) {
      console.error(new Date().toISOString(), host, seen.pathname, String(error?.message ?? error).slice(0, 200));
      return new Response("This page could not be read just now.", { status: 502, headers: { "content-type": "text/plain; charset=utf-8", "retry-after": "30" } });
    }
  },
});
console.log(`estate origin: http://127.0.0.1:${server.port}`);
