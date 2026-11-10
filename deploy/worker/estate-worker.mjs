/**
 * The estate, served from the edge.
 *
 * Every page this estate publishes is composed by its own organ and written to
 * an object store; this hands those objects to whoever asks, anywhere, with no
 * limit on how many there are. That limit mattered: a static site host stops at
 * twenty thousand files, and the estate passed that while still holding only a
 * fraction of what it has read.
 *
 * It composes almost nothing itself. The one exception is search, which a set
 * of pre-written files cannot answer: a question nobody asked in advance has no
 * file, so the results are drawn from the estate's own index of what it has
 * kept, and said in the same plain words the rest of the estate uses.
 *
 * What it will not do: invent material, rank by anything but the index's own
 * score and recency, or claim a source still publishes what this estate read.
 */

const HTML = "text/html; charset=utf-8";
const JSON_CT = "application/json; charset=utf-8";

/* A machine that asked in JSON is refused in JSON.
 *
 * The failure this is written against: an auth boundary that answered a client
 * asking an API for JSON with a page of prose it could not act on. This edge
 * did the same thing at every refusal -- 404 and 405 both returned sentences
 * meant for a person -- so anything automated reading this estate got HTML
 * where it expected a reason it could branch on.
 *
 * A path under a machine surface counts as a machine's question even when the
 * caller forgot to say so, because nothing serves a person at those addresses. */
function speaksJson(request, url) {
  const accept = (request.headers.get("accept") || "").toLowerCase();
  if (accept.includes("application/json")) return true;
  return /^\/(studio|api|\.well-known)\//.test(url.pathname);
}

function refuse(status, error, reason, extra) {
  return new Response(JSON.stringify(Object.assign({
    error, reason, status,
    cannot_infer: "a refusal from this edge is a fact about this estate's published surface. "
      + "It is not a statement about any other estate, and this edge holds no authority "
      + "to create, grant or perform anything it declined.",
  }, extra || {}), null, 1) + "\n", { status, headers: { "content-type": JSON_CT } });
}

/* A source-ledger coordinate is public, but an R2 object store cannot answer
 * its query string dynamically.  The static publisher writes only the proved
 * coordinates below under channels/__q/.  Arbitrary query strings never
 * become object keys: that would make the bucket namespace caller-controlled
 * and make an unexported query look like a public claim. */
const SOURCE_LEDGER_QUERY_KEYS = [
  "channel", "root", "family", "roots", "way", "publishes", "standing", "q", "from",
];
const SOURCE_LEDGER_MAX_MATERIAL_LEAF = 180;
const PEOPLE_LEDGER_QUERY_KEYS = ["who", "source"];

async function sha256Hex(value) {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
}

async function sourceLedgerAddress(url) {
  if (url.pathname !== "/channels" || !url.search) return null;
  const seen = new Set();
  for (const [key, value] of url.searchParams) {
    if (!SOURCE_LEDGER_QUERY_KEYS.includes(key) || seen.has(key) || !value || value.length > 1024) return null;
    seen.add(key);
  }
  if (!seen.size) return null;
  const parts = [];
  for (const key of SOURCE_LEDGER_QUERY_KEYS) {
    const value = url.searchParams.get(key);
    if (value) parts.push(key + "=" + encodeURIComponent(value));
  }
  if (!parts.length) return null;
  const query = parts.join("&");
  const direct = "channels/__q/" + query + ".html";
  // The URL remains the source's semantic coordinate. The digest is only the
  // carrier-safe material address of a query too long for a filesystem leaf.
  return direct.length <= SOURCE_LEDGER_MAX_MATERIAL_LEAF
    ? direct
    : "channels/__q/h/" + await sha256Hex(query) + ".html";
}

/* A credited name is evidence scoped to the source that printed it. These are
 * finite static exports; arbitrary query strings cannot become R2 keys or turn
 * equal spellings into a fabricated identity. */
async function peopleLedgerAddress(url) {
  if (url.pathname !== "/people" || !url.search) return null;
  const seen = new Set();
  for (const [key, value] of url.searchParams) {
    if (!PEOPLE_LEDGER_QUERY_KEYS.includes(key) || seen.has(key) || !value || value.length > 4096) return null;
    seen.add(key);
  }
  const who = url.searchParams.get("who");
  const source = url.searchParams.get("source");
  if (!who || !seen.has("who")) return null;
  const query = "who=" + encodeURIComponent(who) + (source ? "&source=" + encodeURIComponent(source) : "");
  // Person/byline text is Unicode while common publication filesystems are
  // normalization/case-insensitive. The semantic query remains readable;
  // every physical leaf is a digest so distinct evidence cannot collide.
  return "people/__q/h/" + await sha256Hex(query) + ".html";
}

async function addressFor(pathname, url) {
  const sourceLedger = await sourceLedgerAddress(url);
  if (sourceLedger) return sourceLedger;
  const peopleLedger = await peopleLedgerAddress(url);
  if (peopleLedger) return peopleLedger;
  // The address a person was given, mapped to the object that holds it.
  let path = decodeURIComponent(pathname);
  if (path.includes("..") || path.includes("\0")) return null;
  if (path === "/" || path === "") return "index.html";
  if (path.endsWith("/")) path = path.slice(0, -1);
  path = path.replace(/^\//, "");
  if (!path) return "index.html";
  // Assets and anything already naming its own file are taken as they are.
  // An address that already names a file is taken as it is. The extension may
  // be long: .parquet is seven characters, and a rule written for .html and
  // .css turned every catalogue request into a request for a page that does
  // not exist.
  if (/\.[a-z0-9]{2,8}$/i.test(path)) return path;
  return path + ".html";
}

function subjectPrefix(hostname) {
  // A subject's address is not a path. The estate answers on hundreds of hosts
  // -- a publisher it reads, a member standing in a territory -- and each one
  // composes that subject's own material. Resolving by path alone handed every
  // one of them the territory's front page under the subject's name, which is
  // the one thing an address must never do.
  // The www alias keeps the shared estate; every other name under aurekai
  // looks in its own tree first.
  let host = String(hostname || "").toLowerCase().replace(/:\d+$/, "");
  if (!host || !/^[a-z0-9][a-z0-9.-]{0,120}$/.test(host)) return null;
  // www is the same address said twice; it resolves to the same tree rather
  // than falling back to the shared estate, which would have handed
  // www.aurekai.live the pages of a different territory.
  if (host.startsWith("www.")) host = host.slice(4);
  // A territory apex is a host too. Serving it by path alone gave every
  // territory the same front page: .work and .shop showed .life's evening
  // under their own names, which is the subdomain failure one level up.
  if (!host.includes(".aurekai.") && !host.startsWith("aurekai.")) return null;
  return "hosts/" + host + "/";
}

/* The public estate is material-first, but a person does not become static
 * material at the moment they sign in.  Member routes therefore move to the
 * dedicated native session host for the same territory.  It is deliberately a
 * sibling host rather than a catch-all no-Worker exception on the territory
 * apex: a scriptless route there would fall through the apex's static origin,
 * not the managed Tunnel.  The session cookie remains constrained to the
 * territory parent domain by the native owner. */
const NATIVE_MEMBER_PREFIXES = [
  "/login", "/signup", "/logout", "/account", "/saved", "/studio", "/cms",
];

function nativeMemberHost(hostname) {
  const host = String(hostname || "").toLowerCase().replace(/:\d+$/, "");
  if (host === "aurekai.life" || host.endsWith(".aurekai.life")) return "auth.aurekai.life";
  if (host === "aurekai.live" || host.endsWith(".aurekai.live")) return "auth.aurekai.live";
  if (host === "aurekai.shop" || host.endsWith(".aurekai.shop")) return "auth.aurekai.shop";
  if (host === "aurekai.work" || host.endsWith(".aurekai.work")) return "signin.aurekai.work";
  return null;
}

function isNativeMemberPath(pathname) {
  return NATIVE_MEMBER_PREFIXES.some(prefix => pathname === prefix || pathname.startsWith(prefix + "/"));
}

function escapeText(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

async function shellOf(env) {
  // The search page wears the estate's own stylesheet rather than a second
  // design: whatever dresses the rest of the estate dresses this too.
  return {
    head:
      '<!doctype html><html lang="en"><head><meta charset="utf-8">' +
      '<meta name="viewport" content="width=device-width,initial-scale=1">' +
      '<link rel="stylesheet" href="/assets/tailwind.css">' +
      '<link rel="stylesheet" href="/assets/world.css">',
    open: '</head><body><main class="wrap">',
    close: "</main></body></html>",
  };
}

async function search(request, env, url) {
  const asked = (url.searchParams.get("q") || "").trim().slice(0, 200);
  const shell = await shellOf(env);
  const pieces = [shell.head, "<title>", escapeText(asked || "Search"),
                  " · Aurekai</title>", shell.open];

  pieces.push(
    '<header class="region"><h1>Search</h1>',
    '<form class="ledger-find" method="get" action="/search">',
    '<label>Find <input type="search" name="q" value="', escapeText(asked),
    '" placeholder="a word, a name, a place"></label>',
    '<button class="button primary" type="submit">Look</button></form></header>');

  if (!asked) {
    pieces.push('<p class="src">Ask for a word and this estate will say what it has kept that carries it.</p>');
    pieces.push(shell.close);
    return new Response(pieces.join(""), { headers: { "content-type": HTML } });
  }

  let rows = [];
  let failed = "";
  try {
    const answer = await fetch(env.SUPABASE_URL + "/rest/v1/rpc/estate_search", {
      method: "POST",
      headers: {
        apikey: env.SUPABASE_ANON_KEY,
        authorization: "Bearer " + env.SUPABASE_ANON_KEY,
        "content-type": "application/json",
      },
      body: JSON.stringify({ q: asked, want: 60 }),
    });
    if (!answer.ok) throw new Error("index answered " + answer.status);
    rows = await answer.json();
  } catch (problem) {
    failed = String(problem && problem.message || problem).slice(0, 140);
  }

  if (failed) {
    pieces.push('<section class="region"><p class="src">The index did not answer, so this estate cannot say ',
                'what it holds for that word right now. Nothing has been guessed in its place.</p></section>');
  } else if (!rows.length) {
    pieces.push('<section class="region"><p class="src">Nothing this estate has kept carries that word. ',
                'That is a fact about what it has read, not about the world.</p></section>');
  } else {
    pieces.push('<section class="region"><h2 class="region-label">', String(rows.length),
                rows.length === 1 ? " thing kept" : " things kept", "</h2><ol class=\"more\">");
    for (const row of rows) {
      pieces.push('<li><a href="/', escapeText(row.address), '">', escapeText(row.title || row.address), "</a>");
      pieces.push('<span class="by">');
      if (row.publisher) pieces.push("<span>", escapeText(row.publisher), "</span>");
      if (row.kind) pieces.push("<span>", escapeText(row.kind), "</span>");
      pieces.push("</span></li>");
    }
    pieces.push("</ol>",
      '<p class="src">These are things this estate read and kept, ordered by how strongly each carries ',
      'the word and how recently it was published. A result is not a claim that its source still ',
      'publishes it, nor that what it says is true.</p></section>');
  }
  pieces.push(shell.close);
  return new Response(pieces.join(""), {
    headers: { "content-type": HTML, "cache-control": "public, max-age=60" },
  });
}

/* ------------------------------------------------------------------ MCP
 *
 * The estate's machine aperture used to live behind an SSH tunnel to a host
 * that no longer exists, so /mcp answered 404 on every territory and the
 * connector that asked for it could not begin. The adapter it tunnelled to is
 * a Node server; this is the same contract where the estate actually still
 * runs -- at the edge, over the objects it has already published.
 *
 * It is deliberately read-only. An aperture at the edge holds no authority: it
 * can say what this estate has published and refuse everything else, which is
 * the honest shape for a surface with no session and no owner behind it. A
 * write tool is not merely absent, it is refused by name, so a caller learns
 * the boundary rather than a silence.
 */
const MCP_PROTOCOL = "2025-06-18";

const MCP_READ_TOOLS = [
  {
    name: "estate_search",
    description: "Search what this estate has read and kept. Returns things it retained, "
      + "ordered by how strongly each carries the word and how recently it was published. "
      + "A result is not a claim that its source still publishes it, nor that it is true.",
    inputSchema: {
      type: "object",
      properties: { q: { type: "string" }, want: { type: "integer" } },
      required: ["q"],
    },
  },
  {
    name: "estate_page",
    description: "Read a page this estate composed, by the address a person would use. "
      + "What comes back is what was composed when it was written, not a live reading of "
      + "its sources.",
    inputSchema: {
      type: "object",
      properties: { address: { type: "string" } },
      required: ["address"],
    },
  },
  {
    name: "estate_boundary",
    description: "What this aperture is, and what it refuses. Ask this before assuming an "
      + "absent tool means an unavailable capability.",
    inputSchema: { type: "object", properties: {} },
  },
];

/* Named so a caller is refused by name rather than by omission. These exist in
 * the estate and require an owner, a session and an authority this edge has
 * none of. */
const MCP_REFUSED_TOOLS = {
  media_transport_lease: "a lease is an effect; the edge holds no authority to grant one",
  bonfyre_aperture_refresh: "an aperture refresh is an owner operation, not a public read",
  creator_channel_surface: "a channel surface depends on a visibility decision this edge cannot make",
  media_library_catalog: "a library catalogue depends on membership the edge cannot check",
  media_playback_plan: "a playback plan binds a transport this edge cannot bind",
};

const MCP_BOUNDARY = {
  aperture: "bonfyre-edge-aperture.v1",
  serves: "read-only questions about what this estate has published",
  holds_no: ["session", "authority", "payment", "publication", "external effect"],
  cannot_infer: "a tool advertised here is not authority, evidence, proof, provider admission, "
    + "commitment, payment approval or effect permission; every exact owner rechecks any "
    + "operation forwarded to it, and this aperture forwards none",
};

function mcpResult(id, result) {
  return new Response(JSON.stringify({ jsonrpc: "2.0", id, result }),
    { headers: { "content-type": "application/json", "cache-control": "no-store" } });
}

function mcpError(id, code, message, data) {
  return new Response(JSON.stringify({
    jsonrpc: "2.0", id,
    error: { code, message, ...(data === undefined ? {} : { data }) },
  }), { headers: { "content-type": "application/json", "cache-control": "no-store" } });
}

async function mcpSearchRows(env, asked, want) {
  if (!env.SUPABASE_URL || !env.SUPABASE_ANON_KEY) return null;
  const answer = await fetch(env.SUPABASE_URL + "/rest/v1/rpc/estate_search", {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_ANON_KEY,
      authorization: "Bearer " + env.SUPABASE_ANON_KEY,
      "content-type": "application/json",
    },
    body: JSON.stringify({ q: asked, want }),
  });
  if (!answer.ok) throw new Error("index answered " + answer.status);
  return answer.json();
}

async function mcp(request, env, url) {
  if (request.method === "GET") {
    /* A connector that probes with GET is told what this is rather than 405'd. */
    return new Response(JSON.stringify({
      ...MCP_BOUNDARY,
      transport: "JSON-RPC 2.0 over HTTP POST to this same address",
      protocolVersion: MCP_PROTOCOL,
    }), { headers: { "content-type": "application/json" } });
  }
  let body = null;
  try {
    body = await request.json();
  } catch {
    return mcpError(null, -32700, "parse_error");
  }
  const batch = Array.isArray(body) ? body : [body];
  const one = batch[0] || {};
  const id = one.id === undefined ? null : one.id;
  if (one.jsonrpc !== "2.0" || typeof one.method !== "string") {
    return mcpError(id, -32600, "invalid_request");
  }

  if (one.method === "initialize") {
    return mcpResult(id, {
      protocolVersion: MCP_PROTOCOL,
      capabilities: { tools: { listChanged: false } },
      serverInfo: { name: "bonfyre-edge-aperture", version: "1.0.0" },
      instructions: MCP_BOUNDARY.cannot_infer,
    });
  }
  if (one.method === "notifications/initialized") return new Response(null, { status: 202 });
  if (one.method === "ping") return mcpResult(id, {});
  if (one.method === "resources/list") return mcpResult(id, { resources: [] });
  if (one.method === "prompts/list") return mcpResult(id, { prompts: [] });
  if (one.method === "tools/list") return mcpResult(id, { tools: MCP_READ_TOOLS });

  if (one.method === "tools/call") {
    const name = String(one.params?.name || "");
    const args = one.params?.arguments || {};
    if (MCP_REFUSED_TOOLS[name]) {
      return mcpError(id, -32003, "refused_at_this_aperture",
        { tool: name, why: MCP_REFUSED_TOOLS[name] });
    }
    if (name === "estate_boundary") {
      return mcpResult(id, { content: [{ type: "text", text: JSON.stringify(MCP_BOUNDARY) }] });
    }
    if (name === "estate_search") {
      const asked = String(args.q || "").trim().slice(0, 200);
      if (!asked) return mcpError(id, -32602, "estate_search needs q");
      const want = Math.min(Math.max(Number(args.want) || 20, 1), 60);
      try {
        const rows = await mcpSearchRows(env, asked, want);
        if (rows === null) return mcpError(id, -32003, "no index is configured at this aperture");
        return mcpResult(id, {
          content: [{ type: "text", text: JSON.stringify({ asked, kept: rows.length, rows }) }],
          _meta: { "io.bonfyre/cannotInfer": "a result is a thing this estate read and kept; it "
            + "is not a claim that its source still publishes it, nor that it is true" },
        });
      } catch (problem) {
        return mcpError(id, -32003, "the index did not answer",
          { detail: String(problem && problem.message || problem).slice(0, 140) });
      }
    }
    if (name === "estate_page") {
      const address = String(args.address || "/");
      const key = await addressFor(address.startsWith("/") ? address : "/" + address,
                                  new URL(url.origin + (address.startsWith("/") ? address : "/" + address)));
      if (!key) return mcpError(id, -32602, "not a readable address");
      const object = await env.ESTATE.get(key);
      if (!object) return mcpError(id, -32002, "this estate has published no page at that address");
      const text = await object.text();
      return mcpResult(id, {
        content: [{ type: "text", text: text.slice(0, 400000) }],
        _meta: { "io.bonfyre/cannotInfer": "this is what the estate composed when the page was "
          + "written, not a live reading of its sources" },
      });
    }
    return mcpError(id, -32601, "no such tool at this aperture", { tool: name });
  }
  return mcpError(id, -32601, "method_not_found", { method: one.method });
}

export default {
  async fetch(request, env, ctx) {
    let url = new URL(request.url);
    // The zone's rewrite rule addresses every page by its host, so that a page
    // published as a static file is served without this script running.  A
    // request that reached the script is a miss; it is the same page, read
    // here by its own path.
    if (url.pathname.startsWith("/h/")) {
      const rest = url.pathname.slice(3), slash = rest.indexOf("/");
      const host = slash < 0 ? rest : rest.slice(0, slash);
      if (host === url.hostname) {
        url = new URL(url);
        url.pathname = slash < 0 ? "/" : rest.slice(slash);
        request = new Request(url, request);
      }
    }

    /* The machine aperture speaks JSON-RPC over POST, so it is answered before
     * the read-only guard below. Everything else on this estate is a read. */
    if (url.pathname === "/mcp" || url.pathname === "/mcp/") return mcp(request, env, url);
    if (url.pathname === "/.well-known/oauth-protected-resource"
        || url.pathname === "/.well-known/oauth-protected-resource/mcp") {
      /* No authorization server stands behind this aperture, and pretending one
       * did would send a connector into a flow that cannot complete. It reads
       * without a credential, and says so. */
      return new Response(JSON.stringify({
        resource: url.origin + "/mcp",
        authorization_servers: [],
        scopes_supported: [],
        bearer_methods_supported: [],
        resource_documentation: url.origin + "/protocols",
        cannot_infer: "this aperture requires no credential because it grants no authority; "
          + "an absent authorization server is not a missing one",
      }), { headers: { "content-type": "application/json" } });
    }
    if (request.method !== "GET" && request.method !== "HEAD") {
      if (isNativeMemberPath(url.pathname)) {
        const host = nativeMemberHost(url.hostname);
        if (host) {
          return Response.redirect("https://" + host + url.pathname + url.search, 307);
        }
      }
      if (speaksJson(request, url)) {
        return refuse(405, "read_only", "this estate only answers questions; it accepts none",
                      { allow: "GET, HEAD" });
      }
      return new Response("This estate only answers questions.",
                          { status: 405, headers: { "content-type": HTML, allow: "GET, HEAD" } });
    }
    if (url.pathname === "/search") return search(request, env, url);
    if (isNativeMemberPath(url.pathname)) {
      const host = nativeMemberHost(url.hostname);
      if (host) {
        return Response.redirect("https://" + host + url.pathname + url.search, 302);
      }
    }
    if (url.pathname === "/healthz") {
      return new Response(JSON.stringify({
        schema: "bonfyre-edge-estate.v1",
        serves: "pages this estate composed, from its own object store",
        cannot_infer: "a page served here is what the estate composed when it was written, not a live reading of its sources",
      }), { headers: { "content-type": "application/json" } });
    }

    const key = await addressFor(url.pathname, url);
    if (!key) return new Response("Not a readable address.", { status: 400 });

    // The edge keeps what it has already handed out.
    const cache = caches.default;
    const cached = await cache.match(request);
    if (cached) return cached;

    // A reader that asks for part of an object gets that part. This is what
    // makes a columnar catalogue worth publishing at all: a question about one
    // column of thirty-five thousand rows should not cost the whole file.
    const asked = request.headers.get("range");
    let ranged = null;
    if (asked) {
      const match = /^bytes=(\d*)-(\d*)$/.exec(asked.trim());
      if (match) {
        const from = match[1] === "" ? undefined : Number(match[1]);
        const to = match[2] === "" ? undefined : Number(match[2]);
        ranged = from === undefined ? { suffix: to }
               : to === undefined ? { offset: from }
               : { offset: from, length: to - from + 1 };
      }
    }

    // The subject's own tree answers first; the shared estate answers the rest,
    // so one published estate carries every address without a copy of itself
    // under each of them. Assets and the catalogue are deliberately shared.
    const prefix = subjectPrefix(url.hostname);
    let object = null;
    if (prefix) object = await env.ESTATE.get(prefix + key, ranged ? { range: ranged } : undefined);
    // The estate's surfaces are shared and answer at every address; the front
    // page of an address is that subject's own or it does not exist. Without
    // that second half, any subdomain anyone invented would have been handed
    // the territory's front page under a name this estate never gave out --
    // which is the failure this whole prefix exists to prevent.
    const claimsAnApex = prefix && key === "index.html";
    if (!object && !claimsAnApex) object = await env.ESTATE.get(key, ranged ? { range: ranged } : undefined);
    if (!object) {
      if (speaksJson(request, url)) {
        return refuse(404, "no_page", "this estate has published no page at that address",
                      { address: url.pathname });
      }
      const missing = await env.ESTATE.get("404.html");
      const body = missing ? await missing.text()
                 : "This estate has no page at that address.";
      return new Response(body, { status: 404, headers: { "content-type": HTML } });
    }

    const headers = new Headers();
    object.writeHttpMetadata(headers);
    if (!headers.get("content-type")) {
      headers.set("content-type", key.endsWith(".css") ? "text/css; charset=utf-8"
                                : key.endsWith(".js") ? "application/javascript; charset=utf-8"
                                : HTML);
    }
    headers.set("etag", object.httpEtag);
    /* How long the edge may keep this, by what kind of thing it is.
     *
     * One policy for everything meant a 33 MB WebAssembly engine and a seven
     * megabyte catalogue were re-fetched from the object store every five
     * minutes, for every reader, on every territory. A session of ordinary
     * publishing put this account past its request budget and every territory
     * answered HTTP 429.
     *
     * A URL carrying a content stamp cannot change without the URL changing,
     * so it is immutable and nothing needs to ask again. The catalogue is a
     * publication pass and is allowed to be an hour stale while it revalidates
     * in the background. A composed page is the thing that actually moves, and
     * it keeps the short window it had. */
    const stamped = url.searchParams.has("b");
    const isCatalogue = key.startsWith("catalog/");
    headers.set("cache-control",
      stamped ? "public, max-age=31536000, immutable"
      : isCatalogue ? "public, max-age=3600, stale-while-revalidate=86400"
      : "public, max-age=300, stale-while-revalidate=600");

    headers.set("accept-ranges", "bytes");
    /* A reader that already holds this object says which copy it holds. When
     * that is still the one published, it is told so and sent nothing: the
     * field keeps the catalogue on the device and asks this on every visit,
     * and eleven megabytes answered again each time is the request budget
     * spent on bytes the reader already has. */
    if (!ranged && request.headers.get("if-none-match") === object.httpEtag) {
      return new Response(null, { status: 304, headers });
    }
    if (ranged && object.range) {
      const start = object.range.offset ?? 0;
      const length = object.range.length ?? (object.size - start);
      headers.set("content-range", `bytes ${start}-${start + length - 1}/${object.size}`);
      // A partial answer is this reader's slice, not the object; caching it
      // under the object's own address would hand the slice to the next asker.
      return new Response(object.body, { status: 206, headers });
    }

    const answer = new Response(object.body, { headers });
    ctx.waitUntil(cache.put(request, answer.clone()));
    return answer;
  },
};
