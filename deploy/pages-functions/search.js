const HTML = "text/html; charset=utf-8";

function addressFor(pathname) {
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


export async function onRequestGet(context) {
  const url = new URL(context.request.url);
  return search(context.request, context.env, url);
}
