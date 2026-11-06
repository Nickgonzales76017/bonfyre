/* Client organs: capabilities the server-composed shell does not have.
 *
 * The estate composes its own HTML and that stays true. None of this decides
 * what a page says, what material it carries, or what anyone may do -- the
 * browser is not a compositor here and never becomes a second source of
 * authority. These organs do the four things a server cannot do from a
 * distance: show what changed, anchor an explanation beside the thing it
 * explains, keep an enormous list navigable, and spend the reader's own idle
 * hardware instead of ours.
 *
 * Every organ binds to markup the estate already emits -- `.region`, `ol.more`,
 * `.src`, `.by`, `[data-bf-slot]` -- so it reaches every surface at once rather
 * than being wired page by page. Every organ is absent-tolerant: if its vendor
 * file did not load, the page keeps working exactly as it did before, because
 * the page was already complete when it arrived.
 */
(function () {
  "use strict";

  var ORGANS = [];
  var loaded = Object.create(null);

  /* What each vendored file provides, stated rather than discovered.
   *
   * The first version of this loaded a UMD script and then diffed `window` to
   * see what had appeared. That finds nothing: a global declared with `var` is
   * not enumerable, so every UMD organ silently received an empty object and
   * did nothing, while the libraries themselves had loaded perfectly. Naming
   * the export is duller and actually works.
   */
  var VENDOR = {
    "autoanimate.min.js":       { module: true },
    "nanostores.min.js":        { module: true },
    "capnweb.min.js":           { module: true },
    "floating-ui-core.min.js":  { global: "FloatingUICore" },
    "floating-ui-dom.min.js":   { global: "FloatingUIDOM" },
    "uplot.min.js":             { global: "uPlot" },
    "flatbush.min.js":          { global: "Flatbush" },
    "comlink.min.js":           { global: "Comlink" },
    "pmtiles.min.js":           { global: "pmtiles" },
    "vlist.min.js":             { module: true },
  };

  /* Nothing here fails silently. An organ that could not start says so, on the
   * page, where the next person looks -- a swallowed exception is why a whole
   * shelf of these appeared to do nothing while every library loaded fine. */
  var FAILED = (window.bfOrganErrors = []);

  function vendor(name) {
    if (loaded[name]) return loaded[name];
    var how = VENDOR[name] || {};
    var url = "/assets/vendor-" + name;
    if (how.module) {
      loaded[name] = import(url).catch(function (why) {
        FAILED.push({ file: name, why: String(why).slice(0, 160) });
        return null;
      });
      return loaded[name];
    }
    loaded[name] = new Promise(function (resolve) {
      if (how.global && window[how.global]) return resolve(window[how.global]);
      var tag = document.createElement("script");
      tag.src = url;
      tag.async = false;
      tag.onload = function () {
        var got = how.global ? window[how.global] : true;
        if (!got) FAILED.push({ file: name, why: "loaded but set no " + how.global });
        resolve(got || null);
      };
      tag.onerror = function () {
        FAILED.push({ file: name, why: "did not load" });
        resolve(null);
      };
      document.head.appendChild(tag);
    });
    return loaded[name];
  }

  function organ(name, needs, start) {
    ORGANS.push({ name: name, needs: needs, start: start });
  }

  function each(selector, fn) {
    var found = document.querySelectorAll(selector);
    for (var i = 0; i < found.length; i++) fn(found[i]);
    return found.length;
  }

  /* ---------------------------------------------------------------- motion
   *
   * A list that changes without saying so makes the reader diff it themselves.
   * Every region, every continuation, every slot animates its own insertions,
   * removals and reorders -- which is most of what this estate does when an
   * htmx swap lands or a walk goes one page further. Reduced-motion is honoured
   * by the library itself, so a reader who asked for stillness gets it. */
  organ("motion", ["autoanimate.min.js"], function (mods) {
    var autoAnimate = mods[0] && (mods[0].default || mods[0].autoAnimate);
    if (typeof autoAnimate !== "function") return 0;
    return each([
      ".region", "ol.more", ".application-stream", ".application-board",
      ".source-roots", ".site-program-slot", "[data-bf-slot]", ".application-thread",
    ].join(","), function (node) {
      if (node.dataset.bfAnimated) return;
      node.dataset.bfAnimated = "1";
      try { autoAnimate(node); } catch (ignored) { /* a list that will not animate still reads */ }
    });
  });

  /* -------------------------------------------------------------- apertures
   *
   * This estate writes what it cannot infer beside almost everything it shows,
   * in `.src` and `.by` notes. Those sentences are the most valuable thing on
   * the page and they are also the easiest to scroll past. Anchored to the
   * thing they qualify, positioned so they never fall off the screen, they
   * become something a reader opens on purpose.
   *
   * The text is the server's own words. Nothing is summarised, shortened or
   * rephrased here: an aperture that paraphrased a boundary would be inventing
   * a weaker one. */
  organ("aperture", ["floating-ui-core.min.js", "floating-ui-dom.min.js"], function (mods) {
    var dom = mods[1];
    if (!dom || typeof dom.computePosition !== "function") return 0;
    var open = null;

    function shut() {
      if (open) { open.remove(); open = null; }
    }

    /* Dismissal is registered once, not once per note. Registering it inside
     * the loop gave every note its own listener closed over its own `note`, so
     * opening one aperture immediately fired eighteen others that each saw a
     * click on something that was not their note and shut it again. */
    document.addEventListener("click", function (event) {
      if (open && !open.contains(event.target)
          && !(event.target.classList && event.target.classList.contains("bf-aperture-source"))) {
        shut();
      }
    });
    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape") shut();
    });

    /* Every shape a boundary note takes on this estate. A note the reader
     * cannot open is a note the reader scrolls past. */
    return each([".src", ".by", ".site-program-empty", ".application-data > header > p",
                 "[data-bf-cannot-infer]"].join(","), function (note) {
      if (note.dataset.bfAperture) return;
      var words = (note.textContent || "").trim();
      if (words.length < 40) return;          /* a short note is already read */
      note.dataset.bfAperture = "1";
      note.classList.add("bf-aperture-source");
      note.setAttribute("tabindex", "0");
      note.setAttribute("role", "button");
      note.setAttribute("aria-label", "what this estate cannot infer from what it just showed");

      function show(event) {
        event.preventDefault();
        shut();
        var panel = document.createElement("div");
        panel.className = "bf-aperture";
        panel.setAttribute("role", "dialog");
        /* The server's sentence, moved, never rewritten. */
        panel.textContent = words;
        document.body.appendChild(panel);
        open = panel;
        dom.computePosition(note, panel, {
          placement: "top-start",
          middleware: [dom.offset(8), dom.flip(), dom.shift({ padding: 12 })],
        }).then(function (at) {
          panel.style.left = at.x + "px";
          panel.style.top = at.y + "px";
        });
      }

      note.addEventListener("click", show);
      note.addEventListener("keydown", function (event) {
        if (event.key === "Enter" || event.key === " ") show(event);
        if (event.key === "Escape") shut();
      });
    });
  });

  /* ------------------------------------------------------------------ state
   *
   * Islands need somewhere to agree without a framework and without becoming a
   * second copy of the estate. These are the only things the browser is allowed
   * to hold an opinion about: what the reader is looking at right now. Nothing
   * here is durable, shared, or true anywhere but this tab. */
  organ("state", ["nanostores.min.js"], function (mods) {
    var nano = mods[0];
    if (!nano || typeof nano.atom !== "function") return 0;
    window.bfState = {
      selected: nano.atom(null),      /* the subject under the cursor */
      aperture: nano.atom(null),      /* which boundary note is open */
      window: nano.atom(null),        /* the time or map extent being looked at */
      cannot_infer: "browser state is what this reader is looking at; it is never "
        + "durable, never shared, and never a fact about the estate",
    };
    return 1;
  });

  /* ------------------------------------------------------------------ time
   *
   * The estate accumulates occurrence history and has almost no way to show it.
   * Any element carrying `data-bf-series` -- a JSON array of [unix seconds,
   * value] pairs the server already computed -- becomes a real time chart.
   *
   * The server supplies the numbers. This draws them and nothing else: no
   * interpolation, no smoothing, no trend line, because a line drawn between
   * two observations is a claim nobody made. */
  organ("time", ["uplot.min.js"], function (mods) {
    var uPlot = mods[0];
    if (typeof uPlot !== "function") return 0;
    return each("[data-bf-series]", function (node) {
      if (node.dataset.bfPlotted) return;
      node.dataset.bfPlotted = "1";
      var rows;
      try { rows = JSON.parse(node.dataset.bfSeries); } catch (bad) { return; }
      if (!Array.isArray(rows) || rows.length < 2) return;
      var xs = rows.map(function (r) { return r[0]; });
      var ys = rows.map(function (r) { return r[1]; });
      try {
        new uPlot({
          width: node.clientWidth || 640,
          height: Number(node.dataset.bfHeight) || 180,
          title: node.dataset.bfTitle || "",
          series: [{}, { label: node.dataset.bfLabel || "observed", stroke: "currentColor" }],
          axes: [{}, {}],
          cursor: { drag: { x: true, y: false } },
        }, [xs, ys], node);
      } catch (ignored) { /* a chart that will not draw leaves the numbers in the DOM */ }
    });
  });

  /* ------------------------------------------------------------------ list
   *
   * A room that holds 37,159 pages cannot render 37,159 rows and stay usable,
   * and paginating them turns one question into forty requests. So the server
   * sends the rows it already knows and the browser keeps only the ones on
   * screen in the layout.
   *
   * This virtualizer measures and positions rows that already exist in the
   * document; it does not render them. That distinction is the whole reason it
   * is safe here: the room remains one server-rendered HTML document, readable
   * with no script at all, and the organ is an improvement to how much of it
   * the browser has to lay out at once. A crawler, a reader with scripting off
   * and a reader with it on all receive the same material.
   *
   *   <div data-bf-list data-bf-row-height="34">
   *     <div class="bf-row">...</div>   <- as many as the server sent
   *   </div>
   */
  organ("list", ["vlist.min.js"], function (mods) {
    var V = mods[0] && mods[0].Virtualizer;
    if (typeof V !== "function") return 0;
    return each("[data-bf-list]", function (node) {
      if (node.dataset.bfListed) return;
      var rows = [].slice.call(node.children), i;
      /* Below this there is nothing to win, and the cost is a scroll container
       * where the page had none. */
      if (rows.length < 60) return;
      node.dataset.bfListed = "1";

      var guess = Number(node.dataset.bfRowHeight) || rows[0].offsetHeight || 34;
      var tall = Number(node.dataset.bfHeight) || Math.min(640, guess * 14);
      var inner = document.createElement("div");
      node.style.overflow = "auto";
      node.style.height = tall + "px";
      node.style.position = "relative";
      inner.style.position = "relative";
      inner.style.width = "100%";
      for (i = 0; i < rows.length; i++) {
        rows[i].style.position = "absolute";
        rows[i].style.top = "0";
        rows[i].style.left = "0";
        rows[i].style.width = "100%";
        inner.appendChild(rows[i]);
      }
      node.appendChild(inner);

      var shown = [];
      var v = new V({
        count: rows.length,
        getScrollElement: function () { return node; },
        estimateSize: function () { return guess; },
        overscan: 8,
        scrollToFn: mods[0].elementScroll,
        observeElementRect: mods[0].observeElementRect,
        observeElementOffset: mods[0].observeElementOffset,
        onChange: draw,
      });

      function draw() {
        var items = v.getVirtualItems(), k, seen = Object.create(null);
        inner.style.height = v.getTotalSize() + "px";
        for (k = 0; k < items.length; k++) {
          var it = items[k], row = rows[it.index];
          seen[it.index] = 1;
          row.style.transform = "translateY(" + it.start + "px)";
          row.hidden = false;
          if (!row.dataset.bfMeasured) {
            row.dataset.bfMeasured = "1";
            v.measureElement(row);
          }
        }
        for (k = 0; k < shown.length; k++)
          if (!seen[shown[k]]) rows[shown[k]].hidden = true;
        shown = Object.keys(seen).map(Number);
      }

      v._willUpdate();
      draw();
      /* The count is stated where a reader can see it, because a list that
       * scrolls without saying how long it is hides its own size. */
      if (node.dataset.bfCount !== "0") {
        var tag = document.createElement("p");
        tag.className = "bf-list-count";
        tag.textContent = rows.length + " rows, laid out as you reach them";
        node.parentNode.insertBefore(tag, node.nextSibling);
      }
    });
  });

  /* ----------------------------------------------------------------- space
   *
   * A map that stops at fifty points is not a map of an estate holding
   * millions of observations. A packed Hilbert R-tree over what the server sent
   * lets the browser answer "what is in this viewport" locally, so panning does
   * not become a request. The index is over material already delivered; it
   * never reaches for more. */
  organ("space", ["flatbush.min.js"], function (mods) {
    var Flatbush = mods[0];
    if (typeof Flatbush !== "function") return 0;
    return each("[data-bf-points]", function (node) {
      if (node.dataset.bfIndexed) return;
      node.dataset.bfIndexed = "1";
      var points;
      try { points = JSON.parse(node.dataset.bfPoints); } catch (bad) { return; }
      if (!Array.isArray(points) || !points.length) return;
      var index = new Flatbush(points.length);
      for (var i = 0; i < points.length; i++) {
        index.add(points[i][0], points[i][1], points[i][0], points[i][1]);
      }
      index.finish();
      node.bfWithin = function (minX, minY, maxX, maxY) {
        return index.search(minX, minY, maxX, maxY).map(function (at) { return points[at]; });
      };
      node.dataset.bfIndexedCount = String(points.length);
    });
  });

  /* ---------------------------------------------------------------- offload
   *
   * Heavy reading belongs off the thread that has to stay responsive. This
   * exposes the worker bridge so a surface can hand work sideways; it starts no
   * worker on its own, because a page that spins up compute nobody asked for is
   * spending someone else's battery. */
  organ("offload", ["comlink.min.js"], function (mods) {
    var Comlink = mods[0];
    if (!Comlink || typeof Comlink.wrap !== "function") return 0;
    window.bfOffload = function (workerPath) {
      try { return Comlink.wrap(new Worker(workerPath)); }
      catch (ignored) { return null; }
    };
    return 1;
  });

  /* ------------------------------------------------------------------ tiles
   *
   * Map tiles read straight out of the estate's own object store by byte range,
   * so a planet-sized basemap needs no tile server and no request to us per
   * tile. Registered only if a map library is present to hand it to. */
  organ("tiles", ["pmtiles.min.js"], function (mods) {
    var pm = mods[0];
    if (!pm || typeof pm.PMTiles !== "function") return 0;
    window.bfTiles = function (url) {
      try { return new pm.PMTiles(url); } catch (ignored) { return null; }
    };
    if (window.maplibregl && pm.Protocol) {
      try {
        var protocol = new pm.Protocol();
        window.maplibregl.addProtocol("pmtiles", protocol.tile);
      } catch (ignored) { /* the map still draws whatever it already had */ }
    }
    return 1;
  });

  /* ------------------------------------------------------------ capability
   *
   * The estate's machine aperture answers JSON-RPC at /mcp. This exposes it as
   * something a surface can call, and the boundary travels with it: the
   * aperture is read-only, so what comes back is what the estate has published
   * and never permission to do anything. Holding this does not confer
   * authority; there is no authority at that address to confer. */
  organ("aperture-rpc", [], function (mods) {
    window.bfAsk = async function (tool, args) {
      var answer = await fetch("/mcp", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          jsonrpc: "2.0", id: Date.now(),
          method: "tools/call", params: { name: tool, arguments: args || {} },
        }),
      });
      var said = await answer.json();
      if (said.error) throw new Error(said.error.message + ": "
        + JSON.stringify(said.error.data || {}));
      return said.result;
    };
    window.bfAsk.cannot_infer = "this aperture reads what the estate published; it grants no "
      + "authority, performs no effect, and refuses every tool that would";
    return 1;
  });

  /* ------------------------------------------------------------------- run */

  function wake() {
    ORGANS.forEach(function (each_organ) {
      Promise.all(each_organ.needs.map(vendor)).then(function (mods) {
        try {
          var bound = each_organ.start(mods);
          if (bound) {
            (window.bfOrgans = window.bfOrgans || {})[each_organ.name] = bound;
          }
        } catch (why) {
          /* One organ failing leaves the others and the page alone -- but it
           * says so rather than vanishing. */
          FAILED.push({ organ: each_organ.name, why: String(why).slice(0, 160) });
        }
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", wake);
  } else {
    wake();
  }

  /* An htmx swap brings new markup that has never met these organs. Re-binding
   * is idempotent -- each organ marks what it has taken -- so this is cheap. */
  document.addEventListener("htmx:afterSwap", wake);
  document.addEventListener("bonfyre:composed", wake);
})();
