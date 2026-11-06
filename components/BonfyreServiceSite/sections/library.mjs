// Search and read a large collection. The page holds none of it: it fetches
// small compressed pieces as the visitor searches, browses a kind, or opens an
// entry, and says how much it has fetched. With nothing on file the section is absent.
export const needs = ["title"];
export const present = s => s.count > 0 && Boolean(s.data);
export function render(s, site, h) {
  const n = Number(s.count).toLocaleString("en-US");
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-12 sm:py-16 border-b border-surface-container-high bg-surface-container-lowest">
    <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8" data-library="${h.esc(s.data)}" data-noun="${h.esc(s.noun ?? "entries")}"${s.meter ? ' data-meter="yes"' : ""}>
      ${h.heading(s, "mb-8")}
      <form role="search" class="relative" data-lib-search>
        <label for="lib-q-${h.esc(s.id)}" class="sr-only">Search ${n} ${h.esc(s.noun ?? "entries")}</label>
        ${h.icon("search", "absolute left-3.5 top-1/2 -translate-y-1/2 text-outline text-xl")}
        <input id="lib-q-${h.esc(s.id)}" type="search" autocomplete="off" enterkeyhint="search" placeholder="Search ${n} ${h.esc(s.noun ?? "entries")}" class="w-full bg-surface-container border border-surface-container-high focus:border-secondary focus:ring-1 focus:ring-secondary text-display placeholder:text-outline text-base rounded-xl pl-11 pr-4 py-3.5 outline-none" />
      </form>
      <div class="mt-4" data-lib-kinds>
        <span class="block text-xs font-mono text-on-surface-variant mb-2 uppercase tracking-wider">${h.esc(s.browse ?? "Or browse by kind")}</span>
        <div class="flex flex-wrap gap-2" data-lib-chips></div>
      </div>
      <p class="mt-6 text-sm text-on-surface-variant" data-lib-status aria-live="polite"></p>
      <ul class="mt-2 divide-y divide-surface-container-high border-y border-surface-container-high empty:border-0" data-lib-list></ul>
      <button type="button" data-lib-more class="hidden mt-4 w-full min-h-[44px] rounded-lg border border-surface-container-highest bg-surface-container-high text-display text-sm font-semibold">Show more</button>
      <article class="hidden mt-6 bg-surface-container border border-surface-container-highest rounded-2xl p-5 sm:p-8" data-lib-entry tabindex="-1"></article>
      <p class="mt-6 text-xs text-on-surface-variant leading-relaxed" data-lib-fetched></p>
      ${s.note ? `<p class="mt-2 text-xs text-on-surface-variant leading-relaxed">${h.esc(s.note)}</p>` : ""}
      <noscript><p class="mt-4 text-sm text-on-surface-variant">Searching this collection needs scripting turned on in your browser.</p></noscript>
    </div>
    <script>
    document.querySelectorAll("[data-library]").forEach(function (box) {
      var base = box.dataset.library, noun = box.dataset.noun, q = function (sel) { return box.querySelector(sel); };
      var list = q("[data-lib-list]"), status = q("[data-lib-status]"), more = q("[data-lib-more]"), entry = q("[data-lib-entry]"), chips = q("[data-lib-chips]"), input = q("input[type=search]");
      var about = null, fetched = 0, pieces = 0, held = {}, shown = [], upto = 0, turn = 0, pending = null;
      var text = function (tag, cls, words) { var e = document.createElement(tag); if (cls) e.className = cls; e.textContent = words; return e; };
      var count = function (n) { return Number(n).toLocaleString("en-US"); };
      var say = function () { if (box.dataset.meter !== "yes") return; q("[data-lib-fetched]").textContent = about ? "All " + count(about.count) + " " + noun + " are here to search. To show you this, your device has fetched " + (fetched < 1048576 ? Math.max(1, Math.round(fetched / 1024)) + " KB" : (fetched / 1048576).toFixed(1) + " MB") + " in " + pieces + (pieces === 1 ? " piece." : " pieces.") : ""; };
      var piece = function (path) {
        if (held[path]) return held[path];
        return held[path] = fetch(base + path).then(function (answer) {
          if (!answer.ok) throw new Error("missing");
          return answer.arrayBuffer();
        }).then(function (bytes) {
          fetched += bytes.byteLength; pieces++; say();
          if (!/\\.gz$/.test(path)) return JSON.parse(new TextDecoder().decode(bytes));
          return new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"))).json();
        }).catch(function (error) { delete held[path]; throw error; });
      };
      var fail = function () { status.textContent = "That could not be fetched just now. Check your connection and try again."; };
      var draw = function (reset) {
        if (reset) { list.textContent = ""; upto = 0; }
        shown.slice(upto, upto + 40).forEach(function (row) {
          var li = document.createElement("li"), a = text("a", "flex items-center min-h-[48px] py-2 text-display hover:text-secondary", row[1]);
          a.href = "#" + box.closest("section").id + "=" + row[0]; li.appendChild(a); list.appendChild(li);
        });
        upto = Math.min(shown.length, upto + 40);
        more.classList.toggle("hidden", upto >= shown.length);
      };
      var show = function (rows, words) { shown = rows; entry.classList.add("hidden"); list.classList.remove("hidden"); status.textContent = rows.length ? count(rows.length) + " " + words : "Nothing matches. Try another word."; draw(true); };
      var search = function () {
        pending = null;
        var words = input.value.toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, " ").trim().split(" ").filter(Boolean), mine = ++turn;
        var long = words.filter(function (w) { return w.length >= 3 && (about.common || []).indexOf(w) < 0; });
        if (!long.length) { shown = []; list.textContent = ""; more.classList.add("hidden"); status.textContent = !input.value.trim() ? "" : words.some(function (w) { return w.length >= 3; }) ? "Add a word that is not in nearly every title." : "Type at least three letters."; return; }
        var prefix = long[0].slice(0, 3);
        if ((" " + about.words + " ").indexOf(" " + prefix + " ") < 0) return show([], "");
        status.textContent = "Searching…";
        piece("w/" + prefix + ".json.gz").then(function (rows) {
          if (mine !== turn) return;
          show(rows.filter(function (row) { var title = " " + row[1].toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, " ") + " "; return words.every(function (w) { return title.indexOf(" " + w) >= 0; }); }), "matching “" + input.value.trim() + "”");
        }).catch(fail);
      };
      var kind = function (group, at, rows) {
        var mine = ++turn; status.textContent = "Fetching…";
        piece("k/" + group[0] + "/" + at + ".json.gz").then(function (page) {
          if (mine !== turn) return;
          rows = rows.concat(page);
          var words = "of " + count(group[2]) + " in “" + group[1] + "”";
          // A large kind arrives a page at a time: the next is fetched when the visitor reaches the end of this one.
          pending = rows.length < group[2] && page.length ? function () { kind(group, at + 1, rows); } : null;
          if (at === 0) show(rows, words); else { shown = rows; status.textContent = count(rows.length) + " " + words; draw(false); }
          more.classList.toggle("hidden", upto >= shown.length && !pending);
        }).catch(fail);
      };
      var open = function (n) {
        var mine = ++turn; status.textContent = "Fetching…";
        piece("e/" + Math.floor(n / about.run) + ".json.gz").then(function (run) {
          var e = run[n % about.run]; if (mine !== turn || !e) return;
          entry.textContent = "";
          var back = text("button", "min-h-[44px] mb-3 text-sm font-semibold text-secondary", "← Back to the list"); back.type = "button";
          back.addEventListener("click", function () { history.replaceState(null, "", location.pathname + location.search); entry.classList.add("hidden"); list.classList.remove("hidden"); more.classList.toggle("hidden", upto >= shown.length); status.textContent = shown.length ? count(shown.length) + " shown" : ""; input.focus({ preventScroll: true }); });
          entry.appendChild(back);
          entry.appendChild(text("h3", "font-headline font-bold text-2xl text-display", e[1]));
          if (e[2].length || e[3]) entry.appendChild(text("p", "mt-1 text-sm text-on-surface-variant", [e[2].join(", "), e[3]].filter(Boolean).join(" · ")));
          var ways = e[8] || [];
          if (e[4].length) { entry.appendChild(text("h4", "mt-6 mb-2 text-xs font-mono uppercase tracking-wider text-secondary", ways.length ? "The base, in every one" : "What goes in")); var ul = document.createElement("ul"); ul.className = "space-y-1.5 text-display"; e[4].forEach(function (line) { ul.appendChild(text("li", "", line)); }); entry.appendChild(ul); }
          if (e[5]) { entry.appendChild(text("h4", "mt-6 mb-2 text-xs font-mono uppercase tracking-wider text-secondary", "What to do")); entry.appendChild(text("p", "text-on-surface leading-relaxed whitespace-pre-line", e[5])); }
          // A family: each way is the base plus what that way adds, opened one at a time.
          if (ways.length) {
            entry.appendChild(text("h4", "mt-8 mb-2 text-xs font-mono uppercase tracking-wider text-secondary", ways.length + " ways to finish it"));
            ways.forEach(function (way, i) {
              var d = document.createElement("details"); d.className = "border-t border-surface-container-high py-1"; if (!i) d.open = true;
              var sum = document.createElement("summary"); sum.className = "list-none [&::-webkit-details-marker]:hidden cursor-pointer min-h-[48px] py-2 flex items-center justify-between gap-3";
              sum.appendChild(text("span", "font-headline font-bold text-lg text-display", way[0])); sum.appendChild(text("span", "text-xs font-mono text-on-surface-variant shrink-0", way[1].length ? "adds " + way[1].length : "nothing added"));
              d.appendChild(sum);
              if (way[1].length) { var add = document.createElement("ul"); add.className = "space-y-1 text-display mb-3"; way[1].forEach(function (line) { add.appendChild(text("li", "", "+ " + line)); }); d.appendChild(add); }
              if (way[3]) d.appendChild(text("p", "text-sm text-on-surface-variant mb-2", way[3]));
              if (way[2]) d.appendChild(text("p", "text-on-surface leading-relaxed whitespace-pre-line mb-3", way[2]));
              entry.appendChild(d);
            });
          }
          if (e[6].length) { entry.appendChild(text("h4", "mt-6 mb-2 text-xs font-mono uppercase tracking-wider text-secondary", "The source also says")); e[6].forEach(function (line) { entry.appendChild(text("p", "text-sm text-on-surface-variant", line)); }); }
          if (e[7]) entry.appendChild(text("p", "mt-6 pt-4 border-t border-surface-container-high text-xs text-on-surface-variant", e[7]));
          list.classList.add("hidden"); more.classList.add("hidden"); entry.classList.remove("hidden"); status.textContent = ""; entry.focus({ preventScroll: true }); entry.scrollIntoView({ block: "start" });
        }).catch(fail);
      };
      var route = function () { var m = new RegExp("^#" + box.closest("section").id + "=(\\\\d+)$").exec(location.hash); if (m && about && Number(m[1]) < about.count) open(Number(m[1])); };
      var wait; input.addEventListener("input", function () { clearTimeout(wait); wait = setTimeout(search, 220); });
      q("[data-lib-search]").addEventListener("submit", function (event) { event.preventDefault(); clearTimeout(wait); search(); input.blur(); });
      more.addEventListener("click", function () { if (upto < shown.length) { draw(false); more.classList.toggle("hidden", upto >= shown.length && !pending); } else if (pending) pending(); });
      window.addEventListener("hashchange", route);
      piece("about.json").then(function (found) {
        about = found; say();
        var draws = function (all) {
          chips.textContent = "";
          about.kinds.slice(0, all ? about.kinds.length : 18).forEach(function (group) {
            var b = text("button", "min-h-[44px] px-3.5 rounded-full border border-surface-container-high bg-surface-container text-sm text-on-surface hover:border-secondary", group[1] + " " + count(group[2])); b.type = "button";
            b.addEventListener("click", function () { input.value = ""; kind(group, 0, []); }); chips.appendChild(b);
          });
          if (!all && about.kinds.length > 18) { var rest = text("button", "min-h-[44px] px-3.5 rounded-full border border-dashed border-surface-container-highest text-sm text-secondary", "All " + count(about.kinds.length) + " kinds"); rest.type = "button"; rest.addEventListener("click", function () { draws(true); }); chips.appendChild(rest); }
        };
        draws(false); route();
      }).catch(fail);
    });
    </script>
  </section>`;
}
