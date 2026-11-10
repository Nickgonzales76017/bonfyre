/* Reading aids for a site's pages: copy a code block, show one of a set of
 * code blocks at a time, find a page by name or section, and mark the
 * section being read.  The page is complete without any of this.
 */
(function () {
  document.querySelectorAll(".code-copy").forEach(function (button) {
    button.addEventListener("click", function () {
      var code = button.closest(".code").querySelector("code");
      if (!navigator.clipboard) return;
      navigator.clipboard.writeText(code.textContent.replace(/\n$/, "")).then(function () {
        button.textContent = "Copied"; button.setAttribute("data-done", "");
        setTimeout(function () { button.textContent = "Copy"; button.removeAttribute("data-done"); }, 1600);
      });
    });
  });

  document.querySelectorAll(".code-set").forEach(function (set) {
    var blocks = Array.prototype.slice.call(set.querySelectorAll(".code"));
    if (blocks.length < 2) return;
    var show = function (chosen) {
      blocks.forEach(function (block, index) {
        block.hidden = index !== chosen;
        block.querySelectorAll(".code-set-tabs button").forEach(function (tab, at) {
          tab.setAttribute("aria-selected", at === chosen ? "true" : "false");
        });
      });
    };
    var names = blocks.map(function (block) { return block.querySelector("figcaption span").textContent; });
    blocks.forEach(function (block) {
      var label = block.querySelector("figcaption span"), tabs = document.createElement("div");
      tabs.className = "code-set-tabs"; tabs.setAttribute("role", "tablist");
      blocks.forEach(function (other, index) {
        var tab = document.createElement("button");
        tab.type = "button"; tab.setAttribute("role", "tab");
        tab.textContent = names[index];
        tab.addEventListener("click", function () { show(index); });
        tabs.appendChild(tab);
      });
      label.parentNode.replaceChild(tabs, label);
    });
    set.setAttribute("data-ready", "");
    show(0);
  });

  var find = document.querySelector(".manual-find input"), shelf = document.querySelector(".manual-pages");
  if (find && shelf) {
    var links = Array.prototype.slice.call(shelf.querySelectorAll("a")), none = shelf.querySelector(".manual-none");
    var groups = Array.prototype.slice.call(shelf.querySelectorAll("p:not(.manual-none)"));
    find.addEventListener("input", function () {
      var words = find.value.toLowerCase().split(/\s+/).filter(Boolean), shown = 0;
      links.forEach(function (link) {
        var text = link.getAttribute("data-find").toLowerCase();
        link.hidden = !words.every(function (word) { return text.indexOf(word) >= 0; });
        if (!link.hidden) shown++;
      });
      groups.forEach(function (group) {
        var next = group.nextElementSibling, any = false;
        while (next && next.tagName === "A") { if (!next.hidden) any = true; next = next.nextElementSibling; }
        group.hidden = !any;
      });
      none.hidden = shown > 0;
    });
    /* Sections that answer the question, by meaning as well as by words:
     * the site's own index (/_search, built from its published sections),
     * read in this browser by /assets/static-embed.mjs. Loaded on the first
     * question; a site without an index keeps the page-name filter alone. */
    var answers = document.createElement("div"), search = null, waiting = 0;
    answers.className = "manual-answers"; answers.hidden = true; answers.setAttribute("aria-live", "polite");
    shelf.insertBefore(answers, shelf.firstChild);
    var loadSearch = function () {
      if (search) return search;
      search = Promise.all([import("/assets/static-embed.mjs"), fetch("/_search/model.bfse"), fetch("/_search/index.bfsi"), fetch("/_search/passages.json")])
        .then(function (got) {
          if (!got[1].ok || !got[2].ok || !got[3].ok) throw new Error("no index");
          return Promise.all([got[1].arrayBuffer(), got[2].arrayBuffer(), got[3].json()]).then(function (bodies) {
            var lib = got[0], model = lib.loadStaticModel(bodies[0]);
            return lib.loadIndex(bodies[1], bodies[0]).then(function (index) {
              if (index.count !== bodies[2].passages.length) throw new Error("index and passages differ");
              return { lib: lib, model: model, index: index, passages: bodies[2].passages, kw: lib.keywordIndex(bodies[2].passages) };
            });
          });
        });
      search.catch(function () {});
      return search;
    };
    var ask = function () {
      var question = find.value.trim(), ticket = ++waiting;
      if (question.length < 3) { answers.hidden = true; answers.textContent = ""; return; }
      loadSearch().then(function (s) {
        if (ticket !== waiting) return;
        var seen = {}, shown = [];
        s.lib.hybridTop(question, s.model, s.index, s.kw, 12).forEach(function (hit) {
          var p = s.passages[hit[1]], key = p.route + "#" + p.anchor;
          if (hit[0] < 0.35 || seen[key] || shown.length >= 5) return;
          seen[key] = 1; shown.push(p);
        });
        answers.textContent = "";
        if (!shown.length) { answers.hidden = true; return; }
        var head = document.createElement("p"); head.textContent = "Sections that answer this"; answers.appendChild(head);
        shown.forEach(function (p) {
          var a = document.createElement("a"), title = document.createElement("strong"), text = document.createElement("span");
          a.href = p.route + (p.anchor ? "#" + p.anchor : "");
          title.textContent = p.heading && p.heading !== p.page ? p.page + " · " + p.heading : p.page;
          text.textContent = p.excerpt.length > 140 ? p.excerpt.slice(0, 140).replace(/\s\S*$/, "") + "…" : p.excerpt;
          a.appendChild(title); a.appendChild(text); answers.appendChild(a);
        });
        answers.hidden = false; none.hidden = true;
      }, function () { answers.hidden = true; });
    };
    var timer = 0;
    find.addEventListener("input", function () { clearTimeout(timer); timer = setTimeout(ask, 180); });

    find.addEventListener("keydown", function (event) {
      var first = links.filter(function (link) { return !link.hidden; })[0];
      if (event.key === "Enter" && first && find.value) location.href = first.getAttribute("href");
      if (event.key === "Escape") { find.value = ""; find.dispatchEvent(new Event("input")); find.blur(); }
    });
    document.addEventListener("keydown", function (event) {
      var tag = document.activeElement && document.activeElement.tagName;
      if (event.key === "/" && tag !== "INPUT" && tag !== "TEXTAREA") { event.preventDefault(); find.focus(); }
    });
  }

  var marks = Array.prototype.slice.call(document.querySelectorAll(".manual-outline a"));
  if (marks.length && "IntersectionObserver" in window) {
    var watch = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (!entry.isIntersecting) return;
        marks.forEach(function (mark) {
          if (mark.getAttribute("href") === "#" + entry.target.id) mark.setAttribute("data-here", ""); else mark.removeAttribute("data-here");
        });
      });
    }, { rootMargin: "-15% 0px -70% 0px" });
    document.querySelectorAll(".manual-article section[id]").forEach(function (section) { watch.observe(section); });
  }
})();
