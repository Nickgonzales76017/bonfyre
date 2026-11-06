/* Existing navigation routes receive their live composition here.
 *
 * This is a Web-lowering adapter, not another app: the addressed page keeps
 * its donor grammar and visual identity. Native WebAddress resolves the
 * browser's presentation class; the external-estate lane supplies only the
 * same bounded public candidate reservoir for every class. The browser never
 * selects an actor, audience, role, provider, or effect.
 */
(() => {
  const current = document.currentScript;
  const grammar = current?.dataset?.bonfyreGrammar;
  if (!grammar) return;

  const allowed = new Set(["q", "kind", "world", "domain", "affordance", "source_kind", "capability", "near", "state", "time", "relation", "context", "mode"]);
  const params = new URLSearchParams();
  for (const [key, value] of new URLSearchParams(location.search)) if (allowed.has(key) && value) params.set(key, value);
  params.set("extent", "section");

  const grammarPaths = Object.freeze({
    discover: "/discover", stage: "/stage", channels: "/channels", community: "/community",
    board: "/board", spatial: "/parcels", listings: "/listings", inbox: "/inbox",
    territory: "/territory", observations: "/observations", worlds: "/w/regional-storm",
    packs: "/packs", link: "/link", studio: "/studio", console: "/console",
  });
  const text = (tag, value, className) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    node.textContent = String(value || "");
    return node;
  };
  const el = (tag, className) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    return node;
  };
  const sourceTags = (record) => [
    ...(record.projection?.candidate_domains || []).slice(0, 2),
    ...(record.projection?.candidate_affordances || []).slice(0, 2),
  ];
  const contextualLink = (path) => {
    const link = document.createElement("a");
    const shape = new URLSearchParams(params);
    shape.delete("extent");
    link.href = shape.size ? `${path}?${shape}` : path;
    return link;
  };

  const main = document.querySelector("main") || document.body;
  const root = el("section", "bf-live-composition");
  root.setAttribute("aria-live", "polite");
  root.append(text("p", "Compiling this addressed surface from native closure and the current external candidate reservoir…", "bf-live-status"));
  main.prepend(root);

  const style = document.createElement("style");
  style.textContent = ".bf-live-composition{content-visibility:auto;contain-intrinsic-size:360px;margin:0 auto 1rem;max-width:min(1180px,calc(100% - 2rem));padding:1rem 1.15rem;border:1px solid color-mix(in srgb,currentColor 22%,transparent);border-radius:12px;background:color-mix(in srgb,Canvas 90%,currentColor 10%);color:CanvasText;font:14px/1.45 system-ui,sans-serif}.bf-live-kicker{margin:0;color:color-mix(in srgb,currentColor 58%,transparent);font:600 11px/1.2 ui-monospace,monospace;letter-spacing:.09em;text-transform:uppercase}.bf-live-title{margin:.4rem 0;font-size:clamp(1.1rem,2vw,1.55rem);letter-spacing:-.025em}.bf-live-note{margin:.35rem 0;color:color-mix(in srgb,currentColor 68%,transparent)}.bf-live-layout{display:grid;grid-template-columns:minmax(0,1fr) minmax(220px,.55fr);gap:.9rem;margin-top:.8rem}.bf-live-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.6rem}.bf-live-card,.bf-live-closure{padding:.75rem;border:1px solid color-mix(in srgb,currentColor 16%,transparent);border-radius:8px;background:color-mix(in srgb,Canvas 94%,currentColor 6%)}.bf-live-card h3{font-size:.94rem;margin:.3rem 0}.bf-live-card p,.bf-live-closure p{margin:.2rem 0;color:color-mix(in srgb,currentColor 68%,transparent);font-size:.82rem}.bf-live-card a,.bf-live-neighbors a{color:inherit;font-weight:700}.bf-live-meta,.bf-live-neighbors{display:flex;gap:.4rem;flex-wrap:wrap;margin-top:.45rem}.bf-live-meta span,.bf-live-neighbors a{font:10px/1.2 ui-monospace,monospace;padding:.22rem .4rem;border:1px solid color-mix(in srgb,currentColor 20%,transparent);border-radius:99px}.bf-live-neighbors a{text-decoration:none}";
  document.head.append(style);

  Promise.all([
    fetch(`/_surface/presentation-class?grammar=${encodeURIComponent(grammar)}`, { headers: { Accept: "application/json" }, credentials: "same-origin" }),
    fetch(`/external-estate/v1/surfaces/${encodeURIComponent(grammar)}?${params}`, { headers: { Accept: "application/json" }, credentials: "same-origin" }),
    fetch("/external-estate/v1/presentation-cells", { headers: { Accept: "application/json" }, credentials: "same-origin" }),
  ]).then(async ([closureResponse, sliceResponse, cellsResponse]) => {
    if (!closureResponse.ok) throw new Error(`native closure ${closureResponse.status}`);
    if (!sliceResponse.ok) throw new Error(`candidate slice ${sliceResponse.status}`);
    if (!cellsResponse.ok) throw new Error(`presentation algebra ${cellsResponse.status}`);
    const [closure, slice, algebra] = await Promise.all([closureResponse.json(), sliceResponse.json(), cellsResponse.json()]);
    const cell = (algebra.cells || []).find((candidate) => candidate.grammar === grammar && candidate.presentation_class === closure.presentation_class);
    if (!cell) throw new Error("resolved presentation cell unavailable");

    root.replaceChildren();
    root.append(text("p", `${grammar} · ${closure.presentation_class} · ${slice.resolved_territory}`, "bf-live-kicker"));
    root.append(text("h2", cell.question, "bf-live-title"));
    const reservoir = Number(slice.candidate_reservoir_count || slice.candidate_count || 0);
    const relevant = Number(slice.candidate_count || 0);
    root.append(text("p", `${relevant} source-derived candidates overlap this grammar's declared demand${reservoir !== relevant ? `, selected from ${reservoir} candidates in the addressed reservoir` : ""}. ${cell.experience_character}`, "bf-live-note"));

    const layout = el("div", "bf-live-layout");
    const candidates = el("div", "bf-live-grid");
    const records = (slice.candidates || []).slice(0, 3);
    if (!records.length) candidates.append(text("p", cell.empty_character, "bf-live-note"));
    for (const record of records) {
      const card = el("article", "bf-live-card");
      card.append(text("p", `supplied candidate · ${record.source_kind} · ${record.source_ref}`));
      card.append(text("h3", record.observed_name));
      card.append(text("p", record.section));
      const inspect = document.createElement("a");
      inspect.href = `/external-estate/occurrences/${encodeURIComponent(record.id)}`;
      inspect.textContent = "Inspect supplied source";
      card.append(inspect);
      const meta = el("div", "bf-live-meta");
      for (const value of sourceTags(record)) meta.append(text("span", value));
      for (const value of record.surface_relevance?.matched_demand_affordances || []) meta.append(text("span", `demand:${String(value).toLowerCase()}`));
      card.append(meta);
      candidates.append(card);
    }
    layout.append(candidates);

    const closurePanel = el("aside", "bf-live-closure");
    closurePanel.append(text("p", "This page is a resolved presentation cell", "bf-live-kicker"));
    closurePanel.append(text("p", `Renderer: ${cell.preferred_renderers.join(" · ")}`));
    closurePanel.append(text("p", `Visible here: ${cell.visible_sections.join(" · ")}`));
    closurePanel.append(text("p", `External demand: ${cell.external_demand_roles.join(" · ")}`));
    if (slice.surface_demand?.world_demand) {
      const world = slice.surface_demand.world_demand;
      closurePanel.append(text("p", `World demand: ${world.requested} → ${world.resolved} (${world.resolution.replaceAll("_", " ")})`));
    }
    closurePanel.append(text("p", `Closure prerequisites: ${cell.requires.join(" · ")}`));
    layout.append(closurePanel);
    root.append(layout);

    const neighbors = el("nav", "bf-live-neighbors");
    for (const neighbor of cell.navigation_neighbors || []) {
      const path = grammarPaths[neighbor];
      if (!path || neighbor === grammar) continue;
      const link = contextualLink(path);
      link.textContent = `Open ${neighbor}`;
      neighbors.append(link);
    }
    if (neighbors.children.length) root.append(neighbors);
    root.append(text("p", "Changing presentation class changes projection, never candidate identity. Candidate material is not provider admission, evidence, access, payment, settlement, or an effect; InformationPolicy and native owners still decide every further move.", "bf-live-note"));
  }).catch((error) => {
    root.replaceChildren(text("p", `This route’s live composition is unavailable (${error.message}). No candidate or closure was invented.`, "bf-live-status"));
  });
})();
