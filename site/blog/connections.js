/* Progressive enhancement only; all 66 essays and links exist in static HTML. */
(() => {
  "use strict";
  const input = document.querySelector("[data-atlas-search]");
  const buttons = Array.from(document.querySelectorAll("[data-atlas-filter]"));
  const cards = Array.from(document.querySelectorAll("[data-essay]"));
  const groups = Array.from(document.querySelectorAll("[data-atlas-section]"));
  const status = document.querySelector("[data-atlas-status]");
  const empty = document.querySelector("[data-atlas-empty]");
  if (!input || !cards.length || !status) return;
  let family = "all";
  const update = () => {
    const query = input.value.trim().toLocaleLowerCase();
    let visible = 0;
    for (const card of cards) {
      const show = (family === "all" || card.dataset.family === family) &&
        (!query || (card.dataset.search || "").includes(query));
      card.hidden = !show;
      if (show) visible++;
    }
    for (const section of groups) {
      section.hidden = !Array.from(section.querySelectorAll("[data-essay]")).some(card => !card.hidden);
    }
    status.textContent = visible + " of " + cards.length + " essays shown";
    if (empty) empty.hidden = visible > 0;
    for (const button of buttons) button.setAttribute("aria-pressed", String(button.dataset.atlasFilter === family));
  };
  for (const button of buttons) button.addEventListener("click", () => {
    family = button.dataset.atlasFilter || "all";
    update();
  });
  input.addEventListener("input", update);
  update();
})();