(() => {
  const search = document.getElementById("journal-search");
  const cards = [...document.querySelectorAll("[data-article]")];
  const filters = [...document.querySelectorAll("[data-filter]")];
  const count = document.getElementById("journal-result-count");
  const empty = document.getElementById("journal-empty");
  if (!search || !count || !empty) return;
  let category = "All";
  const apply = () => {
    const q = search.value.trim().toLowerCase();
    let shown = 0;
    for (const card of cards) {
      const matched = (category === "All" || card.dataset.category === category) &&
        (!q || card.dataset.search.includes(q));
      card.hidden = !matched;
      if (matched) shown += 1;
    }
    count.textContent = "Showing " + shown + " of " + cards.length + " articles";
    empty.hidden = shown !== 0;
  };
  for (const button of filters) button.addEventListener("click", () => {
    category = button.dataset.filter;
    for (const other of filters) {
      const active = other === button;
      other.classList.toggle("active", active);
      other.setAttribute("aria-pressed", String(active));
    }
    apply();
  });
  search.addEventListener("input", apply);
})();