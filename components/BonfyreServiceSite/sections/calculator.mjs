// A rule-of-thumb sum the visitor works for themselves: a share of the finished
// value, less repairs. It is arithmetic on the visitor's own numbers and is
// labelled as that; nothing typed leaves the page.
export const needs = ["title"];
export function render(s, site, h) {
  const share = Number.isFinite(s.share) ? s.share : 70;
  const field = (key, label, value) => `<div><label for="sum-${key}" class="block text-xs font-mono text-on-surface-variant mb-1 uppercase tracking-wider">${h.esc(label)}</label>
            <input id="sum-${key}" data-sum="${key}" type="number" inputmode="numeric" min="0" step="1000" value="${value}" class="w-full bg-surface-container-lowest border border-surface-container-high focus:border-secondary focus:ring-1 focus:ring-secondary text-display text-base rounded-lg px-4 py-3.5 outline-none font-mono" /></div>`;
  return `<section id="${h.esc(s.id)}" class="scroll-mt-20 py-16 sm:py-20 border-b border-surface-container-high bg-surface">
    <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8">
      ${h.heading(s, "mb-10")}
      <div class="bg-surface-container border border-surface-container-highest rounded-2xl p-5 sm:p-8" data-sum-box data-share="${share}">
        <div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
          ${field("value", s.value_label ?? "Value after repairs ($)", "")}
          ${field("repairs", s.repairs_label ?? "Repairs ($)", "")}
          ${field("share", "Share of value (%)", share)}
        </div>
        <p class="mt-6 text-sm text-on-surface-variant">${h.esc(s.result_label ?? "Most to pay")}: <output data-sum-out class="block font-mono font-bold text-3xl text-secondary mt-1" aria-live="polite">—</output></p>
        <p class="mt-4 text-xs text-on-surface-variant leading-relaxed">${h.esc(s.note ?? "Value after repairs × share, less repairs. A rule of thumb on your own numbers, not an offer or a valuation.")}</p>
      </div>
    </div>
    <script>
    document.querySelectorAll("[data-sum-box]").forEach(function (box) {
      var get = function (k) { return parseFloat(box.querySelector("[data-sum=" + k + "]").value); }, out = box.querySelector("[data-sum-out]");
      var work = function () { var v = get("value"), r = get("repairs") || 0, s = get("share"); out.textContent = v > 0 && s > 0 && s <= 100 ? "$" + Math.max(0, Math.round(v * s / 100 - r)).toLocaleString("en-US") : "—"; };
      box.addEventListener("input", work);
    });
    </script>
  </section>`;
}
