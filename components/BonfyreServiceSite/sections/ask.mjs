// The request form on its own, for a page whose opening is given to something
// else (a search, a list) and asks further down. A page has one form, here or in its hero.
import { renderForm } from "../form.mjs";
export const needs = ["form"];
export function render(s, site, h) {
  return `<section class="py-16 sm:py-20 border-b border-surface-container-high bg-surface">
    <div class="max-w-2xl mx-auto px-4 sm:px-6 lg:px-8">
      ${s.title ? h.heading(s, "mb-8") : ""}
      ${renderForm(s.form, site, h)}
    </div>
  </section>`;
}
