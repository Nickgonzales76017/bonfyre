export const needs = ['title', 'data'];
export function render(section, site, h) {
  if (!/^\/data\/[a-z][a-z0-9-]*\/$/.test(section.data)) throw Error('Workbook requires packaged review material.');
  return `<section id="${h.esc(section.id)}" style="padding:16px 24px;max-width:1800px;margin:auto">
    <a href="/">${h.esc(site.business.name)}</a>
    <h1>${h.esc(section.title)}</h1>
    <link rel="stylesheet" href="${h.esc(section.data)}workbook-client.css">
    <div data-workbook-config="${h.esc(section.data)}review.json" aria-label="Financial workbook">Opening review…</div>
    <script type="module" src="${h.esc(section.data)}workbook-client.js"></script>
  </section>`;
}
