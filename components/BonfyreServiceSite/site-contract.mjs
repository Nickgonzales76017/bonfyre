// Authored service-site contract; refuses invalid content before composition.
import { publicOrigin } from "./seo.mjs";
import { siteHref } from "./navigation.mjs";
import { checkForm } from "./form.mjs";
import { kinds } from "./section-registry.mjs";

// The home page is the record's own sections; further pages are listed under pages.
export const pagesOf = content => [{ path: "", menu: content.home_menu ?? "Home", sections: content.sections }, ...(content.pages ?? [])];

// Everything that would make a broken or dishonest page is refused here, by name.
export function validate(content) {
  const fail = why => { throw new Error(`site record: ${why}`); };
  if (content.schema !== "service-site.v1") fail(`schema is "${content.schema}", expected service-site.v1`);
  if (!/^[a-z0-9][a-z0-9-]{1,56}$/.test(content.site_id ?? "")) fail("site_id must be lower-case letters, digits and hyphens");
  if (!content.business?.name) fail("business.name is required");
  // A business shows the ways to reach it that it has. A phone and a name are optional; an email is needed
  // unless the site keeps requests itself, because otherwise a request would have nowhere to go.
  const c = content.contact ?? {};
  const digits = (c.phone ?? "").replace(/\D/g, "");
  if (c.phone && digits.length !== 10) fail("contact.phone must be a ten-digit US number");
  if (c.email && !/^[^@\s"'<>\\]+@[^@\s"'<>\\]+\.[^@\s"'<>\\]+$/.test(c.email)) fail("contact.email is not an email address");
  if (!c.email && content.leads !== "server") fail("contact.email is required when requests are sent by mail");
  if (!content.action) fail("action (the words on the main button) is required");
  if (!["mail", "server", undefined].includes(content.leads)) fail(`leads is "${content.leads}"; it is "mail" or "server"`);
  publicOrigin(content);
  if(content.navigation){if(!Array.isArray(content.navigation)||content.navigation.length>12)fail("navigation must contain at most twelve links");for(const link of content.navigation){if(!link.label||typeof link.label!=="string")fail("navigation label required");siteHref(link.href);}}
  const paths = new Set(), formIds = new Set();
  for (const page of pagesOf(content)) {
    const where = page.path ? `page "${page.path}"` : "the home page", bad = why => fail(`${where}: ${why}`);
    if (page.path && (!/^[a-z][a-z0-9-]*$/.test(page.path) || ["assets", "api", "data"].includes(page.path))) bad("path must be lower-case letters, digits and hyphens");
    if (paths.has(page.path)) bad("path is repeated");
    paths.add(page.path);
    if (page.path && (!page.title || !page.menu)) bad("needs a title and a menu label");
    const sections = page.sections;
    if (!Array.isArray(sections) || !sections.length) bad("must list at least one section");
    if (page.layout && page.layout !== "workspace") bad("unknown page layout");
    if (page.layout === "workspace" && (!page.path || sections.length !== 1 || !['catalogue','planner','creator','workbook','shipments','simulation','synthesis','schema','plan','pricing','opportunity'].includes(sections[0].kind))) bad("a workspace needs its own page and one catalogue, planner, creator, workbook, shipments, simulation, synthesis, schema, plan, pricing or opportunity section");
    if (page.layout !== "workspace" && sections[0].kind !== "hero") bad("the first section must be the hero");
    const withForm = sections.filter(s => s.form);
    if (withForm.length > 1) bad("a page has one request form");
    if (!page.path && !withForm.length) bad("the home page needs the request form, in its hero or in an ask section");
    const ids = new Set();
    sections.forEach((s, i) => {
      const kind = kinds[s.kind];
      if (!kind) bad(`section ${i + 1} is of kind "${s.kind}"; the kit has ${Object.keys(kinds).sort().join(", ")}`);
      // What a section takes as material is filled at build and checked then; the record is not asked to state it.
      for (const need of kind.needs) if ((s[need] == null || s[need].length === 0) && !(s.material && need !== "title")) bad(`section ${i + 1} (${s.kind}) needs ${need}`);
      if (s.material && !/^[a-zA-Z][a-zA-Z0-9]*$/.test(s.material)) bad(`section ${i + 1} (${s.kind}) names material "${s.material}"`);
      if (s.kind === "hero" && i > 0) bad("only one hero");
      if (s.form && !["hero", "ask"].includes(s.kind)) bad(`section ${i + 1} (${s.kind}) carries a form; the form belongs to the hero or to an ask section`);
      if (s.review) {
        if (s.kind !== 'catalogue' || s.review !== true || content.leads !== 'server' || !s.id) bad('catalogue review requires a named catalogue section and server intake');
        const reviewId = 'catalogue-' + s.id;
        if (formIds.has(reviewId)) bad('catalogue review form id is repeated');
        formIds.add(reviewId);
      }
      if (s.kind === 'planner') {
        if (content.leads !== 'server' || !s.id || page.layout !== 'workspace') bad('a planner requires its own named workspace and server intake');
        const plannerId='planner-'+s.id;
        if(formIds.has(plannerId))bad('planner form id is repeated');
        formIds.add(plannerId);
      }
      if (s.kind === "ask" && s.id) bad("an ask section is reached at #ask and takes no id of its own");
      if (s.id) { if (!/^[a-z][a-z0-9-]*$/.test(s.id) || s.id === "ask" || ids.has(s.id)) bad(`section ${i + 1} id "${s.id}" is not usable or is repeated`); ids.add(s.id); }
      if (s.menu && !s.id) bad(`section ${i + 1} (${s.kind}) is in the menu but has no id`);
      if (s.kind === "person" && !c.name) bad("the person section needs contact.name");
    });
    const form = withForm[0]?.form;
    if (form) { checkForm(form, bad); if (formIds.has(form.id)) bad(`form id "${form.id}" is used on another page`); formIds.add(form.id); }
  }
  return digits;
}
