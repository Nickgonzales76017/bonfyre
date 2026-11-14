/** The estate's own semantic classes, styled by Tailwind.
 *
 * The pages name what a thing is -- a play control, a way on, a provenance
 * note -- and never carry utility classes in their markup, because the markup
 * is composed in C and read by people. Tailwind supplies the design behind
 * those names through @apply, and its own tokens stay available for anything
 * authored by hand.
 *
 * Content is the served markup itself: C sources (class attributes behind
 * C escapes), authoring templates and HTML shells. The transform reads only
 * class attribute values, so C keywords never become utilities. The semantic
 * layer below is safelisted whole: it is the estate's design vocabulary and
 * ships even where no current page names a word of it.
 */
const sources = [
  "../../cmd/BonfyreWebAddress/src/**/*.{h,c}",
  "../../cmd/*/src/**/*.{h,c}",
  "../../components/BonfyreServiceSite/**/*.mjs",
  "../../integrations/browser/*.mjs",
  "../../site/matrix/*.html",
];
const classValuesOnly = (content) => {
  const out = [];
  const re = /class=\\?["']([^"']*)\\?["']/g;
  let m;
  while ((m = re.exec(content))) out.push(m[1].replace(/\\/g, " "));
  return out.join(" ");
};
module.exports = {
  content: {
    files: process.env.BONFYRE_STYLE_LAYER === "1" ? [] : sources,
    transform: Object.fromEntries(
      ["h", "c", "mjs", "js", "html"].map((ext) => [ext, classValuesOnly])
    ),
  },
  safelist: [
    "button", "lift", "play-on-page", "see-all", "keep-reading", "deck",
    "pager", "ledger-find", "src", "terms", "subj-note", "eyebrow",
    "region-label", "kicker", "np-kicker", "by", "flow", "item-body",
    "lede", "primary",
  ],
  darkMode: ["variant", "&:where([data-theme='dusk'], [data-theme='dusk'] *)"],
  theme: { extend: {} },
  plugins: [require("@tailwindcss/typography"), require("@tailwindcss/forms")],
};
