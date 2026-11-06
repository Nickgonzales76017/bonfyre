/** The estate's own semantic classes, styled by Tailwind.
 *
 * The pages name what a thing is -- a play control, a way on, a provenance
 * note -- and never carry utility classes in their markup, because the markup
 * is composed in C and read by people. Tailwind supplies the design behind
 * those names through @apply, and its own tokens stay available for anything
 * authored by hand.
 */
module.exports = {
  content: ["./scan/**/*.html"],
  darkMode: ["variant", "&:where([data-theme='dusk'], [data-theme='dusk'] *)"],
  theme: { extend: {} },
  plugins: [require("@tailwindcss/typography"), require("@tailwindcss/forms")],
};
