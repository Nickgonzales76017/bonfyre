// CSS is a build artifact, compiled separately from route/render logic.
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
const here=import.meta.dir;
// The stylesheet holds only what these pages use, in this site's colours.
export function compileCss(pages, theme, into) {
  const work = mkdtempSync(join(tmpdir(), "service-site-css-"));
  try {
    pages.forEach((page, i) => writeFileSync(join(work, `page-${i}.html`), page.html));
    const look = theme.look ? `:root{${Object.entries(theme.vars).map(([k, v]) => `${k}:${v}`).join(";")}}\n` + readFileSync(join(here, "looks", `${theme.look}.css`), "utf8") : "";
    writeFileSync(join(work, "in.css"), "@tailwind base;@tailwind components;@tailwind utilities;\n.material-symbols-outlined{font-variation-settings:'FILL' 0,'wght' 400,'GRAD' 0,'opsz' 24}\nhtml{scroll-behavior:smooth}@media (prefers-reduced-motion:reduce){html{scroll-behavior:auto}*,*::before,*::after{animation:none!important;transition:none!important}}\n" + look + "");
    writeFileSync(join(work, "tailwind.config.cjs"), `module.exports=${JSON.stringify({ content: ["./page-*.html"], darkMode: "class",
      theme: { extend: { colors: theme.colors, fontFamily: theme.fontFamily } } })}`);
    const run = Bun.spawnSync(["bunx", "tailwindcss@3.4.17", "-c", "tailwind.config.cjs", "-i", "in.css", "-o", into, "--minify"], { cwd: work, stdout: "pipe", stderr: "pipe" });
    if (run.exitCode !== 0 || !existsSync(into)) throw new Error(`stylesheet did not compile: ${run.stderr.toString().trim().split("\n").pop()}`);
  } finally { rmSync(work, { recursive: true, force: true }); }
}
