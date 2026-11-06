// A site's look is a small set of named roles -- the same roles a
// presentation_template row carries (ground, ink, display, accent, muted,
// panel, faces, font_href) -- so a client site can wear one of the estate's
// authored languages by name, or state its own. Every shade a section uses is
// worked out from those roles here; a section never names a colour.
import { readFileSync } from "node:fs";
import { join } from "node:path";

const authored = join(import.meta.dir, "../../site/matrix/surfaces/build/presentation-templates.jsonl");
const hex = value => {
  const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(String(value ?? "").trim());
  if (!m) return null;
  const s = m[1].length === 3 ? [...m[1]].map(c => c + c).join("") : m[1];
  return [0, 2, 4].map(i => parseInt(s.slice(i, i + 2), 16));
};
const mix = (a, b, t) => "#" + hex(a).map((v, i) => Math.round(v + (hex(b)[i] - v) * t).toString(16).padStart(2, "0")).join("");
const luminance = value => { const [r, g, b] = hex(value).map(v => { v /= 255; return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4; }); return 0.2126 * r + 0.7152 * g + 0.0722 * b; };
export const contrast = (a, b) => { const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x); return (hi + 0.05) / (lo + 0.05); };

export function wornTemplate(name) {
  for (const line of readFileSync(authored, "utf8").split("\n")) {
    if (!line.trim()) continue;
    const row = JSON.parse(line);
    if (row.template === name) return typeof row.layout_tokens === "string" ? JSON.parse(row.layout_tokens) : row.layout_tokens;
  }
  throw new Error(`theme.template "${name}" is not an authored presentation template`);
}

export const LOOKS = ["editorial", "night", "aurekai"];
const defaults = { face_display: "'Plus Jakarta Sans',sans-serif", face_body: "Inter,sans-serif", face_mono: "'JetBrains Mono',monospace",
  font_href: "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600;700&family=Plus+Jakarta+Sans:wght@600;700;800&display=swap" };

export function resolveTheme(theme = {}) {
  const worn = theme.template ? wornTemplate(theme.template) : {};
  const t = { ...(theme.template ? {} : defaults), ...worn, ...theme };
  for (const role of ["ground", "ink", "display", "accent", "muted"]) if (!hex(t[role])) throw new Error(`theme.${role} must be a hex colour`);
  const panel = hex(t.panel) ? t.panel : mix(t.ground, t.ink, 0.06);
  const second = hex(t.second) ? t.second : t.accent;
  const colors = {
    surface: t.ground, "surface-bright": mix(t.ground, t.ink, 0.2),
    "surface-container-lowest": hex(t.deep) ? t.deep : t.ground, "surface-container-low": mix(t.ground, panel, 0.6),
    "surface-container": panel, "surface-container-high": mix(panel, t.ink, 0.06), "surface-container-highest": mix(panel, t.ink, 0.12),
    primary: second, "primary-light": hex(t.second_soft) ? t.second_soft : mix(second, t.display, 0.35),
    secondary: t.accent, "secondary-dark": mix(t.accent, "#000000", 0.14),
    "on-surface": t.ink, "on-surface-variant": t.muted, outline: mix(t.ground, t.ink, 0.32), display: t.display,
    ...(theme.shades ?? {}),
  };
  // A look that cannot be read is refused at build, not discovered by a visitor.
  const checks = [["display on ground", colors.display, colors.surface, 4.5], ["ink on ground", colors["on-surface"], colors.surface, 4.5],
    ["muted on panel", colors["on-surface-variant"], colors["surface-container"], 4.5], ["button label on accent", colors.surface, colors.secondary, 3]];
  const unreadable = checks.filter(([, a, b, need]) => contrast(a, b) < need).map(([what, a, b]) => `${what} ${contrast(a, b).toFixed(2)}:1`);
  if (unreadable.length) throw new Error(`theme is not readable: ${unreadable.join("; ")}`);
  // A look is a named way of drawing the same sections. It is the kit's, not the record's to invent.
  if (theme.look && !LOOKS.includes(theme.look)) throw new Error(`theme.look "${theme.look}" is not one the kit has (${LOOKS.join(", ")})`);
  return { colors, dark: luminance(t.ground) < 0.4, template: theme.template ?? null, font_href: t.font_href, look: theme.look ?? null,
    vars: { "--paper": colors.surface, "--panel": colors["surface-container"], "--ink": colors.display, "--hot": colors.secondary, "--muted": colors["on-surface-variant"], "--rule": mix(t.ground, t.ink, 0.16), "--display": t.face_display, "--mono": t.face_mono, "--body": t.face_body,
      // A look may set a picture behind its opening; the record names a file in the site's own folder.
      ...(theme.backdrop ? { "--backdrop": `url(/${String(theme.backdrop).replace(/[^A-Za-z0-9._\/-]/g, "")})` } : {}) },
    fontFamily: { headline: t.face_display, body: t.face_body, mono: t.face_mono } };
}
