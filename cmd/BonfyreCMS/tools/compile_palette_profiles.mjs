#!/usr/bin/env bun
// Original estate palettes, not observed donor designs or selected site profiles.
import { writeFileSync } from 'node:fs';

const postures = {
  magazine: { face_display: "'Instrument Serif',Georgia,serif", weight: '500', size: 'clamp(2.4rem,5.2vw,4.6rem)', tracking: '-.02em', leading: '1.02', radius: '2px', gap: '.9rem', measure: '62ch', transform: 'none' },
  editorial: { face_display: "'Newsreader',Georgia,serif", weight: '600', size: 'clamp(2rem,4.2vw,3.4rem)', tracking: '-.01em', leading: '1.1', radius: '3px', gap: '.8rem', measure: '66ch', transform: 'none' },
  newsroom: { face_display: "'DM Sans',system-ui,sans-serif", weight: '700', size: 'clamp(1.7rem,3.4vw,2.6rem)', tracking: '-.015em', leading: '1.08', radius: '2px', gap: '.6rem', measure: '58ch', transform: 'none' },
  portfolio: { face_display: "'Archivo Black','Arial Black',sans-serif", weight: '400', size: 'clamp(2.6rem,6vw,5.2rem)', tracking: '-.03em', leading: '.98', radius: '0', gap: '1.1rem', measure: '52ch', transform: 'uppercase' },
  gallery: { face_display: "'DM Sans',system-ui,sans-serif", weight: '500', size: 'clamp(1.5rem,3vw,2.2rem)', tracking: '.02em', leading: '1.15', radius: '0', gap: '.4rem', measure: '48ch', transform: 'none' },
  audio: { face_display: "'Spline Sans Mono',ui-monospace,monospace", weight: '600', size: 'clamp(1.6rem,3.2vw,2.4rem)', tracking: '-.01em', leading: '1.1', radius: '6px', gap: '.7rem', measure: '54ch', transform: 'none' },
  letter: { face_display: "'Newsreader',Georgia,serif", weight: '400', size: 'clamp(1.6rem,3vw,2.3rem)', tracking: '0', leading: '1.2', radius: '4px', gap: '.9rem', measure: '70ch', transform: 'none' },
  docs: { face_display: "'DM Mono',ui-monospace,monospace", weight: '500', size: 'clamp(1.4rem,2.6vw,2rem)', tracking: '0', leading: '1.25', radius: '4px', gap: '.6rem', measure: '74ch', transform: 'none' },
};
const hues = [['rose',8],['cherry',20],['ember',32],['clay',46],['amber',62],['wheat',88],['olive',108],['moss',132],['fern',150],['sea',168],['teal',192],['lagoon',206],['sky',224],['cobalt',244],['indigo',268],['violet',292],['orchid',304],['plum',318],['magenta',332],['crimson',350],['slate',250],['stone',70],['ash',110],['ink',280]];
const tempers = [['still',0.75],['plain',1],['loud',1.35]];
const oklch = (lightness, chroma, hue) => `oklch(${lightness.toFixed(3)} ${chroma.toFixed(3)} ${hue})`;
export function palette(hue, dark, temper = 1) {
  const ink = dark ? oklch(.94,.010,hue) : oklch(.22,.020,hue);
  return dark ? { ground:oklch(.17,.018,hue), panel:oklch(.22,.024,hue), ink, display:oklch(.98,.012,hue), muted:oklch(.68,.020,hue), rule:`color-mix(in oklch, ${ink} 26%, transparent)`, accent:oklch(.78,.140*temper,hue) }
    : { ground:oklch(.975,.008,hue), panel:oklch(.995,.004,hue), ink, display:oklch(.16,.026,hue), muted:oklch(.52,.026,hue), rule:`color-mix(in oklch, ${ink} 18%, transparent)`, accent:oklch(.55,.170*temper,hue) };
}
export function compilePalettes(perFamily = 144) {
  const rows = [];
  for (const [family, posture] of Object.entries(postures)) {
    let made = 0;
    for (const dark of [false, true]) for (const [temperName, temper] of tempers) for (const [name, hue] of hues) {
      if (made >= perFamily) break;
      const mode = dark ? 'dusk' : 'day';
      const tokens = { ...palette(hue, dark, temper), face_display: posture.face_display, face_body: "'DM Sans',system-ui,-apple-system,sans-serif", face_mono: "'DM Mono',ui-monospace,monospace",
        display_size: posture.size, display_weight: posture.weight, display_transform: posture.transform, display_tracking: posture.tracking, display_leading: posture.leading,
        radius: posture.radius, card_border: '1px solid var(--w-rule)', card_shadow: 'none', gap: posture.gap, measure: posture.measure, content_max: '84rem', grid_min: '19rem' };
      rows.push({ template: `estate-${family}-${name}-${temperName}-${mode}`, family, reference_source: 'estate-palette-scale', reference_name: `${name} ${family}, ${temperName} ${mode}`,
        reference_url: 'bonfyre:cmd/BonfyreCMS/tools/compile_palette_profiles.mjs', reference_categories: `palette,${family},${temperName},${mode}`, reference_author: 'this estate',
        renderer_mix: 'tokens-only', density: ['letter','docs','editorial'].includes(family) ? 'comfortable' : 'dense', hero_mode: 'none', navigation_mode: 'inherited',
        media_mode: ({ gallery:'pictures', audio:'recordings', portfolio:'pictures' })[family] || 'mixed', typography_mode: posture.face_display.split(',')[0].replaceAll("'", ''),
        card_mode: 'no-cards', chrome_mode: 'minimal', semantic_roles: 'region-dressing', territory_affinity: 'any',
        acceptance_contract: 'supplies colour and typographic tokens to a region this estate composes; it selects no material, orders nothing, and names no publisher',
        cannot_infer: "a catalogued palette is colours and measurements generated here; it is not an observed design, not a studied profile, not anyone else's work, and says nothing about the material it happens to dress",
        status: 'reference_candidate', layout_tokens: tokens });
      made++;
    }
  }
  return rows;
}
if (import.meta.main) {
  const args = { out: '', 'per-family': '144', flagships: null };
  for (let i = 2; i < process.argv.length; i++) { const key = process.argv[i].slice(2); if (!Object.hasOwn(args, key)) throw Error(`unknown option ${process.argv[i]}`); args[key] = key === 'flagships' ? true : process.argv[++i]; }
  if (!args.out || !/^\d+$/.test(args['per-family'])) throw Error('usage: compile_palette_profiles.mjs --out ROWS.jsonl [--per-family 28] [--flagships]');
  let rows = compilePalettes(Number(args['per-family']));
  /* Selectable catalogue: plain temper, day and dusk, three wheel-spread hues
   * per family, published as the account/profile/org theme options.
   * Mechanical variety; a designer pass may swap hues without changing the
   * rule. The full 1152 stay reference candidates. */
  if (args.flagships) rows = rows
    .filter(r => /-plain-(day|dusk)$/.test(r.template) && /(rose|olive|indigo)/.test(r.template))
    .map(r => ({ ...r, status: 'published' }));
  writeFileSync(args.out, rows.map(x => JSON.stringify(x)).join('\n') + (rows.length ? '\n' : ''));
  console.log(JSON.stringify({ profiles: rows.length, families: Object.keys(postures).length,
    cannot_infer: "a catalogued palette is a set of colours and measurements this estate generated, never an observed design, a studied profile, or anyone else's work" }));
}
