#!/usr/bin/env bun
// Authoring compiler: World.grammars -> CMS world_fragment rows.
import { Database } from 'bun:sqlite';
import { fileURLToPath } from 'node:url';

const CONTROL = fileURLToPath(new URL('../../../schemas/control/', import.meta.url));
const cue = expression => {
  const result = Bun.spawnSync(['cue', 'export', '.', '-e', expression], { cwd: CONTROL });
  if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
  return JSON.parse(new TextDecoder().decode(result.stdout));
};
let cached;
export function worldFragmentProfiles() {
  return cached ||= { law: cue('worldFragmentLaw'), presentation: cue('grammarPresentation') };
}

export function fragmentRow(world, grammar, ordinal, profiles = worldFragmentProfiles()) {
  const { law, presentation } = profiles;
  const definition = law.profiles[grammar] || law.historical[grammar];
  if (!definition) throw new Error(`unsupported World grammar: ${world.world}:${grammar}`);
  const base = presentation[definition.canonical || grammar];
  if (!base) throw new Error(`World grammar has no shared presentation profile: ${grammar}`);
  const surfaceGrammar = law.canonical_aliases[grammar] || definition.canonical || grammar;
  return { fragment_id: `${world.world}:${grammar}`, world: world.world, ordinal,
    subject_address: world.subject || `world:${world.world}`, surface_grammar: surfaceGrammar,
    renderer_family: definition.family || base.renderer_family,
    renderer_variant: definition.variant || base.renderer_variant,
    extent: definition.extent || base.extent_defaults, template: world.template,
    source_projection: `${law.source_projection_prefix}?world=${world.world}&fragment=${grammar}`,
    media_posture: law.media_posture, temporal_posture: law.temporal_posture,
    continuity_group: world.world, visible_sections: law.visible_sections, action_aperture: '',
    provenance: law.provenance, cannot_infer: law.cannot_infer, status: 'published' };
}

export function missingWorldFragments(db, profiles = worldFragmentProfiles()) {
  const known = new Set(db.query("SELECT namespace,fragment_id FROM world_fragment WHERE status='published'")
    .all().map(row => `${row.namespace}\0${row.fragment_id}`));
  const worlds = db.query("SELECT namespace,world,COALESCE(subject,'') AS subject,COALESCE(template,'') AS template,COALESCE(grammars,'') AS grammars FROM world WHERE status='published' ORDER BY id").all();
  const wanted = [];
  for (const world of worlds) {
    const grammars = world.grammars.split(',').map(value => value.trim()).filter(Boolean);
    grammars.forEach((grammar, index) => {
      const item = fragmentRow(world, grammar, index + 10, profiles);
      const identity = `${world.namespace}\0${item.fragment_id}`;
      if (known.has(identity)) return;
      known.add(identity);
      wanted.push({ namespace: world.namespace, item });
    });
  }
  return wanted;
}

export function materializeWorldFragments(args, run = argv => Bun.spawnSync(argv)) {
  const db = new Database(args.db);
  let wanted;
  try { wanted = missingWorldFragments(db); } finally { db.close(); }
  if (args.check) return { result: { missing: wanted.length,
    fragment_ids: wanted.map(row => row.item.fragment_id) }, exitCode: wanted.length ? 1 : 0 };
  for (const { namespace, item } of wanted) {
    const command = [args.cms, 'entry', 'create', 'world_fragment', JSON.stringify(item),
      '--ns', namespace, '--db', args.db, '--schemas', args.schemas];
    const result = run(command);
    if (result.exitCode !== 0) throw new Error(`world_fragment creation refused: ${item.fragment_id}: ` +
      new TextDecoder().decode(result.stderr || new Uint8Array()).trim());
  }
  return { result: { materialized: wanted.length, state: 'world_fragment_rows' }, exitCode: 0 };
}

if (import.meta.main) {
  try {
    const args = { check: false }, argv = process.argv.slice(2);
    for (let index = 0; index < argv.length; index++) {
      const key = argv[index];
      if (key === '--check') { args.check = true; continue; }
      if (!key.startsWith('--') || !argv[index + 1]) throw new Error('expected --key value arguments');
      args[key.slice(2)] = argv[++index];
    }
    if (!args.db || !args.cms || !args.schemas) throw new Error('--db, --cms and --schemas are required');
    const { result, exitCode } = materializeWorldFragments(args);
    console.log(JSON.stringify(result));
    process.exitCode = exitCode;
  } catch (error) { console.error(`world fragment materialization: ${error.message}`); process.exitCode = 2; }
}
