#!/usr/bin/env bun
// One effect-free accounting cycle. The authored CUE plan owns its artifacts and runners.
import { copyFileSync, existsSync, mkdirSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = fileURLToPath(new URL('.', import.meta.url));
const text = bytes => new TextDecoder().decode(bytes);
const exists = path => !!path && existsSync(path);
const nonempty = path => exists(path) && statSync(path).size > 0;
const addInputs = (argv, specs) => [...argv, ...specs.flatMap(spec => ['--source-input', spec])];
const utc = () => new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
const jsonFiles = directory => directory && exists(directory) && statSync(directory).isDirectory()
  ? readdirSync(directory).filter(name => name.endsWith('.json') && statSync(join(directory, name)).isFile())
    .toSorted().map(name => join(directory, name)) : [];

export function cyclePolicy() {
  const cue = fileURLToPath(new URL('../source-fabric-cycle.cue', import.meta.url));
  const result = Bun.spawnSync(['cue', 'export', cue, '-e', 'sourceFabricCycle']);
  if (result.exitCode !== 0) throw new Error(`Source Fabric CUE: ${text(result.stderr).trim()}`);
  return JSON.parse(text(result.stdout));
}

export function parseArgs(argv) {
  const args = { input: [] };
  for (let i = 0; i < argv.length; i += 2) {
    if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
    const key = argv[i].slice(2);
    if (key === 'input') args.input.push(argv[i + 1]); else args[key] = argv[i + 1];
  }
  if (!args.input.length || !args['out-dir']) throw new Error('--input LABEL=PATH and --out-dir are required');
  for (const spec of args.input) {
    const split = spec.indexOf('=');
    if (split < 1 || !exists(spec.slice(split + 1))) throw new Error(`source inventory does not exist: ${spec}`);
  }
  return args;
}

export function runCycle(args, policy = cyclePolicy(), run = argv => Bun.spawnSync(argv)) {
  if (Object.keys(args).some(key => key.startsWith('decision-')))
    throw new Error('decision experiments are not a Source Fabric cycle step; no avoided work has been measured');
  const outputDir = args['out-dir'];
  mkdirSync(outputDir, { recursive: true });
  const outputs = Object.fromEntries(Object.entries(policy.outputs).map(([key, name]) => [key, join(outputDir, name)]));
  const inputs = [...args.input];
  const record = { schema: policy.schema, started: utc(), inputs: [...inputs], steps: [], truth: policy.truth };
  const program = (key, argv) => {
    const entry = policy.programs[key];
    if (!entry || !entry.runtime || !entry.file) throw new Error(`undeclared Source Fabric program: ${key}`);
    return [entry.runtime, join(HERE, entry.file), ...argv];
  };
  const step = (name, command, reason) => {
    if (!command) {
      record.steps.push({ step: name, ok: true, skipped: true, reason });
      return;
    }
    const result = run(command), stdout = text(result.stdout || new Uint8Array()), stderr = text(result.stderr || new Uint8Array());
    const last = stdout.trimEnd().split(/\r?\n/).at(-1) || '';
    let summary = null;
    if (last) { try { summary = JSON.parse(last); } catch { summary = last.slice(0, 500); } }
    const entry = { step: name, ok: result.exitCode === 0, returncode: result.exitCode, summary };
    if (stderr.trim()) entry.stderr = stderr.trim().slice(-1000);
    if (result.exitCode !== 0) throw new Error(`${basename(command[1])} failed: ${entry.stderr || JSON.stringify(summary)}`);
    record.steps.push(entry);
  };

  if (exists(args['cms-db']) && args['foreign-root'] && exists(args['foreign-root']) && statSync(args['foreign-root']).isDirectory()) {
    step('recursive-channels', program('recursive', ['--cms-db', args['cms-db'], '--foreign-root', args['foreign-root'], '--out', outputs.recursive]));
    if (nonempty(outputs.recursive)) inputs.push(`${policy.recursive_inventory_label}=${outputs.recursive}`);
  } else step('recursive-channels', null, 'CMS DB and retained ForeignTwin root not both present');

  step('frontier', program('frontier', ['--out', outputs.manifest, '--roots', outputs.roots,
    '--channels', outputs.channels, '--gaps', outputs.gaps,
    ...inputs.flatMap(spec => ['--input', spec])]));

  const closures = jsonFiles(args['closure-dir']);
  if (closures.length) step('pressure', program('pressure', addInputs(['--roots', outputs.roots,
    '--channels', outputs.channels, '--out', outputs.pressure, '--receipts', outputs.pressure_summary], inputs)
    .concat(closures.flatMap(path => ['--closure', path]))));
  else step('pressure', null, 'no ScopedDemand closure JSON files supplied');

  if (exists(args['scoped-frontier-db']) && args['fact-deltas'] && exists(args['fact-deltas']) && statSync(args['fact-deltas']).isDirectory()) {
    const argv = addInputs(['--frontier-db', args['scoped-frontier-db'], '--fact-delta-dir', args['fact-deltas'],
      '--out', outputs.impact, '--receipts', outputs.impact_receipts], inputs);
    if (exists(args['scene-db'])) argv.push('--scene-db', args['scene-db']);
    step('change-impact', program('impact', argv));
  } else step('change-impact', null, 'scoped frontier DB and fact-delta directory not both present');

  if (exists(args['scoped-frontier-db']) && exists(args['scene-db'])) {
    if (nonempty(outputs.scene)) copyFileSync(outputs.scene, outputs.scene_previous);
    step('scene-source-receipts', program('scene', addInputs(['--frontier-db', args['scoped-frontier-db'],
      '--scene-db', args['scene-db'], '--out', outputs.scene, '--summary', outputs.scene_summary], inputs)));
    if (nonempty(outputs.scene_previous) && nonempty(outputs.scene))
      step('scene-source-delta', program('scene_delta', ['--before', outputs.scene_previous,
        '--after', outputs.scene, '--out', outputs.scene_delta, '--root-out', outputs.root_cochange]));
    else step('scene-source-delta', null, 'no prior Scene source receipt snapshot exists yet');
    if (exists(args['cms-db'])) step('legacy-scene-sources', program('legacy_scene', ['--scene-receipts', outputs.scene,
      '--cms-db', args['cms-db'], '--out', outputs.legacy_scene_sources]));
    else step('legacy-scene-sources', null, 'CMS DB not supplied');
  } else {
    step('scene-source-receipts', null, 'scoped frontier DB and scene DB not both present');
    step('scene-source-delta', null, 'Scene source receipts unavailable');
    step('legacy-scene-sources', null, 'Scene source receipts unavailable');
  }

  const forward = ['--roots', outputs.roots, '--gaps', outputs.gaps, '--out', outputs.forward,
    '--opportunities', outputs.relationships];
  if (nonempty(outputs.impact)) forward.push('--demand', outputs.impact);
  step('forward', program('forward', forward));
  const attention = ['--roots', outputs.roots, '--gaps', outputs.gaps, '--out', outputs.attention];
  if (nonempty(outputs.pressure)) attention.push('--pressure', outputs.pressure);
  if (nonempty(outputs.impact_receipts)) attention.push('--impact', outputs.impact_receipts);
  step('attention', program('attention', attention));
  const starvation = ['--roots', outputs.roots, '--gaps', outputs.gaps, '--attention', outputs.attention,
    '--out', outputs.starvation, '--scene-out', outputs.scene_starvation];
  if (nonempty(outputs.pressure)) starvation.push('--pressure', outputs.pressure);
  if (nonempty(outputs.impact_receipts)) starvation.push('--impact', outputs.impact_receipts);
  if (nonempty(outputs.scene)) starvation.push('--scene-receipts', outputs.scene);
  step('starvation', program('starvation', starvation));

  record.finished = utc();
  record.effective_inputs = inputs;
  record.outputs = Object.fromEntries(Object.entries(outputs).filter(([key, path]) => key !== 'cycle' && exists(path)));
  record.cannot_infer = policy.cannot_infer;
  writeFileSync(outputs.cycle, JSON.stringify(record, null, 2) + '\n');
  return { cycle: outputs.cycle, steps: record.steps.map(item => [item.step, item.ok, !!item.skipped]),
    outputs: Object.keys(record.outputs).length, effective_inputs: inputs.length };
}

if (import.meta.main) {
  try { console.log(JSON.stringify(runCycle(parseArgs(process.argv.slice(2))))); }
  catch (error) { console.error(`source fabric cycle: ${error.message}`); process.exitCode = 2; }
}
