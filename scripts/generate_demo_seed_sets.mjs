#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + '\n');
}

function slugify(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'seed';
}

function pickArray(value, fallback) {
  return Array.isArray(value) && value.length ? value : fallback;
}

function interpolate(template, context) {
  return String(template || '').replace(/\{([a-zA-Z0-9_]+)\}/g, (_, key) => {
    const value = context[key];
    return value == null ? '' : String(value);
  });
}

function cartesian(dimensions) {
  const entries = Object.entries(dimensions || {});
  if (!entries.length) return [{}];
  let results = [{}];
  for (const [key, values] of entries) {
    const next = [];
    for (const result of results) {
      for (const value of values || []) {
        next.push({ ...result, [key]: value });
      }
    }
    results = next;
  }
  return results;
}

function expandTemplateSeeds(app) {
  const template = app.template;
  if (!template || !template.dimensions) return app.seeds || [];
  const combos = cartesian(template.dimensions);
  return combos.map((context, index) => {
    const merged = { ...context, n: index + 1, app: app.slug || app.repo };
    return {
      id: interpolate(template.id || '{app}-{n}', merged),
      file: interpolate(template.file || `${app.title} {n}`, merged),
      brief: interpolate(template.brief || app.default_brief || `${app.title} reference record {n}.`, merged),
      tags: pickArray(template.tags, []).map(tag => interpolate(tag, merged)),
      outputs: pickArray(template.outputs, app.default_outputs || []).map(out => interpolate(out, merged)),
      searchSummary: interpolate(template.searchSummary || '', merged),
      whyItMatters: interpolate(template.whyItMatters || app.why_it_matters || '', merged),
      searchIntro: interpolate(template.searchIntro || app.search_intro || '', merged),
      searchOutputs: pickArray(template.searchOutputs, []).map(out => interpolate(out, merged)),
      flagged: Boolean(template.flagged_every && ((index + 1) % Number(template.flagged_every) === 0)),
      status: template.status || 'complete',
      time: template.time || 'Reference corpus'
    };
  });
}

function buildRecord(seed, app, index) {
  const idBase = slugify(seed.id || seed.file || `${app.slug}-${index + 1}`);
  const tags = pickArray(seed.tags, app.default_tags || []);
  const outputs = pickArray(seed.outputs, app.default_outputs || []);
  return {
    id: idBase,
    file: seed.file || `${app.title} Corpus ${index + 1}`,
    time: seed.time || 'Reference corpus',
    status: seed.status || 'complete',
    brief: seed.brief || app.default_brief || `${app.title} reference record ${index + 1}.`,
    tags,
    flagged: Boolean(seed.flagged),
    demo: true,
    outputs,
    outputNotes: seed.outputNotes || {},
    outputLinks: seed.outputLinks || {},
    sourceLinks: pickArray(seed.sourceLinks, []),
    sourceTitle: seed.sourceTitle || '',
    sourceUrl: seed.sourceUrl || '',
    sourceLabel: seed.sourceLabel || '',
    sourceCopy: seed.sourceCopy || '',
    publisher: seed.publisher || '',
    license: seed.license || '',
    searchSummary: seed.searchSummary || `${tags.slice(0, 3).join(', ')}`,
    whyItMatters: seed.whyItMatters || app.why_it_matters || `${app.title} becomes more convincing when users can explore multiple real-looking records in-app.`,
    searchIntro: seed.searchIntro || app.search_intro || `Search across the ${app.slug} corpus to see how this app handles multiple records instead of one isolated example.`,
    searchOutputs: pickArray(seed.searchOutputs, outputs.slice(0, 1))
  };
}

function generateFromManifest(manifest) {
  const outputs = [];
  for (const app of manifest.apps || []) {
    const items = [];
    const seedCount = Number(app.count || manifest.default_count || 100);
    const sourceSeeds = expandTemplateSeeds(app);
    if (!sourceSeeds.length) {
      throw new Error(`App ${app.repo} has no seeds in manifest`);
    }
    for (let i = 0; i < seedCount; i += 1) {
      const source = sourceSeeds[i % sourceSeeds.length];
      const seed = {
        ...source,
        id: `${slugify(source.id || source.file || app.slug)}-${String(i + 1).padStart(3, '0')}`,
        file: seedCount > sourceSeeds.length
          ? `${source.file || app.title} ${String(i + 1).padStart(3, '0')}`
          : source.file
      };
      items.push(buildRecord(seed, app, i));
    }
    outputs.push({
      repo: app.repo,
      outPath: app.out_path,
      items
    });
  }
  return outputs;
}

function main() {
  const manifestPath = process.argv[2];
  if (!manifestPath) {
    console.error('usage: generate_demo_seed_sets.mjs <manifest.json>');
    process.exit(1);
  }

  const manifest = readJson(manifestPath);
  const generated = generateFromManifest(manifest);
  for (const entry of generated) {
    const outPath = path.resolve(entry.outPath);
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    writeJson(outPath, entry.items);
    console.log(`${entry.repo}: wrote ${entry.items.length} records -> ${outPath}`);
  }
}

main();
