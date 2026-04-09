#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + '\n');
}

function pickArray(value, fallback) {
  return Array.isArray(value) && value.length ? value : fallback;
}

function toReviewedApp(app) {
  const approved = (app.sources || []).filter((source) => String(source.review_status || '').toLowerCase() === 'approved');
  return {
    repo: app.repo,
    slug: app.slug,
    title: app.title,
    out_path: app.out_path,
    default_outputs: app.default_outputs || [],
    default_tags: app.default_tags || [],
    why_it_matters: app.why_it_matters || '',
    search_intro: app.search_intro || '',
    source_copy: app.source_copy || '',
    sources: approved.map((source) => ({
      id: source.id,
      title: source.title,
      file: source.file || source.title,
      published_at: source.published_at || 'Public source',
      public_url: source.public_url,
      publisher: source.publisher || '',
      license: source.license || 'Public platform origin',
      sourceLabel: source.sourceLabel || 'Watch original public source',
      brief: source.brief || '',
      tags: pickArray(source.tags, app.default_tags || []),
      outputs: pickArray(source.outputs, app.default_outputs || []),
      searchSummary: source.searchSummary || '',
      searchOutputs: pickArray(source.searchOutputs, [])
    }))
  };
}

function main() {
  const queuePath = process.argv[2];
  const outPath = process.argv[3];
  if (!queuePath || !outPath) {
    console.error('usage: build_reviewed_manifest_from_queue.mjs <queue.json> <out.json>');
    process.exit(1);
  }

  const queue = readJson(queuePath);
  const manifest = {
    apps: (queue.apps || [])
      .map(toReviewedApp)
      .filter((app) => app.sources.length)
  };

  writeJson(path.resolve(outPath), manifest);

  for (const app of manifest.apps) {
    console.log(`${app.repo}: ${app.sources.length} approved source(s)`);
  }
}

main();
