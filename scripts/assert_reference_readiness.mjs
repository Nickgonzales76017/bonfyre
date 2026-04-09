#!/usr/bin/env node

import fs from 'node:fs';

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function scoreSource(source) {
  const signal = source.signal || {};
  const messy = Number(signal.messy_audio || 0);
  const jargon = Number(signal.jargon_density || 0);
  const social = Number(signal.social_complexity || 0);
  const fit = Number(signal.bonfyre_fit || 0);
  const provenance = Number(signal.provenance_confidence || 0);
  const safety = Number(signal.public_safety || 0);
  return messy * 1.4 + jargon * 1.2 + social * 1.1 + fit * 1.8 + provenance * 1.3 + safety * 1.0;
}

function summarizeApp(app, targetDistinctSources) {
  const sources = Array.isArray(app.sources) ? app.sources : [];
  const approved = sources.filter((source) => String(source.review_status || '').toLowerCase() === 'approved');
  const queued = sources.filter((source) => String(source.review_status || '').toLowerCase() === 'queued');
  const missingUrl = approved.filter((source) => !source.public_url).length;
  const missingPublisher = approved.filter((source) => !source.publisher).length;
  const gap = Math.max(0, targetDistinctSources - approved.length);
  const queuedScore = queued.reduce((sum, source) => sum + scoreSource(source), 0);
  const readinessScore = Math.max(0, approved.length * 20 + Math.min(queuedScore, 40) - gap * 4);
  return {
    repo: app.repo,
    approved: approved.length,
    missingUrl,
    missingPublisher,
    gap,
    readinessScore
  };
}

function parseArgs(argv) {
  const args = {
    queuePath: '',
    targetDistinctSources: 10,
    minReadiness: 30,
    repos: []
  };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (!args.queuePath && !value.startsWith('--')) {
      args.queuePath = value;
      continue;
    }
    if (value === '--target') {
      args.targetDistinctSources = Number(argv[index + 1] || args.targetDistinctSources);
      index += 1;
      continue;
    }
    if (value === '--min-readiness') {
      args.minReadiness = Number(argv[index + 1] || args.minReadiness);
      index += 1;
      continue;
    }
    if (value === '--repo') {
      const repo = argv[index + 1];
      if (repo) {
        args.repos.push(repo);
      }
      index += 1;
    }
  }

  return args;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.queuePath) {
    console.error('usage: assert_reference_readiness.mjs <queue.json> [--target N] [--min-readiness N] [--repo name]');
    process.exit(1);
  }

  const queue = readJson(args.queuePath);
  const apps = Array.isArray(queue.apps) ? queue.apps : [];
  const selectedApps = args.repos.length
    ? apps.filter((app) => args.repos.includes(app.repo))
    : apps;

  const failures = [];

  for (const app of selectedApps) {
    const summary = summarizeApp(app, args.targetDistinctSources);
    if (summary.approved < args.targetDistinctSources) {
      failures.push(`${summary.repo}: approved=${summary.approved} below target=${args.targetDistinctSources}`);
    }
    if (summary.gap > 0) {
      failures.push(`${summary.repo}: gap_to_target=${summary.gap}`);
    }
    if (summary.readinessScore < args.minReadiness) {
      failures.push(`${summary.repo}: readiness=${summary.readinessScore.toFixed(1)} below min=${args.minReadiness}`);
    }
    if (summary.missingUrl > 0) {
      failures.push(`${summary.repo}: approved_missing_public_url=${summary.missingUrl}`);
    }
    if (summary.missingPublisher > 0) {
      failures.push(`${summary.repo}: approved_missing_publisher=${summary.missingPublisher}`);
    }
  }

  if (failures.length) {
    console.error('reference-readiness gate failed:');
    for (const failure of failures) {
      console.error(`- ${failure}`);
    }
    process.exit(2);
  }

  console.log(`reference-readiness gate passed for ${selectedApps.length} app(s)`);
}

main();
