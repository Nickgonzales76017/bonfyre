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

function averageSignal(sources, key) {
  if (!sources.length) {
    return 0;
  }
  const total = sources.reduce((sum, source) => sum + Number((source.signal || {})[key] || 0), 0);
  return total / sources.length;
}

function getPatterns(source) {
  const tags = Array.isArray(source.tags) ? source.tags : [];
  return tags.slice(1);
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
  const approvedPatterns = [...new Set(approved.flatMap((source) => getPatterns(source)))];
  const queuedPatterns = [...new Set(queued.flatMap((source) => getPatterns(source)))];
  const stressFloor = {
    messy: averageSignal(approved, 'messy_audio'),
    jargon: averageSignal(approved, 'jargon_density'),
    social: averageSignal(approved, 'social_complexity'),
    fit: averageSignal(approved, 'bonfyre_fit'),
    provenance: averageSignal(approved, 'provenance_confidence'),
    safety: averageSignal(approved, 'public_safety')
  };
  const maxApprovedStress = approved.reduce((max, source) => Math.max(max, scoreSource(source)), 0);
  return {
    repo: app.repo,
    approved: approved.length,
    missingUrl,
    missingPublisher,
    gap,
    readinessScore,
    approvedPatterns,
    queuedPatterns,
    stressFloor,
    maxApprovedStress
  };
}

function parseArgs(argv) {
  const args = {
    queuePath: '',
    targetDistinctSources: 10,
    minReadiness: 30,
    minPatternCoverage: 3,
    minMessy: 2.5,
    minJargon: 3.5,
    minSocial: 3.5,
    minFit: 4.0,
    minProvenance: 3.5,
    minSafety: 3.5,
    minStressScore: 24,
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
    if (value === '--min-pattern-coverage') {
      args.minPatternCoverage = Number(argv[index + 1] || args.minPatternCoverage);
      index += 1;
      continue;
    }
    if (value === '--min-messy') {
      args.minMessy = Number(argv[index + 1] || args.minMessy);
      index += 1;
      continue;
    }
    if (value === '--min-jargon') {
      args.minJargon = Number(argv[index + 1] || args.minJargon);
      index += 1;
      continue;
    }
    if (value === '--min-social') {
      args.minSocial = Number(argv[index + 1] || args.minSocial);
      index += 1;
      continue;
    }
    if (value === '--min-fit') {
      args.minFit = Number(argv[index + 1] || args.minFit);
      index += 1;
      continue;
    }
    if (value === '--min-provenance') {
      args.minProvenance = Number(argv[index + 1] || args.minProvenance);
      index += 1;
      continue;
    }
    if (value === '--min-safety') {
      args.minSafety = Number(argv[index + 1] || args.minSafety);
      index += 1;
      continue;
    }
    if (value === '--min-stress-score') {
      args.minStressScore = Number(argv[index + 1] || args.minStressScore);
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
    if (summary.approvedPatterns.length < args.minPatternCoverage) {
      failures.push(`${summary.repo}: approved_patterns=${summary.approvedPatterns.length} below min=${args.minPatternCoverage}`);
    }
    if (summary.missingUrl > 0) {
      failures.push(`${summary.repo}: approved_missing_public_url=${summary.missingUrl}`);
    }
    if (summary.missingPublisher > 0) {
      failures.push(`${summary.repo}: approved_missing_publisher=${summary.missingPublisher}`);
    }
    if (summary.approved > 0) {
      if (summary.stressFloor.messy < args.minMessy) {
        failures.push(`${summary.repo}: approved_avg_messy=${summary.stressFloor.messy.toFixed(1)} below min=${args.minMessy}`);
      }
      if (summary.stressFloor.jargon < args.minJargon) {
        failures.push(`${summary.repo}: approved_avg_jargon=${summary.stressFloor.jargon.toFixed(1)} below min=${args.minJargon}`);
      }
      if (summary.stressFloor.social < args.minSocial) {
        failures.push(`${summary.repo}: approved_avg_social=${summary.stressFloor.social.toFixed(1)} below min=${args.minSocial}`);
      }
      if (summary.stressFloor.fit < args.minFit) {
        failures.push(`${summary.repo}: approved_avg_fit=${summary.stressFloor.fit.toFixed(1)} below min=${args.minFit}`);
      }
      if (summary.stressFloor.provenance < args.minProvenance) {
        failures.push(`${summary.repo}: approved_avg_provenance=${summary.stressFloor.provenance.toFixed(1)} below min=${args.minProvenance}`);
      }
      if (summary.stressFloor.safety < args.minSafety) {
        failures.push(`${summary.repo}: approved_avg_safety=${summary.stressFloor.safety.toFixed(1)} below min=${args.minSafety}`);
      }
      if (summary.maxApprovedStress < args.minStressScore) {
        failures.push(`${summary.repo}: approved_max_stress=${summary.maxApprovedStress.toFixed(1)} below min=${args.minStressScore}`);
      }
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
