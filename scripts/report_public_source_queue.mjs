#!/usr/bin/env node

import fs from 'node:fs';

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function getPatterns(source) {
  const tags = Array.isArray(source.tags) ? source.tags : [];
  return tags.slice(1);
}

function summarizeApp(app, targetDistinctSources) {
  const sources = Array.isArray(app.sources) ? app.sources : [];
  const approved = sources.filter((source) => String(source.review_status || '').toLowerCase() === 'approved');
  const queued = sources.filter((source) => String(source.review_status || '').toLowerCase() === 'queued');
  const rejected = sources.filter((source) => String(source.review_status || '').toLowerCase() === 'rejected');
  const missingUrl = approved.filter((source) => !source.public_url).length;
  const missingPublisher = approved.filter((source) => !source.publisher).length;
  const gap = Math.max(0, targetDistinctSources - approved.length);

  const scoreSource = (source) => {
    const signal = source.signal || {};
    const messy = Number(signal.messy_audio || 0);
    const jargon = Number(signal.jargon_density || 0);
    const social = Number(signal.social_complexity || 0);
    const fit = Number(signal.bonfyre_fit || 0);
    const provenance = Number(signal.provenance_confidence || 0);
    const safety = Number(signal.public_safety || 0);
    return messy * 1.4 + jargon * 1.2 + social * 1.1 + fit * 1.8 + provenance * 1.3 + safety * 1.0;
  };

  const approvedScore = approved.reduce((sum, source) => sum + scoreSource(source), 0);
  const queuedScore = queued.reduce((sum, source) => sum + scoreSource(source), 0);
  const approvedPatterns = [...new Set(approved.flatMap((source) => getPatterns(source)))];
  const queuedPatterns = [...new Set(queued.flatMap((source) => getPatterns(source)))];
  const missingPatterns = queuedPatterns.filter((pattern) => !approvedPatterns.includes(pattern)).slice(0, 4);
  const bestQueued = queued
    .map((source) => ({
      title: source.title,
      query: source.query || '',
      score: scoreSource(source),
      patterns: getPatterns(source)
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 2);

  const readinessScore = Math.max(0, approved.length * 20 + Math.min(queuedScore, 40) - gap * 4);

  return {
    repo: app.repo,
    approved: approved.length,
    queued: queued.length,
    rejected: rejected.length,
    missingUrl,
    missingPublisher,
    gap,
    approvedScore,
    queuedScore,
    readinessScore,
    bestQueued,
    approvedPatterns,
    queuedPatterns,
    missingPatterns
  };
}

function main() {
  const queuePath = process.argv[2];
  const targetDistinctSources = Number(process.argv[3] || 10);
  if (!queuePath) {
    console.error('usage: report_public_source_queue.mjs <queue.json> [target_distinct_sources]');
    process.exit(1);
  }

  const queue = readJson(queuePath);
  const summaries = (queue.apps || []).map((app) => summarizeApp(app, targetDistinctSources));

  const ranked = summaries.slice().sort((a, b) => b.readinessScore - a.readinessScore);

  for (const summary of ranked) {
    console.log(
      `${summary.repo}: readiness=${summary.readinessScore.toFixed(1)} approved=${summary.approved} queued=${summary.queued} rejected=${summary.rejected} gap_to_${targetDistinctSources}=${summary.gap} approved_patterns=${summary.approvedPatterns.length}` +
      (summary.missingUrl ? ` missing_public_url=${summary.missingUrl}` : '') +
      (summary.missingPublisher ? ` missing_publisher=${summary.missingPublisher}` : '')
    );
    if (summary.missingPatterns.length) {
      console.log(`  missing-patterns: ${summary.missingPatterns.join(', ')}`);
    }
    for (const candidate of summary.bestQueued) {
      const patternSuffix = candidate.patterns.length ? ` patterns=${candidate.patterns.join('/')}` : '';
      console.log(`  next: ${candidate.title} [score ${candidate.score.toFixed(1)}]${candidate.query ? ` query=\"${candidate.query}\"` : ''}${patternSuffix}`);
    }
  }
}

main();
