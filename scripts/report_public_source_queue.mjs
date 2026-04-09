#!/usr/bin/env node

import fs from 'node:fs';

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function summarizeApp(app, targetDistinctSources) {
  const sources = Array.isArray(app.sources) ? app.sources : [];
  const approved = sources.filter((source) => String(source.review_status || '').toLowerCase() === 'approved');
  const queued = sources.filter((source) => String(source.review_status || '').toLowerCase() === 'queued');
  const rejected = sources.filter((source) => String(source.review_status || '').toLowerCase() === 'rejected');
  const missingUrl = approved.filter((source) => !source.public_url).length;
  const missingPublisher = approved.filter((source) => !source.publisher).length;
  const gap = Math.max(0, targetDistinctSources - approved.length);

  return {
    repo: app.repo,
    approved: approved.length,
    queued: queued.length,
    rejected: rejected.length,
    missingUrl,
    missingPublisher,
    gap
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

  for (const summary of summaries) {
    console.log(
      `${summary.repo}: approved=${summary.approved} queued=${summary.queued} rejected=${summary.rejected} gap_to_${targetDistinctSources}=${summary.gap}` +
      (summary.missingUrl ? ` missing_public_url=${summary.missingUrl}` : '') +
      (summary.missingPublisher ? ` missing_publisher=${summary.missingPublisher}` : '')
    );
  }
}

main();
