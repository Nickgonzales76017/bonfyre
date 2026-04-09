#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const AUDIO_VIDEO_EXT = /\.(wav|mp3|m4a|ogg|flac|webm|mp4|mov)$/i;
const BANNED_COPY = [
  /\bseeded\b/i,
  /\bdemo dataset\b/i,
  /\bdemo data\b/i
];

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function exists(filePath) {
  try {
    fs.accessSync(filePath);
    return true;
  } catch (_) {
    return false;
  }
}

function listMirroredMedia(dirPath) {
  if (!exists(dirPath)) return [];
  const results = [];
  for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
    const child = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      results.push(...listMirroredMedia(child));
      continue;
    }
    if (AUDIO_VIDEO_EXT.test(entry.name)) results.push(child);
  }
  return results;
}

function isPublicHttp(url) {
  return /^https?:\/\//i.test(String(url || ''));
}

function sourceLinksFor(item) {
  const links = [];
  if (item && item.sourceUrl) {
    links.push({
      href: item.sourceUrl,
      label: item.sourceLabel || 'Open public source'
    });
  }
  if (Array.isArray(item && item.sourceLinks)) {
    for (const link of item.sourceLinks) {
      if (link && link.href) links.push(link);
    }
  }
  return links;
}

function countTemplateDuplicates(items) {
  const counts = new Map();
  for (const item of items) {
    const key = JSON.stringify({
      file: item.file,
      brief: item.brief,
      tags: item.tags || [],
      outputs: item.outputs || []
    });
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  let maxDuplicate = 0;
  for (const value of counts.values()) {
    if (value > maxDuplicate) maxDuplicate = value;
  }
  return maxDuplicate;
}

function validateCorpus(filePath, options) {
  const issues = [];
  const warnings = [];
  const items = readJson(filePath);
  if (!Array.isArray(items) || !items.length) {
    issues.push('corpus is empty or not an array');
    return { items: [], issues, warnings };
  }

  const ids = new Set();
  for (const [index, item] of items.entries()) {
    const prefix = `item ${index + 1}`;
    if (!item.id) issues.push(`${prefix} missing id`);
    if (item.id && ids.has(item.id)) issues.push(`${prefix} duplicate id: ${item.id}`);
    ids.add(item.id);
    if (!item.file) issues.push(`${prefix} missing file`);
    if (!Array.isArray(item.outputs) || !item.outputs.length) issues.push(`${prefix} missing outputs`);
    if (!item.brief) warnings.push(`${prefix} missing brief`);

    const textFields = [item.file, item.time, item.brief, item.whyItMatters, item.searchIntro, item.sourceCopy];
    for (const pattern of BANNED_COPY) {
      if (textFields.some((value) => pattern.test(String(value || '')))) {
        issues.push(`${prefix} contains staged-demo copy: ${pattern}`);
      }
    }

    const links = sourceLinksFor(item);
    if (options.strictProvenance && !links.length) {
      issues.push(`${prefix} missing public provenance link`);
    }
    for (const link of links) {
      if (!isPublicHttp(link.href)) {
        issues.push(`${prefix} has non-public provenance link: ${link.href}`);
      }
    }
  }

  if (items.length < options.minCount) {
    issues.push(`corpus has ${items.length} records, below minimum ${options.minCount}`);
  }

  const maxDuplicate = countTemplateDuplicates(items);
  if (maxDuplicate > options.maxDuplicateTemplate) {
    warnings.push(`largest repeated record template count is ${maxDuplicate}`);
  }

  return { items, issues, warnings };
}

function validateRepo(repoPath, options) {
  const sitePath = path.join(repoPath, 'site');
  const demoDir = path.join(sitePath, 'demos');
  const corpusFiles = [];

  function walk(dirPath) {
    if (!exists(dirPath)) return;
    for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
      const child = path.join(dirPath, entry.name);
      if (entry.isDirectory()) {
        walk(child);
        continue;
      }
      if (entry.name === 'demo-items.json') corpusFiles.push(child);
    }
  }

  walk(demoDir);

  const mirroredMedia = listMirroredMedia(demoDir);
  const corpora = corpusFiles.map((filePath) => ({
    filePath,
    ...validateCorpus(filePath, options)
  }));

  return {
    repoPath,
    corpora,
    mirroredMedia
  };
}

function parseArgs(argv) {
  const options = {
    root: '/Users/nickgonzales/Projects',
    repos: [],
    minCount: 25,
    maxDuplicateTemplate: 8,
    strictProvenance: false
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--root') options.root = argv[++i];
    else if (arg === '--repo') options.repos.push(argv[++i]);
    else if (arg === '--min-count') options.minCount = Number(argv[++i] || options.minCount);
    else if (arg === '--max-duplicate-template') options.maxDuplicateTemplate = Number(argv[++i] || options.maxDuplicateTemplate);
    else if (arg === '--strict-provenance') options.strictProvenance = true;
  }

  if (!options.repos.length) {
    options.repos = fs.readdirSync(options.root)
      .filter((name) => /^(pages-|bonfyre-)/.test(name))
      .map((name) => path.join(options.root, name))
      .filter((repoPath) => exists(path.join(repoPath, '.git')));
  } else {
    options.repos = options.repos.map((repo) => path.isAbsolute(repo) ? repo : path.join(options.root, repo));
  }

  return options;
}

function main() {
  const options = parseArgs(process.argv);
  let totalIssues = 0;
  let totalWarnings = 0;

  for (const repoPath of options.repos) {
    const report = validateRepo(repoPath, options);
    const repoName = path.basename(repoPath);
    if (!report.corpora.length && !report.mirroredMedia.length) continue;

    console.log(`\n== ${repoName} ==`);

    if (report.mirroredMedia.length) {
      totalIssues += report.mirroredMedia.length;
      console.log(`ERROR mirrored media files in public demos:`);
      for (const filePath of report.mirroredMedia) console.log(`  - ${filePath}`);
    }

    for (const corpus of report.corpora) {
      const rel = path.relative(repoPath, corpus.filePath);
      console.log(`corpus ${rel}: ${corpus.items.length} records`);
      if (!corpus.issues.length && !corpus.warnings.length) {
        console.log('  ok');
        continue;
      }
      for (const issue of corpus.issues) {
        totalIssues += 1;
        console.log(`  ERROR ${issue}`);
      }
      for (const warning of corpus.warnings) {
        totalWarnings += 1;
        console.log(`  WARN  ${warning}`);
      }
    }
  }

  console.log(`\nsummary: ${totalIssues} issue(s), ${totalWarnings} warning(s)`);
  process.exit(totalIssues ? 1 : 0);
}

main();
