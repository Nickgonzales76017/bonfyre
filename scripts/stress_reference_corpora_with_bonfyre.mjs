#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + '\n', 'utf8');
}

function sanitizeSlug(value) {
  return String(value || 'item')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'item';
}

function getSignal(source, key) {
  return Number((source.signal || {})[key] || 0);
}

function inferContract(app) {
  const repo = String(app.repo || '');
  if (repo.includes('town-box')) {
    return { input_type: 'audio', objective: 'civic-searchable-memory', latency_class: 'interactive', surface: 'pages+jobs' };
  }
  if (repo.includes('podcast-plant')) {
    return { input_type: 'audio', objective: 'publishable-multi-output', latency_class: 'interactive', surface: 'pages+jobs' };
  }
  if (repo.includes('release-radio')) {
    return { input_type: 'audio', objective: 'release-broadcast-fanout', latency_class: 'interactive', surface: 'pages+jobs' };
  }
  if (repo.includes('oss-cockpit')) {
    return { input_type: 'text', objective: 'maintainer-triage-memory', latency_class: 'interactive', surface: 'pages+jobs' };
  }
  if (repo.includes('explain-repo')) {
    return { input_type: 'text', objective: 'repo-walkthrough-memory', latency_class: 'interactive', surface: 'pages+jobs' };
  }
  if (repo.includes('customer-voice') || repo.includes('sales-distiller')) {
    return { input_type: 'audio', objective: 'customer-theme-retrieval', latency_class: 'interactive', surface: 'pages+jobs' };
  }
  if (repo.includes('family-history') || repo.includes('memory-atlas') || repo.includes('local-archive')) {
    return { input_type: 'audio', objective: 'oral-history-memory', latency_class: 'batch', surface: 'pages+jobs' };
  }
  if (repo.includes('legal-prep') || repo.includes('grant-evidence') || repo.includes('freelancer-evidence')) {
    return { input_type: 'audio', objective: 'evidence-bundle-proof', latency_class: 'batch', surface: 'pages+jobs' };
  }
  if (repo.includes('postmortem-atlas')) {
    return { input_type: 'audio', objective: 'incident-chronology-memory', latency_class: 'interactive', surface: 'pages+jobs' };
  }
  if (repo.includes('shift-handoff') || repo.includes('async-standup')) {
    return { input_type: 'audio', objective: 'operational-handoff-summary', latency_class: 'interactive', surface: 'pages+jobs' };
  }
  return { input_type: 'audio', objective: 'publishable-multi-output', latency_class: 'interactive', surface: 'pages+jobs' };
}

function deriveFeedback(source, plan) {
  const messy = getSignal(source, 'messy_audio');
  const jargon = getSignal(source, 'jargon_density');
  const social = getSignal(source, 'social_complexity');
  const fit = getSignal(source, 'bonfyre_fit');
  const provenance = getSignal(source, 'provenance_confidence');
  const safety = getSignal(source, 'public_safety');
  const complexity = (messy + jargon + social) / 15;
  const strength = (fit + provenance + safety) / 15;
  const qualityGain = Number((0.08 + complexity * 0.18 + strength * 0.08).toFixed(3));
  const latencyDelta = Number((0.01 + messy * 0.008 + social * 0.004).toFixed(3));
  const utilityBias = Number((fit / 5).toFixed(3));
  return {
    quality_gain: qualityGain,
    latency_delta: latencyDelta,
    feedback_mode: 'signal-derived',
    exec: Number((0.45 + complexity * 0.35).toFixed(3)),
    artifact: Number((0.50 + fit * 0.09).toFixed(3)),
    tensor: Number((0.42 + jargon * 0.08 + messy * 0.03).toFixed(3)),
    cms: Number((0.38 + fit * 0.08 + provenance * 0.04).toFixed(3)),
    retrieval: Number((0.40 + jargon * 0.07 + social * 0.05).toFixed(3)),
    value: Number((0.35 + utilityBias * 0.35).toFixed(3)),
    predicted_policy_score: Number(plan.predicted_policy_score || 0)
  };
}

function runJson(binary, args, label) {
  let lastResult = null;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const result = spawnSync(binary, args, { encoding: 'utf8' });
    lastResult = result;
    if (result.status === 0 && result.stdout) {
      return JSON.parse(result.stdout);
    }
  }
  const stderr = (lastResult?.stderr || '').trim();
  const stdout = (lastResult?.stdout || '').trim();
  const signal = lastResult?.signal ? ` signal=${lastResult.signal}` : '';
  throw new Error(`${label} failed (${lastResult?.status ?? 'null'}${signal}): ${stderr || stdout || 'no output'}`);
}

function runVoid(binary, args, label) {
  const result = spawnSync(binary, args, { encoding: 'utf8' });
  if (result.status !== 0) {
    const stderr = (result.stderr || '').trim();
    const stdout = (result.stdout || '').trim();
    throw new Error(`${label} failed (${result.status}): ${stderr || stdout || 'no output'}`);
  }
  return result.stdout.trim();
}

function parseArgs(argv) {
  const args = {
    queuePath: '',
    outDir: '',
    repos: [],
    includeQueued: false,
    orchestrateBin: '',
    queueBin: '',
    policyDb: ''
  };

  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (!args.queuePath && !value.startsWith('--')) {
      args.queuePath = value;
      continue;
    }
    if (value === '--out-dir') {
      args.outDir = argv[index + 1] || '';
      index += 1;
      continue;
    }
    if (value === '--repo') {
      const repo = argv[index + 1];
      if (repo) args.repos.push(repo);
      index += 1;
      continue;
    }
    if (value === '--include-queued') {
      args.includeQueued = true;
      continue;
    }
    if (value === '--orchestrate-bin') {
      args.orchestrateBin = argv[index + 1] || '';
      index += 1;
      continue;
    }
    if (value === '--queue-bin') {
      args.queueBin = argv[index + 1] || '';
      index += 1;
      continue;
    }
    if (value === '--policy-db') {
      args.policyDb = argv[index + 1] || '';
      index += 1;
    }
  }

  return args;
}

function average(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function uniqueCount(values) {
  return new Set(values).size;
}

function renderMarkdown(report) {
  const lines = [];
  lines.push('# Bonfyre Reference Corpus Stress Report');
  lines.push('');
  lines.push(`Generated: ${report.generated_at}`);
  lines.push('');
  lines.push('## Totals');
  lines.push('');
  lines.push(`- Apps stressed: ${report.totals.apps}`);
  lines.push(`- Sources stressed: ${report.totals.sources}`);
  lines.push(`- Queued Bonfyre jobs: ${report.totals.queued_jobs}`);
  lines.push(`- Average policy score: ${report.totals.avg_policy_score}`);
  lines.push(`- Average information gain: ${report.totals.avg_information_gain}`);
  lines.push(`- Average latency: ${report.totals.avg_latency}`);
  lines.push(`- Apps with warnings: ${report.totals.apps_with_warnings}`);
  lines.push(`- Approved public sources: ${report.totals.approved_sources}`);
  lines.push(`- Queued public sources: ${report.totals.queued_sources}`);
  lines.push(`- Provenance-backed ratio: ${report.totals.provenance_ratio}`);
  lines.push('');

  for (const app of report.apps) {
    lines.push(`## ${app.title}`);
    lines.push('');
    lines.push(`- Repo: \`${app.repo}\``);
    lines.push(`- Sources stressed: ${app.source_count}`);
    lines.push(`- Differentiation score: ${app.differentiation_score}`);
    lines.push(`- Average policy score: ${app.avg_policy_score}`);
    lines.push(`- Average information gain: ${app.avg_information_gain}`);
    lines.push(`- Average latency: ${app.avg_latency}`);
    lines.push(`- Unique state keys: ${app.unique_state_keys}`);
    lines.push(`- Unique output sets: ${app.unique_output_sets}`);
    lines.push(`- Approved sources: ${app.approved_sources}`);
    lines.push(`- Queued sources: ${app.queued_sources}`);
    lines.push(`- Provenance-backed ratio: ${app.provenance_ratio}`);
    lines.push(`- Modes: ${Object.entries(app.mode_counts).map(([k, v]) => `${k}=${v}`).join(', ') || 'none'}`);
    lines.push(`- Warnings: ${app.warnings.length ? app.warnings.join(', ') : 'none'}`);
    lines.push('');
    lines.push('| Source | Review | Policy | State | Public origin |');
    lines.push('|---|---|---|---|---|');
    for (const source of app.sources) {
      const origin = source.public_url ? `[link](${source.public_url})` : 'pending review';
      lines.push(`| ${source.title} | ${source.review_status} | ${source.policy_source} | \`${source.state_key}\` | ${origin} |`);
    }
    lines.push('');
  }

  return lines.join('\n') + '\n';
}

function renderSummaryJson(report) {
  return {
    generated_at: report.generated_at,
    apps: report.totals.apps,
    sources: report.totals.sources,
    queued_jobs: report.totals.queued_jobs,
    avg_policy_score: report.totals.avg_policy_score,
    avg_information_gain: report.totals.avg_information_gain,
    avg_latency: report.totals.avg_latency,
    apps_with_warnings: report.totals.apps_with_warnings,
    approved_sources: report.totals.approved_sources,
    queued_sources: report.totals.queued_sources,
    provenance_ratio: report.totals.provenance_ratio
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.queuePath) {
    console.error('usage: stress_reference_corpora_with_bonfyre.mjs <queue.json> [--out-dir DIR] [--repo NAME] [--include-queued] [--orchestrate-bin PATH] [--queue-bin PATH] [--policy-db PATH]');
    process.exit(1);
  }

  const root = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
  const orchestrateBin = args.orchestrateBin || path.join(root, 'cmd/BonfyreOrchestrate/bonfyre-orchestrate');
  const queueBin = args.queueBin || path.join(root, 'cmd/BonfyreQueue/bonfyre-queue');
  const outDir = args.outDir || path.join(root, 'site/demos/reference-stress');
  const policyDb = args.policyDb || path.join(outDir, 'orchestrate.db');
  const queueFile = path.join(outDir, 'reference-stress.queue.tsv');
  const workDir = path.join(outDir, 'runs');
  const artifactDir = path.join(outDir, 'artifact');
  fs.mkdirSync(workDir, { recursive: true });
  fs.mkdirSync(artifactDir, { recursive: true });

  const queue = readJson(args.queuePath);
  const apps = (queue.apps || []).filter((app) => !args.repos.length || args.repos.includes(app.repo));
  const report = {
    generated_at: new Date().toISOString(),
    queue_path: path.resolve(args.queuePath),
    out_dir: outDir,
    queue_file: queueFile,
    policy_db: policyDb,
    include_queued: args.includeQueued,
    totals: {
      apps: 0,
      sources: 0,
      queued_jobs: 0,
      avg_policy_score: 0,
      avg_information_gain: 0,
      avg_latency: 0
    },
    apps: []
  };

  const previousPolicyDb = process.env.BONFYRE_ORCHESTRATE_POLICY_DB;
  process.env.BONFYRE_ORCHESTRATE_POLICY_DB = policyDb;

  try {
    for (const app of apps) {
      const contract = inferContract(app);
      const sources = (app.sources || []).filter((source) => {
        const status = String(source.review_status || '').toLowerCase();
        return status === 'approved' || (args.includeQueued && status === 'queued');
      });
      const appSlug = sanitizeSlug(app.repo);
      const appDir = path.join(workDir, appSlug);
      fs.mkdirSync(appDir, { recursive: true });

      const appSummary = {
        repo: app.repo,
        title: app.title || app.repo,
        contract,
        source_count: sources.length,
        approved_sources: 0,
        queued_sources: 0,
        provenance_ratio: 0,
        mode_counts: {},
        avg_policy_score: 0,
        avg_information_gain: 0,
        avg_latency: 0,
        avg_cost: 0,
        unique_state_keys: 0,
        unique_policy_sources: 0,
        unique_output_sets: 0,
        differentiation_score: 0,
        warnings: [],
        binaries: {},
        sources: []
      };

      for (const source of sources) {
        const sourceSlug = sanitizeSlug(source.id || source.title);
        const requestPath = path.join(appDir, `${sourceSlug}.request.json`);
        const feedbackPath = path.join(appDir, `${sourceSlug}.feedback.json`);
        const payloadPath = path.join(appDir, `${sourceSlug}.payload.json`);
        const planPath = path.join(appDir, `${sourceSlug}.plan.json`);

        const request = {
          ...contract,
          artifact_path: `reference://${app.repo}/${sourceSlug}`,
          reference_repo: app.repo,
          reference_title: source.title,
          reference_url: source.public_url || '',
          reference_status: source.review_status || 'unknown',
          source_query: source.query || '',
          source_tags: Array.isArray(source.tags) ? source.tags.join(', ') : '',
          source_messy: getSignal(source, 'messy_audio'),
          source_jargon: getSignal(source, 'jargon_density'),
          source_social: getSignal(source, 'social_complexity'),
          source_fit: getSignal(source, 'bonfyre_fit')
        };
        writeJson(requestPath, request);

        const plan = runJson(orchestrateBin, ['plan', requestPath], `orchestrate plan ${source.title}`);
        writeJson(planPath, plan);

        const feedback = deriveFeedback(source, plan);
        writeJson(feedbackPath, feedback);
        runVoid(orchestrateBin, ['feedback', requestPath, feedbackPath], `orchestrate feedback ${source.title}`);

        const payload = {
          app: app.repo,
          source_id: source.id,
          source_title: source.title,
          public_url: source.public_url || '',
          review_status: source.review_status,
          query: source.query || '',
          plan_path: planPath,
          feedback_path: feedbackPath,
          policy_source: plan.policy_source || '',
          predicted_policy_score: Number(plan.predicted_policy_score || 0),
          predicted_information_gain: Number(plan.predicted_information_gain || 0),
          predicted_latency: Number(plan.predicted_latency || 0),
          expected_outputs: Array.isArray(plan.expected_outputs) ? plan.expected_outputs : [],
          selected_binaries: Array.isArray(plan.selected_binaries) ? plan.selected_binaries : [],
          booster_binaries: Array.isArray(plan.booster_binaries) ? plan.booster_binaries : []
        };
        writeJson(payloadPath, payload);

        const enqueue = runJson(
          queueBin,
          ['enqueue', queueFile, `${appSlug}-${sourceSlug}`, payloadPath, '--source', app.repo, '--priority', String(source.review_status === 'approved' ? 10 : 30)],
          `queue enqueue ${source.title}`
        );

        appSummary.mode_counts[plan.mode || 'unknown'] = (appSummary.mode_counts[plan.mode || 'unknown'] || 0) + 1;
        for (const binary of [...(plan.selected_binaries || []), ...(plan.booster_binaries || [])]) {
          appSummary.binaries[binary] = (appSummary.binaries[binary] || 0) + 1;
        }

        appSummary.sources.push({
          id: source.id,
          title: source.title,
          public_url: source.public_url || '',
          review_status: source.review_status,
          queue_job_id: enqueue.id,
          mode: plan.mode,
          policy_source: plan.policy_source,
          predicted_policy_score: Number(plan.predicted_policy_score || 0),
          predicted_information_gain: Number(plan.predicted_information_gain || 0),
          predicted_latency: Number(plan.predicted_latency || 0),
          predicted_cost: Number(plan.predicted_cost || 0),
          objective_family: plan.objective_family || '',
          state_key: plan.state_key || '',
          expected_outputs: payload.expected_outputs,
          selected_binaries: payload.selected_binaries,
          booster_binaries: payload.booster_binaries,
          feedback_mode: feedback.feedback_mode
        });
      }

      appSummary.avg_policy_score = Number(average(appSummary.sources.map((source) => source.predicted_policy_score)).toFixed(3));
      appSummary.avg_information_gain = Number(average(appSummary.sources.map((source) => source.predicted_information_gain)).toFixed(3));
      appSummary.avg_latency = Number(average(appSummary.sources.map((source) => source.predicted_latency)).toFixed(3));
      appSummary.avg_cost = Number(average(appSummary.sources.map((source) => source.predicted_cost)).toFixed(3));
      appSummary.approved_sources = appSummary.sources.filter((source) => source.review_status === 'approved').length;
      appSummary.queued_sources = appSummary.sources.filter((source) => source.review_status === 'queued').length;
      appSummary.provenance_ratio = appSummary.source_count
        ? Number((appSummary.approved_sources / appSummary.source_count).toFixed(3))
        : 0;
      appSummary.unique_state_keys = uniqueCount(appSummary.sources.map((source) => source.state_key));
      appSummary.unique_policy_sources = uniqueCount(appSummary.sources.map((source) => source.policy_source));
      appSummary.unique_output_sets = uniqueCount(appSummary.sources.map((source) => source.expected_outputs.join('|')));
      appSummary.differentiation_score = Number((
        (appSummary.unique_state_keys / Math.max(appSummary.source_count, 1)) * 0.45 +
        (appSummary.unique_output_sets / Math.max(appSummary.source_count, 1)) * 0.35 +
        (appSummary.unique_policy_sources / Math.max(appSummary.source_count, 1)) * 0.20
      ).toFixed(3));
      if (appSummary.source_count > 1 && appSummary.unique_state_keys <= 1) {
        appSummary.warnings.push('state-collapse');
      }
      if (appSummary.source_count > 1 && appSummary.unique_output_sets <= 1) {
        appSummary.warnings.push('output-collapse');
      }
      if (appSummary.source_count > 1 && appSummary.differentiation_score < 0.55) {
        appSummary.warnings.push('low-plan-differentiation');
      }
      report.apps.push(appSummary);
    }
  } finally {
    if (previousPolicyDb === undefined) {
      delete process.env.BONFYRE_ORCHESTRATE_POLICY_DB;
    } else {
      process.env.BONFYRE_ORCHESTRATE_POLICY_DB = previousPolicyDb;
    }
  }

  report.totals.apps = report.apps.length;
  report.totals.sources = report.apps.reduce((sum, app) => sum + app.source_count, 0);
  report.totals.queued_jobs = report.apps.reduce((sum, app) => sum + app.sources.length, 0);
  report.totals.avg_policy_score = Number(average(report.apps.map((app) => app.avg_policy_score)).toFixed(3));
  report.totals.avg_information_gain = Number(average(report.apps.map((app) => app.avg_information_gain)).toFixed(3));
  report.totals.avg_latency = Number(average(report.apps.map((app) => app.avg_latency)).toFixed(3));
  report.totals.apps_with_warnings = report.apps.filter((app) => app.warnings.length > 0).length;
  report.totals.approved_sources = report.apps.reduce((sum, app) => sum + app.approved_sources, 0);
  report.totals.queued_sources = report.apps.reduce((sum, app) => sum + app.queued_sources, 0);
  report.totals.provenance_ratio = report.totals.sources
    ? Number((report.totals.approved_sources / report.totals.sources).toFixed(3))
    : 0;

  const queueStats = runJson(queueBin, ['stats', queueFile], 'queue stats');
  report.queue_stats = queueStats;
  const jsonReportPath = path.join(outDir, 'reference-stress-report.json');
  const markdownReportPath = path.join(outDir, 'reference-stress-report.md');
  writeJson(jsonReportPath, report);
  fs.writeFileSync(markdownReportPath, renderMarkdown(report), 'utf8');

  const artifactBriefPath = path.join(artifactDir, 'brief.md');
  const artifactSummaryPath = path.join(artifactDir, 'summary.json');
  fs.writeFileSync(artifactBriefPath, renderMarkdown(report), 'utf8');
  writeJson(artifactSummaryPath, renderSummaryJson(report));

  const emitBin = path.join(root, 'cmd/BonfyreEmit/bonfyre-emit');
  if (fs.existsSync(emitBin)) {
    try {
      runVoid(emitBin, [artifactDir, '--format', 'bundle', '--out', artifactDir], 'emit stress bundle');
      report.publish_artifact = {
        artifact_dir: artifactDir,
        bundle_dir: path.join(artifactDir, 'emit'),
        markdown: artifactBriefPath,
        summary_json: artifactSummaryPath
      };
      writeJson(jsonReportPath, report);
    } catch (error) {
      report.publish_artifact = {
        artifact_dir: artifactDir,
        error: String(error.message || error)
      };
      writeJson(jsonReportPath, report);
    }
  }
  console.log(JSON.stringify(report, null, 2));
}

main();
