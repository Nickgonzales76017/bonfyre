#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const BONFYRE_OSS = '/Users/nickgonzales/Projects/bonfyre-oss';
const PROJECTS = '/Users/nickgonzales/Projects';

const sharedHelpers = [
  {
    source: path.join(BONFYRE_OSS, 'site/runtime/github-intake.js'),
    dest: 'site/js/github-intake.js'
  },
  {
    source: path.join(BONFYRE_OSS, 'site/runtime/github-jobs.js'),
    dest: 'site/js/github-jobs.js'
  }
];

const APPS = {
  'pages-async-standup': {
    routeLabel: 'standup intake',
    appName: 'Async Standup Newspaper',
    dropzoneText: 'Choose standup recordings or written updates to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, detects tone/blockers → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.tone.json', step: 'tone' },
      { suffix: '.tags.json', step: null }
    ],
    completeWhenAny: ['tone', 'tags'],
    flagTerms: ['blocker', 'blocked', 'risk'],
    cardMode: 'prefix',
    cardPrefix: '📰 ',
    emptyBody: 'Composing edition…',
    trueBadge: '<span class="tag tag-urgent">BLOCKER</span>',
    falseBadge: ''
  },
  'pages-competitive-intel': {
    routeLabel: 'competitive intel',
    appName: 'Competitive Intelligence Scrapbook',
    dropzoneText: 'Choose competitor recordings, articles, or analysis docs to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, tags, embeds into vector DB → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.tags.json', step: 'tag' },
      { suffix: '.embed.json', step: 'embed' }
    ],
    completeWhenAny: ['tags', 'embed'],
    flagTerms: ['threat', 'urgent', 'risk'],
    cardMode: 'prefix',
    cardPrefix: '🔍 ',
    emptyBody: 'Analyzing competitive signal…',
    trueBadge: '<span class="tag tag-urgent">THREAT</span>',
    falseBadge: ''
  },
  'pages-customer-voice': {
    routeLabel: 'customer voice',
    appName: 'Customer Voice Observatory',
    dropzoneText: 'Choose customer call recordings to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, detects tone, tags → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.tone.json', step: 'tone' },
      { suffix: '.tags.json', step: 'tag' },
      { suffix: '.embed.json', step: 'embed' }
    ],
    completeWhenAny: ['tone', 'tags', 'embed'],
    flagTerms: ['negative', 'frustrated', 'churn', 'angry'],
    cardMode: 'flagIcon',
    trueIcon: '🔴',
    falseIcon: '🟢',
    emptyBody: 'Analyzing sentiment…',
    trueBadge: '<span class="tag tag-urgent">NEGATIVE</span>',
    falseBadge: '<span class="tag">POSITIVE</span>'
  },
  'pages-grant-evidence': {
    routeLabel: 'grant evidence',
    appName: 'Grant-Writing Evidence Base',
    dropzoneText: 'Choose interview recordings, impact reports, or program docs to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, clips, tags, meters → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.clips.json', step: 'clips' },
      { suffix: '.tags.json', step: 'tag' },
      { suffix: '.embed.json', step: 'embed' },
      { suffix: '.meter.json', step: 'meter' },
      { suffix: '.pack.json', step: 'pack' }
    ],
    completeWhenAny: ['clips', 'tags', 'embed', 'meter', 'pack'],
    flagTerms: ['impact', 'story', 'outcome'],
    cardMode: 'prefix',
    cardPrefix: '📊 ',
    emptyBody: 'Extracting evidence…',
    trueBadge: '<span class="tag" style="background:#1a2e1a;color:var(--green)">IMPACT STORY</span>',
    falseBadge: ''
  },
  'pages-legal-prep': {
    routeLabel: 'legal prep',
    appName: 'Personal Legal Prep Binder',
    dropzoneText: 'Choose legal recordings, contracts, or case docs to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, proofs, packages → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.proof.json', step: 'proof' },
      { suffix: '.pack.json', step: 'pack' }
    ],
    completeWhenAny: ['proof', 'pack'],
    flagTerms: ['key evidence', 'material', 'important'],
    cardMode: 'prefix',
    cardPrefix: '⚖️ ',
    emptyBody: 'Preparing binder…',
    trueBadge: '<span class="tag tag-urgent">KEY EVIDENCE</span>',
    falseBadge: ''
  },
  'pages-local-archive': {
    routeLabel: 'local archive',
    appName: 'Local Archive Explorer',
    dropzoneText: 'Choose historical recordings, documents, or photos to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, tags, indexes → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.tags.json', step: 'tag' },
      { suffix: '.embed.json', step: 'embed' },
      { suffix: '.index.json', step: 'index' }
    ],
    completeWhenAny: ['tags', 'embed', 'index'],
    flagTerms: ['highlight', 'featured'],
    cardMode: 'prefix',
    cardPrefix: '📜 ',
    emptyBody: 'Indexing archive item…',
    trueBadge: '<span class="tag">HIGHLIGHTED</span>',
    falseBadge: ''
  },
  'pages-museum-exhibit': {
    routeLabel: 'museum exhibit',
    appName: 'Museum Exhibit Builder',
    dropzoneText: 'Choose oral histories, artifact photos, or curatorial notes to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, clips, tags → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.clips.json', step: 'clips' },
      { suffix: '.tags.json', step: 'tag' }
    ],
    completeWhenAny: ['clips', 'tags'],
    flagTerms: ['featured', 'hero'],
    cardMode: 'prefix',
    cardPrefix: '🏛 ',
    emptyBody: 'Curating exhibit…',
    trueBadge: '<span class="tag">FEATURED</span>',
    falseBadge: ''
  },
  'pages-oss-cockpit': {
    routeLabel: 'maintainer cockpit',
    appName: 'OSS Maintainer Cockpit',
    dropzoneText: 'Choose issue exports, meeting notes, or triage recordings to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> ingests, tags, embeds → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.tags.json', step: 'tag' },
      { suffix: '.embed.json', step: 'embed' }
    ],
    completeWhenAny: ['tags', 'embed'],
    flagTerms: ['priority', 'urgent', 'p0', 'p1'],
    cardMode: 'prefix',
    cardPrefix: '🛠 ',
    emptyBody: 'Triaging maintainer signal…',
    trueBadge: '<span class="tag tag-urgent">PRIORITY</span>',
    falseBadge: ''
  },
  'pages-postmortem-atlas': {
    routeLabel: 'postmortem atlas',
    appName: 'Incident Postmortem Atlas',
    dropzoneText: 'Choose incident recordings or postmortem notes to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, tags, indexes → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.tags.json', step: 'tag' },
      { suffix: '.embed.json', step: 'embed' },
      { suffix: '.index.json', step: 'index' }
    ],
    completeWhenAny: ['tags', 'embed', 'index'],
    flagTerms: ['sev-high', 'critical', 'p0', 'outage'],
    cardMode: 'flagIcon',
    trueIcon: '🔴',
    falseIcon: '🟡',
    emptyBody: 'Analyzing incident…',
    trueBadge: '<span class="tag tag-urgent">SEV-HIGH</span>',
    falseBadge: '<span class="tag">SEV-LOW</span>'
  },
  'pages-procurement-memory': {
    routeLabel: 'procurement memory',
    appName: 'Procurement Memory Site',
    dropzoneText: 'Choose vendor call recordings, contracts, or RFP docs to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, tags, values via ledger → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.tags.json', step: 'tag' },
      { suffix: '.embed.json', step: 'embed' },
      { suffix: '.ledger.json', step: 'ledger' }
    ],
    completeWhenAny: ['tags', 'embed', 'ledger'],
    flagTerms: ['renewal', 'renewal due', 'expiration'],
    cardMode: 'prefix',
    cardPrefix: '📦 ',
    emptyBody: 'Indexing procurement record…',
    trueBadge: '<span class="tag tag-urgent">RENEWAL DUE</span>',
    falseBadge: ''
  },
  'pages-release-radio': {
    routeLabel: 'release radio',
    appName: 'Release-Note Radio',
    dropzoneText: 'Choose changelogs, release notes, or commentary audio to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> briefs, narrates, renders → <strong style="color:#818cf8">WASM</strong> preview in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.narrate.json', step: 'narrate' }
    ],
    completeWhenAny: ['narrate'],
    completeAfterBriefOnly: true,
    flagTerms: ['breaking', 'migration', 'deprecated'],
    cardMode: 'prefix',
    cardPrefix: '📻 ',
    emptyBody: 'Broadcasting release…',
    trueBadge: '<span class="tag tag-urgent">BREAKING</span>',
    falseBadge: ''
  },
  'pages-sales-distiller': {
    routeLabel: 'sales distiller',
    appName: 'Sales Call Distiller',
    dropzoneText: 'Choose sales call recordings to prepare for GitHub-native intake',
    heroText: 'Prepare intake copy → upload with <strong style="color:var(--accent)">GitHub’s own web UI</strong><br>→ <strong style="color:var(--accent)">Actions</strong> transcribes, detects tone, clips highlights → <strong style="color:#818cf8">WASM</strong> briefs in-browser → Pages deploys',
    extraArtifacts: [
      { suffix: '.tone.json', step: 'tone' },
      { suffix: '.clips.json', step: 'clips' },
      { suffix: '.tags.json', step: 'tag' }
    ],
    completeWhenAny: ['tone', 'clips', 'tags'],
    flagTerms: ['objection', 'price', 'hesitation', 'blocked'],
    cardMode: 'flagIcon',
    trueIcon: '🔴',
    falseIcon: '🟢',
    emptyBody: 'Distilling call…',
    trueBadge: '<span class="tag tag-urgent">OBJECTION</span>',
    falseBadge: ''
  }
};

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function buildReadme(repoName, currentReadme) {
  const lines = currentReadme.split('\n');
  const heading = lines.find((line) => line.startsWith('# ')) || `# ${repoName}`;
  const subtitle = lines.find((line) => line.startsWith('> ')) || '>';
  return `${heading}

${subtitle}


## Pipeline Intelligence

| Capability | Detail |
|---|---|
| Transcription quality | **0.997** (HCP v3.2, tiny q5_0, 29 MB model) |
| Hallucination detection | **9 layers** — 0.09% error rate (1 in 1,096 segments) |
| Pipeline latency | **< 8 ms** per stage |
| Total binary size | **~2.1 MB** (48 static C11 binaries) |
| JSON compression | **9.3%** ratio with O(1) field reads (Lambda Tensors) |
| OpenAI compatibility | Drop-in via **bonfyre-proxy** (\`/v1/audio/transcriptions\`, \`/v1/chat/completions\`) |
| Tests passing | **167** |

## Quick Start

### Browser-first GitHub-native intake

1. Open the live Pages app.
2. Select a file and download the intake-ready renamed copy.
3. Open the repo \`input/\` folder from the app and upload that file with GitHub's own web UI.
4. Commit to \`main\`; the Bonfyre Actions runtime processes it and republishes the site.

### Local repo workflow

\`\`\`bash
make setup
cp ~/my-recording.wav input/
git add input/ && git commit -m "add recording"
git push
\`\`\`

## Architecture

\`\`\`
browser prepare  →  GitHub web upload  →  input/
                                          ↓
                             reusable Bonfyre runtime workflow
                                          ↓
                         artifacts/ + site/job.json + site/summary.json
                                          ↓
                                   site/  →  GitHub Pages
\`\`\`

## Powered By

[Bonfyre](https://github.com/Nickgonzales76017/bonfyre) — 48 composable C11 binaries. ~2.1 MB total.
`;
}

function buildCardTitleExpr(meta) {
  if (meta.cardMode === 'flagIcon') {
    return '${cardIcon} ${h.file}';
  }
  return `${meta.cardPrefix}\${h.file}`;
}

function buildRenderPrelude(meta) {
  if (meta.cardMode === 'flagIcon') {
    return `const cardIcon = h.flagged ? ${JSON.stringify(meta.trueIcon)} : ${JSON.stringify(meta.falseIcon)};`;
  }
  return '';
}

function buildTagsExpr(meta) {
  const trueBadge = meta.trueBadge || '';
  const falseBadge = meta.falseBadge || '';
  if (falseBadge) {
    return `\${h.flagged ? ${JSON.stringify(trueBadge)} : ${JSON.stringify(falseBadge)}}\${(h.tags||[]).map(t=>\`<span class="tag">\${t}</span>\`).join("")}`;
  }
  return `\${h.flagged ? ${JSON.stringify(trueBadge)} : ""}\${(h.tags||[]).map(t=>\`<span class="tag">\${t}</span>\`).join("")}`;
}

function buildScript(repoName, meta) {
  const storeKey = `bonfyre_${repoName}_config`;
  const itemsKey = `bonfyre_${repoName}_items`;
  const defaultRepo = `Nickgonzales76017/${repoName}`;
  const artifactsJson = JSON.stringify(meta.extraArtifacts);
  const completeWhenAny = JSON.stringify(meta.completeWhenAny || []);
  const flagTerms = JSON.stringify(meta.flagTerms || []);
  const renderPrelude = buildRenderPrelude(meta);
  const cardTitleExpr = buildCardTitleExpr(meta);
  const tagsExpr = buildTagsExpr(meta);

  return `const STORE_KEY = '${storeKey}';
const ITEMS_KEY = '${itemsKey}';
const INPUT_PATH = 'input';
const DEFAULT_REPO = '${defaultRepo}';
const DEFAULT_BRANCH = 'main';
const APP_NAME = ${JSON.stringify(meta.appName)};
const ROUTE_LABEL = ${JSON.stringify(meta.routeLabel)};
const EXTRA_ARTIFACTS = ${artifactsJson};
const COMPLETE_WHEN_ANY = ${completeWhenAny};
const FLAG_TERMS = ${flagTerms};
const COMPLETE_AFTER_BRIEF_ONLY = ${meta.completeAfterBriefOnly ? 'true' : 'false'};

let config = Object.assign({ repo: DEFAULT_REPO, branch: DEFAULT_BRANCH }, JSON.parse(localStorage.getItem(STORE_KEY) || '{}'));
let items = JSON.parse(localStorage.getItem(ITEMS_KEY) || '[]');
let briefWorker = null;
let activeIntake = null;

function toggleConfig() {
  const panel = document.getElementById('configPanel');
  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
  if (panel.style.display === 'block') {
    document.getElementById('cfgRepo').value = config.repo || '';
    document.getElementById('cfgBranch').value = config.branch || 'main';
  }
}

function saveConfig() {
  config = {
    repo: document.getElementById('cfgRepo').value.trim() || DEFAULT_REPO,
    branch: document.getElementById('cfgBranch').value.trim() || DEFAULT_BRANCH
  };
  localStorage.setItem(STORE_KEY, JSON.stringify(config));
  document.getElementById('configPanel').style.display = 'none';
}

const dropzone = document.getElementById('dropzone');
const fileInput = document.getElementById('fileInput');

dropzone.addEventListener('click', () => fileInput.click());
dropzone.addEventListener('dragover', e => { e.preventDefault(); dropzone.classList.add('drag-over'); });
dropzone.addEventListener('dragleave', () => dropzone.classList.remove('drag-over'));
dropzone.addEventListener('drop', e => {
  e.preventDefault();
  dropzone.classList.remove('drag-over');
  [...e.dataTransfer.files].forEach(f => handleFile(f));
});
fileInput.addEventListener('change', e => { [...e.target.files].forEach(f => handleFile(f)); });

function safeJson(raw) {
  try {
    return JSON.parse(raw);
  } catch (_) {
    return null;
  }
}

function matchesFlagTerms(value) {
  const haystack = typeof value === 'string' ? value.toLowerCase() : JSON.stringify(value || {}).toLowerCase();
  return FLAG_TERMS.some(term => haystack.includes(term));
}

function applyArtifactSignals(item, name, raw) {
  const parsed = safeJson(raw);
  if (name === 'tags' && parsed && Array.isArray(parsed.tags)) {
    item.tags = parsed.tags;
    if (!item.flagged && matchesFlagTerms(parsed.tags)) item.flagged = true;
  }
  if (name === 'tone') {
    if (!item.flagged && (matchesFlagTerms(raw) || matchesFlagTerms(parsed))) item.flagged = true;
  }
  if (!item.flagged && matchesFlagTerms(raw)) item.flagged = true;
}

async function handleFile(file) {
  if (!config.repo) {
    toggleConfig();
    alert('Configure the GitHub repository first.');
    return;
  }

  const preparedName = BonfyreGitHubIntake.createPreparedName(file.name);
  const itemId = preparedName.replace(/\\.[^.]*$/, '');
  const targetPath = INPUT_PATH + '/' + preparedName;

  showPipeline();
  updateStep('prepare', 'active');
  updateStep('brief', 'wasm');

  activeIntake = {
    id: itemId,
    file: preparedName,
    originalFile: file.name,
    targetPath,
    uploadFolderUrl: buildUploadFolderUrl(),
    pollingStarted: false
  };

  try {
    const preparedDownload = BonfyreGitHubIntake.createPreparedDownload(file, preparedName);
    updateStep('prepare', 'done');
    updateStep('commit', 'active');
    BonfyreGitHubJobs.showIntakePreview({
      intake: activeIntake,
      preparedDownload,
      routeLabel: ROUTE_LABEL,
      branch: config.branch
    });

    const item = {
      id: itemId,
      file: preparedName,
      time: new Date().toLocaleString(),
      status: 'awaiting-upload',
      brief: null,
      tags: [],
      flagged: false,
      targetPath
    };

    items = BonfyreGitHubJobs.upsertAndPersist(ITEMS_KEY, items, item);
    renderBoard();

    if (BonfyreGitHubIntake.isTextLikeFile(file)) {
      const text = await file.text();
      runBriefWasm(text, itemId);
    } else {
      showWasmPreview(BonfyreGitHubJobs.buildReadyMessage(APP_NAME));
    }
  } catch (err) {
    console.error('Intake preparation failed:', err);
    alert('Intake preparation failed: ' + err.message);
    hidePipeline();
  }
}

async function githubGetFile(filePath) {
  return BonfyreGitHubIntake.fetchRawFile({
    repo: config.repo,
    branch: config.branch,
    path: filePath
  });
}

function buildUploadFolderUrl() {
  return BonfyreGitHubIntake.buildUploadFolderUrl({
    repo: config.repo,
    branch: config.branch,
    inputPath: INPUT_PATH
  });
}

function markUploaded() {
  items = BonfyreGitHubJobs.markUploaded({
    activeIntake,
    items,
    storageKey: ITEMS_KEY,
    updateStep,
    renderBoard,
    onStartPoll: function(intake) {
      pollForCompletion(intake.id, intake.file);
    }
  });
}

function initBriefWorker() {
  if (window.Worker) {
    try {
      briefWorker = new Worker('js/brief-worker.js');
      briefWorker.onmessage = function(e) {
        if (e.data.type === 'brief') {
          showWasmPreview(e.data.result);
          items = BonfyreGitHubJobs.updateAndPersist(ITEMS_KEY, items, e.data.id, function(item) {
            item.brief = e.data.result;
            item.status = 'previewed';
          });
          renderBoard();
        }
      };
    } catch (_) {}
  }
}

function runBriefWasm(transcript, itemId) {
  if (briefWorker) {
    briefWorker.postMessage({ type: 'brief', text: transcript, id: itemId });
    updateStep('brief', 'active');
  }
}

function pollForCompletion(itemId, fileName) {
  const base = fileName.replace(/\\.[^.]*$/, '');
  const seen = {};

  BonfyreGitHubJobs.pollArtifacts({
    onError: function(err) {
      console.warn('[bonfyre] poll:', err.message);
    },
    onPoll: async function(ctrl) {
      const transcript = await githubGetFile('artifacts/' + base + '.transcript.json');
      if (!transcript) return;

      updateStep('process', 'done');
      runBriefWasm(transcript, itemId);

      const brief = await githubGetFile('artifacts/' + base + '.brief.json');
      if (brief) updateStep('brief', 'done');

      for (const artifact of EXTRA_ARTIFACTS) {
        const raw = await githubGetFile('artifacts/' + base + artifact.suffix);
        if (!raw) continue;
        const key = artifact.suffix.replace(/^\\./, '').replace(/\\.(json|txt|md|html)$/,'');
        seen[key] = raw;
        if (artifact.step) updateStep(artifact.step, 'done');
      }

      const idx = items.findIndex(h => h.id === itemId);
      if (idx >= 0) {
        const next = Object.assign({}, items[idx]);
        if (brief) next.brief = BonfyreGitHubIntake.parseBriefPayload(brief);
        Object.keys(seen).forEach(name => applyArtifactSignals(next, name, seen[name]));
        items = BonfyreGitHubJobs.updateAndPersist(ITEMS_KEY, items, itemId, function(item) {
          item.brief = next.brief;
          item.tags = next.tags;
          item.flagged = next.flagged;
        });
      }

      const hasCompletionSignal = COMPLETE_WHEN_ANY.some(name => seen[name]);
      if (brief && (hasCompletionSignal || COMPLETE_AFTER_BRIEF_ONLY)) {
        updateStep('emit', 'done');
        items = BonfyreGitHubJobs.updateAndPersist(ITEMS_KEY, items, itemId, function(item) {
          item.status = 'complete';
          if (brief) item.brief = BonfyreGitHubIntake.parseBriefPayload(brief);
        });
        renderBoard();
        BonfyreGitHubJobs.touchLastUpdate('lastUpdate');
        setTimeout(hidePipeline, 2000);
        ctrl.stop();
        return;
      }

      renderBoard();
    }
  });
}

function showPipeline() {
  document.getElementById('pipeline').style.display = 'block';
  document.getElementById('statusBadge').className = 'badge badge-processing';
  document.getElementById('statusBadge').textContent = 'READY';
}

function hidePipeline() {
  document.getElementById('statusBadge').className = 'badge badge-live';
  document.getElementById('statusBadge').textContent = 'LIVE';
}

function updateStep(step, state) {
  const el = document.querySelector('[data-step="' + step + '"]');
  if (el) el.className = 'pipeline-step ' + state;
}

function showWasmPreview(text) {
  document.getElementById('wasmPreview').style.display = 'block';
  document.getElementById('previewContent').textContent = text;
}

function renderBoard() {
  const container = document.getElementById('boardContent');
  if (!items.length) return;
  const activeFilter = document.querySelector('.filter-item.active')?.dataset.filter || 'all';
  const filtered = activeFilter === 'all' ? items :
                   activeFilter === 'flagged' ? items.filter(h => h.flagged) :
                   items.filter(h => h.tags?.includes(activeFilter));
  let html = '';
  for (const h of filtered) {
    const statusColor = BonfyreGitHubJobs.statusColor(h.status);
    const statusLabel = BonfyreGitHubJobs.statusLabel(h.status);
    ${renderPrelude}
    html += \`<div class="card"><h3>${cardTitleExpr}</h3><div class="card-meta"><span>\${h.time}</span><span style="color:\${statusColor}">\${statusLabel}</span></div><div class="card-body">\${h.brief || "<em style=\\"color:var(--dim)\\">${escapeHtml(meta.emptyBody)}</em>"}</div><div class="card-tags">${tagsExpr}</div></div>\`;
  }
  container.innerHTML = html;
  const countAll = document.getElementById('countAll');
  if (countAll) countAll.textContent = items.length;
}

document.getElementById('filterList').addEventListener('click', e => {
  const item = e.target.closest('.filter-item');
  if (!item) return;
  document.querySelectorAll('.filter-item').forEach(i => i.classList.remove('active'));
  item.classList.add('active');
  renderBoard();
});

document.getElementById('markUploaded').addEventListener('click', markUploaded);

initBriefWorker();
renderBoard();
`;
}

function rewriteIndex(repoName, meta) {
  const file = path.join(PROJECTS, repoName, 'site/index.html');
  let html = fs.readFileSync(file, 'utf8');

  html = html.replace(
    /<div class="dropzone-text">[\s\S]*?<br><small>or click to browse<\/small><\/div>/,
    `<div class="dropzone-text">${meta.dropzoneText}<br><small>or click to browse</small></div>`
  );

  html = html.replace(
    /<div class="pipeline-step" data-step="upload">[\s\S]*?<\/div>/,
    '<div class="pipeline-step" data-step="prepare"><span class="label">Prepare intake file</span><span class="where">Browser</span></div>\n        <div class="pipeline-step" data-step="commit"><span class="label">Upload + commit</span><span class="where">GitHub Web</span></div>'
  );

  if (!html.includes('id="intakePreview"')) {
    html = html.replace(
      /(<div class="preview" id="wasmPreview"[\s\S]*?<\/div>)/,
      `$1\n      <div class="preview" id="intakePreview" style="display:none;">\n        <h4>GitHub Intake</h4>\n        <pre id="intakeContent">Preparing upload…</pre>\n        <div style="margin-top:.75rem;display:flex;gap:.5rem;flex-wrap:wrap;">\n          <a class="btn btn-primary" id="downloadPrepared" href="#" download>Download Intake Copy</a>\n          <a class="btn btn-ghost" id="openUploadFolder" href="#" target="_blank" rel="noreferrer">Open GitHub Folder</a>\n          <button class="btn btn-ghost" id="markUploaded" type="button">I Uploaded It</button>\n        </div>\n      </div>`
    );
  }

  html = html.replace(
    new RegExp(`placeholder="you/${repoName}"`, 'g'),
    `placeholder="Nickgonzales76017/${repoName}"`
  );

  html = html.replace(/\s*<label>Token \(PAT with repo scope\)<\/label>\s*<input type="password" id="cfgToken" placeholder="ghp_…">\s*/g, '\n');

  html = html.replace(
    /<strong style="color:var\(--accent\);">Path 3 — Hybrid Architecture<\/strong><br>[\s\S]*?<\/p>/,
    `<strong style="color:var(--accent);">Path 3 — Hybrid Architecture</strong><br>\n          ${meta.heroText}\n        </p>`
  );

  html = html.replace(/<script>[\s\S]*<\/script>\s*<\/body>\s*<\/html>\s*$/, '');

  const scriptBlock = `  <script src="js/github-intake.js"></script>\n  <script src="js/github-jobs.js"></script>\n  <script>\n${buildScript(repoName, meta)}  </script>\n</body>\n</html>\n`;
  fs.writeFileSync(file, html + scriptBlock);
}

function copyHelpers(repoName) {
  for (const helper of sharedHelpers) {
    const dest = path.join(PROJECTS, repoName, helper.dest);
    fs.copyFileSync(helper.source, dest);
  }
}

for (const [repoName, meta] of Object.entries(APPS)) {
  copyHelpers(repoName);
  rewriteIndex(repoName, meta);
  const readmePath = path.join(PROJECTS, repoName, 'README.md');
  const currentReadme = fs.readFileSync(readmePath, 'utf8');
  fs.writeFileSync(readmePath, buildReadme(repoName, currentReadme));
  process.stdout.write(`updated ${repoName}\n`);
}
