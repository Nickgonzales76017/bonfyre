/* Bonfyre — Frontend Application */

const API = '';  // same origin

// ── Use Case Data ───────────────────────────────────────────
const USE_CASES = [
  // Pipeline
  { n: 1, cat: 'pipeline', title: 'Batch Audio Transcription', desc: 'Bring in dozens of files and turn them into transcripts automatically with local acceleration.', bins: ['ingest', 'transcribe'] },
  { n: 2, cat: 'pipeline', title: 'Speaker-Separated Transcripts', desc: 'Split voices, label speakers, and turn rough text into readable paragraphs.', bins: ['transcribe', 'paragraph', 'clean'] },
  { n: 3, cat: 'pipeline', title: 'Podcast Post-Production', desc: 'Take an episode from raw audio to show notes, proof, and delivery-ready assets.', bins: ['ingest', 'transcribe', 'brief', 'proof'] },
  { n: 4, cat: 'pipeline', title: 'Audio QA Pipeline', desc: 'Score transcript quality, flag weak spots, and keep visible proof of what passed review.', bins: ['media', 'transcribe', 'proof'] },
  { n: 5, cat: 'pipeline', title: 'Full Ingest-to-Delivery', desc: 'Run the complete Bonfyre path from upload through cleanup, summary, packaging, and distribution.', bins: ['ingest', 'media', 'transcribe', 'clean', 'brief', 'pack', 'distribute'] },

  // Content
  { n: 6, cat: 'content', title: 'Executive Summaries', desc: 'Turn long transcripts into clear briefs that decision-makers can scan fast.', bins: ['brief'] },
  { n: 7, cat: 'content', title: 'Content Repurposing', desc: 'Reuse transcripts as posts, social cuts, training material, and client-ready drafts.', bins: ['brief', 'paragraph', 'pack'] },
  { n: 8, cat: 'content', title: 'Legal Transcript Cleanup', desc: 'Clean rough transcript output into a format closer to legal or court-ready review.', bins: ['clean', 'paragraph', 'proof'] },
  { n: 9, cat: 'content', title: 'Multi-Format Delivery', desc: 'Ship the same work as PDF, DOCX, SRT, VTT, or JSON without rebuilding it each time.', bins: ['pack'] },
  { n: 10, cat: 'content', title: 'Research Corpus Builder', desc: 'Turn transcripts into a searchable archive people can revisit instead of losing in folders.', bins: ['ingest', 'transcribe', 'embed'] },

  // Business
  { n: 11, cat: 'business', title: 'Per-Job Cost Tracking', desc: 'See what each job costs to run so pricing stays grounded in real numbers.', bins: ['meter', 'ledger'] },
  { n: 12, cat: 'business', title: 'Automated Invoicing', desc: 'Turn metered usage into invoices, credits, and payment tracking with less manual work.', bins: ['meter', 'pay'] },
  { n: 13, cat: 'business', title: 'Margin Dashboard', desc: 'Watch margin at the component and package level before you underprice the work.', bins: ['finance', 'meter'] },
  { n: 14, cat: 'business', title: 'Service Bundle Pricing', desc: 'Assemble service packages and see pricing and margin update right away.', bins: ['finance', 'offer'] },
  { n: 15, cat: 'business', title: 'Usage-Based Billing', desc: 'Bill by minutes, pages, or processed calls without losing the underlying proof trail.', bins: ['meter', 'gate', 'pay'] },

  // Sales
  { n: 16, cat: 'sales', title: 'Proposal Generation', desc: 'Create proposals with scope, pricing, and deliverables without starting from a blank doc.', bins: ['offer', 'brief'] },
  { n: 17, cat: 'sales', title: 'Outreach Campaigns', desc: 'Send offers across channels and keep follow-up organized instead of scattered.', bins: ['outreach', 'offer'] },
  { n: 18, cat: 'sales', title: 'Follow-Up Automation', desc: 'Track who needs another touch and keep the pipeline moving after the first send.', bins: ['outreach'] },
  { n: 19, cat: 'sales', title: 'Service Marketplace', desc: 'Package services behind a self-serve surface with access control already built in.', bins: ['offer', 'gate', 'auth'] },
  { n: 20, cat: 'sales', title: 'Affiliate Distribution', desc: 'Push the same packaged service through partner channels with attribution intact.', bins: ['distribute', 'outreach'] },

  // Infra
  { n: 21, cat: 'infra', title: 'API Key Management', desc: 'Issue and control keys with enough guardrails to run Bonfyre as a real service.', bins: ['gate', 'auth'] },
  { n: 22, cat: 'infra', title: 'Artifact Integrity', desc: 'Keep visible proof that files were not changed silently as they moved through the stack.', bins: ['graph', 'media'] },
  { n: 23, cat: 'infra', title: 'Job Queue Management', desc: 'Submit, retry, and prioritize work without losing the operational view.', bins: ['queue', 'pipeline'] },
  { n: 24, cat: 'infra', title: 'Binary Health Monitoring', desc: 'See whether the Bonfyre stack is healthy before failures become product problems.', bins: ['api', 'pipeline'] },
  { n: 25, cat: 'infra', title: 'Data Sync & Backup', desc: 'Keep artifacts and state in sync across nodes instead of trapped on one machine.', bins: ['sync', 'graph'] },

  // Platform
  { n: 26, cat: 'platform', title: 'Self-Hosted Transcription', desc: 'Run the platform on your own hardware and keep the data path local.', bins: ['api', 'auth', 'transcribe'] },
  { n: 27, cat: 'platform', title: 'White-Label Service', desc: 'Wrap the full Bonfyre flow in your own brand and sell it as a product.', bins: ['api', 'auth', 'gate', 'pay'] },
  { n: 28, cat: 'platform', title: 'Multi-Tenant SaaS', desc: 'Serve multiple customers with separate access, billing, and data boundaries.', bins: ['auth', 'gate', 'pay', 'cms'] },
  { n: 29, cat: 'platform', title: 'On-Premise Enterprise', desc: 'Install the full binary family on-site when control matters more than cloud convenience.', bins: ['api', 'auth', 'pipeline'] },
  { n: 30, cat: 'platform', title: 'Edge Deployment', desc: 'Run lightweight Bonfyre pieces closer to the source when latency matters.', bins: ['ingest', 'transcribe', 'sync'] }
];

const CAT_LABELS = {
  all: 'All', pipeline: 'Audio → Transcript', content: 'Content & Deliverables',
  business: 'Business Operations', sales: 'Sales & Distribution',
  infra: 'Infrastructure', platform: 'Self-Hosted Platform'
};

const PAGES_LIBRARY = [
  {
    id: 'podcast-plant',
    surface: 'bonfyre',
    name: 'Podcast Plant',
    status: 'live',
    route: '/pages/podcast-plant',
    repo: 'pages-podcast-plant',
    audience: 'Media teams',
    summary: 'Raw episode audio becomes a published podcast microsite with transcripts, briefs, and RSS output.',
    stack: ['bonfyre-media-prep', 'bonfyre-transcribe', 'bonfyre-brief', 'bonfyre-emit'],
    outputs: ['Episode pages', 'RSS feed', 'Share snippets'],
    checklist: ['Transcript locked', 'RSS valid', 'Cover art attached'],
    freshness: 'Synced 12m ago',
    metric: '18 published episodes'
  },
  {
    id: 'customer-voice-board',
    surface: 'bonfyre',
    name: 'Customer Voice Board',
    status: 'draft',
    route: '/pages/customer-voice',
    repo: 'pages-customer-voice',
    audience: 'Product + CX',
    summary: 'Interviews are distilled into theme cards, searchable clips, and reusable proof blocks for landing pages.',
    stack: ['bonfyre-transcribe', 'bonfyre-tone', 'bonfyre-tag', 'bonfyre-embed'],
    outputs: ['Insight board', 'Theme archive', 'Proof snippets'],
    checklist: ['Themes reviewed', 'Embeds indexed', 'CTA blocks approved'],
    freshness: 'Draft updated 2h ago',
    metric: '64 insight cards'
  },
  {
    id: 'shift-handoff',
    surface: 'bonfyre',
    name: 'Shift Handoff Board',
    status: 'live',
    route: '/pages/shift-handoff',
    repo: 'pages-shift-handoff',
    audience: 'Operations',
    summary: 'Field recordings become traceable handoff cards with summaries, action items, and proof bundles.',
    stack: ['bonfyre-transcribe', 'bonfyre-brief', 'bonfyre-proof', 'bonfyre-pack'],
    outputs: ['Shift cards', 'Action queue', 'Proof bundle'],
    checklist: ['Ops template set', 'Summary QA passed', 'Proof archive attached'],
    freshness: 'Synced 28m ago',
    metric: '31 active handoffs'
  },
  {
    id: 'oss-cockpit',
    surface: 'oss',
    name: 'OSS Maintainer Cockpit',
    status: 'live',
    route: '/pages/oss-cockpit',
    repo: 'pages-oss-cockpit',
    audience: 'Maintainers',
    summary: 'Issues, PRs, and release notes are transformed into a searchable maintainer dashboard with operational context.',
    stack: ['bonfyre-ingest', 'bonfyre-tag', 'bonfyre-embed', 'bonfyre-index'],
    outputs: ['PR digest', 'Issue backlog view', 'Repo memory'],
    checklist: ['GitHub sync healthy', 'Index built', 'Release queue clean'],
    freshness: 'Synced 7m ago',
    metric: '412 indexed changes'
  },
  {
    id: 'release-radio',
    surface: 'oss',
    name: 'Release-Note Radio',
    status: 'prototype',
    route: '/pages/release-radio',
    repo: 'pages-release-radio',
    audience: 'Developer relations',
    summary: 'Changelogs are narrated into an audio-first release site with feed-ready episodes and transcript pages.',
    stack: ['bonfyre-narrate', 'bonfyre-render', 'bonfyre-emit'],
    outputs: ['Release episodes', 'Transcript pages', 'RSS archive'],
    checklist: ['Audio render passed', 'Episode summary approved', 'Feed metadata complete'],
    freshness: 'Prototype run yesterday',
    metric: '9 release episodes'
  },
  {
    id: 'explain-repo',
    surface: 'oss',
    name: 'Explain This Repo',
    status: 'draft',
    route: '/pages/explain-repo',
    repo: 'pages-explain-repo',
    audience: 'New contributors',
    summary: 'A source tree is ingested and rewritten into an onboarding guide with architecture notes and code tours.',
    stack: ['bonfyre-ingest', 'bonfyre-canon', 'bonfyre-brief', 'bonfyre-render'],
    outputs: ['Repo guide', 'Architecture map', 'Starter checklist'],
    checklist: ['Canon pass complete', 'Guide reviewed', 'Navigation linked'],
    freshness: 'Draft updated 48m ago',
    metric: '23 architecture notes'
  }
];

const BONFYRE_COVERAGE = [
  {
    key: 'pipeline',
    label: 'Delivery Flow',
    count: 10,
    proof: '10 steps from raw input to finished deliverable',
    note: 'ingest, cleanup, summary, proof, pricing, and packaging'
  },
  {
    key: 'platform',
    label: 'Product Base',
    count: 5,
    proof: '5 core surfaces that make Bonfyre usable',
    note: 'cms, api, auth, gate, and runtime'
  },
  {
    key: 'orchestration',
    label: 'Job Control',
    count: 4,
    proof: '4 systems for moving work through the stack',
    note: 'pipeline, queue, sync, and stitch'
  },
  {
    key: 'commerce',
    label: 'Revenue',
    count: 6,
    proof: '6 tools tied to pricing, billing, and selling',
    note: 'offers, usage, billing, finance, outreach, and pay'
  },
  {
    key: 'knowledge',
    label: 'Search Layer',
    count: 6,
    proof: '6 tools for retrieval, memory, and publishing',
    note: 'embed, index, graph, render, emit, and narrate'
  },
  {
    key: 'proof',
    label: 'Proof',
    count: 30,
    proof: '30 use cases shown in this frontend',
    note: 'visible examples people can inspect instead of just claims'
  }
];

// ── State ───────────────────────────────────────────────────
let currentPage = 'dashboard';
let token = localStorage.getItem('bfy_token') || '';
let userEmail = localStorage.getItem('bfy_email') || '';
let currentPagesSurface = 'bonfyre';
let currentPagesStatus = 'all';
let currentPagesQuery = '';
let currentPagesId = 'podcast-plant';

// ── Init ────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  setupNav();
  renderUseCases('all');
  initPagesStudio();
  setupUpload();
  setupBundleCalc();
  showPage('dashboard');
  refreshDashboard();
  updateUserUI();
});

// ── Navigation ──────────────────────────────────────────────
function setupNav() {
  document.querySelectorAll('.nav-links a').forEach(link => {
    link.addEventListener('click', e => {
      e.preventDefault();
      showPage(link.dataset.page);
    });
  });
}

function showPage(name) {
  currentPage = name;
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-links a').forEach(a => a.classList.remove('active'));

  const page = document.getElementById('page-' + name);
  const nav = document.querySelector(`[data-page="${name}"]`);
  if (page) page.classList.add('active');
  if (nav) nav.classList.add('active');

  if (name === 'dashboard') refreshDashboard();
  if (name === 'pipeline') refreshJobs();
  if (name === 'outreach') refreshOutreach();
  if (name === 'finance') refreshFinance();
  if (name === 'pages') renderPagesStudio();
}

// ── API helpers ─────────────────────────────────────────────
async function api(path, opts = {}) {
  const headers = { 'Accept': 'application/json', ...(opts.headers || {}) };
  if (token) headers['Authorization'] = 'Bearer ' + token;
  if (opts.json) {
    headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(opts.json);
  }
  try {
    const res = await fetch(API + path, { ...opts, headers });
    if (res.headers.get('content-type')?.includes('json')) return await res.json();
    return { ok: res.ok, status: res.status };
  } catch (e) {
    return { error: e.message };
  }
}

// ── Dashboard ───────────────────────────────────────────────
async function refreshDashboard() {
  const data = await api('/api/status');
  if (data.error) return;
  setText('stat-jobs', data.total_jobs || 0);
  setText('stat-completed', data.completed_jobs || 0);
  setText('stat-uploads', data.total_uploads || 0);
  setText('stat-binaries', data.available_binaries || 0);

  const jobs = await api('/api/jobs');
  const tbody = document.getElementById('recent-jobs');
  if (!tbody) return;
  if (!jobs || !jobs.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No jobs yet — upload a file to start</td></tr>';
    return;
  }
  tbody.innerHTML = jobs.slice(0, 10).map(j => `
    <tr>
      <td>${esc(j.id)}</td>
      <td>${esc(j.binary || j.command || '—')}</td>
      <td>${esc(j.status || 'unknown')}</td>
      <td>${timeAgo(j.created_at)}</td>
      <td><button class="btn btn-sm" onclick="viewJob(${j.id})">View</button></td>
    </tr>
  `).join('');
}

async function viewJob(id) {
  const j = await api('/api/jobs/' + id);
  if (j.error) return alert('Job not found');
  alert(`Job #${j.id}\nBinary: ${j.binary || j.command}\nStatus: ${j.status}\nOutput:\n${j.output || '(none)'}`);
}

// ── Pipeline / Upload ───────────────────────────────────────
function setupUpload() {
  const zone = document.getElementById('upload-zone');
  const input = document.getElementById('file-input');
  if (!zone || !input) return;

  zone.addEventListener('click', () => input.click());
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.style.borderColor = 'var(--accent)'; });
  zone.addEventListener('dragleave', () => { zone.style.borderColor = ''; });
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.style.borderColor = '';
    if (e.dataTransfer.files.length) uploadFile(e.dataTransfer.files[0]);
  });
  input.addEventListener('change', () => { if (input.files.length) uploadFile(input.files[0]); });
}

async function uploadFile(file) {
  const status = document.getElementById('upload-status');
  status.textContent = `Uploading ${file.name}...`;
  status.className = 'upload-status';
  status.classList.remove('hidden');

  const form = new FormData();
  form.append('file', file);

  try {
    const res = await fetch(API + '/api/upload', { method: 'POST', body: form });
    const data = await res.json();
    if (data.error) {
      status.textContent = 'Error: ' + data.error;
      status.style.background = 'var(--negative)';
      return;
    }
    status.textContent = `Uploaded: ${data.filename || file.name} — Ready for pipeline`;
    status.style.background = '#3fb95033';

    // Auto-submit job
    await api('/api/jobs', { method: 'POST', json: { binary: 'bonfyre-ingest', args: ['--file', data.path || data.filename] } });
    refreshJobs();
  } catch (e) {
    status.textContent = 'Upload failed: ' + e.message;
    status.style.background = 'var(--negative)';
  }
}

async function refreshJobs() {
  const jobs = await api('/api/jobs');
  const tbody = document.getElementById('all-jobs');
  if (!tbody) return;
  if (!jobs || !jobs.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-state">No pipeline jobs yet</td></tr>';
    return;
  }
  tbody.innerHTML = jobs.map(j => `
    <tr>
      <td>#${j.id}</td>
      <td>${esc(j.binary || j.command || '—')}</td>
      <td>${esc(j.input_file || '—')}</td>
      <td>${esc(j.status || 'unknown')}</td>
      <td>${timeAgo(j.created_at)}</td>
    </tr>
  `).join('');
}

// ── Outreach ────────────────────────────────────────────────
function logSend(e) {
  e.preventDefault();
  const channel = document.getElementById('send-channel').value;
  const target = document.getElementById('send-target').value;
  const offer = document.getElementById('send-offer').value;
  if (!target) return;

  api('/api/binaries/bonfyre-outreach/send', {
    method: 'POST',
    json: { channel, target, offer }
  }).then(() => {
    document.getElementById('send-target').value = '';
    document.getElementById('send-offer').value = '';
    refreshOutreach();
  });
}

async function refreshOutreach() {
  const data = await api('/api/binaries/bonfyre-outreach/status');
  if (data && !data.error) {
    setText('out-total', data.total || 0);
    setText('out-pending', data.pending || 0);
    setText('out-positive', data.positive || 0);
    setText('out-rate', (data.rate || 0) + '%');
  }
}

// ── Finance ─────────────────────────────────────────────────
function setupBundleCalc() {
  document.querySelectorAll('.bundle-builder input[type="checkbox"]').forEach(cb => {
    cb.addEventListener('change', recalcBundle);
  });
}

function recalcBundle() {
  let cost = 0, price = 0;
  document.querySelectorAll('.bundle-builder input:checked').forEach(cb => {
    cost += parseFloat(cb.dataset.cost || 0);
    price += parseFloat(cb.dataset.price || 0);
  });
  const margin = price > 0 ? ((price - cost) / price * 100).toFixed(0) : 0;
  setText('bundle-cost', '$' + cost.toFixed(2));
  setText('bundle-price', '$' + price.toFixed(2));
  setText('bundle-margin', margin + '%');
}

async function refreshFinance() {
  const data = await api('/api/binaries/bonfyre-finance/report');
  if (data && !data.error) {
    setText('fin-revenue', '$' + (data.revenue || 0));
    setText('fin-costs', '$' + (data.costs || 0));
    setText('fin-margin', (data.margin || 0) + '%');
    setText('fin-bundles', data.bundles || 0);
  }
}

// ── Use Cases ───────────────────────────────────────────────
function renderUseCases(filter) {
  // Update filter buttons
  document.querySelectorAll('.filter-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.filter === filter);
    btn.onclick = () => renderUseCases(btn.dataset.filter);
  });

  const grid = document.getElementById('use-case-grid');
  if (!grid) return;

  const items = filter === 'all' ? USE_CASES : USE_CASES.filter(u => u.cat === filter);
  grid.innerHTML = items.map(u => `
    <div class="use-case-card" data-cat="${u.cat}">
      <span class="uc-number">#${u.n}</span>
      <h3>${esc(u.title)}</h3>
      <p>${esc(u.desc)}</p>
      <div class="uc-binaries">
        ${u.bins.map(b => `<span class="uc-binary">bonfyre-${b}</span>`).join('')}
      </div>
    </div>
  `).join('');
}

// ── Pages Studio ─────────────────────────────────────────────
function initPagesStudio() {
  const search = document.getElementById('pages-search');
  if (search) {
    search.addEventListener('input', () => {
      currentPagesQuery = search.value.trim().toLowerCase();
      renderPagesStudio();
    });
  }
  renderPagesStudio();
}

function renderPagesStudio() {
  const surfaceRoot = document.getElementById('pages-surface-switch');
  const statusRoot = document.getElementById('pages-status-switch');
  const coverageRoot = document.getElementById('pages-coverage');
  const listRoot = document.getElementById('pages-list');
  const detailRoot = document.getElementById('pages-detail');
  const metaRoot = document.getElementById('pages-meta');
  if (!surfaceRoot || !statusRoot || !coverageRoot || !listRoot || !detailRoot || !metaRoot) return;

  const filtered = getFilteredPages();
  if (!filtered.some(page => page.id === currentPagesId)) {
    currentPagesId = filtered[0]?.id || '';
  }
  const active = filtered.find(page => page.id === currentPagesId) || filtered[0] || null;

  surfaceRoot.innerHTML = [
    { key: 'bonfyre', label: 'Bonfyre' },
    { key: 'oss', label: 'Bonfyre OSS' }
  ].map(item => `
    <button class="switch-pill ${item.key === currentPagesSurface ? 'active' : ''}" data-surface="${item.key}">
      ${item.label}
    </button>
  `).join('');

  statusRoot.innerHTML = [
    { key: 'all', label: 'All stages' },
    { key: 'live', label: 'Live' },
    { key: 'draft', label: 'Draft' },
    { key: 'prototype', label: 'Prototype' }
  ].map(item => `
    <button class="switch-pill ${item.key === currentPagesStatus ? 'active' : ''}" data-status="${item.key}">
      ${item.label}
    </button>
  `).join('');

  surfaceRoot.querySelectorAll('[data-surface]').forEach(btn => {
    btn.onclick = () => {
      currentPagesSurface = btn.dataset.surface;
      renderPagesStudio();
    };
  });

  statusRoot.querySelectorAll('[data-status]').forEach(btn => {
    btn.onclick = () => {
      currentPagesStatus = btn.dataset.status;
      renderPagesStudio();
    };
  });

  listRoot.innerHTML = filtered.length ? filtered.map(page => `
    <button class="pages-item ${page.id === currentPagesId ? 'active' : ''}" data-page-id="${page.id}">
      <span class="pages-item-top">
        <span class="pages-item-name">${esc(page.name)}</span>
        <span class="status-badge ${page.status}">${esc(page.status)}</span>
      </span>
      <span class="pages-item-summary">${esc(page.summary)}</span>
      <span class="pages-item-meta">${esc(page.repo)} · ${esc(page.metric)}</span>
    </button>
  `).join('') : '<div class="empty-state">No page apps match this surface yet.</div>';

  listRoot.querySelectorAll('[data-page-id]').forEach(btn => {
    btn.onclick = () => {
      currentPagesId = btn.dataset.pageId;
      renderPagesStudio();
    };
  });

  setText('pages-results-count', `${filtered.length} result${filtered.length === 1 ? '' : 's'}`);
  setText('pages-stat-total', String(PAGES_LIBRARY.length));
  setText('pages-stat-live', String(PAGES_LIBRARY.filter(page => page.status === 'live').length));
  setText('pages-stat-surface', currentPagesSurface === 'bonfyre' ? 'Bonfyre' : 'Bonfyre OSS');

  coverageRoot.innerHTML = BONFYRE_COVERAGE.map(area => `
    <div class="card coverage-card coverage-${area.key}">
      <div class="coverage-top">
        <span class="coverage-number">${esc(area.count)}</span>
        <span class="coverage-label">${esc(area.label)}</span>
      </div>
      <div class="coverage-proof">${esc(area.proof)}</div>
      <div class="coverage-note">${esc(area.note)}</div>
    </div>
  `).join('');

  if (!active) {
    detailRoot.innerHTML = '<div class="empty-state">Pick a page app to inspect its publishing flow.</div>';
    metaRoot.innerHTML = '<div class="empty-state">Publishing details will appear here.</div>';
    return;
  }

  detailRoot.innerHTML = `
    <div class="pages-detail-head">
      <div>
        <div class="eyebrow">${active.surface === 'bonfyre' ? 'Bonfyre surface' : 'Bonfyre OSS surface'}</div>
        <h2>${esc(active.name)}</h2>
        <p>${esc(active.summary)}</p>
      </div>
      <div class="pages-detail-actions">
        <button class="btn btn-secondary" type="button">Preview</button>
        <button class="btn" type="button">Publish update</button>
      </div>
    </div>
    <div class="pages-detail-grid">
      <div class="pages-preview-card">
        <div class="preview-window">
          <div class="preview-bar">
            <span></span><span></span><span></span>
          </div>
          <div class="preview-body">
            <div class="preview-route">${esc(active.route)}</div>
            <h3>${esc(active.name)}</h3>
            <p>Built for ${esc(active.audience)} with clear outputs, visible proof, and a route that is ready to ship.</p>
            <div class="preview-chip-row">
              ${active.outputs.map(item => `<span class="preview-chip">${esc(item)}</span>`).join('')}
            </div>
          </div>
        </div>
      </div>
      <div class="pages-blueprint">
        <div class="blueprint-block">
          <span class="blueprint-label">Repository</span>
          <strong>${esc(active.repo)}</strong>
        </div>
        <div class="blueprint-block">
          <span class="blueprint-label">Audience</span>
          <strong>${esc(active.audience)}</strong>
        </div>
        <div class="blueprint-block wide">
          <span class="blueprint-label">Pipeline</span>
          <div class="pipeline-tag-list">
            ${active.stack.map(item => `<span class="pipeline-tag">${esc(item)}</span>`).join('')}
          </div>
        </div>
        <div class="blueprint-block wide">
          <span class="blueprint-label">Outputs</span>
          <ul class="pages-checklist">
            ${active.outputs.map(item => `<li>${esc(item)}</li>`).join('')}
          </ul>
        </div>
      </div>
    </div>
  `;

  metaRoot.innerHTML = `
    <div class="meta-card">
      <span class="meta-label">Freshness</span>
      <strong>${esc(active.freshness)}</strong>
      <p>${esc(active.metric)}</p>
    </div>
    <div class="meta-card">
      <span class="meta-label">Publish checklist</span>
      <ul class="pages-checklist">
        ${active.checklist.map(item => `<li>${esc(item)}</li>`).join('')}
      </ul>
    </div>
    <div class="meta-card">
      <span class="meta-label">Why this matters</span>
      <p>${active.surface === 'bonfyre'
        ? 'Bonfyre pages turn processed work into simple, client-facing surfaces without hiding where the output came from.'
        : 'Bonfyre OSS pages turn repo activity and model output into proof apps people can browse, trust, and share.'}</p>
    </div>
  `;
}

function getFilteredPages() {
  return PAGES_LIBRARY.filter(page => {
    if (page.surface !== currentPagesSurface) return false;
    if (currentPagesStatus !== 'all' && page.status !== currentPagesStatus) return false;
    if (!currentPagesQuery) return true;
    const haystack = [
      page.name,
      page.summary,
      page.repo,
      page.audience,
      page.outputs.join(' ')
    ].join(' ').toLowerCase();
    return haystack.includes(currentPagesQuery);
  });
}

// ── Settings / Auth ─────────────────────────────────────────
async function handleLogin() {
  const email = document.getElementById('login-email').value;
  const pw = document.getElementById('login-password').value;
  if (!email || !pw) return;

  const data = await api('/api/binaries/bonfyre-auth/login', {
    method: 'POST', json: { email, password: pw }
  });

  if (data.token) {
    token = data.token;
    userEmail = email;
    localStorage.setItem('bfy_token', token);
    localStorage.setItem('bfy_email', email);
    updateUserUI();
  } else {
    alert('Login failed: ' + (data.error || 'unknown error'));
  }
}

async function handleSignup() {
  const email = document.getElementById('login-email').value;
  const pw = document.getElementById('login-password').value;
  if (!email || !pw) return;

  const data = await api('/api/binaries/bonfyre-auth/signup', {
    method: 'POST', json: { email, password: pw }
  });

  if (data.gate_key) {
    alert('Account created! Your API key: ' + data.gate_key);
    await handleLogin();
  } else {
    alert('Signup failed: ' + (data.error || 'unknown error'));
  }
}

function handleLogout() {
  token = '';
  userEmail = '';
  localStorage.removeItem('bfy_token');
  localStorage.removeItem('bfy_email');
  updateUserUI();
}

function updateUserUI() {
  const info = document.getElementById('user-info');
  const form = document.getElementById('login-form');
  const navUser = document.querySelector('.nav-user');

  if (token && userEmail) {
    if (info) info.classList.remove('hidden');
    if (form) form.classList.add('hidden');
    if (navUser) navUser.textContent = userEmail;
    setText('settings-email', userEmail);
    setText('settings-key', token.substring(0, 20) + '...');
  } else {
    if (info) info.classList.add('hidden');
    if (form) form.classList.remove('hidden');
    if (navUser) navUser.textContent = '';
  }
}

async function checkSystem() {
  const health = await api('/api/health');
  setText('sys-status', health.status || 'unknown');
  setText('sys-version', health.version || '—');

  const status = await api('/api/status');
  const list = document.getElementById('sys-binaries');
  if (list && status.binaries) {
    list.innerHTML = status.binaries.map(b => `<li>✓ ${esc(b)}</li>`).join('');
  }
}

// ── Utilities ───────────────────────────────────────────────
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function esc(s) {
  if (!s) return '';
  const d = document.createElement('div');
  d.textContent = String(s);
  return d.innerHTML;
}

function timeAgo(ts) {
  if (!ts) return '—';
  const sec = Math.floor((Date.now() / 1000) - ts);
  if (sec < 60) return 'just now';
  if (sec < 3600) return Math.floor(sec / 60) + 'm ago';
  if (sec < 86400) return Math.floor(sec / 3600) + 'h ago';
  return Math.floor(sec / 86400) + 'd ago';
}
