/* Bonfyre — Frontend Application */

const API = '';  // same origin

// ── Use Case Data ───────────────────────────────────────────
const USE_CASES = [
  // Pipeline
  { n: 1, cat: 'pipeline', title: 'Batch Audio Transcription', desc: 'Upload dozens of audio files → automatic Whisper transcription with GPU acceleration.', bins: ['ingest', 'transcribe'] },
  { n: 2, cat: 'pipeline', title: 'Speaker-Separated Transcripts', desc: 'Transcribe and tag by speaker using diarization, then format into clean paragraphs.', bins: ['transcribe', 'paragraph', 'clean'] },
  { n: 3, cat: 'pipeline', title: 'Podcast Post-Production', desc: 'Ingest episode audio, transcribe, generate show notes, proof, and deliver.', bins: ['ingest', 'transcribe', 'brief', 'proof'] },
  { n: 4, cat: 'pipeline', title: 'Audio QA Pipeline', desc: 'Hash source files, transcribe, run quality scoring, flag low-confidence segments.', bins: ['media', 'transcribe', 'proof'] },
  { n: 5, cat: 'pipeline', title: 'Full Ingest-to-Delivery', desc: 'End-to-end: upload → ingest → media prep → transcribe → clean → brief → pack → distribute.', bins: ['ingest', 'media', 'transcribe', 'clean', 'brief', 'pack', 'distribute'] },

  // Content
  { n: 6, cat: 'content', title: 'Executive Summaries', desc: 'Generate concise briefs from any transcript for stakeholder review.', bins: ['brief'] },
  { n: 7, cat: 'content', title: 'Content Repurposing', desc: 'Turn transcripts into blog posts, social snippets, or training materials.', bins: ['brief', 'paragraph', 'pack'] },
  { n: 8, cat: 'content', title: 'Legal Transcript Cleanup', desc: 'Clean raw transcripts to deposition or court-ready format with proof pass.', bins: ['clean', 'paragraph', 'proof'] },
  { n: 9, cat: 'content', title: 'Multi-Format Delivery', desc: 'Package transcripts as PDF, DOCX, SRT, VTT, JSON — one command.', bins: ['pack'] },
  { n: 10, cat: 'content', title: 'Research Corpus Builder', desc: 'Build searchable transcript archives with Weaviate vector indexing.', bins: ['ingest', 'transcribe', 'embed'] },

  // Business
  { n: 11, cat: 'business', title: 'Per-Job Cost Tracking', desc: 'Track compute time, storage, API calls per job for precise cost allocation.', bins: ['meter', 'ledger'] },
  { n: 12, cat: 'business', title: 'Automated Invoicing', desc: 'Generate invoices from metered usage, apply credits, track payments.', bins: ['meter', 'pay'] },
  { n: 13, cat: 'business', title: 'Margin Dashboard', desc: 'See per-component and per-bundle margins in real time.', bins: ['finance', 'meter'] },
  { n: 14, cat: 'business', title: 'Service Bundle Pricing', desc: 'Build custom bundles from components, auto-calculate pricing and margins.', bins: ['finance', 'offer'] },
  { n: 15, cat: 'business', title: 'Usage-Based Billing', desc: 'Bill clients based on actual audio minutes, pages, or API calls processed.', bins: ['meter', 'gate', 'pay'] },

  // Sales
  { n: 16, cat: 'sales', title: 'Proposal Generation', desc: 'Auto-generate client proposals with scope, pricing, and deliverables.', bins: ['offer', 'brief'] },
  { n: 17, cat: 'sales', title: 'Outreach Campaigns', desc: 'Send offers via email, SMS, or webhook with automated follow-up scheduling.', bins: ['outreach', 'offer'] },
  { n: 18, cat: 'sales', title: 'Follow-Up Automation', desc: 'Queue and track follow-ups across channels with response tracking.', bins: ['outreach'] },
  { n: 19, cat: 'sales', title: 'Service Marketplace', desc: 'List transcription packages on a self-serve portal with API key provisioning.', bins: ['offer', 'gate', 'auth'] },
  { n: 20, cat: 'sales', title: 'Affiliate Distribution', desc: 'Distribute packaged services through partner channels with tracked attribution.', bins: ['distribute', 'outreach'] },

  // Infra
  { n: 21, cat: 'infra', title: 'API Key Management', desc: 'Issue, rotate, and revoke API keys with per-key rate limiting.', bins: ['gate', 'auth'] },
  { n: 22, cat: 'infra', title: 'Artifact Integrity', desc: 'Merkle-DAG verification chain ensuring no file tampering across pipeline.', bins: ['graph', 'media'] },
  { n: 23, cat: 'infra', title: 'Job Queue Management', desc: 'Submit, monitor, retry, and prioritize pipeline jobs.', bins: ['queue', 'pipeline'] },
  { n: 24, cat: 'infra', title: 'Binary Health Monitoring', desc: 'Check status of all 38 binaries, uptime, and resource usage.', bins: ['api', 'pipeline'] },
  { n: 25, cat: 'infra', title: 'Data Sync & Backup', desc: 'Sync artifacts and databases across nodes with conflict resolution.', bins: ['sync', 'graph'] },

  // Platform
  { n: 26, cat: 'platform', title: 'Self-Hosted Transcription', desc: 'Run the full platform on your own hardware — no cloud dependency.', bins: ['api', 'auth', 'transcribe'] },
  { n: 27, cat: 'platform', title: 'White-Label Service', desc: 'Rebrand and resell the full pipeline under your own name.', bins: ['api', 'auth', 'gate', 'pay'] },
  { n: 28, cat: 'platform', title: 'Multi-Tenant SaaS', desc: 'Serve multiple clients with isolated data, keys, and billing.', bins: ['auth', 'gate', 'pay', 'cms'] },
  { n: 29, cat: 'platform', title: 'On-Premise Enterprise', desc: 'Install the full binary family on-prem with the unified installer.', bins: ['api', 'auth', 'pipeline'] },
  { n: 30, cat: 'platform', title: 'Edge Deployment', desc: 'Run lightweight binaries at the edge for low-latency processing.', bins: ['ingest', 'transcribe', 'sync'] }
];

const CAT_LABELS = {
  all: 'All', pipeline: 'Audio → Transcript', content: 'Content & Deliverables',
  business: 'Business Operations', sales: 'Sales & Distribution',
  infra: 'Infrastructure', platform: 'Self-Hosted Platform'
};

// ── State ───────────────────────────────────────────────────
let currentPage = 'dashboard';
let token = localStorage.getItem('bfy_token') || '';
let userEmail = localStorage.getItem('bfy_email') || '';

// ── Init ────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  setupNav();
  renderUseCases('all');
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
