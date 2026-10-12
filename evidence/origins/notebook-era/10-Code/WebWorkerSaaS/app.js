/* ─── constants ────────────────────────────────── */

const DB_NAME = "bonfyre-intake-console";
const DB_VERSION = 1;
const STORE_NAME = "jobs";
const MAX_FILE_SIZE = 100 * 1024 * 1024;
const WARN_FILE_SIZE = 25 * 1024 * 1024;

/* ─── elements ─────────────────────────────────── */

const dropZone = document.getElementById("drop-zone");
const browseBtn = document.getElementById("browse-btn");
const audioInput = document.getElementById("audio-file");
const installBanner = document.getElementById("install-banner");
const installAppBtn = document.getElementById("install-app");
const dismissInstallBtn = document.getElementById("dismiss-install");
const fileReady = document.getElementById("file-ready");
const fileNameEl = document.getElementById("file-name");
const fileSizeEl = document.getElementById("file-size");
const clearFileBtn = document.getElementById("clear-file");
const contextForm = document.getElementById("context-form");
const jobTitleInput = document.getElementById("job-title");
const pickButtons = document.querySelectorAll(".pick");

const filesSection = document.getElementById("files-section");
const jobList = document.getElementById("job-list");
const jobSearch = document.getElementById("job-search");
const statusFilter = document.getElementById("status-filter");
const operatorMode = document.getElementById("operator-mode");
const selectionSummary = document.getElementById("selection-summary");
const selectVisibleBtn = document.getElementById("select-visible");
const clearSelectionBtn = document.getElementById("clear-selection");
const markSelectedReadyBtn = document.getElementById("mark-selected-ready");
const exportSelectedManifestsBtn = document.getElementById("export-selected-manifests");
const exportSelectedPackagesBtn = document.getElementById("export-selected-packages");
const importSelectedStatusBtn = document.getElementById("import-selected-status-btn");
const importSelectedStatusInput = document.getElementById("import-selected-status");
const importSelectedResultsBtn = document.getElementById("import-selected-results-btn");
const importSelectedResultsInput = document.getElementById("import-selected-results");

const detailDrawer = document.getElementById("detail-drawer");
const closeDrawerBtn = document.getElementById("close-drawer");
const detailClient = document.getElementById("detail-client");
const detailTitle = document.getElementById("detail-title");
const detailFile = document.getElementById("detail-file");
const detailGoal = document.getElementById("detail-goal");
const detailBuyer = document.getElementById("detail-buyer");
const detailContact = document.getElementById("detail-contact");
const detailTurnaround = document.getElementById("detail-turnaround");
const detailCreated = document.getElementById("detail-created");
const detailCompleted = document.getElementById("detail-completed");
const detailContext = document.getElementById("detail-context");
const detailContextBlock = document.getElementById("detail-context-block");
const detailRouting = document.getElementById("detail-routing");
const detailBrief = document.getElementById("detail-brief");
const detailStatus = document.getElementById("detail-status");
const saveStatusBtn = document.getElementById("save-status");

const copyBriefBtn = document.getElementById("copy-brief");
const exportBriefBtn = document.getElementById("export-brief");
const exportManifestBtn = document.getElementById("export-manifest");
const exportPackageBtn = document.getElementById("export-package");
const downloadAudioBtn = document.getElementById("download-audio");
const deleteJobBtn = document.getElementById("delete-job");

const importStatusBtn = document.getElementById("import-status-btn");
const importStatusInput = document.getElementById("import-status");
const importResultsBtn = document.getElementById("import-results-btn");
const importResultsInput = document.getElementById("import-results");
const deliverableEmpty = document.getElementById("deliverable-empty");
const deliverableContent = document.getElementById("deliverable-content");
const deliverableRendered = document.getElementById("deliverable-rendered");
const deliverableText = document.getElementById("deliverable-text");
const copyDeliverableBtn = document.getElementById("copy-deliverable");
const exportDeliverableBtn = document.getElementById("export-deliverable");
const clearDeliverableBtn = document.getElementById("clear-deliverable");

const toastContainer = document.getElementById("toast-container");
const cardTemplate = document.getElementById("job-card-template");

/* ─── state ────────────────────────────────────── */

let db;
let jobs = [];
let selectedJobId = null;
let pendingFile = null;
let activePreset = null;
let selectedJobIds = new Set();
let deferredInstallPrompt = null;

const PRESETS = {
  "founder-memo": {
    buyerType: "founders and operators",
    outputGoal: "voice-memo-to-plan",
    turnaroundTarget: "same-day",
    contextNotes: "Founder voice memo. Pull out key decisions, priorities, and immediate next steps.",
  },
  "customer-call": {
    buyerType: "customer-research teams",
    outputGoal: "meeting-recap",
    turnaroundTarget: "24h",
    contextNotes: "Customer conversation. Extract pain points, objections, signals, and follow-up actions.",
  },
  "consultant-recap": {
    buyerType: "consultants and client-service operators",
    outputGoal: "transcript-summary-actions",
    turnaroundTarget: "24h",
    contextNotes: "Client-facing recap. Keep wording crisp and decision-ready for handoff.",
  },
};

const ROUTING_RULES = {
  "voice-memo-to-plan": {
    serviceLane: "Local AI Transcription Service",
    outputShape: "decision-ready memo with action items",
    nextStep: "export one-file package and process through LocalAITranscriptionService",
  },
  "meeting-recap": {
    serviceLane: "Local AI Transcription Service",
    outputShape: "customer recap with pain points, insights, and follow-ups",
    nextStep: "export package, then promote strong outputs into PersonalMarketLayer proof assets",
  },
  "transcript-summary-actions": {
    serviceLane: "Local AI Transcription Service",
    outputShape: "transcript, executive summary, and action list",
    nextStep: "export package for processing, then route reviewed proof into monetization and distribution",
  },
  custom: {
    serviceLane: "Operator Review",
    outputShape: "custom deliverable",
    nextStep: "review job context before assigning downstream system",
  },
};

/* ─── IndexedDB ────────────────────────────────── */

function openDatabase() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = (e) => {
      const d = e.target.result;
      if (!d.objectStoreNames.contains(STORE_NAME)) {
        const s = d.createObjectStore(STORE_NAME, { keyPath: "id" });
        s.createIndex("createdAt", "createdAt");
        s.createIndex("status", "status");
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function tx(mode = "readonly") {
  return db.transaction(STORE_NAME, mode).objectStore(STORE_NAME);
}

function saveJob(job) {
  return new Promise((resolve, reject) => {
    const r = tx("readwrite").put(job);
    r.onsuccess = () => resolve(job);
    r.onerror = () => reject(r.error);
  });
}

function deleteJobFromDB(id) {
  return new Promise((resolve, reject) => {
    const r = tx("readwrite").delete(id);
    r.onsuccess = () => resolve();
    r.onerror = () => reject(r.error);
  });
}

function loadJobs() {
  return new Promise((resolve, reject) => {
    const r = tx().getAll();
    r.onsuccess = () => resolve(r.result.sort((a, b) => b.createdAt.localeCompare(a.createdAt)));
    r.onerror = () => reject(r.error);
  });
}

/* ─── helpers ──────────────────────────────────── */

function slugify(v) {
  return v.toLowerCase().trim().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "file";
}

function fmtDate(v) { return new Date(v).toLocaleString(); }

function fmtBytes(b) {
  if (!b) return "0 B";
  const u = ["B", "KB", "MB", "GB"];
  let s = b, i = 0;
  while (s >= 1024 && i < u.length - 1) { s /= 1024; i++; }
  return `${s.toFixed(s >= 10 || i === 0 ? 0 : 1)} ${u[i]}`;
}

function fmtTurnaround(v) {
  if (v === "same-day") return "Same Day";
  if (v === "24h") return "24 Hours";
  if (v === "48h") return "48 Hours";
  return "Flexible";
}

function fmtBuyerType(v) {
  return v || "\u2014";
}

function statusLabel(s) {
  if (s === "ready") return "Ready";
  if (s === "processing") return "Processing";
  if (s === "done") return "Done";
  return "New";
}

function toast(msg, type = "info", ms = 3000) {
  const el = document.createElement("div");
  el.className = `toast toast-${type}`;
  el.textContent = msg;
  toastContainer.appendChild(el);
  setTimeout(() => {
    el.classList.add("toast-out");
    el.addEventListener("animationend", () => el.remove());
  }, ms);
}

function setInstallBannerVisible(visible) {
  if (!installBanner) return;
  installBanner.classList.toggle("hidden", !visible);
}

function updateSelectionSummary() {
  selectionSummary.textContent = `${selectedJobIds.size} selected`;
}

function confirmAction(msg) {
  return new Promise((resolve) => {
    const dlg = document.getElementById("confirm-dialog");
    document.getElementById("confirm-message").textContent = msg;
    const y = document.getElementById("confirm-yes");
    const n = document.getElementById("confirm-no");
    function done(r) { y.removeEventListener("click", onY); n.removeEventListener("click", onN); dlg.removeEventListener("close", onC); dlg.close(); resolve(r); }
    function onY() { done(true); }
    function onN() { done(false); }
    function onC() { done(false); }
    y.addEventListener("click", onY);
    n.addEventListener("click", onN);
    dlg.addEventListener("close", onC);
    dlg.showModal();
  });
}

function downloadBlob(blob, name) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

function blobToBase64(blob) {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => {
      const res = r.result;
      if (typeof res !== "string") { reject(new Error("Read failed")); return; }
      resolve(res.split(",", 2)[1]);
    };
    r.onerror = () => reject(r.error);
    r.readAsDataURL(blob);
  });
}

function headingLabel(line) {
  return line.replace(/^#{1,6}\s+/, "").trim();
}

function bulletMatch(line) {
  return line.match(/^(\s*)-\s+(.*)$/);
}

function parseDeliverable(markdown) {
  const lines = (markdown || "").replace(/\r\n/g, "\n").split("\n");
  const sections = [];
  let current = { title: "Deliverable", blocks: [] };
  let paragraphLines = [];

  function flushParagraph() {
    const text = paragraphLines.join(" ").replace(/\s+/g, " ").trim();
    if (text) current.blocks.push({ type: "paragraph", text });
    paragraphLines = [];
  }

  function flushSection() {
    flushParagraph();
    if (current.blocks.length || current.title) sections.push(current);
  }

  function ensureListBlock() {
    const last = current.blocks[current.blocks.length - 1];
    if (last && last.type === "list") return last;
    const block = { type: "list", items: [] };
    current.blocks.push(block);
    return block;
  }

  function addListItem(items, depth, text) {
    if (depth <= 0) {
      items.push({ text, children: [] });
      return;
    }
    const last = items[items.length - 1];
    if (!last) {
      items.push({ text, children: [] });
      return;
    }
    addListItem(last.children, depth - 1, text);
  }

  for (const line of lines) {
    if (!line.trim()) {
      flushParagraph();
      continue;
    }

    if (/^#{1,6}\s+/.test(line)) {
      flushSection();
      current = { title: headingLabel(line), blocks: [] };
      continue;
    }

    const bullet = bulletMatch(line);
    if (bullet) {
      flushParagraph();
      const indent = Math.floor(bullet[1].length / 2);
      const text = bullet[2].trim();
      if (text) {
        const list = ensureListBlock();
        addListItem(list.items, indent, text);
      }
      continue;
    }

    paragraphLines.push(line.trim());
  }

  flushSection();
  return sections.filter((section) => section.blocks.length);
}

function buildList(items) {
  const ul = document.createElement("ul");
  ul.className = "results-list";
  for (const item of items) {
    const li = document.createElement("li");
    li.textContent = item.text;
    if (item.children.length) {
      li.appendChild(buildList(item.children));
    }
    ul.appendChild(li);
  }
  return ul;
}

function findJobForStatusPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (payload.jobId) {
    const match = jobs.find((job) => job.id === String(payload.jobId));
    if (match) return match;
  }
  if (payload.jobSlug) {
    return jobBySlug(String(payload.jobSlug));
  }
  return null;
}

function mergeStatusPayload(job, payload) {
  let changed = false;

  if (payload.status && job.status !== String(payload.status)) {
    job.status = String(payload.status);
    changed = true;
  }

  if (payload.completedAt && job.completedAt !== String(payload.completedAt)) {
    job.completedAt = String(payload.completedAt);
    changed = true;
  }

  if (payload.deliverableMarkdown && job.deliverable !== String(payload.deliverableMarkdown)) {
    job.deliverable = String(payload.deliverableMarkdown);
    changed = true;
  }

  if (payload.quality && typeof payload.quality === "object") {
    job.syncQuality = payload.quality;
    changed = true;
  }

  if (payload.processingNotes && Array.isArray(payload.processingNotes)) {
    job.syncNotes = payload.processingNotes;
    changed = true;
  }

  job.lastSyncedAt = payload.exportedAt ? String(payload.exportedAt) : new Date().toISOString();
  return changed;
}

async function importStatusFiles(files, { selectedOnly = false } = {}) {
  let imported = 0;

  for (const file of files) {
    let payload;
    try {
      payload = JSON.parse(await file.text());
    } catch {
      continue;
    }

    const job = findJobForStatusPayload(payload);
    if (!job) continue;
    if (selectedOnly && !selectedJobIds.has(job.id)) continue;
    if (!mergeStatusPayload(job, payload)) continue;

    await saveJob(job);
    imported += 1;
  }

  jobs = await loadJobs();
  renderFilesList();
  renderDrawer();

  if (imported) toast(`Imported ${imported} status file${imported === 1 ? "" : "s"}`, "success");
  else toast("No matching jobs found in these status files", "warn");
}

function renderDeliverable(markdown) {
  deliverableRendered.innerHTML = "";
  const sections = parseDeliverable(markdown);
  if (!sections.length) {
    const empty = document.createElement("div");
    empty.className = "results-empty-state";
    empty.textContent = "The imported file did not contain structured markdown yet.";
    deliverableRendered.appendChild(empty);
    return;
  }

  for (const section of sections) {
    const article = document.createElement("article");
    article.className = "results-section";

    if (section.title && section.title !== "Deliverable") {
      const heading = document.createElement("h4");
      heading.textContent = section.title;
      article.appendChild(heading);
    }

    for (const block of section.blocks) {
      if (block.type === "paragraph") {
        const p = document.createElement("p");
        p.className = "results-paragraph";
        p.textContent = block.text;
        article.appendChild(p);
      } else if (block.type === "list") {
        article.appendChild(buildList(block.items));
      }
    }

    deliverableRendered.appendChild(article);
  }
}

/* ─── manifest / brief / package builders ──────── */

function buildManifest(job) {
  const routing = getRoutingProfile(job);
  return {
    jobId: job.id,
    jobSlug: job.jobSlug,
    clientName: job.clientName,
    clientContact: job.clientContact,
    jobTitle: job.jobTitle,
    outputGoal: job.outputGoal,
    presetType: job.presetType || null,
    buyerType: job.buyerType || null,
    serviceLane: routing.serviceLane,
    outputShape: routing.outputShape,
    nextStep: routing.nextStep,
    turnaroundTarget: job.turnaroundTarget,
    contextNotes: job.contextNotes,
    status: job.status,
    fileName: job.fileName,
    fileType: job.fileType,
    fileSize: job.fileSize,
    createdAt: job.createdAt,
    handoffNotes: [
      "Source file stored locally in browser IndexedDB.",
      "Export the audio file separately if the pipeline needs a filesystem copy.",
    ],
  };
}

function buildBrief(job) {
  const routing = getRoutingProfile(job);
  const lines = [
    `Job: ${job.jobTitle}`,
    job.clientName ? `Client: ${job.clientName}` : null,
    job.buyerType ? `Buyer: ${job.buyerType}` : null,
    `Turnaround: ${fmtTurnaround(job.turnaroundTarget)}`,
    `Goal: ${job.outputGoal}`,
    `Service Lane: ${routing.serviceLane}`,
    `Output Shape: ${routing.outputShape}`,
    `Status: ${statusLabel(job.status)}`,
    `File: ${job.fileName} (${fmtBytes(job.fileSize)})`,
    `Submitted: ${fmtDate(job.createdAt)}`,
    "",
    `Next Step:\n${routing.nextStep}`,
    "",
    job.contextNotes ? `Notes:\n${job.contextNotes}` : null,
  ];
  return lines.filter(Boolean).join("\n");
}

function getRoutingProfile(job) {
  return ROUTING_RULES[job.outputGoal] || ROUTING_RULES.custom;
}

async function buildPackage(job) {
  const manifest = buildManifest(job);
  manifest.handoffNotes = [
    "Source file is embedded in this package for one-file local import.",
    "Use LocalAITranscriptionService --intake-package to process directly.",
  ];
  return {
    schemaVersion: 1,
    exportedAt: new Date().toISOString(),
    manifest,
    sourceFile: {
      name: job.fileName,
      type: job.fileType,
      size: job.fileSize,
      dataBase64: await blobToBase64(job.fileBlob),
    },
  };
}

/* ─── drop zone + file acceptance ──────────────── */

function acceptFile(file) {
  if (!file) return;
  if (file.size > MAX_FILE_SIZE) {
    toast(`Too large (${fmtBytes(file.size)}). Max 100 MB.`, "error");
    return;
  }
  if (file.size > WARN_FILE_SIZE) {
    toast(`Large file — storage may be limited on some browsers.`, "warn");
  }
  pendingFile = file;
  fileNameEl.textContent = file.name;
  fileSizeEl.textContent = fmtBytes(file.size);
  dropZone.classList.add("hidden");
  fileReady.classList.remove("hidden");
  jobTitleInput.focus();
}

function clearPendingFile() {
  pendingFile = null;
  activePreset = null;
  audioInput.value = "";
  contextForm.reset();
  pickButtons.forEach((b) => b.classList.remove("active"));
  fileReady.classList.add("hidden");
  dropZone.classList.remove("hidden");
}

dropZone.addEventListener("click", () => audioInput.click());
browseBtn.addEventListener("click", (e) => { e.stopPropagation(); audioInput.click(); });
audioInput.addEventListener("change", () => acceptFile(audioInput.files[0]));
clearFileBtn.addEventListener("click", clearPendingFile);

dropZone.addEventListener("dragover", (e) => { e.preventDefault(); dropZone.classList.add("drag-over"); });
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("drag-over"));
dropZone.addEventListener("drop", (e) => {
  e.preventDefault();
  dropZone.classList.remove("drag-over");
  const file = e.dataTransfer.files[0];
  if (file) acceptFile(file);
});

dropZone.addEventListener("keydown", (e) => {
  if (e.key === "Enter" || e.key === " ") { e.preventDefault(); audioInput.click(); }
});

/* ─── presets (quick picks) ────────────────────── */

pickButtons.forEach((btn) => {
  btn.addEventListener("click", () => {
    const key = btn.dataset.preset;
    const preset = PRESETS[key];
    if (!preset) return;

    if (activePreset === key) {
      btn.classList.remove("active");
      activePreset = null;
      document.getElementById("output-goal").value = "transcript-summary-actions";
      document.getElementById("turnaround-target").value = "flexible";
      document.getElementById("context-notes").value = "";
      return;
    }

    pickButtons.forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    activePreset = key;
    document.getElementById("job-title").placeholder = btn.textContent.trim();
    document.getElementById("output-goal").value = preset.outputGoal;
    document.getElementById("turnaround-target").value = preset.turnaroundTarget;
    document.getElementById("context-notes").value = preset.contextNotes;
  });
});

/* ─── submit ───────────────────────────────────── */

contextForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  if (!pendingFile) return;

  const title = jobTitleInput.value.trim();
  if (!title) { toast("Give it a name first", "warn"); return; }

  const clientName = (document.getElementById("client-name").value || "").trim();
  const clientContact = (document.getElementById("client-contact").value || "").trim();
  const outputGoal = document.getElementById("output-goal").value;
  const turnaroundTarget = document.getElementById("turnaround-target").value;
  const contextNotes = (document.getElementById("context-notes").value || "").trim();

  const job = {
    id: crypto.randomUUID(),
    jobSlug: slugify(title),
    jobTitle: title,
    clientName,
    clientContact,
    outputGoal,
    presetType: activePreset,
    buyerType: activePreset && PRESETS[activePreset] ? PRESETS[activePreset].buyerType : null,
    turnaroundTarget,
    contextNotes,
    status: "captured",
    createdAt: new Date().toISOString(),
    completedAt: null,
    deliverable: null,
    fileName: pendingFile.name,
    fileType: pendingFile.type,
    fileSize: pendingFile.size,
    fileBlob: pendingFile,
  };

  try {
    await saveJob(job);
    jobs = await loadJobs();
    selectedJobId = job.id;
    clearPendingFile();
    renderFilesList();
    renderDrawer();
    toast("Submitted", "success");
  } catch (err) {
    toast(`Save failed: ${err.message}`, "error");
  }
});

/* ─── file list rendering ──────────────────────── */

function filteredJobs() {
  const q = jobSearch.value.trim().toLowerCase();
  const s = statusFilter.value;
  return jobs.filter((j) => {
    if (operatorMode.checked && j.status === "done") return false;
    if (s !== "all" && j.status !== s) return false;
    if (!q) return true;
    return [j.jobTitle, j.clientName, j.fileName].filter(Boolean).join(" ").toLowerCase().includes(q);
  });
}

function selectedJobs() {
  return jobs.filter((job) => selectedJobIds.has(job.id));
}

function jobBySlug(slug) {
  return jobs.find((job) => job.jobSlug === slug);
}

function renderFilesList() {
  if (!jobs.length) {
    filesSection.classList.add("hidden");
    return;
  }
  filesSection.classList.remove("hidden");

  jobList.innerHTML = "";
  const visible = filteredJobs();

  if (!visible.length) {
    jobList.innerHTML = `<p style="color:var(--muted);text-align:center;padding:24px;">No files match.</p>`;
    return;
  }

  visible.forEach((job) => {
    const frag = cardTemplate.content.cloneNode(true);
    const card = frag.querySelector(".job-card");
    const checkbox = frag.querySelector(".job-select-checkbox");
    frag.querySelector(".job-card-title").textContent = job.jobTitle;
    frag.querySelector(".job-card-meta").textContent = `${job.fileName} · ${fmtDate(job.createdAt)}`;
    const badge = frag.querySelector(".job-card-status");
    badge.textContent = statusLabel(job.status);
    badge.dataset.status = job.status;
    checkbox.checked = selectedJobIds.has(job.id);
    checkbox.addEventListener("click", (e) => e.stopPropagation());
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) selectedJobIds.add(job.id);
      else selectedJobIds.delete(job.id);
      updateSelectionSummary();
    });

    if (job.id === selectedJobId) card.classList.add("active");

    card.addEventListener("click", () => { selectedJobId = job.id; renderFilesList(); renderDrawer(); });
    card.addEventListener("keydown", (e) => { if (e.key === "Enter") { selectedJobId = job.id; renderFilesList(); renderDrawer(); }});

    jobList.appendChild(frag);
  });
  updateSelectionSummary();
}

jobSearch.addEventListener("input", renderFilesList);
statusFilter.addEventListener("change", renderFilesList);
operatorMode.addEventListener("change", renderFilesList);

/* ─── detail drawer rendering ──────────────────── */

function renderDrawer() {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) {
    detailDrawer.classList.add("hidden");
    return;
  }
  detailDrawer.classList.remove("hidden");

  detailClient.textContent = job.clientName || "";
  detailTitle.textContent = job.jobTitle;
  detailFile.textContent = `${job.fileName} (${fmtBytes(job.fileSize)})`;
  detailGoal.textContent = job.outputGoal;
  detailBuyer.textContent = fmtBuyerType(job.buyerType);
  detailContact.textContent = job.clientContact || "\u2014";
  detailTurnaround.textContent = fmtTurnaround(job.turnaroundTarget);
  detailCreated.textContent = fmtDate(job.createdAt);
  detailStatus.value = job.status;
  detailBrief.value = buildBrief(job);
  const routing = getRoutingProfile(job);
  detailRouting.textContent = `${routing.serviceLane} -> ${routing.outputShape}. Next: ${routing.nextStep}.`;

  if (job.completedAt) {
    const h = Math.round((new Date(job.completedAt) - new Date(job.createdAt)) / 3600000);
    detailCompleted.textContent = `${fmtDate(job.completedAt)} (${h}h)`;
  } else {
    detailCompleted.textContent = "\u2014";
  }

  if (job.contextNotes) {
    detailContextBlock.classList.remove("hidden");
    detailContext.textContent = job.contextNotes;
  } else {
    detailContextBlock.classList.add("hidden");
  }

  if (job.deliverable) {
    deliverableEmpty.classList.add("hidden");
    deliverableContent.classList.remove("hidden");
    renderDeliverable(job.deliverable);
    deliverableText.textContent = job.deliverable;
  } else {
    deliverableEmpty.classList.remove("hidden");
    deliverableContent.classList.add("hidden");
    deliverableRendered.innerHTML = "";
    deliverableText.textContent = "";
  }

  detailDrawer.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

closeDrawerBtn.addEventListener("click", () => {
  selectedJobId = null;
  detailDrawer.classList.add("hidden");
  renderFilesList();
});

/* ─── drawer actions ───────────────────────────── */

saveStatusBtn.addEventListener("click", async () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) return;
  const ns = detailStatus.value;
  if (ns === "done" && job.status !== "done") job.completedAt = new Date().toISOString();
  job.status = ns;
  try {
    await saveJob(job);
    jobs = await loadJobs();
    renderFilesList();
    renderDrawer();
    toast("Updated", "success");
  } catch (err) { toast(`Update failed: ${err.message}`, "error"); }
});

copyBriefBtn.addEventListener("click", async () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) return;
  try { await navigator.clipboard.writeText(buildBrief(job)); toast("Copied", "success"); }
  catch { toast("Copy failed", "error"); }
});

exportBriefBtn.addEventListener("click", () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) return;
  downloadBlob(new Blob([buildBrief(job)], { type: "text/markdown" }), `${job.jobSlug}.brief.md`);
  toast("Exported", "success");
});

exportManifestBtn.addEventListener("click", () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) return;
  downloadBlob(new Blob([JSON.stringify(buildManifest(job), null, 2)], { type: "application/json" }), `${job.jobSlug}.manifest.json`);
  toast("Exported", "success");
});

exportPackageBtn.addEventListener("click", async () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) return;
  try {
    toast("Building\u2026", "info");
    const pkg = await buildPackage(job);
    downloadBlob(new Blob([JSON.stringify(pkg, null, 2)], { type: "application/json" }), `${job.jobSlug}.package.json`);
    toast("Package exported", "success");
  } catch (err) { toast(`Export failed: ${err.message}`, "error"); }
});

downloadAudioBtn.addEventListener("click", () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job || !job.fileBlob) { toast("No audio file", "error"); return; }
  downloadBlob(job.fileBlob, job.fileName);
  toast("Downloaded", "success");
});

deleteJobBtn.addEventListener("click", async () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) return;
  if (!(await confirmAction(`Delete \u201c${job.jobTitle}\u201d? This can\u2019t be undone.`))) return;
  try {
    await deleteJobFromDB(job.id);
    jobs = await loadJobs();
    selectedJobId = null;
    detailDrawer.classList.add("hidden");
    renderFilesList();
    toast("Deleted", "success");
  } catch (err) { toast(`Delete failed: ${err.message}`, "error"); }
});

/* ─── deliverable import / export ──────────────── */

importStatusBtn.addEventListener("click", () => importStatusInput.click());
importResultsBtn.addEventListener("click", () => importResultsInput.click());

importStatusInput.addEventListener("change", async () => {
  const files = Array.from(importStatusInput.files || []);
  if (!files.length) return;
  try {
    await importStatusFiles(files);
  } catch (err) {
    toast(`Status import failed: ${err.message}`, "error");
  } finally {
    importStatusInput.value = "";
  }
});

importResultsInput.addEventListener("change", async () => {
  const file = importResultsInput.files[0];
  if (!file) return;
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) return;
  try {
    job.deliverable = await file.text();
    if (job.status !== "done") { job.status = "done"; job.completedAt = new Date().toISOString(); }
    await saveJob(job);
    jobs = await loadJobs();
    renderFilesList();
    renderDrawer();
    toast("Results imported", "success");
  } catch (err) { toast(`Import failed: ${err.message}`, "error"); }
  finally { importResultsInput.value = ""; }
});

copyDeliverableBtn.addEventListener("click", async () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job || !job.deliverable) return;
  try { await navigator.clipboard.writeText(job.deliverable); toast("Copied", "success"); }
  catch { toast("Copy failed", "error"); }
});

exportDeliverableBtn.addEventListener("click", () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job || !job.deliverable) return;
  downloadBlob(new Blob([job.deliverable], { type: "text/markdown" }), `${job.jobSlug}.deliverable.md`);
  toast("Exported", "success");
});

clearDeliverableBtn.addEventListener("click", async () => {
  const job = jobs.find((j) => j.id === selectedJobId);
  if (!job) return;
  if (!(await confirmAction("Clear the results? This can\u2019t be undone."))) return;
  job.deliverable = null;
  try {
    await saveJob(job);
    jobs = await loadJobs();
    renderDrawer();
    toast("Cleared", "success");
  } catch (err) { toast(`Failed: ${err.message}`, "error"); }
});

/* ─── operator batch actions ───────────────────── */

selectVisibleBtn.addEventListener("click", () => {
  filteredJobs().forEach((job) => selectedJobIds.add(job.id));
  renderFilesList();
  toast("Visible jobs selected", "success");
});

clearSelectionBtn.addEventListener("click", () => {
  selectedJobIds.clear();
  renderFilesList();
});

markSelectedReadyBtn.addEventListener("click", async () => {
  const targets = selectedJobs();
  if (!targets.length) { toast("Select jobs first", "warn"); return; }
  try {
    for (const job of targets) {
      if (job.status === "captured") {
        job.status = "ready";
        await saveJob(job);
      }
    }
    jobs = await loadJobs();
    renderFilesList();
    renderDrawer();
    toast("Selected jobs marked ready", "success");
  } catch (err) {
    toast(`Batch update failed: ${err.message}`, "error");
  }
});

exportSelectedManifestsBtn.addEventListener("click", () => {
  const targets = selectedJobs();
  if (!targets.length) { toast("Select jobs first", "warn"); return; }
  for (const job of targets) {
    downloadBlob(new Blob([JSON.stringify(buildManifest(job), null, 2)], { type: "application/json" }), `${job.jobSlug}.manifest.json`);
  }
  toast(`Exported ${targets.length} manifest${targets.length === 1 ? "" : "s"}`, "success");
});

exportSelectedPackagesBtn.addEventListener("click", async () => {
  const targets = selectedJobs();
  if (!targets.length) { toast("Select jobs first", "warn"); return; }
  try {
    for (const job of targets) {
      const pkg = await buildPackage(job);
      downloadBlob(new Blob([JSON.stringify(pkg, null, 2)], { type: "application/json" }), `${job.jobSlug}.package.json`);
    }
    toast(`Exported ${targets.length} package${targets.length === 1 ? "" : "s"}`, "success");
  } catch (err) {
    toast(`Batch export failed: ${err.message}`, "error");
  }
});

importSelectedStatusBtn.addEventListener("click", () => {
  if (!selectedJobIds.size) { toast("Select jobs first", "warn"); return; }
  importSelectedStatusInput.click();
});

importSelectedStatusInput.addEventListener("change", async () => {
  const files = Array.from(importSelectedStatusInput.files || []);
  if (!files.length) return;
  try {
    await importStatusFiles(files, { selectedOnly: true });
  } catch (err) {
    toast(`Bulk status import failed: ${err.message}`, "error");
  } finally {
    importSelectedStatusInput.value = "";
  }
});

importSelectedResultsBtn.addEventListener("click", () => {
  if (!selectedJobIds.size) { toast("Select jobs first", "warn"); return; }
  importSelectedResultsInput.click();
});

importSelectedResultsInput.addEventListener("change", async () => {
  const files = Array.from(importSelectedResultsInput.files || []);
  if (!files.length) return;

  let imported = 0;
  try {
    for (const file of files) {
      const slug = file.name.replace(/\.(md|txt|markdown)$/i, "");
      const job = jobBySlug(slug);
      if (!job || !selectedJobIds.has(job.id)) continue;
      job.deliverable = await file.text();
      if (job.status !== "done") {
        job.status = "done";
        job.completedAt = new Date().toISOString();
      }
      await saveJob(job);
      imported += 1;
    }
    jobs = await loadJobs();
    renderFilesList();
    renderDrawer();
    if (imported) toast(`Imported ${imported} result${imported === 1 ? "" : "s"}`, "success");
    else toast("No matching selected jobs found for these files", "warn");
  } catch (err) {
    toast(`Bulk import failed: ${err.message}`, "error");
  } finally {
    importSelectedResultsInput.value = "";
  }
});

/* ─── PWA shell ────────────────────────────────── */

async function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  try {
    await navigator.serviceWorker.register("./sw.js");
  } catch (err) {
    console.error("service worker registration failed", err);
  }
}

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  deferredInstallPrompt = event;
  setInstallBannerVisible(true);
});

window.addEventListener("appinstalled", () => {
  deferredInstallPrompt = null;
  setInstallBannerVisible(false);
  toast("App installed", "success");
});

installAppBtn?.addEventListener("click", async () => {
  if (!deferredInstallPrompt) {
    toast("Install is not available in this browser yet", "warn");
    return;
  }
  deferredInstallPrompt.prompt();
  const choice = await deferredInstallPrompt.userChoice;
  if (choice.outcome !== "accepted") {
    setInstallBannerVisible(true);
    return;
  }
  deferredInstallPrompt = null;
  setInstallBannerVisible(false);
});

dismissInstallBtn?.addEventListener("click", () => {
  setInstallBannerVisible(false);
});

/* ─── boot ─────────────────────────────────────── */

async function boot() {
  await registerServiceWorker();
  db = await openDatabase();
  jobs = await loadJobs();
  renderFilesList();
}

boot().catch((err) => {
  console.error(err);
  toast(`Failed to load: ${err.message}`, "error");
});
