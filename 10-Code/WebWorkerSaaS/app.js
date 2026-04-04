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

const detailDrawer = document.getElementById("detail-drawer");
const closeDrawerBtn = document.getElementById("close-drawer");
const detailClient = document.getElementById("detail-client");
const detailTitle = document.getElementById("detail-title");
const detailFile = document.getElementById("detail-file");
const detailGoal = document.getElementById("detail-goal");
const detailContact = document.getElementById("detail-contact");
const detailTurnaround = document.getElementById("detail-turnaround");
const detailCreated = document.getElementById("detail-created");
const detailCompleted = document.getElementById("detail-completed");
const detailContext = document.getElementById("detail-context");
const detailContextBlock = document.getElementById("detail-context-block");
const detailBrief = document.getElementById("detail-brief");
const detailStatus = document.getElementById("detail-status");
const saveStatusBtn = document.getElementById("save-status");

const copyBriefBtn = document.getElementById("copy-brief");
const exportBriefBtn = document.getElementById("export-brief");
const exportManifestBtn = document.getElementById("export-manifest");
const exportPackageBtn = document.getElementById("export-package");
const downloadAudioBtn = document.getElementById("download-audio");
const deleteJobBtn = document.getElementById("delete-job");

const importResultsBtn = document.getElementById("import-results-btn");
const importResultsInput = document.getElementById("import-results");
const deliverableEmpty = document.getElementById("deliverable-empty");
const deliverableContent = document.getElementById("deliverable-content");
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

const PRESETS = {
  "founder-memo": {
    outputGoal: "voice-memo-to-plan",
    turnaroundTarget: "same-day",
    contextNotes: "Founder voice memo. Pull out key decisions, priorities, and immediate next steps.",
  },
  "customer-call": {
    outputGoal: "meeting-recap",
    turnaroundTarget: "24h",
    contextNotes: "Customer conversation. Extract pain points, objections, signals, and follow-up actions.",
  },
  "consultant-recap": {
    outputGoal: "transcript-summary-actions",
    turnaroundTarget: "24h",
    contextNotes: "Client-facing recap. Keep wording crisp and decision-ready for handoff.",
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

/* ─── manifest / brief / package builders ──────── */

function buildManifest(job) {
  return {
    jobId: job.id,
    jobSlug: job.jobSlug,
    clientName: job.clientName,
    clientContact: job.clientContact,
    jobTitle: job.jobTitle,
    outputGoal: job.outputGoal,
    presetType: job.presetType || null,
    buyerType: job.buyerType || null,
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
  const lines = [
    `Job: ${job.jobTitle}`,
    job.clientName ? `Client: ${job.clientName}` : null,
    `Turnaround: ${fmtTurnaround(job.turnaroundTarget)}`,
    `Goal: ${job.outputGoal}`,
    `Status: ${statusLabel(job.status)}`,
    `File: ${job.fileName} (${fmtBytes(job.fileSize)})`,
    `Submitted: ${fmtDate(job.createdAt)}`,
    "",
    job.contextNotes ? `Notes:\n${job.contextNotes}` : null,
  ];
  return lines.filter(Boolean).join("\n");
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
    buyerType: null,
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
    if (s !== "all" && j.status !== s) return false;
    if (!q) return true;
    return [j.jobTitle, j.clientName, j.fileName].filter(Boolean).join(" ").toLowerCase().includes(q);
  });
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
    frag.querySelector(".job-card-title").textContent = job.jobTitle;
    frag.querySelector(".job-card-meta").textContent = `${job.fileName} · ${fmtDate(job.createdAt)}`;
    const badge = frag.querySelector(".job-card-status");
    badge.textContent = statusLabel(job.status);
    badge.dataset.status = job.status;

    if (job.id === selectedJobId) card.classList.add("active");

    card.addEventListener("click", () => { selectedJobId = job.id; renderFilesList(); renderDrawer(); });
    card.addEventListener("keydown", (e) => { if (e.key === "Enter") { selectedJobId = job.id; renderFilesList(); renderDrawer(); }});

    jobList.appendChild(frag);
  });
}

jobSearch.addEventListener("input", renderFilesList);
statusFilter.addEventListener("change", renderFilesList);

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
  detailContact.textContent = job.clientContact || "\u2014";
  detailTurnaround.textContent = fmtTurnaround(job.turnaroundTarget);
  detailCreated.textContent = fmtDate(job.createdAt);
  detailStatus.value = job.status;
  detailBrief.value = buildBrief(job);

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
    deliverableText.textContent = job.deliverable;
  } else {
    deliverableEmpty.classList.remove("hidden");
    deliverableContent.classList.add("hidden");
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

importResultsBtn.addEventListener("click", () => importResultsInput.click());

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

/* ─── boot ─────────────────────────────────────── */

async function boot() {
  db = await openDatabase();
  jobs = await loadJobs();
  renderFilesList();
}

boot().catch((err) => {
  console.error(err);
  toast(`Failed to load: ${err.message}`, "error");
});
