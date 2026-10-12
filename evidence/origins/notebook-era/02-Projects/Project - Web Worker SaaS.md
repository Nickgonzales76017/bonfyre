---
type: project
cssclasses:
  - project
title: Web Worker SaaS
created: 2026-04-03
updated: 2026-04-04
status: planned
stage: setup
priority: medium
review_cadence: weekly
system_role: Builder
idea_link: [[01-Ideas/Browser-Based Compute SaaS]]
tags:
  - project
  - planned
aliases:
  - Web Worker SaaS
  - Browser-Based Compute SaaS
---

# Project: Web Worker SaaS

> **This is the product packaging layer. The transcription service proves the workflow; this project turns it into software people can touch.**

## Summary
Build a browser-based local-first application platform, starting with a transcription intake console and evolving into a general-purpose client-side SaaS shell. Users upload files, the browser handles intake, state, and UX. Processing is hybrid: easy jobs run in-browser via WebAssembly, heavy jobs hand off to a local or remote pipeline through a structured manifest contract. Zero backend on day one.

## Objective
Ship a working browser product that a paying transcription customer can use to submit a file, track its status, and receive their deliverable — replacing awkward email/DM handoff with a professional intake experience. Then generalize the shell so adjacent services (arbitrage, data engine, market layer) can reuse the same local-first infrastructure.

## Success Criteria
| Milestone | Metric | Target |
|---|---|---|
| **Intake console live** | Browser accepts files, stores locally, exports handoff | **Done** (v0 exists) |
| **Operator loop closed** | File submitted in browser → deliverable returned in browser | Not yet |
| **First real job** | A paying customer submits via the browser console | After transcription revenue proof |
| **Handoff friction < 2 min** | Time from export to pipeline processing start | Currently ~5 min manual |
| **Product-grade UX** | PWA installable, offline-capable, polished enough for a stranger | Not yet |
| **Reusable shell** | Second service (not transcription) uses the same intake infrastructure | Not yet |
| **In-browser compute** | One processing step runs via WebAssembly without leaving the browser | Not yet |

## Product Thesis
The hardest part of selling a local AI service isn't the AI — it's the intake. How does a file get from a customer's device to your pipeline? Email attachments, shared drives, and DMs are lossy, unprofessional, and untrackable.

A browser-based intake console solves this without building a backend. The file never leaves the customer's browser until they explicitly export or share it. Local-first means:
- **No server cost** — zero marginal cost per customer session
- **No auth system** — no accounts, no passwords, no OAuth
- **No hosting complexity** — static files served from anywhere
- **Privacy by architecture** — data stays on-device by default
- **Instant distribution** — link to an HTML file, done

The playbook: start with transcription intake (proven workflow), then abstract the shell so any service vertical can plug in.

## The Machine (What We're Building)

### Architecture
```
Customer's Browser
 │
 ▼
[Intake Console — static HTML/JS/CSS]
 │
 ├── File upload → stored in IndexedDB (never leaves browser)
 ├── Metadata capture (client, context, buyer type, turnaround)
 ├── Job state management (queued, processing, completed, archived)
 ├── Preset workflows (Founder Memo, Customer Call, Consultant Recap)
 │
 ├──▶ EXPORT PATH A: Manifest JSON + source file download
 │     └── Operator imports into LocalAITranscriptionService
 │
 ├──▶ EXPORT PATH B: One-file intake package (manifest + base64 audio)
 │     └── Pipeline consumes directly via --intake-package
 │
 ├──▶ FUTURE PATH C: WebAssembly in-browser processing
 │     └── whisper.cpp WASM → transcript generated client-side
 │     └── Summary extraction runs in Web Worker
 │     └── Result displayed inline, no export needed
 │
 └──▶ FUTURE PATH D: Lightweight sync layer
       └── Optional WebSocket or polling bridge
       └── Operator sees queued jobs without manual export
       └── Results pushed back to customer's browser session
```

### What Already Exists (`10-Code/WebWorkerSaaS`)
| Component | File | Status |
|---|---|---|
| **Intake form** | `index.html` | **Working** — upload, metadata, presets, context |
| **Job persistence** | `app.js` (IndexedDB) | **Working** — CRUD, filtering, search |
| **Manifest builder** | `app.js` | **Working** — structured JSON with full metadata |
| **Package builder** | `app.js` | **Working** — one-file base64 embedded export |
| **Intake brief** | `app.js` | **Working** — human-readable markdown brief |
| **Routing layer** | `app.js` | **Working** — buyer, service lane, output shape, and next-step hints |
| **Job queue UI** | `index.html` | **Working** — status badges, search, filters |
| **Batch operator bar** | `index.html` + `app.js` | **Working** — select visible, mark ready, bulk export manifests/packages |
| **Bulk results import** | `index.html` + `app.js` | **Working** — selected jobs can ingest multiple deliverables by slug |
| **Operator mode** | `index.html` + `app.js` | **Working** — hides completed jobs so the queue reads like live work |
| **Result rendering** | `app.js` + `index.html` | **Working** — imported markdown now renders as structured sections and nested bullets |
| **PWA shell** | `manifest.webmanifest` + `sw.js` | **Working** — install prompt, app manifest, asset caching, offline fallback |
| **Status sync** | `app.js` + `browser-status.json` | **Working** — browser imports pipeline status JSON by `jobId`/`jobSlug` and merges deliverables back in |
| **Presets** | `app.js` | **Working** — Founder Memo, Customer Call, Consultant Recap |
| **Styles** | `styles.css` | **Working** — warm paper aesthetic, responsive layout |
| **README** | `README.md` | **Working** — setup, purpose, one-file handoff docs |

### What's Missing (Ordered by Blocking Priority)
1. **Operator/customer split** — the operator queue is good enough for local work, but there is still no separate operator/customer split or cross-session sync.
2. **Live sync** — file-based status sync exists, but there is still no push/poll bridge for automatic updates.
3. **WebAssembly compute** — no in-browser processing. The whisper.cpp WASM port exists in the wild but hasn't been evaluated for our model/quality constraints.
4. **Multi-service abstraction** — the intake form is still transcription-first. The shell needs pluggable service definitions.
5. **File size handling** — IndexedDB can handle ~50MB reliably, but there's no chunking or progress indicator for big uploads.
6. **Security hardening** — no CSP headers, no input sanitization beyond browser defaults, no CORS considerations for the eventual sync layer.
7. **Analytics** — no usage tracking, no conversion funnel data, no way to know how many jobs reach completion.
8. **PDF/polish exports** — deliverables are markdown-backed and browser-rendered, but customers still need better export formats.

### Technology Map
| Layer | Technology | Maturity | Notes |
|---|---|---|---|
| **Storage** | IndexedDB | Production-ready | ~50-100MB practical limit per origin. Sufficient for intake metadata + small-medium audio files. Large files need chunked storage or File System Access API. |
| **Background compute** | Web Workers | Production-ready | True parallel threads. No DOM access. Perfect for CPU-bound processing (parsing, summarization, formatting). |
| **Offline capability** | Service Workers | Production-ready | Required for PWA. Caches static assets, intercepts fetch, enables offline-first. Must handle cache invalidation carefully. |
| **Heavy compute** | WebAssembly | Production-ready runtime, immature toolchain | WASM runs at ~60-80% native speed on M3. `whisper.cpp` has a WASM port. Main challenge: model size (base = ~150MB WASM) and memory allocation (requires SharedArrayBuffer + COOP/COEP headers). |
| **File access** | File System Access API | Chrome/Edge only | Allows read/write to real filesystem. Not available in Safari/Firefox. Can't depend on it for cross-browser. Stick with IndexedDB + download for now. |
| **Sync** | WebSocket / SSE | Production-ready | For future operator-customer bridge. Requires a micro-server (even a single-file Node/Deno/Bun script). Not needed for v0-v1. |
| **Install** | PWA (manifest.json + SW) | Production-ready | All major browsers support install. Safari added home screen web apps in 2023. Full-screen, icon, splash screen. |
| **UI framework** | Vanilla JS | N/A | Current codebase is zero-dependency. Keep it this way as long as possible. Framework = dependency = build step = complexity. |

### WebAssembly Compute Analysis
**What WASM can realistically do in-browser on M3 16GB:**

| Task | Feasibility | Notes |
|---|---|---|
| Whisper base transcription (short files) | **Feasible but slow** | whisper.cpp WASM runs ~3-5x slower than native Metal. A 5-min file takes ~15-25 min in WASM vs ~5 min native. Acceptable for "fire and forget" use case. |
| Whisper base transcription (30+ min files) | **Impractical** | 90+ min WASM processing. Tab can't be backgrounded without Service Worker tricks. Battery drain is severe. |
| Sentence splitting + cleanup | **Trivial** | Pure string processing. Web Worker handles this instantly. No WASM needed. |
| Summary extraction (heuristic) | **Trivial** | Scoring + ranking in JS is fine. No WASM needed. |
| Summary extraction (LLM-based) | **Not yet feasible** | Mistral 7B won't fit in browser memory. Smaller models (TinyLlama, Phi-2) could work but quality is unproven. |
| Paragraph formatting | **Trivial** | JS string processing. |
| Template rendering | **Trivial** | JS string processing. |
| FFmpeg audio normalization | **Feasible** | ffmpeg.wasm exists and works. Adds ~8MB to initial load. Worth it if doing in-browser transcription. |

**Verdict:** The first useful WASM workload is short-file transcription (under 10 min) with a clear "this runs on YOUR device" privacy pitch. Everything else is standard JS.

## Tooling
### Project Tooling
- **primary**: browser APIs (IndexedDB, Web Workers, Service Workers, File API), vanilla HTML/CSS/JS, WebAssembly
- **supporting**: Obsidian project/research notes, LocalAITranscriptionService pipeline learnings
- **implementation path**: `10-Code/WebWorkerSaaS`
- **upstream pipeline**: `10-Code/LocalAITranscriptionService` (consumes intake manifests and packages)
- **related prototype**: `10-Code/WebWorkerSaaS/index.html` (current intake console)

### Meta Tooling
- shared tooling or infrastructure: vault operating system, template stack, automation roadmap, unified queue dispatcher
- linked meta system:
  - [[04-Systems/04-Meta/Meta-System]]
  - [[04-Systems/04-Meta/Vault Operating System]]
  - [[04-Systems/04-Meta/Tooling Stack]]
  - [[04-Systems/04-Meta/n8n Workflow Map]]

## Market & Offer

### Who Uses This
| User Type | Role | What They Want | Priority |
|---|---|---|---|
| **Transcription customer** | Submits files, gets deliverables | Professional intake, status visibility, clean results | **Primary (v0-v1)** |
| **Operator (you)** | Receives jobs, runs pipeline, returns results | Job queue visibility, batch export, status management | **Primary (v0-v1)** |
| **Self-serve user** | Uploads file, gets transcript without operator | In-browser WASM processing, zero friction | **Future (v2+)** |
| **Adjacent service customer** | Uses the shell for non-transcription work | Pluggable service definitions | **Future (v3+)** |

### Why Customers Would Use This Over Email
| Pain Point | Current (Email/DM) | Browser Console |
|---|---|---|
| File transfer | Attachment size limits, compression artifacts, lost in inbox | Direct upload, stored locally, no size negotiation |
| Context capture | "Here's a recording, can you transcribe it?" — zero context | Structured form: buyer type, turnaround, output goal, notes |
| Status visibility | "Hey did you finish that file?" — async ping-pong | Status badges: queued → processing → complete → delivered |
| Deliverable access | Email attachment, Google Doc link, lost in thread | In-browser results view, download, copy |
| Trust / professionalism | Feels like asking a friend for a favor | Feels like using a product |
| Privacy | File sits in email server forever | File stays in customer's browser until explicit export |

### Competitive Position
| Approach | Cost | Complexity | Privacy | UX |
|---|---|---|---|---|
| **Google Form + Drive** | Free | Low | Poor (files on Google) | Amateur |
| **Typeform + Zapier** | $50+/mo | Medium | Poor (files on 3rd party) | Good but generic |
| **Custom backend (Django/Next.js)** | Hosting cost + dev time | High | Depends | Professional but expensive |
| **Tally.so / JotForm** | $0-40/mo | Low | Poor | Good for simple forms |
| **Our browser console** | **$0** | **Low** | **Excellent (local-first)** | **Professional, purpose-built** |

### Monetization Path
The console itself is not the product — it's the *delivery mechanism* for the service. But it becomes a product when:
1. **Self-serve transcription** (WASM) — charge per file, no operator needed
2. **White-label intake** — sell the shell to other service operators
3. **Premium features** — batch upload, team queues, priority processing, PDF export
4. **Plugin marketplace** — other developers build service-specific intake plugins

Initial pricing stays with the transcription tiers ($12-35/file). The console is the distribution layer, not the revenue line.

## Build Plan

### Phase 0 — Foundation (Current)
> **Goal: intake console captures files and exports cleanly.**
- [x] HTML/CSS/JS intake form with presets
- [x] IndexedDB persistence and filtering
- [x] JSON manifest export
- [x] One-file package export (manifest + base64 embedded audio)
- [x] Source file download
- [x] Human-readable intake brief
- [x] Status management (draft, ready, processing, complete, archived)
- [x] Search and filter in job queue

### Phase 1 — Operator Loop (NEXT)
> **Goal: operator can see jobs and return results without leaving the browser.**
- [x] Stamp jobs with buyer-aware routing metadata for operator handoff
- [x] Add operator view: list of all pending exports with batch action buttons
- [x] Add "Import Results" — operator drops deliverable markdown back into the console
- [x] Results display: transcript, summary, and nested bullets rendered in the detail panel
- [x] Add file-based status sync import from pipeline-generated `browser-status.json`
- [x] Add operator-only mode so live work is visible without done-job clutter
- [x] Copy/download deliverable from the browser
- [x] Tie browser intake, local processing, and sync-back into a named `browser-fulfillment` product loop

### Phase 2 — PWA Shell
> **Goal: installable, offline-capable, professional.**
- [x] Add `manifest.webmanifest` with icons, name, theme colors
- [x] Add service worker for static asset caching
- [x] Add offline fallback page
- [ ] Test PWA install on macOS Safari, iOS Safari, Chrome
- [ ] Add viewport meta tag for mobile
- [ ] Add loading states for large file uploads

### Phase 3 — Deliverable Polish
> **Goal: output quality matches the service promise.**
- [ ] Add styled HTML deliverable view (not just raw markdown)
- [ ] Add PDF export (client-side, using `jsPDF` or `html2canvas` — no server)
- [ ] Add email-ready "share results" with formatted summary
- [ ] Add job timing: submitted → completed, turnaround visible to customer

### Phase 4 — WebAssembly In-Browser Processing
> **Goal: short files can be transcribed entirely in the browser.**
- [ ] Evaluate whisper.cpp WASM: binary size, memory requirements, quality vs native
- [ ] Build WASM loader: download model on first use, cache in IndexedDB
- [ ] Build Web Worker transcription wrapper: queue file → WASM → transcript
- [ ] Add ffmpeg.wasm for audio normalization pre-step
- [ ] Add progress indicator (WASM Whisper can report decode progress)
- [ ] Decision gate: if WASM quality ≥80% of native AND files under 10 min process in <20 min → ship it
- [ ] If gate passes: add "Process Locally" button that runs entirely in-browser
- [ ] If gate fails: keep hybrid model, document why, revisit when hardware improves

### Phase 5 — Sync Layer
> **Goal: customer submits, operator sees, results return — no manual export/import.**
- [ ] Build micro-server: single-file Bun/Deno script, WebSocket, <50 LOC
- [ ] Browser sends job notification to local server on submit
- [ ] Server queues job directly into `.bonfyre-runtime/` pipeline queue
- [ ] Pipeline results push back to browser via WebSocket
- [ ] Customer sees real-time status updates
- [ ] Operator dashboard shows live queue

### Phase 6 — Multi-Service Abstraction
> **Goal: the shell accepts any service, not just transcription.**
- [ ] Define service plugin interface: `{ name, fields, presets, exportFormat, resultParser }`
- [ ] Extract transcription-specific form fields into a plugin definition
- [ ] Build one additional service plugin as proof (e.g., document formatting, meeting recap)
- [ ] Service selector dropdown in the intake form
- [ ] Each service gets its own IndexedDB object store

### Phase 7 — Distribution
> **Goal: strangers can find and use this.**
- [ ] Deploy static files to a free host (GitHub Pages, Cloudflare Pages, Netlify)

## Execution Log
### 2026-04-04
- intake jobs now carry structured routing data: buyer, service lane, output shape, and next step
- detail drawer now shows buyer and routing so the operator sees downstream fit without reading raw notes
- exported manifests and briefs now preserve that routing layer for downstream systems
- queue now has a lightweight operator batch bar for bulk selection, bulk export, and bulk `ready` updates
- queue now supports bulk results import by matching selected jobs against deliverable filenames
- [ ] Add landing page with value prop, demo, install instructions
- [ ] Add link from [[02-Projects/Project - Quiet Distribution Engine]] for distribution
- [ ] Submit to Product Hunt / Hacker News "Show HN"
- [ ] Add analytics (privacy-respecting: Plausible, Umami, or custom)

## Tasks (Current Sprint)

### NOW — When This Project Activates
- [ ] Add "Import Results" flow so operator can return deliverables to the browser
- [ ] Add deliverable display panel in job detail view
- [ ] Add `manifest.json` and basic service worker for PWA install
- [ ] Test PWA install on macOS Safari and mobile Safari
- [ ] Process one real transcription job through the full browser → export → pipeline → import results loop

### NEXT — After Operator Loop Is Proven
- [ ] Add styled HTML deliverable view
- [ ] Add PDF export (client-side)
- [ ] Evaluate whisper.cpp WASM binary size and decode quality on M3
- [ ] Build progress indicators for file upload and processing
- [ ] Add job timing metrics

### LATER — After Product Loop Is Proven
- [ ] Build micro-server sync bridge
- [ ] Build service plugin interface
- [ ] Deploy to static host
- [ ] Distribution campaign

## Constraints
- **No backend on day one.** Static files only. The value of this project is proving that local-first actually works as a product, not building another CRUD server.
- **No framework.** Vanilla JS until the complexity genuinely demands a framework. `app.js` is ~400 lines and readable. That's the constraint.
- **No build step.** No bundler, no transpiler, no npm. Open `index.html` in a browser. If a tool needs a build step, it's the wrong tool for this phase.
- **Browser is king.** Chrome, Safari, Firefox must all work. No Chrome-only APIs (File System Access) in the critical path.
- **1 developer, 0 budget.** Everything open-source, everything local, everything achievable solo.
- **Transcription service comes first.** This project does not activate fully until the manual transcription loop has revenue proof. The intake prototype exists to reduce friction for that loop, not to replace it.

## Current Blockers (Ordered)
1. **Manual transcription loop not yet proven with revenue** — this project stays "planned" until the first transcription dollars come in
2. **No deliverable return path** — customer submits but can't get results back in the browser
3. **No PWA shell** — not installable, not offline-capable
4. **WASM transcription unproven** — whisper.cpp WASM exists but hasn't been tested on our hardware with our quality bar
5. **No sync mechanism** — fully manual export/import between browser and pipeline

## Risks
| Risk | Impact | Mitigation |
|---|---|---|
| WASM Whisper quality too low | In-browser transcription is unusable → stays hybrid | Test early with real files. Keep hybrid path as permanent fallback. Quality gate before shipping. |
| WASM Whisper too slow for long files | Users tab-away, processing dies | Web Workers + Service Worker keep-alive. Limit WASM to <10 min files. Clear UX expectations. |
| IndexedDB limits hit | Large audio files fail to store | Add file size validation. Use File System Access API as progressive enhancement. Warn users. |
| Multi-browser compat | Safari Service Worker bugs, Firefox IndexedDB quirks | Test on all 3 browsers every phase. Avoid bleeding-edge APIs. |
| Scope creep into framework/tooling | Months of infrastructure, zero product value | **RULE: no framework until vanilla JS fails at something specific**. Document the failure before switching. |
| Building product before service is proven | Beautiful software, zero customers | **RULE: this project does not fully activate until transcription has first revenue.** Current work is limited to intake friction reduction. |
| Over-abstracting the multi-service shell too early | Months building a "platform" nobody uses | Keep transcription as the only service until Phase 6. Only abstract when a real second service demands it. |

## Metrics
| Metric | Current | Target |
|---|---|---|
| Intake console status | v0 prototype working | Production-grade PWA |
| Jobs submitted via browser | 0 real jobs | First real job through browser → pipeline → results |
| Handoff time (browser → pipeline) | ~5 min manual | <2 min (with sync: near-instant) |
| Deliverable return time | Manual email/DM | In-browser results view |
| WASM transcription feasibility | Untested | Decision gate: ship or defer |
| Services supported | 1 (transcription only) | 2+ after transcription is proven |
| Install-capable (PWA) | No | Yes |
| Offline-capable | No | Yes |

## Dependencies
### Requires
- [[02-Projects/Project - Local AI Transcription Service]] (upstream service that proves the workflow)
- [[01-Ideas/Browser-Based Compute SaaS]] (source idea with philosophy and system flows)
- [[04-Systems/01-Core Systems/Web Worker SaaS]] (system design document)
- [[03-Research/Research - Web Worker SaaS Prototype Scope]] (prototype boundary research)
- [[04-Systems/04-Meta/Tooling Stack]] (shared infrastructure context)

### Enables
- [[02-Projects/Project - Personal Market Layer]] (software packaging for the marketplace)
- [[02-Projects/Project - Quiet Distribution Engine]] (distributable product artifact)
- [[02-Projects/Project - Repackaged Service Marketplace]] (reusable intake shell for bundled services)

### Blocks
- Self-serve transcription product (requires WASM evaluation from Phase 4)
- White-label intake product (requires multi-service abstraction from Phase 6)

## Execution Log
### 2026-04-03
- project created from browser SaaS idea
- positioned as productization path for local transcription, not a separate immediate bet
- intake prototype scaffold created in `10-Code/WebWorkerSaaS` (HTML + CSS + JS)
- IndexedDB job persistence, metadata capture, preset workflows implemented
- exportable intake manifests now connect to `LocalAITranscriptionService` through a reusable handoff contract
- exported intake folders can be processed in batch by `LocalAITranscriptionService`
- browser intake can export a one-file package with embedded source audio that pipeline processes via `--intake-package`
- intake now captures buyer type and turnaround, local queue filters by status and search
### 2026-04-04
- project file rewritten with full maximalist spec: architecture, technology map, WASM analysis, build plan through Phase 7, competitive landscape, detailed blocker/risk/metric tracking

## Decisions
| Decision | Rationale |
|---|---|
| Start with intake, not in-browser processing | Intake friction is the real bottleneck. WASM transcription is nice but secondary. |
| Vanilla JS, no framework | ~400 lines of readable code. No build step. No dependency chain. Framework when complexity demands it. |
| Static-only, no backend | Proves local-first thesis. Zero cost. Forces smarter architecture. |
| Hybrid processing before fully local | Keeps quality high. WASM is slower and potentially lower quality. Let quality gate decide. |
| Transcription first, abstract later | One proven service > flexible empty platform. Abstract only when second service demands it. |
| IndexedDB over localStorage | Structured data, larger storage, async API. localStorage has 5MB limit and is synchronous. |
| One-file package format | Reduces handoff from 2 files (manifest + audio) to 1. Less operator friction. |
| No auth system | Adds enormous complexity for zero-customer-count product. Add only if sync layer demands it. |
| PWA over native app | Single codebase, cross-platform, no App Store review. Install is free and instant. |

## Open Technical Questions
1. **SharedArrayBuffer for WASM Whisper** — requires COOP/COEP headers, which means a real server, not `file://`. Does this break the static-only constraint? (Probably yes — requires even a minimal static server with correct headers.)
2. **IndexedDB storage limit** — Safari limits to ~1GB per origin in practice. Chrome is more generous. What's the reasonable max file size before we warn the user?
3. **Service Worker + IndexedDB interaction** — SW can't access DOM but CAN access IndexedDB. Can we use SW as the background sync bridge without a separate server?
4. **Base64 encoding overhead** — one-file packages embed audio as base64, which is ~33% larger. For a 50MB audio file, that's 67MB package. Is this acceptable or do we need a binary format?
5. **Mobile Safari PWA limitations** — push notifications don't work, background processing is limited. How much does this matter for our use case?

## Links
### Source Idea
- [[01-Ideas/Browser-Based Compute SaaS]]

### Related Projects
- [[02-Projects/Project - Local AI Transcription Service]]
- [[02-Projects/Project - Personal Market Layer]]
- [[02-Projects/Project - Quiet Distribution Engine]]
- [[02-Projects/Project - Repackaged Service Marketplace]]

### Systems
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
- [[04-Systems/01-Core Systems/Web Worker SaaS]]
- [[04-Systems/04-Meta/Tooling Stack]]

### Adjacent Ideas
- [[01-Ideas/Simple Intake Portal]]

### Research
- [[03-Research/Research - Web Worker SaaS Prototype Scope]]

### Code
- `10-Code/WebWorkerSaaS`

### Concepts
- [[04-Systems/03-Concepts/Local-First]]
- [[04-Systems/03-Concepts/Infrastructure]]
- [[04-Systems/03-Concepts/Automation]]
- [[04-Systems/03-Concepts/Distribution]]
- [[04-Systems/03-Concepts/WebAssembly]]
- [[04-Systems/03-Concepts/Decentralization]]

## Tags
#project #planned #infrastructure #compute
