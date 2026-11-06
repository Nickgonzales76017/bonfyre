# Retiring 80% of the .mjs estate

Status: in progress. The October 22 inventory is the baseline; the progress entries below record native ports and verified retirements. The 80% target remains incomplete.

## What was counted

| Place | .mjs files | Lines | Unique contents not in the repo |
|---|---|---|---|
| `~/Developer/Bonfyre` | 1,907 | 224,799 | n/a |
| `~/.bonfyre` | 2,338 | 218,426 | 135 |
| `~/Downloads` | 1,989 | 221,482 | 283 |

`node_modules` (751 files) is excluded. Across all three places there are 2,221
distinct file contents. `~/.bonfyre` is almost all copies: seven
`workspace-packages/*/data` trees of 144 files each, one `dev-fabric` worktree
and dozens of `releases/*/browser` snapshots. `~/Downloads` is mostly older
hand-off bundles; 725 of its files are byte-identical to repo files.

So the real work is the repo. The two outside trees need a triage pass, not a port.

## The repo, by role

`inventory.tsv` lists every repo file with its role, line count, wave and stage.
Roles come from content heuristics (browser APIs, `spawn`/`execFile` of native
binaries, `Bun.serve`, test imports), so a few files will be misfiled; fix them
in the ledger as each wave touches them.

| Wave | Files | Lines | Share | What it is | Where it goes |
|---|---|---|---|---|---|
| W0 | 241 | 19,155 | 8.5% | Checked-in `dist/`, `public/`, `generated/` copies; `output/verification` candidates; `aurekai-continuity-core` (copy of `Aurekai/src`) | Delete. Regenerate build output from source. |
| W1 | 188 | 24,081 | 10.7% | JS that shapes arguments and shells out to a C organ | One YAFF command contract per organ, checked by CUE, run by a single C dispatcher |
| W2 | 89 | 17,115 | 7.6% | `integrations/mcp`: eight hand-written SQLite stores, the daemon, the gateway | YAFF store schemas, one C store organ, MCP tool schemas generated from YAFF |
| W3 | 12 | 1,533 | 0.7% | `architecture/*.mjs` atlas checkers and query | CUE constraints over the exported atlas; query through the C YAFF lexer (`cmd/BonfyreLex`) |
| W4 | 844 | 78,300 | 34.8% | Tests | Golden vectors (data) plus one native runner. Tests of retired JS leave with it. |
| W5 | 468 | 45,849 | 20.4% | Other authored logic: ServiceSite build, `Aurekai/src`, browser integration, ExternalEstate and SurfaceScene tooling | Triage file by file: C organ, CUE/YAFF data, or keep |
| keep | 65 | 38,772 | 17.2% | Browser runtime (10.8k) and vendored SheetJS (28.2k, third-party) | Stays JS. Compute kernels move to C compiled to WASM, as `creator-wasm` already does. |

Retiring W0 through W4 removes 62% of the lines. Getting to 80% needs about
two thirds of W5. That is the number to watch: if W5 triage shows most of it is
real logic with no native owner, the target is closer to 70%.

## The pattern to reuse

`schemas/control/` already did this once, Python to C11: extract YAFF schemas,
record golden replay vectors from the old code, build the native kernel, run the
vectors against it (15/15), then cut over call sites. Every wave below follows
the same five steps. The old JS stays as the conformance oracle until its
vectors pass natively, and then it is deleted, not kept alongside.

## Waves in order

**W0, delete (days).** No behaviour change. Confirm each build-output tree is
produced by a build command, add it to `.gitignore`, delete it. For
`~/.bonfyre/workspace-packages` and `releases`, use the density skill: they are
content-addressed copies and compress to one set.

**W1, command contracts (1–2 weeks).** The 188 wrapper files mostly build argv,
call a binary in `~/.bonfyre/bin`, and parse JSON back. Put the argv shape and
result shape in the organ's YAFF entry, validate with CUE, and let `BonfyreCLI`
or a small generic dispatcher run them. Largest clusters: `cmd/BonfyreSourceFrontier`
(2.0k lines), `cmd/BonfyreExternalEstate` (1.9k), `scripts/representation-manifold-*`.
Hazard: binaries in `~/.bonfyre/bin` already lag `cmd/` builds. The dispatcher
must refuse a binary whose build hash does not match the contract.

**W2, MCP stores (2–3 weeks).** `canary`, `fabric`, `hypervisor`, `operator`,
`recursive`, `spines`, `v6` and `project_operator` each open SQLite and write
JSON side files in their own way. One YAFF schema per store, one C organ that
owns all of them, tool input schemas generated from the same YAFF. The OAuth
gateway and the MCP transport move to a C server as well (decided: the faster
option), hosted by the long-lived organ host from WX rule 5.

**W3, atlas checkers (days).** Maturity backing, witness grade, falsifiable
claims and build edges are constraints, which is what CUE is for. Run
`cue vet` over the exported atlas in CI and in the `.yaff` edit hook.

**W4, tests (runs alongside every wave).** Do not port 78k lines of tests. For
each organ, capture its current JS tests as input/output vectors, run them
through one native runner, and delete the JS tests when the code they cover is
gone. `cmd/BonfyreWebAddress` (5.8k test lines) and `components/BonfyreControlPlane`
(5.2k) already test C and Python through Bun; those become vector files first.

**W5, triage (ongoing).** Largest blocks: `components/BonfyreServiceSite` (6.5k,
125 files), `Aurekai/src` (4.7k), `integrations/browser` (3.6k),
`components/BonfyreDecision` (3.5k). For each file, check the atlas and
`native_tool_inventory` for an owner first. If an organ owns it, the JS goes. If
it is configuration written as code, it becomes YAFF or CUE. If it is genuine
logic with no owner, it stays until someone builds that owner.

## Outside the repo

`out-of-repo-unique.tsv` lists the 418 files whose contents exist nowhere in
the repo. Most are hand-off bundles: `aurekai-cloud-capability-recovery-2026-10-06`
(109), `aurekai-skills-to-native-demos` (89), `aurekai-session-recovery-r1` (23).
Each needs one decision: absorb into the repo or record in the hand-off backfill
ledger. Then the bundle is archived (decided 2026-10-22): one zstd-compressed,
content-addressed archive with a manifest of hashes, replacing the loose
folders. None should be ported directly.

## Progress (ledger: `inventory.tsv`, stage column)

### 2026-10-22, first session

Guards (so this does not grow back):

- `scripts/native-ratchet.sh` + `architecture/native-ratchet.cue`: budgets for
  authored `.mjs` files and lines, other scripts, `bun build --compile` sites,
  JS-runtime programs installed and the largest installed program. Run by the
  pre-commit hook, by CI (`.github/workflows/native-ratchet.yml`) and by a
  Claude PostToolUse hook on every script write. Budgets only fall
  (`tighten`). Session-start context shows the figures.
- `scripts/retire.sh`: verified, path-ordered zstd archives with a sha256
  manifest; nothing is removed unless it restores entry for entry.
- CLAUDE.md and AGENTS.md carry the rule for Claude and Codex.
- Atlas: `native-ratchet`, `js-compat-primitives`, `retirement-archive`
  (architecture/optimization/native-retirement.yaff).

Shared C substrate, in libbonfyre (each with a parity suite against Bun):

- `bf_jsonw`: JSON writer byte-identical to `JSON.stringify` (300k random
  doubles exact), canonical digests.
- `bf_jv`: mutable JSON value in JS property order, hash-indexed large
  objects, size-class allocator.
- `bf_re`: JS-compatible regular expressions (51 cases).
- SHA-256 on ARMv8 instructions; `-ffp-contract=off` for JS-exact float sums.

WZ, done:

| Program | Before | After | Parity |
|---|---|---|---|
| bonfyre-operator | 63 MB bun-compiled TS | 54 KB C, 3x faster | 33 vectors + live run8 projection |
| bonfyre-books, -monitor, -manifest | 3 x 63 MB bun-compiled | one 246 KB C program, hard-linked | 47 vectors (learn, CSV, XLSX in/out, learned owner, local model via a mock front, OCR, ITAD) |

Books on 1,500 lines: 0.96 s vs 1.12 s; learning 6,000 lines: 0.20 s vs
1.43 s; a small file: 0.05 s vs 0.16 s; learned weights byte-identical.
MCP `project_products` and the ServiceSite workbook tests now call the native
programs. Corrections are listed in `cmd/*/tests/CORRECTIONS`.

W0, partly done:

- `~/Downloads` bundles dated on or before 2026-10-19: 6,077 files, 262 MB,
  archived to 62 MB (`~/.bonfyre/archive/retired/`). Newer bundles kept.
- `Aurekai/generated` (649 tracked files, drifted from its generator) retired;
  regenerate with `bun Aurekai/bin/akai.mjs integrate:scaffold-repos --all`.
- 2026-10-23: `packs/aurekai-work-samples/site/dist` retired into the
  restore-verified `~/.bonfyre/archive/retired/work-samples-dist-20261023.tar.zst`:
  163 files, 11,922,837 bytes to 5,115,190 archive bytes, including 65 generated
  `.mjs` files and 6,597 lines. Authored site records, material providers, data
  and assets remain in place. A temporary rebuild produced the same 15 page
  paths and ten data families. Regenerate before running the historical
  `site/design/verification-home-20261008/verify-home.mjs` preview with
  `bun components/BonfyreServiceSite/build.mjs packs/aurekai-work-samples/site`.
  The existing global `dist/` ignore already excluded these copies from the
  ratchet; the exact path now also has an explicit ignore entry. This archive
  reduces loose output on disk and does not lower the authored-script count.
  It is output cleanup, not a native port.
- 2026-10-23: archived the generated
  `packs/no-simulation/catalogue-release-20261017/public/` after public activation
  and its acceptance process finished; no process had the tree open. The
  restore-verified archive is
  `~/.bonfyre/archive/retired/catalogue-release-output-20261023.tar.zst`
  (109 files, 10,158,283 loose bytes, 4,608,658 archive bytes; SHA-256
  `fae29c01f90f29a69c7c644e56781c6054c3cb79e2556270f79f60a71dfb6342`).
  Its source manifests, build/test logs and dated browser findings remain;
  the findings now name the archive. Rebuilding the retained site with
  `bun components/BonfyreServiceSite/build.mjs packs/aurekai-work-samples/site --out /tmp/catalogue-preview`
  passed and retained all nine historical page paths and source assets.
  Current output has 15 pages and 118 data files; restoring the archive is
  required to recover the exact historical bytes. This removes 38 counted
  `.mjs` copies and 3,159 lines, plus two other script copies. The exact output
  path is ignored to prevent regenerated copies from returning to the ratchet.
- 2026-10-23: archived the obsolete `aurekai-continuity-core/` generated split
  fragment (one 1,469-line module and a Finder file). The active package uses
  `Aurekai/kernel/`; the fragment's three sibling imports were already absent.
  The existing generator rebuilt a temporary standalone package and its
  existing verifier passed all eleven exported APIs. Regenerate with
  `bash Aurekai/scripts/prepare-kernel-repo-split.sh --out-dir /tmp/continuity-core`
  and verify with
  `bash Aurekai/scripts/verify-kernel-repo-split.sh --out-dir /tmp/continuity-core`.
  No process had the old tree open. Its exact historical bytes are in
  `~/.bonfyre/archive/retired/continuity-core-split-20261023.tar.zst`
  (11,778 bytes; SHA-256
  `cc6da0109d768faa885e58297450cf4f20ccb0ae9344dc234b234d7daa38924e`).
  The generated destination is now ignored. The unstaged tracked deletion
  lowers counted lines but remains in Git's file census until the index changes.

Restore either historical tree from the repository root with
`zstd -dc --long=27 ~/.bonfyre/archive/retired/NAME.tar.zst | tar -xf -`.
Each archive also has a sibling `.sha256` manifest and a restore entry in the
archive `README.txt`; `retire.sh` compared a full extraction before removal.

W3, 2026-10-23:

- `bonfyre-atlas build-edges [--strict] [--json]` now owns the former
  `architecture/check-build-edges.mjs` audit, whose retired JS index-loader
  import prevented it from running. Six-family fixtures preserve its text,
  JSON and strict/refusal behavior; the actual 35-family checkout report also
  matched the old audit body with only its missing loader restored for capture.
  The report uses current YAFF, simple Makefile assignments and installed-file
  hashes. It does not evaluate Makefile variables or prove a twin is unused.
  The owner-local CUE shape and native fixture are in `cmd/BonfyreAtlas/`.
  The 64-line JS audit is restore-verified in
  `~/.bonfyre/archive/retired/atlas-build-edges-js-20261023.tar.zst`
  (2,459 bytes; SHA-256
  `2d7192122e8f26a0b87afbe60c274c038ccaa4d4d338aaaddcd337926cbdb0b3`).
- Measured across these three retirements: 1,779 to 1,740 counted `.mjs`
  files, 179,979 to 175,287 lines, and 337 to 335 other scripts. This removes
  4,692 counted lines. The line budget is still exceeded by 309; no budget was
  raised and `tighten` has not run while this check fails. Further native work
  must close that remaining gap. Generated-copy cleanup is distinct from a
  native port, and neither establishes model quality or live deployment.

### 2026-10-27, reference census

A census of every authored `.mjs` against scripts, Makefiles, workflows,
launchd plists, package files, C headers, pages, the ServiceSite section and
material loaders, and tests (inventory with callers in the session scratchpad
as `js-refactor-inventory.tsv`). Most of the estate is reachable: about 950
files and 118k lines from a real root, about 31k lines of tests, and about
21k lines of library code that only tests reach.

- 22 files with no reference of any kind were retired into
  `js-unreferenced-20261027` (3,141 lines). They include the April demo-seeding
  scripts, the August tier-B page migration, the hypervisor cutover script and
  evidence-pack witness scripts. `docs/demo-seeding.md` names the restore
  command.
- Four evidence copies of upstream Creator files were retired into
  `creator-upstream-copies-20261027` (544 lines). Three were byte-identical to
  the live Aurekai files; `sources.json` keeps their digests.
- `integrations/mcp/hypervisor_catalog.mjs` (829 lines, one literal) now
  imports `hypervisor_catalog.json`. The 33 tool schemas serialize identically
  and the MCP suite is unchanged at 659 pass, 12 fail, 1 error.
- Counted figures: 1,734 to 1,714 files and 174,761 to 170,272 lines.

Not retired: code that only its own tests reach (media clip review, the
experience hand-off and handoff-fabric libraries, `scripts/research`, the
SurfaceScene reusable renderers and the ForeignTwin fabric CLI). A test is
a reference, and several carry atlas entries. Each needs a keep-or-retire
decision. The browser extension's `recurring-commerce.mjs` and `contents.mjs`
are tested byte-for-byte projections of `scripts/recurring-commerce/` and are
counted twice.

### 2026-10-27/29, test-only clusters and W4 vectors

Keep-or-retire by maturity (Nick: "whatever is most mature and hard win").
Retired with their own tests, each restore-verified by `retire.sh`:

| Archive | Lines | Why |
|---|---|---|
| `media-clip-review-20261027` | 738 | orchestrator already retired; no atlas entry or caller; tests 9 pass, 1 fail |
| `foreign-twin-fabric-20261027` | 981 | no atlas entry, launchd, MCP or script caller; tests 15 pass |
| `research-consequence-prototype-20261027` | 589 | self-described unvalidated prototype (`docs/experiences/sear-consequence-study.md`); tests 28 pass |
| `experience-handoff-protocol-20261027` | 635 | generic handoff protocol and `handoff-fabric`, no caller outside the cluster; tests 4 pass |

Kept: experience-handoffs `contract.mjs`, `artifacts.mjs`, `source-review-owner.mjs`
and `handoff-fabric/native-owner.mjs` (the Cloudflare admission path; 23 tests
still pass); SurfaceScene renderers and authored-* views (`REUSE.md` names them
as the reuse path and they ship in public PR 50); ExternalEstate, SourceFrontier,
CMS and SurfaceScene `compile_*` tools (atlas-declared producers of committed
data); `architecture/check-*` (measured, operator checks).

W4: `tests/vectors/bf_vectors.c` is one C runner over libbonfyre's `bf_jv` for
`bonfyre.command-vectors.v1` files at `ORGAN/tests/vectors/*.json`
(`make -C tests/vectors check` replays them all). Converted and retired:

| Suite | Bun before | Vectors after | Lines retired |
|---|---|---|---|
| BonfyreExperienceMap native_definition, native_resolution, policy_compilation | 20 pass | 20 cases, 1,659 checks | 210 |
| BonfyreFinance zero_close, payment_options, supplier_quote_memo, relationship_states | 4 pass | 4 cases, 110 checks | 240 |

Both organ Makefiles and the atlas witnesses now name the vector files. Not
converted: suites that build fault-injection binaries (BonfyreHash), race two
writers or grep C source (BonfyreCMS, whose source was being edited), read live
stores, or select rows out of JSONL output (components/BonfyreProject).
Ratchet tightened to 1,653 files and 167,492 lines (concurrent work included).

Earlier recorded ratchet: 1780 -> 1778 `.mjs` files, 182,482 -> 176,351 lines, compile sites
1 -> 0, JS-runtime programs 3 -> 0, largest installed program 63.6 -> 13.8 MB.

### Needs Nick

- The classifier blocked archiving cold release trees (irreversible local
  removal). 61 trees under `~/.bonfyre/releases` (1.04 GB) are referenced by no
  process, launchd plist, symlink, deploy or script; 9 live ones are excluded.
  To run it (list: `releases-cold-20261022.txt` beside this plan):
  `cd ~/.bonfyre/releases && tr '\n' '\0' < ~/Developer/Bonfyre/docs/engineering/mjs-retirement-20261022/releases-cold-20261022.txt | RETIRE_FROM=$PWD xargs -0 sh ~/Developer/Bonfyre/scripts/retire.sh bonfyre-releases-cold-20261022 "cold release trees"`.
- Live MCP services still run the Bun servers (9 processes, ~270 MB RSS);
  cutting them over to C is outward-facing and waits on W2 plus your go.

### Next

1. WX: multi-call `bonfyre` binary over libbonfyre (largest program now 13.8 MB).
2. W1: the 188 wrapper files, by YAFF command contracts.
3. W2: C MCP host; stores first (one schema per store, one C organ).
4. W5: DecisionPlane core (`decision-plane.mjs`, `typed-decision.mjs`,
   `local-inference.mjs`) has three remaining JS callers (ExternalEstate
   decision program plane, MCP recursive overlay, ContextCut); give them the
   C plane through `bonfyre-decide` and retire the JS.

## Before any deletion

1. Caller census. `architecture/check-organ-callers.mjs` and a grep of
   `package.json` scripts, launchd plists, `.githooks` and Workers for each file.
   Nothing is deleted while something still imports or runs it.
2. Atlas. Each wave that builds a native owner updates that owner's `.yaff`
   entry with a real witness, and adds a `forbidden_inference`: a passing vector
   set says the native code matches the JS on recorded inputs, not that either is
   correct.
3. Run `bun architecture/atlas.mjs validate` and the control-plane tests.

## Decisions (Nick, 2026-10-22)

- MCP transport: whichever is faster. That means a C server; the JS gateway
  and adapter go in W2 rather than staying as a shim.
- `~/Downloads` bundles: archived after triage.
- 70% with honest owners is acceptable if W5 is mostly unowned logic.
- Added scope: retire the JS and also hunt waste hard. Keep every behaviour
  that exists today, and the functionality the native hand-off targets
  (`~/Downloads/aurekai-skills-to-native-demos/native-handoff-section.md`),
  but build it from a small set of shared native primitives instead of
  per-demo code.

## Waste already measured

| Finding | Number | Wave |
|---|---|---|
| `bonfyre-books`, `-monitor`, `-manifest`, `-operator` in `~/.bonfyre/bin` are `bun build --compile` outputs (`components/BonfyreDecision/build-products.sh`), each carrying the whole Bun runtime | 4 files, 253 MB | WZ |
| The other 159 binaries are real C | 159 files, 32 MB, 200 KB average | baseline |
| So 4 JS "organs" weigh 8× all real C organs together | | |
| 194 separate `main.c` programs, each built alone with `-O2`, no LTO, no shared multi-call binary | 159 installed | WX |
| `~/.bonfyre/releases` snapshots | 1.5 GB | W0 |
| `~/.bonfyre/workspace-packages`, seven copies of one 144-file tree | 290 MB | W0 |
| `~/.bonfyre` JSON files | 62k files, 55 over 1 MB | WX |
| Eight MCP stores, each with its own SQLite open, schema and JSON side files | 8 | W2 |
| Checked-in `dist/`/`public/` copies of the same modules (`planner-model.mjs` ×8, `catalogue-*` ×4, `creator-*` ×4) | 224 files | W0 |

## WZ, fake natives first (days)

`bonfyre-books`, `bonfyre-monitor` and `bonfyre-manifest` are 40–104 line JS
entries over `components/BonfyreDecision`, with the CUE policy baked in at
compile time. Rewrite them as C over the same `decision-plane.cue` export
(embed it as a generated header, not a runtime string), and do the same for
`bonfyre-operator`. Target: under 1 MB each, down from 63 MB. Add a check that
fails the build when anything in `~/.bonfyre/bin` contains a JS runtime.

## WX, efficiency rules for every port

These apply to each wave, not as a separate phase. Each port records its
numbers before and after in `inventory.tsv` (binary bytes, RSS, cold start,
bytes on disk per record).

1. **One binary, many organs.** Build a single multi-call `bonfyre` binary
   (busybox style: `argv[0]` or first argument selects the organ) over
   `libbonfyre` and `engine/core`, with `-Os -flto -ffunction-sections
   -Wl,-dead_strip`. Keep per-organ symlinks so existing callers and
   `native_tool_invoke` still resolve. Shared code links once instead of 159
   times.
2. **Declare, then generate.** Argv shapes, store schemas, tool schemas,
   lifecycle states and policy live in YAFF and are constrained in CUE. C
   structs, parsers, validators, SQLite DDL and MCP tool JSON are generated from
   them at build time. No hand-written parser for a shape that is declared.
3. **No JSON at rest.** JSON stays at the edges (HTTP, MCP, browser). Inside,
   state is SQLite with `STRICT` tables or Arrow (`BonfyreArrowStream`
   exists). JSON side files are migrated in when their store moves.
4. **Compress by default, with trained dictionaries.** Cold data: zstd with a
   dictionary trained per record family (`BonfyreCompress`, `BonfyreMFADict`
   exist; measure first). Content-addressed blobs deduplicate across releases
   and workspaces (`BonfyreHash`). Wire: compressed responses, and
   `BonfyreWire`/`BonfyreEnvelope` for binary framing between organs.
5. **No process per call where a loaded process will do.** A long-lived
   organ host takes requests over a Unix socket; spawning a binary per MCP
   call is the fallback, not the norm.
6. **Browser gets only what it renders.** One shared browser runtime;
   compute kernels as C compiled to WASM; pages carry data, not per-page
   copies of model code.
7. **Waste found is waste recorded.** Every finding goes in the table above
   with a number and a wave, so the hunt is visible and nothing is claimed
   saved without a measurement.

## Shared primitives the demos must collapse onto

The hand-off's sections I and II say the same thing this plan does from the
product side: stop building per demo, target over 80% shared infrastructure,
and forbid bespoke auth, provider connection, revisions, activity feeds,
inboxes, importers, tables, approvals, timelines, search, sharing, bulk
actions, agent panels and setup wizards. Each of those becomes one native
primitive, declared in YAFF, with one browser renderer. Candidate owners from
`cmd/` (to confirm against the atlas before building):

| Primitive (hand-off item) | Likely owner |
|---|---|
| Object registry, ownership (2, 13) | `BonfyreEntity`, `BonfyreGraph` |
| Continuation and reverse continuation (3, 4), dependency propagation (7) | `BonfyreGraph`, `BonfyreExperienceMap` |
| Lifecycle vocabulary and state machines (11, 12) | `BonfyreLifecycle`, `engine/core` `bf_workgraph` |
| Transaction envelope (6) | `BonfyreEnvelope`, `BonfyreLedger` |
| Revisions, tombstones, attribution (10, 25) | `BonfyreLedger` |
| Inbox, assignments, review queue (17, 27, 30) | `BonfyreQueue`, `BonfyreReview` |
| Deadlines, timelines, calendar (18, 31) | `BonfyreTime` |
| Labels and taxonomy (19) | `BonfyreTag` |
| Search (32) | `BonfyreIndex`, static semantic search |
| Importers (28) | `BonfyreIngest` |
| Auth, sharing, provider connection (23, 24, 33) | `BonfyreAuth`, `BonfyreCredentials`, `BonfyreProvider` |
| Attachments (15) | `BonfyreHash` content-addressed store |
| Object actions API (20) | generated from YAFF, served by the C MCP server |

W5 triage uses this table: a JS file that implements one of these rows is
retired onto the primitive, not ported as-is. That is where most of W5's 46k
lines should go.

## Order

WZ, W0 and WX's multi-call binary first; they are cheap and they set the
measuring habit. Then W1 and W3, then W2 (C MCP server and stores), with W4
vectors captured before each cut. W5 runs as the primitives land.
