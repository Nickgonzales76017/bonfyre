# Nightly Brainstorm

**Autonomous batch cognition over your Obsidian vault.**

Not "chat with my vault." Instead: nightly specialized AI passes that expand ideas, review projects, wire systems, generate coding-agent briefs, and deliver a morning digest — all while you sleep.

Runtime guardrails now protect the MacBook:
- nightly refuses to start if another heavy Bonfyre process already holds the shared runtime lock
- nightly refuses to start if the machine's current 1-minute load average exceeds the configured safety limit
- launchd runs it at lower priority with `Nice=10` and `ProcessType=Background`
- heavy model and log folders are marked with `.metadata_never_index` so Spotlight does not waste cycles indexing them

```
Obsidian vault (source of truth)
        ↓
Perl scans + builds context bundles (deterministic)
        ↓
llama.cpp runs specialized jobs (per-pass inference profiles)
        ↓
Outputs written back into vault as:
  • idea expansions
  • project reviews
  • system wiring suggestions
  • coding-agent task packets
  • morning digest
        ↓
You review with coffee. Approve. Dispatch.
```

---

## Architecture

```
10-Code/NightlyBrainstorm/
├── nightly.pl              ← Orchestrator: runs all passes in sequence
├── nightly.json            ← Global config: model path, backend, safety caps
├── setup.sh                ← One-time install: llama.cpp + model download
├── com.bonfyre.nightly-brainstorm.plist  ← macOS launchd for 2 AM runs
│
├── lib/
│   ├── VaultParse.pm       ← Parse frontmatter, sections, wiki links
│   ├── VaultScan.pm        ← Find notes by type/status/path selectors
│   ├── ContextBundle.pm    ← Build bounded context per job recipe
│   ├── LlamaRun.pm         ← Call llama.cpp (CLI or HTTP server)
│   ├── VaultWrite.pm       ← Append sections or create new notes (never overwrites)
│   ├── PiperTTS.pm         ← Text-to-speech via Piper (4 audio passes)
│   └── NightlyQueue.pm     ← File-backed job queue for deferred pass execution
│
├── profiles/
│   ├── idea_expand.json        ← Pass 1: Deepen promising ideas
│   ├── project_review.json     ← Pass 2: Strengthen active projects
│   ├── system_wire.json        ← Pass 3: Propose system connections
│   ├── agent_brief.json        ← Pass 4: Generate coding-agent specs
│   ├── morning_digest.json     ← Pass 5: Morning review note
│   ├── morning_brief_audio.json ← Pass 6: Spoken morning brief (TTS)
│   ├── project_narrator.json   ← Pass 7: Spoken project summaries (TTS)
│   ├── idea_playback.json      ← Pass 8: Spoken idea recaps (TTS)
│   └── distribution_snippet.json ← Pass 9: Spoken distribution hooks (TTS)
│
├── models/
│   ├── mistral-7b-instruct-v0.2.Q5_K_M.gguf  ← 5.1 GB LLM model
│   └── piper/
│       ├── en_US-lessac-medium.onnx           ← Piper TTS voice model
│       └── en_US-lessac-medium.onnx.json
│
├── logs/                   ← Run logs (JSON per night)
└── t/
    └── test_pipeline.pl    ← Integration test suite (74 tests)
```

---

## Core Design Principle

**You do not want one giant general-purpose overnight AI.**

You want **multiple narrow nightly passes**, each with:

| Component | Purpose |
|---|---|
| **Role** | Who the model pretends to be |
| **Task type** | What kind of thinking it does |
| **Bounded context** | Fixed recipe of vault content — no unbounded retrieval |
| **Strict output format** | Exact markdown sections, nothing else |
| **Destination** | Which note or folder gets the output |

The complexity is **declarative** (in profile JSON files), not scattered through code.

---

## The Nine Nightly Passes

Five text passes generate written vault content. Four audio passes synthesize spoken versions via Piper TTS.

### Pass 1: Idea Expansion (`idea_expand`)

| | |
|---|---|
| **Target** | Ideas with `verdict: ACTIVE`, `status: active/exploratory`, `project_created: no` |
| **Role** | Venture architect / systems thinker |
| **Temperature** | 0.75 (moderate-high — creative but bounded) |
| **Context recipe** | Note frontmatter + Summary + Core Insight + First Use Case + Monetization + max 2 related ideas + max 3 concepts |
| **Output sections** | Why Now · Failure Modes · Cheapest Validation · One Better First Use Case · One Smaller MVP · Missing Links |
| **Writes to** | Appends `## AI Expansion — YYYY-MM-DD` to the source idea note |

### Pass 2: Project Review (`project_review`)

| | |
|---|---|
| **Target** | Projects with `status: active/planned` |
| **Role** | Execution strategist / technical PM |
| **Temperature** | 0.5 (lower — precise, actionable) |
| **Context recipe** | Frontmatter + Summary + Success Criteria + Execution + Next Action + Bottlenecks + Tooling + linked idea (frontmatter+summary) + linked system |
| **Output sections** | Refined Next Action · Blockers · Scope Cut · Smallest Testable Deliverable · Milestones to Proof · Hidden Assumptions |
| **Writes to** | Appends `## AI Project Review — YYYY-MM-DD` to the source project note |

### Pass 3: System Wire-Up (`system_wire`)

| | |
|---|---|
| **Target** | Active projects + active systems + pipelines |
| **Role** | Systems architect |
| **Temperature** | 0.6 (medium — analytical) |
| **Context recipe** | Frontmatter + Purpose + Core Mechanism + Inputs + Outputs + Flow + max 3 related systems + max 2 pipelines |
| **Output sections** | Upstream Dependencies · Downstream Consumers · Missing Connections · Pipeline Suggestion · System Promotion |
| **Writes to** | Appends `## AI Systemization — YYYY-MM-DD` to the source note |

### Pass 4: Coding Agent Briefs (`agent_brief`)

| | |
|---|---|
| **Target** | Projects with `status: active`, `stage: build/launch` |
| **Role** | Staff engineer / technical lead |
| **Temperature** | 0.35 (low — precise, unambiguous) |
| **Context recipe** | Full project body (up to 10K chars) + linked idea + linked systems (frontmatter + purpose + core mechanism) |
| **Output sections** | Brief Title · Objective · Acceptance Criteria · File Plan · Implementation Steps · Constraints · Test Criteria · Context Files · Out of Scope |
| **Writes to** | Creates `07-Agent Briefs/Brief - {title} - YYYY-MM-DD.md` |

### Pass 5: Morning Digest (`morning_digest`)

| | |
|---|---|
| **Target** | All outputs from tonight's passes |
| **Role** | Chief of staff |
| **Temperature** | 0.45 (concise, opinionated) |
| **Context recipe** | Aggregated summaries of all AI outputs from passes 1-4 |
| **Output sections** | What Changed Overnight · What Got Stronger · What to Ignore · Ready for Execution · Recommended First Move · Agent Briefs Created |
| **Writes to** | Creates `06-Logs/Morning Review — YYYY-MM-DD.md` |

### Pass 6: Morning Brief Audio (`morning_brief_audio`)

| | |
|---|---|
| **Target** | Morning digest output from pass 5 |
| **Role** | Audio narrator |
| **Action** | Synthesize spoken .wav of the morning brief via Piper TTS |
| **Writes to** | `07-Audio/morning-brief/Morning Brief — YYYY-MM-DD.wav` |

### Pass 7: Project Narrator (`project_narrator`)

| | |
|---|---|
| **Target** | Active project review outputs from pass 2 |
| **Role** | Audio narrator |
| **Action** | Synthesize spoken .wav summaries of project reviews |
| **Writes to** | `07-Audio/project-narration/{project} — YYYY-MM-DD.wav` |

### Pass 8: Idea Playback (`idea_playback`)

| | |
|---|---|
| **Target** | Idea expansion outputs from pass 1 |
| **Role** | Audio narrator |
| **Action** | Synthesize spoken .wav recaps of expanded ideas |
| **Writes to** | `07-Audio/idea-playback/{idea} — YYYY-MM-DD.wav` |

### Pass 9: Distribution Snippet (`distribution_snippet`)

| | |
|---|---|
| **Target** | Best overnight outputs (ideas or projects with strong hooks) |
| **Role** | Audio narrator |
| **Action** | Synthesize short spoken hooks for distribution |
| **Writes to** | `07-Audio/distribution/{slug} — YYYY-MM-DD.wav` |

---

## Profile Anatomy

Every profile is a self-contained JSON file in `profiles/`. Here's the structure:

```json
{
  "name": "idea_expand",
  "description": "What this pass does",

  "selector": {
    "type": "idea",
    "status": ["active", "exploratory"],
    "verdict": ["ACTIVE"],
    "paths": ["01-Ideas/"]
  },

  "context_recipe": {
    "include_sections": ["Summary", "Core Insight", ...],
    "max_related_ideas": 2,
    "max_related_concepts": 3,
    "related_depth": "frontmatter+summary",
    "include_frontmatter": true,
    "max_context_chars": 6000
  },

  "prompt": {
    "system": "You are a venture architect...",
    "user_template": "Here is the note...\n{{frontmatter}}\n{{body}}\n{{#if related_notes}}...",
    "output_contract": "Return EXACTLY these sections:\n\n## Why Now\n..."
  },

  "inference": {
    "temperature": 0.75,
    "top_p": 0.9,
    "min_p": 0.05,
    "repeat_penalty": 1.1,
    "num_predict": 1024
  },

  "output": {
    "mode": "append",
    "heading": "## AI Expansion — {{date}}",
    "destination": "source"
  }
}
```

### Key design decisions:

- **Selector** determines which notes this pass runs on
- **Context recipe** bounds what the model sees — hardcoded per job, no unbounded retrieval
- **Prompt** has three parts: system role, user task with template variables, output contract
- **Inference** lets each pass have its own personality (creative vs precise)
- **Output** controls where results go (append to source or create new file)

---

## Parameter Strategy

This matters more than most people realize.

| Job Type | Temperature | Sampling | Why |
|---|---|---|---|
| **Brainstorm / expansion** | 0.7-0.8 | Broad (top_p 0.9) | Creative but bounded by output sections |
| **Planning / decomposition** | 0.4-0.55 | Tighter (top_p 0.85) | Precise, actionable, no fluff |
| **Review / audit** | 0.55-0.65 | Medium | Critical but not robotic |
| **Technical specs** | 0.3-0.4 | Tight (top_p 0.8) | Unambiguous, testable |
| **Digest / summary** | 0.4-0.5 | Medium | Concise, opinionated |

---

## Context Recipes

Each pass gets a **fixed context recipe**. This is the most important safety mechanism.

### Why fixed recipes?

- Zero human interaction overnight
- No uncontrolled retrieval
- No recursive expansion
- Predictable prompt sizes
- Deterministic behavior

### Recipe structure:

```
Idea expansion:
  ├── Source note frontmatter
  ├── Source note: Summary + Core Insight + First Use Case + Monetization
  ├── Max 2 related ideas (frontmatter + summary only)
  └── Max 3 related concepts (frontmatter + summary only)
  Total: ≤ 6000 chars

Project review:
  ├── Source note frontmatter
  ├── Source note: Summary + Success Criteria + Execution + Next Action + Tooling
  ├── Linked idea (frontmatter + summary)
  └── Linked system (frontmatter + summary)
  Total: ≤ 8000 chars

System wire-up:
  ├── Source note frontmatter
  ├── Source note: Purpose + Core Mechanism + Inputs + Outputs + Flow
  ├── Max 3 related systems (frontmatter + purpose + inputs + outputs)
  └── Max 2 related pipelines (same)
  Total: ≤ 7000 chars
```

---

## Safety Rules

The model **never** decides:

- Which folders to write into freely
- Whether to overwrite source notes
- Whether to create dozens of notes
- Whether to recursively expand forever

The **script** decides:

- Input files (via selector)
- Allowed output locations (via profile output config)
- Allowed output sections (via output contract)
- Max files per run (via `nightly.json` safety caps)

### Built-in safety:

| Rule | Implementation |
|---|---|
| **Never overwrite** | `VaultWrite::create_note()` refuses to write if file exists |
| **Never duplicate** | `VaultWrite::append_to_note()` checks for heading before appending |
| **Bounded context** | `ContextBundle` hard-truncates at `max_context_chars` |
| **Max notes per pass** | `--limit` flag + `safety.max_notes_per_pass` in config |
| **Append-only** | AI outputs are appended as dated sections, never replacing content |
| **Run logging** | Every run writes a JSON log to `logs/run-YYYY-MM-DD.json` |

---

## Installation

### Prerequisites

- macOS with Apple Silicon (M1/M2/M3)
- Homebrew
- Perl 5.34+ (ships with macOS)

### Quick start

```bash
cd /Users/nickgonzales/Documents/Bonfyre/10-Code/NightlyBrainstorm
chmod +x setup.sh
./setup.sh
```

This will:
1. Install `llama.cpp` via Homebrew (Metal-optimized)
2. Download Mistral 7B Instruct Q5_K_M (~5.1 GB)
3. Update `nightly.json` with the model path
4. Run a dry-run test

### Manual model selection

If you prefer a different model, place any `.gguf` file in `models/` and update `nightly.json`:

```json
{
  "model_path": "/path/to/your/model.gguf",
  "chat_template": "chatml"
}
```

Chat template options: `chatml`, `llama3`, `mistral`, `simple`

### Recommended models for M3 16GB

| Model | Size | Template | Best For |
|---|---|---|---|
| Mistral 7B Instruct Q5_K_M | 5.1 GB | `mistral` | General purpose, good balance |
| Llama 3 8B Instruct Q5_K_M | 5.7 GB | `llama3` | Best overall quality |
| Phi-3 Mini Q5_K_M | 2.8 GB | `chatml` | Faster, lighter |
| Qwen2.5 7B Instruct Q5_K_M | 5.0 GB | `chatml` | Strong reasoning |

---

## Usage

### Dry run (no model needed)

```bash
perl nightly.pl --dry-run --verbose
```

Generates placeholder outputs to verify:
- Note selection works
- Context bundles build correctly
- Output paths are correct
- No file conflicts

### Run one pass

```bash
perl nightly.pl --pass idea_expand --limit 3
perl nightly.pl --pass project_review --verbose
perl nightly.pl --pass agent_brief
```

### Run all passes

```bash
perl nightly.pl
```

### Queue passes for later

When the machine is busy, enqueue passes to run when load drops:

```bash
perl nightly.pl --enqueue-pass idea_expand --limit 2
perl nightly.pl --enqueue-pass project_review
perl nightly.pl --queue-status
perl nightly.pl --process-queued --max-queued-jobs 3 --no-audio
```

Queued jobs are stored in `.bonfyre-runtime/nightly-brainstorm-queue.json` and can be drained manually or by the unified dispatcher.

### Unified Queue Dispatcher

Both NightlyBrainstorm and LocalAITranscriptionService share a unified dispatcher at `.bonfyre-runtime/drain_all_queues.pl`. It:

1. Acquires its own `dispatcher.lock` to prevent overlapping drain cycles
2. Checks both queue files for pending work
3. Drains transcription first (customer-facing), then nightly brainstorm
4. Each child acquires `heavy-process.lock` independently
5. Re-checks load between services — if transcription spiked CPU, nightly is deferred

```bash
# Check both queues
perl .bonfyre-runtime/drain_all_queues.pl --status

# Dry run
perl .bonfyre-runtime/drain_all_queues.pl --dry-run --verbose

# Real drain
perl .bonfyre-runtime/drain_all_queues.pl --max-jobs 4
```

A single launchd plist (`com.bonfyre.drain-all-queues.plist`) replaces the per-project queue plists.

### CLI options

| Flag | Effect |
|---|---|
| `--pass NAME` | Run only this pass (default: all 9) |
| `--dry-run` | Generate placeholder outputs, skip inference |
| `--verbose` | Print context sizes, prompt lengths |
| `--limit N` | Max notes per pass |
| `--no-audio` | Skip TTS audio synthesis passes |
| `--config PATH` | Use alternate config file |
| `--enqueue-pass NAME` | Queue a pass for later execution |
| `--queue-status` | Print queue state as JSON |
| `--process-queued` | Drain queued passes (respects guardrails) |
| `--max-queued-jobs N` | Max jobs per drain cycle (default: 3) |
| `--unsafe-skip-guardrails` | Bypass load average checks |

---

## Scheduling

### macOS launchd (recommended)

```bash
# Install the launch agent
cp com.bonfyre.nightly-brainstorm.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.bonfyre.nightly-brainstorm.plist
```

Runs at 2:00 AM every night. Logs to `logs/launchd-stdout.log`.

To unload:
```bash
launchctl unload ~/Library/LaunchAgents/com.bonfyre.nightly-brainstorm.plist
```

### crontab alternative

```bash
crontab -e
# Add:
0 2 * * * cd /Users/nickgonzales/Documents/Bonfyre/10-Code/NightlyBrainstorm && /usr/bin/perl nightly.pl >> logs/cron.log 2>&1
```

---

## Using llama-server Instead of CLI

For faster repeated inference (avoids model reload per call), run the server:

```bash
# Start server (background)
llama-server -m models/mistral-7b-instruct-v0.2.Q5_K_M.gguf -c 4096 -ngl 99 --port 8080

# Update config
# In nightly.json, set:
#   "backend": "server"
#   "server_url": "http://127.0.0.1:8080"

# Then run normally
perl nightly.pl
```

The server backend uses the `/v1/chat/completions` API, which handles chat templates automatically.

---

## Output Strategy

AI outputs are **never** written in place. Everything is either appended or created new.

### Append outputs (passes 1-3)

```markdown
## AI Expansion — 2026-04-03

## Why Now
(AI-generated content...)

## Failure Modes
- (AI-generated content...)
```

Appended to the end of the source note. Uses the exact heading from the profile's `output.heading`.

### Created outputs (passes 4-5)

Agent briefs → `07-Agent Briefs/Brief - {Title} - YYYY-MM-DD.md`
Morning digest → `06-Logs/Morning Review — YYYY-MM-DD.md`

Created with proper frontmatter. Will not overwrite if a file with the same name exists.

### Auditing

Every run produces:
- `logs/run-YYYY-MM-DD.json` — which notes were processed, pass name, output length, errors
- Console output with timestamps and note-by-note progress
- Dry-run mode for testing without touching the vault

---

## Perl Module Reference

### VaultParse.pm

```perl
use VaultParse qw(parse_note parse_frontmatter extract_sections extract_wiki_links section_text);

my $note = parse_note($filepath);
# → { path, filename, frontmatter, body, sections, wiki_links }

my $fm = parse_frontmatter(\$raw_text);
# → { type => 'idea', status => 'active', tags => ['idea', 'ai'], ... }

my $sections = extract_sections($body);
# → [ { level => 2, heading => 'Summary', text => '...' }, ... ]

my $links = extract_wiki_links($body);
# → [ '01-Ideas/Foo', 'Bar', 'Baz' ]

my $text = section_text($note, 'Summary', 'Objective', 'Purpose');
# → Concatenated text of all matching sections
```

### VaultScan.pm

```perl
use VaultScan qw(scan_vault find_notes find_notes_by_profile);

my $all = scan_vault($vault_root, '01-Ideas', '02-Projects');
# → arrayref of parsed notes

my $ideas = find_notes($vault_root, {
    type   => 'idea',
    status => ['active', 'exploratory'],
    paths  => ['01-Ideas/'],
});
# → filtered arrayref

my $notes = find_notes_by_profile($vault_root, $profile);
# → uses profile's selector config
```

### ContextBundle.pm

```perl
use ContextBundle qw(build_context build_digest_context);

my $ctx = build_context($note, $profile, $vault_root, $all_notes);
# → { frontmatter_yaml, body_text, related_notes_text }

my $digest = build_digest_context(\@run_log);
# → formatted string for morning digest prompt
```

### LlamaRun.pm

```perl
use LlamaRun qw(run_inference build_prompt load_config);

my $config = load_config('nightly.json');
my $prompt = build_prompt($profile, $context);
# → { system, user, output_contract }

my $output = run_inference($config, $profile, $prompt);
# → AI-generated text (cleaned)
```

### VaultWrite.pm

```perl
use VaultWrite qw(write_output append_to_note create_note);

write_output($profile, $note, $ai_output, $vault_root);
# Routes to append or create based on profile config

append_to_note($filepath, '## AI Expansion — 2026-04-03', $content);
# Appends. Prevents duplicates.

create_note($filepath, \%frontmatter, $body);
# Creates. Refuses to overwrite.
```

---

## Creating Custom Passes

1. Create a new profile in `profiles/your_pass.json`
2. Define: selector, context_recipe, prompt, inference, output
3. Run: `perl nightly.pl --pass your_pass --dry-run`

Example: Bottleneck detection pass

```json
{
  "name": "bottleneck_detect",
  "selector": {
    "type": "project",
    "status": ["active"],
    "paths": ["02-Projects/"]
  },
  "context_recipe": {
    "include_sections": ["Bottlenecks", "Next Action", "Execution", "Constraints"],
    "max_context_chars": 5000
  },
  "prompt": {
    "system": "You are a ruthless efficiency consultant. Find what is actually blocking progress.",
    "user_template": "Project: {{frontmatter}}\n\n{{body}}",
    "output_contract": "## Actual Bottleneck\n(The one thing blocking progress...)\n\n## Why It Persists\n...\n\n## Break-Through Move\n..."
  },
  "inference": { "temperature": 0.5, "top_p": 0.85, "num_predict": 512 },
  "output": { "mode": "append", "heading": "## AI Bottleneck Detection — {{date}}", "destination": "source" }
}
```

Then add it to `nightly.json` → `pass_order` array, or run standalone:
```bash
perl nightly.pl --pass bottleneck_detect
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `model_path required` | Set `model_path` in `nightly.json` to your .gguf file |
| `llama-cli failed` | Verify: `which llama-cli` and `llama-cli --version` |
| No notes selected | Check that your vault notes have matching frontmatter (type, status, etc.) |
| Duplicate sections | VaultWrite prevents this — check the heading text matches exactly |
| Wide character warning | Harmless — UTF-8 in note titles/paths |
| Context too large | Reduce `max_context_chars` in the profile's context_recipe |
| Server connection refused | Start: `llama-server -m model.gguf --port 8080` |

---

## Test Suite

```bash
perl t/test_pipeline.pl
```

Tests all 5 modules against the real vault:
- VaultParse: frontmatter parsing, section extraction, wiki links, edge cases
- VaultScan: vault scanning, selector filtering
- ContextBundle: context building, size bounds, digest formatting
- LlamaRun: prompt assembly, template substitution
- VaultWrite: append safety, create safety, duplicate prevention, overwrite protection

46 tests, all passing.

---

## File Manifest

| File | Lines | Purpose |
|---|---|---|
| `nightly.pl` | ~295 | Main orchestrator |
| `nightly.json` | ~30 | Global config |
| `setup.sh` | ~100 | Install script |
| `lib/VaultParse.pm` | ~170 | Markdown + YAML parser |
| `lib/VaultScan.pm` | ~120 | Note finder + selector engine |
| `lib/ContextBundle.pm` | ~220 | Bounded context builder |
| `lib/LlamaRun.pm` | ~230 | llama.cpp interface (CLI + server) |
| `lib/VaultWrite.pm` | ~130 | Safe vault writer |
| `profiles/idea_expand.json` | ~65 | Idea expansion pass config |
| `profiles/project_review.json` | ~65 | Project review pass config |
| `profiles/system_wire.json` | ~60 | System wire-up pass config |
| `profiles/agent_brief.json` | ~70 | Agent brief generator config |
| `profiles/morning_digest.json` | ~55 | Morning digest config |
| `t/test_pipeline.pl` | ~200 | Integration test suite |
| `com.bonfyre.nightly-brainstorm.plist` | ~30 | macOS launchd schedule |

---

## The Philosophy

> Simple routing, advanced prompts.

The Perl side stays boring:
- Select notes
- Parse frontmatter
- Build bundle
- Choose prompt preset
- Call llama.cpp
- Write result

The intelligence lives in:
- Prompt profiles
- Note typing
- Output contracts

Not in:
- Agent loops
- Embeddings
- Recursive search
- Autonomous browsing
