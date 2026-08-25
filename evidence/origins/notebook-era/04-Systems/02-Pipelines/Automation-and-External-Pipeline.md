# Automation and External Pipeline

## Summary
This system connects the Obsidian vault to external automation tools, AI services, APIs, and local utilities while keeping the vault as the central source of truth.

The goal is to create a lightweight personal operating system that can:
- ingest information
- structure information
- decide what should happen next
- trigger external workflows
- write results back into the vault
- preserve history and state over time

This is not meant to be a bloated app stack. It is meant to be a modular execution layer built around simple files, explicit state, and low-overhead automation.

---

## Core Principle

Obsidian = source of truth  
n8n = orchestration layer  
AI = transformation layer  
APIs = enrichment and action layer  
Local tools = low-cost execution layer  

The vault stores:
- intent
- notes
- state
- plans
- logs
- outputs
- history

The external pipeline handles:
- automation
- parsing
- enrichment
- routing
- notifications
- generation
- synchronization

---

## System Goals

- reduce friction between idea and execution
- turn notes into machine-readable state
- keep workflows inspectable
- avoid platform dependence
- preserve human control
- prefer local and low-cost execution when possible

---

## Tooling Stack

### 1. Vault Layer
Primary system of record.

Folders:
- [[00-Inbox]]
- [[01-Ideas]]
- [[02-Projects]]
- [[03-Research]]
- [[04-Systems]]
- [[05-Monetization]]
- [[06-Logs]]

The vault contains:
- raw capture
- structured plans
- task state
- research
- workflow definitions
- monetization experiments
- automation outputs

---

### 2. Orchestration Layer
Primary candidates:
- n8n
- n8n alternatives
- cron
- Apple Shortcuts
- Make-style workflow tools
- shell or Python schedulers

Purpose:
- watch for events
- route work
- call AI
- call APIs
- update downstream systems
- write outputs back to vault

n8n is especially useful for:
- webhook triggers
- scheduled workflows
- conditional routing
- API chaining
- notifications
- light ETL
- workflow visibility

---

### 3. AI Layer
Primary candidates:
- OpenAI
- local LLMs
- whisper
- llama.cpp
- embeddings tools
- lightweight classification/summarization models

Purpose:
- classify notes
- extract structure
- summarize research
- generate checklists
- detect monetizable assets
- propose plans
- convert vague intent into operational state

Use cloud AI for:
- stronger reasoning
- complex transformations
- enrichment
- hard classification tasks

Use local AI for:
- privacy-sensitive data
- cheap repetitive processing
- transcription
- lightweight summarization
- fallback execution

---

### 4. API Layer
Use free and low-cost APIs as enrichment and action tools.

Examples:
- recipes / nutrition APIs
- weather APIs
- maps / places APIs
- market or finance APIs
- calendar APIs
- email APIs
- RSS / feed APIs
- scraping endpoints
- text extraction APIs
- OCR when truly needed
- search APIs
- task manager APIs
- payment APIs
- messaging APIs

Purpose:
- enrich notes
- validate information
- trigger external actions
- fetch structured data
- connect planning to the real world

---

### 5. Local Execution Layer
Local tools run cheap, close to the vault.

Examples:
- Python scripts
- Node scripts
- shell scripts
- sqlite
- local file watchers
- ffmpeg
- whisper
- local embeddings
- local vector indexes
- browser automation
- local document converters

Purpose:
- watch files
- transform markdown
- preprocess inputs
- avoid unnecessary cloud cost
- maintain resilience if external services fail

---

## Architectural Model

### Model A: Vault-Centered
Obsidian is the master system.

Flow:
vault note → n8n trigger → AI/API processing → result written back to vault

Best for:
- idea pipelines
- project management
- research systems
- execution systems
- monetization tracking

---

### Model B: Event-Centered
An external event triggers work that later updates the vault.

Examples:
- webhook form submission
- saved link
- voice note received
- email forwarded
- calendar event created

Flow:
external event → n8n workflow → processing → note created or updated in vault

Best for:
- capture systems
- intake forms
- external service pipelines
- monetization leads

---

### Model C: Scheduled Review
A recurring workflow scans the vault and creates summaries, next actions, and status updates.

Flow:
scheduled run → scan notes → identify stale or active items → generate review → write into logs or dashboard

Best for:
- weekly reviews
- project maintenance
- monetization review
- stale note detection

---

## State Model

Automation should run from explicit note state.

Recommended fields:

status:
automation:
pipeline:
priority:
created:
updated:
last_processed:
last_reviewed:
next_action:
owner:
verdict:
monetizable:
run_now:

Example state:

status: active
automation: true
pipeline: idea-to-project
priority: high
verdict: ACTIVE
last_reviewed: 2026-04-02

This makes workflows inspectable and reliable.

---

## High-Value Workflows

### 1. Inbox Structuring Workflow
Purpose:
Turn messy inputs into organized notes.

Input:
- raw notes in [[00-Inbox]]
- pasted links
- copied text
- rough ideas

Flow:
- detect new note
- classify type
- extract title
- extract tags
- assign folder
- generate summary
- write cleaned note back into vault

Useful tools:
- n8n trigger
- OpenAI classification
- local markdown writer

Output:
- new structured note in [[01-Ideas]] or [[03-Research]]
- automation log entry

---

### 2. Idea Promotion Workflow
Purpose:
Turn promising ideas into projects.

Trigger:
- verdict = ACTIVE
- promote_to_project = true
- automation = true

Flow:
- read idea note
- extract execution path
- generate project stub
- generate milestones
- generate first actions
- create note in [[02-Projects]]
- backlink to original idea

Useful tools:
- n8n or Python
- OpenAI for project decomposition
- markdown templater

Output:
- project file
- checklist
- log record

---

### 3. Research Enrichment Workflow
Purpose:
Turn saved material into usable intelligence.

Input:
- links
- copied notes
- transcripts
- documents
- saved research notes

Flow:
- summarize content
- extract opportunities
- extract risks
- identify linked ideas
- identify monetization paths
- write structured sections back

Useful tools:
- OpenAI
- text extraction API
- local parser
- n8n scheduling

Output:
- enriched note in [[03-Research]]
- links to related ideas and projects

---

### 4. Weekly Review Workflow
Purpose:
Maintain continuity and reduce drift.

Trigger:
- scheduled weekly
- or manual run_now = true

Flow:
- scan active projects
- scan stale notes
- summarize recent progress
- identify blockers
- suggest next actions
- update dashboard
- write weekly review note

Useful tools:
- n8n scheduler
- file parser
- OpenAI synthesis
- local markdown output

Output:
- weekly review note
- dashboard update
- logs

---

### 5. Monetization Detection Workflow
Purpose:
Find outputs that can become products, services, or offers.

Input:
- ideas
- systems
- research notes
- repeated workflows
- reusable templates

Flow:
- scan for repeatable assets
- identify serviceable outputs
- identify packageable knowledge
- generate offer drafts
- generate pricing hypotheses
- write note to [[05-Monetization]]

Useful tools:
- OpenAI analysis
- vault scanner
- n8n routing

Output:
- monetization note
- pricing experiments
- offer ideas

---

### 6. Personal Execution Workflow
Purpose:
Turn vague personal goals into usable plans.

Input examples:
- “I want to eat better this week”
- ingredients list
- saved recipes
- current schedule
- fitness or spending notes

Flow:
- interpret intent
- pull relevant state from vault
- enrich with external APIs if helpful
- generate plan
- create checklist
- create calendar or routine suggestions
- log plan in vault

Useful tools:
- OpenAI planning
- recipe / nutrition APIs
- calendar APIs
- n8n
- local note writer

Output:
- plan note
- checklist
- optional grocery list
- optional reminders

This is especially important because it aligns directly with the personal execution system vision.

---

## Recommended Role for n8n

n8n should be used as the workflow fabric, not the memory system.

Use n8n for:
- monitoring folders indirectly
- webhooks
- scheduled runs
- API fan-out
- notifications
- branching logic
- lightweight queueing
- workflow observability

Do not use n8n as the long-term canonical store for:
- plans
- state history
- project truth
- core notes

That belongs in the vault.

---

## Recommended Role for OpenAI

Use OpenAI when you need:
- note classification
- summarization
- extraction of structure
- plan generation
- prioritization
- linking ideas
- drafting offers
- synthesis across many notes

Best use cases:
- transform messy inputs into structure
- reason over multiple note fragments
- convert intent into next actions
- create useful written outputs that are written back into the vault

Avoid using it for:
- deterministic file routing alone
- tasks simple scripts can handle
- unnecessary repeated calls on unchanged notes

---

## Recommended Role for Local Tools

Use local tools for:
- transcription
- markdown parsing
- file watching
- scheduled scans
- preprocessing
- local search
- data cleanup
- lightweight database tasks
- resilience if API access fails

Best local stack:
- Python
- sqlite
- file watcher
- whisper
- llama.cpp where useful
- ffmpeg
- shell scripts

This keeps costs low and makes the system more durable.

---

## API Strategy

Use APIs in three categories:

### 1. Enrichment APIs
Add useful context.
Examples:
- weather
- nutrition
- maps
- finance
- search
- content extraction

### 2. Action APIs
Do something in the world.
Examples:
- email
- messaging
- calendar
- payment
- form submission
- task manager updates

### 3. Verification APIs
Check assumptions or fill gaps.
Examples:
- market data
- geocoding
- metadata lookup
- event feeds

Rule:
APIs should improve execution, not become the core dependency.

---

## Workflow Patterns

### Pattern 1: Note-In, Note-Out
A note is created or changed, then a workflow writes back a better note.

This is the safest pattern.

---

### Pattern 2: Note-In, Action-Out
A note triggers an external action.

Examples:
- send reminder
- create calendar event
- send email
- launch offer draft
- update task system

Use only when note state is explicit.

---

### Pattern 3: External-In, Note-Out
External events create or update notes.

Examples:
- webhook captures form
- email parser creates inbox note
- voice memo becomes transcript note

This is ideal for expanding the system without breaking the vault-centered model.

---

## Minimal Useful External Pipeline

A very good first version is:

1. Obsidian vault
2. Python parser / writer
3. n8n for orchestration
4. OpenAI for transformation
5. sqlite for lightweight structured state
6. simple APIs for enrichment
7. logs written back to vault

That is enough to build a real personal operating system.

---

## Good Early Integrations

### 1. Saved Link → Research Note
Paste link somewhere → n8n extracts content → AI summarizes → note appears in [[03-Research]]

### 2. Voice Memo → Structured Note
Audio file → local whisper → AI cleanup → note in [[00-Inbox]] or [[03-Research]]

### 3. Idea Marked ACTIVE → Project Created
Change field in idea note → workflow creates project and next actions

### 4. Weekly Scan → Dashboard Refresh
Scheduled workflow scans vault and updates current focus note

### 5. Monetizable Note → Offer Draft
A reusable process note gets turned into a service or product draft in [[05-Monetization]]

---

## Guardrails

- never silently overwrite important notes
- always log automation events
- make note state explicit
- avoid hidden magic
- prefer append-only or versioned changes
- keep workflows modular
- preserve manual override
- keep the vault readable without automation

---

## Folder Recommendation

Create:

[[04-Systems/Automation-Specs]]

Suggested specs:
- [[Inbox Processing Spec]]
- [[Idea to Project Spec]]
- [[Research Enrichment Spec]]
- [[Weekly Review Spec]]
- [[Monetization Detection Spec]]
- [[External Integrations]]
- [[04-Systems/04-Meta/n8n Workflow Map]]
- [[AI Prompt Library]]

---

## Recommended First Build Order

### Phase 1
- standardize note fields
- create one parser
- create one logger
- create one n8n workflow

### Phase 2
- Inbox Processing
- Weekly Review

### Phase 3
- Idea → Project
- Research Enrichment

### Phase 4
- Monetization Detection
- Personal Execution workflows

### Phase 5
- external forms
- notifications
- APIs
- optional dashboards

---

## Highest-Leverage First Workflows

If the goal is usefulness fast, build these first:

1. Inbox Processing
2. Weekly Review
3. Idea → Project
4. Saved Link → Research Note
5. Voice Memo → Structured Note

If the goal is money fast, build these first:

1. Monetization Detection
2. Idea → Project
3. Offer Draft Generator
4. Lead / inquiry intake
5. Execution checklist generator

---

## Meta Insight

This system gets powerful when each layer does one thing well:

- vault remembers
- n8n routes
- AI interprets
- APIs enrich
- local tools execute cheaply
- logs preserve continuity

The result is not just automation.

It is a personal infrastructure layer that turns:
- vague intent into structure
- structure into action
- action into outputs
- outputs into leverage

---

## Verdict
CORE SYSTEM