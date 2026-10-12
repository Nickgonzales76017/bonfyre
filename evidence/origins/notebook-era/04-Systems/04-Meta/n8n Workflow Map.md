# n8n Workflow Map

## Purpose
Map the external automation workflows that should eventually connect back into the vault.

## Design Rule
The vault stays the source of truth. n8n should orchestrate, transform, enrich, or route, then write useful outputs back into notes.

## Priority Workflows

### 1. Inbox Processing
Trigger:
- new file or note appears in `00-Inbox`

Flow:
1. detect new item
2. extract text or metadata
3. classify as idea, research, task, or raw capture
4. create or update the correct note type

Vault destination:
- `01-Ideas`
- `03-Research`
- `06-Logs`

### 2. Research Enrichment
Trigger:
- a research note is marked for enrichment

Flow:
1. gather source links or inputs
2. summarize and structure findings
3. append implications and next actions

Vault destination:
- `03-Research`

### 3. Monetization Detection
Trigger:
- a project or system reaches a useful output stage

Flow:
1. scan note for deliverables and outcomes
2. propose offer angles
3. create draft monetization note

Vault destination:
- `05-Monetization`

### 4. Daily Review Helper
Trigger:
- end of day or manual run

Flow:
1. collect changed project and system notes
2. summarize what moved
3. append prompts to the daily log

Vault destination:
- `06-Logs`

### 5. Audio Intake Pipeline
Trigger:
- audio file added to intake folder

Flow:
1. send file to local transcription process
2. collect transcript
3. optionally run summary step
4. write structured output note

Vault destination:
- `00-Inbox`
- `03-Research`
- service-specific project folders later

## Readiness Order
1. daily review helper
2. audio intake pipeline
3. research enrichment
4. monetization detection
5. inbox processing

## Guardrails
- do not automate before the manual path is understood
- every workflow needs a visible output note
- every workflow should fail safely and keep raw inputs
- prefer simple file triggers before complex app integrations
