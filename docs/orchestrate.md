# bonfyre-orchestrate

`bonfyre-orchestrate` is a machine-only planner for Bonfyre. It is not a chat UI and it does not expose human prompting to the end user.

## Goals

- Keep Bonfyre fully usable without a model
- Add an optional planner that can choose higher-leverage Bonfyre blocks automatically
- Use strict structured JSON and a hidden system contract
- Respect latency so the orchestrator boosts the system instead of slowing it down

## Why Gemma 4

Google introduced Gemma 4 on April 2, 2026 as a new open model family aimed at advanced reasoning and agentic workflows, with native function-calling, structured JSON output, and system instructions.

Sources:

- https://blog.google/innovation-and-ai/technology/developers-tools/gemma-4/
- https://huggingface.co/google/gemma-4-E4B

## Runtime model

Bonfyre uses three layers here:

1. Deterministic Bonfyre baseline
2. `bonfyre-orchestrate` heuristic planner
3. Optional Gemma 4 assist over an OpenAI-compatible endpoint

If no endpoint is configured, `bonfyre-orchestrate` still produces a valid boost plan from Bonfyre's operator registry and the request contract.

## Commands

```bash
bonfyre-orchestrate status
bonfyre-orchestrate surfaces --json
bonfyre-orchestrate template audio
bonfyre-orchestrate plan request.json
```

## Environment

```bash
export BONFYRE_ORCHESTRATE_ENDPOINT=http://127.0.0.1:8000/v1/chat/completions
export BONFYRE_ORCHESTRATE_MODEL=google/gemma-4-E4B
export BONFYRE_ORCHESTRATE_API_KEY=...
```

## Request contract

```json
{
  "input_type": "audio",
  "objective": "publishable-multi-output",
  "latency_class": "interactive",
  "surface": "pages+jobs",
  "artifact_path": "site/demos/input/example"
}
```

## Output contract

```json
{
  "mode": "gemma4-assisted",
  "model": "google/gemma-4-E4B",
  "selected_binaries": ["bonfyre-ingest", "bonfyre-media-prep", "bonfyre-transcribe", "bonfyre-brief"],
  "booster_binaries": ["bonfyre-narrate", "bonfyre-render", "bonfyre-emit", "bonfyre-pack"],
  "control_surfaces": ["bonfyre-render", "bonfyre-emit", "bonfyre-queue"],
  "expected_outputs": ["normalized-audio", "transcript", "brief", "rendered-output", "formatted-output"]
}
```

## Design constraints

- No end-user prompt text
- No replacement of core deterministic Bonfyre operators
- Optional boost path only
- Registry-bounded: only known Bonfyre operators can be selected
- Low-latency aware: interactive flows get fewer always-on stages and more optional boosters
