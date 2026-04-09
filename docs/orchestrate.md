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

When Gemma is configured, it operates as a bounded delta planner over the deterministic baseline, not as a freeform plan replacement.

## Commands

```bash
bonfyre-orchestrate status
bonfyre-orchestrate plan request.json
bonfyre-orchestrate feedback request.json 0.22 0.08
bonfyre-orchestrate feedback request.json feedback.json
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
  "expected_outputs": ["normalized-audio", "transcript", "brief", "rendered-output", "formatted-output"],
  "predicted_cost": 0.452,
  "predicted_latency": 0.387,
  "predicted_confidence": 0.731,
  "predicted_reversibility": 0.804,
  "predicted_utility": 0.779,
  "predicted_information_gain": 0.512,
  "predicted_policy_score": 0.603
}
```

## Design constraints

- No end-user prompt text
- No replacement of core deterministic Bonfyre operators
- Optional boost path only
- Registry-bounded: only known Bonfyre operators can be selected
- Low-latency aware: interactive flows get fewer always-on stages and more optional boosters

## Control profile

Bonfyre derives a control profile for each operator from the typed registry. The orchestrator now scores plans over:

- `cost`
- `latency`
- `confidence`
- `reversibility`
- `utility`
- `information_gain`

This is the bridge from theory to runtime:

- information theory: call the model only when expected information gain is meaningful
- control theory: optimize a bounded plan under latency and stability constraints
- rate-distortion: trade compute against realization quality instead of generating freeform output

## Policy memory

The orchestrator keeps a compact SQLite policy store keyed by:

- `input_type`
- `objective`
- `latency_class`
- `surface`

That means Bonfyre can reuse winning boost sets without calling the model again for the same orchestration signature.

Gemma is gated by the current plan itself:

- low expected information gain: skip the model
- already-high confidence: skip the model
- known policy signature: reuse cached booster set first

When Gemma is called, its proposed boosters are only accepted if they pass a stability gate against the baseline plan:

- policy score must improve
- latency cannot widen beyond the bounded delta
- cost cannot widen beyond the bounded delta
- confidence cannot materially degrade
- reversibility cannot materially degrade

Accepted model deltas surface as `mode: "gemma4-delta"`.

## Feedback and regret

Bonfyre can now record simple post-run feedback:

```bash
bonfyre-orchestrate feedback request.json 0.22 0.08
bonfyre-orchestrate feedback request.json feedback.json
```

Meaning:

- `0.22` = observed quality gain
- `0.08` = observed latency delta
- `feedback.json` = optional typed feedback payload when Bonfyre already has domain-level observations

Example:

```json
{
  "quality_gain": 0.21,
  "latency_delta": 0.05,
  "exec": 0.88,
  "artifact": 0.74,
  "tensor": 0.93,
  "cms": 0.62,
  "retrieval": 0.91,
  "value": 0.58
}
```

The orchestrator stores:

- average quality gain
- average latency delta
- average regret
- sample count

This is the first thin evaluation loop needed for policy distillation and long-run adaptive control.

## Domain back-feed

Bonfyre now also derives compact domain signals from each feedback event:

- `exec`
- `artifact`
- `tensor`
- `cms`
- `retrieval`
- `value`

These are stored in the same policy table and folded into a composite `policy_score`.

That means the planner can start treating Lambda Tensors, CMS relational fit, retrieval lift, and execution quality as different observability surfaces instead of flattening everything into one scalar too early.

When Bonfyre already has direct measurements from those surfaces, the JSON feedback path lets them flow into policy memory without first collapsing back to a flat quality/latency proxy.

## Objective weighting

The composite `policy_score` is not flat. Bonfyre now reweights domains by request intent before scoring:

- CMS and publish-heavy requests bias toward `cms` and `artifact`
- retrieval and semantic requests bias toward `retrieval` and `tensor`
- tensor and compression requests bias toward `tensor`
- value workflows bias toward `value`
- fast and interactive surfaces bias toward `exec`

That same objective-aware weighting is used for `predicted_policy_score`, so the planner compares paths against the right control surface for the current job instead of a universal reward curve.

## Booster frontier

The baseline planner no longer keeps every keyword-matched booster. It now scores candidate boosters against the request's objective-weighted control surface and keeps only the highest-value frontier under the current latency budget.

That means:

- interactive flows keep a smaller booster set
- batch flows can keep a wider frontier
- retrieval jobs favor `query`, `embed`, `vec`, `graph`, and `index`
- value workflows favor `offer`, `ledger`, `gate`, and `meter`

`expected_outputs` now reflects both always-on stages and retained boosters, so the output contract matches the actual planned path more closely.
