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
  "mode": "gemma4-delta",
  "policy_source": "stability-gated-gemma-delta",
  "model": "google/gemma-4-E4B",
  "selected_binaries": ["bonfyre-ingest", "bonfyre-media-prep", "bonfyre-transcribe", "bonfyre-brief"],
  "booster_binaries": ["bonfyre-narrate", "bonfyre-render", "bonfyre-emit", "bonfyre-pack"],
  "control_surfaces": ["bonfyre-render", "bonfyre-emit", "bonfyre-queue"],
  "objective_family": "publish",
  "state_key": "m100-s101-l10-o1000-a11",
  "expected_outputs": ["normalized-audio", "transcript", "brief", "rendered-output", "formatted-output"],
  "predicted_cost": 0.452,
  "predicted_latency": 0.387,
  "predicted_confidence": 0.731,
  "predicted_reversibility": 0.804,
  "predicted_utility": 0.779,
  "predicted_information_gain": 0.512,
  "predicted_policy_score": 0.603,
  "baseline_frontier_metrics": {
    "cost": 0.390,
    "latency": 0.310,
    "confidence": 0.700,
    "reversibility": 0.760,
    "utility": 0.690,
    "information_gain": 0.420,
    "policy_score": 0.560
  },
  "frontier_uplift": {
    "policy_score": 0.043,
    "latency": 0.077,
    "cost": 0.062,
    "confidence": 0.031,
    "reversibility": 0.044,
    "utility": 0.089,
    "information_gain": 0.092
  },
  "active_domain_weights": {
    "exec": 0.270,
    "artifact": 0.207,
    "tensor": 0.108,
    "cms": 0.216,
    "retrieval": 0.117,
    "value": 0.081
  },
  "state_vector": {
    "modality_audio": true,
    "surface_pages": true,
    "surface_jobs": true,
    "latency_interactive": true,
    "objective_publish": true,
    "artifact_local": true,
    "artifact_structured": true
  }
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

If there is no exact signature hit, Bonfyre can also fall back to a proven workload-family prior when the same `input_type`, `latency_class`, and `surface` have already produced a low-regret frontier in the same objective family.

Between those two, Bonfyre can also reuse a frontier by exact compressed machine state when the same `state_key` has already produced a strong low-regret outcome.

Bonfyre also distills those successful frontiers back into the deterministic planner as ranking priors, so repeated wins can bias booster ordering even before an exact memory hit is available.

Gemma is gated by the current plan itself:

- low expected information gain: skip the model
- already-high confidence: skip the model
- known policy signature: reuse cached booster set first
- known compressed state: reuse cached state frontier next
- no exact hit but proven family prior: reuse that frontier before asking the model

When Gemma is called, its proposed boosters are only accepted if they pass a stability gate against the baseline plan:

- policy score must improve
- latency cannot widen beyond the bounded delta
- cost cannot widen beyond the bounded delta
- confidence cannot materially degrade
- reversibility cannot materially degrade

Accepted model deltas surface as `mode: "gemma4-delta"`.

State-prior reuse surfaces as `mode: "state-memory"`.

Family-prior reuse surfaces as `mode: "family-memory"`.

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

The plan JSON also exposes:

- `policy_source`
- `objective_family`
- `active_domain_weights`

So downstream Bonfyre surfaces can see which control layer won and what domain mix shaped the plan, without introducing a human prompt surface.

## State compression

Each request is also compressed into a small machine state:

- `state_key`
- `state_vector`

This is Bonfyre's sufficient-statistics layer for orchestration. It keeps the planner oriented around modality, surface, latency class, objective family, and artifact shape instead of letting orchestration drift into raw-string prompt logic.

The Gemma path now uses that same compressed substrate too. When Bonfyre asks the model for a delta, it sends:

- `state_key`
- `state_vector`
- `active_domain_weights`
- the deterministic baseline frontier
- the stability gate
- the typed operator registry

So the model sees Bonfyre's machine ontology and bounded control surface rather than a loose human-style request blob.

## Distillation

Successful state-level and family-level frontiers now feed back into deterministic booster ranking as thin priors. That means:

- exact memory reuse is still strongest
- state and family memory still reuse directly when available
- but even the heuristic frontier can start preferring historically successful boosters for the same machine conditions

This is the first real distillation loop from policy memory back into native planning.

## Counterfactuals

Each plan now also exposes:

- `baseline_frontier_metrics`
- `frontier_uplift`
- `booster_contributions`
- `frontier_decision`

That gives Bonfyre an explicit counterfactual against the deterministic floor, so the system can measure what the retained frontier actually changed instead of treating the chosen path as an opaque result.

Bonfyre now also uses that counterfactual internally as an uplift gate. If the retained frontier does not clear the minimum policy gain or a bounded utility tradeoff, the planner collapses back to the deterministic floor instead of keeping decorative or harmful boosters.

That uplift gate is now adaptive by workload memory. When similar state or family histories show boosters paying off consistently, the gate relaxes slightly. When history shows regret or poor payoff, the gate tightens.

When boosters do survive, `booster_contributions` shows their individual frontier-ranking scores. When the uplift gate collapses the frontier, that list is empty by design.

`frontier_decision` makes that explicit by reporting whether the frontier was retained or collapsed, plus how many boosters existed before and after the gate.

## Booster frontier

The baseline planner no longer keeps every keyword-matched booster. It now scores candidate boosters against the request's objective-weighted control surface and keeps only the highest-value frontier under the current latency budget.

That means:

- interactive flows keep a smaller booster set
- batch flows can keep a wider frontier
- retrieval jobs favor `query`, `embed`, `vec`, `graph`, and `index`
- value workflows favor `offer`, `ledger`, `gate`, and `meter`

`expected_outputs` now reflects both always-on stages and retained boosters, so the output contract matches the actual planned path more closely.
