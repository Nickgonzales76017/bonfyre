import { createHash } from "node:crypto";

function stableHash(payload) {
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

function nowIso() {
  return new Date().toISOString();
}

export function createTransition(input = {}) {
  const transitionId = input.transition_id || stableHash({ input, now: nowIso() }).slice(0, 24);
  return {
    schema_version: "aurekai.transition.v1",
    transition_id: transitionId,
    transition_type: input.transition_type || "continuity-projection",
    phase: input.phase || "prepare",
    witness_hash: input.witness_hash || stableHash({ transitionId, witness: input.witness_payload || input }),
    predecessor_state_hash: input.predecessor_state_hash || stableHash({ predecessor: transitionId }),
    successor_state_hash: input.successor_state_hash || stableHash({ successor: transitionId }),
    emitted_at: input.emitted_at || nowIso(),
    metadata: input.metadata || {},
  };
}

export function createTransitionSequence(steps = [], input = {}) {
  const baseState = input.predecessor_state_hash || stableHash({ state: input.target || "unknown" });
  return steps.map((step, index) => createTransition({
    ...input,
    phase: String(step || `step-${index + 1}`),
    predecessor_state_hash: baseState,
    metadata: {
      ...(input.metadata || {}),
      step_index: index,
      canonical_step: String(step || `step-${index + 1}`),
      final_step: index === steps.length - 1,
    },
  }));
}
