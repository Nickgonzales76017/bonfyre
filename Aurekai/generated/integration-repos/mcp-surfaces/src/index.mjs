import {
  createStateSnapshot,
  createTransitionSequence,
  createTrajectory,
  createContinuityClaim,
} from "@aurekai/continuity-core";

export function mapHostEventToKernel(event) {
  const committedState = createStateSnapshot({
    chart_family: "workflow_state",
    policy_family: "workflow",
    payload: { target: "MCP surfaces", event },
  });

  const transitions = createTransitionSequence(["prepare", "commit"], {
    transition_type: "continuity-projection",
    predecessor_state_hash: committedState.commitment_ref,
    metadata: { target: "MCP surfaces" },
  });

  const trajectory = createTrajectory({
    target: "MCP surfaces",
    transitions,
    lineage_edges: [],
  });

  const claim = createContinuityClaim({
    target_commitment: committedState.commitment_ref,
    proof_ref: trajectory.folded_witness,
    threshold: 0.7,
    value: Number(event?.deformation_score || 0),
  });

  return {
    committed_state: committedState,
    transitions,
    trajectory,
    policies: [],
    lineage_edges: [],
    export_projections: [],
    functional_claims: [claim],
  };
}

export function mapGenerationResultToKernel(result) {
  if (!result || result.schema_version !== "bonfyre.native.generation.result.v1") {
    throw new Error("A native generation result is required");
  }
  const committedState = createStateSnapshot({
    chart_family: "native_generation",
    policy_family: "replay_safe_generation",
    payload: {
      output_sha256: result.output_sha256,
      prompt_sha256: result.prompt_sha256,
      model: result.model,
      request: result.request,
    },
  });
  const transitions = createTransitionSequence(["generated", "projectable"], {
    transition_type: "native-generation",
    predecessor_state_hash: committedState.commitment_ref,
    metadata: { output_sha256: result.output_sha256 },
  });
  const trajectory = createTrajectory({
    target: "native-generation",
    transitions,
    lineage_edges: [],
  });
  const claim = createContinuityClaim({
    target_commitment: committedState.commitment_ref,
    proof_ref: trajectory.folded_witness,
    threshold: 0.7,
    value: result.output ? 1 : 0,
  });
  return {
    committed_state: committedState,
    transitions,
    trajectory,
    policies: [],
    lineage_edges: [],
    export_projections: [],
    functional_claims: [claim],
  };
}
