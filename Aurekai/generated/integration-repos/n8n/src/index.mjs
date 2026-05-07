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
    payload: { target: "n8n", event },
  });

  const transitions = createTransitionSequence(["prepare", "commit"], {
    transition_type: "continuity-projection",
    predecessor_state_hash: committedState.commitment_ref,
    metadata: { target: "n8n" },
  });

  const trajectory = createTrajectory({
    target: "n8n",
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
