import {
  createStateSnapshot,
  createTransitionSequence,
  createTrajectory,
  createContinuityClaim,
} from "@aurekai/continuity-core";

export function mapHostEventToKernel(event) {
  const committedState = createStateSnapshot({
    chart_family: "execution_batch",
    policy_family: "finality",
    payload: { target: "Starknet", event },
  });

  const transitions = createTransitionSequence(["prepare", "commit"], {
    transition_type: "continuity-projection",
    predecessor_state_hash: committedState.commitment_ref,
    metadata: { target: "Starknet" },
  });

  const trajectory = createTrajectory({
    target: "Starknet",
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
