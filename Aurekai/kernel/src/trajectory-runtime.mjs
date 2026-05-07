import { createHash } from "node:crypto";
import { buildTrajectoryCalculus } from "./trajectory-calculus.mjs";

function stableHash(payload) {
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

export function createTrajectory(input = {}) {
  const transitions = Array.isArray(input.transitions) ? input.transitions : [];
  const lineageEdges = Array.isArray(input.lineage_edges) ? input.lineage_edges : [];
  const target = input.target || "unknown-target";
  const runId = input.run_id || stableHash({ target, transitions, lineageEdges }).slice(0, 16);

  const trajectoryRoot = stableHash({ target, run_id: runId, transitions });
  const historyAccumulator = stableHash({ trajectoryRoot, lineageEdges });
  const foldedWitness = stableHash({ historyAccumulator, mode: input.replay_mode || "lineage-unfold" });
  const calculus = buildTrajectoryCalculus({ transitions, lineageEdges, defaultQueries: input.default_queries || [] });

  return {
    schema_version: "aurekai.trajectory.v1",
    trajectory_id: input.trajectory_id || `${target}:${runId}:trajectory`,
    trajectory_root: trajectoryRoot,
    history_accumulator: historyAccumulator,
    folded_witness: foldedWitness,
    step_count: transitions.length,
    edge_count: lineageEdges.length,
    edge_families: calculus.edge_families,
    branch_count: calculus.branch_count,
    branches: calculus.branches,
    replay_slices: calculus.replay_slices,
    branch_compression: calculus.branch_compression,
    continuity_queries: calculus.continuity_queries,
    replay_mode: input.replay_mode || "lineage-unfold",
    continuity_summary: input.continuity_summary || {},
  };
}
