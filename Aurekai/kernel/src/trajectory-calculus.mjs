import { createHash } from "node:crypto";

function stableHash(payload) {
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

function toArray(value) {
  return Array.isArray(value) ? value : [];
}

export function buildReplaySlices(transitions = [], lineageEdges = []) {
  const orderedTransitions = toArray(transitions);
  const orderedEdges = toArray(lineageEdges);

  const allTransitionIds = orderedTransitions.map((item) => item.transition_id).filter(Boolean);
  const allEdgeIds = orderedEdges.map((item) => item.edge_id).filter(Boolean);

  const prepareTransitions = orderedTransitions
    .filter((item) => String(item.phase || "").toLowerCase() === "prepare")
    .map((item) => item.transition_id)
    .filter(Boolean);

  const commitTransitions = orderedTransitions
    .filter((item) => String(item.phase || "").toLowerCase() === "commit")
    .map((item) => item.transition_id)
    .filter(Boolean);

  const replayAnchorEdges = orderedEdges
    .filter((item) => String(item.edge_type || "").toLowerCase() === "replay_anchor")
    .map((item) => item.edge_id)
    .filter(Boolean);

  const slices = [
    {
      slice_id: "full-path",
      mode: "full",
      transition_ids: allTransitionIds,
      edge_ids: allEdgeIds,
      witness_root: stableHash({ allTransitionIds, allEdgeIds }),
    },
    {
      slice_id: "prepare-window",
      mode: "phase-window",
      transition_ids: prepareTransitions,
      edge_ids: allEdgeIds,
      witness_root: stableHash({ prepareTransitions, allEdgeIds }),
    },
    {
      slice_id: "commit-window",
      mode: "phase-window",
      transition_ids: commitTransitions,
      edge_ids: replayAnchorEdges,
      witness_root: stableHash({ commitTransitions, replayAnchorEdges }),
    },
  ];

  return slices;
}

export function buildBranchModel(lineageEdges = []) {
  const edges = toArray(lineageEdges);
  const byFamily = new Map();

  for (const edge of edges) {
    const family = String(edge?.edge_family || "execution-edge");
    if (!byFamily.has(family)) byFamily.set(family, []);
    byFamily.get(family).push(edge);
  }

  const branches = [];
  for (const [family, familyEdges] of byFamily.entries()) {
    const edgeIds = familyEdges.map((item) => item.edge_id).filter(Boolean);
    const branchId = `${family}:${stableHash(edgeIds).slice(0, 12)}`;
    branches.push({
      branch_id: branchId,
      edge_family: family,
      edge_ids: edgeIds,
      fold_count: edgeIds.length,
      folded_witness: stableHash({ family, edgeIds }),
    });
  }

  return branches;
}

export function compressBranches(branches = []) {
  const rows = toArray(branches);
  const summary = {
    algorithm: "family-fold-v1",
    input_branch_count: rows.length,
    output_branch_count: 0,
    compression_ratio: 1,
    compressed_families: [],
  };

  if (rows.length === 0) {
    return summary;
  }

  const compact = new Map();
  for (const branch of rows) {
    const family = String(branch?.edge_family || "execution-edge");
    const existing = compact.get(family) || { family, edge_ids: new Set() };
    for (const edgeId of toArray(branch?.edge_ids)) {
      existing.edge_ids.add(edgeId);
    }
    compact.set(family, existing);
  }

  summary.output_branch_count = compact.size;
  summary.compression_ratio = Number((compact.size / rows.length).toFixed(4));
  summary.compressed_families = Array.from(compact.values()).map((row) => ({
    edge_family: row.family,
    edge_count: row.edge_ids.size,
    folded_witness: stableHash({ edge_family: row.family, edge_ids: Array.from(row.edge_ids).sort() }),
  }));

  return summary;
}

export function runContinuityQuery(calculus, query = {}) {
  const type = String(query.type || "edge-family-count");

  if (type === "edge-family-count") {
    const counts = {};
    for (const branch of toArray(calculus?.branches)) {
      counts[branch.edge_family] = (counts[branch.edge_family] || 0) + Number(branch.fold_count || 0);
    }
    return { type, result: counts };
  }

  if (type === "replay-slice") {
    const mode = String(query.mode || "full");
    const slices = toArray(calculus?.replay_slices).filter((item) => String(item.mode || "") === mode);
    return {
      type,
      mode,
      result: {
        count: slices.length,
        slices: slices.map((item) => item.slice_id),
      },
    };
  }

  if (type === "branch") {
    const family = String(query.edge_family || "");
    const branches = toArray(calculus?.branches).filter((item) => !family || item.edge_family === family);
    return {
      type,
      edge_family: family || null,
      result: {
        count: branches.length,
        branch_ids: branches.map((item) => item.branch_id),
      },
    };
  }

  return {
    type,
    result: {
      unknown_query: true,
    },
  };
}

export function buildTrajectoryCalculus({ transitions = [], lineageEdges = [], defaultQueries = [] } = {}) {
  const replaySlices = buildReplaySlices(transitions, lineageEdges);
  const branches = buildBranchModel(lineageEdges);
  const compression = compressBranches(branches);

  const queries = toArray(defaultQueries).map((query) => runContinuityQuery({ replay_slices: replaySlices, branches }, query));

  return {
    replay_slices: replaySlices,
    branches,
    branch_compression: compression,
    continuity_queries: queries,
    edge_families: Array.from(new Set(branches.map((branch) => branch.edge_family))).sort(),
    branch_count: branches.length,
  };
}
