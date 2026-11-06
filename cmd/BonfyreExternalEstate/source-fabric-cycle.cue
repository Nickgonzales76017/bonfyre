sourceFabricCycle: {
  schema: "bonfyre.source-fabric-cycle.v4"
  truth: "effect_free_compilation_and_dependency_measurement_only"
  outputs: {
    recursive: "recursive-channel-candidates.jsonl"
    manifest: "source-frontier.json"
    roots: "source-roots.jsonl"
    channels: "source-channels.jsonl"
    gaps: "source-gaps.jsonl"
    forward: "source-forward.json"
    relationships: "source-relationship-opportunities.jsonl"
    pressure: "source-pressure.jsonl"
    pressure_summary: "source-pressure-summary.json"
    impact: "source-impact-measurements.jsonl"
    impact_receipts: "source-impact-receipts.jsonl"
    scene: "scene-source-receipts.jsonl"
    scene_previous: "scene-source-receipts.previous.jsonl"
    scene_summary: "scene-source-summary.json"
    scene_delta: "scene-source-delta.jsonl"
    root_cochange: "source-scene-cochange.jsonl"
    legacy_scene_sources: "scene-legacy-sources.jsonl"
    attention: "source-attention.jsonl"
    starvation: "source-starvation.jsonl"
    scene_starvation: "scene-source-starvation.jsonl"
    cycle: "source-fabric-cycle.json"
  }
  programs: {
    recursive: {runtime: "bun", file: "compile_recursive_channels.mjs"}
    frontier: {runtime: "bun", file: "compile_source_frontier.mjs"}
    pressure: {runtime: "bun", file: "compile_source_pressure.mjs"}
    impact: {runtime: "bun", file: "compile_source_change_impact.mjs"}
    scene: {runtime: "bun", file: "compile_scene_source_receipts.mjs"}
    scene_delta: {runtime: "bun", file: "compile_scene_source_delta.mjs"}
    legacy_scene: {runtime: "bun", file: "compile_scene_legacy_sources.mjs"}
    forward: {runtime: "bun", file: "compile_source_forward.mjs"}
    attention: {runtime: "bun", file: "compile_source_attention.mjs"}
    starvation: {runtime: "bun", file: "compile_source_starvation.mjs"}
  }
  recursive_inventory_label: "RECURSIVE_RETAINED_DISCOVERY"
  cannot_infer: [
    "recursive channel candidate -> reachable or admitted channel",
    "SourceRoot -> admitted or active source",
    "candidate pressure -> contribution",
    "dependency impact -> visible material changed",
    "Scene co-change -> source causality",
    "attention temperature -> permission to fetch or spend",
    "relationship opportunity -> permission to contact, enroll or transact",
  ]
}
