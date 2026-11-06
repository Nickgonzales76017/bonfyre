package mcp

// The project node owns these commands. The overlay only republishes the
// owner's current schema and forwards calls; this list grants no authority.
registry: {
  forwardedProjectTools: [
    "institutional_prepare", "financial_membrane", "frappe_compile",
    "capability_fulfillment", "native_tool_inventory", "native_tool_invoke",
    "mcp_capability_lens", "estate_capabilities", "estate_adapter_probe",
    "bonfyre_moe_generate", "bonfyre_moe_status", "bonfyre_quic_fpq_generate",
    "project_status", "project_tree", "project_search", "project_read",
    "project_diff", "project_patch_preview", "project_apply_patch",
    "project_apply_low_risk_patch", "project_revert_receipt",
    "project_test_profile", "project_commit_receipt", "operator_status",
    "organ_drift_status", "organ_drift_sync",
    "operator_goal_add", "operator_pause", "operator_resume",
  ]
  localTools: {
    recursive_closure_frontier: {readOnly: true}
    foreign_twin_observe: {readOnly: false}
    cms_record: {readOnly: false}
    foreign_resolution: {readOnly: true}
    surface_project: {readOnly: true}
    capability_circuit_compile: {readOnly: true}
    decision_tool_shortlist: {readOnly: false}
  }
  nativeRichTools: ["architecture_atlas", "mcp_call_history", "project_recent_deltas", "runtime_get", "runtime_preview", "institutional_measure", "membrane_state", "finance_book", "protocol_fetch", "desk_open_decisions", "oracle_service_status", "oracle_command_describe", "oracle_command_probe", "oracle_profile_run", "oracle_profile_receipts", "oracle_deep_flow", "artifact_search", "artifact_read", "foreign_discover", "sitemap_delta", "finance_price_set", "studio_client_register", "studio_client_list", "studio_fleming_watch", "studio_fleming_query", "studio_membrane_status", "studio_membrane_budget_set", "studio_work_claim", "studio_work_complete", "studio_fleming_opportunity", "studio_economy_cost_record", "mesh_status", "release_attest", "task_start", "task_status", "model_local_inventory", "model_local_generate", "estate_query"]
  flemingProjections: ["estate", "coverage", "markets", "owners"]
  oracle: {
    profiles: ["skill-promotion", "provider-credit", "provider-market", "lambda-model", "resource-activation", "api-publicgap", "measured-finance", "cloud-placement", "workcell-swarm", "graph-closure", "truth-replay", "media-shoutout", "release-attestation"]
    flows: {
      local_oracle_execution_matrix: "live-local-oracle-mcp-execution-matrix.ts"
      local_oracle_recursive_interaction: "live-local-oracle-mcp-recursive-interaction-flow.ts"
      mcp_resource_matrix: "live-mcp-local-oracle-resource-matrix.ts"
      oracle_mcp_capture: "live-oracle-mcp-capture-closure.ts"
      oracle_auth_money_mcp: "live-oracle-real-read-auth-money-mcp-deep-flow.ts"
      opportunity_mcp_matrix: "live-opportunity-mcp-matrix.ts"
      operator_action_loop: "live-operator-action-loop-runner.ts"
      power_user_reconcile: "live-power-user-live-apply-reconcile.ts"
      aggressive_operator_firehose: "live-aggressive-operator-firehose.ts"
      max_compounding_cycle: "live-max-compounding-cycle.ts"
      total_recursive_closure: "live-total-recursive-closure-max-cycle.ts"
      model_department_total: "live-model-department-total-cycle.ts"
      power_user_model_closure: "live-power-user-model-closure-compiler.ts"
      power_user_product_activation: "live-power-user-product-activation.ts"
      power_user_provider_fanout: "live-power-user-provider-fanout.ts"
    }
    units: ["bonfyred.service", "bonfyre-continuation-loop.service", "bonfyre-continuation-loop.timer", "bonfyre-autonomous-worker-loop.service", "bonfyre-autonomous-worker-loop.timer", "bonfyre-recursive-institutional-growth.service", "bonfyre-recursive-institutional-growth.timer", "bonfyre-surface-fabric.service", "bonfyre-surface-fabric.timer"]
  }
  protocolVariants: {
    ogc_items: "/ogc/collections/parcels/items"
    activitypub_outbox: "/ap/outbox"
  }
  excludedCarrierNames: ["mcp"]
  previewPaths: [
    "/api/bonfyre-mcp/graphs/preview",
    "/api/bonfyre-mcp/mesh/deep-test",
    "/api/runtime-parity/checks/run-live-read",
    "/api/runtime-mesh/flows/preview",
    "/api/interactions/scenarios/run-matrix-fixture",
    "/api/resources/provider-enrich",
    "/api/deep-tests/run-all-fixture",
  ]
  resources: [
    {uri: "bonfyre://closure/frontier", name: "Recursive institutional closure frontier", description: "Live owner-backed five-plane preparation cut. Reading it never grants effect authority.", mimeType: "application/json"},
    {uri: "bonfyre://closure/operating-contract", name: "Recursive closure operating contract", description: "Non-escalation laws for Matrix, observation, CMS, cognition, Work and money.", mimeType: "application/json"},
  ]
  richResourceTools: {
    "bonfyre://mesh/status": {owner: "rich", tool: "mesh_status"}
    "bonfyre://project/status": {owner: "project", tool: "project_status"}
    "bonfyre://mcp/toolsets": {owner: "project", tool: "mcp_capability_lens"}
    "bonfyre://estate/capabilities": {owner: "project", tool: "estate_capabilities"}
    "bonfyre://oracle/release": {owner: "rich", tool: "release_attest"}
    "bonfyre://phase94/receipts": {owner: "rich", tool: "oracle_profile_receipts", args: {limit: 300}}
    "bonfyre://mcp/calls": {owner: "rich", tool: "mcp_call_history", args: {limit: 300}}
  }
  promptBodies: {
    diagnose_live_system: "Diagnose the live Bonfyre {focus}. Start with mesh_status, project_status, release_attest, failed Phase 94 receipts, runtime parity, and current deltas. Separate active, reachable, authenticated, verified, degraded, and failed."
    implement_project_change: "Implement this Bonfyre project change: {request}. Search and read the live project first, inspect the diff, produce a unified patch, call project_patch_preview, and do not call project_apply_patch without explicit approval."
    certify_oracle: "Certify Oracle through release_attest, command probes, service state, route reads, MCP deep flows, profile receipts, and local/Oracle parity. Report exact evidence."
  }
}
