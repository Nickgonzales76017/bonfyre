package control

// The operator queue is a durable execution ledger.  This declaration names
// its physical shape without turning a row into permission to run it.
#OperatorStore: {
	schema_version: "bonfyre.operator_store.v1"
	durability:     "full"
	tables: [
		"goals",
		"runs",
		"actions",
		"meta",
		"budgets",
		"operator_response_usage",
		"operator_run_owners",
		"operator_provider_health",
	]
	indexes: [
		"operator_goals_due",
		"operator_runs_goal_open",
		"operator_runs_started",
		"operator_actions_run",
	]
	archival?: {
		requires_stop:  bool
		accepts_states: [...string]
		refuses:        [...string]
		preserves:      [...string]
		identity:       string & != ""
	}
	reconciliation?: {
		requires_stop:  bool
		run_state:      string & != ""
		goal_state:     string & != ""
		action_state:   string & != ""
		final_state:    string & != ""
		preserves:      [...string]
	}
	routing?: {
		reads_only: true
		classes: [...string]
		tiers: [...string]
		gates: [...string]
		requires_stop_path: bool
		exact_zero_spend_tiers: [...string]
		unmetered_tiers: [...string]
	}
	quality?: {
		schema:          "bonfyre.operator_store.quality.v1"
		evidence_schema: "bonfyre.operator_store.quality_evidence.v1"
		classes: [...string]
		// The owner recomputes the verdict from scored items; callers cannot assert it.
		verdict_from:   "items_passed / items >= threshold"
		witness:        "sha256 of the evidence bytes"
		append_only:    true
		refuses: [...string]
	}
	cannot_infer: string & != ""
}

operatorStore: #OperatorStore & {
	archival: {
		requires_stop:  true
		accepts_states: ["queued", "retry", "blocked"]
		refuses: ["recurring", "open_run", "symlinked_database", "missing_goal"]
		preserves: ["goals", "runs", "actions", "receipts", "evidence", "failure context"]
		identity: "exact lower-case hexadecimal goal identifiers"
	}
	reconciliation: {
		requires_stop: true
		run_state:     "running"
		goal_state:    "queued"
		action_state:  "reverted"
		final_state:   "blocked"
		preserves:     ["goals", "runs", "actions", "receipts", "evidence", "failure context"]
	}
	routing: {
		reads_only: true
		classes: ["deterministic_read", "short_analysis", "code_review", "workflow"]
		// Cheapest first; the first admissible tier is named, else hold.
		tiers: ["native_owner", "local_model", "agent_relay", "paid_provider", "hold"]
		gates: ["goal_not_schedulable", "uncertain_effect_requires_reconciliation", "open_run_requires_reconciliation", "attempts_exhausted", "not_due"]
		requires_stop_path: true
		exact_zero_spend_tiers: ["native_owner", "local_model", "hold"]
		unmetered_tiers: ["agent_relay", "paid_provider"]
	}
	quality: {
		schema:          "bonfyre.operator_store.quality.v1"
		evidence_schema: "bonfyre.operator_store.quality_evidence.v1"
		classes: ["short_analysis", "code_review", "workflow"]
		verdict_from: "items_passed / items >= threshold"
		witness:      "sha256 of the evidence bytes"
		append_only:  true
		refuses: ["unmeasured_evidence", "empty_or_malformed_items", "threshold_outside_unit_interval", "path_shaped_model", "unknown_class", "symlinked_quality_file"]
	}
	cannot_infer: "an initialized, inspected, or STOP-gated archived operator store grants scheduling, completion, model, provider, project, or external-effect authority"
}
