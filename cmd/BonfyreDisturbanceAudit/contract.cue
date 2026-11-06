package disturbanceaudit

#Group: {
	name: string & != ""
	kinds: [string & != "", ...string]
}

#Threshold: {
	metric: string & != ""
	floor: int & >=0
}

disturbanceContract: {
	event_schema: "bonfyre.disturbance-event.v1"
	acceptance_schema: "bonfyre.disturbance-acceptance.v1"
	required_owners: [
		"ForeignTwinGraph",
		"DomainProfile",
		"SemanticDerivationGraph",
		"EvidenceGraph",
		"BlockerGraph",
		"UnlockGraph",
		"WorkGraph",
		"CommitmentGraph",
		"PublicationProgram",
		"RoundTripTwin",
	]
	economic_owners: ["MoneyGraph", "FinancialMetabolism", "CommerceChain"]
	kind_groups: [
		{name: "blockers_opened", kinds: ["blocker_opened"]},
		{name: "blockers_resolved", kinds: ["blocker_resolved"]},
		{name: "retractions", kinds: ["retracted", "assumption_retracted", "fact_retracted"]},
		{name: "compensations", kinds: ["compensated", "commitment_compensated", "reservation_compensated"]},
		{name: "joins_admitted", kinds: ["join_admitted", "opportunity_admitted"]},
		{name: "human_decisions", kinds: ["human_decision"]},
		{name: "commitments_opened", kinds: ["commitment_opened"]},
		{name: "quotes", kinds: ["quoted", "quote_changed"]},
		{name: "settlements", kinds: ["settled", "reconciled"]},
		{name: "projections", kinds: ["projected", "publication_projected", "external_projection_changed"]},
		{name: "corrections", kinds: ["correction_opened", "correction_applied"]},
		{name: "improper_erases", kinds: ["erased", "commitment_erased", "reservation_erased"]},
	]
	thresholds: [
		{metric: "distinct_owners", floor: 10},
		{metric: "max_owner_braid_depth", floor: 8},
		{metric: "blockers_opened", floor: 1},
		{metric: "blockers_resolved", floor: 1},
		{metric: "joins_admitted", floor: 1},
		{metric: "human_decisions", floor: 1},
		{metric: "commitments_opened", floor: 1},
		{metric: "quotes", floor: 1},
		{metric: "settlements", floor: 1},
		{metric: "projections", floor: 1},
		{metric: "round_trip_total", floor: 1},
	]
	minimum_retraction_or_compensation: 1
	forbid_improper_erases: true
	require_drift_correction: true
}
