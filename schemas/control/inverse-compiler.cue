package control

// No subject fixture lives here. TargetObligation owns arbitrary target
// PredicateRefs; this declaration owns reusable transformation law only.
#PredicatePattern: {owner: #PredicateOwner, predicate: string & != "", expected_state: #ExpectedPredicateState}
#SemanticCost: {
	elapsed_minutes: int & >=0
	cash_delta_micros: int
	accrued_cost_micros: int & >=0
	resource_consumption: int & >=0
	rights_conceded: int & >=0
	ownership_dilution_bps: int & >=0
	reversibility_frontier: int & >=0
	compensation_cost_micros: int & >=0
	success_probability_ppm: int & >=0 & <=1000000
	evidence_strength: int & >=0
	proof_distance: int & >=0
	authority_distance: int & >=0
	information_disclosure: int & >=0
	provider_dependence: int & >=0
	failure_domain_concentration: int & >=0
	freshness_exposure: int & >=0
	human_attention_minutes: int & >=0
	roundtrip_observability: int & >=0
	semantic_loss: int & >=0
}
#TransformationFamily: {
	id: string & =~"^[a-z][a-z0-9_]*$"
	requires: [...#PredicatePattern]
	establishes: [#PredicatePattern, ...#PredicatePattern]
	retracts: [...#PredicatePattern]
	consumes: [...string & != ""]
	produces: [...string & != ""]
	creates_commitments: [...string & != ""]
	discharges_commitments: [...string & != ""]
	grants_rights: [...string & != ""]
	concedes_rights: [...string & != ""]
	conflicts_with_rights: [...string & != ""]
	obligations: [...string & != ""]
	protocol_pattern: string & != ""
	resource_pattern: string & != ""
	effect_kind: string & != ""
	reversibility: "reversible" | "compensatable" | "irreversible"
	cost: #SemanticCost
}
#ClosureEquivalence: {lhs: [string & =~"^[a-z][a-z0-9_]*$", ...string], rhs: string & =~"^[a-z][a-z0-9_]*$"}
#PreferencePolicy: {
	id: string & =~"^[a-z][a-z0-9_]*$"
	prefer: ["elapsed_minutes" | "cash_delta_micros" | "accrued_cost_micros" | "resource_consumption" | "rights_conceded" | "ownership_dilution_bps" | "reversibility_frontier" | "compensation_cost_micros" | "success_probability_ppm" | "evidence_strength" | "proof_distance" | "authority_distance" | "information_disclosure" | "provider_dependence" | "failure_domain_concentration" | "freshness_exposure" | "human_attention_minutes" | "roundtrip_observability" | "semantic_loss", ...string]
}

// All effects remain proposals: the lowerer emits cross-owner residuals,
// never a direct reservation, commitment, payment, publication or proof.
inverseTransformationFamilies: [
	{
		id:"reserve_capacity", requires:[], establishes:[{owner:"ResourceReservation",predicate:"capacity",expected_state:"reserved"}], retracts:[]
		consumes:["temporal-capacity"], produces:["reservation-hold"], creates_commitments:["reservation-hold"], discharges_commitments:[], grants_rights:[], concedes_rights:[], conflicts_with_rights:[]
		obligations:["authority-recheck-at-effect-time","independent-roundtrip-observation"], protocol_pattern:"resource-reservation", resource_pattern:"temporal-capacity", effect_kind:"reservation", reversibility:"compensatable"
		cost:{elapsed_minutes:20,cash_delta_micros:0,accrued_cost_micros:0,resource_consumption:1,rights_conceded:0,ownership_dilution_bps:0,reversibility_frontier:1,compensation_cost_micros:0,success_probability_ppm:900000,evidence_strength:2,proof_distance:1,authority_distance:1,information_disclosure:0,provider_dependence:1,failure_domain_concentration:1,freshness_exposure:1,human_attention_minutes:5,roundtrip_observability:2,semantic_loss:0}
	},
	{
		id:"enact_protocol_commitment", requires:[], establishes:[{owner:"CommitmentGraph",predicate:"commitment",expected_state:"discharged"}], retracts:[]
		consumes:["counterparty-capacity"], produces:["commitment-receipt"], creates_commitments:["counterparty-commitment"], discharges_commitments:["counterparty-commitment"], grants_rights:[], concedes_rights:[], conflicts_with_rights:[]
		obligations:["protocol-conformance","evidence-receipt"], protocol_pattern:"commitment-enactment", resource_pattern:"counterparty-capacity", effect_kind:"protocol", reversibility:"compensatable"
		cost:{elapsed_minutes:30,cash_delta_micros:0,accrued_cost_micros:0,resource_consumption:1,rights_conceded:0,ownership_dilution_bps:0,reversibility_frontier:1,compensation_cost_micros:0,success_probability_ppm:850000,evidence_strength:3,proof_distance:1,authority_distance:1,information_disclosure:1,provider_dependence:1,failure_domain_concentration:1,freshness_exposure:1,human_attention_minutes:10,roundtrip_observability:2,semantic_loss:0}
	},
	{
		id:"settle_commerce", requires:[{owner:"CommerceChain",predicate:"quote",expected_state:"valid"}], establishes:[{owner:"CommerceChain",predicate:"payment",expected_state:"settled"}], retracts:[]
		consumes:["cash","quote-validity-window"], produces:["settlement-receipt"], creates_commitments:["settlement-obligation"], discharges_commitments:["settlement-obligation"], grants_rights:[], concedes_rights:["payment-control"], conflicts_with_rights:[]
		obligations:["payment-authority-recheck","delivery-roundtrip"], protocol_pattern:"commerce-settlement", resource_pattern:"funding-source", effect_kind:"payment", reversibility:"irreversible"
		cost:{elapsed_minutes:5,cash_delta_micros:1000000,accrued_cost_micros:0,resource_consumption:1,rights_conceded:1,ownership_dilution_bps:0,reversibility_frontier:2,compensation_cost_micros:0,success_probability_ppm:950000,evidence_strength:3,proof_distance:2,authority_distance:2,information_disclosure:1,provider_dependence:1,failure_domain_concentration:1,freshness_exposure:2,human_attention_minutes:2,roundtrip_observability:3,semantic_loss:0}
	},
	{
		id:"independent_roundtrip_observe", requires:[], establishes:[{owner:"ForeignTwin",predicate:"result",expected_state:"observed"}], retracts:[]
		consumes:["observation-budget"], produces:["foreign-twin-receipt"], creates_commitments:[], discharges_commitments:[], grants_rights:[], concedes_rights:[], conflicts_with_rights:[]
		obligations:["independent-observation"], protocol_pattern:"foreign-twin-observation", resource_pattern:"observation-budget", effect_kind:"observe", reversibility:"reversible"
		cost:{elapsed_minutes:2,cash_delta_micros:0,accrued_cost_micros:0,resource_consumption:1,rights_conceded:0,ownership_dilution_bps:0,reversibility_frontier:0,compensation_cost_micros:0,success_probability_ppm:800000,evidence_strength:3,proof_distance:1,authority_distance:0,information_disclosure:0,provider_dependence:1,failure_domain_concentration:1,freshness_exposure:2,human_attention_minutes:1,roundtrip_observability:3,semantic_loss:0}
	},
	{
		id:"prove_invariant", requires:[{owner:"ForeignTwin",predicate:"result",expected_state:"observed"}], establishes:[{owner:"ProofFrontier",predicate:"invariant",expected_state:"proven"}], retracts:[]
		consumes:["independent-evidence"], produces:["proof-receipt"], creates_commitments:[], discharges_commitments:[], grants_rights:[], concedes_rights:[], conflicts_with_rights:[]
		obligations:["proof-frontier-validation"], protocol_pattern:"proof-frontier", resource_pattern:"independent-evidence", effect_kind:"prove", reversibility:"reversible"
		cost:{elapsed_minutes:60,cash_delta_micros:0,accrued_cost_micros:0,resource_consumption:2,rights_conceded:0,ownership_dilution_bps:0,reversibility_frontier:0,compensation_cost_micros:0,success_probability_ppm:700000,evidence_strength:5,proof_distance:0,authority_distance:0,information_disclosure:0,provider_dependence:0,failure_domain_concentration:0,freshness_exposure:1,human_attention_minutes:30,roundtrip_observability:3,semantic_loss:0}
	},
]
inverseClosureEquivalences: [{lhs:["enact_protocol_commitment","reserve_capacity"],rhs:"reserve_capacity"}]
inversePreferencePolicies: [
	{id:"resilient",prefer:["failure_domain_concentration","proof_distance","authority_distance","rights_conceded","reversibility_frontier","roundtrip_observability","success_probability_ppm","elapsed_minutes","cash_delta_micros"]},
	{id:"urgent",prefer:["elapsed_minutes","success_probability_ppm","authority_distance","roundtrip_observability","cash_delta_micros","failure_domain_concentration"]},
	{id:"frugal",prefer:["cash_delta_micros","accrued_cost_micros","resource_consumption","rights_conceded","semantic_loss","elapsed_minutes"]},
]
inverseCompiler: {owners:nativeOwnerPredicateGrammar, transformations:inverseTransformationFamilies, equivalences:inverseClosureEquivalences, preference_policies:inversePreferencePolicies}
