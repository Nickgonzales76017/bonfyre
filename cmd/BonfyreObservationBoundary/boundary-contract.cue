package observationboundary

#State: {
	name: string & != ""
	policy_decision?: string
	require_fetch: bool
	require_transport_success: bool
	require_http_success: bool
	require_body: bool
	require_content_hash: bool
}

boundaryContract: {
	schema: "bonfyre.observation-boundary.v1"
	receipt_schema: "bonfyre-foreign-observation.v1"
	states: [
		{
			name: "policy_excluded"
			policy_decision: "robots_disallow"
			require_fetch: false
			require_transport_success: false
			require_http_success: false
			require_body: false
			require_content_hash: false
		},
		{
			name: "policy_indeterminate"
			policy_decision: "robots_preflight_unavailable"
			require_fetch: false
			require_transport_success: false
			require_http_success: false
			require_body: false
			require_content_hash: false
		},
		{
			name: "source_observed"
			policy_decision: "allow"
			require_fetch: true
			require_transport_success: true
			require_http_success: true
			require_body: true
			require_content_hash: true
		},
	]
	cannot_infer: [
		"documented public feed -> lawful observation by this agent",
		"public observe_right -> robots or terms permission",
		"successful transport -> source authority",
		"content hash -> retained body availability",
	]
}
