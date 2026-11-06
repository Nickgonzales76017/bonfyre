package participation

#NamedSet: {name: string & != "", members: [string & != "", ...string]}
#Rung: {axis: string & != "", state: string & != "", depth: int & >=1, depth_name: string & != ""}

participationContract: {
	axes: ["discovery", "observation", "semantic", "protocol", "capability", "economic", "round_trip"]
	generic_human: ["discover"]
	generic_protocol: ["machine-generic"]
	capability_roles: [
		"action_adapter", "authoring_adapter", "compiler_toolchain",
		"external_service_candidate", "verification_tool", "media_service",
	]
	roundtrip_roles: ["action_adapter", "authoring_adapter", "publication_carrier", "verification_tool"]
	roundtrip_human: ["creator-publish", "publication"]
	roundtrip_protocols: ["activitypub", "atproto", "nostr", "webmention", "websub"]
	economic_terms: [
		"commerce", "finance", "investing", "trade", "x402", "web-payment",
		"interledger", "iso20022", "payment-insurance-interchange", "agent-commerce",
	]
	role_classes: [
		{name:"material", members:["material_source", "media_service"]},
		{name:"action", members:["action_adapter", "authoring_adapter"]},
		{name:"interaction_reference", members:["interaction_reference"]},
		{name:"design_reference", members:["design_reference"]},
		{name:"compiler", members:["compiler_toolchain"]},
		{name:"carrier", members:["transport_carrier", "publication_carrier"]},
		{name:"verification", members:["verification_tool"]},
		{name:"service", members:["external_service_candidate"]},
		{name:"standard", members:["reference_standard"]},
	]
	join_families: [
		{name:"economic", members:["commerce", "finance", "investing", "enterprise-gtm", "resource-market", "insurance"]},
		{name:"physical", members:[
			"agriculture", "energy", "food", "housing", "logistics", "manufacturing",
			"mobility", "place", "property", "trip", "utility", "vehicle",
		]},
		{name:"social", members:["care", "community", "dating", "hiring", "household", "professional", "relationship"]},
		{name:"knowledge_compute", members:["ai-work", "knowledge", "learning", "research", "science", "software-work"]},
		{name:"media_attention", members:[
			"audience-growth", "creator-publish", "listen", "live-event", "media-production",
			"publication", "sports", "watch",
		]},
		{name:"civic_regulated", members:[
			"civic", "emergency", "grant-funding", "healthcare", "identity-access", "service-case",
		]},
		{name:"coordination", members:["collaboration", "mission", "project", "support"]},
	]
	ladders: [
		{axis:"discovery", state:"indexed", depth:1, depth_name:"indexed"},
		{axis:"discovery", state:"routable", depth:2, depth_name:"owner-routable"},
		{axis:"discovery", state:"maintained", depth:3, depth_name:"maintained"},
		{axis:"observation", state:"policy_excluded", depth:1, depth_name:"policy-preflighted"},
		{axis:"observation", state:"policy_indeterminate", depth:1, depth_name:"policy-preflighted"},
		{axis:"observation", state:"boundary_response", depth:2, depth_name:"transport-observed"},
		{axis:"observation", state:"source_observed", depth:3, depth_name:"body-observed"},
		{axis:"observation", state:"source_retained", depth:4, depth_name:"body-retained"},
		{axis:"observation", state:"repeat_observed", depth:5, depth_name:"repeat-observed"},
		{axis:"semantic", state:"interpreted", depth:1, depth_name:"interpreted"},
		{axis:"semantic", state:"profile_bound", depth:2, depth_name:"profile-bound"},
		{axis:"semantic", state:"carrier_conformant", depth:3, depth_name:"carrier-conformant"},
		{axis:"protocol", state:"contract_bound", depth:1, depth_name:"contract-bound"},
		{axis:"protocol", state:"compiled", depth:2, depth_name:"compiled"},
		{axis:"protocol", state:"enacted", depth:3, depth_name:"enacted"},
		{axis:"protocol", state:"obligations_closed", depth:4, depth_name:"obligations-closed"},
		{axis:"capability", state:"qualified", depth:1, depth_name:"qualified"},
		{axis:"capability", state:"eligible", depth:2, depth_name:"eligible"},
		{axis:"capability", state:"authorized", depth:3, depth_name:"authorized"},
		{axis:"capability", state:"activated", depth:4, depth_name:"activated"},
		{axis:"economic", state:"quoted", depth:1, depth_name:"quoted"},
		{axis:"economic", state:"committed", depth:2, depth_name:"committed"},
		{axis:"economic", state:"paid", depth:3, depth_name:"paid"},
		{axis:"economic", state:"settled", depth:4, depth_name:"settled"},
		{axis:"economic", state:"reconciled", depth:5, depth_name:"reconciled"},
		{axis:"round_trip", state:"projected", depth:1, depth_name:"projected"},
		{axis:"round_trip", state:"observed", depth:2, depth_name:"independently-observed"},
		{axis:"round_trip", state:"faithful", depth:3, depth_name:"closed-faithful"},
		{axis:"round_trip", state:"drifted", depth:3, depth_name:"closed-drifted"},
		{axis:"round_trip", state:"unreachable", depth:3, depth_name:"closed-unreachable"},
	]
}
