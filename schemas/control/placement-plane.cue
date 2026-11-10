package control

// PlacementPlane decides where a realization may live.  It deliberately does
// not own the represented subject, the authority to act, or public meaning:
// those stay with their semantic owners.  A provider is a replaceable physical
// realization choice, not a second application identity.

#PlacementProvider: "cloudflare" | "supabase" | "oracle"

#PlacementDemand: {
	authority: "none" | "transport" | "projection" | "native-effect"
	mutability: "immutable" | "append-only" | "mutable"
	latency: "edge" | "interactive" | "batch"
	durability: "cache" | "material" | "shared-projection" | "authoritative-runtime"
	visibility: "public" | "private" | "mixed"
	mode: "request" | "stream" | "batch" | "coordination"
	effect: "none" | "reversible" | "effectful"
	data_gravity: "edge" | "relational" | "native" | "material-lake"
	failure_tolerance: "serve-last-proven" | "degrade" | "must-run"
	cost_ceiling: "free-edge" | "shared-free" | "always-free-native"
}

#PlacementService: {
	id: string & =~"^[a-z][a-z0-9-]*$"
	semantic_owner: string & != ""
	demand: #PlacementDemand
	primary: #PlacementProvider
	projections?: [...#PlacementProvider]
	public_origin?: string & =~"^https://[a-z0-9.-]+$"
	legacy_origins?: [...string & =~"^https://[a-z0-9.-]+$"]
	binds?: [...string & != ""]
	cannot_infer: string & != ""
}

#PlacementPlane: {
	schema_version: "bonfyre.placement-plane.v1"
	law: "No realization may be the sole residence of truth required to reconstruct itself."
	providers: [...#PlacementProvider]
	legacy_worker_exclusions: [...string & =~"^[a-z0-9.-]+/[.a-z0-9*/-]+$"]
	canonical_worker_exclusions: [...string & =~"^[a-z0-9.-]+/[.a-z0-9*/-]+$"]
	services: [...#PlacementService]
}

placementPlane: #PlacementPlane & {
	schema_version: "bonfyre.placement-plane.v1"
	law: "No realization may be the sole residence of truth required to reconstruct itself."
	providers: ["cloudflare", "supabase", "oracle"]
	legacy_worker_exclusions: [
		"aurekai.work/oauth*",
		"aurekai.work/mcp*",
		"aurekai.work/.well-known/oauth-*",
	]
	// These are not optional just because their DNS records target the Tunnel:
	// the broad *.aurekai.work Worker route would otherwise still consume them.
	canonical_worker_exclusions: [
		"mcp.aurekai.work/*",
		"auth.aurekai.work/*",
	]
	services: [
		{
			id: "public-estate", semantic_owner: "PageComposition", primary: "cloudflare", projections: ["supabase"], public_origin: "https://aurekai.live", binds: ["/*"],
			demand: {authority: "none", mutability: "immutable", latency: "edge", durability: "material", visibility: "public", mode: "request", effect: "none", data_gravity: "material-lake", failure_tolerance: "serve-last-proven", cost_ceiling: "free-edge"},
			cannot_infer: "a public delivery copy is semantic ownership or authority",
		},
		{
			id: "mcp-resource", semantic_owner: "McpApertureIR", primary: "oracle", projections: ["cloudflare", "supabase"], public_origin: "https://mcp.aurekai.work", legacy_origins: ["https://aurekai.work"], binds: ["/mcp"],
			demand: {authority: "transport", mutability: "mutable", latency: "interactive", durability: "authoritative-runtime", visibility: "mixed", mode: "request", effect: "effectful", data_gravity: "native", failure_tolerance: "degrade", cost_ceiling: "always-free-native"},
			cannot_infer: "MCP transport admission is authority to invoke an owner effect",
		},
		{
			id: "oauth-authority", semantic_owner: "BonfyreAuth", primary: "oracle", projections: ["cloudflare", "supabase"], public_origin: "https://auth.aurekai.work", legacy_origins: ["https://aurekai.work"], binds: ["/oauth/authorize", "/oauth/token", "/.well-known/oauth-authorization-server"],
			demand: {authority: "native-effect", mutability: "mutable", latency: "interactive", durability: "authoritative-runtime", visibility: "mixed", mode: "request", effect: "effectful", data_gravity: "native", failure_tolerance: "degrade", cost_ceiling: "always-free-native"},
			cannot_infer: "an OAuth transaction projection can mint native authority",
		},
		{
			id: "continuity-projection", semantic_owner: "PlacementPlane", primary: "supabase", projections: ["cloudflare", "oracle"],
			demand: {authority: "projection", mutability: "append-only", latency: "interactive", durability: "shared-projection", visibility: "private", mode: "coordination", effect: "reversible", data_gravity: "relational", failure_tolerance: "serve-last-proven", cost_ceiling: "shared-free"},
			cannot_infer: "a continuity projection is canonical semantic authority",
		},
		{
			id: "immutable-material", semantic_owner: "ForeignTwin", primary: "cloudflare", projections: ["supabase", "oracle"], public_origin: "https://material.aurekai.work", binds: ["/*"],
			demand: {authority: "none", mutability: "append-only", latency: "edge", durability: "material", visibility: "public", mode: "batch", effect: "none", data_gravity: "material-lake", failure_tolerance: "serve-last-proven", cost_ceiling: "free-edge"},
			cannot_infer: "an immutable material object grants publication or evidence authority",
		},
		{
			id: "spatial-observation-compiler", semantic_owner: "SpatialObservation", primary: "oracle", projections: ["cloudflare", "supabase"],
			demand: {authority: "projection", mutability: "append-only", latency: "batch", durability: "authoritative-runtime", visibility: "mixed", mode: "batch", effect: "reversible", data_gravity: "native", failure_tolerance: "serve-last-proven", cost_ceiling: "always-free-native"},
			cannot_infer: "a spatial projection creates a subject, observation, disclosure right, or authority",
		},
	]
}
