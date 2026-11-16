package stores

// cmd/BonfyreSurfaceMaturity audits the estate by joining its owners' stores
// to one private in-memory connection, read-only, under fixed schema names.
// Which stores join, and which must, is declared here; the owner names each
// file from the estate directory (or its override) at run time.
store_schemas: "cmd/BonfyreSurfaceMaturity": stores: estate_audit: {
	kind:  "dynamic"
	macro: "SURFACE_MATURITY_ESTATE"
	dynamic: {
		shard: "named"
		joins: [
			{as: "cms", required: true},
			{as: "items"},
			{as: "frontier"},
			{as: "fabric"},
			{as: "acct", env: "BONFYRE_ACCOUNT_WORK_DB"},
			{as: "meter", env: "BONFYRE_METER_DB"},
			{as: "control", env: "BONFYRE_CONTROL_DB"},
		]
	}
}
