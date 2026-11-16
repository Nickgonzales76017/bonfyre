package stores

// cmd/BonfyreAdmission: the grant ledger bonfyre-admission writes
// (resource_grants; created by engine/core admission.c). WebAddress reads it
// read-only for the admin fact surface /x/resource-grants
// (schemas/control/fact-surfaces.cue); it never writes it.
store_schemas: "cmd/BonfyreAdmission": stores: admission: {
	kind:       "sqlite"
	macro:      "ADMISSION"
	env:        "BONFYRE_ADMISSION_DB"
	written_by: "bonfyre-admission"
	opened_by: ["cmd/BonfyreWebAddress"]
	lease: {}
}
