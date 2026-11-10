package architecture

// YAFF declarations project names and shapes; grants and deployment remain owner decisions.
#AppLineages: {
 schema_version: "bonfyre.app-lineages.contract.v1"
 source: "architecture/apps/app-lineages.contract.yaff"
 count: int & >0
 if len(lineages) > 0 {count: len(lineages)}
 lineages: [...{
  lineage: string
  flow_stage: string
  representative_doctype: string
  company_flow_after: string
  app_dir?: string
  dialect?: string
  owns?: string
  consumes?: string
  publishes?: string
 }]
 forbidden_inference: string
}
#Carriers: {
 schema_version: "bonfyre.carriers.contract.v1"
 source: "architecture/external/carriers.contract.yaff"
 count: int & >0
 if len(carriers) > 0 {count: len(carriers)}
 carriers: [...{name: string, path: =~"^/"}]
 forbidden_inference: string
}
#Institutions: {
 schema_version: "bonfyre.institution.contract.v1"
 source: "architecture/institutional/namespaces.yaff"
 roots: [string, ...string]
 managed_estates: [string, ...string]
 slice_estates: [...string]
 parents: [string]: string
 inherited_roles: [string]: string
 role_ranks: [string]: int
 delegated_leadership: {} | {
  estate: string & !=""
  leadership_role: string & !=""
  leadership_permission: string & !=""
  leadership_object: string & !=""
 }
 worlds: [string]: string
 persona_lattice: [string]: [...{persona: string, rank: string, capabilities: string}]
 note: string
 forbidden_inference: string
}
