// Constraint for the draft inventory written by `bonfyre-brief inventory`
// (src/inventory.c) and read by `bonfyre-brief cited --inventory`.
// Check: cue vet -c cmd/BonfyreBrief/data/hosting-inventory.cue INVENTORY.json
package brief

#Fact:   =~"^[a-z][a-z0-9_]{0,47}$"
#Sha256: =~"^[0-9a-f]{64}$"
#Panel:  "cpanel" | "interworx" | "plesk"
#From:   "intake" | =~"^export:(cpanel|interworx|plesk)(\\+(cpanel|interworx|plesk))*$"

#Listing:
	"whm-listaccts" | "cpanel-api2-listaddondomains" | "cpanel-api2-listpops" | "cpanel-api2-mysqlfe-listdbs" |
	"interworx-siteworx-listAccounts" | "interworx-siteworx-listAllDomains" |
	"interworx-email-box-listEmailBoxes" | "interworx-mysql-db-listMysqlDatabases" |
	"plesk-xml-site-get"

#Intake: {
	kind:         "intake"
	reference:    string & !=""
	about:        string | null
	received_at:  string | null
	inbox_sha256: #Sha256
}

#Export: {
	kind:    "export"
	panel:   #Panel
	listing: #Listing
	file:    string & !=""
	sha256:  #Sha256
	bytes:   int & >0
	items:   int & >=0
}

schema: "bonfyre.hosting-inventory.v1"
// A draft until the owner has gone through it; cited raises it as an exception.
draft:   true
subject: =~"^[A-Za-z0-9:._-]{3,127}$" & =~":"
scope:   "self" | "institution" | "household"
// Intake alone is the customer's statement; an export needs the owner's authorization.
grant_ref:  string & !=""
grant_kind: "customer-statement" | "export-authorization"

facts: {
	source_panel?:    string & !=""
	site_count?:      int & >=0
	domain_list?:     [...=~"^[a-z0-9.-]+\\.[a-z0-9.-]+$"]
	subdomain_count?: int & >=0
	mailbox_count?:   int & >=0
	database_count?:  int & >=0
	dns_provider?:    string & !=""
	backup_location?: string & !=""
}
// Every stated fact names where it came from.
provenance: {[#Fact]: #From}
disagreements: [...{fact: #Fact, intake: string, export: string}]
unparsed_answers: [...#Fact]
inputs: [...(#Intake | #Export)] & [_, ...]
secret_fields_dropped: int & >=0
panel_error_results:   int & >=0
not_read_from_exports: [...#Fact]
effects: {network: false, panel_connection: false, cms_write: false, publication: false}

if grant_kind == "customer-statement" {
	inputs: [#Intake]
}
