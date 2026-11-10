// Constraint for data/migration-brief-types.json, read by `bonfyre-brief cited`.
// Check: cue vet -c cmd/BonfyreBrief/data/migration-brief-types.cue cmd/BonfyreBrief/data/migration-brief-types.json
package brief

#Atom: =~"^[a-z0-9][a-z0-9.-]{0,63}$"

#Criterion: {
	id:    #Atom
	text:  string & !=""
	needs: [...=~"^[a-z][a-z0-9_]{0,47}$"] & [_, ...]
	// The DatabaseBridge receipt kind that bears on this criterion, when one is cited.
	evidence?: "schema-map" | "map-verify"
}

#Estimate: {
	low:        number & >0
	high:       number & >=low
	currency:   "USD"
	unit:       string & !=""
	scale_fact: =~"^[a-z][a-z0-9_]{0,47}$"
	basis:      string & !=""
}

#Task: {
	operator: =~"^queue\\.migration\\.[a-z0-9-]+$"
	title:    string & !=""
}

#Type: {
	id:          #Atom
	title:       string & !=""
	recipient:   string & !=""
	collections: [...=~"^[a-z0-9-]+/[a-z0-9.-]+$"] & [_, ...]
	families:    [...=~"^software-documentation:[a-z]+$"] & [_, ...]
	selectors:   [...string & =~"^[a-z0-9 .-]{2,32}$"] & [_, ...]
	criteria:    [...#Criterion] & [_, ...]
	estimate:    #Estimate
	tasks:       [...#Task] & [_, ...]
}

schema:           "bonfyre.migration-brief-types.v1"
program_template: #Atom
estimate_label:   string & !=""
cannot_infer:     [...string] & [_, ...]
types:            [...#Type] & [_, _, _, _, _, _, ...]
