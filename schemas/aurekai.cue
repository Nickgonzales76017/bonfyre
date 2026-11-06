// CUE schemas for the AUREKAI generation, not the legacy Bonfyre shapes.
//
// The session's CUE work vetted partner profiles, funding routes, invariants
// and capabilities in ~/.bonfyre state. Those are legacy-layer objects. The
// product's own registry -- command-truth-matrix, capabilities, product-map --
// had no schema at all, which is how a command could declare
// execution_state 'hyper-delegated' while no implementation existed anywhere
// in the 49-repo org.

package aurekai

import "list"

#ExecutionState: "native" | "hyper-delegated" | "integration-only"

// A command may only claim hyper-delegated if something can actually serve it.
// Seven commands currently claim it with no implementation in any repo, and
// akai's own fallback resolves to a symlink pointing back at akai.
#Command: {
	command:         string & !=""
	execution_state: #ExecutionState
	family?:         string
	if execution_state == "hyper-delegated" {
		// a delegated command must name what serves it
		delegate_target?: string & !=""
	}
}

#Family: {
	id:       string & =~"^[a-z][a-z0-9_-]*$"
	commands: [...string]
	// families carry presentation fields too; accept them rather than
	// forcing the registry to match a schema written from a partial read
	...
}

#CapabilityRegistry: {
	schema_version: string & !=""
	runtime: {
		name:           string & !=""
		cli:            string & !=""
		binary_prefix:  string & !=""
		native_prefix:  string & !=""
		release:        string & !=""
		...
	}
	families: [...#Family] & list.MinItems(1)
	...
}
