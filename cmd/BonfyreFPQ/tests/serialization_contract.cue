package fpq

// Defaults make individual malformed metadata fields replaceable in the
// negative checks. The positive binary witnesses come from serialization.c.
#ContractCase: {
	magic:        0x21515046
	version:      *4 | int
	flags:        *1 | int
	layout:       *"dictionary" | string
	tensor_count: *1 | int
	tensors: [{
		rows:             1
		cols:             256
		block_count:      *1 | int
		mode:             *0 | int
		has_base:         false
		dictionary_count: *1 | int
		dictionary: [{target_dim: 255, node_count: 3}]
		blocks: [{
			dictionary_index: *0 | int
			target_dim:       255
			node_count:       *3 | int
			qjl_elements:     255
			qjl_projections:  65
			qjl_words:        *2 | int
		}]
	}]
}
