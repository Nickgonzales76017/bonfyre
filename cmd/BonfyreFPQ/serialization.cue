package fpq

import "list"

// Metadata contract for the existing FPQ! serializer. The native test projects
// real emitted files and legacy fixtures into this shape. CUE checks their
// header and dictionary relations, not binary framing, combinator semantics,
// float bits, reconstruction quality, or parser resource bounds.
//
// All integers and float payloads on disk are little endian. After the four
// u32 header fields (magic, version, tensor count, flags), each tensor stores
// its name and dimensions/counters. Versions 3 and 4 then store mode and an
// optional full base seed. Flag 0 stores radius/full seed/QJL per block.
// Version 4 flag 1 stores a u32 dictionary count, (dimension, node count, tree)
// entries, then radius/u32 index/distortion/QJL per block. Distortion belongs
// to the block; exact tree bytes and target dimension define dictionary identity.

#Program: {
	target_dim: 255
	node_count: int & >=1 & <=1048576
}
#Block: {
	#Program
	qjl_elements:      255
	qjl_projections:   uint32
	qjl_words:         int & >=0 & >=(qjl_projections / 64) & <(qjl_projections/64 + 1)
	dictionary_index?: uint32
}
#Tensor: {
	rows:             int & >=0
	cols:             int & >=0
	block_count:      int & >=0 & >=(rows * cols / 256) & <(rows*cols/256 + 1)
	mode:             0 | 1 // FWHT or DIRECT; FPQ0 owns coordinate payloads.
	has_base:         bool
	dictionary_count: uint32 & <=block_count
	dictionary: [...#Program] & list.MinItems(dictionary_count) & list.MaxItems(dictionary_count)
	blocks: [...#Block] & list.MinItems(block_count) & list.MaxItems(block_count)
}
#File: {
	magic:        0x21515046 // FPQ!
	version:      1 | 2 | 3 | 4
	flags:        0 | 1
	layout:       "per-block" | "dictionary"
	tensor_count: uint32
	tensors: [...#Tensor] & list.MinItems(tensor_count) & list.MaxItems(tensor_count)
	if flags == 1 {
		version: 4
		layout:  "dictionary"
		tensors: [for tensor in tensors {
			if tensor.block_count > 0 {dictionary_count: >=1}
			blocks: [for block in tensor.blocks {
				dictionary_index: uint32 & <tensor.dictionary_count
				target_dim:       tensor.dictionary[block.dictionary_index].target_dim
				node_count:       tensor.dictionary[block.dictionary_index].node_count
			}]
		}]
	}
	if flags == 0 {
		layout: "per-block"
		tensors: [for tensor in tensors {
			dictionary_count: 0
			blocks: [for block in tensor.blocks {dictionary_index?: _|_}]
			if version < 3 {mode: 0, has_base: false}
		}]
	}
}

files: [...#File]
