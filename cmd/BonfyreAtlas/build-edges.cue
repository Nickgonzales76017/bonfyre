package atlas

import "list"

// Simple Makefile assignment observations, not make evaluation or proof that
// either directory is safe to remove. The native fixture retains JS parity.
families:       uint
both_have_main: uint & <=families
delegating: [...string]
atlas_names_unbuilt_source: [...string]
installed_older_than_source: [...string]
cannot_infer: [string, string, string]
rows: [...{
	family: string
	cmd_builds_from: [...string]
	delegates_to_components:   bool
	both_have_main:            bool
	installed:                 "not installed" | "cmd build" | "components build" | "older than its source" | "another build of this source"
	atlas:                     "no entry" | "names the built source" | "names a source the build does not use"
	unbuilt_beside_build_edge: bool
}] & list.MinItems(families) & list.MaxItems(families)
