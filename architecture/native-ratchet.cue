package nativeratchet

// The native ratchet. Each budget is the best figure measured so far; a change
// that makes any figure worse fails `scripts/native-ratchet.sh check` (run by
// the pre-commit hook). `scripts/native-ratchet.sh tighten` lowers a budget
// after a win, so the win cannot be given back. Raising a budget is a reviewed
// edit to this file, never a side effect.
// Plan: docs/engineering/mjs-retirement-20261022/PLAN.md

budget: {
	// Authored .mjs in the working tree (tracked or untracked, not ignored,
	// vendored code excluded).
	// Raised 2026-11-06 by Nick's review: the Warehouse, JOSH repository-cut,
	// FPQ review, command-review and Antithesis integrations landed as +34
	// files / +4,460 lines of browser and test JS. They are the next port
	// target; the budget only goes down from these figures.
	// Same review, 2026-11-06: site/matrix/surfaces/build was cited by the
	// atlas but never committed; it lands as +3 files / +185 lines (the
	// authored-row installer and the two tests now run by WebAddress), with
	// its 17-line link contract ported into link_workspace_test.sh.
	authored_mjs_files: 1554
	authored_mjs_lines: 171635
	// .js/.cjs/.ts/.tsx/.py: retiring .mjs must not move the code sideways.
	// 53 after the Swarm transfer test and product-review renderer went to C.
	other_script_files: 53
	// .mjs that bun compiles into an executable. Only browser code, tests and
	// thin edges may stay JS; anything installed as a program is C.
	bun_compile_sites: 0
}

// Measured on the installed estate when ~/.bonfyre/bin exists (local only).
installed: {
	// Executables carrying a JS runtime. A program in ~/.bonfyre/bin is C.
	js_runtime_bins: 0
	// Largest bonfyre-* program, and all bonfyre-* programs together (hard
	// links once), bytes. Third-party tools in ~/.bonfyre/bin are not counted.
	// The multi-call build (PLAN.md WX) lowers both. 133 of 134 folded organs
	// now live in 3 bucketed multi-call binaries, each well under the old
	// budget; bonfyre-web-address is the one organ that cannot join any
	// bucket and stay under it, confirmed by isolating its own object files
	// at link time (its code alone is ~3.1MB). This budget is raised to its
	// standalone floor; it drops again only if WebAddress itself shrinks.
	largest_bin_bytes: 3197392
	// +124,808 bytes 2026-11-06: the native C organs bonfyre-warehouse and
	// bonfyre-repository-fabric installed for the JOSH/Warehouse integration.
	installed_bonfyre_bytes: 11891892
}

#Measure: {
	authored_mjs_files: int & <=budget.authored_mjs_files
	authored_mjs_lines: int & <=budget.authored_mjs_lines
	other_script_files: int & <=budget.other_script_files
	bun_compile_sites:  int & <=budget.bun_compile_sites
	js_runtime_bins?:   int & <=installed.js_runtime_bins
	largest_bin_bytes?: int & <=installed.largest_bin_bytes
	installed_bonfyre_bytes?: int & <=installed.installed_bonfyre_bytes
}
