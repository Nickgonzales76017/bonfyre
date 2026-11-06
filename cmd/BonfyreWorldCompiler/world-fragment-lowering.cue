package worldcompiler

// World.grammars is authoring intent. Lowering turns each authored grammar into
// one presentation-only WorldFragment plan; it never imports source truth,
// provider admission, actor authority, payment authority, or an effect grant.
#WorldFragmentLowerer: {
	input_grammar: string & != ""
	surface_grammar: string & != ""
	renderer_family: string & != ""
	renderer_variant: string & != ""
	extent: "page" | "section" | "workspace"
}

nativeWorldFragmentLowerers: [
	{input_grammar: "discover", surface_grammar: "discover", renderer_family: "vertical", renderer_variant: "faceted_stream", extent: "page"},
	{input_grammar: "stage", surface_grammar: "stage", renderer_family: "stage", renderer_variant: "calendar_timeline", extent: "page"},
	{input_grammar: "channels", surface_grammar: "channels", renderer_family: "thread", renderer_variant: "stream_topics", extent: "section"},
	{input_grammar: "community", surface_grammar: "community", renderer_family: "community", renderer_variant: "activity_body", extent: "section"},
	{input_grammar: "board", surface_grammar: "board", renderer_family: "board", renderer_variant: "masonry_collection", extent: "page"},
	{input_grammar: "spatial", surface_grammar: "spatial", renderer_family: "spatial", renderer_variant: "map_layers", extent: "page"},
	{input_grammar: "listings", surface_grammar: "listings", renderer_family: "market", renderer_variant: "availability_compare", extent: "page"},
	{input_grammar: "search", surface_grammar: "discover", renderer_family: "search", renderer_variant: "query_results", extent: "page"},
	{input_grammar: "inbox", surface_grammar: "inbox", renderer_family: "work", renderer_variant: "obligation_timeline", extent: "workspace"},
	{input_grammar: "territory", surface_grammar: "territory", renderer_family: "inspector", renderer_variant: "address_inspector", extent: "page"},
	{input_grammar: "observations", surface_grammar: "observations", renderer_family: "thread", renderer_variant: "observation_timeline", extent: "section"},
	{input_grammar: "worlds", surface_grammar: "worlds", renderer_family: "board", renderer_variant: "world_shelf", extent: "page"},
	{input_grammar: "packs", surface_grammar: "packs", renderer_family: "work", renderer_variant: "dependency_kanban", extent: "workspace"},
	{input_grammar: "link", surface_grammar: "link", renderer_family: "inspector", renderer_variant: "relationship_inspector", extent: "page"},
	{input_grammar: "studio", surface_grammar: "studio", renderer_family: "work", renderer_variant: "composition_kanban", extent: "workspace"},
	{input_grammar: "console", surface_grammar: "console", renderer_family: "inspector", renderer_variant: "observatory", extent: "workspace"},

	// Historical World rows used renderer-family words in `grammars`. They are
	// accepted only as explicit aliases and lower into the stable public grammar.
	{input_grammar: "vertical", surface_grammar: "discover", renderer_family: "vertical", renderer_variant: "faceted_stream", extent: "page"},
	{input_grammar: "thread", surface_grammar: "channels", renderer_family: "thread", renderer_variant: "stream_topics", extent: "section"},
	{input_grammar: "market", surface_grammar: "listings", renderer_family: "market", renderer_variant: "availability_compare", extent: "page"},
	{input_grammar: "work", surface_grammar: "inbox", renderer_family: "work", renderer_variant: "obligation_timeline", extent: "workspace"},
	{input_grammar: "inspector", surface_grammar: "territory", renderer_family: "inspector", renderer_variant: "address_inspector", extent: "page"},
]
