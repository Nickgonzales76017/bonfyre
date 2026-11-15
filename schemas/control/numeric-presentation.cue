package control

// The numeric presentation vocabulary: the only layout inputs an authored
// block or a native page builder may carry. One responsive style per viewport,
// each property either a closed enum or an exact integer range. A style is
// content-addressed: sha256(JSON.stringify(["bonfyre.numeric-presentation.v1",
// style])) with viewports and properties in the declared order below, so the
// browser editor, the C compiler and the WebAddress builders agree on one
// address for one layout.
//
// Generated readers: cmd/BonfyreSurfaceScene/tools/compile_numeric_vocabulary.sh
// writes the C header (cmd/BonfyreSurfaceScene/generated/native_numeric_presentation.h)
// and the browser table (components/BonfyreServiceSite/authoring/numeric-presentation.vocabulary.json).
//
// Presentation only. A style cannot pick a source, name a route or account,
// or grant publication; it is the same bytes whoever authored it.

#Viewport: {
	name:  "desktop" | "tablet" | "phone"
	media: string
}

#EnumProperty: {
	name: string
	kind: "enum"
	values: [...string] & [_, ...]
}

#RangeProperty: {
	name: string
	kind: "range"
	min:  int & >=0
	max:  int & >=min
}

numeric_presentation: {
	schema: "bonfyre.numeric-presentation.v1"
	// Order is identity: normalization emits viewports and properties in
	// exactly this order before hashing.
	viewports: [...#Viewport] & [
		{name: "desktop", media: "(min-width:1025px)"},
		{name: "tablet", media: "(min-width:761px) and (max-width:1024px)"},
		{name: "phone", media: "(max-width:760px)"},
	]
	properties: [...(#EnumProperty | #RangeProperty)] & [
		{name: "align", kind: "enum", values: ["start", "center", "end", "stretch", "between"]},
		{name: "background", kind: "enum", values: ["default", "surface", "panel", "muted", "accent", "reversed", "dark", "light"]},
		{name: "columns", kind: "range", min: 1, max: 12},
		{name: "gap", kind: "range", min: 0, max: 64},
		{name: "justify", kind: "enum", values: ["start", "center", "end", "stretch", "between"]},
		{name: "layout", kind: "enum", values: ["stack", "grid", "columns", "carousel", "masonry", "split", "full-bleed", "feature"]},
		{name: "padding", kind: "range", min: 0, max: 64},
		{name: "radius", kind: "range", min: 0, max: 64},
		{name: "span", kind: "range", min: 1, max: 12},
		{name: "typography", kind: "enum", values: ["body", "display", "serif", "sans", "mono", "editorial"]},
		{name: "width", kind: "enum", values: ["auto", "narrow", "medium", "wide", "full"]},
	]
	// Bounds of the numerical selection compiler (HVM4 lowering).
	selection: {
		max_candidates: 12
		fields: ["gap", "padding", "radius", "columns", "span"]
		ops: ["<", "<=", ">", ">=", "=="]
	}
	// Bound on a factored family.
	max_family: 512
	// The derived, unprivileged content-addressed cache. Bodies are shared
	// across CMS scopes; a reference keeps its own namespace, route,
	// coordinate and source revision. It is never the CMS owner.
	store: [
		"CREATE TABLE IF NOT EXISTS _scene_numeric_body (digest TEXT PRIMARY KEY CHECK(length(digest)=64), body TEXT NOT NULL)",
		"CREATE TABLE IF NOT EXISTS _scene_numeric_ref (namespace TEXT NOT NULL, route TEXT NOT NULL, coordinate TEXT NOT NULL,source_revision TEXT NOT NULL, digest TEXT NOT NULL REFERENCES _scene_numeric_body(digest),PRIMARY KEY(namespace,route,coordinate))",
	]
}
