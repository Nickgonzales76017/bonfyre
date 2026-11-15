package stores

// cmd/BonfyreFrappeCompiler: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreFrappeCompiler/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreFrappeCompiler": {
	fragments: [
		{
			macro:  "BF_STORE_FRAPPE_COMPILER_FRAPPE_PROGRAM"
			source: "cmd/BonfyreFrappeCompiler/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "frappe_program", sql: "CREATE TABLE IF NOT EXISTS frappe_program( program_id TEXT PRIMARY KEY, slug TEXT NOT NULL, app TEXT NOT NULL, doctype TEXT NOT NULL, module TEXT, title_field TEXT, description TEXT, submittable INT, child INT, single INT, tree INT, read_only INT, guest_readable INT, field_count INT, rendered INT, withheld INT, layout INT, organs TEXT, cannot_infer TEXT);"},
				{kind: "table", name: "frappe_field", sql: "CREATE TABLE IF NOT EXISTS frappe_field( program_id TEXT NOT NULL, ord INT NOT NULL, tab TEXT, tab_ord INT, tab_empty INT, section TEXT, section_ord INT, collapsible INT, column_ord INT, field TEXT, label TEXT, presentation TEXT, organ TEXT, required INT, read_only INT, in_list_view INT, reference TEXT, depends_on TEXT, withheld TEXT, PRIMARY KEY(program_id, ord));"},
				{kind: "index", name: "frappe_program_app", sql: "CREATE INDEX IF NOT EXISTS frappe_program_app ON frappe_program(app);"},
				{kind: "index", name: "frappe_program_slug", sql: "CREATE UNIQUE INDEX IF NOT EXISTS frappe_program_slug ON frappe_program(slug);"},
				{kind: "index", name: "frappe_field_program", sql: "CREATE INDEX IF NOT EXISTS frappe_field_program ON frappe_field(program_id, ord);"},
			]
		},
		{
			macro:  "BF_STORE_FRAPPE_COMPILER_FRAPPE_CONDITION"
			source: "cmd/BonfyreFrappeCompiler/src/main.c"
			statements: [
				{kind: "table", name: "frappe_condition", sql: "CREATE TABLE IF NOT EXISTS frappe_condition( program_id TEXT NOT NULL, app TEXT NOT NULL, field TEXT NOT NULL, property TEXT NOT NULL, status TEXT NOT NULL, plan_id TEXT, plan TEXT, kind TEXT NOT NULL, said TEXT, reads TEXT, missing_operation TEXT, source TEXT NOT NULL, PRIMARY KEY(program_id, field, property));"},
				{kind: "index", name: "frappe_condition_plan", sql: "CREATE INDEX IF NOT EXISTS frappe_condition_plan ON frappe_condition(plan_id);"},
			]
		},
		{
			macro:  "BF_STORE_FRAPPE_COMPILER_FRAPPE_METRIC"
			source: "cmd/BonfyreFrappeCompiler/src/main.c"
			statements: [
				{kind: "table", name: "frappe_metric", sql: "CREATE TABLE IF NOT EXISTS frappe_metric( app TEXT NOT NULL, metric TEXT NOT NULL, kind TEXT NOT NULL, doctype TEXT NOT NULL, status TEXT NOT NULL, said TEXT, needs TEXT, missing_operation TEXT, definition TEXT, PRIMARY KEY(app, kind, metric));"},
				{kind: "index", name: "frappe_metric_doctype", sql: "CREATE INDEX IF NOT EXISTS frappe_metric_doctype ON frappe_metric(doctype);"},
			]
		},
	]
}
