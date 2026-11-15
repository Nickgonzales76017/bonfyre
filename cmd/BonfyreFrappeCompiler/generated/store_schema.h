/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREFRAPPECOMPILER_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREFRAPPECOMPILER_H
/* cmd/BonfyreFrappeCompiler/src/main.c */
#define BF_STORE_FRAPPE_COMPILER_FRAPPE_PROGRAM \
    "PRAGMA journal_mode=WAL;" \
    "CREATE TABLE IF NOT EXISTS frappe_program( program_id TEXT PRIMARY KEY, slug TEXT NOT NULL, app TEXT NOT NULL, doctype TEXT NOT NULL, module TEXT, title_field TEXT, description TEXT, submittable INT, child INT, single INT, tree INT, read_only INT, guest_readable INT, field_count INT, rendered INT, withheld INT, layout INT, organs TEXT, cannot_infer TEXT);" \
    "CREATE TABLE IF NOT EXISTS frappe_field( program_id TEXT NOT NULL, ord INT NOT NULL, tab TEXT, tab_ord INT, tab_empty INT, section TEXT, section_ord INT, collapsible INT, column_ord INT, field TEXT, label TEXT, presentation TEXT, organ TEXT, required INT, read_only INT, in_list_view INT, reference TEXT, depends_on TEXT, withheld TEXT, PRIMARY KEY(program_id, ord));" \
    "CREATE INDEX IF NOT EXISTS frappe_program_app ON frappe_program(app);" \
    "CREATE UNIQUE INDEX IF NOT EXISTS frappe_program_slug ON frappe_program(slug);" \
    "CREATE INDEX IF NOT EXISTS frappe_field_program ON frappe_field(program_id, ord);"
/* cmd/BonfyreFrappeCompiler/src/main.c */
#define BF_STORE_FRAPPE_COMPILER_FRAPPE_CONDITION \
    "CREATE TABLE IF NOT EXISTS frappe_condition( program_id TEXT NOT NULL, app TEXT NOT NULL, field TEXT NOT NULL, property TEXT NOT NULL, status TEXT NOT NULL, plan_id TEXT, plan TEXT, kind TEXT NOT NULL, said TEXT, reads TEXT, missing_operation TEXT, source TEXT NOT NULL, PRIMARY KEY(program_id, field, property));" \
    "CREATE INDEX IF NOT EXISTS frappe_condition_plan ON frappe_condition(plan_id);"
/* cmd/BonfyreFrappeCompiler/src/main.c */
#define BF_STORE_FRAPPE_COMPILER_FRAPPE_METRIC \
    "CREATE TABLE IF NOT EXISTS frappe_metric( app TEXT NOT NULL, metric TEXT NOT NULL, kind TEXT NOT NULL, doctype TEXT NOT NULL, status TEXT NOT NULL, said TEXT, needs TEXT, missing_operation TEXT, definition TEXT, PRIMARY KEY(app, kind, metric));" \
    "CREATE INDEX IF NOT EXISTS frappe_metric_doctype ON frappe_metric(doctype);"
#endif
