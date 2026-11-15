package stores

// Every native owner's SQLite store DDL, declared once. Each fragment is the
// exact text one owner hands to SQLite (sqlite3_exec, a prepared statement
// or a format string); scripts/compile-store-schemas.sh compiles each owner's
// fragments to <owner>/generated/store_schema.h as string macros, so a store
// created by the owner is byte-for-byte what the hand-written literal made.
// Statements are split at top-level semicolons; joined in order they are the
// fragment. kind and name make the registry readable by machines: which
// owner creates which table, index, view or trigger.

#Kind: "table" | "virtual_table" | "index" | "view" | "trigger" | "other"

#Statement: {
	kind: #Kind
	name: string
	sql:  string & !=""
	if kind == "table" {sql: =~"(?i)^\\s*CREATE\\s+(TEMP(ORARY)?\\s+)?TABLE\\s"}
	if kind == "virtual_table" {sql: =~"(?i)^\\s*CREATE\\s+VIRTUAL\\s+TABLE\\s"}
	if kind == "index" {sql: =~"(?i)^\\s*CREATE\\s+(UNIQUE\\s+)?INDEX\\s"}
	if kind == "view" {sql: =~"(?i)^\\s*CREATE\\s+(TEMP(ORARY)?\\s+)?VIEW\\s"}
	if kind == "trigger" {sql: =~"(?i)^\\s*CREATE\\s+(TEMP(ORARY)?\\s+)?TRIGGER\\s"}
	if kind != "other" {name: !=""}
}

#Fragment: {
	// The C macro the owner's source uses in place of the literal.
	macro: =~"^BF_STORE_[A-Z0-9_]+$"
	// The source file that executes it.
	source: =~"^(cmd|components)/Bonfyre[A-Za-z0-9]+/src/"
	statements: [#Statement, ...#Statement]
}

#Store: fragments: [#Fragment, ...#Fragment]

store_schemas: [=~"^(cmd|components)/Bonfyre[A-Za-z0-9]+$"]: #Store
