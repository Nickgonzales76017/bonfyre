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

#Store: {
	fragments: [...#Fragment]
	stores?: [=~"^[a-z][a-z0-9_]*$"]: #StoreDecl
}

store_schemas: [=~"^(cmd|components)/Bonfyre[A-Za-z0-9]+$"]: #Store

// ---- store kinds and durability -------------------------------------------
//
// A store is more than its DDL. Each owner may declare the stores it opens:
// the physical kind, and what the store promises about its journal. The
// compiler emits, per declared store, BF_STORE_<NAME>_KIND, the kind's shape
// constants, and BF_STORE_<NAME>_DURABILITY, a BfStoreDurability initializer
// (lib/libbonfyre/include/bf_store_durability.h) that the owner applies at
// open. Every owner listed in opened_by gets the same constants in its own
// header, so a store shared by a writer and a reader is declared once.

#StoreKindName: "sqlite" | "vector" | "ranker" | "columnar" | "dynamic"

#Durability: {
	journal_mode: *"WAL" | "DELETE" | "MEMORY" | "OFF"
	synchronous:  *"NORMAL" | "FULL" | "OFF"
	// Pages after which a commit runs a passive checkpoint.
	wal_autocheckpoint: int & >=0 | *1000
	// Bytes the WAL keeps after it restarts; -1 keeps all.
	journal_size_limit: int & >=-1 | *67108864
	// Pages after which a commit forces a TRUNCATE checkpoint, waiting for
	// readers on older snapshots. Must sit above the autocheckpoint, or every
	// commit would wait on readers.
	wal_truncate_pages: int & >0 | *16384
	busy_timeout_ms:    int & >=0 | *5000
	// Longest a request may hold the store open; a statement still running
	// past it is interrupted, and the request's end closes what it left.
	read_txn_max_ms: int & >0 | *120000
	if journal_mode == "WAL" {wal_truncate_pages: >wal_autocheckpoint}
}

#StoreDecl: {
	kind: #StoreKindName
	// Constant prefix: BF_STORE_<macro>_KIND, _DURABILITY ...
	macro: =~"^[A-Z0-9_]+$"
	// The environment variable that names the store's file, when one does.
	env?: =~"^[A-Z][A-Z0-9_]*$"
	// Other owners that open the same file; they get the same constants.
	opened_by: [...=~"^(cmd|components)/Bonfyre[A-Za-z0-9]+$"] | *[]
	// What the owner applies at open. Left out when the owner does not apply
	// a policy yet; a declared policy that nothing applies would be a claim.
	durability?: #Durability
	// Opened by attaching it to another connection under a fixed schema name;
	// the compiler emits _ATTACH, bound with the file at ?1. Read-only
	// attaches go through bf_store_attach() as a file: URI with mode=ro.
	attach?: {
		as:       =~"^[a-z_][a-z0-9_]*$"
		readonly: bool | *true
	}
	if kind == "vector" {
		vector: {
			table: =~"^[a-z_][a-z0-9_]*$"
			// A fixed width, or "runtime" when the owner learns it from the
			// model it loads; then _VECTOR_DDL is a format taking the width.
			dimension: int & >0 | "runtime"
			// sqlite-vec vec0 measures l2 unless told otherwise.
			metric:       *"l2" | "cosine" | "l1"
			quantization: "float32" | "int8" | "bit"
			// The embed model passport whose vectors the table holds.
			model: string & !=""
		}
	}
	if kind == "ranker" {
		ranker: {
			// The declared key space the ranker orders, and its width.
			key_space: string & !=""
			key_bits:  int & >0
			weights:   string & !=""
		}
	}
	if kind == "columnar" {
		columnar: {
			format: "parquet" | "arrow"
			// Cold files, then the hot SQLite table that shadows them.
			cold_glob: string & !=""
			hot_table: =~"^[a-z_][a-z0-9_]*$"
			columns: [...{name: =~"^[a-z_][a-z0-9_]*$", type: "int64" | "float64" | "utf8" | "bool" | "timestamp"}] & [_, ...]
			// When a cold row is promoted to the hot table.
			promote: string & !=""
		}
	}
	if kind == "dynamic" {
		dynamic: {
			// The table whose rows declare the shape of further rows.
			declaring_table: =~"^[a-z_][a-z0-9_]*$"
			// tenant: one attached file per tenant; site: per site; none: rows only.
			shard: "tenant" | "site" | "none"
			columns: [...=~"^[a-z_][a-z0-9_]*$"] & [_, ...]
		}
	}
}

