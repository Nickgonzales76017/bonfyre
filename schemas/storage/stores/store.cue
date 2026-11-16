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

// What a store promises a reader that does not write it: how long an open
// waits on a held lock and how long a request may hold it. A store declared
// with lease and no durability compiles to a BfStoreDurability with no journal
// policy, so opening it read-write never changes the writer's pragmas.
#Lease: {
	busy_timeout_ms: int & >=0 | *1000
	read_txn_max_ms: int & >0 | *120000
}

#Ident: =~"^[a-z_][a-z0-9_]*$"

#StoreDecl: {
	kind: #StoreKindName
	// Constant prefix: BF_STORE_<macro>_KIND, _DURABILITY ...
	macro: =~"^[A-Z0-9_]+$"
	// The environment variable that names the store's file, when one does.
	env?: =~"^[A-Z][A-Z0-9_]*$"
	// The program that writes the file, when the declarer only reads it.
	written_by?: string & !=""
	// Other owners that open the same file; they get the same constants.
	opened_by: [...=~"^(cmd|components)/Bonfyre[A-Za-z0-9]+$"] | *[]
	// What the owner applies at open. Left out when the owner does not apply
	// a policy yet; a declared policy that nothing applies would be a claim.
	durability?: #Durability
	// The reader's side only (see #Lease). A store has durability or lease.
	lease?: #Lease
	if durability != _|_ {lease?: _|_}
	// Opened by attaching it to another connection under a fixed schema name.
	// engine sqlite: _ATTACH is "ATTACH DATABASE ? AS <as>", bound with the
	// file at ?1; read-only attaches go through bf_store_attach() as a file:
	// URI with mode=ro. engine duckdb: _DUCKDB_ATTACH is the DuckDB sqlite
	// scanner statement, a format taking the file once.
	attach?: {
		as:       #Ident
		readonly: bool | *true
		engine:   *"sqlite" | "duckdb"
	}
	if kind == "vector" {
		vector: {
			table: #Ident
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
		// A learned order over a key space someone else bounded. The run table
		// says which representation ordered which frontier; the frontier
		// table holds every key of the run at its rank, so a later slice
		// widens the attempt without rerunning the model.
		ranker: {
			key_space:      string & !=""
			run_table:      #Ident
			frontier_table: #Ident
			run_column:     #Ident | *"run_id"
			key_column:     #Ident
			order_column:   #Ident
			// The representation passport whose vectors order the keys.
			weights: string & !=""
		}
	}
	if kind == "columnar" {
		columnar: {
			format: "parquet" | "arrow"
			// Where cold files live (a glob, or "argument" when the caller names them).
			cold_glob: string & !=""
			// The cold schema every file must carry (a file may carry more).
			columns: [...{name: #Ident, type: "int32" | "int64" | "float64" | "utf8" | "bool" | "timestamp"}] & [_, ...]
			// The order a cold file is written in.
			order_by?: #Ident
			// The SQLite table that shadows the cold rows, when one does.
			//   ddl fragment: the owner's own fragment creates it; the hot
			//     table is the source and the cold file its lossless export.
			//   ddl columns: the compiler emits _COLUMNAR_HOT_DDL from columns.
			hot?: {
				table:     #Ident
				attach_as: #Ident | *"hot"
				ddl:       "fragment" | "columns"
			}
			// When a cold row is promoted to (or restored into) a hot table.
			promote: string & !=""
		}
	}
	if kind == "dynamic" {
		dynamic: {
			// How further stores join at run time.
			//   named: a declared set of joins, each attached when present;
			//   tenant / site: one file per tenant or site, named by rows of
			//   declaring_table; none: the rows only.
			shard: "named" | "tenant" | "site" | "none"
			if shard == "named" {
				joins: [...{
					as:  #Ident
					env?: =~"^[A-Z][A-Z0-9_]*$"
					// The table whose presence makes the join usable.
					probe?:   #Ident
					required: bool | *false
				}] & [_, ...]
			}
			if shard != "named" {
				// The table whose rows declare the shape of further rows.
				declaring_table: #Ident
				columns: [...#Ident] & [_, ...]
			}
		}
	}
}
