package stores

// The stores the WebAddress serve workers open while answering a request,
// besides the CMS store (declared by cmd/BonfyreCMS). WebAddress writes few of
// them; each is declared here with the reader's lease (#Lease): the busy
// timeout every open gets and the longest a request may hold the file. Every
// open goes through bf_store_open() (lib/libbonfyre/include/bf_store_durability.h),
// which leases the connection to the request that opened it; the serve loop
// closes whatever a request left open. A store's writer keeps its own journal
// policy: a lease names none, so a read-write open here never changes it.
//
// written_by names the program whose file it is; when that owner declares the
// store itself, the declaration moves there and WebAddress joins opened_by.

store_schemas: "cmd/BonfyreWebAddress": stores: {
	auth: {kind: "sqlite", macro: "WEB_AUTH", env: "BONFYRE_AUTH_DB", written_by: "bonfyre-auth", lease: {}}
	matrix: {kind: "sqlite", macro: "WEB_MATRIX", env: "BONFYRE_MATRIX_DB", written_by: "the matrix closure compiler (engine/core)", lease: {}}
	source_frontier: {kind: "sqlite", macro: "WEB_SOURCE_FRONTIER", env: "BONFYRE_SOURCE_FRONTIER_DB", written_by: "bonfyre-source-frontier", lease: {}}
	participation: {kind: "sqlite", macro: "WEB_PARTICIPATION", env: "BONFYRE_PARTICIPATION_DB", written_by: "bonfyre-participation (BONFYRE_DATING_DB names the same file on older hosts)", lease: {}}
	account_work: {kind: "sqlite", macro: "WEB_ACCOUNT_WORK", env: "BONFYRE_ACCOUNT_WORK_DB", written_by: "bonfyre-queue account work", lease: {}}
	operator: {kind: "sqlite", macro: "WEB_OPERATOR", written_by: "bonfyre-operator-store (beside the account work index)", lease: {busy_timeout_ms: 3000}}
	item_archive: {kind: "sqlite", macro: "WEB_ITEM_ARCHIVE", env: "BONFYRE_ITEM_ARCHIVE_DB", written_by: "the item archive builder", lease: {busy_timeout_ms: 1500}}
	finance: {kind: "sqlite", macro: "WEB_FINANCE", env: "BONFYRE_FINANCE_DB", written_by: "bonfyre-finance", lease: {}}
	held_message: {kind: "sqlite", macro: "WEB_HELD_MESSAGE", env: "BONFYRE_HELD_MESSAGE_DB", written_by: "the outbound message hold", lease: {}}
	browser_queue: {kind: "sqlite", macro: "WEB_BROWSER_QUEUE", env: "BONFYRE_BROWSER_QUEUE_DB", written_by: "the browser page queue", lease: {}}
	internet: {kind: "sqlite", macro: "WEB_INTERNET", env: "BONFYRE_INTERNET_DB", written_by: "WebAddress internet observatory", lease: {}}
	websub: {kind: "sqlite", macro: "WEB_WEBSUB", env: "BONFYRE_WEBSUB_DB", written_by: "WebAddress WebSub subscriber", lease: {}}
	surfaceir: {kind: "sqlite", macro: "WEB_SURFACEIR", env: "BONFYRE_SURFACEIR_DB", written_by: "WebAddress room composer", lease: {busy_timeout_ms: 2000}}
	latent_atlas: {kind: "sqlite", macro: "WEB_LATENT_ATLAS", env: "BONFYRE_LATENT_ATLAS_DB", written_by: "bonfyre-latent-atlas", lease: {}}
	commitment: {kind: "sqlite", macro: "WEB_COMMITMENT", env: "BONFYRE_COMMITMENT_DB", written_by: "the commitment owner (engine/core)", lease: {}}
	commerce: {kind: "sqlite", macro: "WEB_COMMERCE", env: "BONFYRE_COMMERCE_DB", written_by: "the commerce owner (engine/core)", lease: {}}
	model_catalog: {kind: "sqlite", macro: "WEB_MODEL_CATALOG", env: "BONFYRE_MODEL_CATALOG", written_by: "bonfyre-model", lease: {}}
	prices: {kind: "sqlite", macro: "WEB_PRICES", env: "BONFYRE_PRICES_DB", written_by: "the price observation index", lease: {}}
	meter: {kind: "sqlite", macro: "WEB_METER", env: "BONFYRE_METER_DB", written_by: "bonfyre-meter", lease: {}}
	pay: {kind: "sqlite", macro: "WEB_PAY", written_by: "bonfyre-pay (<home>/pay.db)", lease: {}}
	commitments_home: {kind: "sqlite", macro: "WEB_COMMITMENTS_HOME", written_by: "the commitment owner (<home>/commitments.db)", lease: {}}
	public_projection: {kind: "sqlite", macro: "WEB_PUBLIC_PROJECTION", written_by: "WebAddress public projection cache", lease: {busy_timeout_ms: 2000}}
	frappe_surface: {kind: "sqlite", macro: "WEB_FRAPPE_SURFACE", env: "BONFYRE_FRAPPE_SURFACE_DB", written_by: "bonfyre-frappe-compiler", lease: {}}
	detail_depth: {kind: "sqlite", macro: "WEB_DETAIL_DEPTH", env: "BONFYRE_DETAIL_DEPTH_DB", written_by: "bonfyre-web-address detail-depth build", lease: {}}
	subject_fabric: {kind: "sqlite", macro: "WEB_SUBJECT_FABRIC", env: "BONFYRE_SUBJECT_FABRIC_DB", written_by: "the subject fabric compiler", lease: {}}
	entity: {kind: "sqlite", macro: "WEB_ENTITY", env: "BONFYRE_ENTITY_DB", written_by: "bonfyre-entity", lease: {}}
	membrane: {kind: "sqlite", macro: "WEB_MEMBRANE", env: "BONFYRE_MEMBRANE_DB", written_by: "the financial membrane", lease: {}}
	// A content-addressed receipt pack, held open for the life of the process
	// (bf_store_open_held): immutable, read-only, never leased.
	receipt_pack: {kind: "sqlite", macro: "WEB_RECEIPT_PACK", env: "BONFYRE_FOREIGN_RECEIPT_PACK", written_by: "the ForeignTwin receipt packer", lease: {}}

	// Fleming's workgraph, read by DuckDB's sqlite scanner (bonfyre-query) as w.
	fleming_work: {kind: "sqlite", macro: "WEB_FLEMING_WORK", written_by: "Fleming (<fleming root>/fleming-work.db)", lease: {}, attach: {as: "w", engine: "duckdb"}}
	// The economy and finance stores joined by the financial membrane query.
	economy_membrane: {kind: "sqlite", macro: "WEB_ECONOMY_SCAN", env: "BONFYRE_ECONOMY_DB", written_by: "bonfyre-economy", attach: {as: "ec", engine: "duckdb"}}
	finance_membrane: {kind: "sqlite", macro: "WEB_FINANCE_SCAN", env: "BONFYRE_FINANCE_DB", written_by: "bonfyre-finance", attach: {as: "fi", engine: "duckdb"}}
	// Any estate SQLite file the observatory queries through bonfyre-query, as e.
	estate_scan: {kind: "sqlite", macro: "WEB_ESTATE_SCAN", written_by: "whichever owner the caller names", attach: {as: "e", engine: "duckdb"}}

	// The item page's depth: one read-only connection to the item archive,
	// joined by the stores that deepen an item, each used only when its probe
	// table is present. bf_store_join() attaches them by these rows.
	item_depth: {
		kind:       "dynamic"
		macro:      "WEB_ITEM_DEPTH"
		env:        "BONFYRE_ITEM_ARCHIVE_DB"
		written_by: "the item archive builder"
		lease: {busy_timeout_ms: 1500}
		dynamic: {
			shard: "named"
			joins: [
				{as: "fr", env: "BONFYRE_SOURCE_FRONTIER_DB", probe: "channel"},
				{as: "fp", env: "BONFYRE_FRAPPE_SURFACE_DB", probe: "frappe_program"},
				{as: "dd", env: "BONFYRE_DETAIL_DEPTH_DB", probe: "item_facet"},
				{as: "jt", env: "BONFYRE_CMS_DB", probe: "internet_source"},
			]
		}
	}
	// The cms copy a people projection reads beside its own store.
	people_cms: {kind: "sqlite", macro: "WEB_PEOPLE_CMS", env: "BONFYRE_CMS_DB", written_by: "bonfyre-cms", attach: {as: "cms_source"}}

	// Foreign observations kept cold as Parquet; the source page counts them
	// with DuckDB and keeps no hot copy.
	foreign_observations: {
		kind:       "columnar"
		macro:      "WEB_FOREIGN_OBSERVATIONS"
		written_by: "the ForeignTwin observation exporter"
		columnar: {
			format:    "parquet"
			cold_glob: "argument: the observation Parquet file the source page is given"
			columns: [
				{name: "src_id", type: "utf8"},
				{name: "page_sha256", type: "utf8"},
				{name: "candidate_id", type: "utf8"},
				{name: "observed_at", type: "utf8"},
			]
			promote: "never: observations stay cold; the page counts them in place"
		}
	}
}
