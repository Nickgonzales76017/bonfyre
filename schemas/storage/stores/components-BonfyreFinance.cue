package stores

// components/BonfyreFinance: store DDL, byte-for-byte what the owner executes.
// Compiled to components/BonfyreFinance/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "components/BonfyreFinance": {
	fragments: [
		{
			macro:  "BF_STORE_C_FINANCE_SERVICES"
			source: "components/BonfyreFinance/src/main.c"
			statements: [
				{kind: "table", name: "services", sql: "CREATE TABLE IF NOT EXISTS services (  id INTEGER PRIMARY KEY AUTOINCREMENT,  name TEXT NOT NULL UNIQUE,  category TEXT NOT NULL DEFAULT 'general',  buy_price REAL NOT NULL,  sell_price REAL NOT NULL,  source TEXT NOT NULL,  created_at TEXT NOT NULL);"},
				{kind: "table", name: "service_jobs", sql: "CREATE TABLE IF NOT EXISTS service_jobs (  id INTEGER PRIMARY KEY AUTOINCREMENT,  service_name TEXT NOT NULL,  actual_cost REAL NOT NULL,  sold_for REAL NOT NULL,  quality TEXT NOT NULL DEFAULT 'pass',  notes TEXT DEFAULT '',  completed_at TEXT NOT NULL,  handoff_job_id INTEGER);"},
				{kind: "table", name: "pipeline_jobs", sql: "CREATE TABLE IF NOT EXISTS pipeline_jobs (  id INTEGER PRIMARY KEY AUTOINCREMENT,  input_file TEXT NOT NULL,  job_type TEXT NOT NULL DEFAULT 'transcription',  created_at TEXT NOT NULL,  status TEXT NOT NULL DEFAULT 'pending',  ai_cost REAL DEFAULT 0.0,  review_cost REAL DEFAULT 0.0,  sell_price REAL DEFAULT 0.0,  reviewer TEXT,  qa_result TEXT,  qa_notes TEXT,  reviewed_at TEXT,  output_file TEXT,  source_system TEXT,  source_job_id INTEGER,  source_service TEXT);"},
				{kind: "table", name: "bundles", sql: "CREATE TABLE IF NOT EXISTS bundles (  id INTEGER PRIMARY KEY AUTOINCREMENT,  name TEXT NOT NULL UNIQUE,  components TEXT NOT NULL,  sell_price REAL NOT NULL,  total_cost REAL NOT NULL,  alacarte_total REAL NOT NULL,  target TEXT DEFAULT 'general',  created_at TEXT NOT NULL);"},
			]
		},
	]
}
