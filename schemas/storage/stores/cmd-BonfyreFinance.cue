package stores

// cmd/BonfyreFinance: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreFinance/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreFinance": {
	fragments: [
		{
			macro:  "BF_STORE_FINANCE_SERVICES"
			source: "cmd/BonfyreFinance/src/main.c"
			statements: [
				{kind: "table", name: "services", sql: "CREATE TABLE IF NOT EXISTS services (  id INTEGER PRIMARY KEY AUTOINCREMENT,  name TEXT NOT NULL UNIQUE,  category TEXT NOT NULL DEFAULT 'general',  buy_price REAL NOT NULL,  sell_price REAL NOT NULL,  source TEXT NOT NULL,  created_at TEXT NOT NULL);"},
				{kind: "table", name: "service_jobs", sql: "CREATE TABLE IF NOT EXISTS service_jobs (  id INTEGER PRIMARY KEY AUTOINCREMENT,  service_name TEXT NOT NULL,  actual_cost REAL NOT NULL,  sold_for REAL NOT NULL,  quality TEXT NOT NULL DEFAULT 'pass',  notes TEXT DEFAULT '',  completed_at TEXT NOT NULL,  handoff_job_id INTEGER);"},
				{kind: "table", name: "pipeline_jobs", sql: "CREATE TABLE IF NOT EXISTS pipeline_jobs (  id INTEGER PRIMARY KEY AUTOINCREMENT,  input_file TEXT NOT NULL,  job_type TEXT NOT NULL DEFAULT 'transcription',  created_at TEXT NOT NULL,  status TEXT NOT NULL DEFAULT 'pending',  ai_cost REAL DEFAULT 0.0,  review_cost REAL DEFAULT 0.0,  sell_price REAL DEFAULT 0.0,  reviewer TEXT,  qa_result TEXT,  qa_notes TEXT,  reviewed_at TEXT,  output_file TEXT,  source_system TEXT,  source_job_id INTEGER,  source_service TEXT);"},
				{kind: "table", name: "bundles", sql: "CREATE TABLE IF NOT EXISTS bundles (  id INTEGER PRIMARY KEY AUTOINCREMENT,  name TEXT NOT NULL UNIQUE,  components TEXT NOT NULL,  sell_price REAL NOT NULL,  total_cost REAL NOT NULL,  alacarte_total REAL NOT NULL,  target TEXT DEFAULT 'general',  created_at TEXT NOT NULL);"},
			]
		},
		{
			macro:  "BF_STORE_FINANCE_OPPORTUNITIES"
			source: "cmd/BonfyreFinance/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "opportunities", sql: "CREATE TABLE IF NOT EXISTS opportunities ( id TEXT PRIMARY KEY, record_digest TEXT NOT NULL, source_kind TEXT NOT NULL, source_ref TEXT NOT NULL, source_digest TEXT NOT NULL, observed_at TEXT NOT NULL, title TEXT NOT NULL, desired_state TEXT NOT NULL, compensation_cents INTEGER, currency TEXT NOT NULL, deadline TEXT NOT NULL DEFAULT '', contact_boundary TEXT NOT NULL, expected_compute_cents INTEGER NOT NULL DEFAULT 0, expected_external_cents INTEGER NOT NULL DEFAULT 0, expected_human_minutes INTEGER NOT NULL DEFAULT 0, reuse_points INTEGER NOT NULL DEFAULT 0, risk_points INTEGER NOT NULL DEFAULT 0, stage TEXT NOT NULL DEFAULT 'observed', proposal_ref TEXT, authority_ref TEXT, customer_ref TEXT, execution_ref TEXT, verification_ref TEXT, delivery_ref TEXT, payment_ref TEXT, retention_ref TEXT, actual_revenue_cents INTEGER, actual_cost_cents INTEGER, actual_human_minutes INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"},
				{kind: "table", name: "opportunity_requirements", sql: "CREATE TABLE IF NOT EXISTS opportunity_requirements ( opportunity_id TEXT NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE, kind TEXT NOT NULL, requirement_key TEXT NOT NULL, owner_ref TEXT NOT NULL, state TEXT NOT NULL, evidence_ref TEXT NOT NULL DEFAULT '', row_digest TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(opportunity_id,kind,requirement_key)) WITHOUT ROWID;"},
				{kind: "table", name: "opportunity_events", sql: "CREATE TABLE IF NOT EXISTS opportunity_events ( id INTEGER PRIMARY KEY AUTOINCREMENT, opportunity_id TEXT NOT NULL REFERENCES opportunities(id) ON DELETE CASCADE, kind TEXT NOT NULL, from_stage TEXT NOT NULL, to_stage TEXT NOT NULL, evidence_ref TEXT NOT NULL, created_at TEXT NOT NULL);"},
				{kind: "index", name: "opportunity_stage_rank", sql: "CREATE INDEX IF NOT EXISTS opportunity_stage_rank ON opportunities(stage,compensation_cents);"},
				{kind: "index", name: "opportunity_requirement_state", sql: "CREATE INDEX IF NOT EXISTS opportunity_requirement_state ON opportunity_requirements(opportunity_id,kind,state);"},
				{kind: "index", name: "opportunity_event_subject", sql: "CREATE INDEX IF NOT EXISTS opportunity_event_subject ON opportunity_events(opportunity_id,id);"},
			]
		},
	]
}
