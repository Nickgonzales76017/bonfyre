package stores

// cmd/BonfyreEconomy: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreEconomy/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreEconomy": {
	fragments: [
		{
			macro:  "BF_STORE_ECONOMY_VERIFICATION_CASES"
			source: "cmd/BonfyreEconomy/src/verification_planner.c"
			statements: [
				{kind: "table", name: "verification_cases", sql: "CREATE TABLE IF NOT EXISTS verification_cases(case_id TEXT PRIMARY KEY,domain TEXT NOT NULL,subject_ref TEXT NOT NULL,budget_cents INTEGER NOT NULL CHECK(budget_cents>=0),state TEXT NOT NULL DEFAULT 'proposal' CHECK(state='proposal'),created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP) STRICT;"},
				{kind: "table", name: "verification_needs", sql: "CREATE TABLE IF NOT EXISTS verification_needs(case_id TEXT NOT NULL REFERENCES verification_cases(case_id),need_id TEXT NOT NULL,kind TEXT NOT NULL,weight INTEGER NOT NULL CHECK(weight IN(1,3,5)),source_ref TEXT NOT NULL,PRIMARY KEY(case_id,need_id)) WITHOUT ROWID,STRICT;"},
				{kind: "table", name: "verification_offers", sql: "CREATE TABLE IF NOT EXISTS verification_offers(case_id TEXT NOT NULL REFERENCES verification_cases(case_id),route_id TEXT NOT NULL,provider_ref TEXT NOT NULL,method TEXT NOT NULL,base_cents INTEGER NOT NULL CHECK(base_cents>=0),labor_cents INTEGER NOT NULL CHECK(labor_cents>=0),travel_cents INTEGER NOT NULL CHECK(travel_cents>=0),visit_group TEXT NOT NULL,quote_state TEXT NOT NULL CHECK(quote_state IN('estimate','quoted')),availability TEXT NOT NULL CHECK(availability IN('unknown','conditional','ready')),quote_ref TEXT NOT NULL,availability_ref TEXT NOT NULL,PRIMARY KEY(case_id,route_id)) WITHOUT ROWID,STRICT;"},
				{kind: "table", name: "verification_coverage", sql: "CREATE TABLE IF NOT EXISTS verification_coverage(case_id TEXT NOT NULL,route_id TEXT NOT NULL,need_id TEXT NOT NULL,PRIMARY KEY(case_id,route_id,need_id),FOREIGN KEY(case_id,route_id) REFERENCES verification_offers(case_id,route_id),FOREIGN KEY(case_id,need_id) REFERENCES verification_needs(case_id,need_id)) WITHOUT ROWID,STRICT;"},
			]
		},
		{
			macro:  "BF_STORE_ECONOMY_BUDGETS"
			source: "cmd/BonfyreEconomy/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "other", name: "", sql: "PRAGMA foreign_keys=ON;"},
				{kind: "table", name: "budgets", sql: "CREATE TABLE IF NOT EXISTS budgets(  recipe       TEXT PRIMARY KEY,  max_usd      REAL NOT NULL,  spent        REAL NOT NULL DEFAULT 0.0,  reset_period TEXT NOT NULL DEFAULT 'monthly',  updated      INTEGER NOT NULL);"},
				{kind: "table", name: "costs", sql: "CREATE TABLE IF NOT EXISTS costs(  id       INTEGER PRIMARY KEY AUTOINCREMENT,  recipe   TEXT NOT NULL,  stage    TEXT NOT NULL,  model    TEXT NOT NULL,  usd      REAL NOT NULL,  tokens   INTEGER,  ts       INTEGER NOT NULL);"},
				{kind: "table", name: "routes", sql: "CREATE TABLE IF NOT EXISTS routes(  id           INTEGER PRIMARY KEY AUTOINCREMENT,  recipe       TEXT NOT NULL,  chosen_model TEXT NOT NULL,  reason       TEXT,  ts           INTEGER NOT NULL);"},
				{kind: "index", name: "idx_costs_recipe", sql: "CREATE INDEX IF NOT EXISTS idx_costs_recipe ON costs(recipe);"},
			]
		},
	]
}
