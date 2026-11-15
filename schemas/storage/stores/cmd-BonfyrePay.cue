package stores

// cmd/BonfyrePay: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyrePay/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyrePay": {
	fragments: [
		{
			macro:  "BF_STORE_PAY_INVOICES"
			source: "cmd/BonfyrePay/src/main.c"
			statements: [
				{kind: "table", name: "invoices", sql: "CREATE TABLE IF NOT EXISTS invoices (  id INTEGER PRIMARY KEY AUTOINCREMENT,  user_id INTEGER,  api_key TEXT,  period_start TEXT,  period_end TEXT,  amount_cents INTEGER NOT NULL DEFAULT 0,  line_items_json TEXT,  status TEXT NOT NULL DEFAULT 'pending',  created_at TEXT NOT NULL);"},
				{kind: "table", name: "payments", sql: "CREATE TABLE IF NOT EXISTS payments (  id INTEGER PRIMARY KEY AUTOINCREMENT,  invoice_id INTEGER,  user_id INTEGER,  amount_cents INTEGER NOT NULL,  method TEXT NOT NULL,  reference TEXT,  created_at TEXT NOT NULL,  FOREIGN KEY (invoice_id) REFERENCES invoices(id));"},
				{kind: "table", name: "credits", sql: "CREATE TABLE IF NOT EXISTS credits (  id INTEGER PRIMARY KEY AUTOINCREMENT,  user_id INTEGER NOT NULL,  amount_cents INTEGER NOT NULL,  reason TEXT,  created_at TEXT NOT NULL);"},
			]
		},
	]
}
