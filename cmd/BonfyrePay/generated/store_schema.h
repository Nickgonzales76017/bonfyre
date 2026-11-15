/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYREPAY_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYREPAY_H
/* cmd/BonfyrePay/src/main.c */
#define BF_STORE_PAY_INVOICES \
    "CREATE TABLE IF NOT EXISTS invoices (  id INTEGER PRIMARY KEY AUTOINCREMENT,  user_id INTEGER,  api_key TEXT,  period_start TEXT,  period_end TEXT,  amount_cents INTEGER NOT NULL DEFAULT 0,  line_items_json TEXT,  status TEXT NOT NULL DEFAULT 'pending',  created_at TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS payments (  id INTEGER PRIMARY KEY AUTOINCREMENT,  invoice_id INTEGER,  user_id INTEGER,  amount_cents INTEGER NOT NULL,  method TEXT NOT NULL,  reference TEXT,  created_at TEXT NOT NULL,  FOREIGN KEY (invoice_id) REFERENCES invoices(id));" \
    "CREATE TABLE IF NOT EXISTS credits (  id INTEGER PRIMARY KEY AUTOINCREMENT,  user_id INTEGER NOT NULL,  amount_cents INTEGER NOT NULL,  reason TEXT,  created_at TEXT NOT NULL);"
#endif
