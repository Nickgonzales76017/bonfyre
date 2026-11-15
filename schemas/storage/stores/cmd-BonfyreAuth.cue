package stores

// cmd/BonfyreAuth: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreAuth/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreAuth": {
	fragments: [
		{
			macro:  "BF_STORE_AUTH_USERS"
			source: "cmd/BonfyreAuth/src/main.c"
			statements: [
				{kind: "table", name: "users", sql: "CREATE TABLE IF NOT EXISTS users (  id INTEGER PRIMARY KEY AUTOINCREMENT,  email TEXT UNIQUE NOT NULL,  name TEXT NOT NULL,  password_hash TEXT NOT NULL,  salt TEXT NOT NULL,  tier TEXT NOT NULL DEFAULT 'free',  gate_key TEXT,  created_at TEXT NOT NULL,  active INTEGER NOT NULL DEFAULT 1);"},
				{kind: "table", name: "sessions", sql: "CREATE TABLE IF NOT EXISTS sessions (  id INTEGER PRIMARY KEY AUTOINCREMENT,  user_id INTEGER NOT NULL,  token TEXT UNIQUE NOT NULL,  created_at TEXT NOT NULL,  expires_at TEXT NOT NULL,  scope TEXT NOT NULL DEFAULT 'read',  active INTEGER NOT NULL DEFAULT 1,  FOREIGN KEY (user_id) REFERENCES users(id));"},
				{kind: "table", name: "institution_grants", sql: "CREATE TABLE IF NOT EXISTS institution_grants (  id INTEGER PRIMARY KEY AUTOINCREMENT,  user_id INTEGER NOT NULL,  institution TEXT NOT NULL,  role TEXT NOT NULL CHECK(role IN ('read','write','admin')),  granted_at TEXT NOT NULL,  expires_at TEXT,  active INTEGER NOT NULL DEFAULT 1,  UNIQUE(user_id,institution),  FOREIGN KEY (user_id) REFERENCES users(id));"},
				{kind: "index", name: "institution_grants_lookup", sql: "CREATE INDEX IF NOT EXISTS institution_grants_lookup   ON institution_grants(user_id,institution,active);"},
				{kind: "table", name: "actor_closure_bindings", sql: "CREATE TABLE IF NOT EXISTS actor_closure_bindings (  user_id INTEGER PRIMARY KEY,  actor_subject TEXT NOT NULL,  state TEXT NOT NULL CHECK(state IN ('active','revoked')),  bound_at TEXT NOT NULL,  FOREIGN KEY (user_id) REFERENCES users(id));"},
				{kind: "index", name: "actor_closure_bindings_lookup", sql: "CREATE INDEX IF NOT EXISTS actor_closure_bindings_lookup   ON actor_closure_bindings(actor_subject,state);"},
				{kind: "table", name: "external_account_links", sql: "CREATE TABLE IF NOT EXISTS external_account_links (  id INTEGER PRIMARY KEY AUTOINCREMENT,  user_id INTEGER NOT NULL,  provider TEXT NOT NULL,  remote_account TEXT NOT NULL,  profile_url TEXT NOT NULL,  display_name TEXT NOT NULL DEFAULT '',  proof_state TEXT NOT NULL CHECK(proof_state IN ('declared','verified','revoked')) DEFAULT 'declared',  connected_at TEXT NOT NULL,  updated_at TEXT NOT NULL,  UNIQUE(user_id,provider,remote_account),  FOREIGN KEY (user_id) REFERENCES users(id));"},
				{kind: "index", name: "external_account_links_user_current", sql: "CREATE INDEX IF NOT EXISTS external_account_links_user_current   ON external_account_links(user_id,proof_state,updated_at);"},
			]
		},
		{
			macro:  "BF_STORE_AUTH_SESSION_HANDOFFS"
			source: "cmd/BonfyreAuth/src/main.c"
			statements: [
				{kind: "table", name: "session_handoffs", sql: "CREATE TABLE IF NOT EXISTS session_handoffs (code_hash TEXT PRIMARY KEY, origin_session INTEGER NOT NULL, destination TEXT NOT NULL,state_hash TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL,consumed_at TEXT, FOREIGN KEY(origin_session) REFERENCES sessions(id));"},
			]
		},
		{
			macro:  "BF_STORE_AUTH_INSTITUTION_ORIGINS"
			source: "cmd/BonfyreAuth/src/organization_workspace.h"
			statements: [
				{kind: "table", name: "institution_origins", sql: "CREATE TABLE IF NOT EXISTS institution_origins(institution TEXT PRIMARY KEY,creator INTEGER NOT NULL,request_digest TEXT NOT NULL,label TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(creator,request_digest),FOREIGN KEY(creator) REFERENCES users(id));"},
				{kind: "table", name: "institution_invitations", sql: "CREATE TABLE IF NOT EXISTS institution_invitations(digest TEXT PRIMARY KEY,institution TEXT NOT NULL,issuer INTEGER NOT NULL,email TEXT NOT NULL,role TEXT NOT NULL CHECK(role IN('read','write','admin')),created_at TEXT NOT NULL,expires_at TEXT NOT NULL,grant_expires_at TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('pending','accepted','revoked')),revision INTEGER NOT NULL DEFAULT 1,accepted_by INTEGER,request_hours INTEGER NOT NULL,FOREIGN KEY(institution) REFERENCES institution_origins(institution));"},
				{kind: "index", name: "institution_invitations_scope", sql: "CREATE INDEX IF NOT EXISTS institution_invitations_scope ON institution_invitations(institution,digest);"},
				{kind: "table", name: "institution_subaccounts", sql: "CREATE TABLE IF NOT EXISTS institution_subaccounts(subaccount TEXT PRIMARY KEY,institution TEXT NOT NULL,creator INTEGER NOT NULL,request_digest TEXT NOT NULL,label TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(institution,creator,request_digest),FOREIGN KEY(institution) REFERENCES institution_origins(institution));"},
			]
		},
	]
}
