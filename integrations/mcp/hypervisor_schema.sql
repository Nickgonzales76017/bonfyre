PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS providers (
  id TEXT PRIMARY KEY, repo TEXT NOT NULL, homepage TEXT NOT NULL, license TEXT NOT NULL,
  integration TEXT NOT NULL, mode TEXT NOT NULL, installed INTEGER NOT NULL DEFAULT 0,
  command_path TEXT, version TEXT, source_ref TEXT, source_digest TEXT,
  license_approved INTEGER NOT NULL DEFAULT 0, health TEXT NOT NULL DEFAULT 'unknown',
  metadata_json TEXT NOT NULL DEFAULT '{}', discovered_at REAL, updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS capabilities (
  id TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '', effect_class TEXT NOT NULL DEFAULT 'read',
  schema_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS provider_capabilities (
  provider_id TEXT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  capability_id TEXT NOT NULL REFERENCES capabilities(id) ON DELETE CASCADE,
  state TEXT NOT NULL DEFAULT 'declared', score REAL NOT NULL DEFAULT 0,
  evidence_json TEXT NOT NULL DEFAULT '[]', updated_at REAL NOT NULL,
  PRIMARY KEY(provider_id, capability_id)
);
CREATE TABLE IF NOT EXISTS sources (
  id TEXT PRIMARY KEY, repo TEXT NOT NULL, requested_ref TEXT, resolved_ref TEXT,
  checkout_path TEXT NOT NULL, state TEXT NOT NULL, license TEXT, digest TEXT,
  manifest_json TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL, updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS missions (
  id TEXT PRIMARY KEY, goal TEXT NOT NULL, state TEXT NOT NULL, spec_path TEXT NOT NULL,
  mission_json TEXT NOT NULL, route_json TEXT NOT NULL DEFAULT '{}', context_packet_id TEXT,
  external_task_id TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS mission_tasks (
  id TEXT NOT NULL, mission_id TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
  title TEXT NOT NULL, state TEXT NOT NULL, dependencies_json TEXT NOT NULL DEFAULT '[]',
  capability TEXT, provider_id TEXT, receipt_id TEXT, attempts INTEGER NOT NULL DEFAULT 0,
  error TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL, updated_at REAL NOT NULL,
  PRIMARY KEY(mission_id,id)
);
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY, provider_id TEXT NOT NULL, native_session_id TEXT NOT NULL,
  mission_id TEXT, cwd TEXT, parent_session_id TEXT, state TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL, updated_at REAL NOT NULL,
  UNIQUE(provider_id, native_session_id)
);
CREATE TABLE IF NOT EXISTS context_packets (
  id TEXT PRIMARY KEY, mission_id TEXT NOT NULL, path TEXT NOT NULL, digest TEXT NOT NULL,
  token_estimate INTEGER NOT NULL, summary_json TEXT NOT NULL, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS evaluations (
  id TEXT PRIMARY KEY, provider_id TEXT NOT NULL, capability_id TEXT NOT NULL,
  state TEXT NOT NULL, score REAL NOT NULL, metrics_json TEXT NOT NULL,
  evidence_json TEXT NOT NULL, started_at REAL NOT NULL, completed_at REAL
);
CREATE TABLE IF NOT EXISTS routes (
  capability_id TEXT NOT NULL, scope TEXT NOT NULL DEFAULT 'global', provider_id TEXT NOT NULL,
  state TEXT NOT NULL, weight REAL NOT NULL DEFAULT 1, policy_json TEXT NOT NULL DEFAULT '{}',
  updated_at REAL NOT NULL, PRIMARY KEY(capability_id, scope, provider_id)
);
CREATE TABLE IF NOT EXISTS receipts (
  id TEXT PRIMARY KEY, mission_id TEXT, task_id TEXT, provider_id TEXT, capability_id TEXT,
  state TEXT NOT NULL, path TEXT NOT NULL, digest TEXT NOT NULL, previous_digest TEXT,
  payload_json TEXT NOT NULL, created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, source TEXT NOT NULL,
  kind TEXT NOT NULL, mission_id TEXT, payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bugs (
  id TEXT PRIMARY KEY, mission_id TEXT NOT NULL, invariant_id TEXT, symptom TEXT NOT NULL,
  root_cause TEXT, regression_test TEXT, state TEXT NOT NULL,
  evidence_json TEXT NOT NULL DEFAULT '[]', created_at REAL NOT NULL, updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS source_updates (
  id TEXT PRIMARY KEY, provider_id TEXT NOT NULL, old_ref TEXT, new_ref TEXT, state TEXT NOT NULL,
  compatibility_json TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL, updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS hypervisor_receipt_time ON receipts(created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS hypervisor_evaluation_latest ON evaluations(capability_id,provider_id,completed_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS hypervisor_sessions_updated ON sessions(updated_at DESC,id);
