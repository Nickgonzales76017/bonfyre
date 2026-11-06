CREATE TABLE IF NOT EXISTS subject_bundle(
 subject TEXT NOT NULL, scope TEXT NOT NULL, bundle_digest TEXT NOT NULL,
 closure_digest TEXT NOT NULL, scene_digest TEXT NOT NULL DEFAULT '',
 compiled_at TEXT NOT NULL, counts_json TEXT NOT NULL, payload_json TEXT NOT NULL,
 PRIMARY KEY(subject,scope));
CREATE TABLE IF NOT EXISTS subject_reader(
 subject TEXT NOT NULL, scope TEXT NOT NULL, census_source TEXT NOT NULL,
 root_id TEXT NOT NULL, channel_id TEXT NOT NULL, material TEXT NOT NULL DEFAULT '',
 protocol TEXT NOT NULL DEFAULT '', state TEXT NOT NULL DEFAULT '',
 last_contributed TEXT NOT NULL DEFAULT '', contributions INTEGER NOT NULL DEFAULT 0,
 first_seen TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(subject,scope,census_source,channel_id,material));
CREATE INDEX IF NOT EXISTS subject_reader_by_subject ON subject_reader(subject,scope);
