package stores

// cmd/BonfyreCMS: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreCMS/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreCMS": {
	fragments: [
		{
			macro:  "BF_STORE_CMS_OPS"
			source: "cmd/BonfyreCMS/src/ephemeral.c"
			statements: [
				{kind: "table", name: "_ops", sql: "CREATE TABLE IF NOT EXISTS _ops (  id        INTEGER PRIMARY KEY AUTOINCREMENT,  op_type   TEXT NOT NULL,  target    TEXT NOT NULL,  target_id INTEGER,  namespace TEXT NOT NULL DEFAULT 'root',  payload   TEXT NOT NULL,  hash      TEXT NOT NULL,  parent    TEXT,  ts        TEXT NOT NULL,  actor     TEXT DEFAULT '');"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_OPS_TARGET"
			source: "cmd/BonfyreCMS/src/ephemeral.c"
			statements: [
				{kind: "index", name: "idx_ops_target", sql: "CREATE INDEX IF NOT EXISTS idx_ops_target ON _ops(target, target_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_OPS_HASH"
			source: "cmd/BonfyreCMS/src/ephemeral.c"
			statements: [
				{kind: "index", name: "idx_ops_hash", sql: "CREATE INDEX IF NOT EXISTS idx_ops_hash ON _ops(hash);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_OPS_NS"
			source: "cmd/BonfyreCMS/src/ephemeral.c"
			statements: [
				{kind: "index", name: "idx_ops_ns", sql: "CREATE INDEX IF NOT EXISTS idx_ops_ns ON _ops(namespace);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_OPS_HASH_UNIQ"
			source: "cmd/BonfyreCMS/src/ephemeral.c"
			statements: [
				{kind: "index", name: "idx_ops_hash_uniq", sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_hash_uniq ON _ops(hash);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_HOUSEHOLD_AUTHORITY"
			source: "cmd/BonfyreCMS/src/household_members.h"
			statements: [
				{kind: "table", name: "_household_authority", sql: "CREATE TABLE IF NOT EXISTS _household_authority(household_row INTEGER PRIMARY KEY,epoch INTEGER NOT NULL);"},
				{kind: "table", name: "_household_membership", sql: "CREATE TABLE IF NOT EXISTS _household_membership(household_row INTEGER NOT NULL,member TEXT NOT NULL,role TEXT NOT NULL,revision INTEGER NOT NULL,authority_epoch INTEGER NOT NULL,expires_at INTEGER NOT NULL,granted_by TEXT NOT NULL,PRIMARY KEY(household_row,member));"},
				{kind: "other", name: "", sql: "INSERT OR IGNORE INTO _household_authority SELECT id,1 FROM household;"},
				{kind: "trigger", name: "_household_authority_changed", sql: "CREATE TRIGGER IF NOT EXISTS _household_authority_changed AFTER UPDATE OF steward,status,household_id,namespace ON household WHEN OLD.steward IS NOT NEW.steward OR OLD.status IS NOT NEW.status OR OLD.household_id IS NOT NEW.household_id OR OLD.namespace IS NOT NEW.namespace BEGIN INSERT INTO _household_authority VALUES(NEW.id,1) ON CONFLICT(household_row) DO UPDATE SET epoch=epoch+1; END;"},
				{kind: "trigger", name: "_household_authority_deleted", sql: "CREATE TRIGGER IF NOT EXISTS _household_authority_deleted AFTER DELETE ON household BEGIN UPDATE _household_authority SET epoch=epoch+1 WHERE household_row=OLD.id; END;"},
				{kind: "trigger", name: "_household_authority_inserted", sql: "CREATE TRIGGER IF NOT EXISTS _household_authority_inserted AFTER INSERT ON household BEGIN INSERT INTO _household_authority VALUES(NEW.id,1) ON CONFLICT(household_row) DO UPDATE SET epoch=epoch+1; END;"},
			]
		},
		{
			macro:  "BF_STORE_CMS_FAMILIES"
			source: "cmd/BonfyreCMS/src/canonical.c"
			statements: [
				{kind: "table", name: "_families", sql: "CREATE TABLE IF NOT EXISTS _families (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  family_hash   TEXT NOT NULL,  content_type  TEXT NOT NULL,  generator     TEXT,  member_count  INTEGER DEFAULT 0,  created_at    TEXT NOT NULL,  UNIQUE(family_hash, content_type));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_FAMILIES_HASH"
			source: "cmd/BonfyreCMS/src/canonical.c"
			statements: [
				{kind: "index", name: "idx_families_hash", sql: "CREATE INDEX IF NOT EXISTS idx_families_hash ON _families(family_hash);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_FAMILIES_TYPE"
			source: "cmd/BonfyreCMS/src/canonical.c"
			statements: [
				{kind: "index", name: "idx_families_type", sql: "CREATE INDEX IF NOT EXISTS idx_families_type ON _families(content_type);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_FAMILY_MEMBERS"
			source: "cmd/BonfyreCMS/src/canonical.c"
			statements: [
				{kind: "table", name: "_family_members", sql: "CREATE TABLE IF NOT EXISTS _family_members (  family_id     INTEGER NOT NULL,  target_type   TEXT NOT NULL,  target_id     INTEGER NOT NULL,  bindings      TEXT,  PRIMARY KEY(target_type, target_id),  FOREIGN KEY(family_id) REFERENCES _families(id));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_TRANSFER_TEMPLATES"
			source: "cmd/BonfyreCMS/src/transfer.c"
			statements: [
				{kind: "table", name: "_transfer_templates", sql: "CREATE TABLE IF NOT EXISTS _transfer_templates (  template_id  INTEGER PRIMARY KEY AUTOINCREMENT,  name         TEXT NOT NULL,  family_id    TEXT NOT NULL,  generator    TEXT NOT NULL,  arity        INTEGER NOT NULL,  member_count INTEGER NOT NULL DEFAULT 0,  created_at   TEXT NOT NULL);"},
				{kind: "table", name: "_transfer_log", sql: "CREATE TABLE IF NOT EXISTS _transfer_log (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  donor_id      TEXT NOT NULL,  recipient_id  TEXT NOT NULL,  similarity    REAL NOT NULL,  positions_seeded INTEGER NOT NULL DEFAULT 0,  transferred_at TEXT NOT NULL);"},
				{kind: "index", name: "idx_transfer_tpl_gen", sql: "CREATE INDEX IF NOT EXISTS idx_transfer_tpl_gen   ON _transfer_templates(generator);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_INCR_EIGEN"
			source: "cmd/BonfyreCMS/src/incremental.c"
			statements: [
				{kind: "table", name: "_incr_eigen", sql: "CREATE TABLE IF NOT EXISTS _incr_eigen (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  family_id  INTEGER NOT NULL,  position   INTEGER NOT NULL,  field_name TEXT NOT NULL,  count      INTEGER DEFAULT 0,  mean       REAL DEFAULT 0.0,  m2         REAL DEFAULT 0.0,  variance   REAL DEFAULT 0.0,  eigenvalue REAL DEFAULT 0.0,  min_val    REAL DEFAULT 0.0,  max_val    REAL DEFAULT 0.0,  is_numeric INTEGER DEFAULT 0,  distinct_count INTEGER DEFAULT 0,  updated_at TEXT NOT NULL,  UNIQUE(family_id, position));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_IEIG_FAM"
			source: "cmd/BonfyreCMS/src/incremental.c"
			statements: [
				{kind: "index", name: "idx_ieig_fam", sql: "CREATE INDEX IF NOT EXISTS idx_ieig_fam ON _incr_eigen(family_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_ABSORBED_CONSTANTS"
			source: "cmd/BonfyreCMS/src/incremental.c"
			statements: [
				{kind: "table", name: "_absorbed_constants", sql: "CREATE TABLE IF NOT EXISTS _absorbed_constants (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  family_id   INTEGER NOT NULL,  position    INTEGER NOT NULL,  field_name  TEXT NOT NULL,  const_value TEXT NOT NULL,  absorbed_at TEXT NOT NULL,  UNIQUE(family_id, position));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_EQUIVALENCES"
			source: "cmd/BonfyreCMS/src/lt_egraph.c"
			statements: [
				{kind: "table", name: "_equivalences", sql: "CREATE TABLE IF NOT EXISTS _equivalences (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  class_id      INTEGER NOT NULL,  repr_type     TEXT NOT NULL,  repr_hash     TEXT NOT NULL,  target_type   TEXT NOT NULL,  target_id     INTEGER NOT NULL,  materialized  INTEGER DEFAULT 0,  cost          REAL DEFAULT 1.0,  data          TEXT,  ts            TEXT NOT NULL);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_EQ_CLASS"
			source: "cmd/BonfyreCMS/src/lt_egraph.c"
			statements: [
				{kind: "index", name: "idx_eq_class", sql: "CREATE INDEX IF NOT EXISTS idx_eq_class ON _equivalences(class_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_EQ_TARGET"
			source: "cmd/BonfyreCMS/src/lt_egraph.c"
			statements: [
				{kind: "index", name: "idx_eq_target", sql: "CREATE INDEX IF NOT EXISTS idx_eq_target ON _equivalences(target_type, target_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_EQ_REPR"
			source: "cmd/BonfyreCMS/src/lt_egraph.c"
			statements: [
				{kind: "index", name: "idx_eq_repr", sql: "CREATE INDEX IF NOT EXISTS idx_eq_repr ON _equivalences(target_type, target_id, repr_type);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_EQ_MAT"
			source: "cmd/BonfyreCMS/src/lt_egraph.c"
			statements: [
				{kind: "index", name: "idx_eq_mat", sql: "CREATE INDEX IF NOT EXISTS idx_eq_mat ON _equivalences(materialized, cost);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_EGRAPH_META"
			source: "cmd/BonfyreCMS/src/lt_egraph.c"
			statements: [
				{kind: "table", name: "_egraph_meta", sql: "CREATE TABLE IF NOT EXISTS _egraph_meta (  key   TEXT PRIMARY KEY,  value TEXT NOT NULL);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_MERKLE_TREE"
			source: "cmd/BonfyreCMS/src/provenance.c"
			statements: [
				{kind: "table", name: "_merkle_tree", sql: "CREATE TABLE IF NOT EXISTS _merkle_tree (  node_id    INTEGER PRIMARY KEY AUTOINCREMENT,  ns         TEXT NOT NULL,  depth      INTEGER NOT NULL,  left_child INTEGER,  right_child INTEGER,  op_id      INTEGER,  hash       INTEGER NOT NULL);"},
				{kind: "index", name: "idx_merkle_ns", sql: "CREATE INDEX IF NOT EXISTS idx_merkle_ns   ON _merkle_tree(ns, depth);"},
				{kind: "index", name: "idx_merkle_op", sql: "CREATE INDEX IF NOT EXISTS idx_merkle_op   ON _merkle_tree(op_id);"},
				{kind: "table", name: "_compaction_manifests", sql: "CREATE TABLE IF NOT EXISTS _compaction_manifests (  manifest_id  INTEGER PRIMARY KEY AUTOINCREMENT,  content_type TEXT NOT NULL,  merkle_root  INTEGER NOT NULL,  family_count INTEGER NOT NULL DEFAULT 0,  gen_hash     INTEGER NOT NULL,  eigen_hash   INTEGER NOT NULL,  created_at   TEXT NOT NULL);"},
				{kind: "index", name: "idx_manifest_ct", sql: "CREATE INDEX IF NOT EXISTS idx_manifest_ct   ON _compaction_manifests(content_type);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_TENSOR_EIGEN"
			source: "cmd/BonfyreCMS/src/tensor_ops.c"
			statements: [
				{kind: "table", name: "_tensor_eigen", sql: "CREATE TABLE IF NOT EXISTS _tensor_eigen (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  family_id   INTEGER NOT NULL,  position    INTEGER NOT NULL,  field_name  TEXT NOT NULL,  eigenvalue  REAL DEFAULT 0.0,  variance    REAL DEFAULT 0.0,  mean        REAL DEFAULT 0.0,  updated_at  TEXT NOT NULL,  UNIQUE(family_id, position));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_TEIG_FAM"
			source: "cmd/BonfyreCMS/src/tensor_ops.c"
			statements: [
				{kind: "index", name: "idx_teig_fam", sql: "CREATE INDEX IF NOT EXISTS idx_teig_fam ON _tensor_eigen(family_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_TENSOR_CELLS"
			source: "cmd/BonfyreCMS/src/tensor_ops.c"
			statements: [
				{kind: "table", name: "_tensor_cells", sql: "CREATE TABLE IF NOT EXISTS _tensor_cells (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  family_id   INTEGER NOT NULL,  target_type TEXT NOT NULL,  target_id   INTEGER NOT NULL,  position    INTEGER NOT NULL,  field_name  TEXT NOT NULL,  val_text    TEXT,  val_num     REAL,  val_type    INTEGER DEFAULT 0,  UNIQUE(family_id, target_id, position));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_TCELL_FAM"
			source: "cmd/BonfyreCMS/src/tensor_ops.c"
			statements: [
				{kind: "index", name: "idx_tcell_fam", sql: "CREATE INDEX IF NOT EXISTS idx_tcell_fam ON _tensor_cells(family_id, position);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_TCELL_TARGET"
			source: "cmd/BonfyreCMS/src/tensor_ops.c"
			statements: [
				{kind: "index", name: "idx_tcell_target", sql: "CREATE INDEX IF NOT EXISTS idx_tcell_target ON _tensor_cells(target_type, target_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_TCELL_FIELD"
			source: "cmd/BonfyreCMS/src/tensor_ops.c"
			statements: [
				{kind: "index", name: "idx_tcell_field", sql: "CREATE INDEX IF NOT EXISTS idx_tcell_field ON _tensor_cells(family_id, field_name);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_META_FAMILIES"
			source: "cmd/BonfyreCMS/src/meta_gen.c"
			statements: [
				{kind: "table", name: "_meta_families", sql: "CREATE TABLE IF NOT EXISTS _meta_families (  meta_family_id INTEGER PRIMARY KEY AUTOINCREMENT,  content_type   TEXT NOT NULL,  meta_generator TEXT NOT NULL,  meta_arity     INTEGER NOT NULL DEFAULT 0,  member_count   INTEGER NOT NULL DEFAULT 0,  level          INTEGER NOT NULL DEFAULT 2,  compression    REAL NOT NULL DEFAULT 0,  created_at     TEXT NOT NULL);"},
				{kind: "table", name: "_meta_members", sql: "CREATE TABLE IF NOT EXISTS _meta_members (  meta_family_id INTEGER NOT NULL,  family_id      TEXT NOT NULL,  generator      TEXT NOT NULL,  meta_bindings  TEXT NOT NULL DEFAULT '[]',  PRIMARY KEY (meta_family_id, family_id));"},
				{kind: "index", name: "idx_meta_members_ct", sql: "CREATE INDEX IF NOT EXISTS idx_meta_members_ct   ON _meta_families(content_type);"},
				{kind: "index", name: "idx_meta_members_fam", sql: "CREATE INDEX IF NOT EXISTS idx_meta_members_fam   ON _meta_members(family_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_REDUCE_MEMO"
			source: "cmd/BonfyreCMS/src/hybrid_reduce.c"
			statements: [
				{kind: "table", name: "_reduce_memo", sql: "CREATE TABLE IF NOT EXISTS _reduce_memo (  family_id  TEXT NOT NULL,  target_id  INTEGER NOT NULL,  strategy   INTEGER NOT NULL,  result     TEXT NOT NULL,  created_at TEXT NOT NULL,  PRIMARY KEY (family_id, target_id));"},
				{kind: "table", name: "_reduce_profiles", sql: "CREATE TABLE IF NOT EXISTS _reduce_profiles (  family_id       TEXT NOT NULL,  strategy        INTEGER NOT NULL,  elapsed_us      INTEGER NOT NULL,  result_bytes    INTEGER NOT NULL,  profiled_at     TEXT NOT NULL,  PRIMARY KEY (family_id, strategy));"},
				{kind: "index", name: "idx_memo_family", sql: "CREATE INDEX IF NOT EXISTS idx_memo_family   ON _reduce_memo(family_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_CONTENT_TYPES"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "_content_types", sql: "CREATE TABLE IF NOT EXISTS _content_types (  name TEXT PRIMARY KEY,  schema_json TEXT NOT NULL,  created_at TEXT NOT NULL,  updated_at TEXT NOT NULL);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_RELATIONS"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "_relations", sql: "CREATE TABLE IF NOT EXISTS _relations (  id INTEGER PRIMARY KEY AUTOINCREMENT,  source_type TEXT NOT NULL,  source_id INTEGER NOT NULL,  target_type TEXT NOT NULL,  target_id INTEGER NOT NULL,  relation TEXT NOT NULL,  kind TEXT NOT NULL,  position INTEGER DEFAULT 0,  UNIQUE(source_type, source_id, target_type, target_id, relation));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_REL_SOURCE"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "index", name: "idx_rel_source", sql: "CREATE INDEX IF NOT EXISTS idx_rel_source ON _relations(source_type, source_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_REL_TARGET"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "index", name: "idx_rel_target", sql: "CREATE INDEX IF NOT EXISTS idx_rel_target ON _relations(target_type, target_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_HOOKS"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "_hooks", sql: "CREATE TABLE IF NOT EXISTS _hooks (  id INTEGER PRIMARY KEY AUTOINCREMENT,  content_type TEXT NOT NULL,  event TEXT NOT NULL,  command TEXT NOT NULL,  enabled INTEGER DEFAULT 1);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_PERMISSIONS"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "_permissions", sql: "CREATE TABLE IF NOT EXISTS _permissions (  token_hash TEXT NOT NULL,  content_type TEXT NOT NULL,  action TEXT NOT NULL,  namespace_scope TEXT DEFAULT 'root',  PRIMARY KEY(token_hash, content_type, action));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_TOKENS"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "_tokens", sql: "CREATE TABLE IF NOT EXISTS _tokens (  token_hash TEXT PRIMARY KEY,  label TEXT,  scope TEXT DEFAULT 'root',  created_at TEXT NOT NULL,  expires_at TEXT,  active INTEGER DEFAULT 1);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_W"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "%w", sql: "CREATE TABLE IF NOT EXISTS \"%w\" (id INTEGER PRIMARY KEY AUTOINCREMENT,namespace TEXT DEFAULT 'root' NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,published_at TEXT,status TEXT DEFAULT 'draft'"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_W_NS"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "index", name: "idx_%w_ns", sql: "CREATE INDEX IF NOT EXISTS \"idx_%w_ns\" ON \"%w\"(namespace)"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_W_W_NS"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "index", name: "idx_%w_%w_ns", sql: "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_%w_%w_ns\" ON \"%w\"(namespace,\"%w\")"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_W_W"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "index", name: "idx_%w_%w", sql: "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_%w_%w\" ON \"%w\"(\"%w\")"},
			]
		},
		{
			macro:  "BF_STORE_CMS_PUBLICATION_DELTA_OUTBOX"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "_publication_delta_outbox", sql: "CREATE TABLE IF NOT EXISTS _publication_delta_outbox(namespace TEXT NOT NULL,source TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(namespace,source))"},
			]
		},
		{
			macro:  "BF_STORE_CMS_PAGE_WRITE_RETIRED"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "page_write_retired", sql: "CREATE TEMP TABLE page_write_retired AS SELECT id,coordinate,status FROM site_fragment WHERE 0"},
			]
		},
		{
			macro:  "BF_STORE_CMS_BASELINE_RAW"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "_baseline_raw", sql: "CREATE TABLE IF NOT EXISTS _baseline_raw (id INTEGER PRIMARY KEY, data TEXT)"},
			]
		},
		{
			macro:  "BF_STORE_CMS_DEFERRED_BENCH"
			source: "cmd/BonfyreCMS/src/main.c"
			statements: [
				{kind: "table", name: "_deferred_bench", sql: "CREATE TABLE IF NOT EXISTS _deferred_bench (id INTEGER PRIMARY KEY, title TEXT, slug TEXT, vertical TEXT, base_price INTEGER, min_threshold INTEGER, current_stake_count INTEGER, offer_status TEXT, namespace TEXT, created_at TEXT, updated_at TEXT, status TEXT)"},
			]
		},
		{
			macro:  "BF_STORE_CMS_ANN_BUCKETS"
			source: "cmd/BonfyreCMS/src/ann_index.c"
			statements: [
				{kind: "table", name: "_ann_buckets", sql: "CREATE TABLE IF NOT EXISTS _ann_buckets (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  content_type TEXT NOT NULL,  table_idx   INTEGER NOT NULL,  bucket_hash TEXT NOT NULL,  family_id   INTEGER NOT NULL,  target_id   INTEGER NOT NULL,  UNIQUE(content_type, table_idx, target_id));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_ANN_LOOKUP"
			source: "cmd/BonfyreCMS/src/ann_index.c"
			statements: [
				{kind: "index", name: "idx_ann_lookup", sql: "CREATE INDEX IF NOT EXISTS idx_ann_lookup ON _ann_buckets(content_type, table_idx, bucket_hash);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_ANN_TARGET"
			source: "cmd/BonfyreCMS/src/ann_index.c"
			statements: [
				{kind: "index", name: "idx_ann_target", sql: "CREATE INDEX IF NOT EXISTS idx_ann_target ON _ann_buckets(content_type, target_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_ANN_PROJECTIONS"
			source: "cmd/BonfyreCMS/src/ann_index.c"
			statements: [
				{kind: "table", name: "_ann_projections", sql: "CREATE TABLE IF NOT EXISTS _ann_projections (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  content_type TEXT NOT NULL,  table_idx  INTEGER NOT NULL,  bit_idx    INTEGER NOT NULL,  dimension  INTEGER NOT NULL,  weight     REAL NOT NULL,  UNIQUE(content_type, table_idx, bit_idx, dimension));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_ANN_QUANTIZED"
			source: "cmd/BonfyreCMS/src/ann_index.c"
			statements: [
				{kind: "table", name: "_ann_quantized", sql: "CREATE TABLE IF NOT EXISTS _ann_quantized (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  content_type  TEXT NOT NULL,  family_id     INTEGER NOT NULL,  target_id     INTEGER NOT NULL,  dim           INTEGER NOT NULL,  rot_dim       INTEGER NOT NULL,  norm          REAL NOT NULL,  clip          REAL NOT NULL,  residual_avg  REAL NOT NULL,  q4_codes      BLOB NOT NULL,  residual_bits BLOB NOT NULL,  UNIQUE(content_type, target_id));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_ANN_QUANTIZED_LOOKUP"
			source: "cmd/BonfyreCMS/src/ann_index.c"
			statements: [
				{kind: "index", name: "idx_ann_quantized_lookup", sql: "CREATE INDEX IF NOT EXISTS idx_ann_quantized_lookup ON _ann_quantized(content_type, target_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_SYNTHESIS_CANDIDATES"
			source: "cmd/BonfyreCMS/src/synthesis.c"
			statements: [
				{kind: "table", name: "_synthesis_candidates", sql: "CREATE TABLE IF NOT EXISTS _synthesis_candidates (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  content_type  TEXT NOT NULL,  generator     TEXT NOT NULL,  arity         INTEGER DEFAULT 0,  coverage      INTEGER DEFAULT 0,  score         REAL DEFAULT 0.0,  compression   REAL DEFAULT 0.0,  annotations   TEXT,  created_at    TEXT NOT NULL,  UNIQUE(content_type, generator));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_SYNTH_CT"
			source: "cmd/BonfyreCMS/src/synthesis.c"
			statements: [
				{kind: "index", name: "idx_synth_ct", sql: "CREATE INDEX IF NOT EXISTS idx_synth_ct ON _synthesis_candidates(content_type, score DESC);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_SYNC_STATE"
			source: "cmd/BonfyreCMS/src/delta_sync.c"
			statements: [
				{kind: "table", name: "_sync_state", sql: "CREATE TABLE IF NOT EXISTS _sync_state (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  remote_id   TEXT NOT NULL,  ns          TEXT NOT NULL,  last_hash   TEXT NOT NULL,  last_op_id  INTEGER DEFAULT 0,  synced_at   TEXT NOT NULL,  UNIQUE(remote_id, ns));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_SYNC_REMOTE"
			source: "cmd/BonfyreCMS/src/delta_sync.c"
			statements: [
				{kind: "index", name: "idx_sync_remote", sql: "CREATE INDEX IF NOT EXISTS idx_sync_remote ON _sync_state(remote_id, ns);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_TYPE_CONSTRAINTS"
			source: "cmd/BonfyreCMS/src/type_check.c"
			statements: [
				{kind: "table", name: "_type_constraints", sql: "CREATE TABLE IF NOT EXISTS _type_constraints (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  family_id   INTEGER NOT NULL,  position    INTEGER NOT NULL,  field_name  TEXT NOT NULL,  kind        INTEGER NOT NULL,  range_lo    REAL DEFAULT 0.0,  range_hi    REAL DEFAULT 0.0,  enum_vals   TEXT,  pattern     TEXT,  is_optional INTEGER DEFAULT 0,  inferred_at TEXT NOT NULL,  UNIQUE(family_id, position));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_IDX_TC_FAM"
			source: "cmd/BonfyreCMS/src/type_check.c"
			statements: [
				{kind: "index", name: "idx_tc_fam", sql: "CREATE INDEX IF NOT EXISTS idx_tc_fam ON _type_constraints(family_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_ACCOUNT_WORLD_BINDING"
			source: "cmd/BonfyreCMS/src/account_world.c"
			statements: [
				{kind: "table", name: "account_world_binding", sql: "CREATE TABLE IF NOT EXISTS account_world_binding(world_id INTEGER PRIMARY KEY,subject TEXT NOT NULL,namespace TEXT NOT NULL);"},
				{kind: "table", name: "account_world_space", sql: "CREATE TABLE IF NOT EXISTS account_world_space(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,title TEXT NOT NULL,renderer TEXT NOT NULL,template_ref TEXT NOT NULL,layout_ref TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0),PRIMARY KEY(world_id,space_id));"},
				{kind: "table", name: "account_world_space_style", sql: "CREATE TABLE IF NOT EXISTS account_world_space_style(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,medium TEXT NOT NULL,character TEXT NOT NULL,lede TEXT NOT NULL,PRIMARY KEY(world_id,space_id));"},
				{kind: "table", name: "account_world_item", sql: "CREATE TABLE IF NOT EXISTS account_world_item(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,position INTEGER NOT NULL CHECK(position BETWEEN 1 AND 24),ref TEXT NOT NULL,role TEXT NOT NULL,note TEXT NOT NULL,PRIMARY KEY(world_id,space_id,position));"},
				{kind: "table", name: "account_world_release", sql: "CREATE TABLE IF NOT EXISTS account_world_release(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,namespace TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('public','withdrawn')),revision INTEGER NOT NULL CHECK(revision>0),title TEXT NOT NULL,renderer TEXT NOT NULL,medium TEXT NOT NULL,character TEXT NOT NULL,lede TEXT NOT NULL,PRIMARY KEY(world_id,space_id));"},
				{kind: "index", name: "account_world_release_ns", sql: "CREATE INDEX IF NOT EXISTS account_world_release_ns ON account_world_release(namespace,space_id,state);"},
				{kind: "table", name: "account_world_release_item", sql: "CREATE TABLE IF NOT EXISTS account_world_release_item(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,position INTEGER NOT NULL,ref TEXT NOT NULL,role TEXT NOT NULL,note TEXT NOT NULL,PRIMARY KEY(world_id,space_id,position));"},
				{kind: "trigger", name: "account_world_bound_delete", sql: "CREATE TRIGGER IF NOT EXISTS account_world_bound_delete BEFORE DELETE ON world WHEN EXISTS(SELECT 1 FROM account_world_binding WHERE world_id=OLD.id) BEGIN SELECT RAISE(ABORT,'bound world cannot be deleted'); END;"},
				{kind: "trigger", name: "account_world_bound_identity", sql: "CREATE TRIGGER IF NOT EXISTS account_world_bound_identity BEFORE UPDATE OF id,subject,namespace,world ON world WHEN EXISTS(SELECT 1 FROM account_world_binding WHERE world_id=OLD.id) AND (NEW.id IS NOT OLD.id OR NEW.subject IS NOT OLD.subject OR NEW.namespace IS NOT OLD.namespace OR NEW.world IS NOT OLD.world) BEGIN SELECT RAISE(ABORT,'bound world identity cannot change'); END;"},
			]
		},
		{
			macro:  "BF_STORE_CMS_ACCOUNT_WORLD_SCREENING"
			source: "cmd/BonfyreCMS/src/account_world.c"
			statements: [
				{kind: "table", name: "account_world_screening", sql: "CREATE TABLE IF NOT EXISTS account_world_screening(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,screening_id TEXT NOT NULL,starts TEXT NOT NULL,venue TEXT NOT NULL,ref TEXT NOT NULL,note TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('draft','announced','cancelled')),PRIMARY KEY(world_id,space_id,screening_id));"},
				{kind: "table", name: "account_world_club", sql: "CREATE TABLE IF NOT EXISTS account_world_club(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('open','closed')),PRIMARY KEY(world_id,space_id));"},
				{kind: "table", name: "account_world_club_member", sql: "CREATE TABLE IF NOT EXISTS account_world_club_member(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,member TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('member','left','removed')),PRIMARY KEY(world_id,space_id,member));"},
				{kind: "table", name: "account_world_club_pick", sql: "CREATE TABLE IF NOT EXISTS account_world_club_pick(pick_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,space_id TEXT NOT NULL,ref TEXT NOT NULL,note TEXT NOT NULL,proposed_by TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('open','hidden')));"},
				{kind: "index", name: "account_world_club_pick_space", sql: "CREATE INDEX IF NOT EXISTS account_world_club_pick_space ON account_world_club_pick(world_id,space_id,state);"},
				{kind: "table", name: "account_world_club_vote", sql: "CREATE TABLE IF NOT EXISTS account_world_club_vote(pick_id INTEGER NOT NULL,member TEXT NOT NULL,PRIMARY KEY(pick_id,member));"},
				{kind: "table", name: "account_world_club_post", sql: "CREATE TABLE IF NOT EXISTS account_world_club_post(post_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,space_id TEXT NOT NULL,author TEXT NOT NULL,ref TEXT NOT NULL,body TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('shown','hidden','retracted')));"},
				{kind: "index", name: "account_world_club_post_space", sql: "CREATE INDEX IF NOT EXISTS account_world_club_post_space ON account_world_club_post(world_id,space_id,post_id);"},
				{kind: "table", name: "account_world_note", sql: "CREATE TABLE IF NOT EXISTS account_world_note(note_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,ref TEXT NOT NULL,watched TEXT NOT NULL,rating INTEGER NOT NULL CHECK(rating BETWEEN 0 AND 10),rewatch INTEGER NOT NULL CHECK(rewatch IN(0,1)),body TEXT NOT NULL,visibility TEXT NOT NULL CHECK(visibility IN('private','public')));"},
				{kind: "index", name: "account_world_note_day", sql: "CREATE INDEX IF NOT EXISTS account_world_note_day ON account_world_note(world_id,watched,note_id);"},
			]
		},
		{
			macro:  "BF_STORE_CMS_APPLICATION_WORKSPACE"
			source: "cmd/BonfyreCMS/src/workspace_store.h"
			statements: [
				{kind: "table", name: "_application_workspace", sql: "CREATE TABLE IF NOT EXISTS _application_workspace(binding_id TEXT PRIMARY KEY,cms_id INTEGER NOT NULL UNIQUE,installation_id INTEGER NOT NULL,family TEXT NOT NULL,release TEXT NOT NULL,entry TEXT NOT NULL);"},
				{kind: "index", name: "_application_workspace_install", sql: "CREATE INDEX IF NOT EXISTS _application_workspace_install ON _application_workspace(installation_id);"},
				{kind: "table", name: "_workspace_body", sql: "CREATE TABLE IF NOT EXISTS _workspace_body(scope_key TEXT NOT NULL,digest TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope_key,digest));"},
				{kind: "table", name: "_workspace_record", sql: "CREATE TABLE IF NOT EXISTS _workspace_record(scope_key TEXT NOT NULL,record_key TEXT NOT NULL,revision INTEGER NOT NULL,body_digest TEXT,deleted INTEGER NOT NULL,actor TEXT NOT NULL,PRIMARY KEY(scope_key,record_key));"},
				{kind: "table", name: "_workspace_blob", sql: "CREATE TABLE IF NOT EXISTS _workspace_blob(id INTEGER PRIMARY KEY,scope_key TEXT NOT NULL,digest TEXT NOT NULL,bytes INTEGER NOT NULL,complete INTEGER NOT NULL,body BLOB NOT NULL,UNIQUE(scope_key,digest));"},
				{kind: "table", name: "_workspace_blob_piece", sql: "CREATE TABLE IF NOT EXISTS _workspace_blob_piece(blob_id INTEGER NOT NULL,offset INTEGER NOT NULL,bytes INTEGER NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(blob_id,offset));"},
				{kind: "table", name: "_workspace_epoch", sql: "CREATE TABLE IF NOT EXISTS _workspace_epoch(scope_key TEXT PRIMARY KEY,revision INTEGER NOT NULL);"},
				{kind: "table", name: "_workspace_operation", sql: "CREATE TABLE IF NOT EXISTS _workspace_operation(scope_key TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,actor TEXT NOT NULL,PRIMARY KEY(scope_key,request_id));"},
				{kind: "table", name: "_workspace_operation_row", sql: "CREATE TABLE IF NOT EXISTS _workspace_operation_row(scope_key TEXT NOT NULL,request_id TEXT NOT NULL,ordinal INTEGER NOT NULL,record_key TEXT NOT NULL,revision INTEGER NOT NULL,body_digest TEXT,deleted INTEGER NOT NULL,PRIMARY KEY(scope_key,request_id,ordinal));"},
			]
		},
		{
			macro:  "BF_STORE_CMS_PACK_INSTALLATION_STATE"
			source: "cmd/BonfyreCMS/src/pack_lifecycle.h"
			statements: [
				{kind: "table", name: "_pack_installation_state", sql: "CREATE TABLE IF NOT EXISTS _pack_installation_state(installation_id INTEGER PRIMARY KEY,revision INTEGER NOT NULL,source_digest TEXT NOT NULL,last_operation TEXT NOT NULL,last_actor TEXT NOT NULL);"},
				{kind: "table", name: "_pack_installation_pin", sql: "CREATE TABLE IF NOT EXISTS _pack_installation_pin(installation_id INTEGER NOT NULL,pack TEXT NOT NULL,version TEXT NOT NULL,root TEXT NOT NULL,PRIMARY KEY(installation_id,pack));"},
			]
		},
	]
}
