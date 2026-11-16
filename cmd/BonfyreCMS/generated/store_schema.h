/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRECMS_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRECMS_H
/* cmd/BonfyreCMS/src/ephemeral.c */
#define BF_STORE_CMS_OPS \
    "CREATE TABLE IF NOT EXISTS _ops (  id        INTEGER PRIMARY KEY AUTOINCREMENT,  op_type   TEXT NOT NULL,  target    TEXT NOT NULL,  target_id INTEGER,  namespace TEXT NOT NULL DEFAULT 'root',  payload   TEXT NOT NULL,  hash      TEXT NOT NULL,  parent    TEXT,  ts        TEXT NOT NULL,  actor     TEXT DEFAULT '');"
/* cmd/BonfyreCMS/src/ephemeral.c */
#define BF_STORE_CMS_IDX_OPS_TARGET \
    "CREATE INDEX IF NOT EXISTS idx_ops_target ON _ops(target, target_id);"
/* cmd/BonfyreCMS/src/ephemeral.c */
#define BF_STORE_CMS_IDX_OPS_HASH \
    "CREATE INDEX IF NOT EXISTS idx_ops_hash ON _ops(hash);"
/* cmd/BonfyreCMS/src/ephemeral.c */
#define BF_STORE_CMS_IDX_OPS_NS \
    "CREATE INDEX IF NOT EXISTS idx_ops_ns ON _ops(namespace);"
/* cmd/BonfyreCMS/src/ephemeral.c */
#define BF_STORE_CMS_IDX_OPS_HASH_UNIQ \
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_ops_hash_uniq ON _ops(hash);"
/* cmd/BonfyreCMS/src/household_members.h */
#define BF_STORE_CMS_HOUSEHOLD_AUTHORITY \
    "CREATE TABLE IF NOT EXISTS _household_authority(household_row INTEGER PRIMARY KEY,epoch INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS _household_membership(household_row INTEGER NOT NULL,member TEXT NOT NULL,role TEXT NOT NULL,revision INTEGER NOT NULL,authority_epoch INTEGER NOT NULL,expires_at INTEGER NOT NULL,granted_by TEXT NOT NULL,PRIMARY KEY(household_row,member));" \
    "INSERT OR IGNORE INTO _household_authority SELECT id,1 FROM household;" \
    "CREATE TRIGGER IF NOT EXISTS _household_authority_changed AFTER UPDATE OF steward,status,household_id,namespace ON household WHEN OLD.steward IS NOT NEW.steward OR OLD.status IS NOT NEW.status OR OLD.household_id IS NOT NEW.household_id OR OLD.namespace IS NOT NEW.namespace BEGIN INSERT INTO _household_authority VALUES(NEW.id,1) ON CONFLICT(household_row) DO UPDATE SET epoch=epoch+1; END;" \
    "CREATE TRIGGER IF NOT EXISTS _household_authority_deleted AFTER DELETE ON household BEGIN UPDATE _household_authority SET epoch=epoch+1 WHERE household_row=OLD.id; END;" \
    "CREATE TRIGGER IF NOT EXISTS _household_authority_inserted AFTER INSERT ON household BEGIN INSERT INTO _household_authority VALUES(NEW.id,1) ON CONFLICT(household_row) DO UPDATE SET epoch=epoch+1; END;"
/* cmd/BonfyreCMS/src/canonical.c */
#define BF_STORE_CMS_FAMILIES \
    "CREATE TABLE IF NOT EXISTS _families (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  family_hash   TEXT NOT NULL,  content_type  TEXT NOT NULL,  generator     TEXT,  member_count  INTEGER DEFAULT 0,  created_at    TEXT NOT NULL,  UNIQUE(family_hash, content_type));"
/* cmd/BonfyreCMS/src/canonical.c */
#define BF_STORE_CMS_IDX_FAMILIES_HASH \
    "CREATE INDEX IF NOT EXISTS idx_families_hash ON _families(family_hash);"
/* cmd/BonfyreCMS/src/canonical.c */
#define BF_STORE_CMS_IDX_FAMILIES_TYPE \
    "CREATE INDEX IF NOT EXISTS idx_families_type ON _families(content_type);"
/* cmd/BonfyreCMS/src/canonical.c */
#define BF_STORE_CMS_FAMILY_MEMBERS \
    "CREATE TABLE IF NOT EXISTS _family_members (  family_id     INTEGER NOT NULL,  target_type   TEXT NOT NULL,  target_id     INTEGER NOT NULL,  bindings      TEXT,  PRIMARY KEY(target_type, target_id),  FOREIGN KEY(family_id) REFERENCES _families(id));"
/* cmd/BonfyreCMS/src/transfer.c */
#define BF_STORE_CMS_TRANSFER_TEMPLATES \
    "CREATE TABLE IF NOT EXISTS _transfer_templates (  template_id  INTEGER PRIMARY KEY AUTOINCREMENT,  name         TEXT NOT NULL,  family_id    TEXT NOT NULL,  generator    TEXT NOT NULL,  arity        INTEGER NOT NULL,  member_count INTEGER NOT NULL DEFAULT 0,  created_at   TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS _transfer_log (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  donor_id      TEXT NOT NULL,  recipient_id  TEXT NOT NULL,  similarity    REAL NOT NULL,  positions_seeded INTEGER NOT NULL DEFAULT 0,  transferred_at TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_transfer_tpl_gen   ON _transfer_templates(generator);"
/* cmd/BonfyreCMS/src/incremental.c */
#define BF_STORE_CMS_INCR_EIGEN \
    "CREATE TABLE IF NOT EXISTS _incr_eigen (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  family_id  INTEGER NOT NULL,  position   INTEGER NOT NULL,  field_name TEXT NOT NULL,  count      INTEGER DEFAULT 0,  mean       REAL DEFAULT 0.0,  m2         REAL DEFAULT 0.0,  variance   REAL DEFAULT 0.0,  eigenvalue REAL DEFAULT 0.0,  min_val    REAL DEFAULT 0.0,  max_val    REAL DEFAULT 0.0,  is_numeric INTEGER DEFAULT 0,  distinct_count INTEGER DEFAULT 0,  updated_at TEXT NOT NULL,  UNIQUE(family_id, position));"
/* cmd/BonfyreCMS/src/incremental.c */
#define BF_STORE_CMS_IDX_IEIG_FAM \
    "CREATE INDEX IF NOT EXISTS idx_ieig_fam ON _incr_eigen(family_id);"
/* cmd/BonfyreCMS/src/incremental.c */
#define BF_STORE_CMS_ABSORBED_CONSTANTS \
    "CREATE TABLE IF NOT EXISTS _absorbed_constants (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  family_id   INTEGER NOT NULL,  position    INTEGER NOT NULL,  field_name  TEXT NOT NULL,  const_value TEXT NOT NULL,  absorbed_at TEXT NOT NULL,  UNIQUE(family_id, position));"
/* cmd/BonfyreCMS/src/lt_egraph.c */
#define BF_STORE_CMS_EQUIVALENCES \
    "CREATE TABLE IF NOT EXISTS _equivalences (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  class_id      INTEGER NOT NULL,  repr_type     TEXT NOT NULL,  repr_hash     TEXT NOT NULL,  target_type   TEXT NOT NULL,  target_id     INTEGER NOT NULL,  materialized  INTEGER DEFAULT 0,  cost          REAL DEFAULT 1.0,  data          TEXT,  ts            TEXT NOT NULL);"
/* cmd/BonfyreCMS/src/lt_egraph.c */
#define BF_STORE_CMS_IDX_EQ_CLASS \
    "CREATE INDEX IF NOT EXISTS idx_eq_class ON _equivalences(class_id);"
/* cmd/BonfyreCMS/src/lt_egraph.c */
#define BF_STORE_CMS_IDX_EQ_TARGET \
    "CREATE INDEX IF NOT EXISTS idx_eq_target ON _equivalences(target_type, target_id);"
/* cmd/BonfyreCMS/src/lt_egraph.c */
#define BF_STORE_CMS_IDX_EQ_REPR \
    "CREATE INDEX IF NOT EXISTS idx_eq_repr ON _equivalences(target_type, target_id, repr_type);"
/* cmd/BonfyreCMS/src/lt_egraph.c */
#define BF_STORE_CMS_IDX_EQ_MAT \
    "CREATE INDEX IF NOT EXISTS idx_eq_mat ON _equivalences(materialized, cost);"
/* cmd/BonfyreCMS/src/lt_egraph.c */
#define BF_STORE_CMS_EGRAPH_META \
    "CREATE TABLE IF NOT EXISTS _egraph_meta (  key   TEXT PRIMARY KEY,  value TEXT NOT NULL);"
/* cmd/BonfyreCMS/src/provenance.c */
#define BF_STORE_CMS_MERKLE_TREE \
    "CREATE TABLE IF NOT EXISTS _merkle_tree (  node_id    INTEGER PRIMARY KEY AUTOINCREMENT,  ns         TEXT NOT NULL,  depth      INTEGER NOT NULL,  left_child INTEGER,  right_child INTEGER,  op_id      INTEGER,  hash       INTEGER NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_merkle_ns   ON _merkle_tree(ns, depth);" \
    "CREATE INDEX IF NOT EXISTS idx_merkle_op   ON _merkle_tree(op_id);" \
    "CREATE TABLE IF NOT EXISTS _compaction_manifests (  manifest_id  INTEGER PRIMARY KEY AUTOINCREMENT,  content_type TEXT NOT NULL,  merkle_root  INTEGER NOT NULL,  family_count INTEGER NOT NULL DEFAULT 0,  gen_hash     INTEGER NOT NULL,  eigen_hash   INTEGER NOT NULL,  created_at   TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS idx_manifest_ct   ON _compaction_manifests(content_type);"
/* cmd/BonfyreCMS/src/tensor_ops.c */
#define BF_STORE_CMS_TENSOR_EIGEN \
    "CREATE TABLE IF NOT EXISTS _tensor_eigen (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  family_id   INTEGER NOT NULL,  position    INTEGER NOT NULL,  field_name  TEXT NOT NULL,  eigenvalue  REAL DEFAULT 0.0,  variance    REAL DEFAULT 0.0,  mean        REAL DEFAULT 0.0,  updated_at  TEXT NOT NULL,  UNIQUE(family_id, position));"
/* cmd/BonfyreCMS/src/tensor_ops.c */
#define BF_STORE_CMS_IDX_TEIG_FAM \
    "CREATE INDEX IF NOT EXISTS idx_teig_fam ON _tensor_eigen(family_id);"
/* cmd/BonfyreCMS/src/tensor_ops.c */
#define BF_STORE_CMS_TENSOR_CELLS \
    "CREATE TABLE IF NOT EXISTS _tensor_cells (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  family_id   INTEGER NOT NULL,  target_type TEXT NOT NULL,  target_id   INTEGER NOT NULL,  position    INTEGER NOT NULL,  field_name  TEXT NOT NULL,  val_text    TEXT,  val_num     REAL,  val_type    INTEGER DEFAULT 0,  UNIQUE(family_id, target_id, position));"
/* cmd/BonfyreCMS/src/tensor_ops.c */
#define BF_STORE_CMS_IDX_TCELL_FAM \
    "CREATE INDEX IF NOT EXISTS idx_tcell_fam ON _tensor_cells(family_id, position);"
/* cmd/BonfyreCMS/src/tensor_ops.c */
#define BF_STORE_CMS_IDX_TCELL_TARGET \
    "CREATE INDEX IF NOT EXISTS idx_tcell_target ON _tensor_cells(target_type, target_id);"
/* cmd/BonfyreCMS/src/tensor_ops.c */
#define BF_STORE_CMS_IDX_TCELL_FIELD \
    "CREATE INDEX IF NOT EXISTS idx_tcell_field ON _tensor_cells(family_id, field_name);"
/* cmd/BonfyreCMS/src/meta_gen.c */
#define BF_STORE_CMS_META_FAMILIES \
    "CREATE TABLE IF NOT EXISTS _meta_families (  meta_family_id INTEGER PRIMARY KEY AUTOINCREMENT,  content_type   TEXT NOT NULL,  meta_generator TEXT NOT NULL,  meta_arity     INTEGER NOT NULL DEFAULT 0,  member_count   INTEGER NOT NULL DEFAULT 0,  level          INTEGER NOT NULL DEFAULT 2,  compression    REAL NOT NULL DEFAULT 0,  created_at     TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS _meta_members (  meta_family_id INTEGER NOT NULL,  family_id      TEXT NOT NULL,  generator      TEXT NOT NULL,  meta_bindings  TEXT NOT NULL DEFAULT '[]',  PRIMARY KEY (meta_family_id, family_id));" \
    "CREATE INDEX IF NOT EXISTS idx_meta_members_ct   ON _meta_families(content_type);" \
    "CREATE INDEX IF NOT EXISTS idx_meta_members_fam   ON _meta_members(family_id);"
/* cmd/BonfyreCMS/src/hybrid_reduce.c */
#define BF_STORE_CMS_REDUCE_MEMO \
    "CREATE TABLE IF NOT EXISTS _reduce_memo (  family_id  TEXT NOT NULL,  target_id  INTEGER NOT NULL,  strategy   INTEGER NOT NULL,  result     TEXT NOT NULL,  created_at TEXT NOT NULL,  PRIMARY KEY (family_id, target_id));" \
    "CREATE TABLE IF NOT EXISTS _reduce_profiles (  family_id       TEXT NOT NULL,  strategy        INTEGER NOT NULL,  elapsed_us      INTEGER NOT NULL,  result_bytes    INTEGER NOT NULL,  profiled_at     TEXT NOT NULL,  PRIMARY KEY (family_id, strategy));" \
    "CREATE INDEX IF NOT EXISTS idx_memo_family   ON _reduce_memo(family_id);"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_CONTENT_TYPES \
    "CREATE TABLE IF NOT EXISTS _content_types (  name TEXT PRIMARY KEY,  schema_json TEXT NOT NULL,  created_at TEXT NOT NULL,  updated_at TEXT NOT NULL);"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_RELATIONS \
    "CREATE TABLE IF NOT EXISTS _relations (  id INTEGER PRIMARY KEY AUTOINCREMENT,  source_type TEXT NOT NULL,  source_id INTEGER NOT NULL,  target_type TEXT NOT NULL,  target_id INTEGER NOT NULL,  relation TEXT NOT NULL,  kind TEXT NOT NULL,  position INTEGER DEFAULT 0,  UNIQUE(source_type, source_id, target_type, target_id, relation));"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_IDX_REL_SOURCE \
    "CREATE INDEX IF NOT EXISTS idx_rel_source ON _relations(source_type, source_id);"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_IDX_REL_TARGET \
    "CREATE INDEX IF NOT EXISTS idx_rel_target ON _relations(target_type, target_id);"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_HOOKS \
    "CREATE TABLE IF NOT EXISTS _hooks (  id INTEGER PRIMARY KEY AUTOINCREMENT,  content_type TEXT NOT NULL,  event TEXT NOT NULL,  command TEXT NOT NULL,  enabled INTEGER DEFAULT 1);"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_PERMISSIONS \
    "CREATE TABLE IF NOT EXISTS _permissions (  token_hash TEXT NOT NULL,  content_type TEXT NOT NULL,  action TEXT NOT NULL,  namespace_scope TEXT DEFAULT 'root',  PRIMARY KEY(token_hash, content_type, action));"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_TOKENS \
    "CREATE TABLE IF NOT EXISTS _tokens (  token_hash TEXT PRIMARY KEY,  label TEXT,  scope TEXT DEFAULT 'root',  created_at TEXT NOT NULL,  expires_at TEXT,  active INTEGER DEFAULT 1);"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_W \
    "CREATE TABLE IF NOT EXISTS \"%w\" (id INTEGER PRIMARY KEY AUTOINCREMENT,namespace TEXT DEFAULT 'root' NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,published_at TEXT,status TEXT DEFAULT 'draft'"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_IDX_W_NS \
    "CREATE INDEX IF NOT EXISTS \"idx_%w_ns\" ON \"%w\"(namespace)"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_IDX_W_W_NS \
    "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_%w_%w_ns\" ON \"%w\"(namespace,\"%w\")"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_IDX_W_W \
    "CREATE UNIQUE INDEX IF NOT EXISTS \"idx_%w_%w\" ON \"%w\"(\"%w\")"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_PUBLICATION_DELTA_OUTBOX \
    "CREATE TABLE IF NOT EXISTS _publication_delta_outbox(namespace TEXT NOT NULL,source TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(namespace,source))"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_PAGE_WRITE_RETIRED \
    "CREATE TEMP TABLE page_write_retired AS SELECT id,coordinate,status FROM site_fragment WHERE 0"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_BASELINE_RAW \
    "CREATE TABLE IF NOT EXISTS _baseline_raw (id INTEGER PRIMARY KEY, data TEXT)"
/* cmd/BonfyreCMS/src/main.c */
#define BF_STORE_CMS_DEFERRED_BENCH \
    "CREATE TABLE IF NOT EXISTS _deferred_bench (id INTEGER PRIMARY KEY, title TEXT, slug TEXT, vertical TEXT, base_price INTEGER, min_threshold INTEGER, current_stake_count INTEGER, offer_status TEXT, namespace TEXT, created_at TEXT, updated_at TEXT, status TEXT)"
/* cmd/BonfyreCMS/src/ann_index.c */
#define BF_STORE_CMS_ANN_BUCKETS \
    "CREATE TABLE IF NOT EXISTS _ann_buckets (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  content_type TEXT NOT NULL,  table_idx   INTEGER NOT NULL,  bucket_hash TEXT NOT NULL,  family_id   INTEGER NOT NULL,  target_id   INTEGER NOT NULL,  UNIQUE(content_type, table_idx, target_id));"
/* cmd/BonfyreCMS/src/ann_index.c */
#define BF_STORE_CMS_IDX_ANN_LOOKUP \
    "CREATE INDEX IF NOT EXISTS idx_ann_lookup ON _ann_buckets(content_type, table_idx, bucket_hash);"
/* cmd/BonfyreCMS/src/ann_index.c */
#define BF_STORE_CMS_IDX_ANN_TARGET \
    "CREATE INDEX IF NOT EXISTS idx_ann_target ON _ann_buckets(content_type, target_id);"
/* cmd/BonfyreCMS/src/ann_index.c */
#define BF_STORE_CMS_ANN_PROJECTIONS \
    "CREATE TABLE IF NOT EXISTS _ann_projections (  id         INTEGER PRIMARY KEY AUTOINCREMENT,  content_type TEXT NOT NULL,  table_idx  INTEGER NOT NULL,  bit_idx    INTEGER NOT NULL,  dimension  INTEGER NOT NULL,  weight     REAL NOT NULL,  UNIQUE(content_type, table_idx, bit_idx, dimension));"
/* cmd/BonfyreCMS/src/ann_index.c */
#define BF_STORE_CMS_ANN_QUANTIZED \
    "CREATE TABLE IF NOT EXISTS _ann_quantized (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  content_type  TEXT NOT NULL,  family_id     INTEGER NOT NULL,  target_id     INTEGER NOT NULL,  dim           INTEGER NOT NULL,  rot_dim       INTEGER NOT NULL,  norm          REAL NOT NULL,  clip          REAL NOT NULL,  residual_avg  REAL NOT NULL,  q4_codes      BLOB NOT NULL,  residual_bits BLOB NOT NULL,  UNIQUE(content_type, target_id));"
/* cmd/BonfyreCMS/src/ann_index.c */
#define BF_STORE_CMS_IDX_ANN_QUANTIZED_LOOKUP \
    "CREATE INDEX IF NOT EXISTS idx_ann_quantized_lookup ON _ann_quantized(content_type, target_id);"
/* cmd/BonfyreCMS/src/synthesis.c */
#define BF_STORE_CMS_SYNTHESIS_CANDIDATES \
    "CREATE TABLE IF NOT EXISTS _synthesis_candidates (  id            INTEGER PRIMARY KEY AUTOINCREMENT,  content_type  TEXT NOT NULL,  generator     TEXT NOT NULL,  arity         INTEGER DEFAULT 0,  coverage      INTEGER DEFAULT 0,  score         REAL DEFAULT 0.0,  compression   REAL DEFAULT 0.0,  annotations   TEXT,  created_at    TEXT NOT NULL,  UNIQUE(content_type, generator));"
/* cmd/BonfyreCMS/src/synthesis.c */
#define BF_STORE_CMS_IDX_SYNTH_CT \
    "CREATE INDEX IF NOT EXISTS idx_synth_ct ON _synthesis_candidates(content_type, score DESC);"
/* cmd/BonfyreCMS/src/delta_sync.c */
#define BF_STORE_CMS_SYNC_STATE \
    "CREATE TABLE IF NOT EXISTS _sync_state (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  remote_id   TEXT NOT NULL,  ns          TEXT NOT NULL,  last_hash   TEXT NOT NULL,  last_op_id  INTEGER DEFAULT 0,  synced_at   TEXT NOT NULL,  UNIQUE(remote_id, ns));"
/* cmd/BonfyreCMS/src/delta_sync.c */
#define BF_STORE_CMS_IDX_SYNC_REMOTE \
    "CREATE INDEX IF NOT EXISTS idx_sync_remote ON _sync_state(remote_id, ns);"
/* cmd/BonfyreCMS/src/type_check.c */
#define BF_STORE_CMS_TYPE_CONSTRAINTS \
    "CREATE TABLE IF NOT EXISTS _type_constraints (  id          INTEGER PRIMARY KEY AUTOINCREMENT,  family_id   INTEGER NOT NULL,  position    INTEGER NOT NULL,  field_name  TEXT NOT NULL,  kind        INTEGER NOT NULL,  range_lo    REAL DEFAULT 0.0,  range_hi    REAL DEFAULT 0.0,  enum_vals   TEXT,  pattern     TEXT,  is_optional INTEGER DEFAULT 0,  inferred_at TEXT NOT NULL,  UNIQUE(family_id, position));"
/* cmd/BonfyreCMS/src/type_check.c */
#define BF_STORE_CMS_IDX_TC_FAM \
    "CREATE INDEX IF NOT EXISTS idx_tc_fam ON _type_constraints(family_id);"
/* cmd/BonfyreCMS/src/account_world.c */
#define BF_STORE_CMS_ACCOUNT_WORLD_BINDING \
    "CREATE TABLE IF NOT EXISTS account_world_binding(world_id INTEGER PRIMARY KEY,subject TEXT NOT NULL,namespace TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS account_world_space(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,title TEXT NOT NULL,renderer TEXT NOT NULL,template_ref TEXT NOT NULL,layout_ref TEXT NOT NULL,revision INTEGER NOT NULL CHECK(revision>0),PRIMARY KEY(world_id,space_id));" \
    "CREATE TABLE IF NOT EXISTS account_world_space_style(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,medium TEXT NOT NULL,character TEXT NOT NULL,lede TEXT NOT NULL,PRIMARY KEY(world_id,space_id));" \
    "CREATE TABLE IF NOT EXISTS account_world_item(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,position INTEGER NOT NULL CHECK(position BETWEEN 1 AND 24),ref TEXT NOT NULL,role TEXT NOT NULL,note TEXT NOT NULL,PRIMARY KEY(world_id,space_id,position));" \
    "CREATE TABLE IF NOT EXISTS account_world_release(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,namespace TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('public','withdrawn')),revision INTEGER NOT NULL CHECK(revision>0),title TEXT NOT NULL,renderer TEXT NOT NULL,medium TEXT NOT NULL,character TEXT NOT NULL,lede TEXT NOT NULL,PRIMARY KEY(world_id,space_id));" \
    "CREATE INDEX IF NOT EXISTS account_world_release_ns ON account_world_release(namespace,space_id,state);" \
    "CREATE TABLE IF NOT EXISTS account_world_release_item(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,position INTEGER NOT NULL,ref TEXT NOT NULL,role TEXT NOT NULL,note TEXT NOT NULL,PRIMARY KEY(world_id,space_id,position));" \
    "CREATE TRIGGER IF NOT EXISTS account_world_bound_delete BEFORE DELETE ON world WHEN EXISTS(SELECT 1 FROM account_world_binding WHERE world_id=OLD.id) BEGIN SELECT RAISE(ABORT,'bound world cannot be deleted'); END;" \
    "CREATE TRIGGER IF NOT EXISTS account_world_bound_identity BEFORE UPDATE OF id,subject,namespace,world ON world WHEN EXISTS(SELECT 1 FROM account_world_binding WHERE world_id=OLD.id) AND (NEW.id IS NOT OLD.id OR NEW.subject IS NOT OLD.subject OR NEW.namespace IS NOT OLD.namespace OR NEW.world IS NOT OLD.world) BEGIN SELECT RAISE(ABORT,'bound world identity cannot change'); END;"
/* cmd/BonfyreCMS/src/account_world.c */
#define BF_STORE_CMS_ACCOUNT_WORLD_SCREENING \
    "CREATE TABLE IF NOT EXISTS account_world_screening(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,screening_id TEXT NOT NULL,starts TEXT NOT NULL,venue TEXT NOT NULL,ref TEXT NOT NULL,note TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('draft','announced','cancelled')),PRIMARY KEY(world_id,space_id,screening_id));" \
    "CREATE TABLE IF NOT EXISTS account_world_club(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('open','closed')),PRIMARY KEY(world_id,space_id));" \
    "CREATE TABLE IF NOT EXISTS account_world_club_member(world_id INTEGER NOT NULL,space_id TEXT NOT NULL,member TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('member','left','removed')),PRIMARY KEY(world_id,space_id,member));" \
    "CREATE TABLE IF NOT EXISTS account_world_club_pick(pick_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,space_id TEXT NOT NULL,ref TEXT NOT NULL,note TEXT NOT NULL,proposed_by TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('open','hidden')));" \
    "CREATE INDEX IF NOT EXISTS account_world_club_pick_space ON account_world_club_pick(world_id,space_id,state);" \
    "CREATE TABLE IF NOT EXISTS account_world_club_vote(pick_id INTEGER NOT NULL,member TEXT NOT NULL,PRIMARY KEY(pick_id,member));" \
    "CREATE TABLE IF NOT EXISTS account_world_club_post(post_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,space_id TEXT NOT NULL,author TEXT NOT NULL,ref TEXT NOT NULL,body TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('shown','hidden','retracted')));" \
    "CREATE INDEX IF NOT EXISTS account_world_club_post_space ON account_world_club_post(world_id,space_id,post_id);" \
    "CREATE TABLE IF NOT EXISTS account_world_note(note_id INTEGER PRIMARY KEY,world_id INTEGER NOT NULL,ref TEXT NOT NULL,watched TEXT NOT NULL,rating INTEGER NOT NULL CHECK(rating BETWEEN 0 AND 10),rewatch INTEGER NOT NULL CHECK(rewatch IN(0,1)),body TEXT NOT NULL,visibility TEXT NOT NULL CHECK(visibility IN('private','public')));" \
    "CREATE INDEX IF NOT EXISTS account_world_note_day ON account_world_note(world_id,watched,note_id);"
/* cmd/BonfyreCMS/src/workspace_store.h */
#define BF_STORE_CMS_APPLICATION_WORKSPACE \
    "CREATE TABLE IF NOT EXISTS _application_workspace(binding_id TEXT PRIMARY KEY,cms_id INTEGER NOT NULL UNIQUE,installation_id INTEGER NOT NULL,family TEXT NOT NULL,release TEXT NOT NULL,entry TEXT NOT NULL);" \
    "CREATE INDEX IF NOT EXISTS _application_workspace_install ON _application_workspace(installation_id);" \
    "CREATE TABLE IF NOT EXISTS _workspace_body(scope_key TEXT NOT NULL,digest TEXT NOT NULL,body BLOB NOT NULL,PRIMARY KEY(scope_key,digest));" \
    "CREATE TABLE IF NOT EXISTS _workspace_record(scope_key TEXT NOT NULL,record_key TEXT NOT NULL,revision INTEGER NOT NULL,body_digest TEXT,deleted INTEGER NOT NULL,actor TEXT NOT NULL,PRIMARY KEY(scope_key,record_key));" \
    "CREATE TABLE IF NOT EXISTS _workspace_blob(id INTEGER PRIMARY KEY,scope_key TEXT NOT NULL,digest TEXT NOT NULL,bytes INTEGER NOT NULL,complete INTEGER NOT NULL,body BLOB NOT NULL,UNIQUE(scope_key,digest));" \
    "CREATE TABLE IF NOT EXISTS _workspace_blob_piece(blob_id INTEGER NOT NULL,offset INTEGER NOT NULL,bytes INTEGER NOT NULL,digest TEXT NOT NULL,PRIMARY KEY(blob_id,offset));" \
    "CREATE TABLE IF NOT EXISTS _workspace_epoch(scope_key TEXT PRIMARY KEY,revision INTEGER NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS _workspace_operation(scope_key TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,actor TEXT NOT NULL,PRIMARY KEY(scope_key,request_id));" \
    "CREATE TABLE IF NOT EXISTS _workspace_operation_row(scope_key TEXT NOT NULL,request_id TEXT NOT NULL,ordinal INTEGER NOT NULL,record_key TEXT NOT NULL,revision INTEGER NOT NULL,body_digest TEXT,deleted INTEGER NOT NULL,PRIMARY KEY(scope_key,request_id,ordinal));"
/* cmd/BonfyreCMS/src/pack_lifecycle.h */
#define BF_STORE_CMS_PACK_INSTALLATION_STATE \
    "CREATE TABLE IF NOT EXISTS _pack_installation_state(installation_id INTEGER PRIMARY KEY,revision INTEGER NOT NULL,source_digest TEXT NOT NULL,last_operation TEXT NOT NULL,last_actor TEXT NOT NULL);" \
    "CREATE TABLE IF NOT EXISTS _pack_installation_pin(installation_id INTEGER NOT NULL,pack TEXT NOT NULL,version TEXT NOT NULL,root TEXT NOT NULL,PRIMARY KEY(installation_id,pack));"
/* store cms: sqlite, declared by cmd/BonfyreCMS */
#define BF_STORE_CMS_DB_KIND "sqlite"
#define BF_STORE_CMS_DB_ENV "BONFYRE_CMS_DB"
#define BF_STORE_CMS_DB_DURABILITY { "cms", "WAL", "NORMAL", 1000, 67108864LL, 16384, 5000, 120000 }
#ifdef BF_STORE_DURABILITY_H
static const BfStoreDurability bf_store_policy_cms = { "cms", "WAL", "NORMAL", 1000, 67108864LL, 16384, 5000, 120000 };
#endif
#endif
