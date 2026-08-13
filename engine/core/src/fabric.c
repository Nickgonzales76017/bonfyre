#include "bonfyre_fabric.h"
#include "bonfyre_fabric_internal.h"
#include "bf_workgraph.h"
#include "bonfyre.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <sqlite3.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

static const char *SCHEMA =
    "PRAGMA foreign_keys=ON;"
    "CREATE TABLE IF NOT EXISTS fabric_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS roots("
    " id TEXT PRIMARY KEY,kind TEXT NOT NULL,locator TEXT NOT NULL UNIQUE,owner TEXT NOT NULL,"
    " authority_class TEXT NOT NULL,durability TEXT NOT NULL,trust_level TEXT NOT NULL,"
    " sensitivity TEXT NOT NULL,access_mode TEXT NOT NULL,retention TEXT NOT NULL,"
    " backup_policy TEXT NOT NULL,hash_strategy TEXT NOT NULL,watch_strategy TEXT NOT NULL,"
    " materialization_policy TEXT NOT NULL,runtime_visibility TEXT NOT NULL,created_at TEXT NOT NULL);"
    /* Git-specific identity for roots whose physical containment does not
     * imply Git ownership -- a path beneath the Bonfyre tree (e.g. a
     * Frappe app, HVM4) that belongs to its own independent repository.
     * One row per such root, referencing roots.id: this is the same
     * physical root graph, extended, not a competing registry. */
    "CREATE TABLE IF NOT EXISTS repository_roots("
    " root_id TEXT PRIMARY KEY REFERENCES roots(id),git_dir TEXT NOT NULL,"
    " worktree_locator TEXT NOT NULL,head_oid TEXT NOT NULL,branch TEXT NOT NULL,"
    " dirty_state_digest TEXT NOT NULL,dirty INTEGER NOT NULL,source_generation TEXT NOT NULL,"
    " parent_root_id TEXT,discovered_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS catalog("
    " id TEXT PRIMARY KEY,kind TEXT NOT NULL,name TEXT NOT NULL,version TEXT NOT NULL,"
    " input_schema TEXT NOT NULL,output_schema TEXT NOT NULL,effect_class TEXT NOT NULL,"
    " authorization_class TEXT NOT NULL,execution_lane TEXT NOT NULL,idempotency TEXT NOT NULL,"
    " retry_semantics TEXT NOT NULL,timeout_seconds INTEGER NOT NULL,source_ref TEXT NOT NULL,"
    " maturity TEXT NOT NULL,health_state TEXT NOT NULL,created_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS namespace_objects("
    " uri TEXT PRIMARY KEY,kind TEXT NOT NULL,owner TEXT NOT NULL,source_authority TEXT NOT NULL,"
    " native_id TEXT,version TEXT NOT NULL,locator TEXT NOT NULL,policy TEXT NOT NULL,"
    " sensitivity TEXT NOT NULL,freshness TEXT NOT NULL,evidence_state TEXT NOT NULL,"
    " operations TEXT NOT NULL,content_contract TEXT NOT NULL,query_contract TEXT NOT NULL,effect_contract TEXT NOT NULL,created_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS artifacts("
    " digest TEXT PRIMARY KEY,uri TEXT NOT NULL UNIQUE,media_type TEXT NOT NULL,source_uri TEXT,"
    " locator TEXT NOT NULL,bytes INTEGER NOT NULL,representation TEXT NOT NULL,created_at TEXT NOT NULL,"
    " FOREIGN KEY(uri) REFERENCES namespace_objects(uri));"
    "CREATE TABLE IF NOT EXISTS missions("
    " id TEXT PRIMARY KEY,status TEXT NOT NULL,context_generation TEXT NOT NULL,catalog_generation TEXT NOT NULL,"
    " provider_generation TEXT NOT NULL,input_snapshot TEXT NOT NULL,artifact_root TEXT NOT NULL,"
    " workgraph_cursor TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS workgraph_nodes("
    " mission_id TEXT NOT NULL,node_id TEXT NOT NULL,operator_id TEXT NOT NULL,input_uri TEXT,"
    " status TEXT NOT NULL,attempt INTEGER NOT NULL DEFAULT 0,retry_limit INTEGER NOT NULL DEFAULT 0,"
    " timeout_seconds INTEGER NOT NULL,depends_on TEXT NOT NULL,output_uri TEXT,lease_owner TEXT,claim_token_hash TEXT,claim_acquired_at_ms INTEGER,lease_expires_at_ms INTEGER,visibility_deadline_ms INTEGER,next_attempt_at_ms INTEGER,last_heartbeat_at_ms INTEGER,backoff_policy TEXT NOT NULL DEFAULT 'bounded:1',failure_class TEXT,failure_message TEXT,cancellation_state TEXT NOT NULL DEFAULT 'none',compensation_operator TEXT,parent_node TEXT,subgraph_id TEXT,fanout_group TEXT,fanin_required INTEGER NOT NULL DEFAULT 0,created_at_ms INTEGER,updated_at_ms INTEGER,terminal_at_ms INTEGER,receipt_id TEXT,"
    " PRIMARY KEY(mission_id,node_id),FOREIGN KEY(mission_id) REFERENCES missions(id));"
    "CREATE TABLE IF NOT EXISTS events("
    " id TEXT PRIMARY KEY,mission_id TEXT,task_id TEXT,attempt INTEGER NOT NULL,actor TEXT NOT NULL,"
    " operator_id TEXT,provider_id TEXT,model_id TEXT,start_at TEXT NOT NULL,end_at TEXT,duration_ms INTEGER,"
    " input_uri TEXT,output_uri TEXT,effect_class TEXT NOT NULL,status TEXT NOT NULL,error_code TEXT,receipt_id TEXT,"
    " FOREIGN KEY(mission_id) REFERENCES missions(id));"
    "CREATE TABLE IF NOT EXISTS effects("
    " id TEXT PRIMARY KEY,mission_id TEXT NOT NULL,effect_kind TEXT NOT NULL,target_uri TEXT NOT NULL,"
    " tier TEXT NOT NULL,intent TEXT NOT NULL,simulation TEXT NOT NULL,state TEXT NOT NULL,"
    " authority_class TEXT NOT NULL,created_at TEXT NOT NULL,approved_at TEXT,committed_at TEXT,receipt_id TEXT,"
    " FOREIGN KEY(mission_id) REFERENCES missions(id));"
    "CREATE TABLE IF NOT EXISTS application_records("
    " uri TEXT PRIMARY KEY,family TEXT NOT NULL,record_type TEXT NOT NULL,record_id TEXT NOT NULL,"
    " parent_uri TEXT,workflow_state TEXT NOT NULL,permissions TEXT NOT NULL,app_pack_revision TEXT NOT NULL,"
    " catalog_generation TEXT NOT NULL,event_id TEXT,receipt_id TEXT,created_at TEXT NOT NULL,"
    " UNIQUE(family,record_type,record_id));"
    "CREATE TABLE IF NOT EXISTS receipts("
    " id TEXT PRIMARY KEY,subject_kind TEXT NOT NULL,subject_id TEXT NOT NULL,content_hash TEXT NOT NULL,"
    " payload TEXT NOT NULL,created_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY,applied_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS declarations("
    " path TEXT PRIMARY KEY,kind TEXT NOT NULL,content_hash TEXT NOT NULL,generation TEXT NOT NULL,compiled_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS catalog_bindings("
    " operator_id TEXT PRIMARY KEY,binary_name TEXT,source_path TEXT,installed_path TEXT,source_hash TEXT,"
    " input_contract TEXT NOT NULL,output_contract TEXT NOT NULL,file_inputs TEXT NOT NULL,streaming TEXT NOT NULL,"
    " cwd_contract TEXT NOT NULL,environment_contract TEXT NOT NULL,timeout_seconds INTEGER NOT NULL,"
    " cancellation TEXT NOT NULL,retry_behavior TEXT NOT NULL,resource_requirements TEXT NOT NULL,"
    " health_probe TEXT NOT NULL,workload_probe TEXT NOT NULL,quality_probe TEXT NOT NULL,produced_families TEXT NOT NULL,"
    " catalog_generation TEXT NOT NULL,binding_state TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS composition_nodes("
    " composition_id TEXT NOT NULL,node_id TEXT NOT NULL,operator_id TEXT NOT NULL,depends_on TEXT NOT NULL,"
    " PRIMARY KEY(composition_id,node_id));"
    "CREATE TABLE IF NOT EXISTS effect_operations("
    " effect_id TEXT PRIMARY KEY,adapter_id TEXT NOT NULL,created_path TEXT,rollback_path TEXT,status TEXT NOT NULL,updated_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS execution_metrics("
    " event_id TEXT PRIMARY KEY,catalog_generation TEXT NOT NULL,runtime_generation TEXT NOT NULL,bytes_in INTEGER,bytes_out INTEGER,"
    " cpu_ms INTEGER,queue_delay_ms INTEGER,first_token_ms INTEGER,throughput REAL,projected_cost REAL,realized_cost REAL,quality_result TEXT);"
    "CREATE TABLE IF NOT EXISTS usage_ledger(event_id TEXT PRIMARY KEY,bytes_in INTEGER,bytes_out INTEGER,duration_ms INTEGER,created_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS economic_ledger(event_id TEXT PRIMARY KEY,projected_cost REAL,realized_cost REAL,created_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS value_ledger(event_id TEXT PRIMARY KEY,accepted INTEGER,created_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS operator_contracts("
    " id TEXT PRIMARY KEY,generation TEXT NOT NULL,invocation_kind TEXT NOT NULL,argv_template TEXT NOT NULL,"
    " input_binding TEXT NOT NULL,output_discovery TEXT NOT NULL,environment_allowlist TEXT NOT NULL,"
    " working_directory_policy TEXT NOT NULL,timeout_seconds INTEGER NOT NULL,output_limit_bytes INTEGER NOT NULL,"
    " retry_policy TEXT NOT NULL,workload_probe TEXT NOT NULL,quality_probe TEXT NOT NULL,"
    " source_path TEXT NOT NULL,source_hash TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS operator_contract_bindings("
    " operator_id TEXT PRIMARY KEY,contract_id TEXT NOT NULL,generation TEXT NOT NULL,family TEXT NOT NULL,argument_defaults TEXT NOT NULL,workload_fixture TEXT,quality_probe_override TEXT,"
    " FOREIGN KEY(operator_id) REFERENCES catalog(id),FOREIGN KEY(contract_id) REFERENCES operator_contracts(id));"
    "CREATE INDEX IF NOT EXISTS idx_events_mission ON events(mission_id,start_at);"
    "CREATE INDEX IF NOT EXISTS idx_effects_mission ON effects(mission_id,state);"
    "CREATE INDEX IF NOT EXISTS idx_nodes_mission ON workgraph_nodes(mission_id,status);";

static int ensure_runtime_state_root(sqlite3 *db, const char *state_dir, FILE *err);

static void now_utc(char out[32]) {
    time_t value = time(NULL);
    struct tm *utc = gmtime(&value);
    if (!utc || strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", utc) == 0)
        snprintf(out, 32, "1970-01-01T00:00:00Z");
}

static int ensure_dir_recursive(const char *path) {
    char work[PATH_MAX];
    size_t n = strlen(path);
    if (!path[0] || n >= sizeof(work)) return -1;
    memcpy(work, path, n + 1);
    for (char *p = work + 1; *p; ++p) {
        if (*p != '/') continue;
        *p = '\0';
        if (mkdir(work, 0700) != 0 && errno != EEXIST) return -1;
        *p = '/';
    }
    return (mkdir(work, 0700) == 0 || errno == EEXIST) ? 0 : -1;
}

int bf_fabric_bootstrap(char *state_dir, size_t state_dir_size,
                        char *db_path, size_t db_path_size, FILE *err) {
    const char *override = getenv("BONFYRE_STATE_DIR");
    const char *home = getenv("HOME");
#ifndef __APPLE__
    const char *xdg = getenv("XDG_DATA_HOME");
#endif
    if (override && override[0]) snprintf(state_dir, state_dir_size, "%s", override);
#ifdef __APPLE__
    else if (home && home[0]) snprintf(state_dir, state_dir_size, "%s/Library/Application Support/Bonfyre", home);
#else
    else if (xdg && xdg[0]) snprintf(state_dir, state_dir_size, "%s/bonfyre", xdg);
    else if (home && home[0]) snprintf(state_dir, state_dir_size, "%s/.local/share/bonfyre", home);
#endif
    else snprintf(state_dir, state_dir_size, "/tmp/bonfyre-%ld", (long)getuid());
    if (ensure_dir_recursive(state_dir) != 0) {
        fprintf(err, "fabric: cannot create state directory %s: %s\n", state_dir, strerror(errno));
        return -1;
    }
    snprintf(db_path, db_path_size, "%s/fabric.db", state_dir);
    return 0;
}

static int open_db(sqlite3 **db_out, FILE *err) {
    char state[PATH_MAX], db_path[PATH_MAX];
    if (bf_fabric_bootstrap(state, sizeof(state), db_path, sizeof(db_path), err) != 0) return -1;
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
        fprintf(err, "fabric: cannot open %s: %s\n", db_path, db ? sqlite3_errmsg(db) : "unknown error");
        sqlite3_close(db);
        return -1;
    }
    if (sqlite3_exec(db, SCHEMA, NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(err, "fabric: schema error: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }
    if (bf_workgraph_migrate_database(db, err) != 0) {
        sqlite3_close(db);
        return -1;
    }
    if (sqlite3_exec(db,
            "INSERT OR IGNORE INTO fabric_meta(key,value) VALUES"
            "('catalog_generation','uncompiled'),"
            "('workspace_generation','uncompiled'),"
            "('runtime_generation','fabric-0.1.0')",
            NULL, NULL, NULL) != SQLITE_OK) {
        fprintf(err, "fabric: metadata initialization failed: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }
    if (ensure_runtime_state_root(db, state, err) != 0) { sqlite3_close(db); return -1; }
    *db_out = db;
    return 0;
}

static int valid_id(const char *value) {
    if (!value || !value[0] || strlen(value) > 120) return 0;
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p)
        if (!(isalnum(*p) || *p == '-' || *p == '_' || *p == '.')) return 0;
    return 1;
}

static int valid_uri(const char *uri) {
    return uri && strncmp(uri, "bonfyre://", 10) == 0 && strchr(uri + 10, '/') != NULL && strlen(uri) < 1024;
}

static void make_id(char *out, size_t out_size, const char *prefix, const char *seed) {
    char material[2048], digest[65];
    char timestamp[32];
    now_utc(timestamp);
    snprintf(material, sizeof(material), "%s|%s|%s|%ld", prefix, seed ? seed : "", timestamp, (long)getpid());
    bf_sha256_hex((const uint8_t *)material, strlen(material), digest);
    snprintf(out, out_size, "%s-%.*s", prefix, 20, digest);
}

static int sql_step(sqlite3 *db, sqlite3_stmt *statement, FILE *err) {
    int rc = sqlite3_step(statement);
    if (rc != SQLITE_DONE) fprintf(err, "fabric: database write failed: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE ? 0 : -1;
}

static int ensure_runtime_state_root(sqlite3 *db, const char *state_dir, FILE *err) {
    char timestamp[32]; now_utc(timestamp);
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO roots(id,kind,locator,owner,authority_class,durability,trust_level,sensitivity,access_mode,retention,backup_policy,hash_strategy,watch_strategy,materialization_policy,runtime_visibility,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,"runtime-state",-1,SQLITE_STATIC); sqlite3_bind_text(st,2,"state",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,state_dir,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,4,"local-user",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,"fabric-core",-1,SQLITE_STATIC); sqlite3_bind_text(st,6,"durable",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,"local",-1,SQLITE_STATIC); sqlite3_bind_text(st,8,"standard",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,9,"read-write",-1,SQLITE_STATIC); sqlite3_bind_text(st,10,"policy-default",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,11,"user-managed",-1,SQLITE_STATIC); sqlite3_bind_text(st,12,"sha256",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,13,"none",-1,SQLITE_STATIC); sqlite3_bind_text(st,14,"native",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,15,"service",-1,SQLITE_STATIC); sqlite3_bind_text(st,16,timestamp,-1,SQLITE_TRANSIENT);
    return sql_step(db, st, err);
}

static int register_namespace(sqlite3 *db, const char *uri, const char *kind,
                              const char *native_id, const char *locator,
                              const char *evidence, const char *operations,
                              const char *content_contract,
                              const char *effect_contract, FILE *err) {
    char timestamp[32]; now_utc(timestamp);
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db, "INSERT OR IGNORE INTO namespace_objects(uri,kind,owner,source_authority,native_id,version,locator,policy,sensitivity,freshness,evidence_state,operations,content_contract,query_contract,effect_contract,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st,1,uri,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,kind,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,"local-user",-1,SQLITE_STATIC); sqlite3_bind_text(st,4,"fabric-core",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,native_id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,6,"1",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,locator,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,8,"default",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,9,"standard",-1,SQLITE_STATIC); sqlite3_bind_text(st,10,"current",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,11,evidence,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,12,operations,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,13,content_contract,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,14,"typed-lookup.v1",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,15,effect_contract,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,16,timestamp,-1,SQLITE_TRANSIENT);
    return sql_step(db, st, err);
}

static int record_receipt(sqlite3 *db, const char *subject_kind, const char *subject_id,
                          const char *payload, char receipt_id[64], FILE *err) {
    char timestamp[32], hash[65];
    now_utc(timestamp);
    make_id(receipt_id, 64, "rcpt", subject_id);
    bf_sha256_hex((const uint8_t *)payload, strlen(payload), hash);
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db, "INSERT INTO receipts(id,subject_kind,subject_id,content_hash,payload,created_at) VALUES(?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st, 1, receipt_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, subject_kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, subject_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, hash, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, payload, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, timestamp, -1, SQLITE_TRANSIENT);
    if (sql_step(db, st, err) != 0) return -1;
    char uri[256], locator[256];
    snprintf(uri, sizeof(uri), "bonfyre://receipt/%s", receipt_id);
    snprintf(locator, sizeof(locator), "receipt:%s", receipt_id);
    return register_namespace(db, uri, "receipt", receipt_id, locator, "hashed", "read,verify", "receipt.v1", "none", err);
}

static void print_usage(FILE *out) {
    fprintf(out,
        "Bonfyre Fabric %s\n\n"
        "Commands:\n"
        "  fabric init | status\n"
        "  root add <id> <kind> <path-or-uri> [owner]\n"
        "  root list\n"
        "  catalog seed | list | add <id> <name> <lane> <effect-class>\n"
        "  catalog compile <catalog.yaff>\n"
        "  catalog import-legacy <declaration.tsv>\n"
        "  artifact ingest <file> [media-type]\n"
        "  namespace show <bonfyre://...>\n"
        "  mission create <id> | show <id>\n"
        "  work add <mission> <node> <operator> [input-uri] | run <mission>\n"
        "  effect request <mission> <kind> <target-uri> [tier]\n"
        "  effect plan <effect-id> | approve <effect-id> | commit <effect-id>\n"
        "  receipt show <receipt-id>\n",
        BF_FABRIC_VERSION);
}

static int cmd_status(sqlite3 *db, FILE *out, FILE *err) {
    const char *tables[] = {"roots", "catalog", "catalog_bindings", "namespace_objects", "artifacts", "missions", "workgraph_nodes", "events", "usage_ledger", "economic_ledger", "value_ledger", "effects", "receipts"};
    char state[PATH_MAX], path[PATH_MAX];
    bf_fabric_bootstrap(state, sizeof(state), path, sizeof(path), err);
    fprintf(out, "state_dir=%s\ndatabase=%s\n", state, path);
    for (size_t i = 0; i < sizeof(tables) / sizeof(tables[0]); ++i) {
        char query[128]; sqlite3_stmt *st = NULL;
        snprintf(query, sizeof(query), "SELECT count(*) FROM %s", tables[i]);
        sqlite3_prepare_v2(db, query, -1, &st, NULL);
        int count = sqlite3_step(st) == SQLITE_ROW ? sqlite3_column_int(st, 0) : -1;
        sqlite3_finalize(st);
        fprintf(out, "%s=%d\n", tables[i], count);
    }
    return 0;
}

static int cmd_root_add(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    if (argc < 5 || !valid_id(argv[2])) { fprintf(err, "usage: root add <id> <kind> <path-or-uri> [owner]\n"); return 2; }
    static const char *kinds = "source estate generated runtime state data artifact model provider host workspace lab archive scratch secret";
    char needle[160]; snprintf(needle, sizeof(needle), " %s ", argv[3]);
    char padded[512]; snprintf(padded, sizeof(padded), " %s ", kinds);
    if (!strstr(padded, needle)) { fprintf(err, "fabric: invalid root kind: %s\n", argv[3]); return 2; }
    char timestamp[32]; now_utc(timestamp);
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO roots(id,kind,locator,owner,authority_class,durability,trust_level,sensitivity,access_mode,retention,backup_policy,hash_strategy,watch_strategy,materialization_policy,runtime_visibility,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &st, NULL);
    sqlite3_bind_text(st,1,argv[2],-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,argv[3],-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,3,argv[4],-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,4,argc > 5 ? argv[5] : "local-user",-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,5,"governed",-1,SQLITE_STATIC); sqlite3_bind_text(st,6,"durable",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,"local",-1,SQLITE_STATIC); sqlite3_bind_text(st,8,"standard",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,9,"read-write",-1,SQLITE_STATIC); sqlite3_bind_text(st,10,"policy-default",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,11,"declared",-1,SQLITE_STATIC); sqlite3_bind_text(st,12,"sha256",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,13,"bounded",-1,SQLITE_STATIC); sqlite3_bind_text(st,14,"reference",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,15,"scoped",-1,SQLITE_STATIC); sqlite3_bind_text(st,16,timestamp,-1,SQLITE_TRANSIENT);
    if (sql_step(db, st, err) != 0) return 1;
    fprintf(out, "root=%s kind=%s locator=%s\n", argv[2], argv[3], argv[4]);
    return 0;
}

static int cmd_root_list(sqlite3 *db, FILE *out) {
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db, "SELECT id,kind,locator,owner,access_mode FROM roots ORDER BY id", -1, &st, NULL);
    fprintf(out, "ID\tKIND\tLOCATOR\tOWNER\tMODE\n");
    while (sqlite3_step(st) == SQLITE_ROW)
        fprintf(out, "%s\t%s\t%s\t%s\t%s\n", sqlite3_column_text(st,0), sqlite3_column_text(st,1), sqlite3_column_text(st,2), sqlite3_column_text(st,3), sqlite3_column_text(st,4));
    sqlite3_finalize(st); return 0;
}

static int add_operator(sqlite3 *db, const char *id, const char *name, const char *lane,
                        const char *effect, const char *maturity, FILE *err) {
    char timestamp[32]; now_utc(timestamp);
    sqlite3_stmt *st = NULL;
    sqlite3_prepare_v2(db,
      "INSERT INTO catalog(id,kind,name,version,input_schema,output_schema,effect_class,authorization_class,execution_lane,idempotency,retry_semantics,timeout_seconds,source_ref,maturity,health_state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,execution_lane=excluded.execution_lane,effect_class=excluded.effect_class,maturity=excluded.maturity,health_state='declared'",
      -1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,"operator",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,name,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,4,"1.0.0",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,5,"bonfyre.artifact.v1",-1,SQLITE_STATIC); sqlite3_bind_text(st,6,"bonfyre.artifact.v1",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,7,effect,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,8,"mission-scoped",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,9,lane,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,10,"idempotent",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,11,"bounded:0",-1,SQLITE_STATIC); sqlite3_bind_int(st,12,30);
    sqlite3_bind_text(st,13,"estate/catalog.yaff",-1,SQLITE_STATIC); sqlite3_bind_text(st,14,maturity,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,15,"declared",-1,SQLITE_STATIC); sqlite3_bind_text(st,16,timestamp,-1,SQLITE_TRANSIENT);
    return sql_step(db,st,err);
}

/* The legacy command estate is imported from a checked-in declaration.  The
 * compiler never discovers command folders at runtime, so command truth is
 * reviewable, reproducible, and independent of the caller's working tree. */
static int import_legacy_catalog(sqlite3 *db, const char *path, FILE *out, FILE *err) {
    FILE *file = fopen(path, "r");
    if (!file) { fprintf(err, "fabric: cannot read catalog declaration %s: %s\n", path, strerror(errno)); return 1; }
    char line[1024]; int imported = 0;
    while (fgets(line, sizeof(line), file)) {
        char *command = line;
        while (*command == ' ' || *command == '\t') command++;
        if (!command[0] || command[0] == '#' || command[0] == '\n') continue;
        char *binary = strchr(command, '\t');
        if (!binary) { fprintf(err, "fabric: malformed legacy catalog row\n"); fclose(file); return 1; }
        *binary++ = '\0';
        char *module = strchr(binary, '\t');
        if (!module) { fprintf(err, "fabric: malformed legacy catalog row\n"); fclose(file); return 1; }
        *module++ = '\0';
        char *end = strpbrk(module, "\r\n"); if (end) *end = '\0';
        if (!valid_id(command) || !valid_id(binary) || !valid_id(module)) { fprintf(err, "fabric: invalid legacy catalog identity\n"); fclose(file); return 1; }
        char id[160]; snprintf(id, sizeof(id), "command.%s", command);
        if (add_operator(db, id, command, "process", "pure-read", "defined", err) != 0) { fclose(file); return 1; }
        sqlite3_stmt *source = NULL;
        sqlite3_prepare_v2(db, "UPDATE catalog SET source_ref=? WHERE id=?", -1, &source, NULL);
        sqlite3_bind_text(source, 1, binary, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(source, 2, id, -1, SQLITE_TRANSIENT);
        if (sql_step(db, source, err) != 0) { fclose(file); return 1; }
        imported++;
    }
    fclose(file);
    fprintf(out, "catalog_generation=bootstrap-1 legacy_identities=%d declaration=%s\n", imported, path);
    return 0;
}

static int compile_yaff_catalog(sqlite3 *db, const char *path, FILE *out, FILE *err) {
    FILE *file = fopen(path, "r");
    if (!file) { fprintf(err, "fabric: cannot read YaFF catalog %s: %s\n", path, strerror(errno)); return 1; }
    char line[1024], id[160] = "", version[64] = "1.0.0", input[256] = "bonfyre.artifact.v1";
    char output[256] = "bonfyre.artifact.v1", effect[96] = "pure-read", authorization[96] = "mission-scoped";
    char lane[96] = "", maturity[96] = "defined";
    int compiled = 0;
    while (fgets(line, sizeof(line), file)) {
        char *cursor = line;
        while (*cursor == ' ' || *cursor == '\t') cursor++;
        if (!cursor[0] || cursor[0] == '#' || cursor[0] == '\n') continue;
        if (!strncmp(cursor, "operator ", 9)) {
            if (id[0]) {
                if (!lane[0]) { fprintf(err, "fabric: operator %s is missing lane\n", id); fclose(file); return 1; }
                if (add_operator(db, id, id, lane, effect, maturity, err) != 0) { fclose(file); return 1; }
                sqlite3_stmt *update = NULL;
                sqlite3_prepare_v2(db, "UPDATE catalog SET version=?,input_schema=?,output_schema=?,authorization_class=?,source_ref=? WHERE id=?", -1, &update, NULL);
                sqlite3_bind_text(update,1,version,-1,SQLITE_TRANSIENT); sqlite3_bind_text(update,2,input,-1,SQLITE_TRANSIENT);
                sqlite3_bind_text(update,3,output,-1,SQLITE_TRANSIENT); sqlite3_bind_text(update,4,authorization,-1,SQLITE_TRANSIENT);
                sqlite3_bind_text(update,5,path,-1,SQLITE_TRANSIENT); sqlite3_bind_text(update,6,id,-1,SQLITE_TRANSIENT);
                if (sql_step(db, update, err) != 0) { fclose(file); return 1; }
                compiled++;
            }
            if (sscanf(cursor, "operator %159s", id) != 1 || !valid_id(id)) { fprintf(err, "fabric: invalid YaFF operator identity\n"); fclose(file); return 1; }
            snprintf(version,sizeof(version),"1.0.0"); snprintf(input,sizeof(input),"bonfyre.artifact.v1"); snprintf(output,sizeof(output),"bonfyre.artifact.v1");
            snprintf(effect,sizeof(effect),"pure-read"); snprintf(authorization,sizeof(authorization),"mission-scoped"); lane[0] = '\0'; snprintf(maturity,sizeof(maturity),"defined");
            continue;
        }
        if (!id[0]) continue;
        char key[64], value[256];
        if (sscanf(cursor, "%63s %255s", key, value) != 2) { fprintf(err, "fabric: malformed YaFF field for %s\n", id); fclose(file); return 1; }
        if (!strcmp(key,"version")) snprintf(version,sizeof(version),"%s",value);
        else if (!strcmp(key,"input")) snprintf(input,sizeof(input),"%s",value);
        else if (!strcmp(key,"output")) snprintf(output,sizeof(output),"%s",value);
        else if (!strcmp(key,"effect")) snprintf(effect,sizeof(effect),"%s",value);
        else if (!strcmp(key,"authorization")) snprintf(authorization,sizeof(authorization),"%s",value);
        else if (!strcmp(key,"lane")) snprintf(lane,sizeof(lane),"%s",value);
        else if (!strcmp(key,"maturity")) snprintf(maturity,sizeof(maturity),"%s",value);
        else { fprintf(err, "fabric: unknown YaFF field %s for %s\n", key, id); fclose(file); return 1; }
    }
    if (id[0]) {
        if (!lane[0]) { fprintf(err, "fabric: operator %s is missing lane\n", id); fclose(file); return 1; }
        if (add_operator(db, id, id, lane, effect, maturity, err) != 0) { fclose(file); return 1; }
        sqlite3_stmt *update = NULL;
        sqlite3_prepare_v2(db, "UPDATE catalog SET version=?,input_schema=?,output_schema=?,authorization_class=?,source_ref=? WHERE id=?", -1, &update, NULL);
        sqlite3_bind_text(update,1,version,-1,SQLITE_TRANSIENT); sqlite3_bind_text(update,2,input,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(update,3,output,-1,SQLITE_TRANSIENT); sqlite3_bind_text(update,4,authorization,-1,SQLITE_TRANSIENT);
        sqlite3_bind_text(update,5,path,-1,SQLITE_TRANSIENT); sqlite3_bind_text(update,6,id,-1,SQLITE_TRANSIENT);
        if (sql_step(db, update, err) != 0) { fclose(file); return 1; }
        compiled++;
    }
    fclose(file);
    if (!compiled) { fprintf(err, "fabric: YaFF catalog contains no operators\n"); return 1; }
    fprintf(out, "catalog_generation=bootstrap-1 operators=%d declaration=%s\n", compiled, path);
    return 0;
}

static int cmd_catalog(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    if (argc < 2) { fprintf(err, "usage: catalog seed|list|add ...\n"); return 2; }
    if (!strcmp(argv[1], "seed") || !strcmp(argv[1], "compile") || !strcmp(argv[1], "import-legacy")) {
        fprintf(err, "fabric: catalog declarations are authoritative; use 'bonfyre fabric compile'\n");
        return 2;
    }
    if (!strcmp(argv[1], "seed")) {
        int rc = add_operator(db,"core.identity","Identity artifact","in-process","pure-read","workload_proven",err);
        if (!rc) rc = add_operator(db,"core.intake","Intake package normalization","in-process","local-derived-state","defined",err);
        if (!rc) rc = add_operator(db,"core.publish","Governed publication","process","reversible-external-write","defined",err);
        if (!rc) fprintf(out,"catalog_generation=bootstrap-1 seeded=3\n");
        return rc;
    }
    if (!strcmp(argv[1], "add")) {
        if (argc < 6 || !valid_id(argv[2])) { fprintf(err,"usage: catalog add <id> <name> <lane> <effect-class>\n"); return 2; }
        if (add_operator(db,argv[2],argv[3],argv[4],argv[5],"defined",err)) return 1;
        fprintf(out,"operator=%s maturity=defined\n",argv[2]); return 0;
    }
    if (!strcmp(argv[1], "import-legacy")) {
        if (argc < 3) { fprintf(err, "usage: catalog import-legacy <declaration.tsv>\n"); return 2; }
        return import_legacy_catalog(db, argv[2], out, err);
    }
    if (!strcmp(argv[1], "compile")) {
        if (argc < 3) { fprintf(err, "usage: catalog compile <catalog.yaff>\n"); return 2; }
        return compile_yaff_catalog(db, argv[2], out, err);
    }
    if (!strcmp(argv[1], "list")) {
        sqlite3_stmt *st=NULL; sqlite3_prepare_v2(db,"SELECT id,name,execution_lane,effect_class,maturity,health_state FROM catalog ORDER BY id",-1,&st,NULL);
        fprintf(out,"ID\tNAME\tLANE\tEFFECT\tMATURITY\tHEALTH\n");
        while(sqlite3_step(st)==SQLITE_ROW) fprintf(out,"%s\t%s\t%s\t%s\t%s\t%s\n",sqlite3_column_text(st,0),sqlite3_column_text(st,1),sqlite3_column_text(st,2),sqlite3_column_text(st,3),sqlite3_column_text(st,4),sqlite3_column_text(st,5));
        sqlite3_finalize(st); return 0;
    }
    if (!strcmp(argv[1], "stamp-generation")) {
        char state[PATH_MAX], fabric_db_path[PATH_MAX], discovery_db_path[PATH_MAX] = "";
        if (bf_fabric_bootstrap(state, sizeof(state), fabric_db_path, sizeof(fabric_db_path), err) != 0) return 1;
        if (argc > 2) snprintf(discovery_db_path, sizeof(discovery_db_path), "%s", argv[2]);
        if (bf_catalog_stamp_generation(argc > 2 ? discovery_db_path : NULL, fabric_db_path) != 0) {
            fprintf(err, "fabric: catalog stamp-generation failed (has 'fabric compile' been run?)\n");
            return 1;
        }
        {
            char generation[128] = "";
            sqlite3_stmt *st = NULL;
            sqlite3_prepare_v2(db, "SELECT value FROM fabric_meta WHERE key='catalog_generation'", -1, &st, NULL);
            if (sqlite3_step(st) == SQLITE_ROW) {
                const char *value = (const char *)sqlite3_column_text(st, 0);
                if (value) snprintf(generation, sizeof(generation), "%s", value);
            }
            sqlite3_finalize(st);
            fprintf(out, "discovery_catalog_stamped=%s fabric_catalog_generation=%s\n",
                    argc > 2 ? discovery_db_path : "default", generation);
        }
        return 0;
    }
    fprintf(err,"usage: catalog seed|list|add|stamp-generation ...\n"); return 2;
}


static int cmd_artifact_ingest(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    if (argc < 3) { fprintf(err,"usage: artifact ingest <file> [media-type]\n"); return 2; }
    char resolved[PATH_MAX];
    const char *source = realpath(argv[2], resolved) ? resolved : argv[2];
    struct stat sb;
    if (stat(source,&sb) != 0 || !S_ISREG(sb.st_mode)) { fprintf(err,"fabric: readable regular file required: %s\n",source); return 2; }
    char digest[65], uri[256], timestamp[32];
    if (bf_sha256_file(source,digest) != 0) { fprintf(err,"fabric: cannot hash %s\n",source); return 1; }
    snprintf(uri,sizeof(uri),"bonfyre://artifact/%s",digest); now_utc(timestamp);
    sqlite3_stmt *ns=NULL;
    sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO namespace_objects(uri,kind,owner,source_authority,native_id,version,locator,policy,sensitivity,freshness,evidence_state,operations,content_contract,query_contract,effect_contract,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",-1,&ns,NULL);
    sqlite3_bind_text(ns,1,uri,-1,SQLITE_TRANSIENT); sqlite3_bind_text(ns,2,"artifact",-1,SQLITE_STATIC); sqlite3_bind_text(ns,3,"local-user",-1,SQLITE_STATIC); sqlite3_bind_text(ns,4,"local-root",-1,SQLITE_STATIC); sqlite3_bind_text(ns,5,digest,-1,SQLITE_TRANSIENT); sqlite3_bind_text(ns,6,"sha256",-1,SQLITE_STATIC); sqlite3_bind_text(ns,7,source,-1,SQLITE_TRANSIENT); sqlite3_bind_text(ns,8,"default",-1,SQLITE_STATIC); sqlite3_bind_text(ns,9,"standard",-1,SQLITE_STATIC); sqlite3_bind_text(ns,10,"observed",-1,SQLITE_STATIC); sqlite3_bind_text(ns,11,"hashed",-1,SQLITE_STATIC); sqlite3_bind_text(ns,12,"read,derive",-1,SQLITE_STATIC); sqlite3_bind_text(ns,13,"artifact.v1",-1,SQLITE_STATIC); sqlite3_bind_text(ns,14,"artifact.lookup.v1",-1,SQLITE_STATIC); sqlite3_bind_text(ns,15,"none",-1,SQLITE_STATIC); sqlite3_bind_text(ns,16,timestamp,-1,SQLITE_TRANSIENT);
    if(sql_step(db,ns,err)) return 1;
    sqlite3_stmt *art=NULL; sqlite3_prepare_v2(db,"INSERT OR IGNORE INTO artifacts(digest,uri,media_type,source_uri,locator,bytes,representation,created_at) VALUES(?,?,?,?,?,?,?,?)",-1,&art,NULL);
    sqlite3_bind_text(art,1,digest,-1,SQLITE_TRANSIENT); sqlite3_bind_text(art,2,uri,-1,SQLITE_TRANSIENT); sqlite3_bind_text(art,3,argc>3?argv[3]:"application/octet-stream",-1,SQLITE_TRANSIENT); sqlite3_bind_null(art,4); sqlite3_bind_text(art,5,source,-1,SQLITE_TRANSIENT); sqlite3_bind_int64(art,6,(sqlite3_int64)sb.st_size); sqlite3_bind_text(art,7,"zero-copy-reference",-1,SQLITE_STATIC); sqlite3_bind_text(art,8,timestamp,-1,SQLITE_TRANSIENT);
    if(sql_step(db,art,err)) return 1;
    fprintf(out,"uri=%s\ndigest=%s\nbytes=%lld\n",uri,digest,(long long)sb.st_size); return 0;
}

static int cmd_namespace_show(sqlite3 *db, const char *uri, FILE *out, FILE *err) {
    if (!valid_uri(uri)) { fprintf(err,"fabric: invalid Bonfyre URI\n"); return 2; }
    sqlite3_stmt *st=NULL; sqlite3_prepare_v2(db,"SELECT kind,owner,source_authority,native_id,version,locator,policy,sensitivity,freshness,evidence_state,operations,content_contract,query_contract,effect_contract FROM namespace_objects WHERE uri=?",-1,&st,NULL); sqlite3_bind_text(st,1,uri,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)!=SQLITE_ROW){fprintf(err,"fabric: namespace object not found: %s\n",uri);sqlite3_finalize(st);return 1;}
    fprintf(out,"uri=%s\nkind=%s\nowner=%s\nsource_authority=%s\nnative_id=%s\nversion=%s\nlocator=%s\npolicy=%s\nsensitivity=%s\nfreshness=%s\nevidence=%s\noperations=%s\ncontent_contract=%s\nquery_contract=%s\neffect_contract=%s\n",uri,sqlite3_column_text(st,0),sqlite3_column_text(st,1),sqlite3_column_text(st,2),sqlite3_column_text(st,3),sqlite3_column_text(st,4),sqlite3_column_text(st,5),sqlite3_column_text(st,6),sqlite3_column_text(st,7),sqlite3_column_text(st,8),sqlite3_column_text(st,9),sqlite3_column_text(st,10),sqlite3_column_text(st,11),sqlite3_column_text(st,12),sqlite3_column_text(st,13)); sqlite3_finalize(st); return 0;
}

static int emit_event(sqlite3 *db,const char *mission,const char *node,const char *operator_id,const char *status,const char *input_uri,const char *output_uri,const char *receipt,FILE *err);

static int cmd_mission(sqlite3 *db, int argc, char **argv, FILE *out, FILE *err) {
    if(argc<3 || !valid_id(argv[2])) {fprintf(err,"usage: mission create|show <id>\n");return 2;}
    if(!strcmp(argv[1],"create")) {
        char timestamp[32],uri[256],locator[256];now_utc(timestamp); sqlite3_stmt *st=NULL; sqlite3_prepare_v2(db,"INSERT INTO missions(id,status,context_generation,catalog_generation,provider_generation,input_snapshot,artifact_root,workgraph_cursor,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",-1,&st,NULL); sqlite3_bind_text(st,1,argv[2],-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,2,"defined",-1,SQLITE_STATIC); sqlite3_bind_text(st,3,"context-1",-1,SQLITE_STATIC); sqlite3_bind_text(st,4,"bootstrap-1",-1,SQLITE_STATIC); sqlite3_bind_text(st,5,"providers-1",-1,SQLITE_STATIC); sqlite3_bind_text(st,6,"{}",-1,SQLITE_STATIC); sqlite3_bind_text(st,7,"",-1,SQLITE_STATIC); sqlite3_bind_text(st,8,"",-1,SQLITE_STATIC); sqlite3_bind_text(st,9,timestamp,-1,SQLITE_TRANSIENT); sqlite3_bind_text(st,10,timestamp,-1,SQLITE_TRANSIENT); if(sql_step(db,st,err)) return 1; snprintf(uri,sizeof(uri),"bonfyre://mission/%s",argv[2]);snprintf(locator,sizeof(locator),"mission:%s",argv[2]);if(register_namespace(db,uri,"mission",argv[2],locator,"declared","read,run,review","mission.v1","governed",err))return 1; if(emit_event(db,argv[2],argv[2],"mission","defined",NULL,NULL,NULL,err))return 1; fprintf(out,"mission=%s status=defined\n",uri); return 0;
    }
    if(!strcmp(argv[1],"show")) {sqlite3_stmt *st=NULL;sqlite3_prepare_v2(db,"SELECT status,context_generation,catalog_generation,provider_generation,input_snapshot,artifact_root,workgraph_cursor,created_at,updated_at FROM missions WHERE id=?",-1,&st,NULL);sqlite3_bind_text(st,1,argv[2],-1,SQLITE_TRANSIENT);if(sqlite3_step(st)!=SQLITE_ROW){fprintf(err,"fabric: mission not found: %s\n",argv[2]);sqlite3_finalize(st);return 1;}fprintf(out,"mission=bonfyre://mission/%s\nstatus=%s\ncontext_generation=%s\ncatalog_generation=%s\nprovider_generation=%s\ninput_snapshot=%s\nartifact_root=%s\nworkgraph_cursor=%s\ncreated_at=%s\nupdated_at=%s\n",argv[2],sqlite3_column_text(st,0),sqlite3_column_text(st,1),sqlite3_column_text(st,2),sqlite3_column_text(st,3),sqlite3_column_text(st,4),sqlite3_column_text(st,5),sqlite3_column_text(st,6),sqlite3_column_text(st,7),sqlite3_column_text(st,8));sqlite3_finalize(st);return 0;}
    fprintf(err,"usage: mission create|show <id>\n");return 2;
}

static int mission_exists(sqlite3 *db,const char *id){sqlite3_stmt *st=NULL;sqlite3_prepare_v2(db,"SELECT 1 FROM missions WHERE id=?",-1,&st,NULL);sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT);int yes=sqlite3_step(st)==SQLITE_ROW;sqlite3_finalize(st);return yes;}
static int operator_exists(sqlite3 *db,const char *id){sqlite3_stmt *st=NULL;sqlite3_prepare_v2(db,"SELECT 1 FROM catalog WHERE id=?",-1,&st,NULL);sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT);int yes=sqlite3_step(st)==SQLITE_ROW;sqlite3_finalize(st);return yes;}

static int cmd_work_add(sqlite3 *db,int argc,char **argv,FILE *out,FILE *err){
    if(argc<5||!valid_id(argv[2])||!valid_id(argv[3])||!valid_id(argv[4])){fprintf(err,"usage: work add <mission> <node> <operator> [input-uri]\n");return 2;}
    if(!mission_exists(db,argv[2])||!operator_exists(db,argv[4])){fprintf(err,"fabric: mission or catalog operator does not exist\n");return 1;}
    sqlite3_stmt *st=NULL;sqlite3_prepare_v2(db,"INSERT INTO workgraph_nodes(mission_id,node_id,operator_id,input_uri,status,attempt,retry_limit,timeout_seconds,depends_on,created_at_ms,updated_at_ms) VALUES(?,?,?,?,'defined',0,1,30,'[]',?,?)",-1,&st,NULL);sqlite3_bind_text(st,1,argv[2],-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,argv[3],-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,3,argv[4],-1,SQLITE_TRANSIENT);if(argc>5)sqlite3_bind_text(st,4,argv[5],-1,SQLITE_TRANSIENT);else sqlite3_bind_null(st,4);sqlite3_bind_int64(st,5,(sqlite3_int64)time(NULL)*1000);sqlite3_bind_int64(st,6,(sqlite3_int64)time(NULL)*1000);if(sql_step(db,st,err))return 1;fprintf(out,"node=bonfyre://mission/%s/%s status=defined\n",argv[2],argv[3]);return 0;
}

static int emit_event(sqlite3 *db,const char *mission,const char *node,const char *operator_id,const char *status,const char *input_uri,const char *output_uri,const char *receipt,FILE *err){char eid[64],timestamp[32];make_id(eid,sizeof(eid),"evt",node);now_utc(timestamp);sqlite3_stmt *st=NULL;sqlite3_prepare_v2(db,"INSERT INTO events(id,mission_id,task_id,attempt,actor,operator_id,provider_id,model_id,start_at,end_at,duration_ms,input_uri,output_uri,effect_class,status,error_code,receipt_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",-1,&st,NULL);sqlite3_bind_text(st,1,eid,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,mission,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,3,node,-1,SQLITE_TRANSIENT);sqlite3_bind_int(st,4,1);sqlite3_bind_text(st,5,"bonfyre",-1,SQLITE_STATIC);sqlite3_bind_text(st,6,operator_id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,7,"native",-1,SQLITE_STATIC);sqlite3_bind_null(st,8);sqlite3_bind_text(st,9,timestamp,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,10,timestamp,-1,SQLITE_TRANSIENT);sqlite3_bind_int(st,11,0);if(input_uri)sqlite3_bind_text(st,12,input_uri,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(st,12);if(output_uri)sqlite3_bind_text(st,13,output_uri,-1,SQLITE_TRANSIENT);else sqlite3_bind_null(st,13);sqlite3_bind_text(st,14,"pure-read",-1,SQLITE_STATIC);sqlite3_bind_text(st,15,status,-1,SQLITE_TRANSIENT);sqlite3_bind_null(st,16);sqlite3_bind_text(st,17,receipt,-1,SQLITE_TRANSIENT);return sql_step(db,st,err);}

static int cmd_work_run(sqlite3 *db,const char *mission,FILE *out,FILE *err){
    if(!mission_exists(db,mission)){fprintf(err,"fabric: mission not found: %s\n",mission);return 1;}
    sqlite3_stmt *read=NULL;sqlite3_prepare_v2(db,"SELECT node_id,operator_id,input_uri FROM workgraph_nodes WHERE mission_id=? AND status='defined' ORDER BY node_id",-1,&read,NULL);sqlite3_bind_text(read,1,mission,-1,SQLITE_TRANSIENT);int ran=0,failed=0;
    while(sqlite3_step(read)==SQLITE_ROW){const char *node=(const char*)sqlite3_column_text(read,0);const char *op=(const char*)sqlite3_column_text(read,1);const char *input=(const char*)sqlite3_column_text(read,2);if(strcmp(op,"core.identity")!=0){sqlite3_stmt *blocked=NULL;sqlite3_prepare_v2(db,"UPDATE workgraph_nodes SET status='blocked' WHERE mission_id=? AND node_id=?",-1,&blocked,NULL);sqlite3_bind_text(blocked,1,mission,-1,SQLITE_TRANSIENT);sqlite3_bind_text(blocked,2,node,-1,SQLITE_TRANSIENT);sql_step(db,blocked,err);fprintf(err,"fabric: operator %s has no workload-proven in-process handler; node %s is blocked\n",op,node);failed=1;break;}char payload[1024],receipt[64];snprintf(payload,sizeof(payload),"{\"mission\":\"%s\",\"node\":\"%s\",\"operator\":\"%s\",\"input\":\"%s\",\"outcome\":\"identity artifact\"}",mission,node,op,input?input:"");if(record_receipt(db,"workgraph-node",node,payload,receipt,err)!=0){failed=1;break;}sqlite3_stmt *upd=NULL;sqlite3_prepare_v2(db,"UPDATE workgraph_nodes SET status='complete',attempt=attempt+1,receipt_id=? WHERE mission_id=? AND node_id=?",-1,&upd,NULL);sqlite3_bind_text(upd,1,receipt,-1,SQLITE_TRANSIENT);sqlite3_bind_text(upd,2,mission,-1,SQLITE_TRANSIENT);sqlite3_bind_text(upd,3,node,-1,SQLITE_TRANSIENT);if(sql_step(db,upd,err)||emit_event(db,mission,node,op,"complete",input,NULL,receipt,err)){failed=1;break;}fprintf(out,"node=%s status=complete receipt=bonfyre://receipt/%s\n",node,receipt);ran++;}
    sqlite3_finalize(read);char timestamp[32];now_utc(timestamp);sqlite3_stmt *m=NULL;sqlite3_prepare_v2(db,"UPDATE missions SET status=?,workgraph_cursor=?,updated_at=? WHERE id=?",-1,&m,NULL);sqlite3_bind_text(m,1,failed?"partial":"complete",-1,SQLITE_STATIC);sqlite3_bind_text(m,2,failed?"failure":"terminal",-1,SQLITE_STATIC);sqlite3_bind_text(m,3,timestamp,-1,SQLITE_TRANSIENT);sqlite3_bind_text(m,4,mission,-1,SQLITE_TRANSIENT);sql_step(db,m,err);if(!ran&&!failed)fprintf(out,"mission=%s no runnable nodes\n",mission);return failed?1:0;
}

static int cmd_effect_request(sqlite3 *db,int argc,char **argv,FILE *out,FILE *err){if(argc<5||!valid_id(argv[2])||!valid_uri(argv[4])){fprintf(err,"usage: effect request <mission> <kind> <target-uri> [tier]\n");return 2;}if(!mission_exists(db,argv[2])){fprintf(err,"fabric: mission not found: %s\n",argv[2]);return 1;}char id[64],timestamp[32],simulation[1536],uri[256],locator[256];make_id(id,sizeof(id),"effect",argv[3]);now_utc(timestamp);snprintf(simulation,sizeof(simulation),"Plan only: %s on %s. No mutation has occurred; commit requires a separately recorded approval and an execution adapter.",argv[3],argv[4]);sqlite3_stmt *st=NULL;sqlite3_prepare_v2(db,"INSERT INTO effects(id,mission_id,effect_kind,target_uri,tier,intent,simulation,state,authority_class,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",-1,&st,NULL);sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,2,argv[2],-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,3,argv[3],-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,4,argv[4],-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,5,argc>5?argv[5]:"reversible-local-write",-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,6,"typed-intent",-1,SQLITE_STATIC);sqlite3_bind_text(st,7,simulation,-1,SQLITE_TRANSIENT);sqlite3_bind_text(st,8,"planned",-1,SQLITE_STATIC);sqlite3_bind_text(st,9,"mission-scoped",-1,SQLITE_STATIC);sqlite3_bind_text(st,10,timestamp,-1,SQLITE_TRANSIENT);if(sql_step(db,st,err))return 1;snprintf(uri,sizeof(uri),"bonfyre://effect/%s",id);snprintf(locator,sizeof(locator),"effect:%s",id);if(register_namespace(db,uri,"effect",id,locator,"planned","read,plan,approve","effect.v1","governed",err))return 1;if(emit_event(db,argv[2],id,argv[3],"planned",argv[4],NULL,NULL,err))return 1;fprintf(out,"effect=%s\nstate=planned\nsimulation=%s\n",uri,simulation);return 0;}

static int cmd_effect_state(sqlite3 *db,const char *verb,const char *id,FILE *out,FILE *err){if(!valid_id(id)){fprintf(err,"fabric: invalid effect id\n");return 2;}sqlite3_stmt *st=NULL;sqlite3_prepare_v2(db,"SELECT mission_id,effect_kind,target_uri,tier,intent,simulation,state,authority_class,receipt_id FROM effects WHERE id=?",-1,&st,NULL);sqlite3_bind_text(st,1,id,-1,SQLITE_TRANSIENT);if(sqlite3_step(st)!=SQLITE_ROW){fprintf(err,"fabric: effect not found: %s\n",id);sqlite3_finalize(st);return 1;}char state[32];snprintf(state,sizeof(state),"%s",sqlite3_column_text(st,6));char approve_mission[64],approve_kind[64],approve_target[256];snprintf(approve_mission,sizeof(approve_mission),"%s",sqlite3_column_text(st,0));snprintf(approve_kind,sizeof(approve_kind),"%s",sqlite3_column_text(st,1));snprintf(approve_target,sizeof(approve_target),"%s",sqlite3_column_text(st,2));if(!strcmp(verb,"plan")){fprintf(out,"effect=bonfyre://effect/%s\nmission=%s\nkind=%s\ntarget=%s\ntier=%s\nintent=%s\nstate=%s\nauthority=%s\nsimulation=%s\n",id,sqlite3_column_text(st,0),sqlite3_column_text(st,1),sqlite3_column_text(st,2),sqlite3_column_text(st,3),sqlite3_column_text(st,4),state,sqlite3_column_text(st,7),sqlite3_column_text(st,5));sqlite3_finalize(st);return 0;}sqlite3_finalize(st);if(!strcmp(verb,"approve")){if(strcmp(state,"planned")){fprintf(err,"fabric: only planned effects can be approved\n");return 1;}char timestamp[32];now_utc(timestamp);sqlite3_stmt *u=NULL;sqlite3_prepare_v2(db,"UPDATE effects SET state='approved',approved_at=? WHERE id=?",-1,&u,NULL);sqlite3_bind_text(u,1,timestamp,-1,SQLITE_TRANSIENT);sqlite3_bind_text(u,2,id,-1,SQLITE_TRANSIENT);if(sql_step(db,u,err))return 1;if(emit_event(db,approve_mission,id,approve_kind,"approved",approve_target,NULL,NULL,err))return 1;fprintf(out,"effect=bonfyre://effect/%s state=approved\n",id);return 0;}if(!strcmp(verb,"commit")){fprintf(err,"fabric: commit refused: no bound external execution adapter; effect remains non-mutating\n");return 1;}fprintf(err,"fabric: unknown effect verb\n");return 2;}

static int cmd_receipt_show(sqlite3 *db, const char *id, FILE *out, FILE *err) {
    sqlite3_stmt *statement = NULL;

    if (!valid_id(id)) {
        fprintf(err, "fabric: invalid receipt id\n");
        return 2;
    }
    if (sqlite3_prepare_v2(db,
            "SELECT subject_kind,subject_id,content_hash,payload,created_at,"
            "COALESCE(previous_receipt_id,''),COALESCE(chain_hash,'') "
            "FROM receipts WHERE id=?",
            -1, &statement, NULL) != SQLITE_OK) {
        fprintf(err, "fabric: receipt query failed\n");
        return 1;
    }
    sqlite3_bind_text(statement, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(statement) != SQLITE_ROW) {
        fprintf(err, "fabric: receipt not found: %s\n", id);
        sqlite3_finalize(statement);
        return 1;
    }
    fprintf(out,
            "receipt=bonfyre://receipt/%s\nsubject_kind=%s\nsubject_id=%s\n"
            "content_hash=%s\npayload=%s\ncreated_at=%s\nprevious_receipt_id=%s\nchain_hash=%s\n",
            id, sqlite3_column_text(statement, 0), sqlite3_column_text(statement, 1),
            sqlite3_column_text(statement, 2), sqlite3_column_text(statement, 3),
            sqlite3_column_text(statement, 4), sqlite3_column_text(statement, 5),
            sqlite3_column_text(statement, 6));
    sqlite3_finalize(statement);
    return 0;
}

int bf_fabric_dispatch(int argc,char **argv,FILE *out,FILE *err){
    if(argc<1){print_usage(out);return 2;}sqlite3 *db=NULL;if(open_db(&db,err)!=0)return 1;int rc=2;
    int extended = bf_fabric_extended_dispatch(db, argc, argv, out, err);
    if (extended != BF_FABRIC_NOT_HANDLED) { sqlite3_close(db); return extended; }
    if(!strcmp(argv[0],"help")||!strcmp(argv[0],"--help")){print_usage(out);rc=0;}
    else if(!strcmp(argv[0],"fabric")){if(argc<2||!strcmp(argv[1],"init")){char state[PATH_MAX],path[PATH_MAX];bf_fabric_bootstrap(state,sizeof(state),path,sizeof(path),err);fprintf(out,"fabric=%s\nstate_dir=%s\ndatabase=%s\n",BF_FABRIC_VERSION,state,path);rc=0;}else if(!strcmp(argv[1],"status"))rc=cmd_status(db,out,err);else{print_usage(err);rc=2;}}
    else if(!strcmp(argv[0],"root")){if(argc>1&&!strcmp(argv[1],"add"))rc=cmd_root_add(db,argc,argv,out,err);else if(argc>1&&!strcmp(argv[1],"list"))rc=cmd_root_list(db,out);else{fprintf(err,"usage: root add|list\n");rc=2;}}
    else if(!strcmp(argv[0],"catalog"))rc=cmd_catalog(db,argc,argv,out,err);
    else if(!strcmp(argv[0],"artifact")&&argc>1&&!strcmp(argv[1],"ingest"))rc=cmd_artifact_ingest(db,argc,argv,out,err);
    else if(!strcmp(argv[0],"namespace")&&argc>2&&!strcmp(argv[1],"show"))rc=cmd_namespace_show(db,argv[2],out,err);
    else if(!strcmp(argv[0],"mission"))rc=cmd_mission(db,argc,argv,out,err);
    else if(!strcmp(argv[0],"work")){if(argc>1&&!strcmp(argv[1],"add"))rc=cmd_work_add(db,argc,argv,out,err);else if(argc>2&&!strcmp(argv[1],"run"))rc=cmd_work_run(db,argv[2],out,err);else{fprintf(err,"usage: work add|run\n");rc=2;}}
    else if(!strcmp(argv[0],"effect")){if(argc>1&&!strcmp(argv[1],"request"))rc=cmd_effect_request(db,argc,argv,out,err);else if(argc>2&&(!strcmp(argv[1],"plan")||!strcmp(argv[1],"approve")||!strcmp(argv[1],"commit")))rc=cmd_effect_state(db,argv[1],argv[2],out,err);else{fprintf(err,"usage: effect request|plan|approve|commit\n");rc=2;}}
    else if(!strcmp(argv[0],"receipt")&&argc>2&&!strcmp(argv[1],"show"))rc=cmd_receipt_show(db,argv[2],out,err);
    else {print_usage(err);rc=2;}
    sqlite3_close(db);return rc;
}
