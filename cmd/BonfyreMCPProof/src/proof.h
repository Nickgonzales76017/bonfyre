#ifndef BONFYRE_MCP_PROOF_H
#define BONFYRE_MCP_PROOF_H

#include <limits.h>
#include <sqlite3.h>
#include <stddef.h>

#define BF_MCP_PROOF_VERSION "0.2.0"

typedef struct {
    int receipt_schema;
    int receipt_status;
    int manifest;
    int cms_entry;
    int cms_hash;
    int cms_declared;
    int cms_schema;
    int graph_atom;
    int graph_hash;
    int graph_operator;
    int flow_record;
    int flow_definition;
    int discipl_contract;
} BfProofChecks;

typedef struct {
    char receipt_path[PATH_MAX];
    char receipt_id[128];
    char expected_hash[80];
    char model[512];
    char control_home[PATH_MAX];
    char repo_root[PATH_MAX];
    char cms_db[PATH_MAX];
    char cms_schemas[PATH_MAX];
    char cms_namespace[128];
    char cms_external_id[1024];
    long long cms_entry_id;
    char graph_db[PATH_MAX];
    char graph_operator[256];
    char graph_output_atom[256];
    char flow_db[PATH_MAX];
    char flow_definition[PATH_MAX];
    char flow_id[256];
    char actor_db[PATH_MAX];
    char actor_root[PATH_MAX];
} BfProofContext;

char *bf_proof_read_file(const char *path);
void bf_dirname_copy(const char *path, char *out, size_t cap);
void bf_init_repo_root(const char *argv0, char *out, size_t cap);
const char *bf_owner_bin(const char *env_name, const char *name,
                         char *buf, size_t cap);
int bf_run_capture(char *const argv[], const char *env_name,
                   const char *env_value, char *out, size_t out_cap,
                   char *err, size_t err_cap);
void bf_json_string(const char *text);

sqlite3 *bf_json_db(void);
int bf_jtext(sqlite3 *db, const char *json, const char *path,
             char *out, size_t cap);
int bf_jint(sqlite3 *db, const char *json, const char *path, long long *out);
int bf_jtype(sqlite3 *db, const char *json, const char *path,
             char *out, size_t cap);
char *bf_jraw(sqlite3 *db, const char *json, const char *path);
char *bf_array_object_by(sqlite3 *db, const char *json, const char *array_path,
                         const char *field_path, const char *value);
int bf_array_has_two(sqlite3 *db, const char *json, const char *array_path,
                     const char *p1, const char *v1,
                     const char *p2, const char *v2);
int bf_json_tree_equal(sqlite3 *db, const char *a, const char *b);

int bf_load_context(sqlite3 *db, const char *receipt, const char *receipt_path,
                    const char *repo_root, BfProofContext *ctx,
                    BfProofChecks *checks);
int bf_verify_cms(sqlite3 *db, const char *receipt, BfProofContext *ctx,
                  BfProofChecks *checks);
int bf_verify_graph(sqlite3 *db, BfProofContext *ctx, BfProofChecks *checks);
int bf_verify_flow(sqlite3 *db, BfProofContext *ctx, BfProofChecks *checks);
int bf_verify_discipl(sqlite3 *db, BfProofContext *ctx, BfProofChecks *checks);
void bf_emit_report(const BfProofContext *ctx, const BfProofChecks *checks);

#endif
