/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif


#include <limits.h>
#include <errno.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

#include <bonfyre.h>
#include <bf_json.h>
#include <bf_command_lookup.h>
#include "bf_lex.h"

typedef struct {
    char family[128];
    char artifact_id[256];
} ChainNode;

typedef struct {
    char src[128];
    char dst[128];
    char relationship[128];
    char required_bridge[128];
    double success_prior;
    double transform_cost;
    double semantic_loss;
} ContractRow;

typedef struct {
    sqlite3 *db;
    sqlite3_stmt *st;
} DisciplActorWriter;

static const char *CONTRACT_IMPORT_PAIRS[][2] = {
    {"T_MOE_ROUTER","T_MOE_EXPERT"},
    {"T_PROJECTOR_BRIDGE","T_VISION_PATCH"},
    {"T_PROJECTOR_BRIDGE","T_SHARED_QK"},
    {"T_EMBED_POOL","T_RETRIEVAL_HEAD"},
    {"T_POLICY_ROUTE","T_SAFETY_HEAD"},
    {"T_AUDIO_MODEL","T_MODAL_FUSION"},
    {"T_DIFFUSION_UNET","T_TEXT_ENCODER"},
    {"T_KV_CACHE","T_SHARED_QK"},
    {"T_LATENCY_ROUTE","T_MOE_ROUTER"},
    {"T_AUDIO_MODEL","T_AUDIO_GENERATOR"},
    {"T_AUDIO_GENERATOR","T_SAMPLE_OUTPUT"},
    {"T_SAMPLE_OUTPUT","T_LATENT_SPACE"},
    {"T_LATENT_SPACE","T_DIFFUSION_UNET"},
    {"T_DIFFUSION_UNET","T_VIDEO_OUTPUT"},
    {"T_VISION_GROUNDING","T_OBJECT_DETECTOR"},
    {"T_GRAPH_STRUCTURE","T_PLANNER"},
    {"T_PLANNER","T_EXECUTION"},
    {"T_QUEUE_JOB","T_EXECUTION"},
    {"T_EXECUTION","T_LEDGER_EVENT"},
    {"T_METER_EVENT","T_LEDGER_EVENT"},
    {"T_LEDGER_EVENT","T_VALUE_CAPTURE"},
    {"T_GRAPH_STRUCTURE","T_VERIFY"},
    {"T_ALERT","T_RESPONSE"},
    {"T_WORKFLOW_STAGE","T_QUEUE_JOB"},
    {"T_PIPELINE_STAGE","T_ARTIFACT_OUTPUT"},
    {"T_CAPABILITY","T_ACTOR_SELECTION"},
    {"T_TELEMETRY_EVENT","T_DIAGNOSTIC"},
    {"T_S2_CELL","T_RASTER_FEATURE"},
    {"T_BUILT_ENVIRONMENT","T_RASTER_FEATURE"},
    {"T_RASTER_FEATURE","T_MASKED_AUTOENCODER"},
    {"T_MASKED_AUTOENCODER","T_GEOSPATIAL_EMBED"},
    {"T_GEOSPATIAL_EMBED","T_SOCIOECONOMIC_HEAD"},
    {"T_GEOSPATIAL_EMBED","T_ENVIRONMENTAL_HEAD"},
    {"T_GEOSPATIAL_EMBED","T_MULTIMODAL_FUSION"},
    {NULL,NULL}
};

static const char *arg_value(int argc, char **argv, const char *flag) {
    for (int i = 1; i < argc - 1; i++) if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    return NULL;
}

static int ensure_dir_recursive(const char *path) {
    char work[PATH_MAX];
    size_t length;
    char *cursor;
    if (!path || !path[0] || (length = strlen(path)) >= sizeof(work)) return 1;
    memcpy(work, path, length + 1);
    for (cursor = work + 1; *cursor; ++cursor) {
        if (*cursor != '/') continue;
        *cursor = '\0';
        if (mkdir(work, 0700) != 0 && errno != EEXIST) return 1;
        *cursor = '/';
    }
    return mkdir(work, 0700) != 0 && errno != EEXIST;
}

static int json_copy_str_local(const bf_json_doc_t *doc, const bf_json_node_t *obj,
                               const char *key, char *out, size_t out_sz) {
    const bf_json_node_t *n = bf_json_obj_get(doc, obj, key);
    if (!n) {
        if (out_sz) out[0] = '\0';
        return 0;
    }
    return bf_json_get_str_copy(n, out, out_sz) > 0;
}

static void now_iso_local(char out[32]) {
    time_t now = time(NULL);
    struct tm tmv;
    if (!out) return;
    memset(&tmv, 0, sizeof(tmv));
#if defined(_WIN32)
    gmtime_s(&tmv, &now);
#else
    gmtime_r(&now, &tmv);
#endif
    strftime(out, 32, "%Y-%m-%dT%H:%M:%SZ", &tmv);
}

static void actor_type_name_local(bf_discipl_actor_type_t type, char *out, size_t out_sz) {
    const char *s;
    switch (type) {
        case BF_DISCIPL_ACTOR_LAYER: s = "layer"; break;
        case BF_DISCIPL_ACTOR_COMMAND: s = "command"; break;
        case BF_DISCIPL_ACTOR_PIPELINE_STAGE: s = "pipeline_stage"; break;
        case BF_DISCIPL_ACTOR_BRIDGE: s = "bridge"; break;
        case BF_DISCIPL_ACTOR_SERVICE: s = "service"; break;
        case BF_DISCIPL_ACTOR_VALUE_ACTOR: s = "value_actor"; break;
        case BF_DISCIPL_ACTOR_GRAPH_ACTOR: s = "graph_actor"; break;
        case BF_DISCIPL_ACTOR_QUEUE_ACTOR: s = "queue_actor"; break;
        default: break;
    }
    if (out && out_sz) snprintf(out, out_sz, "%s", s);
}

static int actor_writer_begin(const char *root, DisciplActorWriter *writer) {
    char db_path[PATH_MAX];
    if (!writer) return 1;
    memset(writer, 0, sizeof(*writer));
    if (bf_discipl_init_db(root) != 0) return 1;
    if (bf_layer_state_db_path(root, "discipl.db", db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_open(db_path, &writer->db) != SQLITE_OK) return 1;
    sqlite3_exec(writer->db, "BEGIN IMMEDIATE TRANSACTION", NULL, NULL, NULL);
    if (sqlite3_prepare_v2(writer->db,
        "INSERT OR REPLACE INTO discipl_actors(actor_id,actor_type,family,domain,modality,capabilities_json,role_affordances_json,confidence,uncertainty,cost,latency,source_ref,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        -1, &writer->st, NULL) != SQLITE_OK) {
        sqlite3_close(writer->db);
        memset(writer, 0, sizeof(*writer));
        return 1;
    }
    return 0;
}

static int actor_writer_upsert(DisciplActorWriter *writer, const bf_discipl_actor_t *actor) {
    char actor_type[64], caps_json[64], roles_json[256], source_ref[512], created_at[32];
    if (!writer || !writer->db || !writer->st || !actor) return 1;
    actor_type_name_local(actor->actor_type, actor_type, sizeof(actor_type));
    snprintf(caps_json, sizeof(caps_json), "[\"0x%llx\"]", (unsigned long long)actor->capabilities_bitset);
    snprintf(roles_json, sizeof(roles_json),
             "{\"can_boss\":%s,\"can_follow\":%s,\"can_verify\":%s,\"can_decompose\":%s,\"can_route\":%s,\"can_price\":%s,\"can_execute\":%s}",
             actor->affordances.can_boss ? "true" : "false",
             actor->affordances.can_follow ? "true" : "false",
             actor->affordances.can_verify ? "true" : "false",
             actor->affordances.can_decompose ? "true" : "false",
             actor->affordances.can_route ? "true" : "false",
             actor->affordances.can_price ? "true" : "false",
             actor->affordances.can_execute ? "true" : "false");
    snprintf(source_ref, sizeof(source_ref), "%s%s%s",
             actor->source_artifact_id,
             actor->source_artifact_id[0] && actor->source_command[0] ? " | " : "",
             actor->source_command);
    now_iso_local(created_at);

    sqlite3_reset(writer->st);
    sqlite3_clear_bindings(writer->st);
    sqlite3_bind_text(writer->st,1,actor->id,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(writer->st,2,actor_type,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(writer->st,3,actor->family,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(writer->st,4,actor->domain,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(writer->st,5,actor->modality,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(writer->st,6,caps_json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(writer->st,7,roles_json,-1,SQLITE_TRANSIENT);
    sqlite3_bind_double(writer->st,8,actor->confidence);
    sqlite3_bind_double(writer->st,9,actor->uncertainty);
    sqlite3_bind_double(writer->st,10,actor->cost);
    sqlite3_bind_double(writer->st,11,actor->latency);
    sqlite3_bind_text(writer->st,12,source_ref,-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(writer->st,13,created_at,-1,SQLITE_TRANSIENT);
    return sqlite3_step(writer->st) == SQLITE_DONE ? 0 : 1;
}

static void actor_writer_end(DisciplActorWriter *writer, int commit) {
    if (!writer) return;
    if (writer->db) sqlite3_exec(writer->db, commit ? "COMMIT" : "ROLLBACK", NULL, NULL, NULL);
    if (writer->st) sqlite3_finalize(writer->st);
    if (writer->db) sqlite3_close(writer->db);
    memset(writer, 0, sizeof(*writer));
}

static int run_capture(const char *const argv[], char *out, size_t out_sz) {
    int pipefd[2];
    pid_t pid;
    size_t total = 0;
    ssize_t nread;
    int status = 0;
    if (pipe(pipefd) < 0) return -1;
    pid = fork();
    if (pid < 0) {
        close(pipefd[0]); close(pipefd[1]);
        return -1;
    }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        execvp(argv[0], (char *const *)argv);
        _exit(127);
    }
    close(pipefd[1]);
    while ((nread = read(pipefd[0], out + total, out_sz - total - 1)) > 0) {
        total += (size_t)nread;
        if (total >= out_sz - 1) break;
    }
    close(pipefd[0]);
    out[total] = '\0';
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static int require_discipl(const char *root) {
    if (!bf_discipl_tables_exist(root)) {
        fprintf(stderr, "bonfyre-discipl: DisCIPL tables are not initialized\n");
        fprintf(stderr, "run: bonfyre discipl init --root %s\n", root ? root : "layeros/state");
        return 0;
    }
    return 1;
}

static int import_command_actors(DisciplActorWriter *writer) {
    char repo_root[PATH_MAX], cli_path[PATH_MAX], json[65536], err[128];
    const char *argvv[4];
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root_json = NULL, *sections = NULL;
    if (!bf_catalog_find_repo_root(repo_root, sizeof(repo_root))) return 1;
    snprintf(cli_path, sizeof(cli_path), "%s/cmd/BonfyreCLI/bonfyre", repo_root);
    argvv[0] = cli_path; argvv[1] = "list"; argvv[2] = "--json"; argvv[3] = NULL;
    if (run_capture(argvv, json, sizeof(json)) != 0) return 1;
    doc = bf_json_parse_str(json, err, sizeof(err));
    if (!doc) return 1;
    root_json = bf_json_root(doc);
    sections = bf_json_obj_get(doc, root_json, "sections");
    for (const bf_json_node_t *sec = sections ? bf_json_child_first(doc, sections) : NULL;
         sec; sec = bf_json_child_next(doc, sec)) {
        char section_name[128] = "";
        const bf_json_node_t *commands = bf_json_obj_get(doc, sec, "commands");
        json_copy_str_local(doc, sec, "name", section_name, sizeof(section_name));
        for (const bf_json_node_t *cmd = commands ? bf_json_child_first(doc, commands) : NULL;
             cmd; cmd = bf_json_child_next(doc, cmd)) {
            bf_discipl_actor_t actor;
            char cmd_name[128] = "", desc[256] = "";
            memset(&actor, 0, sizeof(actor));
            json_copy_str_local(doc, cmd, "command", cmd_name, sizeof(cmd_name));
            json_copy_str_local(doc, cmd, "description", desc, sizeof(desc));
            snprintf(actor.id, sizeof(actor.id), "command:%s", cmd_name);
            actor.actor_type = BF_DISCIPL_ACTOR_COMMAND;
            snprintf(actor.family, sizeof(actor.family), "T_COMMAND");
            snprintf(actor.domain, sizeof(actor.domain), "%s", section_name);
            snprintf(actor.modality, sizeof(actor.modality), "cli");
            actor.capabilities_bitset = 1ull << 8;
            if (strstr(section_name, "Infrastructure")) actor.capabilities_bitset |= 1ull << 6;
            if (strstr(section_name, "Value")) actor.capabilities_bitset |= 1ull << 4;
            actor.affordances.can_follow = 1;
            actor.affordances.can_execute = 1;
            actor.affordances.can_verify = strstr(desc, "verify") != NULL;
            actor.affordances.can_route = strstr(desc, "orchestration") != NULL || strstr(desc, "pipeline") != NULL;
            actor.affordances.can_boss = actor.affordances.can_route || strstr(cmd_name, "orchestrate") != NULL;
            actor.affordances.can_price = strstr(section_name, "Value") != NULL;
            actor.confidence = 0.90;
            actor.uncertainty = 0.05;
            actor.cost = 0.01;
            actor.latency = 0.02;
            snprintf(actor.source_command, sizeof(actor.source_command), "%s", cmd_name);
            if (actor_writer_upsert(writer, &actor) != 0) { bf_json_free(doc); return 1; }
        }
    }
    bf_json_free(doc);
    return 0;
}

static int import_layer_actors(const char *root, DisciplActorWriter *writer) {
    char db_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    /* Layer artifacts are optional.  Command and service actors still form a
     * valid actor graph on a machine that has not materialized layer state. */
    if (bf_layer_state_db_path(root, "layers.db", db_path, sizeof(db_path)) != 0) return 0;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return 0;
    if (sqlite3_prepare_v2(db, "SELECT artifact_json FROM layer_artifacts ORDER BY artifact_id", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db); return 0;
    }
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *artifact_json = (const char *)sqlite3_column_text(st, 0);
        bf_discipl_actor_t actor;
        if (!artifact_json) continue;
        if (bf_discipl_actor_from_layer_json(artifact_json, &actor) == 0 &&
            actor_writer_upsert(writer, &actor) != 0) {
            sqlite3_finalize(st);
            sqlite3_close(db);
            return 1;
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return 0;
}

static int import_service_actors(DisciplActorWriter *writer) {
    const char *ids[][4] = {
        {"service:graph", "T_GRAPH_STRUCTURE", "infra", "graph"},
        {"service:queue", "T_QUEUE_JOB", "infra", "queue"},
        {"service:ledger", "T_LEDGER_EVENT", "value", "ledger"},
        {"service:meter", "T_METER_EVENT", "value", "meter"},
        {"service:workflow", "T_WORKFLOW_STAGE", "workflow", "workflow"},
        {"service:pipeline", "T_PIPELINE_STAGE", "pipeline", "pipeline"},
        {"service:capability", "T_CAPABILITY", "infra", "capabilities"},
        {"service:telemetry", "T_TELEMETRY_EVENT", "infra", "tel"},
        {NULL, NULL, NULL, NULL}
    };
    for (int i = 0; ids[i][0]; i++) {
        bf_discipl_actor_t actor;
        memset(&actor, 0, sizeof(actor));
        snprintf(actor.id, sizeof(actor.id), "%s", ids[i][0]);
        actor.actor_type = strstr(ids[i][0], "queue") ? BF_DISCIPL_ACTOR_QUEUE_ACTOR :
                           strstr(ids[i][0], "graph") ? BF_DISCIPL_ACTOR_GRAPH_ACTOR :
                           strstr(ids[i][0], "ledger") || strstr(ids[i][0], "meter") ? BF_DISCIPL_ACTOR_VALUE_ACTOR :
                           BF_DISCIPL_ACTOR_SERVICE;
        snprintf(actor.family, sizeof(actor.family), "%s", ids[i][1]);
        snprintf(actor.domain, sizeof(actor.domain), "%s", ids[i][2]);
        snprintf(actor.modality, sizeof(actor.modality), "service");
        snprintf(actor.source_command, sizeof(actor.source_command), "%s", ids[i][3]);
        actor.affordances.can_follow = 1;
        actor.affordances.can_execute = 1;
        actor.affordances.can_verify = 1;
        actor.affordances.can_route = strstr(ids[i][1], "QUEUE") || strstr(ids[i][1], "GRAPH");
        actor.affordances.can_boss = actor.affordances.can_route;
        actor.affordances.can_price = strstr(ids[i][1], "LEDGER") || strstr(ids[i][1], "METER");
        actor.confidence = 0.85;
        actor.uncertainty = 0.10;
        actor.cost = 0.01;
        actor.latency = 0.01;
        if (actor_writer_upsert(writer, &actor) != 0) return 1;
    }
    return 0;
}

/* Every contract, however many there are.
 *
 * The fixed-size readers this grew out of took 128 rows in one caller and 256
 * in another. The seeded command estate alone contracts more edges than that,
 * and the truncation was silent in the worst place: a hop whose contract fell
 * off the end was not reported missing, it was scored `invalid` at 0.10
 * confidence -- a real edge downgraded to a broken one by an array bound. */
static ContractRow *load_all_contracts(const char *root, int *out_count)
{
    char db_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    ContractRow *rows = NULL;
    int n = 0, cap = 0;
    *out_count = 0;
    if (bf_layer_state_db_path(root, "discipl.db", db_path, sizeof(db_path)) != 0) return NULL;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return NULL;
    if (sqlite3_prepare_v2(db, "SELECT src_family,dst_family,relationship,required_bridge,"
                               "success_prior,transform_cost,semantic_loss FROM discipl_contracts "
                               "ORDER BY src_family,dst_family", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    while (sqlite3_step(st) == SQLITE_ROW) {
        ContractRow *grown;
        if (n == cap) {
            cap = cap ? cap * 2 : 256;
            grown = realloc(rows, sizeof(*rows) * (size_t)cap);
            if (!grown) { free(rows); sqlite3_finalize(st); sqlite3_close(db); *out_count = -1; return NULL; }
            rows = grown;
        }
        snprintf(rows[n].src, sizeof(rows[n].src), "%s", sqlite3_column_text(st,0) ? (const char *)sqlite3_column_text(st,0) : "");
        snprintf(rows[n].dst, sizeof(rows[n].dst), "%s", sqlite3_column_text(st,1) ? (const char *)sqlite3_column_text(st,1) : "");
        snprintf(rows[n].relationship, sizeof(rows[n].relationship), "%s", sqlite3_column_text(st,2) ? (const char *)sqlite3_column_text(st,2) : "");
        snprintf(rows[n].required_bridge, sizeof(rows[n].required_bridge), "%s", sqlite3_column_text(st,3) ? (const char *)sqlite3_column_text(st,3) : "");
        rows[n].success_prior = sqlite3_column_double(st,4);
        rows[n].transform_cost = sqlite3_column_double(st,5);
        rows[n].semantic_loss = sqlite3_column_double(st,6);
        n++;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    *out_count = n;
    return rows;
}

static int first_artifact_for_family(const char *root, const char *family, char *out, size_t out_sz) {
    char *json = NULL, err[128];
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root_json = NULL;
    if (!family || !out || out_sz == 0) return 1;
    out[0] = '\0';
    if (bf_layer_query_json(root, family, NULL, NULL, NULL, NULL, 0, &json) != 0 || !json) return 1;
    doc = bf_json_parse_str(json, err, sizeof(err));
    if (!doc) { free(json); return 1; }
    root_json = bf_json_root(doc);
    {
        const bf_json_node_t *first = bf_json_arr_get(doc, root_json, 0);
        if (first) json_copy_str_local(doc, first, "artifact_id", out, out_sz);
    }
    bf_json_free(doc);
    free(json);
    return out[0] ? 0 : 1;
}

static int resolve_chain_nodes(const char *root, int count, char **tokens, ChainNode *nodes) {
    for (int i = 0; i < count; i++) {
        snprintf(nodes[i].family, sizeof(nodes[i].family), "%s", tokens[i]);
        if (strchr(tokens[i], ':')) {
            char *json = NULL, err[128];
            bf_json_doc_t *doc = NULL;
            const bf_json_node_t *root_json = NULL, *fams = NULL, *first = NULL;
            snprintf(nodes[i].artifact_id, sizeof(nodes[i].artifact_id), "%s", tokens[i]);
            if (bf_layer_load_json(root, tokens[i], &json) == 0 && json) {
                doc = bf_json_parse_str(json, err, sizeof(err));
                if (doc) {
                    root_json = bf_json_root(doc);
                    fams = bf_json_obj_get(doc, root_json, "families");
                    first = fams ? bf_json_arr_get(doc, fams, 0) : NULL;
                    if (first) bf_json_get_str_copy(first, nodes[i].family, sizeof(nodes[i].family));
                    bf_json_free(doc);
                }
                free(json);
            }
        } else if (first_artifact_for_family(root, tokens[i], nodes[i].artifact_id, sizeof(nodes[i].artifact_id)) != 0) {
            snprintf(nodes[i].artifact_id, sizeof(nodes[i].artifact_id), "family:%s", tokens[i]);
        }
    }
    return 0;
}

static int cmd_init(const char *root) {
    if (root && ensure_dir_recursive(root) != 0) return 1;
    if (bf_discipl_init_db(root) != 0) return 1;
    printf("{\"status\":\"ok\",\"root\":\"%s\"}\n", root ? root : "layeros/state");
    return 0;
}

static int cmd_actors_import(const char *root) {
    DisciplActorWriter writer;
    int ok = 0;
    if (actor_writer_begin(root, &writer) != 0) return 1;
    if (import_command_actors(&writer) == 0 &&
        import_layer_actors(root, &writer) == 0 &&
        import_service_actors(&writer) == 0) {
        ok = 1;
    }
    actor_writer_end(&writer, ok);
    if (!ok) return 1;
    puts("{\"status\":\"ok\",\"imported\":\"actors\"}");
    return 0;
}

static int cmd_contracts_import(const char *root) {
    int count = 0;
    if (bf_discipl_init_db(root) != 0) return 1;
    for (int i = 0; CONTRACT_IMPORT_PAIRS[i][0]; i++) {
        bf_discipl_contract_t c;
        if (!bf_discipl_contract_from_family_relation(CONTRACT_IMPORT_PAIRS[i][0], CONTRACT_IMPORT_PAIRS[i][1], &c)) {
            if (bf_discipl_upsert_contract(root, &c) != 0) return 1;
            count++;
        }
    }
    printf("{\"status\":\"ok\",\"imported_contracts\":%d}\n", count);
    return 0;
}

/* One row per occurrence, joined across three evidence files.
 *
 * The join has to be per occurrence and not per evidence row, and getting that
 * wrong is not a rounding error. Counting observation rows says 2,882 of 4,171
 * attempts retained bytes -- 0.691 -- but an occurrence observed twice
 * contributes twice, and the lattice does not start from an attempt. It starts
 * from an occurrence. Deduplicated, 2,505 of 4,891 occurrences have a body:
 * 0.512. The first number answers "did this fetch work"; only the second
 * answers "if I name a thing in this estate, do I hold its bytes", which is
 * the question every chain rooted at a phenotype is actually asking. */
#define OCC_SLOTS 16384u

typedef struct {
    char id[72];
    unsigned long phenotypes;   /* bitmask over bf_discipl_phenotype_index */
    int has_body;
} OccRow;

static OccRow *occ_table(void)
{
    static OccRow table[OCC_SLOTS];
    return table;
}

static OccRow *occ_find(const char *id, int create)
{
    OccRow *t = occ_table();
    unsigned long h = 1469598103934665603UL;
    unsigned slot;
    const unsigned char *p;
    unsigned probes;
    if (!id || !id[0]) return NULL;
    for (p = (const unsigned char *)id; *p; ++p) { h ^= *p; h *= 1099511628211UL; }
    slot = (unsigned)(h % OCC_SLOTS);
    for (probes = 0; probes < OCC_SLOTS; probes++, slot = (slot + 1u) % OCC_SLOTS) {
        if (!t[slot].id[0]) {
            if (!create) return NULL;
            snprintf(t[slot].id, sizeof(t[slot].id), "%s", id);
            return &t[slot];
        }
        if (strcmp(t[slot].id, id) == 0) return &t[slot];
    }
    return NULL;   /* table full; the caller counts what it could hold */
}

/* Measure what fraction of this estate's reads it can still hand to a binary.
 *
 * Two numbers come out of this and they disagree, which is the reason it is
 * measured here rather than read off the evidence. `body_bytes` and
 * `body_path` are a row's own claim that it retained something. stat() is
 * whether the file is there. Every chain that starts from a retained body
 * crosses this edge, so the prior on it is the smaller of the two or the whole
 * lattice quotes the estate's opinion of itself back to it. */
static int cmd_seed_estate(const char *root, const char *observations,
                           const char *conformance, const char *interpretation)
{
    FILE *fh = NULL;
    char *line = NULL;
    size_t cap = 0;
    ssize_t got;
    unsigned long occurrences = 0, with_body = 0, rows_read = 0, rows_claimed = 0;
    unsigned long pheno_bodies[64];
    unsigned actors = 0, contracts = 0, carriers_seeded = 0, carrier_edges = 0;
    unsigned long carrier_population = 0;
    int npheno = bf_discipl_phenotype_count();
    double rate;
    int rc, i;

    if (!observations || !interpretation) {
        fprintf(stderr, "seed-estate requires --observations JSONL and --interpretation JSONL\n");
        return 2;
    }
    memset(pheno_bodies, 0, sizeof(pheno_bodies));

    /* Which occurrences this estate has typed, and as what. */
    fh = fopen(interpretation, "r");
    if (!fh) { fprintf(stderr, "seed-estate: cannot open %s\n", interpretation); return 1; }
    while ((got = getline(&line, &cap, fh)) > 0) {
        char id[72];
        const char *arr;
        OccRow *row;
        if (got < 2) continue;
        if (!bf_lex_json_string(line, "occurrence_id", id, sizeof(id)) || !id[0]) continue;
        row = occ_find(id, 1);
        if (!row) continue;
        if (!row->phenotypes && !row->has_body) occurrences++;
        arr = bf_lex_json_seek_path(line, "phenotypes");
        if (arr && *arr == '[') {
            const char *p = arr + 1, *next = NULL;
            char name[64];
            while (*p && *p != ']') {
                if (*p != '"') { p++; continue; }
                if (!bf_lex_json_string_at(p + 1, name, sizeof(name), &next)) break;
                { int idx = bf_discipl_phenotype_index(name);
                  if (idx >= 0 && idx < 64) row->phenotypes |= (1UL << idx); }
                p = next ? next : p + 1;
            }
        }
    }
    free(line); line = NULL; cap = 0;
    fclose(fh);

    /* Which of them this estate still holds bytes for, checked with stat()
     * rather than believed from the row that claims retention, and counted
     * once per occurrence rather than once per attempt. */
    fh = fopen(observations, "r");
    if (!fh) { fprintf(stderr, "seed-estate: cannot open %s\n", observations); return 1; }
    while ((got = getline(&line, &cap, fh)) > 0) {
        char body_path[PATH_MAX], body_bytes[64], id[72];
        struct stat st;
        OccRow *row;
        if (got < 2) continue;
        rows_read++;
        if (!bf_lex_json_string(line, "twin_id", id, sizeof(id)) || !id[0]) continue;
        if (!bf_lex_json_string(line, "body_path", body_path, sizeof(body_path))) continue;
        if (!body_path[0]) continue;
        if (!bf_lex_json_scalar(line, "body_bytes", body_bytes, sizeof(body_bytes))) continue;
        if (strtol(body_bytes, NULL, 10) <= 0) continue;
        rows_claimed++;
        if (stat(body_path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0) continue;
        row = occ_find(id, 1);
        if (!row || row->has_body) continue;
        if (!row->phenotypes) occurrences++;   /* observed but never typed */
        row->has_body = 1;
        with_body++;
        for (i = 0; i < npheno && i < 64; i++)
            if (row->phenotypes & (1UL << i)) pheno_bodies[i]++;
    }
    free(line); line = NULL; cap = 0;
    fclose(fh);

    if (occurrences == 0) {
        fprintf(stderr, "seed-estate: no occurrences in %s\n", interpretation);
        return 1;
    }
    rate = (double)with_body / (double)occurrences;
    rc = bf_discipl_seed_estate_bridge(root, rate, &actors, &contracts);
    if (rc == 2) {
        fprintf(stderr, "seed-estate: measured retention is %.6f; nothing on disk to bridge\n", rate);
        return 1;
    }
    if (rc != 0) return 1;

    /* Carriers, per phenotype, counted from the conformance evidence. The
     * rate on each edge is how often an occurrence of that phenotype that we
     * still hold bytes for turned out to carry this carrier. Nothing is
     * seeded for a phenotype-carrier pair nobody has observed. */
    if (conformance) {
        struct { char carrier[64]; char phenotype[64]; unsigned long n; } seen[64];
        int nseen = 0, k;
        fh = fopen(conformance, "r");
        if (!fh) { fprintf(stderr, "seed-estate: cannot open %s\n", conformance); return 1; }
        while (getline(&line, &cap, fh) > 0) {
            char carrier[64], via[64], state[64];
            if (!bf_lex_json_string(line, "state", state, sizeof(state))) continue;
            if (strcmp(state, "carrier_conformant") != 0) continue;
            if (!bf_lex_json_string(line, "carrier", carrier, sizeof(carrier)) || !carrier[0]) continue;
            if (!bf_lex_json_string(line, "via_phenotype", via, sizeof(via)) || !via[0]) continue;
            for (k = 0; k < nseen; k++)
                if (!strcmp(seen[k].carrier, carrier) && !strcmp(seen[k].phenotype, via)) break;
            if (k == nseen) {
                if (nseen == (int)(sizeof(seen)/sizeof(seen[0]))) continue;
                snprintf(seen[nseen].carrier, sizeof(seen[0].carrier), "%s", carrier);
                snprintf(seen[nseen].phenotype, sizeof(seen[0].phenotype), "%s", via);
                seen[nseen].n = 0;
                nseen++;
            }
            seen[k].n++;
        }
        free(line); line = NULL;
        fclose(fh);
        for (k = 0; k < nseen; k++) {
            unsigned a = 0, c = 0;
            int idx = bf_discipl_phenotype_index(seen[k].phenotype);
            unsigned long denom = (idx >= 0 && idx < 64) ? pheno_bodies[idx] : 0;
            double detection;
            int crc;
            if (!denom) {
                fprintf(stderr, "seed-estate: %s conformances via %s, but no occurrence of that "
                                "phenotype has a body; not seeded\n",
                        seen[k].carrier, seen[k].phenotype);
                continue;
            }
            detection = (double)seen[k].n / (double)denom;
            if (detection > 1.0) detection = 1.0;   /* multi-carrier occurrences */
            crc = bf_discipl_seed_carrier_bridge(root, seen[k].carrier, seen[k].phenotype,
                                                 detection, seen[k].n, &a, &c);
            if (crc == 3) {
                fprintf(stderr, "seed-estate: carrier %s (%lu occurrences) declares no "
                                "lattice kinds; not seeded\n", seen[k].carrier, seen[k].n);
                continue;
            }
            if (crc != 0 && crc != 2) return 1;
            if (crc != 0) continue;
            actors += a; contracts += c;
            carrier_edges++;
            carrier_population += seen[k].n;
        }
        for (k = 0; k < nseen; k++) {
            int dup = 0, j;
            for (j = 0; j < k; j++) if (!strcmp(seen[j].carrier, seen[k].carrier)) dup = 1;
            if (!dup) carriers_seeded++;
        }
    }

    printf("{\"schema\":\"bonfyre-discipl-estate-bridge.v2\",\"status\":\"ok\","
           "\"occurrences\":%lu,\"occurrences_with_body\":%lu,"
           "\"retention_rate_measured\":%.6f,"
           "\"observation_rows\":%lu,\"rows_claiming_a_body\":%lu,"
           "\"per_row_rate_not_used\":%.6f,"
           "\"actors\":%u,\"contracts\":%u,\"carriers\":%u,"
           "\"phenotype_carrier_edges\":%u,\"carrier_population\":%lu,"
           "\"prior_source\":\"stat() per occurrence, deduplicated; not per observation row "
           "and not the evidence row's own claim\","
           "\"cannot_infer\":[\"a present body -> a parseable or useful one\","
           "\"a seeded edge -> an executed transform; nothing here has run a binary\","
           "\"a phenotype-carrier rate -> a distribution; an occurrence may carry several "
           "carriers and the rates need not sum to one\"]}\n",
           occurrences, with_body, rate, rows_read, rows_claimed,
           rows_read ? (double)rows_claimed / (double)rows_read : 0.0,
           actors, contracts, carriers_seeded, carrier_edges, carrier_population);
    return 0;
}

static int cmd_seed_commands(const char *root) {
    unsigned actors = 0, contracts = 0;
    if (root && ensure_dir_recursive(root) != 0) return 1;
    if (bf_discipl_seed_command_estate(root, &actors, &contracts) != 0) {
        /* It failed without a word when the repository could not be found. */
        fprintf(stderr, "bonfyre-discipl seed-commands: could not seed; is BONFYRE_REPO_ROOT (or the working directory) this repository?\n");
        return 1;
    }
    printf("{\"status\":\"ok\",\"seeded\":\"command_estate\",\"actors\":%u,\"contracts\":%u}\n", actors, contracts);
    return 0;
}

static int cmd_contracts_list(const char *family_filter) {
    char *json = NULL;
    if (bf_discipl_contracts_json(family_filter, &json) != 0 || !json) {
        fprintf(stderr, "bonfyre-discipl contracts list: failed to export contracts\n");
        free(json);
        return 1;
    }
    puts(json);
    free(json);
    return 0;
}

static int emit_chain(const char *root, const char *goal, int token_count, char **tokens) {
    ChainNode nodes[BF_DISCIPL_MAX_ITEMS];
    ContractRow *rows = NULL;
    bf_discipl_chain_program_t chain;
    bf_discipl_contract_t contracts[BF_DISCIPL_MAX_ITEMS];
    char *json = NULL;
    int nrows = 0;
    memset(&chain, 0, sizeof(chain));
    memset(contracts, 0, sizeof(contracts));
    resolve_chain_nodes(root, token_count, tokens, nodes);
    snprintf(chain.goal, sizeof(chain.goal), "%s", goal ? goal : "discipl-chain");
    snprintf(chain.materialization_policy, sizeof(chain.materialization_policy), "metadata-first");
    snprintf(chain.status, sizeof(chain.status), "proposed");
    /* Identified by the families it walks, not by the goal label. Keying on
     * the label gave every `chain-plan` invocation the same row, so each plan
     * silently replaced the last one and the chain table could never hold more
     * than one plan per verb. */
    {
        char seed[4096], digest[65];
        size_t used = 0;
        int t;
        used += (size_t)snprintf(seed + used, sizeof(seed) - used, "%s", chain.goal);
        for (t = 0; t < token_count && used < sizeof(seed) - 1; t++)
            used += (size_t)snprintf(seed + used, sizeof(seed) - used, "|%s", nodes[t].family);
        bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest);
        snprintf(chain.chain_id, sizeof(chain.chain_id), "discipl_chain:%.*s", 24, digest);
    }
    chain.edge_count = token_count;
    rows = load_all_contracts(root, &nrows);
    for (int i = 0; i < token_count; i++) {
        snprintf(chain.families[i], sizeof(chain.families[i]), "%s", nodes[i].family);
        snprintf(chain.artifact_ids[i], sizeof(chain.artifact_ids[i]), "%s", nodes[i].artifact_id);
        snprintf(chain.per_hop_status[i], sizeof(chain.per_hop_status[i]), "resolved");
        chain.per_hop_confidence[i] = 0.90;
    }
    for (int i = 0; i < token_count - 1; i++) {
        int found = 0;
        for (int j = 0; j < nrows; j++) {
            if (strcmp(rows[j].src, nodes[i].family) == 0 && strcmp(rows[j].dst, nodes[i + 1].family) == 0) {
                snprintf(chain.bridge_requirements[i], sizeof(chain.bridge_requirements[i]), "%s", rows[j].required_bridge);
                snprintf(chain.per_hop_status[i], sizeof(chain.per_hop_status[i]), "%s", rows[j].required_bridge[0] ? "bridge_required" : "valid");
                chain.per_hop_confidence[i] = rows[j].success_prior;
                bf_discipl_contract_from_family_relation(rows[j].src, rows[j].dst, &contracts[i]);
                found = 1;
                break;
            }
        }
        if (!found) {
            snprintf(chain.per_hop_status[i], sizeof(chain.per_hop_status[i]), "%s", strcmp(nodes[i].family, nodes[i + 1].family) == 0 ? "equivalent" : "invalid");
            chain.per_hop_confidence[i] = strcmp(nodes[i].family, nodes[i + 1].family) == 0 ? 0.70 : 0.10;
            memset(&contracts[i], 0, sizeof(contracts[i]));
        }
    }
    bf_discipl_chain_score(&chain, contracts, token_count > 1 ? token_count - 1 : 0);
    free(rows);
    if (bf_discipl_chain_to_json(&chain, &json) != 0) return 1;
    bf_discipl_upsert_chain(root, &chain);
    puts(json);
    free(json);
    return 0;
}

static int cmd_chain_plan(const char *root, int argc, char **argv) {
    char *tokens[BF_DISCIPL_MAX_ITEMS];
    int n = 0;
    if (!require_discipl(root)) return 1;
    for (int i = 2; i < argc && n < BF_DISCIPL_MAX_ITEMS; i++) {
        if (strcmp(argv[i], "--root") == 0 && i + 1 < argc) { i++; continue; }
        if (argv[i][0] == '-') continue;
        tokens[n++] = argv[i];
    }
    if (n < 2) {
        fprintf(stderr, "bonfyre-discipl chain-plan requires at least two families or artifact ids\n");
        return 1;
    }
    return emit_chain(root, "discipl-chain-plan", n, tokens);
}

/* ------------------------------------------------------- chain search ----
 *
 * What this replaced, and why it had to be replaced.
 *
 * `propose` accepted `--depth N` and honoured exactly two values of it. It
 * looked for a direct edge, then for one intermediate, then printed
 * `"candidate_chains":[]`. Every deeper composition in the graph was reported
 * as absent. The estate holds T_WORKFLOW_STAGE -> T_QUEUE_JOB -> T_EXECUTION
 * -> T_LEDGER_EVENT -> T_VALUE_CAPTURE as four contracted edges in its own
 * table, and `propose --from T_WORKFLOW_STAGE --to T_VALUE_CAPTURE --depth 6`
 * answered that no chain existed. That is the worst shape a search can fail
 * in: not an error, an authoritative negative about a path the engine is
 * holding. The composition calculus was unusable past three nodes and the
 * emptiness of discipl_chains is the evidence -- nothing was ever composed.
 *
 * It also returned the *first* path it stumbled on rather than the best one,
 * and gave every proposal the same chain_id (`discipl_chain:discipl-proposal`),
 * so each persisted proposal overwrote the previous one. A search that keeps
 * one result under a constant key is a search whose output cannot accumulate.
 *
 * This is a bounded enumeration of simple paths, scored and ranked. Bounded
 * matters: a dense graph has exponentially many paths, so the walk carries an
 * expansion budget and, when it exhausts it, says `"truncated":true` rather
 * than presenting a partial frontier as a complete one. */

#define PROPOSE_MAX_DEPTH 12
#define PROPOSE_MAX_CANDIDATES 16
#define PROPOSE_EXPANSION_BUDGET 400000L

typedef struct {
    int hops;                          /* edges; nodes == hops + 1 */
    int edge[PROPOSE_MAX_DEPTH];       /* index into the contract table */
    double product;                    /* unfloored product of success_prior */
    double cost;
    double drift;
    double score;
} PathCandidate;

typedef struct {
    const ContractRow *rows;
    int nrows;
    const char *target;
    int max_depth;
    long budget;
    int truncated;
    char visited[PROPOSE_MAX_DEPTH + 1][128];
    int visited_n;
    int edge_stack[PROPOSE_MAX_DEPTH];
    PathCandidate best[PROPOSE_MAX_CANDIDATES];
    int best_n;
} PathSearch;

/* Scored the same way bf_discipl_chain_score scores a chain -- with one
 * deliberate difference, recorded here because it changes what a number means.
 * That function clamps confidence to a 0.35 floor, which makes a ten-hop chain
 * whose priors multiply to 0.004 report the same confidence as a two-hop chain
 * at 0.35. A floor is survivable when you are describing one chain and fatal
 * when you are ranking many, because it erases the very difference the ranking
 * exists to express. Ranking here uses the raw product; the floored value is
 * still what gets persisted, so the stored chain keeps agreeing with the rest
 * of the runtime. Both are emitted. */
static double path_score(double product, double cost, double drift, int hops)
{
    return product - drift - (cost / (1.0 + (double)hops));
}

static void path_candidate_offer(PathSearch *s, const PathCandidate *cand)
{
    int i, at;
    if (s->best_n < PROPOSE_MAX_CANDIDATES) {
        at = s->best_n;
        s->best[s->best_n++] = *cand;
    } else if (cand->score > s->best[PROPOSE_MAX_CANDIDATES - 1].score) {
        at = PROPOSE_MAX_CANDIDATES - 1;
        s->best[at] = *cand;
    } else {
        return;
    }
    /* `at` is where the candidate landed, and the walk up starts there. An
     * earlier version started at `at - 1`, which compared the two elements
     * *below* the new one and left it wherever it was written -- so the list
     * stayed in discovery order and the best chain was reported second. */
    for (i = at; i > 0; i--) {
        PathCandidate tmp;
        if (s->best[i - 1].score >= s->best[i].score) break;
        tmp = s->best[i - 1]; s->best[i - 1] = s->best[i]; s->best[i] = tmp;
    }
}

static int path_seen(const PathSearch *s, const char *family)
{
    int i;
    for (i = 0; i < s->visited_n; i++)
        if (strcmp(s->visited[i], family) == 0) return 1;
    return 0;
}

static void path_walk(PathSearch *s, const char *family, int hops,
                      double product, double cost, double drift)
{
    int i;
    if (hops >= s->max_depth) return;
    for (i = 0; i < s->nrows; i++) {
        double p, c, d;
        if (strcmp(s->rows[i].src, family) != 0) continue;
        if (s->budget-- <= 0) { s->truncated = 1; return; }
        /* Simple paths only. A cycle cannot add reachability and every loop
         * around it multiplies in another success_prior below one, so a walk
         * that revisits is strictly worse than the walk that did not. */
        if (path_seen(s, s->rows[i].dst)) continue;
        p = product * (s->rows[i].success_prior > 0.0 ? s->rows[i].success_prior : 0.5);
        c = cost + s->rows[i].transform_cost;
        d = drift + s->rows[i].semantic_loss;
        s->edge_stack[hops] = i;
        if (strcmp(s->rows[i].dst, s->target) == 0) {
            PathCandidate cand;
            memset(&cand, 0, sizeof(cand));
            cand.hops = hops + 1;
            memcpy(cand.edge, s->edge_stack, sizeof(int) * (size_t)cand.hops);
            cand.product = p; cand.cost = c; cand.drift = d;
            cand.score = path_score(p, c, d, cand.hops);
            path_candidate_offer(s, &cand);
            /* Do not walk past the target. A path that reaches the goal and
             * keeps going is a different goal. */
            continue;
        }
        snprintf(s->visited[s->visited_n], sizeof(s->visited[0]), "%s", s->rows[i].dst);
        s->visited_n++;
        path_walk(s, s->rows[i].dst, hops + 1, p, c, d);
        s->visited_n--;
        if (s->truncated) return;
    }
}

/* A round trip leaves a modality and comes back to it: speak the document
 * aloud, then listen to what was said. Every edge of it is contracted and it
 * would run. It is refused only when a shorter chain to the same goal passes
 * through the same families in the same order without the trip, at no greater
 * cost or loss -- reading the document was already a chain. A longer chain
 * that changes what the later steps receive (hash the transcript rather than
 * the recording) is a different output and is kept. Returns the index of the
 * covering chain, or -1. */
static int path_round_trip(const PathSearch *s, const PathCandidate *cand)
{
    int a, b;
    for (a = 0; a < cand->hops; a++) {
        int out_from = 0, out_to = 0;
        if (!bf_discipl_family_crossing(s->rows[cand->edge[a]].dst, &out_from, &out_to)) continue;
        for (b = a + 1; b < cand->hops; b++) {
            int back_from = 0, back_to = 0;
            if (!bf_discipl_family_crossing(s->rows[cand->edge[b]].dst, &back_from, &back_to)) continue;
            if (back_from == out_to && back_to == out_from) return 1;
            break;   /* crossed on to somewhere else; not a return */
        }
    }
    return 0;
}

static int path_covered_by(const PathSearch *s, int at)
{
    const PathCandidate *long_one = &s->best[at];
    int k;
    if (!path_round_trip(s, long_one)) return -1;
    for (k = 0; k < s->best_n; k++) {
        const PathCandidate *short_one = &s->best[k];
        int i = 0, j = 0;
        if (k == at || short_one->hops >= long_one->hops) continue;
        if (short_one->cost > long_one->cost || short_one->drift > long_one->drift) continue;
        /* The cover has to be the chain without the trip, not a shorter chain
         * that makes the same trip and skips some other step. */
        if (path_round_trip(s, short_one)) continue;
        while (i < short_one->hops && j < long_one->hops) {
            if (!strcmp(s->rows[short_one->edge[i]].dst, s->rows[long_one->edge[j]].dst)) i++;
            j++;
        }
        if (i == short_one->hops) return k;
    }
    return -1;
}

/* A chain identified by what it is, so two proposals for different paths do
 * not collide and the same path proposed twice stays one row. */
static void path_chain_id(const ContractRow *rows, const PathCandidate *cand,
                          const char *from, char *out, size_t out_sz)
{
    char seed[4096];
    char digest[65];
    size_t used = 0;
    int i;
    used += (size_t)snprintf(seed + used, sizeof(seed) - used, "%s", from);
    for (i = 0; i < cand->hops && used < sizeof(seed) - 1; i++)
        used += (size_t)snprintf(seed + used, sizeof(seed) - used, "|%s",
                                 rows[cand->edge[i]].dst);
    bf_sha256_hex((const uint8_t *)seed, strlen(seed), digest);
    snprintf(out, out_sz, "discipl_chain:%.*s", 24, digest);
}

static void path_persist(const char *root, const ContractRow *rows,
                         const PathCandidate *cand, const char *from,
                         const char *to, const char *chain_id)
{
    bf_discipl_chain_program_t chain;
    bf_discipl_contract_t contracts[PROPOSE_MAX_DEPTH];
    int i;
    if (cand->hops + 1 > BF_DISCIPL_MAX_ITEMS) return;
    memset(&chain, 0, sizeof(chain));
    memset(contracts, 0, sizeof(contracts));
    snprintf(chain.chain_id, sizeof(chain.chain_id), "%s", chain_id);
    snprintf(chain.goal, sizeof(chain.goal), "%s -> %s", from, to);
    snprintf(chain.materialization_policy, sizeof(chain.materialization_policy), "metadata-first");
    snprintf(chain.status, sizeof(chain.status), "proposed");
    chain.edge_count = cand->hops + 1;
    snprintf(chain.families[0], sizeof(chain.families[0]), "%s", from);
    snprintf(chain.artifact_ids[0], sizeof(chain.artifact_ids[0]), "family:%s", from);
    snprintf(chain.per_hop_status[0], sizeof(chain.per_hop_status[0]), "resolved");
    for (i = 0; i < cand->hops; i++) {
        const ContractRow *r = &rows[cand->edge[i]];
        snprintf(chain.families[i + 1], sizeof(chain.families[0]), "%s", r->dst);
        snprintf(chain.artifact_ids[i + 1], sizeof(chain.artifact_ids[0]), "family:%s", r->dst);
        snprintf(chain.bridge_requirements[i], sizeof(chain.bridge_requirements[0]), "%s", r->required_bridge);
        snprintf(chain.per_hop_status[i], sizeof(chain.per_hop_status[0]), "%s",
                 r->required_bridge[0] ? "bridge_required" : "valid");
        chain.per_hop_confidence[i] = r->success_prior;
        bf_discipl_contract_from_family_relation(r->src, r->dst, &contracts[i]);
        contracts[i].success_prior = r->success_prior;
        contracts[i].transform_cost = r->transform_cost;
        contracts[i].semantic_loss = r->semantic_loss;
    }
    snprintf(chain.per_hop_status[cand->hops], sizeof(chain.per_hop_status[0]), "resolved");
    chain.per_hop_confidence[cand->hops] = 0.90;
    bf_discipl_chain_score(&chain, contracts, cand->hops);
    bf_discipl_upsert_chain(root, &chain);
}

/* ------------------------------------------------------------- run ------
 *
 * Walk a proposed chain with the real binaries.
 *
 * Everything above this produces contracts: what could transform into what,
 * with what prior. None of it had ever executed anything, which means the
 * lattice was a claim and its scores were claims about a claim. A chain that
 * has never run is a hypothesis with a confidence attached.
 *
 * This runs one, hop by hop, feeding each command's output to the next, and
 * stops at the first hop with no checked argv or a non-zero exit -- naming
 * which hop and why. A partial walk is the honest outcome for a lattice whose
 * commands mostly have no invocation bound yet, and the receipt reports how
 * far it got rather than how far it was proposed. */

/* What the hop produced, asked of the hop rather than assumed.
 *
 * The first version of this hardcoded `normalized.txt` for ingest, which is
 * the name ingest uses when it detects text and not the name it uses
 * otherwise. On a body with no extension it detected `unknown`, wrote the file
 * under its original name, and the guess missed. That alone would have been a
 * missing file; what made it dangerous was the fallback underneath it, which
 * quietly substituted the hop's own stdout log. Both hops then reported `ok`
 * and the chain carried ingest's log forward into the hash. Exit zero twice,
 * and the composition was wrong.
 *
 * BonfyreIngest writes artifact.json naming every atom it produced. That is
 * the binary's own account of its output and it is what gets read. A form that
 * genuinely produces nothing but stdout says so explicitly; a form that
 * declares an output and does not leave one fails the hop. */
static int hop_output_path(const char *hopdir, int form, char *out, size_t out_sz)
{
    switch (form) {
        case BF_INVOKE_INPUT_OUTDIR: {
            char manifest[PATH_MAX];
            char *doc = NULL;
            long len = 0;
            FILE *fh;
            char atom[512] = {0};
            const char *cursor;
            if (snprintf(manifest, sizeof(manifest), "%s/artifact.json", hopdir) >= (int)sizeof(manifest))
                return 1;
            if (!(fh = fopen(manifest, "rb"))) return 1;
            fseek(fh, 0, SEEK_END); len = ftell(fh); fseek(fh, 0, SEEK_SET);
            if (len <= 0 || len > (1 << 22)) { fclose(fh); return 1; }
            doc = malloc((size_t)len + 1);
            if (!doc) { fclose(fh); return 1; }
            if (fread(doc, 1, (size_t)len, fh) != (size_t)len) { free(doc); fclose(fh); return 1; }
            doc[len] = '\0';
            fclose(fh);
            /* The last atom is the most-derived one; earlier atoms are the raw
             * intake this hop was handed. */
            for (cursor = doc; (cursor = strstr(cursor, "\"path\"")) != NULL; cursor += 6) {
                const char *p = strchr(cursor + 6, '"');
                if (!p) break;
                if (!bf_lex_json_string_at(p + 1, atom, sizeof(atom), NULL)) break;
            }
            free(doc);
            if (!atom[0]) return 1;
            return snprintf(out, out_sz, "%s/%s", hopdir, atom) < (int)out_sz ? 0 : 1;
        }
        case BF_INVOKE_PARSE_OUTDIR:
            return snprintf(out, out_sz, "%s", hopdir) < (int)out_sz ? 0 : 1;
        case BF_INVOKE_TEXT_OUT:
            return snprintf(out, out_sz, "%s/embedding.json", hopdir) < (int)out_sz ? 0 : 1;
        default:
            /* bonfyre-hash prints its digest and writes nothing. Its stdout is
             * the artifact, and that is a property of the form, not a guess. */
            return snprintf(out, out_sz, "%s/stdout", hopdir) < (int)out_sz ? 0 : 1;
    }
}

static int run_hop(const char *binary, int form, const char *input,
                   const char *hopdir, const char *stdout_path,
                   const char *declared_type)
{
    pid_t pid;
    int status = 0;
    char outfile[PATH_MAX];
    const char *argv[8];
    int n = 0;

    snprintf(outfile, sizeof(outfile), "%s/embedding.json", hopdir);
    argv[n++] = binary;
    switch (form) {
        case BF_INVOKE_INPUT_OUTDIR:
            argv[n++] = input; argv[n++] = hopdir;
            /* The chain already knows what this material is: it crossed a
             * carrier family to get here. Ingest detecting `unknown` on a feed
             * whose carrier was measured as RSS is the lattice failing to tell
             * a command something it holds. */
            if (declared_type) { argv[n++] = "--type"; argv[n++] = declared_type; }
            break;
        case BF_INVOKE_FILE_SUBCMD:  argv[n++] = "file"; argv[n++] = input; break;
        case BF_INVOKE_PARSE_OUTDIR: argv[n++] = "parse"; argv[n++] = input; argv[n++] = hopdir; break;
        case BF_INVOKE_TEXT_OUT:
            argv[n++] = "--text"; argv[n++] = input;
            argv[n++] = "--out"; argv[n++] = outfile;
            break;
        default: return -1;
    }
    argv[n] = NULL;

    pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        int fd = open(stdout_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd >= 0) { dup2(fd, STDOUT_FILENO); dup2(fd, STDERR_FILENO); close(fd); }
        execv(binary, (char *const *)argv);
        _exit(127);
    }
    if (waitpid(pid, &status, 0) < 0) return -1;
    if (!WIFEXITED(status)) return -1;
    return WEXITSTATUS(status);
}

static int file_digest(const char *path, char *hex)
{
    FILE *fh = fopen(path, "rb");
    unsigned char *buf;
    size_t n, cap = 1u << 20;
    struct stat st;
    if (!fh) return 1;
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) { fclose(fh); return 1; }
    if ((size_t)st.st_size < cap) cap = (size_t)st.st_size ? (size_t)st.st_size : 1;
    buf = malloc(cap);
    if (!buf) { fclose(fh); return 1; }
    n = fread(buf, 1, cap, fh);
    fclose(fh);
    bf_sha256_hex(buf, n, hex);
    free(buf);
    return 0;
}

/* ------------------------------------------------------ bind-commands ----
 *
 * Let the estate discover its own invocations, and make it prove them.
 *
 * Four of twenty-seven commands had a checked argv and the other twenty-three
 * were going to be done one at a time, by hand, forever. But nothing about
 * that work needs a person: every command prints a usage block describing how
 * to call it, and whether a call worked is decidable by running it and looking
 * at what appeared on disk.
 *
 * So this reads each binary's own usage, derives the shapes it claims to
 * accept, and then tries them on a fixture in a scratch directory. A shape is
 * bound only if the binary exits zero AND leaves a file behind, or exits zero
 * and prints something when its usage offers no output directory. Nothing is
 * bound because it looked plausible.
 *
 * Two refusals keep this safe rather than merely automatic. Only shapes that
 * consume a file and, at most, write to a directory are tried -- a usage token
 * naming a database, a URL, a model or a key is skipped, because probing those
 * would mean writing to real state or touching the network to find out how to
 * call something. And every probe runs under a timeout, so a command that
 * waits on input cannot wedge the sweep. */

#define PROBE_TIMEOUT_SECONDS 20

typedef struct {
    char subcommand[64];   /* empty when the usage takes no verb */
    int  wants_outdir;
    int  rank;             /* 0 best: the call this command says it is for */
} ProbeShape;

/* Case-insensitive substring, written out rather than reached for.
 *
 * strcasestr is a GNU extension that macOS exposes and glibc hides under
 * _POSIX_C_SOURCE, which this file sets. Two builds, two behaviours, and the
 * one that breaks is the deploy host -- the same class of break the feature
 * macro at the top of this file exists to prevent. Eight lines is cheaper than
 * a portable incantation. */
static const char *ci_find(const char *haystack, const char *needle)
{
    size_t n = strlen(needle);
    if (!n) return haystack;
    for (; *haystack; haystack++)
        if (strncasecmp(haystack, needle, n) == 0) return haystack;
    return NULL;
}

/* Usage tokens that mean "this call reaches something that is not a file". */
static int probe_token_is_unsafe(const char *usage_line)
{
    static const char *const UNSAFE[] = {
        "--db", "<db", "database", "url", "http", "--key", "model.bin",
        "<model", "--insert-db", "--root", "serve", "daemon", "listen",
        "--host", "--port", "publish", "push", "delete", "reset", NULL
    };
    size_t i;
    for (i = 0; UNSAFE[i]; ++i)
        if (ci_find(usage_line, UNSAFE[i])) return 1;
    return 0;
}

/* Read a binary's usage, in whatever way it offers one. */
static int probe_usage_text(const char *binary, char *out, size_t out_sz)
{
    static const char *const PROBES[] = { "--help", "-h", NULL };
    size_t attempt;
    for (attempt = 0; attempt <= 2; attempt++) {
        char tmp[] = "/tmp/bf-usage-XXXXXX";
        int fd = mkstemp(tmp);
        pid_t pid;
        int status = 0;
        FILE *fh;
        size_t got;
        if (fd < 0) return 1;
        pid = fork();
        if (pid < 0) { close(fd); unlink(tmp); return 1; }
        if (pid == 0) {
            const char *av[3];
            int n = 0;
            alarm(PROBE_TIMEOUT_SECONDS);
            av[n++] = binary;
            if (attempt < 2 && PROBES[attempt]) av[n++] = PROBES[attempt];
            av[n] = NULL;
            dup2(fd, STDOUT_FILENO); dup2(fd, STDERR_FILENO); close(fd);
            execv(binary, (char *const *)av);
            _exit(127);
        }
        waitpid(pid, &status, 0);
        close(fd);
        fh = fopen(tmp, "rb");
        got = fh ? fread(out, 1, out_sz - 1, fh) : 0;
        if (fh) fclose(fh);
        unlink(tmp);
        out[got] = '\0';
        if (ci_find(out, "usage") || ci_find(out, "commands")) return 0;
    }
    return 1;
}

/* The shapes a usage block claims, filtered to the file-in/dir-out ones. */
static int probe_shapes(const char *usage, const char *binname,
                        ProbeShape *shapes, int max_shapes)
{
    const char *line = usage;
    int n = 0;
    while (*line && n < max_shapes) {
        const char *eol = strchr(line, '\n');
        size_t len = eol ? (size_t)(eol - line) : strlen(line);
        char buf[512];
        const char *at;
        if (len >= sizeof(buf)) len = sizeof(buf) - 1;
        memcpy(buf, line, len); buf[len] = '\0';
        line = eol ? eol + 1 : line + strlen(line);
        at = strstr(buf, binname);
        if (!at) continue;
        if (probe_token_is_unsafe(buf)) continue;
        /* Needs a file-shaped argument to be worth trying at all. */
        if (!strstr(buf, "<file") && !strstr(buf, "<path") && !strstr(buf, "<input") &&
            !strstr(buf, "<text") && !strstr(buf, "--text")) continue;
        {
            const char *rest = at + strlen(binname);
            char verb[64] = {0};
            int k = 0;
            while (*rest == ' ') rest++;
            while (*rest && *rest != ' ' && k < (int)sizeof(verb) - 1 &&
                   ((*rest >= 'a' && *rest <= 'z') || *rest == '-')) verb[k++] = *rest++;
            verb[k] = '\0';
            snprintf(shapes[n].subcommand, sizeof(shapes[n].subcommand), "%s", verb);
            shapes[n].wants_outdir = strstr(buf, "output-dir") || strstr(buf, "<out") ||
                                     strstr(buf, "[output") || strstr(buf, "--out") ? 1 : 0;
            n++;
        }
    }
    return n;
}

/* Does this verb share a stem with something the command declares it makes, or
 * with the command's own name? Four letters is enough for tag/tags,
 * embed/embedding, transcribe/transcript, and short enough not to match by
 * accident across a 27-word vocabulary. */
static int probe_stem_matches(const char *verb, const char *word)
{
    size_t i;
    if (!verb || !*verb || !word || !*word) return 0;
    for (i = 0; i < 4; i++) {
        if (!verb[i] || !word[i]) return 0;
        if (tolower((unsigned char)verb[i]) != tolower((unsigned char)word[i])) return 0;
    }
    return 1;
}

static int probe_shape_rank(const ProbeShape *shape, int command_index, const char *family)
{
    int k;
    const char *produced;
    if (!shape->subcommand[0]) return 0;          /* the bare form */
    if (!strcmp(shape->subcommand, "--text")) return 0;
    for (k = 0; (produced = bf_discipl_command_produces(command_index, k)) != NULL; k++)
        if (probe_stem_matches(shape->subcommand, produced)) return 1;
    if (probe_stem_matches(shape->subcommand, family + 2)) return 1;  /* T_TAG -> TAG */
    return 2;
}

static int probe_dir_has_file(const char *dir)
{
    DIR *d = opendir(dir);
    struct dirent *ent;
    int found = 0;
    if (!d) return 0;
    while (!found && (ent = readdir(d))) {
        char path[PATH_MAX];
        struct stat st;
        if (ent->d_name[0] == '.') continue;
        if (snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name) >= (int)sizeof(path)) continue;
        if (stat(path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0) found = 1;
    }
    closedir(d);
    return found;
}

static int probe_try(const char *binary, const ProbeShape *shape,
                     const char *fixture, const char *scratch)
{
    const char *av[8];
    int n = 0, status = 0;
    pid_t pid;
    char outdir[PATH_MAX], logpath[PATH_MAX], outfile[PATH_MAX];
    struct stat st;

    snprintf(outdir, sizeof(outdir), "%s/out", scratch);
    snprintf(logpath, sizeof(logpath), "%s/log", scratch);
    snprintf(outfile, sizeof(outfile), "%s/out/result.json", scratch);
    if (ensure_dir_recursive(outdir) != 0) return 1;

    av[n++] = binary;
    if (shape->subcommand[0] && strcmp(shape->subcommand, "--text")) av[n++] = shape->subcommand;
    if (!strcmp(shape->subcommand, "--text")) {
        av[n++] = "--text"; av[n++] = fixture; av[n++] = "--out"; av[n++] = outfile;
    } else {
        av[n++] = fixture;
        if (shape->wants_outdir) av[n++] = outdir;
    }
    av[n] = NULL;

    pid = fork();
    if (pid < 0) return 1;
    if (pid == 0) {
        int fd = open(logpath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        alarm(PROBE_TIMEOUT_SECONDS);
        if (fd >= 0) { dup2(fd, STDOUT_FILENO); dup2(fd, STDERR_FILENO); close(fd); }
        execv(binary, (char *const *)av);
        _exit(127);
    }
    if (waitpid(pid, &status, 0) < 0) return 1;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return 1;
    /* Exit zero is not enough. Something has to exist that did not before. */
    if (probe_dir_has_file(outdir)) return 0;
    if (stat(outfile, &st) == 0 && st.st_size > 0) return 0;
    if (stat(logpath, &st) == 0 && st.st_size > 0 && !shape->wants_outdir) return 0;
    return 1;
}

static int cmd_bind_commands(const char *root, int argc, char **argv)
{
    const char *fixture = arg_value(argc, argv, "--fixture");
    const char *workdir = arg_value(argc, argv, "--workdir");
    char scratch_base[PATH_MAX], made_fixture[PATH_MAX] = {0};
    int total = bf_discipl_command_total();
    int i, bound = 0, already = 0, no_usage = 0, no_safe_shape = 0, refused = 0, first = 1;
    sqlite3 *db = NULL;
    char db_path[PATH_MAX];

    if (!workdir) { fprintf(stderr, "bind-commands requires --workdir DIR\n"); return 2; }
    if (ensure_dir_recursive(workdir) != 0) { fprintf(stderr, "bind-commands: cannot create %s\n", workdir); return 1; }
    snprintf(scratch_base, sizeof(scratch_base), "%s", workdir);
    if (!fixture) {
        /* A plain text fixture, because every declared input kind that is not
         * a model or a database can be handed one. */
        FILE *fh;
        snprintf(made_fixture, sizeof(made_fixture), "%s/fixture.txt", workdir);
        fh = fopen(made_fixture, "w");
        if (!fh) return 1;
        fputs("bonfyre invocation probe fixture\nsecond line\n", fh);
        fclose(fh);
        fixture = made_fixture;
    }
    if (bf_discipl_init_db(root) != 0) return 1;
    if (bf_layer_state_db_path(root, "discipl.db", db_path, sizeof(db_path)) != 0) return 1;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) return 1;
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS discipl_invocation("
        " family TEXT PRIMARY KEY, binary TEXT, subcommand TEXT, wants_outdir INTEGER,"
        " verified_at TEXT, fixture TEXT)", NULL, NULL, NULL);

    printf("{\"schema\":\"bonfyre-discipl-invocation-binding.v1\",\"commands\":%d,\"bound\":[", total);
    for (i = 0; i < total; i++) {
        char family[128], binary[PATH_MAX], usage[65536], scratch[PATH_MAX];
        ProbeShape shapes[16];
        int nshapes, k, ok = 0;
        if (bf_discipl_command_at(i, family, sizeof(family), binary, sizeof(binary)) != 0) continue;
        if (access(binary, X_OK) != 0) { refused++; continue; }
        if (bf_discipl_command_invocation(family, NULL, 0) != BF_INVOKE_UNBOUND) { already++; continue; }
        if (probe_usage_text(binary, usage, sizeof(usage)) != 0) { no_usage++; continue; }
        nshapes = probe_shapes(usage, strrchr(binary, '/') ? strrchr(binary, '/') + 1 : binary,
                               shapes, (int)(sizeof(shapes)/sizeof(shapes[0])));
        if (!nshapes) { no_safe_shape++; continue; }
        /* Order matters, and the harness proved why on its first run: offered
         * every safe shape bonfyre-tag accepts, it bound `detect-lang`, which
         * exits zero and writes a file and is not what Tag declares it makes.
         * Exit status proves a call works; it cannot prove it is the right
         * call. So shapes are tried in the order of what the command says it
         * produces -- the bare form first, then a verb that shares a stem with
         * a declared output kind or with the command's own name, then the
         * rest, which only get a turn if nothing better ran. */
        {
            int a, b;
            for (a = 0; a < nshapes; a++) shapes[a].rank = probe_shape_rank(&shapes[a], i, family);
            for (a = 1; a < nshapes; a++)
                for (b = a; b > 0 && shapes[b].rank < shapes[b - 1].rank; b--) {
                    ProbeShape t = shapes[b - 1]; shapes[b - 1] = shapes[b]; shapes[b] = t;
                }
        }
        for (k = 0; k < nshapes && !ok; k++) {
            snprintf(scratch, sizeof(scratch), "%s/probe-%d-%d", scratch_base, i, k);
            if (ensure_dir_recursive(scratch) != 0) continue;
            if (probe_try(binary, &shapes[k], fixture, scratch) != 0) continue;
            ok = 1;
            {
                sqlite3_stmt *st = NULL;
                char stamp[32];
                now_iso_local(stamp);
                if (sqlite3_prepare_v2(db,
                    "INSERT OR REPLACE INTO discipl_invocation"
                    "(family,binary,subcommand,wants_outdir,verified_at,fixture)"
                    " VALUES(?,?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(st,1,family,-1,SQLITE_TRANSIENT);
                    sqlite3_bind_text(st,2,binary,-1,SQLITE_TRANSIENT);
                    sqlite3_bind_text(st,3,shapes[k].subcommand,-1,SQLITE_TRANSIENT);
                    sqlite3_bind_int(st,4,shapes[k].wants_outdir);
                    sqlite3_bind_text(st,5,stamp,-1,SQLITE_TRANSIENT);
                    sqlite3_bind_text(st,6,fixture,-1,SQLITE_TRANSIENT);
                    sqlite3_step(st);
                    sqlite3_finalize(st);
                }
            }
            printf("%s{\"family\":\"%s\",\"subcommand\":\"%s\",\"wants_outdir\":%s,"
                   "\"names_a_declared_output\":%s}",
                   first ? "" : ",", family, shapes[k].subcommand,
                   shapes[k].wants_outdir ? "true" : "false",
                   shapes[k].rank <= 1 ? "true" : "false");
            first = 0;
            bound++;
        }
    }
    sqlite3_close(db);
    printf("],\"newly_bound\":%d,\"already_bound\":%d,\"no_usage_block\":%d,"
           "\"no_file_shaped_safe_call\":%d,\"binary_missing\":%d,"
           "\"acceptance\":\"exit zero and something on disk that was not there before\","
           "\"cannot_infer\":[\"a bound invocation -> a correct or meaningful transform; it means the "
           "command ran and produced output on a text fixture\","
           "\"an unbound command -> a broken one; it means no file-in call could be derived safely\"]}\n",
           bound, already, no_usage, no_safe_shape, refused);
    return 0;
}

/* An invocation this estate discovered and proved, if it has one.
 *
 * The compiled table holds the four argvs a person checked. bind-commands
 * holds every one the estate derived from a binary's own usage and then proved
 * by running it. The runner reads both, compiled first, because a checked argv
 * outranks a discovered one -- and reports which kind it used, along with
 * whether the discovered call names something the command declares it makes.
 * `bonfyre-tag detect-lang` runs and produces a file and is not what Tag is
 * for; a receipt that did not say so would be claiming more than it knows. */
typedef struct {
    char binary[PATH_MAX];
    char subcommand[64];
    int  wants_outdir;
    int  found;
} DiscoveredInvocation;

static void discovered_invocation(const char *root, const char *family,
                                  DiscoveredInvocation *out)
{
    char db_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    memset(out, 0, sizeof(*out));
    if (bf_layer_state_db_path(root, "discipl.db", db_path, sizeof(db_path)) != 0) return;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return;
    if (sqlite3_prepare_v2(db, "SELECT binary,subcommand,wants_outdir FROM discipl_invocation "
                               "WHERE family=?1", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, family, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const unsigned char *b = sqlite3_column_text(st, 0);
            const unsigned char *c = sqlite3_column_text(st, 1);
            snprintf(out->binary, sizeof(out->binary), "%s", b ? (const char *)b : "");
            snprintf(out->subcommand, sizeof(out->subcommand), "%s", c ? (const char *)c : "");
            out->wants_outdir = sqlite3_column_int(st, 2);
            out->found = out->binary[0] ? 1 : 0;
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
}

static int run_discovered_hop(const DiscoveredInvocation *inv, const char *input,
                              const char *hopdir, const char *stdout_path)
{
    ProbeShape shape;
    memset(&shape, 0, sizeof(shape));
    snprintf(shape.subcommand, sizeof(shape.subcommand), "%s", inv->subcommand);
    shape.wants_outdir = inv->wants_outdir;
    (void)hopdir;
    (void)stdout_path;
    return probe_try(inv->binary, &shape, input, hopdir);
}

/* Emit the portfolio question rather than answering it.
 *
 * `propose` ranks chains one at a time, and an operator does not run one
 * chain -- they run as many as a budget allows, and which combination fits is
 * a different question with a different shape. It is a subset-sum over the
 * chains' accumulated cost, and this estate has a tier for exactly that kind
 * of question.
 *
 * What stops this from simply shelling out to a reducer is the boundary the
 * pure tier exists to hold: purity is checked before dispatch and placement is
 * asked per backend, because a machine holding every reducer is still not
 * necessarily allowed to see the input. So the composition engine states the
 * question in the tier's own job shape and the tier decides where, and
 * whether, it may be computed.
 *
 * Costs are emitted in milli-units because the lattice compares integers: a
 * float total that disagreed with the reference in the last bit would present
 * as two reducers disagreeing about a pure computation. */
static int cmd_portfolio_job(const char *root, int argc, char **argv)
{
    const char *from = arg_value(argc, argv, "--from");
    const char *to = arg_value(argc, argv, "--to");
    const char *budget_s = arg_value(argc, argv, "--budget-milli");
    const char *depth_s = arg_value(argc, argv, "--depth");
    ContractRow *rows = NULL;
    PathSearch s;
    int nrows = 0, depth = depth_s ? atoi(depth_s) : 9, i, j;
    long budget = budget_s ? atol(budget_s) : 0;

    if (!from || !to || !budget_s) {
        fprintf(stderr, "portfolio-job requires --from FAMILY --to FAMILY --budget-milli N\n");
        return 2;
    }
    if (budget <= 0) { fprintf(stderr, "portfolio-job: budget must be positive\n"); return 2; }
    if (!require_discipl(root)) return 1;
    if (depth < 1) depth = 1;
    if (depth > PROPOSE_MAX_DEPTH) depth = PROPOSE_MAX_DEPTH;
    rows = load_all_contracts(root, &nrows);
    if (!rows && nrows > 0) return 1;
    memset(&s, 0, sizeof(s));
    s.rows = rows; s.nrows = nrows; s.target = to;
    s.max_depth = depth; s.budget = PROPOSE_EXPANSION_BUDGET;
    snprintf(s.visited[0], sizeof(s.visited[0]), "%s", from);
    s.visited_n = 1;
    path_walk(&s, from, 0, 1.0, 0.0, 0.0);

    printf("{\"job\":\"chain-portfolio-%s-to-%s\",\"effect_class\":\"PURE\","
           "\"work_class\":\"discovery\",\"data_scope\":\"public\","
           "\"candidate_space\":[", from, to);
    for (i = 0; i < s.best_n; i++) {
        const PathCandidate *cand = &s.best[i];
        char chain_id[128];
        path_chain_id(rows, cand, from, chain_id, sizeof(chain_id));
        printf("%s{\"id\":\"%s\",\"cost_milli\":%ld,\"confidence_milli\":%ld,"
               "\"drift_milli\":%ld,\"hops\":%d}",
               i ? "," : "", chain_id,
               (long)(cand->cost * 1000.0 + 0.5),
               (long)(cand->product * 1000.0 + 0.5),
               (long)(cand->drift * 1000.0 + 0.5),
               cand->hops);
    }
    printf("],\"subset_budget\":{\"field\":\"cost_milli\",\"budget\":%ld},"
           "\"cannot_infer\":[\"a feasible portfolio -> a runnable one; these chains are "
           "contracts and most of their commands have no checked invocation\","
           "\"accumulated transform cost -> money, time or energy\","
           "\"a budget that fits -> authority to spend it\"]}\n", budget);
    (void)j;
    free(rows);
    return s.best_n > 0 ? 0 : 1;
}

static int cmd_run(const char *root, int argc, char **argv)
{
    const char *from = arg_value(argc, argv, "--from");
    const char *to = arg_value(argc, argv, "--to");
    const char *input = arg_value(argc, argv, "--input");
    const char *workdir = arg_value(argc, argv, "--workdir");
    const char *depth_s = arg_value(argc, argv, "--depth");
    ContractRow *rows = NULL;
    PathSearch s;
    int nrows = 0, depth = depth_s ? atoi(depth_s) : 6;
    int hop, executed = 0, first = 1;
    const char *declared_type = NULL;
    char current[PATH_MAX];
    const PathCandidate *cand;

    if (!from || !to || !input || !workdir) {
        fprintf(stderr, "run requires --from FAMILY --to FAMILY --input FILE --workdir DIR\n");
        return 2;
    }
    if (!require_discipl(root)) return 1;
    if (depth < 1) depth = 1;
    if (depth > PROPOSE_MAX_DEPTH) depth = PROPOSE_MAX_DEPTH;
    if (ensure_dir_recursive(workdir) != 0) { fprintf(stderr, "run: cannot create %s\n", workdir); return 1; }
    if (access(input, R_OK) != 0) { fprintf(stderr, "run: cannot read %s\n", input); return 1; }
    snprintf(current, sizeof(current), "%s", input);
    /* The start family counts too. Only hop destinations were inspected, so a
     * walk that began at T_CARRIER_RSS -- the case where the carrier is most
     * certainly known, because the caller named it -- told ingest nothing and
     * ingest detected `unknown` on a measured feed. */
    if (!strncmp(from, "T_CARRIER_", 10)) declared_type = "text";

    rows = load_all_contracts(root, &nrows);
    if (!rows && nrows > 0) return 1;
    memset(&s, 0, sizeof(s));
    s.rows = rows; s.nrows = nrows; s.target = to;
    s.max_depth = depth; s.budget = PROPOSE_EXPANSION_BUDGET;
    snprintf(s.visited[0], sizeof(s.visited[0]), "%s", from);
    s.visited_n = 1;
    path_walk(&s, from, 0, 1.0, 0.0, 0.0);
    if (!rows || !s.best_n) {
        printf("{\"schema\":\"bonfyre-discipl-run.v1\",\"goal\":\"%s -> %s\","
               "\"ran\":false,\"reason\":\"no chain proposed at this depth\"}\n", from, to);
        free(rows);
        return 1;
    }
    cand = &s.best[0];

    printf("{\"schema\":\"bonfyre-discipl-run.v1\",\"goal\":\"%s -> %s\","
           "\"hops_proposed\":%d,\"input\":\"%s\",\"hops\":[", from, to, cand->hops, input);
    for (hop = 0; hop < cand->hops; hop++) {
        const char *family = rows[cand->edge[hop]].dst;
        char binary[PATH_MAX], hopdir[PATH_MAX], stdout_path[PATH_MAX], produced[PATH_MAX];
        char digest[65] = {0};
        int form = bf_discipl_command_invocation(family, binary, sizeof(binary));
        int rc;

        /* Every carrier this lattice declares kinds for is carried as text. */
        if (!strncmp(family, "T_CARRIER_", 10)) declared_type = "text";

        if (form == BF_INVOKE_UNBOUND) {
            DiscoveredInvocation inv;
            discovered_invocation(root, family, &inv);
            if (inv.found) {
                char probe_out[PATH_MAX];
                snprintf(hopdir, sizeof(hopdir), "%s/hop%d", workdir, hop);
                if (ensure_dir_recursive(hopdir) != 0) break;
                if (run_discovered_hop(&inv, current, hopdir, NULL) != 0) {
                    printf("%s{\"family\":\"%s\",\"status\":\"discovered_invocation_failed\","
                           "\"executed\":true,\"subcommand\":\"%s\"}",
                           first ? "" : ",", family, inv.subcommand);
                    break;
                }
                snprintf(probe_out, sizeof(probe_out), "%s/out", hopdir);
                if (hop_output_path(probe_out, BF_INVOKE_INPUT_OUTDIR, produced, sizeof(produced)) != 0 ||
                    access(produced, R_OK) != 0)
                    snprintf(produced, sizeof(produced), "%s", probe_out);
                if (file_digest(produced, digest) != 0) snprintf(digest, sizeof(digest), "directory");
                printf("%s{\"family\":\"%s\",\"status\":\"ok\",\"executed\":true,"
                       "\"binary\":\"%s\",\"subcommand\":\"%s\",\"produced\":\"%s\","
                       "\"sha256\":\"%s\",\"invocation\":\"discovered_by_probe\"}",
                       first ? "" : ",", family, inv.binary, inv.subcommand, produced, digest);
                first = 0;
                executed++;
                snprintf(current, sizeof(current), "%s", produced);
                continue;
            }
            /* Not a failure of the chain. Either the family is a marker rather
             * than a command -- T_RETAINED_BODY, T_CARRIER_RSS, T_VALUE_CAPTURE
             * name material and outcomes, not binaries -- or no argv for this
             * command has been checked by hand or proved by the prober. */
            printf("%s{\"family\":\"%s\",\"status\":\"no_checked_invocation\",\"executed\":false}",
                   first ? "" : ",", family);
            break;
        }
        snprintf(hopdir, sizeof(hopdir), "%s/hop%d", workdir, hop);
        if (ensure_dir_recursive(hopdir) != 0) break;
        snprintf(stdout_path, sizeof(stdout_path), "%s/stdout", hopdir);
        rc = run_hop(binary, form, current, hopdir, stdout_path, declared_type);
        if (rc != 0) {
            printf("%s{\"family\":\"%s\",\"status\":\"exit_%d\",\"executed\":true,"
                   "\"log\":\"%s\"}", first ? "" : ",", family, rc, stdout_path);
            break;
        }
        if (hop_output_path(hopdir, form, produced, sizeof(produced)) != 0 ||
            access(produced, R_OK) != 0) {
            /* The binary exited zero and left nothing this runner can find.
             * That is a failure of the hop, not an invitation to pass its log
             * to the next command. */
            printf("%s{\"family\":\"%s\",\"status\":\"output_not_found\",\"executed\":true,"
                   "\"log\":\"%s\"}", first ? "" : ",", family, stdout_path);
            break;
        }
        if (file_digest(produced, digest) != 0) snprintf(digest, sizeof(digest), "unreadable");
        printf("%s{\"family\":\"%s\",\"status\":\"ok\",\"executed\":true,"
               "\"binary\":\"%s\",\"produced\":\"%s\",\"sha256\":\"%s\","
               "\"invocation\":\"checked_by_hand\"}",
               first ? "" : ",", family, binary, produced, digest);
        first = 0;
        executed++;
        snprintf(current, sizeof(current), "%s", produced);
    }
    printf("],\"hops_executed\":%d,\"reached_target\":%s,"
           "\"truth\":\"these_binaries_ran_on_this_material\","
           "\"cannot_infer\":[\"an executed hop -> a correct or useful output; exit zero is not review\","
           "\"a completed walk -> an external effect; every binary here read and wrote local files\"]}\n",
           executed, executed == cand->hops ? "true" : "false");
    free(rows);
    return executed > 0 ? 0 : 1;
}

static int cmd_propose(const char *root, const char *from, const char *to, int depth)
{
    ContractRow *rows = NULL;
    PathSearch s;
    int nrows = 0, i, j;
    if (!require_discipl(root)) return 1;
    if (depth < 1) depth = 1;
    if (depth > PROPOSE_MAX_DEPTH) depth = PROPOSE_MAX_DEPTH;
    rows = load_all_contracts(root, &nrows);
    if (!rows && nrows > 0) return 1;

    memset(&s, 0, sizeof(s));
    s.rows = rows; s.nrows = nrows; s.target = to;
    s.max_depth = depth; s.budget = PROPOSE_EXPANSION_BUDGET;
    snprintf(s.visited[0], sizeof(s.visited[0]), "%s", from);
    s.visited_n = 1;
    path_walk(&s, from, 0, 1.0, 0.0, 0.0);

    printf("{\"schema\":\"bonfyre-discipl-proposal.v2\",\"goal\":\"%s -> %s\","
           "\"max_depth\":%d,\"contracts_searched\":%d,\"truncated\":%s,"
           "\"candidate_chains\":[",
           from, to, depth, nrows, s.truncated ? "true" : "false");
    {
        int emitted = 0;
        for (i = 0; i < s.best_n; i++) {
            const PathCandidate *cand = &s.best[i];
            char chain_id[128];
            /* A detour is neither offered nor stored as a proposal. */
            if (path_covered_by(&s, i) >= 0) continue;
            path_chain_id(rows, cand, from, chain_id, sizeof(chain_id));
            path_persist(root, rows, cand, from, to, chain_id);
            printf("%s{\"chain_id\":\"%s\",\"hops\":%d,\"families\":[\"%s\"",
                   emitted++ ? "," : "", chain_id, cand->hops, from);
            for (j = 0; j < cand->hops; j++)
                printf(",\"%s\"", rows[cand->edge[j]].dst);
            printf("],\"relationships\":[");
            for (j = 0; j < cand->hops; j++)
                printf("%s\"%s\"", j ? "," : "", rows[cand->edge[j]].relationship);
            printf("],\"bridges_required\":[");
            {
                int first = 1;
                for (j = 0; j < cand->hops; j++) {
                    if (!rows[cand->edge[j]].required_bridge[0]) continue;
                    printf("%s\"%s\"", first ? "" : ",", rows[cand->edge[j]].required_bridge);
                    first = 0;
                }
            }
            printf("],\"confidence_product\":%.6f,\"accumulated_cost\":%.4f,"
                   "\"semantic_drift\":%.4f,\"score\":%.6f}",
                   cand->product, cand->cost, cand->drift, cand->score);
        }
        printf("],\"refused_detours\":[");
        emitted = 0;
        for (i = 0; i < s.best_n; i++) {
            const PathCandidate *cand = &s.best[i];
            int cover = path_covered_by(&s, i);
            char covering_id[128];
            if (cover < 0) continue;
            path_chain_id(rows, &s.best[cover], from, covering_id, sizeof(covering_id));
            printf("%s{\"hops\":%d,\"families\":[\"%s\"", emitted++ ? "," : "", cand->hops, from);
            for (j = 0; j < cand->hops; j++)
                printf(",\"%s\"", rows[cand->edge[j]].dst);
            printf("],\"covered_by\":\"%s\",\"covered_by_hops\":%d,"
                   "\"extra_cost\":%.4f,\"extra_drift\":%.4f,"
                   "\"reason\":\"it leaves a modality and returns to it, and a shorter chain reaches the same goal through the same families without the trip\"}",
                   covering_id, s.best[cover].hops, cand->cost - s.best[cover].cost, cand->drift - s.best[cover].drift);
        }
    }
    printf("],\"ranked_by\":\"unfloored_confidence_product_minus_drift_minus_amortized_cost\","
           "\"cannot_infer\":[\"a proposed chain -> a runnable chain; binding to real "
           "binaries is capability-closure's job, not this search's\","
           "\"a bridge named in bridges_required -> that bridge existing\","
           "\"score -> measured success; success_prior is a declared prior, never an "
           "observed rate\","
           "\"a chain not refused as a detour -> a useful chain; only a modality round trip "
           "covered by a shorter chain is refused, not a chain whose output serves nobody\"]}\n");
    free(rows);
    return 0;
}

static int cmd_recurse(const char *root, const char *goal) {
    bf_discipl_chain_program_t chain;
    bf_discipl_loop_t loop;
    bf_discipl_actor_t actors[4];
    char verify_json[1024];
    char *json = NULL;
    memset(&chain, 0, sizeof(chain));
    snprintf(chain.chain_id, sizeof(chain.chain_id), "discipl_chain:%s", goal);
    snprintf(chain.goal, sizeof(chain.goal), "%s", goal);
    chain.global_confidence = 0.55;
    snprintf(chain.status, sizeof(chain.status), "proposed");
    bf_discipl_upsert_chain(root, &chain);
    bf_discipl_loop_init(goal, &chain, &loop);
    memset(actors, 0, sizeof(actors));
    snprintf(actors[0].id, sizeof(actors[0].id), "service:planner");
    actors[0].affordances.can_boss = 1;
    actors[0].affordances.can_follow = 1;
    snprintf(actors[1].id, sizeof(actors[1].id), "service:verifier");
    actors[1].affordances.can_follow = 1;
    snprintf(actors[2].id, sizeof(actors[2].id), "service:executor");
    actors[2].affordances.can_follow = 1;
    bf_discipl_loop_assign_roles(&loop, actors, 3);
    bf_discipl_upsert_loop(root, &loop);
    bf_discipl_loop_verify(&loop, &chain, 1, verify_json, sizeof(verify_json));
    (void)verify_json;
    if (bf_discipl_loop_to_json(&loop, &json) != 0) return 1;
    puts(json);
    free(json);
    return 0;
}

static int cmd_verify(const char *root, const char *id) {
    bf_discipl_chain_program_t chain;
    bf_discipl_loop_t loop;
    char buf[1024];
    if (!require_discipl(root)) return 1;
    if (strncmp(id, "discipl_loop:", 13) == 0) {
        if (bf_discipl_load_loop(root, id, &loop) != 0) return 1;
        return bf_discipl_loop_verify(&loop, NULL, 0, buf, sizeof(buf)) == 0 ? (puts(buf), 0) : 1;
    }
    if (bf_discipl_load_chain(root, id, &chain) != 0) return 1;
    printf("{\"chain_id\":\"%s\",\"status\":\"%s\",\"global_confidence\":%.2f}\n", chain.chain_id, chain.status, chain.global_confidence);
    return 0;
}

static void usage(void) {
    fprintf(stderr,
        "BonfyreDiscipl — Recursive DisCIPL runtime substrate\n\n"
        "Usage:\n"
        "  bonfyre-discipl init [--root DIR]\n"
        "  bonfyre-discipl actors import [--root DIR]\n"
        "  bonfyre-discipl contracts import [--root DIR]\n"
        "  bonfyre-discipl seed-commands [--root DIR]\n"
        "  bonfyre-discipl seed-estate --observations JSONL --interpretation JSONL\n"
        "                              [--conformance JSONL] [--root DIR]\n"
        "  bonfyre-discipl contracts list [--family T_FAMILY]\n"
        "  bonfyre-discipl chain-plan [--root DIR] <family-or-artifact> ...\n"
        "  bonfyre-discipl propose --from FAMILY --to FAMILY --depth N [--root DIR]\n"
        "  bonfyre-discipl recurse --goal TEXT [--root DIR]\n"
        "  bonfyre-discipl portfolio-job --from FAMILY --to FAMILY --budget-milli N [--depth N]\n"
        "  bonfyre-discipl bind-commands --workdir DIR [--fixture FILE] [--root DIR]\n"
        "  bonfyre-discipl run --from FAMILY --to FAMILY --input FILE --workdir DIR\n"
        "                      [--depth N] [--root DIR]\n"
        "  bonfyre-discipl verify <chain_id|loop_id> [--root DIR]\n");
}

#define DISCIPL_COMMANDS(X) \
    X("--help", DC_HELP) \
    X("actors", DC_ACTORS) \
    X("bind-commands", DC_BIND_COMMANDS) \
    X("chain-plan", DC_CHAIN_PLAN) \
    X("contracts", DC_CONTRACTS) \
    X("help", DC_HELP_ALIAS) \
    X("init", DC_INIT) \
    X("portfolio-job", DC_PORTFOLIO_JOB) \
    X("propose", DC_PROPOSE) \
    X("recurse", DC_RECURSE) \
    X("run", DC_RUN) \
    X("seed-commands", DC_SEED_COMMANDS) \
    X("seed-estate", DC_SEED_ESTATE) \
    X("verify", DC_VERIFY)

enum DisciplCommand {
#define DISCIPL_ENUM(name, command_id) command_id,
    DISCIPL_COMMANDS(DISCIPL_ENUM)
#undef DISCIPL_ENUM
};

static const BfCommandEntry discipl_commands[] = {
#define DISCIPL_ENTRY(name, command_id) {name, command_id, 0, NULL},
    DISCIPL_COMMANDS(DISCIPL_ENTRY)
#undef DISCIPL_ENTRY
};

int main(int argc, char **argv) {
    const char *root = arg_value(argc, argv, "--root");
    const BfCommandEntry *command;
    if (argc < 2) { usage(); return 1; }
    command = bf_command_lookup(discipl_commands,
                                sizeof(discipl_commands) / sizeof(discipl_commands[0]), argv[1]);
    if (command) {
        switch (command->id) {
            case DC_HELP:
            case DC_HELP_ALIAS: usage(); return 0;
            case DC_INIT: return cmd_init(root);
            case DC_ACTORS:
                if (argc >= 3 && strcmp(argv[2], "import") == 0) return cmd_actors_import(root);
                break;
            case DC_CONTRACTS:
                if (argc >= 3 && strcmp(argv[2], "import") == 0) return cmd_contracts_import(root);
                if (argc >= 3 && strcmp(argv[2], "list") == 0) return cmd_contracts_list(arg_value(argc, argv, "--family"));
                break;
            case DC_SEED_COMMANDS: return cmd_seed_commands(root);
            case DC_RUN: return cmd_run(root, argc, argv);
            case DC_PORTFOLIO_JOB: return cmd_portfolio_job(root, argc, argv);
            case DC_BIND_COMMANDS: return cmd_bind_commands(root, argc, argv);
            case DC_SEED_ESTATE:
                return cmd_seed_estate(root, arg_value(argc, argv, "--observations"),
                                       arg_value(argc, argv, "--conformance"),
                                       arg_value(argc, argv, "--interpretation"));
            case DC_CHAIN_PLAN: return cmd_chain_plan(root, argc, argv);
            case DC_PROPOSE: {
                const char *from = arg_value(argc, argv, "--from");
                const char *to = arg_value(argc, argv, "--to");
                int depth = arg_value(argc, argv, "--depth") ? atoi(arg_value(argc, argv, "--depth")) : 2;
                if (!from || !to) { usage(); return 1; }
                return cmd_propose(root, from, to, depth);
            }
            case DC_RECURSE: {
                const char *goal = arg_value(argc, argv, "--goal");
                if (!goal) { usage(); return 1; }
                return cmd_recurse(root, goal);
            }
            case DC_VERIFY:
                if (argc >= 3) return cmd_verify(root, argv[2]);
                break;
            default: break;
        }
    }
    usage();
    return 1;
}
