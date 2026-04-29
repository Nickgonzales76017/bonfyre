#include <limits.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <bonfyre.h>
#include <bf_json.h>

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
    const char *s = "service";
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
    if (bf_layer_state_db_path(root, "layers.db", db_path, sizeof(db_path)) != 0) return 1;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return 1;
    if (sqlite3_prepare_v2(db, "SELECT artifact_json FROM layer_artifacts ORDER BY artifact_id", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db); return 1;
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

static int load_contract_rows(const char *root, ContractRow *rows, int max_rows) {
    char db_path[PATH_MAX];
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int n = 0;
    if (bf_layer_state_db_path(root, "discipl.db", db_path, sizeof(db_path)) != 0) return 0;
    if (bf_sqlite3_open_ro(db_path, &db) != SQLITE_OK) return 0;
    if (sqlite3_prepare_v2(db, "SELECT src_family,dst_family,relationship,required_bridge,success_prior,transform_cost,semantic_loss FROM discipl_contracts ORDER BY src_family,dst_family", -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db); return 0;
    }
    while (sqlite3_step(st) == SQLITE_ROW && n < max_rows) {
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
    return n;
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
    ContractRow rows[128];
    bf_discipl_chain_program_t chain;
    bf_discipl_contract_t contracts[128];
    char *json = NULL;
    int nrows;
    memset(&chain, 0, sizeof(chain));
    resolve_chain_nodes(root, token_count, tokens, nodes);
    snprintf(chain.goal, sizeof(chain.goal), "%s", goal ? goal : "discipl-chain");
    snprintf(chain.materialization_policy, sizeof(chain.materialization_policy), "metadata-first");
    snprintf(chain.status, sizeof(chain.status), "proposed");
    snprintf(chain.chain_id, sizeof(chain.chain_id), "discipl_chain:%s", chain.goal);
    chain.edge_count = token_count;
    nrows = load_contract_rows(root, rows, 128);
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

static int cmd_propose(const char *root, const char *from, const char *to, int depth) {
    ContractRow rows[256];
    int nrows;
    int found = 0;
    if (!require_discipl(root)) return 1;
    nrows = load_contract_rows(root, rows, 256);
    for (int i = 0; i < nrows; i++) {
        if (strcmp(rows[i].src, from) == 0 && strcmp(rows[i].dst, to) == 0) {
            char *tokens[2] = { (char *)from, (char *)to };
            return emit_chain(root, "discipl-proposal", 2, tokens);
        }
    }
    if (depth >= 2) {
        for (int i = 0; i < nrows && !found; i++) {
            if (strcmp(rows[i].src, from) != 0) continue;
            for (int j = 0; j < nrows; j++) {
                if (strcmp(rows[j].src, rows[i].dst) == 0 && strcmp(rows[j].dst, to) == 0) {
                    char *tokens[3] = { (char *)from, rows[i].dst, (char *)to };
                    return emit_chain(root, "discipl-proposal", 3, tokens);
                }
            }
        }
    }
    printf("{\"goal\":\"%s -> %s\",\"candidate_chains\":[]}\n", from, to);
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
        "  bonfyre-discipl contracts list [--family T_FAMILY]\n"
        "  bonfyre-discipl chain-plan [--root DIR] <family-or-artifact> ...\n"
        "  bonfyre-discipl propose --from FAMILY --to FAMILY --depth N [--root DIR]\n"
        "  bonfyre-discipl recurse --goal TEXT [--root DIR]\n"
        "  bonfyre-discipl verify <chain_id|loop_id> [--root DIR]\n");
}

int main(int argc, char **argv) {
    const char *root = arg_value(argc, argv, "--root");
    if (argc < 2) { usage(); return 1; }
    if (strcmp(argv[1], "init") == 0) return cmd_init(root);
    if (strcmp(argv[1], "actors") == 0 && argc >= 3 && strcmp(argv[2], "import") == 0) return cmd_actors_import(root);
    if (strcmp(argv[1], "contracts") == 0 && argc >= 3 && strcmp(argv[2], "import") == 0) return cmd_contracts_import(root);
    if (strcmp(argv[1], "contracts") == 0 && argc >= 3 && strcmp(argv[2], "list") == 0) return cmd_contracts_list(arg_value(argc, argv, "--family"));
    if (strcmp(argv[1], "chain-plan") == 0) return cmd_chain_plan(root, argc, argv);
    if (strcmp(argv[1], "propose") == 0) {
        const char *from = arg_value(argc, argv, "--from");
        const char *to = arg_value(argc, argv, "--to");
        int depth = arg_value(argc, argv, "--depth") ? atoi(arg_value(argc, argv, "--depth")) : 2;
        if (!from || !to) { usage(); return 1; }
        return cmd_propose(root, from, to, depth);
    }
    if (strcmp(argv[1], "recurse") == 0) {
        const char *goal = arg_value(argc, argv, "--goal");
        if (!goal) { usage(); return 1; }
        return cmd_recurse(root, goal);
    }
    if (strcmp(argv[1], "verify") == 0 && argc >= 3) return cmd_verify(root, argv[2]);
    usage();
    return 1;
}
