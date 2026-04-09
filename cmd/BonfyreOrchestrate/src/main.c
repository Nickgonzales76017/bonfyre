#define _POSIX_C_SOURCE 200809L
#include "bonfyre.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sqlite3.h>

#define MAX_PLAN_STEPS 32
#define MAX_TEXT 128
#define MODEL_TEXT 96

typedef struct {
    char input_type[MAX_TEXT];
    char objective[MAX_TEXT];
    char latency_class[MAX_TEXT];
    char surface[MAX_TEXT];
    char artifact_path[256];
} OrchestrateRequest;

typedef struct {
    double exec;
    double artifact;
    double tensor;
    double cms;
    double retrieval;
    double value;
} BfFeedbackDomains;

typedef struct {
    int selected[MAX_PLAN_STEPS];
    int selected_count;
    int boosters[MAX_PLAN_STEPS];
    int booster_count;
    const char *outputs[MAX_PLAN_STEPS];
    int output_count;
    const char *surfaces[8];
    int surface_count;
    char mode[24];
    char model[MODEL_TEXT];
    double predicted_cost;
    double predicted_latency;
    double predicted_confidence;
    double predicted_reversibility;
    double predicted_utility;
    double predicted_information_gain;
} OrchestratePlan;

static const char *DEFAULT_MODEL = "google/gemma-4-E4B";
static const char *DEFAULT_POLICY_DB = ".bonfyre/orchestrate.db";
static const char *SYSTEM_PROMPT =
    "Bonfyre Orchestrate. Machine-only. No user prompting. "
    "Choose the smallest Bonfyre boost set that improves quality without slowing the fast path. "
    "Only return JSON with keys selected_binaries and booster_binaries.";

static void usage(void) {
    fprintf(stderr,
            "bonfyre-orchestrate\n\n"
            "Usage:\n"
            "  bonfyre-orchestrate status\n"
            "  bonfyre-orchestrate plan <request.json>\n"
            "  bonfyre-orchestrate feedback <request.json> <quality_gain> <latency_delta>\n\n"
            "Environment:\n"
            "  BONFYRE_ORCHESTRATE_ENDPOINT  OpenAI-compatible Gemma endpoint\n"
            "  BONFYRE_ORCHESTRATE_MODEL     Model name (default: google/gemma-4-E4B)\n"
            "  BONFYRE_ORCHESTRATE_API_KEY   Optional bearer token\n"
            "  BONFYRE_ORCHESTRATE_POLICY_DB Optional SQLite policy path\n");
}

static void copy_text(char *dst, size_t dst_sz, const char *src) {
    if (!dst || dst_sz == 0) return;
    snprintf(dst, dst_sz, "%s", src ? src : "");
}

static double clamp01(double value) {
    if (value < 0.0) return 0.0;
    if (value > 1.0) return 1.0;
    return value;
}

static BfFeedbackDomains default_domains(double quality_gain, double latency_delta) {
    BfFeedbackDomains d;
    d.exec = clamp01(quality_gain - latency_delta + 0.5);
    d.artifact = clamp01(quality_gain);
    d.tensor = clamp01(quality_gain * 0.8);
    d.cms = clamp01(quality_gain * 0.75);
    d.retrieval = clamp01(quality_gain * 0.85);
    d.value = clamp01(quality_gain * 0.65);
    return d;
}

static double domain_policy_score(BfFeedbackDomains d) {
    return
        d.exec * 0.22 +
        d.artifact * 0.18 +
        d.tensor * 0.12 +
        d.cms * 0.16 +
        d.retrieval * 0.18 +
        d.value * 0.14;
}

static int icontains(const char *haystack, const char *needle) {
    if (!haystack || !needle || !needle[0]) return 0;
    size_t n = strlen(needle);
    for (const char *p = haystack; *p; ++p) {
        if (strncasecmp(p, needle, n) == 0) return 1;
    }
    return 0;
}

static int json_string(const char *json, const char *key, char *dst, size_t dst_sz) {
    char needle[64];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != ':') return 0;
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '"') return 0;
    p++;
    size_t j = 0;
    while (*p && *p != '"' && j + 1 < dst_sz) {
        if (*p == '\\' && p[1]) {
            p++;
            dst[j++] = (*p == 'n') ? '\n' : *p;
        } else {
            dst[j++] = *p;
        }
        p++;
    }
    dst[j] = '\0';
    return j > 0;
}

static void infer_defaults(OrchestrateRequest *req) {
    if (!req->input_type[0]) {
        if (icontains(req->artifact_path, ".wav") || icontains(req->artifact_path, ".mp3") ||
            icontains(req->artifact_path, ".m4a") || icontains(req->artifact_path, ".flac")) {
            copy_text(req->input_type, sizeof(req->input_type), "audio");
        } else if (icontains(req->artifact_path, "artifact.json")) {
            copy_text(req->input_type, sizeof(req->input_type), "artifact");
        } else {
            copy_text(req->input_type, sizeof(req->input_type), "text");
        }
    }
    if (!req->objective[0]) copy_text(req->objective, sizeof(req->objective), "boost-bonfyre-flow");
    if (!req->latency_class[0]) copy_text(req->latency_class, sizeof(req->latency_class), "interactive");
    if (!req->surface[0]) copy_text(req->surface, sizeof(req->surface), "pages");
}

static int load_request(const char *path, OrchestrateRequest *req) {
    memset(req, 0, sizeof(*req));
    copy_text(req->artifact_path, sizeof(req->artifact_path), path);
    char *json = bf_read_file(path, NULL);
    if (!json) return 1;
    json_string(json, "input_type", req->input_type, sizeof(req->input_type));
    json_string(json, "objective", req->objective, sizeof(req->objective));
    json_string(json, "latency_class", req->latency_class, sizeof(req->latency_class));
    json_string(json, "surface", req->surface, sizeof(req->surface));
    json_string(json, "artifact_path", req->artifact_path, sizeof(req->artifact_path));
    free(json);
    infer_defaults(req);
    return 0;
}

static int op_index(const char *name_or_binary) {
    const BfOperator *op = bf_operator_find(name_or_binary);
    if (!op) op = bf_operator_find_by_name(name_or_binary);
    return op ? (int)(op - BF_OPERATORS) : -1;
}

static int contains_idx(const int *items, int count, int idx) {
    for (int i = 0; i < count; ++i) {
        if (items[i] == idx) return 1;
    }
    return 0;
}

static void add_selected(OrchestratePlan *plan, const char *name_or_binary) {
    int idx = op_index(name_or_binary);
    if (idx < 0 || plan->selected_count >= MAX_PLAN_STEPS || contains_idx(plan->selected, plan->selected_count, idx)) return;
    plan->selected[plan->selected_count++] = idx;
}

static void add_booster(OrchestratePlan *plan, const char *name_or_binary) {
    int idx = op_index(name_or_binary);
    if (idx < 0 || plan->booster_count >= MAX_PLAN_STEPS ||
        contains_idx(plan->selected, plan->selected_count, idx) ||
        contains_idx(plan->boosters, plan->booster_count, idx)) return;
    plan->boosters[plan->booster_count++] = idx;
}

static void add_surface(OrchestratePlan *plan, const char *surface) {
    if (!surface || !surface[0] || plan->surface_count >= 8) return;
    for (int i = 0; i < plan->surface_count; ++i) {
        if (strcmp(plan->surfaces[i], surface) == 0) return;
    }
    plan->surfaces[plan->surface_count++] = surface;
}

static void collect_outputs(OrchestratePlan *plan) {
    plan->output_count = 0;
    for (int i = 0; i < plan->selected_count; ++i) {
      const BfOperator *op = &BF_OPERATORS[plan->selected[i]];
      for (int j = 0; j < BF_MAX_TYPES && op->output_types[j]; ++j) {
        const char *out = op->output_types[j];
        int dup = 0;
        for (int k = 0; k < plan->output_count; ++k) {
          if (strcmp(plan->outputs[k], out) == 0) {
            dup = 1;
            break;
          }
        }
        if (!dup && plan->output_count < MAX_PLAN_STEPS) {
          plan->outputs[plan->output_count++] = out;
        }
      }
    }
}

static void compute_plan_metrics(OrchestratePlan *plan) {
    double cost = 0.0;
    double latency = 0.0;
    double confidence = 0.0;
    double reversibility = 0.0;
    double utility = 0.0;
    double information_gain = 0.0;
    int count = 0;

    for (int i = 0; i < plan->selected_count; ++i) {
        BfOperatorProfile profile = bf_operator_profile(&BF_OPERATORS[plan->selected[i]]);
        cost += profile.cost;
        latency += profile.latency;
        confidence += profile.confidence;
        reversibility += profile.reversibility;
        utility += profile.utility;
        information_gain += profile.information_gain;
        count++;
    }
    for (int i = 0; i < plan->booster_count; ++i) {
        BfOperatorProfile profile = bf_operator_profile(&BF_OPERATORS[plan->boosters[i]]);
        cost += profile.cost * 0.45;
        latency += profile.latency * 0.45;
        confidence += profile.confidence * 0.45;
        reversibility += profile.reversibility * 0.45;
        utility += profile.utility * 0.60;
        information_gain += profile.information_gain * 0.75;
        count++;
    }

    if (count <= 0) count = 1;
    plan->predicted_cost = cost / (double)count;
    plan->predicted_latency = latency / (double)count;
    plan->predicted_confidence = confidence / (double)count;
    plan->predicted_reversibility = reversibility / (double)count;
    plan->predicted_utility = utility / (double)count;
    plan->predicted_information_gain = information_gain / (double)count;
}

static void build_signature(const OrchestrateRequest *req, char *dst, size_t dst_sz) {
    snprintf(dst, dst_sz, "%s|%s|%s|%s",
             req->input_type, req->objective, req->latency_class, req->surface);
}

static const char *policy_db_path(void) {
    const char *path = getenv("BONFYRE_ORCHESTRATE_POLICY_DB");
    if (path && path[0]) return path;
    static char fallback[512];
    const char *home = getenv("HOME");
    snprintf(fallback, sizeof(fallback), "%s/%s", home && home[0] ? home : ".", DEFAULT_POLICY_DB);
    return fallback;
}

static int ensure_policy_db(sqlite3 **db) {
    if (!db) return 1;
    *db = NULL;
    const char *path = policy_db_path();
    char parent[512];
    snprintf(parent, sizeof(parent), "%s", path);
    char *slash = strrchr(parent, '/');
    if (slash) {
        *slash = '\0';
        if (parent[0]) bf_ensure_dir(parent);
    }
    if (sqlite3_open(path, db) != SQLITE_OK) return 1;
    const char *sql =
        "CREATE TABLE IF NOT EXISTS orchestration_policy ("
        "signature TEXT PRIMARY KEY,"
        "booster_csv TEXT NOT NULL,"
        "predicted_confidence REAL NOT NULL,"
        "predicted_information_gain REAL NOT NULL,"
        "avg_quality_gain REAL NOT NULL DEFAULT 0,"
        "avg_latency_delta REAL NOT NULL DEFAULT 0,"
        "avg_regret REAL NOT NULL DEFAULT 0,"
        "exec_score REAL NOT NULL DEFAULT 0,"
        "artifact_score REAL NOT NULL DEFAULT 0,"
        "tensor_score REAL NOT NULL DEFAULT 0,"
        "cms_score REAL NOT NULL DEFAULT 0,"
        "retrieval_score REAL NOT NULL DEFAULT 0,"
        "value_score REAL NOT NULL DEFAULT 0,"
        "policy_score REAL NOT NULL DEFAULT 0,"
        "samples INTEGER NOT NULL DEFAULT 0,"
        "updated_at TEXT NOT NULL"
        ");";

    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN exec_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN artifact_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN tensor_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN cms_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN retrieval_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN value_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    sqlite3_exec(*db, "ALTER TABLE orchestration_policy ADD COLUMN policy_score REAL NOT NULL DEFAULT 0;", NULL, NULL, NULL);
    if (sqlite3_exec(*db, sql, NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(*db);
        *db = NULL;
        return 1;
    }
    return 0;
}

static void import_booster_csv(OrchestratePlan *plan, const char *csv) {
    if (!csv || !csv[0]) return;
    char *copy = strdup(csv);
    if (!copy) return;
    for (char *save = NULL, *token = strtok_r(copy, ",", &save); token; token = strtok_r(NULL, ",", &save)) {
        add_booster(plan, token);
    }
    free(copy);
    collect_outputs(plan);
    compute_plan_metrics(plan);
}

static int load_policy_memory(const OrchestrateRequest *req, OrchestratePlan *plan) {
    sqlite3 *db = NULL;
    if (ensure_policy_db(&db) != 0) return 0;
    char signature[512];
    build_signature(req, signature, sizeof(signature));

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT booster_csv, predicted_confidence, predicted_information_gain, avg_regret, samples, policy_score "
        "FROM orchestration_policy WHERE signature = ?1;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    sqlite3_bind_text(stmt, 1, signature, -1, SQLITE_STATIC);
    int found = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char *csv = sqlite3_column_text(stmt, 0);
        double cached_conf = sqlite3_column_double(stmt, 1);
        double cached_regret = sqlite3_column_double(stmt, 3);
        int samples = sqlite3_column_int(stmt, 4);
        double policy_score = sqlite3_column_double(stmt, 5);
        if (csv && cached_conf >= plan->predicted_confidence &&
            (samples < 3 || (cached_regret <= 0.15 && policy_score >= 0.45))) {
            import_booster_csv(plan, (const char *)csv);
            copy_text(plan->mode, sizeof(plan->mode), "policy-memory");
            found = 1;
        }
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return found;
}

static void save_policy_memory(const OrchestrateRequest *req, const OrchestratePlan *plan) {
    sqlite3 *db = NULL;
    if (ensure_policy_db(&db) != 0) return;
    char signature[512];
    char updated_at[32];
    build_signature(req, signature, sizeof(signature));
    bf_iso_timestamp(updated_at, sizeof(updated_at));

    char booster_csv[1024];
    booster_csv[0] = '\0';
    for (int i = 0; i < plan->booster_count; ++i) {
        const char *binary = BF_OPERATORS[plan->boosters[i]].binary;
        if (i) strncat(booster_csv, ",", sizeof(booster_csv) - strlen(booster_csv) - 1);
        strncat(booster_csv, binary, sizeof(booster_csv) - strlen(booster_csv) - 1);
    }

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO orchestration_policy(signature, booster_csv, predicted_confidence, predicted_information_gain, updated_at) "
        "VALUES(?1, ?2, ?3, ?4, ?5) "
        "ON CONFLICT(signature) DO UPDATE SET "
        "booster_csv=excluded.booster_csv, "
        "predicted_confidence=excluded.predicted_confidence, "
        "predicted_information_gain=excluded.predicted_information_gain, "
        "updated_at=excluded.updated_at;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, signature, -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, booster_csv, -1, SQLITE_STATIC);
        sqlite3_bind_double(stmt, 3, plan->predicted_confidence);
        sqlite3_bind_double(stmt, 4, plan->predicted_information_gain);
        sqlite3_bind_text(stmt, 5, updated_at, -1, SQLITE_STATIC);
        sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

static int command_feedback(const char *path, const char *quality_gain_text, const char *latency_delta_text) {
    OrchestrateRequest req;
    if (load_request(path, &req) != 0) {
        fprintf(stderr, "Failed to read request file: %s\n", path);
        return 1;
    }

    double quality_gain = atof(quality_gain_text);
    double latency_delta = atof(latency_delta_text);
    double regret = latency_delta - quality_gain;
    BfFeedbackDomains domains = default_domains(quality_gain, latency_delta);
    double policy_score = domain_policy_score(domains);

    sqlite3 *db = NULL;
    if (ensure_policy_db(&db) != 0) {
        fprintf(stderr, "Failed to open policy db\n");
        return 1;
    }

    char signature[512];
    char updated_at[32];
    build_signature(&req, signature, sizeof(signature));
    bf_iso_timestamp(updated_at, sizeof(updated_at));

    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO orchestration_policy(signature, booster_csv, predicted_confidence, predicted_information_gain, avg_quality_gain, avg_latency_delta, avg_regret, exec_score, artifact_score, tensor_score, cms_score, retrieval_score, value_score, policy_score, samples, updated_at) "
        "VALUES(?1, '', 0, 0, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1, ?12) "
        "ON CONFLICT(signature) DO UPDATE SET "
        "avg_quality_gain=((avg_quality_gain*samples)+excluded.avg_quality_gain)/(samples+1), "
        "avg_latency_delta=((avg_latency_delta*samples)+excluded.avg_latency_delta)/(samples+1), "
        "avg_regret=((avg_regret*samples)+excluded.avg_regret)/(samples+1), "
        "exec_score=((exec_score*samples)+excluded.exec_score)/(samples+1), "
        "artifact_score=((artifact_score*samples)+excluded.artifact_score)/(samples+1), "
        "tensor_score=((tensor_score*samples)+excluded.tensor_score)/(samples+1), "
        "cms_score=((cms_score*samples)+excluded.cms_score)/(samples+1), "
        "retrieval_score=((retrieval_score*samples)+excluded.retrieval_score)/(samples+1), "
        "value_score=((value_score*samples)+excluded.value_score)/(samples+1), "
        "policy_score=((policy_score*samples)+excluded.policy_score)/(samples+1), "
        "samples=samples+1, "
        "updated_at=excluded.updated_at;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        fprintf(stderr, "Failed to prepare feedback statement\n");
        return 1;
    }
    sqlite3_bind_text(stmt, 1, signature, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 2, quality_gain);
    sqlite3_bind_double(stmt, 3, latency_delta);
    sqlite3_bind_double(stmt, 4, regret);
    sqlite3_bind_double(stmt, 5, domains.exec);
    sqlite3_bind_double(stmt, 6, domains.artifact);
    sqlite3_bind_double(stmt, 7, domains.tensor);
    sqlite3_bind_double(stmt, 8, domains.cms);
    sqlite3_bind_double(stmt, 9, domains.retrieval);
    sqlite3_bind_double(stmt, 10, domains.value);
    sqlite3_bind_double(stmt, 11, policy_score);
    sqlite3_bind_text(stmt, 12, updated_at, -1, SQLITE_STATIC);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    printf("{\"status\":\"ok\",\"signature\":\"%s\",\"quality_gain\":%.3f,\"latency_delta\":%.3f,\"regret\":%.3f,"
           "\"domains\":{\"exec\":%.3f,\"artifact\":%.3f,\"tensor\":%.3f,\"cms\":%.3f,\"retrieval\":%.3f,\"value\":%.3f},"
           "\"policy_score\":%.3f}\n",
           signature, quality_gain, latency_delta, regret,
           domains.exec, domains.artifact, domains.tensor, domains.cms, domains.retrieval, domains.value,
           policy_score);
    return 0;
}

static void init_plan(OrchestratePlan *plan, const char *model) {
    memset(plan, 0, sizeof(*plan));
    copy_text(plan->mode, sizeof(plan->mode), "heuristic");
    copy_text(plan->model, sizeof(plan->model), model && model[0] ? model : DEFAULT_MODEL);
}

static void heuristic_plan(const OrchestrateRequest *req, OrchestratePlan *plan) {
    int fast = icontains(req->latency_class, "fast") || icontains(req->latency_class, "interactive") || icontains(req->latency_class, "realtime");

    if (icontains(req->input_type, "audio")) {
        add_selected(plan, "ingest");
        add_selected(plan, "media-prep");
        add_selected(plan, "transcribe");
        add_selected(plan, fast ? "brief" : "transcript-clean");
        if (fast) {
            add_booster(plan, "transcript-clean");
            add_booster(plan, "paragraph");
        } else {
            add_selected(plan, "paragraph");
            add_selected(plan, "brief");
        }
        add_booster(plan, "proof");
        add_booster(plan, "tag");
    } else if (icontains(req->input_type, "artifact")) {
        add_selected(plan, "hash");
        add_selected(plan, "canon");
        add_selected(plan, "render");
        add_booster(plan, "query");
        add_booster(plan, "graph");
    } else {
        add_selected(plan, "ingest");
        add_selected(plan, "canon");
        add_selected(plan, "brief");
        add_booster(plan, "tag");
        add_booster(plan, "render");
    }

    if (icontains(req->objective, "podcast") || icontains(req->objective, "publish") ||
        icontains(req->objective, "release") || icontains(req->objective, "radio")) {
        add_booster(plan, "narrate");
        add_booster(plan, "clips");
        add_booster(plan, "render");
        add_booster(plan, "emit");
        add_booster(plan, "pack");
        add_booster(plan, "distribute");
    }

    if (icontains(req->objective, "memory") || icontains(req->objective, "search") ||
        icontains(req->objective, "semantic") || icontains(req->objective, "repo") ||
        icontains(req->objective, "civic") || icontains(req->objective, "atlas")) {
        add_booster(plan, "embed");
        add_booster(plan, "index");
        add_booster(plan, "vec");
        add_booster(plan, "query");
        add_booster(plan, "graph");
    }

    if (icontains(req->objective, "legal") || icontains(req->objective, "evidence") ||
        icontains(req->objective, "sales") || icontains(req->objective, "grant") ||
        icontains(req->objective, "procurement") || icontains(req->objective, "consult")) {
        add_booster(plan, "offer");
        add_booster(plan, "ledger");
        add_booster(plan, "gate");
        add_booster(plan, "meter");
    }

    if (icontains(req->objective, "shift") || icontains(req->objective, "handoff") ||
        icontains(req->objective, "live") || icontains(req->objective, "call")) {
        add_booster(plan, "segment");
        add_booster(plan, "speechloop");
        add_booster(plan, "tone");
    }

    if (icontains(req->surface, "pages")) {
        add_surface(plan, "bonfyre-render");
        add_surface(plan, "bonfyre-emit");
    }
    if (icontains(req->surface, "api") || icontains(req->surface, "backend")) {
        add_surface(plan, "bonfyre-api");
        add_surface(plan, "bonfyre-auth");
    }
    if (icontains(req->surface, "jobs") || icontains(req->surface, "queue") || icontains(req->surface, "actions")) {
        add_surface(plan, "bonfyre-queue");
        add_surface(plan, "bonfyre-runtime");
    }
    if (!plan->surface_count) add_surface(plan, "bonfyre-runtime");

    collect_outputs(plan);
    compute_plan_metrics(plan);
}

static void escape_json(FILE *fp, const char *text) {
    for (const char *p = text ? text : ""; *p; ++p) {
        if (*p == '\\') fputs("\\\\", fp);
        else if (*p == '"') fputs("\\\"", fp);
        else if (*p == '\n') fputs("\\n", fp);
        else fputc(*p, fp);
    }
}

static void write_registry(FILE *fp) {
    fputc('[', fp);
    for (int i = 0; i < BF_OPERATOR_COUNT; ++i) {
        const BfOperator *op = &BF_OPERATORS[i];
        if (i) fputc(',', fp);
        fprintf(fp, "{\"binary\":\"");
        escape_json(fp, op->binary);
        fprintf(fp, "\",\"layer\":\"");
        escape_json(fp, op->layer);
        fprintf(fp, "\",\"group\":\"");
        escape_json(fp, op->group);
        fprintf(fp, "\"}");
    }
    fputc(']', fp);
}

static char *slurp(FILE *fp) {
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) return NULL;
    int ch;
    while ((ch = fgetc(fp)) != EOF) {
        if (len + 2 >= cap) {
            cap *= 2;
            char *next = realloc(buf, cap);
            if (!next) {
                free(buf);
                return NULL;
            }
            buf = next;
        }
        buf[len++] = (char)ch;
    }
    buf[len] = '\0';
    return buf;
}

static int shell_safe(const char *text) {
    if (!text) return 0;
    for (const char *p = text; *p; ++p) {
        if (!(isalnum((unsigned char)*p) || *p == ':' || *p == '/' || *p == '.' || *p == '-' || *p == '_' || *p == '?'
              || *p == '=' || *p == '&' || *p == '%')) return 0;
    }
    return 1;
}

static void adopt_model_boosters(OrchestratePlan *plan, const char *response) {
    if (!response) return;
    int added = 0;
    for (int i = 0; i < BF_OPERATOR_COUNT; ++i) {
        if (icontains(response, BF_OPERATORS[i].binary) || icontains(response, BF_OPERATORS[i].name)) {
            int before = plan->booster_count;
            add_booster(plan, BF_OPERATORS[i].binary);
            if (plan->booster_count != before) added = 1;
        }
    }
    if (added) copy_text(plan->mode, sizeof(plan->mode), "gemma4-assisted");
    collect_outputs(plan);
    compute_plan_metrics(plan);
}

static void maybe_call_model(const OrchestrateRequest *req, OrchestratePlan *plan) {
    const char *endpoint = getenv("BONFYRE_ORCHESTRATE_ENDPOINT");
    const char *api_key = getenv("BONFYRE_ORCHESTRATE_API_KEY");
    if (plan->predicted_information_gain < 0.45 || plan->predicted_confidence > 0.78) return;
    if (!endpoint || !endpoint[0] || !shell_safe(endpoint)) return;

    char request_path[] = "/tmp/bonfyre-orchestrate-XXXXXX";
    int fd = mkstemp(request_path);
    if (fd < 0) return;
    FILE *fp = fdopen(fd, "w");
    if (!fp) return;

    fprintf(fp, "{\"model\":\"");
    escape_json(fp, plan->model);
    fprintf(fp, "\",\"temperature\":0.1,\"response_format\":{\"type\":\"json_object\"},\"messages\":[");
    fprintf(fp, "{\"role\":\"system\",\"content\":\"");
    escape_json(fp, SYSTEM_PROMPT);
    fprintf(fp, "\"},{\"role\":\"user\",\"content\":\"request={\\\"input_type\\\":\\\"");
    escape_json(fp, req->input_type);
    fprintf(fp, "\\\",\\\"objective\\\":\\\"");
    escape_json(fp, req->objective);
    fprintf(fp, "\\\",\\\"latency_class\\\":\\\"");
    escape_json(fp, req->latency_class);
    fprintf(fp, "\\\",\\\"surface\\\":\\\"");
    escape_json(fp, req->surface);
    fprintf(fp, "\\\"},operators=");
    write_registry(fp);
    fprintf(fp, "\"}]}");
    fclose(fp);

    char cmd[4096];
    if (api_key && api_key[0] && shell_safe(api_key)) {
        snprintf(cmd, sizeof(cmd),
                 "curl -sS -X POST '%s' -H 'Content-Type: application/json' -H 'Authorization: Bearer %s' --data-binary @%s",
                 endpoint, api_key, request_path);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "curl -sS -X POST '%s' -H 'Content-Type: application/json' --data-binary @%s",
                 endpoint, request_path);
    }

    FILE *pipe = popen(cmd, "r");
    unlink(request_path);
    if (!pipe) return;
    char *response = slurp(pipe);
    pclose(pipe);
    if (response) {
        adopt_model_boosters(plan, response);
        free(response);
    }
}

static void print_plan(const OrchestrateRequest *req, const OrchestratePlan *plan) {
    printf("{\n");
    printf("  \"mode\": \"%s\",\n", plan->mode);
    printf("  \"model\": \"%s\",\n", plan->model);
    printf("  \"input_type\": \"%s\",\n", req->input_type);
    printf("  \"objective\": \"%s\",\n", req->objective);
    printf("  \"latency_class\": \"%s\",\n", req->latency_class);
    printf("  \"surface\": \"%s\",\n", req->surface);
    printf("  \"selected_binaries\": [");
    for (int i = 0; i < plan->selected_count; ++i) {
        if (i) printf(", ");
        printf("\"%s\"", BF_OPERATORS[plan->selected[i]].binary);
    }
    printf("],\n");
    printf("  \"booster_binaries\": [");
    for (int i = 0; i < plan->booster_count; ++i) {
        if (i) printf(", ");
        printf("\"%s\"", BF_OPERATORS[plan->boosters[i]].binary);
    }
    printf("],\n");
    printf("  \"control_surfaces\": [");
    for (int i = 0; i < plan->surface_count; ++i) {
        if (i) printf(", ");
        printf("\"%s\"", plan->surfaces[i]);
    }
    printf("],\n");
    printf("  \"expected_outputs\": [");
    for (int i = 0; i < plan->output_count; ++i) {
        if (i) printf(", ");
        printf("\"%s\"", plan->outputs[i]);
    }
    printf("],\n");
    printf("  \"predicted_cost\": %.3f,\n", plan->predicted_cost);
    printf("  \"predicted_latency\": %.3f,\n", plan->predicted_latency);
    printf("  \"predicted_confidence\": %.3f,\n", plan->predicted_confidence);
    printf("  \"predicted_reversibility\": %.3f,\n", plan->predicted_reversibility);
    printf("  \"predicted_utility\": %.3f,\n", plan->predicted_utility);
    printf("  \"predicted_information_gain\": %.3f\n", plan->predicted_information_gain);
    printf("}\n");
}

static int command_status(void) {
    const char *endpoint = getenv("BONFYRE_ORCHESTRATE_ENDPOINT");
    const char *model = getenv("BONFYRE_ORCHESTRATE_MODEL");
    printf("{\"status\":\"ok\",\"binary\":\"bonfyre-orchestrate\",\"operators\":%d,"
           "\"endpoint_configured\":%s,\"model\":\"%s\",\"machine_only\":true,\"human_prompting\":false}\n",
           BF_OPERATOR_COUNT,
           (endpoint && endpoint[0]) ? "true" : "false",
           (model && model[0]) ? model : DEFAULT_MODEL);
    return 0;
}

static int command_plan(const char *path) {
    OrchestrateRequest req;
    if (load_request(path, &req) != 0) {
        fprintf(stderr, "Failed to read request file: %s\n", path);
        return 1;
    }
    OrchestratePlan plan;
    init_plan(&plan, getenv("BONFYRE_ORCHESTRATE_MODEL"));
    heuristic_plan(&req, &plan);
    if (!load_policy_memory(&req, &plan)) {
        maybe_call_model(&req, &plan);
    }
    save_policy_memory(&req, &plan);
    print_plan(&req, &plan);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        usage();
        return 1;
    }
    if (strcmp(argv[1], "status") == 0) return command_status();
    if (strcmp(argv[1], "plan") == 0 && argc >= 3) return command_plan(argv[2]);
    if (strcmp(argv[1], "feedback") == 0 && argc >= 5) return command_feedback(argv[2], argv[3], argv[4]);
    if (strcmp(argv[1], "help") == 0 || strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
        usage();
        return 0;
    }
    usage();
    return 1;
}
