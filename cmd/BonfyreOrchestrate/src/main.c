#define _POSIX_C_SOURCE 200809L
#include "bonfyre.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#define MAX_PLAN_STEPS 64
#define MAX_OUTPUTS 128
#define EXCERPT_CHARS 640

typedef struct {
    char input_type[64];
    char objective[128];
    char latency_class[64];
    char surface[128];
    char artifact_path[PATH_MAX];
} OrchestrateRequest;

typedef struct {
    int selected[MAX_PLAN_STEPS];
    int selected_count;
    int boosters[MAX_PLAN_STEPS];
    int booster_count;
    const char *outputs[MAX_OUTPUTS];
    int output_count;
    const char *control_surfaces[MAX_OUTPUTS];
    int control_surface_count;
    char mode[32];
    char model[128];
    char response_excerpt[EXCERPT_CHARS];
} OrchestratePlan;

static const char *DEFAULT_MODEL = "google/gemma-4-E4B";
static const char *HIDDEN_SYSTEM_PROMPT =
    "You are Bonfyre Orchestrate, a machine-only orchestration planner. "
    "There is no end-user prompting. "
    "Your job is to improve Bonfyre pipelines by choosing the smallest set of extra Bonfyre operators "
    "that materially increase output quality, coverage, and leverage while respecting latency. "
    "Never invent operators. "
    "Use only operators present in the registry. "
    "Prefer optional boost paths over replacing deterministic fast paths. "
    "Return strict JSON with keys selected_binaries, booster_binaries, control_surfaces, expected_outputs, rationale.";

static void print_usage(void) {
    fprintf(stderr,
            "bonfyre-orchestrate\n\n"
            "Usage:\n"
            "  bonfyre-orchestrate status\n"
            "  bonfyre-orchestrate surfaces [--json]\n"
            "  bonfyre-orchestrate template [audio|text|artifact]\n"
            "  bonfyre-orchestrate plan <request.json> [--endpoint URL] [--model NAME] [--out FILE]\n\n"
            "Environment:\n"
            "  BONFYRE_ORCHESTRATE_ENDPOINT  OpenAI-compatible endpoint serving Gemma 4\n"
            "  BONFYRE_ORCHESTRATE_MODEL     Model name (default: google/gemma-4-E4B)\n"
            "  BONFYRE_ORCHESTRATE_API_KEY   Optional bearer token\n");
}

static int command_exists(const char *name) {
    const char *path = getenv("PATH");
    if (!path || !name || !name[0]) return 0;
    char *copy = strdup(path);
    if (!copy) return 0;
    int found = 0;
    for (char *save = NULL, *dir = strtok_r(copy, ":", &save); dir; dir = strtok_r(NULL, ":", &save)) {
        char candidate[PATH_MAX];
        snprintf(candidate, sizeof(candidate), "%s/%s", dir, name);
        if (access(candidate, X_OK) == 0) {
            found = 1;
            break;
        }
    }
    free(copy);
    return found;
}

static int ci_contains(const char *haystack, const char *needle) {
    if (!haystack || !needle || !needle[0]) return 0;
    size_t nlen = strlen(needle);
    for (const char *p = haystack; *p; ++p) {
        if (strncasecmp(p, needle, nlen) == 0) return 1;
    }
    return 0;
}

static void copy_token(char *dst, size_t dst_sz, const char *src) {
    if (!dst || dst_sz == 0) return;
    if (!src) {
        dst[0] = '\0';
        return;
    }
    snprintf(dst, dst_sz, "%s", src);
}

static int json_find_string(const char *json, const char *key, char *dst, size_t dst_sz) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++;
    if (*p != ':') return 0;
    p++;
    while (*p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++;
    if (*p != '"') return 0;
    p++;
    size_t j = 0;
    while (*p && *p != '"' && j + 1 < dst_sz) {
        if (*p == '\\' && p[1]) {
            p++;
            if (*p == 'n') dst[j++] = '\n';
            else dst[j++] = *p;
        } else {
            dst[j++] = *p;
        }
        p++;
    }
    dst[j] = '\0';
    return j > 0;
}

static void infer_request_defaults(OrchestrateRequest *request) {
    if (!request->input_type[0]) {
        if (ci_contains(request->artifact_path, ".wav") || ci_contains(request->artifact_path, ".mp3") ||
            ci_contains(request->artifact_path, ".m4a") || ci_contains(request->artifact_path, ".flac")) {
            copy_token(request->input_type, sizeof(request->input_type), "audio");
        } else if (ci_contains(request->artifact_path, "artifact.json")) {
            copy_token(request->input_type, sizeof(request->input_type), "artifact");
        } else {
            copy_token(request->input_type, sizeof(request->input_type), "text");
        }
    }
    if (!request->objective[0]) {
        copy_token(request->objective, sizeof(request->objective), "boost-bonfyre-flow");
    }
    if (!request->latency_class[0]) {
        copy_token(request->latency_class, sizeof(request->latency_class), "interactive");
    }
    if (!request->surface[0]) {
        copy_token(request->surface, sizeof(request->surface), "pages");
    }
}

static int load_request(const char *path, OrchestrateRequest *request) {
    memset(request, 0, sizeof(*request));
    copy_token(request->artifact_path, sizeof(request->artifact_path), path);
    char *json = bf_read_file(path, NULL);
    if (!json) return 1;
    json_find_string(json, "input_type", request->input_type, sizeof(request->input_type));
    json_find_string(json, "objective", request->objective, sizeof(request->objective));
    json_find_string(json, "latency_class", request->latency_class, sizeof(request->latency_class));
    json_find_string(json, "surface", request->surface, sizeof(request->surface));
    json_find_string(json, "artifact_path", request->artifact_path, sizeof(request->artifact_path));
    free(json);
    infer_request_defaults(request);
    return 0;
}

static void plan_init(OrchestratePlan *plan, const char *model) {
    memset(plan, 0, sizeof(*plan));
    copy_token(plan->mode, sizeof(plan->mode), "heuristic");
    copy_token(plan->model, sizeof(plan->model), model && model[0] ? model : DEFAULT_MODEL);
}

static int find_operator_index(const char *name_or_binary) {
    const BfOperator *op = bf_operator_find(name_or_binary);
    if (!op) op = bf_operator_find_by_name(name_or_binary);
    if (!op) return -1;
    return (int)(op - BF_OPERATORS);
}

static int plan_has(const int *items, int count, int idx) {
    for (int i = 0; i < count; ++i) {
        if (items[i] == idx) return 1;
    }
    return 0;
}

static void plan_add_selected(OrchestratePlan *plan, const char *name_or_binary) {
    int idx = find_operator_index(name_or_binary);
    if (idx < 0 || plan->selected_count >= MAX_PLAN_STEPS || plan_has(plan->selected, plan->selected_count, idx)) return;
    plan->selected[plan->selected_count++] = idx;
}

static void plan_add_booster(OrchestratePlan *plan, const char *name_or_binary) {
    int idx = find_operator_index(name_or_binary);
    if (idx < 0 || plan->booster_count >= MAX_PLAN_STEPS ||
        plan_has(plan->selected, plan->selected_count, idx) ||
        plan_has(plan->boosters, plan->booster_count, idx)) return;
    plan->boosters[plan->booster_count++] = idx;
}

static void plan_add_surface(OrchestratePlan *plan, const char *surface) {
    if (!surface || !surface[0] || plan->control_surface_count >= MAX_OUTPUTS) return;
    for (int i = 0; i < plan->control_surface_count; ++i) {
        if (strcmp(plan->control_surfaces[i], surface) == 0) return;
    }
    plan->control_surfaces[plan->control_surface_count++] = surface;
}

static void plan_collect_outputs(OrchestratePlan *plan) {
    plan->output_count = 0;
    for (int i = 0; i < plan->selected_count; ++i) {
        const BfOperator *op = &BF_OPERATORS[plan->selected[i]];
        for (int j = 0; j < BF_MAX_TYPES && op->output_types[j]; ++j) {
            const char *output = op->output_types[j];
            int dup = 0;
            for (int k = 0; k < plan->output_count; ++k) {
                if (strcmp(plan->outputs[k], output) == 0) {
                    dup = 1;
                    break;
                }
            }
            if (!dup && plan->output_count < MAX_OUTPUTS) {
                plan->outputs[plan->output_count++] = output;
            }
        }
    }
}

static int latency_is_fast(const OrchestrateRequest *request) {
    return ci_contains(request->latency_class, "fast") ||
           ci_contains(request->latency_class, "interactive") ||
           ci_contains(request->latency_class, "realtime");
}

static void build_heuristic_plan(const OrchestrateRequest *request, OrchestratePlan *plan) {
    int fast = latency_is_fast(request);
    if (ci_contains(request->input_type, "audio")) {
        plan_add_selected(plan, "ingest");
        plan_add_selected(plan, "media-prep");
        plan_add_selected(plan, "transcribe");
        plan_add_selected(plan, fast ? "brief" : "transcript-clean");
        if (!fast) {
            plan_add_selected(plan, "paragraph");
            plan_add_selected(plan, "brief");
        } else {
            plan_add_booster(plan, "transcript-clean");
            plan_add_booster(plan, "paragraph");
        }
        plan_add_booster(plan, "proof");
        plan_add_booster(plan, "tag");
    } else if (ci_contains(request->input_type, "artifact")) {
        plan_add_selected(plan, "hash");
        plan_add_selected(plan, "canon");
        plan_add_selected(plan, "render");
        plan_add_booster(plan, "query");
        plan_add_booster(plan, "graph");
    } else {
        plan_add_selected(plan, "ingest");
        plan_add_selected(plan, "canon");
        plan_add_selected(plan, "brief");
        plan_add_booster(plan, "tag");
        plan_add_booster(plan, "render");
    }

    if (ci_contains(request->objective, "podcast") || ci_contains(request->objective, "release") ||
        ci_contains(request->objective, "publish") || ci_contains(request->objective, "radio") ||
        ci_contains(request->objective, "newsletter")) {
        plan_add_booster(plan, "narrate");
        plan_add_booster(plan, "clips");
        plan_add_booster(plan, "render");
        plan_add_booster(plan, "emit");
        plan_add_booster(plan, "pack");
        plan_add_booster(plan, "distribute");
    }

    if (ci_contains(request->objective, "memory") || ci_contains(request->objective, "search") ||
        ci_contains(request->objective, "semantic") || ci_contains(request->objective, "atlas") ||
        ci_contains(request->objective, "repo") || ci_contains(request->objective, "cockpit") ||
        ci_contains(request->objective, "town") || ci_contains(request->objective, "civic")) {
        plan_add_booster(plan, "embed");
        plan_add_booster(plan, "index");
        plan_add_booster(plan, "vec");
        plan_add_booster(plan, "query");
        plan_add_booster(plan, "graph");
    }

    if (ci_contains(request->objective, "legal") || ci_contains(request->objective, "evidence") ||
        ci_contains(request->objective, "sales") || ci_contains(request->objective, "grant") ||
        ci_contains(request->objective, "procurement") || ci_contains(request->objective, "consult")) {
        plan_add_booster(plan, "proof");
        plan_add_booster(plan, "offer");
        plan_add_booster(plan, "ledger");
        plan_add_booster(plan, "gate");
        plan_add_booster(plan, "meter");
    }

    if (ci_contains(request->objective, "call") || ci_contains(request->objective, "handoff") ||
        ci_contains(request->objective, "live") || ci_contains(request->objective, "shift")) {
        plan_add_booster(plan, "segment");
        plan_add_booster(plan, "speechloop");
        plan_add_booster(plan, "tone");
    }

    if (ci_contains(request->surface, "pages")) {
        plan_add_surface(plan, "bonfyre-render");
        plan_add_surface(plan, "bonfyre-emit");
    }
    if (ci_contains(request->surface, "api") || ci_contains(request->surface, "backend")) {
        plan_add_surface(plan, "bonfyre-api");
        plan_add_surface(plan, "bonfyre-auth");
    }
    if (ci_contains(request->surface, "jobs") || ci_contains(request->surface, "actions") ||
        ci_contains(request->surface, "queue")) {
        plan_add_surface(plan, "bonfyre-queue");
        plan_add_surface(plan, "bonfyre-runtime");
        plan_add_surface(plan, "bonfyre-sync");
    }

    if (plan->control_surface_count == 0) {
        plan_add_surface(plan, "bonfyre-runtime");
        plan_add_surface(plan, "bonfyre-queue");
    }
    plan_collect_outputs(plan);
}

static void json_escape(FILE *fp, const char *text) {
    for (const char *p = text ? text : ""; *p; ++p) {
        switch (*p) {
            case '\\': fputs("\\\\", fp); break;
            case '"':  fputs("\\\"", fp); break;
            case '\n': fputs("\\n", fp); break;
            case '\r': fputs("\\r", fp); break;
            case '\t': fputs("\\t", fp); break;
            default: fputc(*p, fp); break;
        }
    }
}

static void write_registry_json(FILE *fp) {
    fputc('[', fp);
    for (int i = 0; i < BF_OPERATOR_COUNT; ++i) {
        const BfOperator *op = &BF_OPERATORS[i];
        if (i) fputc(',', fp);
        fprintf(fp, "{\"name\":\"");
        json_escape(fp, op->name);
        fprintf(fp, "\",\"binary\":\"");
        json_escape(fp, op->binary);
        fprintf(fp, "\",\"layer\":\"");
        json_escape(fp, op->layer);
        fprintf(fp, "\",\"group\":\"");
        json_escape(fp, op->group);
        fprintf(fp, "\",\"inputs\":[");
        int first = 1;
        for (int j = 0; j < BF_MAX_TYPES && op->input_types[j]; ++j) {
            if (!first) fputc(',', fp);
            fprintf(fp, "\"");
            json_escape(fp, op->input_types[j]);
            fprintf(fp, "\"");
            first = 0;
        }
        fprintf(fp, "],\"outputs\":[");
        first = 1;
        for (int j = 0; j < BF_MAX_TYPES && op->output_types[j]; ++j) {
            if (!first) fputc(',', fp);
            fprintf(fp, "\"");
            json_escape(fp, op->output_types[j]);
            fprintf(fp, "\"");
            first = 0;
        }
        fprintf(fp, "]}");
    }
    fputc(']', fp);
}

static char *read_stream(FILE *fp) {
    size_t cap = 8192;
    size_t len = 0;
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

static void extract_content_string(const char *json, char *dst, size_t dst_sz) {
    dst[0] = '\0';
    const char *p = strstr(json, "\"content\"");
    if (!p) return;
    p = strchr(p, ':');
    if (!p) return;
    p++;
    while (*p && isspace((unsigned char)*p)) p++;
    if (*p != '"') return;
    p++;
    size_t j = 0;
    while (*p && j + 1 < dst_sz) {
        if (*p == '"' ) break;
        if (*p == '\\' && p[1]) {
            p++;
            switch (*p) {
                case 'n': dst[j++] = '\n'; break;
                case 'r': dst[j++] = '\r'; break;
                case 't': dst[j++] = '\t'; break;
                case '"': dst[j++] = '"'; break;
                case '\\': dst[j++] = '\\'; break;
                default: dst[j++] = *p; break;
            }
            p++;
            continue;
        }
        dst[j++] = *p++;
    }
    dst[j] = '\0';
}

static int call_model(const OrchestrateRequest *request, OrchestratePlan *plan, const char *endpoint, const char *api_key) {
    if (!endpoint || !endpoint[0] || !command_exists("curl")) return 0;

    char req_path[] = "/tmp/bonfyre-orchestrate-req-XXXXXX";
    int fd = mkstemp(req_path);
    if (fd < 0) return 0;
    FILE *req_fp = fdopen(fd, "w");
    if (!req_fp) {
        close(fd);
        unlink(req_path);
        return 0;
    }

    fprintf(req_fp, "{\"model\":\"");
    json_escape(req_fp, plan->model);
    fprintf(req_fp, "\",\"temperature\":0.1,\"response_format\":{\"type\":\"json_object\"},\"messages\":[");
    fprintf(req_fp, "{\"role\":\"system\",\"content\":\"");
    json_escape(req_fp, HIDDEN_SYSTEM_PROMPT);
    fprintf(req_fp, "\"},");
    fprintf(req_fp, "{\"role\":\"user\",\"content\":\"");
    json_escape(req_fp, "request=");
    fprintf(req_fp, "{\\\"input_type\\\":\\\"");
    json_escape(req_fp, request->input_type);
    fprintf(req_fp, "\\\",\\\"objective\\\":\\\"");
    json_escape(req_fp, request->objective);
    fprintf(req_fp, "\\\",\\\"latency_class\\\":\\\"");
    json_escape(req_fp, request->latency_class);
    fprintf(req_fp, "\\\",\\\"surface\\\":\\\"");
    json_escape(req_fp, request->surface);
    fprintf(req_fp, "\\\"}, operators=");
    write_registry_json(req_fp);
    json_escape(req_fp, ", heuristic_selected=");
    fputc('[', req_fp);
    for (int i = 0; i < plan->selected_count; ++i) {
        if (i) fputc(',', req_fp);
        fprintf(req_fp, "\\\"");
        json_escape(req_fp, BF_OPERATORS[plan->selected[i]].binary);
        fprintf(req_fp, "\\\"");
    }
    fputc(']', req_fp);
    fprintf(req_fp, "\"}]}");
    fclose(req_fp);

    char cmd[4096];
    if (api_key && api_key[0]) {
        snprintf(cmd, sizeof(cmd),
                 "curl -sS -X POST '%s' -H 'Content-Type: application/json' -H 'Authorization: Bearer %s' --data-binary @%s",
                 endpoint, api_key, req_path);
    } else {
        snprintf(cmd, sizeof(cmd),
                 "curl -sS -X POST '%s' -H 'Content-Type: application/json' --data-binary @%s",
                 endpoint, req_path);
    }

    FILE *pipe = popen(cmd, "r");
    unlink(req_path);
    if (!pipe) return 0;
    char *response = read_stream(pipe);
    int rc = pclose(pipe);
    if (!response || rc != 0) {
        free(response);
        return 0;
    }

    char content[8192];
    extract_content_string(response, content, sizeof(content));
    if (!content[0]) {
        copy_token(content, sizeof(content), response);
    }

    copy_token(plan->response_excerpt, sizeof(plan->response_excerpt), content);
    int adopted = 0;
    for (int i = 0; i < BF_OPERATOR_COUNT; ++i) {
        const BfOperator *op = &BF_OPERATORS[i];
        if (ci_contains(content, op->binary) || ci_contains(content, op->name)) {
            if (!plan_has(plan->selected, plan->selected_count, i) && !plan_has(plan->boosters, plan->booster_count, i)) {
                plan_add_booster(plan, op->binary);
                adopted = 1;
            }
        }
    }
    if (adopted) copy_token(plan->mode, sizeof(plan->mode), "gemma4-assisted");
    free(response);
    plan_collect_outputs(plan);
    return adopted;
}

static void print_plan_json(FILE *fp, const OrchestrateRequest *request, const OrchestratePlan *plan) {
    fprintf(fp, "{\n");
    fprintf(fp, "  \"mode\": \"%s\",\n", plan->mode);
    fprintf(fp, "  \"model\": \"%s\",\n", plan->model);
    fprintf(fp, "  \"input_type\": \"%s\",\n", request->input_type);
    fprintf(fp, "  \"objective\": \"%s\",\n", request->objective);
    fprintf(fp, "  \"latency_class\": \"%s\",\n", request->latency_class);
    fprintf(fp, "  \"surface\": \"%s\",\n", request->surface);
    fprintf(fp, "  \"selected_binaries\": [");
    for (int i = 0; i < plan->selected_count; ++i) {
        if (i) fprintf(fp, ", ");
        fprintf(fp, "\"%s\"", BF_OPERATORS[plan->selected[i]].binary);
    }
    fprintf(fp, "],\n");
    fprintf(fp, "  \"booster_binaries\": [");
    for (int i = 0; i < plan->booster_count; ++i) {
        if (i) fprintf(fp, ", ");
        fprintf(fp, "\"%s\"", BF_OPERATORS[plan->boosters[i]].binary);
    }
    fprintf(fp, "],\n");
    fprintf(fp, "  \"control_surfaces\": [");
    for (int i = 0; i < plan->control_surface_count; ++i) {
        if (i) fprintf(fp, ", ");
        fprintf(fp, "\"%s\"", plan->control_surfaces[i]);
    }
    fprintf(fp, "],\n");
    fprintf(fp, "  \"expected_outputs\": [");
    for (int i = 0; i < plan->output_count; ++i) {
        if (i) fprintf(fp, ", ");
        fprintf(fp, "\"%s\"", plan->outputs[i]);
    }
    fprintf(fp, "]");
    if (plan->response_excerpt[0]) {
        fprintf(fp, ",\n  \"gemma_excerpt\": \"");
        json_escape(fp, plan->response_excerpt);
        fprintf(fp, "\"\n");
    } else {
        fprintf(fp, "\n");
    }
    fprintf(fp, "}\n");
}

static int command_status(void) {
    const char *endpoint = getenv("BONFYRE_ORCHESTRATE_ENDPOINT");
    const char *model = getenv("BONFYRE_ORCHESTRATE_MODEL");
    printf("{\"status\":\"ok\",\"binary\":\"bonfyre-orchestrate\",\"operators\":%d,"
           "\"curl\":%s,\"endpoint_configured\":%s,\"model\":\"%s\","
           "\"machine_only\":true,\"human_prompting\":false}\n",
           BF_OPERATOR_COUNT,
           command_exists("curl") ? "true" : "false",
           (endpoint && endpoint[0]) ? "true" : "false",
           (model && model[0]) ? model : DEFAULT_MODEL);
    return 0;
}

static int command_surfaces(int json_mode) {
    if (json_mode) {
        write_registry_json(stdout);
        fputc('\n', stdout);
        return 0;
    }
    printf("Bonfyre control surfaces\n\n");
    for (int i = 0; i < BF_OPERATOR_COUNT; ++i) {
        const BfOperator *op = &BF_OPERATORS[i];
        printf("  %-22s %-11s %-12s %s\n", op->binary, op->layer, op->group, op->description);
    }
    return 0;
}

static int command_template(const char *profile) {
    const char *input = "audio";
    const char *objective = "publishable-multi-output";
    const char *surface = "pages+jobs";
    if (profile && strcmp(profile, "text") == 0) {
        input = "text";
        objective = "semantic-memory";
        surface = "pages+api";
    } else if (profile && strcmp(profile, "artifact") == 0) {
        input = "artifact";
        objective = "artifact-analytics";
        surface = "api+jobs";
    }
    printf("{\n"
           "  \"input_type\": \"%s\",\n"
           "  \"objective\": \"%s\",\n"
           "  \"latency_class\": \"interactive\",\n"
           "  \"surface\": \"%s\",\n"
           "  \"artifact_path\": \"site/demos/input/example\"\n"
           "}\n",
           input, objective, surface);
    return 0;
}

static int command_plan(int argc, char **argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }
    const char *request_path = argv[2];
    const char *endpoint = getenv("BONFYRE_ORCHESTRATE_ENDPOINT");
    const char *model = getenv("BONFYRE_ORCHESTRATE_MODEL");
    const char *api_key = getenv("BONFYRE_ORCHESTRATE_API_KEY");
    const char *out_path = NULL;

    for (int i = 3; i < argc; ++i) {
        if (strcmp(argv[i], "--endpoint") == 0 && i + 1 < argc) endpoint = argv[++i];
        else if (strcmp(argv[i], "--model") == 0 && i + 1 < argc) model = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_path = argv[++i];
        else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            return 1;
        }
    }

    OrchestrateRequest request;
    if (load_request(request_path, &request) != 0) {
        fprintf(stderr, "Failed to read request file: %s\n", request_path);
        return 1;
    }

    OrchestratePlan plan;
    plan_init(&plan, model);
    build_heuristic_plan(&request, &plan);
    call_model(&request, &plan, endpoint, api_key);

    FILE *out = stdout;
    if (out_path) {
        out = fopen(out_path, "w");
        if (!out) {
            fprintf(stderr, "Failed to open %s: %s\n", out_path, strerror(errno));
            return 1;
        }
    }
    print_plan_json(out, &request, &plan);
    if (out_path) fclose(out);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    if (strcmp(argv[1], "status") == 0) return command_status();
    if (strcmp(argv[1], "surfaces") == 0) {
        int json_mode = (argc > 2 && strcmp(argv[2], "--json") == 0);
        return command_surfaces(json_mode);
    }
    if (strcmp(argv[1], "template") == 0) {
        const char *profile = argc > 2 ? argv[2] : "audio";
        return command_template(profile);
    }
    if (strcmp(argv[1], "plan") == 0) return command_plan(argc, argv);
    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "help") == 0) {
        print_usage();
        return 0;
    }
    fprintf(stderr, "Unknown command: %s\n", argv[1]);
    print_usage();
    return 1;
}
