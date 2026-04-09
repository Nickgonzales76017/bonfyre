#define _POSIX_C_SOURCE 200809L
#include "bonfyre.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

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
} OrchestratePlan;

static const char *DEFAULT_MODEL = "google/gemma-4-E4B";
static const char *SYSTEM_PROMPT =
    "Bonfyre Orchestrate. Machine-only. No user prompting. "
    "Choose the smallest Bonfyre boost set that improves quality without slowing the fast path. "
    "Only return JSON with keys selected_binaries and booster_binaries.";

static void usage(void) {
    fprintf(stderr,
            "bonfyre-orchestrate\n\n"
            "Usage:\n"
            "  bonfyre-orchestrate status\n"
            "  bonfyre-orchestrate plan <request.json>\n\n"
            "Environment:\n"
            "  BONFYRE_ORCHESTRATE_ENDPOINT  OpenAI-compatible Gemma endpoint\n"
            "  BONFYRE_ORCHESTRATE_MODEL     Model name (default: google/gemma-4-E4B)\n"
            "  BONFYRE_ORCHESTRATE_API_KEY   Optional bearer token\n");
}

static void copy_text(char *dst, size_t dst_sz, const char *src) {
    if (!dst || dst_sz == 0) return;
    snprintf(dst, dst_sz, "%s", src ? src : "");
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
}

static void maybe_call_model(const OrchestrateRequest *req, OrchestratePlan *plan) {
    const char *endpoint = getenv("BONFYRE_ORCHESTRATE_ENDPOINT");
    const char *api_key = getenv("BONFYRE_ORCHESTRATE_API_KEY");
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
    printf("]\n}\n");
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
    maybe_call_model(&req, &plan);
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
    if (strcmp(argv[1], "help") == 0 || strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
        usage();
        return 0;
    }
    usage();
    return 1;
}
