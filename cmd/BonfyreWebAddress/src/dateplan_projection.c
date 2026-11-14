/* AI date-plan generation, verification and history. Prompt and plan
 * contract derive from start-again-06/AI-Integrated-Dating-App (MIT,
 * Copyright 2026 Anjan Mahapatra); native additions are cost admission,
 * mutual-gated companions, persisted acceptance and hashed receipts. */
#define _POSIX_C_SOURCE 200809L
#include "dateplan_projection.h"
#include "bf_json.h"
#include "bf_jsonw.h"
#include "form_tree.h"
#include "bf_lex.h"
#include "bonfyre.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define DATEPLAN_TIMEOUT_MS 120000
#define DATEPLAN_MODEL_ENV "BONFYRE_DATEPLAN_MODEL"
#define DATEPLAN_QWEN_ENV "BONFYRE_QWEN_BINARY"
#define DATEPLAN_TOKENIZER_ENV "BONFYRE_DATEPLAN_TOKENIZER"

static const char *dateplan_prompt_template =
    "You are an AI date planning assistant.\n"
    "\n"
    "User details:\n"
    "- Location: %s\n"
    "- Dating intent: %s\n"
    "- Budget: %ld to %ld\n"
    "- Interests: %s\n"
    "\n"
    "Rules:\n"
    "- First date only\n"
    "- Public and safe venues\n"
    "- Suitable for women & LGBTQ+ users\n"
    "- Budget must be respected\n"
    "- Every plan states its own cost range in the same currency units\n"
    "\n"
    "Return STRICT JSON in the following format:\n"
    "\n"
    "{\n"
    "  \"plans\": [\n"
    "    {\n"
    "      \"title\": \"\",\n"
    "      \"venue\": \"\",\n"
    "      \"time\": \"\",\n"
    "      \"reason\": \"\",\n"
    "      \"safety_note\": \"\",\n"
    "      \"cost_min\": 0,\n"
    "      \"cost_max\": 0\n"
    "    }\n"
    "  ],\n"
    "  \"backup_plan\": {\n"
    "    \"title\": \"\",\n"
    "    \"venue\": \"\",\n"
    "    \"time\": \"\",\n"
    "    \"reason\": \"\"\n"
    "  }\n"
    "}\n";

static int bounded_text(const char *s, size_t max) {
    size_t n;
    if (!s || !(n = strlen(s)) || n > max) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c < 32 && c != '\n' && c != '\t') return 0;
        if (c == 127) return 0;
    }
    return 1;
}

int bf_dateplan_prompt(const char *location, const char *intent, long budget_min,
                       long budget_max, const char *interests,
                       char *out, size_t cap, char sha_hex[65]) {
    int n;
    if (!bounded_text(location, 120) || !bounded_text(intent, 120) ||
        budget_min < 0 || budget_max < 0 || budget_min > budget_max ||
        budget_max > 100000 || (interests && strlen(interests) > 500) ||
        !out || cap < 1024 || !sha_hex) return -1;
    n = snprintf(out, cap, dateplan_prompt_template, location, intent,
                 budget_min, budget_max, interests && *interests ? interests : "none stated");
    if (n < 0 || (size_t)n >= cap) return -1;
    bf_sha256_hex((const uint8_t *)out, (size_t)n, sha_hex);
    return 0;
}

typedef struct {
    char title[221], venue[221], time[121], reason[1001], safety[501];
    long cost_min, cost_max;
} DatePlanItem;

static int plan_string(bf_json_doc_t *doc, const bf_json_node_t *obj, const char *key,
                       char *out, size_t cap, size_t max) {
    const bf_json_node_t *node = bf_json_obj_get(doc, obj, key);
    int len = 0;
    const char *raw;
    char *plain = NULL;
    size_t bytes = 0;
    if (!node || node->type != BF_JSON_STRING) return 0;
    raw = bf_json_get_str(node, &len);
    if (!raw || len <= 0) return 0;
    plain = bf_json_unescape(raw, len, &bytes);
    if (!plain || !bytes || bytes > max || strlen(plain) != bytes) {
        free(plain);
        return 0;
    }
    for (size_t i = 0; i < bytes; i++) {
        unsigned char c = (unsigned char)plain[i];
        if ((c < 32 && c != '\n' && c != '\t') || c == 127) {
            free(plain);
            return 0;
        }
    }
    snprintf(out, cap, "%s", plain);
    free(plain);
    return 1;
}

static int plan_number(bf_json_doc_t *doc, const bf_json_node_t *obj, const char *key,
                       long *out) {
    const bf_json_node_t *node = bf_json_obj_get(doc, obj, key);
    double d;
    if (!node) return 0;
    if (node->type == BF_JSON_INT) d = (double)bf_json_get_int(node);
    else if (node->type == BF_JSON_DOUBLE) d = bf_json_get_double(node);
    else return 0;
    if (!(d >= 0) || d > 100000) return 0;
    *out = (long)d;
    return 1;
}

static int plan_item(bf_json_doc_t *doc, const bf_json_node_t *node, DatePlanItem *item,
                     int needs_safety) {
    if (!node || node->type != BF_JSON_OBJECT) return 0;
    memset(item, 0, sizeof(*item));
    if (!plan_string(doc, node, "title", item->title, sizeof(item->title), 220)) return 0;
    if (!plan_string(doc, node, "venue", item->venue, sizeof(item->venue), 220)) return 0;
    if (!plan_string(doc, node, "time", item->time, sizeof(item->time), 120)) return 0;
    if (!plan_string(doc, node, "reason", item->reason, sizeof(item->reason), 1000)) return 0;
    if (needs_safety &&
        !plan_string(doc, node, "safety_note", item->safety, sizeof(item->safety), 500))
        return 0;
    if (needs_safety) {
        if (!plan_number(doc, node, "cost_min", &item->cost_min)) return 0;
        if (!plan_number(doc, node, "cost_max", &item->cost_max)) return 0;
        if (item->cost_min > item->cost_max) return 0;
    }
    return 1;
}

int bf_dateplan_verify(const char *output, long budget_max, const char *model,
                       const char *prompt_sha, BfDatePlanResult *result) {
    bf_json_doc_t *doc = NULL;
    const bf_json_node_t *root, *plans, *backup, *child;
    DatePlanItem items[8], fallback;
    bf_jsonw w;
    int kept = 0, dropped = 0, ok = 0;
    long lo = 0, hi = 0;
    size_t outlen;
    if (!output || (outlen = strlen(output)) == 0 || outlen > BF_DATEPLAN_OUTPUT_CAP ||
        budget_max < 0 || budget_max > 100000 || !model || !*model || strlen(model) > 120 ||
        !prompt_sha || strlen(prompt_sha) != 64 || !result) return -1;
    memset(result, 0, sizeof(*result));
    doc = bf_json_parse_str(output, NULL, 0);
    if (!doc) return 0;
    root = bf_json_root(doc);
    if (!root || root->type != BF_JSON_OBJECT) goto done;
    plans = bf_json_obj_get(doc, root, "plans");
    backup = bf_json_obj_get(doc, root, "backup_plan");
    if (!plans || plans->type != BF_JSON_ARRAY || !backup) goto done;
    if (!plan_item(doc, backup, &fallback, 0)) goto done;
    for (child = bf_json_child_first(doc, plans); child;
         child = bf_json_child_next(doc, child)) {
        DatePlanItem item;
        if (kept >= 8) {
            dropped++;
            continue;
        }
        if (!plan_item(doc, child, &item, 1) || item.cost_min > budget_max) {
            dropped++;
            continue;
        }
        if (!kept || item.cost_min < lo) lo = item.cost_min;
        if (!kept || item.cost_max > hi) hi = item.cost_max;
        items[kept++] = item;
    }
    if (!kept) goto done;
    bf_jsonw_init(&w, 0);
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "plans");
    bf_jsonw_arr(&w);
    for (int i = 0; i < kept; i++) {
        bf_jsonw_obj(&w);
        bf_jsonw_key(&w, "title"); bf_jsonw_str(&w, items[i].title);
        bf_jsonw_key(&w, "venue"); bf_jsonw_str(&w, items[i].venue);
        bf_jsonw_key(&w, "time"); bf_jsonw_str(&w, items[i].time);
        bf_jsonw_key(&w, "reason"); bf_jsonw_str(&w, items[i].reason);
        bf_jsonw_key(&w, "safety_note"); bf_jsonw_str(&w, items[i].safety);
        bf_jsonw_key(&w, "cost_min"); bf_jsonw_int(&w, items[i].cost_min);
        bf_jsonw_key(&w, "cost_max"); bf_jsonw_int(&w, items[i].cost_max);
        bf_jsonw_obj_end(&w);
    }
    bf_jsonw_arr_end(&w);
    bf_jsonw_key(&w, "backup_plan");
    bf_jsonw_obj(&w);
    bf_jsonw_key(&w, "title"); bf_jsonw_str(&w, fallback.title);
    bf_jsonw_key(&w, "venue"); bf_jsonw_str(&w, fallback.venue);
    bf_jsonw_key(&w, "time"); bf_jsonw_str(&w, fallback.time);
    bf_jsonw_key(&w, "reason"); bf_jsonw_str(&w, fallback.reason);
    bf_jsonw_obj_end(&w);
    bf_jsonw_obj_end(&w);
    if (!w.oom && w.buf && strlen(w.buf) < sizeof(result->admitted)) {
        snprintf(result->admitted, sizeof(result->admitted), "%s", w.buf);
        snprintf(result->model, sizeof(result->model), "%s", model);
        snprintf(result->prompt_sha256, sizeof(result->prompt_sha256), "%s", prompt_sha);
        bf_sha256_hex((const uint8_t *)output, outlen, result->output_sha256);
        result->output_bytes = (long)outlen;
        result->admitted_min = lo;
        result->admitted_max = hi;
        result->plans_admitted = kept;
        result->plans_dropped = dropped;
        ok = 1;
    }
    bf_jsonw_free(&w);
done:
    bf_json_free(doc);
    return ok;
}

static long now_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return -1;
    return (long)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

static int run_bounded(char *const argv[], long timeout_ms, long *elapsed_ms,
                         const char *err_path) {
    pid_t pid;
    long start = now_ms();
    int status = 0, done = 0;
    struct timespec nap = {0, 50000000};
    if (start < 0) return -1;
    pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        int errfd = err_path ? open(err_path, O_WRONLY | O_CREAT | O_TRUNC, 0600) : -1;
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            if (devnull > STDERR_FILENO) close(devnull);
        }
        if (errfd >= 0) {
            dup2(errfd, STDERR_FILENO);
            if (errfd > STDERR_FILENO) close(errfd);
        } else if (devnull >= 0) {
            dup2(devnull, STDERR_FILENO);
        }
        execvp(argv[0], argv);
        _exit(127);
    }
    while (!done) {
        pid_t got = waitpid(pid, &status, WNOHANG);
        if (got == pid) {
            done = 1;
        } else if (got < 0 && errno != EINTR) {
            kill(pid, SIGKILL);
            waitpid(pid, &status, 0);
            return -1;
        } else {
            long now = now_ms();
            if (now < 0 || now - start > timeout_ms) {
                kill(pid, SIGKILL);
                waitpid(pid, &status, 0);
                if (elapsed_ms) *elapsed_ms = timeout_ms;
                return -2;
            }
            nanosleep(&nap, NULL);
        }
    }
    if (elapsed_ms) {
        long now = now_ms();
        *elapsed_ms = now >= start ? now - start : 0;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return -1;
    return 0;
}

static char *read_bounded_file(const char *path, size_t cap, size_t *len_out) {
    FILE *f;
    char *buf;
    size_t n;
    long size;
    if (!path || !len_out) return NULL;
    f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) < 0 || (size_t)size > cap) {
        fclose(f);
        return NULL;
    }
    rewind(f);
    buf = malloc((size_t)size + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    n = fread(buf, 1, (size_t)size, f);
    fclose(f);
    if (n != (size_t)size) {
        free(buf);
        return NULL;
    }
    buf[n] = '\0';
    *len_out = n;
    return buf;
}

static int dateplan_model_id(char *out, size_t cap) {
    const char *pin = getenv(DATEPLAN_MODEL_ENV);
    const char *catalog;
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (pin && *pin && strlen(pin) <= 120) {
        snprintf(out, cap, "%s", pin);
        return 1;
    }
    catalog = getenv("BONFYRE_MODEL_CATALOG");
    if (!catalog || !*catalog) return 0;
    if (sqlite3_open_v2(catalog, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 0;
    if (sqlite3_prepare_v2(db, "SELECT dst_node_id FROM catalog_edges"
                           " WHERE src_node_id='capability:dateplan-generate'"
                           " AND rel='uses_model' LIMIT 1", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW) {
        const char *dst = (const char *)sqlite3_column_text(st, 0);
        if (dst && !strncmp(dst, "model:", 6) && strlen(dst + 6) <= 120 && dst[6]) {
            snprintf(out, cap, "%s", dst + 6);
            ok = 1;
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return ok;
}

int bf_dateplan_price(const char *model, double *in_rate, double *out_rate,
                      char *observed_at, size_t observed_cap) {
    const char *prices = getenv("BONFYRE_PRICES_DB");
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int found = 0;
    if (!model || !*model || strlen(model) > 120 || !in_rate || !out_rate ||
        !observed_at || observed_cap < 11) return -1;
    *in_rate = 0;
    *out_rate = 0;
    observed_at[0] = '\0';
    if (!prices || !*prices) return 0;
    if (sqlite3_open_v2(prices, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) return 0;
    if (sqlite3_prepare_v2(db,
            "SELECT MAX(CASE WHEN unit='token in' THEN amount END),"
            " MAX(CASE WHEN unit='token out' THEN amount END), MAX(observed_at)"
            " FROM observed_price WHERE kind='inference' AND currency='USD' AND item=?1"
            " HAVING MAX(CASE WHEN unit='token in' THEN amount END)>0"
            " AND COUNT(DISTINCT CASE WHEN unit='token in' THEN amount END)=1"
            " AND COUNT(DISTINCT CASE WHEN unit='token out' THEN amount END)<=1",
            -1, &st, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    sqlite3_bind_text(st, 1, model, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL) {
        const char *at = (const char *)sqlite3_column_text(st, 2);
        *in_rate = sqlite3_column_double(st, 0);
        *out_rate = sqlite3_column_type(st, 1) == SQLITE_NULL
                        ? 0
                        : sqlite3_column_double(st, 1);
        if (at) snprintf(observed_at, observed_cap, "%.10s", at);
        found = 1;
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    return found;
}

int bf_dateplan_generate(sqlite3 *db, const char *location, const char *intent,
                         long budget_min, long budget_max, const char *interests,
                         BfDatePlanResult *result) {
    const char *qwen, *tokenizer;
    char model[128];
    char prompt_path[] = "/tmp/bonfyre-dateplan-prompt-XXXXXX";
    char out_path[] = "/tmp/bonfyre-dateplan-out-XXXXXX";
    char err_path[] = "/tmp/bonfyre-dateplan-err-XXXXXX";
    char prompt[BF_DATEPLAN_PROMPT_CAP], sha[65];
    char *output = NULL;
    size_t outlen = 0;
    int pfd = -1, ofd = -1, efd = -1, rc;
    long elapsed = 0;
    char *argv[12];
    int argc = 0;
    (void)db;
    if (!result) return -1;
    memset(result, 0, sizeof(*result));
    qwen = getenv(DATEPLAN_QWEN_ENV);
    if (!qwen || !*qwen) qwen = "bonfyre-qwen-fpq";
    if (!dateplan_model_id(model, sizeof(model))) {
        snprintf(result->unavail_reason, sizeof(result->unavail_reason),
                 "no generation model selected: set %s or register capability:dateplan-generate",
                 DATEPLAN_MODEL_ENV);
        return -2;
    }
    tokenizer = getenv(DATEPLAN_TOKENIZER_ENV);
    if (bf_dateplan_prompt(location, intent, budget_min, budget_max, interests,
                           prompt, sizeof(prompt), sha) != 0) return -1;
    pfd = mkstemp(prompt_path);
    ofd = mkstemp(out_path);
    efd = mkstemp(err_path);
    if (pfd < 0 || ofd < 0 || efd < 0) goto fail;
    if (write(pfd, prompt, strlen(prompt)) != (ssize_t)strlen(prompt)) goto fail;
    close(pfd);
    pfd = -1;
    close(ofd);
    ofd = -1;
    close(efd);
    efd = -1;
    argv[argc++] = (char *)qwen;
    argv[argc++] = "generate-json";
    argv[argc++] = "--model-id";
    argv[argc++] = (char *)model;
    if (tokenizer && *tokenizer) {
        argv[argc++] = "--tokenizer-id";
        argv[argc++] = (char *)tokenizer;
    }
    argv[argc++] = "--prompt";
    argv[argc++] = prompt_path;
    argv[argc++] = "--out";
    argv[argc++] = out_path;
    argv[argc] = NULL;
    rc = run_bounded(argv, DATEPLAN_TIMEOUT_MS, &elapsed, err_path);
    if (rc != 0) {
        size_t elen = 0;
        char *etext = read_bounded_file(err_path, 255, &elen);
        if (etext && elen) {
            char *nl = strchr(etext, '\n');
            if (nl) *nl = '\0';
            for (char *p = etext; *p; p++)
                if ((unsigned char)*p < 32 || (unsigned char)*p == 127) *p = ' ';
            snprintf(result->unavail_reason, sizeof(result->unavail_reason), "%.200s", etext);
        } else {
            snprintf(result->unavail_reason, sizeof(result->unavail_reason),
                     "local runner refused the run");
        }
        free(etext);
        unlink(prompt_path);
        unlink(out_path);
        unlink(err_path);
        return -2;
    }
    unlink(err_path);
    output = read_bounded_file(out_path, BF_DATEPLAN_OUTPUT_CAP, &outlen);
    unlink(prompt_path);
    unlink(out_path);
    if (!output || !outlen) {
        free(output);
        snprintf(result->unavail_reason, sizeof(result->unavail_reason),
                 "local runner produced no output");
        return -2;
    }
    rc = bf_dateplan_verify(output, budget_max, model, sha, result);
    free(output);
    if (rc > 0) {
        snprintf(result->model, sizeof(result->model), "%s", model);
        result->duration_ms = elapsed;
    }
    return rc;
fail:
    if (pfd >= 0) close(pfd);
    if (ofd >= 0) close(ofd);
    if (efd >= 0) close(efd);
    unlink(prompt_path);
    unlink(out_path);
    unlink(err_path);
    return -1;
}

int bf_dateplan_history(FILE *out, sqlite3 *db, const char *actor, unsigned limit) {
    static const char *sql =
        "SELECT plan, location, intent, budget_min, budget_max, interests,"
        " plan_json, costs_admitted, accepted, generation_model, created_at"
        " FROM date_plan WHERE actor_subject=?1 AND status='published'"
        " ORDER BY id DESC LIMIT ?2";
    sqlite3_stmt *st = NULL;
    BfAuthoringTree *tree = NULL;
    unsigned n = 0;
    int ok = 0;
    if (!out || !db || !actor || strncmp(actor, "person:", 7) || strlen(actor) > 191)
        return -1;
    if (!limit || limit > 24) limit = 24;
    /* A store without the type has no history, not an error. */
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    tree = calloc(1, sizeof(*tree));
    if (!tree) {
        sqlite3_finalize(st);
        return -1;
    }
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 2, (int)limit);
    while (n < 24 && sqlite3_step(st) == SQLITE_ROW) {
        const char *plan = (const char *)sqlite3_column_text(st, 0);
        const char *location = (const char *)sqlite3_column_text(st, 1);
        const char *json = (const char *)sqlite3_column_text(st, 6);
        const char *costs = (const char *)sqlite3_column_text(st, 7);
        const char *accepted = (const char *)sqlite3_column_text(st, 8);
        bf_json_doc_t *pdoc = NULL;
        char id[32], child[48], first_title[224], admitted[64];
        int pos = 0;
        if (!plan) continue;
        first_title[0] = admitted[0] = '\0';
        if (json && *json && (pdoc = bf_json_parse_str(json, NULL, 0)) != NULL) {
            const bf_json_node_t *root = bf_json_root(pdoc);
            const bf_json_node_t *plans = root ? bf_json_obj_get(pdoc, root, "plans") : NULL;
            const bf_json_node_t *first =
                plans ? bf_json_child_first(pdoc, plans) : NULL;
            const bf_json_node_t *title =
                first ? bf_json_obj_get(pdoc, first, "title") : NULL;
            int tlen = 0;
            const char *traw = title ? bf_json_get_str(title, &tlen) : NULL;
            if (traw && tlen > 0 && tlen < 220) {
                char *plain = NULL;
                size_t bytes = 0;
                plain = bf_json_unescape(traw, tlen, &bytes);
                if (plain && bytes && strlen(plain) == bytes)
                    snprintf(first_title, sizeof(first_title), "%s", plain);
                free(plain);
            }
            bf_json_free(pdoc);
        }
        if (costs && *costs) snprintf(admitted, sizeof(admitted), "admitted %s", costs);
        snprintf(id, sizeof(id), "plan-%u", n);
        if (!ft_card(tree, id, NULL, (int)n, location && *location ? location : plan, first_title, NULL, 0)) goto done;
        if (accepted && !strcmp(accepted, "true")) {
            snprintf(child, sizeof(child), "plan-%u-acc", n);
            if (!ft_badge(tree, child, id, pos++, "Accepted")) goto done;
        }
        if (admitted[0]) {
            snprintf(child, sizeof(child), "plan-%u-cost", n);
            if (!ft_badge(tree, child, id, pos++, admitted)) goto done;
        }
        n++;
    }
    ok = 1;
done:
    sqlite3_finalize(st);
    if (!ok) {
        if (tree) {
            bf_authoring_tree_clear(tree);
            free(tree);
        }
        return -1;
    }
    if (!ft_render(tree, out)) return -1;
    return (int)n;
}
