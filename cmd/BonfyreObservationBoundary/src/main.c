#define _POSIX_C_SOURCE 200809L
#include "../generated/observation_boundary_contract.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

typedef struct {
    long long attempts;
    long long valid;
    long long rejected;
    long long *states;
    long long *decisions;
    int body_observed;
} Summary;

static char *json_text(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) == SQLITE_TEXT)
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}

static long long json_int(sqlite3 *db, const char *json, const char *path, int *present) {
    sqlite3_stmt *st = NULL;
    long long out = 0;
    *present = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_type(?1,?2),json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) == SQLITE_TEXT) {
        const char *type = (const char *)sqlite3_column_text(st, 0);
        if (!strcmp(type, "integer")) {
            out = sqlite3_column_int64(st, 1);
            *present = 1;
        }
    }
    sqlite3_finalize(st);
    return out;
}

static int json_bool(sqlite3 *db, const char *json, const char *path, int *present) {
    sqlite3_stmt *st = NULL;
    int out = 0;
    *present = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_type(?1,?2),json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) == SQLITE_TEXT) {
        const char *type = (const char *)sqlite3_column_text(st, 0);
        if (!strcmp(type, "true") || !strcmp(type, "false")) {
            out = !strcmp(type, "true");
            *present = 1;
        }
    }
    sqlite3_finalize(st);
    return out;
}

static int json_null_or_missing(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_type(?1,?2)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW) {
        if (sqlite3_column_type(st, 0) == SQLITE_NULL) ok = 1;
        else if (sqlite3_column_type(st, 0) == SQLITE_TEXT)
            ok = !strcmp((const char *)sqlite3_column_text(st, 0), "null");
    }
    sqlite3_finalize(st);
    return ok;
}

static int hex64(const char *s) {
    if (!s || strlen(s) != 64) return 0;
    for (size_t i = 0; i < 64; ++i)
        if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) return 0;
    return 1;
}

static int state_index(const char *name) {
    for (size_t i = 0; i < BF_OBSERVATION_STATE_COUNT; ++i)
        if (!strcmp(name, bf_observation_states[i].name)) return (int)i;
    return -1;
}

static int validate_receipt(sqlite3 *db, const char *json, int *state_out) {
    char *schema = json_text(db, json, "$.schema");
    char *twin = json_text(db, json, "$.twin_id");
    char *url = json_text(db, json, "$.source_url");
    char *kind = json_text(db, json, "$.observation_kind");
    char *decision = json_text(db, json, "$.policy_decision");
    char *hash = json_text(db, json, "$.content_sha256");
    int present_fetch = 0, present_transport = 0, present_http = 0, present_body = 0;
    int fetch = json_bool(db, json, "$.fetch_attempted", &present_fetch);
    long long transport = json_int(db, json, "$.transport_exit", &present_transport);
    long long http = json_int(db, json, "$.http_status", &present_http);
    long long body = json_int(db, json, "$.body_bytes", &present_body);
    int idx = kind ? state_index(kind) : -1;
    int ok = schema && !strcmp(schema, bf_observation_receipt_schema) &&
             twin && *twin && url && *url && decision && *decision &&
             idx >= 0 && present_fetch && present_transport && present_http && present_body;
    if (ok) {
        const BfObservationState *s = &bf_observation_states[idx];
        if (*s->policy_decision && strcmp(decision, s->policy_decision)) ok = 0;
        if (fetch != s->require_fetch) ok = 0;
        if (s->require_transport_success && transport != 0) ok = 0;
        if (s->require_http_success && (http < 200 || http >= 300)) ok = 0;
        if (s->require_body && body <= 0) ok = 0;
        if (!s->require_body && body != 0) ok = 0;
        if (s->require_content_hash && !hex64(hash)) ok = 0;
        if (!s->require_content_hash && !json_null_or_missing(db, json, "$.content_sha256")) ok = 0;
    }
    if (ok) *state_out = idx;
    free(schema); free(twin); free(url); free(kind); free(decision); free(hash);
    return ok;
}

static char *quote(sqlite3 *db, const char *value) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, value, -1, SQLITE_STATIC);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}

static int write_projection(sqlite3 *db, const char *path, const Summary *s) {
    FILE *out = path ? fopen(path, "w") : stdout;
    if (!out) return 0;
    fprintf(out, "{\"schema\":");
    char *q = quote(db, bf_observation_boundary_schema);
    if (!q) { if (path) fclose(out); return 0; }
    fputs(q, out); free(q);
    fprintf(out, ",\"attempts\":%lld,\"valid_receipts\":%lld,\"rejected_receipts\":%lld,\"states\":{",
            s->attempts, s->valid, s->rejected);
    int first = 1;
    for (size_t i = 0; i < BF_OBSERVATION_STATE_COUNT; ++i) {
        if (!s->states[i]) continue;
        q = quote(db, bf_observation_states[i].name);
        if (!q) { if (path) fclose(out); return 0; }
        fprintf(out, "%s%s:%lld", first ? "" : ",", q, s->states[i]);
        free(q); first = 0;
    }
    fprintf(out, "},\"body_observed\":%s,\"policy_decisions\":{", s->body_observed ? "true" : "false");
    first = 1;
    for (size_t i = 0; i < BF_OBSERVATION_STATE_COUNT; ++i) {
        if (!s->decisions[i]) continue;
        q = quote(db, bf_observation_states[i].policy_decision);
        if (!q) { if (path) fclose(out); return 0; }
        fprintf(out, "%s%s:%lld", first ? "" : ",", q, s->decisions[i]);
        free(q); first = 0;
    }
    fputs("},\"cannot_infer\":[", out);
    for (size_t i = 0; i < BF_OBSERVATION_CANNOT_INFER_COUNT; ++i) {
        q = quote(db, bf_observation_cannot_infer[i]);
        if (!q) { if (path) fclose(out); return 0; }
        fprintf(out, "%s%s", i ? "," : "", q);
        free(q);
    }
    fputs("]}\n", out);
    if (path) fclose(out);
    return 1;
}

static const char *opt(int argc, char **argv, const char *name, const char *fallback) {
    for (int i = 2; i + 1 < argc; ++i) if (!strcmp(argv[i], name)) return argv[i + 1];
    return fallback;
}

int main(int argc, char **argv) {
    sqlite3 *db = NULL;
    FILE *in = NULL;
    char *line = NULL;
    size_t cap = 0;
    Summary summary = {0};
    const char *receipts, *output;
    int rc = 1;
    enum { OB_PROJECT };
    static const BfCommandEntry boundary_commands[] = {
        {"project", OB_PROJECT, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(boundary_commands,
        sizeof(boundary_commands) / sizeof(boundary_commands[0]), argv[1]);
    if (argc < 2 || !command || command->id != OB_PROJECT) {
        fprintf(stderr, "Usage: bonfyre-observation-boundary project --receipts FILE [--output FILE]\n");
        return 1;
    }
    receipts = opt(argc, argv, "--receipts", NULL);
    output = opt(argc, argv, "--output", NULL);
    if (!receipts || sqlite3_open(":memory:", &db) != SQLITE_OK) goto done;
    summary.states = calloc(BF_OBSERVATION_STATE_COUNT, sizeof(*summary.states));
    summary.decisions = calloc(BF_OBSERVATION_STATE_COUNT, sizeof(*summary.decisions));
    if (!summary.states || !summary.decisions || !(in = fopen(receipts, "r"))) goto done;
    while (getline(&line, &cap, in) >= 0) {
        int idx = -1;
        if (!*line || *line == '\n') continue;
        summary.attempts++;
        if (!validate_receipt(db, line, &idx)) { summary.rejected++; continue; }
        summary.valid++;
        summary.states[idx]++;
        summary.decisions[idx]++;
        if (!strcmp(bf_observation_states[idx].name, "source_observed")) summary.body_observed = 1;
    }
    if (!write_projection(db, output, &summary)) goto done;
    rc = 0;
done:
    free(line);
    free(summary.states);
    free(summary.decisions);
    if (in) fclose(in);
    if (db) sqlite3_close(db);
    return rc;
}
