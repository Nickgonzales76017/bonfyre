#define _POSIX_C_SOURCE 200809L
#include "protocol_binding.h"
#include "json_canon.h"

#include <bonfyre.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    long size;
    char *body;
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) < 0 || fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return NULL;
    }
    body = (char *)malloc((size_t)size + 1);
    if (!body) { fclose(f); return NULL; }
    if (size && fread(body, 1, (size_t)size, f) != (size_t)size) {
        fclose(f);
        free(body);
        return NULL;
    }
    body[size] = '\0';
    fclose(f);
    return body;
}

static char *latest_domain_binding(sqlite3 *db, const char *path,
                                   const char *occurrence_id) {
    FILE *f = fopen(path, "r");
    char *line = NULL;
    char *found = NULL;
    size_t cap = 0;
    if (!f) return NULL;
    while (getline(&line, &cap, f) >= 0) {
        char schema[96] = "", occurrence[512] = "", owner[128] = "", state[128] = "";
        if (!*line || *line == '\n') continue;
        if (!bf_experience_json_text(db, line, "$.schema", schema, sizeof(schema)) ||
            strcmp(schema, "bonfyre-domain-binding.v1") != 0 ||
            !bf_experience_json_text(db, line, "$.occurrence_id", occurrence, sizeof(occurrence)) ||
            strcmp(occurrence, occurrence_id) != 0 ||
            !bf_experience_json_text(db, line, "$.owner", owner, sizeof(owner)) ||
            strcmp(owner, "DomainProfile") != 0 ||
            !bf_experience_json_text(db, line, "$.state", state, sizeof(state)) ||
            strcmp(state, "profile_bound") != 0 ||
            !bf_experience_json_true(db, line, "$.binding_validated")) {
            continue;
        }
        free(found);
        found = strdup(line);
    }
    free(line);
    fclose(f);
    return found;
}

static int already_appended(sqlite3 *db, const char *path, const char *occurrence_id,
                            const char *domain_hash) {
    FILE *f;
    char *line = NULL;
    size_t cap = 0;
    int found = 0;
    if (!path || !(f = fopen(path, "r"))) return 0;
    while (getline(&line, &cap, f) >= 0) {
        char schema[96] = "", occurrence[512] = "", hash[80] = "";
        if (bf_experience_json_text(db, line, "$.schema", schema, sizeof(schema)) &&
            !strcmp(schema, "bonfyre-protocol-binding.v1") &&
            bf_experience_json_text(db, line, "$.occurrence_id", occurrence, sizeof(occurrence)) &&
            !strcmp(occurrence, occurrence_id) &&
            bf_experience_json_text(db, line, "$.domain_binding_sha256", hash, sizeof(hash)) &&
            !strcmp(hash, domain_hash)) {
            found = 1;
            break;
        }
    }
    free(line);
    fclose(f);
    return found;
}

static char *quote(sqlite3 *db, const char *value) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &st, NULL) != SQLITE_OK)
        return NULL;
    sqlite3_bind_text(st, 1, value ? value : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}

int bf_experience_bind_protocol(sqlite3 *db, const char *occurrence_id,
                                const char *evidence_path,
                                const char *native_receipt_path,
                                const char *output_path) {
    char *domain = NULL, *native = NULL;
    char semantic_hash[80] = "", native_schema[128] = "", native_occurrence[512] = "";
    char contract[256] = "", state[128] = "";
    char domain_hash[65], native_hash[65];
    long long programs = 0;
    char *q_occurrence = NULL, *q_contract = NULL;
    char *receipt = NULL;
    size_t need;
    int rc = 2;

    if (!db || !occurrence_id || !evidence_path || !native_receipt_path) goto done;
    domain = latest_domain_binding(db, evidence_path, occurrence_id);
    if (!domain ||
        !bf_experience_json_text(db, domain, "$.semantic_record_sha256",
                                semantic_hash, sizeof(semantic_hash)) ||
        strlen(semantic_hash) != 64 ||
        !bf_experience_json_sha256(db, domain, domain_hash)) {
        fprintf(stderr, "experience-map: no validated DomainProfile receipt for %s\n",
                occurrence_id);
        goto done;
    }

    native = read_file(native_receipt_path);
    if (!native ||
        !bf_experience_json_text(db, native, "$.schema", native_schema, sizeof(native_schema)) ||
        strcmp(native_schema, "bonfyre-protocol-observation-bridge.v1") != 0 ||
        !bf_experience_json_true(db, native, "$.registered") ||
        !bf_experience_json_text(db, native, "$.state", state, sizeof(state)) ||
        strcmp(state, "compiled") != 0 ||
        !bf_experience_json_text(db, native, "$.occurrence_id",
                                native_occurrence, sizeof(native_occurrence)) ||
        strcmp(native_occurrence, occurrence_id) != 0 ||
        !bf_experience_json_text(db, native, "$.contract", contract, sizeof(contract)) ||
        strcmp(contract, "ogc-api-common-observation") != 0 ||
        !bf_experience_json_int(db, native, "$.programs", &programs) ||
        programs != 3 ||
        !bf_experience_json_true(db, native, "$.scoped") ||
        bf_sha256_file(native_receipt_path, native_hash) != 0) {
        fprintf(stderr, "experience-map: native protocol bridge receipt refused\n");
        goto done;
    }

    q_occurrence = quote(db, occurrence_id);
    q_contract = quote(db, contract);
    if (!q_occurrence || !q_contract) goto done;
    need = strlen(q_occurrence) + strlen(q_contract) + strlen(semantic_hash) +
           strlen(domain_hash) + strlen(native_hash) + 1200;
    receipt = (char *)malloc(need);
    if (!receipt) goto done;
    snprintf(receipt, need,
        "{\"axis\":\"protocol\",\"binding_validated\":true,"
        "\"cannot_infer\":[\"compiled contract -> OGC conformance\","
        "\"compiled contract -> enacted request\","
        "\"compiled contract -> provider admission, authority or effect\"],"
        "\"contract\":%s,\"domain_binding_sha256\":\"%s\","
        "\"from_owner\":\"DomainProfile\",\"native_registration_receipt_sha256\":\"%s\","
        "\"occurrence_id\":%s,\"owner\":\"ProtocolContract\",\"programs\":%lld,"
        "\"schema\":\"bonfyre-protocol-binding.v1\","
        "\"semantic_record_sha256\":\"%s\",\"state\":\"compiled\","
        "\"to_owner\":\"ProtocolContract\"}",
        q_contract, domain_hash, native_hash, q_occurrence, programs, semantic_hash);

    if (output_path && *output_path && !already_appended(db, output_path, occurrence_id, domain_hash)) {
        FILE *out = fopen(output_path, "a");
        if (!out) {
            fprintf(stderr, "experience-map: cannot append protocol binding to %s\n",
                    output_path);
            goto done;
        }
        fputs(receipt, out);
        fputc('\n', out);
        fclose(out);
    }
    puts(receipt);
    rc = 0;

done:
    free(receipt);
    free(q_contract);
    free(q_occurrence);
    free(native);
    free(domain);
    return rc;
}
