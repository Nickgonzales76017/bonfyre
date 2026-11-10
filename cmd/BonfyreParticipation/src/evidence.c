#define _POSIX_C_SOURCE 200809L
#include "evidence.h"
#include "json.h"
#include "participation_contract.h"

#include <ctype.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int starts_external(const char *v) { return v && !strncmp(v, "external:", 9); }

static int hex64(const char *v) {
    if (!v || strlen(v) != 64) return 0;
    for (size_t i = 0; i < 64; ++i)
        if (!((v[i] >= '0' && v[i] <= '9') || (v[i] >= 'a' && v[i] <= 'f'))) return 0;
    return 1;
}

static int len64(const char *v) { return v && strlen(v) == 64; }

static int known_rung(const char *axis, const char *state) {
    if (!axis || !state) return 0;
    for (size_t i = 0; i < bf_part_ladders_COUNT; ++i)
        if (!strcmp(bf_part_ladders[i].axis, axis) && !strcmp(bf_part_ladders[i].state, state)) return 1;
    return 0;
}

static int insert_fact(sqlite3 *db, const char *occ, const char *axis,
                       const char *owner, const char *state,
                       const char *source, const char *target) {
    sqlite3_stmt *st = NULL;
    if (!occ || !*occ || !axis || !*axis || !owner || !*owner || !state || !*state) return 0;
    if (sqlite3_prepare_v2(db,
        "INSERT INTO participation_fact(occurrence,axis,owner,state) VALUES(?1,?2,?3,?4)",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, occ, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, axis, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, owner, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, state, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { sqlite3_finalize(st); return 0; }
    sqlite3_finalize(st);
    if (!source || !*source || !target || !*target || !strcmp(source, target)) return 1;
    if (sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO participation_edge(occurrence,source,target) VALUES(?1,?2,?3)",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, occ, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, source, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, target, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) { sqlite3_finalize(st); return 0; }
    sqlite3_finalize(st);
    return 1;
}

int bf_part_evidence_init(sqlite3 *db) {
    const char *sql =
        "CREATE TABLE participation_fact(occurrence TEXT NOT NULL,axis TEXT NOT NULL,owner TEXT NOT NULL,state TEXT NOT NULL);"
        "CREATE INDEX participation_fact_occurrence ON participation_fact(occurrence,axis);"
        "CREATE TABLE participation_edge(occurrence TEXT NOT NULL,source TEXT NOT NULL,target TEXT NOT NULL,"
        "UNIQUE(occurrence,source,target));";
    return db && sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK;
}

static int body_retained(const char *path) {
    struct stat st;
    return path && *path && stat(path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0;
}

static int normalize_domain(sqlite3 *db, const char *json) {
    char *occ = NULL, *owner = NULL, *state = NULL;
    int ok = 1;
    if (!bf_part_json_true(db, json, "$.binding_validated")) return 1;
    owner = bf_part_json_text(db, json, "$.owner");
    state = bf_part_json_text(db, json, "$.state");
    occ = bf_part_json_text(db, json, "$.occurrence_id");
    if (owner && !strcmp(owner, "DomainProfile") && state && !strcmp(state, "profile_bound") && occ)
        ok = insert_fact(db, occ, "semantic", "DomainProfile", "profile_bound", "ForeignTwin", "DomainProfile");
    free(occ); free(owner); free(state);
    return ok;
}

static int normalize_protocol(sqlite3 *db, const char *json) {
    char *occ = NULL, *owner = NULL, *state = NULL, *from = NULL, *to = NULL;
    char *semantic = NULL, *domain = NULL, *native = NULL, *contract = NULL;
    long long programs = 0;
    int ok = 1;
    if (!bf_part_json_true(db, json, "$.binding_validated")) return 1;
    owner = bf_part_json_text(db, json, "$.owner");
    state = bf_part_json_text(db, json, "$.state");
    from = bf_part_json_text(db, json, "$.from_owner");
    to = bf_part_json_text(db, json, "$.to_owner");
    semantic = bf_part_json_text(db, json, "$.semantic_record_sha256");
    domain = bf_part_json_text(db, json, "$.domain_binding_sha256");
    native = bf_part_json_text(db, json, "$.native_registration_receipt_sha256");
    contract = bf_part_json_text(db, json, "$.contract");
    occ = bf_part_json_text(db, json, "$.occurrence_id");
    if (owner && !strcmp(owner, "ProtocolContract") && state && !strcmp(state, "compiled") &&
        from && !strcmp(from, "DomainProfile") && to && !strcmp(to, "ProtocolContract") &&
        len64(semantic) && len64(domain) && len64(native) && contract && *contract &&
        bf_part_json_int(db, json, "$.programs", &programs) && programs >= 1 && occ)
        ok = insert_fact(db, occ, "protocol", "ProtocolContract", "compiled", "DomainProfile", "ProtocolContract");
    free(occ); free(owner); free(state); free(from); free(to);
    free(semantic); free(domain); free(native); free(contract);
    return ok;
}

static int normalize_foreign(sqlite3 *db, const char *json) {
    char *occ = bf_part_json_text(db, json, "$.twin_id");
    char *state = bf_part_json_text(db, json, "$.observation_kind");
    char *body = NULL;
    int ok = 1;
    if (!starts_external(occ)) goto done;
    if (!state) state = bf_part_json_text(db, json, "$.policy_decision");
    if (!state) state = strdup("receipted");
    if (!state) { ok = 0; goto done; }
    if (!strcmp(state, "source_retained")) { free(state); state = strdup("source_observed"); }
    body = bf_part_json_text(db, json, "$.body_path");
    if (state && !strcmp(state, "source_observed") && body && body_retained(body)) {
        free(state); state = strdup("source_retained");
    }
    if (!state) { ok = 0; goto done; }
    ok = insert_fact(db, occ, "observation", "ForeignTwin", state, "ExternalEstate", "ForeignTwin");
done:
    free(occ); free(state); free(body);
    return ok;
}

static int normalize_owned(sqlite3 *db, const char *json, int enactment) {
    char *axis = bf_part_json_text(db, json, "$.axis");
    char *state = bf_part_json_text(db, json, "$.state");
    char *occ = bf_part_json_text(db, json, "$.occurrence_id");
    char *basis = bf_part_json_text(db, json, "$.basis_sha256");
    char *evidence = bf_part_json_text(db, json, "$.evidence_sha256");
    char *owner = bf_part_json_text(db, json, "$.owner");
    char *from = bf_part_json_text(db, json, "$.from_owner");
    char *to = bf_part_json_text(db, json, "$.to_owner");
    char *contract = enactment ? bf_part_json_text(db, json, "$.contract") : NULL;
    char *produced = enactment ? NULL : bf_part_json_text(db, json, "$.produced_by");
    int ok = 1;
    if (!known_rung(axis, state) || !starts_external(occ) || !hex64(basis) || !hex64(evidence)) goto done;
    if (enactment) {
        if (!contract || !*contract) goto done;
        ok = insert_fact(db, occ, axis, owner && *owner ? owner : "ProtocolContract", state, from, to);
    } else {
        if (!owner || !*owner || !from || !*from || !to || !*to || !produced || !*produced) goto done;
        ok = insert_fact(db, occ, axis, owner, state, from, to);
    }
done:
    free(axis); free(state); free(occ); free(basis); free(evidence); free(owner);
    free(from); free(to); free(contract); free(produced);
    return ok;
}

static int normalize_line(sqlite3 *db, const char *json) {
    char *schema = bf_part_json_text(db, json, "$.schema");
    int ok = 1;
    if (!schema) return 1;
    if (!strcmp(schema, "bonfyre-domain-binding.v1")) ok = normalize_domain(db, json);
    else if (!strcmp(schema, "bonfyre-protocol-binding.v1")) ok = normalize_protocol(db, json);
    else if (!strcmp(schema, "bonfyre-foreign-observation.v1")) ok = normalize_foreign(db, json);
    else if (!strcmp(schema, "bonfyre-protocol-enactment.v1")) ok = normalize_owned(db, json, 1);
    else if (!strcmp(schema, "bonfyre-owner-participation.v1")) ok = normalize_owned(db, json, 0);
    free(schema);
    return ok;
}

static int load_file(sqlite3 *db, const char *path, long long *raw_rows) {
    FILE *f = fopen(path, "r");
    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    if (!f) return 0;
    while ((n = getline(&line, &cap, f)) >= 0) {
        char *p = line;
        (void)n;
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) continue;
        (*raw_rows)++;
        if (!normalize_line(db, p)) { free(line); fclose(f); return 0; }
    }
    free(line);
    fclose(f);
    return 1;
}

static int json_extension(const char *name) {
    size_t n = strlen(name);
    return (n >= 5 && !strcmp(name + n - 5, ".json")) ||
           (n >= 6 && !strcmp(name + n - 6, ".jsonl"));
}

static int load_path(sqlite3 *db, const char *path, long long *raw_rows) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    if (S_ISREG(st.st_mode)) return load_file(db, path, raw_rows);
    if (!S_ISDIR(st.st_mode)) return 0;
    DIR *dir = opendir(path);
    struct dirent *ent;
    if (!dir) return 0;
    while ((ent = readdir(dir))) {
        char child[4096];
        if (!strcmp(ent->d_name, ".") || !strcmp(ent->d_name, "..")) continue;
        if (snprintf(child, sizeof(child), "%s/%s", path, ent->d_name) >= (int)sizeof(child)) {
            closedir(dir); return 0;
        }
        if (stat(child, &st) != 0) { closedir(dir); return 0; }
        if (S_ISDIR(st.st_mode)) {
            if (!load_path(db, child, raw_rows)) { closedir(dir); return 0; }
        } else if (S_ISREG(st.st_mode) && json_extension(ent->d_name)) {
            if (!load_file(db, child, raw_rows)) { closedir(dir); return 0; }
        }
    }
    closedir(dir);
    return 1;
}

int bf_part_evidence_load(sqlite3 *db, const char *path, long long *raw_rows) {
    if (!db || !path || !raw_rows) return 0;
    *raw_rows = 0;
    return load_path(db, path, raw_rows);
}
