#define _POSIX_C_SOURCE 200809L
#include "domain_binding.h"
#include "html_metadata.h"
#include "../../BonfyreExperienceMap/src/json_canon.h"
#include "../generated/native_domain_profiles.h"

#include <bonfyre.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BF_DOMAIN_BINDING_SCHEMA "bonfyre-domain-binding.v1"
#define BF_DOMAIN_PROFILE "reference-standard"
#define BF_DOMAIN_PARSER "html-metadata-v1"

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

static char *json_quote(sqlite3 *db, const char *value) {
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

static int safe_relative_path(const char *path) {
    const char *p;
    if (!path || !*path || path[0] == '/') return 0;
    for (p = path; *p; ++p) {
        if ((p == path || p[-1] == '/') && p[0] == '.' && p[1] == '.' &&
            (p[2] == '/' || p[2] == '\0')) return 0;
    }
    return 1;
}

static char *join_path(const char *dir, const char *leaf) {
    size_t dl, ll;
    char *out;
    if (!dir || !leaf) return NULL;
    dl = strlen(dir);
    ll = strlen(leaf);
    out = (char *)malloc(dl + ll + 2);
    if (!out) return NULL;
    memcpy(out, dir, dl);
    if (dl && dir[dl - 1] != '/') out[dl++] = '/';
    memcpy(out + dl, leaf, ll + 1);
    return out;
}

static char *latest_observation(sqlite3 *db, const char *path, const char *occurrence_id) {
    FILE *f = fopen(path, "r");
    char *line = NULL, *found = NULL;
    size_t cap = 0;
    if (!f) return NULL;
    while (getline(&line, &cap, f) >= 0) {
        char schema[96] = "", twin[512] = "", kind[96] = "", body_path[4096] = "";
        if (!*line || *line == '\n') continue;
        if (!bf_experience_json_text(db, line, "$.schema", schema, sizeof(schema)) ||
            strcmp(schema, "bonfyre-foreign-observation.v1") != 0 ||
            !bf_experience_json_text(db, line, "$.twin_id", twin, sizeof(twin)) ||
            strcmp(twin, occurrence_id) != 0 ||
            !bf_experience_json_text(db, line, "$.observation_kind", kind, sizeof(kind)) ||
            strcmp(kind, "source_observed") != 0 ||
            !bf_experience_json_text(db, line, "$.body_path", body_path, sizeof(body_path)) ||
            !*body_path) continue;
        free(found);
        found = strdup(line);
    }
    free(line);
    fclose(f);
    return found;
}

static const BfBindingDomainProfile *profile_for(const char *name) {
    size_t i;
    for (i = 0; i < BF_BINDING_DOMAIN_PROFILE_COUNT; ++i)
        if (!strcmp(bf_binding_domain_profiles[i].name, name))
            return &bf_binding_domain_profiles[i];
    return NULL;
}

static int profile_field(const BfBindingDomainProfile *profile, const char *name) {
    size_t i;
    if (!profile) return 0;
    for (i = 0; i < profile->field_count; ++i)
        if (!strcmp(profile->fields[i].name, name)) return 1;
    return 0;
}

static const BfBindingProjection *profile_projection(const BfBindingDomainProfile *profile,
                                                     const char *carrier) {
    size_t i;
    if (!profile) return NULL;
    for (i = 0; i < profile->projection_count; ++i)
        if (!strcmp(profile->projections[i].carrier, carrier))
            return &profile->projections[i];
    return NULL;
}

static int projection_identity_field(const BfBindingProjection *projection, const char *name) {
    size_t i;
    if (!projection) return 0;
    for (i = 0; i < projection->field_count; ++i)
        if (!strcmp(projection->fields[i].semantic, name) &&
            !strcmp(projection->fields[i].carrier, name)) return 1;
    return 0;
}

static int evidence_satisfied(const BfBindingDomainProfile *profile) {
    size_t i;
    int foreign = 0, ingest = 0;
    if (!profile) return 0;
    for (i = 0; i < profile->evidence_requirement_count; ++i) {
        const char *requirement = profile->evidence_requirements[i];
        if (!strcmp(requirement, "foreign_observation_receipt")) foreign = 1;
        else if (!strcmp(requirement, "ingest_artifact")) ingest = 1;
        else return 0;
    }
    return foreign && ingest;
}

static int record_allowed(const BfBindingDomainProfile *profile, const BfHtmlMetadata *metadata) {
    static const char *required[] = {"authority", "description", "name", "source_url"};
    const BfBindingProjection *projection = profile_projection(profile, "semantic-json");
    size_t i;
    if (!profile || !metadata || !projection ||
        strcmp(projection->standard, "Bonfyre semantic record") != 0) return 0;
    for (i = 0; i < sizeof(required) / sizeof(required[0]); ++i)
        if (!profile_field(profile, required[i]) ||
            !projection_identity_field(projection, required[i])) return 0;
    if (metadata->updated_at &&
        (!profile_field(profile, "updated_at") ||
         !projection_identity_field(projection, "updated_at"))) return 0;
    return 1;
}

static int already_appended(sqlite3 *db, const char *path, const char *occurrence_id,
                            const char *semantic_hash) {
    FILE *f;
    char *line = NULL;
    size_t cap = 0;
    int found = 0;
    if (!path || !(f = fopen(path, "r"))) return 0;
    while (getline(&line, &cap, f) >= 0) {
        char schema[96] = "", occurrence[512] = "", hash[80] = "";
        if (bf_experience_json_text(db, line, "$.schema", schema, sizeof(schema)) &&
            !strcmp(schema, BF_DOMAIN_BINDING_SCHEMA) &&
            bf_experience_json_text(db, line, "$.occurrence_id", occurrence, sizeof(occurrence)) &&
            !strcmp(occurrence, occurrence_id) &&
            bf_experience_json_text(db, line, "$.semantic_record_sha256", hash, sizeof(hash)) &&
            !strcmp(hash, semantic_hash)) {
            found = 1;
            break;
        }
    }
    free(line);
    fclose(f);
    return found;
}

int bf_experience_bind_domain(sqlite3 *db, const char *occurrence_id,
                              const char *observation_receipts,
                              const char *ingest_dir,
                              const char *output_path) {
    const BfBindingDomainProfile *profile = profile_for(BF_DOMAIN_PROFILE);
    char *observation = NULL, *manifest = NULL, *manifest_path = NULL, *normalized_path = NULL;
    char body_path[4096] = "", source_url[4096] = "", observed_hash[80] = "";
    char status[64] = "", manifest_source_hash[80] = "", normalized_leaf[4096] = "";
    char normalized_hash[80] = "", actual_hash[65], foreign_hash[65], manifest_hash[65];
    char semantic_hash[65];
    BfHtmlMetadata metadata = {0};
    char *q_name = NULL, *q_authority = NULL, *q_description = NULL;
    char *q_source_url = NULL, *q_updated = NULL, *q_occurrence = NULL, *q_receipt_source = NULL;
    char *record = NULL, *receipt = NULL;
    sqlite3_str *builder = NULL;
    int rc = 2;

    if (!db || !occurrence_id || !observation_receipts || !ingest_dir ||
        !profile || !evidence_satisfied(profile)) {
        fprintf(stderr, "experience-binding: DomainProfile declaration unavailable or unsatisfied\n");
        goto done;
    }

    observation = latest_observation(db, observation_receipts, occurrence_id);
    if (!observation ||
        !bf_experience_json_text(db, observation, "$.body_path", body_path, sizeof(body_path)) ||
        !bf_experience_json_text(db, observation, "$.source_url", source_url, sizeof(source_url)) ||
        !bf_experience_json_text(db, observation, "$.content_sha256", observed_hash, sizeof(observed_hash)) ||
        strlen(observed_hash) != 64 ||
        bf_sha256_file(body_path, actual_hash) != 0 ||
        strcmp(actual_hash, observed_hash) != 0 ||
        !bf_experience_json_sha256(db, observation, foreign_hash)) {
        fprintf(stderr, "experience-binding: retained ForeignTwin observation refused\n");
        goto done;
    }

    manifest_path = join_path(ingest_dir, "intake-manifest.json");
    if (!manifest_path || !(manifest = read_file(manifest_path)) ||
        !bf_experience_json_text(db, manifest, "$.status", status, sizeof(status)) ||
        strcmp(status, "ready") != 0 ||
        !bf_experience_json_text(db, manifest, "$.source_content_hash",
                                 manifest_source_hash, sizeof(manifest_source_hash)) ||
        strcmp(manifest_source_hash, observed_hash) != 0 ||
        !bf_experience_json_text(db, manifest, "$.normalized_path",
                                 normalized_leaf, sizeof(normalized_leaf)) ||
        !safe_relative_path(normalized_leaf) ||
        !bf_experience_json_text(db, manifest, "$.normalized_content_hash",
                                 normalized_hash, sizeof(normalized_hash)) ||
        strlen(normalized_hash) != 64 ||
        bf_sha256_file(manifest_path, manifest_hash) != 0) {
        fprintf(stderr, "experience-binding: ingest manifest refused\n");
        goto done;
    }

    normalized_path = join_path(ingest_dir, normalized_leaf);
    if (!normalized_path || bf_sha256_file(normalized_path, actual_hash) != 0 ||
        strcmp(actual_hash, normalized_hash) != 0 ||
        !bf_html_metadata_parse_file(normalized_path, source_url, &metadata) ||
        !record_allowed(profile, &metadata)) {
        fprintf(stderr, "experience-binding: normalized semantic observation refused\n");
        goto done;
    }

    q_name = json_quote(db, metadata.name);
    q_authority = json_quote(db, metadata.authority);
    q_description = json_quote(db, metadata.description);
    q_source_url = json_quote(db, metadata.source_url);
    if (metadata.updated_at) q_updated = json_quote(db, metadata.updated_at);
    q_occurrence = json_quote(db, occurrence_id);
    q_receipt_source = json_quote(db, source_url);
    if (!q_name || !q_authority || !q_description || !q_source_url ||
        (metadata.updated_at && !q_updated) || !q_occurrence || !q_receipt_source) goto done;

    builder = sqlite3_str_new(db);
    if (!builder) goto done;
    sqlite3_str_appendf(builder,
        "{\"authority\":%s,\"description\":%s,\"name\":%s,\"source_url\":%s",
        q_authority, q_description, q_name, q_source_url);
    if (q_updated) sqlite3_str_appendf(builder, ",\"updated_at\":%s", q_updated);
    sqlite3_str_appendall(builder, "}");
    record = sqlite3_str_finish(builder);
    builder = NULL;
    if (!record || !bf_experience_json_sha256(db, record, semantic_hash)) goto done;

    builder = sqlite3_str_new(db);
    if (!builder) goto done;
    sqlite3_str_appendf(builder,
        "{\"axis\":\"semantic\",\"binding_validated\":true,"
        "\"cannot_infer\":[\"profile binding -> external standard conformance\","
        "\"semantic extraction -> source authority\","
        "\"retained bytes -> provider admission or callable capability\"],"
        "\"evidence_requirements\":[\"foreign_observation_receipt\",\"ingest_artifact\"],"
        "\"foreign_receipt_sha256\":\"%s\",\"from_owner\":\"ForeignTwin\","
        "\"ingest_manifest_sha256\":\"%s\",\"normalized_content_sha256\":\"%s\","
        "\"occurrence_id\":%s,\"owner\":\"DomainProfile\",\"parser\":\"" BF_DOMAIN_PARSER "\","
        "\"profile\":\"" BF_DOMAIN_PROFILE "\",\"schema\":\"" BF_DOMAIN_BINDING_SCHEMA "\","
        "\"semantic_fields\":[\"authority\",\"description\",\"name\",\"source_url\"",
        foreign_hash, manifest_hash, normalized_hash, q_occurrence);
    if (metadata.updated_at) sqlite3_str_appendall(builder, ",\"updated_at\"");
    sqlite3_str_appendf(builder,
        "],\"semantic_record_sha256\":\"%s\",\"source_content_sha256\":\"%s\","
        "\"source_url\":%s,\"state\":\"profile_bound\",\"to_owner\":\"DomainProfile\"}",
        semantic_hash, observed_hash, q_receipt_source);
    receipt = sqlite3_str_finish(builder);
    builder = NULL;
    if (!receipt) goto done;

    if (output_path && *output_path &&
        !already_appended(db, output_path, occurrence_id, semantic_hash)) {
        FILE *out = fopen(output_path, "a");
        if (!out) {
            fprintf(stderr, "experience-binding: cannot append domain binding to %s\n", output_path);
            goto done;
        }
        fputs(receipt, out);
        fputc('\n', out);
        fclose(out);
    }
    puts(receipt);
    rc = 0;

done:
    if (builder) sqlite3_free(sqlite3_str_finish(builder));
    sqlite3_free(receipt);
    sqlite3_free(record);
    free(q_receipt_source);
    free(q_occurrence);
    free(q_updated);
    free(q_source_url);
    free(q_description);
    free(q_authority);
    free(q_name);
    bf_html_metadata_free(&metadata);
    free(normalized_path);
    free(manifest_path);
    free(manifest);
    free(observation);
    return rc;
}
