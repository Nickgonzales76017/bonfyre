#define _POSIX_C_SOURCE 200809L
#include "compiler.h"
#include "chains.h"
#include "match.h"
#include "json_ascii.h"

#include <bonfyre.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct { char *data; size_t len, cap; } Buf;
typedef struct { char *kind; long long count; } KindCount;
typedef struct {
    long long records, with_bindings, edges, unbound;
    KindCount *kinds;
    size_t kind_count;
    BfSha256 sha;
} Metrics;

static int reserve(Buf *b, size_t extra) {
    size_t need = b->len + extra + 1, cap = b->cap ? b->cap : 512;
    char *p;
    while (cap < need) cap *= 2;
    p = realloc(b->data, cap);
    if (!p) return 0;
    b->data = p; b->cap = cap; return 1;
}
static int putn(Buf *b, const char *s, size_t n) {
    if (!reserve(b, n)) return 0;
    memcpy(b->data + b->len, s, n); b->len += n; b->data[b->len] = 0; return 1;
}
static int putsb(Buf *b, const char *s) { return putn(b, s, strlen(s)); }
static int putc1(Buf *b, char c) { return putn(b, &c, 1); }

static char *json_quote(sqlite3 *db, const char *text) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_quote(?1)", -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, text ? text : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}
static int put_json_string(sqlite3 *db, Buf *b, const char *text) {
    char *q = json_quote(db, text);
    int ok = q && putsb(b, q);
    free(q); return ok;
}
static char *json_extract_dup(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}
static int json_bool(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL; int out = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)=1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) out = sqlite3_column_int(st, 0);
    sqlite3_finalize(st); return out;
}
static int array_values(sqlite3 *db, const char *json, const char *path, BfStageStrings *out) {
    sqlite3_stmt *st = NULL;
    memset(out, 0, sizeof(*out));
    if (sqlite3_prepare_v2(db, "SELECT value FROM json_each(?1,?2) ORDER BY CAST(key AS INTEGER)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *v = (const char *)sqlite3_column_text(st, 0);
        char **next = realloc(out->items, (out->count + 1) * sizeof(*next));
        if (!next || !v) { sqlite3_finalize(st); bf_stage_strings_free(out); return 0; }
        out->items = next; out->items[out->count] = strdup(v);
        if (!out->items[out->count]) { sqlite3_finalize(st); bf_stage_strings_free(out); return 0; }
        out->count++;
    }
    sqlite3_finalize(st); return 1;
}
static int strings_add_unique(BfStageStrings *out, const char *value) {
    for (size_t i = 0; i < out->count; ++i) if (!strcmp(out->items[i], value)) return 1;
    char **next = realloc(out->items, (out->count + 1) * sizeof(*next));
    if (!next) return 0;
    out->items = next; out->items[out->count] = strdup(value);
    if (!out->items[out->count]) return 0;
    out->count++; return 1;
}
static int append_list_unique(BfStageStrings *dst, const BfStageStrings *src) {
    for (size_t i = 0; i < src->count; ++i) if (!strings_add_unique(dst, src->items[i])) return 0;
    return 1;
}
static int put_string_array(sqlite3 *db, Buf *b, const BfStageStrings *values) {
    if (!putc1(b, '[')) return 0;
    for (size_t i = 0; i < values->count; ++i) {
        if (i && !putc1(b, ',')) return 0;
        if (!put_json_string(db, b, values->items[i])) return 0;
    }
    return putc1(b, ']');
}
static int put_bindings(sqlite3 *db, Buf *b, const BfStageChains *chains,
                        const BfStageStrings *human, const BfStageStrings *hints,
                        int presentation, int reference_only,
                        long long *edge_count, long long *bound_count) {
    int first = 1;
    *edge_count = 0; *bound_count = 0;
    if (!putc1(b, '{')) return 0;
    if (!reference_only) for (size_t i = 0; i < human->count; ++i) {
        const BfStageChain *chain = bf_stage_chain_find(chains, human->items[i]);
        BfStageStrings chosen = {0};
        if (!chain) return 0;
        if (!(presentation ? bf_stage_select_presentation(chain, &chosen) :
                             bf_stage_select(chain, (const char *const *)hints->items, hints->count, &chosen)))
            return 0;
        if (chosen.count) {
            if (!first && !putc1(b, ',')) { bf_stage_strings_free(&chosen); return 0; }
            first = 0;
            if (!put_json_string(db, b, human->items[i]) || !putc1(b, ':') ||
                !put_string_array(db, b, &chosen)) { bf_stage_strings_free(&chosen); return 0; }
            *edge_count += (long long)chosen.count;
            (*bound_count)++;
        }
        bf_stage_strings_free(&chosen);
    }
    return putc1(b, '}');
}
static int kind_inc(Metrics *m, const char *kind) {
    for (size_t i = 0; i < m->kind_count; ++i) if (!strcmp(m->kinds[i].kind, kind)) {
        m->kinds[i].count++; return 1;
    }
    KindCount *next = realloc(m->kinds, (m->kind_count + 1) * sizeof(*next));
    if (!next) return 0;
    m->kinds = next; m->kinds[m->kind_count].kind = strdup(kind);
    if (!m->kinds[m->kind_count].kind) return 0;
    m->kinds[m->kind_count].count = 1; m->kind_count++; return 1;
}
static int kind_cmp(const void *a, const void *b) {
    return strcmp(((const KindCount *)a)->kind, ((const KindCount *)b)->kind);
}

static int raw_table_load(sqlite3 *db, const char *path) {
    FILE *f = fopen(path, "r"); char *line = NULL; size_t cap = 0; sqlite3_stmt *ins = NULL;
    if (!f || sqlite3_exec(db, "CREATE TABLE raw_external(id TEXT PRIMARY KEY,json TEXT NOT NULL)", NULL, NULL, NULL) != SQLITE_OK) goto fail;
    if (sqlite3_prepare_v2(db, "INSERT INTO raw_external VALUES(json_extract(?1,'$.id'),?1)", -1, &ins, NULL) != SQLITE_OK) goto fail;
    while (getline(&line, &cap, f) >= 0) {
        sqlite3_bind_text(ins, 1, line, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(ins) != SQLITE_DONE) goto fail;
        sqlite3_reset(ins); sqlite3_clear_bindings(ins);
    }
    sqlite3_finalize(ins); free(line); fclose(f); return 1;
fail:
    sqlite3_finalize(ins); free(line); if (f) fclose(f); return 0;
}
static char *raw_lookup(sqlite3 *db, const char *id) {
    sqlite3_stmt *st = NULL; char *out = NULL;
    if (sqlite3_prepare_v2(db, "SELECT json FROM raw_external WHERE id=?1", -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) out = strdup((const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st); return out;
}

static int finish_row(sqlite3 *db, FILE *out, Buf *row, Metrics *m,
                      const char *kind, size_t human_count,
                      long long edges, long long bound_count) {
    char *canon = bf_stage_json_ascii_canonical(db, row->data);
    if (!canon || !kind_inc(m, kind)) { free(canon); return 0; }
    if (fprintf(out, "%s\n", row->data) < 0) { free(canon); return 0; }
    bf_sha256_update(&m->sha, (const uint8_t *)canon, strlen(canon));
    bf_sha256_update(&m->sha, (const uint8_t *)"\n", 1);
    free(canon);
    m->records++; m->edges += edges;
    if (bound_count) m->with_bindings++;
    m->unbound += (long long)human_count - bound_count;
    return 1;
}
static int put_key(sqlite3 *db, Buf *b, const char *key, int *first) {
    if (!*first && !putc1(b, ',')) return 0;
    *first = 0;
    return put_json_string(db, b, key) && putc1(b, ':');
}
static int put_raw_field(sqlite3 *db, Buf *b, const char *key, const char *raw, int *first) {
    return put_key(db, b, key, first) && putsb(b, raw ? raw : "null");
}
static int put_text_field(sqlite3 *db, Buf *b, const char *key, const char *value, int *first) {
    return put_key(db, b, key, first) && put_json_string(db, b, value ? value : "");
}

static int compile_external(sqlite3 *db, const BfStageChains *chains, const char *path, FILE *out, Metrics *m) {
    FILE *f = fopen(path, "r"); char *line = NULL; size_t cap = 0; int ok = 0;
    if (!f) return 0;
    while (getline(&line, &cap, f) >= 0) {
        char *id = json_extract_dup(db, line, "$.id"), *name = json_extract_dup(db, line, "$.name");
        char *kind = json_extract_dup(db, line, "$.record_kind"), *raw = id ? raw_lookup(db, id) : NULL;
        char *truth = raw ? json_extract_dup(db, raw, "$.truth_level") : NULL;
        char *human_raw = json_extract_dup(db, line, "$.human_chains");
        char *aff_raw = raw ? json_extract_dup(db, raw, "$.projection.candidate_affordances") : NULL;
        char *role_raw = raw ? json_extract_dup(db, raw, "$.projection.estate_roles") : NULL;
        char *domains_raw = raw ? json_extract_dup(db, raw, "$.projection.direct_domains") : NULL;
        BfStageStrings human = {0}, aff = {0}, roles = {0}, hints = {0};
        Buf row = {0}, binds = {0}; int first = 1; long long edges = 0, bound = 0;
        if (!id || !name || !kind || !raw || !truth || !human_raw || !aff_raw || !role_raw || !domains_raw ||
            !array_values(db, line, "$.human_chains", &human) ||
            !array_values(db, raw, "$.projection.candidate_affordances", &aff) ||
            !array_values(db, raw, "$.projection.estate_roles", &roles) ||
            !append_list_unique(&hints, &aff) || !append_list_unique(&hints, &roles) ||
            !put_bindings(db, &binds, chains, &human, &hints, 0, 0, &edges, &bound) ||
            !putc1(&row, '{') ||
            !put_raw_field(db, &row, "basis", "{}", &first)) goto row_fail;
        row.len = 1; row.data[1] = 0; first = 1;
        if (!put_key(db, &row, "basis", &first) || !putc1(&row, '{')) goto row_fail;
        int bf = 1;
        if (!put_raw_field(db, &row, "candidate_affordances", aff_raw, &bf) ||
            !put_raw_field(db, &row, "direct_domains", domains_raw, &bf) ||
            !put_raw_field(db, &row, "reagent_roles", role_raw, &bf) ||
            !put_text_field(db, &row, "truth_level", truth, &bf) || !putc1(&row, '}') ||
            !put_raw_field(db, &row, "cannot_infer",
              "[\"stage fit -> source admission\",\"stage fit -> authority\",\"stage fit -> effect\"]", &first) ||
            !put_raw_field(db, &row, "chain_bindings", binds.data, &first) ||
            !put_raw_field(db, &row, "human_chains", human_raw, &first) ||
            !put_text_field(db, &row, "id", id, &first) ||
            !put_text_field(db, &row, "name", name, &first) ||
            !put_text_field(db, &row, "record_kind", kind, &first) || !putc1(&row, '}') ||
            !finish_row(db, out, &row, m, kind, human.count, edges, bound)) goto row_fail;
        free(id); free(name); free(kind); free(raw); free(truth); free(human_raw); free(aff_raw); free(role_raw); free(domains_raw);
        bf_stage_strings_free(&human); bf_stage_strings_free(&aff); bf_stage_strings_free(&roles); bf_stage_strings_free(&hints);
        free(row.data); free(binds.data); continue;
row_fail:
        free(id); free(name); free(kind); free(raw); free(truth); free(human_raw); free(aff_raw); free(role_raw); free(domains_raw);
        bf_stage_strings_free(&human); bf_stage_strings_free(&aff); bf_stage_strings_free(&roles); bf_stage_strings_free(&hints);
        free(row.data); free(binds.data); goto done;
    }
    ok = 1;
done:
    free(line); fclose(f); return ok;
}

static int compile_simple(sqlite3 *db, const BfStageChains *chains, const char *path,
                          FILE *out, Metrics *m, int ghost) {
    FILE *f = fopen(path, "r"); char *line = NULL; size_t cap = 0; int ok = 0;
    if (!f) return 0;
    while (getline(&line, &cap, f) >= 0) {
        char *id = json_extract_dup(db, line, "$.id"), *name = json_extract_dup(db, line, "$.name");
        char *kind = json_extract_dup(db, line, "$.record_kind");
        char *human_raw = json_extract_dup(db, line, "$.human_chains");
        char *source = ghost ? NULL : json_extract_dup(db, line, "$.source_ref");
        char *categories = ghost ? json_extract_dup(db, line, "$.categories") : NULL;
        char *families = ghost ? json_extract_dup(db, line, "$.presentation_families") : NULL;
        char *mixes = ghost ? json_extract_dup(db, line, "$.renderer_mixes") : NULL;
        int reference_only = ghost ? 0 : json_bool(db, line, "$.reference_only");
        BfStageStrings human = {0}, hints = {0};
        Buf row = {0}, binds = {0}; int first = 1; long long edges = 0, bound = 0;
        if (!id || !name || !kind || !human_raw || !array_values(db, line, "$.human_chains", &human)) goto row_fail;
        if (!ghost && !reference_only && !bf_stage_app_hints(name, &hints)) goto row_fail;
        if (!put_bindings(db, &binds, chains, &human, &hints, ghost, reference_only, &edges, &bound) ||
            !putc1(&row, '{') || !put_key(db, &row, "basis", &first) || !putc1(&row, '{')) goto row_fail;
        int bf = 1;
        if (ghost) {
            if (!categories || !families || !mixes ||
                !put_raw_field(db, &row, "categories", categories, &bf) ||
                !put_raw_field(db, &row, "presentation_families", families, &bf) ||
                !put_raw_field(db, &row, "renderer_mixes", mixes, &bf)) goto row_fail;
        } else {
            if (!source || !put_text_field(db, &row, "source_ref", source, &bf) ||
                !put_text_field(db, &row, "mapping",
                    reference_only ? "declared interaction evidence" : "name+declared estate role", &bf) ||
                !put_key(db, &row, "reference_only", &bf) ||
                !putsb(&row, reference_only ? "true" : "false")) goto row_fail;
        }
        if (!putc1(&row, '}')) goto row_fail;
        if (ghost) {
            if (!put_raw_field(db, &row, "cannot_infer",
                "[\"presentation stage fit -> semantic ownership\",\"reference -> copied theme code\"]", &first)) goto row_fail;
        } else {
            if (!put_raw_field(db, &row, "cannot_infer", reference_only ?
                "[\"tool stage fit -> semantic ownership\",\"tool stage fit -> authority\",\"interaction reference -> executable stage\"]" :
                "[\"tool stage fit -> semantic ownership\",\"tool stage fit -> authority\",\"\"]", &first)) goto row_fail;
        }
        if (!put_raw_field(db, &row, "chain_bindings", binds.data, &first) ||
            !put_raw_field(db, &row, "human_chains", human_raw, &first) ||
            !put_text_field(db, &row, "id", id, &first) ||
            !put_text_field(db, &row, "name", name, &first) ||
            !put_text_field(db, &row, "record_kind", kind, &first) || !putc1(&row, '}') ||
            !finish_row(db, out, &row, m, kind, human.count, edges, bound)) goto row_fail;
        free(id); free(name); free(kind); free(human_raw); free(source); free(categories); free(families); free(mixes);
        bf_stage_strings_free(&human); bf_stage_strings_free(&hints); free(row.data); free(binds.data); continue;
row_fail:
        free(id); free(name); free(kind); free(human_raw); free(source); free(categories); free(families); free(mixes);
        bf_stage_strings_free(&human); bf_stage_strings_free(&hints); free(row.data); free(binds.data); goto done;
    }
    ok = 1;
done:
    free(line); fclose(f); return ok;
}

static long long line_count(const char *path) {
    FILE *f = fopen(path, "r"); long long n = 0; int c;
    if (!f) return -1;
    while ((c = fgetc(f)) != EOF) if (c == '\n') n++;
    fclose(f); return n;
}
static void metrics_free(Metrics *m) {
    for (size_t i = 0; i < m->kind_count; ++i) free(m->kinds[i].kind);
    free(m->kinds);
}
static int write_manifest(sqlite3 *db, const BfStageCompileOptions *o,
                          const BfStageChains *chains, Metrics *m, const char digest[65]) {
    FILE *f = fopen(o->manifest_json, "w");
    long long expected = line_count(o->external_bindings) + line_count(o->ghost_bindings) + line_count(o->app_bindings);
    if (!f || expected < 0) { if (f) fclose(f); return 0; }
    qsort(m->kinds, m->kind_count, sizeof(*m->kinds), kind_cmp);
    Buf b = {0}; int first = 1;
    if (!putc1(&b, '{') ||
        !put_key(db, &b, "binding_records", &first)) goto fail;
    char num[64]; snprintf(num, sizeof(num), "%lld", m->records); if (!putsb(&b, num)) goto fail;
    if (!put_key(db, &b, "chain_stage_edges", &first)) goto fail;
    snprintf(num, sizeof(num), "%lld", m->edges); if (!putsb(&b, num)) goto fail;
    if (!put_key(db, &b, "coverage", &first) || !putsb(&b, m->records == expected ? "true" : "false") ||
        !put_text_field(db, &b, "digest", digest, &first) ||
        !put_key(db, &b, "expected_binding_records", &first)) goto fail;
    snprintf(num, sizeof(num), "%lld", expected); if (!putsb(&b, num)) goto fail;
    if (!put_key(db, &b, "human_chain_count", &first)) goto fail;
    snprintf(num, sizeof(num), "%zu", chains->count); if (!putsb(&b, num)) goto fail;
    if (!put_key(db, &b, "record_kind_counts", &first) || !putc1(&b, '{')) goto fail;
    for (size_t i = 0; i < m->kind_count; ++i) {
        if (i && !putc1(&b, ',')) goto fail;
        if (!put_json_string(db, &b, m->kinds[i].kind) || !putc1(&b, ':')) goto fail;
        snprintf(num, sizeof(num), "%lld", m->kinds[i].count); if (!putsb(&b, num)) goto fail;
    }
    if (!putc1(&b, '}') || !put_key(db, &b, "records_with_stage_bindings", &first)) goto fail;
    snprintf(num, sizeof(num), "%lld", m->with_bindings); if (!putsb(&b, num)) goto fail;
    if (!put_text_field(db, &b, "schema", "bonfyre.experience-stage-map.v1", &first) ||
        !put_key(db, &b, "unbound_experience_references", &first)) goto fail;
    snprintf(num, sizeof(num), "%lld", m->unbound); if (!putsb(&b, num) || !putc1(&b, '}')) goto fail;
    fprintf(f, "%s\n", b.data); free(b.data); fclose(f); return m->records == expected;
fail:
    free(b.data); fclose(f); return 0;
}

int bf_stage_compile(const BfStageCompileOptions *o) {
    sqlite3 *db = NULL; BfStageChains chains = {0}; FILE *out = NULL; Metrics m = {0};
    uint8_t digest_raw[32]; char digest[65]; int ok = 0;
    if (!o || sqlite3_open(":memory:", &db) != SQLITE_OK || !bf_stage_chains_load(o->chains_tsv, &chains) ||
        !raw_table_load(db, o->external_raw)) goto done;
    out = fopen(o->output_jsonl, "w");
    if (!out) goto done;
    bf_sha256_init(&m.sha);
    if (!compile_external(db, &chains, o->external_bindings, out, &m) ||
        !compile_simple(db, &chains, o->ghost_bindings, out, &m, 1) ||
        !compile_simple(db, &chains, o->app_bindings, out, &m, 0)) goto done;
    if (fclose(out) != 0) { out = NULL; goto done; }
    out = NULL;
    bf_sha256_final(&m.sha, digest_raw); bf_sha256_digest_hex(digest_raw, digest);
    if (!write_manifest(db, o, &chains, &m, digest)) goto done;
    printf("{\"binding_records\":%lld,\"chain_stage_edges\":%lld,\"records_with_stage_bindings\":%lld,"
           "\"unbound_experience_references\":%lld,\"human_chain_count\":%zu,\"digest\":\"%s\"}\n",
           m.records, m.edges, m.with_bindings, m.unbound, chains.count, digest);
    ok = 1;
done:
    if (out) fclose(out);
    metrics_free(&m); bf_stage_chains_free(&chains); if (db) sqlite3_close(db);
    return ok ? 0 : 1;
}
