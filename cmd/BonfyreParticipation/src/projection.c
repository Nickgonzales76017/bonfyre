#define _POSIX_C_SOURCE 200809L
#include "projection.h"
#include "json.h"
#include "participation_contract.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    long long total;
    long long candidate[7];
    long long measured[7];
    long long join_power[16];
    long long braid_depth[32];
} PartMetrics;

static int cstr_in(const char *value, const char *const *items, size_t count) {
    if (!value) return 0;
    for (size_t i = 0; i < count; ++i) if (!strcmp(value, items[i])) return 1;
    return 0;
}

static int set_intersects_array(const BfPartStrings *set, const char *const *items, size_t count) {
    for (size_t i = 0; i < set->count; ++i) if (cstr_in(set->items[i], items, count)) return 1;
    return 0;
}

static int set_intersects_named(const BfPartStrings *set, const BfPartSet *named) {
    return set_intersects_array(set, named->members, named->member_count);
}

static int filtered_array(sqlite3 *db, const char *json, const char *path,
                          const char *const *excluded, size_t excluded_count,
                          BfPartStrings *out) {
    BfPartStrings raw = {0};
    if (!bf_part_json_array(db, json, path, &raw)) return 0;
    memset(out, 0, sizeof(*out));
    for (size_t i = 0; i < raw.count; ++i)
        if (!cstr_in(raw.items[i], excluded, excluded_count) && !bf_part_strings_add(out, raw.items[i])) {
            bf_part_strings_free(&raw);
            bf_part_strings_free(out);
            return 0;
        }
    bf_part_strings_free(&raw);
    return 1;
}

static int union_into(BfPartStrings *dst, const BfPartStrings *src) {
    for (size_t i = 0; i < src->count; ++i) if (!bf_part_strings_add(dst, src->items[i])) return 0;
    return 1;
}

static int join_power(const BfPartStrings *chains) {
    int power = 0, residual = 0;
    for (size_t i = 0; i < bf_part_join_families_COUNT; ++i)
        if (set_intersects_named(chains, &bf_part_join_families[i])) power++;
    for (size_t i = 0; i < chains->count && !residual; ++i) {
        int covered = 0;
        for (size_t j = 0; j < bf_part_join_families_COUNT && !covered; ++j)
            covered = cstr_in(chains->items[i], bf_part_join_families[j].members,
                              bf_part_join_families[j].member_count);
        if (!covered) residual = 1;
    }
    return power + residual;
}

static int candidate_row(sqlite3 *db, const char *json, PartMetrics *m) {
    BfPartStrings human = {0}, protocols = {0}, direct = {0}, support = {0}, roles = {0};
    BfPartStrings chains = {0}, econ = {0};
    char *url = NULL;
    int ok = 0, power;
    if (!filtered_array(db, json, "$.human_chains", bf_part_generic_human, bf_part_generic_human_COUNT, &human) ||
        !filtered_array(db, json, "$.protocol_chains", bf_part_generic_protocol, bf_part_generic_protocol_COUNT, &protocols) ||
        !bf_part_json_array(db, json, "$.direct_domains", &direct) ||
        !bf_part_json_array(db, json, "$.support_chains", &support) ||
        !bf_part_json_array(db, json, "$.reagent_roles", &roles))
        goto done;
    if (!union_into(&chains, &human) || !union_into(&chains, &support) ||
        !union_into(&econ, &direct) || !union_into(&econ, &chains) || !union_into(&econ, &protocols))
        goto done;
    url = bf_part_json_text(db, json, "$.url");
    m->total++;
    m->candidate[0]++;
    if (url && *url) m->candidate[1]++;
    if (direct.count) m->candidate[2]++;
    if (protocols.count) m->candidate[3]++;
    if (set_intersects_array(&roles, bf_part_capability_roles, bf_part_capability_roles_COUNT)) m->candidate[4]++;
    if (set_intersects_array(&econ, bf_part_economic_terms, bf_part_economic_terms_COUNT)) m->candidate[5]++;
    if (set_intersects_array(&roles, bf_part_roundtrip_roles, bf_part_roundtrip_roles_COUNT) ||
        set_intersects_array(&human, bf_part_roundtrip_human, bf_part_roundtrip_human_COUNT) ||
        set_intersects_array(&protocols, bf_part_roundtrip_protocols, bf_part_roundtrip_protocols_COUNT))
        m->candidate[6]++;
    power = join_power(&chains);
    if (power < 0 || power >= (int)(sizeof(m->join_power) / sizeof(m->join_power[0]))) goto done;
    m->join_power[power]++;
    ok = 1;
done:
    free(url);
    bf_part_strings_free(&human); bf_part_strings_free(&protocols); bf_part_strings_free(&direct);
    bf_part_strings_free(&support); bf_part_strings_free(&roles); bf_part_strings_free(&chains); bf_part_strings_free(&econ);
    return ok;
}

static int load_candidates(sqlite3 *db, const char *path, PartMetrics *m) {
    FILE *f = fopen(path, "r");
    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    if (!f) { fprintf(stderr, "participation: cannot open bindings %s\n", path); return 0; }
    long long row = 0;
    while ((n = getline(&line, &cap, f)) >= 0) {
        (void)n;
        ++row;
        if (!candidate_row(db, line, m)) {
            fprintf(stderr, "participation: bindings rejected at %s:%lld\n", path, row);
            free(line); fclose(f); return 0;
        }
    }
    free(line);
    fclose(f);
    return 1;
}

static int axis_index(const char *axis) {
    for (size_t i = 0; i < bf_part_axes_COUNT; ++i) if (!strcmp(axis, bf_part_axes[i])) return (int)i;
    return -1;
}

static int measured_counts(sqlite3 *db, PartMetrics *m) {
    sqlite3_stmt *st = NULL;
    m->measured[0] = m->total;
    if (sqlite3_prepare_v2(db,
        "SELECT axis,COUNT(DISTINCT occurrence) FROM participation_fact GROUP BY axis",
        -1, &st, NULL) != SQLITE_OK) return 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *axis = (const char *)sqlite3_column_text(st, 0);
        int idx = axis_index(axis);
        if (idx > 0) m->measured[idx] = sqlite3_column_int64(st, 1);
    }
    sqlite3_finalize(st);
    return 1;
}

static int braid_counts(sqlite3 *db, PartMetrics *m) {
    sqlite3_stmt *st = NULL;
    const char *sql =
        "WITH RECURSIVE walk(occ,node,depth,path) AS ("
        " SELECT DISTINCT occurrence,'ExternalEstate',0,'|ExternalEstate|' FROM participation_edge"
        " UNION ALL "
        " SELECT w.occ,e.target,w.depth+1,w.path||e.target||'|'"
        " FROM walk w JOIN participation_edge e ON e.occurrence=w.occ AND e.source=w.node"
        " WHERE instr(w.path,'|'||e.target||'|')=0"
        ") SELECT occ,MAX(depth) FROM walk GROUP BY occ";
    m->braid_depth[0] = m->total;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
    while (sqlite3_step(st) == SQLITE_ROW) {
        int depth = sqlite3_column_int(st, 1);
        if (depth > 0 && depth < (int)(sizeof(m->braid_depth) / sizeof(m->braid_depth[0]))) {
            m->braid_depth[0]--;
            m->braid_depth[depth]++;
        }
    }
    sqlite3_finalize(st);
    return 1;
}

static void emit_counts(FILE *f, const long long *values, size_t count, int skip_zero) {
    int first = 1;
    fputc('{', f);
    for (size_t i = 0; i < count; ++i) {
        if (skip_zero && !values[i]) continue;
        if (!first) fputc(',', f);
        fprintf(f, "\"%zu\":%lld", i, values[i]);
        first = 0;
    }
    fputc('}', f);
}

static int write_manifest(const char *path, const PartMetrics *m, long long raw_rows) {
    FILE *f = fopen(path, "w");
    if (!f) return 0;
    fprintf(f, "{\"braid_depth_distribution\":");
    emit_counts(f, m->braid_depth, sizeof(m->braid_depth) / sizeof(m->braid_depth[0]), 1);
    fprintf(f, ",\"candidate_axis_counts\":{");
    for (size_t i = 0; i < bf_part_axes_COUNT; ++i)
        fprintf(f, "%s\"%s\":%lld", i ? "," : "", bf_part_axes[i], m->candidate[i]);
    fprintf(f, "},\"cannot_infer\":["
        "\"unknown realized depth -> zero realized depth\","
        "\"high investigation priority -> source admission, authority or value\","
        "\"latent join power -> semantic compatibility or permission to join data\""
        "],\"frontier_limit\":%lld,\"join_power_distribution\":" ,
        m->total < 100 ? m->total : 100);
    emit_counts(f, m->join_power, sizeof(m->join_power) / sizeof(m->join_power[0]), 1);
    fprintf(f, ",\"measured_axis_counts\":{");
    int first = 1;
    for (size_t i = 0; i < bf_part_axes_COUNT; ++i) {
        if (!m->measured[i]) continue;
        fprintf(f, "%s\"%s\":%lld", first ? "" : ",", bf_part_axes[i], m->measured[i]);
        first = 0;
    }
    fprintf(f,
        "},\"owner_evidence_rows\":%lld,"
        "\"schema\":\"bonfyre.external-participation-manifest.v2\","
        "\"total_occurrences\":%lld,"
        "\"truth_level\":\"compiled_diagnostic_projection\"}\n",
        raw_rows, m->total);
    fclose(f);
    return 1;
}

int bf_part_project_manifest(sqlite3 *db, const char *external_bindings,
                             long long raw_evidence_rows, const char *output_path) {
    PartMetrics m = {0};
    if (!db || !external_bindings || !output_path) {
        fputs("participation: missing manifest inputs\n", stderr); return 1;
    }
    if (!load_candidates(db, external_bindings, &m)) {
        fputs("participation: candidate projection failed\n", stderr); return 1;
    }
    if (!measured_counts(db, &m)) {
        fputs("participation: measured-axis accounting failed\n", stderr); return 1;
    }
    if (!braid_counts(db, &m)) {
        fputs("participation: evidence-chain accounting failed\n", stderr); return 1;
    }
    if (!write_manifest(output_path, &m, raw_evidence_rows)) {
        fprintf(stderr, "participation: cannot write manifest %s\n", output_path); return 1;
    }
    printf("{\"total_occurrences\":%lld,\"owner_evidence_rows\":%lld,"
           "\"candidate\":{\"capability\":%lld,\"discovery\":%lld,\"economic\":%lld,"
           "\"observation\":%lld,\"protocol\":%lld,\"round_trip\":%lld,\"semantic\":%lld},"
           "\"measured\":{\"discovery\":%lld,\"observation\":%lld,\"semantic\":%lld,\"protocol\":%lld},"
           "\"braid\":{\"0\":%lld,\"1\":%lld,\"3\":%lld}}\n",
           m.total, raw_evidence_rows,
           m.candidate[4], m.candidate[0], m.candidate[5], m.candidate[1],
           m.candidate[3], m.candidate[6], m.candidate[2],
           m.measured[0], m.measured[1], m.measured[2], m.measured[3],
           m.braid_depth[0], m.braid_depth[1], m.braid_depth[3]);
    return 0;
}
