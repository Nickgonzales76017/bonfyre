#define _POSIX_C_SOURCE 200809L
#include "activation.h"
#include "json.h"
#include "participation_contract.h"
#include "activation_contract.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *id;
    char *name;
    char *source_kind;
    char *url;
    int planes[7];
    int join_power;
    int chain_span;
    int plane_count;
    int measured[7];
    int depth[7];
    int braid_depth;
    int branch_points;
    int rejoin_points;
    int braid_excess;
    int unknown_axes;
    int investigation_priority;
    unsigned long long modes;
    int activation_score;
    const char *next_owner;
    const char *next_action;
    const char *next_why;
} ActivationRow;

typedef struct {
    char *name;
    long long count;
} NameCount;

static int cstr_in(const char *value, const char *const *items, size_t count) {
    if (!value) return 0;
    for (size_t i = 0; i < count; ++i) if (!strcmp(value, items[i])) return 1;
    return 0;
}

static int set_intersects_array(const BfPartStrings *set, const char *const *items, size_t count) {
    for (size_t i = 0; i < set->count; ++i) if (cstr_in(set->items[i], items, count)) return 1;
    return 0;
}

static int filtered_array(sqlite3 *db, const char *json, const char *path,
                          const char *const *excluded, size_t excluded_count,
                          BfPartStrings *out) {
    BfPartStrings raw = {0};
    if (!bf_part_json_array(db, json, path, &raw)) return 0;
    memset(out, 0, sizeof(*out));
    for (size_t i = 0; i < raw.count; ++i)
        if (!cstr_in(raw.items[i], excluded, excluded_count) && !bf_part_strings_add(out, raw.items[i])) {
            bf_part_strings_free(&raw); bf_part_strings_free(out); return 0;
        }
    bf_part_strings_free(&raw);
    return 1;
}

static int union_into(BfPartStrings *dst, const BfPartStrings *src) {
    for (size_t i = 0; i < src->count; ++i) if (!bf_part_strings_add(dst, src->items[i])) return 0;
    return 1;
}

static const BfPartSet *named_set(const char *name, const BfPartSet *sets, size_t count) {
    if (!name || !*name) return NULL;
    for (size_t i = 0; i < count; ++i) if (!strcmp(name, sets[i].name)) return &sets[i];
    return NULL;
}

static int role_class_present(const BfPartStrings *roles, const char *name) {
    const BfPartSet *set = named_set(name, bf_part_role_classes, bf_part_role_classes_COUNT);
    return set && set_intersects_array(roles, set->members, set->member_count);
}

static int candidate_plane_index(const char *name) {
    for (size_t i = 0; i < bf_part_axes_COUNT; ++i) if (!strcmp(name, bf_part_axes[i])) return (int)i;
    return -1;
}

static int candidate_join_power(const BfPartStrings *chains) {
    int power = 0, residual = 0;
    for (size_t i = 0; i < bf_part_join_families_COUNT; ++i)
        if (set_intersects_array(chains, bf_part_join_families[i].members, bf_part_join_families[i].member_count)) power++;
    for (size_t i = 0; i < chains->count && !residual; ++i) {
        int covered = 0;
        for (size_t j = 0; j < bf_part_join_families_COUNT && !covered; ++j)
            covered = cstr_in(chains->items[i], bf_part_join_families[j].members,
                              bf_part_join_families[j].member_count);
        if (!covered) residual = 1;
    }
    return power + residual;
}

static int candidate(sqlite3 *db, const char *json, ActivationRow *row,
                     BfPartStrings *roles, BfPartStrings *protocols) {
    BfPartStrings human = {0}, direct = {0}, support = {0}, chains = {0}, economic = {0};
    int ok = 0;
    if (!filtered_array(db, json, "$.human_chains", bf_part_generic_human, bf_part_generic_human_COUNT, &human) ||
        !filtered_array(db, json, "$.protocol_chains", bf_part_generic_protocol, bf_part_generic_protocol_COUNT, protocols) ||
        !bf_part_json_array(db, json, "$.direct_domains", &direct) ||
        !bf_part_json_array(db, json, "$.support_chains", &support) ||
        !bf_part_json_array(db, json, "$.reagent_roles", roles))
        goto done;
    if (!union_into(&chains, &human) || !union_into(&chains, &support) ||
        !union_into(&economic, &direct) || !union_into(&economic, &chains) || !union_into(&economic, protocols))
        goto done;
    row->planes[0] = 1;
    row->planes[1] = row->url && *row->url;
    row->planes[2] = direct.count > 0;
    row->planes[3] = protocols->count > 0;
    row->planes[4] = set_intersects_array(roles, bf_part_capability_roles, bf_part_capability_roles_COUNT);
    row->planes[5] = set_intersects_array(&economic, bf_part_economic_terms, bf_part_economic_terms_COUNT);
    row->planes[6] =
        set_intersects_array(roles, bf_part_roundtrip_roles, bf_part_roundtrip_roles_COUNT) ||
        set_intersects_array(&human, bf_part_roundtrip_human, bf_part_roundtrip_human_COUNT) ||
        set_intersects_array(protocols, bf_part_roundtrip_protocols, bf_part_roundtrip_protocols_COUNT);
    for (size_t i = 0; i < bf_part_axes_COUNT; ++i) row->plane_count += row->planes[i] != 0;
    row->chain_span = (int)chains.count;
    row->join_power = candidate_join_power(&chains);
    ok = 1;
done:
    bf_part_strings_free(&human); bf_part_strings_free(&direct); bf_part_strings_free(&support);
    bf_part_strings_free(&chains); bf_part_strings_free(&economic);
    return ok;
}

static int ladder_depth(const char *axis, const char *state, const char **depth_name) {
    for (size_t i = 0; i < bf_part_ladders_COUNT; ++i)
        if (!strcmp(axis, bf_part_ladders[i].axis) && !strcmp(state, bf_part_ladders[i].state)) {
            if (depth_name) *depth_name = bf_part_ladders[i].depth_name;
            return bf_part_ladders[i].depth;
        }
    if (depth_name) *depth_name = "owner-fact";
    return 1;
}

static int realized_axis(sqlite3 *db, const char *occ, int axis_index, int *measured, int *depth) {
    sqlite3_stmt *st = NULL;
    int count = 0, observed = 0, best = 0;
    const char *axis = bf_part_axes[axis_index];
    if (axis_index == 0) { *measured = 1; *depth = 1; return 1; }
    if (sqlite3_prepare_v2(db,
        "SELECT state FROM participation_fact WHERE occurrence=?1 AND axis=?2",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, occ, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, axis, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *state = (const char *)sqlite3_column_text(st, 0);
        int d = ladder_depth(axis, state ? state : "", NULL);
        count++;
        if (d > best) best = d;
        if (axis_index == 1 && state && (!strcmp(state, "source_observed") || !strcmp(state, "source_retained"))) observed++;
    }
    sqlite3_finalize(st);
    if (axis_index == 1 && observed >= 2) {
        int repeat = ladder_depth("observation", "repeat_observed", NULL);
        if (repeat > best) best = repeat;
    }
    *measured = count > 0;
    *depth = count ? best : 0;
    return 1;
}

static long long scalar_query(sqlite3 *db, const char *sql, const char *occ) {
    sqlite3_stmt *st = NULL;
    long long out = -1;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, occ, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) out = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return out;
}

static int braid(sqlite3 *db, const char *occ, ActivationRow *row) {
    long long transitions, owners, branches, rejoins, depth;
    transitions = scalar_query(db, "SELECT COUNT(*) FROM participation_edge WHERE occurrence=?1", occ);
    owners = scalar_query(db,
        "SELECT COUNT(*) FROM (SELECT source owner FROM participation_edge WHERE occurrence=?1 "
        "UNION SELECT target FROM participation_edge WHERE occurrence=?1)", occ);
    branches = scalar_query(db,
        "SELECT COUNT(*) FROM (SELECT source FROM participation_edge WHERE occurrence=?1 "
        "GROUP BY source HAVING COUNT(DISTINCT target)>1)", occ);
    rejoins = scalar_query(db,
        "SELECT COUNT(*) FROM (SELECT target FROM participation_edge WHERE occurrence=?1 "
        "GROUP BY target HAVING COUNT(DISTINCT source)>1)", occ);
    depth = scalar_query(db,
        "WITH RECURSIVE walk(node,depth,path) AS ("
        " SELECT 'ExternalEstate',0,'|ExternalEstate|'"
        " UNION ALL SELECT e.target,w.depth+1,w.path||e.target||'|'"
        " FROM walk w JOIN participation_edge e ON e.occurrence=?1 AND e.source=w.node"
        " WHERE instr(w.path,'|'||e.target||'|')=0)"
        " SELECT MAX(depth) FROM walk", occ);
    if (transitions < 0 || owners < 0 || branches < 0 || rejoins < 0 || depth < 0) return 0;
    row->braid_depth = (int)depth;
    row->branch_points = (int)branches;
    row->rejoin_points = (int)rejoins;
    row->braid_excess = (int)(transitions - (owners > 0 ? owners - 1 : 0));
    if (row->braid_excess < 0) row->braid_excess = 0;
    return 1;
}

static int mode_matches(const BfActivationMode *mode, const ActivationRow *row,
                        const BfPartStrings *roles, const BfPartStrings *protocols) {
    if (*mode->require_role_class && !role_class_present(roles, mode->require_role_class)) return 0;
    if (*mode->forbid_role_class && role_class_present(roles, mode->forbid_role_class)) return 0;
    if (*mode->require_candidate_plane) {
        int idx = candidate_plane_index(mode->require_candidate_plane);
        if (idx < 0 || !row->planes[idx]) return 0;
    }
    if (mode->join_power_min && row->join_power < mode->join_power_min) return 0;
    if (*mode->or_source_kind || *mode->or_protocol || *mode->or_role_class) {
        int any = 0;
        if (*mode->or_source_kind && row->source_kind && !strcmp(row->source_kind, mode->or_source_kind)) any = 1;
        if (*mode->or_protocol && bf_part_strings_has(protocols, mode->or_protocol)) any = 1;
        if (*mode->or_role_class && role_class_present(roles, mode->or_role_class)) any = 1;
        if (!any) return 0;
    }
    return 1;
}

static void classify_modes(ActivationRow *row, const BfPartStrings *roles, const BfPartStrings *protocols) {
    int bonus = 0;
    for (size_t i = 0; i < BF_ACTIVATION_MODE_COUNT; ++i) {
        if (mode_matches(&bf_activation_modes[i], row, roles, protocols)) {
            row->modes |= 1ULL << i;
            bonus += bf_activation_modes[i].bonus;
        }
    }
    if (!row->modes) row->modes = 1ULL << BF_ACTIVATION_MODE_COUNT;
    row->activation_score += bonus;
}

static void next_activation(ActivationRow *row) {
    for (size_t i = 0; i < BF_ACTIVATION_NEXT_RULE_COUNT; ++i) {
        const BfActivationNextRule *r = &bf_activation_next_rules[i];
        int plane = candidate_plane_index(r->candidate_plane);
        int axis = candidate_plane_index(r->test_axis);
        if (plane < 0 || axis < 0 || !row->planes[plane]) continue;
        if (r->require_unknown && row->measured[axis]) continue;
        if (r->depth_lt && row->depth[axis] >= r->depth_lt) continue;
        row->next_owner = r->owner;
        row->next_action = r->action;
        row->next_why = r->why;
        return;
    }
    row->next_owner = bf_activation_default_owner;
    row->next_action = bf_activation_default_action;
    row->next_why = bf_activation_default_why;
}

static int finish_row(sqlite3 *db, const char *json, ActivationRow *row) {
    BfPartStrings roles = {0}, protocols = {0};
    row->id = bf_part_json_text(db, json, "$.id");
    row->name = bf_part_json_text(db, json, "$.name");
    row->source_kind = bf_part_json_text(db, json, "$.source_kind");
    row->url = bf_part_json_text(db, json, "$.url");
    if (!row->id || !row->name || !row->source_kind || !candidate(db, json, row, &roles, &protocols)) goto fail;
    row->unknown_axes = 0;
    for (size_t i = 0; i < bf_part_axes_COUNT; ++i) {
        if (!realized_axis(db, row->id, (int)i, &row->measured[i], &row->depth[i])) goto fail;
        if (!row->measured[i]) row->unknown_axes++;
    }
    if (!braid(db, row->id, row)) goto fail;
    row->investigation_priority =
        row->join_power * bf_activation_score.investigation_join_power_weight +
        (row->chain_span < bf_activation_score.investigation_chain_span_cap ? row->chain_span : bf_activation_score.investigation_chain_span_cap) +
        row->plane_count * bf_activation_score.investigation_candidate_plane_weight +
        row->unknown_axes * bf_activation_score.investigation_unknown_axis_weight +
        row->braid_depth * bf_activation_score.investigation_braid_depth_weight +
        row->branch_points * bf_activation_score.investigation_branch_weight +
        row->rejoin_points * bf_activation_score.investigation_rejoin_weight +
        row->braid_excess * bf_activation_score.investigation_braid_excess_weight;
    row->activation_score =
        row->investigation_priority +
        row->join_power * bf_activation_score.activation_join_power_weight +
        (row->chain_span < bf_activation_score.activation_chain_span_cap ? row->chain_span : bf_activation_score.activation_chain_span_cap) +
        row->braid_depth * bf_activation_score.activation_braid_depth_weight;
    classify_modes(row, &roles, &protocols);
    next_activation(row);
    bf_part_strings_free(&roles); bf_part_strings_free(&protocols);
    return 1;
fail:
    bf_part_strings_free(&roles); bf_part_strings_free(&protocols);
    return 0;
}

static void row_free(ActivationRow *row) {
    free(row->id); free(row->name); free(row->source_kind); free(row->url);
    memset(row, 0, sizeof(*row));
}

static int row_cmp(const void *a, const void *b) {
    const ActivationRow *x = a, *y = b;
    if (x->activation_score != y->activation_score) return x->activation_score > y->activation_score ? -1 : 1;
    return strcmp(x->id, y->id);
}

static int quote_out(sqlite3 *db, FILE *f, const char *value) {
    char *q = bf_part_json_quote(db, value ? value : "");
    if (!q) return 0;
    int ok = fprintf(f, "%s", q) >= 0;
    free(q);
    return ok;
}

static const char *mode_name(size_t bit) {
    return bit < BF_ACTIVATION_MODE_COUNT ? bf_activation_modes[bit].name : bf_activation_default_mode;
}

static int write_modes(FILE *f, const ActivationRow *row, sqlite3 *db) {
    int first = 1;
    fputc('[', f);
    for (size_t i = 0; i <= BF_ACTIVATION_MODE_COUNT; ++i) if (row->modes & (1ULL << i)) {
        if (!first) fputc(',', f);
        if (!quote_out(db, f, mode_name(i))) return 0;
        first = 0;
    }
    fputc(']', f);
    return 1;
}

static int write_frontier_row(FILE *f, sqlite3 *db, const ActivationRow *r) {
    fprintf(f, "{\"activation_score\":%d,\"candidate_plane_count\":%d,"
               "\"cannot_infer\":[\"activation rank -> authority to observe or activate\","
               "\"leverage mode -> provider admission\",\"next owner -> guaranteed successful transition\"],"
               "\"chain_span\":%d,\"id\":", r->activation_score, r->plane_count, r->chain_span);
    if (!quote_out(db, f, r->id)) return 0;
    fprintf(f, ",\"latent_join_power\":%d,\"leverage_modes\":", r->join_power);
    if (!write_modes(f, r, db)) return 0;
    fputs(",\"name\":", f); if (!quote_out(db, f, r->name)) return 0;
    fputs(",\"next_activation\":{\"action\":", f); if (!quote_out(db, f, r->next_action)) return 0;
    fputs(",\"owner\":", f); if (!quote_out(db, f, r->next_owner)) return 0;
    fputs(",\"why\":", f); if (!quote_out(db, f, r->next_why)) return 0;
    fprintf(f, "},\"realized_braid_depth\":%d,\"schema\":\"bonfyre.external-activation-candidate.v1\","
               "\"source_kind\":", r->braid_depth);
    if (!quote_out(db, f, r->source_kind)) return 0;
    fputs(",\"url\":", f);
    if (r->url) { if (!quote_out(db, f, r->url)) return 0; } else fputs("null", f);
    fputs("}\n", f);
    return 1;
}

static int name_count_inc(NameCount **items, size_t *count, const char *name) {
    for (size_t i = 0; i < *count; ++i) if (!strcmp((*items)[i].name, name)) {
        (*items)[i].count++; return 1;
    }
    NameCount *next = realloc(*items, (*count + 1) * sizeof(*next));
    if (!next) return 0;
    *items = next;
    (*items)[*count].name = strdup(name);
    if (!(*items)[*count].name) return 0;
    (*items)[*count].count = 1;
    (*count)++;
    return 1;
}

static int name_count_cmp(const void *a, const void *b) {
    return strcmp(((const NameCount *)a)->name, ((const NameCount *)b)->name);
}

static long long name_count_get(const NameCount *items, size_t count, const char *name) {
    for (size_t i = 0; i < count; ++i) if (!strcmp(items[i].name, name)) return items[i].count;
    return 0;
}

static int write_manifest(FILE *f, sqlite3 *db, ActivationRow *rows, size_t count, int limit,
                          const long long *mode_counts, NameCount *owners, size_t owner_count) {
    fputs("{\"cannot_infer\":[\"rank -> source admission, authority, budget or value\","
          "\"cohort membership -> semantic equivalence between members\"],\"cohort_heads\":{", f);
    int first_mode = 1;
    for (size_t mode = 0; mode <= BF_ACTIVATION_MODE_COUNT; ++mode) {
        if (!mode_counts[mode]) continue;
        if (!first_mode) fputc(',', f);
        first_mode = 0;
        if (!quote_out(db, f, mode_name(mode))) return 0;
        fputs(":[", f);
        int emitted = 0;
        for (size_t i = 0; i < count && emitted < 10; ++i) if (rows[i].modes & (1ULL << mode)) {
            if (emitted++) fputc(',', f);
            fputs("{\"id\":", f); if (!quote_out(db, f, rows[i].id)) return 0;
            fputs(",\"name\":", f); if (!quote_out(db, f, rows[i].name)) return 0;
            fputs(",\"next_owner\":", f); if (!quote_out(db, f, rows[i].next_owner)) return 0;
            fprintf(f, ",\"score\":%d}", rows[i].activation_score);
        }
        fputc(']', f);
    }
    fprintf(f, "},\"frontier_rows\":%d,\"leverage_mode_counts\":{", limit);
    int first = 1;
    for (size_t i = 0; i <= BF_ACTIVATION_MODE_COUNT; ++i) if (mode_counts[i]) {
        if (!first) fputc(',', f); first = 0;
        if (!quote_out(db, f, mode_name(i))) return 0;
        fprintf(f, ":%lld", mode_counts[i]);
    }
    fputs("},\"next_owner_counts\":{", f);
    qsort(owners, owner_count, sizeof(*owners), name_count_cmp);
    for (size_t i = 0; i < owner_count; ++i) {
        if (i) fputc(',', f);
        if (!quote_out(db, f, owners[i].name)) return 0;
        fprintf(f, ":%lld", owners[i].count);
    }
    fprintf(f, "},\"schema\":\"bonfyre.external-activation-policy-manifest.v1\","
               "\"total_occurrences\":%zu,\"truth_level\":\"compiled_diagnostic_projection\"}\n", count);
    return 1;
}

int bf_part_activation_compile(sqlite3 *db, const char *external_bindings,
                               const char *manifest_path, const char *frontier_path,
                               int frontier_limit) {
    FILE *in = NULL, *frontier = NULL, *manifest = NULL;
    char *line = NULL;
    size_t cap = 0, count = 0, capacity = 0;
    ActivationRow *rows = NULL;
    NameCount *owners = NULL;
    size_t owner_count = 0;
    long long mode_counts[BF_ACTIVATION_MODE_COUNT + 1];
    int rc = 1;
    memset(mode_counts, 0, sizeof(mode_counts));
    if (!db || !external_bindings || !manifest_path || !frontier_path) return 1;
    in = fopen(external_bindings, "r");
    if (!in) goto done;
    while (getline(&line, &cap, in) >= 0) {
        if (count == capacity) {
            size_t next_cap = capacity ? capacity * 2 : 1024;
            ActivationRow *next = realloc(rows, next_cap * sizeof(*next));
            if (!next) goto done;
            rows = next; memset(rows + capacity, 0, (next_cap - capacity) * sizeof(*rows)); capacity = next_cap;
        }
        if (!finish_row(db, line, &rows[count])) goto done;
        count++;
    }
    fclose(in); in = NULL;
    qsort(rows, count, sizeof(*rows), row_cmp);
    for (size_t i = 0; i < count; ++i) {
        if (!name_count_inc(&owners, &owner_count, rows[i].next_owner)) goto done;
        for (size_t mode = 0; mode <= BF_ACTIVATION_MODE_COUNT; ++mode)
            if (rows[i].modes & (1ULL << mode)) mode_counts[mode]++;
    }
    if (frontier_limit < 1) frontier_limit = 1;
    if ((size_t)frontier_limit > count) frontier_limit = (int)count;
    frontier = fopen(frontier_path, "w");
    if (!frontier) goto done;
    for (int i = 0; i < frontier_limit; ++i) if (!write_frontier_row(frontier, db, &rows[i])) goto done;
    if (fclose(frontier) != 0) { frontier = NULL; goto done; }
    frontier = NULL;
    manifest = fopen(manifest_path, "w");
    if (!manifest || !write_manifest(manifest, db, rows, count, frontier_limit, mode_counts, owners, owner_count)) goto done;
    if (fclose(manifest) != 0) { manifest = NULL; goto done; }
    manifest = NULL;
    printf("{\"total_occurrences\":%zu,\"frontier_rows\":%d,\"top_id\":\"%s\",\"top_score\":%d,"
           "\"next_foreign_twin\":%lld,\"next_domain_profile\":%lld}\n",
           count, frontier_limit, count ? rows[0].id : "", count ? rows[0].activation_score : 0,
           name_count_get(owners, owner_count, "ForeignTwin"),
           name_count_get(owners, owner_count, "DomainProfile"));
    rc = 0;
done:
    if (in) fclose(in);
    if (frontier) fclose(frontier);
    if (manifest) fclose(manifest);
    free(line);
    for (size_t i = 0; i < count; ++i) row_free(&rows[i]);
    free(rows);
    for (size_t i = 0; i < owner_count; ++i) free(owners[i].name);
    free(owners);
    return rc;
}
