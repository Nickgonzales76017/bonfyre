#define _POSIX_C_SOURCE 200809L
#include "disturbance_contract.h"

#include <sqlite3.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

typedef struct { char **v; size_t n; } Strings;
typedef struct { char *key; int count; } CounterItem;
typedef struct { CounterItem *v; size_t n; } Counters;
typedef struct { char *source; char *target; } Edge;
typedef struct { Edge *v; size_t n; } Edges;

typedef struct {
    char *id;
    char *owner;
    Strings parents;
    int visit;
    int event_depth;
    int owner_depth;
} Event;
typedef struct { Event *v; size_t n; } Events;

typedef struct {
    sqlite3 *db;
    Strings failures;
    Strings owners;
    Strings disturbance_ids;
    Counters kinds;
    Counters verdicts;
    Edges edges;
    Events events;
    long long trace_events;
    int max_event_depth;
    int max_owner_depth;
    int cycle_reported;
} Audit;

static char *dup_text(const unsigned char *text) {
    return text ? strdup((const char *)text) : NULL;
}

static char *json_text(sqlite3 *db, const char *json, const char *path) {
    sqlite3_stmt *st = NULL;
    char *out = NULL;
    if (sqlite3_prepare_v2(db,
        "SELECT CASE WHEN json_type(?1,?2)='text' THEN json_extract(?1,?2) END",
        -1, &st, NULL) != SQLITE_OK) return NULL;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) out = dup_text(sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out;
}

static int json_valid(sqlite3 *db, const char *json) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_valid(?1)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) ok = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return ok;
}

static int json_type_is(sqlite3 *db, const char *json, const char *path, const char *type, int missing_ok) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (sqlite3_prepare_v2(db, "SELECT json_type(?1,?2)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const unsigned char *got = sqlite3_column_text(st, 0);
        ok = got ? !strcmp((const char *)got, type) : missing_ok;
    }
    sqlite3_finalize(st);
    return ok;
}

static int sv_add(Strings *s, const char *value) {
    char **next;
    if (!value) return 0;
    next = realloc(s->v, (s->n + 1) * sizeof(*next));
    if (!next) return 0;
    s->v = next;
    s->v[s->n] = strdup(value);
    if (!s->v[s->n]) return 0;
    s->n++;
    return 1;
}

static int sv_has(const Strings *s, const char *value) {
    for (size_t i = 0; i < s->n; ++i) if (!strcmp(s->v[i], value)) return 1;
    return 0;
}

static int sv_add_unique(Strings *s, const char *value) {
    return sv_has(s, value) ? 1 : sv_add(s, value);
}

static int cmp_string(const void *a, const void *b) {
    const char *const *aa = a, *const *bb = b;
    return strcmp(*aa, *bb);
}

static void sv_sort(Strings *s) {
    qsort(s->v, s->n, sizeof(*s->v), cmp_string);
}

static void sv_free(Strings *s) {
    for (size_t i = 0; i < s->n; ++i) free(s->v[i]);
    free(s->v);
    memset(s, 0, sizeof(*s));
}

static int failure(Audit *a, const char *fmt, ...) {
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    return sv_add(&a->failures, buf);
}

static int counter_add(Counters *c, const char *key) {
    CounterItem *next;
    if (!key) return 1;
    for (size_t i = 0; i < c->n; ++i) {
        if (!strcmp(c->v[i].key, key)) { c->v[i].count++; return 1; }
    }
    next = realloc(c->v, (c->n + 1) * sizeof(*next));
    if (!next) return 0;
    c->v = next;
    c->v[c->n].key = strdup(key);
    if (!c->v[c->n].key) return 0;
    c->v[c->n].count = 1;
    c->n++;
    return 1;
}

static int counter_get(const Counters *c, const char *key) {
    for (size_t i = 0; i < c->n; ++i) if (!strcmp(c->v[i].key, key)) return c->v[i].count;
    return 0;
}

static int cmp_counter(const void *a, const void *b) {
    const CounterItem *aa = a, *bb = b;
    return strcmp(aa->key, bb->key);
}

static void counter_sort(Counters *c) {
    qsort(c->v, c->n, sizeof(*c->v), cmp_counter);
}

static void counter_free(Counters *c) {
    for (size_t i = 0; i < c->n; ++i) free(c->v[i].key);
    free(c->v);
    memset(c, 0, sizeof(*c));
}

static int edge_add_unique(Edges *e, const char *source, const char *target) {
    Edge *next;
    if (!source || !target || !strcmp(source, target)) return 1;
    for (size_t i = 0; i < e->n; ++i)
        if (!strcmp(e->v[i].source, source) && !strcmp(e->v[i].target, target)) return 1;
    next = realloc(e->v, (e->n + 1) * sizeof(*next));
    if (!next) return 0;
    e->v = next;
    e->v[e->n].source = strdup(source);
    e->v[e->n].target = strdup(target);
    if (!e->v[e->n].source || !e->v[e->n].target) return 0;
    e->n++;
    return 1;
}

static int cmp_edge(const void *a, const void *b) {
    const Edge *aa = a, *bb = b;
    int c = strcmp(aa->source, bb->source);
    return c ? c : strcmp(aa->target, bb->target);
}

static void edges_free(Edges *e) {
    for (size_t i = 0; i < e->n; ++i) { free(e->v[i].source); free(e->v[i].target); }
    free(e->v);
    memset(e, 0, sizeof(*e));
}

static void event_free(Event *e) {
    free(e->id);
    free(e->owner);
    sv_free(&e->parents);
    memset(e, 0, sizeof(*e));
}

static int event_find(const Events *events, const char *id) {
    for (size_t i = 0; i < events->n; ++i) if (!strcmp(events->v[i].id, id)) return (int)i;
    return -1;
}

static int events_put(Events *events, Event *event, int *duplicate) {
    int found = event_find(events, event->id);
    Event *next;
    *duplicate = found >= 0;
    if (found >= 0) {
        event_free(&events->v[found]);
        events->v[found] = *event;
        memset(event, 0, sizeof(*event));
        return 1;
    }
    next = realloc(events->v, (events->n + 1) * sizeof(*next));
    if (!next) return 0;
    events->v = next;
    events->v[events->n++] = *event;
    memset(event, 0, sizeof(*event));
    return 1;
}

static void events_free(Events *events) {
    for (size_t i = 0; i < events->n; ++i) event_free(&events->v[i]);
    free(events->v);
    memset(events, 0, sizeof(*events));
}

static int json_string_array(sqlite3 *db, const char *json, const char *path, Strings *out, int *valid) {
    sqlite3_stmt *st = NULL;
    *valid = 1;
    if (!json_type_is(db, json, path, "array", 1)) { *valid = 0; return 1; }
    if (sqlite3_prepare_v2(db, "SELECT value,type FROM json_each(?1,?2)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, json, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *type = (const char *)sqlite3_column_text(st, 1);
        const char *value = (const char *)sqlite3_column_text(st, 0);
        if (!type || strcmp(type, "text") || !value || !*value) { *valid = 0; continue; }
        if (!sv_add(out, value)) { sqlite3_finalize(st); return 0; }
    }
    sqlite3_finalize(st);
    return 1;
}

static int contract_array(sqlite3 *db, const char *path, Strings *out) {
    int valid = 0;
    if (!json_string_array(db, bf_disturbance_contract_json, path, out, &valid)) return 0;
    return valid;
}

static int contract_int(sqlite3 *db, const char *path, int fallback) {
    sqlite3_stmt *st = NULL;
    int value = fallback;
    if (sqlite3_prepare_v2(db, "SELECT json_extract(?1,?2)", -1, &st, NULL) != SQLITE_OK) return fallback;
    sqlite3_bind_text(st, 1, bf_disturbance_contract_json, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_type(st, 0) != SQLITE_NULL) value = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return value;
}

static int contract_length(sqlite3 *db, const char *path) {
    sqlite3_stmt *st = NULL;
    int value = 0;
    if (sqlite3_prepare_v2(db, "SELECT COALESCE(json_array_length(json_extract(?1,?2)),0)", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, bf_disturbance_contract_json, -1, SQLITE_STATIC);
    sqlite3_bind_text(st, 2, path, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) value = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return value;
}

static char *contract_text(sqlite3 *db, const char *path) {
    return json_text(db, bf_disturbance_contract_json, path);
}

static int load_trace(Audit *a, const char *path) {
    FILE *f = fopen(path, "r");
    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    int row = 0;
    if (!f) return 1;
    while ((n = getline(&line, &cap, f)) >= 0) {
        Event event = {0};
        char *schema = NULL, *disturbance_id = NULL, *kind = NULL, *subject = NULL;
        char *evidence = NULL, *receipt = NULL, *verdict = NULL;
        int parents_valid = 1, duplicate = 0;
        if (n && line[n - 1] == '\n') line[--n] = '\0';
        if (!*line) continue;
        row++;
        a->trace_events++;
        if (!json_valid(a->db, line)) { failure(a, "row %d: invalid JSON", row); continue; }

        schema = json_text(a->db, line, "$.schema");
        event.id = json_text(a->db, line, "$.event_id");
        event.owner = json_text(a->db, line, "$.owner");
        disturbance_id = json_text(a->db, line, "$.disturbance_id");
        kind = json_text(a->db, line, "$.kind");
        subject = json_text(a->db, line, "$.subject");
        verdict = json_text(a->db, line, "$.verdict");
        if (event.owner) sv_add_unique(&a->owners, event.owner);
        if (kind) counter_add(&a->kinds, kind);
        if (event.owner && !strcmp(event.owner, "RoundTripTwin") && verdict) counter_add(&a->verdicts, verdict);

        if (!schema || strcmp(schema, "bonfyre.disturbance-event.v1")) failure(a, "row %d: wrong disturbance schema", row);
        if (!event.id || !*event.id) {
            failure(a, "row %d: missing event_id", row);
            free(schema); free(disturbance_id); free(kind); free(subject); free(evidence); free(receipt); free(verdict);
            event_free(&event);
            continue;
        }
        if (event_find(&a->events, event.id) >= 0) failure(a, "duplicate event_id %s", event.id);
        if (!event.owner || !*event.owner) failure(a, "%s: missing owner", event.id);
        if (!disturbance_id || !*disturbance_id) failure(a, "%s: missing disturbance_id", event.id);
        if (!kind || !*kind) failure(a, "%s: missing kind", event.id);
        if (!subject || !*subject) failure(a, "%s: missing subject", event.id);
        if (disturbance_id && *disturbance_id) sv_add_unique(&a->disturbance_ids, disturbance_id);

        evidence = json_text(a->db, line, "$.evidence_ref");
        if (!evidence || !*evidence) {
            free(evidence);
            evidence = NULL;
            receipt = json_text(a->db, line, "$.receipt");
            if (receipt) evidence = strdup(receipt);
        }
        if (!evidence) failure(a, "%s: missing owner evidence reference", event.id);

        if (!json_string_array(a->db, line, "$.causal_parents", &event.parents, &parents_valid)) {
            free(schema); free(disturbance_id); free(kind); free(subject); free(evidence); free(receipt); free(verdict);
            event_free(&event); free(line); fclose(f); return 0;
        }
        if (!parents_valid) failure(a, "%s: invalid causal_parents", event.id);
        if (!events_put(&a->events, &event, &duplicate)) {
            free(schema); free(disturbance_id); free(kind); free(subject); free(evidence); free(receipt); free(verdict);
            event_free(&event); free(line); fclose(f); return 0;
        }
        free(schema); free(disturbance_id); free(kind); free(subject); free(evidence); free(receipt); free(verdict);
    }
    free(line);
    fclose(f);
    return 1;
}

static int depth_event(Audit *a, int index, int *event_depth, int *owner_depth) {
    Event *e = &a->events.v[index];
    int best_event = 1, best_owner = 0;
    if (e->visit == 2) { *event_depth = e->event_depth; *owner_depth = e->owner_depth; return 1; }
    if (e->visit == 1) {
        if (!a->cycle_reported) { failure(a, "causal graph contains a cycle"); a->cycle_reported = 1; }
        *event_depth = 0; *owner_depth = 0; return 1;
    }
    e->visit = 1;
    for (size_t i = 0; i < e->parents.n; ++i) {
        int parent = event_find(&a->events, e->parents.v[i]);
        int pe = 0, po = 0;
        if (parent < 0) continue;
        if (!depth_event(a, parent, &pe, &po)) return 0;
        if (pe + 1 > best_event) best_event = pe + 1;
        if (po + ((a->events.v[parent].owner && e->owner && strcmp(a->events.v[parent].owner, e->owner)) ? 1 : 0) > best_owner)
            best_owner = po + ((a->events.v[parent].owner && e->owner && strcmp(a->events.v[parent].owner, e->owner)) ? 1 : 0);
    }
    e->visit = 2;
    e->event_depth = best_event;
    e->owner_depth = best_owner;
    if (best_event > a->max_event_depth) a->max_event_depth = best_event;
    if (best_owner > a->max_owner_depth) a->max_owner_depth = best_owner;
    *event_depth = best_event; *owner_depth = best_owner;
    return 1;
}

static int build_graph(Audit *a) {
    int roots = 0;
    for (size_t i = 0; i < a->events.n; ++i) {
        Event *e = &a->events.v[i];
        if (!e->parents.n) roots++;
        for (size_t p = 0; p < e->parents.n; ++p) {
            int parent = event_find(&a->events, e->parents.v[p]);
            if (parent < 0) {
                failure(a, "%s: missing causal parent %s", e->id, e->parents.v[p]);
                continue;
            }
            if (!edge_add_unique(&a->edges, a->events.v[parent].owner, e->owner)) return 0;
        }
    }
    if (roots != 1) failure(a, "expected one root disturbance, got %d", roots);
    for (size_t i = 0; i < a->events.n; ++i) {
        int ed = 0, od = 0;
        if (!depth_event(a, (int)i, &ed, &od)) return 0;
    }
    qsort(a->edges.v, a->edges.n, sizeof(*a->edges.v), cmp_edge);
    return 1;
}

static int group_count(sqlite3 *db, const Counters *kinds, int group_index, char **name_out) {
    char path[128];
    Strings members = {0};
    int total = 0;
    snprintf(path, sizeof(path), "$.kind_groups[%d].name", group_index);
    *name_out = contract_text(db, path);
    snprintf(path, sizeof(path), "$.kind_groups[%d].kinds", group_index);
    if (!contract_array(db, path, &members)) { sv_free(&members); return -1; }
    for (size_t i = 0; i < members.n; ++i) total += counter_get(kinds, members.v[i]);
    sv_free(&members);
    return total;
}

static int metric_value(const char *name, int distinct_owners, int max_owner_depth,
                        int round_trip_total, char **group_names, int *group_values, int group_count_value) {
    if (!strcmp(name, "distinct_owners")) return distinct_owners;
    if (!strcmp(name, "max_owner_braid_depth")) return max_owner_depth;
    if (!strcmp(name, "round_trip_total")) return round_trip_total;
    for (int i = 0; i < group_count_value; ++i) if (!strcmp(name, group_names[i])) return group_values[i];
    return 0;
}

static void emit_string(FILE *out, const char *s) {
    fputc('"', out);
    if (s) for (const unsigned char *p = (const unsigned char *)s; *p; ++p) {
        switch (*p) {
            case '"': fputs("\\\"", out); break;
            case '\\': fputs("\\\\", out); break;
            case '\b': fputs("\\b", out); break;
            case '\f': fputs("\\f", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default:
                if (*p < 0x20) fprintf(out, "\\u%04x", *p);
                else fputc(*p, out);
        }
    }
    fputc('"', out);
}

static void emit_result(FILE *out, Audit *a, const char *schema,
                        char **group_names, int *group_values, int group_n) {
    sv_sort(&a->owners);
    counter_sort(&a->verdicts);
    fprintf(out, "{\"schema\":"); emit_string(out, schema);
    fprintf(out, ",\"accepted\":%s,\"metrics\":{\"trace_events\":%lld,\"distinct_owners\":%zu,\"owners\":[",
            a->failures.n ? "false" : "true", a->trace_events, a->owners.n);
    for (size_t i = 0; i < a->owners.n; ++i) { if (i) fputc(',', out); emit_string(out, a->owners.v[i]); }
    fprintf(out, "],\"owner_transition_kinds\":%zu,\"owner_transition_pairs\":[", a->edges.n);
    for (size_t i = 0; i < a->edges.n; ++i) {
        if (i) fputc(',', out);
        fputc('[', out); emit_string(out, a->edges.v[i].source); fputc(',', out); emit_string(out, a->edges.v[i].target); fputc(']', out);
    }
    fprintf(out, "],\"max_event_depth\":%d,\"max_owner_braid_depth\":%d", a->max_event_depth, a->max_owner_depth);
    for (int i = 0; i < group_n; ++i) { fputc(',', out); emit_string(out, group_names[i]); fprintf(out, ":%d", group_values[i]); }
    fputs(",\"round_trip\":{", out);
    for (size_t i = 0; i < a->verdicts.n; ++i) {
        if (i) fputc(',', out);
        emit_string(out, a->verdicts.v[i].key); fprintf(out, ":%d", a->verdicts.v[i].count);
    }
    int round_total = 0;
    for (size_t i = 0; i < a->verdicts.n; ++i) round_total += a->verdicts.v[i].count;
    fprintf(out, "},\"round_trip_total\":%d},\"failures\":[", round_total);
    for (size_t i = 0; i < a->failures.n; ++i) { if (i) fputc(',', out); emit_string(out, a->failures.v[i]); }
    fputs("],\"cannot_infer\":[\"event count -> propagation depth\",\"sequence/step number -> causality\",\"owner label -> owner receipt\",\"completed protocol -> discharged commitment\"]}\n", out);
}

static int finalize_audit(Audit *a, const char *output) {
    Strings required = {0}, economic = {0};
    char *acceptance_schema = contract_text(a->db, "$.acceptance_schema");
    int group_n = contract_length(a->db, "$.kind_groups");
    char **group_names = calloc((size_t)group_n, sizeof(*group_names));
    int *group_values = calloc((size_t)group_n, sizeof(*group_values));
    FILE *file = NULL;
    int round_total = 0;
    if (!acceptance_schema || !group_names || !group_values ||
        !contract_array(a->db, "$.required_owners", &required) ||
        !contract_array(a->db, "$.economic_owners", &economic)) goto fail;

    if (!a->trace_events) failure(a, "no disturbance trace supplied");
    if (a->disturbance_ids.n != 1) failure(a, "expected one disturbance_id, got %zu", a->disturbance_ids.n);
    if (!build_graph(a)) goto fail;

    sv_sort(&required);
    for (size_t i = 0; i < required.n; ++i)
        if (!sv_has(&a->owners, required.v[i])) failure(a, "missing required owner %s", required.v[i]);
    int economic_seen = 0;
    for (size_t i = 0; i < economic.n; ++i) if (sv_has(&a->owners, economic.v[i])) economic_seen = 1;
    if (!economic_seen) failure(a, "missing economic owner");

    for (int i = 0; i < group_n; ++i) {
        group_values[i] = group_count(a->db, &a->kinds, i, &group_names[i]);
        if (group_values[i] < 0 || !group_names[i]) goto fail;
    }
    for (size_t i = 0; i < a->verdicts.n; ++i) round_total += a->verdicts.v[i].count;

    int threshold_n = contract_length(a->db, "$.thresholds");
    for (int i = 0; i < threshold_n; ++i) {
        char path[128];
        char *metric;
        int floor;
        snprintf(path, sizeof(path), "$.thresholds[%d].metric", i);
        metric = contract_text(a->db, path);
        snprintf(path, sizeof(path), "$.thresholds[%d].floor", i);
        floor = contract_int(a->db, path, 0);
        if (!metric) goto fail;
        int value = metric_value(metric, (int)a->owners.n, a->max_owner_depth, round_total, group_names, group_values, group_n);
        if (value < floor) failure(a, "%s=%d below %d", metric, value, floor);
        free(metric);
    }

    int retractions = 0, compensations = 0, improper = 0, corrections = 0;
    for (int i = 0; i < group_n; ++i) {
        if (!strcmp(group_names[i], "retractions")) retractions = group_values[i];
        else if (!strcmp(group_names[i], "compensations")) compensations = group_values[i];
        else if (!strcmp(group_names[i], "improper_erases")) improper = group_values[i];
        else if (!strcmp(group_names[i], "corrections")) corrections = group_values[i];
    }
    if (retractions + compensations < contract_int(a->db, "$.minimum_retraction_or_compensation", 1))
        failure(a, "no retraction or compensation");
    if (contract_int(a->db, "$.forbid_improper_erases", 1) && improper)
        failure(a, "history was erased instead of compensated/retracted");
    if (contract_int(a->db, "$.require_drift_correction", 1) && counter_get(&a->verdicts, "drifted") > corrections)
        failure(a, "round-trip drift lacks correction work");

    emit_result(stdout, a, acceptance_schema, group_names, group_values, group_n);
    if (output) {
        file = fopen(output, "w");
        if (!file) goto fail;
        emit_result(file, a, acceptance_schema, group_names, group_values, group_n);
        if (fclose(file)) { file = NULL; goto fail; }
        file = NULL;
    }

    for (int i = 0; i < group_n; ++i) free(group_names[i]);
    free(group_names); free(group_values); free(acceptance_schema);
    sv_free(&required); sv_free(&economic);
    return 1;
fail:
    if (file) fclose(file);
    if (group_names) { for (int i = 0; i < group_n; ++i) free(group_names[i]); }
    free(group_names); free(group_values); free(acceptance_schema);
    sv_free(&required); sv_free(&economic);
    return 0;
}

static void audit_free(Audit *a) {
    sv_free(&a->failures);
    sv_free(&a->owners);
    sv_free(&a->disturbance_ids);
    counter_free(&a->kinds);
    counter_free(&a->verdicts);
    edges_free(&a->edges);
    events_free(&a->events);
}

static const char *opt(int argc, char **argv, const char *name, const char *fallback) {
    for (int i = 2; i + 1 < argc; ++i) if (!strcmp(argv[i], name)) return argv[i + 1];
    return fallback;
}

int main(int argc, char **argv) {
    Audit audit = {0};
    const char *trace, *output;
    int rc = 1;
    enum { DA_HELP, DA_AUDIT };
    static const BfCommandEntry disturbance_commands[] = {
        {"--help", DA_HELP, 0, NULL},
        {"audit", DA_AUDIT, 0, NULL},
        {"help", DA_HELP, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(disturbance_commands,
        sizeof(disturbance_commands) / sizeof(disturbance_commands[0]), argv[1]);
    if (argc < 2 || (command && command->id == DA_HELP)) {
        puts("Usage: bonfyre-disturbance-audit audit --trace FILE [--output FILE]");
        return argc < 2 ? 1 : 0;
    }
    if (!command || command->id != DA_AUDIT) return 1;
    trace = opt(argc, argv, "--trace", "../BonfyreExperienceMap/generated/disturbance_trace.jsonl");
    output = opt(argc, argv, "--output", NULL);
    if (sqlite3_open(":memory:", &audit.db) != SQLITE_OK) goto done;
    if (!load_trace(&audit, trace)) goto done;
    if (!finalize_audit(&audit, output)) goto done;
    rc = 0;
done:
    if (audit.db) sqlite3_close(audit.db);
    audit_free(&audit);
    return rc;
}
