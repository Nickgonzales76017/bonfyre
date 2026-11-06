#include <sqlite3.h>
#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

typedef struct { char id[320]; } Seen;
typedef struct { char pack[256]; char activation[512]; } ActivePack;

static const char *arg(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static void json_text(const char *s) {
    putchar('"');
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        if (*p == '"' || *p == '\\') { putchar('\\'); putchar(*p); }
        else if (*p < 32) printf("\\u%04x", *p);
        else putchar(*p);
    }
    putchar('"');
}

static int table_exists(sqlite3 *db, const char *name) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (!db || sqlite3_prepare_v2(db,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, name, -1, SQLITE_TRANSIENT);
    ok = sqlite3_step(st) == SQLITE_ROW;
    sqlite3_finalize(st);
    return ok;
}

static int normalize(const char *in, char *out, size_t cap) {
    size_t n = 0;
    int sep = 0;
    if (!in || !*in || cap < 2) return 0;
    for (const unsigned char *p = (const unsigned char *)in; *p; p++) {
        unsigned char c = (unsigned char)tolower(*p);
        if (isalnum(c) || c == '_') {
            if (n + 1 >= cap) return 0;
            out[n++] = (char)c;
            sep = 0;
        } else if (n && !sep) {
            if (n + 1 >= cap) return 0;
            out[n++] = '-';
            sep = 1;
        }
    }
    while (n && out[n - 1] == '-') n--;
    out[n] = '\0';
    return n > 0;
}

static int one_term(sqlite3 *db, const char *identity, const char *axis, char *out, size_t cap) {
    sqlite3_stmt *st = NULL;
    out[0] = '\0';
    if (sqlite3_prepare_v2(db,
        "SELECT term FROM candidate_advertisement_term "
        "WHERE identity=?1 AND axis=?2 ORDER BY term LIMIT 1",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, identity, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, axis, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0))
        snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    return out[0] != '\0';
}

static uint64_t hash64(const char *s) {
    uint64_t h = 1469598103934665603ULL;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        h ^= *p;
        h *= 1099511628211ULL;
    }
    return h;
}

static int ensure_dir(const char *path) {
    return path && *path && (mkdir(path, 0755) == 0 || errno == EEXIST);
}

static int seen_find(Seen *seen, int count, const char *identity) {
    for (int i = 0; i < count; i++) if (!strcmp(seen[i].id, identity)) return 1;
    return 0;
}

static int seen_add(Seen *seen, int *count, const char *identity) {
    if (*count >= 4096) return 0;
    snprintf(seen[*count].id, sizeof(seen[*count].id), "%s", identity);
    (*count)++;
    return 1;
}

static int emit_handoff(const char *dir, const char *origin, const char *identity, const char *owner) {
    char sub[1024], path[1200], temp[1240];
    FILE *f = NULL;
    if (!dir) return 1;
    if (!ensure_dir(dir) ||
        snprintf(sub, sizeof(sub), "%s/MaterialCompiler-external", dir) >= (int)sizeof(sub) ||
        !ensure_dir(sub)) return 0;
    if (snprintf(path, sizeof(path), "%s/%016llx.json", sub,
                 (unsigned long long)hash64(identity)) >= (int)sizeof(path)) return 0;
    if (!access(path, F_OK)) return 2;
    if (snprintf(temp, sizeof(temp), "%s.tmp", path) >= (int)sizeof(temp) ||
        !(f = fopen(temp, "w"))) return 0;
    fprintf(f,
        "{\"schema\":\"bonfyre-lowering-handoff.v1\",\"delta_id\":\"scoped-demand:%s\","
        "\"identity\":\"%s\",\"candidate_kind\":\"external_occurrence\","
        "\"owner_ref\":\"%s\",\"lowerer_ref\":\"MaterialCompiler:external\","
        "\"truth\":\"dependency_cut_only\"}\n",
        origin, identity, owner);
    if (fclose(f) || rename(temp, path)) { remove(temp); return 0; }
    return 1;
}

static int emit_query(sqlite3 *db, const char *sql, const char *key, const char *origin,
                      const char *dir, int *left, Seen *seen, int *seen_count,
                      int *total, int *dispatched) {
    sqlite3_stmt *st = NULL;
    int found = 0, emitted = 0;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, 4096);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *identity = (const char *)sqlite3_column_text(st, 0);
        const char *owner = (const char *)sqlite3_column_text(st, 1);
        int reused = seen_find(seen, *seen_count, identity);
        if (!reused && *left < 1) continue;
        if (!reused && !seen_add(seen, seen_count, identity)) { sqlite3_finalize(st); return -1; }
        if (!reused) {
            int hr = emit_handoff(dir, origin, identity, owner);
            if (!hr) { sqlite3_finalize(st); return -1; }
            if (hr == 1 && dir) (*dispatched)++;
            (*left)--;
            (*total)++;
        }
        if (emitted++) putchar(',');
        fputs("{\"identity\":", stdout); json_text(identity);
        fputs(",\"owner_ref\":", stdout); json_text(owner);
        fputs(",\"exact_verifier\":", stdout); json_text((const char *)sqlite3_column_text(st, 2));
        fputs(",\"lowerer_ref\":", stdout); json_text((const char *)sqlite3_column_text(st, 3));
        printf(",\"reused\":%s}", reused ? "true" : "false");
        found++;
    }
    sqlite3_finalize(st);
    return found;
}

static int by_source(sqlite3 *db, const char *source, const char *origin, const char *dir,
                     int *left, Seen *seen, int *seen_count, int *total, int *dispatched) {
    const char *sql =
        "SELECT DISTINCT a.identity,a.owner_ref,a.exact_verifier,a.lowerer_ref "
        "FROM candidate_advertisement a LEFT JOIN occurrence o ON o.id=a.identity "
        "WHERE a.candidate_kind='external_occurrence' AND "
        "(EXISTS(SELECT 1 FROM candidate_advertisement_term t "
        "WHERE t.identity=a.identity AND t.axis='source' AND t.term=?1) "
        "OR lower(coalesce(o.source_ref,''))=?1) ORDER BY a.identity LIMIT ?2";
    return emit_query(db, sql, source, origin, dir, left, seen, seen_count, total, dispatched);
}

static int by_topic(sqlite3 *db, const char *topic, const char *origin, const char *dir,
                    int *left, Seen *seen, int *seen_count, int *total, int *dispatched) {
    const char *sql =
        "SELECT DISTINCT a.identity,a.owner_ref,a.exact_verifier,a.lowerer_ref "
        "FROM candidate_advertisement a JOIN occurrence o ON o.id=a.identity "
        "WHERE a.candidate_kind='external_occurrence' AND o.primary_url=?1 "
        "ORDER BY a.identity LIMIT ?2";
    return emit_query(db, sql, topic, origin, dir, left, seen, seen_count, total, dispatched);
}

static int activation_conditions_satisfied(sqlite3 *address, sqlite3 *conditions,
                                           const char *activation, const char *subject,
                                           const char *scope, int *conditional) {
    sqlite3_stmt *terms = NULL, *lookup = NULL;
    int ok = 1, count = 0;
    if (sqlite3_prepare_v2(address,
        "SELECT term FROM candidate_advertisement_term "
        "WHERE identity=?1 AND axis='condition' ORDER BY term",
        -1, &terms, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(terms, 1, activation, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(terms) == SQLITE_ROW) {
        const char *condition = (const char *)sqlite3_column_text(terms, 0);
        count++;
        if (!conditions || !table_exists(conditions, "pack_condition_state")) { ok = 0; continue; }
        if (sqlite3_prepare_v2(conditions,
            "SELECT state FROM pack_condition_state "
            "WHERE condition_ref=?1 AND subject=?2 AND scope=?3",
            -1, &lookup, NULL) != SQLITE_OK) { ok = 0; continue; }
        sqlite3_bind_text(lookup, 1, condition, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(lookup, 2, subject, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(lookup, 3, scope, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(lookup) != SQLITE_ROW ||
            strcmp((const char *)sqlite3_column_text(lookup, 0), "satisfied")) ok = 0;
        sqlite3_finalize(lookup);
        lookup = NULL;
    }
    if (lookup) sqlite3_finalize(lookup);
    sqlite3_finalize(terms);
    *conditional = count > 0;
    return count ? ok : 1;
}

static int resolve(sqlite3 *db, sqlite3 *conditions, const char *subject_raw, const char *scope_raw,
                   int limit, const char *dir) {
    sqlite3_stmt *packs = NULL, *demands = NULL, *sources = NULL, *worlds = NULL, *world_deps = NULL;
    char subject[256], scope[256], demand_kind[128], requirement[256], regime[128];
    ActivePack active[4096];
    Seen seen[4096];
    int active_count = 0, conditional_count = 0, blocked_count = 0;
    int demand_count = 0, world_count = 0, survivor_count = 0, unresolved = 0, dispatched = 0;
    int seen_count = 0, left = limit, first = 1;
    const char *pack_sql =
        "SELECT DISTINCT x.term,a.identity FROM candidate_advertisement a "
        "JOIN candidate_advertisement_term x ON x.identity=a.identity AND x.axis='pack' "
        "JOIN candidate_advertisement_term y ON y.identity=a.identity AND y.axis='subject' AND y.term=?1 "
        "JOIN candidate_advertisement_term z ON z.identity=a.identity AND z.axis='scope' AND z.term=?2 "
        "WHERE a.candidate_kind='pack' ORDER BY x.term,a.identity";
    if (!normalize(subject_raw, subject, sizeof(subject)) ||
        !normalize(scope_raw, scope, sizeof(scope)) ||
        !table_exists(db, "candidate_advertisement") ||
        !table_exists(db, "candidate_advertisement_term") ||
        !table_exists(db, "candidate_advertisement_dependency") ||
        !table_exists(db, "occurrence")) return 0;
    if (sqlite3_prepare_v2(db, pack_sql, -1, &packs, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(packs, 1, subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(packs, 2, scope, -1, SQLITE_TRANSIENT);
    while (sqlite3_step(packs) == SQLITE_ROW) {
        const char *pack = (const char *)sqlite3_column_text(packs, 0);
        const char *activation = (const char *)sqlite3_column_text(packs, 1);
        int conditional = 0;
        int enabled = activation_conditions_satisfied(db, conditions, activation, subject, scope, &conditional);
        if (conditional) conditional_count++;
        if (!enabled) { blocked_count++; continue; }
        if (active_count >= 4096) goto fail;
        snprintf(active[active_count].pack, sizeof(active[active_count].pack), "%s", pack);
        snprintf(active[active_count].activation, sizeof(active[active_count].activation), "%s", activation);
        active_count++;
    }
    sqlite3_finalize(packs);
    packs = NULL;

    fputs("{\"schema\":\"bonfyre-scoped-demand-closure.v3\",\"subject\":", stdout);
    json_text(subject_raw);
    fputs(",\"scope\":", stdout); json_text(scope_raw);
    fputs(",\"active_packs\":[", stdout);
    for (int i = 0; i < active_count; i++) {
        if (i) putchar(',');
        fputs("{\"pack\":", stdout); json_text(active[i].pack);
        fputs(",\"activation\":", stdout); json_text(active[i].activation);
        putchar('}');
    }

    const char *demand_sql =
        "SELECT a.identity FROM candidate_advertisement a "
        "JOIN candidate_advertisement_term x ON x.identity=a.identity "
        "AND x.axis='pack' AND x.term=?1 "
        "WHERE a.candidate_kind='pack_demand' ORDER BY a.identity";
    const char *source_sql =
        "SELECT term FROM candidate_advertisement_term "
        "WHERE identity=?1 AND axis='source' ORDER BY term";
    if (sqlite3_prepare_v2(db, demand_sql, -1, &demands, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, source_sql, -1, &sources, NULL) != SQLITE_OK) goto fail;
    fputs("],\"pack_demands\":[", stdout);
    first = 1;
    for (int i = 0; i < active_count; i++) {
        sqlite3_bind_text(demands, 1, active[i].pack, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(demands) == SQLITE_ROW) {
            const char *identity = (const char *)sqlite3_column_text(demands, 0);
            int first_source = 1, source_count = 0, matched = 0;
            one_term(db, identity, "demand", demand_kind, sizeof(demand_kind));
            one_term(db, identity, "requirement", requirement, sizeof(requirement));
            one_term(db, identity, "regime", regime, sizeof(regime));
            if (!first) putchar(',');
            first = 0;
            demand_count++;
            fputs("{\"identity\":", stdout); json_text(identity);
            fputs(",\"pack\":", stdout); json_text(active[i].pack);
            fputs(",\"demand_kind\":", stdout); json_text(demand_kind);
            fputs(",\"requirement\":", stdout); json_text(requirement);
            fputs(",\"regime\":", stdout); json_text(regime);
            fputs(",\"candidate_groups\":[", stdout);
            sqlite3_bind_text(sources, 1, identity, -1, SQLITE_TRANSIENT);
            while (sqlite3_step(sources) == SQLITE_ROW) {
                const char *source = (const char *)sqlite3_column_text(sources, 0);
                if (!first_source) putchar(',');
                first_source = 0;
                source_count++;
                fputs("{\"source\":", stdout); json_text(source);
                fputs(",\"candidates\":[", stdout);
                int n = by_source(db, source, identity, dir, &left, seen, &seen_count,
                                  &survivor_count, &dispatched);
                if (n < 0) goto fail;
                if (n) matched += n;
                fputs("]}", stdout);
            }
            sqlite3_reset(sources);
            sqlite3_clear_bindings(sources);
            if (!source_count || !matched) unresolved++;
            fputs("],\"state\":", stdout); json_text(source_count && matched ? "candidate_cut" : "unresolved");
            putchar('}');
        }
        sqlite3_reset(demands);
        sqlite3_clear_bindings(demands);
    }

    const char *world_sql =
        "SELECT DISTINCT a.identity FROM candidate_advertisement a "
        "JOIN candidate_advertisement_term s ON s.identity=a.identity "
        "AND s.axis='subject' AND s.term=?1 "
        "WHERE a.candidate_kind='world_publication' ORDER BY a.identity";
    const char *world_dep_sql =
        "SELECT dependency_ref FROM candidate_advertisement_dependency "
        "WHERE identity=?1 AND (dependency_ref LIKE 'cms:internet_source:%' "
        "OR dependency_ref LIKE 'cms:publication_source:%' "
        "OR dependency_ref LIKE 'ForeignTwin:%') ORDER BY dependency_ref";
    if (sqlite3_prepare_v2(db, world_sql, -1, &worlds, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, world_dep_sql, -1, &world_deps, NULL) != SQLITE_OK) goto fail;
    sqlite3_bind_text(worlds, 1, subject, -1, SQLITE_TRANSIENT);
    fputs("],\"world_demands\":[", stdout);
    first = 1;
    while (sqlite3_step(worlds) == SQLITE_ROW) {
        const char *identity = (const char *)sqlite3_column_text(worlds, 0);
        int first_dep = 1, matched = 0;
        if (!first) putchar(',');
        first = 0;
        world_count++;
        fputs("{\"identity\":", stdout); json_text(identity);
        fputs(",\"dependencies\":[", stdout);
        sqlite3_bind_text(world_deps, 1, identity, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(world_deps) == SQLITE_ROW) {
            const char *dep = (const char *)sqlite3_column_text(world_deps, 0);
            int n = 0;
            if (!first_dep) putchar(',');
            first_dep = 0;
            fputs("{\"dependency\":", stdout); json_text(dep);
            fputs(",\"candidates\":[", stdout);
            if (!strncmp(dep, "cms:internet_source:", 20))
                n = by_source(db, dep + 20, identity, dir, &left, seen, &seen_count, &survivor_count, &dispatched);
            else if (!strncmp(dep, "cms:publication_source:", 23))
                n = by_source(db, dep + 23, identity, dir, &left, seen, &seen_count, &survivor_count, &dispatched);
            else if (!strncmp(dep, "ForeignTwin:", 12))
                n = by_topic(db, dep + 12, identity, dir, &left, seen, &seen_count, &survivor_count, &dispatched);
            if (n < 0) goto fail;
            if (n) matched += n;
            fputs("]}", stdout);
        }
        sqlite3_reset(world_deps);
        sqlite3_clear_bindings(world_deps);
        if (!matched) unresolved++;
        fputs("],\"state\":", stdout); json_text(matched ? "candidate_cut" : "unresolved");
        putchar('}');
    }

    sqlite3_finalize(world_deps);
    sqlite3_finalize(worlds);
    sqlite3_finalize(sources);
    sqlite3_finalize(demands);
    printf("],\"counts\":{\"active_packs\":%d,\"conditional_packs\":%d,\"blocked_packs\":%d,"
           "\"pack_demands\":%d,\"world_publications\":%d,\"candidate_survivors\":%d,"
           "\"unresolved_demands\":%d,\"handoffs_dispatched\":%d},",
           active_count, conditional_count, blocked_count, demand_count, world_count,
           survivor_count, unresolved, dispatched);
    fputs("\"truth\":\"scoped_candidate_cut_exact_owner_recheck_required\","
          "\"cannot_infer\":[\"pack activation -> authority\","
          "\"condition satisfied -> authority\","
          "\"world source dependency -> observation\","
          "\"declared demand -> retained body\","
          "\"candidate match -> event truth\","
          "\"unresolved demand -> no source exists\"]}\n", stdout);
    return 1;

fail:
    if (world_deps) sqlite3_finalize(world_deps);
    if (worlds) sqlite3_finalize(worlds);
    if (sources) sqlite3_finalize(sources);
    if (demands) sqlite3_finalize(demands);
    if (packs) sqlite3_finalize(packs);
    return 0;
}

int main(int argc, char **argv) {
    const char *db_path, *condition_path, *subject, *scope, *limit_raw, *dir;
    sqlite3 *db = NULL, *conditions = NULL;
    int limit, ok;
    enum { SD_DISPATCH, SD_RESOLVE };
    static const BfCommandEntry demand_commands[] = {
        {"dispatch", SD_DISPATCH, 0, NULL},
        {"resolve", SD_RESOLVE, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(demand_commands,
        sizeof(demand_commands) / sizeof(demand_commands[0]), argv[1]);
    if (argc < 2 || !command) {
        fputs("usage: bonfyre-scoped-demand resolve|dispatch --db FILE --subject SUBJECT --scope SCOPE "
              "[--condition-db FILE] [--dir HANDOFFS] [--limit N]\n", stderr);
        return 2;
    }
    db_path = arg(argc, argv, "--db");
    condition_path = arg(argc, argv, "--condition-db");
    subject = arg(argc, argv, "--subject");
    scope = arg(argc, argv, "--scope");
    limit_raw = arg(argc, argv, "--limit");
    dir = command->id == SD_DISPATCH ? arg(argc, argv, "--dir") : NULL;
    if (!db_path || !subject || !scope || (command->id == SD_DISPATCH && !dir)) return 2;
    limit = limit_raw ? atoi(limit_raw) : 256;
    if (limit < 1) limit = 1;
    if (limit > 4096) limit = 4096;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto fail;
    if (condition_path &&
        sqlite3_open_v2(condition_path, &conditions, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) goto fail;
    ok = resolve(db, conditions, subject, scope, limit, dir);
    if (conditions) sqlite3_close(conditions);
    sqlite3_close(db);
    return ok ? 0 : 1;
fail:
    if (conditions) sqlite3_close(conditions);
    if (db) sqlite3_close(db);
    return 1;
}
