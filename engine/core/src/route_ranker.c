/* Ternary route ranker.  See bf_route_ranker.h. */
#include "bf_route_ranker.h"

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

void bf_route_train(const BfRouteRecord *rec, size_t n, double th, BfRouteRanker *out) {
    memset(out, 0, sizeof *out); out->trained_on = n;
    if (!rec || !n) return;
    for (int j = 0; j < BF_ROUTE_FEATURES; j++) {
        double ps = 0, ns = 0; size_t pn = 0, nn = 0;
        for (size_t i = 0; i < n; i++) { if (rec[i].success) { ps += rec[i].f[j]; pn++; } else { ns += rec[i].f[j]; nn++; } }
        double diff = (pn ? ps / pn : 0) - (nn ? ns / nn : 0);
        out->w[j] = diff > th ? 1 : diff < -th ? -1 : 0;
    }
}

double bf_route_score(const BfRouteRanker *r, const double f[BF_ROUTE_FEATURES]) {
    double s = 0; for (int j = 0; j < BF_ROUTE_FEATURES; j++) s += r->w[j] * f[j]; return s;
}

static int cmp(const void *a, const void *b) {
    const BfRouteCandidate *x = a, *y = b;
    if (x->score != y->score) return x->score > y->score ? -1 : 1;
    return strcmp(x->id ? x->id : "", y->id ? y->id : "");
}

void bf_route_rank(const BfRouteRanker *r, BfRouteCandidate *c, size_t n) {
    for (size_t i = 0; i < n; i++) c[i].score = bf_route_score(r, c[i].f);
    qsort(c, n, sizeof *c, cmp);
}

static int has_column(sqlite3 *db, const char *col) {
    sqlite3_stmt *st; int found = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM pragma_table_info('work_items') WHERE name=?1", -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, col, -1, SQLITE_STATIC); found = sqlite3_step(st) == SQLITE_ROW; sqlite3_finalize(st);
    return found;
}

long bf_route_records_from_work(void *vdb, BfRouteRecord *out, size_t cap) {
    sqlite3 *db = vdb; sqlite3_stmt *st; long n = 0;
    if (!db || !out) return -1;
    if (!has_column(db, "state")) return 0;
    int attempt = has_column(db, "attempt");
    const char *sql = attempt
        ? "SELECT COALESCE(priority,0), COALESCE(target_plane,''), COALESCE(attempt,0), state, (SELECT MAX(COALESCE(priority,0)) FROM work_items) FROM work_items"
        : "SELECT COALESCE(priority,0), COALESCE(target_plane,''), 0, state, (SELECT MAX(COALESCE(priority,0)) FROM work_items) FROM work_items";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    while ((size_t)n < cap && sqlite3_step(st) == SQLITE_ROW) {
        double maxp = sqlite3_column_double(st, 4); if (maxp <= 0) maxp = 1;
        const char *target = (const char *)sqlite3_column_text(st, 1), *state = (const char *)sqlite3_column_text(st, 3);
        out[n].f[0] = sqlite3_column_double(st, 0) / maxp;
        out[n].f[1] = target && *target ? 1.0 : 0.0;
        out[n].f[2] = sqlite3_column_int(st, 2) == 0 ? 1.0 : 0.0;
        out[n].success = state && (!strcmp(state, "satisfied") || !strcmp(state, "effected"));
        n++;
    }
    sqlite3_finalize(st);
    return n;
}
