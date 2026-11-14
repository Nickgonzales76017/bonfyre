/* Provider bids, placement and parity.  See bf_provider_graph.h. */
#include "bf_provider_graph.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *const BF_PROVIDER_DIM_NAMES[BF_PROVIDER_DIMS] = {
    "cost", "quality", "latency", "trust", "authority", "locality", "semantic_loss", "availability"};

static const char *SCHEMA =
    "CREATE TABLE IF NOT EXISTS provider_bids(capability TEXT NOT NULL, provider TEXT NOT NULL, metric TEXT NOT NULL,"
    " PRIMARY KEY(capability, provider));"
    "CREATE TABLE IF NOT EXISTS placements(resource TEXT PRIMARY KEY, location TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS runtime_parity(capability TEXT NOT NULL, carrier_a TEXT NOT NULL, carrier_b TEXT NOT NULL,"
    " proven INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(capability, carrier_a, carrier_b));";

static int present(const char *s) { return s && *s; }

int bf_provider_graph_open(void *db) { return db && sqlite3_exec(db, SCHEMA, NULL, NULL, NULL) == SQLITE_OK ? 0 : -1; }

int bf_provider_polarity(const char *d) {
    return d && (!strcmp(d, "cost") || !strcmp(d, "latency") || !strcmp(d, "semantic_loss")) ? -1 : 1;
}

static int exec_text(sqlite3 *db, const char *sql, int n, const char **v, const int *ints) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    for (int i = 0; i < n; i++) {
        if (v[i]) sqlite3_bind_text(st, i + 1, v[i], -1, SQLITE_TRANSIENT); else sqlite3_bind_int(st, i + 1, ints[i]);
    }
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? 0 : -1;
}

int bf_provider_record_bid(void *vdb, const char *cap, const char *prov, const double m[BF_PROVIDER_DIMS], const int have[BF_PROVIDER_DIMS]) {
    sqlite3 *db = vdb;
    if (!db || !present(cap) || !present(prov) || !m || bf_provider_graph_open(db)) return -1;
    char json[1024]; size_t w = 0; int first = 1;
    w += (size_t)snprintf(json + w, sizeof json - w, "{");
    for (int i = 0; i < BF_PROVIDER_DIMS; i++) {
        if (have && !have[i]) continue;
        w += (size_t)snprintf(json + w, sizeof json - w, "%s\"%s\":%.17g", first ? "" : ",", BF_PROVIDER_DIM_NAMES[i], m[i]);
        first = 0;
    }
    snprintf(json + w, sizeof json - w, "}");
    const char *v[] = {cap, prov, json};
    return exec_text(db, "INSERT OR REPLACE INTO provider_bids(capability,provider,metric) VALUES(?1,?2,json(?3))", 3, v, NULL);
}

static int rank_cmp(const void *a, const void *b) {
    const BfProviderRank *x = a, *y = b;
    if (x->score != y->score) return x->score > y->score ? -1 : 1;
    return strcmp(x->provider, y->provider);
}

int bf_provider_tournament(void *vdb, const char *cap, const double *weights, int require_authority, BfProviderRank *out, size_t outcap) {
    sqlite3 *db = vdb; sqlite3_stmt *st = NULL;
    if (!db || !present(cap) || bf_provider_graph_open(db)) return -1;
    char sql[1024]; size_t w = (size_t)snprintf(sql, sizeof sql, "SELECT provider");
    for (int i = 0; i < BF_PROVIDER_DIMS; i++) w += (size_t)snprintf(sql + w, sizeof sql - w, ",COALESCE(json_extract(metric,'$.%s'),0)", BF_PROVIDER_DIM_NAMES[i]);
    snprintf(sql + w, sizeof sql - w, " FROM provider_bids WHERE capability=?1");
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, cap, -1, SQLITE_TRANSIENT);
    size_t n = 0; int rc;
    BfProviderRank *all = NULL; size_t allcap = 0;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        double v[BF_PROVIDER_DIMS], score = 0;
        for (int i = 0; i < BF_PROVIDER_DIMS; i++) v[i] = sqlite3_column_double(st, i + 1);
        if (require_authority && v[4] <= 0.0) continue; /* available is not authorized */
        for (int i = 0; i < BF_PROVIDER_DIMS; i++) {
            double wt = weights ? weights[i] : 1.0; if (wt < 0) wt = 0;
            score += wt * bf_provider_polarity(BF_PROVIDER_DIM_NAMES[i]) * v[i];
        }
        if (n == allcap) { allcap = allcap ? allcap * 2 : 16; BfProviderRank *g = realloc(all, allcap * sizeof *all); if (!g) break; all = g; }
        all[n].provider = strdup((const char *)sqlite3_column_text(st, 0)); all[n].score = score; n++;
    }
    sqlite3_finalize(st);
    if (rc != SQLITE_DONE && rc != SQLITE_ROW) { for (size_t i = 0; i < n; i++) free((void *)all[i].provider); free(all); return -1; }
    qsort(all, n, sizeof *all, rank_cmp);
    size_t k = n < outcap ? n : outcap;
    for (size_t i = 0; i < n; i++) { if (i < k && out) out[i] = all[i]; else free((void *)all[i].provider); }
    free(all);
    return (int)k;
}

int bf_provider_record_placement(void *db, const char *res, const char *loc) {
    if (!db || !present(res) || !present(loc) || bf_provider_graph_open(db)) return -1;
    const char *v[] = {res, loc};
    return exec_text(db, "INSERT OR REPLACE INTO placements(resource,location) VALUES(?1,?2)", 2, v, NULL);
}

int bf_provider_placement_of(void *vdb, const char *res, char *out, size_t cap) {
    sqlite3 *db = vdb; sqlite3_stmt *st = NULL;
    if (!db || !present(res) || !out || !cap || bf_provider_graph_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT location FROM placements WHERE resource=?1", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, res, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(st), f = rc == SQLITE_ROW;
    if (f) snprintf(out, cap, "%s", (const char *)sqlite3_column_text(st, 0)); else out[0] = 0;
    sqlite3_finalize(st);
    return (f || rc == SQLITE_DONE) ? f : -1;
}

int bf_provider_record_parity(void *db, const char *cap, const char *a, const char *b, int proven) {
    if (!db || !present(cap) || !present(a) || !present(b) || bf_provider_graph_open(db)) return -1;
    if (strcmp(a, b) > 0) { const char *t = a; a = b; b = t; }
    const char *v[] = {cap, a, b, NULL}; int ints[] = {0, 0, 0, proven ? 1 : 0};
    return exec_text(db, "INSERT OR REPLACE INTO runtime_parity(capability,carrier_a,carrier_b,proven) VALUES(?1,?2,?3,?4)", 4, v, ints);
}

int64_t bf_provider_parity_group(void *vdb, const char *cap, int (*visit)(const char *, const char *, void *), void *ctx) {
    sqlite3 *db = vdb; sqlite3_stmt *st = NULL; int64_t n = 0;
    if (!db || !present(cap) || bf_provider_graph_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT carrier_a,carrier_b FROM runtime_parity WHERE capability=?1 AND proven=1 ORDER BY carrier_a,carrier_b",
                           -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, cap, -1, SQLITE_TRANSIENT);
    int rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) { n++; if (visit && visit((const char *)sqlite3_column_text(st, 0), (const char *)sqlite3_column_text(st, 1), ctx)) { rc = SQLITE_DONE; break; } }
    sqlite3_finalize(st);
    return rc == SQLITE_DONE ? n : -1;
}
