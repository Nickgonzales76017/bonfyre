/*
 * BonfyreGraph — Upgrade X: Cross-Family Transfer Learning
 *
 * Structural similarity, eigenvalue transfer, and template library.
 */
#include "transfer.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

extern uint64_t fnv1a64(const void *data, size_t len);
extern void     iso_timestamp(char *buf, size_t sz);
extern int      db_exec(sqlite3 *db, const char *sql);

/* ------------------------------------------------------------------ */
/*  Bootstrap                                                          */
/* ------------------------------------------------------------------ */
int transfer_bootstrap(sqlite3 *db)
{
    const char *ddl =
        "CREATE TABLE IF NOT EXISTS _transfer_templates ("
        "  template_id  INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  name         TEXT NOT NULL,"
        "  family_id    TEXT NOT NULL,"
        "  generator    TEXT NOT NULL,"
        "  arity        INTEGER NOT NULL,"
        "  member_count INTEGER NOT NULL DEFAULT 0,"
        "  created_at   TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS _transfer_log ("
        "  id            INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  donor_id      TEXT NOT NULL,"
        "  recipient_id  TEXT NOT NULL,"
        "  similarity    REAL NOT NULL,"
        "  positions_seeded INTEGER NOT NULL DEFAULT 0,"
        "  transferred_at TEXT NOT NULL"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_transfer_tpl_gen "
        "  ON _transfer_templates(generator);";
    return db_exec(db, ddl);
}

/* ------------------------------------------------------------------ */
/*  Internal: tokenise generator into key structure                   */
/* ------------------------------------------------------------------ */
#define MAX_TOKENS 256

typedef struct {
    int   is_param;
    char  text[512];
} GenToken;

typedef struct {
    GenToken tokens[MAX_TOKENS];
    int      count;
} TokenizedGen;

static void tokenize_gen(const char *gen, TokenizedGen *out)
{
    out->count = 0;
    if (!gen) return;
    const char *p = gen;
    char buf[512];
    int bi = 0;

    while (*p && out->count < MAX_TOKENS) {
        if (*p == '$' && p[1] >= '0' && p[1] <= '9') {
            /* flush literal */
            if (bi > 0) {
                buf[bi] = '\0';
                GenToken *t = &out->tokens[out->count++];
                t->is_param = 0;
                strncpy(t->text, buf, sizeof(t->text) - 1);
                t->text[sizeof(t->text) - 1] = '\0';
                bi = 0;
            }
            /* parse $N */
            char pbuf[32];
            int pi = 0;
            pbuf[pi++] = *p++;
            while (*p >= '0' && *p <= '9' && pi < 30)
                pbuf[pi++] = *p++;
            pbuf[pi] = '\0';
            GenToken *t = &out->tokens[out->count++];
            t->is_param = 1;
            strncpy(t->text, pbuf, sizeof(t->text) - 1);
            t->text[sizeof(t->text) - 1] = '\0';
        } else {
            if (bi < (int)sizeof(buf) - 1)
                buf[bi++] = *p;
            p++;
        }
    }
    if (bi > 0 && out->count < MAX_TOKENS) {
        buf[bi] = '\0';
        GenToken *t = &out->tokens[out->count++];
        t->is_param = 0;
        strncpy(t->text, buf, sizeof(t->text) - 1);
        t->text[sizeof(t->text) - 1] = '\0';
    }
}

/* ------------------------------------------------------------------ */
/*  transfer_distance — edit distance between generators              */
/* ------------------------------------------------------------------ */
double transfer_distance(const char *gen_a, const char *gen_b)
{
    if (!gen_a && !gen_b) return 0.0;
    if (!gen_a || !gen_b) return 1.0;

    TokenizedGen ta, tb;
    tokenize_gen(gen_a, &ta);
    tokenize_gen(gen_b, &tb);

    if (ta.count == 0 && tb.count == 0) return 0.0;

    int n = ta.count;
    int m = tb.count;
    int max_len = n > m ? n : m;
    if (max_len == 0) return 0.0;

    /* Levenshtein-style DP on tokens */
    /* Allocate (n+1)*(m+1) matrix */
    int *dp = calloc((n + 1) * (m + 1), sizeof(int));
    if (!dp) return 1.0;

    for (int i = 0; i <= n; i++) dp[i * (m + 1)] = i;
    for (int j = 0; j <= m; j++) dp[j] = j;

    for (int i = 1; i <= n; i++) {
        for (int j = 1; j <= m; j++) {
            int cost = 0;
            if (ta.tokens[i-1].is_param != tb.tokens[j-1].is_param) {
                cost = 1; /* structural mismatch: param vs literal */
            } else if (!ta.tokens[i-1].is_param) {
                /* both literals: compare text */
                cost = strcmp(ta.tokens[i-1].text, tb.tokens[j-1].text) != 0 ? 1 : 0;
            }
            /* both params: cost 0 (params are interchangeable) */

            int del = dp[(i-1) * (m+1) + j] + 1;
            int ins = dp[i * (m+1) + (j-1)] + 1;
            int sub = dp[(i-1) * (m+1) + (j-1)] + cost;

            int best = del < ins ? del : ins;
            if (sub < best) best = sub;
            dp[i * (m+1) + j] = best;
        }
    }

    int edit_dist = dp[n * (m+1) + m];
    free(dp);

    return (double)edit_dist / max_len;
}

/* ------------------------------------------------------------------ */
/*  transfer_candidates — find similar families                       */
/* ------------------------------------------------------------------ */
int transfer_candidates(sqlite3 *db, const char *family_id, int max_k,
                        TransferCandidate *out)
{
    if (!db || !family_id || !out || max_k <= 0) return 0;

    /* load target generator */
    char target_gen[4096] = {0};
    {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT generator FROM _families WHERE family_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return 0;
        sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *g = (const char *)sqlite3_column_text(st, 0);
            if (g) strncpy(target_gen, g, sizeof(target_gen) - 1);
        }
        sqlite3_finalize(st);
    }
    if (target_gen[0] == '\0') return 0;

    /* scan all other families, compute distance */
    typedef struct { char fid[128]; char gen[4096]; double sim; int arity; int mc; } Cand;
    int cand_cap = 256;
    Cand *cands = malloc(cand_cap * sizeof(Cand));
    if (!cands) return 0;
    int ncands = 0;

    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT f.family_id, f.generator, "
            "  (SELECT COUNT(DISTINCT position) FROM _tensor_cells tc WHERE tc.family_id = f.family_id), "
            "  (SELECT COUNT(DISTINCT target_id) FROM _tensor_cells tc WHERE tc.family_id = f.family_id) "
            "FROM _families f WHERE f.family_id != ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
            free(cands);
            return 0;
        }
        sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);

        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *fid = (const char *)sqlite3_column_text(st, 0);
            const char *gen = (const char *)sqlite3_column_text(st, 1);
            int arity = sqlite3_column_int(st, 2);
            int mc = sqlite3_column_int(st, 3);

            if (!fid || !gen) continue;

            double dist = transfer_distance(target_gen, gen);
            double sim = 1.0 - dist;

            if (sim < 0.3) continue; /* skip very dissimilar */

            if (ncands >= cand_cap) {
                cand_cap *= 2;
                Cand *nc = realloc(cands, cand_cap * sizeof(Cand));
                if (!nc) break;
                cands = nc;
            }

            strncpy(cands[ncands].fid, fid, sizeof(cands[ncands].fid) - 1);
            cands[ncands].fid[sizeof(cands[ncands].fid) - 1] = '\0';
            strncpy(cands[ncands].gen, gen, sizeof(cands[ncands].gen) - 1);
            cands[ncands].gen[sizeof(cands[ncands].gen) - 1] = '\0';
            cands[ncands].sim = sim;
            cands[ncands].arity = arity;
            cands[ncands].mc = mc;
            ncands++;
        }
        sqlite3_finalize(st);
    }

    /* sort by similarity descending */
    for (int i = 0; i < ncands - 1; i++) {
        for (int j = i + 1; j < ncands; j++) {
            if (cands[j].sim > cands[i].sim) {
                Cand tmp = cands[i];
                cands[i] = cands[j];
                cands[j] = tmp;
            }
        }
    }

    /* fill output */
    int k = ncands < max_k ? ncands : max_k;
    for (int i = 0; i < k; i++) {
        strncpy(out[i].family_id, cands[i].fid, sizeof(out[i].family_id) - 1);
        out[i].family_id[sizeof(out[i].family_id) - 1] = '\0';
        strncpy(out[i].generator, cands[i].gen, sizeof(out[i].generator) - 1);
        out[i].generator[sizeof(out[i].generator) - 1] = '\0';
        out[i].similarity = cands[i].sim;
        out[i].arity = cands[i].arity;
        out[i].member_count = cands[i].mc;
    }

    free(cands);
    return k;
}

/* ------------------------------------------------------------------ */
/*  transfer_eigen — seed eigenvalue estimates from a donor           */
/* ------------------------------------------------------------------ */
int transfer_eigen(sqlite3 *db, const char *donor_id,
                   const char *recipient_id, double weight)
{
    if (!db || !donor_id || !recipient_id) return -1;
    if (weight <= 0.0 || weight > 1.0) weight = 0.5;

    int seeded = 0;
    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    /* load donor eigenvalues */
    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT position, eigenvalue FROM _tensor_eigen "
        "WHERE family_id = ?1 ORDER BY position";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, donor_id, -1, SQLITE_STATIC);

    while (sqlite3_step(st) == SQLITE_ROW) {
        int pos = sqlite3_column_int(st, 0);
        double ev = sqlite3_column_double(st, 1);

        /* scale by similarity weight */
        double seeded_ev = ev * weight;

        /* insert into recipient, only if not already present */
        sqlite3_stmt *ins = NULL;
        const char *isql =
            "INSERT OR IGNORE INTO _tensor_eigen "
            "(family_id, position, eigenvalue, updated_at) "
            "VALUES (?1, ?2, ?3, ?4)";
        if (sqlite3_prepare_v2(db, isql, -1, &ins, NULL) == SQLITE_OK) {
            sqlite3_bind_text(ins, 1, recipient_id, -1, SQLITE_STATIC);
            sqlite3_bind_int(ins, 2, pos);
            sqlite3_bind_double(ins, 3, seeded_ev);
            sqlite3_bind_text(ins, 4, ts, -1, SQLITE_STATIC);
            if (sqlite3_step(ins) == SQLITE_DONE && sqlite3_changes(db) > 0)
                seeded++;
            sqlite3_finalize(ins);
        }
    }
    sqlite3_finalize(st);

    /* log the transfer */
    {
        double sim = weight; /* approximate */
        sqlite3_stmt *log_st = NULL;
        const char *lsql =
            "INSERT INTO _transfer_log "
            "(donor_id, recipient_id, similarity, positions_seeded, transferred_at) "
            "VALUES (?1, ?2, ?3, ?4, ?5)";
        if (sqlite3_prepare_v2(db, lsql, -1, &log_st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(log_st, 1, donor_id, -1, SQLITE_STATIC);
            sqlite3_bind_text(log_st, 2, recipient_id, -1, SQLITE_STATIC);
            sqlite3_bind_double(log_st, 3, sim);
            sqlite3_bind_int(log_st, 4, seeded);
            sqlite3_bind_text(log_st, 5, ts, -1, SQLITE_STATIC);
            sqlite3_step(log_st);
            sqlite3_finalize(log_st);
        }
    }

    return seeded;
}

/* ------------------------------------------------------------------ */
/*  transfer_auto — find best donor and transfer                      */
/* ------------------------------------------------------------------ */
int transfer_auto(sqlite3 *db, const char *family_id)
{
    if (!db || !family_id) return -1;

    TransferCandidate top;
    int found = transfer_candidates(db, family_id, 1, &top);
    if (found <= 0 || top.similarity < 0.5) return -1;

    int seeded = transfer_eigen(db, top.family_id, family_id, top.similarity);
    return seeded > 0 ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/*  transfer_register_template                                        */
/* ------------------------------------------------------------------ */
int transfer_register_template(sqlite3 *db, const char *family_id,
                               const char *name, int64_t *out_template_id)
{
    if (!db || !family_id || !name || !out_template_id) return -1;

    /* load generator and arity */
    char gen[4096] = {0};
    int arity = 0, mc = 0;
    {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT generator FROM _families WHERE family_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                const char *g = (const char *)sqlite3_column_text(st, 0);
                if (g) strncpy(gen, g, sizeof(gen) - 1);
            }
            sqlite3_finalize(st);
        }
    }
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT COUNT(DISTINCT position), COUNT(DISTINCT target_id) "
            "FROM _tensor_cells WHERE family_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                arity = sqlite3_column_int(st, 0);
                mc = sqlite3_column_int(st, 1);
            }
            sqlite3_finalize(st);
        }
    }

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    sqlite3_stmt *ins = NULL;
    const char *sql =
        "INSERT INTO _transfer_templates "
        "(name, family_id, generator, arity, member_count, created_at) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
    if (sqlite3_prepare_v2(db, sql, -1, &ins, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(ins, 1, name, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 2, family_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(ins, 3, gen, -1, SQLITE_STATIC);
    sqlite3_bind_int(ins, 4, arity);
    sqlite3_bind_int(ins, 5, mc);
    sqlite3_bind_text(ins, 6, ts, -1, SQLITE_STATIC);
    if (sqlite3_step(ins) != SQLITE_DONE) {
        sqlite3_finalize(ins);
        return -1;
    }
    sqlite3_finalize(ins);

    *out_template_id = sqlite3_last_insert_rowid(db);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  transfer_match_template                                            */
/* ------------------------------------------------------------------ */
int transfer_match_template(sqlite3 *db, const char *generator,
                            int64_t *out_template_id, double *out_similarity)
{
    if (!db || !generator || !out_template_id || !out_similarity) return -1;

    *out_template_id = -1;
    *out_similarity = 0.0;

    sqlite3_stmt *st = NULL;
    const char *sql = "SELECT template_id, generator FROM _transfer_templates";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;

    double best_sim = 0.0;
    int64_t best_id = -1;

    while (sqlite3_step(st) == SQLITE_ROW) {
        int64_t tid = sqlite3_column_int64(st, 0);
        const char *tgen = (const char *)sqlite3_column_text(st, 1);
        if (!tgen) continue;

        double dist = transfer_distance(generator, tgen);
        double sim = 1.0 - dist;
        if (sim > best_sim) {
            best_sim = sim;
            best_id = tid;
        }
    }
    sqlite3_finalize(st);

    if (best_id >= 0 && best_sim >= 0.5) {
        *out_template_id = best_id;
        *out_similarity = best_sim;
        return 0;
    }
    return -1;
}

/* ------------------------------------------------------------------ */
/*  transfer_accelerate                                                */
/* ------------------------------------------------------------------ */
int transfer_accelerate(sqlite3 *db, const char *family_id)
{
    if (!db || !family_id) return 0;

    /* load generator */
    char gen[4096] = {0};
    {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT generator FROM _families WHERE family_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, family_id, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                const char *g = (const char *)sqlite3_column_text(st, 0);
                if (g) strncpy(gen, g, sizeof(gen) - 1);
            }
            sqlite3_finalize(st);
        }
    }
    if (gen[0] == '\0') return 0;

    /* match against templates */
    int64_t tpl_id;
    double sim;
    if (transfer_match_template(db, gen, &tpl_id, &sim) != 0) return 0;

    /* load template's source family for eigen transfer */
    char donor_fid[128] = {0};
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT family_id FROM _transfer_templates WHERE template_id = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_int64(st, 1, tpl_id);
            if (sqlite3_step(st) == SQLITE_ROW) {
                const char *f = (const char *)sqlite3_column_text(st, 0);
                if (f) strncpy(donor_fid, f, sizeof(donor_fid) - 1);
            }
            sqlite3_finalize(st);
        }
    }
    if (donor_fid[0] == '\0') return 0;

    int seeded = transfer_eigen(db, donor_fid, family_id, sim);
    return seeded > 0 ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/*  transfer_stats                                                     */
/* ------------------------------------------------------------------ */
int transfer_stats(sqlite3 *db, char **out_json)
{
    if (!db || !out_json) return -1;

    int templates = 0, transfers = 0;
    int64_t total_seeded = 0;
    double avg_sim = 0.0;

    {
        sqlite3_stmt *st = NULL;
        const char *sql = "SELECT COUNT(*) FROM _transfer_templates";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW)
                templates = sqlite3_column_int(st, 0);
            sqlite3_finalize(st);
        }
    }
    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT COUNT(*), COALESCE(SUM(positions_seeded), 0), "
            "       COALESCE(AVG(similarity), 0) FROM _transfer_log";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            if (sqlite3_step(st) == SQLITE_ROW) {
                transfers = sqlite3_column_int(st, 0);
                total_seeded = sqlite3_column_int64(st, 1);
                avg_sim = sqlite3_column_double(st, 2);
            }
            sqlite3_finalize(st);
        }
    }

    size_t cap = 512;
    char *buf = malloc(cap);
    if (!buf) return -1;
    snprintf(buf, cap,
        "{\"templates\":%d,\"transfers\":%d,"
        "\"total_positions_seeded\":%lld,\"avg_similarity\":%.4f}",
        templates, transfers, (long long)total_seeded, avg_sim);

    *out_json = buf;
    return 0;
}
