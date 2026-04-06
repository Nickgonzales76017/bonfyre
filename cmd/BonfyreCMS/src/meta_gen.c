/*
 * BonfyreGraph — Upgrade VI: Hierarchical Meta-Generators
 *
 * Groups structurally similar generators into meta-families and
 * extracts meta-generators that parameterize over generator differences.
 *
 * Similarity metric: structural edit distance on generators,
 * treating $N placeholders and literal values differently.
 */
#include "meta_gen.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* shared helpers from main.c */
extern uint64_t fnv1a64(const void *data, size_t len);
extern void     iso_timestamp(char *buf, size_t sz);
extern int      db_exec(sqlite3 *db, const char *sql);

/* ------------------------------------------------------------------ */
/*  Bootstrap                                                          */
/* ------------------------------------------------------------------ */
int meta_bootstrap(sqlite3 *db)
{
    const char *ddl =
        "CREATE TABLE IF NOT EXISTS _meta_families ("
        "  meta_family_id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  content_type   TEXT NOT NULL,"
        "  meta_generator TEXT NOT NULL,"
        "  meta_arity     INTEGER NOT NULL DEFAULT 0,"
        "  member_count   INTEGER NOT NULL DEFAULT 0,"
        "  level          INTEGER NOT NULL DEFAULT 2,"
        "  compression    REAL NOT NULL DEFAULT 0,"
        "  created_at     TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS _meta_members ("
        "  meta_family_id INTEGER NOT NULL,"
        "  family_id      TEXT NOT NULL,"
        "  generator      TEXT NOT NULL,"
        "  meta_bindings  TEXT NOT NULL DEFAULT '[]',"
        "  PRIMARY KEY (meta_family_id, family_id)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_meta_members_ct "
        "  ON _meta_families(content_type);"
        "CREATE INDEX IF NOT EXISTS idx_meta_members_fam "
        "  ON _meta_members(family_id);";
    return db_exec(db, ddl);
}

/* ------------------------------------------------------------------ */
/*  Internal: tokenise a generator string into segments                */
/* ------------------------------------------------------------------ */
#define MAX_SEGMENTS 256

typedef struct {
    int   is_param;          /* 1 = $N placeholder, 0 = literal chunk */
    int   param_idx;         /* index if is_param */
    char  literal[1024];     /* text if literal */
} GenSegment;

typedef struct {
    GenSegment segs[MAX_SEGMENTS];
    int        count;
} GenParsed;

static void parse_generator(const char *gen, GenParsed *out)
{
    out->count = 0;
    if (!gen) return;
    const char *p = gen;
    char buf[1024];
    int  bi = 0;

    while (*p && out->count < MAX_SEGMENTS) {
        if (*p == '$' && (p[1] >= '0' && p[1] <= '9')) {
            /* flush literal buffer */
            if (bi > 0) {
                buf[bi] = '\0';
                GenSegment *s = &out->segs[out->count++];
                s->is_param = 0;
                s->param_idx = -1;
                strncpy(s->literal, buf, sizeof(s->literal) - 1);
                s->literal[sizeof(s->literal) - 1] = '\0';
                bi = 0;
            }
            /* parse param index */
            p++;
            int idx = 0;
            while (*p >= '0' && *p <= '9') {
                idx = idx * 10 + (*p - '0');
                p++;
            }
            GenSegment *s = &out->segs[out->count++];
            s->is_param = 1;
            s->param_idx = idx;
            s->literal[0] = '\0';
        } else {
            if (bi < (int)sizeof(buf) - 1)
                buf[bi++] = *p;
            p++;
        }
    }
    if (bi > 0 && out->count < MAX_SEGMENTS) {
        buf[bi] = '\0';
        GenSegment *s = &out->segs[out->count++];
        s->is_param = 0;
        s->param_idx = -1;
        strncpy(s->literal, buf, sizeof(s->literal) - 1);
        s->literal[sizeof(s->literal) - 1] = '\0';
    }
}

/* ------------------------------------------------------------------ */
/*  Internal: structural similarity between two parsed generators      */
/*  Returns 0.0 (totally different) to 1.0 (identical structure)      */
/* ------------------------------------------------------------------ */
static double gen_similarity(const GenParsed *a, const GenParsed *b)
{
    if (a->count == 0 && b->count == 0) return 1.0;
    if (a->count != b->count) {
        /* penalise segment count difference */
        int mx = a->count > b->count ? a->count : b->count;
        int mn = a->count < b->count ? a->count : b->count;
        if (mx == 0) return 0.0;
        double base = (double)mn / mx;
        /* still compare matching segments */
        int match = 0;
        int lim = mn;
        for (int i = 0; i < lim; i++) {
            if (a->segs[i].is_param == b->segs[i].is_param) {
                if (a->segs[i].is_param) {
                    match++;
                } else if (strcmp(a->segs[i].literal, b->segs[i].literal) == 0) {
                    match++;
                }
            }
        }
        return base * (lim > 0 ? (double)match / lim : 0.0);
    }
    /* same segment count */
    int match = 0;
    int structural_match = 0; /* same type */
    for (int i = 0; i < a->count; i++) {
        if (a->segs[i].is_param == b->segs[i].is_param) {
            structural_match++;
            if (a->segs[i].is_param) {
                match++;
            } else if (strcmp(a->segs[i].literal, b->segs[i].literal) == 0) {
                match++;
            }
        }
    }
    /* weight: 60% structural (param vs literal in same positions),
       40% exact literal matches */
    double s = (double)structural_match / a->count;
    double m = (double)match / a->count;
    return 0.6 * s + 0.4 * m;
}

/* ------------------------------------------------------------------ */
/*  Internal: anti-unify two generators to produce a meta-generator   */
/*  Positions where generators differ become $$N meta-params          */
/* ------------------------------------------------------------------ */
static int anti_unify_generators(const GenParsed *a, const GenParsed *b,
                                 char *meta_out, size_t meta_sz,
                                 char *bind_a_out, size_t ba_sz,
                                 char *bind_b_out, size_t bb_sz)
{
    if (a->count != b->count) return -1;

    char *mp = meta_out;
    char *me = meta_out + meta_sz - 1;
    char *ap = bind_a_out;
    char *ae = bind_a_out + ba_sz - 2;
    char *bp = bind_b_out;
    char *be = bind_b_out + bb_sz - 2;
    int meta_idx = 0;

    *ap++ = '[';
    *bp++ = '[';

    for (int i = 0; i < a->count && mp < me; i++) {
        if (a->segs[i].is_param && b->segs[i].is_param) {
            /* both params — keep as $N in meta-generator */
            int n = snprintf(mp, (size_t)(me - mp), "$%d", a->segs[i].param_idx);
            mp += n;
        } else if (a->segs[i].is_param != b->segs[i].is_param) {
            /* structural difference → meta-param */
            int n = snprintf(mp, (size_t)(me - mp), "$$%d", meta_idx);
            mp += n;
            /* serialise both sides */
            if (meta_idx > 0) { *ap++ = ','; *bp++ = ','; }
            if (a->segs[i].is_param) {
                int w = snprintf(ap, (size_t)(ae - ap), "\"$%d\"", a->segs[i].param_idx);
                ap += w;
            } else {
                int w = snprintf(ap, (size_t)(ae - ap), "\"%s\"", a->segs[i].literal);
                ap += w;
            }
            if (b->segs[i].is_param) {
                int w = snprintf(bp, (size_t)(be - bp), "\"$%d\"", b->segs[i].param_idx);
                bp += w;
            } else {
                int w = snprintf(bp, (size_t)(be - bp), "\"%s\"", b->segs[i].literal);
                bp += w;
            }
            meta_idx++;
        } else {
            /* both literals */
            if (strcmp(a->segs[i].literal, b->segs[i].literal) == 0) {
                /* identical literal — keep verbatim */
                int n = snprintf(mp, (size_t)(me - mp), "%s", a->segs[i].literal);
                mp += n;
            } else {
                /* differing literals → meta-param */
                int n = snprintf(mp, (size_t)(me - mp), "$$%d", meta_idx);
                mp += n;
                if (meta_idx > 0) { *ap++ = ','; *bp++ = ','; }
                int wa = snprintf(ap, (size_t)(ae - ap), "\"%s\"", a->segs[i].literal);
                ap += wa;
                int wb = snprintf(bp, (size_t)(be - bp), "\"%s\"", b->segs[i].literal);
                bp += wb;
                meta_idx++;
            }
        }
    }
    *mp = '\0';
    *ap++ = ']'; *ap = '\0';
    *bp++ = ']'; *bp = '\0';

    return meta_idx; /* meta-arity */
}

/* ------------------------------------------------------------------ */
/*  meta_discover — group similar generators into meta-families       */
/* ------------------------------------------------------------------ */
#define MAX_GEN_ROWS 1024

typedef struct {
    char family_id[128];
    char generator[4096];
    GenParsed parsed;
    int  cluster; /* -1 = unassigned */
} GenRow;

int meta_discover(sqlite3 *db, const char *content_type)
{
    if (!db || !content_type) return -1;

    /* clear previous meta-families for this content_type */
    {
        sqlite3_stmt *st = NULL;
        const char *sql = "DELETE FROM _meta_members WHERE meta_family_id IN "
                          "(SELECT meta_family_id FROM _meta_families WHERE content_type = ?1)";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
        const char *del2 = "DELETE FROM _meta_families WHERE content_type = ?1";
        if (sqlite3_prepare_v2(db, del2, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            sqlite3_step(st);
            sqlite3_finalize(st);
        }
    }

    /* load all generators for this content_type from _families */
    GenRow *rows = calloc(MAX_GEN_ROWS, sizeof(GenRow));
    if (!rows) return -1;
    int nrows = 0;

    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT family_id, generator FROM _families "
            "WHERE content_type = ?1 ORDER BY family_id";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
            free(rows);
            return -1;
        }
        sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW && nrows < MAX_GEN_ROWS) {
            const char *fid = (const char *)sqlite3_column_text(st, 0);
            const char *gen = (const char *)sqlite3_column_text(st, 1);
            if (fid && gen) {
                strncpy(rows[nrows].family_id, fid, sizeof(rows[nrows].family_id) - 1);
                strncpy(rows[nrows].generator, gen, sizeof(rows[nrows].generator) - 1);
                parse_generator(gen, &rows[nrows].parsed);
                rows[nrows].cluster = -1;
                nrows++;
            }
        }
        sqlite3_finalize(st);
    }

    if (nrows < 2) { free(rows); return 0; }

    /* greedy single-linkage clustering with threshold 0.7 */
    const double THRESHOLD = 0.70;
    int num_clusters = 0;

    for (int i = 0; i < nrows; i++) {
        if (rows[i].cluster >= 0) continue;
        rows[i].cluster = num_clusters;
        /* find all unassigned rows similar to i */
        for (int j = i + 1; j < nrows; j++) {
            if (rows[j].cluster >= 0) continue;
            double sim = gen_similarity(&rows[i].parsed, &rows[j].parsed);
            if (sim >= THRESHOLD) {
                rows[j].cluster = num_clusters;
            }
        }
        num_clusters++;
    }

    /* for each cluster with >= 2 members, compute meta-generator */
    int meta_count = 0;
    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    for (int c = 0; c < num_clusters; c++) {
        /* collect cluster members */
        int members[MAX_GEN_ROWS];
        int mc = 0;
        for (int i = 0; i < nrows; i++) {
            if (rows[i].cluster == c) members[mc++] = i;
        }
        if (mc < 2) continue;

        /* use first member as anchor; anti-unify pairwise with anchor */
        int anchor = members[0];
        char meta_gen[8192];
        strncpy(meta_gen, rows[anchor].generator, sizeof(meta_gen) - 1);
        meta_gen[sizeof(meta_gen) - 1] = '\0';

        /* progressively refine meta-generator */
        GenParsed meta_parsed;
        parse_generator(meta_gen, &meta_parsed);
        int meta_arity = 0;

        for (int m = 1; m < mc; m++) {
            char new_meta[8192], ba[4096], bb[4096];
            int arity = anti_unify_generators(&meta_parsed, &rows[members[m]].parsed,
                                              new_meta, sizeof(new_meta),
                                              ba, sizeof(ba), bb, sizeof(bb));
            if (arity >= 0) {
                strncpy(meta_gen, new_meta, sizeof(meta_gen) - 1);
                meta_gen[sizeof(meta_gen) - 1] = '\0';
                parse_generator(meta_gen, &meta_parsed);
                meta_arity = arity;
            }
        }

        /* compute compression: ratio of shared bytes to total */
        size_t total_gen_bytes = 0;
        for (int m = 0; m < mc; m++)
            total_gen_bytes += strlen(rows[members[m]].generator);
        size_t meta_bytes = strlen(meta_gen);
        double compression = total_gen_bytes > 0
            ? 1.0 - (double)(meta_bytes + mc * meta_arity * 16) / total_gen_bytes
            : 0.0;

        /* insert meta-family */
        sqlite3_stmt *ins = NULL;
        const char *sql =
            "INSERT INTO _meta_families "
            "(content_type, meta_generator, meta_arity, member_count, level, compression, created_at) "
            "VALUES (?1, ?2, ?3, ?4, 2, ?5, ?6)";
        if (sqlite3_prepare_v2(db, sql, -1, &ins, NULL) != SQLITE_OK) continue;
        sqlite3_bind_text(ins, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 2, meta_gen, -1, SQLITE_STATIC);
        sqlite3_bind_int(ins, 3, meta_arity);
        sqlite3_bind_int(ins, 4, mc);
        sqlite3_bind_double(ins, 5, compression);
        sqlite3_bind_text(ins, 6, ts, -1, SQLITE_STATIC);
        if (sqlite3_step(ins) != SQLITE_DONE) {
            sqlite3_finalize(ins);
            continue;
        }
        sqlite3_finalize(ins);
        int64_t mfid = sqlite3_last_insert_rowid(db);

        /* insert meta-family members with their meta-bindings */
        for (int m = 0; m < mc; m++) {
            char ba[4096], bb_unused[4096];
            char tmp_meta[8192];
            GenParsed anch_parsed;
            parse_generator(meta_gen, &anch_parsed);

            int a2 = anti_unify_generators(&anch_parsed, &rows[members[m]].parsed,
                                           tmp_meta, sizeof(tmp_meta),
                                           ba, sizeof(ba), bb_unused, sizeof(bb_unused));
            (void)a2;

            /* bb_unused contains the meta-bindings for this member */
            sqlite3_stmt *mi = NULL;
            const char *msql =
                "INSERT OR REPLACE INTO _meta_members "
                "(meta_family_id, family_id, generator, meta_bindings) "
                "VALUES (?1, ?2, ?3, ?4)";
            if (sqlite3_prepare_v2(db, msql, -1, &mi, NULL) == SQLITE_OK) {
                sqlite3_bind_int64(mi, 1, mfid);
                sqlite3_bind_text(mi, 2, rows[members[m]].family_id, -1, SQLITE_STATIC);
                sqlite3_bind_text(mi, 3, rows[members[m]].generator, -1, SQLITE_STATIC);
                sqlite3_bind_text(mi, 4, bb_unused, -1, SQLITE_STATIC);
                sqlite3_step(mi);
                sqlite3_finalize(mi);
            }
        }
        meta_count++;
    }

    free(rows);
    return meta_count;
}

/* ------------------------------------------------------------------ */
/*  meta_extract — return the meta-generator for a meta-family        */
/* ------------------------------------------------------------------ */
int meta_extract(sqlite3 *db, int meta_family_id, char **out_meta_gen)
{
    if (!db || !out_meta_gen) return -1;
    sqlite3_stmt *st = NULL;
    const char *sql = "SELECT meta_generator FROM _meta_families WHERE meta_family_id = ?1";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(st, 1, meta_family_id);
    int rc = -1;
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *mg = (const char *)sqlite3_column_text(st, 0);
        if (mg) {
            *out_meta_gen = strdup(mg);
            rc = 0;
        }
    }
    sqlite3_finalize(st);
    return rc;
}

/* ------------------------------------------------------------------ */
/*  meta_compact — multi-level compaction pass                        */
/* ------------------------------------------------------------------ */
double meta_compact(sqlite3 *db, const char *content_type)
{
    if (!db || !content_type) return 0.0;

    /* measure pre-compaction size */
    sqlite3_stmt *st = NULL;
    int64_t pre_bytes = 0;
    {
        const char *sql =
            "SELECT COALESCE(SUM(LENGTH(generator)), 0) FROM _families "
            "WHERE content_type = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW)
                pre_bytes = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }

    /* run L2 meta-discovery */
    int n = meta_discover(db, content_type);
    if (n <= 0) return 0.0;

    /* measure post: sum of meta-generator sizes + meta-bindings */
    int64_t post_bytes = 0;
    {
        const char *sql =
            "SELECT COALESCE(SUM(LENGTH(mf.meta_generator) + LENGTH(mm.meta_bindings)), 0) "
            "FROM _meta_families mf JOIN _meta_members mm "
            "ON mf.meta_family_id = mm.meta_family_id "
            "WHERE mf.content_type = ?1";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW)
                post_bytes = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }
    /* non-meta families still occupy their original generator bytes */
    {
        const char *sql =
            "SELECT COALESCE(SUM(LENGTH(generator)), 0) FROM _families "
            "WHERE content_type = ?1 AND family_id NOT IN "
            "(SELECT family_id FROM _meta_members)";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW)
                post_bytes += sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }

    if (pre_bytes == 0) return 0.0;
    return 1.0 - (double)post_bytes / pre_bytes;
}

/* ------------------------------------------------------------------ */
/*  meta_stats — JSON summary of meta-family hierarchy                */
/* ------------------------------------------------------------------ */
int meta_stats(sqlite3 *db, const char *content_type, char **out_json)
{
    if (!db || !content_type || !out_json) return -1;

    size_t cap = 4096;
    char *buf = malloc(cap);
    if (!buf) return -1;
    int off = 0;

    off += snprintf(buf + off, cap - off,
                    "{\"content_type\":\"%s\",\"meta_families\":[", content_type);

    sqlite3_stmt *st = NULL;
    const char *sql =
        "SELECT meta_family_id, meta_generator, meta_arity, member_count, "
        "       compression, created_at "
        "FROM _meta_families WHERE content_type = ?1 ORDER BY meta_family_id";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        free(buf);
        return -1;
    }
    sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);

    int first = 1;
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (!first) buf[off++] = ',';
        first = 0;

        int mfid = sqlite3_column_int(st, 0);
        const char *mg = (const char *)sqlite3_column_text(st, 1);
        int arity = sqlite3_column_int(st, 2);
        int mc = sqlite3_column_int(st, 3);
        double comp = sqlite3_column_double(st, 4);
        const char *ca = (const char *)sqlite3_column_text(st, 5);

        /* grow buffer if needed */
        size_t need = strlen(mg ? mg : "") + strlen(ca ? ca : "") + 256;
        while (off + need >= cap) {
            cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); sqlite3_finalize(st); return -1; }
            buf = nb;
        }

        off += snprintf(buf + off, cap - off,
            "{\"meta_family_id\":%d,\"meta_generator\":\"%s\","
            "\"meta_arity\":%d,\"member_count\":%d,"
            "\"compression\":%.4f,\"created_at\":\"%s\"}",
            mfid, mg ? mg : "", arity, mc, comp, ca ? ca : "");
    }
    sqlite3_finalize(st);

    /* totals */
    int total_mf = 0, total_members = 0;
    double avg_comp = 0.0;
    {
        const char *tsql =
            "SELECT COUNT(*), COALESCE(SUM(member_count),0), "
            "       COALESCE(AVG(compression),0) "
            "FROM _meta_families WHERE content_type = ?1";
        if (sqlite3_prepare_v2(db, tsql, -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, content_type, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW) {
                total_mf = sqlite3_column_int(st, 0);
                total_members = sqlite3_column_int(st, 1);
                avg_comp = sqlite3_column_double(st, 2);
            }
            sqlite3_finalize(st);
        }
    }

    while (off + 256 >= (int)cap) {
        cap *= 2;
        char *nb = realloc(buf, cap);
        if (!nb) { free(buf); return -1; }
        buf = nb;
    }
    off += snprintf(buf + off, cap - off,
        "],\"total_meta_families\":%d,\"total_meta_members\":%d,"
        "\"avg_compression\":%.4f}",
        total_mf, total_members, avg_comp);

    *out_json = buf;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  meta_reduce — decompress from meta-level to original JSON         */
/*  Level path: meta-generator → generator → bindings → JSON         */
/* ------------------------------------------------------------------ */
int meta_reduce(sqlite3 *db, int meta_family_id, int family_id,
                int target_id, char **out_json)
{
    if (!db || !out_json) return -1;

    /* Step 1: load meta-generator + meta-bindings for this family */
    char meta_gen[8192] = {0};
    char meta_bindings[4096] = {0};

    {
        sqlite3_stmt *st = NULL;
        const char *sql =
            "SELECT mf.meta_generator, mm.meta_bindings "
            "FROM _meta_families mf JOIN _meta_members mm "
            "ON mf.meta_family_id = mm.meta_family_id "
            "WHERE mf.meta_family_id = ?1 AND mm.family_id = ?2";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
        sqlite3_bind_int(st, 1, meta_family_id);
        char fid_str[64];
        snprintf(fid_str, sizeof(fid_str), "%d", family_id);
        sqlite3_bind_text(st, 2, fid_str, -1, SQLITE_STATIC);
        if (sqlite3_step(st) != SQLITE_ROW) {
            sqlite3_finalize(st);
            return -1;
        }
        const char *mg = (const char *)sqlite3_column_text(st, 0);
        const char *mb = (const char *)sqlite3_column_text(st, 1);
        if (mg) strncpy(meta_gen, mg, sizeof(meta_gen) - 1);
        if (mb) strncpy(meta_bindings, mb, sizeof(meta_bindings) - 1);
        sqlite3_finalize(st);
    }

    /* Step 2: β-reduce meta-generator with meta-bindings → L1 generator */
    /* Parse meta-bindings as JSON array: ["val0","val1",...] */
    char *mb_vals[128];
    int mb_count = 0;
    {
        char *p = meta_bindings;
        if (*p == '[') p++;
        while (*p && *p != ']' && mb_count < 128) {
            while (*p == ' ' || *p == ',') p++;
            if (*p == '"') {
                p++;
                char *start = p;
                while (*p && *p != '"') p++;
                size_t len = (size_t)(p - start);
                mb_vals[mb_count] = malloc(len + 1);
                if (mb_vals[mb_count]) {
                    memcpy(mb_vals[mb_count], start, len);
                    mb_vals[mb_count][len] = '\0';
                    mb_count++;
                }
                if (*p == '"') p++;
            } else if (*p && *p != ']') {
                char *start = p;
                while (*p && *p != ',' && *p != ']') p++;
                size_t len = (size_t)(p - start);
                mb_vals[mb_count] = malloc(len + 1);
                if (mb_vals[mb_count]) {
                    memcpy(mb_vals[mb_count], start, len);
                    mb_vals[mb_count][len] = '\0';
                    mb_count++;
                }
            }
        }
    }

    /* Replace $$N with meta-binding values */
    char l1_gen[8192];
    {
        char *rp = meta_gen;
        char *wp = l1_gen;
        char *we = l1_gen + sizeof(l1_gen) - 1;
        while (*rp && wp < we) {
            if (rp[0] == '$' && rp[1] == '$' && rp[2] >= '0' && rp[2] <= '9') {
                rp += 2;
                int idx = 0;
                while (*rp >= '0' && *rp <= '9') {
                    idx = idx * 10 + (*rp - '0');
                    rp++;
                }
                if (idx < mb_count && mb_vals[idx]) {
                    int n = snprintf(wp, (size_t)(we - wp), "%s", mb_vals[idx]);
                    wp += n;
                }
            } else {
                *wp++ = *rp++;
            }
        }
        *wp = '\0';
    }

    for (int i = 0; i < mb_count; i++) free(mb_vals[i]);

    /* Step 3: load L0 bindings from _tensor_cells for this target_id */
    /* Then β-reduce generator with bindings → final JSON */
    /* Load bindings */
    char *bind_vals[256];
    int bind_count = 0;
    memset(bind_vals, 0, sizeof(bind_vals));

    {
        sqlite3_stmt *st = NULL;
        char fid_str[64];
        snprintf(fid_str, sizeof(fid_str), "%d", family_id);
        const char *sql =
            "SELECT position, value FROM _tensor_cells "
            "WHERE family_id = ?1 AND target_id = ?2 ORDER BY position";
        if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
            *out_json = strdup(l1_gen);
            return 0; /* partial: return generator without reduction */
        }
        sqlite3_bind_text(st, 1, fid_str, -1, SQLITE_STATIC);
        sqlite3_bind_int(st, 2, target_id);
        while (sqlite3_step(st) == SQLITE_ROW && bind_count < 256) {
            int pos = sqlite3_column_int(st, 0);
            const char *val = (const char *)sqlite3_column_text(st, 1);
            if (pos >= 0 && pos < 256 && val) {
                bind_vals[pos] = strdup(val);
                if (pos >= bind_count) bind_count = pos + 1;
            }
        }
        sqlite3_finalize(st);
    }

    /* β-reduce: replace $N with binding values */
    size_t cap = 16384;
    char *result = malloc(cap);
    if (!result) {
        for (int i = 0; i < bind_count; i++) free(bind_vals[i]);
        return -1;
    }
    {
        char *rp = l1_gen;
        char *wp = result;
        char *we = result + cap - 1;
        while (*rp && wp < we) {
            if (rp[0] == '$' && rp[1] >= '0' && rp[1] <= '9' &&
                !(rp > l1_gen && rp[-1] == '$')) {
                rp++;
                int idx = 0;
                while (*rp >= '0' && *rp <= '9') {
                    idx = idx * 10 + (*rp - '0');
                    rp++;
                }
                const char *val = (idx < bind_count && bind_vals[idx])
                    ? bind_vals[idx] : "null";
                int n = snprintf(wp, (size_t)(we - wp), "%s", val);
                wp += n;
            } else {
                *wp++ = *rp++;
            }
        }
        *wp = '\0';
    }

    for (int i = 0; i < bind_count; i++) free(bind_vals[i]);
    *out_json = result;
    return 0;
}
