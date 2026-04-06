/*
 * BonfyreGraph — Upgrade II: Generator Synthesis Engine
 *
 * Anti-unification based generator proposal with value pattern analysis.
 * Discovers lambda templates from raw JSON entries without prior schema.
 */

#include "synthesis.h"
#include "canonical.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

extern unsigned long long fnv1a64(const void *data, size_t len);
extern void iso_timestamp(char *buf, size_t sz);
extern int db_exec(sqlite3 *db, const char *sql);

/* ================================================================
 * Bootstrap
 * ================================================================ */

int synthesis_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS _synthesis_candidates ("
        "  id            INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  content_type  TEXT NOT NULL,"
        "  generator     TEXT NOT NULL,"
        "  arity         INTEGER DEFAULT 0,"
        "  coverage      INTEGER DEFAULT 0,"
        "  score         REAL DEFAULT 0.0,"
        "  compression   REAL DEFAULT 0.0,"
        "  annotations   TEXT,"     /* JSON: type constraints per position */
        "  created_at    TEXT NOT NULL,"
        "  UNIQUE(content_type, generator)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_synth_ct ON _synthesis_candidates(content_type, score DESC);",

        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* ================================================================
 * Internal: flat JSON key-value parser (reuses pattern from canonical.c)
 * ================================================================ */

#define SYN_MAX_KEYS 128
#define SYN_MAX_KEY  256
#define SYN_MAX_VAL  8192

typedef struct {
    char key[SYN_MAX_KEY];
    char val[SYN_MAX_VAL];
    int  type; /* 0=string, 1=number, 2=bool, 3=null, 4=object, 5=array */
} SynKV;

static int syn_cmp(const void *a, const void *b) {
    return strcmp(((const SynKV *)a)->key, ((const SynKV *)b)->key);
}

static int syn_parse_json(const char *json, SynKV *kvs, int max) {
    int count = 0;
    const char *p = json;
    while (*p && *p != '{') p++;
    if (*p == '{') p++;

    while (*p && *p != '}' && count < max) {
        while (*p && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t' || *p == ',')) p++;
        if (*p == '}' || !*p) break;

        if (*p != '"') { p++; continue; }
        p++;
        size_t ki = 0;
        while (*p && *p != '"' && ki < SYN_MAX_KEY - 1)
            kvs[count].key[ki++] = *p++;
        kvs[count].key[ki] = '\0';
        if (*p == '"') p++;
        while (*p && *p != ':') p++;
        if (*p == ':') p++;
        while (*p && (*p == ' ' || *p == '\t')) p++;

        size_t vi = 0;
        if (*p == '"') {
            kvs[count].type = 0;
            p++;
            while (*p && *p != '"' && vi < SYN_MAX_VAL - 1) {
                if (*p == '\\' && *(p+1)) { kvs[count].val[vi++] = *p++; }
                kvs[count].val[vi++] = *p++;
            }
            if (*p == '"') p++;
        } else if (*p == '{') {
            kvs[count].type = 4;
            int depth = 1;
            kvs[count].val[vi++] = *p++;
            while (*p && depth > 0 && vi < SYN_MAX_VAL - 1) {
                if (*p == '{') depth++;
                if (*p == '}') depth--;
                kvs[count].val[vi++] = *p++;
            }
        } else if (*p == '[') {
            kvs[count].type = 5;
            int depth = 1;
            kvs[count].val[vi++] = *p++;
            while (*p && depth > 0 && vi < SYN_MAX_VAL - 1) {
                if (*p == '[') depth++;
                if (*p == ']') depth--;
                kvs[count].val[vi++] = *p++;
            }
        } else if (strncmp(p, "null", 4) == 0) {
            kvs[count].type = 3;
            memcpy(kvs[count].val, "null", 4); vi = 4; p += 4;
        } else if (strncmp(p, "true", 4) == 0) {
            kvs[count].type = 2;
            memcpy(kvs[count].val, "true", 4); vi = 4; p += 4;
        } else if (strncmp(p, "false", 5) == 0) {
            kvs[count].type = 2;
            memcpy(kvs[count].val, "false", 5); vi = 5; p += 5;
        } else {
            kvs[count].type = 1;
            while (*p && *p != ',' && *p != '}' && *p != ' ' && vi < SYN_MAX_VAL - 1)
                kvs[count].val[vi++] = *p++;
        }
        kvs[count].val[vi] = '\0';
        count++;
    }
    return count;
}

/* ================================================================
 * Type name from value type
 * ================================================================ */

static const char *type_name(int t) {
    switch (t) {
        case 0: return "string";
        case 1: return "number";
        case 2: return "boolean";
        case 3: return "null";
        case 4: return "object";
        case 5: return "array";
        default: return "string";
    }
}

/* ================================================================
 * Anti-unification: most specific generalization of two JSON objects
 *
 * Given {"a":"hello","b":42} and {"a":"world","b":99}
 * Result: generator = {"a":"string","b":"number"}
 *         bindings_a = ["hello",42]
 *         bindings_b = ["world",99]
 *
 * Fields present in only one object get type "optional_<type>".
 * ================================================================ */

int synthesis_anti_unify(const char *json_a, const char *json_b,
                         char **out_gen, char **out_bindings_a,
                         char **out_bindings_b) {
    if (!json_a || !json_b) return -1;

    SynKV kvs_a[SYN_MAX_KEYS], kvs_b[SYN_MAX_KEYS];
    int na = syn_parse_json(json_a, kvs_a, SYN_MAX_KEYS);
    int nb = syn_parse_json(json_b, kvs_b, SYN_MAX_KEYS);

    /* Sort both by key */
    qsort(kvs_a, (size_t)na, sizeof(SynKV), syn_cmp);
    qsort(kvs_b, (size_t)nb, sizeof(SynKV), syn_cmp);

    /* Build generator, bindings_a, bindings_b via two-pointer merge */
    size_t gcap = 4096, bacap = 2048, bbcap = 2048;
    char *gen = malloc(gcap);
    char *ba = malloc(bacap);
    char *bb = malloc(bbcap);
    size_t goff = 0, baoff = 0, bboff = 0;
    gen[goff++] = '{';
    ba[baoff++] = '[';
    bb[bboff++] = '[';
    int first = 1;

    int ia = 0, ib = 0;
    while (ia < na || ib < nb) {
        int cmp;
        if (ia >= na) cmp = 1;
        else if (ib >= nb) cmp = -1;
        else cmp = strcmp(kvs_a[ia].key, kvs_b[ib].key);

        if (!first) {
            gen[goff++] = ',';
            ba[baoff++] = ',';
            bb[bboff++] = ',';
        }
        first = 0;

        /* Ensure capacity */
        while (goff + 512 >= gcap) { gcap *= 2; gen = realloc(gen, gcap); }
        while (baoff + SYN_MAX_VAL + 8 >= bacap) { bacap *= 2; ba = realloc(ba, bacap); }
        while (bboff + SYN_MAX_VAL + 8 >= bbcap) { bbcap *= 2; bb = realloc(bb, bbcap); }

        if (cmp == 0) {
            /* Same key in both: generalize type */
            const char *tn = (kvs_a[ia].type == kvs_b[ib].type)
                ? type_name(kvs_a[ia].type) : "any";
            goff += (size_t)snprintf(gen + goff, gcap - goff,
                "\"%s\":\"%s\"", kvs_a[ia].key, tn);

            if (kvs_a[ia].type == 0) {
                baoff += (size_t)snprintf(ba + baoff, bacap - baoff,
                    "\"%s\"", kvs_a[ia].val);
            } else {
                baoff += (size_t)snprintf(ba + baoff, bacap - baoff,
                    "%s", kvs_a[ia].val);
            }
            if (kvs_b[ib].type == 0) {
                bboff += (size_t)snprintf(bb + bboff, bbcap - bboff,
                    "\"%s\"", kvs_b[ib].val);
            } else {
                bboff += (size_t)snprintf(bb + bboff, bbcap - bboff,
                    "%s", kvs_b[ib].val);
            }
            ia++; ib++;
        } else if (cmp < 0) {
            /* Key only in A */
            goff += (size_t)snprintf(gen + goff, gcap - goff,
                "\"%s\":\"optional_%s\"", kvs_a[ia].key, type_name(kvs_a[ia].type));
            if (kvs_a[ia].type == 0)
                baoff += (size_t)snprintf(ba + baoff, bacap - baoff,
                    "\"%s\"", kvs_a[ia].val);
            else
                baoff += (size_t)snprintf(ba + baoff, bacap - baoff,
                    "%s", kvs_a[ia].val);
            bboff += (size_t)snprintf(bb + bboff, bbcap - bboff, "null");
            ia++;
        } else {
            /* Key only in B */
            goff += (size_t)snprintf(gen + goff, gcap - goff,
                "\"%s\":\"optional_%s\"", kvs_b[ib].key, type_name(kvs_b[ib].type));
            baoff += (size_t)snprintf(ba + baoff, bacap - baoff, "null");
            if (kvs_b[ib].type == 0)
                bboff += (size_t)snprintf(bb + bboff, bbcap - bboff,
                    "\"%s\"", kvs_b[ib].val);
            else
                bboff += (size_t)snprintf(bb + bboff, bbcap - bboff,
                    "%s", kvs_b[ib].val);
            ib++;
        }
    }

    gen[goff++] = '}'; gen[goff] = '\0';
    ba[baoff++] = ']'; ba[baoff] = '\0';
    bb[bboff++] = ']'; bb[bboff] = '\0';

    if (out_gen) *out_gen = gen; else free(gen);
    if (out_bindings_a) *out_bindings_a = ba; else free(ba);
    if (out_bindings_b) *out_bindings_b = bb; else free(bb);
    return 0;
}

/* ================================================================
 * synthesis_score — evaluate generator quality
 *
 * Score = (coverage / total_entries) × (1 - generator_bytes / avg_entry_bytes)
 * ================================================================ */

double synthesis_score(sqlite3 *db, const char *content_type,
                       const char *generator) {
    /* Get structural hash of this generator to find matching entries */
    char gen_hash[17];
    structural_hash(generator, gen_hash);

    /* Count entries that match this generator's structural shape */
    sqlite3_stmt *q;
    int coverage = 0, total = 0;

    if (sqlite3_prepare_v2(db,
        "SELECT COUNT(*) FROM _family_members WHERE target_type=?1",
        -1, &q, NULL) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
        if (sqlite3_step(q) == SQLITE_ROW) total = sqlite3_column_int(q, 0);
        sqlite3_finalize(q);
    }

    if (sqlite3_prepare_v2(db,
        "SELECT member_count FROM _families WHERE content_type=?1 AND family_hash=?2",
        -1, &q, NULL) == SQLITE_OK) {
        sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_bind_text(q, 2, gen_hash, -1, SQLITE_STATIC);
        if (sqlite3_step(q) == SQLITE_ROW) coverage = sqlite3_column_int(q, 0);
        sqlite3_finalize(q);
    }

    if (total == 0) return 0.0;

    double coverage_ratio = (double)coverage / total;
    size_t gen_len = strlen(generator);

    /* Estimate compression: generator is shared, so cost is gen_len + binding_bytes_per_member */
    double compression = gen_len > 0 ? 1.0 / (1.0 + (double)gen_len / 100.0) : 0.0;

    return coverage_ratio * 0.7 + compression * 0.3;
}

/* ================================================================
 * synthesis_propose — main synthesis pipeline
 *
 * 1. Sample pairs of entries
 * 2. Anti-unify each pair to get candidate generators
 * 3. Score each candidate
 * 4. Deduplicate and store top candidates
 * ================================================================ */

int synthesis_propose(sqlite3 *db, const char *content_type) {
    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    /* Get all entries for this content type (up to 1000) */
    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT e.id, e.data FROM "
        "(SELECT DISTINCT target_id as id FROM _ops WHERE target=?1 AND op_type='create') ids "
        "LEFT JOIN (SELECT target_id, data FROM _equivalences "
        "  WHERE target_type=?1 AND repr_type='row' AND materialized=1) e "
        "ON e.target_id = ids.id "
        "LIMIT 500",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);

    /* Collect entry JSONs — reconstruct if needed */
    char **entries = NULL;
    int n_entries = 0;
    int cap = 64;
    entries = calloc((size_t)cap, sizeof(char *));

    while (sqlite3_step(q) == SQLITE_ROW) {
        const char *data = (const char *)sqlite3_column_text(q, 1);
        if (!data || !data[0]) continue;
        if (n_entries >= cap) {
            cap *= 2;
            entries = realloc(entries, (size_t)cap * sizeof(char *));
        }
        entries[n_entries++] = strdup(data);
    }
    sqlite3_finalize(q);

    if (n_entries < 2) {
        for (int i = 0; i < n_entries; i++) free(entries[i]);
        free(entries);
        return 0;
    }

    /* Sample pairs and anti-unify.
       For efficiency, use strided sampling: pairs (0,1), (2,3), (0,2), (1,3), etc. */
    int proposed = 0;
    int max_pairs = n_entries < 50 ? n_entries * (n_entries - 1) / 2 : 200;
    int pair_count = 0;

    for (int i = 0; i < n_entries && pair_count < max_pairs; i++) {
        for (int j = i + 1; j < n_entries && pair_count < max_pairs; j += (n_entries / 20 + 1)) {
            char *gen = NULL, *ba = NULL, *bb = NULL;
            if (synthesis_anti_unify(entries[i], entries[j], &gen, &ba, &bb) == 0 && gen) {
                /* Score this candidate */
                double score = synthesis_score(db, content_type, gen);

                /* Count arity (number of commas + 1 in generator) */
                int arity = 1;
                for (const char *p = gen; *p; p++) {
                    if (*p == ',') arity++;
                }

                /* Compute compression ratio */
                size_t gen_bytes = strlen(gen);
                size_t avg_entry = (strlen(entries[i]) + strlen(entries[j])) / 2;
                double compression = avg_entry > 0
                    ? 1.0 - (double)gen_bytes / (double)avg_entry : 0.0;

                /* Store candidate */
                sqlite3_stmt *ins;
                if (sqlite3_prepare_v2(db,
                    "INSERT OR REPLACE INTO _synthesis_candidates "
                    "(content_type, generator, arity, coverage, score, "
                    " compression, created_at) "
                    "VALUES (?1, ?2, ?3, 0, ?4, ?5, ?6)",
                    -1, &ins, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(ins, 1, content_type, -1, SQLITE_STATIC);
                    sqlite3_bind_text(ins, 2, gen, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int(ins, 3, arity);
                    sqlite3_bind_double(ins, 4, score);
                    sqlite3_bind_double(ins, 5, compression);
                    sqlite3_bind_text(ins, 6, ts, -1, SQLITE_STATIC);
                    if (sqlite3_step(ins) == SQLITE_DONE) proposed++;
                    sqlite3_finalize(ins);
                }
            }
            free(gen); free(ba); free(bb);
            pair_count++;
        }
    }

    /* Update coverage counts for all candidates */
    sqlite3_stmt *upd;
    if (sqlite3_prepare_v2(db,
        "UPDATE _synthesis_candidates SET coverage = "
        "(SELECT COALESCE(f.member_count, 0) FROM _families f "
        " WHERE f.content_type = _synthesis_candidates.content_type "
        " AND f.generator = _synthesis_candidates.generator LIMIT 1) "
        "WHERE content_type = ?1",
        -1, &upd, NULL) == SQLITE_OK) {
        sqlite3_bind_text(upd, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_step(upd);
        sqlite3_finalize(upd);
    }

    for (int i = 0; i < n_entries; i++) free(entries[i]);
    free(entries);
    return proposed;
}

/* ================================================================
 * synthesis_candidates — get top N
 * ================================================================ */

int synthesis_candidates(sqlite3 *db, const char *content_type,
                         int top_n, char **out_json) {
    *out_json = NULL;

    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT generator, arity, coverage, score, compression "
        "FROM _synthesis_candidates WHERE content_type=?1 "
        "ORDER BY score DESC LIMIT ?2",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(q, 2, top_n);

    size_t cap = 4096;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';
    int count = 0;

    while (sqlite3_step(q) == SQLITE_ROW) {
        const char *gen = (const char *)sqlite3_column_text(q, 0);
        while (off + 1024 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (count > 0) out[off++] = ',';
        off += (size_t)snprintf(out + off, cap - off,
            "{\"generator\":%s,\"arity\":%d,\"coverage\":%d,"
            "\"score\":%.4f,\"compression\":%.4f}",
            gen ? gen : "{}",
            sqlite3_column_int(q, 1),
            sqlite3_column_int(q, 2),
            sqlite3_column_double(q, 3),
            sqlite3_column_double(q, 4));
        count++;
    }
    sqlite3_finalize(q);

    out[off++] = ']'; out[off] = '\0';
    *out_json = out;
    return count;
}

/* ================================================================
 * synthesis_refine — analyze bindings to add type annotations
 *
 * For each position in a family:
 *   - String: detect enum sets (< 20 distinct values), format patterns
 *   - Number: detect range [min, max], integer vs float
 *   - Boolean: detect always true/false
 * ================================================================ */

int synthesis_refine(sqlite3 *db, int family_id, char **out_refined) {
    *out_refined = NULL;

    /* Get generator */
    sqlite3_stmt *gq;
    if (sqlite3_prepare_v2(db,
        "SELECT generator FROM _families WHERE id=?1",
        -1, &gq, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(gq, 1, family_id);

    char generator[32768] = {0};
    if (sqlite3_step(gq) == SQLITE_ROW) {
        const char *g = (const char *)sqlite3_column_text(gq, 0);
        if (g) strncpy(generator, g, sizeof(generator) - 1);
    }
    sqlite3_finalize(gq);
    if (!generator[0]) return -1;

    /* Parse generator to get fields */
    SynKV fields[SYN_MAX_KEYS];
    int nfields = syn_parse_json(generator, fields, SYN_MAX_KEYS);
    qsort(fields, (size_t)nfields, sizeof(SynKV), syn_cmp);

    /* Build refined generator with annotations */
    size_t cap = 8192;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '{';

    for (int pos = 0; pos < nfields; pos++) {
        if (pos > 0) out[off++] = ',';
        while (off + 2048 >= cap) { cap *= 2; out = realloc(out, cap); }

        const char *fname = fields[pos].key;
        const char *ftype = fields[pos].val;

        /* Analyze this position's bindings */
        sqlite3_stmt *aq;
        int distinct = 0, total = 0, all_int = 1;
        double min_n = 0, max_n = 0;

        if (sqlite3_prepare_v2(db,
            "SELECT COUNT(DISTINCT val_text), COUNT(*), MIN(val_num), MAX(val_num), "
            "val_type FROM _tensor_cells "
            "WHERE family_id=?1 AND position=?2 GROUP BY val_type ORDER BY COUNT(*) DESC LIMIT 1",
            -1, &aq, NULL) == SQLITE_OK) {
            sqlite3_bind_int(aq, 1, family_id);
            sqlite3_bind_int(aq, 2, pos);
            if (sqlite3_step(aq) == SQLITE_ROW) {
                distinct = sqlite3_column_int(aq, 0);
                total = sqlite3_column_int(aq, 1);
                min_n = sqlite3_column_double(aq, 2);
                max_n = sqlite3_column_double(aq, 3);
            }
            sqlite3_finalize(aq);
        }

        /* Check if all numeric values are integers */
        if (strcmp(ftype, "number") == 0) {
            sqlite3_stmt *iq;
            if (sqlite3_prepare_v2(db,
                "SELECT COUNT(*) FROM _tensor_cells "
                "WHERE family_id=?1 AND position=?2 AND val_num != CAST(val_num AS INTEGER)",
                -1, &iq, NULL) == SQLITE_OK) {
                sqlite3_bind_int(iq, 1, family_id);
                sqlite3_bind_int(iq, 2, pos);
                if (sqlite3_step(iq) == SQLITE_ROW) {
                    all_int = (sqlite3_column_int(iq, 0) == 0);
                }
                sqlite3_finalize(iq);
            }
        }

        /* Build annotation */
        if (strcmp(ftype, "string") == 0 && distinct > 0 && distinct <= 20 && total > 5) {
            /* Enum: collect all distinct values */
            off += (size_t)snprintf(out + off, cap - off, "\"%s\":\"enum(", fname);
            sqlite3_stmt *eq;
            if (sqlite3_prepare_v2(db,
                "SELECT DISTINCT val_text FROM _tensor_cells "
                "WHERE family_id=?1 AND position=?2 AND val_type=0 "
                "ORDER BY val_text LIMIT 20",
                -1, &eq, NULL) == SQLITE_OK) {
                sqlite3_bind_int(eq, 1, family_id);
                sqlite3_bind_int(eq, 2, pos);
                int efirst = 1;
                while (sqlite3_step(eq) == SQLITE_ROW) {
                    const char *ev = (const char *)sqlite3_column_text(eq, 0);
                    if (!efirst) { out[off++] = '|'; }
                    efirst = 0;
                    while (off + 256 >= cap) { cap *= 2; out = realloc(out, cap); }
                    off += (size_t)snprintf(out + off, cap - off, "%s", ev ? ev : "");
                }
                sqlite3_finalize(eq);
            }
            off += (size_t)snprintf(out + off, cap - off, ")\"");
        } else if (strcmp(ftype, "number") == 0 && total > 0) {
            if (all_int) {
                off += (size_t)snprintf(out + off, cap - off,
                    "\"%s\":\"int(%lld..%lld)\"", fname,
                    (long long)min_n, (long long)max_n);
            } else {
                off += (size_t)snprintf(out + off, cap - off,
                    "\"%s\":\"float(%.2f..%.2f)\"", fname, min_n, max_n);
            }
        } else {
            off += (size_t)snprintf(out + off, cap - off,
                "\"%s\":\"%s\"", fname, ftype);
        }
    }

    out[off++] = '}'; out[off] = '\0';
    *out_refined = out;
    return nfields;
}
