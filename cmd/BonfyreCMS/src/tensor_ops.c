/*
 * BonfyreGraph — Tensor Operations: Compressed-Domain Lambda Calculus
 *
 * The generator is λ(b₀,...,bₙ).{field₀:b₀,...,fieldₙ:bₙ}
 * Compression = λ-abstraction.  Decompression = β-reduction.
 * All operations choose a reduction strategy rather than decompressing.
 *
 * Eigenvalue tracking monitors the spectral health of each abstraction.
 */

#include "tensor_ops.h"
#include "canonical.h"
#include "ephemeral.h"
#include "lt_egraph.h"
#include "columnar.h"
#include "hybrid_reduce.h"
#include "type_check.h"
#include "bench_metrics.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* Declared in main.c */
extern unsigned long long fnv1a64(const void *data, size_t len);
extern void iso_timestamp(char *buf, size_t sz);
extern int db_exec(sqlite3 *db, const char *sql);

#define MAX_FIELD_NAME_LEN 128

/* ================================================================
 * Bootstrap — _tensor_eigen and _tensor_bindings tables
 * ================================================================ */

int tensor_ops_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        /* Per-position eigenvalue / variance tracking */
        "CREATE TABLE IF NOT EXISTS _tensor_eigen ("
        "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  family_id   INTEGER NOT NULL,"
        "  position    INTEGER NOT NULL,"
        "  field_name  TEXT NOT NULL,"
        "  eigenvalue  REAL DEFAULT 0.0,"
        "  variance    REAL DEFAULT 0.0,"
        "  mean        REAL DEFAULT 0.0,"
        "  updated_at  TEXT NOT NULL,"
        "  UNIQUE(family_id, position)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_teig_fam ON _tensor_eigen(family_id);",

        /* Columnar per-cell binding storage for efficient compressed-domain ops */
        "CREATE TABLE IF NOT EXISTS _tensor_cells ("
        "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  family_id   INTEGER NOT NULL,"
        "  target_type TEXT NOT NULL,"
        "  target_id   INTEGER NOT NULL,"
        "  position    INTEGER NOT NULL,"
        "  field_name  TEXT NOT NULL,"
        "  val_text    TEXT,"
        "  val_num     REAL,"
        "  val_type    INTEGER DEFAULT 0,"   /* 0=string, 1=number, 2=bool, 3=null */
        "  UNIQUE(family_id, target_id, position)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_tcell_fam ON _tensor_cells(family_id, position);",
        "CREATE INDEX IF NOT EXISTS idx_tcell_target ON _tensor_cells(target_type, target_id);",
        "CREATE INDEX IF NOT EXISTS idx_tcell_field ON _tensor_cells(family_id, field_name);",

        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* ================================================================
 * Internal: parse generator to build field→position map
 *
 * Generator format: {"field1":"type1","field2":"type2",...}
 * Fields are in sorted order (canonical). position = order index.
 * ================================================================ */

#define MAX_ARITY 128

typedef struct {
    char name[256];
    char type_tag[32];   /* "string", "number", "boolean", "null", "object", "array" */
    int  position;
} LambdaParam;

typedef struct {
    LambdaParam params[MAX_ARITY];
    int arity;
} LambdaExpr;

/* Parse generator JSON into lambda expression parameters */
static int parse_lambda(const char *generator, LambdaExpr *expr) {
    if (!generator || !expr) return -1;
    expr->arity = 0;

    const char *p = generator;
    while (*p && *p != '{') p++;
    if (*p == '{') p++;

    while (*p && *p != '}' && expr->arity < MAX_ARITY) {
        while (*p && (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t')) p++;
        if (*p == '}' || !*p) break;

        /* field name in quotes */
        if (*p != '"') { p++; continue; }
        p++;
        size_t ni = 0;
        while (*p && *p != '"' && ni < 255) {
            expr->params[expr->arity].name[ni++] = *p++;
        }
        expr->params[expr->arity].name[ni] = '\0';
        if (*p == '"') p++;

        /* skip : */
        while (*p && *p != ':') p++;
        if (*p == ':') p++;
        while (*p && (*p == ' ' || *p == '\t')) p++;

        /* type tag in quotes */
        if (*p == '"') {
            p++;
            size_t ti = 0;
            while (*p && *p != '"' && ti < 31) {
                expr->params[expr->arity].type_tag[ti++] = *p++;
            }
            expr->params[expr->arity].type_tag[ti] = '\0';
            if (*p == '"') p++;
        }

        expr->params[expr->arity].position = expr->arity;
        expr->arity++;
    }
    return expr->arity;
}

/* Find position index for a field name in the lambda. Returns -1 if not found. */
static int lambda_field_pos(const LambdaExpr *expr, const char *field) {
    for (int i = 0; i < expr->arity; i++) {
        if (strcmp(expr->params[i].name, field) == 0) return i;
    }
    return -1;
}

/* ================================================================
 * Internal: parse a JSON array of bindings into separate values
 * ================================================================ */

typedef struct {
    char val[8192];
    int  is_string;  /* 1 if string, 0 if number/bool/null */
    double num_val;  /* numeric value if applicable */
} BindingVal;

static int parse_bindings(const char *bindings_json, BindingVal *vals, int max_vals) {
    if (!bindings_json) return 0;
    int count = 0;
    const char *p = bindings_json;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    while (*p && *p != ']' && count < max_vals) {
        while (*p && (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t')) p++;
        if (*p == ']' || !*p) break;

        vals[count].is_string = 0;
        vals[count].num_val = 0.0;

        if (*p == '"') {
            /* string value */
            vals[count].is_string = 1;
            p++;
            size_t vi = 0;
            while (*p && *p != '"' && vi < sizeof(vals[0].val) - 1) {
                if (*p == '\\' && *(p + 1)) {
                    vals[count].val[vi++] = *p++;
                    if (vi < sizeof(vals[0].val) - 1) vals[count].val[vi++] = *p++;
                    continue;
                }
                vals[count].val[vi++] = *p++;
            }
            vals[count].val[vi] = '\0';
            if (*p == '"') p++;
        } else if (*p == '{' || *p == '[') {
            /* nested object/array */
            char open = *p, close = (open == '{') ? '}' : ']';
            int depth = 1;
            size_t vi = 0;
            vals[count].val[vi++] = *p++;
            while (*p && depth > 0 && vi < sizeof(vals[0].val) - 1) {
                if (*p == open) depth++;
                if (*p == close) depth--;
                vals[count].val[vi++] = *p++;
            }
            vals[count].val[vi] = '\0';
        } else {
            /* number, bool, null */
            size_t vi = 0;
            while (*p && *p != ',' && *p != ']' && vi < sizeof(vals[0].val) - 1) {
                vals[count].val[vi++] = *p++;
            }
            while (vi > 0 && (vals[count].val[vi - 1] == ' ' || vals[count].val[vi - 1] == '\t'))
                vi--;
            vals[count].val[vi] = '\0';

            /* Try parse as number */
            char *end = NULL;
            double d = strtod(vals[count].val, &end);
            if (end && end != vals[count].val && (*end == '\0' || *end == ' ')) {
                vals[count].num_val = d;
            }
        }
        count++;
    }
    return count;
}

/* ================================================================
 * Primitive 1: tensor_abstract
 *
 * Runs family_discover, then decomposes bindings into columnar
 * _tensor_cells and computes eigenvalue profile per family.
 * ================================================================ */

int tensor_abstract(sqlite3 *db, const char *content_type) {
    /* Step 1: run family discovery (from canonical.c) */
    int families_found = family_discover(db, content_type);

    /* Step 2: run lt_compact to populate tensor reprs in e-graph */
    lt_compact(db, content_type);

    if (families_found <= 0) return 0;

    /* Rebuild the tensor projection from scratch for this content type. */
    {
        sqlite3_stmt *wipe_cells = NULL;
        if (sqlite3_prepare_v2(db,
            "DELETE FROM _tensor_cells WHERE target_type=?1",
            -1, &wipe_cells, NULL) != SQLITE_OK) {
            return -1;
        }
        sqlite3_bind_text(wipe_cells, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_step(wipe_cells);
        sqlite3_finalize(wipe_cells);
    }
    if (db_exec(db,
        "DELETE FROM _tensor_eigen WHERE family_id NOT IN (SELECT id FROM _families)") != 0) {
        return -1;
    }

    /* Step 3: for each family, decompose bindings into columnar cells */
    sqlite3_stmt *fam_stmt;
    const char *fam_sql =
        "SELECT id, generator, member_count FROM _families WHERE content_type=?1";
    if (sqlite3_prepare_v2(db, fam_sql, -1, &fam_stmt, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(fam_stmt, 1, content_type, -1, SQLITE_STATIC);

    int abstracted = 0;
    sqlite3_stmt *del_cells = NULL;
    sqlite3_stmt *mem_stmt = NULL;
    sqlite3_stmt *ins_cell = NULL;

    if (sqlite3_prepare_v2(db,
        "DELETE FROM _tensor_cells WHERE family_id=?1",
        -1, &del_cells, NULL) != SQLITE_OK) {
        sqlite3_finalize(fam_stmt);
        return -1;
    }
    if (sqlite3_prepare_v2(db,
        "SELECT target_id, bindings FROM _family_members "
        "WHERE family_id=?1 AND target_type=?2",
        -1, &mem_stmt, NULL) != SQLITE_OK) {
        sqlite3_finalize(del_cells);
        sqlite3_finalize(fam_stmt);
        return -1;
    }
    if (sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO _tensor_cells "
        "(family_id, target_type, target_id, position, field_name, "
        " val_text, val_num, val_type) "
        "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        -1, &ins_cell, NULL) != SQLITE_OK) {
        sqlite3_finalize(mem_stmt);
        sqlite3_finalize(del_cells);
        sqlite3_finalize(fam_stmt);
        return -1;
    }

    while (sqlite3_step(fam_stmt) == SQLITE_ROW) {
        int fam_id = sqlite3_column_int(fam_stmt, 0);
        const char *generator = (const char *)sqlite3_column_text(fam_stmt, 1);
        int member_count = sqlite3_column_int(fam_stmt, 2);
        if (member_count < 1 || !generator) continue;

        /* Parse the lambda (generator) */
        LambdaExpr expr;
        if (parse_lambda(generator, &expr) <= 0) continue;

        /* Clear old cells for this family */
        sqlite3_reset(del_cells);
        sqlite3_clear_bindings(del_cells);
        sqlite3_bind_int(del_cells, 1, fam_id);
        sqlite3_step(del_cells);

        /* For each member, decompose bindings into per-position cells */
        sqlite3_reset(mem_stmt);
        sqlite3_clear_bindings(mem_stmt);
        sqlite3_bind_int(mem_stmt, 1, fam_id);
        sqlite3_bind_text(mem_stmt, 2, content_type, -1, SQLITE_STATIC);

        while (sqlite3_step(mem_stmt) == SQLITE_ROW) {
            int target_id = sqlite3_column_int(mem_stmt, 0);
            const char *bindings = (const char *)sqlite3_column_text(mem_stmt, 1);

            BindingVal bvals[MAX_ARITY];
            int bcount = parse_bindings(bindings, bvals, MAX_ARITY);
            int limit = bcount < expr.arity ? bcount : expr.arity;

            for (int pos = 0; pos < limit; pos++) {
                sqlite3_reset(ins_cell);
                sqlite3_clear_bindings(ins_cell);
                sqlite3_bind_int(ins_cell, 1, fam_id);
                sqlite3_bind_text(ins_cell, 2, content_type, -1, SQLITE_STATIC);
                sqlite3_bind_int(ins_cell, 3, target_id);
                sqlite3_bind_int(ins_cell, 4, pos);
                sqlite3_bind_text(ins_cell, 5, expr.params[pos].name, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins_cell, 6, bvals[pos].val, -1, SQLITE_TRANSIENT);
                sqlite3_bind_double(ins_cell, 7, bvals[pos].num_val);
                sqlite3_bind_int(ins_cell, 8, bvals[pos].is_string ? 0 : 1);
                sqlite3_step(ins_cell);
            }
        }

        /* Step 4: compute eigenvalue profile for this family */
        TensorEigen eig;
        tensor_eigen(db, fam_id, &eig);
        tensor_eigen_free(&eig);

        abstracted++;
    }
    sqlite3_finalize(ins_cell);
    sqlite3_finalize(mem_stmt);
    sqlite3_finalize(del_cells);
    sqlite3_finalize(fam_stmt);

    return abstracted;
}

/* ================================================================
 * Primitive 4: tensor_eigen
 *
 * For each binding position in a family, compute:
 *   - mean (for numeric positions)
 *   - variance
 *   - eigenvalue (= variance for independent positions;
 *     covariance matrix diagonal for correlated positions)
 *
 * For string positions: variance = count of distinct values / total.
 * This gives a [0,1] "information density" measure.
 *
 * Eigenvalue interpretation:
 *   λ ≈ 0  → position is nearly constant → absorb into generator
 *   λ ≈ 1  → position carries full information
 *   Σ(top-k λ) / Σ(λ) → structural leverage ratio
 * ================================================================ */

int tensor_eigen(sqlite3 *db, int family_id, TensorEigen *eigen_out) {
    if (eigen_out) {
        memset(eigen_out, 0, sizeof(TensorEigen));
        eigen_out->family_id = family_id;
    }

    /* Get arity and member count from family */
    sqlite3_stmt *fam_q;
    int arity = 0, member_count = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT MAX(position) + 1, COUNT(DISTINCT target_id) FROM _tensor_cells WHERE family_id=?1",
        -1, &fam_q, NULL) == SQLITE_OK) {
        sqlite3_bind_int(fam_q, 1, family_id);
        if (sqlite3_step(fam_q) == SQLITE_ROW) {
            arity = sqlite3_column_int(fam_q, 0);
            member_count = sqlite3_column_int(fam_q, 1);
        }
        sqlite3_finalize(fam_q);
    }

    if (arity <= 0 || member_count <= 0) return -1;

    double *eigenvalues = calloc((size_t)arity, sizeof(double));
    double *variances = calloc((size_t)arity, sizeof(double));
    if (!eigenvalues || !variances) {
        free(eigenvalues);
        free(variances);
        return -1;
    }

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    /* Clear old eigen data for this family */
    {
        sqlite3_stmt *del;
        if (sqlite3_prepare_v2(db,
            "DELETE FROM _tensor_eigen WHERE family_id=?1",
            -1, &del, NULL) == SQLITE_OK) {
            sqlite3_bind_int(del, 1, family_id);
            sqlite3_step(del);
            sqlite3_finalize(del);
        }
    }

    double total_var = 0.0;
    int optimal_rank = 0;

    /* For each position, compute variance */
    for (int pos = 0; pos < arity; pos++) {
        sqlite3_stmt *cell_q;
        double mean = 0.0, var = 0.0;
        char field_name[256] = {0};
        int is_numeric = 0;

        /* Check if this position is numeric */
        if (sqlite3_prepare_v2(db,
            "SELECT val_type, field_name FROM _tensor_cells "
            "WHERE family_id=?1 AND position=?2 LIMIT 1",
            -1, &cell_q, NULL) == SQLITE_OK) {
            sqlite3_bind_int(cell_q, 1, family_id);
            sqlite3_bind_int(cell_q, 2, pos);
            if (sqlite3_step(cell_q) == SQLITE_ROW) {
                is_numeric = (sqlite3_column_int(cell_q, 0) == 1);
                const char *fn = (const char *)sqlite3_column_text(cell_q, 1);
                if (fn) strncpy(field_name, fn, 255);
            }
            sqlite3_finalize(cell_q);
        }

        if (is_numeric && member_count > 0) {
            /* Numeric: compute actual mean and variance */
            if (sqlite3_prepare_v2(db,
                "SELECT AVG(val_num), "
                "CASE WHEN COUNT(*) > 1 "
                "  THEN SUM((val_num - (SELECT AVG(val_num) FROM _tensor_cells "
                "    WHERE family_id=?1 AND position=?2)) * "
                "   (val_num - (SELECT AVG(val_num) FROM _tensor_cells "
                "    WHERE family_id=?1 AND position=?2))) / (COUNT(*) - 1) "
                "  ELSE 0.0 END "
                "FROM _tensor_cells WHERE family_id=?1 AND position=?2",
                -1, &cell_q, NULL) == SQLITE_OK) {
                sqlite3_bind_int(cell_q, 1, family_id);
                sqlite3_bind_int(cell_q, 2, pos);
                if (sqlite3_step(cell_q) == SQLITE_ROW) {
                    mean = sqlite3_column_double(cell_q, 0);
                    var = sqlite3_column_double(cell_q, 1);
                }
                sqlite3_finalize(cell_q);
            }
        } else {
            /* String/other: variance = distinct_count / total_count (entropy proxy) */
            if (sqlite3_prepare_v2(db,
                "SELECT CAST(COUNT(DISTINCT val_text) AS REAL) / MAX(COUNT(*), 1) "
                "FROM _tensor_cells WHERE family_id=?1 AND position=?2",
                -1, &cell_q, NULL) == SQLITE_OK) {
                sqlite3_bind_int(cell_q, 1, family_id);
                sqlite3_bind_int(cell_q, 2, pos);
                if (sqlite3_step(cell_q) == SQLITE_ROW) {
                    var = sqlite3_column_double(cell_q, 0);
                }
                sqlite3_finalize(cell_q);
            }
            mean = 0.0; /* not meaningful for strings */
        }

        eigenvalues[pos] = var;  /* eigenvalue = variance for independent positions */
        variances[pos] = var;
        total_var += var;

        /* Position has meaningful variance if > 0.01 */
        if (var > 0.01) optimal_rank++;

        /* Store to _tensor_eigen */
        sqlite3_stmt *ins;
        if (sqlite3_prepare_v2(db,
            "INSERT INTO _tensor_eigen (family_id, position, field_name, "
            "eigenvalue, variance, mean, updated_at) "
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            -1, &ins, NULL) == SQLITE_OK) {
            sqlite3_bind_int(ins, 1, family_id);
            sqlite3_bind_int(ins, 2, pos);
            sqlite3_bind_text(ins, 3, field_name, -1, SQLITE_STATIC);
            sqlite3_bind_double(ins, 4, var);
            sqlite3_bind_double(ins, 5, var);
            sqlite3_bind_double(ins, 6, mean);
            sqlite3_bind_text(ins, 7, ts, -1, SQLITE_STATIC);
            sqlite3_step(ins);
            sqlite3_finalize(ins);
        }
    }

    if (eigen_out) {
        eigen_out->arity = arity;
        eigen_out->member_count = member_count;
        eigen_out->eigenvalues = eigenvalues;
        eigen_out->variance = variances;
        eigen_out->total_variance = total_var;
        eigen_out->structural_ratio = (total_var > 0.0 && optimal_rank > 0)
            ? 1.0 - ((double)optimal_rank / arity)  /* higher = more compressible */
            : 0.0;
        eigen_out->optimal_rank = optimal_rank;
    } else {
        free(eigenvalues);
        free(variances);
    }

    return 0;
}

void tensor_eigen_free(TensorEigen *e) {
    if (!e) return;
    free(e->eigenvalues);
    free(e->variance);
    e->eigenvalues = NULL;
    e->variance = NULL;
}

static int decltype_is_integer_like(const char *decl_type) {
    if (!decl_type) return 0;
    return strstr(decl_type, "INT") != NULL || strstr(decl_type, "BOOL") != NULL;
}

static int lookup_column_decl_type(sqlite3 *db, const char *table_name,
                                   const char *column_name,
                                   char *out, size_t out_sz) {
    sqlite3_stmt *stmt = NULL;
    char *sql = NULL;
    int found = -1;

    if (!db || !table_name || !column_name || !out || out_sz == 0) return -1;
    out[0] = '\0';

    sql = sqlite3_mprintf("PRAGMA table_info(\"%w\")", table_name);
    if (!sql) return -1;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_free(sql);
        return -1;
    }
    sqlite3_free(sql);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 1);
        const char *decl_type = (const char *)sqlite3_column_text(stmt, 2);
        if (name && strcmp(name, column_name) == 0) {
            if (decl_type) {
                strncpy(out, decl_type, out_sz - 1);
                out[out_sz - 1] = '\0';
            }
            found = 0;
            break;
        }
    }
    sqlite3_finalize(stmt);
    return found;
}

static int sync_symbolic_update_to_row(sqlite3 *db, const char *target_type, int target_id,
                                       const char *field, const char *value,
                                       int new_type, double new_num) {
    sqlite3_stmt *stmt = NULL;
    char decl_type[64] = {0};
    char ts[64];
    char *sql = NULL;
    int rc = -1;

    if (!db || !target_type || !field || !value) return -1;
    if (lookup_column_decl_type(db, target_type, field, decl_type, sizeof(decl_type)) != 0)
        return -1;

    iso_timestamp(ts, sizeof(ts));

    if (new_type == 2 && strcmp(value, "null") == 0) {
        sql = sqlite3_mprintf(
            "UPDATE \"%w\" SET \"%w\"=NULL, updated_at=?1 WHERE id=?2",
            target_type, field);
        if (!sql) return -1;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) goto cleanup;
        sqlite3_bind_text(stmt, 1, ts, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 2, target_id);
    } else {
        sql = sqlite3_mprintf(
            "UPDATE \"%w\" SET \"%w\"=?1, updated_at=?2 WHERE id=?3",
            target_type, field);
        if (!sql) return -1;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) goto cleanup;

        if (new_type == 0) {
            sqlite3_bind_text(stmt, 1, value, -1, SQLITE_STATIC);
        } else if (new_type == 1) {
            if (decltype_is_integer_like(decl_type) &&
                new_num == (double)(long long)new_num) {
                sqlite3_bind_int64(stmt, 1, (sqlite3_int64)new_num);
            } else {
                sqlite3_bind_double(stmt, 1, new_num);
            }
        } else if (strcmp(value, "true") == 0 || strcmp(value, "false") == 0) {
            if (decltype_is_integer_like(decl_type)) {
                sqlite3_bind_int(stmt, 1, strcmp(value, "true") == 0 ? 1 : 0);
            } else {
                sqlite3_bind_text(stmt, 1, value, -1, SQLITE_STATIC);
            }
        } else {
            sqlite3_bind_text(stmt, 1, value, -1, SQLITE_STATIC);
        }

        sqlite3_bind_text(stmt, 2, ts, -1, SQLITE_STATIC);
        sqlite3_bind_int(stmt, 3, target_id);
    }

    rc = (sqlite3_step(stmt) == SQLITE_DONE) ? 0 : -1;

cleanup:
    if (stmt) sqlite3_finalize(stmt);
    sqlite3_free(sql);
    return rc;
}

/* ================================================================
 * Primitive 2: tensor_reduce
 *
 * REDUCE_FULL:     β-reduce — apply lambda to all bindings → JSON
 * REDUCE_PARTIAL:  Apply with field subset → projected JSON
 * REDUCE_SYMBOLIC: Rewrite a binding in-place → update without decompression
 * ================================================================ */

int tensor_reduce(sqlite3 *db, const char *target_type, int target_id,
                  int strategy, const char *fields, const char *value,
                  char **out_json)
{
    *out_json = NULL;

    /* REDUCE_AUTO: delegate to hybrid cost model for strategy selection */
    if (strategy == REDUCE_AUTO) {
        /* Look up family_id for this target to route through hybrid_reduce */
        sqlite3_stmt *fq = NULL;
        char fid_str[32] = {0};
        if (sqlite3_prepare_v2(db,
            "SELECT m.family_id FROM _family_members m "
            "WHERE m.target_type=?1 AND m.target_id=?2 LIMIT 1",
            -1, &fq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(fq, 1, target_type, -1, SQLITE_STATIC);
            sqlite3_bind_int(fq, 2, target_id);
            if (sqlite3_step(fq) == SQLITE_ROW)
                snprintf(fid_str, sizeof(fid_str), "%d", sqlite3_column_int(fq, 0));
            sqlite3_finalize(fq);
        }
        if (fid_str[0])
            return hybrid_reduce(db, fid_str, target_id, NULL, 0, out_json);
        /* Fall through to REDUCE_FULL if family lookup fails */
        strategy = REDUCE_FULL;
    }

    /* Find which family this entity belongs to */
    sqlite3_stmt *fam_q;
    int family_id = 0;
    char generator[32768] = {0};
    if (sqlite3_prepare_v2(db,
        "SELECT f.id, f.generator FROM _families f "
        "JOIN _family_members m ON m.family_id = f.id "
        "WHERE m.target_type=?1 AND m.target_id=?2 LIMIT 1",
        -1, &fam_q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(fam_q, 1, target_type, -1, SQLITE_STATIC);
    sqlite3_bind_int(fam_q, 2, target_id);
    if (sqlite3_step(fam_q) != SQLITE_ROW) {
        sqlite3_finalize(fam_q);
        return -1;
    }
    family_id = sqlite3_column_int(fam_q, 0);
    const char *gen_text = (const char *)sqlite3_column_text(fam_q, 1);
    if (gen_text) strncpy(generator, gen_text, sizeof(generator) - 1);
    sqlite3_finalize(fam_q);

    /* Parse the lambda */
    LambdaExpr expr;
    if (parse_lambda(generator, &expr) <= 0) return -1;

    /* === REDUCE_FULL: β-reduction — reconstruct original JSON === */
    if (strategy == REDUCE_FULL) {
        /* Read all cells for this entity */
        sqlite3_stmt *cell_q;
        int cell_reads = 0;
        const char *cell_sql =
            "SELECT position, field_name, val_text, val_num, val_type "
            "FROM _tensor_cells WHERE family_id=?1 AND target_id=?2 "
            "ORDER BY position ASC";
        if (sqlite3_prepare_v2(db, cell_sql, -1, &cell_q, NULL) != SQLITE_OK)
            return -1;
        sqlite3_bind_int(cell_q, 1, family_id);
        sqlite3_bind_int(cell_q, 2, target_id);

        size_t cap = 4096;
        char *out = malloc(cap);
        if (!out) { sqlite3_finalize(cell_q); return -1; }
        size_t off = 0;
        out[off++] = '{';
        int first = 1;

        while (sqlite3_step(cell_q) == SQLITE_ROW) {
            cell_reads++;
            const char *fname = (const char *)sqlite3_column_text(cell_q, 1);
            const char *vtext = (const char *)sqlite3_column_text(cell_q, 2);
            double vnum = sqlite3_column_double(cell_q, 3);
            int vtype = sqlite3_column_int(cell_q, 4);

            size_t needed = (fname ? strlen(fname) : 0) + (vtext ? strlen(vtext) : 0) + 64;
            while (off + needed >= cap) { cap *= 2; out = realloc(out, cap); }

            if (!first) out[off++] = ',';
            first = 0;

            if (vtype == 0) {
                /* string */
                off += (size_t)snprintf(out + off, cap - off,
                    "\"%s\":\"%s\"", fname ? fname : "", vtext ? vtext : "");
            } else if (vtype == 1) {
                /* number */
                if (vnum == (double)(long long)vnum) {
                    off += (size_t)snprintf(out + off, cap - off,
                        "\"%s\":%lld", fname ? fname : "", (long long)vnum);
                } else {
                    off += (size_t)snprintf(out + off, cap - off,
                        "\"%s\":%.6f", fname ? fname : "", vnum);
                }
            } else {
                /* bool/null */
                off += (size_t)snprintf(out + off, cap - off,
                    "\"%s\":%s", fname ? fname : "", vtext ? vtext : "null");
            }
        }
        sqlite3_finalize(cell_q);

        out[off++] = '}';
        out[off] = '\0';
        *out_json = out;
        bench_metrics_record_reduce_full(cell_reads);
        return 0;
    }

    /* === REDUCE_PARTIAL: partial β — project selected fields === */
    if (strategy == REDUCE_PARTIAL && fields) {
        /* Parse comma-separated field list */
        char field_list[4096];
        int cell_reads = 0;
        strncpy(field_list, fields, sizeof(field_list) - 1);
        field_list[sizeof(field_list) - 1] = '\0';

        size_t cap = 4096;
        char *out = malloc(cap);
        if (!out) return -1;
        size_t off = 0;
        out[off++] = '{';
        int first = 1;

        char *tok = strtok(field_list, ",");
        while (tok) {
            /* Trim whitespace */
            while (*tok == ' ') tok++;
            char *end = tok + strlen(tok) - 1;
            while (end > tok && *end == ' ') *end-- = '\0';

            int pos = lambda_field_pos(&expr, tok);
            if (pos >= 0) {
                /* Read this specific cell */
                sqlite3_stmt *cell_q;
                if (sqlite3_prepare_v2(db,
                    "SELECT val_text, val_num, val_type FROM _tensor_cells "
                    "WHERE family_id=?1 AND target_id=?2 AND position=?3",
                    -1, &cell_q, NULL) == SQLITE_OK) {
                    sqlite3_bind_int(cell_q, 1, family_id);
                    sqlite3_bind_int(cell_q, 2, target_id);
                    sqlite3_bind_int(cell_q, 3, pos);

                    if (sqlite3_step(cell_q) == SQLITE_ROW) {
                        cell_reads++;
                        const char *vtext = (const char *)sqlite3_column_text(cell_q, 0);
                        double vnum = sqlite3_column_double(cell_q, 1);
                        int vtype = sqlite3_column_int(cell_q, 2);

                        size_t needed = strlen(tok) + (vtext ? strlen(vtext) : 0) + 64;
                        while (off + needed >= cap) { cap *= 2; out = realloc(out, cap); }

                        if (!first) out[off++] = ',';
                        first = 0;

                        if (vtype == 0) {
                            off += (size_t)snprintf(out + off, cap - off,
                                "\"%s\":\"%s\"", tok, vtext ? vtext : "");
                        } else if (vtype == 1) {
                            if (vnum == (double)(long long)vnum)
                                off += (size_t)snprintf(out + off, cap - off,
                                    "\"%s\":%lld", tok, (long long)vnum);
                            else
                                off += (size_t)snprintf(out + off, cap - off,
                                    "\"%s\":%.6f", tok, vnum);
                        } else {
                            off += (size_t)snprintf(out + off, cap - off,
                                "\"%s\":%s", tok, vtext ? vtext : "null");
                        }
                    }
                    sqlite3_finalize(cell_q);
                }
            }
            tok = strtok(NULL, ",");
        }

        out[off++] = '}';
        out[off] = '\0';
        *out_json = out;
        bench_metrics_record_reduce_partial(cell_reads);
        return 0;
    }

    /* === REDUCE_SYMBOLIC: modify binding in-place === */
    if (strategy == REDUCE_SYMBOLIC && fields && value) {
        int pos = lambda_field_pos(&expr, fields);
        if (pos < 0) return -1;
        sqlite3_exec(db, "SAVEPOINT tensor_symbolic_sp", NULL, NULL, NULL);

        /* Determine if new value is numeric */
        int new_type = 0; /* string by default */
        double new_num = 0.0;
        char *endp = NULL;
        double d = strtod(value, &endp);
        if (endp && endp != value && (*endp == '\0' || *endp == ' ')) {
            new_type = 1;
            new_num = d;
        }
        if (strcmp(value, "true") == 0 || strcmp(value, "false") == 0 || strcmp(value, "null") == 0) {
            new_type = 2;
        }

        /* Update the cell directly — no decompression */
        sqlite3_stmt *upd;
        if (sqlite3_prepare_v2(db,
            "UPDATE _tensor_cells SET val_text=?1, val_num=?2, val_type=?3 "
            "WHERE family_id=?4 AND target_id=?5 AND position=?6",
            -1, &upd, NULL) != SQLITE_OK) goto symbolic_fail;
        sqlite3_bind_text(upd, 1, value, -1, SQLITE_STATIC);
        sqlite3_bind_double(upd, 2, new_num);
        sqlite3_bind_int(upd, 3, new_type);
        sqlite3_bind_int(upd, 4, family_id);
        sqlite3_bind_int(upd, 5, target_id);
        sqlite3_bind_int(upd, 6, pos);
        int rc = sqlite3_step(upd);
        sqlite3_finalize(upd);

        if (rc != SQLITE_DONE) goto symbolic_fail;

        if (sync_symbolic_update_to_row(db, target_type, target_id, fields, value,
                                        new_type, new_num) != 0) {
            goto symbolic_fail;
        }

        /* Also update the family_members bindings JSON for consistency */
        /* Re-read all cells and rebuild bindings array */
        sqlite3_stmt *rebuild_q;
        if (sqlite3_prepare_v2(db,
            "SELECT val_text, val_type FROM _tensor_cells "
            "WHERE family_id=?1 AND target_id=?2 ORDER BY position ASC",
            -1, &rebuild_q, NULL) != SQLITE_OK) {
            goto symbolic_fail;
        }
        sqlite3_bind_int(rebuild_q, 1, family_id);
        sqlite3_bind_int(rebuild_q, 2, target_id);

        size_t bsz = 4096;
        char *new_bindings = malloc(bsz);
        size_t boff = 0;
        int bfirst = 1;
        if (!new_bindings) {
            sqlite3_finalize(rebuild_q);
            goto symbolic_fail;
        }
        new_bindings[boff++] = '[';

        while (sqlite3_step(rebuild_q) == SQLITE_ROW) {
            const char *vt = (const char *)sqlite3_column_text(rebuild_q, 0);
            int vty = sqlite3_column_int(rebuild_q, 1);
            char *tmp;

            size_t need = (vt ? strlen(vt) : 4) + 8;
            while (boff + need >= bsz) {
                bsz *= 2;
                tmp = realloc(new_bindings, bsz);
                if (!tmp) {
                    sqlite3_finalize(rebuild_q);
                    free(new_bindings);
                    goto symbolic_fail;
                }
                new_bindings = tmp;
            }

            if (!bfirst) new_bindings[boff++] = ',';
            bfirst = 0;

            if (vty == 0) {
                boff += (size_t)snprintf(new_bindings + boff, bsz - boff,
                    "\"%s\"", vt ? vt : "");
            } else {
                boff += (size_t)snprintf(new_bindings + boff, bsz - boff,
                    "%s", vt ? vt : "null");
            }
        }
        sqlite3_finalize(rebuild_q);

        new_bindings[boff++] = ']';
        new_bindings[boff] = '\0';

        /* Update _family_members */
        sqlite3_stmt *upd_mem;
        if (sqlite3_prepare_v2(db,
            "UPDATE _family_members SET bindings=?1 "
            "WHERE family_id=?2 AND target_type=?3 AND target_id=?4",
            -1, &upd_mem, NULL) != SQLITE_OK) {
            free(new_bindings);
            goto symbolic_fail;
        }
        sqlite3_bind_text(upd_mem, 1, new_bindings, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(upd_mem, 2, family_id);
        sqlite3_bind_text(upd_mem, 3, target_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(upd_mem, 4, target_id);
        if (sqlite3_step(upd_mem) != SQLITE_DONE) {
            sqlite3_finalize(upd_mem);
            free(new_bindings);
            goto symbolic_fail;
        }
        sqlite3_finalize(upd_mem);
        bench_metrics_record_symbolic_update((sqlite3_int64)boff);
        free(new_bindings);

        /* Invalidate the row representation in e-graph (tensor stays valid) */
        egraph_invalidate(db, target_type, target_id);

        /* Log to op log */
        char op_payload[8192];
        snprintf(op_payload, sizeof(op_payload),
            "{\"%s\":%s%s%s}",
            fields,
            new_type == 0 ? "\"" : "",
            value,
            new_type == 0 ? "\"" : "");
        ops_append(db, OP_UPDATE, target_type, (long long)target_id,
                   "root", op_payload, "tensor_reduce", NULL);
        sqlite3_exec(db, "RELEASE tensor_symbolic_sp", NULL, NULL, NULL);

        /* Return the updated bindings via full reduction */
        return tensor_reduce(db, target_type, target_id, REDUCE_FULL,
                             NULL, NULL, out_json);
    }

    return -1;

symbolic_fail:
    sqlite3_exec(db, "ROLLBACK TO tensor_symbolic_sp", NULL, NULL, NULL);
    sqlite3_exec(db, "RELEASE tensor_symbolic_sp", NULL, NULL, NULL);
    return -1;
}

/* ================================================================
 * Primitive 3: tensor_discover
 *
 * Re-analyze a family. Detect:
 *   - near-zero eigenvalue positions → report as constant candidates
 *   - high-variance positions → report as information carriers
 *   Returns count of refinement observations.
 * ================================================================ */

int tensor_discover(sqlite3 *db, const char *content_type, int family_id) {
    /* Recompute eigenvalues */
    TensorEigen eig;
    if (tensor_eigen(db, family_id, &eig) != 0) return 0;

    int refinements = 0;

    /* Detect near-zero eigenvalue positions (constant candidates) */
    for (int i = 0; i < eig.arity; i++) {
        if (eig.eigenvalues[i] < 0.001 && eig.member_count > 1) {
            /* This position is nearly constant — candidate for absorption
               into the generator body (reduce arity by 1).

               For now: mark it in _tensor_eigen by setting eigenvalue to exactly 0.
               A future pass will rewrite the lambda to inline this constant. */
            sqlite3_stmt *upd;
            if (sqlite3_prepare_v2(db,
                "UPDATE _tensor_eigen SET eigenvalue=0.0 "
                "WHERE family_id=?1 AND position=?2",
                -1, &upd, NULL) == SQLITE_OK) {
                sqlite3_bind_int(upd, 1, family_id);
                sqlite3_bind_int(upd, 2, i);
                sqlite3_step(upd);
                sqlite3_finalize(upd);
            }
            refinements++;
        }
    }

    /* Check for potential family split:
       If structural_ratio is very low (< 0.1), the generator isn't capturing
       much shared structure — the family might not be real. */
    (void)content_type; /* reserved for future split logic */

    tensor_eigen_free(&eig);
    return refinements;
}

/* ================================================================
 * Compressed-domain query: filter by field condition
 *
 * Operates entirely on _tensor_cells — no decompression.
 * The lambda tells us which position holds the field.
 * We query that column directly.
 * ================================================================ */

int tensor_query(sqlite3 *db, const char *content_type, const char *field,
                 const char *op, const char *value, char **out_json)
{
    *out_json = NULL;

    /* Validate operator */
    const char *valid_ops[] = {"=", "!=", ">", "<", ">=", "<=", NULL};
    int op_valid = 0;
    for (int i = 0; valid_ops[i]; i++) {
        if (strcmp(op, valid_ops[i]) == 0) { op_valid = 1; break; }
    }
    if (!op_valid) return -1;

    /* Build query: find all cells at this field_name matching the condition.
       field_name is stored per cell, so we don't even need the generator parse. */

    /* Try numeric comparison first, fall back to text */
    char *endp = NULL;
    double num_val = strtod(value, &endp);
    int is_numeric = (endp && endp != value && (*endp == '\0' || *endp == ' '));

    char sql[2048];
    if (is_numeric) {
        snprintf(sql, sizeof(sql),
            "SELECT DISTINCT c.target_id "
            "FROM _tensor_cells c "
            "JOIN _family_members m ON m.family_id = c.family_id "
            "  AND m.target_type = c.target_type AND m.target_id = c.target_id "
            "JOIN _families f ON f.id = c.family_id "
            "WHERE f.content_type = ?1 AND c.field_name = ?2 "
            "AND c.val_num %s ?3 "
            "ORDER BY c.target_id", op);
    } else {
        snprintf(sql, sizeof(sql),
            "SELECT DISTINCT c.target_id "
            "FROM _tensor_cells c "
            "JOIN _family_members m ON m.family_id = c.family_id "
            "  AND m.target_type = c.target_type AND m.target_id = c.target_id "
            "JOIN _families f ON f.id = c.family_id "
            "WHERE f.content_type = ?1 AND c.field_name = ?2 "
            "AND c.val_text %s ?3 "
            "ORDER BY c.target_id", op);
    }

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "[tensor] query: %s\n", sqlite3_errmsg(db));
        return -1;
    }
    sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, field, -1, SQLITE_STATIC);
    if (is_numeric)
        sqlite3_bind_double(stmt, 3, num_val);
    else
        sqlite3_bind_text(stmt, 3, value, -1, SQLITE_STATIC);

    size_t cap = 1024;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';
    int count = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int tid = sqlite3_column_int(stmt, 0);
        while (off + 32 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (count > 0) out[off++] = ',';
        off += (size_t)snprintf(out + off, cap - off, "%d", tid);
        count++;
    }
    {
        sqlite3_int64 fullscan_steps = sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 0);
        bench_metrics_record_tensor_filter(count, fullscan_steps);
    }
    sqlite3_finalize(stmt);

    out[off++] = ']';
    out[off] = '\0';
    *out_json = out;
    return count;
}

/* ================================================================
 * Compressed-domain update: modify field without decompression
 * ================================================================ */

int tensor_update(sqlite3 *db, const char *content_type, int target_id,
                  const char *field, const char *value)
{
    /* Type-check guard: validate value against family constraints
       before committing the symbolic update */
    {
        sqlite3_stmt *fq = NULL;
        if (sqlite3_prepare_v2(db,
            "SELECT m.family_id, e.position FROM _family_members m "
            "JOIN _tensor_eigen e ON e.family_id = m.family_id "
            "WHERE m.target_type=?1 AND m.target_id=?2 AND e.field_name=?3 LIMIT 1",
            -1, &fq, NULL) == SQLITE_OK) {
            sqlite3_bind_text(fq, 1, content_type, -1, SQLITE_STATIC);
            sqlite3_bind_int(fq, 2, target_id);
            sqlite3_bind_text(fq, 3, field, -1, SQLITE_STATIC);
            if (sqlite3_step(fq) == SQLITE_ROW) {
                int fam_id = sqlite3_column_int(fq, 0);
                int pos = sqlite3_column_int(fq, 1);
                char violation[256] = {0};
                if (!tc_validate(db, fam_id, pos, value, violation, sizeof(violation))) {
                    fprintf(stderr, "[tensor] type-check failed for %s.%s: %s\n",
                            content_type, field, violation);
                    sqlite3_finalize(fq);
                    return -1;
                }
            }
            sqlite3_finalize(fq);
        }
    }

    char *result = NULL;
    int rc = tensor_reduce(db, content_type, target_id,
                           REDUCE_SYMBOLIC, field, value, &result);
    free(result);
    return rc;
}

/* ================================================================
 * Compressed-domain aggregate: SUM/AVG/MIN/MAX/COUNT
 *
 * Operates on the binding column directly — pure columnar scan.
 * ================================================================ */

int tensor_aggregate(sqlite3 *db, const char *content_type, const char *field,
                     const char *agg_fn, char **out_json)
{
    *out_json = NULL;

    /* Map agg function name */
    const char *sql_agg = NULL;
    if (strcmp(agg_fn, "sum") == 0) sql_agg = "SUM(c.val_num)";
    else if (strcmp(agg_fn, "avg") == 0) sql_agg = "AVG(c.val_num)";
    else if (strcmp(agg_fn, "min") == 0) sql_agg = "MIN(c.val_num)";
    else if (strcmp(agg_fn, "max") == 0) sql_agg = "MAX(c.val_num)";
    else if (strcmp(agg_fn, "count") == 0) sql_agg = "COUNT(c.val_num)";
    else return -1;

    char sql[2048];
    snprintf(sql, sizeof(sql),
        "SELECT %s FROM _tensor_cells c "
        "JOIN _families f ON f.id = c.family_id "
        "WHERE f.content_type = ?1 AND c.field_name = ?2",
        sql_agg);

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, field, -1, SQLITE_STATIC);

    char *out = malloc(256);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        double result = sqlite3_column_double(stmt, 0);
        snprintf(out, 256,
            "{\"%s_%s\":%.6f,\"field\":\"%s\"}",
            agg_fn, field, result, field);
    } else {
        snprintf(out, 256, "{\"%s_%s\":null}", agg_fn, field);
    }
    bench_metrics_record_tensor_agg(sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_VM_STEP, 0));
    sqlite3_finalize(stmt);

    *out_json = out;
    return 0;
}

/* ================================================================
 * Compressed-domain project: select fields from all family members
 * ================================================================ */

int tensor_project(sqlite3 *db, const char *content_type,
                   const char *fields, char **out_json)
{
    *out_json = NULL;

    /* Get all families for this content type */
    sqlite3_stmt *fam_q;
    if (sqlite3_prepare_v2(db,
        "SELECT id, generator FROM _families WHERE content_type=?1",
        -1, &fam_q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(fam_q, 1, content_type, -1, SQLITE_STATIC);

    size_t cap = 4096;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';
    int total = 0;

    while (sqlite3_step(fam_q) == SQLITE_ROW) {
        int fam_id = sqlite3_column_int(fam_q, 0);

        /* --- Columnar fast path: materialize + direct column read --- */
        /* Map requested field names → position indices for this family */
        int positions[COL_MAX_COLS];
        char field_names[COL_MAX_COLS][MAX_FIELD_NAME_LEN];
        int npos = 0;
        {
            sqlite3_stmt *pos_q = NULL;
            if (fields && fields[0]) {
                /* Resolve positions for requested fields */
                const char *sql_pos =
                    "SELECT position, field_name FROM _tensor_eigen "
                    "WHERE family_id=?1 ORDER BY position";
                if (sqlite3_prepare_v2(db, sql_pos, -1, &pos_q, NULL) == SQLITE_OK) {
                    sqlite3_bind_int(pos_q, 1, fam_id);
                    while (sqlite3_step(pos_q) == SQLITE_ROW && npos < COL_MAX_COLS) {
                        int pos = sqlite3_column_int(pos_q, 0);
                        const char *fn = (const char *)sqlite3_column_text(pos_q, 1);
                        if (!fn) continue;
                        /* Check if this field is in the requested list */
                        const char *p = fields;
                        while (*p) {
                            const char *start = p;
                            while (*p && *p != ',') p++;
                            size_t len = (size_t)(p - start);
                            if (len == strlen(fn) && strncmp(start, fn, len) == 0) {
                                positions[npos] = pos;
                                strncpy(field_names[npos], fn, MAX_FIELD_NAME_LEN - 1);
                                field_names[npos][MAX_FIELD_NAME_LEN - 1] = '\0';
                                npos++;
                                break;
                            }
                            if (*p == ',') p++;
                        }
                    }
                    sqlite3_finalize(pos_q);
                }
            } else {
                /* All fields */
                const char *sql_pos =
                    "SELECT position, field_name FROM _tensor_eigen "
                    "WHERE family_id=?1 ORDER BY position";
                if (sqlite3_prepare_v2(db, sql_pos, -1, &pos_q, NULL) == SQLITE_OK) {
                    sqlite3_bind_int(pos_q, 1, fam_id);
                    while (sqlite3_step(pos_q) == SQLITE_ROW && npos < COL_MAX_COLS) {
                        positions[npos] = sqlite3_column_int(pos_q, 0);
                        const char *fn = (const char *)sqlite3_column_text(pos_q, 1);
                        if (fn) {
                            strncpy(field_names[npos], fn, MAX_FIELD_NAME_LEN - 1);
                            field_names[npos][MAX_FIELD_NAME_LEN - 1] = '\0';
                        } else {
                            field_names[npos][0] = '\0';
                        }
                        npos++;
                    }
                    sqlite3_finalize(pos_q);
                }
            }
        }

        if (npos > 0) {
            /* Attempt columnar materialization */
            char fid_str[32];
            snprintf(fid_str, sizeof(fid_str), "%d", fam_id);
            ColBatch batch;
            if (col_materialize(db, fid_str, positions, npos, &batch) == 0 && batch.nrows > 0) {
                /* Build projected JSON directly from column vectors */
                for (int r = 0; r < batch.nrows; r++) {
                    int tid = batch.target_ids[r];
                    size_t row_cap = 256 + (size_t)npos * 128;
                    while (off + row_cap >= cap) { cap *= 2; out = realloc(out, cap); }
                    if (total > 0) out[off++] = ',';

                    off += (size_t)snprintf(out + off, cap - off, "{\"id\":%d,\"data\":{", tid);
                    for (int c = 0; c < batch.ncols && c < npos; c++) {
                        ColVector *cv = &batch.cols[c];
                        if (c > 0) out[off++] = ',';
                        off += (size_t)snprintf(out + off, cap - off, "\"%s\":", field_names[c]);
                        if (cv->nulls && cv->nulls[r]) {
                            off += (size_t)snprintf(out + off, cap - off, "null");
                        } else if (cv->col_type == COL_TYPE_INT && cv->ints) {
                            off += (size_t)snprintf(out + off, cap - off, "%lld", (long long)cv->ints[r]);
                        } else if (cv->col_type == COL_TYPE_DOUBLE && cv->doubles) {
                            off += (size_t)snprintf(out + off, cap - off, "%.6f", cv->doubles[r]);
                        } else if (cv->texts && cv->texts[r]) {
                            while (off + strlen(cv->texts[r]) + 4 >= cap) { cap *= 2; out = realloc(out, cap); }
                            off += (size_t)snprintf(out + off, cap - off, "\"%s\"", cv->texts[r]);
                        } else {
                            off += (size_t)snprintf(out + off, cap - off, "null");
                        }
                    }
                    off += (size_t)snprintf(out + off, cap - off, "}}");
                    total++;
                }
                col_batch_free(&batch);
                continue; /* skip per-member reduce fallback */
            }
            col_batch_free(&batch);
        }

        /* --- Fallback: per-member tensor_reduce --- */
        sqlite3_stmt *mem_q;
        if (sqlite3_prepare_v2(db,
            "SELECT DISTINCT target_id FROM _tensor_cells "
            "WHERE family_id=?1 ORDER BY target_id",
            -1, &mem_q, NULL) != SQLITE_OK) continue;
        sqlite3_bind_int(mem_q, 1, fam_id);

        while (sqlite3_step(mem_q) == SQLITE_ROW) {
            int tid = sqlite3_column_int(mem_q, 0);

            /* Use partial reduction for this member */
            char *projected = NULL;
            if (tensor_reduce(db, content_type, tid,
                              REDUCE_PARTIAL, fields, NULL, &projected) == 0 && projected) {
                size_t needed = strlen(projected) + 32;
                while (off + needed >= cap) { cap *= 2; out = realloc(out, cap); }
                if (total > 0) out[off++] = ',';

                off += (size_t)snprintf(out + off, cap - off,
                    "{\"id\":%d,\"data\":%s}", tid, projected);
                free(projected);
                total++;
            }
        }
        sqlite3_finalize(mem_q);
    }
    sqlite3_finalize(fam_q);

    out[off++] = ']';
    out[off] = '\0';
    *out_json = out;
    return total;
}

/* ================================================================
 * Eigen stats: per-content-type spectral overview as JSON
 * ================================================================ */

int tensor_eigen_stats(sqlite3 *db, const char *content_type, char **out_json) {
    *out_json = NULL;

    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT f.id, f.family_hash, f.member_count, "
        "  e.position, e.field_name, e.eigenvalue, e.variance, e.mean "
        "FROM _families f "
        "JOIN _tensor_eigen e ON e.family_id = f.id "
        "WHERE f.content_type = ?1 "
        "ORDER BY f.id, e.position";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, content_type, -1, SQLITE_STATIC);

    size_t cap = 8192;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';
    int count = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int fid = sqlite3_column_int(stmt, 0);
        const char *fhash = (const char *)sqlite3_column_text(stmt, 1);
        int members = sqlite3_column_int(stmt, 2);
        int pos = sqlite3_column_int(stmt, 3);
        const char *fname = (const char *)sqlite3_column_text(stmt, 4);
        double eval = sqlite3_column_double(stmt, 5);
        double var = sqlite3_column_double(stmt, 6);
        double mean = sqlite3_column_double(stmt, 7);

        while (off + 512 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (count > 0) out[off++] = ',';

        off += (size_t)snprintf(out + off, cap - off,
            "{\"family_id\":%d,\"family_hash\":\"%s\",\"members\":%d,"
            "\"position\":%d,\"field\":\"%s\","
            "\"eigenvalue\":%.6f,\"variance\":%.6f,\"mean\":%.6f}",
            fid, fhash ? fhash : "", members,
            pos, fname ? fname : "",
            eval, var, mean);
        count++;
    }
    sqlite3_finalize(stmt);

    out[off++] = ']';
    out[off] = '\0';
    *out_json = out;
    return count;
}
