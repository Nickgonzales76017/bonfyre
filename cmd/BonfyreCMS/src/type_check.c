/*
 * BonfyreGraph — Upgrade V: Typed Generators + Static Verification
 *
 * Constraint inference from observed binding values.
 * Type-safe rewrite guard for symbolic reduction.
 */

#include "type_check.h"
#include "tensor_ops.h"
#include "canonical.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <ctype.h>

extern void iso_timestamp(char *buf, size_t sz);
extern int db_exec(sqlite3 *db, const char *sql);

/* ================================================================
 * Bootstrap
 * ================================================================ */

int tc_bootstrap(sqlite3 *db) {
    const char *stmts[] = {
        "CREATE TABLE IF NOT EXISTS _type_constraints ("
        "  id          INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  family_id   INTEGER NOT NULL,"
        "  position    INTEGER NOT NULL,"
        "  field_name  TEXT NOT NULL,"
        "  kind        INTEGER NOT NULL,"
        "  range_lo    REAL DEFAULT 0.0,"
        "  range_hi    REAL DEFAULT 0.0,"
        "  enum_vals   TEXT,"
        "  pattern     TEXT,"
        "  is_optional INTEGER DEFAULT 0,"
        "  inferred_at TEXT NOT NULL,"
        "  UNIQUE(family_id, position)"
        ");",

        "CREATE INDEX IF NOT EXISTS idx_tc_fam ON _type_constraints(family_id);",

        NULL
    };
    for (int i = 0; stmts[i]; i++) {
        if (db_exec(db, stmts[i]) != 0) return -1;
    }
    return 0;
}

/* ================================================================
 * tc_infer — analyze bindings to infer type constraints
 * ================================================================ */

int tc_infer(sqlite3 *db, int family_id) {
    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    /* Clear old constraints */
    {
        sqlite3_stmt *del;
        if (sqlite3_prepare_v2(db,
            "DELETE FROM _type_constraints WHERE family_id=?1",
            -1, &del, NULL) == SQLITE_OK) {
            sqlite3_bind_int(del, 1, family_id);
            sqlite3_step(del);
            sqlite3_finalize(del);
        }
    }

    /* Get arity */
    sqlite3_stmt *aq;
    int arity = 0;
    if (sqlite3_prepare_v2(db,
        "SELECT MAX(position) + 1 FROM _tensor_cells WHERE family_id=?1",
        -1, &aq, NULL) == SQLITE_OK) {
        sqlite3_bind_int(aq, 1, family_id);
        if (sqlite3_step(aq) == SQLITE_ROW) arity = sqlite3_column_int(aq, 0);
        sqlite3_finalize(aq);
    }
    if (arity <= 0) return 0;

    int inferred = 0;

    for (int pos = 0; pos < arity; pos++) {
        TypeConstraint tc;
        memset(&tc, 0, sizeof(tc));
        tc.position = pos;

        /* Get field name and value stats */
        sqlite3_stmt *sq;
        int total = 0, n_numeric = 0, n_string = 0, n_null = 0, n_bool = 0;
        int distinct = 0;
        double min_val = 0, max_val = 0;
        int all_int = 1;
        int has_null = 0;

        /* Count by type */
        if (sqlite3_prepare_v2(db,
            "SELECT field_name, val_type, COUNT(*), "
            "COUNT(DISTINCT val_text), MIN(val_num), MAX(val_num) "
            "FROM _tensor_cells WHERE family_id=?1 AND position=?2 "
            "GROUP BY val_type ORDER BY COUNT(*) DESC",
            -1, &sq, NULL) == SQLITE_OK) {
            sqlite3_bind_int(sq, 1, family_id);
            sqlite3_bind_int(sq, 2, pos);

            while (sqlite3_step(sq) == SQLITE_ROW) {
                const char *fn = (const char *)sqlite3_column_text(sq, 0);
                int vtype = sqlite3_column_int(sq, 1);
                int cnt = sqlite3_column_int(sq, 2);

                if (!tc.field_name[0] && fn)
                    strncpy(tc.field_name, fn, 255);

                total += cnt;
                if (vtype == 0) {
                    n_string += cnt;
                    distinct += sqlite3_column_int(sq, 3);
                }
                if (vtype == 1) {
                    n_numeric += cnt;
                    double lo = sqlite3_column_double(sq, 4);
                    double hi = sqlite3_column_double(sq, 5);
                    if (n_numeric == cnt || lo < min_val) min_val = lo;
                    if (n_numeric == cnt || hi > max_val) max_val = hi;
                }
                if (vtype == 2) n_bool += cnt;
                if (vtype == 3) { n_null += cnt; has_null = 1; }
            }
            sqlite3_finalize(sq);
        }

        tc.is_optional = has_null;

        if (n_numeric > 0 && n_numeric >= total - n_null) {
            /* Primarily numeric */
            /* Check if all are integers */
            if (sqlite3_prepare_v2(db,
                "SELECT COUNT(*) FROM _tensor_cells "
                "WHERE family_id=?1 AND position=?2 AND val_type=1 "
                "AND val_num != CAST(val_num AS INTEGER)",
                -1, &sq, NULL) == SQLITE_OK) {
                sqlite3_bind_int(sq, 1, family_id);
                sqlite3_bind_int(sq, 2, pos);
                if (sqlite3_step(sq) == SQLITE_ROW)
                    all_int = (sqlite3_column_int(sq, 0) == 0);
                sqlite3_finalize(sq);
            }

            if (all_int) {
                tc.kind = TC_INT_RANGE;
            } else {
                tc.kind = TC_FLT_RANGE;
            }
            tc.range_lo = min_val;
            tc.range_hi = max_val;

        } else if (n_bool > 0 && n_bool >= total - n_null) {
            tc.kind = TC_BOOLEAN;

        } else if (n_string > 0) {
            /* String: check for enum pattern */
            if (distinct > 0 && distinct <= 30 && total >= 3) {
                tc.kind = TC_ENUM;
                /* Collect enum values */
                sqlite3_stmt *eq;
                if (sqlite3_prepare_v2(db,
                    "SELECT DISTINCT val_text FROM _tensor_cells "
                    "WHERE family_id=?1 AND position=?2 AND val_type=0 "
                    "ORDER BY val_text LIMIT 30",
                    -1, &eq, NULL) == SQLITE_OK) {
                    sqlite3_bind_int(eq, 1, family_id);
                    sqlite3_bind_int(eq, 2, pos);
                    size_t eoff = 0;
                    while (sqlite3_step(eq) == SQLITE_ROW) {
                        const char *ev = (const char *)sqlite3_column_text(eq, 0);
                        if (ev) {
                            if (eoff > 0 && eoff < sizeof(tc.enum_vals) - 1)
                                tc.enum_vals[eoff++] = '|';
                            size_t elen = strlen(ev);
                            if (eoff + elen < sizeof(tc.enum_vals) - 1) {
                                memcpy(tc.enum_vals + eoff, ev, elen);
                                eoff += elen;
                            }
                        }
                    }
                    tc.enum_vals[eoff] = '\0';
                    sqlite3_finalize(eq);
                }
            } else {
                tc.kind = TC_STRING;
            }
        } else if (n_null == total) {
            tc.kind = TC_NULL;
        } else {
            tc.kind = TC_ANY;
        }

        /* Store constraint */
        sqlite3_stmt *ins;
        if (sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO _type_constraints "
            "(family_id, position, field_name, kind, range_lo, range_hi, "
            " enum_vals, pattern, is_optional, inferred_at) "
            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            -1, &ins, NULL) == SQLITE_OK) {
            sqlite3_bind_int(ins, 1, family_id);
            sqlite3_bind_int(ins, 2, pos);
            sqlite3_bind_text(ins, 3, tc.field_name, -1, SQLITE_STATIC);
            sqlite3_bind_int(ins, 4, tc.kind);
            sqlite3_bind_double(ins, 5, tc.range_lo);
            sqlite3_bind_double(ins, 6, tc.range_hi);
            sqlite3_bind_text(ins, 7, tc.enum_vals, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 8, tc.pattern, -1, SQLITE_STATIC);
            sqlite3_bind_int(ins, 9, tc.is_optional);
            sqlite3_bind_text(ins, 10, ts, -1, SQLITE_STATIC);
            sqlite3_step(ins);
            sqlite3_finalize(ins);
        }
        inferred++;
    }

    return inferred;
}

/* ================================================================
 * tc_infer_all — infer for all families in a content type
 * ================================================================ */

int tc_infer_all(sqlite3 *db, const char *content_type) {
    sqlite3_stmt *fq;
    if (sqlite3_prepare_v2(db,
        "SELECT id FROM _families WHERE content_type=?1",
        -1, &fq, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(fq, 1, content_type, -1, SQLITE_STATIC);

    int total = 0;
    while (sqlite3_step(fq) == SQLITE_ROW) {
        total += tc_infer(db, sqlite3_column_int(fq, 0));
    }
    sqlite3_finalize(fq);
    return total;
}

/* ================================================================
 * tc_validate — check value against a position's constraint
 * ================================================================ */

int tc_validate(sqlite3 *db, int family_id, int position,
                const char *value, char *violation_msg, size_t msg_sz) {
    if (!value) {
        /* NULL is OK for optional, violation otherwise */
        sqlite3_stmt *oq;
        if (sqlite3_prepare_v2(db,
            "SELECT is_optional FROM _type_constraints "
            "WHERE family_id=?1 AND position=?2",
            -1, &oq, NULL) == SQLITE_OK) {
            sqlite3_bind_int(oq, 1, family_id);
            sqlite3_bind_int(oq, 2, position);
            if (sqlite3_step(oq) == SQLITE_ROW) {
                int opt = sqlite3_column_int(oq, 0);
                sqlite3_finalize(oq);
                if (opt) return 1;
                if (violation_msg)
                    snprintf(violation_msg, msg_sz, "position %d: null not allowed", position);
                return 0;
            }
            sqlite3_finalize(oq);
        }
        return 1; /* no constraint = valid */
    }

    /* Load constraint */
    sqlite3_stmt *cq;
    if (sqlite3_prepare_v2(db,
        "SELECT kind, range_lo, range_hi, enum_vals, pattern, field_name "
        "FROM _type_constraints WHERE family_id=?1 AND position=?2",
        -1, &cq, NULL) != SQLITE_OK) return 1;
    sqlite3_bind_int(cq, 1, family_id);
    sqlite3_bind_int(cq, 2, position);

    if (sqlite3_step(cq) != SQLITE_ROW) {
        sqlite3_finalize(cq);
        return 1; /* no constraint */
    }

    int kind = sqlite3_column_int(cq, 0);
    double lo = sqlite3_column_double(cq, 1);
    double hi = sqlite3_column_double(cq, 2);
    const char *enums = (const char *)sqlite3_column_text(cq, 3);
    const char *fname = (const char *)sqlite3_column_text(cq, 5);
    sqlite3_finalize(cq);

    /* Check null/true/false */
    if (strcmp(value, "null") == 0) {
        /* Already checked optional above; re-check here */
        return 1; /* null always passes if we got here */
    }

    switch (kind) {
    case TC_STRING:
        return 1; /* any string is valid */

    case TC_NUMBER:
    case TC_INT_RANGE:
    case TC_FLT_RANGE: {
        char *end = NULL;
        double v = strtod(value, &end);
        if (!end || end == value) {
            if (violation_msg)
                snprintf(violation_msg, msg_sz, "%s: expected number, got '%s'",
                         fname ? fname : "?", value);
            return 0;
        }
        if (kind == TC_INT_RANGE && v != (double)(long long)v) {
            if (violation_msg)
                snprintf(violation_msg, msg_sz, "%s: expected integer, got %.6f",
                         fname ? fname : "?", v);
            return 0;
        }
        if ((kind == TC_INT_RANGE || kind == TC_FLT_RANGE) && (v < lo || v > hi)) {
            if (violation_msg)
                snprintf(violation_msg, msg_sz, "%s: %.6f out of range [%.6f, %.6f]",
                         fname ? fname : "?", v, lo, hi);
            return 0;
        }
        return 1;
    }

    case TC_BOOLEAN:
        if (strcmp(value, "true") != 0 && strcmp(value, "false") != 0) {
            if (violation_msg)
                snprintf(violation_msg, msg_sz, "%s: expected boolean, got '%s'",
                         fname ? fname : "?", value);
            return 0;
        }
        return 1;

    case TC_ENUM: {
        if (!enums || !enums[0]) return 1;
        /* Check if value is in pipe-separated list */
        const char *p = enums;
        while (*p) {
            const char *sep = strchr(p, '|');
            size_t len = sep ? (size_t)(sep - p) : strlen(p);
            if (strlen(value) == len && strncmp(value, p, len) == 0)
                return 1;
            p += len;
            if (*p == '|') p++;
        }
        if (violation_msg)
            snprintf(violation_msg, msg_sz, "%s: '%s' not in enum(%s)",
                     fname ? fname : "?", value, enums);
        return 0;
    }

    case TC_ANY:
    case TC_NULL:
    default:
        return 1;
    }
}

/* ================================================================
 * tc_validate_json — validate full object against family constraints
 * ================================================================ */

int tc_validate_json(sqlite3 *db, int family_id,
                     const char *json, char **out_errors) {
    if (out_errors) *out_errors = NULL;

    /* Extract bindings from JSON */
    char *bindings = extract_bindings(json);
    if (!bindings) return 1;

    /* Parse bindings array */
    const char *p = bindings;
    while (*p && *p != '[') p++;
    if (*p == '[') p++;

    size_t ecap = 2048;
    char *errors = NULL;
    size_t eoff = 0;
    if (out_errors) {
        errors = malloc(ecap);
        errors[eoff++] = '[';
    }

    int all_valid = 1;
    int pos = 0;
    int err_count = 0;

    while (*p && *p != ']') {
        while (*p && (*p == ' ' || *p == ',' || *p == '\n' || *p == '\t')) p++;
        if (*p == ']' || !*p) break;

        /* Extract this value */
        char val[4096] = {0};
        if (*p == '"') {
            p++;
            size_t vi = 0;
            while (*p && *p != '"' && vi < sizeof(val) - 1) {
                if (*p == '\\' && *(p+1)) val[vi++] = *p++;
                val[vi++] = *p++;
            }
            val[vi] = '\0';
            if (*p == '"') p++;
        } else {
            size_t vi = 0;
            while (*p && *p != ',' && *p != ']' && vi < sizeof(val) - 1)
                val[vi++] = *p++;
            while (vi > 0 && val[vi-1] == ' ') vi--;
            val[vi] = '\0';
        }

        char msg[512] = {0};
        if (!tc_validate(db, family_id, pos, val, msg, sizeof(msg))) {
            all_valid = 0;
            if (errors) {
                while (eoff + 600 >= ecap) { ecap *= 2; errors = realloc(errors, ecap); }
                if (err_count > 0) errors[eoff++] = ',';
                eoff += (size_t)snprintf(errors + eoff, ecap - eoff, "\"%s\"", msg);
                err_count++;
            }
        }
        pos++;
    }

    free(bindings);

    if (errors) {
        errors[eoff++] = ']';
        errors[eoff] = '\0';
        *out_errors = errors;
    }

    return all_valid;
}

/* ================================================================
 * tc_constraints_json — export constraints as JSON
 * ================================================================ */

int tc_constraints_json(sqlite3 *db, int family_id, char **out_json) {
    *out_json = NULL;

    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT position, field_name, kind, range_lo, range_hi, "
        "enum_vals, pattern, is_optional "
        "FROM _type_constraints WHERE family_id=?1 ORDER BY position",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(q, 1, family_id);

    static const char *kind_names[] = {
        "string", "number", "boolean", "null", "enum",
        "int_range", "float_range", "pattern", "any", "optional"
    };

    size_t cap = 4096;
    char *out = malloc(cap);
    size_t off = 0;
    out[off++] = '[';
    int count = 0;

    while (sqlite3_step(q) == SQLITE_ROW) {
        while (off + 1024 >= cap) { cap *= 2; out = realloc(out, cap); }
        if (count > 0) out[off++] = ',';

        int k = sqlite3_column_int(q, 2);
        const char *kname = (k >= 0 && k <= 9) ? kind_names[k] : "unknown";
        const char *fn = (const char *)sqlite3_column_text(q, 1);
        const char *ev = (const char *)sqlite3_column_text(q, 5);

        off += (size_t)snprintf(out + off, cap - off,
            "{\"position\":%d,\"field\":\"%s\",\"type\":\"%s\","
            "\"range\":[%.2f,%.2f],\"enum\":\"%s\",\"optional\":%s}",
            sqlite3_column_int(q, 0),
            fn ? fn : "",
            kname,
            sqlite3_column_double(q, 3),
            sqlite3_column_double(q, 4),
            ev ? ev : "",
            sqlite3_column_int(q, 7) ? "true" : "false");
        count++;
    }
    sqlite3_finalize(q);

    out[off++] = ']'; out[off] = '\0';
    *out_json = out;
    return count;
}

/* ================================================================
 * tc_verify_family — check all existing bindings
 * ================================================================ */

int tc_verify_family(sqlite3 *db, int family_id) {
    /* First ensure constraints are inferred */
    tc_infer(db, family_id);

    sqlite3_stmt *q;
    if (sqlite3_prepare_v2(db,
        "SELECT target_id, position, val_text FROM _tensor_cells "
        "WHERE family_id=?1 ORDER BY target_id, position",
        -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(q, 1, family_id);

    int violations = 0;
    while (sqlite3_step(q) == SQLITE_ROW) {
        int pos = sqlite3_column_int(q, 1);
        const char *val = (const char *)sqlite3_column_text(q, 2);
        if (!tc_validate(db, family_id, pos, val, NULL, 0))
            violations++;
    }
    sqlite3_finalize(q);
    return violations;
}

/* ================================================================
 * tc_safe_update — type-checked symbolic update
 * ================================================================ */

int tc_safe_update(sqlite3 *db, const char *content_type,
                   int target_id, const char *field, const char *value) {
    /* Find family and position */
    sqlite3_stmt *fq;
    int family_id = 0, position = -1;

    if (sqlite3_prepare_v2(db,
        "SELECT m.family_id FROM _family_members m "
        "WHERE m.target_type=?1 AND m.target_id=?2 LIMIT 1",
        -1, &fq, NULL) == SQLITE_OK) {
        sqlite3_bind_text(fq, 1, content_type, -1, SQLITE_STATIC);
        sqlite3_bind_int(fq, 2, target_id);
        if (sqlite3_step(fq) == SQLITE_ROW)
            family_id = sqlite3_column_int(fq, 0);
        sqlite3_finalize(fq);
    }
    if (family_id <= 0) return -1;

    /* Find position for field */
    if (sqlite3_prepare_v2(db,
        "SELECT position FROM _tensor_cells "
        "WHERE family_id=?1 AND target_id=?2 AND field_name=?3 LIMIT 1",
        -1, &fq, NULL) == SQLITE_OK) {
        sqlite3_bind_int(fq, 1, family_id);
        sqlite3_bind_int(fq, 2, target_id);
        sqlite3_bind_text(fq, 3, field, -1, SQLITE_STATIC);
        if (sqlite3_step(fq) == SQLITE_ROW)
            position = sqlite3_column_int(fq, 0);
        sqlite3_finalize(fq);
    }
    if (position < 0) return -1;

    /* Validate */
    char msg[512] = {0};
    if (!tc_validate(db, family_id, position, value, msg, sizeof(msg))) {
        fprintf(stderr, "[type_check] violation: %s\n", msg);
        return -1;
    }

    /* Safe to update — delegate to tensor_update */
    return tensor_update(db, content_type, target_id, field, value);
}
