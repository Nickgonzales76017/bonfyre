#include "surface_ir.h"

#include <stdio.h>
#include <string.h>

#include "bf_lex.h"
/* Delegates to the one implementation. Nineteen copies of this shape existed
 * with sixteen different opinions about escaping.
 */
static void json_string(FILE *out, const unsigned char *value) {
    bf_lex_json_emit(out, (const char *)value);
}

static void json_nullable(FILE *out, const unsigned char *value) {
    if (!value) fputs("null", out);
    else json_string(out, value);
}

static int bounded_atom(const char *value, size_t max) {
    size_t n;
    if (!value || !(n = strlen(value)) || n >= max) return 0;
    for (size_t i = 0; i < n; ++i) {
        unsigned char c = (unsigned char)value[i];
        if (c < 0x20 || c == 0x7f) return 0;
    }
    return 1;
}

int bf_surface_ir_compile(sqlite3 *db,
                          const char *namespace_name,
                          const char *page,
                          const char *slot,
                          const char *audience,
                          FILE *out) {
    sqlite3_stmt *st = NULL;
    const char *sql_slot =
        "SELECT id,coordinate,page,slot,audience,kind,position,eyebrow,heading,body,"
        "label,href,variant_key,semantic_tags,projection_contract,source_ref,"
        "source_digest,evidence_ref "
        "FROM site_fragment "
        "WHERE namespace=?1 AND page=?2 AND slot=?3 AND audience=?4 "
        "AND status='published' ORDER BY position,id";
    const char *sql_page =
        "SELECT id,coordinate,page,slot,audience,kind,position,eyebrow,heading,body,"
        "label,href,variant_key,semantic_tags,projection_contract,source_ref,"
        "source_digest,evidence_ref "
        "FROM site_fragment "
        "WHERE namespace=?1 AND page=?2 AND audience=?3 "
        "AND status='published' ORDER BY slot,position,id";
    int rc, rows = 0, first = 1;

    if (!db || !out ||
        !bounded_atom(namespace_name, 128) ||
        !bounded_atom(page, 128) ||
        !bounded_atom(audience, 128) ||
        (slot && *slot && !bounded_atom(slot, 128))) return 2;

    rc = sqlite3_prepare_v2(db, (slot && *slot) ? sql_slot : sql_page, -1, &st, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "bonfyre-surface-ir: prepare failed: %s\n", sqlite3_errmsg(db));
        return 3;
    }

    sqlite3_bind_text(st, 1, namespace_name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, page, -1, SQLITE_TRANSIENT);
    if (slot && *slot) {
        sqlite3_bind_text(st, 3, slot, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, audience, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_text(st, 3, audience, -1, SQLITE_TRANSIENT);
    }

    fputs("{\"schema_version\":\"bonfyre.surface-ir.v1\","
          "\"owner\":\"BonfyreSurface\","
          "\"content_owner\":\"BonfyreCMS\","
          "\"selection\":\"exact\","
          "\"namespace\":", out);
    json_string(out, (const unsigned char *)namespace_name);
    fputs(",\"page\":", out);
    json_string(out, (const unsigned char *)page);
    fputs(",\"slot\":", out);
    if (slot && *slot) json_string(out, (const unsigned char *)slot); else fputs("null", out);
    fputs(",\"audience\":", out);
    json_string(out, (const unsigned char *)audience);
    fputs(",\"approximation\":{\"role\":\"candidate_only\","
          "\"may_override_exact_coordinate\":false},"
          "\"fragments\":[", out);

    while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
        if (!first) fputc(',', out);
        first = 0;
        fputs("{\"id\":", out);
        fprintf(out, "%lld", (long long)sqlite3_column_int64(st, 0));
        fputs(",\"coordinate\":", out); json_nullable(out, sqlite3_column_text(st, 1));
        fputs(",\"page\":", out); json_nullable(out, sqlite3_column_text(st, 2));
        fputs(",\"slot\":", out); json_nullable(out, sqlite3_column_text(st, 3));
        fputs(",\"audience\":", out); json_nullable(out, sqlite3_column_text(st, 4));
        fputs(",\"kind\":", out); json_nullable(out, sqlite3_column_text(st, 5));
        fputs(",\"position\":", out); fprintf(out, "%d", sqlite3_column_int(st, 6));
        fputs(",\"eyebrow\":", out); json_nullable(out, sqlite3_column_text(st, 7));
        fputs(",\"heading\":", out); json_nullable(out, sqlite3_column_text(st, 8));
        fputs(",\"body\":", out); json_nullable(out, sqlite3_column_text(st, 9));
        fputs(",\"label\":", out); json_nullable(out, sqlite3_column_text(st, 10));
        fputs(",\"href\":", out); json_nullable(out, sqlite3_column_text(st, 11));
        fputs(",\"variant_key\":", out); json_nullable(out, sqlite3_column_text(st, 12));
        fputs(",\"semantic_tags_json\":", out); json_nullable(out, sqlite3_column_text(st, 13));
        fputs(",\"projection_contract_json\":", out); json_nullable(out, sqlite3_column_text(st, 14));
        fputs(",\"source_ref\":", out); json_nullable(out, sqlite3_column_text(st, 15));
        fputs(",\"source_digest\":", out); json_nullable(out, sqlite3_column_text(st, 16));
        fputs(",\"evidence_ref\":", out); json_nullable(out, sqlite3_column_text(st, 17));
        fputc('}', out);
        rows++;
    }

    if (rc != SQLITE_DONE) {
        fprintf(stderr, "bonfyre-surface-ir: step failed: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(st);
        return 4;
    }
    sqlite3_finalize(st);

    fprintf(out,
            "],\"count\":%d,"
            "\"publication_rule\":\"only status=published participates in public SurfaceIR\","
            "\"forbidden_inference\":\"a rendered affordance is not effect authority; "
            "an ANN candidate is not an exact address match\"}\n",
            rows);
    return 0;
}
