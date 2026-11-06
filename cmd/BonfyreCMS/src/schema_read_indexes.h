#ifndef BF_CMS_SCHEMA_READ_INDEXES_H
#define BF_CMS_SCHEMA_READ_INDEXES_H
#include "bf_json.h"

/* Physical read indexes belong to the schema owner, not to a web request.
 * Called inside cms_schema_apply after the declared columns exist. These
 * non-unique indexes change no rows, namespace boundaries or identity law. */
static int cms_index_identifier(const char *s) {
    if (!s || !*s || !(isalpha((unsigned char)*s) || *s == '_')) return 0;
    for (; *s; s++) if (!(isalnum((unsigned char)*s) || *s == '_')) return 0;
    return 1;
}

static int cms_read_indexes(sqlite3 *db, const char *table, const char *schema) {
    bf_json_doc_t *doc = bf_json_parse_str(schema, NULL, 0);
    const bf_json_node_t *root = doc ? bf_json_root(doc) : NULL;
    const bf_json_node_t *indexes = root ? bf_json_obj_get(doc, root, "readIndexes") : NULL;
    char seen[16][64]; int count = 0, result = -1;
    sqlite3_stmt *st = NULL;
    if (!doc || !root || root->type != BF_JSON_OBJECT) goto done;
    if (!indexes) { result = 0; goto done; }
    if (indexes->type != BF_JSON_OBJECT || indexes->child_count > 16) goto done;
    for (const bf_json_node_t *index = bf_json_child_first(doc, indexes); index;
         index = bf_json_child_next(doc, index)) {
        char name[256], columns[8][128]; int n = index->child_count, rc, matched = 0;
        if (count >= 16 || !index->key || index->key_len < 1 || index->key_len >= 64 ||
            index->type != BF_JSON_ARRAY || n < 1 || n > 8) goto done;
        memcpy(seen[count], index->key, (size_t)index->key_len);
        seen[count][index->key_len] = 0;
        if (!cms_index_identifier(seen[count])) goto done;
        for (int i = 0; i < count; i++) if (!sqlite3_stricmp(seen[i], seen[count])) goto done;
        if (snprintf(name, sizeof(name), "idx_%s_read_%s", table, seen[count++]) >= (int)sizeof(name)) goto done;
        for (int i = 0; i < n; i++) {
            const bf_json_node_t *column = bf_json_arr_get(doc, index, i);
            if (!column || column->type != BF_JSON_STRING || column->val.str.len < 1 ||
                column->val.str.len >= (int)sizeof(columns[i])) goto done;
            bf_json_get_str_copy(column, columns[i], sizeof(columns[i]));
            if (!cms_index_identifier(columns[i])) goto done;
            for (int j = 0; j < i; j++) if (!sqlite3_stricmp(columns[j], columns[i])) goto done;
            if (sqlite3_prepare_v2(db,
                  "SELECT 1 FROM pragma_table_info(?1) WHERE name=?2 COLLATE NOCASE", -1, &st, NULL) != SQLITE_OK) goto done;
            sqlite3_bind_text(st, 1, table, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, columns[i], -1, SQLITE_TRANSIENT);
            rc = sqlite3_step(st); sqlite3_finalize(st); st = NULL;
            if (rc != SQLITE_ROW) goto done;
        }
        sqlite3_str *sql = sqlite3_str_new(db);
        sqlite3_str_appendf(sql, "CREATE INDEX IF NOT EXISTS \"%w\" ON \"%w\"(", name, table);
        for (int i = 0; i < n; i++) sqlite3_str_appendf(sql, "%s\"%w\"", i ? "," : "", columns[i]);
        sqlite3_str_appendall(sql, ")");
        char *text = sqlite3_str_finish(sql);
        if (!text) goto done;
        rc = sqlite3_exec(db, text, NULL, NULL, NULL); sqlite3_free(text);
        if (rc != SQLITE_OK) goto done;
        /* IF NOT EXISTS alone would accept an unrelated, unique or differently
         * shaped index with the same name. Refuse drift inside the savepoint. */
        if (sqlite3_prepare_v2(db,
              "SELECT [unique],partial FROM pragma_index_list(?1) WHERE name=?2",
              -1, &st, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(st, 1, table, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, name, -1, SQLITE_TRANSIENT);
        rc = sqlite3_step(st);
        int valid = rc == SQLITE_ROW && !sqlite3_column_int(st, 0) && !sqlite3_column_int(st, 1);
        sqlite3_finalize(st); st = NULL;
        if (!valid) goto done;
        if (sqlite3_prepare_v2(db, "SELECT name FROM pragma_index_info(?1) ORDER BY seqno",
              -1, &st, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_text(st, 1, name, -1, SQLITE_TRANSIENT);
        while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
            const char *column = (const char *)sqlite3_column_text(st, 0);
            if (matched >= n || !column || sqlite3_stricmp(column, columns[matched])) goto done;
            matched++;
        }
        sqlite3_finalize(st); st = NULL;
        if (rc != SQLITE_DONE || matched != n) goto done;
    }
    result = 0;
done:
    sqlite3_finalize(st);
    bf_json_free(doc);
    if (result) fprintf(stderr, "[schema] invalid or conflicting readIndexes for %s; schema refused\n", table);
    return result;
}
#endif

/* Supported declaration:
 * readIndexes: { label: [column, ...] }
 * At most sixteen named indexes, each with one to eight actual columns.
 * Order is declared and participates in readback. No SQL expressions, unique
 * constraints, implicit namespace filters or automatic index deletion exist.
 * An omitted declaration leaves an existing physical index alone.
 */
