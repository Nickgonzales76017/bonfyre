/* Witness: the vector store kind builds a store from the registry and reads
 * it back. schemas/storage/stores declares vec_artifacts as kind "vector"
 * (runtime width, l2, float32); the compiler emits BF_STORE_VEC_INDEX_*. This
 * creates the table from _VECTOR_DDL at a width, checks SQLite holds exactly
 * that text, inserts three vectors and asks sqlite-vec for the nearest one.
 * Without the sqlite-vec extension it says so and exits 0 (nothing to witness
 * on this machine), never a pass. */
#include "../generated/store_schema.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    const char *ext = argc > 1 ? argv[1] : getenv("BONFYRE_VEC_EXT");
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; char *err = NULL, ddl[256];
    int failures = 0;
    if (!ext || !*ext) ext = "/opt/homebrew/lib/sqlite_vec/vec0";
    if (strcmp(BF_STORE_VEC_INDEX_KIND, "vector") || BF_STORE_VEC_INDEX_VECTOR_DIMENSION != 0 ||
        strcmp(BF_STORE_VEC_INDEX_VECTOR_METRIC, "l2") || strcmp(BF_STORE_VEC_INDEX_VECTOR_QUANTIZATION, "float32")) {
        fputs("FAIL: vec_artifacts is not declared as a runtime-width l2 float32 vector store\n", stderr);
        return 1;
    }
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) return 2;
    sqlite3_enable_load_extension(db, 1);
    if (sqlite3_load_extension(db, ext, NULL, &err) != SQLITE_OK) {
        printf("vector store kind: sqlite-vec not loadable here (%s); not witnessed\n", err ? err : "?");
        sqlite3_free(err); sqlite3_close(db);
        return 0;
    }
    snprintf(ddl, sizeof(ddl), BF_STORE_VEC_INDEX_VECTOR_DDL, 4);
    if (sqlite3_exec(db, ddl, NULL, NULL, &err) != SQLITE_OK) { fprintf(stderr, "FAIL: %s: %s\n", ddl, err); return 1; }
    if (sqlite3_prepare_v2(db, "SELECT sql FROM sqlite_master WHERE name=?1", -1, &st, NULL) != SQLITE_OK) return 2;
    sqlite3_bind_text(st, 1, BF_STORE_VEC_INDEX_VECTOR_TABLE, -1, SQLITE_STATIC);
    {
        /* SQLite keeps the statement without its IF NOT EXISTS. */
        const char *held = sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "";
        char expect[256];
        snprintf(expect, sizeof(expect), "CREATE VIRTUAL TABLE %s", ddl + strlen("CREATE VIRTUAL TABLE IF NOT EXISTS "));
        if (strcmp(held, expect)) { fprintf(stderr, "FAIL: SQLite holds \"%s\", registry says \"%s\"\n", held, expect); failures++; }
    }
    sqlite3_finalize(st);
    if (sqlite3_exec(db,
            "INSERT INTO vec_artifacts(id, embedding) VALUES"
            "('a','[1,0,0,0]'),('b','[0,1,0,0]'),('c','[0.9,0.1,0,0]');", NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "FAIL: insert: %s\n", err); return 1;
    }
    if (sqlite3_prepare_v2(db, "SELECT id FROM vec_artifacts WHERE embedding MATCH '[1,0,0,0]' AND k = 2 ORDER BY distance", -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "FAIL: knn: %s\n", sqlite3_errmsg(db)); return 1;
    }
    const char *order[2] = {"a", "c"};
    for (int i = 0; i < 2; i++) {
        if (sqlite3_step(st) != SQLITE_ROW || strcmp((const char *)sqlite3_column_text(st, 0), order[i])) {
            fprintf(stderr, "FAIL: nearest %d is not %s\n", i + 1, order[i]); failures++; break;
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(db);
    if (failures) return 1;
    puts("vector store kind: built from the registry and read back (a, c nearest)");
    return 0;
}
