/* Witness: the dynamic store kind joins stores by its declared rows.
 * schemas/storage/stores declares item_depth (kind dynamic, shard named):
 * four joins, each a schema name, an environment variable and a probe table.
 * This builds an item archive and two of the joined stores in temp files,
 * opens the archive through bf_store_open, joins every declared row with
 * bf_store_join, and checks that a join counts only when its file is present
 * and holds its probe table, that rows read back across the join, and that a
 * joined store refuses a write. A schema name bound as ?2 is the attach the
 * owners use, so no schema name is pasted into SQL. */
#include "bf_store_durability.h"
#include "../generated/store_schema.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int make(char *path, const char *sql) {
    sqlite3 *db = NULL; int fd = mkstemp(path), ok;
    if (fd < 0) return 0;
    close(fd);
    ok = sqlite3_open(path, &db) == SQLITE_OK && sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK;
    sqlite3_close(db);
    return ok;
}

int main(void) {
    static const BfStoreJoin joins[] = BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOINS;
    char archive[] = "/tmp/dyn-archive-XXXXXX", frontier[] = "/tmp/dyn-frontier-XXXXXX", depth[] = "/tmp/dyn-depth-XXXXXX";
    sqlite3 *db = NULL; sqlite3_stmt *st = NULL; int failures = 0, got[4];
    if (strcmp(BF_STORE_WEB_ITEM_DEPTH_KIND, "dynamic") || strcmp(BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_SHARD, "named") ||
        BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_COUNT != 4) { fputs("FAIL: item_depth is not a named dynamic store of four joins\n", stderr); return 1; }
    if (!make(archive, "CREATE TABLE item(source TEXT, key TEXT); INSERT INTO item VALUES('s','k');") ||
        !make(frontier, "CREATE TABLE channel(census_source TEXT, root_id TEXT); INSERT INTO channel VALUES('s','r');") ||
        !make(depth, "CREATE TABLE unrelated(x);")) return 2;
    setenv("BONFYRE_SOURCE_FRONTIER_DB", frontier, 1);
    setenv("BONFYRE_DETAIL_DEPTH_DB", depth, 1);   /* present, but without item_facet */
    unsetenv("BONFYRE_FRAPPE_SURFACE_DB");          /* absent */
    unsetenv("BONFYRE_CMS_DB");
    if (bf_store_open(archive, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, &bf_store_policy_item_depth) != SQLITE_OK) return 2;
    for (int i = 0; i < 4; i++) got[i] = bf_store_join(db, &joins[i], NULL, BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_ATTACH);
    if (!got[BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_FR]) { fputs("FAIL: frontier with its probe table did not join\n", stderr); failures++; }
    if (got[BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_FP]) { fputs("FAIL: an absent store counted as joined\n", stderr); failures++; }
    if (got[BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_DD]) { fputs("FAIL: a store without its probe table counted as joined\n", stderr); failures++; }
    if (got[BF_STORE_WEB_ITEM_DEPTH_DYNAMIC_JOIN_JT]) { fputs("FAIL: an unset store counted as joined\n", stderr); failures++; }
    if (sqlite3_prepare_v2(db, "SELECT c.root_id FROM item i JOIN fr.channel c ON c.census_source=i.source", -1, &st, NULL) != SQLITE_OK ||
        sqlite3_step(st) != SQLITE_ROW || strcmp((const char *)sqlite3_column_text(st, 0), "r")) {
        fprintf(stderr, "FAIL: no row across the join: %s\n", sqlite3_errmsg(db)); failures++;
    }
    sqlite3_finalize(st);
    if (sqlite3_exec(db, "DELETE FROM fr.channel", NULL, NULL, NULL) == SQLITE_OK) { fputs("FAIL: a joined store accepted a write\n", stderr); failures++; }
    sqlite3_close(db);
    unlink(archive); unlink(frontier); unlink(depth);
    if (failures) return 1;
    puts("dynamic store kind: item_depth joins by its declared rows; probe and presence decide each join; joined stores are read-only");
    return 0;
}
