/* The shared opener says what it applied, and the durable tier refuses when it
 * cannot have what it asked for. */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "bonfyre.h"

static int failed;
#define CHECK(what, cond) do { if (cond) printf("  ok   %s\n", what); else { printf("  FAIL %s\n", what); failed++; } } while (0)

int main(void) {
    char dir[] = "/tmp/bf-sqlite-policy-XXXXXX", path[256], mode[32];
    sqlite3 *db = NULL;
    int level = -1;
    if (!mkdtemp(dir)) return 2;

    snprintf(path, sizeof(path), "%s/fast.db", dir);
    CHECK("the fast tier opens", bf_sqlite3_open_tier(path, &db, BF_SQLITE_FAST) == SQLITE_OK);
    CHECK("its policy can be read", bf_sqlite3_policy(db, mode, sizeof(mode), &level) == SQLITE_OK);
    CHECK("fast is WAL", strcmp(mode, "wal") == 0);
    CHECK("fast is synchronous NORMAL", level == 1);
    sqlite3_close(db); db = NULL;

    snprintf(path, sizeof(path), "%s/ledger.db", dir);
    CHECK("the durable tier opens", bf_sqlite3_open_tier(path, &db, BF_SQLITE_DURABLE) == SQLITE_OK);
    bf_sqlite3_policy(db, mode, sizeof(mode), &level);
    CHECK("durable is WAL", strcmp(mode, "wal") == 0);
    CHECK("durable is synchronous FULL", level == 2);
    CHECK("a durable store takes a write", sqlite3_exec(db, "CREATE TABLE t(x); INSERT INTO t VALUES(1);", NULL, NULL, NULL) == SQLITE_OK);
    sqlite3_close(db); db = NULL;

    CHECK("an unknown tier is refused", bf_sqlite3_open_tier(path, &db, 7) == SQLITE_MISUSE);
    snprintf(path, sizeof(path), "%s/no-such-dir/x.db", dir);
    CHECK("a store that cannot be opened is an error, not a quiet success", bf_sqlite3_open_tier(path, &db, BF_SQLITE_DURABLE) != SQLITE_OK && db == NULL);

    snprintf(path, sizeof(path), "%s/old.db", dir);
    unsetenv("BONFYRE_SQLITE_DURABILITY");
    CHECK("the long-standing opener still opens", bf_sqlite3_open(path, &db) == SQLITE_OK);
    bf_sqlite3_policy(db, mode, sizeof(mode), &level);
    CHECK("and is NORMAL unless told otherwise", level == 1 && strcmp(mode, "wal") == 0);
    sqlite3_close(db); db = NULL;
    setenv("BONFYRE_SQLITE_DURABILITY", "full", 1);
    CHECK("it opens with the operator's setting", bf_sqlite3_open(path, &db) == SQLITE_OK);
    bf_sqlite3_policy(db, mode, sizeof(mode), &level);
    CHECK("and is FULL when the operator says full", level == 2);
    sqlite3_close(db);

    snprintf(path, sizeof(path), "rm -rf %s", dir); system(path);
    printf("sqlite policy: %s\n", failed ? "FAIL" : "PASS");
    return failed ? 1 : 0;
}
