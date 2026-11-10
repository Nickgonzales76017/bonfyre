#define _POSIX_C_SOURCE 200809L
#include "bf_sqlite_path.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int main(void) {
    char path[] = "/tmp/bonfyre-readonly?#'XXXXXX";
    sqlite3 *writer = NULL, *reader = NULL;
    int fd = mkstemp(path);
    assert(fd >= 0);
    close(fd);
    assert(sqlite3_open(path, &writer) == SQLITE_OK);
    assert(sqlite3_exec(writer, "CREATE TABLE proof(value INTEGER); INSERT INTO proof VALUES(7);", NULL, NULL, NULL) == SQLITE_OK);
    sqlite3_close(writer);

    reader = bf_sqlite_open_readonly_path(path, 1000);
    assert(reader);
    sqlite3_stmt *st = NULL;
    assert(sqlite3_prepare_v2(reader, "SELECT value FROM proof", -1, &st, NULL) == SQLITE_OK);
    assert(sqlite3_step(st) == SQLITE_ROW && sqlite3_column_int(st, 0) == 7);
    sqlite3_finalize(st);
    assert(sqlite3_exec(reader, "INSERT INTO proof VALUES(8)", NULL, NULL, NULL) == SQLITE_READONLY);
    sqlite3_close(reader);
    char uri[256];
    assert(bf_sqlite_readonly_uri(uri, sizeof(uri), path) < (int)sizeof(uri));
    assert(strstr(uri, "%3F%23"));
    assert(sqlite3_open_v2(uri, &reader, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) == SQLITE_OK);
    assert(sqlite3_prepare_v2(reader, "SELECT value FROM proof", -1, &st, NULL) == SQLITE_OK);
    assert(sqlite3_step(st) == SQLITE_ROW && sqlite3_column_int(st, 0) == 7);
    sqlite3_finalize(st);
    sqlite3_close(reader);
    char too_small[8] = "old";
    assert(bf_sqlite_readonly_uri(too_small, sizeof(too_small), path) >= (int)sizeof(too_small));
    assert(!too_small[0]);
    assert(!bf_sqlite_open_readonly_path(NULL, 1000));
    assert(!bf_sqlite_open_readonly_path("", 1000));
    assert(unlink(path) == 0);
    puts("sqlite literal read-only path: PASS");
    return 0;
}
