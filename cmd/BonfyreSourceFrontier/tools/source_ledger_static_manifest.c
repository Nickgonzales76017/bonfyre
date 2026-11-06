#include <sqlite3.h>
#include <bonfyre.h>
#include <bf_static_query.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_MATERIAL_LEAF 180
#define SOURCE_FILTER " FROM channel WHERE state IN ('admitted','active') AND COALESCE(census_source,'')<>''"

static int emit_coordinate(const char *query, size_t query_len) {
    static const char route[] = "/channels?";
    static const char direct[] = "channels/__q/";
    static const char hashed[] = "channels/__q/h/";
    char digest[65];
    if (fwrite(route, 1, sizeof(route) - 1, stdout) != sizeof(route) - 1 ||
        fwrite(query, 1, query_len, stdout) != query_len || fputc('\t', stdout) == EOF)
        return 0;
    if (query_len <= MAX_MATERIAL_LEAF - (sizeof(direct) - 1) - 5) {
        if (fwrite(direct, 1, sizeof(direct) - 1, stdout) != sizeof(direct) - 1 ||
            fwrite(query, 1, query_len, stdout) != query_len) return 0;
    } else {
        bf_sha256_hex((const uint8_t *)query, query_len, digest);
        if (fwrite(hashed, 1, sizeof(hashed) - 1, stdout) != sizeof(hashed) - 1 ||
            fwrite(digest, 1, 64, stdout) != 64) return 0;
    }
    return fwrite(".html\n", 1, 6, stdout) == 6;
}

static int emit_channel(const unsigned char *address, size_t length) {
    char *query, *end;
    int ok;
    if (length > (SIZE_MAX - 16) / 3) return 0;
    query = malloc(length * 3 + 16);
    if (!query) return 0;
    end = query;
    if (address && length) {
        memcpy(end, "channel=", 8); end += 8;
        end = bf_static_query_encode(end, address, length);
    }
    ok = emit_coordinate(query, (size_t)(end - query));
    free(query);
    return ok;
}

static int manifest(sqlite3 *db, uint64_t page_size) {
    sqlite3_stmt *statement = NULL;
    sqlite3_int64 count;
    int rc, empty_emitted = 0;
    if (sqlite3_prepare_v2(db, "SELECT COUNT(*)" SOURCE_FILTER, -1, &statement, NULL) != SQLITE_OK)
        return 0;
    rc = sqlite3_step(statement);
    count = rc == SQLITE_ROW ? sqlite3_column_int64(statement, 0) : -1;
    sqlite3_finalize(statement);
    if (count < 0) return 0;
    for (uint64_t offset = 0; offset < (uint64_t)count;) {
        char query[96];
        int length = offset ? snprintf(query, sizeof(query), "standing=admitted&from=%llu",
                                       (unsigned long long)offset)
                            : snprintf(query, sizeof(query), "standing=admitted");
        if (length < 0 || (size_t)length >= sizeof(query) ||
            !emit_coordinate(query, (size_t)length)) return 0;
        if (page_size >= (uint64_t)count - offset) break;
        offset += page_size;
    }
    if (sqlite3_prepare_v2(db, "SELECT DISTINCT exact_address" SOURCE_FILTER
                               " ORDER BY exact_address", -1, &statement, NULL) != SQLITE_OK)
        return 0;
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        const unsigned char *address = sqlite3_column_text(statement, 0);
        int length = sqlite3_column_bytes(statement, 0);
        if (length < 0) { sqlite3_finalize(statement); return 0; }
        if ((!address || !length) && empty_emitted) continue;
        if (!address || !length) empty_emitted = 1;
        if (!emit_channel(address, length > 0 ? (size_t)length : 0)) {
            sqlite3_finalize(statement);
            return 0;
        }
    }
    sqlite3_finalize(statement);
    return rc == SQLITE_DONE;
}

int main(int argc, char **argv) {
    const char *frontier = NULL;
    sqlite3 *db = NULL;
    uint64_t page_size = 40;
    int ok;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--frontier") && i + 1 < argc) frontier = argv[++i];
        else if (!strcmp(argv[i], "--page-size") && i + 1 < argc) {
            char *end;
            unsigned long long value = strtoull(argv[++i], &end, 10);
            if (!argv[i][0] || *end || !value || argv[i][0] == '-') {
                fputs("source-ledger-static-manifest: --page-size must be positive\n", stderr);
                return 2;
            }
            page_size = (uint64_t)value;
        } else {
            fputs("usage: source-ledger-static-manifest --frontier frontier.db [--page-size n]\n", stderr);
            return 2;
        }
    }
    if (!frontier || sqlite3_open_v2(frontier, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fputs("source-ledger-static-manifest: frontier is not readable\n", stderr);
        if (db) sqlite3_close(db);
        return 1;
    }
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    ok = manifest(db, page_size);
    sqlite3_close(db);
    if (!ok || fflush(stdout) != 0) {
        fputs("source-ledger-static-manifest: frontier query or output failed\n", stderr);
        return 1;
    }
    return 0;
}
