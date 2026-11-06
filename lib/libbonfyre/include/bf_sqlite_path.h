#ifndef BF_SQLITE_PATH_H
#define BF_SQLITE_PATH_H

#include <sqlite3.h>
#include <limits.h>
#include <stddef.h>
#include <string.h>

/* Environment-provided database locations are filesystem paths, not SQLite
 * URIs. In particular, '?' and '#' must remain literal filename bytes. */
static inline sqlite3 *bf_sqlite_open_readonly_path(const char *path, int busy_ms) {
    sqlite3 *db = NULL;
    if (!path || !*path) return NULL;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return NULL;
    }
    sqlite3_busy_timeout(db, busy_ms);
    return db;
}

/* Transitional URI callers retain their existing control flow, but every
 * filename byte with URI meaning is encoded at one boundary. The return value
 * is cap on overflow, matching each caller's snprintf-style capacity check. */
static inline int bf_sqlite_readonly_uri(char *out, size_t cap, const char *path) {
    static const char hex[] = "0123456789ABCDEF";
    static const char prefix[] = "file:";
    static const char suffix[] = "?mode=ro";
    size_t used = sizeof(prefix) - 1;
    if (!out || !cap) return INT_MAX;
    out[0] = '\0';
    if (!path || cap > INT_MAX || used >= cap) return INT_MAX;
    memcpy(out, prefix, used);
    for (const unsigned char *p = (const unsigned char *)path; *p; p++) {
        unsigned char c = *p;
        int safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                   (c >= '0' && c <= '9') || c == '/' || c == '-' ||
                   c == '_' || c == '.' || c == '~';
        size_t step = safe ? 1 : 3;
        if (step >= cap - used) { out[0] = '\0'; return (int)cap; }
        if (safe) out[used++] = (char)c;
        else {
            out[used++] = '%';
            out[used++] = hex[c >> 4];
            out[used++] = hex[c & 15];
        }
    }
    if (sizeof(suffix) > cap - used) { out[0] = '\0'; return (int)cap; }
    memcpy(out + used, suffix, sizeof(suffix));
    return (int)(used + sizeof(suffix) - 1);
}

#endif
