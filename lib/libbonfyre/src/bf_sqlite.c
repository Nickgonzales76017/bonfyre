/*
 * bf_sqlite.c — SQLite open helpers with optimal PRAGMA bundle
 *
 * Every Bonfyre binary that opens a SQLite database should use these
 * helpers instead of calling sqlite3_open() directly.  The wrapped opens
 * apply one PRAGMA bundle.  It trades the pre-write fsync for speed, which is
 * right for a cache or an index and wrong for a ledger, so a caller that
 * keeps money or commitments asks for the durable tier instead (see
 * bf_sqlite3_open_tier).  No throughput figure is claimed here: none has been
 * measured against these stores.
 *
 *   journal_mode=WAL     — concurrent readers + writer, eliminates write
 *                          lock contention (3-10x write throughput)
 *   synchronous=NORMAL   — removes pre-write fsync; still crash-safe
 *                          under WAL; ~2x faster than FULL default
 *   cache_size=-65536    — 64 MB page cache vs SQLite default of 2 MB
 *                          (32x larger: eliminates re-reads on hot tables)
 *   mmap_size=268435456  — 256 MB mmap window; OS page-cache serves reads
 *                          without a copy (zero-copy path for large DBs)
 *   temp_store=MEMORY    — temporary tables/indices held in RAM, not disk
 *
 * bf_sqlite3_open_ro applies cache/mmap/temp_store only (WAL cannot be
 * set on a READONLY connection; the DB picks up WAL if already in that
 * journal mode from a prior write-open).
 */

#include <sqlite3.h>
#include "bonfyre.h"

/* ── PRAGMAs, applied one at a time so a refusal is seen ────────────────── */

#include <stdlib.h>
#include <string.h>

static int apply(sqlite3 *db, const char *pragma) {
    return sqlite3_exec(db, pragma, NULL, NULL, NULL);
}

static int read_text(sqlite3 *db, const char *pragma, char *out, size_t n) {
    sqlite3_stmt *st = NULL;
    int ok = 0;
    if (n) out[0] = '\0';
    if (sqlite3_prepare_v2(db, pragma, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW) {
        const unsigned char *v = sqlite3_column_text(st, 0);
        if (v && n) { strncpy(out, (const char *)v, n - 1); out[n - 1] = '\0'; ok = 1; }
    }
    sqlite3_finalize(st);
    return ok;
}

/* What a connection is actually running under, read back from SQLite rather
 * than assumed from what was asked.  journal receives the journal mode
 * ("wal", "delete", "memory", ...); synchronous receives 0 OFF, 1 NORMAL,
 * 2 FULL, 3 EXTRA.  Returns SQLITE_OK when both could be read. */
int bf_sqlite3_policy(sqlite3 *db, char *journal, size_t journal_size, int *synchronous) {
    char level[16] = "";
    char mode[32] = "";
    if (!db) return SQLITE_MISUSE;
    if (!read_text(db, "PRAGMA journal_mode;", mode, sizeof(mode))) return SQLITE_ERROR;
    if (!read_text(db, "PRAGMA synchronous;", level, sizeof(level))) return SQLITE_ERROR;
    if (journal && journal_size) { strncpy(journal, mode, journal_size - 1); journal[journal_size - 1] = '\0'; }
    if (synchronous) *synchronous = atoi(level);
    return SQLITE_OK;
}

/* ── Public API ─────────────────────────────────────────────────────────── */

/* Open for writing at a stated durability.
 *
 * BF_SQLITE_FAST     WAL + synchronous=NORMAL: a power loss can drop the last
 *                    commits, never corrupt the file.  For caches, indexes
 *                    and anything that can be rebuilt.
 * BF_SQLITE_DURABLE  WAL + synchronous=FULL: a commit that returned is on
 *                    disk.  For ledgers, commitments and receipts.
 *
 * Unlike bf_sqlite3_open this refuses instead of hoping: if the journal mode
 * or the synchronous level does not read back as asked (a network filesystem
 * that cannot hold a WAL, a read-only volume), the connection is closed and
 * an error is returned, because a ledger opened at a weaker level than its
 * caller believes is the failure this exists to prevent. */
int bf_sqlite3_open_tier(const char *path, sqlite3 **db, int tier) {
    char mode[32] = "";
    int level = -1, rc, want = tier == BF_SQLITE_DURABLE ? 2 : 1;
    int in_memory;
    if (!path || !db || (tier != BF_SQLITE_FAST && tier != BF_SQLITE_DURABLE)) return SQLITE_MISUSE;
    in_memory = path[0] == '\0' || strcmp(path, ":memory:") == 0;
    rc = sqlite3_open(path, db);
    /* SQLite hands back a handle even when the open failed. */
    if (rc != SQLITE_OK) { sqlite3_close(*db); *db = NULL; return rc; }
    if ((rc = apply(*db, "PRAGMA journal_mode=WAL;")) != SQLITE_OK ||
        (rc = apply(*db, want == 2 ? "PRAGMA synchronous=FULL;" : "PRAGMA synchronous=NORMAL;")) != SQLITE_OK ||
        (rc = bf_sqlite3_policy(*db, mode, sizeof(mode), &level)) != SQLITE_OK) {
        sqlite3_close(*db); *db = NULL; return rc;
    }
    if (level != want || (!in_memory && strcmp(mode, "wal") != 0)) {
        sqlite3_close(*db); *db = NULL; return SQLITE_CANTOPEN;
    }
    /* These three are hints; a store that declines one still works. */
    apply(*db, "PRAGMA cache_size=-65536;");
    apply(*db, "PRAGMA mmap_size=268435456;");
    apply(*db, "PRAGMA temp_store=MEMORY;");
    return SQLITE_OK;
}

/* The long-standing opener: fast tier, and forgiving, because fourteen
 * commands already depend on it opening wherever SQLite itself would.  An
 * operator can raise every such store to the durable level at once with
 * BONFYRE_SQLITE_DURABILITY=full.  What was actually applied can be read with
 * bf_sqlite3_policy. */
int bf_sqlite3_open(const char *path, sqlite3 **db) {
    const char *asked;
    if (!path || !db) return SQLITE_MISUSE;
    int rc = sqlite3_open(path, db);
    if (rc != SQLITE_OK) return rc;
    asked = getenv("BONFYRE_SQLITE_DURABILITY");
    apply(*db, "PRAGMA journal_mode=WAL;");
    apply(*db, asked && strcmp(asked, "full") == 0 ? "PRAGMA synchronous=FULL;" : "PRAGMA synchronous=NORMAL;");
    apply(*db, "PRAGMA cache_size=-65536;");
    apply(*db, "PRAGMA mmap_size=268435456;");
    apply(*db, "PRAGMA temp_store=MEMORY;");
    return SQLITE_OK;
}

int bf_sqlite3_open_ro(const char *path, sqlite3 **db) {
    if (!path || !db) return SQLITE_MISUSE;
    int rc = sqlite3_open_v2(path, db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) return rc;
    apply(*db, "PRAGMA cache_size=-65536;");
    apply(*db, "PRAGMA mmap_size=268435456;");
    apply(*db, "PRAGMA temp_store=MEMORY;");
    return SQLITE_OK;
}
