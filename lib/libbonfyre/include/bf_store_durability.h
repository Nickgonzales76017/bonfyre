#ifndef BF_STORE_DURABILITY_H
#define BF_STORE_DURABILITY_H

/* What a store promises about its journal, declared once in
 * schemas/storage/stores (durability) and compiled into each owner's
 * generated/store_schema.h as a BfStoreDurability initializer. An owner opens
 * the store, then hands the connection here:
 *
 *   bf_store_writer(db, &policy)  journal mode, synchronous, autocheckpoint,
 *                                 journal_size_limit, and a commit hook that
 *                                 forces a TRUNCATE checkpoint once the WAL
 *                                 passes wal_truncate_pages.
 *   bf_store_lease(db, &policy, readonly)
 *                                 the connection belongs to the current
 *                                 request; a read-only one gets a deadline (a
 *                                 statement still running read_txn_max_ms
 *                                 after the open is interrupted), and
 *                                 bf_store_leases_end() finalizes and closes
 *                                 whatever the request left open.
 *
 * Why both sides. A passive autocheckpoint copies WAL frames back only up to
 * the oldest open reader, and journal_size_limit trims the file only when the
 * WAL restarts, which needs every reader gone. One reader holding a snapshot
 * (a statement stepped and never reset, a connection never closed) pins the
 * WAL forever while writers keep appending. The writer hook closes the gap
 * left by many short overlapping readers; the lease closes the gap left by a
 * leaked one. Neither is a cap on what a single transaction may write. */

#include <signal.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    const char *store;            /* registry name, for diagnostics */
    const char *journal_mode;     /* WAL | DELETE | MEMORY | OFF */
    const char *synchronous;      /* OFF | NORMAL | FULL */
    int wal_autocheckpoint;       /* pages; passive checkpoint after a commit */
    long long journal_size_limit; /* bytes kept after a WAL restart; -1 none */
    int wal_truncate_pages;       /* pages; a commit past this forces TRUNCATE */
    int busy_timeout_ms;          /* how long an open waits on a held lock */
    int read_txn_max_ms;          /* longest a request may hold the store */
} BfStoreDurability;

/* Process-wide state is shared by every translation unit that includes this
 * header: a weak definition in each object, one symbol after linking. A
 * connection leased in site_mcp.c is reaped by the serve loop in main.c. */
#define BF_STORE_SHARED __attribute__((weak))

/* Observed by witnesses; never consulted for authority. */
typedef struct {
    long long passive, truncated, truncate_busy, leases_reaped, interrupted;
} BfStoreDurabilityCounters;
BF_STORE_SHARED BfStoreDurabilityCounters bf_store_durability_counters;

/* After a refused TRUNCATE the next attempt waits for the WAL to grow by
 * another wal_truncate_pages, so a reader that legitimately runs long costs
 * writers one wait per threshold, not one per commit. */
static int bf_store_truncate_next;

static inline int bf_store_wal_commit(void *arg, sqlite3 *db, const char *schema, int frames) {
    const BfStoreDurability *policy = (const BfStoreDurability *)arg;
    int log = 0, done = 0;
    if (frames < policy->wal_truncate_pages) bf_store_truncate_next = 0;
    if (policy->wal_truncate_pages > 0 && frames >= policy->wal_truncate_pages && frames >= bf_store_truncate_next) {
        /* TRUNCATE waits (through the busy handler) for readers on older
         * snapshots to finish, backfills everything and resets the WAL to
         * zero bytes. Readers that start meanwhile read the newest snapshot,
         * so a stream of short readers can no longer starve the restart. */
        int rc = sqlite3_wal_checkpoint_v2(db, schema, SQLITE_CHECKPOINT_TRUNCATE, &log, &done);
        if (rc == SQLITE_OK) {
            bf_store_durability_counters.truncated++;
            bf_store_truncate_next = 0;
        } else {
            bf_store_durability_counters.truncate_busy++;
            bf_store_truncate_next = frames + policy->wal_truncate_pages;
            fprintf(stderr, "store %s: WAL at %d pages (%d of %d copied back), checkpoint %s\n",
                    policy->store ? policy->store : "?", frames, done, log, sqlite3_errstr(rc));
        }
    } else if (policy->wal_autocheckpoint > 0 && frames >= policy->wal_autocheckpoint) {
        /* What sqlite3_wal_autocheckpoint would have done; installing a WAL
         * hook replaces it, so the hook carries it. */
        (void)sqlite3_wal_checkpoint_v2(db, schema, SQLITE_CHECKPOINT_PASSIVE, &log, &done);
        bf_store_durability_counters.passive++;
    }
    return SQLITE_OK;
}

static inline int bf_store_pragma(sqlite3 *db, const char *name, const char *value) {
    char sql[128];
    if (!value || !*value) return 0;
    if (snprintf(sql, sizeof(sql), "PRAGMA %s=%s;", name, value) >= (int)sizeof(sql)) return -1;
    return sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK ? 0 : -1;
}

/* A connection that writes. Returns 0, or -1 when the store refused the
 * policy (the caller decides whether to go on without it). */
static inline int bf_store_writer(sqlite3 *db, const BfStoreDurability *policy) {
    char number[32];
    if (!db || !policy) return -1;
    if (policy->busy_timeout_ms > 0) sqlite3_busy_timeout(db, policy->busy_timeout_ms);
    if (bf_store_pragma(db, "journal_mode", policy->journal_mode) ||
        bf_store_pragma(db, "synchronous", policy->synchronous)) return -1;
    snprintf(number, sizeof(number), "%d", policy->wal_autocheckpoint);
    if (bf_store_pragma(db, "wal_autocheckpoint", number)) return -1;
    snprintf(number, sizeof(number), "%lld", policy->journal_size_limit);
    if (bf_store_pragma(db, "journal_size_limit", number)) return -1;
    if (policy->journal_mode && !strcmp(policy->journal_mode, "WAL"))
        sqlite3_wal_hook(db, bf_store_wal_commit, (void *)policy);
    return 0;
}

/* Attach a declared store (its generated _ATTACH statement) to db. A
 * read-only attach names the file as a file: URI with mode=ro, so db must
 * have been opened with SQLITE_OPEN_URI. Returns 1 when attached. */
/* A filesystem path as a read-only file: URI; every byte with URI meaning
 * ('?', '#', '%', spaces ...) is percent-encoded. Caller frees. */
static inline char *bf_store_readonly_uri(const char *path) {
    static const char hex[] = "0123456789ABCDEF";
    size_t n = path ? strlen(path) : 0, used = 0;
    char *uri = path ? (char *)sqlite3_malloc64(n * 3 + 16) : NULL;
    if (!uri) return NULL;
    memcpy(uri, "file:", 5); used = 5;
    for (const unsigned char *p = (const unsigned char *)path; *p; p++) {
        unsigned char c = *p;
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
            c == '/' || c == '-' || c == '_' || c == '.' || c == '~') uri[used++] = (char)c;
        else { uri[used++] = '%'; uri[used++] = hex[c >> 4]; uri[used++] = hex[c & 15]; }
    }
    memcpy(uri + used, "?mode=ro", 9);
    return uri;
}

/* Attach a declared store (its generated _ATTACH statement, ?1 the file and,
 * when the statement has one, ?2 the schema name) to db. A read-only attach
 * names the file as a file: URI with mode=ro, so db must have been opened
 * with SQLITE_OPEN_URI. Returns 1 when attached. */
static inline int bf_store_attach_as(sqlite3 *db, const char *path, const char *attach_sql, const char *as, int readonly) {
    sqlite3_stmt *st = NULL;
    char *uri = !path || !*path ? NULL : readonly ? bf_store_readonly_uri(path) : sqlite3_mprintf("%s", path);
    int ok = uri && sqlite3_prepare_v2(db, attach_sql, -1, &st, NULL) == SQLITE_OK &&
             sqlite3_bind_text(st, 1, uri, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
             (!as || sqlite3_bind_text(st, 2, as, -1, SQLITE_TRANSIENT) == SQLITE_OK) &&
             sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    sqlite3_free(uri);
    return ok;
}
#define bf_store_attach(db, path, attach_sql, readonly) bf_store_attach_as((db), (path), (attach_sql), NULL, (readonly))

/* ---- dynamic joins -------------------------------------------------------
 * A dynamic store of shard "named" declares which further stores join one
 * connection, under which schema name, and the table whose presence makes the
 * join usable. The compiler emits them as a BfStoreJoin array. */
typedef struct { const char *as; const char *env; const char *probe; int required; } BfStoreJoin;

/* 1 when the schema holds the table. */
static inline int bf_store_schema_has(sqlite3 *db, const char *schema, const char *table) {
    sqlite3_stmt *st = NULL; int ok = 0;
    char *sql = sqlite3_mprintf("SELECT 1 FROM \"%w\".sqlite_master WHERE type='table' AND name=?1 LIMIT 1", schema);
    if (sql && sqlite3_prepare_v2(db, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, table, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(st) == SQLITE_ROW;
    }
    sqlite3_finalize(st);
    sqlite3_free(sql);
    return ok;
}

/* Join one declared store read-only: path, or the join's env when path is
 * NULL. Returns 1 only when it attached and its probe table is there. */
static inline int bf_store_join(sqlite3 *db, const BfStoreJoin *join, const char *path, const char *attach_sql) {
    if (!path && join->env) path = getenv(join->env);
    if (!path || !*path || !bf_store_attach_as(db, path, attach_sql, join->as, 1)) return 0;
    return !join->probe || bf_store_schema_has(db, join->as, join->probe);
}

/* The declared join named as, or NULL. */
static inline const BfStoreJoin *bf_store_join_named(const BfStoreJoin *joins, int count, const char *as) {
    for (int i = 0; i < count; i++) if (!strcmp(joins[i].as, as)) return &joins[i];
    return NULL;
}

/* ---- request leases ------------------------------------------------------
 * A process that serves requests in a loop owns every store connection a
 * request opened only until the request ends. */
#ifndef BF_STORE_LEASES_MAX
#define BF_STORE_LEASES_MAX 64
#endif
typedef struct { sqlite3 *db; const BfStoreDurability *policy; struct timespec opened; const void *caller; const char *site; } BfStoreLease;
BF_STORE_SHARED BfStoreLease bf_store_leases[BF_STORE_LEASES_MAX];
/* Leases exist only in a process that serves requests; a batch command that
 * holds one connection for its whole run is not bounded by a request. */
BF_STORE_SHARED int bf_store_leases_serving;
BF_STORE_SHARED volatile sig_atomic_t bf_store_deadline_armed;

/* The deadline that does not depend on SQLite running. A request blocked
 * outside SQLite (a child process, a socket, a lock) while it holds a
 * statement keeps its snapshot just as surely as a leaked one, and no
 * progress handler fires. When the alarm rings with a lease still open the
 * worker leaves; its descriptors close, the snapshot ends, and the serving
 * supervisor starts a fresh pool. */
static inline void bf_store_deadline_ring(int signo) {
    static const char said[] = "store: a request held a store past its declared read_txn_max_ms; worker exits\n";
    (void)signo;
    for (int i = 0; i < BF_STORE_LEASES_MAX; i++)
        if (bf_store_leases[i].db) { (void)!write(STDERR_FILENO, said, sizeof(said) - 1); _exit(75); }
}

static inline void bf_store_leases_serve(void) {
    struct sigaction ring;
    memset(&ring, 0, sizeof(ring));
    ring.sa_handler = bf_store_deadline_ring;
    ring.sa_flags = SA_RESTART;
    sigemptyset(&ring.sa_mask);
    (void)sigaction(SIGALRM, &ring, NULL);
    bf_store_leases_serving = 1;
}

static inline long long bf_store_elapsed_ms(const struct timespec *from) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (long long)(now.tv_sec - from->tv_sec) * 1000 + (now.tv_nsec - from->tv_nsec) / 1000000;
}

static inline int bf_store_deadline(void *arg) {
    BfStoreLease *lease = (BfStoreLease *)arg;
    if (!lease->db || !lease->policy || lease->policy->read_txn_max_ms <= 0) return 0;
    if (bf_store_elapsed_ms(&lease->opened) <= lease->policy->read_txn_max_ms) return 0;
    bf_store_durability_counters.interrupted++;
    return 1;
}

/* sqlite3_close() announces itself before it checks for open statements.
 * With none left the close succeeds, so the lease ends here; with some left
 * plain sqlite3_close() refuses and the connection stays leased. */
static inline int bf_store_lease_trace(unsigned type, void *arg, void *p, void *x) {
    BfStoreLease *lease = (BfStoreLease *)arg;
    (void)x;
    if (type == SQLITE_TRACE_CLOSE && lease->db == (sqlite3 *)p && !sqlite3_next_stmt(lease->db, NULL))
        memset(lease, 0, sizeof(*lease));
    return 0;
}

/* Lease db to the current request. With no free slot the connection is still
 * usable, only unbounded; returns -1 so a witness can see it. */
/* caller is the code address that asked for the connection (atos or
 * addr2line names it), so a leak report points at the owner that leaked. */
static inline int bf_store_lease_site(sqlite3 *db, const BfStoreDurability *policy, int readonly, const void *caller, const char *site) {
    if (!db || !policy) return -1;
    if (policy->busy_timeout_ms > 0) sqlite3_busy_timeout(db, policy->busy_timeout_ms);
    if (!bf_store_leases_serving) return 0;
    for (int i = 0; i < BF_STORE_LEASES_MAX; i++) {
        BfStoreLease *lease = &bf_store_leases[i];
        if (lease->db) continue;
        lease->db = db; lease->policy = policy; lease->caller = caller; lease->site = site;
        clock_gettime(CLOCK_MONOTONIC, &lease->opened);
        sqlite3_trace_v2(db, SQLITE_TRACE_CLOSE, bf_store_lease_trace, lease);
        if (!bf_store_deadline_armed && policy->read_txn_max_ms > 0) {
            alarm((unsigned)((policy->read_txn_max_ms + 999) / 1000));
            bf_store_deadline_armed = 1;
        }
        if (readonly && policy->read_txn_max_ms > 0) sqlite3_progress_handler(db, 1000, bf_store_deadline, lease);
        return 0;
    }
    return -1;
}
#define bf_store_lease_at(db, policy, readonly, caller) bf_store_lease_site((db), (policy), (readonly), (caller), NULL)
#define bf_store_lease(db, policy, readonly) bf_store_lease_at((db), (policy), (readonly), __builtin_return_address(0))

/* ---- opening a declared store --------------------------------------------
 * The one way an owner that serves requests opens a store file: the call
 * sqlite3_open_v2 would be, plus the store's declared policy. The connection
 * gets the declared busy timeout and is leased to the current request (a
 * read-only one also gets the read deadline). A read-write open of a store
 * that declares durability (journal_mode set) also applies the writer policy;
 * a store declared with lease only names no journal policy and gets none.
 * Returns the SQLite result; on failure *out is NULL and nothing is left open.
 * site names the call for the leak report. */
static inline int bf_store_open_site(const char *path, sqlite3 **out, int flags, const BfStoreDurability *policy, const char *site) {
    sqlite3 *db = NULL;
    int rc;
    *out = NULL;
    if (!path || !*path || !policy) return SQLITE_CANTOPEN;
    rc = sqlite3_open_v2(path, &db, flags, NULL);
    if (rc != SQLITE_OK) { if (db) sqlite3_close(db); return rc; }
    if ((flags & SQLITE_OPEN_READWRITE) && policy->journal_mode && bf_store_writer(db, policy)) {
        sqlite3_close(db);
        return SQLITE_CANTOPEN;
    }
    (void)bf_store_lease_site(db, policy, !(flags & SQLITE_OPEN_READWRITE), NULL, site);
    *out = db;
    return SQLITE_OK;
}
#define BF_STORE_SITE_STR2(x) #x
#define BF_STORE_SITE_STR(x) BF_STORE_SITE_STR2(x)
#define bf_store_open(path, out, flags, policy) bf_store_open_site((path), (out), (flags), (policy), __FILE__ ":" BF_STORE_SITE_STR(__LINE__))

/* bf_sqlite_open_readonly_path() with the store's lease: read-only, the
 * caller's busy timeout in place of the declared one, NULL on failure. */
#define bf_store_open_ro(path, busy_ms, policy) bf_store_open_ro_site((path), (busy_ms), (policy), __FILE__ ":" BF_STORE_SITE_STR(__LINE__))
static inline sqlite3 *bf_store_open_ro_site(const char *path, int busy_ms, const BfStoreDurability *policy, const char *site) {
    sqlite3 *db = NULL;
    if (bf_store_open_site(path, &db, SQLITE_OPEN_READONLY, policy, site) != SQLITE_OK) return NULL;
    if (busy_ms > 0) sqlite3_busy_timeout(db, busy_ms);
    return db;
}

/* The same open for a connection the process keeps for its whole life (a
 * read-only pack consulted by every request): the declared busy timeout,
 * never a lease, because the end of a request must not close it. */
static inline int bf_store_open_held(const char *path, sqlite3 **out, int flags, const BfStoreDurability *policy) {
    sqlite3 *db = NULL;
    int rc;
    *out = NULL;
    if (!path || !*path || !policy) return SQLITE_CANTOPEN;
    rc = sqlite3_open_v2(path, &db, flags, NULL);
    if (rc != SQLITE_OK) { if (db) sqlite3_close(db); return rc; }
    if (policy->busy_timeout_ms > 0) sqlite3_busy_timeout(db, policy->busy_timeout_ms);
    *out = db;
    return SQLITE_OK;
}

static inline int bf_store_open_statements(sqlite3 *db) {
    int n = 0;
    for (sqlite3_stmt *st = sqlite3_next_stmt(db, NULL); st; st = sqlite3_next_stmt(db, st)) n++;
    return n;
}

/* End of request: whatever is still leased was left open by the request.
 * Finalizing its statements ends the read transaction that pinned the WAL;
 * closing it returns the descriptor. Returns how many were reaped. */
static inline int bf_store_leases_end(void) {
    int reaped = 0;
    if (bf_store_deadline_armed) { alarm(0); bf_store_deadline_armed = 0; }
    for (int i = 0; i < BF_STORE_LEASES_MAX; i++) {
        BfStoreLease *lease = &bf_store_leases[i];
        sqlite3 *db = lease->db;
        sqlite3_stmt *st;
        if (!db) continue;
        if (lease->site)
            fprintf(stderr, "store %s: a connection opened at %s outlived its request (%d statements open); closed\n",
                    lease->policy && lease->policy->store ? lease->policy->store : "?", lease->site,
                    bf_store_open_statements(db));
        else
            fprintf(stderr, "store %s: a connection opened from %p outlived its request (%d statements open); closed\n",
                    lease->policy && lease->policy->store ? lease->policy->store : "?", lease->caller,
                    bf_store_open_statements(db));
        while ((st = sqlite3_next_stmt(db, NULL)) != NULL) sqlite3_finalize(st);
        if (!sqlite3_get_autocommit(db)) (void)sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
        memset(lease, 0, sizeof(*lease));
        sqlite3_trace_v2(db, 0, NULL, NULL);
        sqlite3_close(db);
        bf_store_durability_counters.leases_reaped++;
        reaped++;
    }
    return reaped;
}

#endif
