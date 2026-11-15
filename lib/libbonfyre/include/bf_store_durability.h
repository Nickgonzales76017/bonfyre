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

/* Observed by witnesses; never consulted for authority. */
typedef struct {
    long long passive, truncated, truncate_busy, leases_reaped, interrupted;
} BfStoreDurabilityCounters;
static BfStoreDurabilityCounters bf_store_durability_counters;

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
static inline int bf_store_attach(sqlite3 *db, const char *path, const char *attach_sql, int readonly) {
    sqlite3_stmt *st = NULL;
    char *uri = readonly ? sqlite3_mprintf("file:%s?mode=ro", path) : sqlite3_mprintf("%s", path);
    int ok = uri && sqlite3_prepare_v2(db, attach_sql, -1, &st, NULL) == SQLITE_OK &&
             sqlite3_bind_text(st, 1, uri, -1, SQLITE_TRANSIENT) == SQLITE_OK && sqlite3_step(st) == SQLITE_DONE;
    sqlite3_finalize(st);
    sqlite3_free(uri);
    return ok;
}

/* ---- request leases ------------------------------------------------------
 * A process that serves requests in a loop owns every store connection a
 * request opened only until the request ends. */
#ifndef BF_STORE_LEASES_MAX
#define BF_STORE_LEASES_MAX 64
#endif
typedef struct { sqlite3 *db; const BfStoreDurability *policy; struct timespec opened; const void *caller; } BfStoreLease;
static BfStoreLease bf_store_leases[BF_STORE_LEASES_MAX];
/* Leases exist only in a process that serves requests; a batch command that
 * holds one connection for its whole run is not bounded by a request. */
static int bf_store_leases_serving;
static volatile sig_atomic_t bf_store_deadline_armed;

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
static inline int bf_store_lease_at(sqlite3 *db, const BfStoreDurability *policy, int readonly, const void *caller) {
    if (!db || !policy) return -1;
    if (policy->busy_timeout_ms > 0) sqlite3_busy_timeout(db, policy->busy_timeout_ms);
    if (!bf_store_leases_serving) return 0;
    for (int i = 0; i < BF_STORE_LEASES_MAX; i++) {
        BfStoreLease *lease = &bf_store_leases[i];
        if (lease->db) continue;
        lease->db = db; lease->policy = policy; lease->caller = caller;
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
#define bf_store_lease(db, policy, readonly) bf_store_lease_at((db), (policy), (readonly), __builtin_return_address(0))

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
