/* Witness: the CMS store's declared durability keeps its WAL bounded.
 *
 * The CMS WAL once reached 13 GB beside an 837 MB store. A passive checkpoint
 * only copies frames back up to the oldest open snapshot, and the WAL restarts
 * (and journal_size_limit trims it) only when no reader holds an older one.
 * This test builds a store under the policy compiled from
 * schemas/storage/stores (BF_STORE_CMS_DB_DURABILITY) and shows:
 *
 *   1. starvation: a reader process that always has a read transaction open
 *      (two connections taking turns, each overlapping the next) keeps a
 *      passive-only writer's WAL growing past the declared bound;
 *   2. the declared writer policy (a TRUNCATE once the WAL passes
 *      wal_truncate_pages) keeps the same write loop under the bound with the
 *      same reader running;
 *   3. a leaked reader (a statement stepped and never reset, a close that
 *      failed) defeats any writer policy; ending the request's leases releases
 *      it and the next commit truncates the WAL;
 *   4. a request that holds the store past read_txn_max_ms while blocked
 *      outside SQLite is ended by the deadline alarm (exit 75).
 */
#include "../../../lib/libbonfyre/include/bf_store_durability.h"
#include "../generated/store_schema.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static const BfStoreDurability declared = BF_STORE_CMS_DB_DURABILITY;
static char db_path[512], wal_path[600];
static int failures;

#define CHECK(cond, ...) do { if (!(cond)) { failures++; fprintf(stderr, "FAIL: " __VA_ARGS__); fputc('\n', stderr); } } while (0)

static long long wal_bytes(void) {
    struct stat st;
    return stat(wal_path, &st) ? 0 : (long long)st.st_size;
}

static sqlite3 *open_rw(void) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
        fprintf(stderr, "open %s: %s\n", db_path, sqlite3_errmsg(db));
        exit(2);
    }
    return db;
}

/* A reader that is never idle: connection a opens a transaction, then b,
 * then a ends and reopens, then b ... so some snapshot is always older than
 * the newest commit. Each transaction lasts about a millisecond. */
static pid_t start_overlapping_reader(void) {
    pid_t pid = fork();
    if (pid) return pid;
    sqlite3 *r[2] = {NULL, NULL};
    for (int i = 0; i < 2; i++) {
        if (sqlite3_open_v2(db_path, &r[i], SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) _exit(3);
        sqlite3_busy_timeout(r[i], 5000);
    }
    sqlite3_exec(r[0], "BEGIN; SELECT count(*) FROM page;", NULL, NULL, NULL);
    for (int turn = 1;; turn++) {
        sqlite3 *next = r[turn & 1], *prev = r[(turn + 1) & 1];
        sqlite3_exec(next, "BEGIN; SELECT count(*) FROM page;", NULL, NULL, NULL);
        usleep(1000);
        sqlite3_exec(prev, "COMMIT;", NULL, NULL, NULL);
    }
}

static void stop(pid_t pid) {
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
}

/* commits rows of `bytes` each; returns the largest WAL seen. */
static long long write_loop(sqlite3 *w, int commits, int bytes) {
    sqlite3_stmt *ins = NULL;
    long long high = 0;
    char *blob = malloc((size_t)bytes);
    memset(blob, 'x', (size_t)bytes);
    sqlite3_prepare_v2(w, "INSERT INTO page(body) VALUES(?1)", -1, &ins, NULL);
    for (int i = 0; i < commits; i++) {
        blob[i % bytes] ^= 1;
        sqlite3_bind_blob(ins, 1, blob, bytes, SQLITE_STATIC);
        if (sqlite3_step(ins) != SQLITE_DONE) { fprintf(stderr, "insert: %s\n", sqlite3_errmsg(w)); exit(2); }
        sqlite3_reset(ins);
        if (i % 64 == 0) sqlite3_exec(w, "DELETE FROM page WHERE id % 3 = 0;", NULL, NULL, NULL);
        long long now = wal_bytes();
        if (now > high) high = now;
    }
    sqlite3_finalize(ins);
    free(blob);
    return high;
}

static void fresh_store(void) {
    unlink(db_path); unlink(wal_path);
    char shm[620]; snprintf(shm, sizeof(shm), "%s-shm", db_path); unlink(shm);
    sqlite3 *w = open_rw();
    sqlite3_exec(w, "PRAGMA journal_mode=WAL; CREATE TABLE page(id INTEGER PRIMARY KEY, body BLOB);", NULL, NULL, NULL);
    sqlite3_close(w);
}

int main(void) {
    setvbuf(stdout, NULL, _IONBF, 0);
    const char *tmp = getenv("TMPDIR");
    snprintf(db_path, sizeof(db_path), "%s/cms-durability-%d.db", tmp && *tmp ? tmp : "/tmp", (int)getpid());
    snprintf(wal_path, sizeof(wal_path), "%s-wal", db_path);
    const int page = 4096, row = 64 * 1024, commits = 6000;    /* ~390 MB through the WAL */
    /* One commit can carry the WAL past the threshold before the hook runs. */
    const long long bound = (long long)(declared.wal_truncate_pages + 2 * (row / page + 4)) * (page + 24) + 32;

    CHECK(!strcmp(BF_STORE_CMS_DB_KIND, "sqlite"), "the CMS store is declared sqlite");
    CHECK(!strcmp(declared.journal_mode, "WAL") && declared.wal_truncate_pages > declared.wal_autocheckpoint,
          "declared policy is WAL with a truncate threshold above the autocheckpoint");

    /* 1. Starvation: the same pragmas, passive checkpoints only. */
    fresh_store();
    BfStoreDurability passive_only = declared;
    passive_only.wal_truncate_pages = 0;
    pid_t reader = start_overlapping_reader();
    usleep(50000);
    sqlite3 *w = open_rw();
    CHECK(!bf_store_writer(w, &passive_only), "passive-only writer policy applies");
    long long starved = write_loop(w, commits, row);
    sqlite3_close(w);
    stop(reader);
    printf("passive only, reader always open: WAL high water %lld bytes (bound %lld)\n", starved, bound);
    CHECK(starved > 2 * bound, "a reader that is never idle starves passive checkpoints (%lld <= %lld)", starved, 2 * bound);

    /* 2. Declared policy, same reader. */
    fresh_store();
    memset(&bf_store_durability_counters, 0, sizeof(bf_store_durability_counters));
    reader = start_overlapping_reader();
    usleep(50000);
    w = open_rw();
    CHECK(!bf_store_writer(w, &declared), "declared writer policy applies");
    long long held = write_loop(w, commits, row);
    sqlite3_close(w);
    stop(reader);
    printf("declared policy, reader always open: WAL high water %lld bytes (bound %lld), %lld truncations, %lld refused\n",
           held, bound, bf_store_durability_counters.truncated, bf_store_durability_counters.truncate_busy);
    CHECK(held <= bound, "declared policy keeps the WAL bounded (%lld > %lld)", held, bound);
    CHECK(bf_store_durability_counters.truncated > 0, "the writer truncated the WAL");

    /* 3. A leaked reader statement, then the end of its request. */
    fresh_store();
    memset(&bf_store_durability_counters, 0, sizeof(bf_store_durability_counters));
    bf_store_leases_serve();
    w = open_rw();
    bf_store_writer(w, &declared);
    write_loop(w, 16, row);
    sqlite3 *leak = NULL; sqlite3_stmt *forgotten = NULL;
    sqlite3_open_v2(db_path, &leak, SQLITE_OPEN_READONLY, NULL);
    CHECK(!bf_store_lease(leak, &declared, 1), "reader leased");
    sqlite3_prepare_v2(leak, "SELECT id FROM page", -1, &forgotten, NULL);
    CHECK(sqlite3_step(forgotten) == SQLITE_ROW, "leaked statement holds a snapshot");
    CHECK(sqlite3_close(leak) == SQLITE_BUSY, "plain close refuses a connection with a statement open");
    long long pinned = write_loop(w, commits / 2, row);
    printf("leaked reader: WAL high water %lld bytes, %lld truncations refused\n",
           pinned, bf_store_durability_counters.truncate_busy);
    CHECK(pinned > bound && bf_store_durability_counters.truncate_busy > 0, "a leaked snapshot pins the WAL past any writer policy");
    CHECK(bf_store_leases_end() == 1, "end of request reaps the leaked connection");
    write_loop(w, 2, row);    /* the WAL is already past the threshold: the first commit truncates */
    long long after = wal_bytes();
    printf("after the request ended: WAL %lld bytes\n", after);
    CHECK(after <= bound, "once released, the next commits truncate the WAL (%lld)", after);
    /* A connection closed normally ends its own lease. */
    sqlite3 *tidy = NULL;
    sqlite3_open_v2(db_path, &tidy, SQLITE_OPEN_READONLY, NULL);
    bf_store_lease(tidy, &declared, 1);
    sqlite3_close(tidy);
    CHECK(bf_store_leases_end() == 0, "a closed connection leaves no lease");
    sqlite3_close(w);

    /* 4. Blocked outside SQLite past the deadline. */
    pid_t child = fork();
    if (!child) {
        BfStoreDurability brief = declared;
        brief.read_txn_max_ms = 1000;
        sqlite3 *r = NULL; sqlite3_stmt *st = NULL;
        bf_store_leases_serve();
        sqlite3_open_v2(db_path, &r, SQLITE_OPEN_READONLY, NULL);
        bf_store_lease(r, &brief, 1);
        sqlite3_prepare_v2(r, "SELECT id FROM page", -1, &st, NULL);
        sqlite3_step(st);
        sleep(10);    /* a request stuck on something that is not SQLite */
        _exit(0);
    }
    int status = 0;
    waitpid(child, &status, 0);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 75, "the deadline ends a request that holds the store too long (status %d)", status);

    char shm[620]; snprintf(shm, sizeof(shm), "%s-shm", db_path);
    unlink(db_path); unlink(wal_path); unlink(shm);
    if (failures) { fprintf(stderr, "store durability: %d failed\n", failures); return 1; }
    puts("store durability: ok");
    return 0;
}
