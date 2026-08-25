/* Native conformance for the corroboration ledger: shared actor_corroborations
 * store, independence by distinct source, no double-count on a repeated source.
 * The db path is printed so a companion harness can reopen it from Python and
 * confirm the shared store. */

#include "bf_corroboration.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

int main(int argc, char **argv) {
    char default_path[] = "/tmp/bf_corr_XXXXXX";
    const char *db_path = default_path;
    if (argc > 1) {
        db_path = argv[1];
    } else {
        int fd = mkstemp(default_path);
        if (fd >= 0) close(fd);
        unlink(default_path);
    }

    /* actor_corroborations references actor_nodes; create the referenced actor so
     * the foreign key holds (Python shares this same store). */
    sqlite3 *raw = NULL;
    if (sqlite3_open(db_path, &raw) != SQLITE_OK) {
        return fail("open raw");
    }
    sqlite3_exec(raw,
                 "CREATE TABLE IF NOT EXISTS actor_nodes(actor_id TEXT PRIMARY KEY,"
                 " node_kind TEXT, display_name TEXT, role TEXT, org_id TEXT,"
                 " confidence TEXT, provenance TEXT, detail TEXT, created_at TEXT);"
                 "INSERT OR IGNORE INTO actor_nodes(actor_id,node_kind,display_name,"
                 "confidence,provenance,created_at) VALUES('org:hub','organization',"
                 "'Hub','asserted','t','2026-01-01T00:00:00+00:00');",
                 NULL, NULL, NULL);
    sqlite3_close(raw);

    BfCorroboration *ledger = NULL;
    if (bf_corroboration_open(db_path, &ledger, stderr) != 0) {
        return fail("open ledger");
    }

    /* first source: new */
    if (bf_corroboration_record(ledger, "org:hub", "registry", "edge:funds", "", NULL) != 1) {
        return fail("first source should be new");
    }
    /* same source again: not new, count unchanged (no self-bootstrap) */
    if (bf_corroboration_record(ledger, "org:hub", "registry", "edge:funds2", "", NULL) != 0) {
        return fail("repeat source should not be new");
    }
    if (bf_corroboration_independent_sources(ledger, "org:hub") != 1) {
        return fail("count should stay 1 after repeat");
    }
    /* a second distinct source: new, count 2 */
    if (bf_corroboration_record(ledger, "org:hub", "public-web", "url", "", NULL) != 1) {
        return fail("second distinct source should be new");
    }
    if (bf_corroboration_independent_sources(ledger, "org:hub") != 2) {
        return fail("count should be 2");
    }
    /* an actor with no corroborations */
    if (bf_corroboration_independent_sources(ledger, "org:nobody") != 0) {
        return fail("unknown actor should have 0");
    }
    /* invalid inputs */
    if (bf_corroboration_record(ledger, "", "s", NULL, NULL, NULL) != -1) {
        return fail("empty actor should be rejected");
    }

    bf_corroboration_close(ledger);
    printf("DB=%s\n", db_path);
    printf("corroboration_test: PASS\n");
    return 0;
}
