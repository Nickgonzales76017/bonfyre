/* Native conformance for authority grants: shared authority_edges store, and the
 * "never inferred" law -- authority holds only from a matching non-revoked edge
 * inside its time window, with a purpose that is general or matches. Prints DB for
 * a Python has_authority parity read. */

#include "bf_authority.h"

#include <sqlite3.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int fail(const char *msg) {
    fprintf(stderr, "FAIL: %s\n", msg);
    return 1;
}

static BfAuthorityEdge edge(const char *id, const char *actor, const char *perm,
                            const char *subj, const char *purpose,
                            const char *eff, const char *exp) {
    BfAuthorityEdge e;
    memset(&e, 0, sizeof(e));
    e.edge_id = id;
    e.actor = actor;
    e.permission = perm;
    e.subject = subj;
    e.purpose = purpose;
    e.effective_from = eff;
    e.expires_at = exp;
    return e;
}

int main(int argc, char **argv) {
    char default_path[] = "/tmp/bf_auth_XXXXXX";
    const char *db_path = default_path;
    if (argc > 1) {
        db_path = argv[1];
    } else {
        sqlite3 *legacy = NULL;
        char *message = NULL;
        int fd = mkstemp(default_path);
        if (fd >= 0) close(fd);
        unlink(default_path);

        /* Exercise the real absorption boundary: the historical shared store
         * already had authority_edges before the native owner opened it.  This
         * legacy shape intentionally lacks created_at so apply_schema must
         * migrate it without replacing the table. */
        if (sqlite3_open(db_path, &legacy) != SQLITE_OK) return fail("legacy open");
        if (sqlite3_exec(
                legacy,
                "CREATE TABLE authority_edges("
                "edge_id TEXT PRIMARY KEY,actor TEXT NOT NULL,permission TEXT NOT NULL,"
                "subject TEXT NOT NULL,purpose TEXT NOT NULL DEFAULT '',"
                "scope TEXT NOT NULL DEFAULT '',effective_from TEXT,expires_at TEXT,"
                "delegable INTEGER NOT NULL DEFAULT 0,revoked INTEGER NOT NULL DEFAULT 0,"
                "evidence TEXT NOT NULL DEFAULT '',originating_authority TEXT NOT NULL DEFAULT '')",
                NULL, NULL, &message) != SQLITE_OK) {
            fprintf(stderr, "legacy schema: %s\n", message ? message : "?");
            sqlite3_free(message);
            sqlite3_close(legacy);
            return fail("legacy schema");
        }
        sqlite3_close(legacy);
    }

    BfAuthority *auth = NULL;
    if (bf_authority_open(db_path, &auth, stderr) != 0) {
        return fail("open");
    }

    /* no edge -> no authority (never inferred) */
    if (bf_authority_has(auth, "person:a", "activate", "res:1",
                         "2026-06-01T00:00:00+00:00", NULL) != 0) {
        return fail("no edge -> no authority");
    }

    /* a plain grant with no window -> authority holds any time */
    BfAuthorityEdge e = edge("e1", "person:a", "activate", "res:1", "", NULL, NULL);
    if (bf_authority_grant(auth, &e, "2026-05-31T12:34:56+00:00") != 0) return fail("grant e1");

    /* A native grant must be writable against the historical shared schema,
     * and the absorption migration must preserve a creation receipt. */
    {
        sqlite3 *check = NULL;
        sqlite3_stmt *stmt = NULL;
        const char *created = NULL;
        if (sqlite3_open(db_path, &check) != SQLITE_OK) return fail("created_at open");
        if (sqlite3_prepare_v2(
                check, "SELECT created_at FROM authority_edges WHERE edge_id='e1'",
                -1, &stmt, NULL) != SQLITE_OK) {
            sqlite3_close(check);
            return fail("created_at query");
        }
        if (sqlite3_step(stmt) != SQLITE_ROW) {
            sqlite3_finalize(stmt); sqlite3_close(check);
            return fail("created_at missing row");
        }
        created = (const char *)sqlite3_column_text(stmt, 0);
        if (!created || strcmp(created, "2026-05-31T12:34:56+00:00") != 0) {
            sqlite3_finalize(stmt); sqlite3_close(check);
            return fail("created_at not retained");
        }
        sqlite3_finalize(stmt);
        sqlite3_close(check);
    }
    if (bf_authority_has(auth, "person:a", "activate", "res:1",
                         "2026-06-01T00:00:00+00:00", NULL) != 1) {
        return fail("plain grant should hold");
    }
    /* different subject / permission / actor -> no authority */
    if (bf_authority_has(auth, "person:a", "activate", "res:2",
                         "2026-06-01T00:00:00+00:00", NULL) != 0) {
        return fail("wrong subject -> no authority");
    }
    if (bf_authority_has(auth, "person:b", "activate", "res:1",
                         "2026-06-01T00:00:00+00:00", NULL) != 0) {
        return fail("wrong actor -> no authority");
    }

    /* a windowed grant: only inside [2026-01-01, 2026-12-31] */
    BfAuthorityEdge w = edge("e2", "person:c", "decide", "res:3", "",
                             "2026-01-01T00:00:00+00:00", "2026-12-31T23:59:59+00:00");
    bf_authority_grant(auth, &w, NULL);
    if (bf_authority_has(auth, "person:c", "decide", "res:3",
                         "2025-06-01T00:00:00+00:00", NULL) != 0) {
        return fail("before window -> no authority");
    }
    if (bf_authority_has(auth, "person:c", "decide", "res:3",
                         "2026-06-01T00:00:00+00:00", NULL) != 1) {
        return fail("inside window -> authority");
    }
    if (bf_authority_has(auth, "person:c", "decide", "res:3",
                         "2027-06-01T00:00:00+00:00", NULL) != 0) {
        return fail("after window -> no authority");
    }

    /* a purpose-scoped grant matches only that purpose */
    BfAuthorityEdge p = edge("e3", "person:d", "spend", "res:4", "grant-2027", NULL, NULL);
    bf_authority_grant(auth, &p, NULL);
    if (bf_authority_has(auth, "person:d", "spend", "res:4",
                         "2026-06-01T00:00:00+00:00", "grant-2027") != 1) {
        return fail("matching purpose -> authority");
    }
    if (bf_authority_has(auth, "person:d", "spend", "res:4",
                         "2026-06-01T00:00:00+00:00", "other") != 0) {
        return fail("mismatched purpose -> no authority");
    }

    /* revocation removes authority */
    if (bf_authority_revoke(auth, "e1") != 1) return fail("revoke e1");
    if (bf_authority_has(auth, "person:a", "activate", "res:1",
                         "2026-06-01T00:00:00+00:00", NULL) != 0) {
        return fail("revoked edge -> no authority");
    }

    /* Delegation is a narrower derived edge, never a flat-copy escape hatch. */
    BfAuthorityEdge parent = edge("parent", "principal", "approve", "res:delegated",
                                  "grant-2027", "2026-01-01T00:00:00+00:00",
                                  "2026-12-31T00:00:00+00:00");
    parent.delegable = 1;
    if (bf_authority_grant(auth, &parent, NULL) != 0) return fail("grant delegable parent");
    if (bf_authority_delegate(auth, "parent", "child", "delegate", "grant-2027",
                              "2026-06-30T00:00:00+00:00") != 0) return fail("delegate child");
    if (bf_authority_has(auth, "delegate", "approve", "res:delegated",
                         "2026-06-01T00:00:00+00:00", "grant-2027") != 1) return fail("child holds narrow grant");
    if (bf_authority_delegate(auth, "parent", "wide-purpose", "delegate2", "other", NULL) == 0) return fail("purpose widening refused");
    if (bf_authority_delegate(auth, "parent", "wide-time", "delegate2", "grant-2027",
                              "2027-01-01T00:00:00+00:00") == 0) return fail("expiry widening refused");
    if (bf_authority_revoke_cascade(auth, "parent") != 2) return fail("cascade closes parent and child");
    if (bf_authority_has(auth, "delegate", "approve", "res:delegated",
                         "2026-06-01T00:00:00+00:00", "grant-2027") != 0) return fail("cascade closes child authority");

    bf_authority_close(auth);
    printf("DB=%s\n", db_path);
    printf("authority_test: PASS\n");
    return 0;
}
