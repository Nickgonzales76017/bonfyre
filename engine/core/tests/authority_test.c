/* Native conformance for authority grants: shared authority_edges store, and the
 * "never inferred" law -- authority holds only from a matching non-revoked edge
 * inside its time window, with a purpose that is general or matches. Prints DB for
 * a Python has_authority parity read. */

#include "bf_authority.h"

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
        int fd = mkstemp(default_path);
        if (fd >= 0) close(fd);
        unlink(default_path);
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
    if (bf_authority_grant(auth, &e, NULL) != 0) return fail("grant e1");
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

    bf_authority_close(auth);
    printf("DB=%s\n", db_path);
    printf("authority_test: PASS\n");
    return 0;
}
