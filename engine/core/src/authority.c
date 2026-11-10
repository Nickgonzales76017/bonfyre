/*
 * Native authority grants. Shares the authority_edges store with authority.py;
 * has_authority reproduces the exact non-revoked + window + purpose match.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "bf_authority.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Darwin exposes timegm outside its strict C/POSIX declarations. */
#if defined(__APPLE__)
extern time_t timegm(struct tm *);
#endif

struct BfAuthority {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

/* Must match authority.py SCHEMA (authority_edges) exactly. */
static const char *AUTHORITY_SCHEMA =
    "CREATE TABLE IF NOT EXISTS authority_edges("
    "  edge_id TEXT PRIMARY KEY,"
    "  actor TEXT NOT NULL,"
    "  permission TEXT NOT NULL,"
    "  subject TEXT NOT NULL,"
    "  purpose TEXT NOT NULL DEFAULT '',"
    "  scope TEXT NOT NULL DEFAULT '',"
    "  effective_from TEXT,"
    "  expires_at TEXT,"
    "  delegable INTEGER NOT NULL DEFAULT 0,"
    "  revoked INTEGER NOT NULL DEFAULT 0,"
    "  evidence TEXT NOT NULL DEFAULT '',"
    "  originating_authority TEXT NOT NULL DEFAULT '',"
    "  created_at TEXT NOT NULL"
    ");";

static void now_iso(char out[40]) {
    time_t t = time(NULL);
    struct tm utc;
#if defined(_WIN32)
    gmtime_s(&utc, &t);
#else
    gmtime_r(&t, &utc);
#endif
    if (strftime(out, 40, "%Y-%m-%dT%H:%M:%S+00:00", &utc) == 0) {
        snprintf(out, 40, "1970-01-01T00:00:00+00:00");
    }
}

/* Parse an ISO8601 moment (date or datetime, optional zone) to epoch seconds.
 * Returns 0 on success (out set), -1 if the string is empty/unparseable. */
static int parse_iso(const char *iso, int64_t *out) {
    if (!iso || iso[0] == '\0') {
        return -1;
    }
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    int y, mo, d, h = 0, mi = 0, s = 0;
    int n = sscanf(iso, "%d-%d-%dT%d:%d:%d", &y, &mo, &d, &h, &mi, &s);
    if (n < 3) {
        n = sscanf(iso, "%d-%d-%d", &y, &mo, &d);
        if (n < 3) {
            return -1;
        }
    }
    tm.tm_year = y - 1900;
    tm.tm_mon = mo - 1;
    tm.tm_mday = d;
    tm.tm_hour = h;
    tm.tm_min = mi;
    tm.tm_sec = s;
    *out = (int64_t)timegm(&tm);
    return 0;
}

static int apply_schema(sqlite3 *db, FILE *err) {
    char *errmsg = NULL;
    if (sqlite3_exec(db, AUTHORITY_SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (err) {
            fprintf(err, "authority: schema failed: %s\n", errmsg ? errmsg : "?");
        }
        sqlite3_free(errmsg);
        return -1;
    }

    /* The shared ControlPlane authority store predates the native owner and
     * already carries created_at.  Some native-only stores were created during
     * the absorption window without it.  Migrate those stores additively so
     * one AuthorityGraph schema is readable/writable from both lineages. */
    {
        sqlite3_stmt *columns = NULL;
        int found = 0;
        if (sqlite3_prepare_v2(db, "PRAGMA table_info(authority_edges)", -1,
                               &columns, NULL) != SQLITE_OK) {
            return -1;
        }
        while (sqlite3_step(columns) == SQLITE_ROW) {
            const char *name = (const char *)sqlite3_column_text(columns, 1);
            if (name && strcmp(name, "created_at") == 0) {
                found = 1;
                break;
            }
        }
        sqlite3_finalize(columns);
        if (!found && sqlite3_exec(
                db,
                "ALTER TABLE authority_edges "
                "ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
                NULL, NULL, &errmsg) != SQLITE_OK) {
            if (err) {
                fprintf(err, "authority: created_at migration failed: %s\n",
                        errmsg ? errmsg : "?");
            }
            sqlite3_free(errmsg);
            return -1;
        }
    }
    return 0;
}

int bf_authority_open(const char *database_path, BfAuthority **out, FILE *err) {
    if (!database_path || !out) {
        return -1;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) {
            fprintf(err, "authority: cannot open %s: %s\n", database_path,
                    db ? sqlite3_errmsg(db) : "?");
        }
        sqlite3_close(db);
        return -1;
    }
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (apply_schema(db, err) != 0) {
        sqlite3_close(db);
        return -1;
    }
    BfAuthority *auth = calloc(1, sizeof(*auth));
    if (!auth) {
        sqlite3_close(db);
        return -1;
    }
    auth->db = db;
    auth->owns_db = 1;
    auth->err = err;
    *out = auth;
    return 0;
}

int bf_authority_open_database(void *sqlite_database, BfAuthority **out, FILE *err) {
    if (!sqlite_database || !out) {
        return -1;
    }
    sqlite3 *db = (sqlite3 *)sqlite_database;
    if (apply_schema(db, err) != 0) {
        return -1;
    }
    BfAuthority *auth = calloc(1, sizeof(*auth));
    if (!auth) {
        return -1;
    }
    auth->db = db;
    auth->owns_db = 0;
    auth->err = err;
    *out = auth;
    return 0;
}

void bf_authority_close(BfAuthority *auth) {
    if (!auth) {
        return;
    }
    if (auth->owns_db && auth->db) {
        sqlite3_close(auth->db);
    }
    free(auth);
}

static void bind_opt(sqlite3_stmt *s, int i, const char *v) {
    if (v) {
        sqlite3_bind_text(s, i, v, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(s, i);
    }
}

int bf_authority_grant(BfAuthority *auth, const BfAuthorityEdge *e, const char *now) {
    if (!auth || !e || !e->edge_id || e->edge_id[0] == '\0' ||
        !e->actor || e->actor[0] == '\0' || !e->permission || e->permission[0] == '\0' ||
        !e->subject || e->subject[0] == '\0') {
        return -1;
    }
    char created_at[40];
    if (!now || !now[0]) {
        now_iso(created_at);
        now = created_at;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            auth->db,
            "INSERT INTO authority_edges"
            "(edge_id,actor,permission,subject,purpose,scope,effective_from,expires_at,"
            " delegable,revoked,evidence,originating_authority,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,0,?,?,?)"
            " ON CONFLICT(edge_id) DO UPDATE SET actor=excluded.actor,"
            "  permission=excluded.permission, subject=excluded.subject,"
            "  purpose=excluded.purpose, scope=excluded.scope,"
            "  effective_from=excluded.effective_from, expires_at=excluded.expires_at,"
            "  delegable=excluded.delegable, evidence=excluded.evidence,"
            "  originating_authority=excluded.originating_authority",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, e->edge_id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, e->actor, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, e->permission, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 4, e->subject, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 5, e->purpose ? e->purpose : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, e->scope ? e->scope : "", -1, SQLITE_TRANSIENT);
    bind_opt(stmt, 7, e->effective_from);
    bind_opt(stmt, 8, e->expires_at);
    sqlite3_bind_int(stmt, 9, e->delegable ? 1 : 0);
    sqlite3_bind_text(stmt, 10, e->evidence ? e->evidence : "", -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 11, e->originating_authority ? e->originating_authority : "",
                      -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 12, now, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

int bf_authority_revoke(BfAuthority *auth, const char *edge_id) {
    if (!auth || !edge_id) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(auth->db,
                           "UPDATE authority_edges SET revoked=1 WHERE edge_id=? AND revoked=0",
                           -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, edge_id, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    int changed = sqlite3_changes(auth->db);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return -1;
    }
    return changed > 0 ? 1 : 0;
}

int bf_authority_delegate(BfAuthority *auth, const char *parent_edge_id,
                          const char *child_edge_id, const char *to_actor,
                          const char *purpose, const char *expires_at) {
    sqlite3_stmt *q = NULL;
    BfAuthorityEdge child = {0};
    char *permission = NULL, *subject = NULL, *parent_purpose = NULL;
    char *parent_scope = NULL, *effective = NULL, *parent_expiry = NULL;
    int rc = -1;
    if (!auth || !parent_edge_id || !child_edge_id || !to_actor) return -1;
    if (sqlite3_prepare_v2(auth->db, "SELECT permission,subject,purpose,scope,effective_from,expires_at,delegable,revoked FROM authority_edges WHERE edge_id=?", -1, &q, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(q, 1, parent_edge_id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(q) != SQLITE_ROW || !sqlite3_column_int(q, 6) || sqlite3_column_int(q, 7)) goto done;
    /* sqlite3_column_text lifetime ends at finalize/reset.  Copy the parent
     * before inserting the child, rather than handing SQLite-owned pointers to
     * a second statement on the same connection. */
    permission = sqlite3_mprintf("%s", sqlite3_column_text(q, 0));
    subject = sqlite3_mprintf("%s", sqlite3_column_text(q, 1));
    parent_purpose = sqlite3_mprintf("%s", sqlite3_column_text(q, 2));
    parent_scope = sqlite3_mprintf("%s", sqlite3_column_text(q, 3));
    if (sqlite3_column_type(q, 4) != SQLITE_NULL) effective = sqlite3_mprintf("%s", sqlite3_column_text(q, 4));
    if (sqlite3_column_type(q, 5) != SQLITE_NULL) parent_expiry = sqlite3_mprintf("%s", sqlite3_column_text(q, 5));
    if (!permission || !subject || !parent_purpose || !parent_scope ||
        (sqlite3_column_type(q, 4) != SQLITE_NULL && !effective) ||
        (sqlite3_column_type(q, 5) != SQLITE_NULL && !parent_expiry)) goto done;
    if ((parent_purpose && parent_purpose[0] && (!purpose || strcmp(purpose, parent_purpose))) ||
        (parent_expiry && parent_expiry[0] && expires_at && strcmp(expires_at, parent_expiry) > 0)) goto done;
    child.edge_id=child_edge_id; child.actor=to_actor;
    child.permission=permission; child.subject=subject;
    child.purpose=purpose ? purpose : parent_purpose; child.scope=parent_scope;
    child.effective_from=effective; child.expires_at=expires_at ? expires_at : parent_expiry;
    child.originating_authority=parent_edge_id; child.delegable=0;
    sqlite3_finalize(q); q = NULL;
    rc = bf_authority_grant(auth, &child, NULL);
done:
    sqlite3_finalize(q);
    sqlite3_free(permission); sqlite3_free(subject); sqlite3_free(parent_purpose);
    sqlite3_free(parent_scope); sqlite3_free(effective); sqlite3_free(parent_expiry);
    return rc;
}

int bf_authority_revoke_cascade(BfAuthority *auth, const char *edge_id) {
    char ids[256][256]; size_t count=0, at=0; int changed=0;
    sqlite3_stmt *q=NULL;
    if (!auth || !edge_id) return -1;
    if (snprintf(ids[count++], sizeof(ids[0]), "%s", edge_id) >= (int)sizeof(ids[0])) return -1;
    while (at < count) {
        if (sqlite3_prepare_v2(auth->db, "SELECT edge_id FROM authority_edges WHERE originating_authority=?", -1, &q, NULL) != SQLITE_OK) return -1;
        sqlite3_bind_text(q,1,ids[at++],-1,SQLITE_TRANSIENT);
        while (sqlite3_step(q)==SQLITE_ROW) {
            if (count == 256 || snprintf(ids[count], sizeof(ids[0]), "%s", sqlite3_column_text(q, 0)) >= (int)sizeof(ids[0])) { sqlite3_finalize(q); return -1; }
            count++;
        }
        sqlite3_finalize(q); q=NULL;
    }
    for (at=0; at<count; at++) { int r=bf_authority_revoke(auth,ids[at]); if(r<0)return -1; changed+=r; }
    return changed;
}

int bf_authority_has(BfAuthority *auth, const char *actor, const char *permission,
                     const char *subject, const char *at_iso, const char *purpose) {
    if (!auth || !actor || !permission || !subject) {
        return -1;
    }
    char stamp[40];
    if (!at_iso || at_iso[0] == '\0') {
        now_iso(stamp);
        at_iso = stamp;
    }
    int64_t moment;
    if (parse_iso(at_iso, &moment) != 0) {
        return -1;
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            auth->db,
            "SELECT purpose,effective_from,expires_at FROM authority_edges"
            " WHERE actor=? AND permission=? AND subject=? AND revoked=0",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, actor, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, permission, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, subject, -1, SQLITE_STATIC);

    int result = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *edge_purpose = (const char *)sqlite3_column_text(stmt, 0);
        const char *eff = (const char *)sqlite3_column_text(stmt, 1);
        const char *exp = (const char *)sqlite3_column_text(stmt, 2);
        /* purpose asked, edge has a specific purpose that differs -> skip */
        if (purpose && edge_purpose && edge_purpose[0] != '\0' &&
            strcmp(edge_purpose, purpose) != 0) {
            continue;
        }
        int64_t start, end;
        if (parse_iso(eff, &start) == 0 && moment < start) {
            continue; /* before the window opens */
        }
        if (parse_iso(exp, &end) == 0 && moment > end) {
            continue; /* after the window closes */
        }
        result = 1;
        break;
    }
    sqlite3_finalize(stmt);
    return result;
}
