#ifndef BONFYRE_BACKGROUND_ACCESS_H
#define BONFYRE_BACKGROUND_ACCESS_H
#include "bf_msgpack.h"
#include "../../BonfyreCMS/src/household_access.h"

/* A trusted local owner rechecks a previously admitted actor. This is not a
 * browser authentication endpoint and does not create a session or a grant. */
static int auth_user_institution_authorized(sqlite3 *db, int user_id,
        const char *institution, const char *required_role) {
    sqlite3_stmt *statement = NULL;
    int required_rank = institution_role_rank(required_role), allowed = 0;
    char now[64];
    if (user_id <= 0 || !safe_institution(institution) || required_rank < 0) return 0;
    iso_now(now, sizeof(now));
    if (sqlite3_prepare_v2(db,
        "SELECT u.active,g.role,g.expires_at,g.active FROM users u "
        "JOIN institution_grants g ON g.user_id=u.id WHERE u.id=? AND g.institution=?",
        -1, &statement, NULL) != SQLITE_OK) return -1;
    for (const char *candidate = institution; candidate && !allowed; candidate = institution_parent(candidate)) {
        sqlite3_reset(statement);
        sqlite3_clear_bindings(statement);
        sqlite3_bind_int(statement, 1, user_id);
        sqlite3_bind_text(statement, 2, candidate, -1, SQLITE_TRANSIENT);
        int step = sqlite3_step(statement);
        if (step == SQLITE_ROW) {
            const char *role = (const char *)sqlite3_column_text(statement, 1);
            const char *expiry = (const char *)sqlite3_column_text(statement, 2);
            allowed = sqlite3_column_int(statement, 0) && sqlite3_column_int(statement, 3) &&
                (!expiry || !expiry[0] || strcmp(now, expiry) <= 0) &&
                institution_role_rank(role) >= required_rank;
        } else if (step != SQLITE_DONE) { sqlite3_finalize(statement); return -1; }
    }
    sqlite3_finalize(statement);
    return allowed;
}

static int auth_background_active(sqlite3 *db, int user_id) {
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(db, "SELECT active FROM users WHERE id=?", -1, &statement, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_int(statement, 1, user_id);
    int step = sqlite3_step(statement);
    int result = step == SQLITE_ROW ? !!sqlite3_column_int(statement, 0) : step == SQLITE_DONE ? 0 : -1;
    sqlite3_finalize(statement);
    return result;
}

static int auth_background_response(int valid, const char *actor, const char *subject,
        const char *scope, const char *permission, int allowed, const char *code) {
    bf_mp_writer_t out;
    bf_mp_writer_init(&out, 512);
    int bad = bf_mp_pack_array(&out, 3) || bf_mp_pack_uint(&out, 1) || bf_mp_pack_bool(&out, valid);
    if (valid) {
        bad = bad || bf_mp_pack_map(&out, 6) || bf_mp_pack_kv_str(&out, "actor", actor) ||
            bf_mp_pack_kv_str(&out, "subject", subject) || bf_mp_pack_kv_str(&out, "scope", scope) ||
            bf_mp_pack_kv_str(&out, "permission", permission) || bf_mp_pack_kv_bool(&out, "allowed", allowed) ||
            bf_mp_pack_kv_int(&out, "checked_at", (int64_t)time(NULL));
    } else {
        bad = bad || bf_mp_pack_map(&out, 2) || bf_mp_pack_kv_str(&out, "code", code) ||
            bf_mp_pack_kv_str(&out, "message", "The native background access check could not complete.");
    }
    int ok = !bad && fwrite(out.buf, 1, out.len, stdout) == out.len && fflush(stdout) == 0;
    bf_mp_writer_free(&out);
    return valid && ok ? 0 : 1;
}

static int cmd_background_access(const char *auth_path, const char *cms_path, const char *actor,
        const char *subject, const char *scope, const char *permission) {
    int user_id = 0, valid = 1, allowed = 0;
    sqlite3 *auth = NULL, *cms = NULL;
    if (!actor || !subject || !scope || !permission || !bhh_actor(actor) ||
        (strcmp(permission, "read") && strcmp(permission, "write")) ||
        (strcmp(scope, "self") && strcmp(scope, "institution") && strcmp(scope, "household")) ||
        (!strcmp(scope, "self") ? strcmp(actor, subject) :
          strncmp(subject, scope, strlen(scope)) || subject[strlen(scope)] != ':' || !safe_institution(subject + strlen(scope) + 1)))
        return auth_background_response(0, "", "", "", "", 0, "invalid");
    user_id = (int)strtol(actor + 5, NULL, 10);
    if (!auth_path || sqlite3_open_v2(auth_path, &auth, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) valid = 0;
    if (valid) {
        sqlite3_busy_timeout(auth, 3000);
        valid = sqlite3_exec(auth, "BEGIN", NULL, NULL, NULL) == SQLITE_OK;
    }
    if (valid) { allowed = auth_background_active(auth, user_id); valid = allowed >= 0; }
    if (valid && allowed && !strcmp(scope, "institution")) {
        allowed = auth_user_institution_authorized(auth, user_id, subject + 12, permission);
        valid = allowed >= 0;
    }
    if (valid && allowed && !strcmp(scope, "household")) {
        if (!cms_path || sqlite3_open_v2(cms_path, &cms, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) valid = 0;
        if (valid) {
            BfHouseholdAccess context;
            sqlite3_busy_timeout(cms, 3000);
            valid = sqlite3_exec(cms, "BEGIN", NULL, NULL, NULL) == SQLITE_OK;
            if (valid) {
                allowed = bhh_context(cms, subject + 10, actor, &context) &&
                    bhh_rank(context.role) >= (!strcmp(permission, "write") ? 2 : 1);
                valid = sqlite3_errcode(cms) == SQLITE_OK || sqlite3_errcode(cms) == SQLITE_DONE || sqlite3_errcode(cms) == SQLITE_ROW;
            }
        }
    }
    if (cms) { sqlite3_exec(cms, "ROLLBACK", NULL, NULL, NULL); sqlite3_close(cms); }
    if (auth) { sqlite3_exec(auth, "ROLLBACK", NULL, NULL, NULL); sqlite3_close(auth); }
    return auth_background_response(valid, actor, subject, scope, permission, allowed > 0, "unavailable");
}
#endif
