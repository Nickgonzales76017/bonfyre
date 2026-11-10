/* Session-scoped team administration. The local provisioning commands above
 * remain separate from these remotely callable, authority-checked operations. */
static void member_json_string(sqlite3_str *out, const char *value) {
    sqlite3_str_appendall(out, "\"");
    for (const unsigned char *p = (const unsigned char *)(value ? value : ""); *p; ++p) {
        if (*p == '"' || *p == '\\') sqlite3_str_appendf(out, "\\%c", *p);
        else if (*p < 32) sqlite3_str_appendf(out, "\\u%04x", *p);
        else sqlite3_str_appendchar(out, 1, *p);
    }
    sqlite3_str_appendall(out, "\"");
}

static int member_failure(sqlite3 *db, int status, const char *message) {
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    printf("{\"error\":\"%s\",\"status\":%d}\n", message, status);
    return 1;
}

static int cmd_account_profile(sqlite3 *db, const char *token) {
    sqlite3_stmt *st = NULL;
    int user_id = 0;
    if (!session_user_id(db, token, &user_id)) return member_failure(db, 401, "Sign in to view your account.");
    if (sqlite3_prepare_v2(db, "SELECT name,email FROM users WHERE id=? AND active=1", -1, &st, NULL) != SQLITE_OK)
        return member_failure(db, 500, "Could not read your account.");
    sqlite3_bind_int(st, 1, user_id);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return member_failure(db, 401, "Sign in to view your account."); }
    sqlite3_str *out = sqlite3_str_new(db);
    sqlite3_str_appendf(out, "{\"id\":%d,\"name\":", user_id);
    member_json_string(out, (const char *)sqlite3_column_text(st, 0));
    sqlite3_str_appendall(out, ",\"email\":"); member_json_string(out, (const char *)sqlite3_column_text(st, 1));
    sqlite3_str_appendall(out, "}"); sqlite3_finalize(st);
    char *json = sqlite3_str_finish(out);
    if (!json) return member_failure(db, 500, "Could not read your account.");
    puts(json); sqlite3_free(json); return 0;
}

static int member_transaction(sqlite3 *db, const char *token, const char *institution, int write) {
    sqlite3_busy_timeout(db, 3000);
    if (sqlite3_exec(db, write ? "BEGIN IMMEDIATE" : "BEGIN", NULL, NULL, NULL) != SQLITE_OK) return 0;
    return auth_institution_authorized(db, token, institution, "admin");
}

static int cmd_institution_members(sqlite3 *db, const char *token, const char *institution, int after_id) {
    sqlite3_stmt *st = NULL;
    sqlite3_str *out;
    int count = 0, last_id = 0, more = 0, step;
    char now[64]; iso_now(now, sizeof(now));
    if (after_id < 0 || !safe_institution(institution)) return member_failure(db, 400, "Invalid institution or cursor.");
    if (!member_transaction(db, token, institution, 0)) return member_failure(db, 403, "Administrator access is required.");
    if (sqlite3_prepare_v2(db,
        "SELECT u.id,u.name,u.email,g.role,g.expires_at,u.active FROM institution_grants g "
        "JOIN users u ON u.id=g.user_id WHERE g.institution=? AND g.active=1 AND u.id>? ORDER BY u.id LIMIT 51",
        -1, &st, NULL) != SQLITE_OK) return member_failure(db, 500, "Could not read members.");
    sqlite3_bind_text(st, 1, institution, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st, 2, after_id);
    out = sqlite3_str_new(db);
    sqlite3_str_appendall(out, "{\"institution\":"); member_json_string(out, institution);
    sqlite3_str_appendall(out, ",\"members\":[");
    while ((step = sqlite3_step(st)) == SQLITE_ROW) {
        if (count == 50) { more = 1; break; }
        const char *expiry = (const char *)sqlite3_column_text(st, 4);
        last_id = sqlite3_column_int(st, 0);
        sqlite3_str_appendf(out, "%s{\"user_id\":%d,\"name\":", count ? "," : "", last_id);
        member_json_string(out, (const char *)sqlite3_column_text(st, 1));
        sqlite3_str_appendall(out, ",\"email\":"); member_json_string(out, (const char *)sqlite3_column_text(st, 2));
        sqlite3_str_appendall(out, ",\"role\":"); member_json_string(out, (const char *)sqlite3_column_text(st, 3));
        sqlite3_str_appendall(out, ",\"institution\":"); member_json_string(out, institution);
        sqlite3_str_appendall(out, ",\"expires_at\":");
        if (expiry && *expiry) member_json_string(out, expiry); else sqlite3_str_appendall(out, "null");
        sqlite3_str_appendf(out, ",\"effective\":%s}", sqlite3_column_int(st, 5) && (!expiry || !*expiry || strcmp(now, expiry) <= 0) ? "true" : "false");
        count++;
    }
    sqlite3_finalize(st);
    sqlite3_str_appendall(out, "],\"next_user_id\":");
    if (more) sqlite3_str_appendf(out, "%d", last_id); else sqlite3_str_appendall(out, "null");
    sqlite3_str_appendall(out, "}");
    char *json = sqlite3_str_finish(out);
    if ((!more && step != SQLITE_DONE) || !json || sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_free(json); return member_failure(db, 500, "Could not read members.");
    }
    puts(json); sqlite3_free(json); return 0;
}

/* A delegated grant cannot outlive the administrator's best current grant.
 * Follow the generated ancestry graph; string prefixes have no authority. */
static int member_admin_expiry(sqlite3 *db, int user_id, const char *institution, char *expiry, size_t size) {
    sqlite3_stmt *st = NULL;
    char now[64]; int found = 0;
    iso_now(now, sizeof(now)); expiry[0] = '\0';
    if (sqlite3_prepare_v2(db, "SELECT expires_at FROM institution_grants WHERE user_id=? AND institution=? AND role='admin' AND active=1", -1, &st, NULL) != SQLITE_OK) return 0;
    for (const char *candidate = institution; candidate; candidate = institution_parent(candidate)) {
        sqlite3_reset(st); sqlite3_clear_bindings(st);
        sqlite3_bind_int(st, 1, user_id); sqlite3_bind_text(st, 2, candidate, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *value = (const char *)sqlite3_column_text(st, 0);
            if (!value || !*value) { expiry[0] = '\0'; sqlite3_finalize(st); return 1; }
            if (strcmp(value, now) >= 0 && strlen(value) < size) {
                if (!found || strcmp(value, expiry) > 0) snprintf(expiry, size, "%s", value);
                found = 1;
            }
        }
    }
    sqlite3_finalize(st); return found;
}

static int cmd_institution_member_set(sqlite3 *db, const char *token, const char *institution, const char *email, const char *role, const char *expected_role, const char *expected_expiry) {
    sqlite3_stmt *st = NULL;
    int caller = 0, target = 0, remove = role && !strcmp(role, "none"), step;
    char expiry[64], now[64], previous_expiry[64] = "";
    int existing = 0;
    if (!safe_institution(institution) || !email || !*email || strlen(email) > 300 ||
        (!remove && institution_role_rank(role) < 0)) return member_failure(db, 400, "Choose an existing account and access level.");
    if ((expected_role && strcmp(expected_role, "none") &&
         (institution_role_rank(expected_role) < 0 || !expected_expiry)) ||
        (!expected_role && expected_expiry) ||
        (expected_expiry && strlen(expected_expiry) >= sizeof(previous_expiry)))
        return member_failure(db, 400, "Invalid expected access state.");
    if (!member_transaction(db, token, institution, 1) || !session_user_id(db, token, &caller))
        return member_failure(db, 403, "Administrator access is required.");
    if (sqlite3_prepare_v2(db, "SELECT id FROM users WHERE email=? AND active=1", -1, &st, NULL) != SQLITE_OK)
        return member_failure(db, 500, "Could not find this account.");
    sqlite3_bind_text(st, 1, email, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) target = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    if (!target) return member_failure(db, 400, "An active account with this email must already exist.");
    if (target == caller) return member_failure(db, 400, "Ask another administrator to change your access.");
    if (!member_admin_expiry(db, caller, institution, expiry, sizeof(expiry))) return member_failure(db, 403, "Administrator access has expired.");
    if (sqlite3_prepare_v2(db, "SELECT role,expires_at FROM institution_grants WHERE user_id=? AND institution=? AND active=1", -1, &st, NULL) != SQLITE_OK)
        return member_failure(db, 500, "Could not read current access.");
    sqlite3_bind_int(st, 1, target); sqlite3_bind_text(st, 2, institution, -1, SQLITE_TRANSIENT);
    step = sqlite3_step(st);
    int conflict = 0;
    if (step == SQLITE_ROW) {
        existing = 1;
        const char *current_role = (const char *)sqlite3_column_text(st, 0);
        const char *current_expiry = (const char *)sqlite3_column_text(st, 1);
        if (current_expiry && strlen(current_expiry) >= sizeof(previous_expiry)) {
            sqlite3_finalize(st); return member_failure(db, 500, "Could not read current access expiry.");
        }
        snprintf(previous_expiry, sizeof(previous_expiry), "%s", current_expiry ? current_expiry : "");
        conflict = expected_role && (!strcmp(expected_role, "none") ||
            strcmp(expected_role, current_role) || strcmp(expected_expiry, previous_expiry));
    } else if (step == SQLITE_DONE) conflict = expected_role && strcmp(expected_role, "none");
    else { sqlite3_finalize(st); return member_failure(db, 500, "Could not read current access."); }
    sqlite3_finalize(st);
    if (conflict) return member_failure(db, 409, "This account's access changed. Refresh and review the current access.");
    /* Editing a role must not renew an expired grant or extend its lifetime.
     * A temporary administrator also cannot delegate beyond their own expiry. */
    if (existing && *previous_expiry && (!*expiry || strcmp(previous_expiry, expiry) < 0))
        snprintf(expiry, sizeof(expiry), "%s", previous_expiry);
    iso_now(now, sizeof(now));
    const char *sql = remove
        ? "UPDATE institution_grants SET active=0 WHERE user_id=?1 AND institution=?2 AND active=1"
        : "INSERT INTO institution_grants(user_id,institution,role,granted_at,expires_at,active) VALUES(?1,?2,?3,?4,?5,1) "
          "ON CONFLICT(user_id,institution) DO UPDATE SET role=excluded.role,granted_at=excluded.granted_at,expires_at=excluded.expires_at,active=1";
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return member_failure(db, 500, "Could not update access.");
    sqlite3_bind_int(st, 1, target); sqlite3_bind_text(st, 2, institution, -1, SQLITE_TRANSIENT);
    if (!remove) {
        sqlite3_bind_text(st, 3, role, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 4, now, -1, SQLITE_TRANSIENT);
        if (*expiry) sqlite3_bind_text(st, 5, expiry, -1, SQLITE_TRANSIENT); else sqlite3_bind_null(st, 5);
    }
    step = sqlite3_step(st); sqlite3_finalize(st);
    if (step != SQLITE_DONE || sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) return member_failure(db, 500, "Could not update access.");
    printf("{\"updated\":true,\"user_id\":%d,\"institution\":\"%s\",\"role\":\"%s\"}\n", target, institution, role);
    return 0;
}
