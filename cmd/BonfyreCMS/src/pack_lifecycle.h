#ifndef BONFYRE_CMS_PACK_LIFECYCLE_H
#define BONFYRE_CMS_PACK_LIFECYCLE_H
#include "../../BonfyreWebAddress/src/pack_resolution.h"
/* Installation metadata belongs beside the existing CMS rows. It contains
 * version pins, never copied pack bodies, product state, credentials or grants. */
typedef struct {
    long long id, revision, activation;
    char version[64], digest[65], status[24], last_operation[24], last_actor[160];
    BfPackResolution lock;
} CmsPackInstallation;
static int cpack_exists(sqlite3 *db, const char *table) {
    sqlite3_stmt *s = NULL; int yes = 0;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", -1, &s, NULL) == SQLITE_OK) {
        sqlite3_bind_text(s, 1, table, -1, SQLITE_STATIC); yes = sqlite3_step(s) == SQLITE_ROW;
    }
    sqlite3_finalize(s); return yes;
}
static int cpack_schema(sqlite3 *db) {
    sqlite3_stmt *s = NULL; int count = 0, step;
    if (sqlite3_prepare_v2(db, "SELECT name,schema_json FROM _content_types WHERE name IN ('pack_installation','pack_activation') ORDER BY name", -1, &s, NULL) != SQLITE_OK) return 0;
    while ((step = sqlite3_step(s)) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(s, 0);
        const char *raw = (const char *)sqlite3_column_text(s, 1);
        if (!name || !raw) { sqlite3_finalize(s); return 0; }
        if (!find_type(name)) {
            ContentType type = {0};
            if (g_type_count >= CMS_MAX_TYPES || parse_schema(raw, &type) != 0 || strcmp(type.name, name)) {
                free(type.fields); sqlite3_finalize(s); return 0;
            }
            g_types[g_type_count++] = type;
        }
        count++;
    }
    sqlite3_finalize(s); if (step != SQLITE_DONE || count != 2) return 0;
    return sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS _pack_installation_state(installation_id INTEGER PRIMARY KEY,revision INTEGER NOT NULL,source_digest TEXT NOT NULL,last_operation TEXT NOT NULL,last_actor TEXT NOT NULL);"
        "CREATE TABLE IF NOT EXISTS _pack_installation_pin(installation_id INTEGER NOT NULL,pack TEXT NOT NULL,version TEXT NOT NULL,root TEXT NOT NULL,PRIMARY KEY(installation_id,pack));",
        NULL, NULL, NULL) == SQLITE_OK;
}
static int cpack_read(sqlite3 *db, const char *ns, const char *subject, const char *scope,
                      const char *pack, CmsPackInstallation *o) {
    sqlite3_stmt *s = NULL; int step;
    memset(o, 0, sizeof(*o)); snprintf(o->status, sizeof(o->status), "absent");
    if (!cpack_exists(db, "_pack_installation_state")) return 1;
    if (sqlite3_prepare_v2(db, "SELECT i.id,i.pinned_version,i.status,m.revision,m.source_digest,m.last_operation,m.last_actor FROM pack_installation i JOIN _pack_installation_state m ON m.installation_id=i.id WHERE i.namespace=?1 AND i.installed_for=?2 AND i.pack=?3", -1, &s, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(s, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(s, 2, subject, -1, SQLITE_STATIC); sqlite3_bind_text(s, 3, pack, -1, SQLITE_STATIC);
    step = sqlite3_step(s);
    if (step == SQLITE_DONE) { sqlite3_finalize(s); return 1; }
    char row_status[32];
    if (step != SQLITE_ROW || !bfpg_text(s, 1, o->version, sizeof(o->version)) || !bfpg_text(s, 2, row_status, sizeof(row_status)) ||
        !bfpg_text(s, 4, o->digest, sizeof(o->digest)) || !bfpg_text(s, 5, o->last_operation, sizeof(o->last_operation)) || !bfpg_text(s, 6, o->last_actor, sizeof(o->last_actor))) { sqlite3_finalize(s); return 0; }
    o->id = sqlite3_column_int64(s, 0); o->revision = sqlite3_column_int64(s, 3);
    step = sqlite3_step(s); sqlite3_finalize(s); s = NULL;
    if (step != SQLITE_DONE || o->id < 1 || o->id > INT_MAX || o->revision < 1 || strlen(o->digest) != 64) return 0;
    if (strcmp(row_status, "published") && strcmp(row_status, "withdrawn")) return 0;
    snprintf(o->status, sizeof(o->status), "%s", !strcmp(row_status, "withdrawn") ? "removed" : "installed");
    if (sqlite3_prepare_v2(db, "SELECT id,status,conditions FROM pack_activation WHERE namespace=?1 AND pack=?2 AND activated_for=?3 AND scope=?4", -1, &s, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(s, 1, ns, -1, SQLITE_STATIC); sqlite3_bind_text(s, 2, pack, -1, SQLITE_STATIC); sqlite3_bind_text(s, 3, subject, -1, SQLITE_STATIC); sqlite3_bind_text(s, 4, scope, -1, SQLITE_STATIC);
    step = sqlite3_step(s);
    if (step == SQLITE_ROW) {
        char status[32], conditions[128], expected[128];
        o->activation = sqlite3_column_int64(s, 0);
        if (!bfpg_text(s, 1, status, sizeof(status)) || !bfpg_text(s, 2, conditions, sizeof(conditions))) { sqlite3_finalize(s); return 0; }
        snprintf(expected, sizeof(expected), "pack-lock:%s", o->digest);
        if (!strcmp(status, "published")) {
            if (strcmp(conditions, expected) || strcmp(o->status, "installed")) { sqlite3_finalize(s); return 0; }
            snprintf(o->status, sizeof(o->status), "active");
        } else if (strcmp(status, "withdrawn")) { sqlite3_finalize(s); return 0; }
        step = sqlite3_step(s);
    }
    sqlite3_finalize(s); s = NULL;
    if (step != SQLITE_DONE || o->activation > INT_MAX) return 0;
    if (sqlite3_prepare_v2(db, "SELECT pack,version,root FROM _pack_installation_pin WHERE installation_id=?1 ORDER BY pack", -1, &s, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_int64(s, 1, o->id);
    while ((step = sqlite3_step(s)) == SQLITE_ROW) {
        if (o->lock.count >= BFPG_MAX) { sqlite3_finalize(s); return 0; }
        BfPackResolved *n = &o->lock.nodes[o->lock.count++];
        if (!bfpg_text(s, 0, n->pack, sizeof(n->pack)) || !bfpg_text(s, 1, n->version, sizeof(n->version)) || !bfpg_text(s, 2, n->root, sizeof(n->root))) { sqlite3_finalize(s); return 0; }
    }
    sqlite3_finalize(s);
    int index = bfpg_find(&o->lock, pack);
    return step == SQLITE_DONE && index >= 0 && !strcmp(o->lock.nodes[index].version, o->version);
}
static void cpack_output(const char *pack, const CmsPackInstallation *i, const BfPackResolution *p,
                          const char *state, int committed, const char *error) {
    printf("schema\tbonfyre.pack.lifecycle.v1\npack\t%s\nstate\t%s\nrevision\t%lld\ninstallation\t%lld\npinned_digest\t%s\ncommitted\t%d\n",
        pack, state ? state : i->status, i->revision, i->id, i->digest, committed);
    if (error && *error) printf("error\t%s\n", error);
    for (size_t n = 0; n < i->lock.count; n++)
        printf("pin\t%s\t%s\t%s\n", i->lock.nodes[n].pack, i->lock.nodes[n].version, i->lock.nodes[n].root);
    if (p) {
        if (*p->error) printf("reason\t%s\nsubject\t%s\n", p->error, p->subject);
        else {
            int index = bfpg_find(p, pack);
            printf("digest\t%s\nversion\t%s\n", p->digest, index >= 0 ? p->nodes[index].version : "");
            for (size_t n = 0; n < p->count; n++) printf("node\t%s\t%s\t%s\t%s\t%s\t%s\n", p->nodes[n].pack, p->nodes[n].version, p->nodes[n].root, p->nodes[n].layer, p->nodes[n].kind, p->nodes[n].label);
        }
    }
}
static char *cpack_json(sqlite3 *db, const char *sql, const char **v, size_t count) {
    sqlite3_stmt *s = NULL; char *out = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) return NULL;
    for (size_t i = 0; i < count; i++) sqlite3_bind_text(s, (int)i + 1, v[i], -1, SQLITE_TRANSIENT);
    if (sqlite3_step(s) == SQLITE_ROW) out = strdup((const char *)sqlite3_column_text(s, 0));
    sqlite3_finalize(s); return out;
}
#include "workspace_store.h"
static int cpack_lifecycle(sqlite3 *db, const char *operation, const char *pack, const char *version,
                           const char *subject, const char *scope, const char *actor,
                           const char *expected_digest, long long expected_revision) {
    CmsPackInstallation current = {0}; BfPackResolution plan = {0}, locked = {0};
    sqlite3_stmt *s = NULL; int begun = 0, committed = 0, rc = 1, valid_lock = 0;
    char key[512], hash[65], ns[80], conditions[96]; char *body = NULL;
    const char *error = "storage_refused";
    int reading = operation && (!strcmp(operation, "plan") || !strcmp(operation, "current"));
    if (!operation || (!reading && strcmp(operation, "install") && strcmp(operation, "activate") && strcmp(operation, "deactivate") && strcmp(operation, "remove")) ||
        !bfpg_token(pack, 96) || !bfpg_token(subject, 160) || !bfpg_token(actor, 160) ||
        !scope || (strcmp(scope, "self") && strcmp(scope, "institution") && strcmp(scope, "household")) ||
        (version && *version && !bfpg_token(version, 64)) || expected_revision < 0 || expected_revision >= INT_MAX) {
        fputs("error\tinvalid_arguments\n", stdout); return 1;
    }
    if (!reading && (!expected_digest || strlen(expected_digest) != 64 || strspn(expected_digest, "abcdef0123456789") != 64)) { fputs("error\tinvalid_review\n", stdout); return 1; }
    snprintf(key, sizeof(key), "%s\n%s", subject, scope); bf_sha256_hex((const uint8_t *)key, strlen(key), hash);
    snprintf(ns, sizeof(ns), "pack-use-%.40s", hash);
    if (sqlite3_exec(db, reading ? "BEGIN" : "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) goto done;
    begun = 1;
    if (!strcmp(scope,"household")) {
        BfHouseholdAccess access;
        if(strncmp(subject,"household:",10) || !bhh_context(db,subject+10,actor,&access) || bhh_rank(access.role)<(reading?1:3)) {
            error="household_access_refused";goto done;
        }
    }
    if (!reading && !cpack_schema(db)) goto done;
    if (!cpack_read(db, ns, subject, scope, pack, &current)) goto done;
    if (current.id) valid_lock = bfpg_resolve(db, pack, current.version, &current.lock, &locked) && !strcmp(locked.digest, current.digest);
    if (reading) {
        int valid = !strcmp(operation, "current") ? valid_lock : bfpg_resolve(db, pack, version, NULL, &plan);
        BfPackResolution *shown = !strcmp(operation, "current") ? &locked : &plan;
        const char *state = current.id && strcmp(current.status, "removed") && !valid_lock ? "stale" : current.status;
        cpack_output(pack, &current, shown, state, 0, valid ? NULL : "resolution_refused"); rc = valid ? 0 : 1; goto done;
    }
    int installing = !strcmp(operation, "install"), activating = !strcmp(operation, "activate");
    if (installing) {
        if (!bfpg_resolve(db, pack, version, NULL, &plan)) { error = "resolution_refused"; goto done; }
        if (strcmp(plan.digest, expected_digest)) { error = "source_changed"; goto done; }
    } else {
        if (!current.id) { error = "not_installed"; goto done; }
        if (strcmp(current.digest, expected_digest)) { error = "source_changed"; goto done; }
        if (activating && !valid_lock) { plan = locked; error = "installed_source_changed"; goto done; }
        plan = locked;
    }
    /* A lost response can repeat exactly one committed operation. */
    if (current.revision == expected_revision + 1 && !strcmp(current.last_operation, operation) && !strcmp(current.last_actor, actor) && !strcmp(current.digest, expected_digest)) {
        cpack_output(pack, &current, valid_lock || installing ? &plan : NULL, NULL, 1, NULL); rc = 0; goto done;
    }
    if (current.revision != expected_revision) { error = "revision_conflict"; goto done; }
    if (activating && strcmp(current.status, "installed")) { error = "install_before_activation"; goto done; }
    defer_hooks = 1; deferred_write_error = 0;
    if (installing) {
        int index = bfpg_find(&plan, pack);
        const char *v[] = {pack, subject, "stable", plan.nodes[index].version, actor, "published"};
        body = cpack_json(db, "SELECT json_object('pack',?1,'installed_for',?2,'channel',?3,'pinned_version',?4,'installed_by',?5,'status',?6)", v, 6);
        if (!body || (current.id ? entry_update(db, "pack_installation", (int)current.id, ns, body) : entry_create(db, "pack_installation", ns, body, &current.id))) goto done;
        free(body); body = NULL;
        if (sqlite3_prepare_v2(db, "DELETE FROM _pack_installation_pin WHERE installation_id=?1", -1, &s, NULL) != SQLITE_OK) goto done;
        sqlite3_bind_int64(s, 1, current.id); if (sqlite3_step(s) != SQLITE_DONE) goto done; sqlite3_finalize(s); s = NULL;
        if (sqlite3_prepare_v2(db, "INSERT INTO _pack_installation_pin VALUES(?1,?2,?3,?4)", -1, &s, NULL) != SQLITE_OK) goto done;
        for (size_t n = 0; n < plan.count; n++) {
            sqlite3_bind_int64(s, 1, current.id); sqlite3_bind_text(s, 2, plan.nodes[n].pack, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 3, plan.nodes[n].version, -1, SQLITE_TRANSIENT); sqlite3_bind_text(s, 4, plan.nodes[n].root, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(s) != SQLITE_DONE) goto done; sqlite3_reset(s); sqlite3_clear_bindings(s);
        }
        sqlite3_finalize(s); s = NULL;
    }
    if (!strcmp(operation, "remove") && entry_update(db, "pack_installation", (int)current.id, ns, "{\"status\":\"withdrawn\"}")) goto done;
    if (activating || current.activation) {
        snprintf(conditions, sizeof(conditions), "pack-lock:%s", expected_digest);
        const char *v[] = {pack, subject, scope, actor, conditions, activating ? "published" : "withdrawn"};
        body = cpack_json(db, "SELECT json_object('pack',?1,'activated_for',?2,'scope',?3,'activated_by',?4,'conditions',?5,'status',?6)", v, 6);
        if (!body || (current.activation ? entry_update(db, "pack_activation", (int)current.activation, ns, body) : entry_create(db, "pack_activation", ns, body, &current.activation))) goto done;
        free(body); body = NULL;
    }
    if (sqlite3_prepare_v2(db, "INSERT INTO _pack_installation_state VALUES(?1,?2,?3,?4,?5) ON CONFLICT(installation_id) DO UPDATE SET revision=excluded.revision,source_digest=excluded.source_digest,last_operation=excluded.last_operation,last_actor=excluded.last_actor", -1, &s, NULL) != SQLITE_OK) goto done;
    sqlite3_bind_int64(s, 1, current.id); sqlite3_bind_int64(s, 2, current.revision + 1); sqlite3_bind_text(s, 3, expected_digest, -1, SQLITE_STATIC); sqlite3_bind_text(s, 4, operation, -1, SQLITE_STATIC); sqlite3_bind_text(s, 5, actor, -1, SQLITE_STATIC);
    if (sqlite3_step(s) != SQLITE_DONE || deferred_write_error) goto done;
    sqlite3_finalize(s); s = NULL;
    if (!cw_bind(db, operation, ns, subject, scope, pack, &current, &plan) || deferred_write_error) { error = "workspace_binding_refused"; goto done; }
    if (!cpack_read(db, ns, subject, scope, pack, &current)) goto done;
    if (sqlite3_exec(db, "COMMIT", NULL, NULL, NULL) != SQLITE_OK) goto done;
    begun = 0; committed = 1; finish_deferred_hooks(db, 1);
    cpack_output(pack, &current, installing || valid_lock ? &plan : NULL, NULL, 1, NULL);
    /* Row hooks retain the existing CMS contract; delivery is separately reported. */
    int delta_ok = cms_emit_fact_delta("pack_installation", pack, NULL, current.id);
    if (current.activation && !cms_emit_fact_delta("pack_activation", pack, NULL, current.activation)) delta_ok = 0;
    printf("fact_delta\t%s\n", !cms_fact_deltas_enabled() ? "DISABLED" : delta_ok ? "DELIVERED" : "DELIVERY_FAILED");
    rc = delta_ok ? 0 : 2;
 done:
    sqlite3_finalize(s); free(body);
    if (begun) sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    if (!committed && !reading) {
        finish_deferred_hooks(db, 0);
        if (rc && !cpack_read(db, ns, subject, scope, pack, &current)) {
            memset(&current, 0, sizeof(current)); snprintf(current.status, sizeof(current.status), "unavailable");
        }
        if (rc) cpack_output(pack, &current, &plan, NULL, 0, error);
    }
    return rc;
}
#endif
