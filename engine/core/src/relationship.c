/* Relationship trajectory and shadow institution conversion.  See bf_relationship.h. */
#include "bf_relationship.h"

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static const char *STAGES[] = {"discovered", "observed", "contacted", "replied", "engaged", "assigned",
    "reviewed", "corrected", "collaborated", "committed", "adopted", "converted", NULL};

static const char *SCHEMA =
    "CREATE TABLE IF NOT EXISTS relationships(rel_id TEXT PRIMARY KEY, actor TEXT NOT NULL, profile TEXT NOT NULL DEFAULT '',"
    " stage TEXT NOT NULL DEFAULT 'discovered', evidence TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS relationship_transitions(rel_id TEXT NOT NULL, from_stage TEXT NOT NULL, to_stage TEXT NOT NULL,"
    " evidence TEXT NOT NULL DEFAULT '', at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS shadow_institutions(inst_id TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '',"
    " contact_actor TEXT NOT NULL DEFAULT '', profile TEXT NOT NULL DEFAULT '', observed_hierarchy TEXT NOT NULL DEFAULT '[]',"
    " public_attachments TEXT NOT NULL DEFAULT '[]', evidence TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL);";

static int present(const char *s) { return s && *s; }
static const char *nz(const char *s) { return s ? s : ""; }

int bf_relationship_open(void *db) { return db && sqlite3_exec(db, SCHEMA, NULL, NULL, NULL) == SQLITE_OK ? 0 : -1; }

int bf_relationship_stage_rank(const char *s) {
    for (int i = 0; s && STAGES[i]; i++) if (!strcmp(STAGES[i], s)) return i;
    return -1;
}

static void stamp(const char *now, char out[32]) {
    if (present(now)) { snprintf(out, 32, "%s", now); return; }
    time_t t = time(NULL); struct tm u; gmtime_r(&t, &u); strftime(out, 32, "%Y-%m-%dT%H:%M:%S+00:00", &u);
}

static int run(sqlite3 *db, const char *sql, int n, const char *const *v) {
    sqlite3_stmt *st;
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) return -1;
    for (int i = 0; i < n; i++) sqlite3_bind_text(st, i + 1, nz(v[i]), -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? 0 : -1;
}

int bf_relationship_record(void *db, const char *id, const char *actor, const char *profile,
                           const char *stage, const char *evidence, const char *now) {
    const char *s = present(stage) ? stage : "discovered"; char at[32];
    if (!db || !present(id) || !present(actor) || bf_relationship_stage_rank(s) < 0 || bf_relationship_open(db)) return -1;
    stamp(now, at);
    const char *v[] = {id, actor, profile, s, evidence, at};
    return run(db, "INSERT INTO relationships(rel_id,actor,profile,stage,evidence,updated_at) VALUES(?1,?2,?3,?4,?5,?6) "
        "ON CONFLICT(rel_id) DO UPDATE SET actor=excluded.actor,profile=excluded.profile,stage=excluded.stage,"
        "evidence=excluded.evidence,updated_at=excluded.updated_at", 6, v);
}

int bf_relationship_advance(void *vdb, const char *id, const char *to, const char *evidence, const char *now, int allow_regress) {
    sqlite3 *db = vdb; sqlite3_stmt *st; char cur[32] = "", at[32];
    int to_rank = bf_relationship_stage_rank(to);
    if (!db || !present(id) || to_rank < 0 || bf_relationship_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT stage FROM relationships WHERE rel_id=?1", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
    int found = sqlite3_step(st) == SQLITE_ROW;
    if (found) snprintf(cur, sizeof cur, "%s", sqlite3_column_text(st, 0));
    sqlite3_finalize(st);
    if (!found) return 0;
    if (!allow_regress && to_rank < bf_relationship_stage_rank(cur)) return 0;
    stamp(now, at);
    if (sqlite3_exec(db, "SAVEPOINT rel", NULL, NULL, NULL) != SQLITE_OK) return -1;
    const char *u[] = {to, evidence, at, id}, *t[] = {id, cur, to, evidence, at};
    int ok = run(db, "UPDATE relationships SET stage=?1, evidence=?2, updated_at=?3 WHERE rel_id=?4", 4, u) == 0 &&
             run(db, "INSERT INTO relationship_transitions(rel_id,from_stage,to_stage,evidence,at) VALUES(?1,?2,?3,?4,?5)", 5, t) == 0;
    sqlite3_exec(db, ok ? "RELEASE rel" : "ROLLBACK TO rel; RELEASE rel", NULL, NULL, NULL);
    return ok ? 1 : -1;
}

int bf_relationship_stage_at_least(void *vdb, const char *actor, const char *profile, const char *stage) {
    sqlite3 *db = vdb; sqlite3_stmt *st; int want = bf_relationship_stage_rank(stage);
    if (!db || !present(actor) || want < 0 || bf_relationship_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT stage FROM relationships WHERE actor=?1 AND profile=?2", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, actor, -1, SQLITE_TRANSIENT); sqlite3_bind_text(st, 2, nz(profile), -1, SQLITE_TRANSIENT);
    int any = 0, reached = 0, rc;
    while ((rc = sqlite3_step(st)) == SQLITE_ROW) { any = 1; if (bf_relationship_stage_rank((const char *)sqlite3_column_text(st, 0)) >= want) reached = 1; }
    sqlite3_finalize(st);
    if (rc != SQLITE_DONE) return -1;
    return any ? reached : -2;
}

int64_t bf_relationship_transition_count(void *vdb, const char *id) {
    sqlite3 *db = vdb; sqlite3_stmt *st; int64_t n = -1;
    if (!db || !present(id) || bf_relationship_open(db)) return -1;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM relationship_transitions WHERE rel_id=?1", -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
    return n;
}

int bf_shadow_record(void *db, const char *id, const char *name, const char *contact, const char *profile,
                     const char *hierarchy, const char *attachments, const char *evidence, const char *now) {
    char at[32];
    if (!db || !present(id) || bf_relationship_open(db)) return -1;
    stamp(now, at);
    const char *v[] = {id, name, contact, profile, present(hierarchy) ? hierarchy : "[]", present(attachments) ? attachments : "[]", evidence, at};
    return run(db, "INSERT INTO shadow_institutions(inst_id,name,contact_actor,profile,observed_hierarchy,public_attachments,evidence,updated_at)"
        " VALUES(?1,?2,?3,?4,json(?5),json(?6),?7,?8) ON CONFLICT(inst_id) DO UPDATE SET name=excluded.name,contact_actor=excluded.contact_actor,"
        "profile=excluded.profile,observed_hierarchy=excluded.observed_hierarchy,public_attachments=excluded.public_attachments,"
        "evidence=excluded.evidence,updated_at=excluded.updated_at", 8, v);
}

int bf_shadow_conversion_stage(void *vdb, const char *id, char *stage, size_t sc, char *detail, size_t dc) {
    sqlite3 *db = vdb; sqlite3_stmt *st;
    if (!db || !present(id) || !stage || !detail || bf_relationship_open(db)) return -1;
#define SET(s, d) do { snprintf(stage, sc, "%s", s); snprintf(detail, dc, "%s", d); } while (0)
    if (sqlite3_prepare_v2(db, "SELECT contact_actor,profile,json_array_length(public_attachments) FROM shadow_institutions WHERE inst_id=?1",
                           -1, &st, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); SET("observed", "not recorded"); return 0; }
    char contact[256], profile[256]; int attached = sqlite3_column_int(st, 2);
    snprintf(contact, sizeof contact, "%s", nz((const char *)sqlite3_column_text(st, 0)));
    snprintf(profile, sizeof profile, "%s", nz((const char *)sqlite3_column_text(st, 1)));
    sqlite3_finalize(st);
    if (attached <= 0) { SET("observed", "public structure not yet mapped"); return 0; }
    if (!*contact || bf_relationship_stage_at_least(db, contact, profile, "engaged") != 1) { SET("shadow", "no relationship at >= engaged"); return 0; }
    if (bf_relationship_stage_at_least(db, contact, profile, "collaborated") != 1) { SET("relationship_established", "relationship engaged, not yet collaborated"); return 0; }
    SET("active", "relationship collaborated");
    return 0;
#undef SET
}
