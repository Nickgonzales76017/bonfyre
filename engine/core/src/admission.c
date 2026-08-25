/*
 * Native resource-admission controller. Pure decision kernel + a grant ledger
 * on the resource_grants store shared with resource_admission.py.
 */

#include "bf_admission.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/statvfs.h>

#define BF_GIB ((int64_t)1 << 30)

struct BfAdmissionGrants {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

/* Must match resource_admission.py SCHEMA exactly. */
static const char *ADMISSION_SCHEMA =
    "CREATE TABLE IF NOT EXISTS resource_grants("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  plane TEXT NOT NULL,"
    "  kind TEXT NOT NULL,"
    "  volume TEXT NOT NULL,"
    "  estimated_bytes INTEGER NOT NULL,"
    "  granted_at TEXT NOT NULL,"
    "  expires_at TEXT,"
    "  released_at TEXT,"
    "  detail TEXT"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_resource_grants_open"
    "  ON resource_grants(volume, released_at);";

void bf_admission_policy_defaults(BfAdmissionPolicy *policy) {
    if (!policy) {
        return;
    }
    policy->protected_floor_bytes = 10 * BF_GIB;
    policy->per_plane_quota_bytes = 20 * BF_GIB;
    policy->max_grant_bytes = 40 * BF_GIB;
}

static void resolve_policy(const BfAdmissionPolicy *in, BfAdmissionPolicy *out) {
    bf_admission_policy_defaults(out);
    if (in) {
        if (in->protected_floor_bytes > 0) out->protected_floor_bytes = in->protected_floor_bytes;
        if (in->per_plane_quota_bytes > 0) out->per_plane_quota_bytes = in->per_plane_quota_bytes;
        if (in->max_grant_bytes > 0) out->max_grant_bytes = in->max_grant_bytes;
    }
}

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

BfAdmissionVerdict bf_admission_decide(const BfResourceRequest *request,
                                       const BfAdmissionPolicy *policy_in,
                                       int64_t free_bytes, int64_t committed_bytes,
                                       int64_t plane_committed_bytes,
                                       BfAdmissionDecision *out) {
    BfAdmissionPolicy policy;
    resolve_policy(policy_in, &policy);

    int64_t spendable = free_bytes - policy.protected_floor_bytes - committed_bytes;
    BfAdmissionVerdict verdict;
    const char *reason;

    if (!request || request->estimated_bytes <= 0) {
        verdict = BF_ADMISSION_REJECT;
        reason = "request must carry a positive size estimate";
    } else if (request->estimated_bytes > policy.max_grant_bytes) {
        verdict = BF_ADMISSION_REJECT;
        reason = "request exceeds the single-grant ceiling";
    } else if (plane_committed_bytes + request->estimated_bytes > policy.per_plane_quota_bytes) {
        verdict = BF_ADMISSION_DEFER;
        reason = "would exceed the per-plane quota";
    } else if (request->estimated_bytes > spendable) {
        /* Distinguish "never" from "not now". */
        int64_t headroom_if_drained = free_bytes - policy.protected_floor_bytes;
        if (request->estimated_bytes > headroom_if_drained) {
            verdict = BF_ADMISSION_REJECT;
            reason = "cannot fit above the protected floor even if every grant were released";
        } else {
            verdict = BF_ADMISSION_DEFER;
            reason = "exceeds spendable space; grants already committed";
        }
    } else {
        verdict = BF_ADMISSION_ADMIT;
        reason = "spendable above the protected floor";
    }

    if (out) {
        out->verdict = verdict;
        snprintf(out->reason, sizeof(out->reason), "%s", reason);
        out->free_bytes = free_bytes;
        out->committed_bytes = committed_bytes;
        out->spendable_bytes = spendable > 0 ? spendable : 0;
    }
    return verdict;
}

int64_t bf_admission_disk_free(const char *path) {
    struct statvfs st;
    if (!path || statvfs(path, &st) != 0) {
        return -1;
    }
    return (int64_t)st.f_bavail * (int64_t)st.f_frsize;
}

static int apply_schema(sqlite3 *db, FILE *err) {
    char *errmsg = NULL;
    if (sqlite3_exec(db, ADMISSION_SCHEMA, NULL, NULL, &errmsg) != SQLITE_OK) {
        if (err) {
            fprintf(err, "admission: schema failed: %s\n", errmsg ? errmsg : "?");
        }
        sqlite3_free(errmsg);
        return -1;
    }
    return 0;
}

int bf_admission_open(const char *database_path, BfAdmissionGrants **out, FILE *err) {
    if (!database_path || !out) {
        return -1;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(database_path, &db) != SQLITE_OK) {
        if (err) {
            fprintf(err, "admission: cannot open %s: %s\n", database_path,
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
    BfAdmissionGrants *grants = calloc(1, sizeof(*grants));
    if (!grants) {
        sqlite3_close(db);
        return -1;
    }
    grants->db = db;
    grants->owns_db = 1;
    grants->err = err;
    *out = grants;
    return 0;
}

int bf_admission_open_database(void *sqlite_database, BfAdmissionGrants **out, FILE *err) {
    if (!sqlite_database || !out) {
        return -1;
    }
    sqlite3 *db = (sqlite3 *)sqlite_database;
    if (apply_schema(db, err) != 0) {
        return -1;
    }
    BfAdmissionGrants *grants = calloc(1, sizeof(*grants));
    if (!grants) {
        return -1;
    }
    grants->db = db;
    grants->owns_db = 0;
    grants->err = err;
    *out = grants;
    return 0;
}

void bf_admission_close(BfAdmissionGrants *grants) {
    if (!grants) {
        return;
    }
    if (grants->owns_db && grants->db) {
        sqlite3_close(grants->db);
    }
    free(grants);
}

int64_t bf_admission_committed_bytes(BfAdmissionGrants *grants, const char *volume,
                                     const char *plane) {
    if (!grants || !volume) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    const char *sql = plane
        ? "SELECT COALESCE(SUM(estimated_bytes),0) FROM resource_grants"
          " WHERE volume=? AND plane=? AND released_at IS NULL"
        : "SELECT COALESCE(SUM(estimated_bytes),0) FROM resource_grants"
          " WHERE volume=? AND released_at IS NULL";
    int64_t n = -1;
    if (sqlite3_prepare_v2(grants->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, volume, -1, SQLITE_STATIC);
        if (plane) {
            sqlite3_bind_text(stmt, 2, plane, -1, SQLITE_STATIC);
        }
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            n = sqlite3_column_int64(stmt, 0);
        }
    }
    sqlite3_finalize(stmt);
    return n;
}

BfAdmissionVerdict bf_admission_request_grant(BfAdmissionGrants *grants,
                                              const BfResourceRequest *request,
                                              const BfAdmissionPolicy *policy,
                                              int64_t free_bytes, const char *now,
                                              BfAdmissionDecision *out_decision,
                                              int64_t *out_grant_id) {
    if (out_grant_id) {
        *out_grant_id = 0;
    }
    BfAdmissionDecision local;
    BfAdmissionDecision *decision = out_decision ? out_decision : &local;

    const char *volume = (request && request->volume) ? request->volume : "/";
    int64_t committed = bf_admission_committed_bytes(grants, volume, NULL);
    int64_t plane_committed =
        (request && request->plane)
            ? bf_admission_committed_bytes(grants, volume, request->plane)
            : 0;
    if (committed < 0) committed = 0;
    if (plane_committed < 0) plane_committed = 0;

    BfResourceRequest req_norm;
    if (request) {
        req_norm = *request;
        req_norm.volume = volume;
    } else {
        memset(&req_norm, 0, sizeof(req_norm));
    }

    BfAdmissionVerdict verdict =
        bf_admission_decide(request ? &req_norm : NULL, policy, free_bytes, committed,
                            plane_committed, decision);
    if (verdict != BF_ADMISSION_ADMIT) {
        return verdict;
    }

    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            grants->db,
            "INSERT INTO resource_grants"
            "(plane,kind,volume,estimated_bytes,granted_at,detail)"
            " VALUES(?,?,?,?,?,?)",
            -1, &stmt, NULL) != SQLITE_OK) {
        return verdict; /* decision stands; ledger write failed */
    }
    sqlite3_bind_text(stmt, 1, request->plane, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, request->kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, volume, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 4, request->estimated_bytes);
    sqlite3_bind_text(stmt, 5, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, request->detail ? request->detail : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_DONE && out_grant_id) {
        *out_grant_id = sqlite3_last_insert_rowid(grants->db);
    }
    sqlite3_finalize(stmt);
    return verdict;
}

int bf_admission_release_grant(BfAdmissionGrants *grants, int64_t grant_id, const char *now) {
    if (!grants) {
        return -1;
    }
    char stamp[40];
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            grants->db,
            "UPDATE resource_grants SET released_at=? WHERE id=? AND released_at IS NULL",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, grant_id);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* Parse "YYYY-MM-DDTHH:MM:SS[...]" as UTC into epoch seconds; -1 on failure. */
static int64_t parse_iso_utc(const char *iso) {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    if (!iso || sscanf(iso, "%d-%d-%dT%d:%d:%d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
                       &tm.tm_hour, &tm.tm_min, &tm.tm_sec) != 6) {
        return -1;
    }
    tm.tm_year -= 1900;
    tm.tm_mon -= 1;
    return (int64_t)timegm(&tm);
}

static void iso_from_epoch(int64_t epoch, char out[40]) {
    time_t t = (time_t)epoch;
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

int64_t bf_admission_reap_expired(BfAdmissionGrants *grants, int64_t older_than_seconds,
                                  const char *now) {
    if (!grants) {
        return -1;
    }
    int64_t now_epoch;
    char stamp[40];
    if (now && now[0]) {
        now_epoch = parse_iso_utc(now);
        if (now_epoch < 0) {
            return -1;
        }
        snprintf(stamp, sizeof(stamp), "%s", now);
    } else {
        now_epoch = (int64_t)time(NULL);
        iso_from_epoch(now_epoch, stamp);
    }
    char cutoff[40];
    iso_from_epoch(now_epoch - older_than_seconds, cutoff);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(
            grants->db,
            "UPDATE resource_grants SET released_at=?"
            " WHERE released_at IS NULL AND granted_at < ?",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, stamp, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, cutoff, -1, SQLITE_TRANSIENT);
    int rc = sqlite3_step(stmt);
    int changed = sqlite3_changes(grants->db);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? (int64_t)changed : -1;
}

const char *bf_admission_verdict_name(BfAdmissionVerdict verdict) {
    switch (verdict) {
        case BF_ADMISSION_ADMIT: return "admit";
        case BF_ADMISSION_DEFER: return "defer";
        case BF_ADMISSION_REJECT: return "reject";
    }
    return "unknown";
}
