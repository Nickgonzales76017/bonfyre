/* timegm and friends are GNU extensions, not POSIX; glibc needs _GNU_SOURCE
   while macOS declares them unconditionally. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/* POSIX interfaces used below are hidden by glibc under -std=c11 unless a
   feature macro is set. macOS exposes them regardless, which is why this
   whole class only surfaced on the Linux deployment host. */

/*
 * Native resource-admission controller. Pure decision kernel + a grant ledger
 * on the resource_grants store formerly shared with resource_admission.py.archive.
 */

#include "bf_admission.h"

#include <sqlite3.h>

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#ifdef __APPLE__
#include <mach/mach.h>
#include <sys/sysctl.h>
#else
#include <stdio.h>
#endif

#define BF_GIB ((int64_t)1 << 30)

struct BfAdmissionGrants {
    sqlite3 *db;
    int owns_db;
    FILE *err;
};

/* Preserves the archived resource_admission.py schema exactly. */
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

/* Unset, absent and zero are three states, and only two of them were
 * representable. `> 0 ? given : default` collapses "remove this limit" into
 * "I said nothing", so the CLI could accept --protected-floor-bytes 0, discard
 * it, and refuse a 2 KiB grant against the 10 GiB floor the caller had just
 * asked to drop. The floor itself is a real safety property and stays; what
 * changes is that asking to remove it is now something the struct can carry. */
static int64_t resolve_limit(int64_t given, int64_t fallback) {
    if (given > 0) return given;
    if (given < 0) return 0;   /* BF_ADMISSION_LIMIT_NONE: explicitly absent */
    return fallback;           /* unset */
}

static void resolve_policy(const BfAdmissionPolicy *in, BfAdmissionPolicy *out) {
    bf_admission_policy_defaults(out);
    if (in) {
        out->protected_floor_bytes =
            resolve_limit(in->protected_floor_bytes, out->protected_floor_bytes);
        out->per_plane_quota_bytes =
            resolve_limit(in->per_plane_quota_bytes, out->per_plane_quota_bytes);
        out->max_grant_bytes = resolve_limit(in->max_grant_bytes, out->max_grant_bytes);
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
    } else if (!request->plane || request->plane[0] == '\0' ||
               !request->kind || request->kind[0] == '\0') {
        verdict = BF_ADMISSION_REJECT;
        reason = "request must identify a plane and kind";
    } else if (request->estimated_bytes > policy.max_grant_bytes) {
        verdict = BF_ADMISSION_REJECT;
        reason = "request exceeds the single-grant ceiling";
    } else if (plane_committed_bytes + request->estimated_bytes > policy.per_plane_quota_bytes) {
        verdict = BF_ADMISSION_DEFER;
        reason = "would exceed the per-plane quota";
    } else if (request->estimated_bytes > spendable) {
        /* Distinguish "never" from "not now". */
        int64_t headroom_if_drained = free_bytes - policy.protected_floor_bytes;
        if (bf_admission_volume_is_memory(request->volume)) {
            /* Free disk only moves when someone writes or deletes; free memory
             * moves every time a browser tab closes. On the memory volume
             * "never" is the single-grant ceiling (physical RAM less the
             * floor), checked above, and everything else is "not now". */
            verdict = BF_ADMISSION_DEFER;
            reason = request->estimated_bytes > headroom_if_drained
                         ? "other programs are using the memory now; retry when it frees"
                         : "exceeds spendable memory; a loaded model holds the rest";
        } else if (request->estimated_bytes > headroom_if_drained) {
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

void bf_admission_policy_memory(BfAdmissionPolicy *policy) {
    if (!policy) {
        return;
    }
    /* A model is the only thing here that asks for gigabytes of RAM at once.
     * The floor is what the kernel, the window server and the always-on
     * services keep; a load that would eat into it waits instead. */
    int64_t physical = 0;
#ifdef __APPLE__
    size_t size = sizeof(physical);
    if (sysctlbyname("hw.memsize", &physical, &size, NULL, 0) != 0) physical = 0;
#else
    long pages = sysconf(_SC_PHYS_PAGES), page = sysconf(_SC_PAGESIZE);
    if (pages > 0 && page > 0) physical = (int64_t)pages * page;
#endif
    policy->protected_floor_bytes = BF_GIB + BF_GIB / 2;
    /* Never: more than this machine has, less what the system keeps. */
    policy->max_grant_bytes = physical > policy->protected_floor_bytes
                                  ? physical - policy->protected_floor_bytes : 24 * BF_GIB;
    policy->per_plane_quota_bytes = policy->max_grant_bytes;
}

int bf_admission_volume_is_memory(const char *volume) {
    return volume && strcmp(volume, BF_ADMISSION_MEMORY_VOLUME) == 0;
}

/* Memory a new resident could take without paging others out. On macOS this is
 * free + inactive + speculative + purgeable pages, the same pool Activity
 * Monitor calls available. Under critical memory pressure the answer is 0: the
 * compressor and swap are already absorbing the machine, and a model load then
 * is how this Mac hung. */
int64_t bf_admission_memory_available(void) {
#ifdef __APPLE__
    vm_statistics64_data_t vm;
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    vm_size_t page = 0;
    int level = 0;
    size_t level_size = sizeof(level);
    if (sysctlbyname("kern.memorystatus_vm_pressure_level", &level, &level_size, NULL, 0) == 0 &&
        level >= 4) {
        return 0;
    }
    if (host_page_size(mach_host_self(), &page) != KERN_SUCCESS ||
        host_statistics64(mach_host_self(), HOST_VM_INFO64, (host_info64_t)&vm, &count) != KERN_SUCCESS) {
        return -1;
    }
    return ((int64_t)vm.free_count + vm.inactive_count + vm.speculative_count + vm.purgeable_count) *
           (int64_t)page;
#else
    FILE *meminfo = fopen("/proc/meminfo", "r");
    char line[256];
    long long kib = -1;
    if (!meminfo) return -1;
    while (fgets(line, sizeof(line), meminfo))
        if (sscanf(line, "MemAvailable: %lld kB", &kib) == 1) break;
    fclose(meminfo);
    return kib < 0 ? -1 : (int64_t)kib * 1024;
#endif
}

int64_t bf_admission_free(const char *volume) {
    return bf_admission_volume_is_memory(volume) ? bf_admission_memory_available()
                                                 : bf_admission_disk_free(volume);
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
          " WHERE volume=? AND plane=? AND released_at IS NULL AND kind NOT LIKE '%:settled'"
        : "SELECT COALESCE(SUM(estimated_bytes),0) FROM resource_grants"
          " WHERE volume=? AND released_at IS NULL AND kind NOT LIKE '%:settled'";
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
        decision->verdict = BF_ADMISSION_DEFER;
        snprintf(decision->reason, sizeof(decision->reason),
                 "grant ledger write failed; work was not admitted");
        return BF_ADMISSION_DEFER;
    }
    sqlite3_bind_text(stmt, 1, request->plane, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, request->kind, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, volume, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 4, request->estimated_bytes);
    sqlite3_bind_text(stmt, 5, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 6, request->detail ? request->detail : "", -1, SQLITE_TRANSIENT);
    int inserted = sqlite3_step(stmt) == SQLITE_DONE;
    if (inserted && out_grant_id) *out_grant_id = sqlite3_last_insert_rowid(grants->db);
    sqlite3_finalize(stmt);
    if (!inserted) {
        decision->verdict = BF_ADMISSION_DEFER;
        snprintf(decision->reason, sizeof(decision->reason),
                 "grant ledger write failed; work was not admitted");
        return BF_ADMISSION_DEFER;
    }
    return BF_ADMISSION_ADMIT;
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

int bf_admission_settle_grant(BfAdmissionGrants *grants, int64_t grant_id) {
    sqlite3_stmt *stmt = NULL;
    int rc;
    if (!grants) {
        return -1;
    }
    if (sqlite3_prepare_v2(
            grants->db,
            "UPDATE resource_grants SET kind=kind||':settled' WHERE id=? AND released_at IS NULL"
            " AND kind NOT LIKE '%:settled'",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int64(stmt, 1, grant_id);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

int64_t bf_admission_release_plane(BfAdmissionGrants *grants, const char *plane, const char *now) {
    char stamp[40];
    sqlite3_stmt *stmt = NULL;
    int rc;
    if (!grants || !plane) {
        return -1;
    }
    if (!now || now[0] == '\0') {
        now_iso(stamp);
        now = stamp;
    }
    if (sqlite3_prepare_v2(
            grants->db,
            "UPDATE resource_grants SET released_at=? WHERE plane=? AND released_at IS NULL",
            -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, now, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, plane, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? (int64_t)sqlite3_changes(grants->db) : -1;
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

/* ---- GGUF shape ------------------------------------------------------- */

typedef struct GgufReader { FILE *file; int failed; } GgufReader;

static uint64_t gguf_u64(GgufReader *r) {
    uint64_t v = 0;
    if (fread(&v, 8, 1, r->file) != 1) r->failed = 1;
    return v;
}
static uint32_t gguf_u32(GgufReader *r) {
    uint32_t v = 0;
    if (fread(&v, 4, 1, r->file) != 1) r->failed = 1;
    return v;
}
static size_t gguf_scalar_size(uint32_t type) {
    switch (type) {
        case 0: case 1: case 7: return 1;
        case 2: case 3: return 2;
        case 4: case 5: case 6: return 4;
        case 10: case 11: case 12: return 8;
    }
    return 0;
}
static int64_t gguf_integer(GgufReader *r, uint32_t type) {
    unsigned char raw[8] = {0};
    size_t size = gguf_scalar_size(type);
    if (!size || type == 6 || type == 12 || fread(raw, size, 1, r->file) != 1) { r->failed = 1; return 0; }
    switch (type) {
        case 0: return raw[0];
        case 1: return (int8_t)raw[0];
        case 2: { uint16_t v; memcpy(&v, raw, 2); return v; }
        case 3: { int16_t v; memcpy(&v, raw, 2); return v; }
        case 4: { uint32_t v; memcpy(&v, raw, 4); return v; }
        case 5: { int32_t v; memcpy(&v, raw, 4); return v; }
        case 10: { uint64_t v; memcpy(&v, raw, 8); return (int64_t)v; }
        case 11: { int64_t v; memcpy(&v, raw, 8); return v; }
    }
    return 0;
}
/* Skips one value; for an array, returns its element count through count. */
static void gguf_skip(GgufReader *r, uint32_t type, uint64_t *count) {
    if (type == 8) { uint64_t n = gguf_u64(r); if (!r->failed && fseeko(r->file, (off_t)n, SEEK_CUR) != 0) r->failed = 1; return; }
    if (type == 9) {
        uint32_t inner = gguf_u32(r);
        uint64_t n = gguf_u64(r);
        if (count) *count = n;
        if (inner == 8 || inner == 9) { for (uint64_t i = 0; i < n && !r->failed; i++) gguf_skip(r, inner, NULL); }
        else if (!gguf_scalar_size(inner) || fseeko(r->file, (off_t)(n * gguf_scalar_size(inner)), SEEK_CUR) != 0) r->failed = 1;
        return;
    }
    if (!gguf_scalar_size(type) || fseeko(r->file, (off_t)gguf_scalar_size(type), SEEK_CUR) != 0) r->failed = 1;
}

int bf_admission_model_shape(const char *gguf_path, double kv_element_bytes, BfModelShape *out) {
    GgufReader r = {0};
    char magic[4];
    int64_t heads = 0, embedding = 0, key_length = 0;
    uint64_t kv_count;
    struct stat st;
    memset(out, 0, sizeof *out);
    if (!gguf_path || stat(gguf_path, &st) != 0 || !(r.file = fopen(gguf_path, "rb"))) return -1;
    out->file_bytes = (int64_t)st.st_size;
    if (fread(magic, 4, 1, r.file) != 1 || memcmp(magic, "GGUF", 4) != 0 || gguf_u32(&r) < 2) { fclose(r.file); return -1; }
    (void)gguf_u64(&r);                 /* tensor count */
    kv_count = gguf_u64(&r);
    for (uint64_t i = 0; i < kv_count && i < 100000 && !r.failed; i++) {
        char key[256] = {0};
        uint64_t length = gguf_u64(&r), elements = 0;
        uint32_t type;
        const char *suffix;
        if (r.failed || length >= sizeof key || fread(key, (size_t)length, 1, r.file) != 1) { r.failed = 1; break; }
        type = gguf_u32(&r);
        if (strcmp(key, "general.architecture") == 0 && type == 8) {
            uint64_t n = gguf_u64(&r);
            if (n >= sizeof out->architecture || fread(out->architecture, (size_t)n, 1, r.file) != 1) r.failed = 1;
            continue;
        }
        suffix = out->architecture[0] && strncmp(key, out->architecture, strlen(out->architecture)) == 0 &&
                 key[strlen(out->architecture)] == '.' ? key + strlen(out->architecture) + 1 : NULL;
        if (suffix && type != 8 && type != 9 && type != 6 && type != 12) {
            int64_t value = gguf_integer(&r, type);
            if (strcmp(suffix, "block_count") == 0) out->layers = value;
            else if (strcmp(suffix, "attention.head_count_kv") == 0) out->kv_heads = value;
            else if (strcmp(suffix, "attention.head_count") == 0) heads = value;
            else if (strcmp(suffix, "embedding_length") == 0) embedding = value;
            else if (strcmp(suffix, "attention.key_length") == 0) key_length = value;
            else if (strcmp(suffix, "context_length") == 0) out->trained_context = value;
            continue;
        }
        gguf_skip(&r, type, &elements);
        if (strcmp(key, "tokenizer.chat_template") == 0) out->chat = 1;
        if (strcmp(key, "tokenizer.ggml.tokens") == 0) out->vocabulary = (int64_t)elements;
    }
    fclose(r.file);
    if (r.failed || out->layers <= 0) return -1;
    if (out->kv_heads <= 0) out->kv_heads = heads;
    out->head_dim = key_length > 0 ? key_length : heads > 0 ? embedding / heads : 0;
    if (out->kv_heads <= 0 || out->head_dim <= 0) return -1;
    if (out->vocabulary <= 0) out->vocabulary = 32000;
    out->kv_bytes_per_token = (int64_t)(2.0 * (double)out->layers * (double)out->kv_heads * (double)out->head_dim *
                                        (kv_element_bytes > 0 ? kv_element_bytes : 2.0));
    /* Measured on Qwen1.5-MoE: 304 MiB compute at vocab 151936; the logits
     * buffer is 512 x vocab x 4 = 297 MiB of it. 64 MiB covers the rest. */
    out->compute_bytes = 512 * out->vocabulary * 4 + ((int64_t)64 << 20);
    return 0;
}

int64_t bf_admission_model_bytes(const BfModelShape *shape, int64_t context) {
    if (!shape || shape->file_bytes <= 0 || context <= 0) return -1;
    return shape->file_bytes + shape->compute_bytes + context * shape->kv_bytes_per_token;
}

int64_t bf_admission_model_context(const BfModelShape *shape, int64_t spendable, int64_t min_context, int64_t max_context) {
    int64_t top = max_context;
    if (shape->trained_context > 0 && shape->trained_context < top) top = shape->trained_context;
    top -= top % 1024;
    if (top < min_context) top = min_context;
    for (int64_t context = top; context >= min_context; context -= 1024)
        if (bf_admission_model_bytes(shape, context) <= spendable) return context;
    return 0;
}
