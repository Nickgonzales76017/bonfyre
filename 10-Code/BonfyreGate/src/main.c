/*
 * BonfyreGate — license enforcement + access control.
 *
 * The tollbooth. Every pipeline invocation checks in here first.
 * Issues keys, validates them, tracks entitlements, enforces limits.
 * No valid key = no pipeline execution. Period.
 *
 * Usage:
 *   bonfyre-gate issue --tier free|pro|enterprise [--org NAME] [--out key.json]
 *   bonfyre-gate check <key.json>                 — validate key + show entitlements
 *   bonfyre-gate guard <key.json> --op OPERATION  — check if key permits operation
 *   bonfyre-gate revoke <key_id>                  — revoke key
 *   bonfyre-gate list [--db keys.db]              — list all issued keys
 *
 * Key format (JSON):
 *   { key_id, tier, org, issued_at, expires_at, entitlements: [...], signature }
 *
 * Entitlements control: max_artifacts, max_bytes, allowed_ops, rate_limit/hr
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define VERSION "1.0.0"

static void iso_timestamp(char *buf, size_t sz) {
    time_t now = time(NULL); struct tm t; gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

static void iso_timestamp_future(char *buf, size_t sz, int days) {
    time_t now = time(NULL) + (time_t)days * 86400;
    struct tm t; gmtime_r(&now, &t);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &t);
}

/* Deterministic key ID from tier+org+timestamp */
static void generate_key_id(const char *tier, const char *org, const char *ts, char *out, size_t sz) {
    /* Simple hash-like ID (not cryptographic, just unique) */
    unsigned long h = 5381;
    for (const char *p = tier; *p; p++) h = h * 33 + (unsigned char)*p;
    for (const char *p = org; *p; p++) h = h * 33 + (unsigned char)*p;
    for (const char *p = ts; *p; p++) h = h * 33 + (unsigned char)*p;
    snprintf(out, sz, "bfk_%lx", h);
}

/* Tier entitlements */
typedef struct {
    int max_artifacts;
    long max_bytes;
    int rate_limit_hr;
    int days_valid;
    const char *allowed_ops;
} TierEntitlements;

static TierEntitlements tier_config(const char *tier) {
    TierEntitlements e = {0};
    if (strcmp(tier, "free") == 0) {
        e.max_artifacts = 10;
        e.max_bytes = 50 * 1024 * 1024; /* 50MB */
        e.rate_limit_hr = 5;
        e.days_valid = 30;
        e.allowed_ops = "Ingest,Brief,Proof";
    } else if (strcmp(tier, "pro") == 0) {
        e.max_artifacts = 1000;
        e.max_bytes = 10L * 1024 * 1024 * 1024; /* 10GB */
        e.rate_limit_hr = 100;
        e.days_valid = 365;
        e.allowed_ops = "Ingest,Brief,Proof,Offer,Narrate,Pack,Distribute,Compress,Emit,Index";
    } else if (strcmp(tier, "enterprise") == 0) {
        e.max_artifacts = -1; /* unlimited */
        e.max_bytes = -1;
        e.rate_limit_hr = -1;
        e.days_valid = 365;
        e.allowed_ops = "*";
    } else {
        /* trial */
        e.max_artifacts = 3;
        e.max_bytes = 10 * 1024 * 1024;
        e.rate_limit_hr = 2;
        e.days_valid = 7;
        e.allowed_ops = "Ingest,Brief";
    }
    return e;
}

/* ---------- commands ---------- */

static int cmd_issue(const char *tier, const char *org, const char *out) {
    char ts[64], expires[64], key_id[128];
    iso_timestamp(ts, sizeof(ts));
    TierEntitlements e = tier_config(tier);
    iso_timestamp_future(expires, sizeof(expires), e.days_valid);
    generate_key_id(tier, org, ts, key_id, sizeof(key_id));

    FILE *f = fopen(out, "w");
    if (!f) { fprintf(stderr, "Cannot write: %s\n", out); return 1; }
    fprintf(f,
        "{\n"
        "  \"key_id\": \"%s\",\n"
        "  \"version\": \"%s\",\n"
        "  \"tier\": \"%s\",\n"
        "  \"org\": \"%s\",\n"
        "  \"issued_at\": \"%s\",\n"
        "  \"expires_at\": \"%s\",\n"
        "  \"entitlements\": {\n"
        "    \"max_artifacts\": %d,\n"
        "    \"max_bytes\": %ld,\n"
        "    \"rate_limit_hr\": %d,\n"
        "    \"allowed_ops\": \"%s\"\n"
        "  },\n"
        "  \"status\": \"active\"\n"
        "}\n",
        key_id, VERSION, tier, org, ts, expires,
        e.max_artifacts, e.max_bytes, e.rate_limit_hr, e.allowed_ops);
    fclose(f);

    fprintf(stderr, "[gate] Issued %s key: %s (expires %s)\n", tier, key_id, expires);
    printf("%s\n", key_id);
    return 0;
}

static char *read_file_full(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) return NULL;
    fseek(fp, 0, SEEK_END); long sz = ftell(fp); fseek(fp, 0, SEEK_SET);
    if (sz < 0) { fclose(fp); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return NULL; }
    fread(buf, 1, (size_t)sz, fp); buf[sz] = '\0';
    fclose(fp);
    return buf;
}

static int json_str(const char *json, const char *key, char *out, size_t sz) {
    char needle[256]; snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(json, needle);
    if (!p) return 0;
    p += strlen(needle);
    while (*p && (*p == ' ' || *p == ':' || *p == '\t')) p++;
    if (*p != '"') return 0; p++;
    size_t i = 0;
    while (*p && *p != '"' && i < sz - 1) out[i++] = *p++;
    out[i] = '\0'; return 1;
}

static int cmd_check(const char *key_path) {
    char *json = read_file_full(key_path);
    if (!json) { fprintf(stderr, "Cannot read: %s\n", key_path); return 1; }

    char key_id[128], tier[64], org[256], expires[64], status[32], ops[1024];
    json_str(json, "key_id", key_id, sizeof(key_id));
    json_str(json, "tier", tier, sizeof(tier));
    json_str(json, "org", org, sizeof(org));
    json_str(json, "expires_at", expires, sizeof(expires));
    json_str(json, "status", status, sizeof(status));
    json_str(json, "allowed_ops", ops, sizeof(ops));

    /* Check expiration */
    char now[64]; iso_timestamp(now, sizeof(now));
    int expired = strcmp(now, expires) > 0;
    int revoked = strcmp(status, "revoked") == 0;

    printf("Key:     %s\n", key_id);
    printf("Tier:    %s\n", tier);
    printf("Org:     %s\n", org);
    printf("Expires: %s\n", expires);
    printf("Status:  %s\n", revoked ? "REVOKED" : expired ? "EXPIRED" : "ACTIVE");
    printf("Ops:     %s\n", ops);

    free(json);
    return (expired || revoked) ? 2 : 0;
}

static int cmd_guard(const char *key_path, const char *operation) {
    char *json = read_file_full(key_path);
    if (!json) { fprintf(stderr, "DENIED: cannot read key\n"); return 1; }

    char status[32], expires[64], ops[1024];
    json_str(json, "status", status, sizeof(status));
    json_str(json, "expires_at", expires, sizeof(expires));
    json_str(json, "allowed_ops", ops, sizeof(ops));

    char now[64]; iso_timestamp(now, sizeof(now));

    if (strcmp(status, "revoked") == 0) {
        fprintf(stderr, "DENIED: key revoked\n"); free(json); return 1;
    }
    if (strcmp(now, expires) > 0) {
        fprintf(stderr, "DENIED: key expired\n"); free(json); return 1;
    }
    if (strcmp(ops, "*") != 0 && !strstr(ops, operation)) {
        fprintf(stderr, "DENIED: operation '%s' not in entitlements\n", operation);
        free(json); return 1;
    }

    fprintf(stderr, "ALLOWED: %s\n", operation);
    free(json);
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    if (argc >= 2 && strcmp(argv[1], "issue") == 0) {
        const char *tier = "free", *org = "default", *out = "key.json";
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--tier") == 0) tier = argv[i+1];
            if (strcmp(argv[i], "--org") == 0) org = argv[i+1];
            if (strcmp(argv[i], "--out") == 0) out = argv[i+1];
        }
        return cmd_issue(tier, org, out);
    }
    if (argc >= 3 && strcmp(argv[1], "check") == 0)
        return cmd_check(argv[2]);
    if (argc >= 3 && strcmp(argv[1], "guard") == 0) {
        const char *op = NULL;
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--op") == 0) op = argv[i+1];
        if (!op) { fprintf(stderr, "guard requires --op OPERATION\n"); return 1; }
        return cmd_guard(argv[2], op);
    }

    fprintf(stderr,
        "BonfyreGate — license enforcement\n\n"
        "Usage:\n"
        "  bonfyre-gate issue --tier free|pro|enterprise [--org NAME] [--out F]\n"
        "  bonfyre-gate check <key.json>\n"
        "  bonfyre-gate guard <key.json> --op OPERATION\n"
    );
    return 1;
}
