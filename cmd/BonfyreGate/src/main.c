/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

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
#include <ctype.h>
#include <errno.h>
#include "bf_lex.h"
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>   /* strncasecmp: an entitlement is not case */
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#define VERSION "1.0.0"

static void iso_timestamp(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

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

/* ---------- signing ----------
 *
 * A key is a statement by the institution: these entitlements, for this org,
 * until this date. Until 2026-11-07 nothing made it one -- the JSON carried
 * no signature, so editing allowed_ops in key.json granted the edited
 * operation. A key now carries an Ed25519 signature by the institution
 * identity (cmd/BonfyreSign) over every field but the signature itself, and
 * check/guard verify it against a public key this machine was told to trust
 * (BONFYRE_GATE_TRUST: 64 hex or a file holding them). Never against a key the
 * file names: whoever edits the file can name their own. With nothing trusted,
 * every key is refused.
 *
 *   issue  needs BONFYRE_GATE_KEY_DIR, the bonfyre-sign key directory
 *   BONFYRE_SIGN_BIN overrides the bonfyre-sign found on PATH
 */
#include <sys/wait.h>
#include "bf_jv.h"
#include "bf_jsonw.h"

static int run_capture(char *const argv[], char *out, size_t cap) {
    int fds[2];
    if (pipe(fds) != 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(fds[0]); close(fds[1]); return -1; }
    if (pid == 0) {
        dup2(fds[1], 1); close(fds[0]); close(fds[1]);
        execvp(argv[0], argv); _exit(127);
    }
    close(fds[1]);
    size_t len = 0; ssize_t got;
    while (len + 1 < cap && (got = read(fds[0], out + len, cap - 1 - len)) > 0) len += (size_t)got;
    out[len] = '\0';
    char sink[256]; while (read(fds[0], sink, sizeof sink) > 0) {}
    close(fds[0]);
    int st = 0; if (waitpid(pid, &st, 0) < 0) return -1;
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static const char *sign_bin(void) {
    const char *b = getenv("BONFYRE_SIGN_BIN");
    return b && *b ? b : "bonfyre-sign";
}

/* The signed body: the key without its signature, keys sorted, no spaces. */
static char *signed_body(const bf_jv *key) {
    bf_jv *c = bf_jv_clone(key);
    bf_jv_del(c, "signature"); bf_jv_del(c, "signed_by");
    size_t n = 0; char *body = bf_jv_dump(c, 0, 1, &n);
    bf_jv_free(c);
    return body;
}

static int write_temp(const char *body, char *path, size_t cap) {
    const char *dir = getenv("TMPDIR"); if (!dir || !*dir) dir = "/tmp";
    snprintf(path, cap, "%s/bonfyre-gate.XXXXXX", dir);
    int fd = mkstemp(path); if (fd < 0) return -1;
    size_t n = strlen(body);
    int ok = write(fd, body, n) == (ssize_t)n;
    close(fd);
    if (!ok) { unlink(path); return -1; }
    return 0;
}

static int trusted_public(char hex[65]) {
    const char *t = getenv("BONFYRE_GATE_TRUST");
    if (!t || !*t) return -1;
    char buf[256] = {0};
    int is_hex = strlen(t) == 64;
    for (int i = 0; is_hex && i < 64; i++) is_hex = isxdigit((unsigned char)t[i]) != 0;
    if (is_hex) snprintf(buf, sizeof buf, "%s", t);
    else { char *f = bf_read_file(t, NULL); if (!f) return -1; snprintf(buf, sizeof buf, "%.64s", f); free(f); }
    for (int i = 0; i < 64; i++) if (!isxdigit((unsigned char)buf[i])) return -1;
    memcpy(hex, buf, 64); hex[64] = '\0';
    return 0;
}

/* 0 when the key's signature verifies under the trusted public key. */
static int verify_key(const bf_jv *key, const char **why) {
    char pub[65];
    if (trusted_public(pub) != 0) { *why = "no trusted gate public key (set BONFYRE_GATE_TRUST)"; return -1; }
    const char *sig = bf_jv_s(bf_jv_get(key, "signature"));
    if (!sig || strlen(sig) != 128) { *why = "key is unsigned"; return -1; }
    char *body = signed_body(key), path[1024];
    if (!body || write_temp(body, path, sizeof path) != 0) { free(body); *why = "cannot stage key for verification"; return -1; }
    free(body);
    char *argv[] = {(char *)sign_bin(), "verify", "--file", path, "--public", pub, "--signature", (char *)sig, NULL};
    char out[2048];
    int rc = run_capture(argv, out, sizeof out);
    unlink(path);
    if (rc != 0) { *why = rc == 127 ? "bonfyre-sign is not available" : "signature does not verify under the trusted key"; return -1; }
    return 0;
}

static bf_jv *load_key(const char *path) {
    size_t n = 0; char *text = bf_read_file(path, &n);
    if (!text) return NULL;
    char err[128]; bf_jv *v = bf_jv_parse(text, n, err, sizeof err);
    free(text);
    return v;
}

static int cmd_issue(const char *tier, const char *org, const char *out) {
    const char *key_dir = getenv("BONFYRE_GATE_KEY_DIR");
    if (!key_dir || !*key_dir) { fprintf(stderr, "[gate] issuing needs BONFYRE_GATE_KEY_DIR, the institution's bonfyre-sign key directory\n"); return 2; }
    char ts[64], expires[64], key_id[128];
    iso_timestamp(ts, sizeof(ts));
    TierEntitlements e = tier_config(tier);
    iso_timestamp_future(expires, sizeof(expires), e.days_valid);
    generate_key_id(tier, org, ts, key_id, sizeof(key_id));

    bf_jv *key = bf_jv_obj(), *ent = bf_jv_obj();
    bf_jv_set(key, "key_id", bf_jv_str(key_id));
    bf_jv_set(key, "version", bf_jv_str(VERSION));
    bf_jv_set(key, "tier", bf_jv_str(tier));
    bf_jv_set(key, "org", bf_jv_str(org));
    bf_jv_set(key, "issued_at", bf_jv_str(ts));
    bf_jv_set(key, "expires_at", bf_jv_str(expires));
    bf_jv_set(ent, "max_artifacts", bf_jv_num(e.max_artifacts));
    bf_jv_set(ent, "max_bytes", bf_jv_num((double)e.max_bytes));
    bf_jv_set(ent, "rate_limit_hr", bf_jv_num(e.rate_limit_hr));
    bf_jv_set(ent, "allowed_ops", bf_jv_str(e.allowed_ops));
    bf_jv_set(key, "entitlements", ent);
    bf_jv_set(key, "status", bf_jv_str("active"));

    char *body = signed_body(key), path[1024];
    if (!body || write_temp(body, path, sizeof path) != 0) { free(body); bf_jv_free(key); fprintf(stderr, "[gate] cannot stage key for signing\n"); return 1; }
    free(body);
    char *argv[] = {(char *)sign_bin(), "sign", "--key-dir", (char *)key_dir, "--file", path, NULL};
    char att[4096];
    int rc = run_capture(argv, att, sizeof att);
    unlink(path);
    char err[128]; bf_jv *a = rc == 0 ? bf_jv_parse(att, strlen(att), err, sizeof err) : NULL;
    const char *sig = a ? bf_jv_s(bf_jv_get(a, "signature")) : NULL, *pub = a ? bf_jv_s(bf_jv_get(a, "public_key")) : NULL;
    if (!sig || !pub) { fprintf(stderr, "[gate] bonfyre-sign did not sign the key (exit %d)\n", rc); bf_jv_free(a); bf_jv_free(key); return 1; }
    bf_jv_set(key, "signature", bf_jv_str(sig));
    bf_jv_set(key, "signed_by", bf_jv_str(pub));
    bf_jv_free(a);

    size_t n = 0; char *json = bf_jv_dump(key, 2, 0, &n);
    bf_jv_free(key);
    FILE *f = fopen(out, "w");
    if (!f) { free(json); fprintf(stderr, "Cannot write: %s\n", out); return 1; }
    fwrite(json, 1, n, f); fputc('\n', f); fclose(f); free(json);

    fprintf(stderr, "[gate] Issued %s key: %s (expires %s)\n", tier, key_id, expires);
    printf("%s\n", key_id);
    return 0;
}

static int cmd_check(const char *key_path) {
    bf_jv *key = load_key(key_path);
    if (!key) { fprintf(stderr, "Cannot read: %s\n", key_path); return 1; }
    const char *why = NULL;
    int bad = verify_key(key, &why) != 0;
    const char *key_id = bf_jv_s(bf_jv_get(key, "key_id")), *tier = bf_jv_s(bf_jv_get(key, "tier"));
    const char *org = bf_jv_s(bf_jv_get(key, "org")), *expires = bf_jv_s(bf_jv_get(key, "expires_at"));
    const char *status = bf_jv_s(bf_jv_get(key, "status")), *ops = bf_jv_s(bf_jv_path(key, "entitlements.allowed_ops"));
    char now[64]; iso_timestamp(now, sizeof(now));
    int expired = !expires || strcmp(now, expires) > 0;
    int revoked = status && strcmp(status, "revoked") == 0;

    printf("Key:     %s\n", key_id ? key_id : "");
    printf("Tier:    %s\n", tier ? tier : "");
    printf("Org:     %s\n", org ? org : "");
    printf("Expires: %s\n", expires ? expires : "");
    printf("Status:  %s\n", bad ? "INVALID" : revoked ? "REVOKED" : expired ? "EXPIRED" : "ACTIVE");
    printf("Ops:     %s\n", ops ? ops : "");
    if (bad) fprintf(stderr, "[gate] %s\n", why);
    bf_jv_free(key);
    return (bad || expired || revoked) ? 2 : 0;
}

/* Is this operation one of the entitled ones?
 *
 * This was `strstr(ops, operation)`, which is a substring test against the
 * whole comma-joined list, and it failed in both directions at once.
 *
 * It failed open: on a free key entitled to "Ingest,Brief,Proof", the operation
 * "e" is allowed, and so is "In", "rie", "f,Proof" and the entire list passed
 * as a single operation name. Any fragment of any entitlement is an
 * entitlement, which is not a check, it is a coincidence.
 *
 * It failed closed: the entitlement vocabulary is capitalised ("Ingest") and
 * the capability registry the estate actually indexes is lowercase ("ingest"),
 * so every one of the 103 indexed capabilities was denied on free and pro keys.
 * Only enterprise worked, and only because its allowed_ops is "*" and never
 * reaches this comparison -- which is why the tier anyone would test with was
 * the one tier that could not expose the bug.
 *
 * An entitlement is an element of a comma-separated list, matched whole and
 * without regard to case. Surrounding spaces are ignored because a list is
 * written by a person. */
/* allowed_ops is a field of the entitlements object, not of the key, and is
 * addressed as such. Reading it as a top-level key found it anyway while the
 * shared reader searched flat; when that reader learned about depth, this went
 * silently empty and `check` printed a blank Ops line that nothing noticed. A
 * blank entitlement list denies everything, so the gate failed closed, which is
 * the safe direction and still wrong. */
static int op_entitled(const char *ops, const char *operation) {
    size_t want = operation ? strlen(operation) : 0;
    if (!ops || !want) return 0;
    for (const char *p = ops; *p; ) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        const char *start = p;
        while (len && (*start == ' ' || *start == '\t')) { start++; len--; }
        while (len && (start[len-1] == ' ' || start[len-1] == '\t')) len--;
        if (len == want && !strncasecmp(start, operation, want)) return 1;
        if (!comma) break;
        p = comma + 1;
    }
    return 0;
}

static int cmd_guard(const char *key_path, const char *operation) {
    bf_jv *key = load_key(key_path);
    if (!key) { fprintf(stderr, "DENIED: cannot read key\n"); return 1; }
    const char *why = NULL;
    if (verify_key(key, &why) != 0) { fprintf(stderr, "DENIED: %s\n", why); bf_jv_free(key); return 1; }
    const char *status = bf_jv_s(bf_jv_get(key, "status")), *expires = bf_jv_s(bf_jv_get(key, "expires_at"));
    const char *ops = bf_jv_s(bf_jv_path(key, "entitlements.allowed_ops"));
    char now[64]; iso_timestamp(now, sizeof(now));
    int rc = 1;
    if (status && strcmp(status, "revoked") == 0) fprintf(stderr, "DENIED: key revoked\n");
    else if (!expires || strcmp(now, expires) > 0) fprintf(stderr, "DENIED: key expired\n");
    else if (!ops || (strcmp(ops, "*") != 0 && !op_entitled(ops, operation)))
        fprintf(stderr, "DENIED: operation '%s' not in entitlements\n", operation);
    else { fprintf(stderr, "ALLOWED: %s\n", operation); rc = 0; }
    bf_jv_free(key);
    return rc;
}

/* ---------- main ---------- */

static void usage(void) {
    fprintf(stderr,
        "BonfyreGate — license enforcement\n\n"
        "Usage:\n"
        "  bonfyre-gate issue --tier free|pro|enterprise [--org NAME] [--out F]\n"
        "  bonfyre-gate check <key.json>\n"
        "  bonfyre-gate guard <key.json> --op OPERATION\n"
        "  bonfyre-gate layer <artifact_id> [--root DIR]\n"
        "  bonfyre-gate layer-op <operation> <artifact_id> [--root DIR]\n"
    );
}

int main(int argc, char *argv[]) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    enum {
        GT_CHECK, GT_GUARD, GT_ISSUE, GT_LAYER, GT_LAYER_OP,
    };
    static const BfCommandEntry gate_commands[] = {
        {"check", GT_CHECK, 0, NULL},
        {"guard", GT_GUARD, 0, NULL},
        {"issue", GT_ISSUE, 0, NULL},
        {"layer", GT_LAYER, 0, NULL},
        {"layer-op", GT_LAYER_OP, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(gate_commands,
        sizeof(gate_commands) / sizeof(gate_commands[0]), argv[1]);
    if (argc >= 3 && command && command->id == GT_LAYER) {
        const char *root = NULL;
        char *json = NULL, *out = NULL;
        for (int i=1;i<argc-1;i++) if (strcmp(argv[i],"--root")==0) { root = argv[i+1]; break; }
        if (bf_layer_load_json(root, argv[2], &json) != 0) return 1;
        if (bf_layer_gate_json(json, "inspect", &out) != 0) { free(json); return 1; }
        puts(out);
        free(out);
        free(json);
        return 0;
    }
    if (argc >= 4 && command && command->id == GT_LAYER_OP) {
        const char *root = NULL;
        char *json = NULL, *out = NULL;
        for (int i=1;i<argc-1;i++) if (strcmp(argv[i],"--root")==0) { root = argv[i+1]; break; }
        if (bf_layer_load_json(root, argv[3], &json) != 0) return 1;
        if (bf_layer_gate_json(json, argv[2], &out) != 0) { free(json); return 1; }
        puts(out);
        free(out);
        free(json);
        return 0;
    }

    if (argc >= 2 && command && command->id == GT_ISSUE) {
        const char *tier = "free", *org = "default", *out = "key.json";
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--tier") == 0) tier = argv[i+1];
            if (strcmp(argv[i], "--org") == 0) org = argv[i+1];
            if (strcmp(argv[i], "--out") == 0) out = argv[i+1];
        }
        return cmd_issue(tier, org, out);
    }
    if (argc >= 3 && command && command->id == GT_CHECK)
        return cmd_check(argv[2]);
    if (argc >= 3 && command && command->id == GT_GUARD) {
        const char *op = NULL;
        for (int i = 3; i < argc - 1; i++)
            if (strcmp(argv[i], "--op") == 0) op = argv[i+1];
        if (!op) { fprintf(stderr, "guard requires --op OPERATION\n"); return 1; }
        return cmd_guard(argv[2], op);
    }

    usage();
    return 1;
}
