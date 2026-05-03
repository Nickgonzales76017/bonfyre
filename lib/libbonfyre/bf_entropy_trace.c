/* bf_entropy_trace.c — step-by-step JSONL trace writer for HVCP runs.
 *
 * Each step is one JSON object on one line (JSONL/ndjson):
 *
 *   {"step":42,"q_hash":"a3f4b5…","H":-14.233,"K":0.812,"V":-15.045,
 *    "grad_norm":3.1e-7,"gap":1,"mounted":["a91f…"],"candidates":128,
 *    "collisions":9,"nearest":["ref/lagrangian","ref/attention"],
 *    "entropy":0.77}
 *
 * Traces are replayable: same sequence of bf_trace_write() calls over
 * the same physics run always produces the same file content (up to
 * floating-point repr).
 *
 * Used for: diff, cherry-pick, rebase, trace-summary, branch-at.
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <limits.h>
#include "bonfyre.h"

/* ── open / close ─────────────────────────────────────────────── */

BfEntropyTrace *bf_trace_open(const char *path, int append) {
    if (!path) return NULL;
    BfEntropyTrace *t = calloc(1, sizeof(BfEntropyTrace));
    if (!t) return NULL;
    snprintf(t->path, sizeof(t->path), "%s", path);
    t->fp = fopen(path, append ? "ab" : "wb");
    if (!t->fp) { free(t); return NULL; }
    t->events = 0;
    return t;
}

void bf_trace_close(BfEntropyTrace *t) {
    if (!t) return;
    if (t->fp) { fflush(t->fp); fclose(t->fp); }
    free(t);
}

/* ── write one event ──────────────────────────────────────────── */

static void hex64_(char out[65], const uint8_t h[32]) {
    static const char hc[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        out[i*2]   = hc[h[i] >> 4];
        out[i*2+1] = hc[h[i] & 0xf];
    }
    out[64] = '\0';
}

/* Escape a string for JSON — only handle the characters that could
 * appear in hash strings or short ref names.  Full RFC 7159 escaping
 * for anything outside 0x20-0x7e. */
static void json_escape_(char *dst, size_t dsz, const char *src) {
    size_t o = 0;
    for (const char *p = src; *p && o+4 < dsz; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '"')       { dst[o++]='\\'; dst[o++]='"'; }
        else if (c == '\\') { dst[o++]='\\'; dst[o++]='\\'; }
        else if (c < 0x20)  { dst[o++]='?'; }
        else                { dst[o++] = (char)c; }
    }
    dst[o] = '\0';
}

int bf_trace_write(BfEntropyTrace *t, const BfTraceEvent *ev) {
    if (!t || !ev || !t->fp) return -1;

    char q_hex[65] = "0000000000000000000000000000000000000000000000000000000000000000";
    if (ev->q_hash) hex64_(q_hex, ev->q_hash);

    /* start object */
    fprintf(t->fp,
        "{\"step\":%llu,\"q_hash\":\"%s\","
        "\"H\":%.6g,\"K\":%.6g,\"V\":%.6g,"
        "\"grad_norm\":%.6g,\"gap\":%d",
        (unsigned long long)ev->step,
        q_hex,
        (double)ev->H, (double)ev->K, (double)ev->V,
        (double)ev->grad_norm,
        ev->gap ? 1 : 0
    );

    /* mounted array */
    fprintf(t->fp, ",\"mounted\":[");
    for (int i = 0; i < ev->n_mounted && ev->mounted[i]; i++) {
        char esc[128]; json_escape_(esc, sizeof(esc), ev->mounted[i]);
        if (i) fputc(',', t->fp);
        fprintf(t->fp, "\"%s\"", esc);
    }
    fputc(']', t->fp);

    /* candidates / collisions */
    fprintf(t->fp, ",\"candidates\":%d,\"collisions\":%d",
            ev->candidates, ev->collisions);

    /* nearest array */
    fprintf(t->fp, ",\"nearest\":[");
    for (int i = 0; i < ev->n_nearest && ev->nearest[i]; i++) {
        char esc[128]; json_escape_(esc, sizeof(esc), ev->nearest[i]);
        if (i) fputc(',', t->fp);
        fprintf(t->fp, "\"%s\"", esc);
    }
    fputc(']', t->fp);

    /* entropy */
    fprintf(t->fp, ",\"entropy\":%.6g}\n", (double)ev->entropy);

    t->events++;
    return 0;
}

/* ── summary ──────────────────────────────────────────────────── */

/* Lightweight JSONL parser — reads key:value pairs from a trace line.
 * Returns the float value of a numeric key, or NAN if not found. */
static double jl_float_(const char *line, const char *key) {
    char search[64];
    snprintf(search, sizeof(search), "\"%s\":", key);
    const char *p = strstr(line, search);
    if (!p) return NAN;
    p += strlen(search);
    while (*p == ' ') p++;
    return atof(p);
}

static long long jl_ll_(const char *line, const char *key) {
    char search[64];
    snprintf(search, sizeof(search), "\"%s\":", key);
    const char *p = strstr(line, search);
    if (!p) return -1LL;
    p += strlen(search);
    while (*p == ' ') p++;
    return atoll(p);
}

typedef struct {
    uint64_t n_steps;
    uint64_t n_gaps;
    uint32_t n_mounts;       /* unique mount events */
    float    H_first, H_last, H_min, H_max;
    float    H_drift;
    float    entropy_sum, entropy_min, entropy_max;
    uint32_t entropy_n;
    uint64_t candidate_sum;
    uint32_t candidate_n;
    uint32_t collision_sum;
} TraceSummary;

static int summary_cb_(const char *line, void *ctx) {
    TraceSummary *s = ctx;
    long long step = jl_ll_(line, "step");
    (void)step;

    double H       = jl_float_(line, "H");
    double gap     = jl_float_(line, "gap");
    double cand    = jl_float_(line, "candidates");
    double col     = jl_float_(line, "collisions");
    double entropy = jl_float_(line, "entropy");
    const char *mp = strstr(line, "\"mounted\":[");
    int has_mount  = mp && strstr(mp+11, "\"") && strstr(mp+11, "\"") < strchr(mp+11, ']');

    if (isnan(H)) return 0;

    if (s->n_steps == 0) {
        s->H_first = s->H_min = s->H_max = (float)H;
    } else {
        if ((float)H < s->H_min) s->H_min = (float)H;
        if ((float)H > s->H_max) s->H_max = (float)H;
    }
    s->H_last = (float)H;
    s->n_steps++;

    if (!isnan(gap) && gap > 0.5) s->n_gaps++;
    if (has_mount) s->n_mounts++;
    if (!isnan(cand))    { s->candidate_sum += (uint64_t)cand; s->candidate_n++; }
    if (!isnan(col))     { s->collision_sum += (uint32_t)col; }
    if (!isnan(entropy)) {
        s->entropy_sum += (float)entropy;
        if (s->entropy_n == 0) s->entropy_min = s->entropy_max = (float)entropy;
        if ((float)entropy < s->entropy_min) s->entropy_min = (float)entropy;
        if ((float)entropy > s->entropy_max) s->entropy_max = (float)entropy;
        s->entropy_n++;
    }
    return 0;
}

int bf_trace_iterate(const char *path,
                     int (*cb)(const char *line, void *ctx),
                     void *ctx) {
    FILE *f = fopen(path, "rb");
    if (!f) return -1;
    char line[2048];
    while (fgets(line, sizeof(line), f)) {
        /* skip blank lines and comments */
        if (line[0] == '\n' || line[0] == '#' || line[0] == '\0') continue;
        int rc = cb(line, ctx);
        if (rc != 0) { fclose(f); return rc; }
    }
    fclose(f);
    return 0;
}

int bf_trace_summary(const char *path, FILE *out) {
    if (!path || !out) return -1;
    TraceSummary s; memset(&s, 0, sizeof(s));

    int rc = bf_trace_iterate(path, summary_cb_, &s);
    if (rc < 0) { fprintf(out, "trace: cannot open %s\n", path); return -1; }
    if (s.n_steps == 0) { fprintf(out, "trace: empty file %s\n", path); return 0; }

    float mean_entropy = s.entropy_n > 0 ? s.entropy_sum / (float)s.entropy_n : 0;
    float mean_cand    = s.candidate_n > 0
                       ? (float)s.candidate_sum / (float)s.candidate_n : 0;
    float h_drift = fabsf(s.H_last - s.H_first);

    fprintf(out, "trace summary: %s\n", path);
    fprintf(out, "  steps       : %llu\n", (unsigned long long)s.n_steps);
    fprintf(out, "  gaps        : %llu  (%.1f%%)\n",
            (unsigned long long)s.n_gaps,
            100.0f * (float)s.n_gaps / (float)s.n_steps);
    fprintf(out, "  mount events: %u\n",  s.n_mounts);
    fprintf(out, "  H first     : %.6g\n", s.H_first);
    fprintf(out, "  H last      : %.6g\n", s.H_last);
    fprintf(out, "  H range     : [%.6g, %.6g]  drift=%.4g\n",
            s.H_min, s.H_max, h_drift);
    fprintf(out, "  entropy avg : %.4f  range=[%.4f, %.4f]\n",
            mean_entropy, s.entropy_min, s.entropy_max);
    fprintf(out, "  candidates  : %.1f avg/step\n", mean_cand);
    fprintf(out, "  collisions  : %u total\n", s.collision_sum);
    return 0;
}

/* ── trace_branch_at ───────────────────────────────────────────── *
 * Parse "gap:N" specifier: returns step number of the Nth gap (1-indexed).
 * Returns UINT64_MAX if not found. */
typedef struct { int target_n; int found_n; uint64_t result_step; } BranchAtCtx;

static int branch_at_cb_(const char *line, void *ctx) {
    BranchAtCtx *b = ctx;
    double gap = jl_float_(line, "gap");
    if (!isnan(gap) && gap > 0.5) {
        b->found_n++;
        if (b->found_n == b->target_n) {
            b->result_step = (uint64_t)jl_ll_(line, "step");
            return 1; /* stop */
        }
    }
    return 0;
}

uint64_t bf_trace_gap_step(const char *path, int gap_n) {
    BranchAtCtx b = {.target_n = gap_n, .found_n = 0,
                     .result_step = UINT64_MAX};
    bf_trace_iterate(path, branch_at_cb_, &b);
    return b.result_step;
}
