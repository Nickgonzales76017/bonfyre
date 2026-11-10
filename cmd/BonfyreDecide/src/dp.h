/*
 * dp.h — the DecisionPlane in C: typed judgments over exact, finite
 * frontiers, and the products built on it (bonfyre books, monitor, manifest).
 *
 * Port of components/BonfyreDecision/{decision-plane,typed-decision,
 * local-inference,gguf-owner}.mjs. Receipts, digests and printed output are
 * byte-identical to those modules (tests/parity.sh). The policy is the CUE
 * export of decision-plane.cue, compiled into the binary at build time.
 */
#ifndef DP_H
#define DP_H

#include <setjmp.h>
#include <stddef.h>
#include "bf_jv.h"
#include "bf_re.h"

/* ── errors: JS throw/catch ─────────────────────────────────────────────── */

typedef struct DpCatch { jmp_buf jb; char msg[2048]; struct DpCatch *prev; } DpCatch;
extern DpCatch *dp_catch_top;
#define DP_TRY(c) ((c)->prev = dp_catch_top, dp_catch_top = (c), setjmp((c)->jb) == 0)
#define DP_END(c) (dp_catch_top = (c)->prev)
void dp_fail(const char *fmt, ...) __attribute__((noreturn, format(printf, 1, 2)));

/* ── policy and digests ─────────────────────────────────────────────────── */

bf_jv *dp_policy(void);
/* decisionDigest: sha256 of JSON.stringify with entries sorted at every level. */
void   dp_digest(const bf_jv *v, char hex[65]);
char  *dp_digest_str(const bf_jv *v);                 /* malloc'd hex */
/* JSON.parse(sorted(v)): a deep copy with keys in sorted order. */
bf_jv *dp_sorted_clone(const bf_jv *v);
double dp_now_ms(void);
int    dp_is_hex64(const char *s);

/* ── typed decision algebra ─────────────────────────────────────────────── */

typedef struct { char *id; char type[8]; int offset, nopt; char **options; } DpField;
typedef struct { DpField *f; int n, width; } DpSchema;
typedef struct { DpSchema *schema; double *columns; bf_jv *moments; } DpDecoded;

DpSchema *dp_schema(const bf_jv *questions);
void      dp_schema_free(DpSchema *s);
DpDecoded dp_decode(DpSchema *s, const bf_jv *answers, int decimals);
void      dp_decoded_free(DpDecoded *d);

/* ── owners ─────────────────────────────────────────────────────────────── */

typedef struct DpCacheEntry { char key[65]; bf_jv *answers; size_t bytes; double producer_ms; } DpCacheEntry;

typedef struct DpOwner {
    char  *model;
    char   artifact_digest[65];
    int    deterministic;
    const char *question_semantics, *confidence_semantics;
    /* Returns {answers} as JS owners do; the plane validates it. */
    bf_jv *(*evaluate)(struct DpOwner *self, const bf_jv *state, const bf_jv *questions, const DpSchema *schema);
    void  (*close)(struct DpOwner *self);
    void  *ctx;
    DpCacheEntry *cache; int ncache, capcache; size_t cache_bytes;
} DpOwner;

/* ── the plane ──────────────────────────────────────────────────────────── */

bf_jv *dp_compile_case(const bf_jv *input, const bf_jv *policy);
bf_jv *dp_proposal(const bf_jv *receipt, const bf_jv *policy);
bf_jv *dp_assess_hosted(const bf_jv *compiled, const char *provider, int execute, const char *account_id, double timeout_ms, const bf_jv *policy);
/* assessDecisionCases(cases, {localEvaluator, execute: true, maxCalls, concurrency: 1}) */
bf_jv *dp_assess_cases(bf_jv *cases, DpOwner *local, long max_calls, const bf_jv *policy);

/* ── the local model owner (gguf-owner.mjs) ─────────────────────────────── */

DpOwner *dp_gguf_owner(const char *endpoint, const char *prefer);
/* product-cli localOwner(): acquired on first use, closed by the CLI. */
typedef struct { char *endpoint, *prefer; DpOwner *owner; int tried; char error[1024]; } DpLocal;
void     dp_local_init(DpLocal *l, const char *model_endpoint_flag, int model_flag_true, const char *model_flag);
DpOwner *dp_local_acquire(DpLocal *l);
void     dp_local_close(DpLocal *l);

/* ── loopback HTTP ──────────────────────────────────────────────────────── */

typedef struct { int status; char *body; size_t len; char error[256]; } DpHttp;
int  dp_http(const char *origin, const char *method, const char *path, const char *body, double timeout_ms, DpHttp *out);
void dp_http_free(DpHttp *h);

/* ── CLI helpers (product-cli.mjs) ──────────────────────────────────────── */

/* parseFlags: values are strings, or "\x01" for a bare flag (JS true). */
#define DP_TRUE "\x01"
typedef struct { char *key; char **vals; int n; } DpFlag;
typedef struct { DpFlag *f; int n; char **rest; int nrest; } DpFlags;
void        dp_flags(DpFlags *fl, int argc, char **argv, const char *const *repeatable);
const char *dp_flag(const DpFlags *fl, const char *key);           /* last value, NULL if absent */
int         dp_flag_list(const DpFlags *fl, const char *key, char ***vals);
int         dp_flag_is_true(const DpFlags *fl, const char *key);

char *dp_read_file(const char *path, size_t *len);                 /* NULL + errno on failure */
void  dp_write_file(const char *path, const char *data, size_t len);
char *dp_js_trim(const char *s);
char *dp_fmt(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
/* Number.prototype.toFixed(digits) */
char *dp_to_fixed(double v, int digits);

int books_main(int argc, char **argv);
int monitor_main(int argc, char **argv);
int manifest_main(int argc, char **argv);
int plane_main(int argc, char **argv);

#endif
