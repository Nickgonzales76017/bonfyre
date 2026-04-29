/*
 * bf_opts.h — Declarative CLI option parser with subcommand support
 *
 * Replaces hand-rolled strcmp() loops across 53 binaries.
 * Declare options as a static array, call bf_opts_parse(), done.
 *
 * Usage:
 *   bf_opt_t opts[] = {
 *       { "type",   't', BF_OPT_STRING, .str = &type,   "Input type" },
 *       { "out",    'o', BF_OPT_STRING, .str = &outdir,  "Output directory" },
 *       { "jobs",   'j', BF_OPT_INT,    .ival = &jobs,   "Parallel jobs" },
 *       { "verbose",'v', BF_OPT_BOOL,   .flag = &verbose, "Verbose output" },
 *       BF_OPT_END
 *   };
 *   bf_opts_parse(&ctx, argc, argv, opts);
 *   const char *subcmd = bf_opts_subcmd(&ctx);   // "run", "dedup", etc.
 *   const char *pos0   = bf_opts_positional(&ctx, 0); // first positional arg
 */

#ifndef BF_OPTS_H
#define BF_OPTS_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Option types ────────────────────────── */

typedef enum {
    BF_OPT_BOOL   = 0,   /* --flag or -f (no argument) */
    BF_OPT_STRING = 1,   /* --key value or -k value    */
    BF_OPT_INT    = 2,   /* --count 42                 */
    BF_OPT_DOUBLE = 3,   /* --rate 0.5                 */
    BF_OPT_PATH   = 4,   /* same as STRING, but ~ expanded */
} bf_opt_type_t;

/* ── Single option descriptor ────────────── */

typedef struct {
    const char   *name;       /* long name without "--" (NULL = end sentinel) */
    char          short_name; /* single-char alias, or '\0' for none          */
    bf_opt_type_t type;

    /* destination pointer — set the one matching `type` */
    union {
        int        *flag;     /* BF_OPT_BOOL    */
        const char **str;     /* BF_OPT_STRING / BF_OPT_PATH */
        int        *ival;     /* BF_OPT_INT     */
        double     *dval;     /* BF_OPT_DOUBLE  */
    };

    const char   *help;       /* one-line description for --help */
    const char   *defval;     /* default value shown in help (informational) */
} bf_opt_t;

#define BF_OPT_END  { NULL, '\0', BF_OPT_BOOL, {NULL}, NULL, NULL }

/* ── Subcommand descriptor ───────────────── */

typedef struct {
    const char *name;         /* subcommand name (e.g. "run", "dedup") */
    const char *help;         /* one-line description                  */
} bf_subcmd_t;

/* ── Parse context ───────────────────────── */

#define BF_OPTS_MAX_POSITIONAL 32

typedef struct {
    const char *program;                          /* argv[0]           */
    const char *subcmd;                           /* matched subcommand or NULL */
    const char *positional[BF_OPTS_MAX_POSITIONAL];
    int         n_positional;
    int         help_requested;                   /* 1 if --help seen  */

    /* internal */
    const char *version;
    const char *description;
} bf_opts_ctx_t;

/* ── API ─────────────────────────────────── */

/*
 * Initialize a parse context with program metadata.
 * `version` and `description` may be NULL.
 */
void bf_opts_init(bf_opts_ctx_t *ctx, const char *version, const char *description);

/*
 * Parse argc/argv against the given option descriptors.
 * If `subcmds` is non-NULL, the first non-option argument is matched
 * as a subcommand. Remaining non-option args become positional.
 *
 * Returns 0 on success, -1 on error (unknown option, missing value).
 * On error, a diagnostic is printed to stderr.
 *
 * If --help is encountered, prints usage and sets ctx->help_requested = 1.
 */
int bf_opts_parse(bf_opts_ctx_t *ctx,
                  int argc, char **argv,
                  bf_opt_t *opts,
                  const bf_subcmd_t *subcmds);

/* Convenience accessors */
const char *bf_opts_subcmd(const bf_opts_ctx_t *ctx);
const char *bf_opts_positional(const bf_opts_ctx_t *ctx, int index);
int         bf_opts_npositional(const bf_opts_ctx_t *ctx);

/*
 * Print formatted usage/help to stderr.
 * Called automatically on --help, but can be invoked manually.
 */
void bf_opts_usage(const bf_opts_ctx_t *ctx,
                   const bf_opt_t *opts,
                   const bf_subcmd_t *subcmds);

#ifdef __cplusplus
}
#endif

#endif /* BF_OPTS_H */
