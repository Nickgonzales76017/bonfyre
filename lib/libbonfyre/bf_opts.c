/*
 * bf_opts.c — Declarative CLI option parser
 *
 * Supports:
 *   --long-name value   -s value   --flag   -f
 *   --long-name=value   -svalue    (combined short+value)
 *   -- (stop processing options)
 *   Subcommands as first positional token
 *   Automatic --help / -h generation
 *   ~ expansion for BF_OPT_PATH
 */

#include "bf_opts.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── helpers ──────────────────────────────── */

static const bf_opt_t *find_long(const bf_opt_t *opts, const char *name, size_t len)
{
    for (const bf_opt_t *o = opts; o->name; o++) {
        if (strlen(o->name) == len && memcmp(o->name, name, len) == 0)
            return o;
    }
    return NULL;
}

static const bf_opt_t *find_short(const bf_opt_t *opts, char c)
{
    for (const bf_opt_t *o = opts; o->name; o++) {
        if (o->short_name == c) return o;
    }
    return NULL;
}

static int needs_arg(const bf_opt_t *o)
{
    return o->type != BF_OPT_BOOL;
}

static const char *expand_tilde(const char *path)
{
    if (!path || path[0] != '~') return path;
    const char *home = getenv("HOME");
    if (!home) return path;
    size_t hlen = strlen(home);
    size_t plen = strlen(path + 1); /* skip '~' */
    char *buf = malloc(hlen + plen + 1);
    if (!buf) return path;
    memcpy(buf, home, hlen);
    memcpy(buf + hlen, path + 1, plen + 1);
    return buf;
}

static int set_value(const bf_opt_t *o, const char *val)
{
    switch (o->type) {
    case BF_OPT_BOOL:
        if (o->flag) *o->flag = 1;
        break;
    case BF_OPT_STRING:
        if (o->str) *o->str = val;
        break;
    case BF_OPT_PATH:
        if (o->str) *o->str = expand_tilde(val);
        break;
    case BF_OPT_INT:
        if (o->ival) {
            char *end;
            long v = strtol(val, &end, 0);
            if (end == val) {
                fprintf(stderr, "error: --%s expects an integer, got '%s'\n",
                        o->name, val);
                return -1;
            }
            *o->ival = (int)v;
        }
        break;
    case BF_OPT_DOUBLE:
        if (o->dval) {
            char *end;
            double v = strtod(val, &end);
            if (end == val) {
                fprintf(stderr, "error: --%s expects a number, got '%s'\n",
                        o->name, val);
                return -1;
            }
            *o->dval = v;
        }
        break;
    }
    return 0;
}

/* ── init ─────────────────────────────────── */

void bf_opts_init(bf_opts_ctx_t *ctx, const char *version, const char *description)
{
    memset(ctx, 0, sizeof(*ctx));
    ctx->version     = version;
    ctx->description = description;
}

/* ── parse ────────────────────────────────── */

int bf_opts_parse(bf_opts_ctx_t *ctx,
                  int argc, char **argv,
                  bf_opt_t *opts,
                  const bf_subcmd_t *subcmds)
{
    if (argc < 1) return -1;
    ctx->program = argv[0];

    int stop_opts = 0;          /* after "--" */
    int subcmd_found = 0;

    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];

        /* -- stops option processing */
        if (!stop_opts && strcmp(arg, "--") == 0) {
            stop_opts = 1;
            continue;
        }

        /* long option */
        if (!stop_opts && arg[0] == '-' && arg[1] == '-' && arg[2] != '\0') {
            const char *name = arg + 2;

            /* --help / -h */
            if (strcmp(name, "help") == 0) {
                ctx->help_requested = 1;
                bf_opts_usage(ctx, opts, subcmds);
                return 0;
            }

            /* --version */
            if (strcmp(name, "version") == 0 && ctx->version) {
                fprintf(stderr, "%s %s\n", ctx->program, ctx->version);
                ctx->help_requested = 1;
                return 0;
            }

            /* --key=value */
            const char *eq = strchr(name, '=');
            const bf_opt_t *o;
            if (eq) {
                o = find_long(opts, name, (size_t)(eq - name));
                if (!o) {
                    fprintf(stderr, "error: unknown option --%.*s\n",
                            (int)(eq - name), name);
                    return -1;
                }
                if (set_value(o, eq + 1) < 0) return -1;
            } else {
                o = find_long(opts, name, strlen(name));
                if (!o) {
                    fprintf(stderr, "error: unknown option --%s\n", name);
                    return -1;
                }
                if (needs_arg(o)) {
                    if (i + 1 >= argc) {
                        fprintf(stderr, "error: --%s requires a value\n", o->name);
                        return -1;
                    }
                    if (set_value(o, argv[++i]) < 0) return -1;
                } else {
                    if (set_value(o, NULL) < 0) return -1;
                }
            }
            continue;
        }

        /* short option(s) */
        if (!stop_opts && arg[0] == '-' && arg[1] != '\0' && arg[1] != '-') {
            /* -h */
            if (arg[1] == 'h' && arg[2] == '\0') {
                ctx->help_requested = 1;
                bf_opts_usage(ctx, opts, subcmds);
                return 0;
            }

            for (int j = 1; arg[j]; j++) {
                const bf_opt_t *o = find_short(opts, arg[j]);
                if (!o) {
                    fprintf(stderr, "error: unknown option -%c\n", arg[j]);
                    return -1;
                }
                if (needs_arg(o)) {
                    /* rest of this token is the value, or next argv */
                    const char *val;
                    if (arg[j + 1]) {
                        val = arg + j + 1;
                    } else if (i + 1 < argc) {
                        val = argv[++i];
                    } else {
                        fprintf(stderr, "error: -%c requires a value\n",
                                o->short_name);
                        return -1;
                    }
                    if (set_value(o, val) < 0) return -1;
                    break; /* consumed rest of token */
                } else {
                    if (set_value(o, NULL) < 0) return -1;
                }
            }
            continue;
        }

        /* positional / subcommand */
        if (!subcmd_found && subcmds) {
            /* try to match subcommand */
            for (const bf_subcmd_t *sc = subcmds; sc->name; sc++) {
                if (strcmp(arg, sc->name) == 0) {
                    ctx->subcmd = sc->name;
                    subcmd_found = 1;
                    goto next;
                }
            }
        }

        /* plain positional */
        if (ctx->n_positional < BF_OPTS_MAX_POSITIONAL) {
            ctx->positional[ctx->n_positional++] = arg;
        }
    next:;
    }

    return 0;
}

/* ── accessors ────────────────────────────── */

const char *bf_opts_subcmd(const bf_opts_ctx_t *ctx)
{
    return ctx ? ctx->subcmd : NULL;
}

const char *bf_opts_positional(const bf_opts_ctx_t *ctx, int index)
{
    if (!ctx || index < 0 || index >= ctx->n_positional) return NULL;
    return ctx->positional[index];
}

int bf_opts_npositional(const bf_opts_ctx_t *ctx)
{
    return ctx ? ctx->n_positional : 0;
}

/* ── usage / help ─────────────────────────── */

void bf_opts_usage(const bf_opts_ctx_t *ctx,
                   const bf_opt_t *opts,
                   const bf_subcmd_t *subcmds)
{
    const char *prog = ctx->program ? ctx->program : "program";

    if (ctx->description)
        fprintf(stderr, "%s\n\n", ctx->description);

    fprintf(stderr, "Usage: %s", prog);
    if (subcmds) fprintf(stderr, " <command>");
    fprintf(stderr, " [options]");
    fprintf(stderr, " [args...]\n");

    if (ctx->version)
        fprintf(stderr, "Version: %s\n", ctx->version);

    if (subcmds) {
        fprintf(stderr, "\nCommands:\n");
        for (const bf_subcmd_t *sc = subcmds; sc->name; sc++) {
            fprintf(stderr, "  %-16s %s\n", sc->name, sc->help ? sc->help : "");
        }
    }

    if (opts && opts->name) {
        fprintf(stderr, "\nOptions:\n");
        for (const bf_opt_t *o = opts; o->name; o++) {
            char short_buf[8] = "";
            if (o->short_name)
                snprintf(short_buf, sizeof(short_buf), "-%c, ", o->short_name);
            else
                snprintf(short_buf, sizeof(short_buf), "    ");

            const char *arg_hint = "";
            switch (o->type) {
            case BF_OPT_STRING: arg_hint = " <str>"; break;
            case BF_OPT_PATH:   arg_hint = " <path>"; break;
            case BF_OPT_INT:    arg_hint = " <n>"; break;
            case BF_OPT_DOUBLE: arg_hint = " <f>"; break;
            case BF_OPT_BOOL:   arg_hint = ""; break;
            }

            fprintf(stderr, "  %s--%-12s%-8s %s",
                    short_buf, o->name, arg_hint,
                    o->help ? o->help : "");
            if (o->defval)
                fprintf(stderr, " [default: %s]", o->defval);
            fprintf(stderr, "\n");
        }
    }
    fprintf(stderr, "  -h, --help                  Show this help\n");
}
