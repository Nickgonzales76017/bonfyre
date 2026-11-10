/*
 * bonfyre-mcp: the native MCP organ (W2 of the .mjs retirement).
 *
 * First slice: the four owner command clients that the JS CLIs
 * (spines_cli, canary_cli, recursive_cli, v6_cli) implemented. Each keeps its
 * grammar, defaults, catalog validation, wire checks, error text and the
 * JSON.stringify(result, null, 2) output byte for byte.
 */
#ifndef BONFYRE_MCP_H
#define BONFYRE_MCP_H

#include <stddef.h>
#include <stdio.h>
#include "bf_jv.h"

#define ERR_MAX 16384

/* ── JS semantics (jsx.c) ───────────────────────────────────── */
/* JSON.parse over UTF-8 bytes decoded with TextDecoder({fatal:true}):
 * strict grammar, BOM stripped, last duplicate wins, numbers as doubles. */
bf_jv *jsx_parse(const char *text, size_t len, char *err);
/* Truthiness of a value; NULL means undefined. */
int    jsx_truthy(const bf_jv *v);
/* String.prototype.trim into a malloc'd copy. */
char  *jsx_trim(const char *s, size_t len);
/* Number(string); returns 0 and sets *out (NaN on failure semantics). */
double jsx_to_number(const char *s);
int    jsx_safe_integer(double d);
/* message.replaceAll(secret, mark).slice(0, units) as written to a stream. */
void   jsx_redact(char *msg, const char *secret, const char *mark, size_t units);
/* JSON.stringify(v, null, 2) written with a trailing newline. */
void   jsx_print(FILE *out, const bf_jv *v);
/* Property access that yields NULL (undefined) for non-objects. */
bf_jv *jsx_get(const bf_jv *v, const char *key);
/* obj[key] = clone(value) unless value is undefined (JSON.stringify drops it). */
void   jsx_set_opt(bf_jv *obj, const char *key, const bf_jv *value);
int    jsx_str_is(const bf_jv *v, const char *s);

/* ── wire (wire.c) ──────────────────────────────────────────── */
typedef struct {
    char scheme[8];       /* "http" | "https" */
    char host[256];       /* hostname as WHATWG prints it (IPv6 in brackets) */
    char port[8];         /* "" when default */
    int  has_user, has_search, has_hash;
    char path[2048];      /* pathname, at least "/" */
} js_url;
int  url_parse(const char *text, js_url *u);
void url_href(const js_url *u, char *out, size_t n);
int  url_loopback(const js_url *u);

typedef struct {
    int    status;
    long   content_length;  /* -1 when absent; -2 when not all digits */
    int    has_body;
    char  *body;
    size_t body_len;
    int    over_budget;     /* body exceeded max_bytes while reading */
} http_reply;
/* POST json to url with the MCP headers. Returns 0 or -1 with err set to the
 * message fetch() would have thrown. */
int  http_post(const js_url *u, const char *token, const char *body, size_t body_len,
               long timeout_ms, size_t max_bytes, http_reply *r, char *err);
void http_reply_free(http_reply *r);

/* readFileSync(path, 'utf8') with the Bun error text; 0 ok. */
int  read_text_file(const char *path, char **out, size_t *len, char *err);
void random_hex(char *out, size_t bytes);

/* ── schema (schema.c) ──────────────────────────────────────── */
/* validateArguments from http_boundary.mjs; 0 ok, else err set. */
int  schema_validate(const bf_jv *value, const bf_jv *schema, const char *path, char *err);
/* Load an owner catalog (array of tools, or {tools:[...]}) from the repo. */
bf_jv *catalog_load(const char *relative, char *err);
const bf_jv *catalog_tool(const bf_jv *tools, const char *name);
const char *repo_root(void);

/* ── command clients (clis.c) ───────────────────────────────── */
int cli_spines(int argc, char **argv);
int cli_canary(int argc, char **argv);
int cli_recursive(int argc, char **argv);
int cli_v6(int argc, char **argv);

/* ── fixture owner for golden vectors (fixture.c) ───────────── */
int fixture_serve(int argc, char **argv);

#endif
