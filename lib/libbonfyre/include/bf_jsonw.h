/*
 * bf_jsonw.h — one JSON writer for every organ.
 *
 * Output matches JavaScript's JSON.stringify byte for byte: the same string
 * escapes, the same shortest round-trip numbers, and with indent > 0 the same
 * layout as JSON.stringify(value, null, indent). Ports of JS organs therefore
 * keep their digests and their golden outputs.
 *
 * Canonical form (indent 0 plus bf_jsonw_node(..., 1)) sorts object keys and
 * drops whitespace, so bf_jsonw_digest is the sha256 the JS organs computed
 * over their canonical() strings.
 *
 * Keys are compared by UTF-8 bytes; that equals JS sort order except where
 * astral characters meet U+E000..U+FFFF, which no Bonfyre key uses.
 */
#ifndef BF_JSONW_H
#define BF_JSONW_H

#include <stddef.h>
#include <stdint.h>
#include "bf_json.h"

typedef struct {
    char  *buf;
    size_t len, cap;
    int    indent;      /* 0 = compact */
    int    depth;
    int    first[64];   /* per depth: no child written yet */
    int    after_key;
    int    oom;
} bf_jsonw;

void bf_jsonw_init(bf_jsonw *w, int indent);
void bf_jsonw_free(bf_jsonw *w);
void bf_jsonw_reset(bf_jsonw *w);

void bf_jsonw_obj(bf_jsonw *w);
void bf_jsonw_obj_end(bf_jsonw *w);
void bf_jsonw_arr(bf_jsonw *w);
void bf_jsonw_arr_end(bf_jsonw *w);
void bf_jsonw_key(bf_jsonw *w, const char *key);
void bf_jsonw_keyn(bf_jsonw *w, const char *key, size_t len);
void bf_jsonw_str(bf_jsonw *w, const char *s);
void bf_jsonw_strn(bf_jsonw *w, const char *s, size_t len);
void bf_jsonw_num(bf_jsonw *w, double d);
void bf_jsonw_int(bf_jsonw *w, int64_t i);
void bf_jsonw_bool(bf_jsonw *w, int b);
void bf_jsonw_null(bf_jsonw *w);

/* Write a parsed node. sorted=1 orders object keys (canonical form). */
void bf_jsonw_node(bf_jsonw *w, const bf_json_doc_t *doc, const bf_json_node_t *n, int sorted);

/* sha256 hex of the canonical form of a parsed node. */
void bf_jsonw_digest(const bf_json_doc_t *doc, const bf_json_node_t *n, char hex[65]);

/* Decode a raw (escaped) JSON string body into UTF-8. Returns malloc'd text. */
char *bf_json_unescape(const char *raw, int len, size_t *out_len);

/* Compare two raw JSON strings by decoded bytes, as qsort would want. */
int bf_json_rawcmp(const char *a, int alen, const char *b, int blen);

/* String.prototype.localeCompare (root locale) for ASCII text. */
int bf_js_locale_compare(const char *a, const char *b);

/* Format a double as JavaScript's String(number) does. Returns length. */
int bf_js_number(double d, char out[32]);

#endif
