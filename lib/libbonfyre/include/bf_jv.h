/*
 * bf_jv.h — a mutable JSON value, the C stand-in for a JS object graph.
 *
 * Ports of JS organs build, clone, compare and digest JSON values constantly;
 * this is the one shared type for that. Objects keep JS property order (array
 * index keys first, ascending, then insertion order), emission goes through
 * bf_jsonw (byte-identical to JSON.stringify), and bf_jv_digest is the sha256
 * of the canonical (sorted, compact) form every Bonfyre receipt uses.
 *
 * Ownership: a value added to an array or object belongs to it; bf_jv_free
 * frees a whole tree. Getters return borrowed pointers.
 */
#ifndef BF_JV_H
#define BF_JV_H

#include <stddef.h>
#include "bf_jsonw.h"

typedef enum { BF_JV_NULL, BF_JV_BOOL, BF_JV_NUM, BF_JV_STR, BF_JV_ARR, BF_JV_OBJ } bf_jv_type;

typedef struct bf_jv bf_jv;
struct bf_jv {
    bf_jv_type type;
    int        b;
    double     n;
    char      *s;        /* STR: UTF-8, NUL-terminated (may contain NUL; see len) */
    size_t     len;      /* STR byte length; ARR/OBJ item count */
    size_t     cap;
    char     **keys;     /* OBJ */
    bf_jv    **items;    /* ARR and OBJ values */
    int       *ht;       /* OBJ with many keys: open-addressed index into keys, -1 empty */
    size_t     htcap;
};

bf_jv *bf_jv_null(void);
bf_jv *bf_jv_bool(int b);
bf_jv *bf_jv_num(double n);
bf_jv *bf_jv_str(const char *s);
bf_jv *bf_jv_strn(const char *s, size_t len);
bf_jv *bf_jv_arr(void);
bf_jv *bf_jv_obj(void);

void   bf_jv_free(bf_jv *v);
bf_jv *bf_jv_clone(const bf_jv *v);

/* Arrays. push takes ownership of item. */
void   bf_jv_push(bf_jv *arr, bf_jv *item);
bf_jv *bf_jv_at(const bf_jv *arr, size_t i);

/* Objects. set takes ownership of value; an existing key keeps its position. */
void   bf_jv_set(bf_jv *obj, const char *key, bf_jv *value);
bf_jv *bf_jv_get(const bf_jv *obj, const char *key);
int    bf_jv_has(const bf_jv *obj, const char *key);
int    bf_jv_del(bf_jv *obj, const char *key);
/* Path lookup: "a.b.c" through objects only. */
bf_jv *bf_jv_path(const bf_jv *v, const char *dotted);

/* Convenience readers; NULL/0 when the value is absent or another type. */
const char *bf_jv_s(const bf_jv *v);
double      bf_jv_n(const bf_jv *v, double fallback);
int         bf_jv_is(const bf_jv *v, bf_jv_type t);

/* Parse JSON text (JSON.parse semantics: last duplicate key wins, first position). */
bf_jv *bf_jv_parse(const char *text, size_t len, char *err, size_t err_sz);
/* Convert a node of a bf_json document. */
bf_jv *bf_jv_from_node(const bf_json_doc_t *doc, const bf_json_node_t *n);

/* Deep equality, as isDeepStrictEqual on JSON data (object key order ignored). */
int    bf_jv_equal(const bf_jv *a, const bf_jv *b);

/* Key orders for emission and digests:
 *   BF_JV_SORT_NONE   stored order, which is always JS property order
 *   BF_JV_SORT_BYTES  every key by UTF-8 bytes: a canonical() that builds the
 *                     string from Object.keys(v).sort()
 *   BF_JV_SORT_JS     what JSON.stringify prints for an object rebuilt from
 *                     sorted entries: array-index keys ascending, then bytes */
enum { BF_JV_SORT_NONE = 0, BF_JV_SORT_BYTES = 1, BF_JV_SORT_JS = 2 };
void   bf_jv_write(bf_jsonw *w, const bf_jv *v, int sorted);
/* Reorder every object's keys in place (JSON.parse(sorted(v)) in JS). */
void   bf_jv_sort_keys(bf_jv *v, int mode);
/* JSON.stringify(v, null, indent) into a malloc'd string (indent 0: compact). */
char  *bf_jv_dump(const bf_jv *v, int indent, int sorted, size_t *len_out);
/* sha256 hex of the canonical (BF_JV_SORT_BYTES) form, or of a chosen order. */
void   bf_jv_digest(const bf_jv *v, char hex[65]);
void   bf_jv_digest_mode(const bf_jv *v, int mode, char hex[65]);
/* Digest of an object with these members (non-index keys), built only as text. */
void   bf_jv_digest_members(int mode, int n, const char *const *keys, const bf_jv *const *values, char hex[65]);

#endif
