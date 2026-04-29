/*
 * bf_json.h — yyjson-inspired SIMD JSON engine for Bonfyre
 *
 * Replaces strstr()-based extractors with a real DOM parser.
 * Hand-rolled for zero external deps, but designed for SIMD:
 *   - 16-byte aligned scanning for structural chars ({, }, [, ], :, ,, ")
 *   - SSE4.2/NEON accelerated string scanning
 *   - Lazy number parsing (deferred strtod until accessed)
 *
 * DOM is a flat array of nodes (cache-friendly, zero-alloc-per-node).
 * Strings point into the original buffer (zero-copy after parse).
 */

#ifndef BF_JSON_H
#define BF_JSON_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Node types ──────────────────────────────────────────────── */

typedef enum {
    BF_JSON_NULL    = 0,
    BF_JSON_BOOL    = 1,
    BF_JSON_INT     = 2,
    BF_JSON_DOUBLE  = 3,
    BF_JSON_STRING  = 4,
    BF_JSON_ARRAY   = 5,
    BF_JSON_OBJECT  = 6,
} bf_json_type_t;

/* ── Node (32 bytes, cache-line aligned pair) ────────────────── */

typedef struct bf_json_node {
    bf_json_type_t type;
    int            parent;       /* Index of parent node (-1 for root) */
    int            next;         /* Next sibling (-1 for last) */
    int            first_child;  /* First child for objects/arrays (-1 for leaf) */
    int            child_count;  /* Number of children */
    union {
        int64_t     i;           /* BF_JSON_INT */
        double      d;           /* BF_JSON_DOUBLE */
        int         b;           /* BF_JSON_BOOL */
        struct {
            const char *ptr;     /* Points into parse buffer (zero-copy) */
            int         len;
        } str;                   /* BF_JSON_STRING */
    } val;
    const char     *key;         /* Key string for object members (NULL for array elements) */
    int             key_len;
} bf_json_node_t;

/* ── Document (opaque) ───────────────────────────────────────── */

typedef struct bf_json_doc bf_json_doc_t;

/* ── Parse / Free ────────────────────────────────────────────── */

/*
 * Parse JSON text into a document.
 * `buf` must remain valid for the lifetime of the document (zero-copy strings).
 * Returns NULL on parse error. Error message in `err_out` if non-NULL.
 */
bf_json_doc_t *bf_json_parse(const char *buf, size_t len, char *err_out, size_t err_sz);

/* Convenience: parse NUL-terminated string */
bf_json_doc_t *bf_json_parse_str(const char *s, char *err_out, size_t err_sz);

/* Free document */
void bf_json_free(bf_json_doc_t *doc);

/* ── Root access ─────────────────────────────────────────────── */

/* Get root node */
const bf_json_node_t *bf_json_root(const bf_json_doc_t *doc);

/* Get node count */
int bf_json_count(const bf_json_doc_t *doc);

/* Get node by index */
const bf_json_node_t *bf_json_at(const bf_json_doc_t *doc, int idx);

/* ── Object key lookup ───────────────────────────────────────── */

/*
 * Look up a key in an object node.
 * Returns the value node, or NULL if not found.
 * O(n) scan (objects are typically small; hash map overkill).
 */
const bf_json_node_t *bf_json_obj_get(const bf_json_doc_t *doc,
                                       const bf_json_node_t *obj,
                                       const char *key);

/* ── Array element access ────────────────────────────────────── */

/* Get array element at index `i`. O(i) skip. */
const bf_json_node_t *bf_json_arr_get(const bf_json_doc_t *doc,
                                       const bf_json_node_t *arr,
                                       int i);

/* ── Value extractors ────────────────────────────────────────── */

/* Get string value (returns ptr into buffer, NOT NUL-terminated). Use len. */
const char *bf_json_get_str(const bf_json_node_t *n, int *len_out);

/* Copy string value into buffer, NUL-terminated. Returns bytes written. */
int bf_json_get_str_copy(const bf_json_node_t *n, char *out, size_t sz);

int64_t     bf_json_get_int(const bf_json_node_t *n);
double      bf_json_get_double(const bf_json_node_t *n);
int         bf_json_get_bool(const bf_json_node_t *n);

/* ── Convenience: dotpath lookup ─────────────────────────────── */

/*
 * Look up nested value via dot path: "foo.bar.baz" or "arr.0.name"
 * Returns NULL if any segment not found.
 */
const bf_json_node_t *bf_json_dotpath(const bf_json_doc_t *doc, const char *path);

/* ── Convenience: one-shot extractors (compatible with old bf_json_*) ── */

int bf_json2_str(const char *json, const char *key, char *out, size_t out_sz);
int bf_json2_int(const char *json, const char *key, int *out);
int bf_json2_double(const char *json, const char *key, double *out);

/* ── Iteration ───────────────────────────────────────────────── */

/* Iterate children of object/array. Returns NULL after last child. */
const bf_json_node_t *bf_json_child_first(const bf_json_doc_t *doc,
                                            const bf_json_node_t *parent);
const bf_json_node_t *bf_json_child_next(const bf_json_doc_t *doc,
                                           const bf_json_node_t *node);

#ifdef __cplusplus
}
#endif

#endif /* BF_JSON_H */
