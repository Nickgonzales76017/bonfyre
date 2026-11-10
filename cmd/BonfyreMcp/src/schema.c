/* validateArguments (http_boundary.mjs) and the owner catalogs. */
#include <limits.h>
#include <stdint.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mcp.h"

static const char *type_name(const bf_jv *v) {
    switch (v->type) {
    case BF_JV_NULL: case BF_JV_ARR: case BF_JV_OBJ: return "object";
    case BF_JV_BOOL: return "boolean";
    case BF_JV_NUM: return "number";
    default: return "string";
    }
}
/* ToNumber for the relational comparisons (value < schema.minimum). */
static double js_number(const bf_jv *v) {
    switch (v->type) {
    case BF_JV_NULL: return 0;
    case BF_JV_BOOL: return v->b;
    case BF_JV_NUM: return v->n;
    case BF_JV_STR: return jsx_to_number(v->s);
    case BF_JV_ARR: return v->len == 0 ? 0 : NAN;
    default: return NAN;
    }
}
static int same_value_zero(const bf_jv *a, const bf_jv *b) {
    if (a->type != b->type) return 0;
    switch (a->type) {
    case BF_JV_NULL: return 1;
    case BF_JV_BOOL: return a->b == b->b;
    case BF_JV_NUM: return a->n == b->n || (a->n != a->n && b->n != b->n);
    case BF_JV_STR: return a->len == b->len && !memcmp(a->s, b->s, a->len);
    default: return 0;   /* objects compare by identity */
    }
}

int schema_validate(const bf_jv *value, const bf_jv *schema, const char *path, char *err) {
    const bf_jv *type = jsx_get(schema, "type");
    if (type && type->type == BF_JV_ARR) {
        for (size_t i = 0; i < type->len; i++) {
            bf_jv *one = bf_jv_clone(schema); bf_jv_set(one, "type", bf_jv_clone(type->items[i]));
            char e[ERR_MAX]; int r = schema_validate(value, one, path, e); bf_jv_free(one);
            if (!r) return 0;
        }
        bf_jsonw w; bf_jsonw_init(&w, 0); size_t used = 0; char joined[4096] = "";
        for (size_t i = 0; i < type->len; i++) {
            const bf_jv *t = type->items[i]; char piece[512];
            if (t->type == BF_JV_STR) snprintf(piece, sizeof piece, "%s", t->s);
            else if (t->type == BF_JV_NULL) snprintf(piece, sizeof piece, "%s", "");
            else { bf_jsonw_reset(&w); bf_jv_write(&w, t, 0); snprintf(piece, sizeof piece, "%s", w.buf); }
            used += (size_t)snprintf(joined + used, sizeof joined - used, "%s%s", i ? ", " : "", piece);
            if (used >= sizeof joined) break;
        }
        bf_jsonw_free(&w);
        snprintf(err, ERR_MAX, "%s must match one of %s", path, joined);
        return -1;
    }
    const char *t = type && type->type == BF_JV_STR ? type->s : NULL;
    int has_type = jsx_truthy(type);
    int valid;
    if (t && !strcmp(t, "object")) valid = value->type == BF_JV_OBJ;
    else if (t && !strcmp(t, "array")) valid = value->type == BF_JV_ARR;
    else if (t && !strcmp(t, "integer")) valid = value->type == BF_JV_NUM && jsx_safe_integer(value->n);
    else if (t && !strcmp(t, "number")) valid = value->type == BF_JV_NUM && isfinite(value->n);
    else if (t && !strcmp(t, "null")) valid = value->type == BF_JV_NULL;
    else valid = !has_type || (t && !strcmp(type_name(value), t));
    if (!valid) {
        char tn[512];
        if (t) snprintf(tn, sizeof tn, "%s", t);
        else if (type->type == BF_JV_NUM) { bf_jsonw w; bf_jsonw_init(&w, 0); bf_jv_write(&w, type, 0); snprintf(tn, sizeof tn, "%s", w.buf); bf_jsonw_free(&w); }
        else snprintf(tn, sizeof tn, "%s", type->type == BF_JV_BOOL ? "true" : "[object Object]");
        snprintf(err, ERR_MAX, "%s must be %s", path, tn);
        return -1;
    }
    const bf_jv *cst = schema->type == BF_JV_OBJ ? bf_jv_get(schema, "const") : NULL;
    if (schema->type == BF_JV_OBJ && bf_jv_has(schema, "const") && !bf_jv_equal(value, cst)) {
        snprintf(err, ERR_MAX, "%s must equal its declared constant", path); return -1;
    }
    const bf_jv *en = jsx_get(schema, "enum");
    if (jsx_truthy(en)) {
        int found = 0;
        if (en->type == BF_JV_ARR) for (size_t i = 0; i < en->len && !found; i++) found = same_value_zero(value, en->items[i]);
        else if (en->type == BF_JV_STR && value->type == BF_JV_STR) found = strstr(en->s, value->s) != NULL;
        if (!found) { snprintf(err, ERR_MAX, "%s is not an allowed value", path); return -1; }
    }
    const bf_jv *mn = jsx_get(schema, "minimum"), *mx = jsx_get(schema, "maximum");
    double x = js_number(value);
    if ((mn && x < js_number(mn)) || (mx && x > js_number(mx))) {
        snprintf(err, ERR_MAX, "%s is outside its allowed range", path); return -1;
    }
    if (t && !strcmp(t, "object")) {
        const bf_jv *req = jsx_get(schema, "required");
        if (req && req->type == BF_JV_ARR) for (size_t i = 0; i < req->len; i++) {
            const bf_jv *k = req->items[i];
            if (k->type == BF_JV_STR && !bf_jv_has(value, k->s)) { snprintf(err, ERR_MAX, "%s.%s is required", path, k->s); return -1; }
        }
        const bf_jv *props = jsx_get(schema, "properties"), *ap = jsx_get(schema, "additionalProperties");
        for (size_t i = 0; i < value->len; i++) {
            const char *k = value->keys[i];
            size_t pl = strlen(path) + strlen(k) + 2; char *sub = malloc(pl);
            snprintf(sub, pl, "%s.%s", path, k);
            int r = 0;
            if (props && props->type == BF_JV_OBJ && bf_jv_has(props, k)) r = schema_validate(value->items[i], bf_jv_get(props, k), sub, err);
            else if (ap && ap->type == BF_JV_BOOL && !ap->b) { snprintf(err, ERR_MAX, "%s is not allowed", sub); r = -1; }
            free(sub);
            if (r) return r;
        }
    } else if (t && !strcmp(t, "array")) {
        const bf_jv *items = jsx_get(schema, "items");
        if (jsx_truthy(items)) for (size_t i = 0; i < value->len; i++) {
            size_t pl = strlen(path) + 32; char *sub = malloc(pl);
            snprintf(sub, pl, "%s[%zu]", path, i);
            int r = schema_validate(value->items[i], items, sub, err);
            free(sub);
            if (r) return r;
        }
    }
    return 0;
}

const char *repo_root(void) {
    static char root[PATH_MAX];
    if (*root) return root;
    const char *env = getenv("BONFYRE_MCP_REPO");
    if (env && *env) { snprintf(root, sizeof root, "%s", env); return root; }
    /* The binary lives at <repo>/cmd/BonfyreMcp/bonfyre-mcp or is installed with
     * BONFYRE_MCP_REPO set; resolve the build location from the executable. */
    char exe[PATH_MAX], real[PATH_MAX]; int have = 0;
#ifdef __APPLE__
    uint32_t sz = sizeof exe;
    extern int _NSGetExecutablePath(char *, uint32_t *);
    have = _NSGetExecutablePath(exe, &sz) == 0;
#else
    ssize_t el = readlink("/proc/self/exe", exe, sizeof exe - 1); if (el > 0) { exe[el] = 0; have = 1; }
#endif
    if (have && realpath(exe, real)) {
        char *s = strrchr(real, '/'); if (s) *s = 0;
        snprintf(root, sizeof root, "%s/../..", real);
        char r2[PATH_MAX]; if (realpath(root, r2)) snprintf(root, sizeof root, "%s", r2);
        char probe[PATH_MAX]; snprintf(probe, sizeof probe, "%s/integrations/mcp", root);
        if (access(probe, R_OK) == 0) return root;
    }
    snprintf(root, sizeof root, "%s/Developer/Bonfyre", getenv("HOME") ? getenv("HOME") : "");
    return root;
}

bf_jv *catalog_load(const char *relative, char *err) {
    char path[PATH_MAX]; snprintf(path, sizeof path, "%s/%s", repo_root(), relative);
    char *text; size_t n;
    if (read_text_file(path, &text, &n, err)) return NULL;
    bf_jv *v = jsx_parse(text, n, err); free(text);
    if (!v) return NULL;
    if (v->type == BF_JV_OBJ) { bf_jv *tools = bf_jv_clone(bf_jv_get(v, "tools")); bf_jv_free(v); v = tools; }
    if (!v || v->type != BF_JV_ARR) { bf_jv_free(v); snprintf(err, ERR_MAX, "catalog %s has no tools", relative); return NULL; }
    return v;
}
const bf_jv *catalog_tool(const bf_jv *tools, const char *name) {
    for (size_t i = 0; tools && i < tools->len; i++) if (jsx_str_is(jsx_get(tools->items[i], "name"), name)) return tools->items[i];
    return NULL;
}
