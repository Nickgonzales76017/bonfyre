#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/**
 * bonfyre_frappe_condition.c
 *
 * Frappe evaluates a conditional property three ways (form/layout.js,
 * evaluate_depends_on_value): a bare string is a field lookup whose arrays
 * count as present only when non-empty; "eval:" is a JavaScript expression
 * over {doc, parent} whose result is taken by JavaScript truthiness, where an
 * empty array is true; "fn:" triggers a client script. The first two are
 * compiled here under those semantics and nothing looser. A construct outside
 * the supported operations is named rather than approximated.
 */

#include "bonfyre_frappe_condition.h"
#include "bonfyre.h"

#include <ctype.h>
#include <math.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
    N_DOC, N_PARENTROOT, N_FIELD, N_PARENT, N_ENV, N_STR, N_NUM, N_BOOL, N_NULL,
    N_UNDEF, N_ARR, N_NOT, N_NEG, N_AND, N_OR, N_NULLISH, N_BIN, N_COND, N_GET,
    N_OPTGET, N_CALL, N_PRESENT, N_PYBOOL, N_INDEX
};

struct BfCondNode {
    int kind;
    char *text;
    double num;
    BfCondNode **kids;
    size_t n;
};

/* ------------------------------------------------------------------ nodes */

static BfCondNode *node_new(int kind, const char *text) {
    BfCondNode *node = calloc(1, sizeof(*node));
    if (!node) return NULL;
    node->kind = kind;
    if (text && !(node->text = strdup(text))) { free(node); return NULL; }
    return node;
}

static void node_free(BfCondNode *node) {
    if (!node) return;
    for (size_t i = 0; i < node->n; ++i) node_free(node->kids[i]);
    free(node->kids);
    free(node->text);
    free(node);
}

static int node_add(BfCondNode *node, BfCondNode *kid) {
    BfCondNode **next;
    if (!node || !kid) return -1;
    next = realloc(node->kids, sizeof(*next) * (node->n + 1));
    if (!next) return -1;
    node->kids = next;
    node->kids[node->n++] = kid;
    return 0;
}

static BfCondNode *node_with(int kind, const char *text, BfCondNode *a, BfCondNode *b, BfCondNode *c) {
    BfCondNode *node = node_new(kind, text);
    if (!node || (a && node_add(node, a)) || (b && node_add(node, b)) || (c && node_add(node, c))) {
        /* Children already attached are freed with the node; the rest here. */
        size_t attached = node ? node->n : 0;
        if (attached < 1) node_free(a);
        if (attached < 2) node_free(b);
        if (attached < 3) node_free(c);
        node_free(node);
        return NULL;
    }
    return node;
}

/* ------------------------------------------------------------------ lexer */

enum { T_END, T_NUM, T_STR, T_IDENT, T_PUNCT, T_BAD, T_TEMPLATE };

typedef struct {
    const char *src;
    size_t pos;
    int type;
    char text[512];
    double num;
    char missing[96];
    int python;            /* the donor's server-side dialect: and/or/not/in, no coercion */
} Parser;

static void fail(Parser *p, const char *fmt, ...) {
    va_list args;
    if (p->missing[0]) return;
    va_start(args, fmt);
    vsnprintf(p->missing, sizeof(p->missing), fmt, args);
    va_end(args);
}

static void next_token(Parser *p) {
    static const char *const puncts[] = {
        "===", "!==", "==", "!=", "<=", ">=", "&&", "||", "?.", "??", "=>", NULL
    };
    const char *s = p->src;
    size_t i = p->pos, out = 0;
    while (s[i] && isspace((unsigned char)s[i])) ++i;
    p->text[0] = '\0';
    if (!s[i]) { p->type = T_END; p->pos = i; return; }
    if (isdigit((unsigned char)s[i]) || (s[i] == '.' && isdigit((unsigned char)s[i + 1]))) {
        char *end = NULL;
        p->num = strtod(s + i, &end);
        p->type = T_NUM;
        p->pos = (size_t)(end - s);
        return;
    }
    if (s[i] == '`') {
        /* A template literal, kept raw: its ${...} parts are parsed where it is used. */
        size_t start = ++i;
        while (s[i] && s[i] != '`') ++i;
        if (!s[i] || i - start >= sizeof(p->text)) { p->type = T_BAD; snprintf(p->text, sizeof(p->text), "template_literal"); return; }
        memcpy(p->text, s + start, i - start);
        p->text[i - start] = '\0';
        p->type = T_TEMPLATE;
        p->pos = i + 1;
        return;
    }
    if (s[i] == '"' || s[i] == '\'') {
        char quote = s[i++];
        while (s[i] && s[i] != quote) {
            char c = s[i++];
            if (c == '\\' && s[i]) {
                c = s[i++];
                if (c == 'n') c = '\n';
                else if (c == 't') c = '\t';
                else if (c == 'r') c = '\r';
                else if (c == 'u' || c == 'x') { p->type = T_BAD; snprintf(p->text, sizeof(p->text), "string_escape"); return; }
            }
            if (out + 1 >= sizeof(p->text)) { p->type = T_BAD; snprintf(p->text, sizeof(p->text), "string_too_long"); return; }
            p->text[out++] = c;
        }
        if (s[i] != quote) { p->type = T_BAD; snprintf(p->text, sizeof(p->text), "unterminated_string"); return; }
        p->text[out] = '\0';
        p->type = T_STR;
        p->pos = i + 1;
        return;
    }
    if (isalpha((unsigned char)s[i]) || s[i] == '_' || s[i] == '$') {
        while ((isalnum((unsigned char)s[i]) || s[i] == '_' || s[i] == '$') && out + 1 < sizeof(p->text))
            p->text[out++] = s[i++];
        p->text[out] = '\0';
        p->type = T_IDENT;
        p->pos = i;
        return;
    }
    for (size_t k = 0; puncts[k]; ++k) {
        size_t len = strlen(puncts[k]);
        if (!strncmp(s + i, puncts[k], len)) {
            /* "a?.5:b" is a conditional, not an optional chain. */
            if (!strcmp(puncts[k], "?.") && isdigit((unsigned char)s[i + 2])) continue;
            memcpy(p->text, puncts[k], len + 1);
            p->type = T_PUNCT;
            p->pos = i + len;
            return;
        }
    }
    p->text[0] = s[i];
    p->text[1] = '\0';
    p->type = T_PUNCT;
    p->pos = i + 1;
}

static int is_punct(const Parser *p, const char *text) {
    return p->type == T_PUNCT && !strcmp(p->text, text);
}

static int is_word(const Parser *p, const char *text) {
    return p->python && p->type == T_IDENT && !strcmp(p->text, text);
}

/* ----------------------------------------------------------------- parser */

static BfCondNode *parse_expression(Parser *p);

static BfCondNode *make_member(Parser *p, BfCondNode *object, const char *name, int optional) {
    BfCondNode *node;
    if (object->kind == N_DOC || object->kind == N_PARENTROOT) {
        node = node_new(object->kind == N_DOC ? N_FIELD : N_PARENT, name);
        node_free(object);
        return node;
    }
    if (object->kind == N_ENV) {
        size_t len = strlen(object->text) + strlen(name) + 2;
        char *path = malloc(len);
        if (!path) { node_free(object); return NULL; }
        snprintf(path, len, "%s.%s", object->text, name);
        /* The donor binds parent to the form's document, which is cur_frm.doc. */
        if (!strcmp(path, "cur_frm.doc")) {
            free(path);
            node_free(object);
            return node_new(N_PARENTROOT, NULL);
        }
        free(object->text);
        object->text = path;
        return object;
    }
    node = node_with(optional ? N_OPTGET : N_GET, name, object, NULL, NULL);
    if (!node) fail(p, "out_of_memory");
    return node;
}

static BfCondNode *make_call(Parser *p, BfCondNode *callee, BfCondNode **args, size_t argc) {
    BfCondNode *node = NULL;
    const char *op = NULL;
    if (p->python && callee->kind == N_ENV && !strcmp(callee->text, "_") && argc == 1 && args[0]->kind == N_STR) {
        /* _("text") marks text for translation; untranslated it is the text. */
        node_free(callee);
        return args[0];
    }
    if (p->python && callee->kind == N_ENV && strrchr(callee->text, '.')) {
        /* A method on a template variable: the last name is the method. */
        char *dot = strrchr(callee->text, '.');
        const char *method_op = !strcmp(dot + 1, "strftime") && argc == 1 ? "py_strftime"
                              : !strcmp(dot + 1, "date") && argc == 0 ? "py_date"
                              : !strcmp(dot + 1, "format") ? "py_format" : NULL;
        if (method_op && strncmp(callee->text, "frappe.", 7)) {
            *dot = '\0';
            node = node_new(N_CALL, method_op);
            if (!node || node_add(node, callee) != 0) { node_free(node); node = NULL; fail(p, "out_of_memory"); goto refuse; }
            callee = NULL;
            goto attach;
        }
    }
    if (!p->python && callee->kind == N_ENV && !strcmp(callee->text, "frappe.datetime.get_today") && argc == 0) {
        /* Today is whatever day the caller binds; unbound, it refuses. */
        node_free(callee);
        node = node_new(N_ENV, "today");
        if (!node) fail(p, "out_of_memory");
        return node;
    }
    if (!p->python && callee->kind == N_ENV && strrchr(callee->text, '.') && strncmp(callee->text, "frappe.", 7)) {
        /* A method on a bound name, as on a field: the last name is the method. */
        char *dot = strrchr(callee->text, '.');
        const char *method_op = !strcmp(dot + 1, "slice") && (argc == 1 || argc == 2) ? "slice"
                              : !strcmp(dot + 1, "includes") && argc == 1 ? "includes"
                              : !strcmp(dot + 1, "indexOf") && argc == 1 ? "indexof" : NULL;
        if (method_op) {
            *dot = '\0';
            node = node_new(N_CALL, method_op);
            if (!node || node_add(node, callee) != 0) { node_free(node); node = NULL; fail(p, "out_of_memory"); goto refuse; }
            callee = NULL;
            goto attach;
        }
    }
    if (callee->kind == N_ENV) {
        if (p->python && !strcmp(callee->text, "range") && (argc == 1 || argc == 2)) op = "range";
        else if (p->python && (argc == 1 || argc == 2) &&
                 (!strcmp(callee->text, "frappe.utils.formatdate") || !strcmp(callee->text, "frappe.utils.format_date"))) op = "py_formatdate";
        else if (p->python && (argc == 1 || argc == 2) && !strcmp(callee->text, "frappe.utils.format_time")) op = "py_formattime";
        else if (p->python && (argc == 1 || argc == 2) && !strcmp(callee->text, "frappe.utils.format_datetime")) op = "py_formatdatetime";
        else if (p->python && argc == 1 && !strcmp(callee->text, "frappe.format_value")) op = "py_formatvalue";
        else if (p->python && argc == 1 && !strcmp(callee->text, "frappe.bold")) op = "py_bold";
        else if (p->python && (argc == 2 || argc == 3) && !strcmp(callee->text, "frappe.utils.get_link_to_form")) op = "py_linktext";
        else if (p->python && argc == 1 && !strcmp(callee->text, "frappe.utils.get_datetime")) op = "py_datetime";
        else if (p->python && !strcmp(callee->text, "frappe.utils.flt") && (argc == 1 || argc == 2)) op = "py_flt";
        else if (p->python) op = NULL;
        else if (!strcmp(callee->text, "in_list") && argc == 2) op = "in_list";
        else if (!strcmp(callee->text, "cint") && argc == 1) op = "cint";
        else if (!strcmp(callee->text, "Boolean") && argc == 1) op = "boolean";
        if (!op) {
            if (!strcmp(callee->text, "in_list") || !strcmp(callee->text, "cint") || !strcmp(callee->text, "Boolean"))
                fail(p, "call:%.60s/%zu", callee->text, argc);
            else
                fail(p, "call:%.80s", callee->text);
            goto refuse;
        }
        node = node_new(N_CALL, op);
        node_free(callee);
        callee = NULL;
    } else if (callee->kind == N_GET || callee->kind == N_OPTGET) {
        int optional = callee->kind == N_OPTGET;
        if (p->python && !strcmp(callee->text, "format") && !optional) op = "py_format";
        else if (p->python && !strcmp(callee->text, "strftime") && argc == 1 && !optional) op = "py_strftime";
        else if (p->python && !strcmp(callee->text, "date") && argc == 0 && !optional) op = "py_date";
        else if (p->python) op = NULL;
        else if (!strcmp(callee->text, "slice") && (argc == 1 || argc == 2) && !optional) op = "slice";
        else if (!strcmp(callee->text, "includes") && argc == 1) op = optional ? "includes?" : "includes";
        else if (!strcmp(callee->text, "indexOf") && argc == 1) op = optional ? "indexof?" : "indexof";
        if (!op) { fail(p, "method:%.80s", callee->text); goto refuse; }
        node = node_new(N_CALL, op);
        if (node && node_add(node, callee->kids[0]) == 0) callee->n = 0;
        else { node_free(node); node = NULL; }
        node_free(callee);
        callee = NULL;
    } else if (p->python && callee->kind == N_FIELD && !strcmp(callee->text, "get") && argc == 1 && args[0]->kind == N_STR) {
        /* doc.get("x") is the field x, absent as None. */
        node = node_new(N_FIELD, args[0]->text);
        node_free(callee);
        node_free(args[0]);
        if (!node) fail(p, "out_of_memory");
        return node;
    } else if (callee->kind == N_FIELD) {
        fail(p, "call:doc.%.80s", callee->text);
        goto refuse;
    } else if (callee->kind == N_PARENT) {
        fail(p, "call:parent.%.76s", callee->text);
        goto refuse;
    } else {
        fail(p, "call:expression");
        goto refuse;
    }
attach:
    if (!node) { fail(p, "out_of_memory"); goto refuse; }
    for (size_t i = 0; i < argc; ++i) {
        if (node_add(node, args[i]) != 0) {
            fail(p, "out_of_memory");
            for (size_t k = i; k < argc; ++k) node_free(args[k]);
            node_free(node);
            return NULL;
        }
    }
    return node;
refuse:
    node_free(callee);
    for (size_t i = 0; i < argc; ++i) node_free(args[i]);
    return NULL;
}

static BfCondNode *parse_primary(Parser *p) {
    BfCondNode *node = NULL;
    if (p->type == T_NUM) {
        node = node_new(N_NUM, NULL);
        if (node) node->num = p->num;
        next_token(p);
    } else if (p->type == T_STR) {
        node = node_new(N_STR, p->text);
        next_token(p);
    } else if (p->type == T_TEMPLATE) {
        /* `a${x}b` is "a" + x + "b", starting from text so the sum is text. */
        char raw[512];
        const char *c = raw;
        snprintf(raw, sizeof(raw), "%s", p->text);
        node = node_new(N_STR, "");
        while (node && *c) {
            const char *open = strstr(c, "${");
            BfCondNode *piece;
            char part[512];
            size_t len = open ? (size_t)(open - c) : strlen(c);
            if (len) {
                snprintf(part, sizeof(part), "%.*s", (int)len, c);
                piece = node_new(N_STR, part);
                node = piece ? node_with(N_BIN, "add", node, piece, NULL) : (node_free(node), NULL);
            }
            if (!open || !node) break;
            {
                const char *close = strchr(open, '}');
                Parser inner;
                if (!close) { fail(p, "syntax:template_literal"); node_free(node); return NULL; }
                snprintf(part, sizeof(part), "%.*s", (int)(close - open - 2), open + 2);
                memset(&inner, 0, sizeof(inner));
                inner.src = part;
                inner.python = p->python;
                next_token(&inner);
                piece = parse_expression(&inner);
                if (!piece || inner.type != T_END) {
                    fail(p, "%s", inner.missing[0] ? inner.missing : "syntax:template_literal");
                    node_free(piece); node_free(node);
                    return NULL;
                }
                node = node_with(N_BIN, "add", node, piece, NULL);
                c = close + 1;
            }
        }
        next_token(p);
    } else if (p->type == T_IDENT) {
        if (p->python && (!strcmp(p->text, "True") || !strcmp(p->text, "False"))) {
            node = node_new(N_BOOL, NULL);
            if (node) node->num = p->text[0] == 'T';
        } else if (p->python && !strcmp(p->text, "None")) node = node_new(N_NULL, NULL);
        else if (!strcmp(p->text, "true") || !strcmp(p->text, "false")) {
            node = node_new(N_BOOL, NULL);
            if (node) node->num = p->text[0] == 't';
        } else if (!strcmp(p->text, "null")) node = node_new(N_NULL, NULL);
        else if (!strcmp(p->text, "undefined")) node = node_new(N_UNDEF, NULL);
        else if (!strcmp(p->text, "doc")) node = node_new(N_DOC, NULL);
        else if (!strcmp(p->text, "parent")) node = node_new(N_PARENTROOT, NULL);
        else if (!strcmp(p->text, "typeof") || !strcmp(p->text, "new") || !strcmp(p->text, "function") ||
                 !strcmp(p->text, "await") || !strcmp(p->text, "delete") || !strcmp(p->text, "void")) {
            fail(p, "syntax:%.80s", p->text);
            return NULL;
        } else node = node_new(N_ENV, p->text);
        next_token(p);
    } else if (is_punct(p, "(")) {
        next_token(p);
        node = parse_expression(p);
        if (!node) return NULL;
        if (p->python && is_punct(p, ",")) {
            /* A parenthesised list is a tuple, which is a list for membership. */
            BfCondNode *tuple = node_with(N_ARR, NULL, node, NULL, NULL);
            if (!tuple) { fail(p, "out_of_memory"); return NULL; }
            while (is_punct(p, ",")) {
                BfCondNode *item;
                next_token(p);
                if (is_punct(p, ")")) break;
                item = parse_expression(p);
                if (!item || node_add(tuple, item) != 0) { node_free(tuple); return NULL; }
            }
            node = tuple;
        }
        if (!is_punct(p, ")")) { fail(p, "syntax:unbalanced_parenthesis"); node_free(node); return NULL; }
        next_token(p);
        return node;
    } else if (is_punct(p, "[")) {
        node = node_new(N_ARR, NULL);
        next_token(p);
        while (node && !is_punct(p, "]")) {
            BfCondNode *item = parse_expression(p);
            if (!item || node_add(node, item) != 0) { node_free(node); return NULL; }
            if (is_punct(p, ",")) next_token(p);
            else if (!is_punct(p, "]")) { fail(p, "syntax:unbalanced_bracket"); node_free(node); return NULL; }
        }
        next_token(p);
    } else {
        if (p->type == T_END) fail(p, "syntax:unexpected_end");
        else fail(p, "syntax:%.80s", p->text);
        return NULL;
    }
    if (!node) fail(p, "out_of_memory");
    return node;
}

static BfCondNode *parse_postfix(Parser *p) {
    BfCondNode *node = parse_primary(p);
    while (node) {
        if (is_punct(p, ".") || is_punct(p, "?.")) {
            int optional = p->text[0] == '?';
            next_token(p);
            if (p->type != T_IDENT) { fail(p, "syntax:member_name"); node_free(node); return NULL; }
            node = make_member(p, node, p->text, optional);
            next_token(p);
        } else if (is_punct(p, "(")) {
            BfCondNode *args[8];
            size_t argc = 0;
            int bad = 0;
            next_token(p);
            while (!is_punct(p, ")")) {
                BfCondNode *arg = argc < 8 ? parse_expression(p) : NULL;
                if (!arg) { if (argc >= 8) fail(p, "syntax:too_many_arguments"); bad = 1; break; }
                args[argc++] = arg;
                if (is_punct(p, ",")) next_token(p);
                else if (!is_punct(p, ")")) { fail(p, "syntax:unbalanced_parenthesis"); bad = 1; break; }
            }
            if (bad) {
                for (size_t i = 0; i < argc; ++i) node_free(args[i]);
                node_free(node);
                return NULL;
            }
            next_token(p);
            node = make_call(p, node, args, argc);
        } else if (p->python && is_punct(p, "|")) {
            /* A template filter. Only the count of a list is supported. */
            next_token(p);
            if (p->type != T_IDENT || strcmp(p->text, "length")) {
                fail(p, "filter:%.80s", p->type == T_IDENT ? p->text : "?");
                node_free(node);
                return NULL;
            }
            next_token(p);
            node = node_with(N_GET, "length", node, NULL, NULL);
            if (!node) fail(p, "out_of_memory");
        } else if (p->python && is_punct(p, "[")) {
            BfCondNode *index;
            next_token(p);
            index = parse_expression(p);
            if (!index || !is_punct(p, "]")) {
                fail(p, "syntax:unbalanced_bracket");
                node_free(index); node_free(node);
                return NULL;
            }
            next_token(p);
            node = node_with(N_INDEX, NULL, node, index, NULL);
            if (!node) fail(p, "out_of_memory");
        } else if (is_punct(p, "[")) {
            next_token(p);
            if (node->kind == N_DOC && p->type == T_STR) {
                BfCondNode *field = node_new(N_FIELD, p->text);
                next_token(p);
                if (!field || !is_punct(p, "]")) {
                    fail(p, "computed_member");
                    node_free(field); node_free(node);
                    return NULL;
                }
                next_token(p);
                node_free(node);
                node = field;
            } else {
                fail(p, "computed_member");
                node_free(node);
                return NULL;
            }
        } else break;
    }
    return node;
}

static BfCondNode *parse_binary(Parser *p, int level);

static BfCondNode *parse_unary(Parser *p) {
    if (is_word(p, "not")) {
        /* Python's not binds looser than a comparison: not a == b is not (a == b). */
        BfCondNode *operand, *node;
        next_token(p);
        operand = parse_binary(p, 0);
        if (!operand) return NULL;
        node = node_with(N_NOT, NULL, operand, NULL, NULL);
        if (!node) fail(p, "out_of_memory");
        return node;
    }
    if (is_punct(p, "!") || is_punct(p, "-") || is_punct(p, "+")) {
        char op = p->text[0];
        BfCondNode *operand, *node;
        next_token(p);
        operand = parse_unary(p);
        if (!operand) return NULL;
        if (op == '+') { fail(p, "unary_plus"); node_free(operand); return NULL; }
        node = node_with(op == '!' ? N_NOT : N_NEG, NULL, operand, NULL, NULL);
        if (!node) fail(p, "out_of_memory");
        return node;
    }
    return parse_postfix(p);
}

typedef struct { const char *token; const char *name; } BinOp;

static const BinOp MULTIPLICATIVE[] = {{"*", "mul"}, {"/", "div"}, {"%", "mod"}, {NULL, NULL}};
static const BinOp ADDITIVE[] = {{"+", "add"}, {"-", "sub"}, {NULL, NULL}};
static const BinOp RELATIONAL[] = {{"<=", "le"}, {">=", "ge"}, {"<", "lt"}, {">", "gt"}, {NULL, NULL}};
static const BinOp EQUALITY[] = {{"===", "seq"}, {"!==", "sne"}, {"==", "eq"}, {"!=", "ne"}, {NULL, NULL}};

static BfCondNode *parse_binary(Parser *p, int level) {
    static const BinOp *const levels[] = {EQUALITY, RELATIONAL, ADDITIVE, MULTIPLICATIVE};
    BfCondNode *left;
    if (level > 3) return parse_unary(p);
    left = parse_binary(p, level + 1);
    while (left) {
        const BinOp *match = NULL;
        BfCondNode *right, *node;
        if (level == 0 && (is_word(p, "in") || is_word(p, "not") || is_word(p, "is"))) {
            int membership = strcmp(p->text, "is") != 0, negate = !strcmp(p->text, "not");
            next_token(p);
            if (negate && !is_word(p, "in")) { fail(p, "syntax:not"); node_free(left); return NULL; }
            if (negate) next_token(p);
            if (!membership && is_word(p, "not")) { negate = 1; next_token(p); }
            right = parse_binary(p, level + 1);
            if (!right) { node_free(left); return NULL; }
            if (!membership && right->kind != N_NULL) {
                fail(p, "identity_comparison");
                node_free(left); node_free(right);
                return NULL;
            }
            node = membership ? node_with(N_CALL, "py_in", right, left, NULL)
                              : node_with(N_BIN, "pyeq", left, right, NULL);
            if (node && negate) node = node_with(N_NOT, NULL, node, NULL, NULL);
            if (!node) fail(p, "out_of_memory");
            left = node;
            continue;
        }
        for (const BinOp *op = levels[level]; op->token; ++op)
            if (is_punct(p, op->token)) { match = op; break; }
        if (!match) break;
        next_token(p);
        right = parse_binary(p, level + 1);
        if (!right) { node_free(left); return NULL; }
        node = node_with(N_BIN, p->python && level == 0 ? (match->name[0] == 'e' || !strcmp(match->name, "seq") ? "pyeq" : "pyne") : match->name, left, right, NULL);
        if (!node) fail(p, "out_of_memory");
        left = node;
    }
    return left;
}

static BfCondNode *parse_logical(Parser *p, int level) {
    /* level 0: || and ??, level 1: && */
    BfCondNode *left = level ? parse_binary(p, 0) : parse_logical(p, 1);
    while (left) {
        int kind;
        BfCondNode *right, *node;
        if (level && (is_punct(p, "&&") || is_word(p, "and"))) kind = N_AND;
        else if (!level && (is_punct(p, "||") || is_word(p, "or"))) kind = N_OR;
        else if (!level && is_punct(p, "??")) kind = N_NULLISH;
        else break;
        next_token(p);
        right = level ? parse_binary(p, 0) : parse_logical(p, 1);
        if (!right) { node_free(left); return NULL; }
        node = node_with(kind, NULL, left, right, NULL);
        if (!node) fail(p, "out_of_memory");
        left = node;
    }
    return left;
}

static BfCondNode *parse_expression(Parser *p) {
    BfCondNode *test = parse_logical(p, 0), *yes, *no, *node;
    if (!test || !is_punct(p, "?")) return test;
    next_token(p);
    yes = parse_expression(p);
    if (!yes) { node_free(test); return NULL; }
    if (!is_punct(p, ":")) { fail(p, "syntax:conditional"); node_free(test); node_free(yes); return NULL; }
    next_token(p);
    no = parse_expression(p);
    if (!no) { node_free(test); node_free(yes); return NULL; }
    node = node_with(N_COND, NULL, test, yes, no);
    if (!node) fail(p, "out_of_memory");
    return node;
}

/* ------------------------------------------------------- canonical output */

typedef struct { char *data; size_t len, cap; int failed; } Buf;

static void buf_put(Buf *buf, const char *text) {
    size_t len = strlen(text);
    if (buf->failed) return;
    if (buf->len + len + 1 > buf->cap) {
        size_t cap = buf->cap ? buf->cap * 2 : 128;
        char *next;
        while (cap < buf->len + len + 1) cap *= 2;
        next = realloc(buf->data, cap);
        if (!next) { buf->failed = 1; return; }
        buf->data = next;
        buf->cap = cap;
    }
    memcpy(buf->data + buf->len, text, len + 1);
    buf->len += len;
}

static void buf_quoted(Buf *buf, const char *text) {
    char piece[8];
    buf_put(buf, "\"");
    for (const unsigned char *c = (const unsigned char *)text; *c; ++c) {
        if (*c == '"' || *c == '\\') snprintf(piece, sizeof(piece), "\\%c", *c);
        else if (*c < 0x20) snprintf(piece, sizeof(piece), "\\u%04x", *c);
        else snprintf(piece, sizeof(piece), "%c", *c);
        buf_put(buf, piece);
    }
    buf_put(buf, "\"");
}

static int is_identifier(const char *text) {
    if (!text || !(isalpha((unsigned char)*text) || *text == '_')) return 0;
    for (const char *c = text; *c; ++c)
        if (!isalnum((unsigned char)*c) && *c != '_') return 0;
    return 1;
}

static void format_number(double value, char *out, size_t cap) {
    if (isnan(value)) snprintf(out, cap, "NaN");
    else if (isinf(value)) snprintf(out, cap, "%s", value < 0 ? "-Infinity" : "Infinity");
    else if (value == floor(value) && fabs(value) < 1e15) snprintf(out, cap, "%.0f", value);
    else snprintf(out, cap, "%.17g", value);
}

/* erase = 1 writes the shape: the operations, with every name and literal
 * replaced by its kind. Two plans of one shape are not thereby equivalent. */
static void print_node(Buf *buf, const BfCondNode *node, int erase) {
    static const char *const heads[] = {
        [N_ARR] = "arr", [N_NOT] = "not", [N_NEG] = "neg", [N_AND] = "and", [N_OR] = "or",
        [N_NULLISH] = "nullish", [N_COND] = "if", [N_PRESENT] = "present", [N_PYBOOL] = "py", [N_INDEX] = "index"
    };
    char number[40];
    switch (node->kind) {
    case N_DOC: buf_put(buf, "(doc)"); return;
    case N_PARENTROOT: buf_put(buf, "(parent)"); return;
    case N_FIELD: case N_PARENT: case N_ENV:
        buf_put(buf, node->kind == N_FIELD ? "(field " : node->kind == N_PARENT ? "(parent " : "(env ");
        if (erase) buf_put(buf, "_");
        else if (node->kind == N_ENV || is_identifier(node->text)) buf_put(buf, node->text);
        else buf_quoted(buf, node->text);
        buf_put(buf, ")");
        return;
    case N_STR: if (erase) buf_put(buf, "s"); else buf_quoted(buf, node->text); return;
    case N_NUM:
        if (erase) { buf_put(buf, "n"); return; }
        format_number(node->num, number, sizeof(number));
        buf_put(buf, number);
        return;
    case N_BOOL: buf_put(buf, node->num != 0 ? "true" : "false"); return;
    case N_NULL: buf_put(buf, "null"); return;
    case N_UNDEF: buf_put(buf, "undefined"); return;
    case N_GET: case N_OPTGET:
        buf_put(buf, node->kind == N_GET ? "(get " : "(get? ");
        print_node(buf, node->kids[0], erase);
        buf_put(buf, " ");
        buf_put(buf, node->text);
        buf_put(buf, ")");
        return;
    default: break;
    }
    buf_put(buf, "(");
    buf_put(buf, node->kind == N_BIN || node->kind == N_CALL ? node->text : heads[node->kind]);
    if (erase && node->kind == N_ARR) {
        /* A literal list's length is a parameter, not part of the shape. */
        buf_put(buf, node->n ? " ..." : "");
    } else {
        for (size_t i = 0; i < node->n; ++i) {
            buf_put(buf, " ");
            print_node(buf, node->kids[i], erase);
        }
    }
    buf_put(buf, ")");
}

static void hash_id(const char *prefix, const char *text, char out[40]) {
    char hex[65];
    bf_sha256_hex((const uint8_t *)text, strlen(text), hex);
    snprintf(out, 40, "%s%.32s", prefix, hex);
}

/* ------------------------------------------------------- static analysis */

static int add_read(char ***items, size_t *count, const char *value) {
    char **next;
    for (size_t i = 0; i < *count; ++i)
        if (!strcmp((*items)[i], value)) return 0;
    next = realloc(*items, sizeof(char *) * (*count + 1));
    if (!next) return -1;
    *items = next;
    if (!((*items)[*count] = strdup(value))) return -1;
    (*count)++;
    return 0;
}

static int collect_reads(const BfCondNode *node, BfCondPlan *plan) {
    int rc = 0;
    if (node->kind == N_FIELD) rc = add_read(&plan->reads, &plan->read_count, node->text);
    else if (node->kind == N_PARENT) rc = add_read(&plan->parent_reads, &plan->parent_read_count, node->text);
    else if (node->kind == N_ENV) rc = add_read(&plan->env_reads, &plan->env_read_count, node->text);
    /* The whole document used as a value depends on every field of it. */
    else if (node->kind == N_DOC) rc = add_read(&plan->reads, &plan->read_count, "*");
    else if (node->kind == N_PARENTROOT) rc = add_read(&plan->parent_reads, &plan->parent_read_count, "*");
    for (size_t i = 0; !rc && i < node->n; ++i) rc = collect_reads(node->kids[i], plan);
    return rc;
}

/* 1 always truthy, 0 always falsy, -1 depends on the document. Only literal
 * operands are folded, so a verdict here holds for every document that does
 * not make the expression throw. */
static int static_truth(const BfCondNode *node) {
    int a, b;
    switch (node->kind) {
    case N_STR: return node->text[0] != '\0';
    case N_NUM: return node->num != 0 && !isnan(node->num);
    case N_BOOL: return node->num != 0;
    case N_NULL: case N_UNDEF: return 0;
    case N_ARR: return 1;
    case N_NOT:
        a = static_truth(node->kids[0]);
        return a < 0 ? -1 : !a;
    case N_OR:
        a = static_truth(node->kids[0]); b = static_truth(node->kids[1]);
        if (a == 1 || b == 1) return 1;
        return a == 0 && b == 0 ? 0 : -1;
    case N_AND:
        a = static_truth(node->kids[0]); b = static_truth(node->kids[1]);
        if (a == 0 || b == 0) return 0;
        return a == 1 && b == 1 ? 1 : -1;
    case N_COND:
        a = static_truth(node->kids[1]); b = static_truth(node->kids[2]);
        return a == b ? a : -1;
    default: return -1;
    }
}

/* ---------------------------------------------------------------- compile */

static void name_arrow_callback(const char *expression, const char *arrow, char *out, size_t cap) {
    const char *c = arrow;
    while (c > expression) {
        --c;
        if (*c == '(' && c > expression && (isalnum((unsigned char)c[-1]) || c[-1] == '_')) {
            const char *end = c, *start = c;
            while (start > expression && (isalnum((unsigned char)start[-1]) || start[-1] == '_')) --start;
            snprintf(out, cap, "arrow_callback:%.*s", (int)(end - start > 60 ? 60 : end - start), start);
            return;
        }
    }
    snprintf(out, cap, "arrow_callback");
}

static int compile_source(const char *source, int python, BfCondPlan *out) {
    Parser parser;
    Buf plan = {0}, shape = {0};
    BfCondNode *root = NULL;
    int truth;

    memset(out, 0, sizeof(*out));
    if (!source) { snprintf(out->missing_operation, sizeof(out->missing_operation), "empty"); return -1; }

    if (!strncmp(source, "fn:", 3)) {
        snprintf(out->form, sizeof(out->form), "fn");
        snprintf(out->missing_operation, sizeof(out->missing_operation), "client_script:%.70s", source + 3);
        return -1;
    }
    if (strncmp(source, "eval:", 5)) {
        BfCondNode *field = node_new(N_FIELD, source);
        snprintf(out->form, sizeof(out->form), "bare");
        root = node_with(N_PRESENT, NULL, field, NULL, NULL);
        if (!root) { snprintf(out->missing_operation, sizeof(out->missing_operation), "out_of_memory"); return -1; }
        if (!is_identifier(source)) {
            snprintf(out->constant, sizeof(out->constant), "always_false");
            snprintf(out->diagnostic, sizeof(out->diagnostic),
                     "without the eval: prefix the donor looks this text up as a field name and does not evaluate it");
        }
    } else {
        const char *arrow = strstr(source + 5, "=>");
        snprintf(out->form, sizeof(out->form), "eval");
        if (arrow) {
            name_arrow_callback(source + 5, arrow, out->missing_operation, sizeof(out->missing_operation));
            return -1;
        }
        memset(&parser, 0, sizeof(parser));
        parser.python = python;
        parser.src = source + 5;
        next_token(&parser);
        if (parser.type == T_BAD) fail(&parser, "syntax:%.80s", parser.text);
        else root = parse_expression(&parser);
        if (root && parser.type == T_BAD) fail(&parser, "syntax:%.80s", parser.text);
        if (root && !parser.missing[0]) {
            while (is_punct(&parser, ";")) next_token(&parser);
            /* The donor wraps the text as `let out = <text>; return out`, so a
             * second statement would run and be discarded. Refused, not guessed. */
            if (parser.type != T_END) fail(&parser, "syntax:trailing:%.60s", parser.text);
        }
        if (!root || parser.missing[0]) {
            node_free(root);
            snprintf(out->missing_operation, sizeof(out->missing_operation), "%s",
                     parser.missing[0] ? parser.missing : "syntax:unparsed");
            return -1;
        }
        if (python) {
            BfCondNode *wrapped = node_with(N_PYBOOL, NULL, root, NULL, NULL);
            if (!wrapped) {
                snprintf(out->missing_operation, sizeof(out->missing_operation), "out_of_memory");
                return -1;
            }
            root = wrapped;
        }
        truth = python ? -1 : static_truth(root);
        if (truth >= 0) {
            snprintf(out->constant, sizeof(out->constant), truth ? "always_true" : "always_false");
            snprintf(out->diagnostic, sizeof(out->diagnostic),
                     "a literal operand decides the result for every document; the source is kept as written");
        }
    }

    print_node(&plan, root, 0);
    print_node(&shape, root, 1);
    if (plan.failed || shape.failed || collect_reads(root, out) != 0) {
        free(plan.data); free(shape.data); node_free(root);
        bf_cond_plan_free(out);
        snprintf(out->missing_operation, sizeof(out->missing_operation), "out_of_memory");
        return -1;
    }
    out->plan = plan.data;
    out->shape = shape.data;
    out->root = root;
    out->compiled = 1;
    hash_id("cp-", out->plan, out->plan_id);
    hash_id("cs-", out->shape, out->shape_id);
    return 0;
}

static void free_strings(char **items, size_t count) {
    for (size_t i = 0; i < count; ++i) free(items[i]);
    free(items);
}

int bf_cond_compile(const char *source, BfCondPlan *out) {
    return compile_source(source, 0, out);
}

/* The donor evaluates a Notification's condition on its server, in Python. It
 * is given here as the bare expression and compiled under that dialect. */
int bf_cond_compile_python(const char *expression, BfCondPlan *out) {
    size_t len = expression ? strlen(expression) + 6 : 0;
    char *prefixed = len ? malloc(len) : NULL;
    int rc;
    if (!prefixed) {
        memset(out, 0, sizeof(*out));
        snprintf(out->missing_operation, sizeof(out->missing_operation), "empty");
        return -1;
    }
    snprintf(prefixed, len, "eval:%s", expression);
    rc = compile_source(prefixed, 1, out);
    snprintf(out->form, sizeof(out->form), "python");
    free(prefixed);
    return rc;
}

void bf_cond_plan_free(BfCondPlan *plan) {
    if (!plan) return;
    free(plan->plan);
    free(plan->shape);
    free_strings(plan->reads, plan->read_count);
    free_strings(plan->parent_reads, plan->parent_read_count);
    free_strings(plan->env_reads, plan->env_read_count);
    node_free(plan->root);
    plan->plan = plan->shape = NULL;
    plan->reads = plan->parent_reads = plan->env_reads = NULL;
    plan->read_count = plan->parent_read_count = plan->env_read_count = 0;
    plan->root = NULL;
    plan->compiled = 0;
}

/* --------------------------------------------------------------- evaluate */

enum { V_UNDEF, V_NULL, V_BOOL, V_NUM, V_STR, V_ARR, V_OBJ };

typedef struct Value {
    int type;
    double num;
    const char *str;       /* V_STR text, or V_OBJ JSON path */
    struct Value *items;
    size_t count;
    int which;             /* V_OBJ: 0 = doc, 1 = parent */
    int is_float;          /* V_NUM that Python would print with a decimal point */
} Value;

typedef struct {
    sqlite3 *db;
    const char *documents[3];  /* doc, parent, environment */
    char *error;
    size_t error_cap;
    int failed;
    int python;
    void **held;
    size_t held_count;
} Eval;

static Value eval_node(Eval *e, const BfCondNode *node);

static Value undefined_value(void) { Value v; memset(&v, 0, sizeof(v)); return v; }
static Value bool_value(int b) { Value v = undefined_value(); v.type = V_BOOL; v.num = b ? 1 : 0; return v; }
static Value num_value(double n) { Value v = undefined_value(); v.type = V_NUM; v.num = n; return v; }

static Value eval_fail(Eval *e, const char *fmt, ...) {
    if (!e->failed) {
        va_list args;
        e->failed = 1;
        if (e->error && e->error_cap) {
            va_start(args, fmt);
            vsnprintf(e->error, e->error_cap, fmt, args);
            va_end(args);
        }
    }
    return undefined_value();
}

static void *eval_hold(Eval *e, void *pointer) {
    void **next;
    if (!pointer) { eval_fail(e, "out of memory"); return NULL; }
    next = realloc(e->held, sizeof(void *) * (e->held_count + 1));
    if (!next) { free(pointer); eval_fail(e, "out of memory"); return NULL; }
    e->held = next;
    e->held[e->held_count++] = pointer;
    return pointer;
}

static Value str_value(Eval *e, const char *text) {
    Value v = undefined_value();
    char *copy = eval_hold(e, strdup(text ? text : ""));
    if (!copy) return v;
    v.type = V_STR;
    v.str = copy;
    return v;
}

static Value json_atom(Eval *e, const char *type, sqlite3_stmt *stmt, int column, int which, const char *path) {
    Value v = undefined_value();
    if (!type) return v;
    if (!strcmp(type, "null")) v.type = V_NULL;
    else if (!strcmp(type, "true")) return bool_value(1);
    else if (!strcmp(type, "false")) return bool_value(0);
    else if (!strcmp(type, "integer") || !strcmp(type, "real")) {
        v = num_value(sqlite3_column_double(stmt, column));
        v.is_float = type[0] == 'r';
        return v;
    }
    else if (!strcmp(type, "text")) return str_value(e, (const char *)sqlite3_column_text(stmt, column));
    else {
        v = str_value(e, path);
        if (v.type == V_STR) { v.type = V_OBJ; v.which = which; }
    }
    return v;
}

static Value read_path(Eval *e, int which, const char *path) {
    sqlite3_stmt *stmt = NULL;
    Value v = undefined_value();
    const char *document = e->documents[which];
    if (!document) return eval_fail(e, which == 1 ? "parent document is not bound" : "environment is not bound");
    if (sqlite3_prepare_v2(e->db, "SELECT json_valid(?1), json_type(?1, ?2), json_extract(?1, ?2)", -1, &stmt, NULL) != SQLITE_OK)
        return eval_fail(e, "store cannot read JSON");
    sqlite3_bind_text(stmt, 1, document, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, path, -1, SQLITE_STATIC);
    if (sqlite3_step(stmt) != SQLITE_ROW || sqlite3_column_int(stmt, 0) != 1) {
        sqlite3_finalize(stmt);
        return eval_fail(e, "document is not valid JSON");
    }
    {
        const char *type = (const char *)sqlite3_column_text(stmt, 1);
        if (type && !strcmp(type, "array")) {
            sqlite3_stmt *each = NULL;
            sqlite3_finalize(stmt);
            v.type = V_ARR;
            /* Where it was read from is what the donor's JavaScript means by
             * the same list: one place in one record is one object. */
            {
                Value at = str_value(e, path);
                v.str = at.type == V_STR ? at.str : NULL;
                v.which = which;
            }
            if (sqlite3_prepare_v2(e->db, "SELECT type, atom, fullkey FROM json_each(?1, ?2)", -1, &each, NULL) != SQLITE_OK)
                return eval_fail(e, "store cannot read JSON");
            sqlite3_bind_text(each, 1, document, -1, SQLITE_STATIC);
            sqlite3_bind_text(each, 2, path, -1, SQLITE_STATIC);
            while (sqlite3_step(each) == SQLITE_ROW) {
                Value *next = realloc(v.items, sizeof(Value) * (v.count + 1));
                if (!next) { free(v.items); sqlite3_finalize(each); return eval_fail(e, "out of memory"); }
                v.items = next;
                v.items[v.count++] = json_atom(e, (const char *)sqlite3_column_text(each, 0), each, 1, which,
                                               (const char *)sqlite3_column_text(each, 2));
            }
            sqlite3_finalize(each);
            if (v.items && !eval_hold(e, v.items)) return undefined_value();
            return v;
        }
        v = json_atom(e, type, stmt, 2, which, path);
    }
    sqlite3_finalize(stmt);
    return v;
}

static Value read_member(Eval *e, int which, const char *base, const char *name) {
    size_t len = strlen(base) + strlen(name) + 4;
    char *path;
    /* A key holding a quote or backslash cannot be a field of a document. */
    if (strchr(name, '"') || strchr(name, '\\')) return undefined_value();
    path = eval_hold(e, malloc(len));
    if (!path) return undefined_value();
    snprintf(path, len, "%s.\"%s\"", base, name);
    return read_path(e, which, path);
}

static int truthy(const Eval *e, Value v) {
    /* Python counts an empty list as false; JavaScript does not. */
    if (e->python && v.type == V_ARR) return v.count > 0;
    switch (v.type) {
    case V_UNDEF: case V_NULL: return 0;
    case V_BOOL: return v.num != 0;
    case V_NUM: return v.num != 0 && !isnan(v.num);
    case V_STR: return v.str[0] != '\0';
    default: return 1;
    }
}

static Value to_string(Eval *e, Value v) {
    char number[40];
    if (e->python) {
        /* str() as Python prints it. */
        if (v.type == V_UNDEF || v.type == V_NULL) return str_value(e, "None");
        if (v.type == V_BOOL) return str_value(e, v.num != 0 ? "True" : "False");
        if (v.type == V_NUM && v.is_float && !isnan(v.num) && !isinf(v.num)) {
            for (int digits = 15; digits <= 17; ++digits) {
                snprintf(number, sizeof(number), "%.*g", digits, v.num);
                if (strtod(number, NULL) == v.num) break;
            }
            if (!strpbrk(number, ".e")) strncat(number, ".0", sizeof(number) - strlen(number) - 1);
            return str_value(e, number);
        }
    }
    switch (v.type) {
    case V_UNDEF: return str_value(e, "undefined");
    case V_NULL: return str_value(e, "null");
    case V_BOOL: return str_value(e, v.num != 0 ? "true" : "false");
    case V_NUM: format_number(v.num, number, sizeof(number)); return str_value(e, number);
    case V_STR: return v;
    case V_OBJ: return str_value(e, "[object Object]");
    default: {
        Buf buf = {0};
        Value out;
        buf_put(&buf, "");
        for (size_t i = 0; i < v.count; ++i) {
            if (i) buf_put(&buf, ",");
            if (v.items[i].type != V_UNDEF && v.items[i].type != V_NULL) {
                Value item = to_string(e, v.items[i]);
                if (item.type == V_STR) buf_put(&buf, item.str);
            }
        }
        out = str_value(e, buf.data ? buf.data : "");
        free(buf.data);
        return out;
    }
    }
}

static double to_number(Eval *e, Value v) {
    switch (v.type) {
    case V_UNDEF: return NAN;
    case V_NULL: return 0;
    case V_BOOL: case V_NUM: return v.num;
    case V_OBJ: return NAN;
    case V_ARR: return to_number(e, to_string(e, v));
    default: {
        const char *s = v.str;
        char *end = NULL;
        double n;
        while (isspace((unsigned char)*s)) ++s;
        if (!*s) return 0;
        n = strtod(s, &end);
        while (isspace((unsigned char)*end)) ++end;
        return *end ? NAN : n;
    }
    }
}

static int strict_equal(Eval *e, Value a, Value b) {
    if (a.type != b.type) return 0;
    switch (a.type) {
    case V_UNDEF: case V_NULL: return 1;
    case V_BOOL: case V_NUM: return a.num == b.num;
    case V_STR: return !strcmp(a.str, b.str);
    default:
        /* The form dialect compares lists and records by identity. Two values
         * read from the same place in the same record are the same object; a
         * value from anywhere else, or a list the expression itself wrote, is
         * a different one however alike they look. The server dialect compares
         * lists by what they hold, which is not carried here. */
        if (e->python) {
            eval_fail(e, "comparison of two lists or records by value is not supported");
            return 0;
        }
        return a.str && b.str && a.which == b.which && !strcmp(a.str, b.str);
    }
}

static int loose_equal(Eval *e, Value a, Value b) {
    int a_nullish = a.type == V_UNDEF || a.type == V_NULL;
    int b_nullish = b.type == V_UNDEF || b.type == V_NULL;
    if (a_nullish || b_nullish) return a_nullish && b_nullish;
    if (a.type == b.type) return strict_equal(e, a, b);
    if (a.type == V_OBJ || b.type == V_OBJ) {
        eval_fail(e, "loose comparison against a record is not supported");
        return 0;
    }
    if (a.type == V_ARR) return loose_equal(e, to_string(e, a), b);
    if (b.type == V_ARR) return loose_equal(e, a, to_string(e, b));
    return to_number(e, a) == to_number(e, b);
}

static size_t utf16_length(const char *text) {
    size_t units = 0;
    for (const unsigned char *c = (const unsigned char *)text; *c; ++c) {
        if ((*c & 0xC0) == 0x80) continue;
        units += *c >= 0xF0 ? 2 : 1;
    }
    return units;
}

static Value eval_binary(Eval *e, const BfCondNode *node) {
    Value a = eval_node(e, node->kids[0]);
    Value b = e->failed ? undefined_value() : eval_node(e, node->kids[1]);
    const char *op = node->text;
    double x, y;
    if (e->failed) return undefined_value();
    if (!strcmp(op, "pyeq") || !strcmp(op, "pyne")) {
        /* No coercion between text and number; True and 1 are the same value. */
        int numeric = (a.type == V_BOOL || a.type == V_NUM) && (b.type == V_BOOL || b.type == V_NUM);
        int same = numeric ? a.num == b.num
                 : (a.type == V_UNDEF || a.type == V_NULL) && (b.type == V_UNDEF || b.type == V_NULL) ? 1
                 : a.type == b.type && strict_equal(e, a, b);
        return bool_value(op[2] == 'e' ? same : !same);
    }
    if (e->python && (op[0] == 'l' || op[0] == 'g') &&
        !((a.type == V_STR && b.type == V_STR) ||
          ((a.type == V_BOOL || a.type == V_NUM) && (b.type == V_BOOL || b.type == V_NUM))))
        return eval_fail(e, "the donor expression throws here: ordering values of different kinds");
    if (!strcmp(op, "seq")) return bool_value(strict_equal(e, a, b));
    if (!strcmp(op, "sne")) return bool_value(!strict_equal(e, a, b));
    if (!strcmp(op, "eq")) return bool_value(loose_equal(e, a, b));
    if (!strcmp(op, "ne")) return bool_value(!loose_equal(e, a, b));
    if (!strcmp(op, "add") && (a.type >= V_STR || b.type >= V_STR)) {
        Value left = to_string(e, a), right = to_string(e, b), out;
        Buf buf = {0};
        if (e->failed) return undefined_value();
        buf_put(&buf, left.str);
        buf_put(&buf, right.str);
        out = str_value(e, buf.data ? buf.data : "");
        free(buf.data);
        return out;
    }
    if (a.type == V_STR && b.type == V_STR && (op[0] == 'l' || op[0] == 'g')) {
        int order = strcmp(a.str, b.str);
        return bool_value(!strcmp(op, "lt") ? order < 0 : !strcmp(op, "le") ? order <= 0 :
                          !strcmp(op, "gt") ? order > 0 : order >= 0);
    }
    x = to_number(e, a);
    y = to_number(e, b);
    if (!strcmp(op, "lt")) return bool_value(x < y);
    if (!strcmp(op, "le")) return bool_value(x <= y);
    if (!strcmp(op, "gt")) return bool_value(x > y);
    if (!strcmp(op, "ge")) return bool_value(x >= y);
    if (!strcmp(op, "add")) return num_value(x + y);
    if (!strcmp(op, "sub")) return num_value(x - y);
    if (!strcmp(op, "mul")) return num_value(x * y);
    if (!strcmp(op, "div")) return num_value(x / y);
    return num_value(fmod(x, y));
}

/* ---- dates ---------------------------------------------------------------
 * The donor formats a date by a setting of the site or its user. This estate
 * states one format of its own and uses it wherever the donor would consult
 * that setting: the ISO 8601 calendar date first, so text sorts as time does,
 * then the weekday for the person reading it, and the 24-hour clock. No zone
 * is added: a time stored on a record is in whatever zone the record means,
 * and stamping this estate's zone on it would state something nobody recorded.
 *
 *   2026-10-02 Fri          2026-10-02 Fri 14:30
 *
 * An explicit strftime pattern is the template author's own and is honoured. */

typedef struct { struct tm tm; int has_date, has_time; } Moment;

static int parse_moment(const char *text, Moment *out) {
    int y = 0, mo = 0, d = 0, h = 0, mi = 0, s = 0, used = 0;
    static const int cumulative[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};
    memset(out, 0, sizeof(*out));
    while (isspace((unsigned char)*text)) ++text;
    if (sscanf(text, "%4d-%2d-%2d%n", &y, &mo, &d, &used) == 3 && used >= 8) {
        static const int shift[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
        int yy = y - (mo < 3);
        int leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        if (mo < 1 || mo > 12 || d < 1 || d > 31) return -1;
        out->has_date = 1;
        text += used;
        /* Sakamoto's weekday; 0 is Sunday as struct tm counts. */
        out->tm.tm_wday = (yy + yy / 4 - yy / 100 + yy / 400 + shift[mo - 1] + d) % 7;
        out->tm.tm_yday = cumulative[mo - 1] + d - 1 + (leap && mo > 2);
        if (*text == 'T' || *text == ' ') ++text;
    }
    used = 0;
    if (sscanf(text, "%2d:%2d%n", &h, &mi, &used) == 2 && used >= 3) {
        text += used;
        if (*text == ':') sscanf(text + 1, "%2d", &s);
        if (h > 23 || mi > 59 || s > 60) return -1;
        out->has_time = 1;
    } else if (*text && !isspace((unsigned char)*text)) return -1;
    if (!out->has_date && !out->has_time) return -1;
    out->tm.tm_year = y - 1900; out->tm.tm_mon = mo ? mo - 1 : 0; out->tm.tm_mday = d ? d : 1;
    out->tm.tm_hour = h; out->tm.tm_min = mi; out->tm.tm_sec = s;
    return 0;
}

/* want: 1 the date, 2 the time, 3 both. */
static Value estate_moment(Eval *e, Value v, int want) {
    static const char *const days[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    Moment m;
    char text[120];
    size_t len = 0;
    /* The donor prints nothing for a date that is not there. */
    if (v.type == V_UNDEF || v.type == V_NULL || (v.type == V_STR && !v.str[0])) return str_value(e, "");
    if (v.type != V_STR || parse_moment(v.str, &m) != 0)
        return eval_fail(e, "the value is not a date or time this estate can read");
    if ((want & 1) && m.has_date)
        len += (size_t)snprintf(text + len, sizeof(text) - len, "%04d-%02d-%02d %s", m.tm.tm_year + 1900,
                                m.tm.tm_mon + 1, m.tm.tm_mday, days[m.tm.tm_wday]);
    if ((want & 2) && m.has_time)
        len += (size_t)snprintf(text + len, sizeof(text) - len, "%s%02d:%02d", len ? " " : "",
                                m.tm.tm_hour, m.tm.tm_min);
    if (!len) return eval_fail(e, "the value carries no %s", want == 2 ? "time" : "date");
    return str_value(e, text);
}

static Value eval_call(Eval *e, const BfCondNode *node) {
    const char *op = node->text;
    Value a = eval_node(e, node->kids[0]);
    if (e->failed) return undefined_value();
    if (!strcmp(op, "boolean")) return bool_value(truthy(e, a));
    if (!strcmp(op, "slice")) {
        /* String.slice and Array.slice: a negative index counts from the end. */
        Value from = eval_node(e, node->kids[1]);
        Value to = node->n > 2 ? eval_node(e, node->kids[2]) : undefined_value();
        Value out = undefined_value();
        double size, start, stop;
        if (e->failed) return out;
        if (a.type != V_ARR && a.type != V_STR) return eval_fail(e, "the donor expression throws here: slice of a value that has none");
        size = a.type == V_ARR ? (double)a.count : (double)strlen(a.str);
        start = to_number(e, from); stop = node->n > 2 ? to_number(e, to) : size;
        if (isnan(start)) start = 0;
        if (isnan(stop)) stop = 0;
        if (start < 0) start += size;
        if (stop < 0) stop += size;
        if (start < 0) start = 0;
        if (stop > size) stop = size;
        if (stop < start) stop = start;
        if (a.type == V_STR) {
            char *text = eval_hold(e, strndup(a.str + (size_t)start, (size_t)(stop - start)));
            if (!text) return out;
            out.type = V_STR; out.str = text;
            return out;
        }
        out.type = V_ARR;
        out.items = a.items + (size_t)start;
        out.count = (size_t)(stop - start);
        return out;
    }
    if (!strcmp(op, "py_linktext")) {
        /* The donor links a record's name to its form in the donor's desk. No
         * record of that kind is held here and no desk is served, so the name
         * is written and the link is not: a link would claim a page exists. */
        Value label = eval_node(e, node->kids[node->n - 1]);
        return e->failed ? undefined_value() : to_string(e, label);
    }
    if (!strcmp(op, "py_bold")) {
        Value text = to_string(e, a), out;
        Buf buf = {0};
        if (e->failed) return undefined_value();
        buf_put(&buf, "<strong>"); buf_put(&buf, text.str); buf_put(&buf, "</strong>");
        out = str_value(e, buf.data ? buf.data : "");
        free(buf.data);
        return out;
    }
    if (!strcmp(op, "py_formatdate")) return estate_moment(e, a, 1);
    if (!strcmp(op, "py_formattime")) return estate_moment(e, a, 2);
    if (!strcmp(op, "py_formatdatetime")) return estate_moment(e, a, 3);
    if (!strcmp(op, "py_formatvalue")) {
        /* With no field named the donor formats by what the value is. */
        Moment m;
        if (a.type == V_STR && parse_moment(a.str, &m) == 0) return estate_moment(e, a, 3);
        return to_string(e, a);
    }
    if (!strcmp(op, "py_datetime") || !strcmp(op, "py_date")) {
        Moment m;
        char text[40];
        if (a.type != V_STR || parse_moment(a.str, &m) != 0 || !m.has_date)
            return eval_fail(e, "the value is not a date this estate can read");
        if (!strcmp(op, "py_date")) snprintf(text, sizeof(text), "%04d-%02d-%02d", m.tm.tm_year + 1900, m.tm.tm_mon + 1, m.tm.tm_mday);
        else snprintf(text, sizeof(text), "%04d-%02d-%02d %02d:%02d:%02d", m.tm.tm_year + 1900, m.tm.tm_mon + 1,
                      m.tm.tm_mday, m.tm.tm_hour, m.tm.tm_min, m.tm.tm_sec);
        return str_value(e, text);
    }
    if (!strcmp(op, "py_strftime")) {
        Moment m;
        Value pattern = eval_node(e, node->kids[1]);
        char text[200];
        if (e->failed) return undefined_value();
        if (a.type != V_STR || pattern.type != V_STR || parse_moment(a.str, &m) != 0 || !m.has_date)
            return eval_fail(e, "strftime needs a date and a pattern");
        for (const char *c = pattern.str; *c; ++c)
            if (*c == '%' && c[1] && !strchr("aAbBdHIjmMpSyY%", *++c))
                return eval_fail(e, "a strftime field this estate does not write");
        if (!strftime(text, sizeof(text), pattern.str, &m.tm)) text[0] = '\0';
        return str_value(e, text);
    }
    if (!strcmp(op, "py_format")) {
        /* str.format with positional fields: {} in order, or {0} {1} by number. */
        Value text = to_string(e, a), out;
        Buf buf = {0};
        size_t next = 1;
        if (e->failed) return undefined_value();
        buf_put(&buf, "");
        for (const char *c = text.str; *c && !e->failed; ++c) {
            char piece[2] = {*c, '\0'};
            if ((c[0] == '{' && c[1] == '{') || (c[0] == '}' && c[1] == '}')) { buf_put(&buf, piece); ++c; continue; }
            if (*c == '{') {
                const char *close = strchr(c, '}');
                size_t index = next;
                Value arg;
                if (!close) { eval_fail(e, "the donor expression throws here: unmatched brace in format"); break; }
                if (close > c + 1) {
                    char *end = NULL;
                    index = (size_t)strtoul(c + 1, &end, 10) + 1;
                    if (end != close) { eval_fail(e, "a named or styled format field is not supported"); break; }
                } else next++;
                if (index >= node->n) { eval_fail(e, "the donor expression throws here: format index out of range"); break; }
                arg = to_string(e, eval_node(e, node->kids[index]));
                if (!e->failed) buf_put(&buf, arg.str);
                c = close;
                continue;
            }
            buf_put(&buf, piece);
        }
        out = e->failed ? undefined_value() : str_value(e, buf.data ? buf.data : "");
        free(buf.data);
        return out;
    }
    if (!strcmp(op, "range")) {
        Value b = node->n > 1 ? eval_node(e, node->kids[1]) : undefined_value(), list = undefined_value();
        double from = node->n > 1 ? to_number(e, a) : 0, to = node->n > 1 ? to_number(e, b) : to_number(e, a);
        if (e->failed) return undefined_value();
        if (isnan(from) || isnan(to) || to - from > 10000) return eval_fail(e, "a range this long is not produced");
        list.type = V_ARR;
        if (to > from && !(list.items = eval_hold(e, calloc((size_t)(to - from), sizeof(Value))))) return undefined_value();
        for (double i = from; i < to; ++i) list.items[list.count++] = num_value(i);
        return list;
    }
    if (!strcmp(op, "py_flt")) {
        /* frappe.utils.flt: a number, or 0.0 for anything that is not one,
         * rounded to the stated places. */
        Value places = node->n > 1 ? eval_node(e, node->kids[1]) : undefined_value(), out;
        double n = a.type == V_NUM || a.type == V_BOOL ? a.num : a.type == V_STR ? to_number(e, a) : 0;
        if (e->failed) return undefined_value();
        if (isnan(n)) n = 0;
        if (node->n > 1) {
            char text[64];
            double digits = to_number(e, places);
            if (isnan(digits) || digits < 0 || digits > 12) return eval_fail(e, "flt with that precision is not supported");
            snprintf(text, sizeof(text), "%.*f", (int)digits, n);
            n = strtod(text, NULL);
        }
        out = num_value(n);
        out.is_float = 1;
        return out;
    }
    if (!strcmp(op, "cint")) {
        /* frappe/utils/datatype.js: booleans map directly, otherwise the text
         * loses its leading zeros and is read as an integer prefix; NaN is 0. */
        Value text;
        const char *s;
        char *end = NULL;
        long long n;
        if (a.type == V_BOOL) return num_value(a.num);
        text = to_string(e, a);
        if (e->failed) return undefined_value();
        s = text.str;
        if (strcmp(s, "0")) while (*s == '0') ++s;
        while (isspace((unsigned char)*s)) ++s;
        n = strtoll(s, &end, 10);
        return num_value(end == s ? 0 : (double)n);
    }
    {
        int optional = op[strlen(op) - 1] == '?';
        int index_of = !strncmp(op, "indexof", 7);
        Value list = a, item = eval_node(e, node->kids[1]);
        long found = -1;
        if (e->failed) return undefined_value();
        /* in_list(list, item) is list.includes(item) in the donor, so one path serves both. */
        if (list.type == V_UNDEF || list.type == V_NULL) {
            if (optional) return undefined_value();
            return eval_fail(e, "the donor expression throws here: %s of an absent value",
                             index_of ? "indexOf" : "includes");
        }
        if (list.type == V_ARR) {
            for (size_t i = 0; i < list.count && found < 0; ++i) {
                if (list.items[i].type == V_ARR || list.items[i].type == V_OBJ) continue;
                if (item.type == list.items[i].type && strict_equal(e, list.items[i], item)) found = (long)i;
            }
        } else if (list.type == V_STR) {
            Value needle = to_string(e, item);
            const char *at;
            if (e->failed) return undefined_value();
            at = strstr(list.str, needle.str);
            if (at) {
                char *prefix = eval_hold(e, strndup(list.str, (size_t)(at - list.str)));
                if (!prefix) return undefined_value();
                found = (long)utf16_length(prefix);
            }
        } else {
            return eval_fail(e, "the donor expression throws here: %s is not a function of this value",
                             index_of ? "indexOf" : "includes");
        }
        return index_of ? num_value((double)found) : bool_value(found >= 0);
    }
}

/* A name that is neither doc nor parent is read from what the estate binds.
 * A step the estate does not bind refuses; a bound object simply lacking the
 * last name is undefined, as it would be in the donor. */
static Value eval_environment(Eval *e, const char *path) {
    Value v = undefined_value();
    char segment[96];
    const char *c = path;
    if (!e->documents[2]) return eval_fail(e, "environment value is not bound: %s", path);
    v = str_value(e, "$");
    if (v.type != V_STR) return undefined_value();
    v.type = V_OBJ;
    v.which = 2;
    while (*c && !e->failed) {
        size_t len = strcspn(c, ".");
        if (len >= sizeof(segment)) return eval_fail(e, "environment name is too long");
        memcpy(segment, c, len);
        segment[len] = '\0';
        if (v.type == V_UNDEF || v.type == V_NULL)
            return eval_fail(e, "environment value is not bound: %s", path);
        v = v.type == V_OBJ ? read_member(e, v.which, v.str, segment) : undefined_value();
        c += len;
        if (*c == '.') ++c;
    }
    return v;
}

static Value eval_node(Eval *e, const BfCondNode *node) {
    Value a, v = undefined_value();
    if (e->failed) return v;
    switch (node->kind) {
    case N_DOC: case N_PARENTROOT:
        if (node->kind == N_PARENTROOT && !e->documents[1]) return eval_fail(e, "parent document is not bound");
        v = str_value(e, "$");
        if (v.type == V_STR) { v.type = V_OBJ; v.which = node->kind == N_PARENTROOT; }
        return v;
    case N_FIELD: return read_member(e, 0, "$", node->text);
    case N_PARENT: return read_member(e, 1, "$", node->text);
    case N_ENV: return eval_environment(e, node->text);
    case N_STR: return str_value(e, node->text);
    case N_NUM: return num_value(node->num);
    case N_BOOL: return bool_value(node->num != 0);
    case N_NULL: v.type = V_NULL; return v;
    case N_UNDEF: return v;
    case N_ARR:
        v.type = V_ARR;
        if (node->n && !(v.items = eval_hold(e, calloc(node->n, sizeof(Value))))) return undefined_value();
        for (size_t i = 0; i < node->n && !e->failed; ++i) v.items[v.count++] = eval_node(e, node->kids[i]);
        return v;
    case N_NOT: a = eval_node(e, node->kids[0]); return bool_value(!truthy(e, a));
    case N_NEG: a = eval_node(e, node->kids[0]); return num_value(-to_number(e, a));
    case N_AND: a = eval_node(e, node->kids[0]); return truthy(e, a) ? eval_node(e, node->kids[1]) : a;
    case N_OR: a = eval_node(e, node->kids[0]); return truthy(e, a) ? a : eval_node(e, node->kids[1]);
    case N_NULLISH:
        a = eval_node(e, node->kids[0]);
        return a.type == V_UNDEF || a.type == V_NULL ? eval_node(e, node->kids[1]) : a;
    case N_COND: a = eval_node(e, node->kids[0]); return eval_node(e, node->kids[truthy(e, a) ? 1 : 2]);
    case N_BIN: return eval_binary(e, node);
    case N_CALL: return eval_call(e, node);
    case N_PYBOOL:
        e->python = 1;
        a = eval_node(e, node->kids[0]);
        return bool_value(truthy(e, a));
    case N_PRESENT:
        /* The bare form: a list is present only when it holds something. */
        a = eval_node(e, node->kids[0]);
        return bool_value(a.type == V_ARR ? a.count > 0 : truthy(e, a));
    case N_INDEX: {
        Value index;
        a = eval_node(e, node->kids[0]);
        index = e->failed ? v : eval_node(e, node->kids[1]);
        if (e->failed) return v;
        if (a.type != V_ARR || index.type != V_NUM || index.num != floor(index.num))
            return eval_fail(e, "only a list is indexed, and only by a whole number");
        if (index.num < 0) index.num += (double)a.count;
        if (index.num < 0 || index.num >= (double)a.count)
            return eval_fail(e, "the donor expression throws here: list index out of range");
        return a.items[(size_t)index.num];
    }
    case N_GET: case N_OPTGET:
        a = eval_node(e, node->kids[0]);
        if (e->failed) return v;
        /* A template counts a name it was never given as nothing. */
        if (e->python && a.type == V_UNDEF && !strcmp(node->text, "length")) return num_value(0);
        if (a.type == V_UNDEF || a.type == V_NULL) {
            if (node->kind == N_OPTGET) return v;
            return eval_fail(e, "the donor expression throws here: %s of an absent value", node->text);
        }
        if (a.type == V_OBJ) return read_member(e, a.which, a.str, node->text);
        if (!strcmp(node->text, "length")) {
            if (a.type == V_ARR) return num_value((double)a.count);
            if (a.type == V_STR) return num_value((double)utf16_length(a.str));
        }
        return v;
    default: return eval_fail(e, "unknown plan node");
    }
}

int bf_cond_evaluate(const BfCondPlan *plan, sqlite3 *db, const char *doc_json,
                     const char *parent_json, const char *env_json,
                     char *error, size_t error_cap) {
    Eval e;
    Value result;
    int out;
    if (error && error_cap) error[0] = '\0';
    if (!plan || !plan->compiled || !plan->root || !db || !doc_json) {
        if (error && error_cap) snprintf(error, error_cap, "no compiled plan or no document");
        return -1;
    }
    memset(&e, 0, sizeof(e));
    e.db = db;
    e.documents[0] = doc_json;
    e.documents[1] = parent_json;
    e.documents[2] = env_json;
    e.error = error;
    e.error_cap = error_cap;
    result = eval_node(&e, plan->root);
    out = e.failed ? -1 : truthy(&e, result);
    for (size_t i = 0; i < e.held_count; ++i) free(e.held[i]);
    free(e.held);
    return out;
}

/* ------------------------------------------------------------------ words */

/* A plan said in plain words for a reader, with each field named by the label
 * its DocType gives it. Only the forms that read naturally are phrased; the
 * rest return -1 and the caller says which fields the rule depends on. */

typedef struct {
    Buf buf;
    BfCondLabelFn label;
    void *ctx;
} Words;

static int words_term(Words *w, const BfCondNode *node);
static int words_bool(Words *w, const BfCondNode *node, int negated, int parent_kind);

static void words_name(Words *w, const char *fieldname, int *is_check) {
    const char *label = w->label ? w->label(w->ctx, fieldname, is_check) : NULL;
    if (label && label[0]) { buf_put(&w->buf, label); return; }
    for (const char *c = fieldname; *c; ++c) {
        char piece[2] = {*c == '_' ? ' ' : *c, '\0'};
        buf_put(&w->buf, piece);
    }
}

static int words_literal(Words *w, const BfCondNode *node) {
    char number[40];
    if (node->kind == N_STR) {
        buf_put(&w->buf, "\"");
        buf_put(&w->buf, node->text);
        buf_put(&w->buf, "\"");
        return 0;
    }
    if (node->kind == N_NUM) {
        format_number(node->num, number, sizeof(number));
        buf_put(&w->buf, number);
        return 0;
    }
    return -1;
}

static int words_term(Words *w, const BfCondNode *node) {
    switch (node->kind) {
    case N_FIELD: words_name(w, node->text, NULL); return 0;
    case N_PARENT:
        buf_put(&w->buf, "the parent record's ");
        words_name(w, node->text, NULL);
        return 0;
    case N_STR: case N_NUM: return words_literal(w, node);
    case N_CALL:
        if (!strcmp(node->text, "cint")) return words_term(w, node->kids[0]);
        return -1;
    case N_GET:
        if (strcmp(node->text, "length")) return -1;
        buf_put(&w->buf, "the number of ");
        return words_term(w, node->kids[0]);
    default: return -1;
    }
}

static int words_field_state(Words *w, const BfCondNode *node, int negated) {
    int is_check = 0;
    /* Every donor record carries these two; neither has a label. */
    if (node->kind == N_FIELD && !strcmp(node->text, "__islocal")) {
        buf_put(&w->buf, negated ? "the record has been saved" : "the record is new");
        return 0;
    }
    if (node->kind == N_FIELD && !strcmp(node->text, "docstatus")) {
        buf_put(&w->buf, negated ? "the record is a draft" : "the record is not a draft");
        return 0;
    }
    if (node->kind == N_PARENT) buf_put(&w->buf, "the parent record's ");
    words_name(w, node->text, &is_check);
    if (node->kind == N_PARENT) is_check = 0;
    buf_put(&w->buf, is_check ? (negated ? " is not ticked" : " is ticked")
                              : (negated ? " is not set" : " is set"));
    return 0;
}

static int words_membership(Words *w, const BfCondNode *list, const BfCondNode *item, int negated) {
    if (list->kind == N_ARR) {
        if (!list->n) return -1;
        for (size_t i = 0; i < list->n; ++i)
            if (list->kids[i]->kind != N_STR && list->kids[i]->kind != N_NUM) return -1;
        if (words_term(w, item) != 0) return -1;
        if (list->n == 1) buf_put(&w->buf, negated ? " is not " : " is ");
        else buf_put(&w->buf, negated ? " is not one of " : " is one of ");
        for (size_t i = 0; i < list->n; ++i) {
            if (i) buf_put(&w->buf, i + 1 == list->n ? " or " : ", ");
            words_literal(w, list->kids[i]);
        }
        return 0;
    }
    if (item->kind != N_STR || words_term(w, list) != 0) return -1;
    buf_put(&w->buf, negated ? " does not contain " : " contains ");
    return words_literal(w, item);
}

static int is_number(const BfCondNode *node, double value) {
    if (node->kind == N_NUM) return node->num == value;
    return node->kind == N_NEG && node->kids[0]->kind == N_NUM && -node->kids[0]->num == value;
}

static int is_index_of(const BfCondNode *node) {
    return node->kind == N_CALL && !strncmp(node->text, "indexof", 7);
}

static int words_compare(Words *w, const BfCondNode *node, int negated) {
    const BfCondNode *a = node->kids[0], *b = node->kids[1];
    const char *op = node->text;
    int equality = !strcmp(op, "eq") || !strcmp(op, "seq") || !strcmp(op, "ne") || !strcmp(op, "sne") ||
                   !strcmp(op, "pyeq") || !strcmp(op, "pyne");
    int positive = (!strcmp(op, "eq") || !strcmp(op, "seq") || !strcmp(op, "pyeq")) != negated;
    const char *phrase;

    if (is_index_of(b)) { const BfCondNode *t = a; a = b; b = t; if (!equality) return -1; }
    if (is_index_of(a)) {
        /* indexOf compared with -1 or 0 is a membership test in disguise. */
        int member;
        if (equality && is_number(b, -1)) member = !positive;
        else if ((!strcmp(op, "gt") && is_number(b, -1)) || (!strcmp(op, "ge") && is_number(b, 0))) member = !negated;
        else if (!strcmp(op, "lt") && is_number(b, 0)) member = negated;
        else return -1;
        return words_membership(w, a->kids[0], a->kids[1], !member);
    }
    if (equality) {
        if ((b->kind == N_FIELD || b->kind == N_PARENT) && a->kind != N_FIELD && a->kind != N_PARENT) {
            const BfCondNode *t = a; a = b; b = t;
        }
        if (a->kind == N_FIELD && !strcmp(a->text, "docstatus") && b->kind == N_NUM &&
            (b->num == 0 || b->num == 1 || b->num == 2)) {
            buf_put(&w->buf, positive ? "the record is " : "the record is not ");
            buf_put(&w->buf, b->num == 0 ? "a draft" : b->num == 1 ? "submitted" : "cancelled");
            return 0;
        }
        if (a->kind == N_FIELD || a->kind == N_PARENT) {
            int is_check = 0;
            int one = is_number(b, 1) || (b->kind == N_BOOL && b->num != 0) || (b->kind == N_STR && !strcmp(b->text, "1"));
            int zero = is_number(b, 0) || (b->kind == N_BOOL && b->num == 0) || (b->kind == N_STR && !strcmp(b->text, "0"));
            if (a->kind == N_FIELD && w->label) (void)w->label(w->ctx, a->text, &is_check);
            if (is_check && (one || zero)) return words_field_state(w, a, one ? !positive : positive);
            if (b->kind == N_NULL || b->kind == N_UNDEF || (b->kind == N_STR && !b->text[0])) {
                if (words_term(w, a) != 0) return -1;
                buf_put(&w->buf, positive ? " is empty" : " is not empty");
                return 0;
            }
        }
        if (words_term(w, a) != 0) return -1;
        buf_put(&w->buf, positive ? " is " : " is not ");
        return words_term(w, b);
    }
    if (!strcmp(op, "lt")) phrase = negated ? " is at least " : " is less than ";
    else if (!strcmp(op, "le")) phrase = negated ? " is more than " : " is at most ";
    else if (!strcmp(op, "gt")) phrase = negated ? " is at most " : " is more than ";
    else if (!strcmp(op, "ge")) phrase = negated ? " is less than " : " is at least ";
    else return -1;
    if (words_term(w, a) != 0) return -1;
    buf_put(&w->buf, phrase);
    return words_term(w, b);
}

static int words_bool(Words *w, const BfCondNode *node, int negated, int parent_kind) {
    switch (node->kind) {
    case N_FIELD: case N_PARENT: return words_field_state(w, node, negated);
    case N_PRESENT: return words_field_state(w, node->kids[0], negated);
    case N_PYBOOL: return words_bool(w, node->kids[0], negated, parent_kind);
    case N_NOT: return words_bool(w, node->kids[0], !negated, parent_kind);
    case N_AND: case N_OR: {
        /* not (a and b) is said as "not a or not b". */
        int kind = negated ? (node->kind == N_AND ? N_OR : N_AND) : node->kind;
        int wrap = parent_kind && parent_kind != kind;
        if (wrap) buf_put(&w->buf, "(");
        if (words_bool(w, node->kids[0], negated, kind) != 0) return -1;
        buf_put(&w->buf, kind == N_AND ? " and " : " or ");
        if (words_bool(w, node->kids[1], negated, kind) != 0) return -1;
        if (wrap) buf_put(&w->buf, ")");
        return 0;
    }
    case N_BIN: return words_compare(w, node, negated);
    case N_CALL:
        if (!strcmp(node->text, "boolean") || !strcmp(node->text, "cint"))
            return words_bool(w, node->kids[0], negated, parent_kind);
        if (!strcmp(node->text, "in_list") || !strcmp(node->text, "py_in") || !strncmp(node->text, "includes", 8))
            return words_membership(w, node->kids[0], node->kids[1], negated);
        return -1;
    case N_GET:
        if (strcmp(node->text, "length") || words_term(w, node->kids[0]) != 0) return -1;
        buf_put(&w->buf, negated ? " has no entries" : " has entries");
        return 0;
    default: return -1;
    }
}

int bf_cond_words(const BfCondPlan *plan, BfCondLabelFn label, void *ctx, char *out, size_t cap) {
    Words w;
    int rc;
    if (!out || !cap) return -1;
    out[0] = '\0';
    if (!plan || !plan->compiled || !plan->root || plan->constant[0] || plan->env_read_count) return -1;
    memset(&w, 0, sizeof(w));
    w.label = label;
    w.ctx = ctx;
    rc = words_bool(&w, plan->root, 0, 0);
    if (rc == 0 && !w.buf.failed && w.buf.data && w.buf.len < cap) memcpy(out, w.buf.data, w.buf.len + 1);
    else rc = -1;
    free(w.buf.data);
    return rc;
}

/* ------------------------------------------------------------------ value */

/* The value of an expression rather than its truth, for a template that
 * prints it or loops over it. json is the value as JSON text; text is what the
 * donor's template would print. Both are the caller's to free. Returns 1 or 0
 * for the value's truth, and -1 with a reason. */
int bf_cond_evaluate_value(const BfCondPlan *plan, sqlite3 *db, const char *doc_json,
                           const char *parent_json, const char *env_json,
                           char **json, char **text, char *error, size_t error_cap) {
    Eval e;
    Value result, printed;
    const BfCondNode *root;
    int out = -1;
    if (json) *json = NULL;
    if (text) *text = NULL;
    if (error && error_cap) error[0] = '\0';
    if (!plan || !plan->compiled || !plan->root || !db || !doc_json) {
        if (error && error_cap) snprintf(error, error_cap, "no compiled plan or no document");
        return -1;
    }
    memset(&e, 0, sizeof(e));
    e.db = db;
    e.documents[0] = doc_json;
    e.documents[1] = parent_json;
    e.documents[2] = env_json;
    e.error = error;
    e.error_cap = error_cap;
    root = plan->root;
    if (root->kind == N_PYBOOL) { e.python = 1; root = root->kids[0]; }
    result = eval_node(&e, root);
    if (!e.failed && text) {
        printed = to_string(&e, result);
        if (!e.failed && printed.type == V_STR) *text = strdup(printed.str);
    }
    if (!e.failed && json) {
        Buf buf = {0};
        sqlite3_stmt *stmt = NULL;
        if (result.type == V_ARR) {
            buf_put(&buf, "[");
            for (size_t i = 0; i < result.count && !e.failed; ++i) {
                const Value *item = &result.items[i];
                char number[40];
                if (i) buf_put(&buf, ",");
                if (item->type == V_OBJ || item->type == V_STR) {
                    const char *sql = item->type == V_OBJ ? "SELECT json_extract(?1, ?2)" : "SELECT json_quote(?2)";
                    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) { eval_fail(&e, "store cannot read JSON"); break; }
                    if (item->type == V_OBJ) sqlite3_bind_text(stmt, 1, e.documents[item->which], -1, SQLITE_STATIC);
                    sqlite3_bind_text(stmt, 2, item->str, -1, SQLITE_STATIC);
                    if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_text(stmt, 0))
                        buf_put(&buf, (const char *)sqlite3_column_text(stmt, 0));
                    else buf_put(&buf, "null");
                    sqlite3_finalize(stmt);
                } else if (item->type == V_NUM) {
                    format_number(item->num, number, sizeof(number));
                    buf_put(&buf, isnan(item->num) || isinf(item->num) ? "null" : number);
                } else if (item->type == V_BOOL) buf_put(&buf, item->num != 0 ? "true" : "false");
                else if (item->type == V_ARR) eval_fail(&e, "a list of lists is not carried");
                else buf_put(&buf, "null");
            }
            buf_put(&buf, "]");
            if (!e.failed && !buf.failed) *json = buf.data; else free(buf.data);
        } else if (result.type == V_NUM || result.type == V_BOOL) {
            char number[40];
            if (result.type == V_BOOL) snprintf(number, sizeof(number), "%s", result.num != 0 ? "true" : "false");
            else format_number(result.num, number, sizeof(number));
            *json = strdup(number);
        } else if (result.type == V_STR || result.type == V_OBJ) {
            const char *sql = result.type == V_OBJ ? "SELECT json_extract(?1, ?2)" : "SELECT json_quote(?2)";
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                if (result.type == V_OBJ) sqlite3_bind_text(stmt, 1, e.documents[result.which], -1, SQLITE_STATIC);
                sqlite3_bind_text(stmt, 2, result.str, -1, SQLITE_STATIC);
                if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_text(stmt, 0))
                    *json = strdup((const char *)sqlite3_column_text(stmt, 0));
                sqlite3_finalize(stmt);
            }
        } else *json = strdup("null");
    }
    out = e.failed ? -1 : truthy(&e, result);
    for (size_t i = 0; i < e.held_count; ++i) free(e.held[i]);
    free(e.held);
    if (out < 0) {
        if (json) { free(*json); *json = NULL; }
        if (text) { free(*text); *text = NULL; }
    }
    return out;
}
