/*
 * bf_re.c — JS-compatible backtracking regular expressions. See bf_re.h.
 *
 * Pattern -> AST -> program for a backtracking VM (Cox, "Regular Expression
 * Matching: the Virtual Machine Approach", backtracking form). Counted
 * repetition is expanded into copies; loops that could match empty carry a
 * progress check so they cannot spin.
 */
#include "bf_re.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── classes ──────────────────────────────────────────────────────────── */

typedef struct { unsigned char bits[16]; int nonascii; } Class;   /* 128 ASCII bits */

static void cls_set(Class *c, int b) { if (b >= 0 && b < 128) c->bits[b >> 3] |= (unsigned char)(1 << (b & 7)); }
static int  cls_has(const Class *c, int b) { return b < 128 ? (c->bits[b >> 3] >> (b & 7)) & 1 : c->nonascii; }
static void cls_range(Class *c, int a, int z) { for (int b = a; b <= z && b < 128; b++) cls_set(c, b); if (z >= 128) c->nonascii = 1; }
static void cls_negate(Class *c) { for (int i = 0; i < 16; i++) c->bits[i] = (unsigned char)~c->bits[i]; c->nonascii = !c->nonascii; }
static void cls_union(Class *c, const Class *d) { for (int i = 0; i < 16; i++) c->bits[i] |= d->bits[i]; c->nonascii |= d->nonascii; }
static void cls_fold(Class *c) {
    for (int b = 'a'; b <= 'z'; b++) if (cls_has(c, b) || cls_has(c, b - 32)) { cls_set(c, b); cls_set(c, b - 32); }
}
static int is_word(int b) { return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'; }
static void cls_escape(Class *c, char e) {
    Class t; memset(&t, 0, sizeof t);
    switch (e) {
    case 'd': case 'D': cls_range(&t, '0', '9'); break;
    case 'w': case 'W': cls_range(&t, 'a', 'z'); cls_range(&t, 'A', 'Z'); cls_range(&t, '0', '9'); cls_set(&t, '_'); break;
    case 's': case 'S': cls_set(&t, ' '); cls_set(&t, '\t'); cls_set(&t, '\n'); cls_set(&t, '\r'); cls_set(&t, '\v'); cls_set(&t, '\f'); break;
    }
    if (e == 'D' || e == 'W' || e == 'S') cls_negate(&t);
    cls_union(c, &t);
}

/* ── AST ──────────────────────────────────────────────────────────────── */

enum { N_EMPTY, N_CHAR, N_CLASS, N_ANY, N_BOL, N_EOL, N_WORDB, N_NWORDB, N_GROUP, N_CAT, N_ALT, N_REP, N_LOOK, N_BREF };
typedef struct Node { int type, a, b, greedy, neg; int cls; struct Node *l, *r; } Node;

typedef struct {
    const char *p; int icase, multiline, dotall, groups;
    Class *classes; int nclasses, capclasses;
    Node *pool; int npool, cappool;
    const char *err;
} Parser;

static Node *mk(Parser *ps, int type) {
    if (ps->npool == ps->cappool) { ps->err = "pattern too large"; return NULL; }
    Node *n = &ps->pool[ps->npool++]; memset(n, 0, sizeof *n); n->type = type; return n;
}
static int add_class(Parser *ps, Class c) {
    if (ps->nclasses == ps->capclasses) {
        ps->capclasses = ps->capclasses ? ps->capclasses * 2 : 8;
        ps->classes = realloc(ps->classes, sizeof *ps->classes * (size_t)ps->capclasses);
    }
    if (ps->icase) cls_fold(&c);
    ps->classes[ps->nclasses] = c;
    return ps->nclasses++;
}
static int hexv(char c) { return c >= '0' && c <= '9' ? c - '0' : c >= 'a' && c <= 'f' ? c - 'a' + 10 : c >= 'A' && c <= 'F' ? c - 'A' + 10 : -1; }

static Node *parse_alt(Parser *ps);
static int utf8_seq(unsigned char c) { return (c >> 5) == 6 ? 2 : (c >> 4) == 14 ? 3 : (c >> 3) == 30 ? 4 : 1; }

/* A single escaped character value, or -1 when the escape is a class. */
static int escape_char(Parser *ps, char e) {
    switch (e) {
    case 'n': return '\n'; case 'r': return '\r'; case 't': return '\t';
    case 'f': return '\f'; case 'v': return '\v'; case '0': return 0;
    case 'x': if (hexv(ps->p[0]) >= 0 && hexv(ps->p[1]) >= 0) { int v = hexv(ps->p[0]) * 16 + hexv(ps->p[1]); ps->p += 2; return v; } return 'x';
    case 'u': if (hexv(ps->p[0]) >= 0 && hexv(ps->p[1]) >= 0 && hexv(ps->p[2]) >= 0 && hexv(ps->p[3]) >= 0) {
        int v = 0; for (int i = 0; i < 4; i++) v = v * 16 + hexv(ps->p[i]); ps->p += 4; return v; } return 'u';
    case 'd': case 'D': case 'w': case 'W': case 's': case 'S': return -1;
    default: return (unsigned char)e;
    }
}

static Node *char_node(Parser *ps, int c) {
    if (c >= 128) {   /* a non-ASCII escape: match its UTF-8 bytes in sequence */
        unsigned char b[4]; int n = 0;
        if (c < 0x800) { b[0] = (unsigned char)(0xC0 | c >> 6); b[1] = (unsigned char)(0x80 | (c & 63)); n = 2; }
        else { b[0] = (unsigned char)(0xE0 | c >> 12); b[1] = (unsigned char)(0x80 | (c >> 6 & 63)); b[2] = (unsigned char)(0x80 | (c & 63)); n = 3; }
        Node *cat = mk(ps, N_CAT), *prev = NULL;
        if (!cat) return NULL;
        cat->a = 1;
        for (int i = 0; i < n; i++) {
            Node *ch = mk(ps, N_CHAR), *link = mk(ps, N_CAT);
            if (!ch || !link) return NULL;
            ch->a = b[i]; link->l = ch;
            if (!prev) cat->l = link; else prev->r = link;
            prev = link;
        }
        return cat;
    }
    if (ps->icase && ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))) {
        Class k; memset(&k, 0, sizeof k); cls_set(&k, c);
        Node *n = mk(ps, N_CLASS); if (n) n->cls = add_class(ps, k); return n;
    }
    Node *n = mk(ps, N_CHAR); if (n) n->a = c; return n;
}

static Node *parse_class(Parser *ps) {
    Class c; memset(&c, 0, sizeof c);
    int neg = 0;
    if (*ps->p == '^') { neg = 1; ps->p++; }
    while (*ps->p && *ps->p != ']') {
        int lo;
        if (*ps->p == '\\') {
            char e = ps->p[1]; ps->p += 2;
            if (e == 'b') lo = '\b';
            else if ((lo = escape_char(ps, e)) < 0) { cls_escape(&c, e); continue; }
        } else {
            lo = (unsigned char)*ps->p++;
            if (lo >= 0x80) { c.nonascii = 1; while ((*ps->p & 0xC0) == 0x80) ps->p++; continue; }
        }
        if (ps->p[0] == '-' && ps->p[1] && ps->p[1] != ']') {
            int hi; ps->p++;
            if (*ps->p == '\\') { char e = ps->p[1]; ps->p += 2; hi = escape_char(ps, e); if (hi < 0) { cls_set(&c, lo); cls_set(&c, '-'); cls_escape(&c, e); continue; } }
            else hi = (unsigned char)*ps->p++;
            if (hi < lo) { ps->err = "range out of order in character class"; return NULL; }
            cls_range(&c, lo, hi);
        } else if (lo >= 128) c.nonascii = 1;
        else cls_set(&c, lo);
    }
    if (*ps->p != ']') { ps->err = "unterminated character class"; return NULL; }
    ps->p++;
    if (neg) { if (ps->icase) cls_fold(&c); cls_negate(&c); }
    Node *n = mk(ps, N_CLASS);
    if (n) { if (neg) { int save = ps->icase; ps->icase = 0; n->cls = add_class(ps, c); ps->icase = save; } else n->cls = add_class(ps, c); }
    return n;
}

static Node *parse_atom(Parser *ps) {
    char c = *ps->p;
    if (c == '(') {
        ps->p++;
        int cap = 1, look = 0, neg = 0;
        if (ps->p[0] == '?') {
            if (ps->p[1] == ':') cap = 0;
            else if (ps->p[1] == '=') { cap = 0; look = 1; }
            else if (ps->p[1] == '!') { cap = 0; look = 1; neg = 1; }
            else { ps->err = "unsupported group (lookbehind or named group)"; return NULL; }
            ps->p += 2;
        }
        int idx = cap ? ++ps->groups : 0;
        if (idx >= BF_RE_MAX_GROUPS) { ps->err = "too many capture groups"; return NULL; }
        Node *inner = parse_alt(ps);
        if (!inner) return NULL;
        if (*ps->p != ')') { ps->err = "missing )"; return NULL; }
        ps->p++;
        if (look) { Node *n = mk(ps, N_LOOK); if (n) { n->l = inner; n->neg = neg; } return n; }
        if (!cap) return inner;
        Node *n = mk(ps, N_GROUP); if (n) { n->a = idx; n->l = inner; } return n;
    }
    if (c == '[') { ps->p++; return parse_class(ps); }
    if (c == '.') { ps->p++; return mk(ps, N_ANY); }
    if (c == '^') { ps->p++; return mk(ps, N_BOL); }
    if (c == '$') { ps->p++; return mk(ps, N_EOL); }
    if (c == '\\') {
        char e = ps->p[1];
        if (!e) { ps->err = "\\ at end of pattern"; return NULL; }
        ps->p += 2;
        if (e == 'b') return mk(ps, N_WORDB);
        if (e == 'B') return mk(ps, N_NWORDB);
        if (e >= '1' && e <= '9') { Node *n = mk(ps, N_BREF); if (n) n->a = e - '0'; return n; }
        int v = escape_char(ps, e);
        if (v < 0) {
            Class k; memset(&k, 0, sizeof k); cls_escape(&k, e);
            Node *n = mk(ps, N_CLASS); if (n) { int save = ps->icase; ps->icase = 0; n->cls = add_class(ps, k); ps->icase = save; } return n;
        }
        return char_node(ps, v);
    }
    if ((unsigned char)c >= 0x80) {   /* a literal multibyte character is one atom */
        int n = utf8_seq((unsigned char)c);
        Node *cat = mk(ps, N_CAT), *prev = NULL;
        if (!cat) return NULL;
        cat->a = 1;
        for (int i = 0; i < n && *ps->p; i++) {
            Node *ch = mk(ps, N_CHAR), *link = mk(ps, N_CAT);
            if (!ch || !link) return NULL;
            ch->a = (unsigned char)*ps->p++; link->l = ch;
            if (!prev) cat->l = link; else prev->r = link;
            prev = link;
        }
        return cat;
    }
    ps->p++;
    return char_node(ps, (unsigned char)c);
}

static int parse_count(const char **pp, int *min, int *max) {
    const char *p = *pp;   /* p at '{' */
    if (p[1] < '0' || p[1] > '9') return 0;
    p++; int a = 0; while (*p >= '0' && *p <= '9') a = a * 10 + (*p++ - '0');
    int b = a;
    if (*p == ',') { p++; if (*p == '}') b = -1; else { b = 0; if (*p < '0' || *p > '9') return 0; while (*p >= '0' && *p <= '9') b = b * 10 + (*p++ - '0'); } }
    if (*p != '}') return 0;
    *min = a; *max = b; *pp = p + 1;
    return 1;
}

static Node *parse_piece(Parser *ps) {
    Node *atom = parse_atom(ps);
    if (!atom) return NULL;
    for (;;) {
        int min, max; char q = *ps->p;
        if (q == '*') { min = 0; max = -1; ps->p++; }
        else if (q == '+') { min = 1; max = -1; ps->p++; }
        else if (q == '?') { min = 0; max = 1; ps->p++; }
        else if (q == '{' && parse_count(&ps->p, &min, &max)) {}
        else return atom;
        if (max >= 0 && max < min) { ps->err = "numbers out of order in {} quantifier"; return NULL; }
        if (min > 1000 || max > 1000) { ps->err = "repetition count too large"; return NULL; }
        Node *rep = mk(ps, N_REP);
        if (!rep) return NULL;
        rep->l = atom; rep->a = min; rep->b = max; rep->greedy = 1;
        if (*ps->p == '?') { rep->greedy = 0; ps->p++; }
        atom = rep;
    }
}

static Node *parse_cat(Parser *ps) {
    Node *cat = mk(ps, N_CAT), *prev = NULL;
    if (!cat) return NULL;
    while (*ps->p && *ps->p != '|' && *ps->p != ')') {
        Node *piece = parse_piece(ps);
        if (!piece) return NULL;
        Node *link = mk(ps, N_CAT); if (!link) return NULL;   /* list cell */
        link->l = piece;
        if (!prev) cat->l = link; else prev->r = link;
        prev = link;
    }
    cat->a = 1;   /* marks a list head whose cells are N_CAT links */
    return cat;
}

static Node *parse_alt(Parser *ps) {
    Node *left = parse_cat(ps);
    if (!left) return NULL;
    while (*ps->p == '|') {
        ps->p++;
        Node *right = parse_cat(ps);
        if (!right) return NULL;
        Node *alt = mk(ps, N_ALT); if (!alt) return NULL;
        alt->l = left; alt->r = right; left = alt;
    }
    return left;
}

/* ── program ──────────────────────────────────────────────────────────── */

enum { I_CHAR, I_ANY, I_ANYNL, I_CLASS, I_BOL, I_EOL, I_WORDB, I_NWORDB, I_SPLIT, I_JMP, I_SAVE, I_MARK, I_PROG, I_LOOK, I_BREF, I_MATCH };
typedef struct { int op, x, y, z; } Inst;

struct bf_re {
    Inst *code; int ncode, capcode;
    Class *classes; int nclasses;
    int groups, nregs, global, multiline, icase;
};

static int emit(bf_re *re, int op, int x, int y) {
    if (re->ncode == re->capcode) { re->capcode = re->capcode ? re->capcode * 2 : 64; re->code = realloc(re->code, sizeof *re->code * (size_t)re->capcode); }
    re->code[re->ncode] = (Inst){op, x, y, 0};
    return re->ncode++;
}

/* Can this node match the empty string? (Conservative: yes when unsure.) */
static int nullable(const Node *n) {
    if (!n) return 1;
    switch (n->type) {
    case N_CHAR: case N_CLASS: case N_ANY: return 0;
    case N_GROUP: return nullable(n->l);
    case N_CAT:
        if (n->a) { for (const Node *c = n->l; c; c = c->r) if (!nullable(c->l)) return 0; return 1; }
        for (const Node *c = n; c; c = c->r) if (c->l && !nullable(c->l)) return 0;
        return 1;
    case N_ALT: return nullable(n->l) || nullable(n->r);
    case N_REP: return n->a == 0 || nullable(n->l);
    default: return 1;
    }
}

static void gen(bf_re *re, const Node *n, int dotall);

static void gen_star(bf_re *re, const Node *body, int greedy, int dotall) {
    int empty = nullable(body), reg = empty ? re->nregs++ : -1;
    int loop = emit(re, I_SPLIT, 0, 0);
    int start = re->ncode;
    if (empty) emit(re, I_MARK, reg, 0);
    gen(re, body, dotall);
    if (empty) emit(re, I_PROG, reg, 0);
    emit(re, I_JMP, loop, 0);
    int after = re->ncode;
    re->code[loop].x = greedy ? start : after;
    re->code[loop].y = greedy ? after : start;
}

static void gen(bf_re *re, const Node *n, int dotall) {
    if (!n) return;
    switch (n->type) {
    case N_EMPTY: break;
    case N_CHAR: emit(re, I_CHAR, n->a, 0); break;
    case N_CLASS: emit(re, I_CLASS, n->cls, 0); break;
    case N_ANY: emit(re, dotall ? I_ANY : I_ANYNL, 0, 0); break;
    case N_BOL: emit(re, I_BOL, 0, 0); break;
    case N_EOL: emit(re, I_EOL, 0, 0); break;
    case N_WORDB: emit(re, I_WORDB, 0, 0); break;
    case N_NWORDB: emit(re, I_NWORDB, 0, 0); break;
    case N_BREF: emit(re, I_BREF, n->a, 0); break;
    case N_GROUP: emit(re, I_SAVE, 2 * n->a, 0); gen(re, n->l, dotall); emit(re, I_SAVE, 2 * n->a + 1, 0); break;
    case N_CAT:
        if (n->a) { for (const Node *c = n->l; c; c = c->r) gen(re, c->l, dotall); }
        else gen(re, n->l, dotall);
        break;
    case N_ALT: {
        int split = emit(re, I_SPLIT, 0, 0);
        re->code[split].x = re->ncode; gen(re, n->l, dotall);
        int jmp = emit(re, I_JMP, 0, 0);
        re->code[split].y = re->ncode; gen(re, n->r, dotall);
        re->code[jmp].x = re->ncode;
        break;
    }
    case N_REP: {
        for (int i = 0; i < n->a; i++) gen(re, n->l, dotall);
        if (n->b < 0) { gen_star(re, n->l, n->greedy, dotall); break; }
        int pending[1001], np = 0;
        for (int i = n->a; i < n->b; i++) {
            int split = emit(re, I_SPLIT, 0, 0);
            pending[np++] = split;
            int start = re->ncode;
            re->code[split].x = n->greedy ? start : -1;
            re->code[split].y = n->greedy ? -1 : start;
            gen(re, n->l, dotall);
        }
        for (int i = 0; i < np; i++) {   /* every optional copy may stop here */
            Inst *s = &re->code[pending[i]];
            if (s->x < 0) s->x = re->ncode; else s->y = re->ncode;
        }
        break;
    }
    case N_LOOK: {
        int look = emit(re, I_LOOK, 0, n->neg);
        re->code[look].x = re->ncode;
        gen(re, n->l, dotall);
        emit(re, I_MATCH, 0, 0);
        re->code[look].z = re->ncode;   /* continue after the sub-program */
        break;
    }
    }
}

bf_re *bf_re_compile(const char *pattern, const char *flags, char *err, size_t err_sz) {
    Parser ps; memset(&ps, 0, sizeof ps);
    ps.p = pattern;
    int global = 0;
    for (const char *f = flags ? flags : ""; *f; f++) {
        if (*f == 'i') ps.icase = 1; else if (*f == 'g') global = 1;
        else if (*f == 'm') ps.multiline = 1; else if (*f == 's') ps.dotall = 1;
        else { if (err) snprintf(err, err_sz, "unsupported flag %c", *f); return NULL; }
    }
    ps.cappool = 64 + (int)strlen(pattern) * 4;
    ps.pool = calloc((size_t)ps.cappool, sizeof *ps.pool);
    Node *root = parse_alt(&ps);
    if (root && *ps.p) ps.err = *ps.p == ')' ? "unmatched )" : "unexpected character";
    if (!root || ps.err) {
        if (err) snprintf(err, err_sz, "%s", ps.err ? ps.err : "invalid pattern");
        free(ps.pool); free(ps.classes); return NULL;
    }
    bf_re *re = calloc(1, sizeof *re);
    re->groups = ps.groups + 1; re->global = global; re->multiline = ps.multiline; re->icase = ps.icase;
    emit(re, I_SAVE, 0, 0);
    gen(re, root, ps.dotall);
    emit(re, I_SAVE, 1, 0);
    emit(re, I_MATCH, 0, 0);
    re->classes = ps.classes; re->nclasses = ps.nclasses;
    free(ps.pool);
    return re;
}

void bf_re_free(bf_re *re) { if (!re) return; free(re->code); free(re->classes); free(re); }
int bf_re_global(const bf_re *re) { return re && re->global; }

/* ── matching ─────────────────────────────────────────────────────────── */

typedef struct { int kind, a; long b; } Frame;   /* 0 branch(pc a, sp b); 1 cap a=old b; 2 reg a=old b */
typedef struct { Frame *f; int n, cap; Frame *local; } Stack;   /* starts on the C stack */

static void push(Stack *s, int kind, int a, long b) {
    if (s->n == s->cap) {
        int cap = s->cap * 2;
        Frame *f = malloc(sizeof *f * (size_t)cap);
        memcpy(f, s->f, sizeof *f * (size_t)s->n);
        if (s->f != s->local) free(s->f);
        s->f = f; s->cap = cap;
    }
    s->f[s->n++] = (Frame){kind, a, b};
}

static int utf8_len(unsigned char c) { return c < 0x80 ? 1 : (c >> 5) == 6 ? 2 : (c >> 4) == 14 ? 3 : (c >> 3) == 30 ? 4 : 1; }

static int run(const bf_re *re, int pc, long sp, const char *s, long len, long *caps, long *regs, long *steps) {
    Frame first[128];
    Stack st = {first, 0, 128, first};
    int ncap = 2 * re->groups, ok = 0;
    for (;;) {
        if (++*steps > 20000000) break;   /* runaway backtracking: no match */
        const Inst *in = &re->code[pc];
        int fail = 0;
        switch (in->op) {
        case I_CHAR: if (sp < len && (unsigned char)s[sp] == in->x) { sp++; pc++; } else fail = 1; break;
        case I_ANY: if (sp < len) { sp += utf8_len((unsigned char)s[sp]); if (sp > len) sp = len; pc++; } else fail = 1; break;
        case I_ANYNL: if (sp < len && s[sp] != '\n' && s[sp] != '\r') { sp += utf8_len((unsigned char)s[sp]); if (sp > len) sp = len; pc++; } else fail = 1; break;
        case I_CLASS:
            if (sp < len && cls_has(&re->classes[in->x], (unsigned char)s[sp])) {
                sp += (unsigned char)s[sp] < 0x80 ? 1 : utf8_len((unsigned char)s[sp]); if (sp > len) sp = len; pc++;
            } else fail = 1;
            break;
        case I_BOL: if (sp == 0 || (re->multiline && (s[sp - 1] == '\n' || s[sp - 1] == '\r'))) pc++; else fail = 1; break;
        case I_EOL: if (sp == len || (re->multiline && (s[sp] == '\n' || s[sp] == '\r'))) pc++; else fail = 1; break;
        case I_WORDB: case I_NWORDB: {
            int a = sp > 0 && is_word((unsigned char)s[sp - 1]), b = sp < len && is_word((unsigned char)s[sp]);
            if ((a != b) == (in->op == I_WORDB)) pc++; else fail = 1;
            break;
        }
        case I_SPLIT: push(&st, 0, in->y, sp); pc = in->x; break;
        case I_JMP: pc = in->x; break;
        case I_SAVE: if (in->x < ncap) { push(&st, 1, in->x, caps[in->x]); caps[in->x] = sp; } pc++; break;
        case I_MARK: push(&st, 2, in->x, regs[in->x]); regs[in->x] = sp; pc++; break;
        case I_PROG: if (sp != regs[in->x]) pc++; else fail = 1; break;
        case I_BREF: {
            long a = caps[2 * in->x], b = caps[2 * in->x + 1];
            if (a < 0 || b < 0) { pc++; break; }           /* an unset group matches empty */
            long n = b - a;
            int same = sp + n <= len;
            for (long k = 0; same && k < n; k++) {
                int x = (unsigned char)s[a + k], y = (unsigned char)s[sp + k];
                if (re->icase) { if (x >= 'A' && x <= 'Z') x += 32; if (y >= 'A' && y <= 'Z') y += 32; }
                same = x == y;
            }
            if (same) { sp += n; pc++; } else fail = 1;
            break;
        }
        case I_LOOK: {
            long *c2 = malloc(sizeof *c2 * (size_t)ncap), *r2 = malloc(sizeof *r2 * (size_t)(re->nregs + 1));
            memcpy(c2, caps, sizeof *c2 * (size_t)ncap); memcpy(r2, regs, sizeof *r2 * (size_t)(re->nregs + 1));
            int hit = run(re, in->x, sp, s, len, c2, r2, steps);
            if (hit && !in->y) for (int i = 2; i < ncap; i++) if (c2[i] != caps[i]) { push(&st, 1, i, caps[i]); caps[i] = c2[i]; }
            free(c2); free(r2);
            if (hit != in->y) pc = in->z; else fail = 1;
            break;
        }
        case I_MATCH: ok = 1; goto done;
        }
        if (!fail) continue;
        for (;;) {   /* backtrack */
            if (!st.n) goto done;
            Frame f = st.f[--st.n];
            if (f.kind == 1) caps[f.a] = f.b;
            else if (f.kind == 2) regs[f.a] = f.b;
            else { pc = f.a; sp = f.b; break; }
        }
    }
done:
    if (st.f != st.local) free(st.f);
    return ok;
}

int bf_re_exec(const bf_re *re, const char *s, size_t len, size_t from, bf_re_match *m) {
    long caps[2 * BF_RE_MAX_GROUPS], regs[64 + 1] = {0}, steps = 0;
    long *rp = re->nregs > 64 ? calloc((size_t)re->nregs + 1, sizeof *rp) : regs;
    int found = 0;
    for (long at = (long)from; at <= (long)len; at++) {
        if (at > (long)from && at < (long)len && ((unsigned char)s[at] & 0xC0) == 0x80) continue;   /* code point starts only */
        for (int i = 0; i < 2 * re->groups; i++) caps[i] = -1;
        if (run(re, 0, at, s, (long)len, caps, rp, &steps)) { found = 1; break; }
        if (steps > 20000000) break;
    }
    if (rp != regs) free(rp);
    if (!found) return 0;
    if (m) {
        m->groups = re->groups;
        for (int i = 0; i < BF_RE_MAX_GROUPS; i++) {
            m->start[i] = i < re->groups ? caps[2 * i] : -1;
            m->end[i] = i < re->groups ? caps[2 * i + 1] : -1;
            if (m->start[i] < 0 || m->end[i] < 0) m->start[i] = m->end[i] = -1;
        }
    }
    return 1;
}

int bf_re_test(const bf_re *re, const char *s) { return bf_re_exec(re, s, strlen(s), 0, NULL); }

void bf_re_append(bf_re_buf *b, const char *s, size_t n) {
    if (b->len + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap : 64; while (cap < b->len + n + 1) cap *= 2;
        b->buf = realloc(b->buf, cap); b->cap = cap;
    }
    memcpy(b->buf + b->len, s, n); b->len += n; b->buf[b->len] = 0;
}

typedef struct { const char *tmpl; } Tmpl;
static void template_fn(bf_re_buf *out, const char *s, const bf_re_match *m, void *ctx) {
    const char *t = ((Tmpl *)ctx)->tmpl;
    for (; *t; t++) {
        if (*t != '$' || !t[1]) { bf_re_append(out, t, 1); continue; }
        char c = t[1];
        if (c == '$') { bf_re_append(out, "$", 1); t++; }
        else if (c == '&') { bf_re_append(out, s + m->start[0], (size_t)(m->end[0] - m->start[0])); t++; }
        else if (c == '`') { bf_re_append(out, s, (size_t)m->start[0]); t++; }
        else if (c == '\'') { bf_re_append(out, s + m->end[0], strlen(s + m->end[0])); t++; }
        else if (c >= '1' && c <= '9' && c - '0' < m->groups) {
            int g = c - '0';
            if (m->start[g] >= 0) bf_re_append(out, s + m->start[g], (size_t)(m->end[g] - m->start[g]));
            t++;
        } else bf_re_append(out, t, 1);
    }
}

char *bf_re_replace_fn(const bf_re *re, const char *s, bf_re_fn fn, void *ctx) {
    bf_re_buf out = {0};
    size_t len = strlen(s), at = 0, copied = 0;
    bf_re_match m;
    while (at <= len && bf_re_exec(re, s, len, at, &m)) {
        bf_re_append(&out, s + copied, (size_t)m.start[0] - copied);
        fn(&out, s, &m, ctx);
        copied = (size_t)m.end[0];
        if (!re->global) break;
        at = m.end[0] == m.start[0] ? (size_t)m.end[0] + (m.end[0] < (long)len ? utf8_len((unsigned char)s[m.end[0]]) : 1) : (size_t)m.end[0];
    }
    if (copied < len) bf_re_append(&out, s + copied, len - copied);
    if (!out.buf) bf_re_append(&out, "", 0);
    return out.buf;
}

char *bf_re_replace(const bf_re *re, const char *s, const char *tmpl) {
    Tmpl t = {tmpl};
    return bf_re_replace_fn(re, s, template_fn, &t);
}

int bf_re_match_once(const char *pattern, const char *flags, const char *s) {
    bf_re *re = bf_re_compile(pattern, flags, NULL, 0);
    if (!re) return -1;
    int r = bf_re_test(re, s);
    bf_re_free(re);
    return r;
}

char *bf_re_group(const char *s, const bf_re_match *m, int g) {
    if (g >= m->groups || m->start[g] < 0) return strdup("");
    size_t n = (size_t)(m->end[g] - m->start[g]);
    char *o = malloc(n + 1); memcpy(o, s + m->start[g], n); o[n] = 0;
    return o;
}
