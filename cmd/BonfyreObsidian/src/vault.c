/* vault.c: everything bonfyre-obsidian does with a vault's files directly.
 *
 * Nothing here talks to Obsidian. These are the readers and writers used when
 * the desktop app is not reachable (a server, the app closed, a headless sync
 * binding) and the shape every native answer has.
 *
 * Paths: a note path is relative to the vault root, uses '/', and has no
 * empty, '.', '..' or dot-led component; the vault's config folder (.obsidian
 * by default) is refused whatever its case, since APFS folds case. Reads
 * resolve the real path and require it inside the vault's real root. Writes
 * walk every parent with lstat and refuse a symlink anywhere, then open the
 * final name without following one.
 */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#else
#define _GNU_SOURCE
#endif
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include "obsidian.h"
#include "bonfyre.h"

#define MAX_NOTE (16u << 20)

/* ------------------------------------------------------------ names and paths */
int ob_valid_name(const char *s, size_t max) {
    size_t n = s ? strlen(s) : 0;
    if (!n || n > max || s[0] == '.' || s[0] == '-') return 0;
    return strspn(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-") == n;
}

int ob_rel_ok(const char *rel, const char *config_dir, char *why, size_t cap) {
    size_t n = rel ? strlen(rel) : 0;
    if (!n) { snprintf(why, cap, "a note path is required"); return 0; }
    if (n > 1024) { snprintf(why, cap, "the note path is longer than 1024 bytes"); return 0; }
    if (rel[0] == '/') { snprintf(why, cap, "a note path is relative to the vault, not absolute"); return 0; }
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)rel[i];
        if (ch < 0x20 || ch == 0x7f || ch == '\\') { snprintf(why, cap, "the note path holds a control character or backslash"); return 0; }
    }
    const char *p = rel; int first = 1;
    while (*p) {
        const char *e = strchr(p, '/'); size_t len = e ? (size_t)(e - p) : strlen(p);
        if (!len) { snprintf(why, cap, "the note path has an empty component"); return 0; }
        if (p[0] == '.') {
            if ((len == 1) || (len == 2 && p[1] == '.')) { snprintf(why, cap, "the note path leaves the vault"); return 0; }
            if (first && config_dir && strlen(config_dir) == len && !strncasecmp(p, config_dir, len)) {
                snprintf(why, cap, "the vault's %s folder is Obsidian's own settings and is never written or read through this organ", config_dir); return 0;
            }
            snprintf(why, cap, "a dot-led folder or file is hidden from Obsidian and is not reachable here"); return 0;
        }
        if (!e) break;
        if (!e[1]) { snprintf(why, cap, "the note path ends in a slash"); return 0; }
        p = e + 1; first = 0;
    }
    return 1;
}

int ob_inside(const char *root, const char *path) {
    size_t n = strlen(root);
    if (n > 1 && root[n - 1] == '/') n--;
    return !strncmp(path, root, n) && (path[n] == '/' || path[n] == 0);
}

int ob_resolve_read(const char *root, const char *rel, const char *config_dir, char out[OB_PATH], char *why, size_t cap) {
    char joined[OB_PATH]; struct stat st;
    if (!ob_rel_ok(rel, config_dir, why, cap)) return EX_REFUSED;
    if (snprintf(joined, sizeof(joined), "%s/%s", root, rel) >= (int)sizeof(joined)) { snprintf(why, cap, "path too long"); return EX_REFUSED; }
    if (!realpath(joined, out)) { snprintf(why, cap, "no note at %s", rel); return EX_NOTFOUND; }
    if (!ob_inside(root, out)) { snprintf(why, cap, "%s resolves outside the vault through a link", rel); return EX_REFUSED; }
    if (stat(out, &st) || !S_ISREG(st.st_mode)) { snprintf(why, cap, "%s is not a file", rel); return EX_NOTFOUND; }
    return EX_OK;
}

int ob_resolve_write(const char *root, const char *rel, const char *config_dir, int mkdirs, char out[OB_PATH], char *why, size_t cap) {
    struct stat st; size_t rl = strlen(root);
    if (!ob_rel_ok(rel, config_dir, why, cap)) return EX_REFUSED;
    if (snprintf(out, OB_PATH, "%s/%s", root, rel) >= OB_PATH) { snprintf(why, cap, "path too long"); return EX_REFUSED; }
    for (char *s = out + rl + 1; (s = strchr(s, '/')); s++) {
        *s = 0;
        if (lstat(out, &st)) {
            if (errno != ENOENT || !mkdirs || mkdir(out, 0755)) { snprintf(why, cap, "folder %s does not exist", out + rl + 1); *s = '/'; return EX_NOTFOUND; }
        } else if (S_ISLNK(st.st_mode)) { snprintf(why, cap, "folder %s is a symbolic link; writes do not follow links", out + rl + 1); *s = '/'; return EX_REFUSED; }
        else if (!S_ISDIR(st.st_mode)) { snprintf(why, cap, "%s is not a folder", out + rl + 1); *s = '/'; return EX_REFUSED; }
        *s = '/';
    }
    if (!lstat(out, &st) && !S_ISREG(st.st_mode)) { snprintf(why, cap, "%s exists and is not a plain file", rel); return EX_REFUSED; }
    return EX_OK;
}

char *ob_read_file(const char *path, size_t *len, size_t max) {
    int fd = open(path, O_RDONLY | O_NOFOLLOW); struct stat st; char *buf; size_t got = 0;
    if (fd < 0) return NULL;
    if (fstat(fd, &st) || !S_ISREG(st.st_mode) || (size_t)st.st_size > max) { close(fd); errno = EFBIG; return NULL; }
    if (!(buf = malloc((size_t)st.st_size + 1))) { close(fd); return NULL; }
    while (got < (size_t)st.st_size) {
        ssize_t r = read(fd, buf + got, (size_t)st.st_size - got);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) break;
        got += (size_t)r;
    }
    close(fd); buf[got] = 0; if (len) *len = got;
    return buf;
}

/* Write beside the target, fsync, then rename (or link, when the name must not
 * exist yet, so two creators cannot both win). A reader never sees half a note. */
int ob_atomic_write(const char *path, const char *data, size_t len, int exclusive) {
    char tmp[OB_PATH], dir[OB_PATH]; const char *slash = strrchr(path, '/'); int fd, rc = -1; struct stat st; mode_t mode = 0644;
    if (!slash) return -1;
    snprintf(dir, sizeof(dir), "%.*s", (int)(slash - path), path);
    if (snprintf(tmp, sizeof(tmp), "%s/.bonfyre-obsidian.XXXXXX", dir) >= (int)sizeof(tmp)) return -1;
    if (!lstat(path, &st)) { if (exclusive) { errno = EEXIST; return -1; } mode = st.st_mode & 07777; }
    if ((fd = mkstemp(tmp)) < 0) return -1;
    size_t off = 0;
    while (off < len) { ssize_t w = write(fd, data + off, len - off); if (w < 0 && errno == EINTR) continue; if (w <= 0) goto out; off += (size_t)w; }
    if (fchmod(fd, mode) || fsync(fd)) goto out;
    if (close(fd)) { fd = -1; goto out; }
    fd = -1;
    if (exclusive) { if (link(tmp, path)) goto out; unlink(tmp); }
    else if (rename(tmp, path)) goto out;
    { int dfd = open(dir, O_RDONLY); if (dfd >= 0) { fsync(dfd); close(dfd); } }
    return 0;
out:
    { int e = errno; if (fd >= 0) close(fd); unlink(tmp); errno = e; }
    return rc;
}

int ob_sha256_file(const char *path, char hex[65]) { return bf_sha256_file(path, hex); }
void ob_sha256_mem(const void *data, size_t len, char hex[65]) { bf_sha256_hex((const uint8_t *)data, len, hex); }

/* ------------------------------------------------------------ walk */
static int excluded_dir(const char *rel, const char *excluded) {
    const char *p = excluded; size_t rn = strlen(rel);
    while (p && *p) {
        const char *e = strchr(p, ','); size_t n = e ? (size_t)(e - p) : strlen(p);
        while (n && p[n - 1] == '/') n--;
        while (n && *p == '/') { p++; n--; }
        if (n && n == rn && !strncmp(p, rel, n)) return 1;
        p = e ? e + 1 : NULL;
    }
    return 0;
}

static int push(FileList *l, const char *rel, const struct stat *st) {
    if (l->n == l->cap) {
        size_t cap = l->cap ? l->cap * 2 : 256; FileEnt *v = realloc(l->v, cap * sizeof(*v));
        if (!v) return 0;
        l->v = v; l->cap = cap;
    }
    FileEnt *f = &l->v[l->n++];
    memset(f, 0, sizeof(*f));
    f->path = strdup(rel); f->size = (int64_t)st->st_size;
#if defined(__APPLE__)
    f->mtime_ns = (int64_t)st->st_mtimespec.tv_sec * 1000000000 + st->st_mtimespec.tv_nsec;
#else
    f->mtime_ns = (int64_t)st->st_mtim.tv_sec * 1000000000 + st->st_mtim.tv_nsec;
#endif
    return f->path != NULL;
}

static int is_md(const char *name) { size_t n = strlen(name); return n > 3 && !strcasecmp(name + n - 3, ".md"); }

static int walk_dir(const char *root, char *rel, size_t rlen, const char *cfg, const char *excluded, int flags, FileList *out, int depth) {
    char full[OB_PATH]; DIR *d; struct dirent *de; struct stat st;
    if (depth > 64) return 1;
    snprintf(full, sizeof(full), "%s%s%s", root, rlen ? "/" : "", rel);
    if (!(d = opendir(full))) return 0;
    while ((de = readdir(d))) {
        const char *nm = de->d_name; size_t nl = strlen(nm);
        if (!strcmp(nm, ".") || !strcmp(nm, "..")) continue;
        if (nm[0] == '.') {
            int cfg_ok = (flags & WALK_INCLUDE_CFG) && depth == 0 && cfg && !strcmp(nm, cfg);
            if (!cfg_ok) continue;
        }
        if (rlen + nl + 2 >= OB_PATH) { out->skipped++; continue; }
        size_t keep = rlen;
        if (rlen) rel[rlen++] = '/';
        memcpy(rel + rlen, nm, nl + 1); rlen += nl;
        snprintf(full, sizeof(full), "%s/%s", root, rel);
        if (lstat(full, &st)) out->skipped++;
        else if (S_ISLNK(st.st_mode)) out->symlinks++;
        else if (S_ISDIR(st.st_mode)) {
            if (!excluded_dir(rel, excluded) && !walk_dir(root, rel, rlen, cfg, excluded, flags, out, depth + 1)) { closedir(d); return 0; }
        } else if (S_ISREG(st.st_mode)) {
            if (!(flags & WALK_MD_ONLY) || is_md(nm)) { if (!push(out, rel, &st)) { closedir(d); return 0; } }
        } else out->skipped++;
        rlen = keep; rel[rlen] = 0;
    }
    closedir(d);
    return 1;
}

static int ent_cmp(const void *a, const void *b) { return strcmp(((const FileEnt *)a)->path, ((const FileEnt *)b)->path); }

int ob_walk(const char *root, const char *config_dir, const char *excluded, int flags, FileList *out) {
    char rel[OB_PATH] = "";
    memset(out, 0, sizeof(*out));
    if (!walk_dir(root, rel, 0, config_dir, excluded, flags, out, 0)) return 0;
    if (out->n) qsort(out->v, out->n, sizeof(FileEnt), ent_cmp);
    return 1;
}

void ob_files_free(FileList *l) { for (size_t i = 0; i < l->n; i++) free(l->v[i].path); free(l->v); memset(l, 0, sizeof(*l)); }

/* ------------------------------------------------------------ Markdown lines */
typedef struct { const char *s; size_t n; int no; int fm; } Line;
typedef struct { char *buf; size_t len; Line *v; size_t n; } Note;

static int note_load(Note *nt, const char *root, const char *rel) {
    char full[OB_PATH]; size_t cap = 64;
    memset(nt, 0, sizeof(*nt));
    snprintf(full, sizeof(full), "%s/%s", root, rel);
    if (!(nt->buf = ob_read_file(full, &nt->len, MAX_NOTE))) return 0;
    if (!(nt->v = malloc(cap * sizeof(Line)))) return 0;
    const char *p = nt->buf, *end = nt->buf + nt->len; int no = 0;
    while (p < end) {
        const char *e = memchr(p, '\n', (size_t)(end - p)); size_t m = e ? (size_t)(e - p) : (size_t)(end - p);
        if (m && p[m - 1] == '\r') m--;
        if (nt->n == cap) { cap *= 2; Line *v = realloc(nt->v, cap * sizeof(Line)); if (!v) return 0; nt->v = v; }
        nt->v[nt->n++] = (Line){p, m, ++no, 0};
        p = e ? e + 1 : end;
    }
    /* Frontmatter is a first line of --- closed by --- or ...; one that never
     * closes is body text. */
    if (nt->n && nt->v[0].n == 3 && !memcmp(nt->v[0].s, "---", 3)) {
        for (size_t k = 1; k < nt->n; k++) {
            const Line *l = &nt->v[k];
            if (l->n == 3 && (!memcmp(l->s, "---", 3) || !memcmp(l->s, "...", 3))) { for (size_t i = 0; i <= k; i++) nt->v[i].fm = 1; break; }
        }
    }
    return 1;
}
static void note_free(Note *nt) { free(nt->buf); free(nt->v); memset(nt, 0, sizeof(*nt)); }

/* A fence opens with ``` or ~~~ after up to three spaces. */
static int fence_line(const Line *l) {
    size_t i = 0; while (i < l->n && i < 3 && l->s[i] == ' ') i++;
    return l->n - i >= 3 && (!memcmp(l->s + i, "```", 3) || !memcmp(l->s + i, "~~~", 3));
}

/* ------------------------------------------------------------ frontmatter */
#define MAXK 64
#define MAXV 64
typedef struct { char key[128]; int list; int nv; char *val[MAXV]; } Prop;
typedef struct { Prop p[MAXK]; int n; } Props;

static char *trim_dup(const char *s, size_t n) {
    while (n && isspace((unsigned char)*s)) { s++; n--; }
    while (n && isspace((unsigned char)s[n - 1])) n--;
    if (n >= 2 && ((s[0] == '"' && s[n - 1] == '"') || (s[0] == '\'' && s[n - 1] == '\''))) { s++; n -= 2; }
    char *r = malloc(n + 1); if (r) { memcpy(r, s, n); r[n] = 0; } return r;
}
static void prop_add(Prop *p, const char *s, size_t n) { if (p->nv < MAXV) { char *v = trim_dup(s, n); if (v) p->val[p->nv++] = v; } }

static void props_parse(const Note *nt, Props *ps) {
    Prop *cur = NULL; memset(ps, 0, sizeof(*ps));
    for (size_t i = 1; i < nt->n && nt->v[i].fm; i++) {
        const Line *l = &nt->v[i]; size_t j = 0;
        if ((l->n == 3 && (!memcmp(l->s, "---", 3) || !memcmp(l->s, "...", 3)))) break;
        while (j < l->n && l->s[j] == ' ') j++;
        if (cur && j < l->n && l->s[j] == '-' && (j + 1 == l->n || l->s[j + 1] == ' ')) { cur->list = 1; prop_add(cur, l->s + j + 1, l->n - j - 1); continue; }
        if (j) continue;
        const char *colon = memchr(l->s, ':', l->n);
        if (!colon || colon == l->s || ps->n == MAXK) { cur = NULL; continue; }
        cur = &ps->p[ps->n++];
        char *k = trim_dup(l->s, (size_t)(colon - l->s)); snprintf(cur->key, sizeof(cur->key), "%s", k ? k : ""); free(k);
        const char *v = colon + 1; size_t vn = l->n - (size_t)(colon + 1 - l->s);
        while (vn && isspace((unsigned char)*v)) { v++; vn--; }
        while (vn && isspace((unsigned char)v[vn - 1])) vn--;
        if (!vn) continue;
        if (v[0] == '[' && v[vn - 1] == ']' && !(vn >= 4 && v[1] == '[')) {
            cur->list = 1; const char *q = v + 1, *qe = v + vn - 1;
            while (q < qe) { const char *c = memchr(q, ',', (size_t)(qe - q)); size_t m = c ? (size_t)(c - q) : (size_t)(qe - q); if (m) prop_add(cur, q, m); q = c ? c + 1 : qe; }
        } else prop_add(cur, v, vn);
        cur = cur->list ? cur : (cur->nv ? NULL : cur);
    }
}
static void props_free(Props *ps) { for (int i = 0; i < ps->n; i++) for (int j = 0; j < ps->p[i].nv; j++) free(ps->p[i].val[j]); ps->n = 0; }

/* ------------------------------------------------------------ links */
typedef struct { char kind[10]; char raw[512]; char target[512]; int line; } Link;
typedef struct { Link *v; size_t n, cap; } Links;

static void links_push(Links *ls, const char *kind, const char *raw, size_t rn, const char *tgt, size_t tn, int line) {
    if (ls->n == ls->cap) { size_t cap = ls->cap ? ls->cap * 2 : 32; Link *v = realloc(ls->v, cap * sizeof(Link)); if (!v) return; ls->v = v; ls->cap = cap; }
    Link *l = &ls->v[ls->n++];
    snprintf(l->kind, sizeof(l->kind), "%s", kind);
    snprintf(l->raw, sizeof(l->raw), "%.*s", (int)(rn < 511 ? rn : 511), raw);
    snprintf(l->target, sizeof(l->target), "%.*s", (int)(tn < 511 ? tn : 511), tgt);
    l->line = line;
}

static int hexv(int c) { return c >= '0' && c <= '9' ? c - '0' : (c | 32) >= 'a' && (c | 32) <= 'f' ? (c | 32) - 'a' + 10 : -1; }

static void scan_links(const Line *l, Links *ls) {
    const char *s = l->s; size_t n = l->n, i = 0; int code = 0;
    while (i < n) {
        if (s[i] == '`') { code = !code; i++; continue; }
        if (code) { i++; continue; }
        if (i + 1 < n && s[i] == '[' && s[i + 1] == '[') {
            int embed = i > 0 && s[i - 1] == '!';
            const char *st = s + i + 2, *e = NULL;
            for (size_t k = i + 2; k + 1 < n; k++) if (s[k] == ']' && s[k + 1] == ']') { e = s + k; break; }
            if (!e) break;
            size_t tn = (size_t)(e - st);
            for (size_t k = 0; k < tn; k++) if (st[k] == '|' || st[k] == '#' || st[k] == '^') { tn = k; break; }
            links_push(ls, embed ? "embed" : "wikilink", st - 2 - embed, (size_t)(e + 2 - (st - 2 - embed)), st, tn, l->no);
            i = (size_t)(e - s) + 2; continue;
        }
        if (s[i] == ']' && i + 1 < n && s[i + 1] == '(') {
            const char *st = s + i + 2, *e = memchr(st, ')', n - i - 2);
            if (!e) { i++; continue; }
            size_t tn = (size_t)(e - st);
            if (tn >= 2 && st[0] == '<' && st[tn - 1] == '>') { st++; tn -= 2; }
            const char *sp = memchr(st, ' ', tn); if (sp) tn = (size_t)(sp - st);
            int ext = memmem(st, tn, "://", 3) != NULL || (tn >= 7 && !strncasecmp(st, "mailto:", 7));
            size_t hn = tn; for (size_t k = 0; k < tn; k++) if (st[k] == '#') { hn = k; break; }
            if (!ext && hn) {
                char dec[512]; size_t dn = 0;
                for (size_t k = 0; k < hn && dn < sizeof(dec) - 1; k++) {
                    if (st[k] == '%' && k + 2 < hn && hexv(st[k + 1]) >= 0 && hexv(st[k + 2]) >= 0) { dec[dn++] = (char)(hexv(st[k + 1]) * 16 + hexv(st[k + 2])); k += 2; }
                    else dec[dn++] = st[k];
                }
                const char *ob = s + i; while (ob > s && *ob != '[') ob--;
                links_push(ls, "markdown", ob, (size_t)(e + 1 - ob), dec, dn, l->no);
            }
            i = (size_t)(e - s) + 1; continue;
        }
        i++;
    }
}

static void note_links(const Note *nt, Links *ls) {
    int fence = 0;
    for (size_t i = 0; i < nt->n; i++) {
        const Line *l = &nt->v[i];
        if (!l->fm && fence_line(l)) { fence = !fence; continue; }
        if (!fence) scan_links(l, ls);
    }
}

/* Resolution follows Obsidian: a markdown link is relative to its note; a
 * wikilink with a folder matches a path ending in it; a bare name matches any
 * file of that name, preferring the linking note's folder, then the shortest
 * path. Names compare without case (as APFS and Obsidian do). */
typedef struct { char key[256]; size_t idx; } Key;
typedef struct { const FileList *files; Key *keys; size_t nk; } Index;

static void lower_into(char *dst, size_t cap, const char *s, size_t n) { size_t i = 0; for (; i < n && i + 1 < cap; i++) dst[i] = (char)tolower((unsigned char)s[i]); dst[i] = 0; }
static int key_cmp(const void *a, const void *b) { return strcmp(((const Key *)a)->key, ((const Key *)b)->key); }

static int index_build(Index *ix, const FileList *files) {
    ix->files = files; ix->nk = 0;
    if (!(ix->keys = malloc((files->n * 2 + 1) * sizeof(Key)))) return 0;
    for (size_t i = 0; i < files->n; i++) {
        const char *p = files->v[i].path, *b = strrchr(p, '/'); b = b ? b + 1 : p;
        lower_into(ix->keys[ix->nk].key, 256, b, strlen(b)); ix->keys[ix->nk++].idx = i;
        if (is_md(b)) { lower_into(ix->keys[ix->nk].key, 256, b, strlen(b) - 3); ix->keys[ix->nk++].idx = i; }
    }
    qsort(ix->keys, ix->nk, sizeof(Key), key_cmp);
    return 1;
}

static int path_eq_ci(const char *a, const char *b, int with_md) {
    size_t an = strlen(a), bn = strlen(b);
    if (with_md) return an == bn + 3 && !strncasecmp(a, b, bn) && !strcasecmp(a + bn, ".md");
    return an == bn && !strcasecmp(a, b);
}
static int ends_ci(const char *a, const char *suf, int with_md) {
    size_t an = strlen(a), sn = strlen(suf) + (with_md ? 3 : 0);
    if (an <= sn || a[an - sn - 1] != '/') return 0;
    return path_eq_ci(a + an - sn, suf, with_md);
}

static void normalize(const char *base_dir, const char *t, char *out, size_t cap) {
    char tmp[OB_PATH]; char *seg[256]; int ns = 0;
    if (t[0] == '/') snprintf(tmp, sizeof(tmp), "%s", t + 1);
    else snprintf(tmp, sizeof(tmp), "%s%s%s", base_dir, *base_dir ? "/" : "", t);
    for (char *s = strtok(tmp, "/"); s && ns < 256; s = strtok(NULL, "/")) {
        if (!strcmp(s, ".")) continue;
        if (!strcmp(s, "..")) { if (ns) ns--; continue; }
        seg[ns++] = s;
    }
    out[0] = 0; size_t o = 0;
    for (int i = 0; i < ns; i++) o += (size_t)snprintf(out + o, o < cap ? cap - o : 0, "%s%s", i ? "/" : "", seg[i]);
}

static long resolve(const Index *ix, const char *source, const Link *lk) {
    char key[256], rel[OB_PATH], dir[OB_PATH]; const char *t = lk->target, *b = strrchr(t, '/'), *sd = strrchr(source, '/');
    long best = -1; size_t bestlen = (size_t)-1; int best_same = 0;
    if (!*t) return -1;
    snprintf(dir, sizeof(dir), "%.*s", sd ? (int)(sd - source) : 0, source);
    lower_into(key, sizeof(key), b ? b + 1 : t, strlen(b ? b + 1 : t));
    int markdown = !strcmp(lk->kind, "markdown");
    if (markdown) normalize(dir, t, rel, sizeof(rel));
    Key probe; snprintf(probe.key, sizeof(probe.key), "%s", key);
    Key *hit = bsearch(&probe, ix->keys, ix->nk, sizeof(Key), key_cmp);
    if (!hit) return -1;
    while (hit > ix->keys && !strcmp(hit[-1].key, key)) hit--;
    for (; hit < ix->keys + ix->nk && !strcmp(hit->key, key); hit++) {
        const char *p = ix->files->v[hit->idx].path; int ok;
        if (markdown) ok = path_eq_ci(p, rel, 0) || path_eq_ci(p, rel, 1);
        else if (b) ok = path_eq_ci(p, t, 0) || path_eq_ci(p, t, 1) || ends_ci(p, t, 0) || ends_ci(p, t, 1);
        else ok = 1;
        if (!ok) continue;
        const char *ps = strrchr(p, '/'); int same = (ps ? (size_t)(ps - p) : 0) == strlen(dir) && !strncmp(p, dir, strlen(dir));
        size_t pl = strlen(p);
        if (best < 0 || (same && !best_same) || (same == best_same && (pl < bestlen || (pl == bestlen && strcmp(p, ix->files->v[best].path) < 0)))) {
            best = (long)hit->idx; bestlen = pl; best_same = same;
        }
    }
    return best;
}

/* ------------------------------------------------------------ tags */
static int tag_char(unsigned char c) { return isalnum(c) || c == '_' || c == '-' || c == '/' || c >= 0x80; }

typedef void (*tag_fn)(void *ctx, const char *tag, size_t n, int line);

static void note_tags(const Note *nt, tag_fn fn, void *ctx) {
    Props ps; props_parse(nt, &ps);
    for (int i = 0; i < ps.n; i++) if (!strcasecmp(ps.p[i].key, "tags") || !strcasecmp(ps.p[i].key, "tag")) {
        for (int j = 0; j < ps.p[i].nv; j++) {
            const char *v = ps.p[i].val[j];
            while (*v) {
                while (*v == ' ' || *v == ',' || *v == '#') v++;
                size_t n = 0; while (v[n] && v[n] != ' ' && v[n] != ',') n++;
                if (n) fn(ctx, v, n, 0);
                v += n;
            }
        }
    }
    props_free(&ps);
    int fence = 0;
    for (size_t i = 0; i < nt->n; i++) {
        const Line *l = &nt->v[i]; int code = 0;
        if (l->fm) continue;
        if (fence_line(l)) { fence = !fence; continue; }
        if (fence) continue;
        for (size_t k = 0; k < l->n; k++) {
            if (l->s[k] == '`') { code = !code; continue; }
            if (code || l->s[k] != '#' || (k && !isspace((unsigned char)l->s[k - 1]))) continue;
            size_t e = k + 1, digits = 1;
            while (e < l->n && tag_char((unsigned char)l->s[e])) { if (!isdigit((unsigned char)l->s[e])) digits = 0; e++; }
            if (e > k + 1 && !digits) fn(ctx, l->s + k + 1, e - k - 1, l->no);
            k = e - 1;
        }
    }
}

/* ------------------------------------------------------------ tasks */
static int task_parse(const Line *l, size_t *mark_at, char *mark, const char **text, size_t *tn) {
    size_t i = 0; const char *s = l->s; size_t n = l->n;
    while (i < n && (s[i] == ' ' || s[i] == '\t')) i++;
    while (i < n && s[i] == '>') { i++; while (i < n && s[i] == ' ') i++; }
    if (i < n && (s[i] == '-' || s[i] == '*' || s[i] == '+')) i++;
    else { size_t d = i; while (i < n && isdigit((unsigned char)s[i])) i++; if (i == d || i >= n || (s[i] != '.' && s[i] != ')')) return 0; i++; }
    if (i >= n || s[i] != ' ') return 0;
    while (i < n && s[i] == ' ') i++;
    if (i + 2 >= n || s[i] != '[' || s[i + 2] != ']') return 0;
    if (i + 3 < n && s[i + 3] != ' ') return 0;
    *mark_at = i + 1; *mark = s[i + 1];
    *text = s + i + 3; *tn = n - i - 3;
    while (*tn && **text == ' ') { (*text)++; (*tn)--; }
    return 1;
}
static const char *task_status(char m) { return m == ' ' ? "todo" : (m == 'x' || m == 'X') ? "done" : "other"; }

/* ------------------------------------------------------------ verbs */
static void native_head(bf_jsonw *w) { bf_jsonw_key(w, "shape"); bf_jsonw_str(w, "bonfyre-native"); bf_jsonw_key(w, "via"); bf_jsonw_str(w, "native"); }

static int load_md(const char *root, const Binding *b, const char *rel, FileList *fl) {
    if (rel) {
        char full[OB_PATH], why[256]; struct stat st;
        memset(fl, 0, sizeof(*fl));
        if (ob_resolve_read(root, rel, b->config_dir, full, why, sizeof(why)) != EX_OK || stat(full, &st)) return 0;
        return push(fl, rel, &st);
    }
    return ob_walk(root, b->config_dir, b->excluded, WALK_MD_ONLY, fl);
}

static int ci_find(const char *hay, size_t n, const char *needle, size_t nn) {
    if (!nn) return 0;
    for (size_t i = 0; i + nn <= n; i++) {
        size_t k = 0; while (k < nn && tolower((unsigned char)hay[i + k]) == tolower((unsigned char)needle[k])) k++;
        if (k == nn) return 1;
    }
    return 0;
}

int ob_verb_search(bf_jsonw *w, const char *root, const Binding *b, const char *query, const char *prefix, int limit) {
    FileList fl; size_t qn = strlen(query); int count = 0, truncated = 0;
    if (!ob_walk(root, b->config_dir, b->excluded, WALK_MD_ONLY, &fl)) return EX_FAIL;
    bf_jsonw_obj(w); native_head(w);
    bf_jsonw_key(w, "query"); bf_jsonw_str(w, query);
    bf_jsonw_key(w, "file_matches"); bf_jsonw_arr(w);
    for (size_t i = 0; i < fl.n; i++) {
        if (prefix && strncmp(fl.v[i].path, prefix, strlen(prefix))) continue;
        if (ci_find(fl.v[i].path, strlen(fl.v[i].path), query, qn)) bf_jsonw_str(w, fl.v[i].path);
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "matches"); bf_jsonw_arr(w);
    size_t scanned = 0;
    for (size_t i = 0; i < fl.n && !truncated; i++) {
        Note nt;
        if (prefix && strncmp(fl.v[i].path, prefix, strlen(prefix))) continue;
        if (!note_load(&nt, root, fl.v[i].path)) { note_free(&nt); continue; }
        scanned++;
        for (size_t k = 0; k < nt.n; k++) {
            if (!ci_find(nt.v[k].s, nt.v[k].n, query, qn)) continue;
            if (count == limit) { truncated = 1; break; }
            bf_jsonw_obj(w);
            bf_jsonw_key(w, "path"); bf_jsonw_str(w, fl.v[i].path);
            bf_jsonw_key(w, "line"); bf_jsonw_int(w, nt.v[k].no);
            bf_jsonw_key(w, "text"); bf_jsonw_strn(w, nt.v[k].s, nt.v[k].n > 240 ? 240 : nt.v[k].n);
            bf_jsonw_obj_end(w); count++;
        }
        note_free(&nt);
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "files_scanned"); bf_jsonw_int(w, (int64_t)scanned);
    bf_jsonw_key(w, "truncated"); bf_jsonw_bool(w, truncated);
    bf_jsonw_obj_end(w);
    ob_files_free(&fl);
    return EX_OK;
}

int ob_verb_tasks(bf_jsonw *w, const char *root, const Binding *b, const char *status, const char *rel) {
    FileList fl; int64_t c_todo = 0, c_done = 0, c_other = 0;
    if (!load_md(root, b, rel, &fl)) return rel ? EX_NOTFOUND : EX_FAIL;
    bf_jsonw_obj(w); native_head(w);
    bf_jsonw_key(w, "tasks"); bf_jsonw_arr(w);
    for (size_t i = 0; i < fl.n; i++) {
        Note nt; int fence = 0;
        if (!note_load(&nt, root, fl.v[i].path)) { note_free(&nt); continue; }
        for (size_t k = 0; k < nt.n; k++) {
            size_t at; char m; const char *t; size_t tn;
            if (nt.v[k].fm) continue;
            if (fence_line(&nt.v[k])) { fence = !fence; continue; }
            if (fence || !task_parse(&nt.v[k], &at, &m, &t, &tn)) continue;
            const char *st = task_status(m);
            if (st[0] == 't') c_todo++; else if (st[0] == 'd') c_done++; else c_other++;
            if (status && strcmp(status, "all") && strcmp(status, st)) continue;
            char ref[OB_PATH + 16]; snprintf(ref, sizeof(ref), "%s:%d", fl.v[i].path, nt.v[k].no);
            bf_jsonw_obj(w);
            bf_jsonw_key(w, "ref"); bf_jsonw_str(w, ref);
            bf_jsonw_key(w, "path"); bf_jsonw_str(w, fl.v[i].path);
            bf_jsonw_key(w, "line"); bf_jsonw_int(w, nt.v[k].no);
            bf_jsonw_key(w, "status"); bf_jsonw_str(w, st);
            bf_jsonw_key(w, "mark"); bf_jsonw_strn(w, &m, 1);
            bf_jsonw_key(w, "text"); bf_jsonw_strn(w, t, tn);
            bf_jsonw_obj_end(w);
        }
        note_free(&nt);
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "counts"); bf_jsonw_obj(w);
    bf_jsonw_key(w, "done"); bf_jsonw_int(w, c_done);
    bf_jsonw_key(w, "other"); bf_jsonw_int(w, c_other);
    bf_jsonw_key(w, "todo"); bf_jsonw_int(w, c_todo);
    bf_jsonw_obj_end(w);
    bf_jsonw_obj_end(w);
    ob_files_free(&fl);
    return EX_OK;
}

int ob_task_set(const char *file, int line, const char *how, char *why, size_t cap, char before[65], char after[65]) {
    size_t len; char *buf = ob_read_file(file, &len, MAX_NOTE); int no = 1;
    if (!buf) { snprintf(why, cap, "the note cannot be read"); return EX_NOTFOUND; }
    ob_sha256_mem(buf, len, before);
    char *p = buf, *end = buf + len;
    while (no < line && p < end) { char *e = memchr(p, '\n', (size_t)(end - p)); if (!e) { p = end; break; } p = e + 1; no++; }
    if (no != line || p >= end) { free(buf); snprintf(why, cap, "the note has no line %d", line); return EX_NOTFOUND; }
    char *e = memchr(p, '\n', (size_t)(end - p)); Line l = {p, e ? (size_t)(e - p) : (size_t)(end - p), line, 0};
    if (l.n && p[l.n - 1] == '\r') l.n--;
    size_t at; char m; const char *t; size_t tn;
    if (!task_parse(&l, &at, &m, &t, &tn)) { free(buf); snprintf(why, cap, "line %d is not a task", line); return EX_REFUSED; }
    char nm = !strcmp(how, "done") ? 'x' : !strcmp(how, "todo") ? ' ' : (m == ' ' ? 'x' : ' ');
    p[at] = nm;
    int rc = ob_atomic_write(file, buf, len, 0);
    ob_sha256_mem(buf, len, after);
    free(buf);
    if (rc) { snprintf(why, cap, "write failed: %s", strerror(errno)); return EX_FAIL; }
    return EX_OK;
}

int ob_verb_links(bf_jsonw *w, const char *root, const Binding *b, const char *mode, const char *rel) {
    FileList all, md; Index ix;
    if (!ob_walk(root, b->config_dir, b->excluded, 0, &all)) return EX_FAIL;
    if (!index_build(&ix, &all)) { ob_files_free(&all); return EX_FAIL; }
    int out_mode = !strcmp(mode, "links"), back = !strcmp(mode, "backlinks");
    if ((out_mode || back) && !rel) { free(ix.keys); ob_files_free(&all); return EX_USAGE; }
    long target_idx = -1;
    if (back) {
        char full[OB_PATH], why[256];
        if (ob_resolve_read(root, rel, b->config_dir, full, why, sizeof(why)) != EX_OK) { free(ix.keys); ob_files_free(&all); return EX_NOTFOUND; }
        for (size_t i = 0; i < all.n; i++) if (!strcmp(all.v[i].path, rel)) { target_idx = (long)i; break; }
    }
    if (out_mode) { if (!load_md(root, b, rel, &md)) { free(ix.keys); ob_files_free(&all); return EX_NOTFOUND; } }
    else if (!ob_walk(root, b->config_dir, b->excluded, WALK_MD_ONLY, &md)) { free(ix.keys); ob_files_free(&all); return EX_FAIL; }
    bf_jsonw_obj(w); native_head(w);
    if (rel) { bf_jsonw_key(w, back ? "target" : "source"); bf_jsonw_str(w, rel); }
    bf_jsonw_key(w, mode); bf_jsonw_arr(w);
    int64_t total = 0;
    for (size_t i = 0; i < md.n; i++) {
        Note nt; Links ls = {0};
        if (!note_load(&nt, root, md.v[i].path)) { note_free(&nt); continue; }
        note_links(&nt, &ls);
        for (size_t k = 0; k < ls.n; k++) {
            long r = resolve(&ix, md.v[i].path, &ls.v[k]);
            if (back && (r < 0 || r != target_idx)) continue;
            if (!strcmp(mode, "unresolved") && r >= 0) continue;
            bf_jsonw_obj(w);
            if (!out_mode) { bf_jsonw_key(w, "source"); bf_jsonw_str(w, md.v[i].path); }
            bf_jsonw_key(w, "line"); bf_jsonw_int(w, ls.v[k].line);
            bf_jsonw_key(w, "kind"); bf_jsonw_str(w, ls.v[k].kind);
            bf_jsonw_key(w, "raw"); bf_jsonw_str(w, ls.v[k].raw);
            bf_jsonw_key(w, "target"); bf_jsonw_str(w, ls.v[k].target);
            if (out_mode) { bf_jsonw_key(w, "resolved"); if (r >= 0) bf_jsonw_str(w, all.v[r].path); else bf_jsonw_null(w); }
            bf_jsonw_obj_end(w); total++;
        }
        free(ls.v); note_free(&nt);
    }
    bf_jsonw_arr_end(w);
    bf_jsonw_key(w, "count"); bf_jsonw_int(w, total);
    bf_jsonw_obj_end(w);
    free(ix.keys); ob_files_free(&all); ob_files_free(&md);
    return EX_OK;
}

typedef struct { char tag[128]; int64_t count, files; const char *last_file; } TagRow;
typedef struct { TagRow *v; size_t n, cap; const char *file; } TagSet;
static void tag_seen(void *ctx, const char *t, size_t n, int line) {
    TagSet *ts = ctx; char low[128]; (void)line;
    lower_into(low, sizeof(low), t, n);
    while (strlen(low) && low[strlen(low) - 1] == '/') low[strlen(low) - 1] = 0;
    if (!low[0]) return;
    for (size_t i = 0; i < ts->n; i++) if (!strcmp(ts->v[i].tag, low)) {
        ts->v[i].count++; if (ts->v[i].last_file != ts->file) { ts->v[i].files++; ts->v[i].last_file = ts->file; } return;
    }
    if (ts->n == ts->cap) { size_t cap = ts->cap ? ts->cap * 2 : 64; TagRow *v = realloc(ts->v, cap * sizeof(TagRow)); if (!v) return; ts->v = v; ts->cap = cap; }
    TagRow *r = &ts->v[ts->n++]; snprintf(r->tag, sizeof(r->tag), "%s", low); r->count = 1; r->files = 1; r->last_file = ts->file;
}
static int tag_cmp(const void *a, const void *b) { return strcmp(((const TagRow *)a)->tag, ((const TagRow *)b)->tag); }

int ob_verb_tags(bf_jsonw *w, const char *root, const Binding *b, const char *rel) {
    FileList fl; TagSet ts = {0};
    if (!load_md(root, b, rel, &fl)) return rel ? EX_NOTFOUND : EX_FAIL;
    for (size_t i = 0; i < fl.n; i++) {
        Note nt; if (!note_load(&nt, root, fl.v[i].path)) { note_free(&nt); continue; }
        ts.file = fl.v[i].path; note_tags(&nt, tag_seen, &ts); note_free(&nt);
    }
    if (ts.n) qsort(ts.v, ts.n, sizeof(TagRow), tag_cmp);
    bf_jsonw_obj(w); native_head(w);
    if (rel) { bf_jsonw_key(w, "path"); bf_jsonw_str(w, rel); }
    bf_jsonw_key(w, "tags"); bf_jsonw_arr(w);
    for (size_t i = 0; i < ts.n; i++) {
        bf_jsonw_obj(w);
        bf_jsonw_key(w, "tag"); bf_jsonw_str(w, ts.v[i].tag);
        bf_jsonw_key(w, "count"); bf_jsonw_int(w, ts.v[i].count);
        bf_jsonw_key(w, "files"); bf_jsonw_int(w, ts.v[i].files);
        bf_jsonw_obj_end(w);
    }
    bf_jsonw_arr_end(w); bf_jsonw_obj_end(w);
    free(ts.v); ob_files_free(&fl);
    return EX_OK;
}

int ob_verb_properties(bf_jsonw *w, const char *root, const Binding *b, const char *rel) {
    FileList fl;
    if (!load_md(root, b, rel, &fl)) return rel ? EX_NOTFOUND : EX_FAIL;
    bf_jsonw_obj(w); native_head(w);
    if (rel) {
        Note nt; Props ps; ps.n = 0;
        bf_jsonw_key(w, "path"); bf_jsonw_str(w, rel);
        bf_jsonw_key(w, "properties"); bf_jsonw_obj(w);
        if (fl.n && note_load(&nt, root, rel)) {
            props_parse(&nt, &ps);
            for (int i = 0; i < ps.n; i++) {
                int dup = 0; for (int j = 0; j < i; j++) if (!strcmp(ps.p[j].key, ps.p[i].key)) dup = 1;
                if (dup) continue;
                bf_jsonw_key(w, ps.p[i].key);
                if (ps.p[i].list) { bf_jsonw_arr(w); for (int j = 0; j < ps.p[i].nv; j++) bf_jsonw_str(w, ps.p[i].val[j]); bf_jsonw_arr_end(w); }
                else if (ps.p[i].nv) bf_jsonw_str(w, ps.p[i].val[0]);
                else bf_jsonw_null(w);
            }
            props_free(&ps);
        }
        note_free(&nt);
        bf_jsonw_obj_end(w);
    } else {
        typedef struct { char key[128]; int64_t files; } KC;
        KC *kc = NULL; size_t nk = 0, ck = 0;
        for (size_t i = 0; i < fl.n; i++) {
            Note nt; Props ps;
            if (!note_load(&nt, root, fl.v[i].path)) { note_free(&nt); continue; }
            props_parse(&nt, &ps);
            for (int p = 0; p < ps.n; p++) {
                size_t j = 0; for (; j < nk && strcmp(kc[j].key, ps.p[p].key); j++) {}
                if (j == nk) {
                    if (nk == ck) { ck = ck ? ck * 2 : 32; KC *v = realloc(kc, ck * sizeof(KC)); if (!v) break; kc = v; }
                    snprintf(kc[nk].key, sizeof(kc[nk].key), "%s", ps.p[p].key); kc[nk++].files = 0;
                }
                kc[j].files++;
            }
            props_free(&ps); note_free(&nt);
        }
        for (size_t i = 1; i < nk; i++) for (size_t j = i; j && strcmp(kc[j - 1].key, kc[j].key) > 0; j--) { KC t = kc[j]; kc[j] = kc[j - 1]; kc[j - 1] = t; }
        bf_jsonw_key(w, "keys"); bf_jsonw_arr(w);
        for (size_t i = 0; i < nk; i++) { bf_jsonw_obj(w); bf_jsonw_key(w, "key"); bf_jsonw_str(w, kc[i].key); bf_jsonw_key(w, "files"); bf_jsonw_int(w, kc[i].files); bf_jsonw_obj_end(w); }
        bf_jsonw_arr_end(w);
        free(kc);
    }
    bf_jsonw_obj_end(w);
    ob_files_free(&fl);
    return EX_OK;
}

/* ------------------------------------------------------------ daily note */
static const char *const MONTHS[] = {"January","February","March","April","May","June","July","August","September","October","November","December"};
static const char *const DAYS[] = {"Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"};

/* Renders the moment.js tokens Obsidian's daily-notes format commonly uses:
 * YYYY YY MMMM MMM MM M DD D dddd ddd, and [literal]. */
int ob_daily_rel(const char *root, const char *config_dir, const char *date, char *out, size_t cap) {
    char cfgp[OB_PATH], folder[512] = "", format[128] = "YYYY-MM-DD", name[256]; struct tm tm; size_t o = 0;
    snprintf(cfgp, sizeof(cfgp), "%s/%s/daily-notes.json", root, config_dir);
    char *txt = ob_read_file(cfgp, NULL, 1 << 20);
    if (txt) {
        char err[64]; bf_json_doc_t *d = bf_json_parse_str(txt, err, sizeof(err));
        if (d) {
            const bf_json_node_t *r = bf_json_root(d), *f = r ? bf_json_obj_get(d, r, "folder") : NULL, *fm = r ? bf_json_obj_get(d, r, "format") : NULL;
            if (f) bf_json_get_str_copy(f, folder, sizeof(folder));
            if (fm) { char tmp[128] = ""; bf_json_get_str_copy(fm, tmp, sizeof(tmp)); if (tmp[0]) snprintf(format, sizeof(format), "%s", tmp); }
            bf_json_free(d);
        }
        free(txt);
    }
    memset(&tm, 0, sizeof(tm));
    if (date && *date) {
        int y, m, dd;
        if (sscanf(date, "%4d-%2d-%2d", &y, &m, &dd) != 3 || strlen(date) != 10 || m < 1 || m > 12 || dd < 1 || dd > 31) return 0;
        tm.tm_year = y - 1900; tm.tm_mon = m - 1; tm.tm_mday = dd; tm.tm_hour = 12; tm.tm_isdst = -1;
        if (mktime(&tm) == (time_t)-1) return 0;
    } else { time_t now = time(NULL); localtime_r(&now, &tm); }
    for (const char *f = format; *f && o + 16 < sizeof(name);) {
        if (*f == '[') { const char *e = strchr(f, ']'); if (!e) break; for (const char *q = f + 1; q < e && o + 1 < sizeof(name); q++) name[o++] = *q; f = e + 1; continue; }
        if (!strncmp(f, "YYYY", 4)) { o += (size_t)snprintf(name + o, sizeof(name) - o, "%04d", tm.tm_year + 1900); f += 4; }
        else if (!strncmp(f, "YY", 2)) { o += (size_t)snprintf(name + o, sizeof(name) - o, "%02d", (tm.tm_year + 1900) % 100); f += 2; }
        else if (!strncmp(f, "MMMM", 4)) { o += (size_t)snprintf(name + o, sizeof(name) - o, "%s", MONTHS[tm.tm_mon]); f += 4; }
        else if (!strncmp(f, "MMM", 3)) { o += (size_t)snprintf(name + o, sizeof(name) - o, "%.3s", MONTHS[tm.tm_mon]); f += 3; }
        else if (!strncmp(f, "MM", 2)) { o += (size_t)snprintf(name + o, sizeof(name) - o, "%02d", tm.tm_mon + 1); f += 2; }
        else if (*f == 'M') { o += (size_t)snprintf(name + o, sizeof(name) - o, "%d", tm.tm_mon + 1); f += 1; }
        else if (!strncmp(f, "DD", 2)) { o += (size_t)snprintf(name + o, sizeof(name) - o, "%02d", tm.tm_mday); f += 2; }
        else if (*f == 'D') { o += (size_t)snprintf(name + o, sizeof(name) - o, "%d", tm.tm_mday); f += 1; }
        else if (!strncmp(f, "dddd", 4)) { o += (size_t)snprintf(name + o, sizeof(name) - o, "%s", DAYS[tm.tm_wday]); f += 4; }
        else if (!strncmp(f, "ddd", 3)) { o += (size_t)snprintf(name + o, sizeof(name) - o, "%.3s", DAYS[tm.tm_wday]); f += 3; }
        else name[o++] = *f++;
    }
    name[o] = 0;
    char *fl = folder; while (*fl == '/') fl++;
    size_t fn = strlen(fl); while (fn && fl[fn - 1] == '/') fl[--fn] = 0;
    return snprintf(out, cap, "%s%s%s.md", fl, *fl ? "/" : "", name) < (int)cap;
}
