/* Build-edge audit, ported from architecture/check-build-edges.mjs. This
 * deliberately reads simple Makefile assignments, not make's evaluated graph.
 * Directory twins and matching hashes are observations, never deletion proof. */
#include <ctype.h>

static Strs be_dirs(const char *base) {
    Strs out = {0}; DIR *d = opendir(base); if (!d) return out;
    for (struct dirent *e; (e = readdir(d));) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char *p = fmt("%s/%s", base, e->d_name); struct stat s;
        if (!lstat(p, &s) && S_ISDIR(s.st_mode)) push(&out, strdup(e->d_name));
        free(p);
    }
    closedir(d); qsort(out.v, (size_t)out.n, sizeof *out.v, cmp_str); return out;
}
static void be_free(Strs *s) { for (int i = 0; i < s->n; i++) free(s->v[i]); free(s->v); }
static char *be_variable(const char *text, const char *const *names, int count) {
    for (int i = 0; i < count; i++) for (const char *p = text; p && *p;) {
        const char *end = strchr(p, '\n'); if (!end) end = p + strlen(p);
        size_t len = strlen(names[i]);
        if ((size_t)(end - p) >= len && !strncmp(p, names[i], len)) {
            const char *q = p + len; while (q < end && js_ws((unsigned char)*q)) q++;
            if (q < end && (*q == ':' || *q == '?')) q++;
            if (q < end && *q++ == '=') {
                char *raw = strndup(q, (size_t)(end - q)), *value = trim(raw); free(raw);
                if (*value) return value;
                free(value);
            }
        }
        p = *end ? end + 1 : NULL;
    }
    return strdup("");
}
static char *be_slug(const char *name, const char *prefix, int acronym) {
    const char *s = strlen(name) >= 7 ? name + 7 : "";
    char *out = calloc(strlen(prefix) + strlen(s) * 2 + 1, 1); strcpy(out, prefix);
    size_t n = strlen(out);
    for (size_t i = 0; s[i]; i++) {
        if (i && isupper((unsigned char)s[i]) && (!acronym || islower((unsigned char)s[i + 1]))) out[n++] = '-';
        out[n++] = (char)tolower((unsigned char)s[i]);
    }
    return out;
}
/* Lexical resolution is intentional: nonexistent declared sources still appear
 * in the report, just as node:path.resolve/relative retained them. */
static Strs be_parts(const char *path) {
    Strs parts = {0}; char *copy = strdup(path), *save = NULL;
    for (char *s = strtok_r(copy, "/", &save); s; s = strtok_r(NULL, "/", &save)) {
        if (!strcmp(s, ".")) continue;
        if (!strcmp(s, "..")) { if (parts.n) free(parts.v[--parts.n]); }
        else push(&parts, strdup(s));
    }
    free(copy); return parts;
}
static char *be_source(const char *cmd, const char *path) {
    char *absolute = *path == '/' ? strdup(path) : fmt("%s/%s", cmd, path);
    Strs a = be_parts(root), b = be_parts(absolute); free(absolute);
    int common = 0; while (common < a.n && common < b.n && !strcmp(a.v[common], b.v[common])) common++;
    size_t cap = (size_t)(a.n - common) * 3 + 1;
    for (int i = common; i < b.n; i++) cap += strlen(b.v[i]) + 1;
    char *out = calloc(cap, 1);
    for (int i = common; i < a.n; i++) strcat(out, "../");
    for (int i = common; i < b.n; i++) { strcat(out, b.v[i]); if (i + 1 < b.n) strcat(out, "/"); }
    size_t n = strlen(out); if (n && out[n - 1] == '/') out[n - 1] = 0;
    be_free(&a); be_free(&b); return out;
}
static int be_native_source(const char *path, int header) {
    const char *ext = strrchr(path, '.');
    return ext && (!strcmp(ext, ".c") || !strcmp(ext, ".cc") || !strcmp(ext, ".cpp") ||
        !strcmp(ext, ".rs") || !strcmp(ext, ".zig") || !strcmp(ext, ".go") || (header && !strcmp(ext, ".h")));
}
static char *be_digest(const char *path) {
    size_t n; char *body = slurp(path, &n); if (!body) return NULL;
    char *hex = malloc(65); bf_sha256_hex((const uint8_t *)body, n, hex); free(body); return hex;
}
static double be_mtime(const char *path) {
    struct stat st; if (stat(path, &st)) return 0;
#ifdef __APPLE__
    return st.st_mtimespec.tv_sec + st.st_mtimespec.tv_nsec / 1e9;
#else
    return st.st_mtim.tv_sec + st.st_mtim.tv_nsec / 1e9;
#endif
}
static const char *be_text(const bf_jv *o, const char *k) { return bf_jv_s(bf_jv_get(o, k)); }
static int cmd_build_edges(const Atlas *a, int argc, char **argv) {
    int json = 0, strict = 0;
    for (int i = 0; i < argc; i++) {
        if (!strcmp(argv[i], "--json")) json = 1;
        else if (!strcmp(argv[i], "--strict")) strict = 1;
        else { fputs("usage: bonfyre-atlas build-edges [--strict] [--json]\n", stderr); return 2; }
    }
    char *cmds = fmt("%s/cmd", root), *components = fmt("%s/components", root);
    Strs names = be_dirs(cmds), twins = be_dirs(components);
    const char *env = getenv("BONFYRE_BIN"), *home = getenv("HOME");
    char *bins = env ? strdup(env) : fmt("%s/.bonfyre/bin", home ? home : "");
    bf_jv *report = bf_jv_obj(), *rows = bf_jv_arr(), *delegating = bf_jv_arr(), *wrong = bf_jv_arr(), *drift = bf_jv_arr();
    int both = 0;
    for (int i = 0; i < names.n; i++) {
        const char *name = names.v[i]; int twin = 0;
        for (int j = 0; j < twins.n; j++) if (!strcmp(name, twins.v[j])) twin = 1;
        if (!twin) continue;
        char *cmd = fmt("%s/%s", cmds, name), *component = fmt("%s/%s", components, name);
        char *path = fmt("%s/Makefile", cmd), *make = slurp(path, NULL); free(path); if (!make) make = strdup("");
        const char *const target_keys[] = {"TARGET", "BIN", "BINARY"}, *const source_keys[] = {"SOURCE", "SRC", "SRCS", "SOURCES"};
        const char *const owner_keys[] = {"OWNER_DIR"};
        char *target = be_variable(make, target_keys, 3), *declared = be_variable(make, source_keys, 4);
        char *owner_dir = be_variable(make, owner_keys, 1);
        int owner_delegate = *owner_dir && strstr(make, "OWNER_DIR") && strstr(make, "-C");
        if (!*target) { free(target); target = be_slug(name, "bonfyre-", 1); }
        char *main = fmt("%s/src/main.c", cmd), *other_main = fmt("%s/src/main.c", component);
        if (!*declared && owner_delegate) {
            char *owner_main = fmt("%s/src/main.c", owner_dir);
            char *resolved = fmt("%s/%s", cmd, owner_main);
            if (exists(resolved)) { free(declared); declared = owner_main; }
            else free(owner_main);
            free(resolved);
        }
        if (!*declared && exists(main)) { free(declared); declared = strdup("src/main.c"); }
        free(owner_dir);
        bf_jv *built = bf_jv_arr(); char *save = NULL;
        int delegates = 0, names_built = 1; double newest = 0;
        char *slug = be_slug(name, "cmd-", 0); const Block *entry = find(&a->arch, slug); free(slug);
        const bf_jv *sources = list(entry, "source_path");
        for (char *s = strtok_r(declared, " \t\r\n\v\f", &save); s; s = strtok_r(NULL, " \t\r\n\v\f", &save)) {
            char *relative = be_source(cmd, s); bf_jv_push(built, bf_jv_str(relative));
            if (!strncmp(relative, "components/", 11)) delegates = 1;
            path = fmt("%s/%s", root, relative); double mtime = be_mtime(path); free(path); if (mtime > newest) newest = mtime;
            if (be_native_source(relative, 0) && !list_has(sources, relative)) names_built = 0;
            free(relative);
        }
        path = fmt("%s/%s", bins, target); char *installed = be_digest(path); double installed_time = be_mtime(path); free(path);
        path = fmt("%s/%s", cmd, target); char *from_cmd = be_digest(path); free(path);
        path = fmt("%s/%s", component, target); char *from_component = be_digest(path); free(path);
        const char *installation = !installed ? "not installed" : from_cmd && !strcmp(installed, from_cmd) ? "cmd build" :
            from_component && !strcmp(installed, from_component) ? "components build" : installed_time < newest ? "older than its source" : "another build of this source";
        const char *atlas = !entry ? "no entry" : names_built ? "names the built source" : "names a source the build does not use";
        bf_jv *row = bf_jv_obj(); bf_jv_set(row, "family", bf_jv_str(name)); bf_jv_set(row, "cmd_builds_from", built);
        bf_jv_set(row, "delegates_to_components", bf_jv_bool(delegates));
        int has_both = exists(main) && exists(other_main); both += has_both;
        bf_jv_set(row, "both_have_main", bf_jv_bool(has_both)); bf_jv_set(row, "installed", bf_jv_str(installation));
        bf_jv_set(row, "atlas", bf_jv_str(atlas)); bf_jv_set(row, "unbuilt_beside_build_edge", bf_jv_bool(delegates && exists(main)));
        bf_jv_push(rows, row);
        if (delegates) bf_jv_push(delegating, bf_jv_str(name));
        if (entry && !names_built) bf_jv_push(wrong, bf_jv_str(name));
        if (!strcmp(installation, "older than its source")) bf_jv_push(drift, bf_jv_str(name));
        free(installed); free(from_cmd); free(from_component); free(main); free(other_main); free(make); free(target); free(declared); free(cmd); free(component);
    }
    bf_jv_set(report, "families", bf_jv_num(rows->len)); bf_jv_set(report, "both_have_main", bf_jv_num(both));
    bf_jv_set(report, "delegating", delegating); bf_jv_set(report, "atlas_names_unbuilt_source", wrong);
    bf_jv_set(report, "installed_older_than_source", drift); bf_jv_set(report, "rows", rows);
    bf_jv *limits = bf_jv_arr();
    bf_jv_push(limits, bf_jv_str("a components/ twin -> a redundant copy (several carry operations the cmd/ program lacks)"));
    bf_jv_push(limits, bf_jv_str("an installed binary matching the cmd build -> the components program is unused (callers may name it directly)"));
    bf_jv_push(limits, bf_jv_str("a matching hash -> the same behaviour as a rebuild from changed source"));
    bf_jv_set(report, "cannot_infer", limits);
    if (json) { char *s = bf_jv_dump(report, 1, BF_JV_SORT_NONE, NULL); puts(s); free(s); }
    else {
        printf("build edges: %zu families exist under both cmd/ and components/; %d have a main.c in each\n", rows->len, both);
        for (size_t i = 0; i < rows->len; i++) {
            const bf_jv *row = rows->items[i], *built = bf_jv_get(row, "cmd_builds_from");
            printf("  %-26s built from %-44s installed: %-30s atlas %s\n", be_text(row, "family"), built->len ? built->items[0]->s : "?", be_text(row, "installed"), be_text(row, "atlas"));
        }
        printf("  %zu Atlas entries name a source the build does not use\n  %zu installed binaries are older than the source they are built from\n", wrong->len, drift->len);
        for (size_t i = 0; i < limits->len; i++) printf("  cannot infer: %s\n", limits->items[i]->s);
    }
    int rc = strict && wrong->len ? 1 : 0;
    bf_jv_free(report); be_free(&names); be_free(&twins); free(cmds); free(components); free(bins); return rc;
}
