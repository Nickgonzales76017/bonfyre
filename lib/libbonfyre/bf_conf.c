/*
 * bf_conf.c — INI/TOML configuration parser
 *
 * Internal storage: flat array of (section, key, value) triples.
 * Linear scan is fine for <1000 entries (typical config files).
 */

#include "bf_conf.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Internal storage ────────────────────── */

typedef struct {
    char *section;
    char *key;
    char *value;
} bf_conf_entry_t;

struct bf_conf {
    bf_conf_entry_t *entries;
    size_t           count;
    size_t           cap;
};

static char *strdup_safe(const char *s)
{
    if (!s) return NULL;
    size_t len = strlen(s);
    char *d = malloc(len + 1);
    if (d) memcpy(d, s, len + 1);
    return d;
}

/* ── Helpers ─────────────────────────────── */

static char *trim(char *s)
{
    while (*s && isspace((unsigned char)*s)) s++;
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1])) end--;
    *end = '\0';
    return s;
}

static char *unquote(char *s)
{
    size_t len = strlen(s);
    if (len >= 2 && ((s[0] == '"' && s[len-1] == '"') ||
                     (s[0] == '\'' && s[len-1] == '\''))) {
        s[len-1] = '\0';
        return s + 1;
    }
    return s;
}

/* Expand $VAR or ${VAR} in a value string */
static char *expand_env(const char *val)
{
    /* quick check: any $ present? */
    if (!strchr(val, '$')) return strdup_safe(val);

    size_t cap = strlen(val) * 2 + 64;
    char *buf = malloc(cap);
    if (!buf) return strdup_safe(val);

    size_t w = 0;
    const char *p = val;

    while (*p) {
        if (*p == '$') {
            p++;
            int braced = 0;
            if (*p == '{') { braced = 1; p++; }

            const char *name_start = p;
            while (*p && (isalnum((unsigned char)*p) || *p == '_')) p++;
            size_t name_len = (size_t)(p - name_start);

            if (braced && *p == '}') p++;

            char name_buf[256];
            if (name_len >= sizeof(name_buf)) name_len = sizeof(name_buf) - 1;
            memcpy(name_buf, name_start, name_len);
            name_buf[name_len] = '\0';

            const char *env_val = getenv(name_buf);
            if (env_val) {
                size_t elen = strlen(env_val);
                while (w + elen >= cap) { cap *= 2; buf = realloc(buf, cap); }
                memcpy(buf + w, env_val, elen);
                w += elen;
            }
        } else {
            if (w + 1 >= cap) { cap *= 2; buf = realloc(buf, cap); }
            buf[w++] = *p++;
        }
    }
    buf[w] = '\0';
    return buf;
}

/* ── Core operations ─────────────────────── */

bf_conf_t *bf_conf_new(void)
{
    bf_conf_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;
    c->cap = 64;
    c->entries = calloc(c->cap, sizeof(bf_conf_entry_t));
    if (!c->entries) { free(c); return NULL; }
    return c;
}

void bf_conf_free(bf_conf_t *cfg)
{
    if (!cfg) return;
    for (size_t i = 0; i < cfg->count; i++) {
        free(cfg->entries[i].section);
        free(cfg->entries[i].key);
        free(cfg->entries[i].value);
    }
    free(cfg->entries);
    free(cfg);
}

void bf_conf_set(bf_conf_t *cfg, const char *section, const char *key,
                 const char *value)
{
    if (!cfg || !key) return;
    const char *sec = section ? section : "";

    /* update existing */
    for (size_t i = 0; i < cfg->count; i++) {
        if (strcmp(cfg->entries[i].section, sec) == 0 &&
            strcmp(cfg->entries[i].key, key) == 0) {
            free(cfg->entries[i].value);
            cfg->entries[i].value = strdup_safe(value);
            return;
        }
    }

    /* append */
    if (cfg->count >= cfg->cap) {
        cfg->cap *= 2;
        cfg->entries = realloc(cfg->entries, cfg->cap * sizeof(bf_conf_entry_t));
    }
    bf_conf_entry_t *e = &cfg->entries[cfg->count++];
    e->section = strdup_safe(sec);
    e->key     = strdup_safe(key);
    e->value   = strdup_safe(value);
}

static const char *conf_get(const bf_conf_t *cfg, const char *section,
                            const char *key)
{
    if (!cfg || !key) return NULL;
    const char *sec = section ? section : "";
    for (size_t i = 0; i < cfg->count; i++) {
        if (strcmp(cfg->entries[i].section, sec) == 0 &&
            strcmp(cfg->entries[i].key, key) == 0)
            return cfg->entries[i].value;
    }
    return NULL;
}

/* ── Env fallback: BONFYRE_SECTION_KEY ────── */

static const char *env_fallback(const char *section, const char *key)
{
    char env_name[256];
    int n = 0;
    const char *prefix = "BONFYRE_";
    while (*prefix && n < 255) env_name[n++] = *prefix++;
    if (section && *section) {
        const char *s = section;
        while (*s && n < 255) {
            env_name[n++] = (char)toupper((unsigned char)*s);
            s++;
        }
        if (n < 255) env_name[n++] = '_';
    }
    const char *k = key;
    while (*k && n < 255) {
        env_name[n++] = (char)toupper((unsigned char)*k);
        k++;
    }
    env_name[n] = '\0';
    return getenv(env_name);
}

/* ── Typed getters ───────────────────────── */

const char *bf_conf_str(const bf_conf_t *cfg, const char *section,
                        const char *key, const char *def)
{
    const char *v = conf_get(cfg, section, key);
    if (v) return v;
    v = env_fallback(section, key);
    if (v) return v;
    return def;
}

int bf_conf_int(const bf_conf_t *cfg, const char *section,
                const char *key, int def)
{
    const char *v = bf_conf_str(cfg, section, key, NULL);
    if (!v) return def;
    char *end;
    long val = strtol(v, &end, 0);
    return (end != v) ? (int)val : def;
}

double bf_conf_double(const bf_conf_t *cfg, const char *section,
                      const char *key, double def)
{
    const char *v = bf_conf_str(cfg, section, key, NULL);
    if (!v) return def;
    char *end;
    double val = strtod(v, &end);
    return (end != v) ? val : def;
}

int bf_conf_bool(const bf_conf_t *cfg, const char *section,
                 const char *key, int def)
{
    const char *v = bf_conf_str(cfg, section, key, NULL);
    if (!v) return def;
    if (strcmp(v, "true") == 0 || strcmp(v, "yes") == 0 ||
        strcmp(v, "1") == 0 || strcmp(v, "on") == 0)
        return 1;
    if (strcmp(v, "false") == 0 || strcmp(v, "no") == 0 ||
        strcmp(v, "0") == 0 || strcmp(v, "off") == 0)
        return 0;
    return def;
}

/* ── File parser ─────────────────────────── */

static void parse_file(bf_conf_t *cfg, const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) return;

    char line[4096];
    char current_section[256] = "";

    while (fgets(line, (int)sizeof(line), f)) {
        char *s = trim(line);

        /* skip empty lines and comments */
        if (!*s || *s == '#' || *s == ';') continue;

        /* section header */
        if (*s == '[') {
            char *end = strchr(s, ']');
            if (end) {
                *end = '\0';
                char *sec_name = trim(s + 1);
                size_t slen = strlen(sec_name);
                if (slen >= sizeof(current_section))
                    slen = sizeof(current_section) - 1;
                memcpy(current_section, sec_name, slen);
                current_section[slen] = '\0';
            }
            continue;
        }

        /* key = value */
        char *eq = strchr(s, '=');
        if (!eq) continue;

        *eq = '\0';
        char *key = trim(s);
        char *val = trim(eq + 1);

        /* strip inline comment */
        char *comment = NULL;
        int in_quote = 0;
        for (char *c = val; *c; c++) {
            if (*c == '"' || *c == '\'') in_quote = !in_quote;
            if (!in_quote && (*c == '#' || *c == ';')) { comment = c; break; }
        }
        if (comment) { *comment = '\0'; val = trim(val); }

        val = unquote(val);
        char *expanded = expand_env(val);
        bf_conf_set(cfg, current_section, key, expanded);
        free(expanded);
    }
    fclose(f);
}

/* ── Load with search paths ──────────────── */

bf_conf_t *bf_conf_load_file(const char *path)
{
    bf_conf_t *cfg = bf_conf_new();
    if (!cfg) return NULL;
    parse_file(cfg, path);
    return cfg;
}

bf_conf_t *bf_conf_load(const char *name)
{
    bf_conf_t *cfg = bf_conf_new();
    if (!cfg) return NULL;

    char path[1024];

    /* 1. System: /etc/bonfyre/<name>.conf */
    snprintf(path, sizeof(path), "/etc/bonfyre/%s.conf", name);
    parse_file(cfg, path);

    /* 2. User: ~/.config/bonfyre/<name>.conf */
    const char *home = getenv("HOME");
    if (home) {
        snprintf(path, sizeof(path), "%s/.config/bonfyre/%s.conf", home, name);
        parse_file(cfg, path);
    }

    /* 3. Local: ./<name>.conf */
    snprintf(path, sizeof(path), "%s.conf", name);
    parse_file(cfg, path);

    return cfg;
}

/* ── Enumeration ─────────────────────────── */

void bf_conf_each(const bf_conf_t *cfg, bf_conf_visitor_fn fn, void *userdata)
{
    if (!cfg || !fn) return;
    for (size_t i = 0; i < cfg->count; i++) {
        if (fn(cfg->entries[i].section, cfg->entries[i].key,
               cfg->entries[i].value, userdata) != 0)
            break;
    }
}

/* ── Save ────────────────────────────────── */

int bf_conf_save(const bf_conf_t *cfg, const char *path)
{
    if (!cfg || !path) return -1;
    FILE *f = fopen(path, "w");
    if (!f) return -1;

    const char *last_section = "";
    for (size_t i = 0; i < cfg->count; i++) {
        const char *sec = cfg->entries[i].section;
        if (strcmp(sec, last_section) != 0) {
            if (i > 0) fprintf(f, "\n");
            if (sec[0]) fprintf(f, "[%s]\n", sec);
            last_section = sec;
        }
        fprintf(f, "%s = %s\n", cfg->entries[i].key, cfg->entries[i].value);
    }

    fclose(f);
    return 0;
}
