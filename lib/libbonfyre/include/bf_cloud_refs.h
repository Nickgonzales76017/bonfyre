/*
 * bf_cloud_refs.h — how an organ gets a cloud provider's account id or token.
 *
 * One answer for every organ, so none carries its own lookup:
 *
 *   1. the environment: BONFYRE_<PROVIDER>_<FIELD>, then <PROVIDER>_<FIELD>
 *   2. the estate's reference file (BONFYRE_CLOUD_REFS, else
 *      ~/.bonfyre/secrets/cloud.refs.env), reading <PROVIDER>_<account>_<field>
 *      for the account the estate has chosen for that provider, then
 *      <PROVIDER>_<FIELD>
 *
 * The chosen account is BONFYRE_<PROVIDER>_ACCOUNT, else the provider's line in
 * ~/.bonfyre/estate-local/cloud-accounts.tsv ("cloudflare<TAB>proton").
 *
 * The value is written to the caller's buffer and nowhere else.  Callers pass
 * it to a child through a file or the environment, never on a command line,
 * and never print it.
 */
#ifndef BF_CLOUD_REFS_H
#define BF_CLOUD_REFS_H

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

static inline void bf_cloud_upper(char *text) {
    for (; *text; text++) *text = (char)toupper((unsigned char)*text);
}

static inline int bf_cloud_account(const char *provider, char *out, size_t cap) {
    char name[128], path[1024], line[256];
    const char *value, *home = getenv("HOME");
    FILE *f;
    snprintf(name, sizeof(name), "BONFYRE_%s_ACCOUNT", provider);
    bf_cloud_upper(name);
    value = getenv(name);
    if (value && *value) { snprintf(out, cap, "%s", value); return 1; }
    snprintf(path, sizeof(path), "%s/.bonfyre/estate-local/cloud-accounts.tsv", home ? home : "");
    f = fopen(path, "r");
    if (!f) return 0;
    while (fgets(line, sizeof(line), f)) {
        char *tab = strchr(line, '\t');
        if (!tab) continue;
        *tab++ = '\0';
        tab[strcspn(tab, "\r\n\t ")] = '\0';
        if (!strcasecmp(line, provider) && *tab) { snprintf(out, cap, "%s", tab); fclose(f); return 1; }
    }
    fclose(f);
    return 0;
}

/* Finds NAME in a file of `export NAME=value` or `NAME=value` lines. */
static inline int bf_cloud_ref_in_file(const char *path, const char *name, char *out, size_t cap) {
    char line[4096];
    size_t name_len = strlen(name);
    FILE *f = fopen(path, "r");
    int found = 0;
    if (!f) return 0;
    while (!found && fgets(line, sizeof(line), f)) {
        char *p = line;
        size_t len;
        while (*p == ' ' || *p == '\t') p++;
        if (!strncmp(p, "export ", 7)) p += 7;
        if (strncasecmp(p, name, name_len) || p[name_len] != '=') continue;
        p += name_len + 1;
        p[strcspn(p, "\r\n")] = '\0';
        len = strlen(p);
        if (len >= 2 && (p[0] == '"' || p[0] == '\'') && p[len - 1] == p[0]) { p[len - 1] = '\0'; p++; }
        if (*p && strlen(p) < cap) { snprintf(out, cap, "%s", p); found = 1; }
    }
    fclose(f);
    memset(line, 0, sizeof(line));
    return found;
}

/* Returns 1 and fills out when the provider's field is known. */
static inline int bf_cloud_ref(const char *provider, const char *field, char *out, size_t cap) {
    char name[256], account[64], path[1024];
    const char *value, *refs = getenv("BONFYRE_CLOUD_REFS"), *home = getenv("HOME");
    snprintf(name, sizeof(name), "BONFYRE_%s_%s", provider, field);
    bf_cloud_upper(name);
    value = getenv(name);
    if (!value || !*value) value = getenv(name + 8);
    if (value && *value && strlen(value) < cap) { snprintf(out, cap, "%s", value); return 1; }
    if (refs && *refs) snprintf(path, sizeof(path), "%s", refs);
    else snprintf(path, sizeof(path), "%s/.bonfyre/secrets/cloud.refs.env", home ? home : "");
    if (bf_cloud_account(provider, account, sizeof(account))) {
        snprintf(name, sizeof(name), "%s_%s_%s", provider, account, field);
        if (bf_cloud_ref_in_file(path, name, out, cap)) return 1;
        /* A chosen account that the file does not hold is not silently
         * replaced by another account's credential. */
        return 0;
    }
    snprintf(name, sizeof(name), "%s_%s", provider, field);
    return bf_cloud_ref_in_file(path, name, out, cap);
}

#endif
