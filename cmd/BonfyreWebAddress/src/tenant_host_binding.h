#ifndef BONFYRE_TENANT_HOST_BINDING_H
#define BONFYRE_TENANT_HOST_BINDING_H
/*
 * Deployment-owned territory binding. The engine reads an exact host map;
 * it neither owns the tenant nor invents a parent institution from a domain.
 *
 * BONFYRE_TENANT_HOSTS_FILE points to a versioned, operator-owned TSV:
 *   territory apex_host account_host
 *
 * No implicit suffix grants: only the two declared exact hosts are resolved.
 * A missing file path means "not configured" (legacy callers may opt into
 * their explicit compatibility adapter). An invalid configured path refuses.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BF_TENANT_HOST_MAX 16
typedef struct {
    char territory[33];
    char public_host[256];
    char account_host[256];
} BfTenantHostBinding;

static BfTenantHostBinding bf_tenant_hosts[BF_TENANT_HOST_MAX];
static size_t bf_tenant_host_count;
static int bf_tenant_hosts_initialized;
static int bf_tenant_hosts_state; /* 0 absent; 1 valid; -1 invalid */

static inline int bf_tenant_host_valid(const char *host) {
    size_t n = host ? strlen(host) : 0, label = 0;
    if (n < 3 || n > 253 || host[0] == '.' || host[n - 1] == '.') return 0;
    int dot = 0;
    for (size_t i = 0; i < n; i++) {
        char c = host[i];
        if (c == '.') {
            if (!label || label > 63 || host[i - 1] == '-') return 0;
            label = 0; dot = 1;
        } else {
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) return 0;
            if (!label && c == '-') return 0;
            label++;
        }
    }
    return dot && label && label <= 63 && host[n - 1] != '-';
}

static inline int bf_tenant_territory_valid(const char *territory) {
    size_t n = territory ? strlen(territory) : 0;
    if (!n || n > 32) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = territory[i];
        if (!((c >= 'a' && c <= 'z') || (i && ((c >= '0' && c <= '9') || c == '-'))))
            return 0;
    }
    return 1;
}

/* Explicit reload supports deterministic integration tests and release cuts. */
static inline int bf_tenant_hosts_load(const char *path) {
    BfTenantHostBinding candidate[BF_TENANT_HOST_MAX];
    size_t count = 0;
    FILE *file = NULL;
    char line[768];
    int state = -1;
    memset(candidate, 0, sizeof(candidate));
    memset(bf_tenant_hosts, 0, sizeof(bf_tenant_hosts));
    bf_tenant_host_count = 0;
    bf_tenant_hosts_initialized = 1;
    if (!path || !*path) { bf_tenant_hosts_state = 0; return 0; }
    file = fopen(path, "rb");
    if (!file) goto done;
    while (fgets(line, sizeof(line), file)) {
        char key[33] = "", public_host[256] = "", account_host[256] = "", extra = 0;
        size_t len = strlen(line);
        if (!len || (len == sizeof(line) - 1 && line[len - 1] != '\n')) goto done;
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') continue;
        if (sscanf(line, " %32s %255s %255s %c", key, public_host, account_host, &extra) != 3 ||
            !bf_tenant_territory_valid(key) || !bf_tenant_host_valid(public_host) ||
            !bf_tenant_host_valid(account_host) || !strcmp(public_host, account_host) ||
            count == BF_TENANT_HOST_MAX) goto done;
        for (size_t i = 0; i < count; i++) {
            if (!strcmp(key, candidate[i].territory) ||
                !strcmp(public_host, candidate[i].public_host) ||
                !strcmp(public_host, candidate[i].account_host) ||
                !strcmp(account_host, candidate[i].public_host) ||
                !strcmp(account_host, candidate[i].account_host)) goto done;
        }
        snprintf(candidate[count].territory, sizeof(candidate[count].territory), "%s", key);
        snprintf(candidate[count].public_host, sizeof(candidate[count].public_host), "%s", public_host);
        snprintf(candidate[count].account_host, sizeof(candidate[count].account_host), "%s", account_host);
        count++;
    }
    if (ferror(file) || !count) goto done;
    memcpy(bf_tenant_hosts, candidate, sizeof(candidate));
    bf_tenant_host_count = count;
    state = 1;
done:
    if (file) fclose(file);
    bf_tenant_hosts_state = state;
    return state;
}

static inline int bf_tenant_hosts_status(void) {
    if (!bf_tenant_hosts_initialized)
        (void)bf_tenant_hosts_load(getenv("BONFYRE_TENANT_HOSTS_FILE"));
    return bf_tenant_hosts_state;
}

/* Never use a guessed suffix to select a tenant or an authorization origin. */
static inline const char *bf_tenant_public_host(const char *territory) {
    if (!territory || bf_tenant_hosts_status() != 1) return NULL;
    for (size_t i = 0; i < bf_tenant_host_count; i++)
        if (!strcmp(bf_tenant_hosts[i].territory, territory))
            return bf_tenant_hosts[i].public_host;
    return NULL;
}
static inline const char *bf_tenant_account_host(const char *territory) {
    if (!territory || bf_tenant_hosts_status() != 1) return NULL;
    for (size_t i = 0; i < bf_tenant_host_count; i++)
        if (!strcmp(bf_tenant_hosts[i].territory, territory))
            return bf_tenant_hosts[i].account_host;
    return NULL;
}
static inline const char *bf_tenant_territory_for_host(const char *host) {
    if (!host || bf_tenant_hosts_status() != 1) return NULL;
    for (size_t i = 0; i < bf_tenant_host_count; i++)
        if (!strcmp(host, bf_tenant_hosts[i].public_host) ||
            !strcmp(host, bf_tenant_hosts[i].account_host))
            return bf_tenant_hosts[i].territory;
    return NULL;
}
static inline int bf_tenant_is_account_host(const char *host) {
    if (!host || bf_tenant_hosts_status() != 1) return 0;
    for (size_t i = 0; i < bf_tenant_host_count; i++)
        if (!strcmp(host, bf_tenant_hosts[i].account_host)) return 1;
    return 0;
}
static inline int bf_tenant_is_declared_subhost(const char *host, const char *territory) {
    const char *base = bf_tenant_public_host(territory);
    size_t n = host ? strlen(host) : 0, m = base ? strlen(base) : 0;
    return bf_tenant_host_valid(host) && n > m + 1 &&
           host[n - m - 1] == '.' && !strcmp(host + n - m, base);
}
/* Display eligibility only: a retained published site still needs exact
 * subject/source review and must not inherit account/login host authority. */
static inline int bf_tenant_is_member_site_host(const char *host) {
    if (!host || !bf_tenant_host_valid(host) || bf_tenant_hosts_status()!=1 ||
        bf_tenant_is_account_host(host)) return 0;
    for(size_t i=0;i<bf_tenant_host_count;i++) {
        if(bf_tenant_is_declared_subhost(host,bf_tenant_hosts[i].territory))
            return 1;
    }
    return 0;
}
#endif
