#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include <curl/curl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct Domain {
    const char *name;
    const char *secret_name;
} Domain;

static const Domain DOMAINS[] = {
    {"aurekai.life", "AUREKAI_LIFE_DNS_PASSWORD"},
    {"aurekai.live", "AUREKAI_LIVE_DNS_PASSWORD"},
    {"aurekai.shop", "AUREKAI_SHOP_DNS_PASSWORD"},
    {"aurekai.work", "AUREKAI_WORK_DNS_PASSWORD"},
    {"theleaflounge.store", "THE_LEAF_LOUNGE_STORE_DNS_PASSWORD"},
    {NULL, NULL}
};

typedef struct Secret {
    char name[128];
    char value[512];
} Secret;

typedef struct SecretSet {
    Secret values[16];
    size_t count;
} SecretSet;

static const char *option(int argc, char **argv, const char *name) {
    int i;
    for (i = 2; i + 1 < argc; ++i) if (strcmp(argv[i], name) == 0) return argv[i + 1];
    return NULL;
}

static int has_flag(int argc, char **argv, const char *name) {
    int i;
    for (i = 2; i < argc; ++i) if (strcmp(argv[i], name) == 0) return 1;
    return 0;
}

static int is_ip(const char *value) {
    struct in_addr address;
    return value && inet_pton(AF_INET, value, &address) == 1;
}

static int parked(const char *ip) {
    return !strncmp(ip, "162.255.119.", 12) || !strncmp(ip, "192.64.119.", 11) || !strncmp(ip, "198.54.117.", 11);
}

static int resolve_a(const char *hostname, char out[INET_ADDRSTRLEN]) {
    struct addrinfo hints, *result = NULL, *item;
    int rc;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    rc = getaddrinfo(hostname, NULL, &hints, &result);
    if (rc != 0) return 0;
    for (item = result; item; item = item->ai_next) {
        if (item->ai_family == AF_INET && inet_ntop(AF_INET, &((struct sockaddr_in *)item->ai_addr)->sin_addr, out, INET_ADDRSTRLEN)) {
            freeaddrinfo(result);
            return 1;
        }
    }
    freeaddrinfo(result);
    return 0;
}

static void trim(char *value) {
    size_t length;
    while (*value == ' ' || *value == '\t') memmove(value, value + 1, strlen(value));
    length = strlen(value);
    while (length && (value[length - 1] == '\n' || value[length - 1] == '\r' || value[length - 1] == ' ' || value[length - 1] == '\t')) value[--length] = '\0';
    if (value[0] == '\'' || value[0] == '\"') {
        memmove(value, value + 1, strlen(value));
        length = strlen(value);
        if (length && (value[length - 1] == '\'' || value[length - 1] == '\"')) value[length - 1] = '\0';
    }
}

static int load_secrets(const char *path, SecretSet *out) {
    FILE *file; char line[1024];
    memset(out, 0, sizeof(*out));
    if (!path) return 0;
    file = fopen(path, "r");
    if (!file) return -1;
    while (fgets(line, sizeof(line), file) && out->count < sizeof(out->values) / sizeof(out->values[0])) {
        char *equals, *name = line, *value;
        trim(name);
        if (!strncmp(name, "export ", 7)) name += 7;
        if (!*name || *name == '#') continue;
        equals = strchr(name, '=');
        if (!equals) continue;
        *equals = '\0'; value = equals + 1; trim(name); trim(value);
        if (!*name || !*value) continue;
        snprintf(out->values[out->count].name, sizeof(out->values[out->count].name), "%s", name);
        snprintf(out->values[out->count].value, sizeof(out->values[out->count].value), "%s", value);
        out->count++;
    }
    fclose(file);
    return 0;
}

static const char *secret(const SecretSet *set, const char *name) {
    size_t i;
    for (i = 0; i < set->count; ++i) if (!strcmp(set->values[i].name, name)) return set->values[i].value;
    return NULL;
}

typedef struct Response { char body[1024]; size_t used; } Response;
static size_t receive(void *data, size_t size, size_t count, void *context) {
    Response *response = context; size_t bytes = size * count, remain = sizeof(response->body) - response->used - 1;
    if (bytes > remain) bytes = remain;
    memcpy(response->body + response->used, data, bytes); response->used += bytes; response->body[response->used] = '\0';
    return size * count;
}

static int update(const Domain *domain, const char *record, const char *password, const char *ip, char diagnostic[160]) {
    CURL *curl; char *encoded_host, *encoded_domain, *encoded_password, *encoded_ip, url[2048]; Response response = {{0}, 0}; CURLcode status;
    diagnostic[0] = '\0';
    curl = curl_easy_init(); if (!curl) return 0;
    encoded_host = curl_easy_escape(curl, record, 0); encoded_domain = curl_easy_escape(curl, domain->name, 0); encoded_password = curl_easy_escape(curl, password, 0); encoded_ip = curl_easy_escape(curl, ip, 0);
    if (!encoded_host || !encoded_domain || !encoded_password || !encoded_ip || snprintf(url, sizeof(url), "https://dynamicdns.park-your-domain.com/update?host=%s&domain=%s&password=%s&ip=%s", encoded_host, encoded_domain, encoded_password, encoded_ip) >= (int)sizeof(url)) { snprintf(diagnostic,160,"request_construction_failed"); curl_free(encoded_host); curl_free(encoded_domain); curl_free(encoded_password); curl_free(encoded_ip); curl_easy_cleanup(curl); return 0; }
    curl_easy_setopt(curl, CURLOPT_URL, url); curl_easy_setopt(curl, CURLOPT_TIMEOUT, 25L); curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, receive); curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response); status = curl_easy_perform(curl);
    curl_free(encoded_host); curl_free(encoded_domain); curl_free(encoded_password); curl_free(encoded_ip); curl_easy_cleanup(curl);
    if (status != CURLE_OK) { snprintf(diagnostic,160,"transport_%s",curl_easy_strerror(status)); return 0; }
    if (strstr(response.body, "<ErrCount>0</ErrCount>") != NULL) return 1;
    { const char *error=strstr(response.body,"<Err1>"), *end=error ? strstr(error,"</Err1>") : NULL; size_t i;
      if(error && end && end>error+6) snprintf(diagnostic,160,"provider_%.*s",(int)(end-(error+6)),error+6); else snprintf(diagnostic,160,"%.140s",response.body[0] ? response.body : "provider_empty_response");
      for(i=0;diagnostic[i];++i) if(diagnostic[i]=='\r'||diagnostic[i]=='\n'||diagnostic[i]=='\t')diagnostic[i]=' '; }
    return 0;
}

static void usage(void) {
    fputs("usage: bonfyre-domain status [--only DOMAIN] [--secrets FILE]\n"
          "       bonfyre-domain plan --ip IPV4 [--host @] [--only DOMAIN] --secrets FILE\n"
          "       bonfyre-domain apply --ip IPV4 [--host @] [--only DOMAIN] --secrets FILE --apply\n", stderr);
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    const char *verb, *only, *ip, *record, *secrets_path; SecretSet secrets; int applying, any = 0, failures = 0; size_t i;
    if (argc < 2) { usage(); return 2; }
    verb = argv[1]; only = option(argc, argv, "--only"); ip = option(argc, argv, "--ip"); record = option(argc, argv, "--host"); secrets_path = option(argc, argv, "--secrets"); applying = !strcmp(verb, "apply");
    if (strcmp(verb, "status") && strcmp(verb, "plan") && strcmp(verb, "apply")) { usage(); return 2; }
    if ((!strcmp(verb, "plan") || applying) && !is_ip(ip)) { fputs("bonfyre-domain: --ip must be one IPv4 address\n", stderr); return 2; }
    if (applying && !has_flag(argc, argv, "--apply")) { fputs("bonfyre-domain: apply requires explicit --apply\n", stderr); return 2; }
    if (load_secrets(secrets_path, &secrets) < 0) { fprintf(stderr, "bonfyre-domain: cannot read declared secrets file: %s\n", secrets_path ? secrets_path : "(missing --secrets)"); return 2; }
    for (i = 0; DOMAINS[i].name; ++i) {
        const Domain *domain = &DOMAINS[i]; const char *password; char current[INET_ADDRSTRLEN] = "", observed[512];
        if (only && strcmp(only, domain->name)) continue;
        any = 1; password = secret(&secrets, domain->secret_name);
        if (record && strcmp(record, "@")) snprintf(observed, sizeof(observed), "bonfyre-url-probe.%s", domain->name); else snprintf(observed, sizeof(observed), "%s", domain->name);
        resolve_a(observed, current);
        printf("%s current=%s state=%s secret=%s", domain->name, current[0] ? current : "unresolved", current[0] && parked(current) ? "parked" : "live", password ? "present" : "absent");
        if (!ip) { puts(" action=inspect"); continue; }
        if (!password) { puts(" action=blocked_missing_secret"); failures++; continue; }
        if (!strcmp(current, ip)) { puts(" action=already_correct"); continue; }
        if (!applying) { printf(" action=would_set_%s\n", ip); continue; }
        { char diagnostic[160]; if (update(domain, record ? record : "@", password, ip, diagnostic)) printf(" action=updated_%s\n", ip); else { printf(" action=update_failed diagnostic=%s\n", diagnostic); failures++; } }
    }
    if (!any) { fprintf(stderr, "bonfyre-domain: unknown owned domain: %s\n", only ? only : ""); return 2; }
    return failures ? 1 : 0;
}
