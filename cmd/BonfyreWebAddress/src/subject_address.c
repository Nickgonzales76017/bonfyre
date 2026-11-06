#include "subject_address.h"

#include <ctype.h>
#include <stdio.h>
#include <string.h>

const char *territory_of(const char *host) {
    char normalized[256];
    if (!host) return NULL;
    if (!lower_host(host, normalized)) return "independent";
    host = normalized;
    if (!strcmp(host,"aurekai.life") || (strlen(host)>13 && !strcmp(host+strlen(host)-13,".aurekai.life"))) return "life";
    if (!strcmp(host,"aurekai.live") || (strlen(host)>13 && !strcmp(host+strlen(host)-13,".aurekai.live"))) return "live";
    if (!strcmp(host,"aurekai.shop") || (strlen(host)>13 && !strcmp(host+strlen(host)-13,".aurekai.shop"))) return "shop";
    if (!strcmp(host,"aurekai.work") || (strlen(host)>13 && !strcmp(host+strlen(host)-13,".aurekai.work"))) return "work";
    return "independent";
}

int lower_host(const char *in, char out[256]) {
    size_t n = 0;
    if (!in) return 0;
    while (in[n] && in[n] != ':' && n < 255) {
        unsigned char c = (unsigned char)in[n];
        if (!(isalnum(c) || c == '.' || c == '-')) return 0;
        out[n] = (char)tolower(c);
        n++;
    }
    if (n == 255 && in[n] && in[n] != ':') return 0;
    if (in[n] == ':') {
        const char *port = in + n + 1;
        unsigned value = 0;
        if (!*port) return 0;
        for (; *port; port++) {
            unsigned digit;
            if (!isdigit((unsigned char)*port)) return 0;
            digit = (unsigned)(*port - '0');
            if (value > 6553 || (value == 6553 && digit > 5)) return 0;
            value = value * 10 + digit;
        }
        if (!value) return 0;
    }
    out[n] = '\0';
    return n > 0;
}

int page_subject_for_host(sqlite3 *cms, const char *host,
                          char *subject, size_t capacity) {
    char normalized[256], apex[64], www[64];
    const char *territory;
    sqlite3_stmt *st = NULL;
    if (!subject || !capacity) return 0;
    subject[0] = '\0';
    if (!lower_host(host, normalized)) return 0;
    territory = territory_of(normalized);
    if (territory && strcmp(territory, "independent")) {
        snprintf(apex, sizeof(apex), "aurekai.%s", territory);
        snprintf(www, sizeof(www), "www.aurekai.%s", territory);
        if (!strcmp(normalized, apex) || !strcmp(normalized, www))
            return snprintf(subject, capacity, "institution:aurekai:%s", territory) < (int)capacity;
    }
    if (!strcmp(normalized, "internet.aurekai.work"))
        return snprintf(subject, capacity, "institution:aurekai:internet") < (int)capacity;
    if (!cms || sqlite3_prepare_v2(cms,
        "SELECT subject_address FROM site_presentation WHERE host=?1 AND status='published' "
        "AND COALESCE(subject_address,'')<>'' ORDER BY id DESC LIMIT 1",
        -1, &st, NULL) != SQLITE_OK) return 0;
    sqlite3_bind_text(st, 1, normalized, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) {
        int n = snprintf(subject, capacity, "%s", (const char *)sqlite3_column_text(st, 0));
        if (n < 0 || (size_t)n >= capacity) subject[0] = '\0';
    }
    sqlite3_finalize(st);
    return subject[0] != '\0';
}
