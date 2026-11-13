#ifndef BONFYRE_AUTH_CONTINUATION_H
#define BONFYRE_AUTH_CONTINUATION_H
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "tenant_host_binding.h"
#include "compat/aurekai_legacy.h"

/* Authentication carries an address, never draft data or authority. */
#define BF_AUTH_NEXT_MAX 4096
static int bf_auth_hex(unsigned char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
/* 0: absent, 1: one complete field, -1: malformed, duplicate or oversized. */
static int bf_auth_next_field(const char *query, char *out, size_t cap) {
    const char *p = query; int found = 0;
    if (!out || cap < 1) return -1;
    out[0] = '\0';
    if (!p) return 0;
    if (*p == '?') p++;
    while (*p) {
        const char *end = strchr(p, '&'), *eq; size_t used = 0;
        if (!end) end = p + strlen(p);
        eq = memchr(p, '=', (size_t)(end - p));
        if ((eq && eq - p == 4 && !memcmp(p, "next", 4)) ||
            (!eq && end - p == 4 && !memcmp(p, "next", 4))) {
            const char *v;
            if (found++ || !eq) { out[0] = '\0'; return -1; }
            for (v = eq + 1; v < end; v++) {
                unsigned char c = (unsigned char)*v;
                if (c == '%') {
                    int a, b;
                    if (end - v < 3 || (a = bf_auth_hex((unsigned char)v[1])) < 0 ||
                        (b = bf_auth_hex((unsigned char)v[2])) < 0) { out[0] = '\0'; return -1; }
                    c = (unsigned char)((a << 4) | b); v += 2;
                } else if (c == '+') c = ' ';
                if (!c || used + 1 >= cap) { out[0] = '\0'; return -1; }
                out[used++] = (char)c;
            }
            out[used] = '\0';
        }
        p = *end ? end + 1 : end;
    }
    return found ? 1 : 0;
}
static int bf_auth_local_path(const char *value) {
    const unsigned char *p = (const unsigned char *)value;
    int in_path = 1, segment_length = 0, segment_dots = 0;
    if (!p || p[0] != '/' || p[1] == '/' || p[1] == '\\') return 0;
    for (p++; *p; p++) {
        unsigned char c = *p; int encoded = 0;
        if (c == '?' || c == '#') in_path = 0;
        if (c == '%') {
            int a, b;
            if (!p[1] || !p[2] || (a = bf_auth_hex(p[1])) < 0 || (b = bf_auth_hex(p[2])) < 0) return 0;
            c = (unsigned char)((a << 4) | b); p += 2; encoded = 1;
        }
        if (c < 0x20 || (c == 0x20 && !encoded) || c == 0x7f || c == '\\' || c == '"' || c == '<' || c == '>') return 0;
        if (!in_path) continue;
        if (encoded && (c == '/' || c == '%' || c == '?' || c == '#')) return 0;
        if (c == '/') {
            if (segment_length && segment_length <= 2 && segment_length == segment_dots) return 0;
            segment_length = segment_dots = 0;
        } else { segment_length++; if (c == '.') segment_dots++; }
    }
    return !(segment_length && segment_length <= 2 && segment_length == segment_dots);
}
/* Historical cross-host client redirects are a legacy Aurekai adapter.
 * An explicitly installed tenant never inherits their allowlist. */
static int bf_auth_work_host(const char *raw) {
    char host[256]; size_t n=0;
    if(!raw)return 0;
    while(raw[n]&&raw[n]!=':'&&n+1<sizeof(host)){
        host[n]=(char)tolower((unsigned char)raw[n]);n++;
    }
    host[n]='\0';
    if(raw[n]&&raw[n]!=':')return 0;
    if(bf_tenant_hosts_status()!=0)return 0;
    return bf_legacy_aurekai_work_host(host);
}
static int bf_auth_return_address(const char *candidate, const char *host, char *out, size_t cap) {
    const char *path=candidate;
    size_t n=candidate?strlen(candidate):0;
    if(!out||cap<sizeof("/account"))return 0;
    snprintf(out,cap,"/account");
    if(!n||n>=cap||n>=BF_AUTH_NEXT_MAX)return 0;
    int state=bf_tenant_hosts_status();
    if(state<0)return 0; /* Invalid config never falls back to old tenant. */
    if(state==1){
        if(!bf_tenant_territory_for_host(host)||candidate[0]!='/'||
           !bf_auth_local_path(candidate))return 0;
        memcpy(out,candidate,n+1);
        return 1;
    }
    if(!strncmp(candidate,"https://",8)&&bf_auth_work_host(host)){
        const char *end=candidate+8;char target[256];size_t size;
        while(*end&&*end!='/'&&*end!='?'&&*end!='#')end++;
        size=(size_t)(end-candidate-8);
        if(!size||size>=sizeof(target))return 0;
        for(size_t i=0;i<size;i++)target[i]=(char)tolower((unsigned char)candidate[i+8]);
        target[size]='\0';
        if(!bf_legacy_aurekai_redirect_destination(target)||(*end&&*end!='/'))return 0;
        path=*end?end:"/";
    }else if(candidate[0]!='/')return 0;
    if(!bf_auth_local_path(path))return 0;
    memcpy(out,candidate,n+1);
    return 1;
}
static void bf_auth_html(FILE *out, const char *s) {
    for (; s && *s; s++) switch (*s) {
        case '&': fputs("&amp;", out); break;
        case '<': fputs("&lt;", out); break;
        case '>': fputs("&gt;", out); break;
        case '"': fputs("&quot;", out); break;
        default: fputc((unsigned char)*s, out);
    }
}
static void bf_auth_query(FILE *out, const char *s) {
    for (; s && *s; s++) {
        unsigned char c = (unsigned char)*s;
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') fputc(c, out);
        else fprintf(out, "%%%02X", c);
    }
}
/* Lower the existing authored form through the native redirect owner. Its
 * appearance and input contract stay intact; no second browser login parser. */
static int bf_auth_form_document(FILE *out, const char *source, const char *next,
                                 const char *notice, const char *bar) {
    const char *p = source; int forms = 0;
    if (!out || !source || !next) return 0;
    while (*p) {
        if (!strncmp(p, "<script>", 8)) {
            const char *end = strstr(p + 8, "</script>");
            if (!end) return 0;
            p = end + 9; continue;
        }
        if (!strncmp(p, "<body>", 6)) { fputs("<body>", out); if (bar) fputs(bar, out); p += 6; continue; }
        if (!strncmp(p, "<form ", 6)) {
            const char *end = strchr(p, '>'), *q;
            if (!end || forms++) return 0;
            for (q = p; q <= end; q++) {
                if (end - q >= 10 && !strncmp(q, " novalidate", 11)) q += 10;
                else fputc((unsigned char)*q, out);
            }
            fputs("<input type=\"hidden\" name=\"next\" value=\"", out); bf_auth_html(out, next); fputs("\">", out);
            p = end + 1; continue;
        }
        if (!strncmp(p, "href=\"/login\"", 13) || !strncmp(p, "href=\"/signup\"", 14)) {
            int signup = !strncmp(p, "href=\"/signup\"", 14);
            fputs(signup ? "href=\"/signup?next=" : "href=\"/login?next=", out);
            bf_auth_query(out, next); fputc('"', out); p += signup ? 14 : 13; continue;
        }
        if (!strncmp(p, "<p id=\"msg\" class=\"msg\" hidden></p>", 35)) {
            if (notice && *notice) { fputs("<p id=\"msg\" class=\"msg\" role=\"status\">", out); bf_auth_html(out, notice); fputs("</p>", out); }
            else fputs("<p id=\"msg\" class=\"msg\" hidden></p>", out);
            p += 35; continue;
        }
        fputc((unsigned char)*p++, out);
    }
    return forms == 1 && !ferror(out);
}
#endif
