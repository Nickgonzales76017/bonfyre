#ifndef BONFYRE_CAMPAIGN_WORKSPACE_H
#define BONFYRE_CAMPAIGN_WORKSPACE_H

#include "../../BonfyreCampaign/src/campaign.h"
#include "bf_lex.h"
#include "bf_utf8.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

/* The caller must first verify the current Auth grant, workspace binding,
 * campaign pack/entry and ac_verified. This helper neither grants access nor
 * retains records. body and raw are NUL-terminated text from the HTTP reader.
 * aw_header, aw_error and aw_response are supplied by application_workspace.h. */
#define CWL_HEADER_MAX 16384
#define CWL_HTML_MAX (128u * 1024u)
typedef struct {
    char url[BF_CAMPAIGN_URL_MAX + 1];
    char values[6][BF_CAMPAIGN_VALUE_MAX + 1];
} CwlForm;
static const char *const cwl_names[] = {
    "url", "utm_source", "utm_medium", "utm_campaign", "utm_id", "utm_term", "utm_content"
};
static const char *const cwl_labels[] = {
    "Destination URL", "Source", "Medium", "Campaign", "Campaign ID", "Term", "Content"
};
static const char *const cwl_hints[] = {
    "The public page this link should open. Existing query fields and the fragment are kept.",
    "Where the link will appear, such as your newsletter or a partner site.",
    "The channel, such as email, referral or paid_social.",
    "Use the same name for every link in this campaign.",
    "An optional campaign identifier from your campaign records.",
    "An optional keyword or search term.",
    "An optional label that distinguishes this creative or link placement."
};
static char *cwl_field(CwlForm *form, int field) { return field ? form->values[field - 1] : form->url; }
static size_t cwl_capacity(int field) { return field ? BF_CAMPAIGN_VALUE_MAX + 1 : BF_CAMPAIGN_URL_MAX + 1; }
static int cwl_clean(const char *value, size_t length) {
    if (!bf_utf8_valid(value, length, NULL)) return 0;
    for (size_t i = 0; i < length; i++) if ((unsigned char)value[i] < 32 || (unsigned char)value[i] == 127) return 0;
    return 1;
}
static int cwl_decode(const char *value, size_t length, char *out, size_t capacity) {
    char packet[BF_CAMPAIGN_REQUEST_MAX + 3];
    if (length > BF_CAMPAIGN_REQUEST_MAX) return 0;
    memcpy(packet, "v=", 2); memcpy(packet + 2, value, length); packet[length + 2] = 0;
    return bf_lex_form_query(packet, "v", out, capacity) == 1 && cwl_clean(out, strlen(out));
}
static int cwl_form(const char *body, CwlForm *form) {
    unsigned seen = 0;
    memset(form, 0, sizeof(*form));
    for (const char *p = body; *p;) {
        const char *amp = strchr(p, '&'), *end = amp ? amp : p + strlen(p);
        const char *equal = memchr(p, '=', (size_t)(end - p));
        char key[32]; int field = -1;
        if (!equal || !cwl_decode(p, (size_t)(equal - p), key, sizeof(key))) return 0;
        for (int i = 0; i < 7; i++) if (!strcmp(key, cwl_names[i])) field = i;
        if (field < 0 || (seen & (1u << field)) || !cwl_decode(equal + 1, (size_t)(end - equal - 1), cwl_field(form, field), cwl_capacity(field))) return 0;
        seen |= 1u << field;
        if (amp && !amp[1]) return 0;
        p = amp ? amp + 1 : end;
    }
    return (seen & 15) == 15;
}
/* The HTTP reader replaces the first byte of the final CRLFCRLF (or LFLF)
 * with NUL before routing, so its last header has no trailing line ending.
 * Also bound full framed inputs at their first blank line. aw_header must
 * never scan a body. Refuse folded fields and unsupported encodings. */
static int cwl_headers(const char *raw, char out[CWL_HEADER_MAX + 1]) {
    if (!raw) return 0;
    size_t n = strnlen(raw, CWL_HEADER_MAX + 1), boundary = 0;
    for (size_t i = 0; i < n; i++) {
        if (i + 3 < n && !memcmp(raw + i, "\r\n\r\n", 4)) { boundary = i; break; }
        if (i + 1 < n && !memcmp(raw + i, "\n\n", 2)) { boundary = i; break; }
    }
    if (!boundary) boundary = n;
    if (!boundary || boundary > CWL_HEADER_MAX) return 0;
    memcpy(out, raw, boundary); out[boundary] = 0;
    const char *limit = out + boundary, *line = strchr(out, '\n');
    if (!line) return 0;
    for (line++; *line;) {
        const char *newline = strchr(line, '\n'), *end = newline ? newline : limit, *colon;
        if (newline && end > line && end[-1] == '\r') end--;
        if (line == end || !(colon = memchr(line, ':', (size_t)(end - line))) || colon == line) return 0;
        for (const char *p = line; p < colon; p++)
            if (!strchr("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!#$%&'*+-.^_`|~", *p)) return 0;
        for (const char *p = colon + 1; p < end; p++)
            if (((unsigned char)*p < 32 && *p != '\t') || (unsigned char)*p == 127) return 0;
        size_t key_length = (size_t)(colon - line);
        if ((key_length == 17 && !strncasecmp(line, "Transfer-Encoding", 17)) ||
            (key_length == 16 && !strncasecmp(line, "Content-Encoding", 16))) return 0;
        line = newline ? newline + 1 : limit;
    }
    return 1;
}
static int cwl_origin(const char *host, char *out, size_t capacity) {
    if (!host || !*host || strlen(host) > 255 || strspn(host, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-:[]") != strlen(host)) return 0;
    int local = !strcmp(host, "localhost") || !strncmp(host, "localhost:", 10) ||
        !strcmp(host, "127.0.0.1") || !strncmp(host, "127.0.0.1:", 10) ||
        !strcmp(host, "[::1]") || !strncmp(host, "[::1]:", 6);
    return snprintf(out, capacity, "%s://%s", local ? "http" : "https", host) < (int)capacity;
}
/* Accept a single UTF-8 charset parameter, including ordinary browser forms.
 * Every other parameter/encoding is outside this text-only contract. */
static int cwl_media(const char *type) {
    int kind; const char *p;
    if (!strncasecmp(type, "application/json", 16)) { kind = 1; p = type + 16; }
    else if (!strncasecmp(type, "application/x-www-form-urlencoded", 33)) { kind = 2; p = type + 33; }
    else return 0;
    while (*p == ' ' || *p == '\t') p++;
    if (!*p) return kind;
    if (*p++ != ';') return 0;
    while (*p == ' ' || *p == '\t') p++;
    if (strncasecmp(p, "charset", 7)) return 0;
    p += 7; while (*p == ' ' || *p == '\t') p++;
    if (*p++ != '=') return 0;
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '"') { p++; if (strncasecmp(p, "utf-8\"", 6)) return 0; p += 6; }
    else { if (strncasecmp(p, "utf-8", 5)) return 0; p += 5; }
    while (*p == ' ' || *p == '\t') p++;
    return *p ? 0 : kind;
}
/* Only a server-derived workspace path can become a navigation target. */
static int cwl_paths(const char *back, char action[80]) {
    if (!back || strlen(back) < 50 || strlen(back) > 511 || strncmp(back, "/apps/ws-", 9) || back[49] != '/') return 0;
    for (size_t i = 9; i < 49; i++) if (!strchr("0123456789abcdef", back[i])) return 0;
    if (strspn(back + 50, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-/") != strlen(back + 50) || strstr(back + 49, "//")) return 0;
    return snprintf(action, 80, "%.*s_campaign-link", 50, back) < 80;
}
static const char *cwl_explanation(const char *code) {
    if (!strcmp(code, "destination_already_tagged")) return "This destination already has campaign tags. Remove its existing source, medium, campaign, ID, term and content tags before building this link.";
    if (!strcmp(code, "required_utm_missing")) return "Enter a source, medium and campaign name.";
    if (!strcmp(code, "generated_url_too_large")) return "The completed link is too long. Shorten the destination or campaign fields.";
    if (!strcmp(code, "invalid_url") || !strcmp(code, "destination_not_public_dns")) return "Enter a complete public HTTP or HTTPS address without a username or password. The invalid destination was cleared.";
    if (!strcmp(code, "duplicate_utm") || !strcmp(code, "noncanonical_utm_key") || !strcmp(code, "invalid_utm_value") || !strcmp(code, "invalid_query")) return "The destination has invalid or repeated campaign tags. Correct its query fields before building this link.";
    return "Check the fields. Campaign values must be nonempty when supplied, use at most 256 UTF-8 bytes each, and contain no control characters or backslashes.";
}
static int cwl_result_text(const bf_jsonw *result, const char *key, char *out, size_t capacity) {
    bf_json_doc_t *doc = bf_json_parse(result->buf, result->len, NULL, 0);
    const bf_json_node_t *node = doc ? bf_json_obj_get(doc, bf_json_root(doc), key) : NULL;
    size_t length = 0; char *text = node && node->type == BF_JSON_STRING ? bf_json_unescape(node->val.str.ptr, node->val.str.len, &length) : NULL;
    int okay = text && length < capacity && cwl_clean(text, length);
    if (okay) { memcpy(out, text, length); out[length] = 0; }
    free(text); bf_json_free(doc); return okay;
}
/* A different invalid field may make build refuse before inspecting the URL.
 * Check its display eligibility separately so credentials never reappear in
 * the correction form merely because a campaign value was also invalid. */
static int cwl_visible_destination(const char *url) {
    bf_jsonw request, result; bf_jsonw_init(&request, 0); bf_jsonw_init(&result, 0);
    bf_jsonw_obj(&request); bf_jsonw_key(&request, "version"); bf_jsonw_int(&request, 1);
    bf_jsonw_key(&request, "operation"); bf_jsonw_str(&request, "validate");
    bf_jsonw_key(&request, "url"); bf_jsonw_str(&request, url); bf_jsonw_obj_end(&request);
    int visible = 0;
    if (!request.oom && request.buf) {
        int status = bf_campaign_request(request.buf, request.len, &result);
        char code[80];
        if (!result.oom && result.buf) {
            if (!status) visible = 1;
            else if (cwl_result_text(&result, "error", code, sizeof(code)))
                visible = !strcmp(code, "duplicate_utm") || !strcmp(code, "noncanonical_utm_key") || !strcmp(code, "invalid_utm_value") || !strcmp(code, "invalid_query");
        }
    }
    bf_jsonw_free(&request); bf_jsonw_free(&result); return visible;
}
static int cwl_page(int status, const char *back, const char *action, const CwlForm *form, const char *error, const char *result) {
    char *page = NULL; size_t size = 0; FILE *out = open_memstream(&page, &size);
    if (!out) { aw_error(503, "The campaign form could not be prepared.\n"); return 1; }
    fputs("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>Campaign link · Aurekai</title><style>"
        "*{box-sizing:border-box}body{margin:0;background:#f5f3ee;color:#202b29;font:16px/1.6 system-ui,sans-serif}main{max-width:68rem;margin:auto;padding:clamp(1rem,4vw,3rem)}a{color:#176451;text-underline-offset:.2em}h1{font-size:clamp(2rem,5vw,3rem);line-height:1.15;margin:.8rem 0}h2{font-size:1.35rem}p{max-width:65ch}.intro{margin-bottom:2rem}form,.result{padding:clamp(1rem,3vw,2rem);background:#fff;border:1px solid #ccd5ce;border-radius:1rem}.fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1.4rem}.wide{grid-column:1/-1}label{display:block;font-weight:650}label span{font-weight:400;color:#495c55;font-size:.9rem}input,textarea{display:block;width:100%;min-width:0;margin-top:.4rem;padding:.8rem;border:1px solid #71857a;border-radius:.4rem;background:#fff;color:inherit;font:inherit}textarea{resize:vertical;overflow-wrap:anywhere}input:focus-visible,textarea:focus-visible,button:focus-visible,a:focus-visible{outline:3px solid #b05416;outline-offset:3px}.hint{margin:.35rem 0 0;color:#495c55;font-size:.9rem}button{font:inherit;font-weight:650;border:0;border-radius:.45rem;background:#176451;color:#fff;padding:.8rem 1.3rem;cursor:pointer;margin-top:1.8rem}.notice{padding:1rem;border:2px solid #a04719;border-radius:.5rem;background:#fff3e9;margin:1.5rem 0}.result{margin:1.5rem 0;border-color:#176451}.result h2{margin-top:0}footer{margin-top:1.5rem}@media(max-width:38rem){.fields{grid-template-columns:1fr}button{width:100%}}"
        "</style></head><body><main><nav aria-label=\"Workspace\"><a href=\"", out);
    bf_lex_html_emit(out, back);
    fputs("\">Back to campaign workspace</a></nav><header class=\"intro\"><h1>Build a campaign link</h1><p>Add source, medium and campaign tags to a destination. Copy the completed link into the message or creative you plan to share.</p></header>", out);
    if (error && *error) { fputs("<div class=\"notice\" role=\"alert\"><strong>Check this link</strong><p>", out); bf_lex_html_emit(out, error); fputs("</p></div>", out); }
    if (result && *result) {
        fputs("<section class=\"result\" aria-labelledby=\"result-heading\"><h2 id=\"result-heading\">Link ready to copy</h2><label for=\"campaign-result\">Completed campaign URL</label><textarea id=\"campaign-result\" rows=\"5\" readonly spellcheck=\"false\" aria-describedby=\"copy-help\">", out);
        bf_lex_html_emit(out, result);
        fputs("</textarea><p id=\"copy-help\" class=\"hint\">Select the URL and copy it. This page has not opened or shared the destination.</p></section>", out);
    }
    fputs("<form method=\"post\" accept-charset=\"UTF-8\" action=\"", out); bf_lex_html_emit(out, action);
    fputs("\"><p>Source, medium and campaign are required. Keep their spelling and capitalization consistent across your campaign.</p><div class=\"fields\">", out);
    for (int i = 0; i < 7; i++) {
        fprintf(out, "<div%s><label for=\"campaign-%s\">%s%s</label><input id=\"campaign-%s\" name=\"%s\" type=\"%s\" maxlength=\"%u\" autocomplete=\"off\" spellcheck=\"false\" aria-describedby=\"hint-%s\"%s value=\"",
            i ? "" : " class=\"wide\"", cwl_names[i], cwl_labels[i], i > 3 ? " <span>(optional)</span>" : "",
            cwl_names[i], cwl_names[i], i ? "text" : "url", i ? BF_CAMPAIGN_VALUE_MAX : BF_CAMPAIGN_URL_MAX,
            cwl_names[i], i < 4 ? " required" : "");
        bf_lex_html_emit(out, i ? form->values[i - 1] : form->url);
        fprintf(out, "\"><p class=\"hint\" id=\"hint-%s\">%s</p></div>", cwl_names[i], cwl_hints[i]);
    }
    fputs("</div><button type=\"submit\">Build campaign link</button></form><footer><p class=\"hint\">This form creates a link for copying. Campaign tags do not identify a person or prove that a visit or conversion occurred. Save campaign drafts separately in the workspace.</p></footer></main></body></html>", out);
    int failed = ferror(out); if (fclose(out)) failed = 1;
    if (failed || !page || size > CWL_HTML_MAX) aw_error(503, "The campaign form could not be prepared.\n");
    /* Keep the same-origin form Origin; external destinations receive no referrer. */
    else aw_response_with_referrer(status, "text/html; charset=utf-8", page, size, 0, "same-origin");
    free(page); return 1;
}
static int cwl_serve(const char *method, const char *query, const char *body, const char *raw, const char *host, const char *return_path) {
    char action[80]; CwlForm form = {0};
    if (!cwl_paths(return_path, action)) { aw_error(409, "Reopen this link builder from the campaign workspace.\n"); return 1; }
    int api_query = query && *query;
    if (api_query && (strcmp(query, "repr=API") || strcmp(method, "POST"))) { aw_error(400, "Campaign fields belong in the submitted form or JSON request.\n"); return 1; }
    if (!strcmp(method, "GET")) {
        if (body && *body) { aw_error(400, "Open the campaign link form without a request body.\n"); return 1; }
        return cwl_page(200, return_path, action, &form, NULL, NULL);
    }
    if (strcmp(method, "POST")) { aw_error(405, "Use GET to open the form or POST to build or inspect a link.\n"); return 1; }
    char headers[CWL_HEADER_MAX + 1], origin[512], expected[512], type[128], length[32];
    if (!cwl_headers(raw, headers)) { aw_error(400, "Submit an unencoded request with one complete header block.\n"); return 1; }
    if (!cwl_origin(host, expected, sizeof(expected)) || !aw_header(headers, "Origin", origin, sizeof(origin)) || strcmp(origin, expected)) {
        aw_error(403, "Submit this campaign link from its current workspace.\n"); return 1;
    }
    if (!aw_header(headers, "Content-Type", type, sizeof(type))) { aw_error(415, "Use a URL-encoded form or a UTF-8 JSON request.\n"); return 1; }
    int media = cwl_media(type);
    if (!media) { aw_error(415, "Use a URL-encoded form or a UTF-8 JSON request.\n"); return 1; }
    if (api_query && media != 1) { aw_error(400, "The API representation requires a JSON POST request.\n"); return 1; }
    if (!aw_header(headers, "Content-Length", length, sizeof(length)) || !*length || strlen(length) > 5 || strspn(length, "0123456789") != strlen(length)) {
        aw_error(400, "Provide one valid Content-Length for the request body.\n"); return 1;
    }
    size_t size = (size_t)strtoul(length, NULL, 10);
    if (size > BF_CAMPAIGN_REQUEST_MAX || (body && strnlen(body, BF_CAMPAIGN_REQUEST_MAX + 1) > BF_CAMPAIGN_REQUEST_MAX)) {
        aw_error(413, "The campaign link request exceeds 16384 bytes.\n"); return 1;
    }
    if (!body || !size || strlen(body) != size) { aw_error(400, "The campaign link request body is incomplete or has a different length.\n"); return 1; }
    bf_jsonw result; bf_jsonw_init(&result, 0); int status;
    if (media == 1) {
        status = bf_campaign_request(body, size, &result);
        if (result.oom || !result.buf) aw_error(503, "The campaign link owner could not prepare a response.\n");
        else aw_response(status ? 400 : 200, "application/json; charset=utf-8", result.buf, result.len, 0);
        bf_jsonw_free(&result); return 1;
    }
    if (!cwl_form(body, &form)) {
        memset(&form, 0, sizeof(form));
        bf_jsonw_free(&result);
        return cwl_page(400, return_path, action, &form, "The form contains an unknown, repeated, malformed or oversized field. Re-enter the campaign fields.", NULL);
    }
    bf_jsonw request; bf_jsonw_init(&request, 0); bf_jsonw_obj(&request);
    bf_jsonw_key(&request, "version"); bf_jsonw_int(&request, 1);
    bf_jsonw_key(&request, "operation"); bf_jsonw_str(&request, "build");
    bf_jsonw_key(&request, "url"); bf_jsonw_str(&request, form.url);
    bf_jsonw_key(&request, "utm"); bf_jsonw_obj(&request);
    for (int i = 0; i < 6; i++) if (form.values[i][0]) { bf_jsonw_key(&request, cwl_names[i + 1]); bf_jsonw_str(&request, form.values[i]); }
    bf_jsonw_obj_end(&request); bf_jsonw_obj_end(&request);
    if (request.oom || !request.buf) { bf_jsonw_free(&request); bf_jsonw_free(&result); aw_error(503, "The campaign link request could not be prepared.\n"); return 1; }
    status = bf_campaign_request(request.buf, request.len, &result); bf_jsonw_free(&request);
    if (result.oom || !result.buf) { bf_jsonw_free(&result); aw_error(503, "The campaign link owner could not prepare a response.\n"); return 1; }
    char completed[BF_CAMPAIGN_URL_MAX + 1] = "", code[80] = "";
    if (!cwl_result_text(&result, status ? "error" : "url", status ? code : completed, status ? sizeof(code) : sizeof(completed))) {
        bf_jsonw_free(&result); aw_error(503, "The campaign link owner returned an incomplete result.\n"); return 1;
    }
    bf_jsonw_free(&result);
    if (status && !cwl_visible_destination(form.url)) form.url[0] = 0;
    return cwl_page(status ? 400 : 200, return_path, action, &form, status ? cwl_explanation(code) : NULL, completed);
}
#endif
