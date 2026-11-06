#include "territory_home.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int replace_once(char **bodyp, const char *needle, const char *replacement) {
    char *body, *at, *next;
    size_t before, old_len, new_len, after;
    if (!bodyp || !(body = *bodyp) || !needle || !*needle || !replacement) return 0;
    at = strstr(body, needle);
    if (!at) return 0;
    before = (size_t)(at - body);
    old_len = strlen(needle);
    new_len = strlen(replacement);
    after = strlen(at + old_len);
    next = malloc(before + new_len + after + 1);
    if (!next) return 0;
    memcpy(next, body, before);
    memcpy(next + before, replacement, new_len);
    memcpy(next + before + new_len, at + old_len, after + 1);
    free(body);
    *bodyp = next;
    return 1;
}

typedef struct {
    const char *begin;
    const char *end;
    const char *replacement;
} HomeEdit;

static int locate_inner(const char *body, const char *id, const char *replacement, HomeEdit *edit) {
    char needle[128], close_tag[80];
    const char *id_at, *open, *open_end, *tag, *tag_end, *close_at;
    size_t tag_len;
    if (!body || !id || !*id || !replacement || !edit) return 0;
    if (snprintf(needle, sizeof(needle), "id=\"%s\"", id) >= (int)sizeof(needle)) return 0;
    id_at = strstr(body, needle);
    if (!id_at) return 0;
    open = id_at;
    while (open > body && *open != '<') open--;
    if (*open != '<' || open[1] == '/') return 0;
    open_end = strchr(id_at, '>');
    if (!open_end) return 0;
    tag = open + 1;
    tag_end = tag;
    while (*tag_end && (isalnum((unsigned char)*tag_end) || *tag_end == '-')) tag_end++;
    tag_len = (size_t)(tag_end - tag);
    if (!tag_len ||
        snprintf(close_tag, sizeof(close_tag), "</%.*s>", (int)tag_len, tag) >= (int)sizeof(close_tag))
        return 0;
    close_at = strstr(open_end + 1, close_tag);
    if (!close_at) return 0;
    edit->begin = open_end + 1;
    edit->end = close_at;
    edit->replacement = replacement;
    return 1;
}

static int compare_edits(const void *a, const void *b) {
    const HomeEdit *left = a, *right = b;
    return left->begin < right->begin ? -1 : left->begin > right->begin ? 1 : 0;
}

static int valid_body_class(const char *territory) {
    const unsigned char *at = (const unsigned char *)territory;
    size_t length = 0;
    if (!at) return 0;
    for (; *at; at++, length++)
        if (length >= 31 || !((*at >= 'a' && *at <= 'z') ||
                              (*at >= '0' && *at <= '9') || *at == '-')) return 0;
    return length != 0;
}

static int apply_presentation(char **bodyp, const char *territory,
                              const char *const ids[7], const char *const values[7]) {
    HomeEdit edits[8];
    const char *body = *bodyp, *body_at;
    char body_tag[64];
    char *next, *dest;
    size_t i, length, total;
    if (!valid_body_class(territory) ||
        snprintf(body_tag, sizeof(body_tag), "<body class=\"%s\">", territory) >= (int)sizeof(body_tag))
        return 0;
    body_at = strstr(body, "<body>");
    if (!body_at) return 0;
    edits[0] = (HomeEdit){body_at, body_at + sizeof("<body>") - 1, body_tag};
    for (i = 0; i < 7; i++)
        if (!locate_inner(body, ids[i], values[i], &edits[i + 1])) return 0;
    qsort(edits, 8, sizeof(edits[0]), compare_edits);
    length = strlen(body);
    total = length;
    for (i = 0; i < 8; i++) {
        size_t removed = (size_t)(edits[i].end - edits[i].begin);
        size_t added = strlen(edits[i].replacement);
        if ((i && edits[i].begin < edits[i - 1].end) || removed > total || added > SIZE_MAX - (total - removed) - 1)
            return 0;
        total = total - removed + added;
    }
    next = malloc(total + 1);
    if (!next) return 0;
    dest = next;
    body_at = body;
    for (i = 0; i < 8; i++) {
        size_t before = (size_t)(edits[i].begin - body_at);
        size_t added = strlen(edits[i].replacement);
        memcpy(dest, body_at, before);
        dest += before;
        memcpy(dest, edits[i].replacement, added);
        dest += added;
        body_at = edits[i].end;
    }
    strcpy(dest, body_at);
    free(*bodyp);
    *bodyp = next;
    return 1;
}

static void copy_column(char *out, size_t cap, sqlite3_stmt *st, int col) {
    const unsigned char *value = sqlite3_column_text(st, col);
    snprintf(out, cap, "%s", value ? (const char *)value : "");
}

static int apply_body_class(char **bodyp, const char *territory) {
    char body_tag[64];
    if (!valid_body_class(territory) ||
        snprintf(body_tag, sizeof(body_tag), "<body class=\"%s\">", territory) >= (int)sizeof(body_tag))
        return 0;
    return replace_once(bodyp, "<body>", body_tag);
}

int bf_apply_territory_home_presentation(sqlite3 *db, char **bodyp, const char *territory) {
    sqlite3_stmt *st = NULL;
    char label[32], eyebrow[512], head[1024], lede[2048], question[1024];
    char character[256], cannot_infer[2048];
    static const char *ids[] = {"territory", "eyebrow", "title", "sub", "orbit-label", "look-title", "look-copy"};
    const char *values[7];

    if (!db || !bodyp || !*bodyp || !territory || !*territory ||
        sqlite3_prepare_v2(db,
          "SELECT territory,eyebrow,head,lede,question,character,cannot_infer "
          "FROM territory_presentation WHERE territory=?1 AND status='published' "
          "ORDER BY id DESC LIMIT 1", -1, &st, NULL) != SQLITE_OK)
        return apply_body_class(bodyp, territory); /* CMS copy is absent; Matrix territory is still resolved. */
    sqlite3_bind_text(st, 1, territory, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_ROW) { sqlite3_finalize(st); return apply_body_class(bodyp, territory); }
    copy_column(label, sizeof(label), st, 0);
    copy_column(eyebrow, sizeof(eyebrow), st, 1);
    copy_column(head, sizeof(head), st, 2);
    copy_column(lede, sizeof(lede), st, 3);
    copy_column(question, sizeof(question), st, 4);
    copy_column(character, sizeof(character), st, 5);
    copy_column(cannot_infer, sizeof(cannot_infer), st, 6);
    sqlite3_finalize(st);
    if (!label[0]) return 0;

    values[0] = label;
    values[1] = eyebrow;
    values[2] = head;
    values[3] = lede;
    values[4] = question;
    values[5] = character;
    values[6] = cannot_infer;
    return apply_presentation(bodyp, label, ids, values);
}
