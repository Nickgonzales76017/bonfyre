#define _POSIX_C_SOURCE 200809L

/*
 * BonfyreParagraph — turn normalized text into passages.
 *
 * Text that already has structure (blank lines, heading lines) keeps it: a
 * heading opens a new passage and travels with the passages under it, and
 * neighbouring short blocks are packed together.  Text with no structure,
 * such as a transcript, is cut on sentence boundaries as before.
 *
 * --out writes the passages as readable text.  --passages writes one JSON
 * record per passage ({id,text,heading,ordinal,source_sha256}) which Embed and
 * Recall index directly.
 */

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <bonfyre.h>
#include "../../../lib/libbonfyre/include/bf_help.h"

#define TARGET_SENTENCES 4
#define MAX_SENTENCES 7
#define MAX_PASSAGE 1200

typedef struct {
    char **items;
    char **headings;
    char **ids;
    size_t count;
    size_t capacity;
} PassageList;

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} Buffer;

static int buffer_add(Buffer *b, const char *text, size_t len) {
    if (b->len + len + 1 > b->cap) {
        size_t next = b->cap ? b->cap * 2 : 1024;
        char *grown;
        while (next < b->len + len + 1) next *= 2;
        grown = realloc(b->data, next);
        if (!grown) return 1;
        b->data = grown;
        b->cap = next;
    }
    memcpy(b->data + b->len, text, len);
    b->len += len;
    b->data[b->len] = '\0';
    return 0;
}

static void buffer_reset(Buffer *b) {
    b->len = 0;
    if (b->data) b->data[0] = '\0';
}

static int push_passage(PassageList *list, const char *text, size_t len, const char *heading) {
    while (len && isspace((unsigned char)*text)) { text++; len--; }
    while (len && isspace((unsigned char)text[len - 1])) len--;
    if (!len) return 0;
    if (list->count == list->capacity) {
        size_t next = list->capacity ? list->capacity * 2 : 64;
        char **items = realloc(list->items, sizeof(char *) * next);
        char **headings;
        if (!items) return 1;
        list->items = items;
        headings = realloc(list->headings, sizeof(char *) * next);
        if (!headings) return 1;
        list->headings = headings;
        headings = realloc(list->ids, sizeof(char *) * next);
        if (!headings) return 1;
        list->ids = headings;
        list->capacity = next;
    }
    list->items[list->count] = strndup(text, len);
    list->headings[list->count] = strdup(heading ? heading : "");
    list->ids[list->count] = NULL;
    if (!list->items[list->count] || !list->headings[list->count]) return 1;
    list->count++;
    return 0;
}

static void free_passages(PassageList *list) {
    for (size_t i = 0; i < list->count; i++) { free(list->items[i]); free(list->headings[i]); free(list->ids[i]); }
    free(list->items);
    free(list->headings);
    free(list->ids);
}

/* A sentence ends at . ! ? only when whitespace or the end follows, so
 * decimals, timestamps and addresses stay in one piece. */
static const char *sentence_end(const char *p) {
    for (; *p; p++)
        if ((*p == '.' || *p == '!' || *p == '?') && (!p[1] || isspace((unsigned char)p[1])))
            return p + 1;
    return p;
}

static int starts_with_transition(const char *sentence) {
    static const char *patterns[] = {
        "so ", "now ", "anyway", "moving on", "next", "also ", "another",
        "meanwhile", "however", "but ", "on the other hand",
        "let me", "let's", "i want to", "we should", "the next",
        "going back", "okay", "alright", "right", NULL
    };
    while (*sentence && isspace((unsigned char)*sentence)) sentence++;
    for (int j = 0; patterns[j]; j++)
        if (strncasecmp(sentence, patterns[j], strlen(patterns[j])) == 0) return 1;
    return 0;
}

/* A block too long to be one passage is cut on sentence boundaries; a single
 * sentence longer than a passage is cut at the last space that fits. */
static int split_long_block(PassageList *list, const char *block, const char *heading) {
    const char *start = block, *p = block;
    int sentences = 0;
    while (*p) {
        const char *end = sentence_end(p);
        size_t with = (size_t)(end - start);
        if (sentences && (with > MAX_PASSAGE || sentences >= MAX_SENTENCES ||
                          (sentences >= TARGET_SENTENCES && starts_with_transition(p)))) {
            if (push_passage(list, start, (size_t)(p - start), heading)) return 1;
            start = p;
            sentences = 0;
            continue;
        }
        while ((size_t)(end - start) > MAX_PASSAGE) {
            const char *cut = start + MAX_PASSAGE;
            while (cut > start + MAX_PASSAGE / 2 && !isspace((unsigned char)*cut)) cut--;
            if (push_passage(list, start, (size_t)(cut - start), heading)) return 1;
            start = cut;
        }
        sentences++;
        p = end;
    }
    return push_passage(list, start, (size_t)(p - start), heading);
}

static int flush_pack(PassageList *list, Buffer *pack, const char *heading) {
    int rc = pack->len ? push_passage(list, pack->data, pack->len, heading) : 0;
    buffer_reset(pack);
    return rc;
}

static int flush_block(PassageList *list, Buffer *block, Buffer *pack, const char *heading) {
    int rc = 0;
    if (!block->len) return 0;
    if (block->len > MAX_PASSAGE) {
        rc = flush_pack(list, pack, heading) || split_long_block(list, block->data, heading);
    } else {
        if (pack->len + block->len + 1 > MAX_PASSAGE) rc = flush_pack(list, pack, heading);
        if (!rc && pack->len) rc = buffer_add(pack, "\n", 1);
        if (!rc) rc = buffer_add(pack, block->data, block->len);
    }
    buffer_reset(block);
    return rc;
}

/* A heading is one to six # and then a space.  "#hashtag" and "#1 priority"
 * are words, and a line of them is text. */
static int is_heading(const char *line, size_t len) {
    size_t marks = 0;
    while (marks < len && line[marks] == '#') marks++;
    return marks >= 1 && marks <= 6 && marks < len && line[marks] == ' ';
}

/* Three or more of - * or _ alone on a line. */
static int is_break(const char *line, size_t len) {
    while (len && isspace((unsigned char)line[len - 1])) len--;
    if (len < 3 || !strchr("-*_", line[0])) return 0;
    for (size_t i = 1; i < len; i++) if (line[i] != line[0]) return 0;
    return 1;
}

static int build_passages(const char *text, PassageList *list) {
    Buffer block = {0}, pack = {0};
    char heading[512] = "";
    int rc = 0;
    for (const char *line = text; *line && !rc; ) {
        const char *eol = strchr(line, '\n');
        size_t len = eol ? (size_t)(eol - line) : strlen(line);
        const char *first = line;
        size_t blank = len;
        while (blank && isspace((unsigned char)*first)) { first++; blank--; }
        if (!blank) {
            rc = flush_block(list, &block, &pack, heading);
        } else if (is_break(first, blank)) {
            /* A thematic break ends the section: what follows is not under
             * the heading that came before it. */
            rc = flush_block(list, &block, &pack, heading) || flush_pack(list, &pack, heading);
            heading[0] = '\0';
        } else if (is_heading(first, blank)) {
            rc = flush_block(list, &block, &pack, heading) || flush_pack(list, &pack, heading);
            while (blank && (*first == '#' || isspace((unsigned char)*first))) { first++; blank--; }
            while (blank && isspace((unsigned char)first[blank - 1])) blank--;
            if (blank >= sizeof(heading)) blank = sizeof(heading) - 1;
            memcpy(heading, first, blank);
            heading[blank] = '\0';
        } else {
            if (block.len) rc = buffer_add(&block, " ", 1);
            if (!rc) rc = buffer_add(&block, line, len);
        }
        line = eol ? eol + 1 : line + len;
    }
    if (!rc) rc = flush_block(list, &block, &pack, heading) || flush_pack(list, &pack, heading);
    free(block.data);
    free(pack.data);
    return rc;
}

/* Decodes the JSON string value of a top-level key on one record line. */
static char *json_field(const char *line, const char *key) {
    char pattern[64];
    const char *p;
    Buffer out = {0};
    snprintf(pattern, sizeof(pattern), "\"%s\":", key);
    p = strstr(line, pattern);
    if (!p) return NULL;
    p += strlen(pattern);
    while (*p == ' ') p++;
    if (*p != '"') return NULL;
    for (p++; *p && *p != '"'; p++) {
        char c = *p;
        if (c == '\\' && p[1]) {
            p++;
            switch (*p) {
                case 'n': c = '\n'; break;
                case 't': c = '\t'; break;
                case 'r': c = '\r'; break;
                case 'b': case 'f': c = ' '; break;
                case 'u': {
                    unsigned code = 0;
                    char utf[4];
                    size_t n = 0;
                    if (sscanf(p + 1, "%4x", &code) != 1) { free(out.data); return NULL; }
                    p += 4;
                    if (code < 0x80) utf[n++] = (char)code;
                    else if (code < 0x800) { utf[n++] = (char)(0xC0 | (code >> 6)); utf[n++] = (char)(0x80 | (code & 0x3F)); }
                    else { utf[n++] = (char)(0xE0 | (code >> 12)); utf[n++] = (char)(0x80 | ((code >> 6) & 0x3F)); utf[n++] = (char)(0x80 | (code & 0x3F)); }
                    if (buffer_add(&out, utf, n)) { free(out.data); return NULL; }
                    continue;
                }
                default: c = *p;
            }
        }
        if (buffer_add(&out, &c, 1)) { free(out.data); return NULL; }
    }
    if (*p != '"') { free(out.data); return NULL; }
    if (!out.data) return strdup("");
    return out.data;
}

/* Records in, passages out: each {id,text} record is cut the same way a file
 * is, and its passages keep the record's id (id, then id#1, id#2 ...). */
static int build_from_records(const char *text, PassageList *list, size_t *records, size_t *unreadable) {
    char *copy = strdup(text), *save = NULL;
    int rc = copy ? 0 : 1;
    *records = *unreadable = 0;
    for (char *line = copy ? strtok_r(copy, "\n", &save) : NULL; line && !rc; line = strtok_r(NULL, "\n", &save)) {
        char *id, *body;
        size_t from = list->count;
        while (*line && isspace((unsigned char)*line)) line++;
        if (!*line) continue;
        id = json_field(line, "id");
        body = json_field(line, "text");
        if (!id || !*id || !body) { (*unreadable)++; free(id); free(body); continue; }
        (*records)++;
        rc = build_passages(body, list);
        for (size_t i = from; i < list->count && !rc; i++) {
            size_t cap = strlen(id) + 24;
            list->ids[i] = malloc(cap);
            if (!list->ids[i]) { rc = 1; break; }
            if (i == from) snprintf(list->ids[i], cap, "%s", id);
            else snprintf(list->ids[i], cap, "%s#%zu", id, i - from);
        }
        free(id);
        free(body);
    }
    free(copy);
    return rc;
}

static void json_string(FILE *out, const char *text) {
    fputc('"', out);
    for (; *text; text++) {
        unsigned char c = (unsigned char)*text;
        if (c == '"' || c == '\\') { fputc('\\', out); fputc(c, out); }
        else if (c == '\n') fputs("\\n", out);
        else if (c == '\r') fputs("\\r", out);
        else if (c == '\t') fputs("\\t", out);
        else if (c < 0x20) fprintf(out, "\\u%04x", c);
        else fputc(c, out);
    }
    fputc('"', out);
}

static int write_text(const char *path, PassageList *list, int with_headers) {
    FILE *out = fopen(path, "w");
    const char *last = NULL;
    if (!out) return 1;
    for (size_t i = 0; i < list->count; i++) {
        const char *heading = list->headings[i];
        if (with_headers && (!last || strcmp(last, heading))) {
            if (heading[0]) fprintf(out, "## %s\n\n", heading);
            else if (!i) fputs("## Opening\n\n", out);
        }
        last = heading;
        fputs(list->items[i], out);
        fputs(i + 1 < list->count ? "\n\n" : "\n", out);
    }
    return fclose(out) != 0;
}

/* A passage repeated word for word in one source is written once; its later
 * copies are counted, not indexed twice. */
static int write_passages(const char *path, PassageList *list, const char *source_id,
                          const char *source_sha, size_t *written, size_t *repeated) {
    FILE *out = fopen(path, "w");
    char (*seen)[65] = calloc(list->count ? list->count : 1, 65);
    if (!out || !seen) { if (out) fclose(out); free(seen); return 1; }
    *written = *repeated = 0;
    for (size_t i = 0; i < list->count; i++) {
        Buffer text = {0};
        char digest[65];
        int duplicate = 0;
        if (list->headings[i][0] && strncmp(list->items[i], list->headings[i], strlen(list->headings[i]))) {
            buffer_add(&text, list->headings[i], strlen(list->headings[i]));
            buffer_add(&text, "\n", 1);
        }
        if (buffer_add(&text, list->items[i], strlen(list->items[i]))) { free(text.data); fclose(out); free(seen); return 1; }
        bf_sha256_hex((const uint8_t *)list->items[i], strlen(list->items[i]), digest);
        for (size_t j = 0; j < *written && !duplicate; j++) duplicate = !strcmp(seen[j], digest);
        if (duplicate) { (*repeated)++; free(text.data); continue; }
        memcpy(seen[*written], digest, 65);
        fputs("{\"id\":", out);
        if (list->ids[i]) json_string(out, list->ids[i]);
        else {
            char id[600];
            snprintf(id, sizeof(id), "%.500s:%zu", source_id, i);
            json_string(out, id);
        }
        fputs(",\"text\":", out);
        json_string(out, text.data);
        fputs(",\"heading\":", out);
        json_string(out, list->headings[i]);
        fprintf(out, ",\"ordinal\":%zu,\"source_sha256\":\"%s\"}\n", i, source_sha);
        (*written)++;
        free(text.data);
    }
    free(seen);
    return fclose(out) != 0;
}

static int write_status(const char *path, const char *input, const char *output,
                        const char *passages, size_t count, size_t written, size_t repeated,
                        int with_headers) {
    FILE *out = fopen(path, "w");
    if (!out) return 1;
    fputs("{\n  \"sourceSystem\": \"BonfyreParagraph\",\n  \"status\": \"completed\",\n  \"inputPath\": ", out);
    json_string(out, input);
    fputs(",\n  \"outputPath\": ", out);
    json_string(out, output ? output : "");
    fputs(",\n  \"passagesPath\": ", out);
    json_string(out, passages ? passages : "");
    fprintf(out, ",\n  \"paragraphCount\": %zu,\n  \"passagesWritten\": %zu,\n"
                 "  \"passagesRepeated\": %zu,\n  \"withHeaders\": %s,\n  \"deterministic\": true\n}\n",
            count, written, repeated, with_headers ? "true" : "false");
    return fclose(out) != 0;
}

static void usage(void) {
    fprintf(stderr,
        "Usage: bonfyre-paragraph (--input <file> | --input-jsonl <records.jsonl>)\n"
        "                         [--out <file>] [--passages <file.jsonl>]\n"
        "                         [--source-id <label>] [--with-headers] [--dry-run]\n"
        "  --out       passages as readable text\n"
        "  --passages  one {id,text,heading,ordinal,source_sha256} record per passage,\n"
        "              ready for bonfyre-recall index-jsonl; id is <source-id>:<ordinal>\n"
        "  --input-jsonl  {id,text} records; passages keep each record's id (id, id#1 ...)\n"
        "  --source-id label for ids (default: first 12 hex of the input's sha256)\n");
}

int main(int argc, char **argv) {
    if (bf_help_requested(argc, argv)) { usage(); return 0; }
    const char *records_path = NULL;
    size_t records = 0, unreadable = 0;
    const char *input = NULL, *out_path = NULL, *passages_path = NULL, *source_id = NULL;
    int with_headers = 0, dry_run = 0;
    char *text;
    size_t text_len = 0, written = 0, repeated = 0;
    PassageList list = {0};
    char dir[PATH_MAX], status_path[PATH_MAX], source_sha[65], short_id[13];
    const char *anchor;
    char *slash;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--input") == 0 && i + 1 < argc) input = argv[++i];
        else if (strcmp(argv[i], "--input-jsonl") == 0 && i + 1 < argc) input = records_path = argv[++i];
        else if (strcmp(argv[i], "--out") == 0 && i + 1 < argc) out_path = argv[++i];
        else if (strcmp(argv[i], "--passages") == 0 && i + 1 < argc) passages_path = argv[++i];
        else if (strcmp(argv[i], "--source-id") == 0 && i + 1 < argc) source_id = argv[++i];
        else if (strcmp(argv[i], "--with-headers") == 0) with_headers = 1;
        else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        else { usage(); return 1; }
    }
    if (!input || (!out_path && !passages_path)) { usage(); return 1; }
    if (dry_run) {
        printf("Would paragraphize %s -> %s\n", input, out_path ? out_path : passages_path);
        return 0;
    }

    text = bf_read_file(input, &text_len);
    if (!text) {
        fprintf(stderr, "Missing input file: %s\n", input);
        return 2;
    }
    bf_sha256_hex((const uint8_t *)text, text_len, source_sha);
    memcpy(short_id, source_sha, 12);
    short_id[12] = '\0';
    if (!source_id || !*source_id) source_id = short_id;

    if ((records_path ? build_from_records(text, &list, &records, &unreadable)
                      : build_passages(text, &list)) != 0) {
        free(text);
        free_passages(&list);
        return 1;
    }
    free(text);
    if (records_path && unreadable) {
        fprintf(stderr, "%zu record line(s) had no readable id and text\n", unreadable);
        free_passages(&list);
        return 2;
    }

    anchor = out_path ? out_path : passages_path;
    snprintf(dir, sizeof(dir), "%s", anchor);
    slash = strrchr(dir, '/');
    if (slash) {
        *slash = '\0';
        if (dir[0] && bf_ensure_dir(dir) != 0) { free_passages(&list); return 1; }
        snprintf(status_path, sizeof(status_path), "%s/status.json", dir);
    } else {
        snprintf(status_path, sizeof(status_path), "status.json");
    }

    if ((out_path && write_text(out_path, &list, with_headers) != 0) ||
        (passages_path && write_passages(passages_path, &list, source_id, source_sha, &written, &repeated) != 0) ||
        write_status(status_path, input, out_path, passages_path, list.count, written, repeated, with_headers) != 0) {
        free_passages(&list);
        return 1;
    }

    if (records_path) printf("Records: %zu\n", records);
    printf("Paragraphed: %zu paragraphs\n", list.count);
    if (out_path) printf("Written: %s\n", out_path);
    if (passages_path) printf("Passages: %s (%zu written, %zu repeated)\n", passages_path, written, repeated);
    free_passages(&list);
    return 0;
}
