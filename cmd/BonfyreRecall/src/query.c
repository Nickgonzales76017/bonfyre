#define _POSIX_C_SOURCE 200809L
#include "recall.h"

#include <ctype.h>
#include <errno.h>
#include <sqlite3.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

/* Candidate ids come back from the vector organ without their text.  The store
 * keeps the passage each vector was made from, so a caller that asks for
 * passages gets them here instead of opening the store by hand. */
static void print_passages(const char *store, const char *vec_json) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int first = 1;
    fputs(",\"passages\":[", stdout);
    if (sqlite3_open_v2(store, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK &&
        sqlite3_prepare_v2(db, "SELECT text FROM artifacts WHERE id=?1", -1, &st, NULL) == SQLITE_OK) {
        for (const char *p = strstr(vec_json, "\"id\":\""); p; p = strstr(p, "\"id\":\"")) {
            char id[1024];
            size_t used = 0;
            p += 6;
            for (; *p && *p != '"' && used + 1 < sizeof(id); p++) {
                if (*p == '\\' && p[1]) p++;
                id[used++] = *p;
            }
            id[used] = '\0';
            sqlite3_reset(st);
            sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st) != SQLITE_ROW) continue;
            if (!first) fputc(',', stdout);
            first = 0;
            fputs("{\"id\":", stdout); bf_json_stdout(id);
            fputs(",\"text\":", stdout);
            bf_json_stdout((const char *)sqlite3_column_text(st, 0));
            fputc('}', stdout);
        }
    }
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    fputc(']', stdout);
}

/* Two ways of finding a passage, one list.  The vector order says what is near
 * in meaning; the word index says what shares the question's words.  Each
 * passage scores 1/(60+rank) in every list it appears in (reciprocal rank
 * fusion), so agreement between the two rises and neither can bury the other.
 * Local: nothing leaves the machine. */
#define FUSE_MAX 256

static int print_fused(const char *store, const char *vec_json, const char *question, int top_k) {
    static char ids[FUSE_MAX][1024];
    int vector_rank[FUSE_MAX], word_rank[FUSE_MAX], order[FUSE_MAX];
    double score[FUSE_MAX];
    char match[4096] = "";
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    int count = 0, words = 0, rank = 0, rc = 7;
    size_t used = 0;

    for (const char *p = strstr(vec_json, "\"id\":\""); p && count < FUSE_MAX; p = strstr(p, "\"id\":\"")) {
        size_t n = 0;
        p += 6;
        for (; *p && *p != '"' && n + 1 < sizeof(ids[0]); p++) {
            if (*p == '\\' && p[1]) p++;
            ids[count][n++] = *p;
        }
        ids[count][n] = '\0';
        vector_rank[count] = count + 1;
        word_rank[count] = 0;
        count++;
    }
    /* The question's words, any of them: each quoted so none is read as syntax. */
    for (const char *p = question; *p; ) {
        const char *start;
        while (*p && !isalnum((unsigned char)*p) && (unsigned char)*p < 0x80) p++;
        start = p;
        while (*p && (isalnum((unsigned char)*p) || (unsigned char)*p >= 0x80)) p++;
        if (p - start < 3 || used + (size_t)(p - start) + 8 > sizeof(match)) continue;
        /* Words every passage has say nothing about which passage is meant. */
        {
            static const char *common[] = {"the","and","for","that","this","with","what","did","does","was","were",
                "are","has","have","had","not","but","from","they","them","their","you","your","our","about","which",
                "who","how","why","when","where","say","said","says","wrong","into","been","will","would","should",
                "could","can","any","all","its","than","then","there","here","still","just","also","some","more",NULL};
            int skip = 0;
            for (int i = 0; common[i] && !skip; i++)
                skip = strlen(common[i]) == (size_t)(p - start) && !strncasecmp(start, common[i], (size_t)(p - start));
            if (skip) continue;
        }
        used += (size_t)snprintf(match + used, sizeof(match) - used, "%s\"%.*s\"",
                                 words ? " OR " : "", (int)(p - start), start);
        words++;
    }
    if (sqlite3_open_v2(store, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, "SELECT id FROM passage_words WHERE passage_words MATCH ?1 "
                               "ORDER BY bm25(passage_words) LIMIT ?2", -1, &st, NULL) != SQLITE_OK) {
        fputs("this store has no word index; rebuild it with a current bonfyre-recall\n", stderr);
        goto done;
    }
    if (words) {
        sqlite3_bind_text(st, 1, match, -1, SQLITE_STATIC);
        sqlite3_bind_int(st, 2, count > 24 ? count : 24);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *id = (const char *)sqlite3_column_text(st, 0);
            int at = -1;
            rank++;
            for (int i = 0; i < count && at < 0; i++) if (!strcmp(ids[i], id)) at = i;
            if (at < 0 && count < FUSE_MAX) {
                at = count++;
                snprintf(ids[at], sizeof(ids[0]), "%s", id);
                vector_rank[at] = 0;
            }
            if (at >= 0) word_rank[at] = rank;
        }
    }
    sqlite3_finalize(st);
    st = NULL;
    for (int i = 0; i < count; i++) {
        score[i] = (vector_rank[i] ? 1.0 / (60 + vector_rank[i]) : 0) + (word_rank[i] ? 1.0 / (60 + word_rank[i]) : 0);
        order[i] = i;
    }
    for (int i = 1; i < count; i++)
        for (int j = i; j > 0 && score[order[j]] > score[order[j - 1]]; j--) {
            int swap = order[j]; order[j] = order[j - 1]; order[j - 1] = swap;
        }
    if (sqlite3_prepare_v2(db, "SELECT text FROM passage_text WHERE id=?1", -1, &st, NULL) != SQLITE_OK) goto done;
    printf(",\"fused\":{\"method\":\"reciprocal-rank\",\"word_matches\":%d,\"results\":[", rank);
    for (int k = 0, printed = 0; k < count && printed < top_k; k++) {
        int i = order[k];
        sqlite3_reset(st);
        sqlite3_bind_text(st, 1, ids[i], -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) != SQLITE_ROW) continue;
        if (printed++) fputc(',', stdout);
        fputs("{\"id\":", stdout); bf_json_stdout(ids[i]);
        printf(",\"score\":%.6f,\"vector_rank\":%d,\"word_rank\":%d,\"text\":", score[i], vector_rank[i], word_rank[i]);
        bf_json_stdout((const char *)sqlite3_column_text(st, 0));
        fputc('}', stdout);
    }
    fputs("]}", stdout);
    rc = 0;
done:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    return rc;
}

int bf_recall_query(int argc, char **argv) {
    const char *store = bf_opt(argc, argv, "--store");
    const char *text = bf_opt(argc, argv, "--text");
    const char *model = bf_opt(argc, argv, "--model");
    const char *top = bf_opt(argc, argv, "--top");
    const char *tmp = getenv("TMPDIR");
    char embed_buf[PATH_MAX], vec_buf[PATH_MAX];
    char query_path[PATH_MAX] = {0}, vector_path[PATH_MAX] = {0};
    char embed_out[4096], embed_err[8192], vec_out[262144], vec_err[8192];
    const char *embed = bf_recall_binpath("BONFYRE_EMBED_BINARY", "bonfyre-embed",
                                          embed_buf, sizeof(embed_buf));
    const char *vec = bf_recall_binpath("BONFYRE_VEC_BINARY", "bonfyre-vec",
                                        vec_buf, sizeof(vec_buf));
    int top_k = bf_parse_positive(top, 32, 1024);
    int query_fd = -1, vector_fd = -1, rc = 0, hybrid = 0;
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], "--hybrid")) hybrid = 1;

    if (!store || !*store || !text || !*text || top_k < 1) {
        fputs("query requires --store DB --text TEXT [--top 1..1024]\n", stderr);
        return 2;
    }
    if (!model || !*model) model = "nomic-embed-text-onnx";
    if (!tmp || !*tmp) tmp = "/tmp";
    if (access(embed, X_OK) || access(vec, X_OK) || access(store, R_OK)) {
        fputs("semantic recall dependency unavailable\n", stderr);
        return 3;
    }

    snprintf(query_path, sizeof(query_path), "%s/bonfyre-recall-q.XXXXXX", tmp);
    snprintf(vector_path, sizeof(vector_path), "%s/bonfyre-recall-v.XXXXXX", tmp);
    query_fd = mkstemp(query_path);
    vector_fd = mkstemp(vector_path);
    if (query_fd < 0 || vector_fd < 0) { rc = 3; goto done; }

    {
        size_t len = strlen(text), off = 0;
        while (off < len) {
            ssize_t n = write(query_fd, text + off, len - off);
            if (n <= 0) { rc = 3; goto done; }
            off += (size_t)n;
        }
    }
    close(query_fd);
    query_fd = -1;
    close(vector_fd);
    vector_fd = -1;

    {
        char *const av[] = {
            (char *)embed, "--text", query_path, "--out", vector_path,
            "--backend", "onnx", "--model", (char *)model,
            "--dims", "768", "--output-format", "json", NULL
        };
        if (bf_recall_run(av, embed_out, sizeof(embed_out),
                          embed_err, sizeof(embed_err))) {
            fprintf(stderr, "learned query embedding refused: %s", embed_err);
            rc = 4;
            goto done;
        }
    }

    {
        char top_buf[24];
        /* Fusion needs more of the vector order than it will return. */
        snprintf(top_buf, sizeof(top_buf), "%d", hybrid ? (top_k * 4 < 24 ? 24 : top_k * 4 > 128 ? 128 : top_k * 4) : top_k);
        char *const av[] = {
            (char *)vec, "search", (char *)store, vector_path,
            "--exact", "--top", top_buf, NULL
        };
        if (bf_recall_run(av, vec_out, sizeof(vec_out), vec_err, sizeof(vec_err))) {
            fprintf(stderr, "vector recall failed: %s", vec_err);
            rc = 5;
            goto done;
        }
    }

    {
        char *start = vec_out, *end;
        while (*start && isspace((unsigned char)*start)) start++;
        end = start + strlen(start);
        while (end > start && isspace((unsigned char)end[-1])) *--end = '\0';
        if (*start != '{' || end == start || end[-1] != '}') {
            fputs("vector recall returned non-JSON\n", stderr);
            rc = 5;
            goto done;
        }
        fputs("{\"schema\":\"bonfyre.semantic-recall.v1\",\"geometry\":{\"model\":", stdout);
        bf_json_stdout(model);
        fputs(",\"dims\":768,\"metric\":\"cosine\"},\"truth\":\"candidate_only\",\"result\":", stdout);
        fputs(start, stdout);
        for (int i = 2; i < argc; i++)
            if (!strcmp(argv[i], "--passages")) { print_passages(store, start); break; }
        if (hybrid && print_fused(store, start, text, top_k)) {
            fputs("}\n", stdout);
            rc = 7;
            goto done;
        }
        fputs(",\"cannot_infer\":[\"semantic proximity -> exact identity\","
              "\"candidate -> publication or visibility\","
              "\"candidate -> evidence, authority or effect\"]}\n", stdout);
    }

done:
    if (query_fd >= 0) close(query_fd);
    if (vector_fd >= 0) close(vector_fd);
    if (query_path[0]) unlink(query_path);
    if (vector_path[0]) unlink(vector_path);
    return rc;
}
