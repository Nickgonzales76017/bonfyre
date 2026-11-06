#define _POSIX_C_SOURCE 200809L
#include "recall.h"
#include "bonfyre.h"

#include <ctype.h>
#include <sqlite3.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <unistd.h>

static int has_flag(int argc, char **argv, const char *name) {
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], name)) return 1;
    return 0;
}

static const char *coltext(sqlite3_stmt *st, int col) {
    const unsigned char *text = sqlite3_column_text(st, col);
    return text ? (const char *)text : "";
}

static void normalize_text(const char *in, char *out, size_t cap) {
    size_t used = 0;
    int pending_space = 0;
    if (!cap) return;
    for (; in && *in; in++) {
        unsigned char c = (unsigned char)*in;
        if (isspace(c)) {
            if (used) pending_space = 1;
            continue;
        }
        if (pending_space && used + 1 < cap) out[used++] = ' ';
        pending_space = 0;
        if (used + 1 < cap) out[used++] = (char)c;
    }
    out[used] = '\0';
}

static int same_as_prior(char parts[][4096], int count, const char *candidate) {
    for (int i = 0; i < count; i++)
        if (!strcmp(parts[i], candidate)) return 1;
    return 0;
}

static void append_line(char *out, size_t cap, const char *line) {
    size_t used = strlen(out), len = strlen(line);
    if (!len || used + len + 2 > cap) return;
    if (used) out[used++] = '\n';
    memcpy(out + used, line, len + 1);
}

static void fragment_signature(sqlite3_stmt *st, char *out, size_t cap) {
    char parts[4][4096];
    int count = 0;
    out[0] = '\0';
    for (int col = 7; col <= 10; col++) {
        char normalized[4096];
        normalize_text(coltext(st, col), normalized, sizeof(normalized));
        if (!normalized[0] || same_as_prior(parts, count, normalized)) continue;
        snprintf(parts[count], sizeof(parts[count]), "%s", normalized);
        append_line(out, cap, parts[count]);
        count++;
    }
    {
        char tags[4096];
        normalize_text(coltext(st, 13), tags, sizeof(tags));
        if (tags[0]) append_line(out, cap, tags);
    }
}

static void unlink_store(const char *store) {
    char sidecar[PATH_MAX];
    unlink(store);
    snprintf(sidecar, sizeof(sidecar), "%s-shm", store);
    unlink(sidecar);
    snprintf(sidecar, sizeof(sidecar), "%s-wal", store);
    unlink(sidecar);
}

/* The same passages, findable by their words.  Meaning alone misses a name, a
 * number or an exact phrase, and the embedding vocabulary cannot see some
 * writing at all; the word index holds every passage that was offered.  With
 * no input it is built from what the store already holds, so an older store
 * gains it without being embedded again. */
int bf_recall_word_index(const char *store, const char *input) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    char *message = NULL, *line = NULL;
    size_t cap = 0;
    ssize_t n;
    FILE *in = NULL;
    int rc = sqlite3_open_v2(store, &db, SQLITE_OPEN_READWRITE, NULL);
    if (rc == SQLITE_OK)
        rc = sqlite3_exec(db,
            "BEGIN;DROP TABLE IF EXISTS passage_words;DROP TABLE IF EXISTS passage_text;"
            "CREATE TABLE passage_text(id TEXT PRIMARY KEY, text TEXT NOT NULL);", NULL, NULL, &message);
    if (rc == SQLITE_OK && input) {
        in = fopen(input, "r");
        rc = in ? sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO passage_text(id, text) "
            "SELECT json_extract(?1,'$.id'), json_extract(?1,'$.text') "
            "WHERE json_valid(?1) AND json_extract(?1,'$.id') IS NOT NULL AND json_extract(?1,'$.text') IS NOT NULL",
            -1, &st, NULL) : SQLITE_CANTOPEN;
        while (rc == SQLITE_OK && (n = getline(&line, &cap, in)) >= 0) {
            sqlite3_reset(st);
            sqlite3_bind_text(st, 1, line, (int)n, SQLITE_STATIC);
            if (sqlite3_step(st) != SQLITE_DONE) rc = SQLITE_ERROR;
        }
        free(line);
        if (in) fclose(in);
        if (st) sqlite3_finalize(st);
    } else if (rc == SQLITE_OK) {
        rc = sqlite3_exec(db, "INSERT INTO passage_text(id, text) SELECT id, text FROM artifacts;", NULL, NULL, &message);
    }
    if (rc == SQLITE_OK)
        rc = sqlite3_exec(db,
            "CREATE VIRTUAL TABLE passage_words USING fts5(id UNINDEXED, text, tokenize='porter unicode61');"
            "INSERT INTO passage_words(id, text) SELECT id, text FROM passage_text;COMMIT;",
            NULL, NULL, &message);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "word index failed: %s\n", message ? message : (db ? sqlite3_errmsg(db) : "open failed"));
        sqlite3_free(message);
        if (db) sqlite3_close(db);
        return 8;
    }
    sqlite3_close(db);
    return 0;
}

/* declined, when given, receives the number of passages the embedder said its
 * vocabulary cannot see.  Those are not an error: they are absent from the
 * vectors, present in the word index, and reported.  A caller that passes NULL
 * accepts nothing short of every passage embedded. */
static int apply_jsonl(const char *store, const char *input, const char *model,
                       int expected, int *actual, int *declined) {
    int invisible = 0;
    char embed_buf[PATH_MAX], vec_buf[PATH_MAX];
    char out[262144], err[16384];
    const char *embed = bf_recall_binpath("BONFYRE_EMBED_BINARY", "bonfyre-embed",
                                          embed_buf, sizeof(embed_buf));
    const char *vec = bf_recall_binpath("BONFYRE_VEC_BINARY", "bonfyre-vec",
                                        vec_buf, sizeof(vec_buf));
    int artifacts = -1, vectors = -1;
    if (access(embed, X_OK) || access(vec, X_OK)) return 3;
    unlink_store(store);
    if (expected > 0) {
        char *const av[] = {
            (char *)embed, "--input-jsonl", (char *)input,
            "--backend", "onnx", "--model", (char *)model,
            "--dims", "768", "--insert-db", (char *)store, NULL
        };
        if (bf_recall_run(av, out, sizeof(out), err, sizeof(err))) {
            fprintf(stderr, "learned corpus indexing refused: %s", err);
            return 5;
        }
        {
            const char *said = strstr(out, "\"invisible_to_vocabulary\":");
            if (said) invisible = atoi(said + 26);
        }
    } else {
        char *const av[] = {(char *)vec, "init", (char *)store, NULL};
        if (bf_recall_run(av, out, sizeof(out), err, sizeof(err))) {
            fprintf(stderr, "empty vector store init failed: %s", err);
            return 4;
        }
    }
    {
        char *const av[] = {(char *)vec, "count", (char *)store, NULL};
        if (bf_recall_run(av, out, sizeof(out), err, sizeof(err))) {
            fprintf(stderr, "vector count failed: %s", err);
            return 6;
        }
    }
    if (declined) *declined = 0;
    if (sscanf(out, "artifacts: %d, vectors: %d", &artifacts, &vectors) == 2 && declined &&
        artifacts == vectors && invisible > 0 && vectors == expected - invisible) {
        *declined = invisible;
    } else if (sscanf(out, "artifacts: %d, vectors: %d", &artifacts, &vectors) != 2 ||
        artifacts != vectors || vectors != expected) {
        fprintf(stderr, "vector count mismatch: expected %d, got artifacts=%d vectors=%d\n",
                expected, artifacts, vectors);
        return 7;
    }
    if (actual) *actual = vectors;
    return bf_recall_word_index(store, expected > 0 ? input : NULL);
}

int bf_recall_index_jsonl(int argc, char **argv) {
    const char *store = bf_opt(argc, argv, "--store");
    const char *input = bf_opt(argc, argv, "--input-jsonl");
    const char *model = bf_opt(argc, argv, "--model");
    int apply = has_flag(argc, argv, "--apply");
    int expected, actual = 0, rc;
    if (!store || !*store || !input || !*input) {
        fputs("index-jsonl requires --store DB --input-jsonl FILE [--apply]\n", stderr);
        return 2;
    }
    if (!model || !*model) model = "nomic-embed-text-onnx";
    expected = bf_nonempty_jsonl_count(input);
    if (expected < 0) return 3;
    if (!apply) {
        printf("{\"schema\":\"bonfyre.semantic-index-preview.v1\",\"apply\":false,"
               "\"input_count\":%d,\"store\":", expected);
        bf_json_stdout(store);
        fputs(",\"geometry\":{\"model\":", stdout);
        bf_json_stdout(model);
        fputs(",\"dims\":768,\"metric\":\"cosine\"},\"truth\":\"candidate_store\"}\n", stdout);
        return 0;
    }
    rc = apply_jsonl(store, input, model, expected, &actual, NULL);
    if (rc) return rc;
    printf("{\"schema\":\"bonfyre.semantic-index-result.v1\",\"apply\":true,"
           "\"input_count\":%d,\"vector_count\":%d,\"verified_complete\":true,"
           "\"publication_effect\":false,\"store\":", expected, actual);
    bf_json_stdout(store);
    fputs(",\"geometry\":{\"model\":", stdout);
    bf_json_stdout(model);
    fputs(",\"dims\":768,\"metric\":\"cosine\"},\"truth\":\"candidate_store\","
          "\"cannot_infer\":[\"indexed row -> canonical truth\","
          "\"vector presence -> publication, visibility, authority or effect\"]}\n", stdout);
    return 0;
}

int bf_recall_index_fragments(int argc, char **argv) {
    const char *cms_db = bf_opt(argc, argv, "--cms-db");
    const char *store = bf_opt(argc, argv, "--store");
    const char *work_dir = bf_opt(argc, argv, "--work-dir");
    const char *namespace = bf_opt(argc, argv, "--namespace");
    const char *status = bf_opt(argc, argv, "--status");
    const char *model = bf_opt(argc, argv, "--model");
    int apply = has_flag(argc, argv, "--apply");
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    FILE *jsonl = NULL, *manifest = NULL;
    char input_path[PATH_MAX], manifest_path[PATH_MAX], source_db[PATH_MAX];
    char manifest_sha[65];
    int queried = 0, indexable = 0, skipped = 0, first = 1, actual = 0, rc = 0;
    const char *sql =
        "SELECT f.id,COALESCE(f.coordinate,''),COALESCE(f.page,''),COALESCE(f.slot,''),"
        "COALESCE(f.audience,''),COALESCE(f.kind,''),COALESCE(f.status,''),"
        "COALESCE(f.heading,''),COALESCE(f.eyebrow,''),COALESCE(f.body,''),"
        "COALESCE(f.label,''),COALESCE(f.source_ref,''),COALESCE(f.source_digest,''),"
        "COALESCE((SELECT group_concat(value,' ') FROM json_each("
        "CASE WHEN json_valid(f.semantic_tags) THEN f.semantic_tags ELSE '[]' END) "
        "WHERE type='text' AND value NOT IN ('migration','compiled-literal') "
        "AND value NOT GLOB 'function:*' AND value NOT GLOB 'role:*' "
        "AND value NOT GLOB 'equivalence:*'),'') "
        "FROM site_fragment f WHERE f.namespace=?1 AND (?2='all' OR f.status=?2) ORDER BY f.id";
    if (!cms_db || !*cms_db || !store || !*store || !work_dir || !*work_dir) {
        fputs("index-fragments requires --cms-db DB --store DB --work-dir DIR [--apply]\n", stderr);
        return 2;
    }
    if (!namespace || !*namespace) namespace = "aurekai";
    if (!status || !*status) status = "published";
    if (strcmp(status, "all") && strcmp(status, "draft") &&
        strcmp(status, "published") && strcmp(status, "archived")) {
        fputs("status must be draft, published, archived, or all\n", stderr);
        return 2;
    }
    if (!model || !*model) model = "nomic-embed-text-onnx";
    if (bf_mkdir_p(work_dir)) return 3;
    snprintf(input_path, sizeof(input_path), "%s/fragments.jsonl", work_dir);
    snprintf(manifest_path, sizeof(manifest_path), "%s/manifest.json", work_dir);
    jsonl = fopen(input_path, "w");
    manifest = fopen(manifest_path, "w");
    if (!jsonl || !manifest) { rc = 3; goto done; }
    if (sqlite3_open_v2(cms_db, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "cannot read CMS fragments: %s\n", db ? sqlite3_errmsg(db) : "open failed");
        rc = 4;
        goto done;
    }
    sqlite3_bind_text(st, 1, namespace, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, status, -1, SQLITE_TRANSIENT);
    if (!realpath(cms_db, source_db)) snprintf(source_db, sizeof(source_db), "%s", cms_db);
    fputs("{\"schema_version\":\"bonfyre.surface.semantic-index.v2\","
          "\"canonical_owner\":\"BonfyreCMS\",\"semantic_owner\":\"BonfyreRecall\","
          "\"source_db\":", manifest);
    bf_json_file(manifest, source_db);
    fputs(",\"namespace\":", manifest); bf_json_file(manifest, namespace);
    fputs(",\"status\":", manifest); bf_json_file(manifest, status);
    fputs(",\"geometry\":{\"model\":", manifest); bf_json_file(manifest, model);
    fputs(",\"dims\":768,\"metric\":\"cosine\"},\"entries\":{", manifest);

    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *coordinate = coltext(st, 1);
        char signature[16384], key[65], sig_hash[65];
        queried++;
        fragment_signature(st, signature, sizeof(signature));
        if (!coordinate[0] || !signature[0]) { skipped++; continue; }
        bf_sha256_hex((const uint8_t *)coordinate, strlen(coordinate), key);
        key[24] = '\0';
        bf_sha256_hex((const uint8_t *)signature, strlen(signature), sig_hash);
        fputs("{\"id\":", jsonl); bf_json_file(jsonl, key);
        fputs(",\"text\":", jsonl); bf_json_file(jsonl, signature);
        fputs("}\n", jsonl);
        if (!first) fputc(',', manifest);
        first = 0;
        bf_json_file(manifest, key);
        fprintf(manifest, ":{\"id\":%lld,\"coordinate\":", sqlite3_column_int64(st, 0));
        bf_json_file(manifest, coordinate);
        fputs(",\"page\":", manifest); bf_json_file(manifest, coltext(st, 2));
        fputs(",\"slot\":", manifest); bf_json_file(manifest, coltext(st, 3));
        fputs(",\"audience\":", manifest); bf_json_file(manifest, coltext(st, 4));
        fputs(",\"kind\":", manifest); bf_json_file(manifest, coltext(st, 5));
        fputs(",\"status\":", manifest); bf_json_file(manifest, coltext(st, 6));
        fputs(",\"source_ref\":", manifest); bf_json_file(manifest, coltext(st, 11));
        fputs(",\"source_digest\":", manifest); bf_json_file(manifest, coltext(st, 12));
        fputs(",\"signature_sha256\":", manifest); bf_json_file(manifest, sig_hash);
        fputc('}', manifest);
        indexable++;
    }
    fprintf(manifest, "},\"queried_count\":%d,\"indexable_count\":%d,\"skipped_empty\":%d,"
            "\"forbidden_inference\":\"semantic proximity is not exact address equality, "
            "publication authority, effect authority, evidence, or permission to overwrite "
            "canonical content\"}\n", queried, indexable, skipped);
    fclose(jsonl); jsonl = NULL;
    fclose(manifest); manifest = NULL;
    if (bf_sha256_file(manifest_path, manifest_sha)) { rc = 5; goto done; }

    if (apply) {
        rc = apply_jsonl(store, input_path, model, indexable, &actual, NULL);
        if (rc) goto done;
    }
    fputs("{\"schema\":\"bonfyre.surface.semantic-index-build.v2\",\"apply\":", stdout);
    fputs(apply ? "true" : "false", stdout);
    printf(",\"queried_count\":%d,\"indexable_count\":%d,\"skipped_empty\":%d",
           queried, indexable, skipped);
    if (apply) printf(",\"vector_count\":%d,\"verified_complete\":true", actual);
    fputs(",\"publication_effect\":false,\"manifest\":", stdout); bf_json_stdout(manifest_path);
    fputs(",\"manifest_sha256\":", stdout); bf_json_stdout(manifest_sha);
    fputs(",\"vec_db\":", stdout); bf_json_stdout(store);
    fputs(",\"geometry\":{\"model\":", stdout); bf_json_stdout(model);
    fputs(",\"dims\":768,\"metric\":\"cosine\"},\"truth\":\"candidate_store\"}\n", stdout);

done:
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    if (jsonl) fclose(jsonl);
    if (manifest) fclose(manifest);
    return rc;
}

/* ---------- index-sources: anything readable -> one candidate store ----------
 * Recall does not cut text itself.  Files and ingest artifacts go to
 * BonfyreParagraph as text, CMS records go to it as records, and the passages
 * it returns are what gets embedded. */

static int append_file(FILE *out, const char *path, int *lines) {
    FILE *in = fopen(path, "r");
    int c, last = '\n';
    if (!in) return 1;
    while ((c = fgetc(in)) != EOF) {
        fputc(c, out);
        if (c == '\n') (*lines)++;
        last = c;
    }
    if (last != '\n') { fputc('\n', out); (*lines)++; }
    fclose(in);
    return 0;
}

static int run_paragraph(const char *mode, const char *input, const char *passages, const char *label) {
    char bin_buf[PATH_MAX], out[4096], err[8192];
    const char *bin = bf_recall_binpath("BONFYRE_PARAGRAPH_BINARY", "bonfyre-paragraph",
                                        bin_buf, sizeof(bin_buf));
    char *const av[] = {(char *)bin, (char *)mode, (char *)input, "--passages", (char *)passages,
                        "--source-id", (char *)label, NULL};
    if (access(bin, X_OK)) { fputs("passage organ (bonfyre-paragraph) unavailable\n", stderr); return 3; }
    if (bf_recall_run(av, out, sizeof(out), err, sizeof(err))) {
        fprintf(stderr, "passage cutting refused for %s: %s", input, err);
        return 5;
    }
    return 0;
}

/* An ingest artifact directory names its own normalized text. */
static int ingest_text_path(const char *dir, char *path, size_t cap) {
    char manifest[PATH_MAX], line[2048], name[1024] = "";
    int is_text = 0;
    FILE *f;
    snprintf(manifest, sizeof(manifest), "%s/intake-manifest.json", dir);
    f = fopen(manifest, "r");
    if (!f) return 1;
    while (fgets(line, sizeof(line), f)) {
        char *value = strchr(line, ':');
        if (!value) continue;
        if (strstr(line, "\"normalized_path\"")) sscanf(value, ": \"%1023[^\"]", name);
        if (strstr(line, "\"normalized_media_type\"") && strstr(value, "text/plain")) is_text = 1;
    }
    fclose(f);
    if (!name[0] || !is_text) return 1;
    snprintf(path, cap, "%s/%s", dir, name);
    return 0;
}

static int records_to_jsonl(const char *db_path, const char *type, const char *ns,
                            const char *out_path, int *rows) {
    sqlite3 *db = NULL;
    sqlite3_stmt *st = NULL;
    FILE *out = NULL;
    char sql[8192], uri[PATH_MAX + 32], cols[64][128];
    int ncols = 0, has_ns = 0, rc = 4;
    size_t used;
    for (const char *c = type; *c; c++)
        if (!isalnum((unsigned char)*c) && *c != '_') { fputs("record type must be an identifier\n", stderr); return 2; }
    snprintf(sql, sizeof(sql), "PRAGMA table_info(%s)", type);
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK ||
        sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        /* A copied store still in WAL mode opens only as an immutable file. */
        if (st) sqlite3_finalize(st);
        st = NULL;
        if (db) sqlite3_close(db);
        db = NULL;
        snprintf(uri, sizeof(uri), "file:%s?immutable=1", db_path);
        if (sqlite3_open_v2(uri, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL) != SQLITE_OK ||
            sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
            fprintf(stderr, "cannot read record store %s: %s\n", db_path, db ? sqlite3_errmsg(db) : "open failed");
            goto done;
        }
    }
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name = coltext(st, 1), *decl = coltext(st, 2);
        if (!strcmp(name, "namespace")) { has_ns = 1; continue; }
        if (strcasecmp(decl, "TEXT") || !strcmp(name, "created_at") || !strcmp(name, "updated_at") ||
            !strcmp(name, "published_at")) continue;
        if (ncols < 64) snprintf(cols[ncols++], sizeof(cols[0]), "%s", name);
    }
    sqlite3_finalize(st);
    st = NULL;
    if (!ncols) { fprintf(stderr, "record type %s has no text fields in %s\n", type, db_path); goto done; }
    used = (size_t)snprintf(sql, sizeof(sql), "SELECT id");
    for (int i = 0; i < ncols; i++)
        used += (size_t)snprintf(sql + used, sizeof(sql) - used, ",COALESCE(\"%s\",'')", cols[i]);
    snprintf(sql + used, sizeof(sql) - used, " FROM %s%s ORDER BY id", type,
             ns && has_ns ? " WHERE namespace=?1" : "");
    if (ns && !has_ns) { fprintf(stderr, "record type %s has no namespace to filter on\n", type); goto done; }
    if (sqlite3_prepare_v2(db, sql, -1, &st, NULL) != SQLITE_OK) {
        fprintf(stderr, "cannot read %s records: %s\n", type, sqlite3_errmsg(db));
        goto done;
    }
    if (ns) sqlite3_bind_text(st, 1, ns, -1, SQLITE_TRANSIENT);
    out = fopen(out_path, "w");
    if (!out) goto done;
    while (sqlite3_step(st) == SQLITE_ROW) {
        char id[256];
        int wrote = 0;
        /* Short fields read as labelled lines, long ones as body text. */
        for (int pass = 0; pass < 2; pass++)
            for (int i = 0; i < ncols; i++) {
                const char *value = coltext(st, i + 1);
                size_t len = strlen(value);
                char *escaped, *body;
                if (!len || (len <= 200) != (pass == 0)) continue;
                if (!wrote) {
                    snprintf(id, sizeof(id), "%s:%lld", type, sqlite3_column_int64(st, 0));
                    fputs("{\"id\":", out); bf_json_file(out, id);
                    fputs(",\"text\":\"", out);
                }
                body = malloc(len + strlen(cols[i]) + 8);
                if (!body) goto done;
                if (pass == 0) sprintf(body, "%s%s: %s", wrote ? "\n" : "", cols[i], value);
                else sprintf(body, "%s%s", wrote ? "\n\n" : "", value);
                /* bf_json_file writes a quoted string; strip its quotes to append. */
                { char *mem = NULL; size_t mlen = 0; FILE *ms = open_memstream(&mem, &mlen);
                  if (!ms) { free(body); goto done; }
                  bf_json_file(ms, body); fclose(ms);
                  escaped = mem; fwrite(escaped + 1, 1, mlen - 2, out); free(escaped); }
                free(body);
                wrote = 1;
            }
        if (wrote) { fputs("\"}\n", out); (*rows)++; }
    }
    rc = 0;
done:
    if (out) fclose(out);
    if (st) sqlite3_finalize(st);
    if (db) sqlite3_close(db);
    return rc;
}

int bf_recall_index_sources(int argc, char **argv) {
    const char *store = bf_opt(argc, argv, "--store");
    const char *work_dir = bf_opt(argc, argv, "--work-dir");
    const char *model = bf_opt(argc, argv, "--model");
    int apply = has_flag(argc, argv, "--apply");
    char combined[PATH_MAX], part[PATH_MAX], records[PATH_MAX];
    FILE *out, *report;
    char *report_text = NULL;
    size_t report_len = 0;
    int total = 0, sources = 0, actual = 0, declined = 0, rc = 0;
    if (!store || !*store || !work_dir || !*work_dir) {
        fputs("index-sources requires --store DB --work-dir DIR and at least one of\n"
              "  --text FILE|INGEST_DIR[=LABEL]   --records CMS_DB:TYPE[:NAMESPACE]   --jsonl FILE\n", stderr);
        return 2;
    }
    if (!model || !*model) model = "nomic-embed-text-onnx";
    if (bf_mkdir_p(work_dir)) return 3;
    snprintf(combined, sizeof(combined), "%s/passages.jsonl", work_dir);
    out = fopen(combined, "w");
    report = open_memstream(&report_text, &report_len);
    if (!out || !report) return 3;
    for (int i = 2; i + 1 < argc && !rc; i++) {
        int before = total, record_rows = -1;
        const char *kind = argv[i] + 2;
        char arg[PATH_MAX];
        if (strcmp(argv[i], "--text") && strcmp(argv[i], "--records") && strcmp(argv[i], "--jsonl")) continue;
        snprintf(arg, sizeof(arg), "%s", argv[++i]);
        snprintf(part, sizeof(part), "%s/part-%d.jsonl", work_dir, sources);
        if (!strcmp(kind, "jsonl")) {
            if (append_file(out, arg, &total)) { fprintf(stderr, "cannot read %s\n", arg); rc = 3; }
        } else if (!strcmp(kind, "text")) {
            char path[PATH_MAX], *label = strrchr(arg, '=');
            struct stat st;
            if (label) *label++ = '\0';
            if (stat(arg, &st)) { fprintf(stderr, "cannot read %s\n", arg); rc = 3; break; }
            if (S_ISDIR(st.st_mode)) {
                if (ingest_text_path(arg, path, sizeof(path))) {
                    fprintf(stderr, "%s is not an ingest artifact with normalized text\n", arg);
                    rc = 3;
                    break;
                }
            } else snprintf(path, sizeof(path), "%s", arg);
            if (!label || !*label) { label = strrchr(arg, '/'); label = label ? label + 1 : arg; }
            rc = run_paragraph("--input", path, part, label);
            if (!rc && append_file(out, part, &total)) rc = 3;
        } else {
            char *type = strrchr(arg, '/'), *ns;
            type = strchr(type ? type : arg, ':');
            if (!type) { fputs("--records takes CMS_DB:TYPE[:NAMESPACE]\n", stderr); rc = 2; break; }
            *type++ = '\0';
            ns = strchr(type, ':');
            if (ns) *ns++ = '\0';
            snprintf(records, sizeof(records), "%s/records-%d.jsonl", work_dir, sources);
            record_rows = 0;
            rc = records_to_jsonl(arg, type, ns, records, &record_rows);
            if (!rc && record_rows) rc = run_paragraph("--input-jsonl", records, part, type);
            if (!rc && record_rows && append_file(out, part, &total)) rc = 3;
            snprintf(arg, sizeof(arg), "%s", type);
        }
        if (rc) break;
        fprintf(report, "%s{\"kind\":\"%s\",\"source\":", sources ? "," : "", kind);
        bf_json_file(report, arg);
        if (record_rows >= 0) fprintf(report, ",\"records\":%d", record_rows);
        fprintf(report, ",\"passages\":%d}", total - before);
        sources++;
    }
    fclose(out);
    fclose(report);
    if (!rc && !sources) { fputs("index-sources was given no source\n", stderr); rc = 2; }
    if (!rc && apply) rc = apply_jsonl(store, combined, model, total, &actual, &declined);
    if (!rc) {
        printf("{\"schema\":\"bonfyre.semantic-index-result.v1\",\"apply\":%s,\"sources\":[%s],"
               "\"input_count\":%d", apply ? "true" : "false", report_text, total);
        if (apply) printf(",\"vector_count\":%d,\"verified_complete\":%s", actual, declined ? "false" : "true");
        if (declined)
            printf(",\"not_embedded\":%d,\"not_embedded_reason\":\"invisible to the embedding vocabulary; "
                   "held in the word index and found by --hybrid only\"", declined);
        fputs(",\"publication_effect\":false,\"passages\":", stdout); bf_json_stdout(combined);
        fputs(",\"store\":", stdout); bf_json_stdout(store);
        fputs(",\"geometry\":{\"model\":", stdout); bf_json_stdout(model);
        fputs(",\"dims\":768,\"metric\":\"cosine\"},\"truth\":\"candidate_store\","
              "\"cannot_infer\":[\"indexed row -> canonical truth\","
              "\"vector presence -> publication, visibility, authority or effect\"]}\n", stdout);
    }
    free(report_text);
    return rc;
}
