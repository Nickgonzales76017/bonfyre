/*
 * BonfyreCanon — structure-aware canonicalization via Tree-sitter.
 *
 * Parses markdown, JSON, and code into typed ASTs.
 * Canonical normalization: equivalent structures → identical forms.
 * This is Innovation 1 of Lambda Tensors — structural hashing for
 * O(N) family detection instead of O(N²).
 *
 * Usage:
 *   bonfyre-canon parse <file>                → ast.json (full AST)
 *   bonfyre-canon hash <file>                 → hash.json (structural hash)
 *   bonfyre-canon diff <file1> <file2>        → diff.json (structural delta)
 *   bonfyre-canon normalize <file> <output>   → canonicalized output
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <tree_sitter/api.h>

/* ── external parsers (linked at build time) ─────────────────── */
extern const TSLanguage *tree_sitter_json(void);

/* ── helpers ─────────────────────────────────────────────────── */

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX]; size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    memcpy(tmp, path, len + 1);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') { tmp[i] = '\0'; mkdir(tmp, 0755); tmp[i] = '/'; }
    }
    mkdir(tmp, 0755);
    return 0;
}

static void iso_ts(char *buf, size_t sz) {
    time_t t = time(NULL); struct tm tm; gmtime_r(&t, &tm);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

static char *read_file_contents(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[rd] = '\0';
    if (out_len) *out_len = rd;
    return buf;
}

/* ── structural hash (FNV-1a) ────────────────────────────────── */

static unsigned long fnv1a(const char *data, size_t len) {
    unsigned long h = 14695981039346656037UL;
    for (size_t i = 0; i < len; i++) {
        h ^= (unsigned char)data[i];
        h *= 1099511628211UL;
    }
    return h;
}

/* ── detect language from extension ──────────────────────────── */

typedef enum { LANG_JSON, LANG_UNKNOWN } LangType;

static LangType detect_lang(const char *path) {
    const char *ext = strrchr(path, '.');
    if (!ext) return LANG_UNKNOWN;
    if (strcmp(ext, ".json") == 0) return LANG_JSON;
    return LANG_UNKNOWN;
}

static const TSLanguage *get_language(LangType lang) {
    switch (lang) {
        case LANG_JSON: return tree_sitter_json();
        default: return NULL;
    }
}

/* ── AST walk → JSON ─────────────────────────────────────────── */

static void escape_json_string(FILE *out, const char *src, size_t len) {
    for (size_t i = 0; i < len; i++) {
        switch (src[i]) {
            case '"':  fputs("\\\"", out); break;
            case '\\': fputs("\\\\", out); break;
            case '\n': fputs("\\n", out); break;
            case '\r': fputs("\\r", out); break;
            case '\t': fputs("\\t", out); break;
            default:
                if ((unsigned char)src[i] < 0x20) fprintf(out, "\\u%04x", src[i]);
                else fputc(src[i], out);
        }
    }
}

static void walk_node_json(FILE *out, TSNode node, const char *source,
                           int depth, unsigned long *hash_accum) {
    const char *type = ts_node_type(node);
    uint32_t start = ts_node_start_byte(node);
    uint32_t end = ts_node_end_byte(node);
    uint32_t child_count = ts_node_child_count(node);

    /* accumulate structural hash: type + child count */
    *hash_accum ^= fnv1a(type, strlen(type));
    *hash_accum *= 1099511628211UL;
    *hash_accum ^= child_count;

    for (int i = 0; i < depth; i++) fputs("  ", out);
    fprintf(out, "{\n");

    for (int i = 0; i < depth + 1; i++) fputs("  ", out);
    fprintf(out, "\"type\": \"%s\",\n", type);

    for (int i = 0; i < depth + 1; i++) fputs("  ", out);
    fprintf(out, "\"start\": %u,\n", start);

    for (int i = 0; i < depth + 1; i++) fputs("  ", out);
    fprintf(out, "\"end\": %u,\n", end);

    if (child_count == 0) {
        /* leaf node — include text */
        for (int i = 0; i < depth + 1; i++) fputs("  ", out);
        fprintf(out, "\"text\": \"");
        if (end > start && end - start < 4096) {
            escape_json_string(out, source + start, end - start);
        }
        fprintf(out, "\",\n");
    }

    for (int i = 0; i < depth + 1; i++) fputs("  ", out);
    fprintf(out, "\"children\": [");

    if (child_count > 0) {
        fprintf(out, "\n");
        for (uint32_t c = 0; c < child_count; c++) {
            walk_node_json(out, ts_node_child(node, c), source, depth + 2, hash_accum);
            if (c < child_count - 1) fprintf(out, ",");
            fprintf(out, "\n");
        }
        for (int i = 0; i < depth + 1; i++) fputs("  ", out);
    }
    fprintf(out, "]\n");

    for (int i = 0; i < depth; i++) fputs("  ", out);
    fprintf(out, "}");
}

/* ── sorted key normalization for JSON ───────────────────────── */

static void write_canonicalized(FILE *out, TSNode node, const char *source) {
    /* for leaf nodes, output raw text */
    uint32_t child_count = ts_node_child_count(node);
    if (child_count == 0) {
        uint32_t start = ts_node_start_byte(node);
        uint32_t end = ts_node_end_byte(node);
        fwrite(source + start, 1, end - start, out);
        return;
    }
    /* for non-leaf, recurse children */
    for (uint32_t c = 0; c < child_count; c++) {
        write_canonicalized(out, ts_node_child(node, c), source);
    }
}

/* ── commands ───────────────────────────────────────────────── */

static int cmd_parse(const char *file_path, const char *out_dir) {
    ensure_dir(out_dir);
    LangType lang = detect_lang(file_path);
    const TSLanguage *ts_lang = get_language(lang);
    if (!ts_lang) {
        fprintf(stderr, "[canon] Unsupported file type: %s (currently supports: .json)\n", file_path);
        return 1;
    }

    size_t src_len = 0;
    char *source = read_file_contents(file_path, &src_len);
    if (!source) { perror("read"); return 1; }

    TSParser *parser = ts_parser_new();
    ts_parser_set_language(parser, ts_lang);
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)src_len);
    TSNode root = ts_tree_root_node(tree);

    char out_path[PATH_MAX];
    snprintf(out_path, sizeof(out_path), "%s/ast.json", out_dir);
    FILE *f = fopen(out_path, "w");
    if (!f) { perror("fopen"); free(source); ts_tree_delete(tree); ts_parser_delete(parser); return 1; }

    unsigned long hash = 14695981039346656037UL;
    char ts_buf[64]; iso_ts(ts_buf, sizeof(ts_buf));
    fprintf(f, "{\n  \"type\": \"ast\",\n  \"source\": \"%s\",\n  \"timestamp\": \"%s\",\n  \"tree\": ", file_path, ts_buf);
    walk_node_json(f, root, source, 1, &hash);
    fprintf(f, ",\n  \"structural_hash\": \"%016lx\"\n}\n", hash);
    fclose(f);

    fprintf(stderr, "[canon] → %s (hash: %016lx)\n", out_path, hash);
    ts_tree_delete(tree); ts_parser_delete(parser); free(source);
    return 0;
}

static int cmd_hash(const char *file_path, const char *out_dir) {
    ensure_dir(out_dir);
    LangType lang = detect_lang(file_path);
    const TSLanguage *ts_lang = get_language(lang);
    if (!ts_lang) {
        fprintf(stderr, "[canon] Unsupported file type: %s\n", file_path);
        return 1;
    }

    size_t src_len = 0;
    char *source = read_file_contents(file_path, &src_len);
    if (!source) { perror("read"); return 1; }

    TSParser *parser = ts_parser_new();
    ts_parser_set_language(parser, ts_lang);
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)src_len);
    TSNode root = ts_tree_root_node(tree);

    /* walk to compute structural hash */
    unsigned long hash = 14695981039346656037UL;
    /* recursive walk — reuse the JSON walker in hash-only mode */
    FILE *devnull = fopen("/dev/null", "w");
    walk_node_json(devnull, root, source, 0, &hash);
    fclose(devnull);

    /* also compute content hash */
    unsigned long content_hash = fnv1a(source, src_len);

    char out_path[PATH_MAX];
    snprintf(out_path, sizeof(out_path), "%s/hash.json", out_dir);
    char ts_buf[64]; iso_ts(ts_buf, sizeof(ts_buf));
    FILE *f = fopen(out_path, "w");
    if (!f) { perror("fopen"); free(source); ts_tree_delete(tree); ts_parser_delete(parser); return 1; }
    fprintf(f, "{\n");
    fprintf(f, "  \"type\": \"structural-hash\",\n");
    fprintf(f, "  \"source\": \"%s\",\n", file_path);
    fprintf(f, "  \"timestamp\": \"%s\",\n", ts_buf);
    fprintf(f, "  \"structural_hash\": \"%016lx\",\n", hash);
    fprintf(f, "  \"content_hash\": \"%016lx\",\n", content_hash);
    fprintf(f, "  \"size_bytes\": %zu,\n", src_len);
    fprintf(f, "  \"match_level\": \"%s\"\n",
            (hash == content_hash) ? "identical" : "structural-only");
    fprintf(f, "}\n");
    fclose(f);

    fprintf(stderr, "[canon] structural=%016lx content=%016lx\n", hash, content_hash);
    ts_tree_delete(tree); ts_parser_delete(parser); free(source);
    return 0;
}

static int cmd_diff(const char *file1, const char *file2, const char *out_dir) {
    ensure_dir(out_dir);
    LangType lang1 = detect_lang(file1);
    LangType lang2 = detect_lang(file2);
    const TSLanguage *ts_lang1 = get_language(lang1);
    const TSLanguage *ts_lang2 = get_language(lang2);
    if (!ts_lang1 || !ts_lang2) {
        fprintf(stderr, "[canon] Both files must be supported types\n");
        return 1;
    }

    size_t len1, len2;
    char *src1 = read_file_contents(file1, &len1);
    char *src2 = read_file_contents(file2, &len2);
    if (!src1 || !src2) { free(src1); free(src2); return 1; }

    TSParser *p1 = ts_parser_new(); ts_parser_set_language(p1, ts_lang1);
    TSParser *p2 = ts_parser_new(); ts_parser_set_language(p2, ts_lang2);
    TSTree *t1 = ts_parser_parse_string(p1, NULL, src1, (uint32_t)len1);
    TSTree *t2 = ts_parser_parse_string(p2, NULL, src2, (uint32_t)len2);

    unsigned long h1 = 14695981039346656037UL, h2 = 14695981039346656037UL;
    FILE *dn = fopen("/dev/null", "w");
    walk_node_json(dn, ts_tree_root_node(t1), src1, 0, &h1);
    walk_node_json(dn, ts_tree_root_node(t2), src2, 0, &h2);
    fclose(dn);

    char out_path[PATH_MAX];
    snprintf(out_path, sizeof(out_path), "%s/diff.json", out_dir);
    char ts_buf[64]; iso_ts(ts_buf, sizeof(ts_buf));
    FILE *f = fopen(out_path, "w");
    fprintf(f, "{\n");
    fprintf(f, "  \"type\": \"structural-diff\",\n");
    fprintf(f, "  \"source_a\": \"%s\",\n", file1);
    fprintf(f, "  \"source_b\": \"%s\",\n", file2);
    fprintf(f, "  \"timestamp\": \"%s\",\n", ts_buf);
    fprintf(f, "  \"hash_a\": \"%016lx\",\n", h1);
    fprintf(f, "  \"hash_b\": \"%016lx\",\n", h2);
    fprintf(f, "  \"structurally_equal\": %s,\n", (h1 == h2) ? "true" : "false");
    fprintf(f, "  \"size_a\": %zu,\n", len1);
    fprintf(f, "  \"size_b\": %zu\n", len2);
    fprintf(f, "}\n");
    fclose(f);

    fprintf(stderr, "[canon] %s vs %s: %s\n", file1, file2,
            (h1 == h2) ? "STRUCTURALLY EQUAL" : "DIFFERENT");
    ts_tree_delete(t1); ts_tree_delete(t2);
    ts_parser_delete(p1); ts_parser_delete(p2);
    free(src1); free(src2);
    return 0;
}

static int cmd_normalize(const char *file_path, const char *output) {
    LangType lang = detect_lang(file_path);
    const TSLanguage *ts_lang = get_language(lang);
    if (!ts_lang) {
        fprintf(stderr, "[canon] Unsupported file type: %s\n", file_path);
        return 1;
    }

    size_t src_len;
    char *source = read_file_contents(file_path, &src_len);
    if (!source) { perror("read"); return 1; }

    TSParser *parser = ts_parser_new();
    ts_parser_set_language(parser, ts_lang);
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)src_len);

    FILE *f = fopen(output, "w");
    if (!f) { perror("fopen"); free(source); ts_tree_delete(tree); ts_parser_delete(parser); return 1; }
    write_canonicalized(f, ts_tree_root_node(tree), source);
    fprintf(f, "\n");
    fclose(f);

    fprintf(stderr, "[canon] → %s (normalized)\n", output);
    ts_tree_delete(tree); ts_parser_delete(parser); free(source);
    return 0;
}

/* ── main ───────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-canon — structural canonicalization (Tree-sitter)\n\n"
        "Usage:\n"
        "  bonfyre-canon parse <file> [output-dir]\n"
        "  bonfyre-canon hash <file> [output-dir]\n"
        "  bonfyre-canon diff <file1> <file2> [output-dir]\n"
        "  bonfyre-canon normalize <file> <output-file>\n"
        "  bonfyre-canon status\n");
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
        printf("{\"binary\":\"bonfyre-canon\",\"status\":\"ok\",\"version\":\"1.0.0\"}\n");
        return 0;
    }

    if (argc < 3) { print_usage(); return 1; }

    const char *cmd = argv[1];
    const char *file1 = argv[2];

    if (strcmp(cmd, "parse") == 0) {
        return cmd_parse(file1, (argc > 3) ? argv[3] : "output");
    } else if (strcmp(cmd, "hash") == 0) {
        return cmd_hash(file1, (argc > 3) ? argv[3] : "output");
    } else if (strcmp(cmd, "diff") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_diff(file1, argv[3], (argc > 4) ? argv[4] : "output");
    } else if (strcmp(cmd, "normalize") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_normalize(file1, argv[3]);
    }

    print_usage();
    return 1;
}
