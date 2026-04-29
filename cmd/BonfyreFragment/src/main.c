/*
 * BonfyreFragment — Fragment system CLI
 *
 * Commands:
 *   create   --store DB --kind KIND --persp PERSP --conf FLOAT
 *              [--start-ms N] [--end-ms N] [--payload JSON]
 *              [--parent ID ...]
 *
 *   get      --store DB --id ID
 *
 *   query    --store DB [--kind KIND] [--persp PERSP]
 *              [--min-conf FLOAT] [--start-after-ms N] [--end-before-ms N]
 *              [--limit N] [--offset N] [--format json|table]
 *
 *   merge    --dst DB --src DB
 *
 *   diff     --store-a DB --store-b DB [--kind KIND] [--format json|table]
 *
 *   stats    --store DB
 *
 *   ingest   --store DB --from-run-manifest PATH
 *              Reads a BonfyreRun run-manifest.json and ingests all
 *              pipeline outputs as fragments.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../../lib/libfragment/include/fragment.h"

#define MAX_PATH   4096
#define MAX_PARENTS 8

/* ─────────────────────────────────────────────────────────────────
 * Utilities
 * ───────────────────────────────────────────────────────────────── */

static char *read_stdin(void) {
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) return NULL;
    int c;
    while ((c = fgetc(stdin)) != EOF) {
        if (len + 1 >= cap) {
            cap *= 2;
            char *tmp = realloc(buf, cap);
            if (!tmp) { free(buf); return NULL; }
            buf = tmp;
        }
        buf[len++] = (char)c;
    }
    buf[len] = '\0';
    return buf;
}

static char *read_file(const char *path) {
    FILE *fp = fopen(path, "r");
    if (!fp) return NULL;
    fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    rewind(fp);
    if (sz <= 0) { fclose(fp); return strdup(""); }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return NULL; }
    sz = (long)fread(buf, 1, (size_t)sz, fp);
    buf[sz] = '\0';
    fclose(fp);
    return buf;
}

/* ─────────────────────────────────────────────────────────────────
 * create
 * ───────────────────────────────────────────────────────────────── */

static int cmd_create(int argc, char **argv) {
    const char *store_path = NULL;
    const char *kind       = NULL;
    const char *persp      = NULL;
    float       conf       = 0.5f;
    long long   start_ms   = -1;
    long long   end_ms     = -1;
    const char *payload    = NULL;
    const char *parents[MAX_PARENTS] = {0};
    int         nparents   = 0;
    int         from_stdin = 0;

    for (int i = 0; i < argc; i++) {
        if      (!strcmp(argv[i], "--store")        && i+1 < argc) store_path = argv[++i];
        else if (!strcmp(argv[i], "--kind")         && i+1 < argc) kind       = argv[++i];
        else if (!strcmp(argv[i], "--persp")        && i+1 < argc) persp      = argv[++i];
        else if (!strcmp(argv[i], "--conf")         && i+1 < argc) conf       = (float)atof(argv[++i]);
        else if (!strcmp(argv[i], "--start-ms")     && i+1 < argc) start_ms   = atoll(argv[++i]);
        else if (!strcmp(argv[i], "--end-ms")       && i+1 < argc) end_ms     = atoll(argv[++i]);
        else if (!strcmp(argv[i], "--payload")      && i+1 < argc) payload    = argv[++i];
        else if (!strcmp(argv[i], "--stdin"))                       from_stdin = 1;
        else if (!strcmp(argv[i], "--parent")       && i+1 < argc && nparents < MAX_PARENTS)
            parents[nparents++] = argv[++i];
    }

    if (!store_path || !kind || !persp) {
        fprintf(stderr, "create: --store --kind --persp required\n");
        return 1;
    }

    char *payload_buf = NULL;
    if (from_stdin) {
        payload_buf = read_stdin();
        payload = payload_buf;
    }
    if (!payload) payload = "{}";

    bf_fragment_store_t *store = bf_fragment_store_open(store_path);
    if (!store) {
        fprintf(stderr, "cannot open store: %s\n", store_path);
        return 1;
    }

    char id[BF_FRAGMENT_ID_LEN];
    int rc = bf_fragment_create(store, kind, persp, conf,
                                 (int64_t)start_ms, (int64_t)end_ms,
                                 payload, parents, nparents, id);

    if (rc != 0) {
        fprintf(stderr, "create failed: %s\n", bf_fragment_store_errmsg(store));
        bf_fragment_store_close(store);
        free(payload_buf);
        return 1;
    }

    printf("%s\n", id);

    bf_fragment_store_close(store);
    free(payload_buf);
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * get
 * ───────────────────────────────────────────────────────────────── */

static int cmd_get(int argc, char **argv) {
    const char *store_path = NULL;
    const char *id         = NULL;

    for (int i = 0; i < argc; i++) {
        if      (!strcmp(argv[i], "--store") && i+1 < argc) store_path = argv[++i];
        else if (!strcmp(argv[i], "--id")    && i+1 < argc) id         = argv[++i];
    }

    if (!store_path || !id) {
        fprintf(stderr, "get: --store --id required\n");
        return 1;
    }

    bf_fragment_store_t *store = bf_fragment_store_open(store_path);
    if (!store) { fprintf(stderr, "cannot open store\n"); return 1; }

    bf_fragment_t *frag = bf_fragment_get(store, id);
    if (!frag) {
        fprintf(stderr, "fragment not found: %s\n", id);
        bf_fragment_store_close(store);
        return 1;
    }

    printf("{\n");
    printf("  \"id\": \"%s\",\n", frag->id);
    printf("  \"kind\": \"%s\",\n", frag->kind);
    printf("  \"perspective\": \"%s\",\n", frag->perspective);
    printf("  \"confidence\": %.4f,\n", (double)frag->confidence);
    printf("  \"start_ms\": %lld,\n", (long long)frag->start_ms);
    printf("  \"end_ms\": %lld,\n", (long long)frag->end_ms);
    printf("  \"created_at\": %lld,\n", (long long)frag->created_at);
    printf("  \"payload\": %s\n", frag->payload_json);
    printf("}\n");

    bf_fragment_free(frag);
    bf_fragment_store_close(store);
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * query
 * ───────────────────────────────────────────────────────────────── */

static int cmd_query(int argc, char **argv) {
    const char *store_path  = NULL;
    const char *format      = "table";
    bf_fragment_query_t q   = {0};
    q.start_after_ms = -1;
    q.end_before_ms  = -1;

    for (int i = 0; i < argc; i++) {
        if      (!strcmp(argv[i], "--store")          && i+1 < argc) store_path = argv[++i];
        else if (!strcmp(argv[i], "--kind")           && i+1 < argc) q.kind = argv[++i];
        else if (!strcmp(argv[i], "--persp")          && i+1 < argc) q.perspective = argv[++i];
        else if (!strcmp(argv[i], "--min-conf")       && i+1 < argc) q.min_confidence = (float)atof(argv[++i]);
        else if (!strcmp(argv[i], "--start-after-ms") && i+1 < argc) q.start_after_ms = atoll(argv[++i]);
        else if (!strcmp(argv[i], "--end-before-ms")  && i+1 < argc) q.end_before_ms  = atoll(argv[++i]);
        else if (!strcmp(argv[i], "--limit")          && i+1 < argc) q.limit  = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--offset")         && i+1 < argc) q.offset = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--format")         && i+1 < argc) format = argv[++i];
    }

    if (!store_path) {
        fprintf(stderr, "query: --store required\n");
        return 1;
    }

    bf_fragment_store_t *store = bf_fragment_store_open(store_path);
    if (!store) { fprintf(stderr, "cannot open store\n"); return 1; }

    int count = 0;
    bf_fragment_t **results = bf_fragment_query(store, &q, &count);
    bf_fragment_store_close(store);

    if (!results || count < 0) {
        fprintf(stderr, "query failed\n");
        return 1;
    }

    if (!strcmp(format, "json")) {
        printf("[\n");
        for (int i = 0; i < count; i++) {
            bf_fragment_t *f = results[i];
            printf("  {\"id\":\"%s\",\"kind\":\"%s\",\"perspective\":\"%s\","
                   "\"confidence\":%.4f,\"start_ms\":%lld,\"end_ms\":%lld}%s\n",
                   f->id, f->kind, f->perspective, (double)f->confidence,
                   (long long)f->start_ms, (long long)f->end_ms,
                   (i < count - 1) ? "," : "");
        }
        printf("]\n");
    } else {
        printf("%-6s  %-12s  %-20s  %-6s  %-10s  %-10s\n",
               "COUNT", "KIND", "PERSPECTIVE", "CONF", "START_MS", "END_MS");
        printf("─────────────────────────────────────────────────────────────────\n");
        for (int i = 0; i < count; i++) {
            bf_fragment_t *f = results[i];
            printf("%-6d  %-12s  %-20s  %-6.2f  %-10lld  %-10lld\n",
                   i + 1, f->kind, f->perspective, (double)f->confidence,
                   (long long)f->start_ms, (long long)f->end_ms);
        }
        printf("─────────────────────────────────────────────────────────────────\n");
        printf("%d fragment%s\n", count, count == 1 ? "" : "s");
    }

    for (int i = 0; i < count; i++) bf_fragment_free(results[i]);
    free(results);
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * merge
 * ───────────────────────────────────────────────────────────────── */

static int cmd_merge(int argc, char **argv) {
    const char *dst_path = NULL;
    const char *src_path = NULL;

    for (int i = 0; i < argc; i++) {
        if      (!strcmp(argv[i], "--dst") && i+1 < argc) dst_path = argv[++i];
        else if (!strcmp(argv[i], "--src") && i+1 < argc) src_path = argv[++i];
    }

    if (!dst_path || !src_path) {
        fprintf(stderr, "merge: --dst --src required\n");
        return 1;
    }

    bf_fragment_store_t *dst = bf_fragment_store_open(dst_path);
    bf_fragment_store_t *src = bf_fragment_store_open(src_path);

    if (!dst || !src) {
        fprintf(stderr, "cannot open stores\n");
        bf_fragment_store_close(dst);
        bf_fragment_store_close(src);
        return 1;
    }

    bf_merge_result_t result = {0};
    int rc = bf_fragment_merge(dst, src, &result);

    bf_fragment_store_close(dst);
    bf_fragment_store_close(src);

    if (rc != 0) { fprintf(stderr, "merge failed\n"); return 1; }

    printf("Merge complete:\n");
    printf("  fragments_added:     %d\n", result.fragments_added);
    printf("  conflicts_detected:  %d\n", result.conflicts_detected);
    printf("  conflicts_resolved:  %d\n", result.conflicts_resolved);
    printf("  conflicts_deferred:  %d\n", result.conflicts_deferred);
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * diff
 * ───────────────────────────────────────────────────────────────── */

static int cmd_diff(int argc, char **argv) {
    const char *path_a  = NULL;
    const char *path_b  = NULL;
    const char *kind    = NULL;
    const char *format  = "table";

    for (int i = 0; i < argc; i++) {
        if      (!strcmp(argv[i], "--store-a") && i+1 < argc) path_a = argv[++i];
        else if (!strcmp(argv[i], "--store-b") && i+1 < argc) path_b = argv[++i];
        else if (!strcmp(argv[i], "--kind")    && i+1 < argc) kind   = argv[++i];
        else if (!strcmp(argv[i], "--format")  && i+1 < argc) format = argv[++i];
    }

    if (!path_a || !path_b) {
        fprintf(stderr, "diff: --store-a --store-b required\n");
        return 1;
    }

    bf_fragment_store_t *a = bf_fragment_store_open(path_a);
    bf_fragment_store_t *b = bf_fragment_store_open(path_b);

    if (!a || !b) {
        fprintf(stderr, "cannot open stores\n");
        bf_fragment_store_close(a);
        bf_fragment_store_close(b);
        return 1;
    }

    int count = 0;
    bf_fragment_diff_t *diffs = bf_fragment_diff(a, b, kind, &count);

    bf_fragment_store_close(a);
    bf_fragment_store_close(b);

    if (!diffs && count > 0) { fprintf(stderr, "diff failed\n"); return 1; }
    if (count == 0) { printf("No differences found.\n"); return 0; }

    if (!strcmp(format, "json")) {
        printf("[\n");
        for (int i = 0; i < count; i++) {
            bf_fragment_diff_t *d = &diffs[i];
            printf("  {\"kind\":\"%s\",\"perspective_a\":\"%s\","
                   "\"perspective_b\":\"%s\",\"confidence_a\":%.3f,"
                   "\"confidence_b\":%.3f,\"conflict_score\":%.3f,"
                   "\"summary\":\"%s\"}%s\n",
                   d->kind, d->perspective_a, d->perspective_b,
                   (double)d->confidence_a, (double)d->confidence_b,
                   (double)d->conflict_score,
                   d->summary ? d->summary : "",
                   (i < count - 1) ? "," : "");
        }
        printf("]\n");
    } else {
        printf("%-12s  %-16s  %-6s  %-16s  %-6s  %-8s\n",
               "KIND", "PERSPECTIVE-A", "CONF-A", "PERSPECTIVE-B", "CONF-B", "CONFLICT");
        printf("──────────────────────────────────────────────────────────────────────────\n");
        for (int i = 0; i < count; i++) {
            bf_fragment_diff_t *d = &diffs[i];
            printf("%-12s  %-16s  %-6.2f  %-16s  %-6.2f  %-8.2f\n",
                   d->kind, d->perspective_a, (double)d->confidence_a,
                   d->perspective_b[0] ? d->perspective_b : "(none)",
                   (double)d->confidence_b, (double)d->conflict_score);
        }
        printf("──────────────────────────────────────────────────────────────────────────\n");
        printf("%d difference%s\n", count, count == 1 ? "" : "s");
    }

    bf_fragment_diff_free(diffs, count);
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * stats
 * ───────────────────────────────────────────────────────────────── */

static int cmd_stats(int argc, char **argv) {
    const char *store_path = NULL;

    for (int i = 0; i < argc; i++) {
        if (!strcmp(argv[i], "--store") && i+1 < argc) store_path = argv[++i];
    }

    if (!store_path) { fprintf(stderr, "stats: --store required\n"); return 1; }

    bf_fragment_store_t *store = bf_fragment_store_open(store_path);
    if (!store) { fprintf(stderr, "cannot open store\n"); return 1; }

    bf_fragment_stats_t st;
    int rc = bf_fragment_stats(store, &st);
    bf_fragment_store_close(store);

    if (rc != 0) { fprintf(stderr, "stats failed\n"); return 1; }

    printf("Store: %s\n", store_path);
    printf("  total_fragments:     %d\n", st.total_fragments);
    printf("  unique_kinds:        %d\n", st.unique_kinds);
    printf("  unique_perspectives: %d\n", st.unique_perspectives);
    printf("  mean_confidence:     %.3f\n", (double)st.mean_confidence);
    if (st.earliest_ms >= 0)
        printf("  time_span:           %lld ms → %lld ms\n",
               (long long)st.earliest_ms, (long long)st.latest_ms);
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * ingest (from run-manifest.json)
 * ───────────────────────────────────────────────────────────────── */

static int cmd_ingest(int argc, char **argv) {
    const char *store_path    = NULL;
    const char *manifest_path = NULL;

    for (int i = 0; i < argc; i++) {
        if      (!strcmp(argv[i], "--store")         && i+1 < argc) store_path    = argv[++i];
        else if (!strcmp(argv[i], "--from-run-manifest") && i+1 < argc) manifest_path = argv[++i];
    }

    if (!store_path || !manifest_path) {
        fprintf(stderr, "ingest: --store --from-run-manifest required\n");
        return 1;
    }

    char *manifest = read_file(manifest_path);
    if (!manifest) {
        fprintf(stderr, "cannot read manifest: %s\n", manifest_path);
        return 1;
    }

    bf_fragment_store_t *store = bf_fragment_store_open(store_path);
    if (!store) {
        fprintf(stderr, "cannot open store\n");
        free(manifest);
        return 1;
    }

    /* Extract recipe_id from manifest for perspective tag */
    char recipe_id[64] = "unknown-recipe";
    const char *rp = strstr(manifest, "\"recipe_id\":");
    if (rp) {
        rp = strchr(rp, '"');        /* skip past : */
        if (rp) rp = strchr(rp + 1, '"');  /* find value start */
        if (rp) {
            rp++;
            int ri = 0;
            while (*rp && *rp != '"' && ri < 63) recipe_id[ri++] = *rp++;
            recipe_id[ri] = '\0';
        }
    }

    /* Create one fragment per stage in the manifest */
    char id_out[BF_FRAGMENT_ID_LEN];
    int ingested = 0;
    const char *p = manifest;

    while ((p = strstr(p, "\"id\":")) != NULL) {
        /* Extract stage id */
        const char *q = strchr(p, '"');
        if (!q) break;
        q = strchr(q + 1, '"');
        if (!q) break;
        q++;
        char stage_id[64] = {0};
        int si = 0;
        while (*q && *q != '"' && si < 63) stage_id[si++] = *q++;

        /* Extract exit_code */
        int exit_code = -1;
        const char *ec = strstr(p, "\"exit_code\":");
        if (ec) exit_code = atoi(ec + 12);

        /* Build payload */
        char payload[512];
        snprintf(payload, sizeof(payload),
                 "{\"stage_id\":\"%s\",\"exit_code\":%d,\"recipe_id\":\"%s\"}",
                 stage_id, exit_code, recipe_id);

        float conf = (exit_code == 0) ? 1.0f : 0.0f;

        bf_fragment_create(store, BFK_EVENT,
                           recipe_id, conf,
                           -1, -1,
                           payload, NULL, 0,
                           id_out);
        ingested++;

        p = q;
    }

    bf_fragment_store_close(store);
    free(manifest);

    printf("Ingested %d stage fragments from %s\n", ingested, manifest_path);
    return 0;
}

/* ─────────────────────────────────────────────────────────────────
 * main
 * ───────────────────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-fragment — Fragment system CLI\n\n"
        "Commands:\n"
        "  create  --store DB --kind KIND --persp PERSP --conf FLOAT\n"
        "            [--start-ms N] [--end-ms N] [--payload JSON|--stdin]\n"
        "            [--parent ID ...]\n\n"
        "  get     --store DB --id ID\n\n"
        "  query   --store DB [--kind KIND] [--persp PERSP]\n"
        "            [--min-conf FLOAT] [--start-after-ms N] [--end-before-ms N]\n"
        "            [--limit N] [--offset N] [--format json|table]\n\n"
        "  merge   --dst DB --src DB\n\n"
        "  diff    --store-a DB --store-b DB [--kind KIND] [--format json|table]\n\n"
        "  stats   --store DB\n\n"
        "  ingest  --store DB --from-run-manifest PATH\n");
}

int main(int argc, char **argv) {
    if (argc < 2) { print_usage(); return 1; }

    const char *cmd = argv[1];

    if (!strcmp(cmd, "create"))  return cmd_create(argc - 2, argv + 2);
    if (!strcmp(cmd, "get"))     return cmd_get(argc - 2, argv + 2);
    if (!strcmp(cmd, "query"))   return cmd_query(argc - 2, argv + 2);
    if (!strcmp(cmd, "merge"))   return cmd_merge(argc - 2, argv + 2);
    if (!strcmp(cmd, "diff"))    return cmd_diff(argc - 2, argv + 2);
    if (!strcmp(cmd, "stats"))   return cmd_stats(argc - 2, argv + 2);
    if (!strcmp(cmd, "ingest"))  return cmd_ingest(argc - 2, argv + 2);

    fprintf(stderr, "unknown command: %s\n\n", cmd);
    print_usage();
    return 1;
}
