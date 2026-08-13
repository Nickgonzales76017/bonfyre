/*
 * test_fragment.c — libfragment unit tests
 *
 * Tests: create, get, query, merge, diff, stats
 */

#include "../include/fragment.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PASS(name) printf("  ✓ %s\n", (name))
#define FAIL(name, msg) do { printf("  ✗ %s: %s\n", (name), (msg)); exit(1); } while(0)

static void test_create_get(void) {
    printf("test_create_get\n");

    unlink("/tmp/test_frag_a.db");
    bf_fragment_store_t *store = bf_fragment_store_open("/tmp/test_frag_a.db");
    assert(store && "store open failed");

    char id[BF_FRAGMENT_ID_LEN];
    int rc = bf_fragment_create(store,
                                 BFK_CLAIM, "whisper-base",
                                 0.85f,
                                 1000, 5000,
                                 "{\"text\": \"The cat sat on the mat\"}",
                                 NULL, 0,
                                 id);
    assert(rc == 0 && "create failed");
    assert(id[0] != '\0' && "empty id");
    PASS("create returns id");

    /* Idempotent: same content → same id */
    char id2[BF_FRAGMENT_ID_LEN];
    bf_fragment_create(store, BFK_CLAIM, "whisper-base", 0.85f, 1000, 5000,
                        "{\"text\": \"The cat sat on the mat\"}",
                        NULL, 0, id2);
    assert(strcmp(id, id2) == 0 && "idempotency broken");
    PASS("create is idempotent");

    bf_fragment_t *frag = bf_fragment_get(store, id);
    assert(frag && "get failed");
    assert(strcmp(frag->kind, BFK_CLAIM) == 0 && "kind mismatch");
    assert(strcmp(frag->perspective, "whisper-base") == 0 && "perspective mismatch");
    assert(frag->confidence > 0.84f && frag->confidence < 0.86f && "confidence mismatch");
    assert(frag->start_ms == 1000 && "start_ms mismatch");
    assert(frag->end_ms   == 5000 && "end_ms mismatch");
    assert(frag->payload_json && strstr(frag->payload_json, "cat") && "payload mismatch");
    PASS("get returns correct fragment");

    bf_fragment_free(frag);
    bf_fragment_store_close(store);
    printf("\n");
}

static void test_query(void) {
    printf("test_query\n");

    unlink("/tmp/test_frag_q.db");
    bf_fragment_store_t *store = bf_fragment_store_open("/tmp/test_frag_q.db");
    assert(store);

    char id[BF_FRAGMENT_ID_LEN];
    bf_fragment_create(store, BFK_CLAIM, "whisper-base", 0.9f,
                        0, 3000, "{\"text\":\"A\"}", NULL, 0, id);
    bf_fragment_create(store, BFK_CLAIM, "gpt-4",        0.7f,
                        0, 3000, "{\"text\":\"B\"}", NULL, 0, id);
    bf_fragment_create(store, BFK_EVENT, "scene-detect", 0.8f,
                        0, 2000, "{\"label\":\"cut\"}", NULL, 0, id);

    /* Query all */
    bf_fragment_query_t q = {0};
    q.start_after_ms = -1;
    q.end_before_ms  = -1;
    int count = 0;
    bf_fragment_t **results = bf_fragment_query(store, &q, &count);
    assert(results && count == 3 && "expected 3 fragments");
    PASS("query all returns 3 fragments");
    for (int i = 0; i < count; i++) bf_fragment_free(results[i]);
    free(results);

    /* Query by kind */
    q.kind = BFK_CLAIM;
    results = bf_fragment_query(store, &q, &count);
    assert(results && count == 2 && "expected 2 claims");
    PASS("query by kind returns 2 claims");
    for (int i = 0; i < count; i++) bf_fragment_free(results[i]);
    free(results);

    /* Query by min_confidence */
    q.kind = NULL;
    q.min_confidence = 0.85f;
    results = bf_fragment_query(store, &q, &count);
    assert(results && count == 1 && "expected 1 high-conf fragment");
    assert(results[0]->confidence >= 0.85f && "wrong confidence");
    PASS("query by min_confidence returns 1 fragment");
    for (int i = 0; i < count; i++) bf_fragment_free(results[i]);
    free(results);

    bf_fragment_store_close(store);
    printf("\n");
}

static void test_merge(void) {
    printf("test_merge\n");

    unlink("/tmp/test_frag_m1.db");
    unlink("/tmp/test_frag_m2.db");

    bf_fragment_store_t *store_a = bf_fragment_store_open("/tmp/test_frag_m1.db");
    bf_fragment_store_t *store_b = bf_fragment_store_open("/tmp/test_frag_m2.db");
    assert(store_a && store_b);

    char id[BF_FRAGMENT_ID_LEN];

    /* A: one claim */
    bf_fragment_create(store_a, BFK_CLAIM, "model-A", 0.9f,
                        0, 5000, "{\"text\":\"High confidence claim\"}", NULL, 0, id);

    /* B: two claims — one overlapping with lower conf, one new */
    bf_fragment_create(store_b, BFK_CLAIM, "model-B", 0.6f,
                        0, 5000, "{\"text\":\"Lower confidence claim\"}", NULL, 0, id);
    bf_fragment_create(store_b, BFK_EVENT, "detector", 0.8f,
                        6000, 9000, "{\"label\":\"scene-change\"}", NULL, 0, id);

    bf_merge_result_t result = {0};
    int rc = bf_fragment_merge(store_a, store_b, &result);
    assert(rc == 0 && "merge failed");
    assert(result.fragments_added >= 1 && "nothing added");
    PASS("merge adds fragments from src");

    printf("    added=%d conflicts_detected=%d resolved=%d deferred=%d\n",
           result.fragments_added, result.conflicts_detected,
           result.conflicts_resolved, result.conflicts_deferred);

    /* A now should have 3 fragments total */
    bf_fragment_query_t q = {0};
    q.start_after_ms = -1; q.end_before_ms = -1;
    int count = 0;
    bf_fragment_t **results = bf_fragment_query(store_a, &q, &count);
    assert(count >= 2 && "expected at least 2 after merge");
    PASS("merged store has correct fragment count");
    for (int i = 0; i < count; i++) bf_fragment_free(results[i]);
    free(results);

    bf_fragment_store_close(store_a);
    bf_fragment_store_close(store_b);
    printf("\n");
}

static void test_diff(void) {
    printf("test_diff\n");

    unlink("/tmp/test_frag_d1.db");
    unlink("/tmp/test_frag_d2.db");

    bf_fragment_store_t *a = bf_fragment_store_open("/tmp/test_frag_d1.db");
    bf_fragment_store_t *b = bf_fragment_store_open("/tmp/test_frag_d2.db");
    assert(a && b);

    char id[BF_FRAGMENT_ID_LEN];
    bf_fragment_create(a, BFK_CLAIM, "whisper", 0.9f, 0, 3000,
                        "{\"text\":\"A says this\"}", NULL, 0, id);

    bf_fragment_create(b, BFK_CLAIM, "gpt4",    0.7f, 0, 3000,
                        "{\"text\":\"B says that\"}", NULL, 0, id);

    int count = 0;
    bf_fragment_diff_t *diffs = bf_fragment_diff(a, b, BFK_CLAIM, &count);
    assert(count >= 1 && "expected at least 1 diff");
    assert(diffs[0].summary && "missing diff summary");
    printf("    diff[0]: %s\n", diffs[0].summary);
    PASS("diff detects perspective disagreement");

    bf_fragment_diff_free(diffs, count);
    bf_fragment_store_close(a);
    bf_fragment_store_close(b);
    printf("\n");
}

static void test_stats(void) {
    printf("test_stats\n");

    unlink("/tmp/test_frag_s.db");
    bf_fragment_store_t *store = bf_fragment_store_open("/tmp/test_frag_s.db");
    assert(store);

    char id[BF_FRAGMENT_ID_LEN];
    bf_fragment_create(store, BFK_CLAIM, "p1", 0.9f, 0,    5000, "{\"a\":1}", NULL, 0, id);
    bf_fragment_create(store, BFK_CLAIM, "p2", 0.7f, 0,    5000, "{\"b\":2}", NULL, 0, id);
    bf_fragment_create(store, BFK_EVENT, "p1", 0.8f, 6000, 8000, "{\"c\":3}", NULL, 0, id);

    bf_fragment_stats_t stats;
    int rc = bf_fragment_stats(store, &stats);
    assert(rc == 0 && "stats failed");
    assert(stats.total_fragments == 3 && "wrong total");
    assert(stats.unique_kinds == 2 && "wrong kinds count");
    assert(stats.unique_perspectives == 2 && "wrong persp count");
    printf("    total=%d kinds=%d perspectives=%d mean_conf=%.2f\n",
           stats.total_fragments, stats.unique_kinds,
           stats.unique_perspectives, (double)stats.mean_confidence);
    PASS("stats returns correct aggregate values");

    bf_fragment_store_close(store);
    printf("\n");
}

int main(void) {
    printf("═══════════════════════════════════════════════════════\n");
    printf("libfragment — Unit Tests\n");
    printf("═══════════════════════════════════════════════════════\n\n");

    test_create_get();
    test_query();
    test_merge();
    test_diff();
    test_stats();

    printf("═══════════════════════════════════════════════════════\n");
    printf("All tests passed ✓\n");
    printf("═══════════════════════════════════════════════════════\n");
    return 0;
}
